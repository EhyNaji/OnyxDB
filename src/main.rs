mod commit_coordinator;
mod config;
mod persistence;
use bytes::Bytes;
use commit_coordinator::{MasterCommitCoordinator, ObpMutationResult};
use config::{FsyncPolicy, ServerConfig, UpstreamCredentials};
#[cfg(test)]
use flate2::Compression;
#[cfg(test)]
use flate2::write::GzEncoder;
use onyxdb::clock::unix_seconds as now;
use onyxdb::command::is_write_command;
#[cfg(test)]
use onyxdb::engine::EvictionPolicy;
use onyxdb::engine::{DataEntry, OnyxValue};
use onyxdb::execution::{
    CommandOutcome, MutationState, affected_keys as persistent_keys_for_command,
    execute_command as execute_data_command,
};
use onyxdb::protocol::{MAX_OBP_FRAME_SIZE, OBPFrame};
use onyxdb::resp::{
    CLIENT_RESP_LIMITS, RESPReadLimits, RESPValue, decode_buffered_command,
    read_command_with_timeouts,
};
#[cfg(test)]
use onyxdb::store::StoreError;
use onyxdb::store::{MAX_KEYS, MutationRollback, ShardedStore};
#[cfg(test)]
use onyxdb::{protocol, resp};
use persistence::*;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
#[cfg(test)]
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{
    AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as TokioBufReader,
    BufWriter as TokioBufWriter,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
#[cfg(test)]
use tokio::sync::oneshot;
use tracing::{error, info, warn};

// High-performance allocator.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const RUNTIME_LOCK_PATH: &str = "onyx.lock";
const REPLICATION_CHUNK_SIZE: usize = 256 * 1024;
const MAX_REPLICATION_FRAME_BULK_SIZE: i64 = (REPLICATION_CHUNK_SIZE * 2 + 64) as i64;
const COMPACTION_THRESHOLD: usize = 100000;
const CLIENT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const CLIENT_FRAME_TIMEOUT: Duration = Duration::from_secs(30);
const OBP_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const OBP_FRAME_TIMEOUT: Duration = Duration::from_secs(30);
const REPLICATION_FRAME_TIMEOUT: Duration = Duration::from_secs(30);
const REPLICATION_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const REPLICATION_IDLE_TIMEOUT: Duration = Duration::from_secs(5);
const REPLICATION_TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const REPLICATION_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const REPLICA_ACK_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_TRANSACTION_COMMANDS: usize = 1024;
const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;
const MAX_PIPELINED_COMMIT_COMMANDS: usize = 64;

#[derive(Default)]
struct TransactionQueue {
    commands: Vec<Vec<String>>,
    encoded_bytes: usize,
    failed: bool,
}

impl TransactionQueue {
    fn enqueue(&mut self, command: Vec<String>) -> Result<(), &'static str> {
        if self.failed {
            return Err("ERR transaction queue is already invalid");
        }
        if self.commands.len() >= MAX_TRANSACTION_COMMANDS {
            self.failed = true;
            return Err("ERR transaction queue command limit exceeded");
        }
        let command_bytes = command.iter().try_fold(16usize, |total, argument| {
            total.checked_add(argument.len().saturating_add(16))
        });
        let Some(projected_bytes) =
            command_bytes.and_then(|command_bytes| self.encoded_bytes.checked_add(command_bytes))
        else {
            self.failed = true;
            return Err("ERR transaction queue byte limit exceeded");
        };
        if projected_bytes > MAX_TRANSACTION_BYTES {
            self.failed = true;
            return Err("ERR transaction queue byte limit exceeded");
        }
        self.encoded_bytes = projected_bytes;
        self.commands.push(command);
        Ok(())
    }
}

struct RuntimeDirectoryLock {
    file: File,
    directory: PathBuf,
}

impl RuntimeDirectoryLock {
    fn acquire(directory: &Path) -> Result<Self, PersistenceError> {
        fs::create_dir_all(directory).map_err(|error| {
            PersistenceError::new(format!(
                "Unable to create data directory {}: {}",
                directory.display(),
                error
            ))
        })?;
        let directory = fs::canonicalize(directory).map_err(|error| {
            PersistenceError::new(format!(
                "Unable to resolve data directory {}: {}",
                directory.display(),
                error
            ))
        })?;
        let lock_path = directory.join(RUNTIME_LOCK_PATH);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|error| {
                PersistenceError::new(format!(
                    "Unable to open runtime lock {}: {}",
                    lock_path.display(),
                    error
                ))
            })?;
        file.try_lock().map_err(|error| {
            PersistenceError::new(format!(
                "Data directory {} is already owned by another OnyxDB process: {}",
                directory.display(),
                error
            ))
        })?;
        Ok(Self { file, directory })
    }

    fn directory(&self) -> &Path {
        &self.directory
    }
}

impl Drop for RuntimeDirectoryLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}
/// Configured users keyed by username. Authentication grants full command
/// access; command-level authorization is not implemented. `--requirepass`
/// and `ONYXDB_PASSWORD` remain compatible aliases for the `default` user.
static USERS: std::sync::OnceLock<std::collections::HashMap<String, String>> =
    std::sync::OnceLock::new();

fn auth_required() -> bool {
    USERS.get().map(|u| !u.is_empty()).unwrap_or(false)
}

fn check_credentials(username: &str, password: &str) -> bool {
    match USERS.get() {
        Some(users) => users.get(username).map(|p| p == password).unwrap_or(false),
        None => false,
    }
}

/// Opens or creates the binlog and retries transient I/O failures every three
/// seconds. This is intentionally blocking because it runs only at startup.
fn open_binlog_file(path: &Path) -> File {
    loop {
        match OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
        {
            Ok(f) => return f,
            Err(e) => {
                error!(
                    "Unable to open {} ({}). Retrying in 3s...",
                    path.display(),
                    e
                );
                std::thread::sleep(Duration::from_secs(3));
            }
        }
    }
}

// ============================================================
// MEMORY EVICTION — configurable dataset memory limit
// ============================================================
// Admission evaluates the authoritative post-mutation state. A value of zero
// disables the limit; otherwise the configured policy decides whether the
// command can evict unrelated keys or must fail without changing state.
static START_TIME: AtomicU64 = AtomicU64::new(0);
static IS_REPLICA: AtomicBool = AtomicBool::new(false);
// A master gets a new replication ID on every process start. The ID binds an
// offset to one specific master history so a replica cannot partially resume
// against an unrelated process. Zero means that no upstream identity is known.
static REPL_ID: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

fn repl_id() -> u64 {
    *REPL_ID.get().unwrap_or(&0)
}
static ACTIVE_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);
static TOTAL_COMMANDS: AtomicUsize = AtomicUsize::new(0);
static CACHE_HITS: AtomicUsize = AtomicUsize::new(0);
static CACHE_MISSES: AtomicUsize = AtomicUsize::new(0);

/// Maximum number of committed batches retained for partial synchronization.
const BACKLOG_CAPACITY: usize = 10_000;

/// Monitoring state for a connected replica.
struct ReplicaStatus {
    addr: String,
    last_ack_offset: u64,
    last_ack_time: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplicaLifecycleState {
    Running,
    Stopped,
    Failed,
}

struct ReplicaLifecycle {
    stop_tx: tokio::sync::watch::Sender<bool>,
    state_tx: tokio::sync::watch::Sender<ReplicaLifecycleState>,
}

impl ReplicaLifecycle {
    fn new(initially_stopped: bool) -> Self {
        let (stop_tx, _) = tokio::sync::watch::channel(false);
        let initial_state = if initially_stopped {
            ReplicaLifecycleState::Stopped
        } else {
            ReplicaLifecycleState::Running
        };
        let (state_tx, _) = tokio::sync::watch::channel(initial_state);
        Self { stop_tx, state_tx }
    }

    fn subscribe_stop(&self) -> tokio::sync::watch::Receiver<bool> {
        self.stop_tx.subscribe()
    }

    fn stop_requested(&self) -> bool {
        *self.stop_tx.borrow()
    }

    fn mark_running(&self) {
        self.state_tx.send_replace(ReplicaLifecycleState::Running);
    }

    fn mark_stopped(&self) {
        self.state_tx.send_replace(ReplicaLifecycleState::Stopped);
    }

    fn mark_failed(&self) {
        self.state_tx.send_replace(ReplicaLifecycleState::Failed);
    }

    fn request_stop(&self) {
        self.stop_tx.send_replace(true);
    }

    async fn wait_stopped(&self) -> Result<(), PersistenceError> {
        let mut state_rx = self.state_tx.subscribe();
        if *state_rx.borrow() == ReplicaLifecycleState::Running
            && state_rx
                .wait_for(|state| *state != ReplicaLifecycleState::Running)
                .await
                .is_err()
        {
            return Err(PersistenceError::new(
                "Replica lifecycle state channel closed before shutdown completed",
            ));
        }
        match *state_rx.borrow() {
            ReplicaLifecycleState::Stopped => Ok(()),
            ReplicaLifecycleState::Failed => Err(PersistenceError::new(
                "Replica lifecycle failed before upstream cleanup completed",
            )),
            ReplicaLifecycleState::Running => Err(PersistenceError::new(
                "Replica lifecycle did not reach a stopped state",
            )),
        }
    }

    async fn stop_and_wait(&self) -> Result<(), PersistenceError> {
        self.request_stop();
        self.wait_stopped().await
    }
}

struct ReplicaRunGuard(Arc<ReplicaLifecycle>);

impl Drop for ReplicaRunGuard {
    fn drop(&mut self) {
        // A panicked runner has not proven that every child task is quiescent.
        // Mark it failed so promotion is rejected instead of hanging.
        if std::thread::panicking() {
            self.0.mark_failed();
        } else {
            self.0.mark_stopped();
        }
    }
}

struct AbortTaskOnDrop<T> {
    handle: Option<tokio::task::JoinHandle<T>>,
}

impl<T> AbortTaskOnDrop<T> {
    fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    async fn abort_and_wait(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl<T> Drop for AbortTaskOnDrop<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.as_ref() {
            handle.abort();
        }
    }
}

struct Persistence {
    commit_runtime: CommitRuntime,
    master_commit: std::sync::OnceLock<MasterCommitCoordinator>,
    // Broadcasts each committed batch to connected replicas with its sequence.
    replica_tx: tokio::sync::broadcast::Sender<(u64, CommittedBatch)>,
    // Marks a replica that has crossed the authoritative promotion boundary.
    promote_to_master: Arc<AtomicBool>,
    // Recent committed batches retained for gap-free partial synchronization.
    backlog: std::sync::Mutex<std::collections::VecDeque<(u64, CommittedBatch)>>,
    next_replica_id: AtomicU64,
    replica_status: std::sync::Mutex<std::collections::HashMap<u64, ReplicaStatus>>,
    // One broadcast channel carries all ephemeral Pub/Sub messages; each
    // subscriber filters the channels it currently follows.
    pubsub_tx: tokio::sync::broadcast::Sender<(String, String)>,
    next_subscriber_id: AtomicU64,
    // Channel-to-subscriber IDs used to report PUBLISH recipient counts.
    subscriptions:
        std::sync::Mutex<std::collections::HashMap<String, std::collections::HashSet<u64>>>,
    upstream_replid: AtomicU64,
    replication_ready: AtomicBool,
    replica_lifecycle: Arc<ReplicaLifecycle>,
}

impl std::ops::Deref for Persistence {
    type Target = CommitRuntime;

    fn deref(&self) -> &Self::Target {
        &self.commit_runtime
    }
}

fn mark_persistence_failed(persistence: &Persistence, message: impl Into<String>) {
    let message = message.into();
    if let Ok(mut failure) = persistence.failure.lock()
        && failure.is_none()
    {
        *failure = Some(message.clone());
    }
    persistence.accepting_writes.store(false, Ordering::SeqCst);
    persistence.replication_ready.store(false, Ordering::SeqCst);
    error!("Persistence entered a failed state: {}", message);
}

fn enter_persistence_fail_stop_with_boundary(
    persistence: &Persistence,
    boundary: CommitBoundary,
    message: impl Into<String>,
) {
    persistence.replication_ready.store(false, Ordering::SeqCst);
    persistence.replica_lifecycle.request_stop();
    persistence.enter_fail_stop_with_boundary(boundary, message);
}

async fn enter_persistence_fail_stop(persistence: &Persistence, message: impl Into<String>) {
    persistence.replication_ready.store(false, Ordering::SeqCst);
    persistence.replica_lifecycle.request_stop();
    persistence.enter_fail_stop(message).await;
}

fn persistence_unavailable_message(persistence: &Persistence) -> String {
    let reason = persistence
        .failure
        .lock()
        .ok()
        .and_then(|failure| failure.clone());
    match reason {
        Some(reason) => format!("MISCONF persistence is unavailable: {}", reason),
        None => "MISCONF persistence is unavailable".to_string(),
    }
}

#[cfg(test)]
fn capture_entries(
    store: &ShardedStore,
    keys: &[Bytes],
) -> std::collections::HashMap<Bytes, Option<DataEntry>> {
    keys.iter()
        .map(|key| (key.clone(), store.peek_entry(key)))
        .collect()
}

fn replid_allows_partial(requested_replid: u64, current_replid: u64) -> bool {
    requested_replid != 0 && requested_replid == current_replid
}
/// Determines whether retained history covers a requested sequence without gaps.
fn partial_resync_possible(
    requested_offset: u64,
    backlog_oldest: Option<u64>,
    current_repl_offset: u64,
) -> bool {
    if requested_offset > current_repl_offset {
        return false;
    }
    if requested_offset == current_repl_offset {
        return true;
    }
    match backlog_oldest {
        Some(oldest) => requested_offset
            .checked_add(1)
            .is_some_and(|next_offset| oldest <= next_offset),
        None => false,
    }
}

fn execute_command(store: &ShardedStore, args: &[String]) -> CommandOutcome {
    if args.first().is_some_and(|command| command == "INFO") {
        return CommandOutcome::read(info_response(store));
    }
    execute_data_command(store, args)
}

fn info_response(store: &ShardedStore) -> RESPValue {
    let uptime = now().saturating_sub(START_TIME.load(Ordering::Relaxed));
    let role = if IS_REPLICA.load(Ordering::Relaxed) {
        "replica"
    } else {
        "master"
    };
    let num_keys = store.stats().total_keys;
    let active_conns = ACTIVE_CONNECTIONS.load(Ordering::Relaxed);
    let total_cmds = TOTAL_COMMANDS.load(Ordering::Relaxed);
    let hits = CACHE_HITS.load(Ordering::Relaxed);
    let misses = CACHE_MISSES.load(Ordering::Relaxed);
    let hit_rate = if hits + misses > 0 {
        (hits as f64 / (hits + misses) as f64) * 100.0
    } else {
        0.0
    };
    let used_memory = store.used_memory_bytes();
    let mm_limit = store.maxmemory_bytes();
    let mm_policy_str = format!("{:?}", store.maxmemory_policy());

    let info_text = format!(
        "role:{}\\nuptime_seconds:{}\\nconnected_keys:{}\\nmax_keys:{}\\nactive_connections:{}\\ntotal_commands:{}\\ncache_hits:{}\\ncache_misses:{}\\nhit_rate_percent:{:.1}\\nused_memory_bytes:{}\\nmaxmemory_bytes:{}\\nmaxmemory_policy:{}",
        role,
        uptime,
        num_keys,
        MAX_KEYS,
        active_conns,
        total_cmds,
        hits,
        misses,
        hit_rate,
        used_memory,
        mm_limit,
        mm_policy_str
    );
    RESPValue::BulkString(Some(info_text))
}
#[cfg(test)]
fn normalize_for_log(store: &ShardedStore, args: &[String]) -> Vec<String> {
    let cmd = args.first().map(|s| s.as_str()).unwrap_or("");
    let key = args.get(1).map(|s| s.as_str()).unwrap_or("");

    if cmd == "EXPIRE"
        && let Some(exp) = store.get_expiry(key)
    {
        return vec!["EXPIREAT".to_string(), key.to_string(), exp.to_string()];
    }
    if cmd == "SET" && args.len() > 3 {
        // Normalize relative EX/PX expirations to EXAT so persistence and
        // replication reproduce the original absolute deadline.
        if let Some(exp) = store.get_expiry(key) {
            let value = args.get(2).map(|s| s.as_str()).unwrap_or("");
            return vec![
                "SET".to_string(),
                key.to_string(),
                value.to_string(),
                "EXAT".to_string(),
                exp.to_string(),
            ];
        }
    }
    args.to_vec()
}

fn encode_replication_command(args: &[String]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        encoded.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        encoded.extend_from_slice(arg.as_bytes());
        encoded.extend_from_slice(b"\r\n");
    }
    encoded
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hex_decode(encoded: &str) -> Result<Vec<u8>, PersistenceError> {
    if !encoded.len().is_multiple_of(2) {
        return Err(PersistenceError::new(
            "Hex-encoded payload has an odd length",
        ));
    }
    let nibble = |value: u8| match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    };
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let high =
            nibble(pair[0]).ok_or_else(|| PersistenceError::new("Invalid hexadecimal payload"))?;
        let low =
            nibble(pair[1]).ok_or_else(|| PersistenceError::new("Invalid hexadecimal payload"))?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

struct ChunkedReplicationRecord {
    header: Vec<String>,
    chunk_command: &'static str,
    payload: Vec<u8>,
}

async fn write_replication_bytes_with_timeout<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    bytes: &[u8],
    timeout: Duration,
) -> std::io::Result<()> {
    tokio::time::timeout(timeout, writer.write_all(bytes))
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Replication peer write timeout",
            )
        })?
}

async fn write_replication_bytes<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    bytes: &[u8],
) -> std::io::Result<()> {
    write_replication_bytes_with_timeout(writer, bytes, REPLICATION_WRITE_TIMEOUT).await
}

async fn flush_replication_writer<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
) -> std::io::Result<()> {
    tokio::time::timeout(REPLICATION_WRITE_TIMEOUT, writer.flush())
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Replication peer flush timeout",
            )
        })?
}

async fn write_chunked_replication_record<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    record: &ChunkedReplicationRecord,
) -> std::io::Result<()> {
    write_replication_bytes(writer, &encode_replication_command(&record.header)).await?;
    for chunk in record.payload.chunks(REPLICATION_CHUNK_SIZE) {
        let frame =
            encode_replication_command(&[record.chunk_command.to_string(), hex_encode(chunk)]);
        write_replication_bytes(writer, &frame).await?;
    }
    Ok(())
}

fn encode_replication_effect(
    sequence: u64,
    batch: &CommittedBatch,
) -> Result<ChunkedReplicationRecord, PersistenceError> {
    if sequence == 0 {
        return Err(PersistenceError::new(
            "Replication effects require a non-zero sequence",
        ));
    }
    let payload = encode_committed_batch(batch)?;
    if payload.len() > MAX_BINLOG_RECORD_SIZE {
        return Err(PersistenceError::new(
            "Replication effect exceeds the format limit",
        ));
    }
    Ok(ChunkedReplicationRecord {
        header: vec![
            "APPLYEFFECT".to_string(),
            sequence.to_string(),
            payload.len().to_string(),
        ],
        chunk_command: "EFFECTCHUNK",
        payload,
    })
}

fn decode_replication_effect(
    sequence: &str,
    encoded: &[u8],
) -> Result<(u64, CommittedBatch), PersistenceError> {
    let sequence = sequence
        .parse::<u64>()
        .map_err(|_| PersistenceError::new("Invalid replication effect sequence"))?;
    if sequence == 0 {
        return Err(PersistenceError::new(
            "Replication effects require a non-zero sequence",
        ));
    }
    Ok((sequence, decode_committed_batch(encoded)?))
}

fn encode_full_sync_entry(
    key: &[u8],
    entry: &DataEntry,
) -> Result<ChunkedReplicationRecord, PersistenceError> {
    let payload = encode_snapshot_entry(key, entry)?;
    Ok(ChunkedReplicationRecord {
        header: vec!["FULLSYNCENTRY".to_string(), payload.len().to_string()],
        chunk_command: "FULLSYNCCHUNK",
        payload,
    })
}

async fn read_replication_command(
    reader: &mut TokioBufReader<tokio::net::tcp::OwnedReadHalf>,
    scratch: &mut Vec<u8>,
) -> std::io::Result<Option<Vec<String>>> {
    read_replication_command_with_idle(reader, scratch, REPLICATION_TRANSFER_IDLE_TIMEOUT).await
}

async fn read_replication_command_with_idle(
    reader: &mut TokioBufReader<tokio::net::tcp::OwnedReadHalf>,
    scratch: &mut Vec<u8>,
    idle_timeout: Duration,
) -> std::io::Result<Option<Vec<String>>> {
    read_command_with_timeouts(
        reader,
        scratch,
        RESPReadLimits {
            max_array_len: 4,
            max_bulk_len: MAX_REPLICATION_FRAME_BULK_SIZE as usize,
            max_inline_len: 1024,
            max_frame_len: MAX_REPLICATION_FRAME_BULK_SIZE as usize + 4096,
        },
        Some(idle_timeout),
        REPLICATION_FRAME_TIMEOUT,
    )
    .await
}

async fn read_chunked_replication_payload(
    reader: &mut TokioBufReader<tokio::net::tcp::OwnedReadHalf>,
    scratch: &mut Vec<u8>,
    expected_length: usize,
    maximum_length: usize,
    chunk_command: &str,
) -> Result<Vec<u8>, PersistenceError> {
    if expected_length == 0 || expected_length > maximum_length {
        return Err(PersistenceError::new(
            "Replication record length exceeds the format limit",
        ));
    }
    let mut payload = Vec::with_capacity(expected_length.min(REPLICATION_CHUNK_SIZE));
    while payload.len() < expected_length {
        let frame = match read_replication_command(reader, scratch).await {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                return Err(PersistenceError::upstream_unavailable(
                    "Master disconnected during a chunked replication record",
                ));
            }
            Err(error) => {
                return Err(upstream_io_persistence_error(
                    "Unable to read a replication chunk",
                    error,
                ));
            }
        };
        if frame.len() != 2 || frame[0] != chunk_command {
            return Err(PersistenceError::new(
                "Master sent an unexpected replication chunk frame",
            ));
        }
        let chunk = hex_decode(&frame[1])?;
        if chunk.is_empty() || chunk.len() > REPLICATION_CHUNK_SIZE {
            return Err(PersistenceError::new(
                "Replication chunk length exceeds the protocol limit",
            ));
        }
        let new_length = payload
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| PersistenceError::new("Replication record length overflow"))?;
        if new_length > expected_length {
            return Err(PersistenceError::new(
                "Replication chunks exceed the declared record length",
            ));
        }
        payload.extend_from_slice(&chunk);
    }
    Ok(payload)
}

async fn read_full_sync_entry(
    reader: &mut TokioBufReader<tokio::net::tcp::OwnedReadHalf>,
    scratch: &mut Vec<u8>,
) -> Result<(Bytes, DataEntry), PersistenceError> {
    let header = match read_replication_command(reader, scratch).await {
        Ok(Some(header)) => header,
        Ok(None) => {
            return Err(PersistenceError::upstream_unavailable(
                "Master disconnected during full synchronization",
            ));
        }
        Err(error) => {
            return Err(upstream_io_persistence_error(
                "Unable to read full synchronization entry",
                error,
            ));
        }
    };
    if header.len() != 2 || header[0] != "FULLSYNCENTRY" {
        return Err(PersistenceError::new(
            "Master sent an unexpected full synchronization frame",
        ));
    }
    let expected_length = header[1]
        .parse::<usize>()
        .map_err(|_| PersistenceError::new("Invalid full synchronization entry length"))?;
    let payload = read_chunked_replication_payload(
        reader,
        scratch,
        expected_length,
        MAX_SNAPSHOT_RECORD_SIZE,
        "FULLSYNCCHUNK",
    )
    .await?;
    decode_snapshot_entry(&payload)
}

async fn read_replication_effect(
    header: &[String],
    reader: &mut TokioBufReader<tokio::net::tcp::OwnedReadHalf>,
    scratch: &mut Vec<u8>,
) -> Result<(u64, CommittedBatch), PersistenceError> {
    if header.len() != 3 || header[0] != "APPLYEFFECT" {
        return Err(PersistenceError::new(
            "Master sent an unexpected replication effect frame",
        ));
    }
    let expected_length = header[2]
        .parse::<usize>()
        .map_err(|_| PersistenceError::new("Invalid replication effect length"))?;
    let payload = read_chunked_replication_payload(
        reader,
        scratch,
        expected_length,
        MAX_BINLOG_RECORD_SIZE,
        "EFFECTCHUNK",
    )
    .await?;
    decode_replication_effect(&header[1], &payload)
}

async fn handle_client(stream: TcpStream, store: Arc<ShardedStore>, persistence: Arc<Persistence>) {
    let _ = stream.set_nodelay(true);
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    ACTIVE_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
    struct ConnGuard;
    impl Drop for ConnGuard {
        fn drop(&mut self) {
            ACTIVE_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
        }
    }
    let _guard = ConnGuard;
    let (reader, writer) = stream.into_split();
    let mut buf_reader = TokioBufReader::with_capacity(65536, reader);
    let mut buf_writer = TokioBufWriter::with_capacity(65536, writer);
    let mut scratch = Vec::with_capacity(256);
    let mut resp_buf = String::with_capacity(256);
    let mut authenticated = !auth_required();
    let mut transaction: Option<TransactionQueue> = None;
    let mut buffered_protocol_error = None;

    loop {
        if let Some(error) = buffered_protocol_error.take() {
            warn!("Closing RESP connection from {}: {}", peer_addr, error);
            resp_buf.clear();
            RESPValue::Error(format!("ERR Protocol error: {}", error)).encode_into(&mut resp_buf);
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            break;
        }
        let mut args = match read_command_with_timeouts(
            &mut buf_reader,
            &mut scratch,
            CLIENT_RESP_LIMITS,
            Some(CLIENT_IDLE_TIMEOUT),
            CLIENT_FRAME_TIMEOUT,
        )
        .await
        {
            Ok(Some(args)) if !args.is_empty() => args,
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::InvalidData
                        | std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::TimedOut
                ) =>
            {
                warn!("Closing RESP connection from {}: {}", peer_addr, error);
                resp_buf.clear();
                RESPValue::Error(format!("ERR Protocol error: {}", error))
                    .encode_into(&mut resp_buf);
                let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
                let _ = buf_writer.flush().await;
                break;
            }
            Err(_) => break,
        };

        // Normalize the command name once so dispatch, transactions, and
        // replication all observe the same case-insensitive command identity.
        args[0].make_ascii_uppercase();
        let cmd = args[0].as_str();

        // AUTH accepts either `AUTH password` or `AUTH username password`.
        if cmd.eq_ignore_ascii_case("AUTH") {
            let (username, provided_password) = if args.len() >= 3 {
                (args[1].clone(), args[2].clone())
            } else {
                (
                    "default".to_string(),
                    args.get(1).cloned().unwrap_or_default(),
                )
            };
            let response = if !auth_required() {
                RESPValue::Error("ERR no password configured on this server".to_string())
            } else if check_credentials(&username, &provided_password) {
                authenticated = true;
                RESPValue::SimpleString("OK".to_string())
            } else {
                RESPValue::Error("WRONGPASS invalid username or password".to_string())
            };
            resp_buf.clear();
            response.encode_into(&mut resp_buf);
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            continue;
        }
        // No command, including replication and Pub/Sub, bypasses authentication.
        if !authenticated {
            resp_buf.clear();
            RESPValue::Error("NOAUTH authentication required. Use AUTH password".to_string())
                .encode_into(&mut resp_buf);
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            continue;
        }
        // MULTI

        if cmd.eq_ignore_ascii_case("MULTI") {
            resp_buf.clear();
            if transaction.is_some() {
                RESPValue::Error("ERR MULTI calls cannot be nested".to_string())
                    .encode_into(&mut resp_buf);
            } else {
                transaction = Some(TransactionQueue::default());
                RESPValue::SimpleString("OK".to_string()).encode_into(&mut resp_buf);
            }
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            continue;
        }

        // DISCARD
        if cmd.eq_ignore_ascii_case("DISCARD") {
            let response = if transaction.take().is_some() {
                RESPValue::SimpleString("OK".to_string())
            } else {
                RESPValue::Error(
                    "ERR DISCARD without an active transaction (use MULTI first)".to_string(),
                )
            };
            resp_buf.clear();
            response.encode_into(&mut resp_buf);
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            continue;
        }

        // EXEC
        if cmd.eq_ignore_ascii_case("EXEC") {
            let response = match transaction.take() {
                None => RESPValue::Error(
                    "ERR EXEC without an active transaction (use MULTI first)".to_string(),
                ),
                Some(transaction) if transaction.failed => RESPValue::Error(
                    "EXECABORT transaction discarded because its queue limit was exceeded"
                        .to_string(),
                ),
                Some(transaction) => {
                    execute_transaction(
                        &store,
                        &persistence,
                        transaction.commands,
                        IS_REPLICA.load(Ordering::SeqCst),
                    )
                    .await
                }
            };
            resp_buf.clear();
            response.encode_into(&mut resp_buf);
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            continue;
        }

        // Queue commands while a transaction is active.
        if let Some(transaction) = transaction.as_mut() {
            resp_buf.clear();
            match transaction.enqueue(args.clone()) {
                Ok(()) => RESPValue::SimpleString("QUEUED".to_string()).encode_into(&mut resp_buf),
                Err(message) => RESPValue::Error(message.to_string()).encode_into(&mut resp_buf),
            }
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            continue;
        }

        // Pub/Sub messages are ephemeral and never enter persistence.
        if cmd == "PUBLISH" {
            let channel = args.get(1).cloned().unwrap_or_default();
            let message = args.get(2).cloned().unwrap_or_default();
            let receiver_count = persistence
                .subscriptions
                .lock()
                .unwrap()
                .get(&channel)
                .map(|s| s.len())
                .unwrap_or(0);
            let _ = persistence.pubsub_tx.send((channel, message));
            resp_buf.clear();
            RESPValue::Integer(receiver_count as i64).encode_into(&mut resp_buf);
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            continue;
        }

        // SUBSCRIBE enters RESP2-style Pub/Sub mode for this connection.
        if cmd == "SUBSCRIBE" {
            let sub_id = persistence
                .next_subscriber_id
                .fetch_add(1, Ordering::SeqCst)
                + 1;
            let mut my_channels: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            for channel in args[1..].iter().cloned() {
                my_channels.insert(channel.clone());
                persistence
                    .subscriptions
                    .lock()
                    .unwrap()
                    .entry(channel.clone())
                    .or_default()
                    .insert(sub_id);
                let count = my_channels.len();
                let confirm = RESPValue::Array(vec![
                    RESPValue::BulkString(Some("subscribe".to_string())),
                    RESPValue::BulkString(Some(channel)),
                    RESPValue::Integer(count as i64),
                ]);
                resp_buf.clear();
                confirm.encode_into(&mut resp_buf);
                if buf_writer.write_all(resp_buf.as_bytes()).await.is_err() {
                    return;
                }
            }
            let _ = buf_writer.flush().await;

            let mut pubsub_rx = persistence.pubsub_tx.subscribe();

            // A dedicated task owns the read half for later subscription
            // changes while the main loop forwards published messages.
            let (chan_tx, mut chan_rx) =
                tokio::sync::mpsc::unbounded_channel::<(bool, Vec<String>)>();
            let reader_task = tokio::spawn(async move {
                let mut sub_scratch = Vec::new();
                loop {
                    match read_command_with_timeouts(
                        &mut buf_reader,
                        &mut sub_scratch,
                        CLIENT_RESP_LIMITS,
                        None,
                        CLIENT_FRAME_TIMEOUT,
                    )
                    .await
                    {
                        Ok(Some(sub_args)) if !sub_args.is_empty() => {
                            let sub_cmd = sub_args[0].to_ascii_uppercase();
                            if sub_cmd == "SUBSCRIBE" {
                                let _ = chan_tx.send((true, sub_args[1..].to_vec()));
                            } else if sub_cmd == "UNSUBSCRIBE" {
                                let chans = if sub_args.len() > 1 {
                                    sub_args[1..].to_vec()
                                } else {
                                    Vec::new()
                                };
                                let _ = chan_tx.send((false, chans));
                            }
                            // RESP2-style Pub/Sub mode ignores unrelated commands.
                        }
                        Ok(Some(_)) => continue,
                        Ok(None) | Err(_) => break,
                    }
                }
            });

            loop {
                tokio::select! {
                    update = chan_rx.recv() => {
                        let (is_subscribe, channels) = match update {
                            Some(u) => u,
                            None => break, // The client closed the connection.
                        };
                        if is_subscribe {
                            for channel in channels {
                                if my_channels.insert(channel.clone()) {
                                    persistence.subscriptions.lock().unwrap()
                                        .entry(channel.clone()).or_default()
                                        .insert(sub_id);
                                }
                                let count = my_channels.len();
                                let confirm = RESPValue::Array(vec![
                                    RESPValue::BulkString(Some("subscribe".to_string())),
                                    RESPValue::BulkString(Some(channel)),
                                    RESPValue::Integer(count as i64),
                                ]);
                                resp_buf.clear();
                                confirm.encode_into(&mut resp_buf);
                                if buf_writer.write_all(resp_buf.as_bytes()).await.is_err() { break; }
                            }
                            let _ = buf_writer.flush().await;
                        } else {
                            let targets: Vec<String> = if channels.is_empty() {
                                my_channels.iter().cloned().collect()
                            } else {
                                channels
                            };
                            for channel in targets {
                                my_channels.remove(&channel);
                                if let Some(set) = persistence.subscriptions.lock().unwrap().get_mut(&channel) {
                                    set.remove(&sub_id);
                                }
                                let count = my_channels.len();
                                let confirm = RESPValue::Array(vec![
                                    RESPValue::BulkString(Some("unsubscribe".to_string())),
                                    RESPValue::BulkString(Some(channel)),
                                    RESPValue::Integer(count as i64),
                                ]);
                                resp_buf.clear();
                                confirm.encode_into(&mut resp_buf);
                                if buf_writer.write_all(resp_buf.as_bytes()).await.is_err() { break; }
                            }
                            let _ = buf_writer.flush().await;
                        }
                    }
                    msg = pubsub_rx.recv() => {
                        match msg {
                            Ok((channel, payload)) => {
                                if my_channels.contains(&channel) {
                                    let out = RESPValue::Array(vec![
                                        RESPValue::BulkString(Some("message".to_string())),
                                        RESPValue::BulkString(Some(channel)),
                                        RESPValue::BulkString(Some(payload)),
                                    ]);
                                    resp_buf.clear();
                                    out.encode_into(&mut resp_buf);
                                    if buf_writer.write_all(resp_buf.as_bytes()).await.is_err() { break; }
                                    let _ = buf_writer.flush().await;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                warn!("Subscriber {} too slow, some messages lost", sub_id);
                            }
                            Err(_) => break,
                        }
                    }
                }
            }

            reader_task.abort();
            {
                let mut subs = persistence.subscriptions.lock().unwrap();
                for channel in &my_channels {
                    if let Some(set) = subs.get_mut(channel) {
                        set.remove(&sub_id);
                    }
                }
            }
            return;
        }

        if cmd == "SYNC" {
            resp_buf.clear();
            RESPValue::Error(
                "ERR replication protocol version 3 is required; use SYNC3".to_string(),
            )
            .encode_into(&mut resp_buf);
            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
            let _ = buf_writer.flush().await;
            return;
        }

        // Versioned replication handshake: SYNC3 <replid> <offset>.
        if cmd == "SYNC3" {
            if IS_REPLICA.load(Ordering::SeqCst) {
                resp_buf.clear();
                RESPValue::Error("ERR chained replication is not supported".to_string())
                    .encode_into(&mut resp_buf);
                let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
                let _ = buf_writer.flush().await;
                return;
            }
            let (requested_replid, requested_offset, heartbeat_enabled) = match args.as_slice() {
                [_, replid, offset] => match (replid.parse::<u64>(), offset.parse::<u64>()) {
                    (Ok(replid), Ok(offset)) => (replid, offset, false),
                    _ => {
                        resp_buf.clear();
                        RESPValue::Error(
                            "ERR SYNC3 requires numeric replication ID and sequence".to_string(),
                        )
                        .encode_into(&mut resp_buf);
                        let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
                        let _ = buf_writer.flush().await;
                        return;
                    }
                },
                [_, replid, offset, capability] if capability.eq_ignore_ascii_case("HEARTBEAT") => {
                    match (replid.parse::<u64>(), offset.parse::<u64>()) {
                        (Ok(replid), Ok(offset)) => (replid, offset, true),
                        _ => {
                            resp_buf.clear();
                            RESPValue::Error(
                                "ERR SYNC3 requires numeric replication ID and sequence"
                                    .to_string(),
                            )
                            .encode_into(&mut resp_buf);
                            let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
                            let _ = buf_writer.flush().await;
                            return;
                        }
                    }
                }
                _ => {
                    resp_buf.clear();
                    RESPValue::Error("ERR usage: SYNC3 replid sequence [HEARTBEAT]".to_string())
                        .encode_into(&mut resp_buf);
                    let _ = buf_writer.write_all(resp_buf.as_bytes()).await;
                    let _ = buf_writer.flush().await;
                    return;
                }
            };
            let replid_matches = replid_allows_partial(requested_replid, repl_id());
            let replica_id = persistence.next_replica_id.fetch_add(1, Ordering::SeqCst) + 1;

            if requested_replid != 0 && !replid_matches {
                info!(
                    "Replica {} presents a different replication ID from the current one (likely master restart): forcing a full dump",
                    peer_addr
                );
            }

            // Subscribe before capturing backlog or snapshot so concurrent
            // effects are either included in the boundary or queued afterward.
            let mut replica_rx = persistence.replica_tx.subscribe();

            // Partial synchronization is valid only for the same master
            // identity and a gap-free retained backlog.
            let backlog_snapshot: Option<Vec<(u64, CommittedBatch)>> =
                if replid_matches && requested_offset > 0 {
                    let _write_guard = persistence.write_gate.lock().await;
                    let backlog = persistence.backlog.lock().unwrap();
                    let backlog_oldest = backlog.front().map(|(off, _)| *off);
                    let current_offset = persistence.sequence();
                    if partial_resync_possible(requested_offset, backlog_oldest, current_offset) {
                        Some(
                            backlog
                                .iter()
                                .filter(|(off, _)| *off > requested_offset)
                                .cloned()
                                .collect(),
                        )
                    } else {
                        None
                    }
                } else {
                    None
                };

            persistence.replica_status.lock().unwrap().insert(
                replica_id,
                ReplicaStatus {
                    addr: peer_addr.clone(),
                    last_ack_offset: requested_offset,
                    last_ack_time: now(),
                },
            );

            // The ACK task exclusively owns the connection's read half.
            // read_line is not cancellation-safe, so deliberately avoid a
            // select loop that could discard half a line and corrupt the
            // replication stream.
            let persistence_ack = Arc::clone(&persistence);
            let mut reader_task = tokio::spawn(async move {
                let mut ack_scratch = Vec::new();
                loop {
                    match read_command_with_timeouts(
                        &mut buf_reader,
                        &mut ack_scratch,
                        RESPReadLimits {
                            max_array_len: 3,
                            max_bulk_len: 64,
                            max_inline_len: 128,
                            max_frame_len: 512,
                        },
                        Some(REPLICA_ACK_IDLE_TIMEOUT),
                        REPLICATION_FRAME_TIMEOUT,
                    )
                    .await
                    {
                        Ok(Some(ack_args)) if !ack_args.is_empty() => {
                            if ack_args[0].eq_ignore_ascii_case("REPLCONF")
                                && ack_args
                                    .get(1)
                                    .map(|s| s.eq_ignore_ascii_case("ACK"))
                                    .unwrap_or(false)
                                && let Some(off) =
                                    ack_args.get(2).and_then(|s| s.parse::<u64>().ok())
                                && let Some(status) = persistence_ack
                                    .replica_status
                                    .lock()
                                    .unwrap()
                                    .get_mut(&replica_id)
                            {
                                status.last_ack_offset = off;
                                status.last_ack_time = now();
                            }
                        }
                        Ok(Some(_)) => continue,
                        Ok(None) | Err(_) => break,
                    }
                }
            });

            let mut last_sent_offset;

            if let Some(missing) = backlog_snapshot {
                last_sent_offset = requested_offset;
                let marker = format!("+CONTINUE3 {} {}\r\n", repl_id(), requested_offset);
                if write_replication_bytes(&mut buf_writer, marker.as_bytes())
                    .await
                    .is_err()
                {
                    reader_task.abort();
                    persistence
                        .replica_status
                        .lock()
                        .unwrap()
                        .remove(&replica_id);
                    return;
                }
                for (off, batch) in missing {
                    let encoded = match encode_replication_effect(off, &batch) {
                        Ok(encoded) => encoded,
                        Err(error) => {
                            error!("Unable to encode replication effect: {}", error);
                            reader_task.abort();
                            persistence
                                .replica_status
                                .lock()
                                .unwrap()
                                .remove(&replica_id);
                            return;
                        }
                    };
                    if write_chunked_replication_record(&mut buf_writer, &encoded)
                        .await
                        .is_err()
                    {
                        reader_task.abort();
                        persistence
                            .replica_status
                            .lock()
                            .unwrap()
                            .remove(&replica_id);
                        return;
                    }
                    last_sent_offset = off;
                }
                info!(
                    "Replica {} partially synchronized from offset {}",
                    peer_addr, requested_offset
                );
            } else {
                let (full_sync_offset, snapshot_entries) = {
                    let _write_guard = persistence.write_gate.lock().await;
                    (persistence.sequence(), store.raw_entries())
                };
                let marker = format!(
                    "+FULLRESYNC3 {} {} {}\r\n",
                    repl_id(),
                    full_sync_offset,
                    snapshot_entries.len()
                );
                if write_replication_bytes(&mut buf_writer, marker.as_bytes())
                    .await
                    .is_err()
                {
                    reader_task.abort();
                    persistence
                        .replica_status
                        .lock()
                        .unwrap()
                        .remove(&replica_id);
                    return;
                }
                for (key, entry) in snapshot_entries {
                    let encoded_entry = match encode_full_sync_entry(&key, &entry) {
                        Ok(encoded) => encoded,
                        Err(error) => {
                            error!("Unable to encode full synchronization snapshot: {}", error);
                            reader_task.abort();
                            persistence
                                .replica_status
                                .lock()
                                .unwrap()
                                .remove(&replica_id);
                            return;
                        }
                    };
                    if write_chunked_replication_record(&mut buf_writer, &encoded_entry)
                        .await
                        .is_err()
                    {
                        reader_task.abort();
                        persistence
                            .replica_status
                            .lock()
                            .unwrap()
                            .remove(&replica_id);
                        return;
                    }
                }
                last_sent_offset = full_sync_offset;
                info!(
                    "Replica {} received a full synchronization snapshot at offset {}",
                    peer_addr, full_sync_offset
                );
            }

            let syncdone_marker = format!("+SYNCDONE3 {} {}\r\n", repl_id(), last_sent_offset);
            if write_replication_bytes(&mut buf_writer, syncdone_marker.as_bytes())
                .await
                .is_err()
            {
                reader_task.abort();
                persistence
                    .replica_status
                    .lock()
                    .unwrap()
                    .remove(&replica_id);
                return;
            }
            if flush_replication_writer(&mut buf_writer).await.is_err() {
                reader_task.abort();
                persistence
                    .replica_status
                    .lock()
                    .unwrap()
                    .remove(&replica_id);
                return;
            }

            info!(
                "Replica {} entered live streaming at sequence {}",
                peer_addr, last_sent_offset
            );

            let mut heartbeat = tokio::time::interval_at(
                tokio::time::Instant::now() + REPLICATION_HEARTBEAT_INTERVAL,
                REPLICATION_HEARTBEAT_INTERVAL,
            );
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = &mut reader_task => {
                        warn!(
                            "Replica {} stopped sending valid acknowledgements; closing the replication stream",
                            peer_addr
                        );
                        break;
                    }
                    _ = heartbeat.tick(), if heartbeat_enabled => {
                        let frame = format!("REPLCONF PING {}\r\n", last_sent_offset);
                        if write_replication_bytes(&mut buf_writer, frame.as_bytes()).await.is_err()
                            || flush_replication_writer(&mut buf_writer).await.is_err()
                        {
                            break;
                        }
                    }
                    replication = replica_rx.recv() => {
                        match replication {
                            Ok((offset, batch)) => {
                                if offset <= last_sent_offset {
                                    // The initial backlog or snapshot already covers
                                    // this effect from the subscribe/capture window.
                                    continue;
                                }
                                let encoded = match encode_replication_effect(offset, &batch) {
                                    Ok(encoded) => encoded,
                                    Err(error) => {
                                        error!("Unable to encode live replication effect: {}", error);
                                        break;
                                    }
                                };
                                if write_chunked_replication_record(&mut buf_writer, &encoded)
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                                if flush_replication_writer(&mut buf_writer).await.is_err() {
                                    break;
                                }
                                last_sent_offset = offset;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                // A lagged broadcast receiver has an irrecoverable gap
                                // in this stream. Disconnect and negotiate from the
                                // durable sequence again.
                                warn!(
                                    "Replica {} lagged behind the live stream; forcing resynchronization",
                                    peer_addr
                                );
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                }
            }

            reader_task.abort();
            persistence
                .replica_status
                .lock()
                .unwrap()
                .remove(&replica_id);
            info!("Replica {} disconnected", peer_addr);
            return;
        }

        // Normal client command.
        if !IS_REPLICA.load(Ordering::SeqCst) && is_write_command(cmd) {
            let mut commands = Vec::with_capacity(MAX_PIPELINED_COMMIT_COMMANDS);
            let mut buffered_complete_barrier = false;
            commands.push(args);
            while commands.len() < MAX_PIPELINED_COMMIT_COMMANDS {
                match decode_buffered_command(buf_reader.buffer(), CLIENT_RESP_LIMITS) {
                    Ok(Some((mut next, consumed))) if !next.is_empty() => {
                        next[0].make_ascii_uppercase();
                        if !is_write_command(&next[0]) {
                            buffered_complete_barrier = true;
                            break;
                        }
                        buf_reader.consume(consumed);
                        commands.push(next);
                    }
                    Ok(Some(_)) => break,
                    Ok(None) => break,
                    Err(error) => {
                        buffered_protocol_error = Some(error.to_string());
                        buffered_complete_barrier = true;
                        break;
                    }
                }
            }

            TOTAL_COMMANDS.fetch_add(commands.len(), Ordering::Relaxed);
            let responses = execute_ordered_commands(&store, &persistence, commands).await;
            for response in responses {
                resp_buf.clear();
                response.into_response().encode_into(&mut resp_buf);
                if buf_writer.write_all(resp_buf.as_bytes()).await.is_err() {
                    return;
                }
            }
            if !buffered_complete_barrier && buf_writer.flush().await.is_err() {
                return;
            }
            continue;
        }

        let response = if cmd == "SAVE" {
            match compact_store(&store, &persistence).await {
                Ok(_) => RESPValue::SimpleString("OK".to_string()),
                Err(error) => RESPValue::Error(format!("ERR snapshot failed: {}", error)),
            }
        } else if IS_REPLICA.load(Ordering::Relaxed) && is_write_command(cmd) {
            // Replicas accept mutations only from their ordered upstream
            // stream. Direct client writes would silently diverge.
            RESPValue::Error("READONLY this instance is a read-only replica".to_string())
        } else if IS_REPLICA.load(Ordering::SeqCst)
            && cmd.eq_ignore_ascii_case("REPLICAOF")
            && args
                .get(1)
                .map(|s| s.eq_ignore_ascii_case("no"))
                .unwrap_or(false)
            && args
                .get(2)
                .map(|s| s.eq_ignore_ascii_case("one"))
                .unwrap_or(false)
        {
            TOTAL_COMMANDS.fetch_add(1, Ordering::Relaxed);
            match prepare_replica_promotion(&persistence).await {
                Ok(()) => {
                    info!("Received REPLICAOF NO ONE: promoting to master");
                    RESPValue::SimpleString("OK".to_string())
                }
                Err(error) => RESPValue::Error(format!("MISCONF promotion failed: {}", error)),
            }
        } else {
            TOTAL_COMMANDS.fetch_add(1, Ordering::Relaxed);
            let mut resp = execute_ordered_command(&store, &persistence, &args)
                .await
                .into_response();
            if cmd.eq_ignore_ascii_case("INFO")
                && let RESPValue::BulkString(Some(ref mut text)) = resp
            {
                let repl_offset = persistence.sequence();
                let statuses = persistence.replica_status.lock().unwrap();
                let connected_replicas = statuses.len();
                let max_lag = statuses
                    .values()
                    .map(|s| repl_offset.saturating_sub(s.last_ack_offset))
                    .max()
                    .unwrap_or(0);
                text.push_str(&format!(
                    "\nmaster_repl_offset:{}\nconnected_replicas:{}\nmax_replica_lag:{}",
                    repl_offset, connected_replicas, max_lag
                ));
                for (i, status) in statuses.values().enumerate() {
                    let lag = repl_offset.saturating_sub(status.last_ack_offset);
                    let last_ack_secs_ago = now().saturating_sub(status.last_ack_time);
                    text.push_str(&format!(
                        "\nslave{}:addr={},offset={},lag={},last_ack_secs_ago={}",
                        i, status.addr, status.last_ack_offset, lag, last_ack_secs_ago
                    ));
                }
                drop(statuses);
            }
            if !is_write_command(cmd) {
                match &resp {
                    RESPValue::BulkString(None) => {
                        CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
                    }
                    RESPValue::BulkString(Some(_)) => {
                        CACHE_HITS.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }
            resp
        };

        resp_buf.clear();
        response.encode_into(&mut resp_buf);
        let _ = buf_writer.write_all(resp_buf.as_bytes()).await;

        if buf_reader.buffer().is_empty() {
            let _ = buf_writer.flush().await;
        }
    }
    let _ = buf_writer.flush().await;
}

async fn active_expiration_task(store: Arc<ShardedStore>) {
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let count = store.gc_expired();
        if count > 0 {
            println!("GC: removed {} expired keys.", count);
        }
    }
}
async fn run_periodic_sync_once(persistence: &Persistence) -> Result<(), PersistenceError> {
    match persistence.binlog.sync_data().await {
        Ok(()) => Ok(()),
        Err(error) => {
            let message = format!("Periodic binlog sync failed: {}", error);
            if error.is_indeterminate() {
                enter_persistence_fail_stop(persistence, message).await;
            } else {
                mark_persistence_failed(persistence, message);
            }
            Err(error)
        }
    }
}

async fn compact_store(
    store: &Arc<ShardedStore>,
    persistence: &Arc<Persistence>,
) -> Result<u64, PersistenceError> {
    persistence
        .commit_runtime
        .compact(store, &persistence.upstream_replid)
        .await
}

async fn await_persistence_fail_stop(persistence: &Persistence) -> PersistenceError {
    let reason = persistence.wait_for_fail_stop().await;
    persistence.replication_ready.store(false, Ordering::SeqCst);
    persistence.replica_lifecycle.request_stop();
    error!(
        "Terminating without compaction after an indeterminate persistence outcome: {}",
        reason
    );
    PersistenceError::indeterminate(reason)
}

fn format_prometheus_metrics(store: &ShardedStore, persistence: &Persistence) -> String {
    let uptime = now().saturating_sub(START_TIME.load(Ordering::Relaxed));
    let num_keys = store.stats().total_keys;
    let active_conns = ACTIVE_CONNECTIONS.load(Ordering::Relaxed);
    let total_cmds = TOTAL_COMMANDS.load(Ordering::Relaxed);
    let hits = CACHE_HITS.load(Ordering::Relaxed);
    let misses = CACHE_MISSES.load(Ordering::Relaxed);
    let role_value = if IS_REPLICA.load(Ordering::Relaxed) {
        0
    } else {
        1
    };

    let repl_offset = persistence.sequence();
    let statuses = persistence.replica_status.lock().unwrap();
    let connected_replicas = statuses.len();
    let max_lag = statuses
        .values()
        .map(|s| repl_offset.saturating_sub(s.last_ack_offset))
        .max()
        .unwrap_or(0);
    drop(statuses);

    fn push_metric(
        output: &mut String,
        name: &str,
        help: &str,
        metric_type: &str,
        value: impl std::fmt::Display,
    ) {
        use std::fmt::Write as _;
        writeln!(output, "# HELP {name} {help}").expect("writing metrics to a string cannot fail");
        writeln!(output, "# TYPE {name} {metric_type}")
            .expect("writing metrics to a string cannot fail");
        writeln!(output, "{name} {value}").expect("writing metrics to a string cannot fail");
    }

    fn seconds(nanoseconds: u64) -> f64 {
        nanoseconds as f64 / 1_000_000_000.0
    }

    let mut output = String::with_capacity(12 * 1024);
    push_metric(
        &mut output,
        "onyxdb_uptime_seconds",
        "Server uptime in seconds",
        "counter",
        uptime,
    );
    push_metric(
        &mut output,
        "onyxdb_keys_total",
        "Number of currently present keys",
        "gauge",
        num_keys,
    );
    push_metric(
        &mut output,
        "onyxdb_active_connections",
        "Number of active client connections",
        "gauge",
        active_conns,
    );
    push_metric(
        &mut output,
        "onyxdb_commands_total",
        "Total number of executed commands",
        "counter",
        total_cmds,
    );
    push_metric(
        &mut output,
        "onyxdb_cache_hits_total",
        "Number of successful key reads",
        "counter",
        hits,
    );
    push_metric(
        &mut output,
        "onyxdb_cache_misses_total",
        "Number of reads for missing keys",
        "counter",
        misses,
    );
    push_metric(
        &mut output,
        "onyxdb_is_master",
        "1 when this instance is a master, 0 when it is a replica",
        "gauge",
        role_value,
    );
    push_metric(
        &mut output,
        "onyxdb_replication_offset",
        "Current committed replication sequence",
        "counter",
        repl_offset,
    );
    push_metric(
        &mut output,
        "onyxdb_connected_replicas",
        "Number of currently connected replicas",
        "gauge",
        connected_replicas,
    );
    push_metric(
        &mut output,
        "onyxdb_max_replica_lag",
        "Largest connected-replica sequence lag",
        "gauge",
        max_lag,
    );
    push_metric(
        &mut output,
        "onyxdb_memory_bytes",
        "Approximate logical dataset bytes",
        "gauge",
        store.used_memory_bytes(),
    );

    if let Some(coordinator) = persistence.master_commit.get() {
        let metrics = coordinator.metrics_snapshot();
        for (name, help, metric_type, value) in [
            (
                "onyxdb_commit_requests_total",
                "Commit requests admitted to the coordinator",
                "counter",
                metrics.requests_total,
            ),
            (
                "onyxdb_commit_queue_depth",
                "Commit requests currently waiting for coordinator execution",
                "gauge",
                metrics.queue_depth,
            ),
            (
                "onyxdb_commit_queue_depth_max",
                "Highest observed commit coordinator queue depth",
                "gauge",
                metrics.queue_depth_max,
            ),
            (
                "onyxdb_commit_groups_total",
                "Physical coordinator execution groups started",
                "counter",
                metrics.groups_total,
            ),
            (
                "onyxdb_commit_groups_completed_total",
                "Coordinator groups completed authoritatively",
                "counter",
                metrics.groups_completed_total,
            ),
            (
                "onyxdb_commit_groups_rejected_total",
                "Coordinator groups definitively rejected",
                "counter",
                metrics.groups_rejected_total,
            ),
            (
                "onyxdb_commit_groups_indeterminate_total",
                "Coordinator groups with indeterminate persistence outcomes",
                "counter",
                metrics.groups_indeterminate_total,
            ),
            (
                "onyxdb_commit_groups_interrupted_total",
                "Coordinator groups interrupted before an authoritative outcome",
                "counter",
                metrics.groups_interrupted_total,
            ),
            (
                "onyxdb_commit_groups_in_progress",
                "Coordinator groups currently in progress",
                "gauge",
                metrics.groups_in_progress,
            ),
            (
                "onyxdb_commit_group_requests_total",
                "Requests included across all coordinator groups",
                "counter",
                metrics.group_requests_total,
            ),
            (
                "onyxdb_commit_group_requests_max",
                "Largest observed coordinator group by request count",
                "gauge",
                metrics.group_requests_max,
            ),
            (
                "onyxdb_commit_group_input_bytes_total",
                "Estimated decoded input bytes included across coordinator groups",
                "counter",
                metrics.group_input_bytes_total,
            ),
            (
                "onyxdb_commit_group_input_bytes_max",
                "Largest observed coordinator group by estimated decoded input bytes",
                "gauge",
                metrics.group_input_bytes_max,
            ),
            (
                "onyxdb_commit_logical_batches_total",
                "Logical committed batches processed by the coordinator",
                "counter",
                metrics.logical_batches_total,
            ),
        ] {
            push_metric(&mut output, name, help, metric_type, value);
        }
        for (name, help, metric_type, value) in [
            (
                "onyxdb_commit_queue_wait_seconds_total",
                "Cumulative time requests spent waiting in the coordinator queue",
                "counter",
                seconds(metrics.queue_wait_nanoseconds_total),
            ),
            (
                "onyxdb_commit_queue_wait_seconds_max",
                "Longest observed coordinator queue wait",
                "gauge",
                seconds(metrics.queue_wait_nanoseconds_max),
            ),
            (
                "onyxdb_commit_group_duration_seconds_total",
                "Cumulative coordinator group execution time",
                "counter",
                seconds(metrics.group_duration_nanoseconds_total),
            ),
            (
                "onyxdb_commit_group_duration_seconds_max",
                "Longest observed coordinator group execution time",
                "gauge",
                seconds(metrics.group_duration_nanoseconds_max),
            ),
            (
                "onyxdb_commit_storage_duration_seconds_total",
                "Cumulative grouped persistence and publication wait time",
                "counter",
                seconds(metrics.storage_duration_nanoseconds_total),
            ),
            (
                "onyxdb_commit_storage_duration_seconds_max",
                "Longest observed grouped persistence and publication wait",
                "gauge",
                seconds(metrics.storage_duration_nanoseconds_max),
            ),
        ] {
            push_metric(&mut output, name, help, metric_type, value);
        }
    }

    let binlog = persistence.commit_runtime.binlog_metrics();
    for (name, help, metric_type, value) in [
        (
            "onyxdb_binlog_append_attempts_total",
            "Binlog append operations attempted",
            "counter",
            binlog.append_attempts_total,
        ),
        (
            "onyxdb_binlog_append_accepted_total",
            "Binlog append operations accepted",
            "counter",
            binlog.append_accepted_total,
        ),
        (
            "onyxdb_binlog_append_rejected_total",
            "Binlog append operations definitively rejected",
            "counter",
            binlog.append_rejected_total,
        ),
        (
            "onyxdb_binlog_append_indeterminate_total",
            "Binlog append operations with indeterminate outcomes",
            "counter",
            binlog.append_indeterminate_total,
        ),
        (
            "onyxdb_binlog_records_accepted_total",
            "Logical ONX4 records accepted by binlog append operations",
            "counter",
            binlog.records_accepted_total,
        ),
        (
            "onyxdb_binlog_bytes_accepted_total",
            "Framed ONX4 bytes accepted by binlog append operations",
            "counter",
            binlog.bytes_accepted_total,
        ),
        (
            "onyxdb_binlog_records_per_append_max",
            "Largest accepted binlog append by record count",
            "gauge",
            binlog.records_per_append_max,
        ),
        (
            "onyxdb_binlog_bytes_per_append_max",
            "Largest accepted binlog append by framed bytes",
            "gauge",
            binlog.bytes_per_append_max,
        ),
    ] {
        push_metric(&mut output, name, help, metric_type, value);
    }
    push_metric(
        &mut output,
        "onyxdb_binlog_append_ack_seconds_total",
        "Cumulative binlog append submission-to-acknowledgement time",
        "counter",
        seconds(binlog.append_ack_nanoseconds_total),
    );
    push_metric(
        &mut output,
        "onyxdb_binlog_append_ack_seconds_max",
        "Longest observed binlog append submission-to-acknowledgement time",
        "gauge",
        seconds(binlog.append_ack_nanoseconds_max),
    );

    let compaction = persistence.commit_runtime.compaction_metrics();
    push_metric(
        &mut output,
        "onyxdb_compaction_pending",
        "1 when automatic compaction is scheduled or active",
        "gauge",
        u8::from(persistence.compaction_pending.load(Ordering::Relaxed)),
    );
    for (name, help, metric_type, value) in [
        (
            "onyxdb_compaction_attempts_total",
            "Snapshot compaction attempts started",
            "counter",
            compaction.attempts_total,
        ),
        (
            "onyxdb_compaction_completed_total",
            "Snapshot compactions completed",
            "counter",
            compaction.completed_total,
        ),
        (
            "onyxdb_compaction_failed_total",
            "Snapshot compactions that failed or were interrupted",
            "counter",
            compaction.failed_total,
        ),
        (
            "onyxdb_compaction_in_progress",
            "Snapshot compaction attempts currently active or waiting for the write gate",
            "gauge",
            compaction.in_progress,
        ),
    ] {
        push_metric(&mut output, name, help, metric_type, value);
    }
    for (name, help, metric_type, value) in [
        (
            "onyxdb_compaction_duration_seconds_total",
            "Cumulative snapshot compaction duration",
            "counter",
            seconds(compaction.duration_nanoseconds_total),
        ),
        (
            "onyxdb_compaction_duration_seconds_last",
            "Duration of the most recently completed compaction attempt",
            "gauge",
            seconds(compaction.duration_nanoseconds_last),
        ),
        (
            "onyxdb_compaction_duration_seconds_max",
            "Longest observed compaction attempt",
            "gauge",
            seconds(compaction.duration_nanoseconds_max),
        ),
        (
            "onyxdb_compaction_gate_wait_seconds_total",
            "Cumulative time compaction attempts waited for the write gate",
            "counter",
            seconds(compaction.gate_wait_nanoseconds_total),
        ),
        (
            "onyxdb_compaction_gate_wait_seconds_max",
            "Longest observed compaction write-gate wait",
            "gauge",
            seconds(compaction.gate_wait_nanoseconds_max),
        ),
        (
            "onyxdb_compaction_barrier_seconds_total",
            "Cumulative pre-snapshot ordered binlog barrier time",
            "counter",
            seconds(compaction.barrier_nanoseconds_total),
        ),
        (
            "onyxdb_compaction_barrier_seconds_max",
            "Longest observed pre-snapshot ordered binlog barrier",
            "gauge",
            seconds(compaction.barrier_nanoseconds_max),
        ),
        (
            "onyxdb_compaction_snapshot_capture_seconds_total",
            "Cumulative in-memory snapshot capture time",
            "counter",
            seconds(compaction.snapshot_capture_nanoseconds_total),
        ),
        (
            "onyxdb_compaction_snapshot_capture_seconds_max",
            "Longest observed in-memory snapshot capture",
            "gauge",
            seconds(compaction.snapshot_capture_nanoseconds_max),
        ),
        (
            "onyxdb_compaction_snapshot_write_seconds_total",
            "Cumulative snapshot encoding and durable installation time",
            "counter",
            seconds(compaction.snapshot_write_nanoseconds_total),
        ),
        (
            "onyxdb_compaction_snapshot_write_seconds_max",
            "Longest observed snapshot encoding and durable installation",
            "gauge",
            seconds(compaction.snapshot_write_nanoseconds_max),
        ),
        (
            "onyxdb_compaction_rotation_seconds_total",
            "Cumulative replica-state update and binlog rotation time",
            "counter",
            seconds(compaction.rotation_nanoseconds_total),
        ),
        (
            "onyxdb_compaction_rotation_seconds_max",
            "Longest observed replica-state update and binlog rotation",
            "gauge",
            seconds(compaction.rotation_nanoseconds_max),
        ),
    ] {
        push_metric(&mut output, name, help, metric_type, value);
    }

    output
}

async fn run_metrics_server(
    listener: TcpListener,
    address: SocketAddr,
    store: Arc<ShardedStore>,
    persistence: Arc<Persistence>,
) {
    info!(
        "Prometheus metrics server listening on http://{}/metrics",
        address
    );

    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(_) => continue,
        };
        let store_clone = Arc::clone(&store);
        let persistence_clone = Arc::clone(&persistence);

        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;

            let _visibility_guard = persistence_clone.visibility_gate.read().await;
            if persistence_clone.is_fail_stopped() {
                return;
            }
            let body = format_prometheus_metrics(&store_clone, &persistence_clone);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}
// ============================================================
// OBP CONNECTION HANDLER
// ============================================================

async fn handle_obp_client(
    stream: TcpStream,
    store: Arc<ShardedStore>,
    persistence: Arc<Persistence>,
) {
    handle_obp_client_with_timeouts(
        stream,
        store,
        persistence,
        OBP_IDLE_TIMEOUT,
        OBP_FRAME_TIMEOUT,
    )
    .await;
}

async fn handle_obp_client_with_timeouts(
    stream: TcpStream,
    store: Arc<ShardedStore>,
    persistence: Arc<Persistence>,
    idle_timeout: Duration,
    frame_timeout: Duration,
) {
    let _ = stream.set_nodelay(true);
    let peer_address = stream.peer_addr().ok();
    let (reader, writer) = stream.into_split();
    let mut buf_reader = TokioBufReader::with_capacity(65536, reader);
    let mut buf_writer = TokioBufWriter::with_capacity(8192, writer);
    let mut buf = bytes::BytesMut::with_capacity(4096);
    let mut read_buffer = [0u8; 8192];
    let mut authenticated = !auth_required();
    let mut frame_started_at: Option<tokio::time::Instant> = None;
    'connection: loop {
        let read_timeout = frame_started_at.map_or(idle_timeout, |started_at| {
            frame_timeout.saturating_sub(started_at.elapsed())
        });
        if read_timeout.is_zero() {
            warn!("Closing OBP connection after frame assembly timeout");
            break;
        }
        match tokio::time::timeout(read_timeout, buf_reader.read(&mut read_buffer)).await {
            Err(_) => {
                let reason = if frame_started_at.is_some() {
                    "frame assembly timeout"
                } else {
                    "client idle timeout"
                };
                warn!("Closing OBP connection after {}", reason);
                break;
            }
            Ok(Ok(0)) => break,
            Ok(Ok(bytes_read)) => {
                frame_started_at.get_or_insert_with(tokio::time::Instant::now);
                if buf.len().saturating_add(bytes_read) > MAX_OBP_FRAME_SIZE + read_buffer.len() {
                    warn!("Closing OBP connection with an oversized incomplete frame");
                    break;
                }
                buf.extend_from_slice(&read_buffer[..bytes_read]);
            }
            Ok(Err(_)) => break,
        }

        let mut wrote_response = false;
        loop {
            let frame = match OBPFrame::decode(&mut buf) {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(error) => {
                    warn!(
                        "Closing malformed OBP connection{}: {}",
                        peer_address
                            .map(|address| format!(" from {address}"))
                            .unwrap_or_default(),
                        error
                    );
                    break 'connection;
                }
            };
            let replica_mode = IS_REPLICA.load(Ordering::SeqCst);
            let response = execute_obp_command(
                &store,
                &persistence,
                frame,
                &mut authenticated,
                replica_mode,
            )
            .await;
            let mut out = bytes::BytesMut::new();
            if response.encode(&mut out).is_err() {
                return;
            }
            let write_result = tokio::time::timeout(frame_timeout, buf_writer.write_all(&out));
            if !matches!(write_result.await, Ok(Ok(()))) {
                return;
            }
            wrote_response = true;
        }

        if buf.is_empty() {
            frame_started_at = None;
        } else if wrote_response {
            // Do not charge server-side command execution time to the next
            // pipelined frame's assembly budget.
            frame_started_at = Some(tokio::time::Instant::now());
        }

        if wrote_response {
            let flush_result = tokio::time::timeout(frame_timeout, buf_writer.flush()).await;
            if !matches!(flush_result, Ok(Ok(()))) {
                return;
            }
        }
    }

    let _ = tokio::time::timeout(frame_timeout, buf_writer.flush()).await;
}

/// Persists and publishes one already-applied master mutation at its assigned
/// sequence. The caller must own the authoritative commit boundary.
async fn persist_and_publish_master_batch(
    persistence: &Persistence,
    sequence: u64,
    batch: &CommittedBatch,
) -> Result<bool, PersistenceError> {
    let should_compact = persistence
        .accept_next_batch(sequence, batch, COMPACTION_THRESHOLD)
        .await?;
    // The exact same committed batch is published to the backlog and live
    // replication only after its binlog append has been acknowledged.
    {
        let mut backlog = persistence
            .backlog
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        backlog.push_back((sequence, batch.clone()));
        while backlog.len() > BACKLOG_CAPACITY {
            backlog.pop_front();
        }
    }
    let _ = persistence.replica_tx.send((sequence, batch.clone()));

    Ok(should_compact)
}

/// Persists one contiguous group of logical master mutations and publishes
/// every accepted sequence in the same canonical order.
async fn persist_and_publish_master_batches(
    persistence: &Persistence,
    batches: &[(u64, CommittedBatch)],
) -> Result<bool, PersistenceError> {
    let should_compact = persistence
        .accept_next_batches(batches, COMPACTION_THRESHOLD)
        .await?;
    {
        let mut backlog = persistence
            .backlog
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (sequence, batch) in batches {
            backlog.push_back((*sequence, batch.clone()));
            while backlog.len() > BACKLOG_CAPACITY {
                backlog.pop_front();
            }
        }
    }
    for (sequence, batch) in batches {
        let _ = persistence.replica_tx.send((*sequence, batch.clone()));
    }
    Ok(should_compact)
}

struct PersistenceCommitGuard {
    persistence: Arc<Persistence>,
    boundary: Option<CommitBoundary>,
    interruption_context: &'static str,
}

impl PersistenceCommitGuard {
    fn new(
        persistence: Arc<Persistence>,
        boundary: CommitBoundary,
        interruption_context: &'static str,
    ) -> Self {
        Self {
            persistence,
            boundary: Some(boundary),
            interruption_context,
        }
    }

    fn release(mut self) {
        self.boundary.take();
    }

    fn fail_stop(mut self, message: impl Into<String>) {
        let boundary = self
            .boundary
            .take()
            .expect("an armed persistence commit guard owns its boundary");
        enter_persistence_fail_stop_with_boundary(&self.persistence, boundary, message);
    }
}

impl Drop for PersistenceCommitGuard {
    fn drop(&mut self) {
        let Some(boundary) = self.boundary.take() else {
            return;
        };
        enter_persistence_fail_stop_with_boundary(
            &self.persistence,
            boundary,
            format!(
                "{} was interrupted before its persistence outcome became authoritative",
                self.interruption_context
            ),
        );
    }
}

async fn finalize_master_commit(
    store: Arc<ShardedStore>,
    persistence: Arc<Persistence>,
    boundary: CommitBoundary,
    sequence: u64,
    batch: CommittedBatch,
    rollback: MutationRollback,
    failure_context: &'static str,
) -> Result<(), PersistenceError> {
    let commit_guard =
        PersistenceCommitGuard::new(Arc::clone(&persistence), boundary, failure_context);
    let persistence_result = persist_and_publish_master_batch(&persistence, sequence, &batch).await;
    let should_compact = match persistence_result {
        Ok(should_compact) => should_compact,
        Err(error) => {
            if error.is_indeterminate() {
                commit_guard.fail_stop(format!(
                    "{} at sequence {}: {}",
                    failure_context, sequence, error
                ));
                return Err(error);
            }
            rollback.restore(&store);
            mark_persistence_failed(
                &persistence,
                format!("{} at sequence {}: {}", failure_context, sequence, error),
            );
            commit_guard.release();
            return Err(error);
        }
    };

    commit_guard.release();
    schedule_compaction(&store, &persistence, should_compact);
    Ok(())
}

async fn await_commit_finalizer(
    persistence: &Persistence,
    finalizer: tokio::task::JoinHandle<Result<(), PersistenceError>>,
) -> Result<(), PersistenceError> {
    match finalizer.await {
        Ok(result) => result,
        Err(error) => {
            let error = PersistenceError::indeterminate(format!(
                "Commit finalizer completion is indeterminate: {}",
                error
            ));
            if !persistence.is_fail_stopped() {
                enter_persistence_fail_stop(persistence, error.to_string()).await;
            }
            Err(error)
        }
    }
}

fn schedule_compaction(
    store: &Arc<ShardedStore>,
    persistence: &Arc<Persistence>,
    should_compact: bool,
) {
    if !should_compact {
        return;
    }

    let store = Arc::clone(store);
    let persistence = Arc::clone(persistence);
    tokio::spawn(async move {
        if let Err(error) = compact_store(&store, &persistence).await {
            error!("Automatic compaction failed: {}", error);
            persistence
                .write_count
                .store(COMPACTION_THRESHOLD, Ordering::SeqCst);
        }
        persistence
            .compaction_pending
            .store(false, Ordering::SeqCst);
    });
}

async fn persist_and_apply_replica_effect(
    store: &Arc<ShardedStore>,
    persistence: &Arc<Persistence>,
    sequence: u64,
    batch: &CommittedBatch,
) -> Result<(), PersistenceError> {
    let boundary = persistence.acquire_commit_boundary().await;
    if !persistence.accepting_writes.load(Ordering::SeqCst) {
        return Err(PersistenceError::new(persistence_unavailable_message(
            persistence,
        )));
    }
    if !persistence.replication_ready.load(Ordering::SeqCst) {
        return Err(PersistenceError::new(
            "Replica has no installed durable synchronization baseline",
        ));
    }
    let current = persistence.sequence();
    let expected = current
        .checked_add(1)
        .ok_or_else(|| PersistenceError::new("Replica sequence is exhausted"))?;
    if sequence != expected {
        return Err(PersistenceError::new(format!(
            "Replication sequence mismatch: expected {}, received {}",
            expected, sequence
        )));
    }
    let store = Arc::clone(store);
    let persistence_for_finalizer = Arc::clone(persistence);
    let batch = batch.clone();
    let finalizer = tokio::spawn(async move {
        let commit_guard = PersistenceCommitGuard::new(
            Arc::clone(&persistence_for_finalizer),
            boundary,
            "Replicated effect commit finalizer",
        );
        let should_compact = match persistence_for_finalizer
            .accept_next_batch(sequence, &batch, COMPACTION_THRESHOLD)
            .await
        {
            Ok(should_compact) => should_compact,
            Err(error) => {
                if error.is_indeterminate() {
                    commit_guard.fail_stop(format!(
                        "Replicated effect persistence is indeterminate at sequence {}: {}",
                        sequence, error
                    ));
                    return Err(error);
                }
                mark_persistence_failed(
                    &persistence_for_finalizer,
                    format!(
                        "Unable to persist replicated effect at sequence {}: {}",
                        sequence, error
                    ),
                );
                commit_guard.release();
                return Err(error);
            }
        };
        apply_committed_batch(&store, &batch);
        commit_guard.release();
        schedule_compaction(&store, &persistence_for_finalizer, should_compact);
        Ok(())
    });
    await_commit_finalizer(persistence, finalizer).await
}

async fn begin_full_sync_reception(persistence: &Arc<Persistence>) -> Result<(), PersistenceError> {
    let _write_guard = persistence.write_gate.lock().await;
    if persistence.promote_to_master.load(Ordering::SeqCst)
        || persistence.replica_lifecycle.stop_requested()
    {
        return Err(PersistenceError::new(
            "Replica promotion started before full synchronization could begin",
        ));
    }
    if !persistence.accepting_writes.load(Ordering::SeqCst) {
        return Err(PersistenceError::new(persistence_unavailable_message(
            persistence,
        )));
    }
    if let Err(error) = write_replica_installing(&persistence.paths) {
        mark_persistence_failed(
            persistence,
            format!(
                "Unable to invalidate the previous replica baseline before full synchronization: {}",
                error
            ),
        );
        return Err(error);
    }
    persistence.replication_ready.store(false, Ordering::SeqCst);
    persistence.upstream_replid.store(0, Ordering::SeqCst);
    Ok(())
}

async fn install_full_sync(
    store: &Arc<ShardedStore>,
    persistence: &Arc<Persistence>,
    replid: u64,
    sequence: u64,
    staging: ShardedStore,
) -> Result<(), PersistenceError> {
    if replid == 0 {
        return Err(PersistenceError::new(
            "Full synchronization requires a non-zero replication ID",
        ));
    }
    let boundary = persistence.acquire_commit_boundary().await;
    if persistence.promote_to_master.load(Ordering::SeqCst)
        || persistence.replica_lifecycle.stop_requested()
    {
        return Err(PersistenceError::new(
            "Replica promotion started before full synchronization could be installed",
        ));
    }
    if !persistence.accepting_writes.load(Ordering::SeqCst) {
        return Err(PersistenceError::new(persistence_unavailable_message(
            persistence,
        )));
    }
    persistence.replication_ready.store(false, Ordering::SeqCst);
    if let Err(error) = persistence.binlog.flush().await {
        if error.is_indeterminate() {
            enter_persistence_fail_stop_with_boundary(
                persistence,
                boundary,
                format!("Replica baseline flush is indeterminate: {}", error),
            );
        }
        return Err(error);
    }
    // Invalidate promotability durably before truncating the old incremental
    // history. A crash from this point until the new identity is installed
    // must force another full synchronization rather than promote an older
    // snapshot whose post-boundary log may already be gone.
    write_replica_installing(&persistence.paths)?;
    if let Err(error) = persistence.binlog.truncate().await {
        if error.is_indeterminate() {
            enter_persistence_fail_stop_with_boundary(
                persistence,
                boundary,
                format!("Replica baseline rotation is indeterminate: {}", error),
            );
        }
        return Err(error);
    }
    let entries = staging.raw_entries();
    let snapshot_entries = entries.clone();
    let paths = persistence.paths.clone();
    let snapshot_result = tokio::task::spawn_blocking(move || {
        write_snapshot_file(snapshot_entries, sequence, &paths)
    })
    .await
    .map_err(|error| PersistenceError::new(format!("Replica snapshot task failed: {}", error)))
    .and_then(|result| result);
    if let Err(error) = snapshot_result {
        let error = PersistenceError::indeterminate(format!(
            "Replica baseline snapshot failed after binlog rotation: {}",
            error
        ));
        enter_persistence_fail_stop_with_boundary(persistence, boundary, error.to_string());
        return Err(error);
    }
    if let Err(error) = write_replica_identity(
        &persistence.paths,
        ReplicaIdentity {
            replid,
            baseline_sequence: sequence,
        },
    ) {
        let error = PersistenceError::indeterminate(format!(
            "Replica identity installation failed after baseline replacement: {}",
            error
        ));
        enter_persistence_fail_stop_with_boundary(persistence, boundary, error.to_string());
        return Err(error);
    }

    store.replace_all(entries);
    persistence.install_baseline(sequence);
    persistence.upstream_replid.store(replid, Ordering::SeqCst);
    persistence.replication_ready.store(true, Ordering::SeqCst);
    persistence.backlog.lock().unwrap().clear();
    Ok(())
}

async fn prepare_replica_promotion(persistence: &Arc<Persistence>) -> Result<(), PersistenceError> {
    {
        let _write_guard = persistence.write_gate.lock().await;
        if !persistence.replication_ready.load(Ordering::SeqCst) {
            return Err(PersistenceError::new(
                "Replica is not durably synchronized and cannot be promoted",
            ));
        }
        persistence.replica_lifecycle.request_stop();
    }
    persistence.replica_lifecycle.stop_and_wait().await?;
    commit_replica_promotion(persistence).await
}

async fn commit_replica_promotion(persistence: &Arc<Persistence>) -> Result<(), PersistenceError> {
    let boundary = persistence.acquire_commit_boundary().await;
    if !persistence.replication_ready.load(Ordering::SeqCst) {
        return Err(PersistenceError::new(
            "Replica is not durably synchronized and cannot be promoted",
        ));
    }
    if let Err(error) = persistence.binlog.flush().await {
        if error.is_indeterminate() {
            enter_persistence_fail_stop_with_boundary(
                persistence,
                boundary,
                format!("Replica promotion flush is indeterminate: {}", error),
            );
        }
        return Err(error);
    }
    write_replica_detached(&persistence.paths)?;
    persistence.upstream_replid.store(0, Ordering::SeqCst);
    persistence.replication_ready.store(false, Ordering::SeqCst);
    persistence.promote_to_master.store(true, Ordering::SeqCst);
    IS_REPLICA.store(false, Ordering::SeqCst);
    Ok(())
}

async fn execute_transaction(
    store: &Arc<ShardedStore>,
    persistence: &Arc<Persistence>,
    commands: Vec<Vec<String>>,
    replica_mode: bool,
) -> RESPValue {
    let contains_writes = commands.iter().any(|args| {
        args.first()
            .is_some_and(|command| is_write_command(command))
    });

    if !contains_writes {
        let _visibility_guard = persistence.visibility_gate.read().await;
        return RESPValue::Array(
            commands
                .iter()
                .map(|args| execute_command(store, args).into_response())
                .collect(),
        );
    }

    if replica_mode {
        let _visibility_guard = persistence.visibility_gate.read().await;
        return RESPValue::Array(
            commands
                .iter()
                .map(|args| {
                    let command = args.first().map(String::as_str).unwrap_or("");
                    if is_write_command(command) {
                        RESPValue::Error(
                            "READONLY this instance is a read-only replica".to_string(),
                        )
                    } else {
                        execute_command(store, args).into_response()
                    }
                })
                .collect(),
        );
    }

    if let Some(coordinator) = persistence.master_commit.get() {
        return match coordinator.execute_transaction(commands).await {
            Ok(response) => response,
            Err(error) => {
                let message = error.to_string();
                if message.starts_with("MISCONF ") {
                    RESPValue::Error(message)
                } else {
                    RESPValue::Error(format!("MISCONF transaction persistence failed: {}", error))
                }
            }
        };
    }

    let boundary = persistence.acquire_commit_boundary().await;
    if !persistence.accepting_writes.load(Ordering::SeqCst) {
        return RESPValue::Error(persistence_unavailable_message(persistence));
    }
    let current_sequence = persistence.sequence();
    if current_sequence == u64::MAX {
        mark_persistence_failed(persistence, "Persistence sequence is exhausted");
        return RESPValue::Error("MISCONF persistence sequence is exhausted".to_string());
    }

    let mut baseline = std::collections::HashMap::<Bytes, Option<DataEntry>>::new();
    let mut changed_keys = Vec::new();
    let mut changed_key_set = HashSet::new();
    let mut results = Vec::with_capacity(commands.len());

    for args in &commands {
        let command = args.first().map(String::as_str).unwrap_or("");
        if !is_write_command(command) {
            results.push(execute_command(store, args).into_response());
            continue;
        }

        let affected_keys = persistent_keys_for_command(args);
        let mut attempt = store.begin_mutation(&affected_keys);
        for key in &affected_keys {
            if changed_key_set.insert(key.clone()) {
                changed_keys.push(key.clone());
                baseline.insert(
                    key.clone(),
                    attempt.before_entries().get(key).cloned().flatten(),
                );
            }
        }
        let outcome = execute_command(store, args);
        let response = outcome.response;
        if derive_committed_batch(store, &affected_keys, attempt.before_entries(), &[]).is_none() {
            attempt.commit();
            results.push(response);
            continue;
        }

        match attempt.admit(&affected_keys) {
            Ok(()) => {
                for (key, entry) in attempt.evicted_entries() {
                    if changed_key_set.insert(key.clone()) {
                        changed_keys.push(key.clone());
                        baseline.insert(key.clone(), Some(entry.clone()));
                    }
                }
                results.push(response);
                attempt.commit();
            }
            Err(error) => {
                attempt.rollback();
                results.push(RESPValue::Error(error.message().to_string()));
            }
        }
    }

    changed_keys.sort();
    let Some(batch) = derive_committed_batch(store, &changed_keys, &baseline, &[]) else {
        return RESPValue::Array(results);
    };

    let sequence = current_sequence + 1;
    let finalizer = tokio::spawn(finalize_master_commit(
        Arc::clone(store),
        Arc::clone(persistence),
        boundary,
        sequence,
        batch,
        MutationRollback::from_baseline(baseline),
        "Transaction persistence failed",
    ));
    match await_commit_finalizer(persistence, finalizer).await {
        Ok(()) => RESPValue::Array(results),
        Err(error) => {
            RESPValue::Error(format!("MISCONF transaction persistence failed: {}", error))
        }
    }
}

async fn execute_ordered_command(
    store: &Arc<ShardedStore>,
    persistence: &Arc<Persistence>,
    args: &[String],
) -> CommandOutcome {
    let command = args.first().map(|value| value.as_str()).unwrap_or("");
    if !is_write_command(command) {
        let _visibility_guard = persistence.visibility_gate.read().await;
        return execute_command(store, args);
    }

    if let Some(coordinator) = persistence.master_commit.get() {
        return match coordinator.execute_command(args.to_vec()).await {
            Ok(outcome) => outcome,
            Err(error) => {
                let message = error.to_string();
                CommandOutcome {
                    response: RESPValue::Error(if message.starts_with("MISCONF ") {
                        message
                    } else {
                        format!("MISCONF mutation persistence failed: {}", error)
                    }),
                    mutation: MutationState::NoChange,
                }
            }
        };
    }

    let boundary = persistence.acquire_commit_boundary().await;
    if !persistence.accepting_writes.load(Ordering::SeqCst) {
        return CommandOutcome {
            response: RESPValue::Error(persistence_unavailable_message(persistence)),
            mutation: MutationState::NoChange,
        };
    }
    let current_sequence = persistence.sequence();
    if current_sequence == u64::MAX {
        mark_persistence_failed(persistence, "Persistence sequence is exhausted");
        return CommandOutcome {
            response: RESPValue::Error("MISCONF persistence sequence is exhausted".to_string()),
            mutation: MutationState::NoChange,
        };
    }

    let affected_keys = persistent_keys_for_command(args);
    let mut attempt = store.begin_mutation(&affected_keys);
    let mut outcome = execute_command(store, args);
    if derive_committed_batch(store, &affected_keys, attempt.before_entries(), &[]).is_none() {
        attempt.commit();
        outcome.mutation = MutationState::NoChange;
        return outcome;
    }

    match attempt.admit(&affected_keys) {
        Ok(()) => {}
        Err(error) => {
            attempt.rollback();
            return CommandOutcome {
                response: RESPValue::Error(error.message().to_string()),
                mutation: MutationState::NoChange,
            };
        }
    }
    let committed_batch = derive_committed_batch(
        store,
        &affected_keys,
        attempt.before_entries(),
        attempt.evicted_entries(),
    );
    let Some(batch) = committed_batch else {
        attempt.rollback();
        mark_persistence_failed(
            persistence,
            "Committed effect derivation became empty after admission",
        );
        return CommandOutcome {
            response: RESPValue::Error(
                "MISCONF committed effect derivation failed after admission".to_string(),
            ),
            mutation: MutationState::NoChange,
        };
    };

    let sequence = current_sequence + 1;
    let rollback = attempt.into_rollback();
    let finalizer = tokio::spawn(finalize_master_commit(
        Arc::clone(store),
        Arc::clone(persistence),
        boundary,
        sequence,
        batch,
        rollback,
        "Mutation persistence failed",
    ));
    match await_commit_finalizer(persistence, finalizer).await {
        Ok(()) => {
            outcome.mutation = MutationState::Committed;
        }
        Err(error) => {
            return CommandOutcome {
                response: RESPValue::Error(format!(
                    "MISCONF mutation persistence failed: {}",
                    error
                )),
                mutation: MutationState::NoChange,
            };
        }
    }
    outcome
}

async fn execute_ordered_commands(
    store: &Arc<ShardedStore>,
    persistence: &Arc<Persistence>,
    commands: Vec<Vec<String>>,
) -> Vec<CommandOutcome> {
    let command_count = commands.len();
    if let Some(coordinator) = persistence.master_commit.get() {
        return match coordinator.execute_commands(commands).await {
            Ok(outcomes) => outcomes,
            Err(error) => {
                let message = error.to_string();
                let message = if message.starts_with("MISCONF ") {
                    message
                } else {
                    format!("MISCONF mutation persistence failed: {}", error)
                };
                (0..command_count)
                    .map(|_| CommandOutcome {
                        response: RESPValue::Error(message.clone()),
                        mutation: MutationState::NoChange,
                    })
                    .collect()
            }
        };
    }

    let mut outcomes = Vec::with_capacity(command_count);
    for command in commands {
        outcomes.push(execute_ordered_command(store, persistence, &command).await);
    }
    outcomes
}

async fn execute_obp_command(
    store: &Arc<ShardedStore>,
    persistence: &Arc<Persistence>,
    frame: OBPFrame,
    authenticated: &mut bool,
    replica_mode: bool,
) -> OBPFrame {
    let cmd = frame.cmd;
    let args = frame.args;

    // OBP AUTH (0x10) accepts a password or a username/password pair.
    if cmd == 0x10 {
        let credentials = if args.len() >= 2 {
            std::str::from_utf8(&args[0])
                .ok()
                .zip(std::str::from_utf8(&args[1]).ok())
        } else {
            args.first()
                .and_then(|password| std::str::from_utf8(password).ok())
                .map(|password| ("default", password))
        };
        let ok = auth_required()
            && credentials.is_some_and(|(user, password)| check_credentials(user, password));
        if ok {
            *authenticated = true;
        }
        return OBPFrame {
            cmd: 0x00,
            flags: 0,
            correlation_id: frame.correlation_id,
            args: Vec::new(),
            payload: Some(Bytes::from(if ok { "OK" } else { "WRONGPASS" })),
        };
    }

    // Reject every non-authentication command until authentication succeeds.
    if !*authenticated {
        return OBPFrame {
            cmd: 0x00,
            flags: 0,
            correlation_id: frame.correlation_id,
            args: Vec::new(),
            payload: Some(Bytes::from("NOAUTH authentication required")),
        };
    }

    if replica_mode && matches!(cmd, 0x02 | 0x03) {
        return OBPFrame {
            cmd: 0x00,
            flags: 0,
            correlation_id: frame.correlation_id,
            args: Vec::new(),
            payload: Some(Bytes::from("READONLY this instance is a read-only replica")),
        };
    }

    let (value, _is_write) = match cmd {
        0x01 => {
            if let Some(key) = args.first() {
                let _visibility_guard = persistence.visibility_gate.read().await;
                (
                    store
                        .get_entry(key)
                        .map(|e| e.value)
                        .unwrap_or(OnyxValue::Blob(Bytes::new())),
                    false,
                )
            } else {
                (OnyxValue::Blob(Bytes::new()), false)
            }
        }
        0x02 => {
            if args.len() >= 2 {
                if let Some(coordinator) = persistence.master_commit.get() {
                    match coordinator
                        .execute_obp_set(args[0].clone(), args[1].clone())
                        .await
                    {
                        Ok(ObpMutationResult::Value(value)) => (value, true),
                        Ok(ObpMutationResult::Error(message)) => {
                            return OBPFrame {
                                cmd: 0x00,
                                flags: 0,
                                correlation_id: frame.correlation_id,
                                args: Vec::new(),
                                payload: Some(Bytes::from(message)),
                            };
                        }
                        Err(error) => {
                            let message = error.to_string();
                            let message = if message.starts_with("MISCONF ") {
                                message
                            } else {
                                format!("MISCONF mutation persistence failed: {}", error)
                            };
                            return OBPFrame {
                                cmd: 0x00,
                                flags: 0,
                                correlation_id: frame.correlation_id,
                                args: Vec::new(),
                                payload: Some(Bytes::from(message)),
                            };
                        }
                    }
                } else {
                    let boundary = persistence.acquire_commit_boundary().await;
                    if !persistence.accepting_writes.load(Ordering::SeqCst) {
                        return OBPFrame {
                            cmd: 0x00,
                            flags: 0,
                            correlation_id: frame.correlation_id,
                            args: Vec::new(),
                            payload: Some(Bytes::from("MISCONF persistence is unavailable")),
                        };
                    }
                    let current_sequence = persistence.sequence();
                    if current_sequence == u64::MAX {
                        mark_persistence_failed(persistence, "Persistence sequence is exhausted");
                        return OBPFrame {
                            cmd: 0x00,
                            flags: 0,
                            correlation_id: frame.correlation_id,
                            args: Vec::new(),
                            payload: Some(Bytes::from("MISCONF persistence sequence is exhausted")),
                        };
                    }
                    let key = args[0].clone();
                    let affected_keys = [key.clone()];
                    let mut attempt = store.begin_mutation(&affected_keys);
                    let value = OnyxValue::Blob(args[1].clone());
                    store.set_value(key.clone(), value, None);
                    if derive_committed_batch(store, &affected_keys, attempt.before_entries(), &[])
                        .is_none()
                    {
                        attempt.commit();
                        return OBPFrame {
                            cmd: 0x00,
                            flags: 0,
                            correlation_id: frame.correlation_id,
                            args: Vec::new(),
                            payload: Some(Bytes::from(format!(
                                "{:?}",
                                OnyxValue::Blob(Bytes::from_static(b"OK"))
                            ))),
                        };
                    }
                    match attempt.admit(&affected_keys) {
                        Ok(()) => {}
                        Err(error) => {
                            attempt.rollback();
                            return OBPFrame {
                                cmd: 0x00,
                                flags: 0,
                                correlation_id: frame.correlation_id,
                                args: Vec::new(),
                                payload: Some(Bytes::from(error.message())),
                            };
                        }
                    }
                    let batch = derive_committed_batch(
                        store,
                        &affected_keys,
                        attempt.before_entries(),
                        attempt.evicted_entries(),
                    )
                    .expect("OBP SET must produce a committed effect");
                    let sequence = current_sequence + 1;
                    let rollback = attempt.into_rollback();
                    let finalizer = tokio::spawn(finalize_master_commit(
                        Arc::clone(store),
                        Arc::clone(persistence),
                        boundary,
                        sequence,
                        batch,
                        rollback,
                        "OBP mutation persistence failed",
                    ));
                    match await_commit_finalizer(persistence, finalizer).await {
                        Ok(()) => {}
                        Err(error) => {
                            return OBPFrame {
                                cmd: 0x00,
                                flags: 0,
                                correlation_id: frame.correlation_id,
                                args: Vec::new(),
                                payload: Some(Bytes::from(format!(
                                    "MISCONF mutation persistence failed: {}",
                                    error
                                ))),
                            };
                        }
                    }

                    (OnyxValue::Blob(Bytes::from("OK")), true)
                }
            } else {
                (OnyxValue::Blob(Bytes::from("ERR")), false)
            }
        }
        0x03 => {
            if let Some(key) = args.first() {
                if let Some(coordinator) = persistence.master_commit.get() {
                    match coordinator.execute_obp_delete(key.clone()).await {
                        Ok(ObpMutationResult::Value(value)) => (value, true),
                        Ok(ObpMutationResult::Error(message)) => {
                            return OBPFrame {
                                cmd: 0x00,
                                flags: 0,
                                correlation_id: frame.correlation_id,
                                args: Vec::new(),
                                payload: Some(Bytes::from(message)),
                            };
                        }
                        Err(error) => {
                            let message = error.to_string();
                            let message = if message.starts_with("MISCONF ") {
                                message
                            } else {
                                format!("MISCONF mutation persistence failed: {}", error)
                            };
                            return OBPFrame {
                                cmd: 0x00,
                                flags: 0,
                                correlation_id: frame.correlation_id,
                                args: Vec::new(),
                                payload: Some(Bytes::from(message)),
                            };
                        }
                    }
                } else {
                    let boundary = persistence.acquire_commit_boundary().await;
                    if !persistence.accepting_writes.load(Ordering::SeqCst) {
                        return OBPFrame {
                            cmd: 0x00,
                            flags: 0,
                            correlation_id: frame.correlation_id,
                            args: Vec::new(),
                            payload: Some(Bytes::from("MISCONF persistence is unavailable")),
                        };
                    }
                    let current_sequence = persistence.sequence();
                    if current_sequence == u64::MAX {
                        mark_persistence_failed(persistence, "Persistence sequence is exhausted");
                        return OBPFrame {
                            cmd: 0x00,
                            flags: 0,
                            correlation_id: frame.correlation_id,
                            args: Vec::new(),
                            payload: Some(Bytes::from("MISCONF persistence sequence is exhausted")),
                        };
                    }
                    let affected_keys = [key.clone()];
                    let attempt = store.begin_mutation(&affected_keys);
                    let deleted = store.delete_bytes(key);
                    if deleted {
                        let batch = CommittedBatch {
                            effects: vec![CommittedEffect::Delete { key: key.clone() }],
                        };
                        let sequence = current_sequence + 1;
                        let rollback = attempt.into_rollback();
                        let finalizer = tokio::spawn(finalize_master_commit(
                            Arc::clone(store),
                            Arc::clone(persistence),
                            boundary,
                            sequence,
                            batch,
                            rollback,
                            "OBP mutation persistence failed",
                        ));
                        match await_commit_finalizer(persistence, finalizer).await {
                            Ok(()) => {}
                            Err(error) => {
                                return OBPFrame {
                                    cmd: 0x00,
                                    flags: 0,
                                    correlation_id: frame.correlation_id,
                                    args: Vec::new(),
                                    payload: Some(Bytes::from(format!(
                                        "MISCONF mutation persistence failed: {}",
                                        error
                                    ))),
                                };
                            }
                        }
                    }
                    (OnyxValue::Int(if deleted { 1 } else { 0 }), true)
                }
            } else {
                (OnyxValue::Int(0), false)
            }
        }
        0xF0 => (OnyxValue::Blob(Bytes::from("PONG")), false),
        _ => (OnyxValue::Blob(Bytes::from("ERR unknown command")), false),
    };

    OBPFrame {
        cmd: 0x00,
        flags: 0,
        correlation_id: frame.correlation_id,
        args: Vec::new(),
        payload: Some(Bytes::from(format!("{:?}", value))),
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    info!("Starting OnyxDB");
    START_TIME.store(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        Ordering::Relaxed,
    );
    let repl_id_val: u64 = {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let pid = std::process::id() as u64;
        // This identity is not cryptographic; it only needs negligible restart
        // collision probability.
        nanos.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(pid)
    };
    REPL_ID.set(repl_id_val).ok();
    info!("This instance's replication ID: {}", repl_id_val);
    let ServerConfig {
        master_addr,
        upstream_credentials,
        users,
        fsync_policy: policy,
        maxmemory_bytes: maxmemory_val,
        maxmemory_policy: mm_policy,
        auto_failover,
        failover_timeout_secs,
        bind_address,
        data_directory,
        port,
        warnings,
    } = ServerConfig::from_process()?;
    for warning in &warnings {
        warn!("{}", warning);
    }
    let num_users = users.len();
    USERS.set(users).ok();
    if num_users > 0 {
        info!("Authentication required: {} user(s) configured", num_users);
    }
    info!("Binlog fsync policy: {:?}", policy);
    if maxmemory_val > 0 {
        info!(
            "Dataset memory limit: {} bytes, policy {:?}",
            maxmemory_val, mm_policy
        );
    }
    if !bind_address.is_loopback() {
        warn!(
            "Non-loopback bind {} exposes RESP, OBP, and unauthenticated metrics without TLS; use trusted network controls",
            bind_address
        );
    }
    let runtime_directory = RuntimeDirectoryLock::acquire(&data_directory)?;
    info!(
        "Runtime data directory locked: {}",
        runtime_directory.directory().display()
    );
    let bind_addr = SocketAddr::new(bind_address, port);
    let listener = TcpListener::bind(&bind_addr).await?;
    let obp_addr = SocketAddr::new(bind_address, port + 1);
    let obp_listener = TcpListener::bind(&obp_addr).await?;
    let metrics_addr = SocketAddr::new(bind_address, port + 1000);
    let metrics_listener = TcpListener::bind(&metrics_addr).await?;
    info!("Server listening on {}", bind_addr);
    info!("OBP (binary) server listening on {}", obp_addr);

    let store = Arc::new(ShardedStore::with_maxmemory(maxmemory_val, mm_policy));
    let paths = PersistencePaths::in_directory(runtime_directory.directory());
    let recovery = load_data_from_paths(&store, &paths)?;
    let recovered_replica_identity =
        prepare_replication_startup(&paths, recovery.snapshot_watermark, master_addr.is_some())?;
    info!(
        "Persistence recovery complete at sequence {} (snapshot watermark {})",
        recovery.last_sequence, recovery.snapshot_watermark
    );

    let store_gc = Arc::clone(&store);
    tokio::spawn(async move {
        active_expiration_task(store_gc).await;
    });

    let (tx, rx) = mpsc::channel::<LogMessage>(100_000);
    let (replica_tx, _) = tokio::sync::broadcast::channel::<(u64, CommittedBatch)>(4096);
    let (pubsub_tx, _) = tokio::sync::broadcast::channel::<(String, String)>(4096);
    let promote_flag = Arc::new(AtomicBool::new(false));
    let replica_lifecycle = Arc::new(ReplicaLifecycle::new(master_addr.is_none()));
    let persistence = Arc::new(Persistence {
        commit_runtime: CommitRuntime::new(
            BinlogHandle::new(tx),
            recovery.last_sequence,
            paths.clone(),
        ),
        master_commit: std::sync::OnceLock::new(),
        replica_tx,
        promote_to_master: Arc::clone(&promote_flag),
        backlog: std::sync::Mutex::new(std::collections::VecDeque::with_capacity(BACKLOG_CAPACITY)),
        next_replica_id: AtomicU64::new(0),
        replica_status: std::sync::Mutex::new(std::collections::HashMap::new()),
        pubsub_tx,
        next_subscriber_id: AtomicU64::new(0),
        subscriptions: std::sync::Mutex::new(std::collections::HashMap::new()),
        upstream_replid: AtomicU64::new(
            recovered_replica_identity
                .map(|identity| identity.replid)
                .unwrap_or(0),
        ),
        replication_ready: AtomicBool::new(recovered_replica_identity.is_some()),
        replica_lifecycle,
    });

    persistence
        .master_commit
        .set(MasterCommitCoordinator::start(
            Arc::clone(&store),
            &persistence,
        ))
        .unwrap_or_else(|_| panic!("master commit coordinator initialized more than once"));

    let binlog_shared: Arc<std::sync::Mutex<File>> =
        Arc::new(std::sync::Mutex::new(open_binlog_file(&paths.binlog)));

    // The default `everysec` policy synchronizes the current binlog once per second.
    if policy == FsyncPolicy::EverySec {
        let persistence_fsync = Arc::clone(&persistence);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if run_periodic_sync_once(&persistence_fsync).await.is_err() {
                    break;
                }
            }
        });
    }

    let binlog_writer = Arc::clone(&binlog_shared);
    tokio::spawn(run_binlog_worker(rx, binlog_writer, policy));

    if let Some(addr) = master_addr {
        IS_REPLICA.store(true, Ordering::Relaxed);
        if auto_failover {
            warn!(
                "--auto-failover enabled (timeout {}s): this instance will self-promote to master \
                 if it loses contact with the master past the timeout. WARNING: only safe with ONE \
                 replica per master — with multiple replicas all configured with --auto-failover, more \
                 than one could promote in parallel (split-brain), since there is no cross-replica \
                 coordination in this version.",
                failover_timeout_secs
            );
        }
        let store_replica = Arc::clone(&store);
        let persistence_replica = Arc::clone(&persistence);
        let initial_offset = recovered_replica_identity
            .map(|_| recovery.last_sequence)
            .unwrap_or(0);
        let initial_replid = recovered_replica_identity
            .map(|identity| identity.replid)
            .unwrap_or(0);
        tokio::spawn(async move {
            run_replica(
                store_replica,
                persistence_replica,
                ReplicaRuntimeConfig {
                    master_addr: addr,
                    credentials: upstream_credentials,
                    auto_failover,
                    failover_timeout: Duration::from_secs(failover_timeout_secs),
                    liveness_timeout: REPLICATION_IDLE_TIMEOUT,
                    initial_replid,
                    initial_offset,
                },
            )
            .await;
        });
    }

    let store_obp = Arc::clone(&store);
    let persistence_obp = Arc::clone(&persistence);
    tokio::spawn(async move {
        loop {
            let (stream, _) = match obp_listener.accept().await {
                Ok(pair) => pair,
                Err(_) => continue,
            };
            let store_clone = Arc::clone(&store_obp);
            let persistence_clone = Arc::clone(&persistence_obp);
            tokio::spawn(async move {
                handle_obp_client(stream, store_clone, persistence_clone).await;
            });
        }
    });
    let store_metrics = Arc::clone(&store);
    let persistence_metrics = Arc::clone(&persistence);
    tokio::spawn(async move {
        run_metrics_server(
            metrics_listener,
            metrics_addr,
            store_metrics,
            persistence_metrics,
        )
        .await;
    });

    let store_shutdown = Arc::clone(&store);
    let persistence_shutdown = Arc::clone(&persistence);

    tokio::select! {
        result = async {
            loop {
                let (stream, _) = listener.accept().await?;
                let store_clone = Arc::clone(&store);
                let persistence_clone = Arc::clone(&persistence);
                tokio::spawn(async move { handle_client(stream, store_clone, persistence_clone).await; });
            }
            #[allow(unreachable_code)]
            Ok::<(), std::io::Error>(())
        } => {
            result?;
        }
        error = await_persistence_fail_stop(&persistence_shutdown) => {
            return Err(
                Box::new(error) as Box<dyn std::error::Error>
            );
        }
        _ = tokio::signal::ctrl_c() => {
            if persistence_shutdown.is_fail_stopped() {
                let error = await_persistence_fail_stop(&persistence_shutdown).await;
                return Err(Box::new(error) as Box<dyn std::error::Error>);
            }
            info!("Shutdown signal received, saving final state...");
            persistence_shutdown.accepting_writes.store(false, Ordering::SeqCst);
            compact_store(&store_shutdown, &persistence_shutdown).await?;
            info!("Save complete, goodbye!");
        }
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplicaSyncHandshake {
    Full {
        replid: u64,
        sequence: u64,
        entry_count: usize,
    },
    Partial {
        replid: u64,
        requested_sequence: u64,
    },
}

struct ReplicaRuntimeConfig {
    master_addr: String,
    credentials: Option<UpstreamCredentials>,
    auto_failover: bool,
    failover_timeout: Duration,
    liveness_timeout: Duration,
    initial_replid: u64,
    initial_offset: u64,
}

fn parse_replica_sync_handshake(
    marker: &[String],
) -> Result<ReplicaSyncHandshake, PersistenceError> {
    let parse_replid = |value: &str| {
        let replid = value
            .parse::<u64>()
            .map_err(|_| PersistenceError::new("Invalid master replication ID"))?;
        if replid == 0 {
            return Err(PersistenceError::new(
                "Master replication ID must be non-zero",
            ));
        }
        Ok(replid)
    };
    match marker.first().map(String::as_str) {
        Some("+FULLRESYNC3") if marker.len() == 4 => {
            let replid = parse_replid(&marker[1])?;
            let sequence = marker[2]
                .parse::<u64>()
                .map_err(|_| PersistenceError::new("Invalid full synchronization sequence"))?;
            let entry_count = marker[3]
                .parse::<usize>()
                .map_err(|_| PersistenceError::new("Invalid full synchronization entry count"))?;
            if entry_count > MAX_KEYS {
                return Err(PersistenceError::new(
                    "Full synchronization entry count exceeds the configured key limit",
                ));
            }
            Ok(ReplicaSyncHandshake::Full {
                replid,
                sequence,
                entry_count,
            })
        }
        Some("+CONTINUE3") if marker.len() == 3 => {
            let replid = parse_replid(&marker[1])?;
            let requested_sequence = marker[2]
                .parse::<u64>()
                .map_err(|_| PersistenceError::new("Invalid partial synchronization sequence"))?;
            Ok(ReplicaSyncHandshake::Partial {
                replid,
                requested_sequence,
            })
        }
        _ => Err(PersistenceError::new(
            "Unexpected or malformed replication handshake",
        )),
    }
}

fn parse_replica_sync_done(
    marker: &[String],
    expected_replid: u64,
) -> Result<u64, PersistenceError> {
    if marker.len() != 3 || marker[0] != "+SYNCDONE3" {
        return Err(PersistenceError::new(
            "Missing or malformed synchronization completion marker",
        ));
    }
    let replid = marker[1]
        .parse::<u64>()
        .map_err(|_| PersistenceError::new("Invalid completion replication ID"))?;
    if replid != expected_replid {
        return Err(PersistenceError::new(
            "Synchronization completion replication ID does not match the handshake",
        ));
    }
    marker[2]
        .parse::<u64>()
        .map_err(|_| PersistenceError::new("Invalid synchronization completion sequence"))
}

async fn maybe_auto_promote_replica(
    persistence: &Arc<Persistence>,
    unreachable_since: &Option<std::time::Instant>,
    auto_failover: bool,
    failover_timeout: Duration,
) -> bool {
    if !auto_failover
        || !matches!(
            unreachable_since,
            Some(since) if since.elapsed() >= failover_timeout
        )
    {
        return false;
    }
    match commit_replica_promotion(persistence).await {
        Ok(()) => {
            warn!(
                "Master unreachable for over {}s: self-promoting the durably synchronized replica",
                failover_timeout.as_secs()
            );
            true
        }
        Err(error) => {
            error!(
                "Automatic promotion refused because the replica is not safely promotable: {}",
                error
            );
            false
        }
    }
}

async fn await_or_stop<T>(
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    future: impl std::future::Future<Output = T>,
) -> Option<T> {
    if *stop_rx.borrow() {
        return None;
    }
    tokio::select! {
        biased;
        _ = stop_rx.wait_for(|stop| *stop) => None,
        output = future => Some(output),
    }
}

fn encode_upstream_auth(credentials: &UpstreamCredentials) -> String {
    let mut encoded = String::new();
    RESPValue::Array(vec![
        RESPValue::BulkString(Some("AUTH".to_string())),
        RESPValue::BulkString(Some(credentials.username.clone())),
        RESPValue::BulkString(Some(credentials.password.clone())),
    ])
    .encode_into(&mut encoded);
    encoded
}

fn upstream_failure_counts_as_unreachable(error: &std::io::Error) -> bool {
    error.kind() != std::io::ErrorKind::InvalidData
}

fn upstream_io_persistence_error(context: &str, error: std::io::Error) -> PersistenceError {
    let message = format!("{}: {}", context, error);
    if upstream_failure_counts_as_unreachable(&error) {
        PersistenceError::upstream_unavailable(message)
    } else {
        PersistenceError::new(message)
    }
}

fn is_authentication_rejection(frame: &[String]) -> bool {
    frame.first().is_some_and(|value| {
        value.eq_ignore_ascii_case("-NOAUTH") || value.eq_ignore_ascii_case("-WRONGPASS")
    })
}

fn parse_upstream_heartbeat(
    frame: &[String],
    installed_sequence: u64,
) -> Result<bool, PersistenceError> {
    if frame.first().map(String::as_str) != Some("REPLCONF") {
        return Ok(false);
    }
    if frame.len() != 3 || frame[1] != "PING" {
        return Err(PersistenceError::new(
            "Master sent a malformed replication heartbeat",
        ));
    }
    let heartbeat_sequence = frame[2]
        .parse::<u64>()
        .map_err(|_| PersistenceError::new("Master sent an invalid heartbeat sequence"))?;
    if heartbeat_sequence != installed_sequence {
        return Err(PersistenceError::new(format!(
            "Replication heartbeat sequence mismatch: installed {}, received {}",
            installed_sequence, heartbeat_sequence
        )));
    }
    Ok(true)
}

async fn run_replica(
    store: Arc<ShardedStore>,
    persistence: Arc<Persistence>,
    config: ReplicaRuntimeConfig,
) {
    let ReplicaRuntimeConfig {
        master_addr,
        credentials,
        auto_failover,
        failover_timeout,
        liveness_timeout,
        initial_replid,
        initial_offset,
    } = config;
    const MIN_BACKOFF_SECS: u64 = 1;
    const MAX_BACKOFF_SECS: u64 = 30;
    let mut backoff_secs = MIN_BACKOFF_SECS;
    let local_offset = Arc::new(AtomicU64::new(initial_offset));
    let local_replid = Arc::new(AtomicU64::new(initial_replid));
    let mut unreachable_since: Option<std::time::Instant> = None;
    let lifecycle = Arc::clone(&persistence.replica_lifecycle);
    lifecycle.mark_running();
    let _run_guard = ReplicaRunGuard(Arc::clone(&lifecycle));
    let mut stop_rx = lifecycle.subscribe_stop();

    loop {
        if lifecycle.stop_requested() {
            info!("Replica lifecycle stopped before reconnecting to the former upstream");
            return;
        }
        info!("Connecting to master {}...", master_addr);

        let Some(connection) = await_or_stop(&mut stop_rx, TcpStream::connect(&master_addr)).await
        else {
            return;
        };
        match connection {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                let (reader, mut writer) = stream.into_split();
                let mut buf_reader = TokioBufReader::with_capacity(65536, reader);
                let mut scratch = Vec::new();

                if let Some(credentials) = credentials.as_ref() {
                    let auth_command = encode_upstream_auth(credentials);
                    let Some(auth_write) = await_or_stop(
                        &mut stop_rx,
                        write_replication_bytes(&mut writer, auth_command.as_bytes()),
                    )
                    .await
                    else {
                        return;
                    };
                    if auth_write.is_err() {
                        warn!(
                            "Unable to send upstream authentication, retrying in {}s",
                            backoff_secs
                        );
                        unreachable_since.get_or_insert_with(std::time::Instant::now);
                        drop(buf_reader);
                        drop(writer);
                        if maybe_auto_promote_replica(
                            &persistence,
                            &unreachable_since,
                            auto_failover,
                            failover_timeout,
                        )
                        .await
                        {
                            return;
                        }
                        if await_or_stop(
                            &mut stop_rx,
                            tokio::time::sleep(Duration::from_secs(backoff_secs)),
                        )
                        .await
                        .is_none()
                        {
                            return;
                        }
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                        continue;
                    }

                    let Some(auth_reply) = await_or_stop(
                        &mut stop_rx,
                        read_replication_command_with_idle(
                            &mut buf_reader,
                            &mut scratch,
                            liveness_timeout,
                        ),
                    )
                    .await
                    else {
                        return;
                    };
                    let authenticated = match auth_reply {
                        Ok(Some(reply))
                            if reply.len() == 1 && reply[0].eq_ignore_ascii_case("+OK") =>
                        {
                            true
                        }
                        Ok(Some(_)) => {
                            warn!(
                                "Upstream authentication was rejected; automatic failover remains disabled for this reachable master"
                            );
                            unreachable_since = None;
                            false
                        }
                        Ok(None) => {
                            warn!(
                                "Master disconnected during upstream authentication, retrying in {}s",
                                backoff_secs
                            );
                            unreachable_since.get_or_insert_with(std::time::Instant::now);
                            false
                        }
                        Err(error) => {
                            warn!(
                                "Unable to read the upstream authentication response, retrying in {}s",
                                backoff_secs
                            );
                            if upstream_failure_counts_as_unreachable(&error) {
                                unreachable_since.get_or_insert_with(std::time::Instant::now);
                            } else {
                                unreachable_since = None;
                            }
                            false
                        }
                    };
                    if authenticated {
                        backoff_secs = MIN_BACKOFF_SECS;
                    } else {
                        drop(buf_reader);
                        drop(writer);
                        if maybe_auto_promote_replica(
                            &persistence,
                            &unreachable_since,
                            auto_failover,
                            failover_timeout,
                        )
                        .await
                        {
                            return;
                        }
                        if await_or_stop(
                            &mut stop_rx,
                            tokio::time::sleep(Duration::from_secs(backoff_secs)),
                        )
                        .await
                        .is_none()
                        {
                            return;
                        }
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                        continue;
                    }
                }

                let starting_offset = local_offset.load(Ordering::SeqCst);
                let known_replid = local_replid.load(Ordering::SeqCst);
                let sync_cmd = format!("SYNC3 {} {} HEARTBEAT\r\n", known_replid, starting_offset);
                let Some(sync_write) = await_or_stop(
                    &mut stop_rx,
                    write_replication_bytes(&mut writer, sync_cmd.as_bytes()),
                )
                .await
                else {
                    return;
                };
                if sync_write.is_err() {
                    warn!(
                        "Failed to send SYNC3 to master, retrying in {}s",
                        backoff_secs
                    );
                    unreachable_since.get_or_insert_with(std::time::Instant::now);
                    drop(buf_reader);
                    drop(writer);
                    if maybe_auto_promote_replica(
                        &persistence,
                        &unreachable_since,
                        auto_failover,
                        failover_timeout,
                    )
                    .await
                    {
                        return;
                    }
                    if await_or_stop(
                        &mut stop_rx,
                        tokio::time::sleep(Duration::from_secs(backoff_secs)),
                    )
                    .await
                    .is_none()
                    {
                        return;
                    }
                    backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    continue;
                }

                let Some(handshake_read) = await_or_stop(
                    &mut stop_rx,
                    read_replication_command_with_idle(
                        &mut buf_reader,
                        &mut scratch,
                        liveness_timeout,
                    ),
                )
                .await
                else {
                    return;
                };
                let mut handshake_was_unavailable = false;
                let handshake_reached_eof = matches!(&handshake_read, Ok(None));
                let handshake = match handshake_read {
                    Ok(Some(marker)) if is_authentication_rejection(&marker) => {
                        Err(PersistenceError::new(
                            "Master requires valid upstream replication credentials",
                        ))
                    }
                    Ok(Some(marker)) => parse_replica_sync_handshake(&marker),
                    Ok(None) => Err(PersistenceError::new(
                        "Master disconnected before the replication handshake",
                    )),
                    Err(error) => {
                        handshake_was_unavailable = upstream_failure_counts_as_unreachable(&error);
                        Err(PersistenceError::new(format!(
                            "Unable to read the replication handshake: {}",
                            error
                        )))
                    }
                };
                let handshake = match handshake {
                    Ok(handshake) => handshake,
                    Err(error) => {
                        warn!(
                            "Replication handshake failed: {}; retrying in {}s",
                            error, backoff_secs
                        );
                        if handshake_was_unavailable || handshake_reached_eof {
                            unreachable_since.get_or_insert_with(std::time::Instant::now);
                        } else {
                            unreachable_since = None;
                        }
                        drop(buf_reader);
                        drop(writer);
                        if maybe_auto_promote_replica(
                            &persistence,
                            &unreachable_since,
                            auto_failover,
                            failover_timeout,
                        )
                        .await
                        {
                            return;
                        }
                        if await_or_stop(
                            &mut stop_rx,
                            tokio::time::sleep(Duration::from_secs(backoff_secs)),
                        )
                        .await
                        .is_none()
                        {
                            return;
                        }
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                        continue;
                    }
                };

                if let ReplicaSyncHandshake::Partial {
                    replid,
                    requested_sequence,
                } = handshake
                    && (replid != known_replid || requested_sequence != starting_offset)
                {
                    warn!(
                        "Master returned a partial synchronization boundary that does not match the request"
                    );
                    unreachable_since = None;
                    drop(buf_reader);
                    drop(writer);
                    if await_or_stop(
                        &mut stop_rx,
                        tokio::time::sleep(Duration::from_secs(backoff_secs)),
                    )
                    .await
                    .is_none()
                    {
                        return;
                    }
                    backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    continue;
                }

                if matches!(handshake, ReplicaSyncHandshake::Full { .. }) {
                    if let Err(error) = begin_full_sync_reception(&persistence).await {
                        warn!(
                            "Unable to begin full synchronization safely: {}; retrying in {}s",
                            error, backoff_secs
                        );
                        unreachable_since = None;
                        drop(buf_reader);
                        drop(writer);
                        if await_or_stop(
                            &mut stop_rx,
                            tokio::time::sleep(Duration::from_secs(backoff_secs)),
                        )
                        .await
                        .is_none()
                        {
                            return;
                        }
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                        continue;
                    }
                    // The previous identity is no longer eligible for partial
                    // synchronization or promotion once FULLRESYNC3 is accepted.
                    local_replid.store(0, Ordering::SeqCst);
                    local_offset.store(0, Ordering::SeqCst);
                }

                info!("Connected to master, receiving synchronization data...");
                backoff_secs = MIN_BACKOFF_SECS;
                unreachable_since = None;

                // ACKs report only the last effect that is installed locally.
                // During a full transfer this remains the previous durable
                // boundary until the replacement snapshot is committed.
                let ack_offset = Arc::clone(&local_offset);
                let mut ack_stop_rx = lifecycle.subscribe_stop();
                let ack_task = AbortTaskOnDrop::new(tokio::spawn(async move {
                    loop {
                        if await_or_stop(
                            &mut ack_stop_rx,
                            tokio::time::sleep(Duration::from_secs(1)),
                        )
                        .await
                        .is_none()
                        {
                            break;
                        }
                        let off = ack_offset.load(Ordering::SeqCst);
                        let ack_cmd = format!("REPLCONF ACK {}\r\n", off);
                        let Some(write_result) = await_or_stop(
                            &mut ack_stop_rx,
                            write_replication_bytes(&mut writer, ack_cmd.as_bytes()),
                        )
                        .await
                        else {
                            break;
                        };
                        if write_result.is_err() {
                            break;
                        }
                    }
                }));

                let mut initial_failure_unavailable = false;
                let initial_sync_result: Result<(u64, u64), PersistenceError> = async {
                    match handshake {
                    ReplicaSyncHandshake::Full {
                        replid,
                        sequence,
                        entry_count,
                    } => {
                        let staging = ShardedStore::new();
                        let mut seen_keys = std::collections::HashSet::new();
                        let mut receive_result = Ok(());
                        for _ in 0..entry_count {
                            let Some(entry_result) = await_or_stop(
                                &mut stop_rx,
                                read_full_sync_entry(&mut buf_reader, &mut scratch),
                            )
                            .await
                            else {
                                receive_result = Err(PersistenceError::new(
                                    "Replica lifecycle stopped during full synchronization",
                                ));
                                break;
                            };
                            let (key, entry) = match entry_result {
                                Ok(entry) => entry,
                                Err(error) => {
                                    initial_failure_unavailable =
                                        error.indicates_upstream_unavailable();
                                    receive_result = Err(PersistenceError::new(format!(
                                        "Master sent an invalid full synchronization entry: {}",
                                        error
                                    )));
                                    break;
                                }
                            };
                            if !seen_keys.insert(key.clone()) {
                                receive_result = Err(PersistenceError::new(
                                    "Master sent a duplicate key in the full synchronization snapshot",
                                ));
                                break;
                            }
                            staging.apply_entry(key, entry);
                        }
                        if let Err(error) = receive_result {
                            Err(error)
                        } else {
                            let done_read = await_or_stop(
                                &mut stop_rx,
                                read_replication_command_with_idle(
                                    &mut buf_reader,
                                    &mut scratch,
                                    liveness_timeout,
                                ),
                            )
                            .await
                            .ok_or_else(|| {
                                PersistenceError::new(
                                    "Replica lifecycle stopped before full synchronization completed",
                                )
                            })?;
                            let done = match done_read {
                                Ok(Some(marker)) => parse_replica_sync_done(&marker, replid),
                                Ok(None) => {
                                    initial_failure_unavailable = true;
                                    Err(PersistenceError::new(
                                        "Master disconnected before completing full synchronization",
                                    ))
                                }
                                Err(error) => {
                                    initial_failure_unavailable =
                                        upstream_failure_counts_as_unreachable(&error);
                                    Err(PersistenceError::new(format!(
                                        "Unable to read full synchronization completion: {}",
                                        error
                                    )))
                                }
                            }?;
                            if done != sequence {
                                Err(PersistenceError::new(
                                    "Full synchronization completion sequence does not match its snapshot boundary",
                                ))
                            } else {
                                install_full_sync(
                                    &store,
                                    &persistence,
                                    replid,
                                    sequence,
                                    staging,
                                )
                                .await?;
                                local_replid.store(replid, Ordering::SeqCst);
                                local_offset.store(sequence, Ordering::SeqCst);
                                info!(
                                    "Installed full synchronization at sequence {} with {} entries",
                                    sequence, entry_count
                                );
                                Ok((replid, sequence))
                            }
                        }
                    }
                    ReplicaSyncHandshake::Partial {
                        replid,
                        requested_sequence: _,
                    } => loop {
                        let Some(frame_result) = await_or_stop(
                            &mut stop_rx,
                            read_replication_command_with_idle(
                                &mut buf_reader,
                                &mut scratch,
                                liveness_timeout,
                            ),
                        )
                        .await
                        else {
                            break Err(PersistenceError::new(
                                "Replica lifecycle stopped during partial synchronization",
                            ));
                        };
                        let frame = match frame_result {
                            Ok(Some(frame)) => frame,
                            Ok(None) => {
                                initial_failure_unavailable = true;
                                break Err(PersistenceError::new(
                                    "Master disconnected during partial synchronization",
                                ));
                            }
                            Err(error) => {
                                initial_failure_unavailable =
                                    upstream_failure_counts_as_unreachable(&error);
                                break Err(PersistenceError::new(format!(
                                    "Unable to read partial synchronization data: {}",
                                    error
                                )));
                            }
                        };
                        if frame.first().map(String::as_str) == Some("+SYNCDONE3") {
                            let done = parse_replica_sync_done(&frame, replid)?;
                            let installed = local_offset.load(Ordering::SeqCst);
                            if done != installed {
                                break Err(PersistenceError::new(
                                    "Partial synchronization completion sequence does not match the installed sequence",
                                ));
                            }
                            info!("Completed partial synchronization at sequence {}", done);
                            break Ok((replid, done));
                        }
                        let Some(effect_result) = await_or_stop(
                            &mut stop_rx,
                            read_replication_effect(&frame, &mut buf_reader, &mut scratch),
                        )
                        .await
                        else {
                            break Err(PersistenceError::new(
                                "Replica lifecycle stopped while receiving a partial effect",
                            ));
                        };
                        let (effect_sequence, batch) = match effect_result {
                            Ok(effect) => effect,
                            Err(error) => {
                                initial_failure_unavailable =
                                    error.indicates_upstream_unavailable();
                                break Err(error);
                            }
                        };
                        persist_and_apply_replica_effect(
                            &store,
                            &persistence,
                            effect_sequence,
                            &batch,
                        )
                        .await?;
                        local_offset.store(effect_sequence, Ordering::SeqCst);
                    },
                }
                }
                .await;

                let (stream_replid, stream_offset) = match initial_sync_result {
                    Ok(boundary) => boundary,
                    Err(error) => {
                        warn!("Initial replication synchronization failed: {}", error);
                        ack_task.abort_and_wait().await;
                        drop(buf_reader);
                        if lifecycle.stop_requested() {
                            return;
                        }
                        if initial_failure_unavailable {
                            unreachable_since.get_or_insert_with(std::time::Instant::now);
                        } else {
                            unreachable_since = None;
                        }
                        if maybe_auto_promote_replica(
                            &persistence,
                            &unreachable_since,
                            auto_failover,
                            failover_timeout,
                        )
                        .await
                        {
                            return;
                        }
                        if await_or_stop(
                            &mut stop_rx,
                            tokio::time::sleep(Duration::from_secs(backoff_secs)),
                        )
                        .await
                        .is_none()
                        {
                            return;
                        }
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                        continue;
                    }
                };
                local_replid.store(stream_replid, Ordering::SeqCst);
                local_offset.store(stream_offset, Ordering::SeqCst);

                loop {
                    let Some(frame_result) = await_or_stop(
                        &mut stop_rx,
                        read_replication_command_with_idle(
                            &mut buf_reader,
                            &mut scratch,
                            liveness_timeout,
                        ),
                    )
                    .await
                    else {
                        break;
                    };
                    match frame_result {
                        Ok(Some(frame)) if !frame.is_empty() => {
                            match parse_upstream_heartbeat(
                                &frame,
                                local_offset.load(Ordering::SeqCst),
                            ) {
                                Ok(true) => continue,
                                Ok(false) => {}
                                Err(error) => {
                                    warn!("Master sent an invalid heartbeat: {}", error);
                                    unreachable_since = None;
                                    break;
                                }
                            }
                            let Some(effect_result) = await_or_stop(
                                &mut stop_rx,
                                read_replication_effect(&frame, &mut buf_reader, &mut scratch),
                            )
                            .await
                            else {
                                break;
                            };
                            let (effect_sequence, batch) = match effect_result {
                                Ok(effect) => effect,
                                Err(error) => {
                                    warn!("Master sent an invalid replication effect: {}", error);
                                    if error.indicates_upstream_unavailable() {
                                        unreachable_since
                                            .get_or_insert_with(std::time::Instant::now);
                                    } else {
                                        unreachable_since = None;
                                    }
                                    break;
                                }
                            };
                            if let Err(error) = persist_and_apply_replica_effect(
                                &store,
                                &persistence,
                                effect_sequence,
                                &batch,
                            )
                            .await
                            {
                                warn!("Unable to install replicated effect: {}", error);
                                unreachable_since = None;
                                break;
                            }
                            local_offset.store(effect_sequence, Ordering::SeqCst);
                        }
                        Ok(Some(_)) => continue,
                        Ok(None) => {
                            warn!("Master disconnected, retrying in {}s", backoff_secs);
                            unreachable_since.get_or_insert_with(std::time::Instant::now);
                            break;
                        }
                        Err(error) => {
                            warn!(
                                "Error reading from master: {}; retrying in {}s",
                                error, backoff_secs
                            );
                            if upstream_failure_counts_as_unreachable(&error) {
                                unreachable_since.get_or_insert_with(std::time::Instant::now);
                            } else {
                                unreachable_since = None;
                            }
                            break;
                        }
                    }
                }
                ack_task.abort_and_wait().await;
                drop(buf_reader);
                if lifecycle.stop_requested() {
                    info!("Former upstream connection closed before promotion");
                    return;
                }
                if maybe_auto_promote_replica(
                    &persistence,
                    &unreachable_since,
                    auto_failover,
                    failover_timeout,
                )
                .await
                {
                    return;
                }
            }
            Err(error) => {
                warn!(
                    "Master {} is unreachable: {}; retrying in {}s",
                    master_addr, error, backoff_secs
                );
                unreachable_since.get_or_insert_with(std::time::Instant::now);
                if maybe_auto_promote_replica(
                    &persistence,
                    &unreachable_since,
                    auto_failover,
                    failover_timeout,
                )
                .await
                {
                    return;
                }
            }
        }

        if await_or_stop(
            &mut stop_rx,
            tokio::time::sleep(Duration::from_secs(backoff_secs)),
        )
        .await
        .is_none()
        {
            return;
        }
        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    static TEST_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TestPersistenceDirectory {
        root: PathBuf,
        paths: PersistencePaths,
    }

    impl TestPersistenceDirectory {
        fn new() -> Self {
            let sequence = TEST_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::SeqCst);
            let root = env::temp_dir().join(format!(
                "onyxdb-persistence-test-{}-{}",
                std::process::id(),
                sequence
            ));
            fs::create_dir_all(&root).unwrap();
            let paths = PersistencePaths {
                snapshot: root.join("onyx.snapshot"),
                snapshot_temp: root.join("onyx.snapshot.tmp"),
                snapshot_backup: root.join("onyx.snapshot.previous"),
                binlog: root.join("onyx.binlog"),
                replica_state: root.join("onyx.replica"),
                replica_state_temp: root.join("onyx.replica.tmp"),
            };
            Self { root, paths }
        }
    }

    impl Drop for TestPersistenceDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn test_persistence(
        paths: PersistencePaths,
        log_tx: mpsc::Sender<LogMessage>,
        initial_sequence: u64,
    ) -> Arc<Persistence> {
        let (replica_tx, _) = tokio::sync::broadcast::channel(4096);
        let (pubsub_tx, _) = tokio::sync::broadcast::channel(16);
        Arc::new(Persistence {
            commit_runtime: CommitRuntime::new(BinlogHandle::new(log_tx), initial_sequence, paths),
            master_commit: std::sync::OnceLock::new(),
            replica_tx,
            promote_to_master: Arc::new(AtomicBool::new(false)),
            backlog: std::sync::Mutex::new(std::collections::VecDeque::new()),
            next_replica_id: AtomicU64::new(0),
            replica_status: std::sync::Mutex::new(std::collections::HashMap::new()),
            pubsub_tx,
            next_subscriber_id: AtomicU64::new(0),
            subscriptions: std::sync::Mutex::new(std::collections::HashMap::new()),
            upstream_replid: AtomicU64::new(0),
            replication_ready: AtomicBool::new(false),
            replica_lifecycle: Arc::new(ReplicaLifecycle::new(true)),
        })
    }

    fn enable_master_commit_coordinator(store: &Arc<ShardedStore>, persistence: &Arc<Persistence>) {
        assert!(
            persistence
                .master_commit
                .set(MasterCommitCoordinator::start(
                    Arc::clone(store),
                    persistence,
                ))
                .is_ok()
        );
    }

    async fn wait_for_coordinator_queue(persistence: &Persistence, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let queued = persistence
                    .master_commit
                    .get()
                    .expect("coordinator must be installed")
                    .pending_requests();
                if queued >= expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("commit requests did not enter the bounded coordinator queue");
    }

    async fn receive_append_group(
        receiver: &mut mpsc::Receiver<LogMessage>,
    ) -> (Vec<(u64, Vec<u8>)>, oneshot::Sender<StorageResult>) {
        loop {
            match receiver.recv().await.expect("binlog channel closed") {
                LogMessage::Append {
                    records,
                    completion,
                } => return (records, completion),
                LogMessage::Barrier { completion }
                | LogMessage::Flush { completion }
                | LogMessage::SyncData { completion }
                | LogMessage::Truncate { completion } => {
                    let _ = completion.send(Ok(()));
                }
            }
        }
    }

    #[tokio::test]
    async fn prometheus_metrics_include_valid_commit_and_compaction_observability() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, _receiver) = mpsc::channel(1);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);

        let body = format_prometheus_metrics(&store, &persistence);
        assert!(!body.lines().any(|line| line.starts_with("TYPE ")));
        assert!(body.contains("# TYPE onyxdb_keys_total gauge\n"));
        assert!(body.contains("onyxdb_commit_queue_depth 0\n"));
        assert!(body.contains("onyxdb_binlog_append_attempts_total 0\n"));
        assert!(body.contains("onyxdb_compaction_completed_total 0\n"));

        let help_count = body
            .lines()
            .filter(|line| line.starts_with("# HELP "))
            .count();
        let type_count = body
            .lines()
            .filter(|line| line.starts_with("# TYPE "))
            .count();
        let samples = body
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .collect::<Vec<_>>();
        assert_eq!(help_count, samples.len());
        assert_eq!(type_count, samples.len());
        for sample in samples {
            let fields = sample.split_whitespace().collect::<Vec<_>>();
            assert_eq!(fields.len(), 2, "invalid Prometheus sample: {sample}");
            fields[1]
                .parse::<f64>()
                .unwrap_or_else(|_| panic!("invalid Prometheus value: {sample}"));
        }
    }

    fn append_test_binlog_record(paths: &PersistencePaths, sequence: u64, args: &[&str]) {
        let command_args: Vec<String> = args.iter().map(|value| (*value).to_string()).collect();
        let store = ShardedStore::new();
        let recovery =
            load_data_from_paths(&store, paths).expect("existing persistence state must load");
        let keys = persistent_keys_for_command(&command_args);
        let before = capture_entries(&store, &keys);
        let _ = execute_command(&store, &command_args);
        let batch = derive_committed_batch(&store, &keys, &before, &[]).unwrap_or_else(|| {
            assert!(sequence <= recovery.snapshot_watermark);
            CommittedBatch {
                effects: vec![CommittedEffect::Delete {
                    key: Bytes::from_static(b"skipped-before-snapshot"),
                }],
            }
        });
        let effect_record = encode_committed_batch(&batch).expect("encodable committed effect");
        let record = encode_versioned_binlog_record(sequence, &effect_record)
            .expect("encodable versioned binlog record");
        append_raw_binlog_record(paths, &record);
    }

    fn append_raw_binlog_record(paths: &PersistencePaths, record: &[u8]) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&paths.binlog)
            .unwrap();
        file.write_all(&(record.len() as u32).to_be_bytes())
            .unwrap();
        file.write_all(record).unwrap();
        file.sync_all().unwrap();
    }

    async fn start_test_persistence(
        paths: PersistencePaths,
        initial_sequence: u64,
    ) -> (Arc<Persistence>, tokio::task::JoinHandle<()>) {
        let (log_tx, receiver) = mpsc::channel(1024);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&paths.binlog)
            .unwrap();
        let persistence = test_persistence(paths, log_tx, initial_sequence);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::new(std::sync::Mutex::new(file)),
            FsyncPolicy::EverySec,
        ));
        (persistence, worker)
    }

    struct PausedAppendWorker {
        log_tx: mpsc::Sender<LogMessage>,
        persisted: oneshot::Receiver<()>,
        release: oneshot::Sender<()>,
        completed: oneshot::Receiver<()>,
        handle: tokio::task::JoinHandle<()>,
    }

    fn start_paused_append_worker(binlog_path: PathBuf) -> PausedAppendWorker {
        let (log_tx, mut receiver) = mpsc::channel(8);
        let (persisted_tx, persisted) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let (completed_tx, completed) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let mut persisted_tx = Some(persisted_tx);
            let mut release_rx = Some(release_rx);
            let mut completed_tx = Some(completed_tx);
            while let Some(message) = receiver.recv().await {
                match message {
                    LogMessage::Append {
                        records,
                        completion,
                    } => {
                        let mut file = OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&binlog_path)
                            .unwrap();
                        for (sequence, record) in records {
                            let encoded =
                                encode_versioned_binlog_record(sequence, &record).unwrap();
                            file.write_all(&(encoded.len() as u32).to_be_bytes())
                                .unwrap();
                            file.write_all(&encoded).unwrap();
                        }
                        file.sync_all().unwrap();
                        persisted_tx.take().unwrap().send(()).unwrap();
                        release_rx.take().unwrap().await.unwrap();
                        let _ = completion.send(Ok(()));
                        completed_tx.take().unwrap().send(()).unwrap();
                    }
                    LogMessage::Barrier { completion }
                    | LogMessage::Flush { completion }
                    | LogMessage::SyncData { completion }
                    | LogMessage::Truncate { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                }
            }
        });
        PausedAppendWorker {
            log_tx,
            persisted,
            release,
            completed,
            handle,
        }
    }

    async fn wait_for_commit_boundary(persistence: &Persistence) {
        let guard = persistence.write_gate.lock().await;
        drop(guard);
    }

    async fn apply_test_command(
        store: &Arc<ShardedStore>,
        persistence: &Arc<Persistence>,
        args: &[&str],
    ) {
        let command: Vec<String> = args.iter().map(|value| (*value).to_string()).collect();
        let outcome = execute_ordered_command(store, persistence, &command).await;
        assert_eq!(
            outcome.mutation,
            MutationState::Committed,
            "command was not committed as a mutation: {args:?}"
        );
        assert!(
            !matches!(&outcome.response, RESPValue::Error(_)),
            "mutation failed: {:?}",
            outcome.response
        );
    }

    #[test]
    fn test_set_and_get() {
        let store = ShardedStore::new();
        store.set("key1".to_string(), "value1".to_string());
        assert_eq!(store.get("key1"), Ok(Some("value1".to_string())));
    }

    #[test]
    fn test_get_key_not_found() {
        let store = ShardedStore::new();
        assert_eq!(store.get("not_found"), Ok(None));
    }

    #[test]
    fn test_incr_from_zero() {
        let store = ShardedStore::new();
        assert_eq!(store.incr("counter"), Ok(1));
        assert_eq!(store.incr("counter"), Ok(2));
    }

    #[test]
    fn test_incrby() {
        let store = ShardedStore::new();
        assert_eq!(store.incrby("counter", 5), Ok(5));
        assert_eq!(store.incrby("counter", -2), Ok(3));
    }

    #[test]
    fn test_delete() {
        let store = ShardedStore::new();
        store.set("key".to_string(), "value".to_string());
        assert!(store.delete("key"));
        assert!(!store.delete("key"));
        assert_eq!(store.get("key"), Ok(None));
    }

    #[test]
    fn test_lpush_and_lrange() {
        let store = ShardedStore::new();
        assert_eq!(store.lpush("list", "one".to_string()), Ok(1));
        assert_eq!(store.lpush("list", "two".to_string()), Ok(2));
        assert_eq!(
            store.lrange("list", 0, -1),
            Ok(vec!["two".to_string(), "one".to_string()])
        );
    }

    #[test]
    fn test_hash() {
        let store = ShardedStore::new();
        assert_eq!(store.hset("h", "field", "value"), Ok(true));
        assert_eq!(store.hget("h", "field"), Ok(Some("value".to_string())));
        assert_eq!(store.hget("h", "non_existent"), Ok(None));
    }

    #[test]
    fn test_set_type() {
        let store = ShardedStore::new();
        assert_eq!(store.sadd("s", "a"), Ok(true));
        assert_eq!(store.sadd("s", "a"), Ok(false));
        assert_eq!(store.sismember("s", "a"), Ok(true));
        assert_eq!(store.sismember("s", "b"), Ok(false));
    }

    #[test]
    fn logical_presence_is_independent_of_value_type() {
        let store = ShardedStore::new();
        store
            .json_set("document", "$", serde_json::json!({"value": 1}))
            .unwrap();

        let exists = execute_command(&store, &["EXISTS".to_string(), "document".to_string()]);
        assert!(matches!(exists.response, RESPValue::Integer(1)));

        let get = execute_command(&store, &["GET".to_string(), "document".to_string()]);
        assert!(matches!(
            get.response,
            RESPValue::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
        assert_eq!(get.mutation, MutationState::NotRequested);
    }

    #[test]
    fn type_specific_mutations_reject_and_preserve_incompatible_values() {
        let store = ShardedStore::new();
        assert_eq!(store.lpush("value", "original".to_string()), Ok(1));

        for command in ["APPEND", "HSET", "SADD"] {
            let args = match command {
                "HSET" => vec![
                    command.to_string(),
                    "value".to_string(),
                    "field".to_string(),
                    "replacement".to_string(),
                ],
                _ => vec![
                    command.to_string(),
                    "value".to_string(),
                    "replacement".to_string(),
                ],
            };
            let outcome = execute_command(&store, &args);
            assert!(matches!(
                outcome.response,
                RESPValue::Error(ref message) if message.starts_with("WRONGTYPE")
            ));
            assert_eq!(outcome.mutation, MutationState::NoChange);
            assert_eq!(
                store.lrange("value", 0, -1),
                Ok(vec!["original".to_string()])
            );
        }
    }

    #[test]
    fn expired_entries_are_absent_to_conditional_and_collection_mutations() {
        let store = ShardedStore::new();
        store.set_value(
            Bytes::from_static(b"conditional"),
            OnyxValue::Blob(Bytes::from_static(b"stale")),
            Some(now()),
        );
        assert!(store.setnx("conditional", "fresh"));
        assert_eq!(store.get("conditional"), Ok(Some("fresh".to_string())));

        store.set_value(
            Bytes::from_static(b"list"),
            OnyxValue::List(vec![Bytes::from_static(b"stale")]),
            Some(now()),
        );
        assert_eq!(store.rpush("list", "fresh".to_string()), Ok(1));
        assert_eq!(store.lrange("list", 0, -1), Ok(vec!["fresh".to_string()]));
        assert_eq!(store.get_expiry("list"), None);
    }

    #[test]
    fn json_root_replacement_rejects_an_incompatible_existing_value() {
        let store = ShardedStore::new();
        store.set("value".to_string(), "original".to_string());

        let outcome = execute_command(
            &store,
            &[
                "JSON.SET".to_string(),
                "value".to_string(),
                "$".to_string(),
                r#"{"replacement":true}"#.to_string(),
            ],
        );
        assert!(matches!(
            outcome.response,
            RESPValue::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
        assert_eq!(outcome.mutation, MutationState::NoChange);
        assert_eq!(store.get("value"), Ok(Some("original".to_string())));
    }

    #[test]
    fn expired_entries_are_absent_to_all_presence_sensitive_primitives() {
        let store = ShardedStore::new();
        let expired = Some(now());

        for key in ["delete", "expire", "rename", "copy", "conditional"] {
            store.set_value(
                Bytes::copy_from_slice(key.as_bytes()),
                OnyxValue::Blob(Bytes::from_static(b"stale")),
                expired,
            );
        }

        assert!(!store.delete("delete"));
        assert!(!store.expire("expire", 10));
        assert!(!store.rename("rename", "renamed"));
        assert!(!store.copy("copy", "copied"));
        assert!(!store.set_conditional_value(
            Bytes::from_static(b"conditional"),
            OnyxValue::Blob(Bytes::from_static(b"replacement")),
            None,
            Some(false),
        ));
        assert_eq!(store.stats().total_keys, 0);
        assert_eq!(store.used_memory_bytes(), 0);
    }

    #[test]
    fn removing_the_last_collection_element_deletes_the_key_atomically() {
        let store = ShardedStore::new();

        assert_eq!(store.lpush("list", "value".to_string()), Ok(1));
        assert_eq!(store.lpop("list"), Ok(Some("value".to_string())));
        assert!(!store.exists("list"));

        assert_eq!(store.hset("hash", "field", "value"), Ok(true));
        assert_eq!(store.hdel("hash", "field"), Ok(true));
        assert!(!store.exists("hash"));

        assert_eq!(store.sadd("set", "member"), Ok(true));
        assert_eq!(store.srem("set", "member"), Ok(true));
        assert!(!store.exists("set"));
    }

    #[test]
    fn getset_clears_ttl_but_wrong_type_preserves_the_original_entry() {
        let store = ShardedStore::new();
        store.set("string".to_string(), "old".to_string());
        assert!(store.expire("string", 60));
        assert_eq!(store.getset("string", "new"), Ok(Some("old".to_string())));
        assert_eq!(store.get_expiry("string"), None);

        assert_eq!(store.lpush("list", "value".to_string()), Ok(1));
        assert_eq!(
            store.getset("list", "replacement"),
            Err(StoreError::WrongType)
        );
        assert_eq!(store.lrange("list", 0, -1), Ok(vec!["value".to_string()]));

        store.set("long-lived".to_string(), "value".to_string());
        assert!(store.expire_at("long-lived", u64::MAX));
        assert_eq!(store.ttl("long-lived"), i64::MAX);
    }

    #[test]
    fn empty_values_are_data_and_zero_duration_set_does_not_delete() {
        let store = ShardedStore::new();
        let set = execute_command(
            &store,
            &["SET".to_string(), "empty".to_string(), String::new()],
        );
        assert!(matches!(
            set.response,
            RESPValue::SimpleString(ref value) if value == "OK"
        ));
        assert_eq!(set.mutation, MutationState::Tentative);
        assert_eq!(store.get("empty"), Ok(Some(String::new())));

        store.set("protected".to_string(), "original".to_string());
        let invalid = execute_command(
            &store,
            &[
                "SET".to_string(),
                "protected".to_string(),
                "replacement".to_string(),
                "PX".to_string(),
                "0".to_string(),
            ],
        );
        assert!(matches!(invalid.response, RESPValue::Error(_)));
        assert_eq!(invalid.mutation, MutationState::NoChange);
        assert_eq!(store.get("protected"), Ok(Some("original".to_string())));
    }

    #[test]
    fn expired_replication_put_cannot_resurrect_or_mask_a_key() {
        let replica = ShardedStore::new();
        replica.set("key".to_string(), "live".to_string());
        let batch = CommittedBatch::new(vec![CommittedEffect::Put {
            key: Bytes::from_static(b"key"),
            entry: PersistentEntry {
                value: OnyxValue::List(vec![Bytes::from_static(b"stale")]),
                expires_at: Some(now()),
            },
        }])
        .unwrap();

        apply_committed_batch(&replica, &batch);
        assert!(!replica.exists("key"));
        assert_eq!(replica.rpush("key", "fresh".to_string()), Ok(1));
        assert_eq!(replica.lrange("key", 0, -1), Ok(vec!["fresh".to_string()]));
    }

    #[test]
    fn json_observes_the_same_expired_and_wrong_type_boundary() {
        let store = ShardedStore::new();
        store.set_value(
            Bytes::from_static(b"document"),
            OnyxValue::Blob(Bytes::from_static(b"stale")),
            Some(now()),
        );
        assert_eq!(
            store.json_set("document", "$", serde_json::json!({"fresh": true})),
            Ok(())
        );
        assert_eq!(
            store.json_get("document", "$.fresh"),
            Ok(Some("true".to_string()))
        );

        store.set("string".to_string(), "preserve".to_string());
        assert!(
            store
                .json_get("string", "$")
                .is_err_and(|error| error.starts_with("WRONGTYPE"))
        );
        assert!(
            store
                .json_del("string", "$")
                .is_err_and(|error| error.starts_with("WRONGTYPE"))
        );
        assert_eq!(store.get("string"), Ok(Some("preserve".to_string())));
    }

    #[test]
    fn test_ttl_expiration() {
        let store = ShardedStore::new();
        store.set("temp".to_string(), "val".to_string());
        assert_eq!(store.ttl("temp"), -1);
    }

    #[test]
    fn test_rename() {
        let store = ShardedStore::new();
        store.set("old".to_string(), "value".to_string());
        assert!(store.rename("old", "new"));
        assert_eq!(store.get("old"), Ok(None));
        assert_eq!(store.get("new"), Ok(Some("value".to_string())));
    }

    #[test]
    fn test_append() {
        let store = ShardedStore::new();
        assert_eq!(store.append("s", "hello"), Ok(5));
        assert_eq!(store.append("s", "world"), Ok(10));
        assert_eq!(store.get("s"), Ok(Some("helloworld".to_string())));
    }

    #[test]
    fn test_strlen() {
        let store = ShardedStore::new();
        store.set("s".to_string(), "hello".to_string());
        assert_eq!(store.strlen("s"), Ok(5));
        assert_eq!(store.strlen("non_esiste"), Ok(0));
    }
    // ============================================================
    // Every persisted command must survive a legacy command-record round trip.
    // ============================================================

    #[test]
    fn test_binlog_roundtrip_set() {
        let args = vec!["SET".to_string(), "k".to_string(), "v".to_string()];
        let record = command_to_binary_record("SET", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(decoded, vec!["SET", "k", "v"]);
    }

    #[test]
    fn persistence_pipeline_preserves_argument_boundaries() {
        let store = ShardedStore::new();
        let cases = vec![
            vec![
                "SET".to_string(),
                "key with spaces".to_string(),
                "hello world\r\nnext line".to_string(),
            ],
            vec![
                "JSON.SET".to_string(),
                "document".to_string(),
                "$".to_string(),
                r#"{"name": "Marco Rossi"}"#.to_string(),
            ],
        ];

        for args in cases {
            let normalized_args = normalize_for_log(&store, &args);
            let command = normalized_args[0].as_str();
            let record = command_to_binary_record(command, &normalized_args, None).unwrap();
            let decoded = binary_record_to_args(&record).unwrap();

            assert_eq!(decoded, args);
        }
    }

    #[test]
    fn expiring_set_normalization_preserves_argument_boundaries() {
        let store = ShardedStore::new();
        store.set_value(
            Bytes::from("key with spaces"),
            OnyxValue::Blob(Bytes::from("hello world")),
            Some(u64::MAX),
        );
        let args = vec![
            "SET".to_string(),
            "key with spaces".to_string(),
            "hello world".to_string(),
            "EX".to_string(),
            "10".to_string(),
        ];

        let normalized_args = normalize_for_log(&store, &args);
        let record = command_to_binary_record("SET", &normalized_args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();

        assert_eq!(
            decoded,
            vec![
                "SET",
                "key with spaces",
                "hello world",
                "EXAT",
                "18446744073709551615"
            ]
        );
    }

    #[tokio::test]
    async fn replication_writes_to_unresponsive_peers_are_bounded() {
        let (mut writer, _reader) = tokio::io::duplex(1);
        let error = write_replication_bytes_with_timeout(
            &mut writer,
            &[0u8; 4096],
            Duration::from_millis(25),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn replication_wire_preserves_argument_boundaries() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let args = vec![
            "JSON.SET".to_string(),
            "document with spaces".to_string(),
            "$".to_string(),
            r#"{"message": "hello world"}"#.to_string(),
        ];
        let encoded = encode_replication_command(&args);

        let sender = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream.write_all(&encoded).await.unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (reader, _) = stream.into_split();
        let mut reader = TokioBufReader::new(reader);
        let mut scratch = Vec::new();
        let decoded = resp::read_command_with_limits(&mut reader, &mut scratch, CLIENT_RESP_LIMITS)
            .await
            .unwrap()
            .unwrap();
        sender.await.unwrap();

        assert_eq!(decoded, args);
    }

    #[tokio::test]
    async fn replication_reader_rejects_oversized_lines_before_buffering_the_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let oversized_line = format!("+{}\r\n", "x".repeat(1024));

        let sender = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream.write_all(oversized_line.as_bytes()).await.unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (reader, _) = stream.into_split();
        let mut reader = TokioBufReader::new(reader);
        let mut scratch = Vec::new();
        let error = read_replication_command(&mut reader, &mut scratch)
            .await
            .unwrap_err();
        sender.await.unwrap();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn replication_reader_rejects_bulk_strings_without_crlf_terminators() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let sender = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream.write_all(b"*1\r\n$3\r\nSETxx").await.unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (reader, _) = stream.into_split();
        let mut reader = TokioBufReader::new(reader);
        let mut scratch = Vec::new();
        let error = read_replication_command(&mut reader, &mut scratch)
            .await
            .unwrap_err();
        sender.await.unwrap();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn obp_idle_and_partial_frames_have_bounded_lifetimes() {
        for prefix in [Vec::new(), vec![protocol::OBP_MAGIC]] {
            let directory = TestPersistenceDirectory::new();
            let (log_tx, _log_rx) = mpsc::channel::<LogMessage>(1);
            let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
            let store = Arc::new(ShardedStore::new());
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                handle_obp_client_with_timeouts(
                    stream,
                    store,
                    persistence,
                    Duration::from_millis(25),
                    Duration::from_millis(25),
                )
                .await;
            });

            let mut client = TcpStream::connect(address).await.unwrap();
            if !prefix.is_empty() {
                client.write_all(&prefix).await.unwrap();
                client.flush().await.unwrap();
            }

            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .expect("OBP connection must close at its configured deadline")
                .unwrap();
        }
    }

    #[tokio::test]
    async fn clean_shutdown_recovery_does_not_duplicate_non_idempotent_mutations() {
        let directory = TestPersistenceDirectory::new();
        let store = Arc::new(ShardedStore::new());
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;

        apply_test_command(&store, &persistence, &["SET", "plain", "value"]).await;
        apply_test_command(&store, &persistence, &["INCR", "counter"]).await;
        apply_test_command(&store, &persistence, &["INCRBY", "counter", "4"]).await;
        apply_test_command(&store, &persistence, &["APPEND", "text", "alpha"]).await;
        apply_test_command(&store, &persistence, &["APPEND", "text", "-beta"]).await;
        apply_test_command(&store, &persistence, &["LPUSH", "items", "one"]).await;
        apply_test_command(&store, &persistence, &["LPUSH", "items", "two"]).await;
        apply_test_command(
            &store,
            &persistence,
            &["JSON.SET", "document", "$", r#"{"visits":0}"#],
        )
        .await;
        apply_test_command(
            &store,
            &persistence,
            &["JSON.NUMINCRBY", "document", "$.visits", "3"],
        )
        .await;

        let watermark = compact_store(&store, &persistence).await.unwrap();
        assert_eq!(watermark, 9);
        assert_eq!(fs::metadata(&directory.paths.binlog).unwrap().len(), 0);
        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(state.last_sequence, 9);
        assert_eq!(recovered.get("plain"), Ok(Some("value".to_string())));
        assert_eq!(recovered.get("counter"), Ok(Some("5".to_string())));
        assert_eq!(recovered.get("text"), Ok(Some("alpha-beta".to_string())));
        assert_eq!(
            recovered.lrange("items", 0, -1),
            Ok(vec!["two".to_string(), "one".to_string()])
        );
        let visits = recovered
            .json_get("document", "$.visits")
            .unwrap()
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert_eq!(visits, 3.0);
    }

    #[test]
    fn recovery_skips_snapshot_sequences_and_preserves_post_boundary_writes() {
        let directory = TestPersistenceDirectory::new();
        let snapshot_store = ShardedStore::new();
        let snapshot_commands = [
            vec!["SET", "plain", "value"],
            vec!["INCR", "counter"],
            vec!["APPEND", "text", "alpha"],
            vec!["LPUSH", "items", "one"],
            vec!["JSON.SET", "document", "$", r#"{"visits":0}"#],
            vec!["JSON.NUMINCRBY", "document", "$.visits", "2"],
        ];
        for command in &snapshot_commands {
            let args: Vec<String> = command.iter().map(|value| (*value).to_string()).collect();
            let outcome = execute_command(&snapshot_store, &args);
            assert!(!matches!(outcome.response, RESPValue::Error(_)));
        }
        write_snapshot_file(snapshot_store.raw_entries(), 6, &directory.paths).unwrap();

        for (index, command) in snapshot_commands.iter().enumerate() {
            append_test_binlog_record(&directory.paths, index as u64 + 1, command);
        }
        append_test_binlog_record(&directory.paths, 7, &["INCRBY", "counter", "4"]);
        append_test_binlog_record(&directory.paths, 8, &["APPEND", "text", "-beta"]);
        append_test_binlog_record(&directory.paths, 9, &["LPUSH", "items", "two"]);
        append_test_binlog_record(
            &directory.paths,
            10,
            &["JSON.NUMINCRBY", "document", "$.visits", "3"],
        );

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(state.last_sequence, 10);
        assert_eq!(recovered.get("counter"), Ok(Some("5".to_string())));
        assert_eq!(recovered.get("text"), Ok(Some("alpha-beta".to_string())));
        assert_eq!(
            recovered.lrange("items", 0, -1),
            Ok(vec!["two".to_string(), "one".to_string()])
        );
        let visits = recovered
            .json_get("document", "$.visits")
            .unwrap()
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert_eq!(visits, 5.0);
    }

    #[tokio::test]
    async fn failed_snapshot_creation_does_not_request_binlog_truncation() {
        let directory = TestPersistenceDirectory::new();
        let previous_store = ShardedStore::new();
        previous_store.set("safe".to_string(), "old".to_string());
        write_snapshot_file(previous_store.raw_entries(), 1, &directory.paths).unwrap();
        append_test_binlog_record(&directory.paths, 2, &["APPEND", "safe", "-log"]);
        let previous_snapshot = fs::read(&directory.paths.snapshot).unwrap();

        let mut failing_paths = directory.paths.clone();
        failing_paths.snapshot_temp = directory.root.join("missing").join("snapshot.tmp");
        let (log_tx, mut receiver) = mpsc::channel(8);
        let truncate_requested = Arc::new(AtomicBool::new(false));
        let truncate_observer = Arc::clone(&truncate_requested);
        let worker = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    LogMessage::Barrier { completion } | LogMessage::Flush { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                    LogMessage::Truncate { completion } => {
                        truncate_observer.store(true, Ordering::SeqCst);
                        let _ = completion.send(Ok(()));
                    }
                    LogMessage::Append { completion, .. } => {
                        let _ = completion.send(Ok(()));
                    }
                    LogMessage::SyncData { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                }
            }
        });
        let persistence = test_persistence(failing_paths, log_tx, 2);
        let store = Arc::new(ShardedStore::new());
        store.set("safe".to_string(), "new".to_string());

        assert!(compact_store(&store, &persistence).await.is_err());
        assert!(!truncate_requested.load(Ordering::SeqCst));
        assert_eq!(
            fs::read(&directory.paths.snapshot).unwrap(),
            previous_snapshot
        );
        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(state.last_sequence, 2);
        assert_eq!(recovered.get("safe"), Ok(Some("old-log".to_string())));
    }

    #[tokio::test]
    async fn snapshot_is_installed_before_a_failed_binlog_rotation() {
        let directory = TestPersistenceDirectory::new();
        append_test_binlog_record(&directory.paths, 1, &["SET", "safe", "old"]);
        let (log_tx, mut receiver) = mpsc::channel(8);
        let worker = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    LogMessage::Barrier { completion } | LogMessage::Flush { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                    LogMessage::Truncate { completion } => {
                        let _ = completion
                            .send(Err(StorageFailure::rejected("injected rotation failure")));
                    }
                    LogMessage::Append { completion, .. } => {
                        let _ = completion.send(Ok(()));
                    }
                    LogMessage::SyncData { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                }
            }
        });
        let persistence = test_persistence(directory.paths.clone(), log_tx, 7);
        let store = Arc::new(ShardedStore::new());
        store.set("safe".to_string(), "new".to_string());

        assert!(compact_store(&store, &persistence).await.is_err());
        assert!(directory.paths.snapshot.exists());
        assert!(fs::metadata(&directory.paths.binlog).unwrap().len() > 0);
        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(state.snapshot_watermark, 7);
        assert_eq!(recovered.get("safe"), Ok(Some("new".to_string())));
    }

    #[tokio::test]
    async fn indeterminate_binlog_rotation_enters_fail_stop_after_snapshot_installation() {
        let directory = TestPersistenceDirectory::new();
        append_test_binlog_record(&directory.paths, 1, &["SET", "safe", "old"]);
        let (log_tx, mut receiver) = mpsc::channel(8);
        let worker = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    LogMessage::Truncate { completion } => {
                        let _ = completion.send(Err(StorageFailure::indeterminate(
                            "injected ambiguous rotation failure",
                        )));
                    }
                    LogMessage::Append { completion, .. }
                    | LogMessage::Barrier { completion }
                    | LogMessage::Flush { completion }
                    | LogMessage::SyncData { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                }
            }
        });
        let persistence = test_persistence(directory.paths.clone(), log_tx, 7);
        let store = Arc::new(ShardedStore::new());
        store.set("safe".to_string(), "new".to_string());

        let error = compact_store(&store, &persistence).await.unwrap_err();

        assert!(error.is_indeterminate());
        assert!(persistence.is_fail_stopped());
        assert!(directory.paths.snapshot.exists());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                persistence.visibility_gate.read()
            )
            .await
            .is_err()
        );
        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(state.snapshot_watermark, 7);
        assert_eq!(recovered.get("safe"), Ok(Some("new".to_string())));
    }

    #[tokio::test]
    async fn repeated_compaction_replaces_snapshot_and_recovers_latest_state() {
        let directory = TestPersistenceDirectory::new();
        let store = Arc::new(ShardedStore::new());
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;

        apply_test_command(&store, &persistence, &["SET", "key", "first"]).await;
        compact_store(&store, &persistence).await.unwrap();
        apply_test_command(&store, &persistence, &["SET", "key", "second"]).await;
        compact_store(&store, &persistence).await.unwrap();

        assert!(directory.paths.snapshot.exists());
        assert!(directory.paths.snapshot_backup.exists());
        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(state.snapshot_watermark, 2);
        assert_eq!(recovered.get("key"), Ok(Some("second".to_string())));
    }

    #[test]
    fn recovery_uses_previous_snapshot_during_interrupted_installation() {
        let directory = TestPersistenceDirectory::new();
        let store = ShardedStore::new();
        store.set("safe".to_string(), "value".to_string());
        write_snapshot_file(store.raw_entries(), 3, &directory.paths).unwrap();
        append_test_binlog_record(&directory.paths, 1, &["SET", "safe", "old"]);
        fs::rename(&directory.paths.snapshot, &directory.paths.snapshot_backup).unwrap();

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(state.snapshot_watermark, 3);
        assert_eq!(recovered.get("safe"), Ok(Some("value".to_string())));
    }

    #[test]
    fn oversized_snapshot_metadata_is_rejected() {
        let directory = TestPersistenceDirectory::new();
        let file = File::create(&directory.paths.snapshot).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder
            .write_all(&vec![b'x'; MAX_SNAPSHOT_METADATA_SIZE + 1])
            .unwrap();
        encoder.finish().unwrap().sync_all().unwrap();

        let error = inspect_snapshot(&directory.paths.snapshot).unwrap_err();
        assert!(error.to_string().contains("Snapshot line exceeds"));
    }

    #[test]
    fn snapshot_collection_count_is_bounded_by_record_size() {
        let mut record = Vec::new();
        write_u32_be(&mut record, 1);
        record.push(b'k');
        write_u64_be(&mut record, 0);
        record.push(4);
        write_u32_be(&mut record, u32::MAX);

        let error = decode_snapshot_entry(&record).unwrap_err();
        assert!(error.to_string().contains("exceeds the record bounds"));
    }

    #[test]
    fn versioned_snapshot_preserves_binary_and_native_value_types() {
        let directory = TestPersistenceDirectory::new();
        let store = ShardedStore::new();
        let binary_key = Bytes::from_static(b"key\t\xff\n");
        let binary_value = Bytes::from_static(b"value\0|=\xff\n");
        store.set_value(
            binary_key.clone(),
            OnyxValue::Blob(binary_value.clone()),
            Some(u64::MAX),
        );
        store.set_value(
            Bytes::from_static(b"list"),
            OnyxValue::List(vec![
                Bytes::from_static(b"left|right"),
                Bytes::from_static(b"line\n\xff"),
            ]),
            None,
        );
        store.set_value(Bytes::from_static(b"float"), OnyxValue::Float(-0.0), None);
        store.set_value(
            Bytes::from_static(b"vector"),
            OnyxValue::Vector(vec![1.25, -3.5, f32::INFINITY]),
            None,
        );
        let mut hash = std::collections::HashMap::new();
        hash.insert(
            Bytes::from_static(b"field=|"),
            Bytes::from_static(b"value\n\xff"),
        );
        store.set_value(
            Bytes::from_static(b"hash"),
            OnyxValue::Hash(hash.clone()),
            None,
        );
        let set = [
            Bytes::from_static(b"member|one"),
            Bytes::from_static(b"\xff"),
        ]
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
        store.set_value(
            Bytes::from_static(b"set"),
            OnyxValue::Set(set.clone()),
            None,
        );

        write_snapshot_file(store.raw_entries(), 4, &directory.paths).unwrap();
        let recovered = ShardedStore::new();
        load_data_from_paths(&recovered, &directory.paths).unwrap();

        let binary_entry = recovered.get_entry(&binary_key).unwrap();
        assert_eq!(binary_entry.expires_at, Some(u64::MAX));
        assert!(matches!(binary_entry.value, OnyxValue::Blob(value) if value == binary_value));
        let list_entry = recovered.get_entry(&Bytes::from_static(b"list")).unwrap();
        assert!(matches!(
            list_entry.value,
            OnyxValue::List(values)
                if values == vec![Bytes::from_static(b"left|right"), Bytes::from_static(b"line\n\xff")]
        ));
        let float_entry = recovered.get_entry(&Bytes::from_static(b"float")).unwrap();
        assert!(matches!(
            float_entry.value,
            OnyxValue::Float(value) if value.to_bits() == (-0.0f64).to_bits()
        ));
        let vector_entry = recovered.get_entry(&Bytes::from_static(b"vector")).unwrap();
        assert!(matches!(
            vector_entry.value,
            OnyxValue::Vector(values) if values == vec![1.25, -3.5, f32::INFINITY]
        ));
        let hash_entry = recovered.get_entry(&Bytes::from_static(b"hash")).unwrap();
        assert!(matches!(hash_entry.value, OnyxValue::Hash(values) if values == hash));
        let set_entry = recovered.get_entry(&Bytes::from_static(b"set")).unwrap();
        assert!(matches!(set_entry.value, OnyxValue::Set(values) if values == set));
    }

    #[tokio::test]
    async fn full_sync_wire_preserves_binary_keys_ttls_and_all_native_types() {
        let mut hash = std::collections::HashMap::new();
        hash.insert(
            Bytes::from_static(b"field\0\xff"),
            Bytes::from_static(b"hash-value\n\x80"),
        );
        let set = [
            Bytes::from_static(b"member\0one"),
            Bytes::from_static(b"\xffmember"),
        ]
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
        let mut cases = vec![
            (
                Bytes::from_static(b"blob-key\0\xff"),
                DataEntry {
                    value: OnyxValue::Blob(Bytes::from_static(b"blob-value\r\n\0\xff")),
                    expires_at: Some(u64::MAX),
                    created_at: 1,
                    last_accessed: 2,
                },
            ),
            (
                Bytes::from_static(b"integer"),
                DataEntry {
                    value: OnyxValue::Int(i64::MIN),
                    expires_at: None,
                    created_at: 1,
                    last_accessed: 2,
                },
            ),
            (
                Bytes::from_static(b"float"),
                DataEntry {
                    value: OnyxValue::Float(-0.0),
                    expires_at: None,
                    created_at: 1,
                    last_accessed: 2,
                },
            ),
            (
                Bytes::from_static(b"list"),
                DataEntry {
                    value: OnyxValue::List(vec![
                        Bytes::from_static(b"left\0"),
                        Bytes::from_static(b"right\xff"),
                    ]),
                    expires_at: None,
                    created_at: 1,
                    last_accessed: 2,
                },
            ),
            (
                Bytes::from_static(b"hash"),
                DataEntry {
                    value: OnyxValue::Hash(hash),
                    expires_at: None,
                    created_at: 1,
                    last_accessed: 2,
                },
            ),
            (
                Bytes::from_static(b"set"),
                DataEntry {
                    value: OnyxValue::Set(set),
                    expires_at: None,
                    created_at: 1,
                    last_accessed: 2,
                },
            ),
            (
                Bytes::from_static(b"json"),
                DataEntry {
                    value: OnyxValue::Json(serde_json::json!({
                        "nested": [1, true, null, "binary-safe framing"]
                    })),
                    expires_at: None,
                    created_at: 1,
                    last_accessed: 2,
                },
            ),
            (
                Bytes::from_static(b"vector"),
                DataEntry {
                    value: OnyxValue::Vector(vec![1.25, -3.5, f32::INFINITY]),
                    expires_at: None,
                    created_at: 1,
                    last_accessed: 2,
                },
            ),
        ];
        cases.push((
            Bytes::from_static(b"multi-chunk"),
            DataEntry {
                value: OnyxValue::Blob(Bytes::from(vec![0xa5; REPLICATION_CHUNK_SIZE + 17])),
                expires_at: None,
                created_at: 1,
                last_accessed: 2,
            },
        ));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        for (expected_key, expected_entry) in cases {
            let record = encode_full_sync_entry(&expected_key, &expected_entry).unwrap();
            let sender = tokio::spawn(async move {
                let mut stream = TcpStream::connect(address).await.unwrap();
                write_chunked_replication_record(&mut stream, &record)
                    .await
                    .unwrap();
            });
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, _) = stream.into_split();
            let mut reader = TokioBufReader::new(reader);
            let mut scratch = Vec::new();
            let (actual_key, actual_entry) = read_full_sync_entry(&mut reader, &mut scratch)
                .await
                .unwrap();
            sender.await.unwrap();
            assert_eq!(actual_key, expected_key);
            assert_eq!(actual_entry.value, expected_entry.value);
            assert_eq!(actual_entry.expires_at, expected_entry.expires_at);
        }
    }

    #[tokio::test]
    async fn incremental_replication_chunks_large_committed_effects() {
        let batch = CommittedBatch::new(vec![CommittedEffect::Put {
            key: Bytes::from_static(b"large-effect"),
            entry: PersistentEntry {
                value: OnyxValue::Blob(Bytes::from(vec![0x5a; REPLICATION_CHUNK_SIZE + 17])),
                expires_at: Some(u64::MAX),
            },
        }])
        .unwrap();
        let record = encode_replication_effect(19, &batch).unwrap();
        assert!(record.payload.len() > REPLICATION_CHUNK_SIZE);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let sender = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await.unwrap();
            write_chunked_replication_record(&mut stream, &record)
                .await
                .unwrap();
        });
        let (stream, _) = listener.accept().await.unwrap();
        let (reader, _) = stream.into_split();
        let mut reader = TokioBufReader::new(reader);
        let mut scratch = Vec::new();
        let header = read_replication_command(&mut reader, &mut scratch)
            .await
            .unwrap()
            .unwrap();
        let decoded = read_replication_effect(&header, &mut reader, &mut scratch)
            .await
            .unwrap();
        sender.await.unwrap();

        assert_eq!(decoded, (19, batch));
    }

    #[tokio::test]
    async fn installed_full_sync_is_exact_durable_and_sequence_checked() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let store = Arc::new(ShardedStore::new());
        store.set_value(
            Bytes::from_static(b"stale"),
            OnyxValue::Blob(Bytes::from_static(b"must disappear")),
            None,
        );

        let staging = ShardedStore::new();
        staging.set_value(
            Bytes::from_static(b"binary\0\xff"),
            OnyxValue::Blob(Bytes::from_static(b"value\r\n\0\xff")),
            Some(u64::MAX),
        );
        staging.set_value(Bytes::from_static(b"counter"), OnyxValue::Int(9), None);
        staging.set_value(
            Bytes::from_static(b"document"),
            OnyxValue::Json(serde_json::json!({"visits": 2})),
            None,
        );

        install_full_sync(&store, &persistence, 71, 40, staging)
            .await
            .unwrap();
        assert!(store.get_entry(&Bytes::from_static(b"stale")).is_none());
        let binary = store
            .get_entry(&Bytes::from_static(b"binary\0\xff"))
            .unwrap();
        assert_eq!(binary.expires_at, Some(u64::MAX));
        assert!(matches!(
            binary.value,
            OnyxValue::Blob(value) if value == Bytes::from_static(b"value\r\n\0\xff")
        ));
        assert_eq!(persistence.sequence(), 40);
        assert!(persistence.replication_ready.load(Ordering::SeqCst));
        assert_eq!(
            load_durable_replica_state(&directory.paths, 40).unwrap(),
            Some(DurableReplicaState::Ready(ReplicaIdentity {
                replid: 71,
                baseline_sequence: 40,
            }))
        );
        assert_eq!(
            load_replica_identity(&directory.paths, 40).unwrap(),
            Some(ReplicaIdentity {
                replid: 71,
                baseline_sequence: 40,
            })
        );

        let increment = CommittedBatch::new(vec![CommittedEffect::Put {
            key: Bytes::from_static(b"counter"),
            entry: PersistentEntry {
                value: OnyxValue::Int(10),
                expires_at: None,
            },
        }])
        .unwrap();
        persist_and_apply_replica_effect(&store, &persistence, 41, &increment)
            .await
            .unwrap();
        let duplicate = persist_and_apply_replica_effect(&store, &persistence, 41, &increment)
            .await
            .unwrap_err();
        assert!(duplicate.to_string().contains("expected 42, received 41"));
        assert_eq!(persistence.sequence(), 41);
        assert!(matches!(
            store
                .get_entry(&Bytes::from_static(b"counter"))
                .unwrap()
                .value,
            OnyxValue::Int(10)
        ));

        persistence.binlog.flush().await.unwrap();
        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovery.snapshot_watermark, 40);
        assert_eq!(recovery.last_sequence, 41);
        assert!(recovered.get_entry(&Bytes::from_static(b"stale")).is_none());
        assert!(matches!(
            recovered
                .get_entry(&Bytes::from_static(b"counter"))
                .unwrap()
                .value,
            OnyxValue::Int(10)
        ));
        assert_eq!(
            load_replica_identity(&directory.paths, recovery.snapshot_watermark).unwrap(),
            Some(ReplicaIdentity {
                replid: 71,
                baseline_sequence: 40,
            })
        );
    }

    #[tokio::test]
    async fn replica_reads_wait_for_the_atomic_full_sync_installation_boundary() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let store = Arc::new(ShardedStore::new());
        store.set_value(
            Bytes::from_static(b"first"),
            OnyxValue::Blob(Bytes::from_static(b"old-first")),
            None,
        );
        store.set_value(
            Bytes::from_static(b"second"),
            OnyxValue::Blob(Bytes::from_static(b"old-second")),
            None,
        );

        let installation_guard = persistence.visibility_gate.write().await;
        let read_store = Arc::clone(&store);
        let read_persistence = Arc::clone(&persistence);
        let (started_tx, started_rx) = oneshot::channel();
        let mut read_task = tokio::spawn(async move {
            let _ = started_tx.send(());
            execute_ordered_command(
                &read_store,
                &read_persistence,
                &[
                    "MGET".to_string(),
                    "first".to_string(),
                    "second".to_string(),
                ],
            )
            .await
        });
        started_rx.await.unwrap();
        tokio::task::yield_now().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut read_task)
                .await
                .is_err(),
            "replica read crossed an in-progress baseline installation"
        );

        let replacement = ShardedStore::new();
        replacement.set_value(
            Bytes::from_static(b"first"),
            OnyxValue::Blob(Bytes::from_static(b"new-first")),
            None,
        );
        replacement.set_value(
            Bytes::from_static(b"second"),
            OnyxValue::Blob(Bytes::from_static(b"new-second")),
            None,
        );
        store.replace_all(replacement.raw_entries());
        drop(installation_guard);

        let outcome = read_task.await.unwrap();
        assert_eq!(outcome.mutation, MutationState::NotRequested);
        let RESPValue::Array(values) = outcome.response else {
            panic!("expected an MGET array response");
        };
        assert_eq!(values.len(), 2);
        assert!(matches!(
            &values[0],
            RESPValue::BulkString(Some(value)) if value == "new-first"
        ));
        assert!(matches!(
            &values[1],
            RESPValue::BulkString(Some(value)) if value == "new-second"
        ));

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn promotion_flushes_replica_state_and_clears_upstream_identity() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let store = Arc::new(ShardedStore::new());
        let staging = ShardedStore::new();
        staging.set_value(Bytes::from_static(b"counter"), OnyxValue::Int(3), None);
        install_full_sync(&store, &persistence, 81, 12, staging)
            .await
            .unwrap();
        let effect = CommittedBatch::new(vec![CommittedEffect::Put {
            key: Bytes::from_static(b"counter"),
            entry: PersistentEntry {
                value: OnyxValue::Int(4),
                expires_at: None,
            },
        }])
        .unwrap();
        persist_and_apply_replica_effect(&store, &persistence, 13, &effect)
            .await
            .unwrap();

        prepare_replica_promotion(&persistence).await.unwrap();
        assert!(persistence.promote_to_master.load(Ordering::SeqCst));
        assert!(!persistence.replication_ready.load(Ordering::SeqCst));
        assert_eq!(
            load_durable_replica_state(&directory.paths, 12).unwrap(),
            Some(DurableReplicaState::Detached)
        );
        assert_eq!(load_replica_identity(&directory.paths, 12).unwrap(), None);

        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovery.last_sequence, 13);
        assert!(matches!(
            recovered
                .get_entry(&Bytes::from_static(b"counter"))
                .unwrap()
                .value,
            OnyxValue::Int(4)
        ));
    }

    #[tokio::test]
    async fn manual_promotion_cancels_a_blocked_upstream_read() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 7).await;
        persistence.upstream_replid.store(123, Ordering::SeqCst);
        persistence.replication_ready.store(true, Ordering::SeqCst);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_addr = listener.local_addr().unwrap().to_string();
        let (streaming_tx, streaming_rx) = oneshot::channel();
        let master = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut reader = TokioBufReader::new(reader);
            let mut scratch = Vec::new();
            let request = read_replication_command(&mut reader, &mut scratch)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(request, vec!["SYNC3", "123", "7", "HEARTBEAT"]);
            writer
                .write_all(b"+CONTINUE3 123 7\r\n+SYNCDONE3 123 7\r\n")
                .await
                .unwrap();
            streaming_tx.send(()).unwrap();
            let mut byte = [0u8; 1];
            while reader.read(&mut byte).await.unwrap_or(0) != 0 {}
        });

        let replica_store = Arc::new(ShardedStore::new());
        let mut replica = tokio::spawn(run_replica(
            replica_store,
            Arc::clone(&persistence),
            ReplicaRuntimeConfig {
                master_addr,
                credentials: None,
                auto_failover: false,
                failover_timeout: Duration::from_secs(30),
                liveness_timeout: Duration::from_secs(30),
                initial_replid: 123,
                initial_offset: 7,
            },
        ));
        streaming_rx.await.unwrap();
        prepare_replica_promotion(&persistence).await.unwrap();

        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut replica)
                .await
                .is_ok(),
            "promotion left the replica task blocked on the former upstream"
        );
        tokio::time::timeout(Duration::from_millis(200), master)
            .await
            .expect("the former upstream connection remained open")
            .unwrap();

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn upstream_authentication_preserves_boundaries_and_precedes_sync() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 7).await;
        persistence.upstream_replid.store(123, Ordering::SeqCst);
        persistence.replication_ready.store(true, Ordering::SeqCst);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_addr = listener.local_addr().unwrap().to_string();
        let (synchronized_tx, synchronized_rx) = oneshot::channel();
        let master = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut reader = TokioBufReader::new(reader);
            let mut scratch = Vec::new();
            let auth = read_replication_command(&mut reader, &mut scratch)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                auth,
                vec!["AUTH", "replica user", "secret\r\nSYNC3 0 0 HEARTBEAT"]
            );
            writer.write_all(b"+OK\r\n").await.unwrap();
            let sync = read_replication_command(&mut reader, &mut scratch)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(sync, vec!["SYNC3", "123", "7", "HEARTBEAT"]);
            writer
                .write_all(b"+CONTINUE3 123 7\r\n+SYNCDONE3 123 7\r\n")
                .await
                .unwrap();
            synchronized_tx.send(()).unwrap();
            let mut byte = [0u8; 1];
            while reader.read(&mut byte).await.unwrap_or(0) != 0 {}
        });

        let replica = tokio::spawn(run_replica(
            Arc::new(ShardedStore::new()),
            Arc::clone(&persistence),
            ReplicaRuntimeConfig {
                master_addr,
                credentials: Some(UpstreamCredentials {
                    username: "replica user".to_string(),
                    password: "secret\r\nSYNC3 0 0 HEARTBEAT".to_string(),
                }),
                auto_failover: false,
                failover_timeout: Duration::from_secs(30),
                liveness_timeout: Duration::from_secs(30),
                initial_replid: 123,
                initial_offset: 7,
            },
        ));
        synchronized_rx.await.unwrap();
        persistence.replica_lifecycle.stop_and_wait().await.unwrap();
        replica.await.unwrap();
        master.await.unwrap();
        assert!(!persistence.promote_to_master.load(Ordering::SeqCst));

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn authentication_rejection_never_triggers_auto_failover() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 7).await;
        persistence.upstream_replid.store(123, Ordering::SeqCst);
        persistence.replication_ready.store(true, Ordering::SeqCst);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_addr = listener.local_addr().unwrap().to_string();
        let (closed_tx, closed_rx) = oneshot::channel();
        let master = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut reader = TokioBufReader::new(reader);
            let mut scratch = Vec::new();
            let auth = read_replication_command(&mut reader, &mut scratch)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(auth.first().map(String::as_str), Some("AUTH"));
            writer
                .write_all(b"-WRONGPASS invalid username or password\r\n")
                .await
                .unwrap();
            let mut byte = [0u8; 1];
            while reader.read(&mut byte).await.unwrap_or(0) != 0 {}
            closed_tx.send(()).unwrap();
        });

        let replica = tokio::spawn(run_replica(
            Arc::new(ShardedStore::new()),
            Arc::clone(&persistence),
            ReplicaRuntimeConfig {
                master_addr,
                credentials: Some(UpstreamCredentials {
                    username: "replica".to_string(),
                    password: "wrong".to_string(),
                }),
                auto_failover: true,
                failover_timeout: Duration::ZERO,
                liveness_timeout: Duration::from_secs(1),
                initial_replid: 123,
                initial_offset: 7,
            },
        ));
        closed_rx.await.unwrap();
        assert!(!persistence.promote_to_master.load(Ordering::SeqCst));
        persistence.replica_lifecycle.stop_and_wait().await.unwrap();
        replica.await.unwrap();
        master.await.unwrap();

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn silent_connected_master_is_failed_over_after_the_liveness_deadline() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 7).await;
        persistence.upstream_replid.store(123, Ordering::SeqCst);
        persistence.replication_ready.store(true, Ordering::SeqCst);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_addr = listener.local_addr().unwrap().to_string();
        let (streaming_tx, streaming_rx) = oneshot::channel();
        let master = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut reader = TokioBufReader::new(reader);
            let mut scratch = Vec::new();
            let sync = read_replication_command(&mut reader, &mut scratch)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(sync, vec!["SYNC3", "123", "7", "HEARTBEAT"]);
            writer
                .write_all(b"+CONTINUE3 123 7\r\n+SYNCDONE3 123 7\r\n")
                .await
                .unwrap();
            streaming_tx.send(()).unwrap();
            let mut byte = [0u8; 1];
            while reader.read(&mut byte).await.unwrap_or(0) != 0 {}
        });

        let replica = tokio::spawn(run_replica(
            Arc::new(ShardedStore::new()),
            Arc::clone(&persistence),
            ReplicaRuntimeConfig {
                master_addr,
                credentials: None,
                auto_failover: true,
                failover_timeout: Duration::ZERO,
                liveness_timeout: Duration::from_millis(50),
                initial_replid: 123,
                initial_offset: 7,
            },
        ));
        streaming_rx.await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), replica)
            .await
            .expect("silent upstream did not trigger deterministic failover")
            .unwrap();
        master.await.unwrap();
        assert!(persistence.promote_to_master.load(Ordering::SeqCst));
        assert!(!persistence.replication_ready.load(Ordering::SeqCst));

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn valid_heartbeats_keep_an_idle_replication_stream_live() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 7).await;
        persistence.upstream_replid.store(123, Ordering::SeqCst);
        persistence.replication_ready.store(true, Ordering::SeqCst);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_addr = listener.local_addr().unwrap().to_string();
        let (heartbeats_tx, heartbeats_rx) = oneshot::channel();
        let master = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut reader = TokioBufReader::new(reader);
            let mut scratch = Vec::new();
            let sync = read_replication_command(&mut reader, &mut scratch)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(sync, vec!["SYNC3", "123", "7", "HEARTBEAT"]);
            writer
                .write_all(b"+CONTINUE3 123 7\r\n+SYNCDONE3 123 7\r\n")
                .await
                .unwrap();
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                writer.write_all(b"REPLCONF PING 7\r\n").await.unwrap();
            }
            heartbeats_tx.send(()).unwrap();
            let mut byte = [0u8; 1];
            while reader.read(&mut byte).await.unwrap_or(0) != 0 {}
        });

        let replica = tokio::spawn(run_replica(
            Arc::new(ShardedStore::new()),
            Arc::clone(&persistence),
            ReplicaRuntimeConfig {
                master_addr,
                credentials: None,
                auto_failover: true,
                failover_timeout: Duration::ZERO,
                liveness_timeout: Duration::from_millis(100),
                initial_replid: 123,
                initial_offset: 7,
            },
        ));
        heartbeats_rx.await.unwrap();
        assert!(!persistence.promote_to_master.load(Ordering::SeqCst));
        persistence.replica_lifecycle.stop_and_wait().await.unwrap();
        replica.await.unwrap();
        master.await.unwrap();

        drop(persistence);
        worker.await.unwrap();
    }

    #[test]
    fn replication_heartbeat_is_sequence_bound() {
        assert!(
            parse_upstream_heartbeat(
                &["REPLCONF".to_string(), "PING".to_string(), "9".to_string()],
                9,
            )
            .unwrap()
        );
        assert!(
            parse_upstream_heartbeat(
                &["REPLCONF".to_string(), "PING".to_string(), "8".to_string()],
                9,
            )
            .is_err()
        );
        assert!(!parse_upstream_heartbeat(&["EFFECT3".to_string()], 9).unwrap());
    }

    #[tokio::test]
    async fn failed_replica_lifecycle_cannot_claim_quiescence() {
        let lifecycle = ReplicaLifecycle::new(false);
        lifecycle.mark_failed();
        assert!(lifecycle.stop_and_wait().await.is_err());
    }

    #[tokio::test]
    async fn accepting_full_resync_invalidates_the_previous_promotable_identity() {
        let directory = TestPersistenceDirectory::new();
        write_replica_identity(
            &directory.paths,
            ReplicaIdentity {
                replid: 81,
                baseline_sequence: 12,
            },
        )
        .unwrap();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 12).await;
        persistence.upstream_replid.store(81, Ordering::SeqCst);
        persistence.replication_ready.store(true, Ordering::SeqCst);

        begin_full_sync_reception(&persistence).await.unwrap();

        assert!(!persistence.replication_ready.load(Ordering::SeqCst));
        assert_eq!(persistence.upstream_replid.load(Ordering::SeqCst), 0);
        assert_eq!(
            load_durable_replica_state(&directory.paths, 12).unwrap(),
            Some(DurableReplicaState::Installing)
        );
        assert!(prepare_replica_promotion(&persistence).await.is_err());

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn promotion_intent_prevents_a_new_full_sync_from_invalidating_ready_state() {
        let directory = TestPersistenceDirectory::new();
        write_replica_identity(
            &directory.paths,
            ReplicaIdentity {
                replid: 81,
                baseline_sequence: 12,
            },
        )
        .unwrap();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 12).await;
        persistence.upstream_replid.store(81, Ordering::SeqCst);
        persistence.replication_ready.store(true, Ordering::SeqCst);
        persistence.replica_lifecycle.request_stop();

        assert!(begin_full_sync_reception(&persistence).await.is_err());
        assert!(persistence.replication_ready.load(Ordering::SeqCst));
        assert_eq!(persistence.upstream_replid.load(Ordering::SeqCst), 81);
        assert_eq!(
            load_durable_replica_state(&directory.paths, 12).unwrap(),
            Some(DurableReplicaState::Ready(ReplicaIdentity {
                replid: 81,
                baseline_sequence: 12,
            }))
        );

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn replicated_effect_persistence_failure_is_not_applied_or_acknowledged() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(4);
        let worker = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    LogMessage::Append { completion, .. } => {
                        let _ = completion.send(Err(StorageFailure::rejected(
                            "injected replica append failure",
                        )));
                    }
                    LogMessage::Barrier { completion }
                    | LogMessage::Flush { completion }
                    | LogMessage::SyncData { completion }
                    | LogMessage::Truncate { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                }
            }
        });
        let persistence = test_persistence(directory.paths.clone(), log_tx, 5);
        persistence.upstream_replid.store(33, Ordering::SeqCst);
        persistence.replication_ready.store(true, Ordering::SeqCst);
        let store = Arc::new(ShardedStore::new());
        store.set_value(Bytes::from_static(b"counter"), OnyxValue::Int(5), None);
        let effect = CommittedBatch::new(vec![CommittedEffect::Put {
            key: Bytes::from_static(b"counter"),
            entry: PersistentEntry {
                value: OnyxValue::Int(6),
                expires_at: None,
            },
        }])
        .unwrap();

        assert!(
            persist_and_apply_replica_effect(&store, &persistence, 6, &effect)
                .await
                .is_err()
        );
        assert_eq!(persistence.sequence(), 5);
        assert!(!persistence.accepting_writes.load(Ordering::SeqCst));
        assert!(!persistence.replication_ready.load(Ordering::SeqCst));
        assert!(matches!(
            store
                .get_entry(&Bytes::from_static(b"counter"))
                .unwrap()
                .value,
            OnyxValue::Int(5)
        ));

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn failed_full_sync_install_keeps_old_live_state_and_blocks_promotion() {
        let directory = TestPersistenceDirectory::new();
        let old_disk_state = ShardedStore::new();
        old_disk_state.set_value(
            Bytes::from_static(b"old"),
            OnyxValue::Blob(Bytes::from_static(b"durable")),
            None,
        );
        write_snapshot_file(old_disk_state.raw_entries(), 3, &directory.paths).unwrap();
        write_replica_identity(
            &directory.paths,
            ReplicaIdentity {
                replid: 51,
                baseline_sequence: 3,
            },
        )
        .unwrap();
        append_test_binlog_record(&directory.paths, 4, &["SET", "post", "boundary"]);

        let mut failing_paths = directory.paths.clone();
        failing_paths.snapshot_temp = directory.root.join("missing").join("snapshot.tmp");
        let (persistence, worker) = start_test_persistence(failing_paths, 4).await;
        persistence.upstream_replid.store(51, Ordering::SeqCst);
        persistence.replication_ready.store(true, Ordering::SeqCst);
        let store = Arc::new(ShardedStore::new());
        store.set_value(
            Bytes::from_static(b"old"),
            OnyxValue::Blob(Bytes::from_static(b"live")),
            None,
        );
        let staging = ShardedStore::new();
        staging.set_value(
            Bytes::from_static(b"new"),
            OnyxValue::Blob(Bytes::from_static(b"uncommitted")),
            None,
        );

        let error = install_full_sync(&store, &persistence, 91, 8, staging)
            .await
            .unwrap_err();
        assert!(!error.to_string().is_empty());
        assert_eq!(store.get("old"), Ok(Some("live".to_string())));
        assert_eq!(store.get("new"), Ok(None));
        assert!(!persistence.replication_ready.load(Ordering::SeqCst));
        assert!(!persistence.promote_to_master.load(Ordering::SeqCst));
        assert!(prepare_replica_promotion(&persistence).await.is_err());
        assert_eq!(
            load_durable_replica_state(&directory.paths, 3).unwrap(),
            Some(DurableReplicaState::Installing)
        );
        assert_eq!(
            load_replica_identity(&directory.paths, 3).unwrap(),
            None,
            "the old baseline must not remain promotable after destructive installation begins"
        );

        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovery.last_sequence, 3);
        assert_eq!(recovered.get("old"), Ok(Some("durable".to_string())));
        assert!(
            load_replica_identity(&directory.paths, 3)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn master_startup_detaches_ready_history_and_rejects_incomplete_installation() {
        let ready_directory = TestPersistenceDirectory::new();
        write_replica_identity(
            &ready_directory.paths,
            ReplicaIdentity {
                replid: 101,
                baseline_sequence: 9,
            },
        )
        .unwrap();

        assert_eq!(
            prepare_replication_startup(&ready_directory.paths, 9, false).unwrap(),
            None
        );
        assert_eq!(
            load_durable_replica_state(&ready_directory.paths, 9).unwrap(),
            Some(DurableReplicaState::Detached)
        );

        let installing_directory = TestPersistenceDirectory::new();
        write_replica_installing(&installing_directory.paths).unwrap();
        let error = prepare_replication_startup(&installing_directory.paths, 0, false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("baseline installation is incomplete"));
        assert_eq!(
            prepare_replication_startup(&installing_directory.paths, 0, true).unwrap(),
            None
        );
        assert_eq!(
            load_durable_replica_state(&installing_directory.paths, 0).unwrap(),
            Some(DurableReplicaState::Installing)
        );
    }

    #[test]
    fn replication_v3_markers_are_strict_and_sequence_bound() {
        assert_eq!(
            parse_replica_sync_handshake(&[
                "+FULLRESYNC3".to_string(),
                "17".to_string(),
                "23".to_string(),
                "5".to_string(),
            ])
            .unwrap(),
            ReplicaSyncHandshake::Full {
                replid: 17,
                sequence: 23,
                entry_count: 5,
            }
        );
        assert!(
            parse_replica_sync_handshake(&[
                "+FULLRESYNC3".to_string(),
                "17".to_string(),
                "23".to_string(),
            ])
            .is_err()
        );
        assert!(
            parse_replica_sync_handshake(&[
                "+CONTINUE3".to_string(),
                "0".to_string(),
                "23".to_string(),
            ])
            .is_err()
        );
        assert!(
            parse_replica_sync_done(
                &["+SYNCDONE3".to_string(), "18".to_string(), "23".to_string(),],
                17,
            )
            .is_err()
        );
        assert!(decode_replication_effect("0", &[0]).is_err());
        assert!(decode_replication_effect("not-a-sequence", &[0]).is_err());
    }

    #[test]
    fn ambiguous_legacy_snapshot_and_binlog_are_rejected() {
        let directory = TestPersistenceDirectory::new();
        let entry = DataEntry {
            value: OnyxValue::Blob(Bytes::from("snapshot")),
            expires_at: None,
            created_at: 0,
            last_accessed: 0,
        };
        let file = File::create(&directory.paths.snapshot).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        writeln!(encoder, "{}", value_to_line("key", &entry)).unwrap();
        encoder.finish().unwrap().sync_all().unwrap();

        let args = vec![
            "APPEND".to_string(),
            "key".to_string(),
            "suffix".to_string(),
        ];
        let record = command_to_binary_record("APPEND", &args, None).unwrap();
        let mut binlog = File::create(&directory.paths.binlog).unwrap();
        binlog
            .write_all(&(record.len() as u32).to_be_bytes())
            .unwrap();
        binlog.write_all(&record).unwrap();
        binlog.sync_all().unwrap();

        let error = load_data_from_paths(&ShardedStore::new(), &directory.paths).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Unsupported unsafe legacy snapshot format")
        );
    }

    #[test]
    fn transaction_queue_rejects_projected_byte_overflow() {
        let mut queue = TransactionQueue::default();
        let error = queue
            .enqueue(vec!["x".repeat(MAX_TRANSACTION_BYTES)])
            .unwrap_err();

        assert_eq!(error, "ERR transaction queue byte limit exceeded");
        assert!(queue.failed);
        assert!(queue.commands.is_empty());
    }

    #[tokio::test]
    async fn transaction_persists_and_recovers_as_one_committed_batch() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let store = Arc::new(ShardedStore::new());
        let response = execute_transaction(
            &store,
            &persistence,
            vec![
                vec!["SET".to_string(), "first".to_string(), "value".to_string()],
                vec!["INCR".to_string(), "second".to_string()],
            ],
            false,
        )
        .await;

        assert!(matches!(response, RESPValue::Array(results) if results.len() == 2));
        assert_eq!(persistence.sequence(), 1);
        {
            let backlog = persistence.backlog.lock().unwrap();
            assert_eq!(backlog.len(), 1);
            assert_eq!(backlog.front().unwrap().0, 1);
            assert_eq!(backlog.front().unwrap().1.effects.len(), 2);
        }

        persistence.binlog.flush().await.unwrap();
        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovery.last_sequence, 1);
        assert_eq!(recovered.get("first"), Ok(Some("value".to_string())));
        assert_eq!(recovered.get("second"), Ok(Some("1".to_string())));
    }

    #[tokio::test]
    async fn coordinator_groups_queued_mutations_in_authoritative_sequence_order() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);
        let mut replication = persistence.replica_tx.subscribe();

        let first_store = Arc::clone(&store);
        let first_persistence = Arc::clone(&persistence);
        let first = tokio::spawn(async move {
            execute_ordered_command(
                &first_store,
                &first_persistence,
                &["SET".to_string(), "gate".to_string(), "open".to_string()],
            )
            .await
        });
        let (first_records, first_completion) = receive_append_group(&mut receiver).await;
        assert_eq!(first_records.len(), 1);
        assert_eq!(first_records[0].0, 1);

        let mut queued = Vec::new();
        for index in 0..3 {
            let command_store = Arc::clone(&store);
            let command_persistence = Arc::clone(&persistence);
            queued.push(tokio::spawn(async move {
                execute_ordered_command(
                    &command_store,
                    &command_persistence,
                    &[
                        "SET".to_string(),
                        format!("key-{index}"),
                        format!("value-{index}"),
                    ],
                )
                .await
            }));
        }
        wait_for_coordinator_queue(&persistence, 3).await;
        first_completion.send(Ok(())).unwrap();
        assert_eq!(first.await.unwrap().mutation, MutationState::Committed);

        let (grouped_records, grouped_completion) = receive_append_group(&mut receiver).await;
        assert_eq!(
            grouped_records
                .iter()
                .map(|(sequence, _)| *sequence)
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        grouped_completion.send(Ok(())).unwrap();
        for command in queued {
            assert_eq!(command.await.unwrap().mutation, MutationState::Committed);
        }

        for expected in 1..=4 {
            let (sequence, _) = replication.recv().await.unwrap();
            assert_eq!(sequence, expected);
        }
        assert_eq!(persistence.sequence(), 4);
        assert_eq!(persistence.backlog.lock().unwrap().len(), 4);
        for index in 0..3 {
            assert_eq!(
                store.get(&format!("key-{index}")),
                Ok(Some(format!("value-{index}")))
            );
        }
        let metrics = persistence
            .master_commit
            .get()
            .expect("coordinator must be installed")
            .metrics_snapshot();
        assert_eq!(metrics.requests_total, 4);
        assert_eq!(metrics.queue_depth, 0);
        assert!(metrics.queue_depth_max >= 3);
        assert_eq!(metrics.groups_total, 2);
        assert_eq!(metrics.groups_completed_total, 2);
        assert_eq!(metrics.groups_rejected_total, 0);
        assert_eq!(metrics.groups_indeterminate_total, 0);
        assert_eq!(metrics.groups_interrupted_total, 0);
        assert_eq!(metrics.groups_in_progress, 0);
        assert_eq!(metrics.group_requests_total, 4);
        assert_eq!(metrics.group_requests_max, 3);
        assert_eq!(metrics.logical_batches_total, 4);
        assert!(metrics.group_duration_nanoseconds_total > 0);
        assert!(metrics.storage_duration_nanoseconds_total > 0);
    }

    #[tokio::test]
    async fn coordinator_persists_one_pipelined_request_as_distinct_logical_commits() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);

        let command_store = Arc::clone(&store);
        let command_persistence = Arc::clone(&persistence);
        let commands = tokio::spawn(async move {
            execute_ordered_commands(
                &command_store,
                &command_persistence,
                vec![
                    vec!["SET".to_string(), "value".to_string(), "1".to_string()],
                    vec!["INCR".to_string(), "value".to_string()],
                    vec!["APPEND".to_string(), "value".to_string(), "0".to_string()],
                ],
            )
            .await
        });

        let (records, completion) = receive_append_group(&mut receiver).await;
        assert_eq!(
            records
                .iter()
                .map(|(sequence, _)| *sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        completion.send(Ok(())).unwrap();

        let outcomes = commands.await.unwrap();
        assert_eq!(outcomes.len(), 3);
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.mutation == MutationState::Committed)
        );
        assert_eq!(store.get("value"), Ok(Some("20".to_string())));
        assert_eq!(persistence.sequence(), 3);
        assert_eq!(persistence.backlog.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn resp_handler_admits_buffered_write_pipeline_as_one_physical_append() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        client
            .write_all(
                b"*3\r\n$3\r\nSET\r\n$3\r\none\r\n$1\r\n1\r\n\
                  *3\r\n$3\r\nSET\r\n$3\r\ntwo\r\n$1\r\n2\r\n\
                  *3\r\n$3\r\nSET\r\n$5\r\nthree\r\n$1\r\n3\r\n",
            )
            .await
            .unwrap();
        let server_store = Arc::clone(&store);
        let server_persistence = Arc::clone(&persistence);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_client(stream, server_store, server_persistence).await;
        });

        let (records, completion) = receive_append_group(&mut receiver).await;
        assert_eq!(records.len(), 3);
        completion.send(Ok(())).unwrap();

        let mut responses = [0u8; 15];
        tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut responses))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&responses, b"+OK\r\n+OK\r\n+OK\r\n");
        assert_eq!(store.get("one"), Ok(Some("1".to_string())));
        assert_eq!(store.get("two"), Ok(Some("2".to_string())));
        assert_eq!(store.get("three"), Ok(Some("3".to_string())));
        assert_eq!(persistence.sequence(), 3);

        drop(client);
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn partial_following_resp_frame_does_not_delay_a_complete_write_response() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*")
            .await
            .unwrap();
        let server_store = Arc::clone(&store);
        let server_persistence = Arc::clone(&persistence);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_client(stream, server_store, server_persistence).await;
        });

        let (records, completion) = receive_append_group(&mut receiver).await;
        assert_eq!(records.len(), 1);
        completion.send(Ok(())).unwrap();

        let mut response = [0u8; 5];
        tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut response))
            .await
            .expect("the complete write response waited for the partial next frame")
            .unwrap();
        assert_eq!(&response, b"+OK\r\n");
        assert_eq!(store.get("key"), Ok(Some("value".to_string())));

        drop(client);
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn grouped_resp_effects_recover_with_ttl_collections_and_json_intact() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);

        let outcomes = execute_ordered_commands(
            &store,
            &persistence,
            vec![
                vec![
                    "SET".to_string(),
                    "expiring".to_string(),
                    "value".to_string(),
                ],
                vec![
                    "EXPIRE".to_string(),
                    "expiring".to_string(),
                    "120".to_string(),
                ],
                vec!["RPUSH".to_string(), "list".to_string(), "first".to_string()],
                vec![
                    "RPUSH".to_string(),
                    "list".to_string(),
                    "second".to_string(),
                ],
                vec![
                    "JSON.SET".to_string(),
                    "document".to_string(),
                    "$".to_string(),
                    "{\"number\":1}".to_string(),
                ],
                vec![
                    "JSON.NUMINCRBY".to_string(),
                    "document".to_string(),
                    "$.number".to_string(),
                    "2".to_string(),
                ],
            ],
        )
        .await;

        assert_eq!(outcomes.len(), 6);
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.mutation == MutationState::Committed)
        );
        assert_eq!(persistence.sequence(), 6);
        persistence.binlog.flush().await.unwrap();
        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovery.last_sequence, 6);
        assert_eq!(recovered.get("expiring"), Ok(Some("value".to_string())));
        assert!(recovered.ttl("expiring") > 0);
        assert_eq!(
            recovered.lrange("list", 0, -1),
            Ok(vec!["first".to_string(), "second".to_string()])
        );
        assert_eq!(
            recovered.json_get("document", "$.number"),
            Ok(Some("3".to_string()))
        );
    }

    #[tokio::test]
    async fn cancelling_a_queued_client_does_not_remove_its_commit_request() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);

        let gate_store = Arc::clone(&store);
        let gate_persistence = Arc::clone(&persistence);
        let gate = tokio::spawn(async move {
            execute_ordered_command(
                &gate_store,
                &gate_persistence,
                &["SET".to_string(), "gate".to_string(), "open".to_string()],
            )
            .await
        });
        let (_, gate_completion) = receive_append_group(&mut receiver).await;

        let queued_store = Arc::clone(&store);
        let queued_persistence = Arc::clone(&persistence);
        let queued = tokio::spawn(async move {
            execute_ordered_command(
                &queued_store,
                &queued_persistence,
                &[
                    "SET".to_string(),
                    "queued".to_string(),
                    "accepted".to_string(),
                ],
            )
            .await
        });
        wait_for_coordinator_queue(&persistence, 1).await;
        queued.abort();
        assert!(queued.await.unwrap_err().is_cancelled());

        gate_completion.send(Ok(())).unwrap();
        assert_eq!(gate.await.unwrap().mutation, MutationState::Committed);
        let (_, queued_completion) = receive_append_group(&mut receiver).await;
        queued_completion.send(Ok(())).unwrap();
        wait_for_commit_boundary(&persistence).await;

        assert_eq!(store.get("queued"), Ok(Some("accepted".to_string())));
        assert_eq!(persistence.sequence(), 2);
        assert_eq!(persistence.backlog.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn dropped_group_storage_outcome_enters_fail_stop_without_rollback() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);

        let command_store = Arc::clone(&store);
        let command_persistence = Arc::clone(&persistence);
        let command = tokio::spawn(async move {
            execute_ordered_commands(
                &command_store,
                &command_persistence,
                vec![
                    vec!["SET".to_string(), "first".to_string(), "one".to_string()],
                    vec!["SET".to_string(), "second".to_string(), "two".to_string()],
                ],
            )
            .await
        });
        let (records, completion) = receive_append_group(&mut receiver).await;
        assert_eq!(records.len(), 2);
        drop(completion);

        let outcomes = command.await.unwrap();
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes.iter().all(|outcome| {
            matches!(&outcome.response, RESPValue::Error(message) if message.starts_with("MISCONF"))
        }));
        assert!(persistence.is_fail_stopped());
        assert!(!persistence.accepting_writes.load(Ordering::SeqCst));
        assert_eq!(store.get("first"), Ok(Some("one".to_string())));
        assert_eq!(store.get("second"), Ok(Some("two".to_string())));
        assert_eq!(persistence.sequence(), 0);
        assert!(persistence.backlog.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn coordinator_panic_is_supervised_and_retains_the_fail_stop_boundary() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, _receiver) = mpsc::channel(1);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);
        let coordinator = persistence.master_commit.get().unwrap().clone();

        let error = coordinator.panic_worker_for_test().await;
        assert!(error.is_indeterminate());
        let reason = tokio::time::timeout(Duration::from_secs(1), persistence.wait_for_fail_stop())
            .await
            .expect("the coordinator panic was not fail-stopped");

        assert!(reason.contains("Master commit coordinator group"));
        assert!(persistence.is_fail_stopped());
        assert!(!persistence.accepting_writes.load(Ordering::SeqCst));
        assert!(persistence.visibility_gate.try_read().is_err());

        let outcome = execute_ordered_command(
            &store,
            &persistence,
            &[
                "SET".to_string(),
                "after-panic".to_string(),
                "value".to_string(),
            ],
        )
        .await;
        assert!(matches!(
            outcome.response,
            RESPValue::Error(message) if message.starts_with("MISCONF")
        ));
        assert_eq!(store.get("after-panic"), Ok(None));
    }

    #[tokio::test]
    async fn binlog_worker_panic_makes_the_group_indeterminate_and_fail_stops() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(1);
        let storage_worker = tokio::spawn(async move {
            let message = receiver.recv().await.expect("append request missing");
            assert!(matches!(message, LogMessage::Append { .. }));
            panic!("injected binlog worker panic");
        });
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);

        let outcome = execute_ordered_command(
            &store,
            &persistence,
            &[
                "SET".to_string(),
                "key".to_string(),
                "tentative".to_string(),
            ],
        )
        .await;
        let worker_error = storage_worker.await.unwrap_err();

        assert!(worker_error.is_panic());
        assert!(matches!(
            outcome.response,
            RESPValue::Error(message) if message.starts_with("MISCONF")
        ));
        assert!(persistence.is_fail_stopped());
        assert!(!persistence.accepting_writes.load(Ordering::SeqCst));
        assert!(persistence.visibility_gate.try_read().is_err());
        assert_eq!(store.get("key"), Ok(Some("tentative".to_string())));
        assert_eq!(persistence.sequence(), 0);
        assert!(persistence.backlog.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn coordinator_client_cancellation_does_not_cancel_an_owned_commit() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);

        let command_store = Arc::clone(&store);
        let command_persistence = Arc::clone(&persistence);
        let command = tokio::spawn(async move {
            execute_ordered_command(
                &command_store,
                &command_persistence,
                &[
                    "SET".to_string(),
                    "cancelled-client".to_string(),
                    "accepted".to_string(),
                ],
            )
            .await
        });
        let (records, completion) = receive_append_group(&mut receiver).await;
        assert_eq!(records.len(), 1);

        command.abort();
        assert!(command.await.unwrap_err().is_cancelled());
        completion.send(Ok(())).unwrap();
        wait_for_commit_boundary(&persistence).await;

        assert_eq!(
            store.get("cancelled-client"),
            Ok(Some("accepted".to_string()))
        );
        assert_eq!(persistence.sequence(), 1);
        assert_eq!(persistence.backlog.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn coordinator_rejection_rolls_back_dependent_mutations_in_reverse_order() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);

        let gate_store = Arc::clone(&store);
        let gate_persistence = Arc::clone(&persistence);
        let gate = tokio::spawn(async move {
            execute_ordered_command(
                &gate_store,
                &gate_persistence,
                &["SET".to_string(), "gate".to_string(), "open".to_string()],
            )
            .await
        });
        let (_, gate_completion) = receive_append_group(&mut receiver).await;

        let mut increments = Vec::new();
        for _ in 0..2 {
            let increment_store = Arc::clone(&store);
            let increment_persistence = Arc::clone(&persistence);
            increments.push(tokio::spawn(async move {
                execute_ordered_command(
                    &increment_store,
                    &increment_persistence,
                    &["INCR".to_string(), "counter".to_string()],
                )
                .await
            }));
        }
        wait_for_coordinator_queue(&persistence, 2).await;
        gate_completion.send(Ok(())).unwrap();
        assert_eq!(gate.await.unwrap().mutation, MutationState::Committed);

        let (records, completion) = receive_append_group(&mut receiver).await;
        assert_eq!(records.len(), 2);
        completion
            .send(Err(StorageFailure::rejected(
                "injected grouped append rejection",
            )))
            .unwrap();
        for increment in increments {
            let outcome = increment.await.unwrap();
            assert_eq!(outcome.mutation, MutationState::NoChange);
            assert!(
                matches!(outcome.response, RESPValue::Error(message) if message.starts_with("MISCONF"))
            );
        }

        assert_eq!(store.get("counter"), Ok(None));
        assert_eq!(persistence.sequence(), 1);
        assert_eq!(persistence.backlog.lock().unwrap().len(), 1);
        assert!(!persistence.accepting_writes.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn coordinator_group_rejection_restores_eviction_victims() {
        let candidate = ShardedStore::new();
        candidate.set("created".to_string(), "x".repeat(90));
        let limit = candidate.used_memory_bytes();
        let store = Arc::new(ShardedStore::with_maxmemory(
            limit,
            EvictionPolicy::AllKeysLru,
        ));
        store.set("victim".to_string(), "original".to_string());
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        enable_master_commit_coordinator(&store, &persistence);

        let mutation_store = Arc::clone(&store);
        let mutation_persistence = Arc::clone(&persistence);
        let mutation = tokio::spawn(async move {
            execute_ordered_commands(
                &mutation_store,
                &mutation_persistence,
                vec![vec![
                    "SET".to_string(),
                    "created".to_string(),
                    "x".repeat(90),
                ]],
            )
            .await
        });
        let (records, completion) = receive_append_group(&mut receiver).await;
        assert_eq!(records.len(), 1);
        completion
            .send(Err(StorageFailure::rejected(
                "injected grouped eviction rejection",
            )))
            .unwrap();

        let outcomes = mutation.await.unwrap();
        assert!(matches!(
            &outcomes[0].response,
            RESPValue::Error(message) if message.starts_with("MISCONF")
        ));
        assert_eq!(store.get("victim"), Ok(Some("original".to_string())));
        assert_eq!(store.get("created"), Ok(None));
        assert!(store.used_memory_bytes() <= limit);
        assert_eq!(persistence.sequence(), 0);
    }

    #[tokio::test]
    async fn coordinator_groups_resp_transactions_and_obp_mutations() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);

        let gate_store = Arc::clone(&store);
        let gate_persistence = Arc::clone(&persistence);
        let gate = tokio::spawn(async move {
            execute_ordered_command(
                &gate_store,
                &gate_persistence,
                &["SET".to_string(), "gate".to_string(), "open".to_string()],
            )
            .await
        });
        let (_, gate_completion) = receive_append_group(&mut receiver).await;

        let transaction_store = Arc::clone(&store);
        let transaction_persistence = Arc::clone(&persistence);
        let transaction = tokio::spawn(async move {
            execute_transaction(
                &transaction_store,
                &transaction_persistence,
                vec![
                    vec![
                        "SET".to_string(),
                        "transaction".to_string(),
                        "one".to_string(),
                    ],
                    vec!["INCR".to_string(), "number".to_string()],
                ],
                false,
            )
            .await
        });
        let obp_store = Arc::clone(&store);
        let obp_persistence = Arc::clone(&persistence);
        let obp = tokio::spawn(async move {
            let mut authenticated = true;
            execute_obp_command(
                &obp_store,
                &obp_persistence,
                OBPFrame {
                    cmd: 0x02,
                    flags: 0,
                    correlation_id: 11,
                    args: vec![Bytes::from_static(b"binary"), Bytes::from_static(b"two")],
                    payload: None,
                },
                &mut authenticated,
                false,
            )
            .await
        });
        wait_for_coordinator_queue(&persistence, 2).await;
        gate_completion.send(Ok(())).unwrap();
        assert_eq!(gate.await.unwrap().mutation, MutationState::Committed);

        let (records, completion) = receive_append_group(&mut receiver).await;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, 2);
        assert_eq!(records[1].0, 3);
        completion.send(Ok(())).unwrap();

        assert!(
            matches!(transaction.await.unwrap(), RESPValue::Array(values) if values.len() == 2)
        );
        assert!(obp.await.unwrap().payload.is_some());
        assert_eq!(store.get("transaction"), Ok(Some("one".to_string())));
        assert_eq!(store.get("number"), Ok(Some("1".to_string())));
        assert_eq!(store.get("binary"), Ok(Some("two".to_string())));
        assert_eq!(persistence.sequence(), 3);
    }

    #[tokio::test]
    async fn coordinator_splits_sustained_load_at_the_logical_group_bound() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        enable_master_commit_coordinator(&store, &persistence);

        let gate_store = Arc::clone(&store);
        let gate_persistence = Arc::clone(&persistence);
        let gate = tokio::spawn(async move {
            execute_ordered_command(
                &gate_store,
                &gate_persistence,
                &["SET".to_string(), "gate".to_string(), "open".to_string()],
            )
            .await
        });
        let (_, gate_completion) = receive_append_group(&mut receiver).await;

        let mut groups = Vec::new();
        for group in 0..5 {
            let group_store = Arc::clone(&store);
            let group_persistence = Arc::clone(&persistence);
            groups.push(tokio::spawn(async move {
                let commands = (0..MAX_PIPELINED_COMMIT_COMMANDS)
                    .map(|command| {
                        vec![
                            "SET".to_string(),
                            format!("bounded-{group}-{command}"),
                            "value".to_string(),
                        ]
                    })
                    .collect();
                execute_ordered_commands(&group_store, &group_persistence, commands).await
            }));
        }
        wait_for_coordinator_queue(&persistence, 5).await;
        gate_completion.send(Ok(())).unwrap();
        assert_eq!(gate.await.unwrap().mutation, MutationState::Committed);

        let (first_records, first_completion) = receive_append_group(&mut receiver).await;
        assert_eq!(first_records.len(), 256);
        assert_eq!(first_records.first().unwrap().0, 2);
        assert_eq!(first_records.last().unwrap().0, 257);
        first_completion.send(Ok(())).unwrap();

        let (second_records, second_completion) = receive_append_group(&mut receiver).await;
        assert_eq!(second_records.len(), 64);
        assert_eq!(second_records.first().unwrap().0, 258);
        assert_eq!(second_records.last().unwrap().0, 321);
        second_completion.send(Ok(())).unwrap();

        for group in groups {
            let outcomes = group.await.unwrap();
            assert_eq!(outcomes.len(), MAX_PIPELINED_COMMIT_COMMANDS);
            assert!(
                outcomes
                    .iter()
                    .all(|outcome| outcome.mutation == MutationState::Committed)
            );
        }
        assert_eq!(persistence.sequence(), 321);
        assert_eq!(store.stats().total_keys, 321);
    }

    #[tokio::test]
    async fn wrong_type_transaction_error_preserves_state_and_emits_no_commit() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let store = Arc::new(ShardedStore::new());
        assert_eq!(store.lpush("list", "original".to_string()), Ok(1));

        let response = execute_transaction(
            &store,
            &persistence,
            vec![vec![
                "APPEND".to_string(),
                "list".to_string(),
                "replacement".to_string(),
            ]],
            false,
        )
        .await;

        assert!(matches!(
            response,
            RESPValue::Array(values)
                if matches!(&values[..], [RESPValue::Error(message)] if message.starts_with("WRONGTYPE"))
        ));
        assert_eq!(
            store.lrange("list", 0, -1),
            Ok(vec!["original".to_string()])
        );
        assert_eq!(persistence.sequence(), 0);
        assert!(persistence.backlog.lock().unwrap().is_empty());

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn expired_collection_mutation_and_empty_delete_recover_faithfully() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let store = Arc::new(ShardedStore::new());
        store.set_value(
            Bytes::from_static(b"list"),
            OnyxValue::List(vec![Bytes::from_static(b"stale")]),
            Some(now()),
        );

        apply_test_command(&store, &persistence, &["RPUSH", "list", "fresh"]).await;
        apply_test_command(&store, &persistence, &["LPOP", "list"]).await;
        assert!(!store.exists("list"));

        persistence.binlog.flush().await.unwrap();
        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovery.last_sequence, 2);
        assert!(!recovered.exists("list"));
    }

    #[tokio::test]
    async fn transaction_state_is_invisible_until_its_batch_is_persisted() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let (append_started_tx, append_started_rx) = oneshot::channel();
        let (allow_append_tx, allow_append_rx) = oneshot::channel();
        let worker = tokio::spawn(async move {
            let mut append_started_tx = Some(append_started_tx);
            let mut allow_append_rx = Some(allow_append_rx);
            while let Some(message) = receiver.recv().await {
                match message {
                    LogMessage::Append { completion, .. } => {
                        if let Some(started) = append_started_tx.take() {
                            let _ = started.send(());
                        }
                        if let Some(allow) = allow_append_rx.take() {
                            let _ = allow.await;
                        }
                        let _ = completion.send(Ok(()));
                    }
                    LogMessage::Barrier { completion }
                    | LogMessage::Flush { completion }
                    | LogMessage::SyncData { completion }
                    | LogMessage::Truncate { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                }
            }
        });
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        let transaction_store = Arc::clone(&store);
        let transaction_persistence = Arc::clone(&persistence);
        let transaction = tokio::spawn(async move {
            execute_transaction(
                &transaction_store,
                &transaction_persistence,
                vec![
                    vec!["SET".to_string(), "first".to_string(), "one".to_string()],
                    vec!["SET".to_string(), "second".to_string(), "two".to_string()],
                ],
                false,
            )
            .await
        });
        append_started_rx.await.unwrap();

        let read_store = Arc::clone(&store);
        let read_persistence = Arc::clone(&persistence);
        let mut read = tokio::spawn(async move {
            execute_ordered_command(
                &read_store,
                &read_persistence,
                &[
                    "MGET".to_string(),
                    "first".to_string(),
                    "second".to_string(),
                ],
            )
            .await
            .into_response()
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut read)
                .await
                .is_err(),
            "a reader observed transaction state before its batch was persisted"
        );

        allow_append_tx.send(()).unwrap();
        assert!(matches!(transaction.await.unwrap(), RESPValue::Array(_)));
        assert!(matches!(
            read.await.unwrap(),
            RESPValue::Array(values) if values.len() == 2
                && matches!(&values[0], RESPValue::BulkString(Some(value)) if value == "one")
                && matches!(&values[1], RESPValue::BulkString(Some(value)) if value == "two")
        ));

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn transaction_persistence_failure_rolls_back_every_effect() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let worker = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    LogMessage::Append { completion, .. } => {
                        let _ = completion.send(Err(StorageFailure::rejected(
                            "injected transaction failure",
                        )));
                    }
                    LogMessage::Barrier { completion }
                    | LogMessage::Flush { completion }
                    | LogMessage::SyncData { completion }
                    | LogMessage::Truncate { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                }
            }
        });
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        store.set("existing".to_string(), "before".to_string());

        let response = execute_transaction(
            &store,
            &persistence,
            vec![
                vec![
                    "SET".to_string(),
                    "existing".to_string(),
                    "after".to_string(),
                ],
                vec![
                    "SET".to_string(),
                    "created".to_string(),
                    "value".to_string(),
                ],
            ],
            false,
        )
        .await;

        assert!(matches!(response, RESPValue::Error(message) if message.starts_with("MISCONF")));
        assert_eq!(store.get("existing"), Ok(Some("before".to_string())));
        assert_eq!(store.get("created"), Ok(None));
        assert_eq!(persistence.sequence(), 0);
        assert!(persistence.backlog.lock().unwrap().is_empty());
        assert!(!persistence.accepting_writes.load(Ordering::SeqCst));

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn transaction_failure_restores_eviction_victims() {
        let candidate = ShardedStore::new();
        candidate.set("created".to_string(), "x".repeat(90));
        let limit = candidate.used_memory_bytes();
        let store = Arc::new(ShardedStore::with_maxmemory(
            limit,
            EvictionPolicy::AllKeysLru,
        ));
        store.set("victim".to_string(), "original".to_string());
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let worker = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    LogMessage::Append { completion, .. } => {
                        let _ = completion.send(Err(StorageFailure::rejected(
                            "injected transaction failure",
                        )));
                    }
                    LogMessage::Barrier { completion }
                    | LogMessage::Flush { completion }
                    | LogMessage::SyncData { completion }
                    | LogMessage::Truncate { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                }
            }
        });
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);

        let response = execute_transaction(
            &store,
            &persistence,
            vec![vec![
                "SET".to_string(),
                "created".to_string(),
                "x".repeat(90),
            ]],
            false,
        )
        .await;

        assert!(matches!(response, RESPValue::Error(message) if message.starts_with("MISCONF")));
        assert_eq!(store.get("victim"), Ok(Some("original".to_string())));
        assert_eq!(store.get("created"), Ok(None));
        assert!(store.used_memory_bytes() <= limit);

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn aborting_client_after_binlog_write_preserves_live_and_recovered_state() {
        let directory = TestPersistenceDirectory::new();
        let paused = start_paused_append_worker(directory.paths.binlog.clone());
        let persistence = test_persistence(directory.paths.clone(), paused.log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        let command_store = Arc::clone(&store);
        let command_persistence = Arc::clone(&persistence);
        let command = tokio::spawn(async move {
            execute_ordered_command(
                &command_store,
                &command_persistence,
                &["SET".to_string(), "key".to_string(), "value".to_string()],
            )
            .await
        });

        paused.persisted.await.unwrap();
        command.abort();
        assert!(command.await.unwrap_err().is_cancelled());
        paused.release.send(()).unwrap();
        paused.completed.await.unwrap();
        wait_for_commit_boundary(&persistence).await;

        assert_eq!(store.get("key"), Ok(Some("value".to_string())));
        assert_eq!(persistence.sequence(), 1);
        assert_eq!(persistence.backlog.lock().unwrap().len(), 1);

        drop(persistence);
        paused.handle.await.unwrap();
        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovery.last_sequence, 1);
        assert_eq!(recovered.get("key"), Ok(Some("value".to_string())));
    }

    #[tokio::test]
    async fn aborting_transaction_after_binlog_write_completes_the_durable_batch() {
        let directory = TestPersistenceDirectory::new();
        let paused = start_paused_append_worker(directory.paths.binlog.clone());
        let persistence = test_persistence(directory.paths.clone(), paused.log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        let transaction_store = Arc::clone(&store);
        let transaction_persistence = Arc::clone(&persistence);
        let transaction = tokio::spawn(async move {
            execute_transaction(
                &transaction_store,
                &transaction_persistence,
                vec![
                    vec!["SET".to_string(), "first".to_string(), "one".to_string()],
                    vec!["SET".to_string(), "second".to_string(), "two".to_string()],
                ],
                false,
            )
            .await
        });

        paused.persisted.await.unwrap();
        transaction.abort();
        assert!(transaction.await.unwrap_err().is_cancelled());
        paused.release.send(()).unwrap();
        paused.completed.await.unwrap();
        wait_for_commit_boundary(&persistence).await;

        assert_eq!(store.get("first"), Ok(Some("one".to_string())));
        assert_eq!(store.get("second"), Ok(Some("two".to_string())));
        assert_eq!(persistence.sequence(), 1);
        assert_eq!(persistence.backlog.lock().unwrap().len(), 1);

        drop(persistence);
        paused.handle.await.unwrap();
        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovery.last_sequence, 1);
        assert_eq!(recovered.get("first"), Ok(Some("one".to_string())));
        assert_eq!(recovered.get("second"), Ok(Some("two".to_string())));
    }

    #[tokio::test]
    async fn aborting_obp_client_after_binlog_write_completes_the_commit() {
        let directory = TestPersistenceDirectory::new();
        let paused = start_paused_append_worker(directory.paths.binlog.clone());
        let persistence = test_persistence(directory.paths.clone(), paused.log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        let obp_store = Arc::clone(&store);
        let obp_persistence = Arc::clone(&persistence);
        let command = tokio::spawn(async move {
            let mut authenticated = true;
            execute_obp_command(
                &obp_store,
                &obp_persistence,
                OBPFrame {
                    cmd: 0x02,
                    flags: 0,
                    correlation_id: 7,
                    args: vec![Bytes::from_static(b"key"), Bytes::from_static(b"value")],
                    payload: None,
                },
                &mut authenticated,
                false,
            )
            .await
        });

        paused.persisted.await.unwrap();
        command.abort();
        assert!(command.await.unwrap_err().is_cancelled());
        paused.release.send(()).unwrap();
        paused.completed.await.unwrap();
        wait_for_commit_boundary(&persistence).await;

        assert_eq!(store.get("key"), Ok(Some("value".to_string())));
        assert_eq!(persistence.sequence(), 1);
        assert_eq!(persistence.backlog.lock().unwrap().len(), 1);

        drop(persistence);
        paused.handle.await.unwrap();
        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovery.last_sequence, 1);
        assert_eq!(recovered.get("key"), Ok(Some("value".to_string())));
    }

    #[tokio::test]
    async fn aborting_replica_read_after_binlog_write_completes_effect_application() {
        let directory = TestPersistenceDirectory::new();
        let paused = start_paused_append_worker(directory.paths.binlog.clone());
        let persistence = test_persistence(directory.paths.clone(), paused.log_tx, 0);
        persistence.replication_ready.store(true, Ordering::SeqCst);
        let store = Arc::new(ShardedStore::new());
        let replica_store = Arc::clone(&store);
        let replica_persistence = Arc::clone(&persistence);
        let batch = CommittedBatch::new(vec![CommittedEffect::Put {
            key: Bytes::from_static(b"replicated"),
            entry: PersistentEntry {
                value: OnyxValue::Blob(Bytes::from_static(b"value")),
                expires_at: None,
            },
        }])
        .unwrap();
        let apply = tokio::spawn(async move {
            persist_and_apply_replica_effect(&replica_store, &replica_persistence, 1, &batch).await
        });

        paused.persisted.await.unwrap();
        assert_eq!(store.get("replicated"), Ok(None));
        apply.abort();
        assert!(apply.await.unwrap_err().is_cancelled());
        paused.release.send(()).unwrap();
        paused.completed.await.unwrap();
        wait_for_commit_boundary(&persistence).await;

        assert_eq!(store.get("replicated"), Ok(Some("value".to_string())));
        assert_eq!(persistence.sequence(), 1);

        drop(persistence);
        paused.handle.await.unwrap();
        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovery.last_sequence, 1);
        assert_eq!(recovered.get("replicated"), Ok(Some("value".to_string())));
    }

    #[tokio::test]
    async fn promotion_waits_for_a_detached_replica_commit_finalizer() {
        let directory = TestPersistenceDirectory::new();
        let paused = start_paused_append_worker(directory.paths.binlog.clone());
        let persistence = test_persistence(directory.paths.clone(), paused.log_tx, 0);
        persistence.replication_ready.store(true, Ordering::SeqCst);
        let store = Arc::new(ShardedStore::new());
        let replica_store = Arc::clone(&store);
        let replica_persistence = Arc::clone(&persistence);
        let batch = CommittedBatch::new(vec![CommittedEffect::Put {
            key: Bytes::from_static(b"before-promotion"),
            entry: PersistentEntry {
                value: OnyxValue::Blob(Bytes::from_static(b"accepted")),
                expires_at: None,
            },
        }])
        .unwrap();
        let apply = tokio::spawn(async move {
            persist_and_apply_replica_effect(&replica_store, &replica_persistence, 1, &batch).await
        });

        paused.persisted.await.unwrap();
        apply.abort();
        assert!(apply.await.unwrap_err().is_cancelled());
        assert!(persistence.write_gate.try_lock().is_err());

        let promotion_persistence = Arc::clone(&persistence);
        let promotion =
            tokio::spawn(async move { prepare_replica_promotion(&promotion_persistence).await });
        tokio::task::yield_now().await;
        assert!(!promotion.is_finished());

        paused.release.send(()).unwrap();
        paused.completed.await.unwrap();
        promotion.await.unwrap().unwrap();

        assert_eq!(
            store.get("before-promotion"),
            Ok(Some("accepted".to_string()))
        );
        assert_eq!(persistence.sequence(), 1);
        assert!(persistence.promote_to_master.load(Ordering::SeqCst));
        assert!(!persistence.replication_ready.load(Ordering::SeqCst));

        drop(persistence);
        paused.handle.await.unwrap();
    }

    #[tokio::test]
    async fn binlog_append_failure_is_not_acknowledged_or_replicated() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let worker = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    LogMessage::Append { completion, .. } => {
                        let _ = completion
                            .send(Err(StorageFailure::rejected("injected append failure")));
                    }
                    LogMessage::Barrier { completion }
                    | LogMessage::Flush { completion }
                    | LogMessage::SyncData { completion }
                    | LogMessage::Truncate { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                }
            }
        });
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let mut live_receiver = persistence.replica_tx.subscribe();
        let store = Arc::new(ShardedStore::new());

        let command = vec!["SET".to_string(), "key".to_string(), "value".to_string()];
        let outcome = execute_ordered_command(&store, &persistence, &command).await;
        assert_eq!(outcome.mutation, MutationState::NoChange);
        assert!(matches!(
            outcome.response,
            RESPValue::Error(message) if message.starts_with("MISCONF")
        ));
        assert!(!persistence.accepting_writes.load(Ordering::SeqCst));
        assert!(persistence.backlog.lock().unwrap().is_empty());
        assert!(live_receiver.try_recv().is_err());
        assert_eq!(persistence.sequence(), 0);
        assert_eq!(store.get("key"), Ok(None));

        let second_command = vec![
            "SET".to_string(),
            "second".to_string(),
            "rejected".to_string(),
        ];
        let second_outcome = execute_ordered_command(&store, &persistence, &second_command).await;
        assert!(matches!(second_outcome.response, RESPValue::Error(_)));
        assert_eq!(second_outcome.mutation, MutationState::NoChange);
        assert_eq!(store.get("second"), Ok(None));

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_mutations_share_binlog_backlog_and_live_sequence_order() {
        const MUTATION_COUNT: u64 = 32;
        let directory = TestPersistenceDirectory::new();
        let store = Arc::new(ShardedStore::new());
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let mut live_receiver = persistence.replica_tx.subscribe();
        let mut tasks = Vec::new();
        for _ in 0..MUTATION_COUNT {
            let store = Arc::clone(&store);
            let persistence = Arc::clone(&persistence);
            tasks.push(tokio::spawn(async move {
                apply_test_command(&store, &persistence, &["INCR", "counter"]).await;
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        let backlog_sequences: Vec<u64> = persistence
            .backlog
            .lock()
            .unwrap()
            .iter()
            .map(|(sequence, _)| *sequence)
            .collect();
        assert_eq!(backlog_sequences, (1..=MUTATION_COUNT).collect::<Vec<_>>());
        let mut live_sequences = Vec::new();
        while let Ok((sequence, _)) = live_receiver.try_recv() {
            live_sequences.push(sequence);
        }
        assert_eq!(live_sequences, backlog_sequences);
        assert_eq!(persistence.sequence(), MUTATION_COUNT);

        persistence.binlog.flush().await.unwrap();
        let mut binlog_sequences = Vec::new();
        for_each_binlog_record(&directory.paths.binlog, |record| {
            let DecodedBinlogRecord::Versioned { sequence, .. } = decode_binlog_record(record)?;
            binlog_sequences.push(sequence);
            Ok(())
        })
        .unwrap();
        assert_eq!(binlog_sequences, backlog_sequences);
        assert_eq!(store.get("counter"), Ok(Some(MUTATION_COUNT.to_string())));

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_collection_cleanup_cannot_delete_a_concurrent_write() {
        const ITERATIONS: usize = 32;
        let directory = TestPersistenceDirectory::new();
        let store = Arc::new(ShardedStore::new());
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;

        for iteration in 0..ITERATIONS {
            let key = format!("list-{iteration}");
            store.set_value(
                Bytes::copy_from_slice(key.as_bytes()),
                OnyxValue::List(vec![Bytes::from_static(b"old")]),
                None,
            );
            let barrier = Arc::new(tokio::sync::Barrier::new(3));
            let pop_store = Arc::clone(&store);
            let pop_persistence = Arc::clone(&persistence);
            let pop_barrier = Arc::clone(&barrier);
            let pop_key = key.clone();
            let pop = tokio::spawn(async move {
                pop_barrier.wait().await;
                execute_ordered_command(
                    &pop_store,
                    &pop_persistence,
                    &["LPOP".to_string(), pop_key],
                )
                .await
            });
            let push_store = Arc::clone(&store);
            let push_persistence = Arc::clone(&persistence);
            let push_barrier = Arc::clone(&barrier);
            let push_key = key.clone();
            let push = tokio::spawn(async move {
                push_barrier.wait().await;
                execute_ordered_command(
                    &push_store,
                    &push_persistence,
                    &["RPUSH".to_string(), push_key, "new".to_string()],
                )
                .await
            });
            barrier.wait().await;
            pop.await.unwrap();
            push.await.unwrap();

            assert_eq!(store.lrange(&key, 0, -1), Ok(vec!["new".to_string()]));
        }

        persistence.binlog.flush().await.unwrap();
        drop(persistence);
        worker.await.unwrap();
    }

    #[test]
    fn test_binlog_roundtrip_set_with_expiration() {
        let args = vec![
            "SET".to_string(),
            "k".to_string(),
            "v".to_string(),
            "EXAT".to_string(),
            "9999999999".to_string(),
        ];
        let record = command_to_binary_record("SET", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(decoded, vec!["SET", "k", "v", "EXAT", "9999999999"]);
    }

    #[test]
    fn test_binlog_roundtrip_set_without_expiration_does_not_include_exat() {
        // A SET without expiration must not acquire an EXAT during round trip.
        let args = vec!["SET".to_string(), "k".to_string(), "v".to_string()];
        let record = command_to_binary_record("SET", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(decoded.len(), 3);
    }

    #[test]
    fn test_binlog_roundtrip_mset() {
        let args = vec![
            "MSET".to_string(),
            "a".to_string(),
            "1".to_string(),
            "b".to_string(),
            "2".to_string(),
        ];
        let record = command_to_binary_record("MSET", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(decoded, vec!["MSET", "a", "1", "b", "2"]);
    }

    #[test]
    fn test_binlog_roundtrip_del() {
        let args = vec!["DEL".to_string(), "key".to_string()];
        let record = command_to_binary_record("DEL", &args, None).unwrap();
        assert_eq!(binary_record_to_args(&record).unwrap(), vec!["DEL", "key"]);
    }

    #[test]
    fn test_binlog_roundtrip_expire_becomes_expireat() {
        // Relative EXPIRE is encoded as an absolute EXPIREAT deadline.
        let args = vec!["EXPIRE".to_string(), "key".to_string(), "12345".to_string()];
        let record = command_to_binary_record("EXPIRE", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(decoded, vec!["EXPIREAT", "key", "12345"]);
    }

    #[test]
    fn test_binlog_roundtrip_lpush_rpush() {
        let lpush = vec!["LPUSH".to_string(), "list".to_string(), "x".to_string()];
        let record = command_to_binary_record("LPUSH", &lpush, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["LPUSH", "list", "x"]
        );

        let rpush = vec!["RPUSH".to_string(), "list".to_string(), "y".to_string()];
        let record = command_to_binary_record("RPUSH", &rpush, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["RPUSH", "list", "y"]
        );
    }

    #[test]
    fn test_binlog_roundtrip_lpop_rpop() {
        let args = vec!["LPOP".to_string(), "list".to_string()];
        let record = command_to_binary_record("LPOP", &args, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["LPOP", "list"]
        );
    }

    #[test]
    fn test_binlog_roundtrip_hset() {
        let args = vec![
            "HSET".to_string(),
            "h".to_string(),
            "field".to_string(),
            "value".to_string(),
        ];
        let record = command_to_binary_record("HSET", &args, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["HSET", "h", "field", "value"]
        );
    }

    #[test]
    fn test_binlog_roundtrip_sadd_srem() {
        let sadd = vec!["SADD".to_string(), "s".to_string(), "membro".to_string()];
        let record = command_to_binary_record("SADD", &sadd, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["SADD", "s", "membro"]
        );

        let srem = vec!["SREM".to_string(), "s".to_string(), "membro".to_string()];
        let record = command_to_binary_record("SREM", &srem, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["SREM", "s", "membro"]
        );
    }

    #[test]
    fn test_binlog_roundtrip_rename() {
        let args = vec![
            "RENAME".to_string(),
            "vecchia".to_string(),
            "nuova".to_string(),
        ];
        let record = command_to_binary_record("RENAME", &args, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["RENAME", "vecchia", "nuova"]
        );
    }

    #[test]
    fn test_binlog_roundtrip_incrby_decrby() {
        let incr = vec!["INCRBY".to_string(), "c".to_string(), "7".to_string()];
        let record = command_to_binary_record("INCRBY", &incr, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["INCRBY", "c", "7"]
        );
        let decr = vec!["DECRBY".to_string(), "c".to_string(), "3".to_string()];
        let record = command_to_binary_record("DECRBY", &decr, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["DECRBY", "c", "3"]
        );
    }

    #[test]
    fn test_binlog_roundtrip_append() {
        let args = vec!["APPEND".to_string(), "s".to_string(), "suffix".to_string()];
        let record = command_to_binary_record("APPEND", &args, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["APPEND", "s", "suffix"]
        );
    }

    #[test]
    fn test_binlog_roundtrip_hdel() {
        let args = vec!["HDEL".to_string(), "h".to_string(), "field".to_string()];
        let record = command_to_binary_record("HDEL", &args, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["HDEL", "h", "field"]
        );
    }

    #[test]
    fn test_binlog_roundtrip_copy() {
        let args = vec!["COPY".to_string(), "src".to_string(), "dst".to_string()];
        let record = command_to_binary_record("COPY", &args, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["COPY", "src", "dst"]
        );
    }
    #[test]
    fn test_binlog_roundtrip_json_set() {
        let args = vec![
            "JSON.SET".to_string(),
            "user".to_string(),
            "$".to_string(),
            "{\"name\":\"Marco\"}".to_string(),
        ];
        let record = command_to_binary_record("JSON.SET", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(
            decoded,
            vec!["JSON.SET", "user", "$", "{\"name\":\"Marco\"}"]
        );
    }

    #[test]
    fn test_binlog_roundtrip_json_set_nested_path() {
        let args = vec![
            "JSON.SET".to_string(),
            "user".to_string(),
            "$.address.city".to_string(),
            "\"Rome\"".to_string(),
        ];
        let record = command_to_binary_record("JSON.SET", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(
            decoded,
            vec!["JSON.SET", "user", "$.address.city", "\"Rome\""]
        );
    }
    #[test]
    fn test_binlog_roundtrip_json_del() {
        let args = vec![
            "JSON.DEL".to_string(),
            "user".to_string(),
            "$.age".to_string(),
        ];
        let record = command_to_binary_record("JSON.DEL", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(decoded, vec!["JSON.DEL", "user", "$.age"]);
    }

    #[test]
    fn test_binlog_json_set_insufficient_args_returns_none() {
        let args = vec!["JSON.SET".to_string(), "user".to_string()];
        assert!(command_to_binary_record("JSON.SET", &args, None).is_none());
    }

    #[test]
    fn test_binlog_json_del_insufficient_args_returns_none() {
        let args = vec!["JSON.DEL".to_string(), "user".to_string()];
        assert!(command_to_binary_record("JSON.DEL", &args, None).is_none());
    }

    #[test]
    fn test_snapshot_roundtrip_json() {
        let entry = DataEntry {
            value: OnyxValue::Json(serde_json::json!({"name": "Marco", "age": 18})),
            expires_at: None,
            created_at: 0,
            last_accessed: 0,
        };
        let line = value_to_line("k", &entry);
        let (_, decoded) = line_to_entry(&line).unwrap();
        match decoded.value {
            OnyxValue::Json(v) => assert_eq!(v, serde_json::json!({"name": "Marco", "age": 18})),
            _ => panic!("wrong type after round-trip"),
        }
    }

    #[test]
    fn test_binlog_unknown_command_returns_none() {
        let args = vec!["PING".to_string()];
        assert!(command_to_binary_record("PING", &args, None).is_none());
    }

    #[test]
    fn test_binlog_insufficient_args_returns_none() {
        // A short argument array is rejected without panicking.
        let args = vec!["SET".to_string()];
        assert!(command_to_binary_record("SET", &args, None).is_none());
    }

    // ============================================================
    // Corrupt legacy records are rejected without panicking.
    // ============================================================

    #[test]
    fn empty_record_does_not_panic() {
        assert!(binary_record_to_args(&[]).is_none());
    }

    #[test]
    fn record_truncated_mid_key_does_not_panic() {
        // Declare a 100-byte key but provide only two bytes.
        let record = vec![OP_SET, 0x00, 0x64, b'a', b'b'];
        assert!(binary_record_to_args(&record).is_none());
    }

    #[test]
    fn truncated_record_mid_value_does_not_panic() {
        // Provide a valid key, then declare a missing 1,000-byte value.
        let mut record = vec![OP_SET];
        record.extend_from_slice(&[0x00, 0x01]); // key_len = 1
        record.push(b'k');
        record.push(1); // String value type.
        record.extend_from_slice(&[0x00, 0x00, 0x03, 0xE8]); // Declared value length is 1,000 bytes.
        assert!(binary_record_to_args(&record).is_none());
    }

    #[test]
    fn read_u16_be_rejects_a_truncated_buffer() {
        let buf = [0x00u8]; // One byte is insufficient for a u16.
        let mut offset = 0;
        assert_eq!(read_u16_be(&buf, &mut offset), None);
    }

    #[test]
    fn read_u64_be_rejects_a_truncated_buffer() {
        let buf = [0x00u8, 0x01, 0x02]; // Three bytes are insufficient for a u64.
        let mut offset = 0;
        assert_eq!(read_u64_be(&buf, &mut offset), None);
    }

    #[test]
    fn safe_slice_rejects_out_of_bounds_ranges() {
        let buf = [1u8, 2, 3];
        assert!(safe_slice(&buf, 0, 10).is_none());
        assert!(safe_slice(&buf, 5, 1).is_none());
        assert_eq!(safe_slice(&buf, 0, 3), Some(&buf[..]));
    }

    // ============================================================
    // Legacy text snapshot round trips.
    // ============================================================

    #[test]
    fn test_snapshot_roundtrip_string() {
        let entry = DataEntry {
            value: OnyxValue::Blob(Bytes::from("ciao")),
            expires_at: None,
            created_at: 0,
            last_accessed: 0,
        };
        let line = value_to_line("k", &entry);
        let (key, decoded) = line_to_entry(&line).unwrap();
        assert_eq!(key, "k");
        match decoded.value {
            OnyxValue::Blob(b) => assert_eq!(b, Bytes::from("ciao")),
            _ => panic!("wrong type after round-trip"),
        }
    }

    #[test]
    fn test_snapshot_roundtrip_with_expiration() {
        let entry = DataEntry {
            value: OnyxValue::Blob(Bytes::from("v")),
            expires_at: Some(123456),
            created_at: 0,
            last_accessed: 0,
        };
        let line = value_to_line("k", &entry);
        let (_, decoded) = line_to_entry(&line).unwrap();
        assert_eq!(decoded.expires_at, Some(123456));
    }

    #[test]
    fn test_snapshot_roundtrip_empty_list() {
        let entry = DataEntry {
            value: OnyxValue::List(vec![]),
            expires_at: None,
            created_at: 0,
            last_accessed: 0,
        };
        let line = value_to_line("k", &entry);
        let (_, decoded) = line_to_entry(&line).unwrap();
        match decoded.value {
            OnyxValue::List(l) => assert!(l.is_empty()),
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn test_snapshot_roundtrip_hash() {
        let mut h = std::collections::HashMap::new();
        h.insert(Bytes::from("f1"), Bytes::from("v1"));
        let entry = DataEntry {
            value: OnyxValue::Hash(h),
            expires_at: None,
            created_at: 0,
            last_accessed: 0,
        };
        let line = value_to_line("k", &entry);
        let (_, decoded) = line_to_entry(&line).unwrap();
        match decoded.value {
            OnyxValue::Hash(m) => assert_eq!(m.get(&Bytes::from("f1")), Some(&Bytes::from("v1"))),
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn malformed_snapshot_line_returns_none() {
        assert!(line_to_entry("this is not a valid snapshot line").is_none());
        assert!(line_to_entry("").is_none());
    }
    // ============================================================
    // Partial synchronization boundary regression coverage.
    // ============================================================

    #[test]
    fn matching_replication_id_allows_partial_sync() {
        assert!(replid_allows_partial(42, 42));
    }

    #[test]
    fn different_replication_id_requires_full_sync() {
        assert!(!replid_allows_partial(42, 99));
    }

    #[test]
    fn unknown_replication_id_requires_full_sync() {
        // Zero means that the replica has never installed an upstream identity.
        assert!(!replid_allows_partial(0, 99));
    }

    #[test]
    fn partial_sync_backlog_covers_the_requested_offset() {
        // A backlog beginning at 5 can continue a replica installed through 4.
        assert!(partial_resync_possible(4, Some(5), 100));
    }

    #[test]
    fn partial_sync_rejects_a_backlog_gap() {
        // A backlog beginning at 20 cannot continue a replica installed through 4.
        assert!(!partial_resync_possible(4, Some(20), 100));
    }

    #[test]
    fn empty_backlog_allows_an_exactly_aligned_replica() {
        // An empty backlog is sufficient only at the master's current sequence.
        assert!(partial_resync_possible(9, None, 9));
    }

    #[test]
    fn empty_backlog_after_master_restart_rejects_a_stale_offset() {
        // A restarted master at sequence zero cannot accept an offset inherited
        // from the previous process merely because its backlog is empty.
        assert!(!partial_resync_possible(9, None, 0));
    }
    #[test]
    fn json_arrlen_and_objkeys_work_through_the_store() {
        let store = ShardedStore::new();
        store
            .json_set(
                "user",
                "$",
                serde_json::json!({"name": "Marco", "tag": ["dev", "rust"]}),
            )
            .unwrap();

        assert_eq!(store.json_arrlen("user", "$.tag"), Ok(Some(2)));

        let keys = store.json_objkeys("user", "$").unwrap().unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"name".to_string()));
        assert!(keys.contains(&"tag".to_string()));
    }

    #[test]
    fn test_json_arrlen_on_non_array_returns_none() {
        let store = ShardedStore::new();
        store
            .json_set("user", "$", serde_json::json!({"name": "Marco"}))
            .unwrap();
        assert_eq!(store.json_arrlen("user", "$.name"), Ok(None));
    }

    #[test]
    fn test_json_objkeys_on_non_object_returns_none() {
        let store = ShardedStore::new();
        store
            .json_set("user", "$", serde_json::json!({"tag": ["dev"]}))
            .unwrap();
        assert_eq!(store.json_objkeys("user", "$.tag"), Ok(None));
    }
    #[test]
    fn test_json_arrlen_non_existent_key_returns_none() {
        let store = ShardedStore::new();
        assert_eq!(store.json_arrlen("non_esiste", "$"), Ok(None));
    }

    #[test]
    fn test_json_objkeys_non_existent_key_returns_none() {
        let store = ShardedStore::new();
        assert_eq!(store.json_objkeys("non_esiste", "$"), Ok(None));
    }

    #[test]
    fn test_binlog_roundtrip_json_numincrby() {
        let args = vec![
            "JSON.NUMINCRBY".to_string(),
            "user".to_string(),
            "$.visits".to_string(),
            "3".to_string(),
        ];
        let record = command_to_binary_record("JSON.NUMINCRBY", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(decoded, vec!["JSON.NUMINCRBY", "user", "$.visits", "3"]);
    }

    #[test]
    fn test_binlog_roundtrip_json_arrappend() {
        let args = vec![
            "JSON.ARRAPPEND".to_string(),
            "user".to_string(),
            "$.tag".to_string(),
            "\"rust\"".to_string(),
        ];
        let record = command_to_binary_record("JSON.ARRAPPEND", &args, None).unwrap();
        let decoded = binary_record_to_args(&record).unwrap();
        assert_eq!(decoded, vec!["JSON.ARRAPPEND", "user", "$.tag", "\"rust\""]);
    }

    fn persistent_state(store: &ShardedStore) -> std::collections::HashMap<Bytes, PersistentEntry> {
        store
            .raw_entries()
            .into_iter()
            .map(|(key, entry)| (key, entry.into()))
            .collect()
    }

    #[tokio::test]
    async fn failed_setnx_produces_no_committed_effect_or_replay_mutation() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let store = Arc::new(ShardedStore::new());

        apply_test_command(&store, &persistence, &["SET", "key", "original"]).await;
        let command = vec![
            "SETNX".to_string(),
            "key".to_string(),
            "replacement".to_string(),
        ];
        let outcome = execute_ordered_command(&store, &persistence, &command).await;
        assert!(matches!(outcome.response, RESPValue::Integer(0)));
        assert_eq!(outcome.mutation, MutationState::NoChange);
        assert_eq!(persistence.sequence(), 1);
        assert_eq!(persistence.backlog.lock().unwrap().len(), 1);

        persistence.binlog.flush().await.unwrap();
        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovery.last_sequence, 1);
        assert_eq!(recovered.get("key"), Ok(Some("original".to_string())));
    }

    #[tokio::test]
    async fn signed_numeric_effects_and_overflow_recover_exactly() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let store = Arc::new(ShardedStore::new());

        apply_test_command(&store, &persistence, &["SET", "counter", "10"]).await;
        apply_test_command(&store, &persistence, &["INCRBY", "counter", "-2"]).await;
        apply_test_command(&store, &persistence, &["DECRBY", "counter", "-2"]).await;
        apply_test_command(&store, &persistence, &["INCRBY", "counter", "5"]).await;
        apply_test_command(&store, &persistence, &["DECRBY", "counter", "3"]).await;
        apply_test_command(
            &store,
            &persistence,
            &["SET", "maximum", "9223372036854775807"],
        )
        .await;
        let offset_before_overflow = persistence.sequence();
        let overflow = vec!["INCR".to_string(), "maximum".to_string()];
        let outcome = execute_ordered_command(&store, &persistence, &overflow).await;
        assert!(matches!(
            outcome.response,
            RESPValue::Error(message) if message.contains("overflow")
        ));
        assert_eq!(outcome.mutation, MutationState::NoChange);
        assert_eq!(persistence.sequence(), offset_before_overflow);

        persistence.binlog.flush().await.unwrap();
        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovered.get("counter"), Ok(Some("12".to_string())));
        assert_eq!(
            recovered.get("maximum"),
            Ok(Some("9223372036854775807".to_string()))
        );
    }

    #[test]
    fn committed_effect_codec_is_binary_safe_and_strict() {
        let batch = CommittedBatch {
            effects: vec![
                CommittedEffect::Put {
                    key: Bytes::from_static(b"\xff\x00key"),
                    entry: PersistentEntry {
                        value: OnyxValue::Blob(Bytes::from_static(b"\x80\x00value\xff")),
                        expires_at: Some(42),
                    },
                },
                CommittedEffect::Delete {
                    key: Bytes::from_static(b"\xfe\x00deleted"),
                },
            ],
        };
        let encoded = encode_committed_batch(&batch).unwrap();
        assert_eq!(decode_committed_batch(&encoded).unwrap(), batch);
        assert_eq!(
            decode_replication_effect("7", &encoded).unwrap(),
            (7, batch)
        );

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(
            decode_committed_batch(&trailing)
                .unwrap_err()
                .to_string()
                .contains("Trailing bytes")
        );
        for truncated_length in 0..encoded.len() {
            assert!(decode_committed_batch(&encoded[..truncated_length]).is_err());
        }
        let boundary_key = Bytes::from(vec![b'k'; u16::MAX as usize + 1]);
        let boundary_batch = CommittedBatch {
            effects: vec![CommittedEffect::Delete {
                key: boundary_key.clone(),
            }],
        };
        let boundary_encoded = encode_committed_batch(&boundary_batch).unwrap();
        assert_eq!(
            decode_committed_batch(&boundary_encoded).unwrap(),
            boundary_batch
        );
        assert!(
            encode_committed_batch(&CommittedBatch {
                effects: Vec::new()
            })
            .is_err()
        );
        assert!(encode_versioned_binlog_record(0, &encoded).is_err());
        if usize::BITS > 32 {
            assert!(checked_u32_length(u32::MAX as usize + 1, "Test payload").is_err());
        }
    }

    #[test]
    fn structurally_valid_binlog_bit_flip_is_rejected() {
        let directory = TestPersistenceDirectory::new();
        let batch = CommittedBatch::new(vec![CommittedEffect::Put {
            key: Bytes::from_static(b"integrity-key"),
            entry: PersistentEntry {
                value: OnyxValue::Blob(Bytes::from_static(b"original-value")),
                expires_at: None,
            },
        }])
        .unwrap();
        let effects = encode_committed_batch(&batch).unwrap();
        let mut record = encode_versioned_binlog_record(1, &effects).unwrap();
        let value_offset = record
            .windows(b"original-value".len())
            .position(|window| window == b"original-value")
            .expect("encoded value must be present");
        record[value_offset] ^= 1;

        append_raw_binlog_record(&directory.paths, &record);
        let corrupt_length = fs::metadata(&directory.paths.binlog).unwrap().len();

        let error = load_data_from_paths(&ShardedStore::new(), &directory.paths).unwrap_err();
        assert!(error.to_string().contains("checksum"));
        assert_eq!(
            fs::metadata(&directory.paths.binlog).unwrap().len(),
            corrupt_length,
            "a complete corrupt record must not be truncated as a torn tail"
        );
    }

    #[test]
    fn binlog_checksum_covers_sequence_payload_and_checksum_bytes() {
        let batch = CommittedBatch::new(vec![CommittedEffect::Delete {
            key: Bytes::from_static(b"checksum-key"),
        }])
        .unwrap();
        let effects = encode_committed_batch(&batch).unwrap();
        let record = encode_versioned_binlog_record(7, &effects).unwrap();
        assert!(matches!(
            decode_binlog_record(&record).unwrap(),
            DecodedBinlogRecord::Versioned {
                sequence: 7,
                integrity: BinlogRecordIntegrity::Checksummed,
                ..
            }
        ));

        for offset in [
            BINLOG_RECORD_MAGIC.len(),
            BINLOG_RECORD_MAGIC.len() + BINLOG_RECORD_LENGTH_SIZE,
            BINLOG_RECORD_MAGIC.len() + BINLOG_RECORD_LENGTH_SIZE + 8,
            record.len() - 1,
        ] {
            let mut corrupted = record.clone();
            corrupted[offset] ^= 1;
            assert!(
                decode_binlog_record(&corrupted)
                    .unwrap_err()
                    .to_string()
                    .contains("checksum")
            );
        }
    }

    #[test]
    fn corrupt_outer_binlog_length_is_not_mistaken_for_a_torn_tail() {
        let directory = TestPersistenceDirectory::new();
        let batch = CommittedBatch::new(vec![CommittedEffect::Delete {
            key: Bytes::from_static(b"length-key"),
        }])
        .unwrap();
        let effects = encode_committed_batch(&batch).unwrap();
        let record = encode_versioned_binlog_record(1, &effects).unwrap();
        let mut binlog = File::create(&directory.paths.binlog).unwrap();
        binlog
            .write_all(&((record.len() as u32) + 1).to_be_bytes())
            .unwrap();
        binlog.write_all(&record).unwrap();
        binlog.sync_all().unwrap();
        let corrupt_length = fs::metadata(&directory.paths.binlog).unwrap().len();

        let error = load_data_from_paths(&ShardedStore::new(), &directory.paths).unwrap_err();
        assert!(error.to_string().contains("length"));
        assert_eq!(
            fs::metadata(&directory.paths.binlog).unwrap().len(),
            corrupt_length
        );
    }

    #[test]
    fn ambiguous_checksumless_binlog_tail_is_rejected() {
        let directory = TestPersistenceDirectory::new();
        let batch = CommittedBatch::new(vec![CommittedEffect::Delete {
            key: Bytes::from_static(b"legacy-length-key"),
        }])
        .unwrap();
        let effects = encode_committed_batch(&batch).unwrap();
        let mut legacy_record = Vec::new();
        legacy_record.extend_from_slice(CHECKSUMLESS_BINLOG_RECORD_MAGIC);
        write_u64_be(&mut legacy_record, 1);
        legacy_record.extend_from_slice(&effects);

        let mut binlog = File::create(&directory.paths.binlog).unwrap();
        binlog
            .write_all(&((legacy_record.len() as u32) + 1).to_be_bytes())
            .unwrap();
        binlog.write_all(&legacy_record).unwrap();
        binlog.sync_all().unwrap();
        let corrupt_length = fs::metadata(&directory.paths.binlog).unwrap().len();

        let error = load_data_from_paths(&ShardedStore::new(), &directory.paths).unwrap_err();
        assert!(error.to_string().contains("checksumless ONX3"));
        assert_eq!(
            fs::metadata(&directory.paths.binlog).unwrap().len(),
            corrupt_length
        );
    }

    #[test]
    fn incomplete_checksummed_binlog_tail_is_truncated_after_valid_history() {
        let directory = TestPersistenceDirectory::new();
        let first_batch = CommittedBatch::new(vec![CommittedEffect::Put {
            key: Bytes::from_static(b"durable-key"),
            entry: PersistentEntry {
                value: OnyxValue::Blob(Bytes::from_static(b"durable-value")),
                expires_at: None,
            },
        }])
        .unwrap();
        let first_effects = encode_committed_batch(&first_batch).unwrap();
        let first_record = encode_versioned_binlog_record(1, &first_effects).unwrap();
        append_raw_binlog_record(&directory.paths, &first_record);
        let valid_length = fs::metadata(&directory.paths.binlog).unwrap().len();

        let second_batch = CommittedBatch::new(vec![CommittedEffect::Delete {
            key: Bytes::from_static(b"durable-key"),
        }])
        .unwrap();
        let second_effects = encode_committed_batch(&second_batch).unwrap();
        let second_record = encode_versioned_binlog_record(2, &second_effects).unwrap();
        let mut binlog = OpenOptions::new()
            .append(true)
            .open(&directory.paths.binlog)
            .unwrap();
        binlog
            .write_all(&(second_record.len() as u32).to_be_bytes())
            .unwrap();
        binlog
            .write_all(&second_record[..second_record.len() - 1])
            .unwrap();
        binlog.sync_all().unwrap();

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(state.last_sequence, 1);
        assert_eq!(
            recovered.get("durable-key"),
            Ok(Some("durable-value".to_string()))
        );
        assert_eq!(
            fs::metadata(&directory.paths.binlog).unwrap().len(),
            valid_length
        );
    }

    #[test]
    fn recovery_accepts_mixed_checksumless_and_checksummed_binlog_history() {
        let directory = TestPersistenceDirectory::new();
        let legacy_batch = CommittedBatch::new(vec![CommittedEffect::Put {
            key: Bytes::from_static(b"mixed-key"),
            entry: PersistentEntry {
                value: OnyxValue::Blob(Bytes::from_static(b"legacy-value")),
                expires_at: None,
            },
        }])
        .unwrap();
        let legacy_effects = encode_committed_batch(&legacy_batch).unwrap();
        let mut legacy_record = Vec::new();
        legacy_record.extend_from_slice(CHECKSUMLESS_BINLOG_RECORD_MAGIC);
        write_u64_be(&mut legacy_record, 1);
        legacy_record.extend_from_slice(&legacy_effects);
        append_raw_binlog_record(&directory.paths, &legacy_record);

        let current_batch = CommittedBatch::new(vec![CommittedEffect::Put {
            key: Bytes::from_static(b"mixed-key"),
            entry: PersistentEntry {
                value: OnyxValue::Blob(Bytes::from_static(b"checksummed-value")),
                expires_at: None,
            },
        }])
        .unwrap();
        let current_effects = encode_committed_batch(&current_batch).unwrap();
        let current_record = encode_versioned_binlog_record(2, &current_effects).unwrap();
        append_raw_binlog_record(&directory.paths, &current_record);

        let inspection = inspect_binlog(&directory.paths.binlog).unwrap();
        assert!(inspection.contains_checksumless_records);
        assert_eq!(inspection.min_sequence, Some(1));
        assert_eq!(inspection.max_sequence, 2);

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(state.last_sequence, 2);
        assert_eq!(
            recovered.get("mixed-key"),
            Ok(Some("checksummed-value".to_string()))
        );
    }

    #[test]
    fn corrupt_snapshot_is_rejected_without_partially_installing_recovery_state() {
        let directory = TestPersistenceDirectory::new();
        let snapshot_store = ShardedStore::new();
        snapshot_store.set("snapshot-key".to_string(), "snapshot-value".to_string());
        write_snapshot_file(snapshot_store.raw_entries(), 1, &directory.paths).unwrap();

        let mut snapshot_bytes = fs::read(&directory.paths.snapshot).unwrap();
        assert!(snapshot_bytes.len() > 8);
        let checksum_offset = snapshot_bytes.len() - 8;
        snapshot_bytes[checksum_offset] ^= 1;
        fs::write(&directory.paths.snapshot, snapshot_bytes).unwrap();

        let recovered = ShardedStore::new();
        recovered.set("sentinel".to_string(), "preserved".to_string());
        assert!(load_data_from_paths(&recovered, &directory.paths).is_err());
        assert_eq!(recovered.get("sentinel"), Ok(Some("preserved".to_string())));
        assert_eq!(recovered.get("snapshot-key"), Ok(None));
    }

    #[tokio::test]
    async fn obp_binary_set_and_delete_round_trip_through_recovery() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let store = Arc::new(ShardedStore::new());
        let key = Bytes::from_static(b"\xff\x00binary-key");
        let value = Bytes::from_static(b"\x80\x00binary-value\xfe");
        let mut authenticated = true;

        execute_obp_command(
            &store,
            &persistence,
            OBPFrame {
                cmd: 0x02,
                flags: 0,
                correlation_id: 1,
                args: vec![key.clone(), value.clone()],
                payload: None,
            },
            &mut authenticated,
            false,
        )
        .await;
        persistence.binlog.flush().await.unwrap();
        drop(persistence);
        worker.await.unwrap();

        let recovered = Arc::new(ShardedStore::new());
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        let entry = recovered.peek_entry(&key).unwrap();
        assert_eq!(entry.value, OnyxValue::Blob(value));

        let (persistence, worker) =
            start_test_persistence(directory.paths.clone(), recovery.last_sequence).await;
        execute_obp_command(
            &recovered,
            &persistence,
            OBPFrame {
                cmd: 0x03,
                flags: 0,
                correlation_id: 2,
                args: vec![key.clone()],
                payload: None,
            },
            &mut authenticated,
            false,
        )
        .await;
        persistence.binlog.flush().await.unwrap();
        drop(persistence);
        worker.await.unwrap();

        let recovered_after_delete = ShardedStore::new();
        load_data_from_paths(&recovered_after_delete, &directory.paths).unwrap();
        assert!(recovered_after_delete.peek_entry(&key).is_none());
    }

    #[tokio::test]
    async fn unchanged_obp_set_does_not_require_a_persistence_record() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(1);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        let key = Bytes::from_static(b"key");
        let value = Bytes::from_static(b"value");
        store.set_value(key.clone(), OnyxValue::Blob(value.clone()), None);
        let mut authenticated = true;

        let response = execute_obp_command(
            &store,
            &persistence,
            OBPFrame {
                cmd: 0x02,
                flags: 0,
                correlation_id: 1,
                args: vec![key, value],
                payload: None,
            },
            &mut authenticated,
            false,
        )
        .await;

        assert!(response.payload.is_some());
        assert!(receiver.try_recv().is_err());
        assert_eq!(persistence.sequence(), 0);
    }

    #[tokio::test]
    async fn actual_eviction_victims_are_ordered_and_do_not_resurrect() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let store = Arc::new(ShardedStore::new());
        apply_test_command(&store, &persistence, &["SET", "first", "aaaaaaaa"]).await;
        apply_test_command(&store, &persistence, &["SET", "second", "bbbbbbbb"]).await;

        let limit = store.used_memory_bytes().saturating_sub(1);
        let evicted = store.evict_to_fit(limit, EvictionPolicy::AllKeysLru, &HashSet::new());
        assert!(!evicted.is_empty());
        let written_key = Bytes::from_static(b"causing-write");
        store.set_value(
            written_key.clone(),
            OnyxValue::Blob(Bytes::from_static(b"committed")),
            None,
        );
        let written_entry = store
            .peek_entry(&written_key)
            .map(PersistentEntry::from)
            .unwrap();
        let mut effects: Vec<CommittedEffect> = evicted
            .iter()
            .map(|(key, _)| CommittedEffect::Delete { key: key.clone() })
            .collect();
        effects.push(CommittedEffect::Put {
            key: written_key,
            entry: written_entry,
        });
        let batch = CommittedBatch { effects };
        assert!(matches!(
            batch.effects.last(),
            Some(CommittedEffect::Put { .. })
        ));
        let sequence = persistence.next_sequence().unwrap();
        persist_and_publish_master_batch(&persistence, sequence, &batch)
            .await
            .unwrap();
        let expected = persistent_state(&store);

        persistence.binlog.flush().await.unwrap();
        drop(persistence);
        worker.await.unwrap();
        let recovered = ShardedStore::new();
        load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(persistent_state(&recovered), expected);
        for (key, _) in evicted {
            assert!(recovered.peek_entry(&key).is_none());
        }
    }

    #[test]
    fn evicted_target_recreated_with_same_value_is_replayed_as_delete_then_put() {
        let store = ShardedStore::new();
        let key = Bytes::from_static(b"target");
        store.set_value(
            key.clone(),
            OnyxValue::Blob(Bytes::from_static(b"same-value")),
            None,
        );
        let keys = vec![key.clone()];
        let before = capture_entries(&store, &keys);
        let evicted_entry = store.peek_entry(&key).unwrap();
        assert!(store.delete_bytes(&key));
        store.set_value(
            key.clone(),
            OnyxValue::Blob(Bytes::from_static(b"same-value")),
            None,
        );

        let batch = derive_committed_batch(&store, &keys, &before, &[(key.clone(), evicted_entry)])
            .unwrap();
        assert!(matches!(
            batch.effects.as_slice(),
            [CommittedEffect::Delete { .. }, CommittedEffect::Put { .. }]
        ));

        let replayed = ShardedStore::new();
        replayed.set_value(
            key.clone(),
            OnyxValue::Blob(Bytes::from_static(b"same-value")),
            None,
        );
        apply_committed_batch(&replayed, &batch);
        assert_eq!(persistent_state(&replayed), persistent_state(&store));
    }

    #[tokio::test]
    async fn periodic_sync_failure_enters_fail_stop_and_fences_the_dataset() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(4);
        let worker = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    LogMessage::SyncData { completion } => {
                        let _ = completion
                            .send(Err(StorageFailure::indeterminate("injected sync failure")));
                    }
                    LogMessage::Append { completion, .. }
                    | LogMessage::Barrier { completion }
                    | LogMessage::Flush { completion }
                    | LogMessage::Truncate { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                }
            }
        });
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        assert!(run_periodic_sync_once(&persistence).await.is_err());
        assert!(!persistence.accepting_writes.load(Ordering::SeqCst));
        assert!(persistence.is_fail_stopped());
        assert!(
            persistence
                .failure
                .lock()
                .unwrap()
                .as_deref()
                .is_some_and(|message| message.contains("injected sync failure"))
        );

        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                persistence.visibility_gate.read()
            )
            .await
            .is_err()
        );
        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn panicking_commit_finalizer_enters_fail_stop_before_releasing_visibility() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, receiver) = mpsc::channel(1);
        drop(receiver);
        let persistence = test_persistence(directory.paths.clone(), log_tx, 0);
        let boundary = persistence.acquire_commit_boundary().await;
        let guard = PersistenceCommitGuard::new(
            Arc::clone(&persistence),
            boundary,
            "Injected commit finalizer",
        );
        let finalizer = tokio::spawn(async move {
            let _guard = guard;
            panic!("injected commit finalizer panic");
            #[allow(unreachable_code)]
            Ok::<(), PersistenceError>(())
        });

        let error = await_commit_finalizer(&persistence, finalizer)
            .await
            .unwrap_err();

        assert!(error.is_indeterminate());
        assert!(persistence.is_fail_stopped());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                persistence.visibility_gate.read()
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn indeterminate_append_fail_stop_subprocess_child() {
        let Some(root) = env::var_os("ONYXDB_FAIL_STOP_TEST_DIRECTORY") else {
            return;
        };
        let paths = PersistencePaths::in_directory(Path::new(&root));
        let (log_tx, mut receiver) = mpsc::channel(4);
        let binlog_path = paths.binlog.clone();
        let worker = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    LogMessage::Append {
                        records,
                        completion,
                    } => {
                        let mut file = OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&binlog_path)
                            .unwrap();
                        for (sequence, record) in records {
                            let encoded =
                                encode_versioned_binlog_record(sequence, &record).unwrap();
                            file.write_all(&(encoded.len() as u32).to_be_bytes())
                                .unwrap();
                            file.write_all(&encoded).unwrap();
                        }
                        file.flush().unwrap();
                        file.sync_all().unwrap();
                        let _ = completion.send(Err(StorageFailure::indeterminate(
                            "injected ambiguous completion after durable write",
                        )));
                    }
                    LogMessage::Barrier { completion }
                    | LogMessage::Flush { completion }
                    | LogMessage::SyncData { completion }
                    | LogMessage::Truncate { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                }
            }
        });
        let persistence = test_persistence(paths, log_tx, 0);
        let store = Arc::new(ShardedStore::new());
        let command = vec!["SET".to_string(), "key".to_string(), "accepted".to_string()];

        let outcome = execute_ordered_command(&store, &persistence, &command).await;

        assert_eq!(outcome.mutation, MutationState::NoChange);
        assert!(matches!(outcome.response, RESPValue::Error(_)));
        assert_eq!(store.get("key"), Ok(Some("accepted".to_string())));
        let error = await_persistence_fail_stop(&persistence).await;
        assert!(error.is_indeterminate());
        drop(persistence);
        worker.await.unwrap();
        std::process::exit(86);
    }

    #[test]
    fn subprocess_fail_stop_exits_without_compaction_and_recovery_uses_durable_record() {
        let directory = TestPersistenceDirectory::new();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("tests::indeterminate_append_fail_stop_subprocess_child")
            .arg("--nocapture")
            .env("ONYXDB_FAIL_STOP_TEST_DIRECTORY", &directory.root)
            .status()
            .unwrap();

        assert_eq!(status.code(), Some(86));
        assert!(!directory.paths.snapshot.exists());
        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(state.last_sequence, 1);
        assert_eq!(recovered.get("key"), Ok(Some("accepted".to_string())));
    }

    #[test]
    fn partial_resync_rejects_ahead_and_overflowing_offsets() {
        assert!(!partial_resync_possible(101, Some(1), 100));
        assert!(!partial_resync_possible(u64::MAX, Some(1), u64::MAX - 1));
        assert!(partial_resync_possible(u64::MAX, Some(u64::MAX), u64::MAX));
    }
}
