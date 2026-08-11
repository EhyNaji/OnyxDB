mod engine;
mod protocol;
mod resp;
mod storage;
use bytes::Bytes;
use engine::{DataEntry, EntryMutation, EvictionPolicy, OnyxEngine, OnyxValue};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use protocol::{MAX_OBP_FRAME_SIZE, OBPFrame};
use resp::{CLIENT_RESP_LIMITS, RESPReadLimits, RESPValue, read_command_with_timeouts};
use std::collections::HashSet;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader as StdBufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{
    AsyncReadExt, AsyncWriteExt, BufReader as TokioBufReader, BufWriter as TokioBufWriter,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

// 1. ALLOCATORE DI MEMORIA AD ALTE PRESTAZIONI
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const SNAPSHOT_PATH: &str = "onyx.snapshot";
const BINLOG_PATH: &str = "onyx.binlog";
const REPLICA_STATE_PATH: &str = "onyx.replica";
const SNAPSHOT_MAGIC: &str = "ONYXSNAP";
const SNAPSHOT_VERSION: u8 = 2;
const REPLICA_STATE_MAGIC: &str = "ONYXREPL";
const REPLICA_STATE_VERSION: u8 = 2;
const BINLOG_RECORD_MAGIC: &[u8; 4] = b"ONX4";
const CHECKSUMLESS_BINLOG_RECORD_MAGIC: &[u8; 4] = b"ONX3";
const PREVIOUS_BINLOG_RECORD_MAGIC: &[u8; 4] = b"ONX2";
const BINLOG_CHECKSUM_SIZE: usize = std::mem::size_of::<u32>();
const BINLOG_RECORD_LENGTH_SIZE: usize = std::mem::size_of::<u32>();
const MAX_BINLOG_RECORD_SIZE: usize = 512 * 1024 * 1024 + 1024;
const MAX_SNAPSHOT_METADATA_SIZE: usize = 4096;
const MAX_SNAPSHOT_LINE_SIZE: usize = 512 * 1024 * 1024 + 1024;
const MAX_SNAPSHOT_RECORD_SIZE: usize = 512 * 1024 * 1024 + 1024;
const REPLICATION_CHUNK_SIZE: usize = 256 * 1024;
const MAX_REPLICATION_FRAME_BULK_SIZE: i64 = (REPLICATION_CHUNK_SIZE * 2 + 64) as i64;
const COMPACTION_THRESHOLD: usize = 100000;
const MAX_KEYS: usize = 1_000_000;
const CLIENT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const CLIENT_FRAME_TIMEOUT: Duration = Duration::from_secs(30);
const REPLICATION_FRAME_TIMEOUT: Duration = Duration::from_secs(30);
const REPLICATION_IDLE_TIMEOUT: Duration = Duration::from_secs(5);
const REPLICATION_TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const REPLICATION_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const REPLICA_ACK_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_TRANSACTION_COMMANDS: usize = 1024;
const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;

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

#[derive(Clone, Debug)]
struct PersistencePaths {
    snapshot: PathBuf,
    snapshot_temp: PathBuf,
    snapshot_backup: PathBuf,
    binlog: PathBuf,
    replica_state: PathBuf,
    replica_state_temp: PathBuf,
}

impl Default for PersistencePaths {
    fn default() -> Self {
        Self {
            snapshot: PathBuf::from(SNAPSHOT_PATH),
            snapshot_temp: PathBuf::from(format!("{}.tmp", SNAPSHOT_PATH)),
            snapshot_backup: PathBuf::from(format!("{}.previous", SNAPSHOT_PATH)),
            binlog: PathBuf::from(BINLOG_PATH),
            replica_state: PathBuf::from(REPLICA_STATE_PATH),
            replica_state_temp: PathBuf::from(format!("{}.tmp", REPLICA_STATE_PATH)),
        }
    }
}

#[derive(Debug)]
struct PersistenceError {
    message: String,
    upstream_unavailable: bool,
}

impl PersistenceError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            upstream_unavailable: false,
        }
    }

    fn upstream_unavailable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            upstream_unavailable: true,
        }
    }

    fn indicates_upstream_unavailable(&self) -> bool {
        self.upstream_unavailable
    }
}

impl std::fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PersistenceError {}

impl From<std::io::Error> for PersistenceError {
    fn from(error: std::io::Error) -> Self {
        Self::new(error.to_string())
    }
}
/// Utenti autorizzati (nome -> password). Nessuna granularità per comando
/// (quello sarebbe un'altra feature a parte) — qui
/// è "chi ha una password valida può fare tutto", ma con utenti multipli
/// invece di un'unica password condivisa. `--requirepass`/`ONYXDB_PASSWORD`
/// restano supportati per compatibilità: diventano l'utente "default".
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

// ============================================================
// FSYNC POLICY — quanto spesso forzare la scrittura fisica su disco del
// binlog.
// - Always:    fsync dopo ogni batch di scritture. Massima durabilità,
//              più latenza per comando.
// - EverySec:  fsync una volta al secondo in background (default, come Redis).
//              Nel peggiore dei casi si perde fino a ~1s di scritture se il
//              sistema operativo/hardware crasha (non il solo processo).
// - No:        nessun fsync esplicito, solo il flush dei buffer userspace.
//              Più veloce, ma affidato completamente al SO per la durabilità.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FsyncPolicy {
    Always,
    EverySec,
    No,
}

impl FsyncPolicy {
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "always" => Some(FsyncPolicy::Always),
            "everysec" => Some(FsyncPolicy::EverySec),
            "no" => Some(FsyncPolicy::No),
            _ => None,
        }
    }
}

static FSYNC_POLICY: std::sync::OnceLock<FsyncPolicy> = std::sync::OnceLock::new();

fn fsync_policy() -> FsyncPolicy {
    *FSYNC_POLICY.get().unwrap_or(&FsyncPolicy::EverySec)
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
/// Parses memory sizes such as `100mb`, `1gb`, `500kb`, or a raw byte count.
fn parse_memory_size(s: &str) -> Option<usize> {
    let s = s.trim().to_ascii_lowercase();
    let (number_part, multiplier) = if let Some(n) = s.strip_suffix("gb") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("kb") {
        (n, 1024)
    } else if let Some(n) = s.strip_suffix('b') {
        (n, 1)
    } else {
        (s.as_str(), 1)
    };
    number_part
        .trim()
        .parse::<usize>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
}

// 2. FAST TIME (Orologio di Sistema Cachato)
static CURRENT_TIME: AtomicU64 = AtomicU64::new(0);
static START_TIME: AtomicU64 = AtomicU64::new(0);
static IS_REPLICA: AtomicBool = AtomicBool::new(false);
// Replication ID: generato una volta ad ogni avvio del Master (casuale,
// derivato dal tempo di avvio + PID). Serve a distinguere "sono lo stesso
// processo Master di prima" da "sono ripartito da zero" — senza questo,
// una Replica che si riconnette con un vecchio offset dopo un riavvio del
// Master rischia di credersi "già allineata" quando in realtà il nuovo
// processo non ha alcuna memoria di quell'offset. 0 è riservato per
// "replication ID sconosciuto" lato Replica (prima connessione mai fatta).
static REPL_ID: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

fn repl_id() -> u64 {
    *REPL_ID.get().unwrap_or(&0)
}
static ACTIVE_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);
static TOTAL_COMMANDS: AtomicUsize = AtomicUsize::new(0);
static CACHE_HITS: AtomicUsize = AtomicUsize::new(0);
static CACHE_MISSES: AtomicUsize = AtomicUsize::new(0);
fn now() -> u64 {
    CURRENT_TIME.load(Ordering::Relaxed)
}

pub struct ShardedStore {
    engine: OnyxEngine,
    maxmemory_bytes: usize,
    maxmemory_policy: EvictionPolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreError {
    WrongType,
}

impl StoreError {
    fn message(self) -> &'static str {
        match self {
            Self::WrongType => "WRONGTYPE Operation against a key holding the wrong kind of value",
        }
    }
}

impl Default for ShardedStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedStore {
    pub fn new() -> Self {
        Self::with_maxmemory(0, EvictionPolicy::NoEviction)
    }

    pub fn with_maxmemory(maxmemory_bytes: usize, maxmemory_policy: EvictionPolicy) -> Self {
        Self {
            engine: OnyxEngine::new(),
            maxmemory_bytes,
            maxmemory_policy,
        }
    }

    // --- String operations ---
    pub fn set(&self, key: String, value: String) {
        self.engine
            .set(Bytes::from(key), OnyxValue::Blob(Bytes::from(value)), None);
    }

    pub fn set_raw(&self, key: String, entry: DataEntry) {
        self.engine
            .set(Bytes::from(key), entry.value, entry.expires_at);
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, StoreError> {
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), |entry| match &entry.value {
                OnyxValue::Blob(b) => Ok(String::from_utf8_lossy(b).to_string()),
                OnyxValue::Int(n) => Ok(n.to_string()),
                _ => Err(StoreError::WrongType),
            });
        result.transpose()
    }

    pub fn get_raw(&self, key: &str) -> Option<DataEntry> {
        self.engine.get(&Bytes::from(key.to_string()))
    }

    pub fn delete(&self, key: &str) -> bool {
        self.engine.delete(&Bytes::from(key.to_string()))
    }

    pub fn exists(&self, key: &str) -> bool {
        self.engine.peek(&Bytes::from(key.to_string())).is_some()
    }

    pub fn expire_at(&self, key: &str, timestamp: u64) -> bool {
        // Un solo lock: imposta solo la scadenza, senza clonare il valore
        // (prima: get() dell'intera entry + set() di rimpiazzo).
        self.engine
            .set_expiry(&Bytes::from(key.to_string()), timestamp)
    }

    pub fn expire(&self, key: &str, seconds: u64) -> bool {
        self.expire_at(key, now().saturating_add(seconds))
    }

    pub fn ttl(&self, key: &str) -> i64 {
        self.engine
            .read(&Bytes::from(key.to_string()), |entry| {
                if let Some(exp) = entry.expires_at {
                    let remaining = exp.saturating_sub(now());
                    if remaining == 0 {
                        -2
                    } else {
                        i64::try_from(remaining).unwrap_or(i64::MAX)
                    }
                } else {
                    -1
                }
            })
            .unwrap_or(-2)
    }

    pub fn incr(&self, key: &str) -> Result<i64, &'static str> {
        self.incrby(key, 1)
    }

    pub fn incrby(&self, key: &str, delta: i64) -> Result<i64, &'static str> {
        // Read, overflow validation, and mutation occur under one engine lock,
        // so concurrent increments cannot overwrite each other.
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::Int(0),
            move |v| {
                let current = match v {
                    OnyxValue::Int(n) => *n,
                    OnyxValue::Blob(b) => std::str::from_utf8(b)
                        .ok()
                        .and_then(|value| value.parse::<i64>().ok())
                        .ok_or("ERR value is not an integer")?,
                    _ => return Err("ERR value is not an integer"),
                };
                let new_val = current
                    .checked_add(delta)
                    .ok_or("ERR increment or decrement would overflow")?;
                *v = OnyxValue::Int(new_val);
                Ok(new_val)
            },
        )
    }

    pub fn append(&self, key: &str, suffix: &str) -> Result<usize, StoreError> {
        let suffix_owned = suffix.to_string();
        // Stesso discorso di incrby: un solo lock, niente APPEND persi sotto
        // concorrenza.
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::Blob(Bytes::new()),
            move |v| {
                let mut s = match v {
                    OnyxValue::Blob(b) => String::from_utf8_lossy(b).to_string(),
                    OnyxValue::Int(value) => value.to_string(),
                    _ => return Err(StoreError::WrongType),
                };
                s.push_str(&suffix_owned);
                let len = s.len();
                *v = OnyxValue::Blob(Bytes::from(s));
                Ok(len)
            },
        )
    }

    pub fn strlen(&self, key: &str) -> Result<usize, StoreError> {
        self.get(key)
            .map(|value| value.map_or(0, |value| value.len()))
    }

    pub fn getset(&self, key: &str, new_value: &str) -> Result<Option<String>, StoreError> {
        let new_value_owned = new_value.to_string();
        self.engine.update_entry_or_insert_with_presence(
            Bytes::from(key.to_string()),
            || OnyxValue::Blob(Bytes::new()),
            move |entry, existed| {
                let old = match &entry.value {
                    OnyxValue::Blob(b) => String::from_utf8_lossy(b).to_string(),
                    OnyxValue::Int(n) => n.to_string(),
                    _ => return Err(StoreError::WrongType),
                };
                entry.value = OnyxValue::Blob(Bytes::from(new_value_owned));
                entry.expires_at = None;
                Ok(existed.then_some(old))
            },
        )
    }

    pub fn setnx(&self, key: &str, value: &str) -> bool {
        //prima era exists()+set(), con una
        // finestra in cui due SETNX concorrenti potevano credere entrambi
        // di aver "vinto" — il che vanificava il suo uso come lock.
        self.engine.set_if_absent(
            Bytes::from(key.to_string()),
            OnyxValue::Blob(Bytes::from(value.to_string())),
        )
    }

    // --- List operations ---
    pub fn lpush(&self, key: &str, item: String) -> Result<usize, StoreError> {
        let item_b = Bytes::from(item);
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::List(Vec::new()),
            move |v| match v {
                OnyxValue::List(l) => {
                    l.insert(0, item_b);
                    Ok(l.len())
                }
                _ => Err(StoreError::WrongType),
            },
        )
    }

    pub fn rpush(&self, key: &str, item: String) -> Result<usize, StoreError> {
        let item_b = Bytes::from(item);
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::List(Vec::new()),
            move |v| match v {
                OnyxValue::List(l) => {
                    l.push(item_b);
                    Ok(l.len())
                }
                _ => Err(StoreError::WrongType),
            },
        )
    }

    pub fn lpop(&self, key: &str) -> Result<Option<String>, StoreError> {
        let key_b = Bytes::from(key.to_string());
        let result = self
            .engine
            .update_if_exists_with_action(&key_b, |v| match v {
                OnyxValue::List(l) if !l.is_empty() => {
                    let item = l.remove(0);
                    let result = Ok(Some(String::from_utf8_lossy(&item).to_string()));
                    if l.is_empty() {
                        EntryMutation::Delete(result)
                    } else {
                        EntryMutation::Keep(result)
                    }
                }
                OnyxValue::List(_) => EntryMutation::Keep(Ok(None)),
                _ => EntryMutation::Keep(Err(StoreError::WrongType)),
            });
        result.unwrap_or(Ok(None))
    }

    pub fn rpop(&self, key: &str) -> Result<Option<String>, StoreError> {
        let key_b = Bytes::from(key.to_string());
        let result = self
            .engine
            .update_if_exists_with_action(&key_b, |v| match v {
                OnyxValue::List(l) if !l.is_empty() => {
                    let item = l.pop().unwrap();
                    let result = Ok(Some(String::from_utf8_lossy(&item).to_string()));
                    if l.is_empty() {
                        EntryMutation::Delete(result)
                    } else {
                        EntryMutation::Keep(result)
                    }
                }
                OnyxValue::List(_) => EntryMutation::Keep(Ok(None)),
                _ => EntryMutation::Keep(Err(StoreError::WrongType)),
            });
        result.unwrap_or(Ok(None))
    }

    /// LRANGE con start/stop stile Redis: indici 0-based inclusivi su
    /// entrambi gli estremi, indici negativi contano dalla fine (-1 =
    /// ultimo elemento), fuori range vengono "clampati" invece di dare
    /// errore. `LRANGE chiave` (senza indici, dal vecchio comportamento)
    /// continua a funzionare passando start=0, stop=-1 dal chiamante.
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, StoreError> {
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), |entry| match &entry.value {
                OnyxValue::List(l) => {
                    let len = l.len() as i64;
                    if len == 0 {
                        return Ok(Vec::new());
                    }
                    let norm = |idx: i64| -> i64 { if idx < 0 { (len + idx).max(0) } else { idx } };
                    let s = norm(start);
                    let mut e = norm(stop);
                    if s > len - 1 || s > e {
                        return Ok(Vec::new());
                    }
                    if e > len - 1 {
                        e = len - 1;
                    }
                    Ok(l[s as usize..=e as usize]
                        .iter()
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .collect())
                }
                _ => Err(StoreError::WrongType),
            });
        result.unwrap_or(Ok(Vec::new()))
    }

    pub fn llen(&self, key: &str) -> Result<usize, StoreError> {
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), |entry| match &entry.value {
                OnyxValue::List(l) => Ok(l.len()),
                _ => Err(StoreError::WrongType),
            });
        result.unwrap_or(Ok(0))
    }

    // --- Hash operations ---
    pub fn hset(&self, key: &str, field: &str, value: &str) -> Result<bool, StoreError> {
        let field_b = Bytes::from(field.to_string());
        let value_b = Bytes::from(value.to_string());
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::Hash(std::collections::HashMap::new()),
            move |v| match v {
                OnyxValue::Hash(h) => Ok(h.insert(field_b, value_b).is_none()),
                _ => Err(StoreError::WrongType),
            },
        )
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<Option<String>, StoreError> {
        let field_b = Bytes::from(field.to_string());
        self.engine
            .read(&Bytes::from(key.to_string()), move |entry| {
                match &entry.value {
                    OnyxValue::Hash(h) => Ok(h
                        .get(&field_b)
                        .map(|b| String::from_utf8_lossy(b).to_string())),
                    _ => Err(StoreError::WrongType),
                }
            })
            .unwrap_or(Ok(None))
    }

    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, String)>, StoreError> {
        self.engine
            .read(&Bytes::from(key.to_string()), |entry| match &entry.value {
                OnyxValue::Hash(h) => Ok(h
                    .iter()
                    .map(|(k, v)| {
                        (
                            String::from_utf8_lossy(k).to_string(),
                            String::from_utf8_lossy(v).to_string(),
                        )
                    })
                    .collect()),
                _ => Err(StoreError::WrongType),
            })
            .unwrap_or(Ok(Vec::new()))
    }

    pub fn hdel(&self, key: &str, field: &str) -> Result<bool, StoreError> {
        let field_b = Bytes::from(field.to_string());
        let key_b = Bytes::from(key.to_string());
        let result = self
            .engine
            .update_if_exists_with_action(&key_b, move |v| match v {
                OnyxValue::Hash(h) => {
                    let removed = h.remove(&field_b).is_some();
                    let result = Ok(removed);
                    if removed && h.is_empty() {
                        EntryMutation::Delete(result)
                    } else {
                        EntryMutation::Keep(result)
                    }
                }
                _ => EntryMutation::Keep(Err(StoreError::WrongType)),
            });
        result.unwrap_or(Ok(false))
    }

    pub fn hkeys(&self, key: &str) -> Result<Vec<String>, StoreError> {
        self.hgetall(key)
            .map(|hash| hash.into_iter().map(|(field, _)| field).collect())
    }

    pub fn hvals(&self, key: &str) -> Result<Vec<String>, StoreError> {
        self.hgetall(key)
            .map(|hash| hash.into_iter().map(|(_, value)| value).collect())
    }

    // --- Set operations ---
    pub fn sadd(&self, key: &str, member: &str) -> Result<bool, StoreError> {
        let member_b = Bytes::from(member.to_string());
        self.engine.update_or_insert(
            Bytes::from(key.to_string()),
            || OnyxValue::Set(std::collections::HashSet::new()),
            move |v| match v {
                OnyxValue::Set(s) => Ok(s.insert(member_b)),
                _ => Err(StoreError::WrongType),
            },
        )
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<String>, StoreError> {
        self.engine
            .read(&Bytes::from(key.to_string()), |entry| match &entry.value {
                OnyxValue::Set(s) => Ok(s
                    .iter()
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .collect()),
                _ => Err(StoreError::WrongType),
            })
            .unwrap_or(Ok(Vec::new()))
    }

    pub fn srem(&self, key: &str, member: &str) -> Result<bool, StoreError> {
        let member_b = Bytes::from(member.to_string());
        let key_b = Bytes::from(key.to_string());
        let result = self
            .engine
            .update_if_exists_with_action(&key_b, move |v| match v {
                OnyxValue::Set(s) => {
                    let removed = s.remove(&member_b);
                    let result = Ok(removed);
                    if removed && s.is_empty() {
                        EntryMutation::Delete(result)
                    } else {
                        EntryMutation::Keep(result)
                    }
                }
                _ => EntryMutation::Keep(Err(StoreError::WrongType)),
            });
        result.unwrap_or(Ok(false))
    }

    pub fn sismember(&self, key: &str, member: &str) -> Result<bool, StoreError> {
        let member_b = Bytes::from(member.to_string());
        self.engine
            .read(&Bytes::from(key.to_string()), move |entry| {
                match &entry.value {
                    OnyxValue::Set(s) => Ok(s.contains(&member_b)),
                    _ => Err(StoreError::WrongType),
                }
            })
            .unwrap_or(Ok(false))
    }
    // --- JSON operations ---

    /// JSON.SET: se path == "$", sostituisce l'intero documento (creandolo
    /// se la chiave non esiste). Con un path parziale, la chiave deve già
    /// esistere e contenere un valore JSON.
    pub fn json_set(
        &self,
        key: &str,
        path: &str,
        new_value: serde_json::Value,
    ) -> Result<(), &'static str> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let key_b = Bytes::from(key.to_string());

        if segments.is_empty() {
            return self.engine.update_or_insert_with_presence(
                key_b,
                || OnyxValue::Json(serde_json::Value::Null),
                move |value, _| match value {
                    OnyxValue::Json(root) => {
                        *root = new_value;
                        Ok(())
                    }
                    _ => Err(StoreError::WrongType.message()),
                },
            );
        }

        // Path parziale: la chiave deve già esistere con un valore JSON.
        let result = self.engine.update_if_exists(&key_b, move |v| match v {
            OnyxValue::Json(root) => Some(set_json_path(root, &segments, new_value)),
            _ => None, // esiste ma non è JSON: tipo sbagliato
        });
        match result {
            Some(Some(true)) => Ok(()),
            Some(Some(false)) => {
                Err("ERR path not reachable (intermediate element missing or index out of bounds)")
            }
            Some(None) => Err(StoreError::WrongType.message()),
            None => Err("ERR key does not exist: use JSON.SET key $ {...} to create it"),
        }
    }

    pub fn json_get(&self, key: &str, path: &str) -> Result<Option<String>, &'static str> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), move |entry| {
                match &entry.value {
                    OnyxValue::Json(root) => Ok(if segments.is_empty() {
                        Some(root.to_string())
                    } else {
                        get_json_path(root, &segments).map(|v| v.to_string())
                    }),
                    _ => Err(StoreError::WrongType.message()),
                }
            });
        match result {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }

    pub fn json_del(&self, key: &str, path: &str) -> Result<bool, &'static str> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        if segments.is_empty() {
            let result =
                self.engine
                    .update_if_exists_with_action(
                        &Bytes::from(key.to_string()),
                        |value| match value {
                            OnyxValue::Json(_) => EntryMutation::Delete(Ok(true)),
                            _ => EntryMutation::Keep(Err(StoreError::WrongType.message())),
                        },
                    );
            return result.unwrap_or(Ok(false));
        }
        let key_b = Bytes::from(key.to_string());
        let result = self.engine.update_if_exists(&key_b, move |v| match v {
            OnyxValue::Json(root) => Some(delete_json_path(root, &segments)),
            _ => None,
        });
        match result {
            Some(Some(deleted)) => Ok(deleted),
            Some(None) => Err(StoreError::WrongType.message()),
            None => Ok(false), // chiave inesistente: nulla da cancellare
        }
    }

    pub fn json_type(&self, key: &str, path: &str) -> Result<Option<&'static str>, &'static str> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), move |entry| {
                match &entry.value {
                    OnyxValue::Json(root) => {
                        let node = if segments.is_empty() {
                            Some(root)
                        } else {
                            get_json_path(root, &segments)
                        };
                        Ok(node.map(|v| match v {
                            serde_json::Value::Null => "null",
                            serde_json::Value::Bool(_) => "boolean",
                            serde_json::Value::Number(_) => "number",
                            serde_json::Value::String(_) => "string",
                            serde_json::Value::Array(_) => "array",
                            serde_json::Value::Object(_) => "object",
                        }))
                    }
                    _ => Err(StoreError::WrongType.message()),
                }
            });
        match result {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }
    pub fn json_numincrby(&self, key: &str, path: &str, delta: f64) -> Result<f64, String> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let key_b = Bytes::from(key.to_string());
        let result = self.engine.update_if_exists(&key_b, move |v| match v {
            OnyxValue::Json(root) => Some(numincrby_json_path(root, &segments, delta)),
            _ => None,
        });
        match result {
            Some(Some(Ok(new_val))) => Ok(new_val),
            Some(Some(Err(e))) => Err(e.to_string()),
            Some(None) => Err(StoreError::WrongType.message().to_string()),
            None => {
                Err("ERR key does not exist: use JSON.SET key $ {...} to create it".to_string())
            }
        }
    }

    pub fn json_arrappend(
        &self,
        key: &str,
        path: &str,
        new_value: serde_json::Value,
    ) -> Result<usize, String> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let key_b = Bytes::from(key.to_string());
        let result = self.engine.update_if_exists(&key_b, move |v| match v {
            OnyxValue::Json(root) => Some(arrappend_json_path(root, &segments, new_value)),
            _ => None,
        });
        match result {
            Some(Some(Ok(new_len))) => Ok(new_len),
            Some(Some(Err(e))) => Err(e.to_string()),
            Some(None) => Err(StoreError::WrongType.message().to_string()),
            None => {
                Err("ERR key does not exist: use JSON.SET key $ {...} to create it".to_string())
            }
        }
    }
    pub fn json_arrlen(&self, key: &str, path: &str) -> Result<Option<usize>, String> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), move |entry| {
                match &entry.value {
                    OnyxValue::Json(root) => {
                        let node = if segments.is_empty() {
                            Some(root)
                        } else {
                            get_json_path(root, &segments)
                        };
                        Ok(node.and_then(|v| v.as_array().map(|a| a.len())))
                    }
                    _ => Err(StoreError::WrongType.message().to_string()),
                }
            });
        match result {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }

    pub fn json_objkeys(&self, key: &str, path: &str) -> Result<Option<Vec<String>>, String> {
        let segments = parse_json_path(path).ok_or("ERR invalid JSON path")?;
        let result = self
            .engine
            .read(&Bytes::from(key.to_string()), move |entry| {
                match &entry.value {
                    OnyxValue::Json(root) => {
                        let node = if segments.is_empty() {
                            Some(root)
                        } else {
                            get_json_path(root, &segments)
                        };
                        Ok(node.and_then(|v| v.as_object().map(|o| o.keys().cloned().collect())))
                    }
                    _ => Err(StoreError::WrongType.message().to_string()),
                }
            });
        match result {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }
    // --- Key operations ---
    pub fn rename(&self, old_key: &str, new_key: &str) -> bool {
        self.engine.rename(
            &Bytes::from(old_key.to_string()),
            Bytes::from(new_key.to_string()),
        )
    }

    pub fn copy(&self, src: &str, dst: &str) -> bool {
        self.engine
            .copy(&Bytes::from(src.to_string()), Bytes::from(dst.to_string()))
    }

    pub fn value_type(&self, key: &str) -> Option<&'static str> {
        self.engine
            .read(&Bytes::from(key.to_string()), |entry| match &entry.value {
                OnyxValue::Blob(_) => "string",
                OnyxValue::Int(_) => "int",
                OnyxValue::Float(_) => "float",
                OnyxValue::List(_) => "list",
                OnyxValue::Hash(_) => "hash",
                OnyxValue::Set(_) => "set",
                OnyxValue::Json(_) => "json",
                OnyxValue::Vector(_) => "vector",
            })
    }

    pub fn all_keys(&self) -> Vec<String> {
        self.engine
            .all_keys()
            .into_iter()
            .map(|k| String::from_utf8_lossy(&k).to_string())
            .collect()
    }

    pub fn keys_matching(&self, pattern: &str) -> Vec<String> {
        if pattern == "*" || pattern.is_empty() {
            return self.all_keys();
        }
        self.all_keys()
            .into_iter()
            .filter(|k| glob_match(pattern, k))
            .collect()
    }

    pub fn snapshot_entries(&self) -> Vec<(String, DataEntry)> {
        self.engine
            .snapshot_all()
            .into_iter()
            .map(|(k, entry)| (String::from_utf8_lossy(&k).to_string(), entry))
            .collect()
    }

    pub fn used_memory_bytes(&self) -> usize {
        self.engine.total_memory_bytes()
    }

    pub fn maxmemory_bytes(&self) -> usize {
        self.maxmemory_bytes
    }

    pub fn maxmemory_policy(&self) -> EvictionPolicy {
        self.maxmemory_policy
    }

    pub fn expire_conditional(&self, key: &str, seconds: u64, condition: &str) -> bool {
        let require_expiry = match condition {
            "NX" => Some(false),
            "XX" => Some(true),
            _ => return false,
        };
        self.engine.set_expiry_conditional(
            &Bytes::from(key.to_string()),
            now().saturating_add(seconds),
            require_expiry,
        )
    }

    pub fn get_expiry(&self, key: &str) -> Option<u64> {
        self.engine
            .read(&Bytes::from(key.to_string()), |e| e.expires_at)
            .flatten()
    }

    pub fn stats(&self) -> engine::EngineStats {
        self.engine.stats()
    }

    pub fn gc_expired(&self) -> usize {
        self.engine.gc_expired()
    }
    pub fn engine(&self) -> &OnyxEngine {
        &self.engine
    }
}
fn is_expired(entry: &DataEntry) -> bool {
    if let Some(exp) = entry.expires_at {
        now() >= exp
    } else {
        false
    }
}

// ============================================================
// JSON PATH — parser ridotto: solo campi (.nome) e indici di array ([N]).
// Niente wildcard e niente filtri.
// ============================================================

#[derive(Debug, Clone, PartialEq)]
enum JsonPathSegment {
    Field(String),
    Index(usize),
}
/// Interpreta un path stile "$.a.b[2].c" in una sequenza di passi da
/// seguire dentro un serde_json::Value. "$" da solo (documento intero)
/// ritorna un vettore vuoto. Ritorna None se il path è sintatticamente
/// malformato (non se il path "non esiste" nei dati. Quello viene scoperto
/// solo in get_json_path/set_json_path).
fn parse_json_path(path: &str) -> Option<Vec<JsonPathSegment>> {
    let path = path.trim();
    if path != "$" && !path.starts_with('$') {
        return None; // ogni path valido inizia con '$'
    }
    let rest = &path[1..]; // scarta il '$' iniziale
    if rest.is_empty() {
        return Some(Vec::new()); // "$" da solo: documento intero
    }
    if !rest.starts_with('.') && !rest.starts_with('[') {
        return None; // dopo '$' deve seguire '.' o '['
    }

    let mut segments = Vec::new();
    let chars: Vec<char> = rest.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '.' => {
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '.' && chars[i] != '[' {
                    i += 1;
                }
                if start == i {
                    return None; // ".." o "." finale senza nome di campo
                }
                let field: String = chars[start..i].iter().collect();
                segments.push(JsonPathSegment::Field(field));
            }
            '[' => {
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != ']' {
                    i += 1;
                }
                if i >= chars.len() {
                    return None; // '[' senza ']' di chiusura
                }
                let idx_str: String = chars[start..i].iter().collect();
                let idx: usize = idx_str.parse().ok()?; // solo indici >= 0
                segments.push(JsonPathSegment::Index(idx));
                i += 1; // salta il ']'
            }
            _ => return None, // carattere inatteso fuori da un segmento . o [
        }
    }

    Some(segments)
}
/// Naviga `root` seguendo `segments` e ritorna un riferimento al nodo
/// finale, se esiste. None se un passo intermedio non esiste o non è del
/// tipo giusto.
fn get_json_path<'a>(
    root: &'a serde_json::Value,
    segments: &[JsonPathSegment],
) -> Option<&'a serde_json::Value> {
    let mut current = root;
    for seg in segments {
        current = match (seg, current) {
            (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => map.get(f)?,
            (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => arr.get(*idx)?,
            _ => return None,
        };
    }
    Some(current)
}

/// Imposta il valore al path indicato, creando l'ultimo passo se manca ma
/// SENZA creare automaticamente livelli intermedi assenti. Ritorna true se ha scritto, false se il genitore del
/// passo finale non esiste o non è del tipo compatibile.
fn set_json_path(
    root: &mut serde_json::Value,
    segments: &[JsonPathSegment],
    new_value: serde_json::Value,
) -> bool {
    if segments.is_empty() {
        *root = new_value;
        return true;
    }
    let mut current = root;
    for seg in &segments[..segments.len() - 1] {
        current = match (seg, current) {
            (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => {
                match map.get_mut(f) {
                    Some(v) => v,
                    None => return false, // livello intermedio assente: niente auto-creazione
                }
            }
            (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => {
                match arr.get_mut(*idx) {
                    Some(v) => v,
                    None => return false,
                }
            }
            _ => return false,
        };
    }
    match (&segments[segments.len() - 1], current) {
        (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => {
            map.insert(f.clone(), new_value);
            true
        }
        (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => {
            if *idx < arr.len() {
                arr[*idx] = new_value;
                true
            } else if *idx == arr.len() {
                arr.push(new_value); // append in coda, come farebbe un push naturale
                true
            } else {
                false // indice troppo avanti, buco non ammesso
            }
        }
        _ => false,
    }
}

/// Rimuove il nodo al path indicato. Ritorna true se ha rimosso qualcosa.
fn delete_json_path(root: &mut serde_json::Value, segments: &[JsonPathSegment]) -> bool {
    if segments.is_empty() {
        return false; // DEL sul documento intero non passa da qui (si usa DEL normale sulla chiave)
    }
    let mut current = root;
    for seg in &segments[..segments.len() - 1] {
        current = match (seg, current) {
            (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => match map.get_mut(f) {
                Some(v) => v,
                None => return false,
            },
            (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => {
                match arr.get_mut(*idx) {
                    Some(v) => v,
                    None => return false,
                }
            }
            _ => return false,
        };
    }
    match (&segments[segments.len() - 1], current) {
        (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => map.remove(f).is_some(),
        (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) if *idx < arr.len() => {
            arr.remove(*idx);
            true
        }
        _ => false,
    }
}
/// Naviga fino al nodo indicato e ritorna un riferimento MUTABILE (a
/// differenza di get_json_path che ritorna solo &). Serve per
/// NUMINCRBY/ARRAPPEND, che modificano il nodo in-place invece di
/// sostituirlo interamente come fa SET.
fn get_json_path_mut<'a>(
    root: &'a mut serde_json::Value,
    segments: &[JsonPathSegment],
) -> Option<&'a mut serde_json::Value> {
    let mut current = root;
    for seg in segments {
        current = match (seg, current) {
            (JsonPathSegment::Field(f), serde_json::Value::Object(map)) => map.get_mut(f)?,
            (JsonPathSegment::Index(idx), serde_json::Value::Array(arr)) => arr.get_mut(*idx)?,
            _ => return None,
        };
    }
    Some(current)
}

/// Incrementa un numero al path indicato. Niente auto-creazione a 0 se il
/// path non esiste (comportamento diverso da INCR su chiave stringa): dentro
/// un documento JSON un path assente è più probabilmente un errore di
/// battitura da segnalare che un "parti da zero" implicito.
fn numincrby_json_path(
    root: &mut serde_json::Value,
    segments: &[JsonPathSegment],
    delta: f64,
) -> Result<f64, &'static str> {
    let node = get_json_path_mut(root, segments).ok_or("ERR path JSON not found")?;
    let current = node
        .as_f64()
        .ok_or("WRONGTYPE the value at the path is not a number")?;
    let new_val = current + delta;
    let new_number = if new_val.fract() == 0.0 && new_val.abs() < i64::MAX as f64 {
        serde_json::Number::from(new_val as i64)
    } else {
        serde_json::Number::from_f64(new_val)
            .ok_or("ERR invalid numeric result (NaN or infinity)")?
    };
    *node = serde_json::Value::Number(new_number);
    Ok(new_val)
}

/// Aggiunge un elemento in coda all'array al path indicato. Errore se il
/// path non esiste o non punta a un array.
fn arrappend_json_path(
    root: &mut serde_json::Value,
    segments: &[JsonPathSegment],
    new_value: serde_json::Value,
) -> Result<usize, &'static str> {
    let node = get_json_path_mut(root, segments).ok_or("ERR path JSON not found")?;
    match node {
        serde_json::Value::Array(arr) => {
            arr.push(new_value);
            Ok(arr.len())
        }
        _ => Err("WRONGTYPE the value at the path is not an array"),
    }
}
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut p_idx = 0;
    let mut t_idx = 0;
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let mut star_idx = None;
    let mut match_idx = 0;

    while t_idx < t.len() {
        if p_idx < p.len() && p[p_idx] == t[t_idx] {
            // Match letterale: avanza entrambi
            p_idx += 1;
            t_idx += 1;
        } else if p_idx < p.len() && p[p_idx] == '*' {
            // Il '*' e' un punto di backtrack: prova prima a matchare zero
            // caratteri (lo si espandera' un carattere alla volta solo se
            // serve, nel ramo sotto).
            star_idx = Some(p_idx);
            match_idx = t_idx;
            p_idx += 1;
        } else if let Some(star) = star_idx {
            p_idx = star + 1;
            match_idx += 1;
            t_idx = match_idx;
        } else {
            return false;
        }
    }

    while p_idx < p.len() && p[p_idx] == '*' {
        p_idx += 1;
    }
    p_idx == p.len()
}

/// Quante voci tiene al massimo il backlog di replica (in numero di
/// comandi, (non byte) — semplificazione voluta:
/// più facile da ragionare Replica staccata a lungo
/// perde "N comandi" invece di "N byte", è comunque un buon
/// proxy dello stesso concetto).
const BACKLOG_CAPACITY: usize = 10_000;

enum LogMessage {
    Append {
        sequence: u64,
        record: Vec<u8>,
        completion: oneshot::Sender<Result<(), String>>,
    },
    Flush {
        completion: oneshot::Sender<Result<(), String>>,
    },
    SyncData {
        completion: oneshot::Sender<Result<(), String>>,
    },
    Truncate {
        completion: oneshot::Sender<Result<(), String>>,
    },
}

/// Stato di una Replica connessa, per il monitoraggio del lag.
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
    log_tx: mpsc::Sender<LogMessage>,
    write_count: AtomicUsize,
    compaction_pending: AtomicBool,
    accepting_writes: AtomicBool,
    /// Readers take a shared guard while commands observe state. Mutations,
    /// replicated batches, and full-sync installation take an exclusive guard
    /// so no client can observe a partially committed state transition. Code
    /// that needs both gates must acquire write_gate before visibility_gate.
    visibility_gate: tokio::sync::RwLock<()>,
    write_gate: tokio::sync::Mutex<()>,
    paths: PersistencePaths,
    // Canale broadcast: ogni comando di scrittura viene trasmesso a tutte le
    // Replica connesse in tempo reale (in aggiunta al log su disco), taggato
    // con l'offset di replicazione a cui corrisponde.
    replica_tx: tokio::sync::broadcast::Sender<(u64, CommittedBatch)>,
    // Se true, questa istanza smette di comportarsi da Replica e diventa
    // un Master indipendente (promozione manuale via REPLICAOF NO ONE).
    promote_to_master: Arc<AtomicBool>,
    // Offset di replicazione: cresce di 1 a ogni comando di scrittura
    // replicato. Non è byte-accurato ma basta per capire
    // "quanti comandi indietro" è una Replica.
    repl_offset: AtomicU64,
    // Buffer circolare degli ultimi BACKLOG_CAPACITY comandi, con il loro
    // offset. Usato per il resync parziale: una Replica che si riconnette
    // con un offset ancora presente qui riceve solo i comandi mancanti,
    // invece di un dump completo.
    backlog: std::sync::Mutex<std::collections::VecDeque<(u64, CommittedBatch)>>,
    next_replica_id: AtomicU64,
    replica_status: std::sync::Mutex<std::collections::HashMap<u64, ReplicaStatus>>,
    // Pub/Sub: un unico canale broadcast per tutti i canali applicativi
    // (channel_name, payload) — ogni subscriber filtra da solo i messaggi
    // dei canali a cui è iscritto, invece di avere un canale broadcast
    // dedicato per ogni nome di canale (che andrebbe creato/distrutto
    // dinamicamente, più complicato da gestire in modo sicuro).
    pubsub_tx: tokio::sync::broadcast::Sender<(String, String)>,
    next_subscriber_id: AtomicU64,
    // canale -> insieme di id di subscriber iscritti, usato solo per dare
    // a PUBLISH il numero di destinatari (Redis fa lo stesso).
    subscriptions:
        std::sync::Mutex<std::collections::HashMap<String, std::collections::HashSet<u64>>>,
    failure: std::sync::Mutex<Option<String>>,
    upstream_replid: AtomicU64,
    replication_ready: AtomicBool,
    replica_lifecycle: Arc<ReplicaLifecycle>,
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

#[derive(Clone, Debug, PartialEq)]
struct PersistentEntry {
    value: OnyxValue,
    expires_at: Option<u64>,
}

impl From<DataEntry> for PersistentEntry {
    fn from(entry: DataEntry) -> Self {
        Self {
            value: entry.value,
            expires_at: entry.expires_at,
        }
    }
}

impl PersistentEntry {
    fn into_data_entry(self) -> DataEntry {
        let timestamp = now();
        DataEntry {
            value: self.value,
            expires_at: self.expires_at,
            created_at: timestamp,
            last_accessed: timestamp,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum CommittedEffect {
    Put { key: Bytes, entry: PersistentEntry },
    Delete { key: Bytes },
}

#[derive(Clone, Debug, PartialEq)]
struct CommittedBatch {
    effects: Vec<CommittedEffect>,
}

impl CommittedBatch {
    fn new(effects: Vec<CommittedEffect>) -> Result<Self, PersistenceError> {
        if effects.is_empty() {
            return Err(PersistenceError::new(
                "A committed mutation batch cannot be empty",
            ));
        }
        Ok(Self { effects })
    }
}

// ============================================================
// LOG BINARIO - Formato compatto per operazioni di scrittura
// ============================================================
#[cfg(test)]
const OP_SET: u8 = 1;
#[cfg(test)]
const OP_DEL: u8 = 2;
#[cfg(test)]
const OP_EXPIRE: u8 = 3;
#[cfg(test)]
const OP_L_PUSH: u8 = 4;
#[cfg(test)]
const OP_HSET: u8 = 5;
#[cfg(test)]
const OP_SADD: u8 = 6;
#[cfg(test)]
const OP_RENAME: u8 = 7;
#[cfg(test)]
const OP_INCR: u8 = 8;
#[cfg(test)]
const OP_DECR: u8 = 9;
#[cfg(test)]
const OP_APPEND: u8 = 10;
#[cfg(test)]
const OP_HDEL: u8 = 11;
#[cfg(test)]
const OP_SREM: u8 = 12;
#[cfg(test)]
const OP_COPY: u8 = 13;
#[cfg(test)]
const OP_MSET: u8 = 14;
#[cfg(test)]
const OP_R_PUSH: u8 = 15;
#[cfg(test)]
const OP_LPOP: u8 = 16;
#[cfg(test)]
const OP_RPOP: u8 = 17;
#[cfg(test)]
const OP_JSON_SET: u8 = 18;
#[cfg(test)]
const OP_JSON_DEL: u8 = 19;
#[cfg(test)]
const OP_JSON_NUMINCRBY: u8 = 20;
#[cfg(test)]
const OP_JSON_ARRAPPEND: u8 = 21;
#[cfg(test)]
fn write_u16_be(buf: &mut Vec<u8>, val: u16) {
    buf.push((val >> 8) as u8);
    buf.push(val as u8);
}

fn write_u32_be(buf: &mut Vec<u8>, val: u32) {
    buf.push((val >> 24) as u8);
    buf.push((val >> 16) as u8);
    buf.push((val >> 8) as u8);
    buf.push(val as u8);
}

fn write_u64_be(buf: &mut Vec<u8>, val: u64) {
    buf.push((val >> 56) as u8);
    buf.push((val >> 48) as u8);
    buf.push((val >> 40) as u8);
    buf.push((val >> 32) as u8);
    buf.push((val >> 24) as u8);
    buf.push((val >> 16) as u8);
    buf.push((val >> 8) as u8);
    buf.push(val as u8);
}

// Versioni "checked": ritornano None invece di andare in panic se il
// binlog è troncato o corrotto (bit-flip, scrittura interrotta a metà da
// un crash) e i byte richiesti non ci sono davvero. Usate durante il
// recovery all'avvio, dove un binlog danneggiato non deve MAI far
// crashare il processo — nel peggiore dei casi si perde quel singolo
// record, non l'intero avvio.
#[cfg(test)]
fn read_u16_be(bytes: &[u8], offset: &mut usize) -> Option<u16> {
    if offset.checked_add(2)? > bytes.len() {
        return None;
    }
    let val = ((bytes[*offset] as u16) << 8) | (bytes[*offset + 1] as u16);
    *offset += 2;
    Some(val)
}

fn read_u32_be(bytes: &[u8], offset: &mut usize) -> Option<u32> {
    if offset.checked_add(4)? > bytes.len() {
        return None;
    }
    let val = ((bytes[*offset] as u32) << 24)
        | ((bytes[*offset + 1] as u32) << 16)
        | ((bytes[*offset + 2] as u32) << 8)
        | (bytes[*offset + 3] as u32);
    *offset += 4;
    Some(val)
}

fn read_u64_be(bytes: &[u8], offset: &mut usize) -> Option<u64> {
    if offset.checked_add(8)? > bytes.len() {
        return None;
    }
    let val = ((bytes[*offset] as u64) << 56)
        | ((bytes[*offset + 1] as u64) << 48)
        | ((bytes[*offset + 2] as u64) << 40)
        | ((bytes[*offset + 3] as u64) << 32)
        | ((bytes[*offset + 4] as u64) << 24)
        | ((bytes[*offset + 5] as u64) << 16)
        | ((bytes[*offset + 6] as u64) << 8)
        | (bytes[*offset + 7] as u64);
    *offset += 8;
    Some(val)
}

/// Estrae una fetta di `len` byte a partire da `offset`, senza andare in
/// panic se non ci stanno (record troncato/corrotto).
fn safe_slice(bytes: &[u8], offset: usize, len: usize) -> Option<&[u8]> {
    let end = offset.checked_add(len)?;
    bytes.get(offset..end)
}

fn encode_versioned_binlog_record(
    sequence: u64,
    effect_record: &[u8],
) -> Result<Vec<u8>, PersistenceError> {
    if sequence == 0 {
        return Err(PersistenceError::new(
            "Versioned binlog records require a non-zero sequence",
        ));
    }
    let record_length = BINLOG_RECORD_MAGIC
        .len()
        .checked_add(BINLOG_RECORD_LENGTH_SIZE)
        .and_then(|length| length.checked_add(8))
        .and_then(|length| length.checked_add(effect_record.len()))
        .and_then(|length| length.checked_add(BINLOG_CHECKSUM_SIZE))
        .ok_or_else(|| PersistenceError::new("Binlog record length overflow"))?;
    if record_length > MAX_BINLOG_RECORD_SIZE {
        return Err(PersistenceError::new(
            "Binlog record exceeds the format limit",
        ));
    }
    let mut record = Vec::with_capacity(record_length);
    record.extend_from_slice(BINLOG_RECORD_MAGIC);
    write_u32_be(
        &mut record,
        u32::try_from(record_length)
            .map_err(|_| PersistenceError::new("Binlog record length exceeds u32"))?,
    );
    write_u64_be(&mut record, sequence);
    record.extend_from_slice(effect_record);
    let checksum = crc32fast::hash(&record);
    write_u32_be(&mut record, checksum);
    Ok(record)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BinlogRecordIntegrity {
    Checksummed,
    ChecksumlessLegacy,
}

#[derive(Debug)]
enum DecodedBinlogRecord<'a> {
    Versioned {
        sequence: u64,
        effects: &'a [u8],
        integrity: BinlogRecordIntegrity,
    },
}

fn decode_binlog_record(record: &[u8]) -> Result<DecodedBinlogRecord<'_>, PersistenceError> {
    let (magic, effects_end, integrity) = if record.starts_with(BINLOG_RECORD_MAGIC) {
        let checksum_offset = record
            .len()
            .checked_sub(BINLOG_CHECKSUM_SIZE)
            .filter(|offset| *offset >= BINLOG_RECORD_MAGIC.len() + BINLOG_RECORD_LENGTH_SIZE + 8)
            .ok_or_else(|| PersistenceError::new("Truncated checksummed binlog record"))?;
        let expected_checksum = u32::from_be_bytes(
            record[checksum_offset..]
                .try_into()
                .map_err(|_| PersistenceError::new("Invalid binlog record checksum"))?,
        );
        let actual_checksum = crc32fast::hash(&record[..checksum_offset]);
        if actual_checksum != expected_checksum {
            return Err(PersistenceError::new("Binlog record checksum mismatch"));
        }
        (
            BINLOG_RECORD_MAGIC.as_slice(),
            checksum_offset,
            BinlogRecordIntegrity::Checksummed,
        )
    } else if record.starts_with(CHECKSUMLESS_BINLOG_RECORD_MAGIC) {
        (
            CHECKSUMLESS_BINLOG_RECORD_MAGIC.as_slice(),
            record.len(),
            BinlogRecordIntegrity::ChecksumlessLegacy,
        )
    } else {
        let format = if record.starts_with(PREVIOUS_BINLOG_RECORD_MAGIC) {
            "ONX2 command records"
        } else {
            "legacy command records"
        };
        return Err(PersistenceError::new(format!(
            "Unsupported unsafe binlog format: {}",
            format
        )));
    };

    let mut offset = magic.len();
    if integrity == BinlogRecordIntegrity::Checksummed {
        let embedded_length = read_u32_be(record, &mut offset)
            .ok_or_else(|| PersistenceError::new("Missing embedded binlog record length"))?
            as usize;
        if embedded_length != record.len() {
            return Err(PersistenceError::new(format!(
                "Binlog record length mismatch: outer length {}, embedded length {}",
                record.len(),
                embedded_length
            )));
        }
    }
    let sequence = read_u64_be(record, &mut offset)
        .ok_or_else(|| PersistenceError::new("Truncated versioned binlog record header"))?;
    if sequence == 0 {
        return Err(PersistenceError::new(
            "Versioned binlog records must have a non-zero sequence",
        ));
    }
    let effects = record
        .get(offset..effects_end)
        .ok_or_else(|| PersistenceError::new("Missing committed-effect payload"))?;
    if effects.is_empty() {
        return Err(PersistenceError::new(
            "Versioned binlog record contains an empty committed-effect payload",
        ));
    }
    Ok(DecodedBinlogRecord::Versioned {
        sequence,
        effects,
        integrity,
    })
}

/// Converte un comando + entry in record binario per il log
#[cfg(test)]
fn command_to_binary_record(
    cmd: &str,
    args: &[String],
    _entry: Option<&DataEntry>,
) -> Option<Vec<u8>> {
    let mut buf = Vec::with_capacity(256);
    let op_code = match cmd {
        "SET" | "GETSET" | "SETNX" | "MSET" => OP_SET,
        "DEL" => OP_DEL,
        "EXPIRE" | "EXPIREAT" => OP_EXPIRE,
        "LPUSH" => OP_L_PUSH,
        "RPUSH" => OP_R_PUSH,
        "LPOP" => OP_LPOP,
        "RPOP" => OP_RPOP,
        "JSON.SET" => OP_JSON_SET,
        "JSON.DEL" => OP_JSON_DEL,
        "JSON.NUMINCRBY" => OP_JSON_NUMINCRBY,
        "JSON.ARRAPPEND" => OP_JSON_ARRAPPEND,
        "HSET" => OP_HSET,
        "SADD" => OP_SADD,
        "RENAME" => OP_RENAME,
        "INCR" | "INCRBY" => OP_INCR,
        "DECRBY" => OP_DECR,
        "APPEND" => OP_APPEND,
        "HDEL" => OP_HDEL,
        "SREM" => OP_SREM,
        "COPY" => OP_COPY,
        _ => return None, // Comando non persistente
    };

    buf.push(op_code);

    match cmd {
        "SET" | "GETSET" | "SETNX" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let value = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            buf.push(1); // tipo: stringa
            write_u32_be(&mut buf, value.len() as u32);
            buf.extend_from_slice(value.as_bytes());
            // Scadenza assoluta, se presente (SET normalizzato con "EXAT
            // <timestamp>" da normalize_for_log — vedi lì per il perché):
            // senza questo, un SET con EX/PX sopravviverebbe alla
            // persistenza/replica perdendo silenziosamente la scadenza.
            let expiry: u64 = if args.len() >= 5 && args[3].eq_ignore_ascii_case("EXAT") {
                args[4].parse().unwrap_or(0)
            } else {
                0
            };
            write_u64_be(&mut buf, expiry);
        }
        "MSET" => {
            if args.len() < 3 {
                return None;
            }
            buf[0] = OP_MSET;
            let num_pairs = (args.len() - 1) / 2;
            write_u16_be(&mut buf, num_pairs as u16);
            let mut i = 1;
            while i + 1 < args.len() {
                let key = &args[i];
                let value = &args[i + 1];
                write_u16_be(&mut buf, key.len() as u16);
                buf.extend_from_slice(key.as_bytes());
                write_u32_be(&mut buf, value.len() as u32);
                buf.extend_from_slice(value.as_bytes());
                i += 2;
            }
            return Some(buf);
        }
        "DEL" => {
            if args.len() < 2 {
                return None;
            }
            let key = &args[1];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
        }
        "EXPIRE" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let seconds = args[2].parse::<u64>().unwrap_or(0);
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u64_be(&mut buf, seconds);
        }
        "EXPIREAT" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let timestamp = args[2].parse::<u64>().unwrap_or(0);
            buf[0] = OP_EXPIRE; // stesso codice, timestamp assoluto
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u64_be(&mut buf, timestamp);
        }
        "LPUSH" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let item = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u32_be(&mut buf, item.len() as u32);
            buf.extend_from_slice(item.as_bytes());
        }
        "RPUSH" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let item = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u32_be(&mut buf, item.len() as u32);
            buf.extend_from_slice(item.as_bytes());
        }
        "LPOP" | "RPOP" => {
            if args.len() < 2 {
                return None;
            }
            let key = &args[1];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
        }
        "HSET" => {
            if args.len() < 4 {
                return None;
            }
            let key = &args[1];
            let field = &args[2];
            let value = &args[3];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u16_be(&mut buf, field.len() as u16);
            buf.extend_from_slice(field.as_bytes());
            write_u32_be(&mut buf, value.len() as u32);
            buf.extend_from_slice(value.as_bytes());
        }
        "SADD" | "SREM" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let member = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u32_be(&mut buf, member.len() as u32);
            buf.extend_from_slice(member.as_bytes());
        }
        "RENAME" => {
            if args.len() < 3 {
                return None;
            }
            let old_key = &args[1];
            let new_key = &args[2];
            write_u16_be(&mut buf, old_key.len() as u16);
            buf.extend_from_slice(old_key.as_bytes());
            write_u16_be(&mut buf, new_key.len() as u16);
            buf.extend_from_slice(new_key.as_bytes());
        }
        "INCR" | "INCRBY" => {
            if args.len() < 2 {
                return None;
            }
            let key = &args[1];
            let delta = if cmd == "INCR" {
                1
            } else {
                args.get(2).and_then(|s| s.parse::<i64>().ok()).unwrap_or(1)
            };
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u64_be(&mut buf, delta as u64);
        }
        "DECRBY" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let delta = args[2].parse::<i64>().unwrap_or(1);
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u64_be(&mut buf, delta.unsigned_abs());
        }
        "APPEND" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let suffix = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u32_be(&mut buf, suffix.len() as u32);
            buf.extend_from_slice(suffix.as_bytes());
        }
        "HDEL" => {
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let field = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u16_be(&mut buf, field.len() as u16);
            buf.extend_from_slice(field.as_bytes());
        }
        "JSON.SET" => {
            // args: ["JSON.SET", key, path, value_json_compatto]
            if args.len() < 4 {
                return None;
            }
            let key = &args[1];
            let path = &args[2];
            let value = &args[3];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u16_be(&mut buf, path.len() as u16);
            buf.extend_from_slice(path.as_bytes());
            write_u32_be(&mut buf, value.len() as u32);
            buf.extend_from_slice(value.as_bytes());
        }
        "JSON.DEL" => {
            // args: ["JSON.DEL", key, path]
            if args.len() < 3 {
                return None;
            }
            let key = &args[1];
            let path = &args[2];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u16_be(&mut buf, path.len() as u16);
            buf.extend_from_slice(path.as_bytes());
        }
        "JSON.NUMINCRBY" => {
            // args: ["JSON.NUMINCRBY", key, path, delta_come_stringa]
            if args.len() < 4 {
                return None;
            }
            let key = &args[1];
            let path = &args[2];
            let delta = &args[3];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u16_be(&mut buf, path.len() as u16);
            buf.extend_from_slice(path.as_bytes());
            write_u16_be(&mut buf, delta.len() as u16);
            buf.extend_from_slice(delta.as_bytes());
        }
        "JSON.ARRAPPEND" => {
            // args: ["JSON.ARRAPPEND", key, path, value_json_compatto]
            if args.len() < 4 {
                return None;
            }
            let key = &args[1];
            let path = &args[2];
            let value = &args[3];
            write_u16_be(&mut buf, key.len() as u16);
            buf.extend_from_slice(key.as_bytes());
            write_u16_be(&mut buf, path.len() as u16);
            buf.extend_from_slice(path.as_bytes());
            write_u32_be(&mut buf, value.len() as u32);
            buf.extend_from_slice(value.as_bytes());
        }
        "COPY" => {
            if args.len() < 3 {
                return None;
            }
            let src = &args[1];
            let dst = &args[2];
            write_u16_be(&mut buf, src.len() as u16);
            buf.extend_from_slice(src.as_bytes());
            write_u16_be(&mut buf, dst.len() as u16);
            buf.extend_from_slice(dst.as_bytes());
        }
        _ => return None,
    }

    Some(buf)
}

/// Legge un record binario e lo converte in args per execute_command
#[cfg(test)]
fn binary_record_to_args(record: &[u8]) -> Option<Vec<String>> {
    if record.is_empty() {
        return None;
    }

    let op = record[0];
    let mut offset = 1;

    match op {
        OP_SET => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let _val_type = *record.get(offset)?;
            offset += 1;
            let val_len = read_u32_be(record, &mut offset)? as usize;
            let value = String::from_utf8_lossy(safe_slice(record, offset, val_len)?).to_string();
            offset += val_len;
            // I record scritti prima di questa versione non hanno questi 8
            // byte finali con la scadenza: se mancano, va bene lo stesso,
            // significa che "nessuna scadenza" (comportamento invariato).
            let expiry = read_u64_be(record, &mut offset).unwrap_or(0);
            if expiry > 0 {
                Some(vec![
                    "SET".to_string(),
                    key,
                    value,
                    "EXAT".to_string(),
                    expiry.to_string(),
                ])
            } else {
                Some(vec!["SET".to_string(), key, value])
            }
        }
        OP_MSET => {
            let num_pairs = read_u16_be(record, &mut offset)? as usize;
            let mut args = vec!["MSET".to_string()];
            for _ in 0..num_pairs {
                let key_len = read_u16_be(record, &mut offset)? as usize;
                let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
                offset += key_len;
                let val_len = read_u32_be(record, &mut offset)? as usize;
                let value =
                    String::from_utf8_lossy(safe_slice(record, offset, val_len)?).to_string();
                offset += val_len;
                args.push(key);
                args.push(value);
            }
            Some(args)
        }
        OP_DEL => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            Some(vec!["DEL".to_string(), key])
        }
        OP_EXPIRE => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let timestamp = read_u64_be(record, &mut offset)?;
            Some(vec!["EXPIREAT".to_string(), key, timestamp.to_string()])
        }
        OP_L_PUSH => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let item_len = read_u32_be(record, &mut offset)? as usize;
            let item = String::from_utf8_lossy(safe_slice(record, offset, item_len)?).to_string();
            Some(vec!["LPUSH".to_string(), key, item])
        }
        OP_R_PUSH => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let item_len = read_u32_be(record, &mut offset)? as usize;
            let item = String::from_utf8_lossy(safe_slice(record, offset, item_len)?).to_string();
            Some(vec!["RPUSH".to_string(), key, item])
        }
        OP_LPOP => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            Some(vec!["LPOP".to_string(), key])
        }
        OP_RPOP => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            Some(vec!["RPOP".to_string(), key])
        }
        OP_HSET => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let field_len = read_u16_be(record, &mut offset)? as usize;
            let field = String::from_utf8_lossy(safe_slice(record, offset, field_len)?).to_string();
            offset += field_len;
            let value_len = read_u32_be(record, &mut offset)? as usize;
            let value = String::from_utf8_lossy(safe_slice(record, offset, value_len)?).to_string();
            Some(vec!["HSET".to_string(), key, field, value])
        }
        OP_SADD => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let member_len = read_u32_be(record, &mut offset)? as usize;
            let member =
                String::from_utf8_lossy(safe_slice(record, offset, member_len)?).to_string();
            Some(vec!["SADD".to_string(), key, member])
        }
        OP_SREM => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let member_len = read_u32_be(record, &mut offset)? as usize;
            let member =
                String::from_utf8_lossy(safe_slice(record, offset, member_len)?).to_string();
            Some(vec!["SREM".to_string(), key, member])
        }
        OP_RENAME => {
            let old_len = read_u16_be(record, &mut offset)? as usize;
            let old_key = String::from_utf8_lossy(safe_slice(record, offset, old_len)?).to_string();
            offset += old_len;
            let new_len = read_u16_be(record, &mut offset)? as usize;
            let new_key = String::from_utf8_lossy(safe_slice(record, offset, new_len)?).to_string();
            Some(vec!["RENAME".to_string(), old_key, new_key])
        }
        OP_INCR => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let delta = read_u64_be(record, &mut offset)?;
            Some(vec!["INCRBY".to_string(), key, delta.to_string()])
        }
        OP_DECR => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let delta = read_u64_be(record, &mut offset)?;
            Some(vec!["DECRBY".to_string(), key, delta.to_string()])
        }
        OP_APPEND => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let suffix_len = read_u32_be(record, &mut offset)? as usize;
            let suffix =
                String::from_utf8_lossy(safe_slice(record, offset, suffix_len)?).to_string();
            Some(vec!["APPEND".to_string(), key, suffix])
        }
        OP_HDEL => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let field_len = read_u16_be(record, &mut offset)? as usize;
            let field = String::from_utf8_lossy(safe_slice(record, offset, field_len)?).to_string();
            Some(vec!["HDEL".to_string(), key, field])
        }
        OP_COPY => {
            let src_len = read_u16_be(record, &mut offset)? as usize;
            let src = String::from_utf8_lossy(safe_slice(record, offset, src_len)?).to_string();
            offset += src_len;
            let dst_len = read_u16_be(record, &mut offset)? as usize;
            let dst = String::from_utf8_lossy(safe_slice(record, offset, dst_len)?).to_string();
            Some(vec!["COPY".to_string(), src, dst])
        }
        OP_JSON_SET => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let path_len = read_u16_be(record, &mut offset)? as usize;
            let path = String::from_utf8_lossy(safe_slice(record, offset, path_len)?).to_string();
            offset += path_len;
            let val_len = read_u32_be(record, &mut offset)? as usize;
            let value = String::from_utf8_lossy(safe_slice(record, offset, val_len)?).to_string();
            Some(vec!["JSON.SET".to_string(), key, path, value])
        }
        OP_JSON_DEL => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let path_len = read_u16_be(record, &mut offset)? as usize;
            let path = String::from_utf8_lossy(safe_slice(record, offset, path_len)?).to_string();
            Some(vec!["JSON.DEL".to_string(), key, path])
        }
        OP_JSON_NUMINCRBY => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let path_len = read_u16_be(record, &mut offset)? as usize;
            let path = String::from_utf8_lossy(safe_slice(record, offset, path_len)?).to_string();
            offset += path_len;
            let delta_len = read_u16_be(record, &mut offset)? as usize;
            let delta = String::from_utf8_lossy(safe_slice(record, offset, delta_len)?).to_string();
            Some(vec!["JSON.NUMINCRBY".to_string(), key, path, delta])
        }
        OP_JSON_ARRAPPEND => {
            let key_len = read_u16_be(record, &mut offset)? as usize;
            let key = String::from_utf8_lossy(safe_slice(record, offset, key_len)?).to_string();
            offset += key_len;
            let path_len = read_u16_be(record, &mut offset)? as usize;
            let path = String::from_utf8_lossy(safe_slice(record, offset, path_len)?).to_string();
            offset += path_len;
            let val_len = read_u32_be(record, &mut offset)? as usize;
            let value = String::from_utf8_lossy(safe_slice(record, offset, val_len)?).to_string();
            Some(vec!["JSON.ARRAPPEND".to_string(), key, path, value])
        }
        _ => None,
    }
}

fn line_to_entry(line: &str) -> Option<(String, DataEntry)> {
    let mut parts = line.splitn(4, '\t');
    let key = parts.next()?.to_string();
    let val_type = parts.next()?;
    let exp_val = parts.next()?.parse::<u64>().unwrap_or(0);
    let val_str = parts.next()?;

    let expires_at = if exp_val == 0 { None } else { Some(exp_val) };
    let value = match val_type {
        "STR" => Some(OnyxValue::Blob(Bytes::from(val_str.to_string()))),
        "INT" => val_str.parse::<i64>().ok().map(OnyxValue::Int),
        "LIST" => {
            let items: Vec<Bytes> = if val_str.is_empty() {
                Vec::new()
            } else {
                val_str
                    .split('|')
                    .map(|s| Bytes::from(s.to_string()))
                    .collect()
            };
            Some(OnyxValue::List(items))
        }
        "HASH" => {
            let mut map = std::collections::HashMap::new();
            if !val_str.is_empty() {
                for pair in val_str.split('|') {
                    if let Some((k, v)) = pair.split_once('=') {
                        map.insert(Bytes::from(k.to_string()), Bytes::from(v.to_string()));
                    }
                }
            }
            Some(OnyxValue::Hash(map))
        }
        "JSON" => serde_json::from_str::<serde_json::Value>(val_str)
            .ok()
            .map(OnyxValue::Json),
        "SET" => {
            let set: std::collections::HashSet<Bytes> = if val_str.is_empty() {
                std::collections::HashSet::new()
            } else {
                val_str
                    .split('|')
                    .map(|s| Bytes::from(s.to_string()))
                    .collect()
            };
            Some(OnyxValue::Set(set))
        }
        _ => None,
    }?;

    let ts = now();
    Some((
        key,
        DataEntry {
            value,
            expires_at,
            created_at: ts,
            last_accessed: ts,
        },
    ))
}

#[cfg(test)]
fn value_to_line(key: &str, entry: &DataEntry) -> String {
    let (val_type, val_str): (&str, String) = match &entry.value {
        OnyxValue::Blob(b) => ("STR", String::from_utf8_lossy(b).to_string()),
        OnyxValue::Int(n) => ("INT", n.to_string()),
        OnyxValue::Float(f) => ("STR", f.to_string()),
        OnyxValue::List(list) => (
            "LIST",
            list.iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect::<Vec<_>>()
                .join("|"),
        ),
        OnyxValue::Hash(map) => (
            "HASH",
            map.iter()
                .map(|(k, v)| {
                    format!(
                        "{}={}",
                        String::from_utf8_lossy(k),
                        String::from_utf8_lossy(v)
                    )
                })
                .collect::<Vec<_>>()
                .join("|"),
        ),
        OnyxValue::Set(set) => (
            "SET",
            set.iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect::<Vec<_>>()
                .join("|"),
        ),
        OnyxValue::Json(j) => ("JSON", j.to_string()),
        // Vector: non ancora supportato nel formato snapshot testuale
        _ => ("STR", String::new()),
    };

    let exp_val = entry.expires_at.unwrap_or(0);
    format!("{}\t{}\t{}\t{}", key, val_type, exp_val, val_str)
}
/// Elenco dei comandi che scrivono (usato sia per il gate READONLY sulle
/// Replica, sia — indirettamente — coerente con quello che command_to_binary_record
/// sa mappare su un op-code per il binlog). Va prima del dispatch, quindi
/// non richiede aver già eseguito il comando per saperlo.
fn is_write_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "SET"
            | "GETSET"
            | "SETNX"
            | "MSET"
            | "DEL"
            | "EXPIRE"
            | "EXPIREAT"
            | "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "HSET"
            | "SADD"
            | "RENAME"
            | "INCR"
            | "INCRBY"
            | "DECRBY"
            | "APPEND"
            | "HDEL"
            | "SREM"
            | "COPY"
            | "JSON.SET"
            | "JSON.DEL"
            | "JSON.NUMINCRBY"
            | "JSON.ARRAPPEND"
    )
}

fn persistent_keys_for_command(args: &[String]) -> Vec<Bytes> {
    let command = args.first().map(String::as_str).unwrap_or("");
    let mut keys = Vec::new();
    match command {
        "MSET" => {
            let mut index = 1;
            while index + 1 < args.len() {
                keys.push(Bytes::copy_from_slice(args[index].as_bytes()));
                index += 2;
            }
        }
        "RENAME" | "COPY" => {
            if let Some(key) = args.get(1) {
                keys.push(Bytes::copy_from_slice(key.as_bytes()));
            }
            if let Some(key) = args.get(2) {
                keys.push(Bytes::copy_from_slice(key.as_bytes()));
            }
        }
        _ if is_write_command(command) => {
            if let Some(key) = args.get(1) {
                keys.push(Bytes::copy_from_slice(key.as_bytes()));
            }
        }
        _ => {}
    }

    let mut unique = std::collections::HashSet::new();
    keys.retain(|key| unique.insert(key.clone()));
    keys
}

fn capture_entries(
    store: &ShardedStore,
    keys: &[Bytes],
) -> std::collections::HashMap<Bytes, Option<DataEntry>> {
    keys.iter()
        .map(|key| (key.clone(), store.engine.peek(key)))
        .collect()
}

fn derive_committed_batch(
    store: &ShardedStore,
    keys: &[Bytes],
    before: &std::collections::HashMap<Bytes, Option<DataEntry>>,
    evicted_entries: &[(Bytes, DataEntry)],
) -> Option<CommittedBatch> {
    let mut effects = Vec::new();
    let mut deleted = std::collections::HashSet::new();
    for (key, _) in evicted_entries {
        if deleted.insert(key.clone()) {
            effects.push(CommittedEffect::Delete { key: key.clone() });
        }
    }

    for key in keys {
        let previous = before
            .get(key)
            .cloned()
            .flatten()
            .map(PersistentEntry::from);
        let current = store.engine.peek(key).map(PersistentEntry::from);
        let was_evicted = deleted.contains(key);
        if previous == current && !was_evicted {
            continue;
        }
        match current {
            Some(entry) => effects.push(CommittedEffect::Put {
                key: key.clone(),
                entry,
            }),
            None if deleted.insert(key.clone()) => {
                effects.push(CommittedEffect::Delete { key: key.clone() });
            }
            None => {}
        }
    }

    (!effects.is_empty()).then_some(CommittedBatch { effects })
}

fn rollback_attempted_mutation(
    store: &ShardedStore,
    before: &std::collections::HashMap<Bytes, Option<DataEntry>>,
    evicted_entries: &[(Bytes, DataEntry)],
) {
    for (key, previous) in before {
        match previous {
            Some(entry) => {
                store.engine.apply_entry(key.clone(), entry.clone());
            }
            None => {
                store.engine.delete(key);
            }
        }
    }
    for (key, entry) in evicted_entries {
        if !before.contains_key(key) {
            store.engine.apply_entry(key.clone(), entry.clone());
        }
    }
}
/// Un resync parziale è ammissibile solo se il replication ID richiesto
/// dalla Replica coincide con quello attuale del Master: garantisce che
/// stiamo parlando con lo stesso identico processo Master di prima, non
/// con uno ripartito da zero (nel qual caso il vecchio offset non ha più
/// alcun significato, anche se per coincidenza "sembra" plausibile).
/// requested_replid == 0 vuol dire "la Replica non conosce ancora nessun
/// replid" (prima connessione in assoluto): mai ammesso a resync parziale.
fn replid_allows_partial(requested_replid: u64, current_replid: u64) -> bool {
    requested_replid != 0 && requested_replid == current_replid
}
/// Dato che il replication ID combacia, decide se il backlog attuale
/// permette davvero un resync parziale senza buchi.
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteAdmissionError {
    KeyLimit,
    Maxmemory,
}

impl WriteAdmissionError {
    fn message(self) -> &'static str {
        match self {
            Self::KeyLimit => "ERR database key limit reached",
            Self::Maxmemory => {
                "OOM command not allowed because the projected memory usage exceeds maxmemory"
            }
        }
    }
}

/// Validates the authoritative post-mutation state while the write gate is held.
/// A pre-existing over-limit state may only move sideways or downward. Growing
/// mutations must fit after policy-driven eviction without evicting keys involved
/// in the command itself.
fn enforce_write_admission(
    store: &ShardedStore,
    memory_before: usize,
    key_count_before: usize,
    protected_keys: &HashSet<Bytes>,
) -> Result<Vec<(Bytes, DataEntry)>, WriteAdmissionError> {
    let key_count_after = store.engine.stats().total_keys;
    if key_count_after > MAX_KEYS && key_count_after > key_count_before {
        return Err(WriteAdmissionError::KeyLimit);
    }

    let limit = store.maxmemory_bytes();
    let memory_after = store.used_memory_bytes();
    if limit == 0 || memory_after <= limit || memory_after <= memory_before {
        return Ok(Vec::new());
    }
    if store.maxmemory_policy() == EvictionPolicy::NoEviction {
        return Err(WriteAdmissionError::Maxmemory);
    }

    let evicted = store
        .engine
        .evict_to_fit(limit, store.maxmemory_policy(), protected_keys);
    if store.used_memory_bytes() <= limit {
        return Ok(evicted);
    }

    for (key, entry) in &evicted {
        store.engine.apply_entry(key.clone(), entry.clone());
    }
    Err(WriteAdmissionError::Maxmemory)
}

fn execute_command(store: &ShardedStore, args: &[String]) -> (RESPValue, bool) {
    execute_command_dispatch(store, args)
}

fn execute_command_dispatch(store: &ShardedStore, args: &[String]) -> (RESPValue, bool) {
    let cmd = args.first().map(|s| s.as_str()).unwrap_or("");
    let key = args.get(1).map(|s| s.as_str()).unwrap_or("");
    let arg = args.get(2).map(|s| s.as_str()).unwrap_or("");

    match cmd {
        "SET" if args.len() >= 3 => {
            // Opzioni extra, in qualsiasi ordine come in Redis: EX secondi |
            // PX millisecondi | EXAT timestamp_assoluto (quest'ultima solo
            // per uso interno: è così che persistenza/replica ripropongono
            // in modo deterministico un SET con scadenza, senza rivalutare
            // "adesso" al momento del replay), NX | XX.
            let mut expires_at: Option<u64> = None;
            let mut condition: Option<bool> = None; // Some(true)=NX, Some(false)=XX
            let mut i = 3;
            let mut valid = true;
            while i < args.len() {
                match args[i].to_uppercase().as_str() {
                    "EX" => match args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                        Some(secs) if secs > 0 => {
                            expires_at = Some(now().saturating_add(secs));
                            i += 2;
                        }
                        Some(_) | None => {
                            valid = false;
                            break;
                        }
                    },
                    "PX" => match args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                        Some(millis) if millis > 0 => {
                            let seconds = millis.saturating_add(999) / 1000;
                            expires_at = Some(now().saturating_add(seconds));
                            i += 2;
                        }
                        Some(_) | None => {
                            valid = false;
                            break;
                        }
                    },
                    "EXAT" => match args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                        Some(ts) => {
                            expires_at = Some(ts);
                            i += 2;
                        }
                        None => {
                            valid = false;
                            break;
                        }
                    },
                    "NX" => {
                        condition = Some(true);
                        i += 1;
                    }
                    "XX" => {
                        condition = Some(false);
                        i += 1;
                    }
                    _ => {
                        valid = false;
                        break;
                    }
                }
            }
            if !valid {
                (RESPValue::Error("ERR syntax error".to_string()), false)
            } else {
                let ok = store.engine.set_conditional(
                    Bytes::from(key.to_string()),
                    OnyxValue::Blob(Bytes::from(arg.to_string())),
                    expires_at,
                    condition,
                );
                if ok {
                    (RESPValue::SimpleString("OK".to_string()), true)
                } else {
                    // NX con chiave già esistente, o XX con chiave assente:
                    // nessuna scrittura avvenuta, come in Redis risponde nil.
                    (RESPValue::BulkString(None), false)
                }
            }
        }
        "GET" if args.len() >= 2 => match store.get(key) {
            Ok(Some(value)) => (RESPValue::BulkString(Some(value)), false),
            Ok(None) => (RESPValue::BulkString(None), false),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "DEL" if args.len() >= 2 => (
            RESPValue::Integer(if store.delete(key) { 1 } else { 0 }),
            true,
        ),
        "INCR" if args.len() >= 2 => match store.incr(key) {
            Ok(value) => (RESPValue::Integer(value), true),
            Err(message) => (RESPValue::Error(message.to_string()), false),
        },
        "LPUSH" if args.len() >= 3 => match store.lpush(key, arg.to_string()) {
            Ok(length) => (RESPValue::Integer(length as i64), true),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "RPUSH" if args.len() >= 3 => match store.rpush(key, arg.to_string()) {
            Ok(length) => (RESPValue::Integer(length as i64), true),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "LPOP" if args.len() >= 2 => match store.lpop(key) {
            Ok(Some(value)) => (RESPValue::BulkString(Some(value)), true),
            Ok(None) => (RESPValue::BulkString(None), false),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "RPOP" if args.len() >= 2 => match store.rpop(key) {
            Ok(Some(value)) => (RESPValue::BulkString(Some(value)), true),
            Ok(None) => (RESPValue::BulkString(None), false),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "LRANGE" if args.len() >= 2 => {
            let start = args.get(2).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
            let stop = args
                .get(3)
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(-1);
            match store.lrange(key, start, stop) {
                Ok(list) => (
                    RESPValue::Array(
                        list.into_iter()
                            .map(|s| RESPValue::BulkString(Some(s)))
                            .collect(),
                    ),
                    false,
                ),
                Err(error) => (RESPValue::Error(error.message().to_string()), false),
            }
        }

        "EXPIREAT" if args.len() >= 3 => {
            if let Ok(t) = arg.parse::<u64>() {
                (
                    RESPValue::Integer(if store.expire_at(key, t) { 1 } else { 0 }),
                    true,
                )
            } else {
                (RESPValue::Error("ERR invalid timestamp".to_string()), false)
            }
        }
        "TTL" if args.len() >= 2 => (RESPValue::Integer(store.ttl(key)), false),
        "EXISTS" if args.len() >= 2 => (
            RESPValue::Integer(if store.exists(key) { 1 } else { 0 }),
            false,
        ),
        "TYPE" if args.len() >= 2 => match store.value_type(key) {
            Some(t) => (RESPValue::SimpleString(t.to_string()), false),
            None => (RESPValue::SimpleString("none".to_string()), false),
        },
        "JSON.SET" => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("");
            let raw_value = args.get(3).map(|s| s.as_str()).unwrap_or("");
            if args.len() < 4 || path.is_empty() {
                (
                    RESPValue::Error("ERR usage: JSON.SET key path json-value".to_string()),
                    false,
                )
            } else {
                match serde_json::from_str::<serde_json::Value>(raw_value) {
                    Ok(parsed) => match store.json_set(key, path, parsed) {
                        Ok(()) => (RESPValue::SimpleString("OK".to_string()), true),
                        Err(e) => (RESPValue::Error(e.to_string()), false),
                    },
                    Err(_) => (
                        RESPValue::Error("ERR value is not valid JSON".to_string()),
                        false,
                    ),
                }
            }
        }
        "JSON.GET" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("$");
            match store.json_get(key, path) {
                Ok(Some(s)) => (RESPValue::BulkString(Some(s)), false),
                Ok(None) => (RESPValue::BulkString(None), false),
                Err(e) => (RESPValue::Error(e.to_string()), false),
            }
        }
        "JSON.DEL" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("$");
            match store.json_del(key, path) {
                Ok(deleted) => (RESPValue::Integer(if deleted { 1 } else { 0 }), deleted),
                Err(e) => (RESPValue::Error(e.to_string()), false),
            }
        }
        "JSON.TYPE" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("$");
            match store.json_type(key, path) {
                Ok(Some(t)) => (RESPValue::SimpleString(t.to_string()), false),
                Ok(None) => (RESPValue::BulkString(None), false),
                Err(e) => (RESPValue::Error(e.to_string()), false),
            }
        }
        "JSON.NUMINCRBY" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("");
            let delta_str = args.get(3).map(|s| s.as_str()).unwrap_or("");
            match delta_str.parse::<f64>() {
                Ok(delta) => match store.json_numincrby(key, path, delta) {
                    Ok(new_val) => (RESPValue::BulkString(Some(new_val.to_string())), true),
                    Err(e) => (RESPValue::Error(e), false),
                },
                Err(_) => (
                    RESPValue::Error("ERR delta is not a valid number".to_string()),
                    false,
                ),
            }
        }
        "JSON.ARRAPPEND" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("");
            let raw_value = args.get(3).map(|s| s.as_str()).unwrap_or("");
            match serde_json::from_str::<serde_json::Value>(raw_value) {
                Ok(parsed) => match store.json_arrappend(key, path, parsed) {
                    Ok(new_len) => (RESPValue::Integer(new_len as i64), true),
                    Err(e) => (RESPValue::Error(e), false),
                },
                Err(_) => (
                    RESPValue::Error("ERR value is not valid JSON".to_string()),
                    false,
                ),
            }
        }
        "JSON.ARRLEN" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("$");
            match store.json_arrlen(key, path) {
                Ok(Some(len)) => (RESPValue::Integer(len as i64), false),
                Ok(None) => (RESPValue::BulkString(None), false),
                Err(e) => (RESPValue::Error(e), false),
            }
        }
        "JSON.OBJKEYS" if args.len() >= 2 => {
            let path = args.get(2).map(|s| s.as_str()).unwrap_or("$");
            match store.json_objkeys(key, path) {
                Ok(Some(keys)) => (
                    RESPValue::Array(
                        keys.into_iter()
                            .map(|k| RESPValue::BulkString(Some(k)))
                            .collect(),
                    ),
                    false,
                ),
                Ok(None) => (RESPValue::Array(Vec::new()), false),
                Err(e) => (RESPValue::Error(e), false),
            }
        }
        "SADD" if args.len() >= 3 => match store.sadd(key, arg) {
            Ok(added) => (RESPValue::Integer(if added { 1 } else { 0 }), true),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "SMEMBERS" if args.len() >= 2 => match store.smembers(key) {
            Ok(members) => (
                RESPValue::Array(
                    members
                        .into_iter()
                        .map(|m| RESPValue::BulkString(Some(m)))
                        .collect(),
                ),
                false,
            ),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "SREM" if args.len() >= 3 => match store.srem(key, arg) {
            Ok(removed) => (RESPValue::Integer(if removed { 1 } else { 0 }), removed),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "SISMEMBER" if args.len() >= 3 => match store.sismember(key, arg) {
            Ok(present) => (RESPValue::Integer(if present { 1 } else { 0 }), false),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "LLEN" if args.len() >= 2 => match store.llen(key) {
            Ok(length) => (RESPValue::Integer(length as i64), false),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "RENAME" if args.len() >= 3 => {
            if store.rename(key, arg) {
                (RESPValue::SimpleString("OK".to_string()), true)
            } else {
                (RESPValue::Error("ERR no such key".to_string()), false)
            }
        }
        "MSET" => {
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                (
                    RESPValue::Error("ERR wrong number of arguments for 'mset'".to_string()),
                    false,
                )
            } else {
                let mut i = 1;
                while i + 1 < args.len() {
                    store.set(args[i].clone(), args[i + 1].clone());
                    i += 2;
                }
                (RESPValue::SimpleString("OK".to_string()), true)
            }
        }
        "MGET" => {
            let results = args[1..]
                .iter()
                .map(|key| store.get(key))
                .collect::<Result<Vec<_>, _>>();
            match results {
                Ok(values) => (
                    RESPValue::Array(values.into_iter().map(RESPValue::BulkString).collect()),
                    false,
                ),
                Err(error) => (RESPValue::Error(error.message().to_string()), false),
            }
        }
        "KEYS" => {
            let pattern = key;
            let keys = store.keys_matching(pattern);
            (
                RESPValue::Array(
                    keys.into_iter()
                        .map(|k| RESPValue::BulkString(Some(k)))
                        .collect(),
                ),
                false,
            )
        }
        "HSET" if args.len() >= 3 => {
            let field = arg;
            let value = args.get(3).map(|s| s.as_str()).unwrap_or("");
            if args.len() < 4 {
                (
                    RESPValue::Error("ERR wrong number of arguments for 'hset'".to_string()),
                    false,
                )
            } else {
                match store.hset(key, field, value) {
                    Ok(is_new) => (RESPValue::Integer(if is_new { 1 } else { 0 }), true),
                    Err(error) => (RESPValue::Error(error.message().to_string()), false),
                }
            }
        }
        "HGET" if args.len() >= 3 => {
            let field = arg;
            (
                match store.hget(key, field) {
                    Ok(value) => RESPValue::BulkString(value),
                    Err(error) => RESPValue::Error(error.message().to_string()),
                },
                false,
            )
        }
        "HGETALL" if args.len() >= 2 => match store.hgetall(key) {
            Ok(pairs) => {
                let mut flat = Vec::with_capacity(pairs.len() * 2);
                for (f, v) in pairs {
                    flat.push(RESPValue::BulkString(Some(f)));
                    flat.push(RESPValue::BulkString(Some(v)));
                }
                (RESPValue::Array(flat), false)
            }
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "HDEL" if args.len() >= 3 => {
            let field = arg;
            match store.hdel(key, field) {
                Ok(removed) => (RESPValue::Integer(if removed { 1 } else { 0 }), removed),
                Err(error) => (RESPValue::Error(error.message().to_string()), false),
            }
        }
        "REPLICAOF" if key.eq_ignore_ascii_case("no") && arg.eq_ignore_ascii_case("one") => {
            (RESPValue::SimpleString("OK".to_string()), false)
        }
        "INCRBY" if args.len() >= 3 => match arg.parse::<i64>() {
            Ok(delta) => match store.incrby(key, delta) {
                Ok(value) => (RESPValue::Integer(value), true),
                Err(message) => (RESPValue::Error(message.to_string()), false),
            },
            Err(_) => (
                RESPValue::Error("ERR value is not an integer".to_string()),
                false,
            ),
        },
        "DECRBY" if args.len() >= 3 => match arg.parse::<i64>() {
            Ok(delta) => match delta.checked_neg() {
                Some(negated) => match store.incrby(key, negated) {
                    Ok(value) => (RESPValue::Integer(value), true),
                    Err(message) => (RESPValue::Error(message.to_string()), false),
                },
                None => (
                    RESPValue::Error("ERR increment or decrement would overflow".to_string()),
                    false,
                ),
            },
            Err(_) => (
                RESPValue::Error("ERR value is not an integer".to_string()),
                false,
            ),
        },
        "APPEND" if args.len() >= 3 => match store.append(key, arg) {
            Ok(length) => (RESPValue::Integer(length as i64), true),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "STRLEN" if args.len() >= 2 => match store.strlen(key) {
            Ok(length) => (RESPValue::Integer(length as i64), false),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "GETSET" if args.len() >= 3 => match store.getset(key, arg) {
            Ok(old) => (RESPValue::BulkString(old), true),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "INFO" => {
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
                "role:{}\nuptime_seconds:{}\nconnected_keys:{}\nmax_keys:{}\nactive_connections:{}\ntotal_commands:{}\ncache_hits:{}\ncache_misses:{}\nhit_rate_percent:{:.1}\nused_memory_bytes:{}\nmaxmemory_bytes:{}\nmaxmemory_policy:{}",
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
            (RESPValue::BulkString(Some(info_text)), false)
        }
        "SETNX" if args.len() >= 3 => (
            RESPValue::Integer(if store.setnx(key, arg) { 1 } else { 0 }),
            true,
        ),
        "HKEYS" if args.len() >= 2 => match store.hkeys(key) {
            Ok(fields) => (
                RESPValue::Array(
                    fields
                        .into_iter()
                        .map(|f| RESPValue::BulkString(Some(f)))
                        .collect(),
                ),
                false,
            ),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "HVALS" if args.len() >= 2 => match store.hvals(key) {
            Ok(vals) => (
                RESPValue::Array(
                    vals.into_iter()
                        .map(|v| RESPValue::BulkString(Some(v)))
                        .collect(),
                ),
                false,
            ),
            Err(error) => (RESPValue::Error(error.message().to_string()), false),
        },
        "COPY" if args.len() >= 3 => (
            RESPValue::Integer(if store.copy(key, arg) { 1 } else { 0 }),
            true,
        ),
        "EXPIRE" if args.len() >= 3 => {
            let condition = args.get(3).map(|s| s.to_uppercase());
            match arg.parse::<u64>() {
                Ok(s) => {
                    if condition
                        .as_deref()
                        .is_some_and(|value| !matches!(value, "NX" | "XX"))
                    {
                        (RESPValue::Error("ERR syntax error".to_string()), false)
                    } else {
                        let ok = match &condition {
                            Some(c) => store.expire_conditional(key, s, c),
                            None => store.expire(key, s),
                        };
                        (RESPValue::Integer(if ok { 1 } else { 0 }), ok)
                    }
                }
                Err(_) => (
                    RESPValue::Error("ERR invalid expire time".to_string()),
                    false,
                ),
            }
        }
        "PING" => (RESPValue::SimpleString("PONG".to_string()), false),
        _ => (
            RESPValue::Error("ERR unknown command or wrong syntax".to_string()),
            false,
        ),
    }
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
        // SET con opzioni (EX/PX/NX/XX): NX/XX sono a posto così come sono
        // (existence-based, replay deterministico), ma EX/PX sono relativi
        // ad "adesso" — se li rigiocassimo alla lettera in fase di replay
        // (binlog o Replica), "adesso" sarebbe un istante diverso da quello
        // originale. Normalizziamo quindi in "SET chiave valore EXAT
        // <timestamp_assoluto>" quando è risultata una scadenza, così il
        // replay riproduce esattamente la stessa scadenza, non una nuova.
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

async fn write_chunked_replication_record<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    record: &ChunkedReplicationRecord,
) -> std::io::Result<()> {
    writer
        .write_all(&encode_replication_command(&record.header))
        .await?;
    for chunk in record.payload.chunks(REPLICATION_CHUNK_SIZE) {
        let frame =
            encode_replication_command(&[record.chunk_command.to_string(), hex_encode(chunk)]);
        writer.write_all(&frame).await?;
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SnapshotFormat {
    Missing,
    Legacy,
    Versioned { watermark: u64 },
}

#[derive(Debug, Default)]
struct BinlogInspection {
    min_sequence: Option<u64>,
    max_sequence: u64,
    valid_len: u64,
    truncated_tail: bool,
    contains_checksumless_records: bool,
}

#[derive(Debug, Default)]
struct RecoveryState {
    last_sequence: u64,
    snapshot_watermark: u64,
}

fn read_bounded_utf8_line(
    reader: &mut impl BufRead,
    maximum_size: usize,
) -> Result<Option<String>, PersistenceError> {
    let mut bytes = Vec::new();
    let mut limited = reader.take((maximum_size + 1) as u64);
    if limited.read_until(b'\n', &mut bytes)? == 0 {
        return Ok(None);
    }
    if bytes.len() > maximum_size {
        return Err(PersistenceError::new(format!(
            "Snapshot line exceeds the {} byte limit",
            maximum_size
        )));
    }
    let mut line = String::from_utf8(bytes)
        .map_err(|_| PersistenceError::new("Snapshot contains invalid UTF-8"))?;
    while line.ends_with(['\r', '\n']) {
        line.pop();
    }
    Ok(Some(line))
}

fn inspect_snapshot(path: &Path) -> Result<SnapshotFormat, PersistenceError> {
    if !path.exists() {
        return Ok(SnapshotFormat::Missing);
    }
    let file = File::open(path)?;
    let decoder = GzDecoder::new(file);
    let mut reader = StdBufReader::new(decoder);
    let first_line = read_bounded_utf8_line(&mut reader, MAX_SNAPSHOT_METADATA_SIZE)?
        .ok_or_else(|| PersistenceError::new("Snapshot is empty"))?;
    if !first_line.starts_with(SNAPSHOT_MAGIC) {
        return Ok(SnapshotFormat::Legacy);
    }

    let fields: Vec<&str> = first_line.split('\t').collect();
    if fields.len() != 3 || fields[0] != SNAPSHOT_MAGIC {
        return Err(PersistenceError::new("Malformed snapshot metadata header"));
    }
    let version = fields[1]
        .parse::<u8>()
        .map_err(|_| PersistenceError::new("Invalid snapshot format version"))?;
    if version != SNAPSHOT_VERSION {
        return Err(PersistenceError::new(format!(
            "Unsupported snapshot format version: {}",
            version
        )));
    }
    let watermark = fields[2]
        .parse::<u64>()
        .map_err(|_| PersistenceError::new("Invalid snapshot sequence watermark"))?;
    Ok(SnapshotFormat::Versioned { watermark })
}

fn for_each_binlog_record(
    path: &Path,
    mut visitor: impl FnMut(&[u8]) -> Result<(), PersistenceError>,
) -> Result<(u64, bool), PersistenceError> {
    if !path.exists() {
        return Ok((0, false));
    }

    let file = File::open(path)?;
    let mut reader = StdBufReader::new(file);
    let mut valid_len = 0u64;
    loop {
        let record_start = valid_len;
        let mut length_bytes = [0u8; 4];
        match reader.read_exact(&mut length_bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                let file_len = fs::metadata(path)?.len();
                return Ok((record_start, file_len != record_start));
            }
            Err(error) => return Err(error.into()),
        }

        let record_len = u32::from_be_bytes(length_bytes) as usize;
        if record_len == 0 || record_len > MAX_BINLOG_RECORD_SIZE {
            return Err(PersistenceError::new(format!(
                "Invalid binlog record length: {}",
                record_len
            )));
        }
        let header_probe_length =
            record_len.min(BINLOG_RECORD_MAGIC.len() + BINLOG_RECORD_LENGTH_SIZE);
        let file_len = fs::metadata(path)?.len();
        let available_length = file_len.saturating_sub(record_start + 4);
        let readable_header_length = available_length.min(header_probe_length as u64) as usize;
        let mut header = [0u8; BINLOG_RECORD_MAGIC.len() + BINLOG_RECORD_LENGTH_SIZE];
        match reader.read_exact(&mut header[..readable_header_length]) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok((record_start, true));
            }
            Err(error) => return Err(error.into()),
        }
        let visible_header = &header[..readable_header_length];
        if readable_header_length < header_probe_length {
            if visible_header.starts_with(CHECKSUMLESS_BINLOG_RECORD_MAGIC) {
                return Err(PersistenceError::new(
                    "Cannot safely truncate an incomplete checksumless ONX3 binlog record",
                ));
            }
            if readable_header_length >= BINLOG_RECORD_MAGIC.len()
                && !visible_header.starts_with(BINLOG_RECORD_MAGIC)
            {
                return Err(PersistenceError::new(
                    "Cannot safely truncate an incomplete binlog record with unknown framing",
                ));
            }
            return Ok((record_start, true));
        }
        if visible_header.starts_with(BINLOG_RECORD_MAGIC)
            && record_len >= BINLOG_RECORD_MAGIC.len() + BINLOG_RECORD_LENGTH_SIZE
        {
            let mut offset = BINLOG_RECORD_MAGIC.len();
            let embedded_length = read_u32_be(visible_header, &mut offset)
                .expect("the fixed-size record header was read")
                as usize;
            if embedded_length != record_len {
                return Err(PersistenceError::new(format!(
                    "Binlog record length mismatch: outer length {}, embedded length {}",
                    record_len, embedded_length
                )));
            }
        }
        if available_length < record_len as u64 {
            if visible_header.starts_with(CHECKSUMLESS_BINLOG_RECORD_MAGIC) {
                return Err(PersistenceError::new(
                    "Cannot safely truncate an incomplete checksumless ONX3 binlog record",
                ));
            }
            if !visible_header.starts_with(BINLOG_RECORD_MAGIC) {
                return Err(PersistenceError::new(
                    "Cannot safely truncate an incomplete binlog record with unknown framing",
                ));
            }
            return Ok((record_start, true));
        }

        let mut record = vec![0u8; record_len];
        record[..header_probe_length].copy_from_slice(visible_header);
        match reader.read_exact(&mut record[header_probe_length..]) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok((record_start, true));
            }
            Err(error) => return Err(error.into()),
        }
        visitor(&record)?;
        valid_len = record_start + 4 + record_len as u64;
    }
}

fn inspect_binlog(path: &Path) -> Result<BinlogInspection, PersistenceError> {
    let mut inspection = BinlogInspection::default();
    let mut last_sequence: Option<u64> = None;
    let (valid_len, truncated_tail) = for_each_binlog_record(path, |record| {
        let DecodedBinlogRecord::Versioned {
            sequence,
            effects,
            integrity,
        } = decode_binlog_record(record)?;
        if let Some(previous) = last_sequence
            && previous.checked_add(1) != Some(sequence)
        {
            return Err(PersistenceError::new(format!(
                "Non-contiguous binlog sequence: {} after {}",
                sequence, previous
            )));
        }
        decode_committed_batch(effects).map_err(|error| {
            PersistenceError::new(format!(
                "Invalid committed-effect payload at binlog sequence {}: {}",
                sequence, error
            ))
        })?;
        inspection.contains_checksumless_records |=
            integrity == BinlogRecordIntegrity::ChecksumlessLegacy;
        inspection.min_sequence.get_or_insert(sequence);
        inspection.max_sequence = sequence;
        last_sequence = Some(sequence);
        Ok(())
    })?;
    inspection.valid_len = valid_len;
    inspection.truncated_tail = truncated_tail;
    Ok(inspection)
}

fn checked_u32_length(length: usize, description: &str) -> Result<u32, PersistenceError> {
    u32::try_from(length)
        .map_err(|_| PersistenceError::new(format!("{} exceeds the format limit", description)))
}

fn append_snapshot_bytes(record: &mut Vec<u8>, bytes: &[u8]) -> Result<(), PersistenceError> {
    let length = checked_u32_length(bytes.len(), "Snapshot value")?;
    write_u32_be(record, length);
    record.extend_from_slice(bytes);
    Ok(())
}

fn encode_snapshot_entry(key: &[u8], entry: &DataEntry) -> Result<Vec<u8>, PersistenceError> {
    let mut record = Vec::new();
    append_snapshot_bytes(&mut record, key)?;
    write_u64_be(&mut record, entry.expires_at.unwrap_or(0));
    match &entry.value {
        OnyxValue::Blob(value) => {
            record.push(1);
            append_snapshot_bytes(&mut record, value)?;
        }
        OnyxValue::Int(value) => {
            record.push(2);
            record.extend_from_slice(&value.to_be_bytes());
        }
        OnyxValue::Float(value) => {
            record.push(3);
            write_u64_be(&mut record, value.to_bits());
        }
        OnyxValue::List(values) => {
            record.push(4);
            write_u32_be(
                &mut record,
                u32::try_from(values.len())
                    .map_err(|_| PersistenceError::new("Snapshot list is too large"))?,
            );
            for value in values {
                append_snapshot_bytes(&mut record, value)?;
            }
        }
        OnyxValue::Hash(values) => {
            record.push(5);
            write_u32_be(
                &mut record,
                u32::try_from(values.len())
                    .map_err(|_| PersistenceError::new("Snapshot hash is too large"))?,
            );
            for (field, value) in values {
                append_snapshot_bytes(&mut record, field)?;
                append_snapshot_bytes(&mut record, value)?;
            }
        }
        OnyxValue::Set(values) => {
            record.push(6);
            write_u32_be(
                &mut record,
                u32::try_from(values.len())
                    .map_err(|_| PersistenceError::new("Snapshot set is too large"))?,
            );
            for value in values {
                append_snapshot_bytes(&mut record, value)?;
            }
        }
        OnyxValue::Json(value) => {
            record.push(7);
            let encoded = serde_json::to_vec(value)
                .map_err(|error| PersistenceError::new(error.to_string()))?;
            append_snapshot_bytes(&mut record, &encoded)?;
        }
        OnyxValue::Vector(values) => {
            record.push(8);
            write_u32_be(
                &mut record,
                u32::try_from(values.len())
                    .map_err(|_| PersistenceError::new("Snapshot vector is too large"))?,
            );
            for value in values {
                record.extend_from_slice(&value.to_bits().to_be_bytes());
            }
        }
    }
    if record.len() > MAX_SNAPSHOT_RECORD_SIZE {
        return Err(PersistenceError::new(
            "Snapshot entry exceeds the format limit",
        ));
    }
    Ok(record)
}

fn read_snapshot_bytes<'a>(record: &'a [u8], offset: &mut usize) -> Option<&'a [u8]> {
    let length = read_u32_be(record, offset)? as usize;
    let bytes = safe_slice(record, *offset, length)?;
    *offset = offset.checked_add(length)?;
    Some(bytes)
}

fn decode_snapshot_entry(record: &[u8]) -> Result<(Bytes, DataEntry), PersistenceError> {
    let mut offset = 0usize;
    let key = Bytes::copy_from_slice(
        read_snapshot_bytes(record, &mut offset)
            .ok_or_else(|| PersistenceError::new("Invalid snapshot key"))?,
    );
    let expiry = read_u64_be(record, &mut offset)
        .ok_or_else(|| PersistenceError::new("Invalid snapshot expiry"))?;
    let value_type = *record
        .get(offset)
        .ok_or_else(|| PersistenceError::new("Missing snapshot value type"))?;
    offset += 1;

    let read_values = |record: &[u8], offset: &mut usize| {
        let count = read_u32_be(record, offset)
            .ok_or_else(|| PersistenceError::new("Invalid snapshot collection count"))?;
        if count as usize > record.len().saturating_sub(*offset) / 4 {
            return Err(PersistenceError::new(
                "Snapshot collection count exceeds the record bounds",
            ));
        }
        let mut values = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let value = read_snapshot_bytes(record, offset)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot collection value"))?;
            values.push(Bytes::copy_from_slice(value));
        }
        Ok::<Vec<Bytes>, PersistenceError>(values)
    };

    let value = match value_type {
        1 => OnyxValue::Blob(Bytes::copy_from_slice(
            read_snapshot_bytes(record, &mut offset)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot blob"))?,
        )),
        2 => {
            let bytes: [u8; 8] = safe_slice(record, offset, 8)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| PersistenceError::new("Invalid snapshot integer"))?;
            offset += 8;
            OnyxValue::Int(i64::from_be_bytes(bytes))
        }
        3 => {
            let bits = read_u64_be(record, &mut offset)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot float"))?;
            OnyxValue::Float(f64::from_bits(bits))
        }
        4 => OnyxValue::List(read_values(record, &mut offset)?),
        5 => {
            let count = read_u32_be(record, &mut offset)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot hash count"))?;
            if count as usize > record.len().saturating_sub(offset) / 8 {
                return Err(PersistenceError::new(
                    "Snapshot hash count exceeds the record bounds",
                ));
            }
            let mut values = std::collections::HashMap::with_capacity(count as usize);
            for _ in 0..count {
                let field = Bytes::copy_from_slice(
                    read_snapshot_bytes(record, &mut offset)
                        .ok_or_else(|| PersistenceError::new("Invalid snapshot hash field"))?,
                );
                let value = Bytes::copy_from_slice(
                    read_snapshot_bytes(record, &mut offset)
                        .ok_or_else(|| PersistenceError::new("Invalid snapshot hash value"))?,
                );
                values.insert(field, value);
            }
            OnyxValue::Hash(values)
        }
        6 => OnyxValue::Set(read_values(record, &mut offset)?.into_iter().collect()),
        7 => {
            let bytes = read_snapshot_bytes(record, &mut offset)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot JSON value"))?;
            OnyxValue::Json(
                serde_json::from_slice(bytes)
                    .map_err(|error| PersistenceError::new(error.to_string()))?,
            )
        }
        8 => {
            let count = read_u32_be(record, &mut offset)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot vector count"))?
                as usize;
            let byte_length = count
                .checked_mul(4)
                .ok_or_else(|| PersistenceError::new("Snapshot vector length overflow"))?;
            let bytes = safe_slice(record, offset, byte_length)
                .ok_or_else(|| PersistenceError::new("Invalid snapshot vector"))?;
            let values = bytes
                .chunks_exact(4)
                .map(|chunk| {
                    f32::from_bits(u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                })
                .collect();
            offset += byte_length;
            OnyxValue::Vector(values)
        }
        _ => return Err(PersistenceError::new("Unknown snapshot value type")),
    };
    if offset != record.len() {
        return Err(PersistenceError::new("Trailing bytes in snapshot entry"));
    }

    let timestamp = now();
    Ok((
        key,
        DataEntry {
            value,
            expires_at: (expiry != 0).then_some(expiry),
            created_at: timestamp,
            last_accessed: timestamp,
        },
    ))
}

const EFFECT_PUT: u8 = 1;
const EFFECT_DELETE: u8 = 2;

fn encode_committed_batch(batch: &CommittedBatch) -> Result<Vec<u8>, PersistenceError> {
    if batch.effects.is_empty() {
        return Err(PersistenceError::new(
            "Committed-effect batch cannot be empty",
        ));
    }
    let count = u32::try_from(batch.effects.len())
        .map_err(|_| PersistenceError::new("Committed-effect batch is too large"))?;
    let mut encoded = Vec::new();
    write_u32_be(&mut encoded, count);
    for effect in &batch.effects {
        match effect {
            CommittedEffect::Put { key, entry } => {
                encoded.push(EFFECT_PUT);
                let data_entry = DataEntry {
                    value: entry.value.clone(),
                    expires_at: entry.expires_at,
                    created_at: 0,
                    last_accessed: 0,
                };
                let record = encode_snapshot_entry(key, &data_entry)?;
                append_snapshot_bytes(&mut encoded, &record)?;
            }
            CommittedEffect::Delete { key } => {
                encoded.push(EFFECT_DELETE);
                append_snapshot_bytes(&mut encoded, key)?;
            }
        }
    }
    if encoded.len() > MAX_BINLOG_RECORD_SIZE {
        return Err(PersistenceError::new(
            "Committed-effect batch exceeds the binlog record limit",
        ));
    }
    Ok(encoded)
}

fn decode_committed_batch(encoded: &[u8]) -> Result<CommittedBatch, PersistenceError> {
    let mut offset = 0usize;
    let count = read_u32_be(encoded, &mut offset)
        .ok_or_else(|| PersistenceError::new("Missing committed-effect count"))?;
    if count == 0 {
        return Err(PersistenceError::new(
            "Committed-effect batch cannot be empty",
        ));
    }
    if count as usize > encoded.len().saturating_sub(offset) / 5 {
        return Err(PersistenceError::new(
            "Committed-effect count exceeds the record bounds",
        ));
    }

    let mut effects = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let opcode = *encoded
            .get(offset)
            .ok_or_else(|| PersistenceError::new("Missing committed-effect opcode"))?;
        offset += 1;
        let payload = read_snapshot_bytes(encoded, &mut offset)
            .ok_or_else(|| PersistenceError::new("Invalid committed-effect payload"))?;
        match opcode {
            EFFECT_PUT => {
                let (key, entry) = decode_snapshot_entry(payload)?;
                effects.push(CommittedEffect::Put {
                    key,
                    entry: entry.into(),
                });
            }
            EFFECT_DELETE => effects.push(CommittedEffect::Delete {
                key: Bytes::copy_from_slice(payload),
            }),
            _ => {
                return Err(PersistenceError::new(format!(
                    "Unknown committed-effect opcode: {}",
                    opcode
                )));
            }
        }
    }
    if offset != encoded.len() {
        return Err(PersistenceError::new(
            "Trailing bytes in committed-effect batch",
        ));
    }
    CommittedBatch::new(effects)
}

fn apply_committed_batch(store: &ShardedStore, batch: &CommittedBatch) {
    for effect in &batch.effects {
        match effect {
            CommittedEffect::Put { key, entry } => {
                store
                    .engine
                    .apply_entry(key.clone(), entry.clone().into_data_entry());
            }
            CommittedEffect::Delete { key } => {
                store.engine.delete(key);
            }
        }
    }
}

fn load_snapshot_entries(
    store: &ShardedStore,
    path: &Path,
    format: SnapshotFormat,
) -> Result<usize, PersistenceError> {
    if format == SnapshotFormat::Missing {
        return Ok(0);
    }
    let file = File::open(path)?;
    let decoder = GzDecoder::new(file);
    let mut reader = StdBufReader::new(decoder);
    if matches!(format, SnapshotFormat::Versioned { .. }) {
        read_bounded_utf8_line(&mut reader, MAX_SNAPSHOT_METADATA_SIZE)?
            .ok_or_else(|| PersistenceError::new("Snapshot metadata header is missing"))?;

        let mut count = 0;
        loop {
            let mut length_bytes = [0u8; 4];
            if reader.read(&mut length_bytes[..1])? == 0 {
                break;
            }
            reader.read_exact(&mut length_bytes[1..])?;
            let record_length = u32::from_be_bytes(length_bytes) as usize;
            if record_length == 0 || record_length > MAX_SNAPSHOT_RECORD_SIZE {
                return Err(PersistenceError::new(format!(
                    "Invalid snapshot record length: {}",
                    record_length
                )));
            }
            let mut record = vec![0u8; record_length];
            reader.read_exact(&mut record)?;
            let (key, entry) = decode_snapshot_entry(&record)?;
            if !is_expired(&entry) {
                store.engine.set(key, entry.value, entry.expires_at);
                count += 1;
            }
        }
        return Ok(count);
    }

    let mut count = 0;
    let mut skipped = 0;
    while let Some(line) = read_bounded_utf8_line(&mut reader, MAX_SNAPSHOT_LINE_SIZE)? {
        match line_to_entry(&line) {
            Some((key, entry)) if !is_expired(&entry) => {
                store.set_raw(key, entry);
                count += 1;
            }
            Some(_) => {}
            None => skipped += 1,
        }
    }
    if skipped > 0 {
        warn!(
            "Legacy snapshot: {} malformed entries were skipped",
            skipped
        );
    }
    Ok(count)
}

fn load_data_from_paths(
    store: &ShardedStore,
    paths: &PersistencePaths,
) -> Result<RecoveryState, PersistenceError> {
    CURRENT_TIME.store(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| PersistenceError::new(error.to_string()))?
            .as_secs(),
        Ordering::SeqCst,
    );

    let snapshot_path = if paths.snapshot.exists() {
        &paths.snapshot
    } else if paths.snapshot_backup.exists() {
        warn!(
            "Primary snapshot is missing; recovering from {}",
            paths.snapshot_backup.display()
        );
        &paths.snapshot_backup
    } else {
        &paths.snapshot
    };
    let snapshot_format = inspect_snapshot(snapshot_path)?;
    if snapshot_format == SnapshotFormat::Legacy {
        return Err(PersistenceError::new(
            "Unsupported unsafe legacy snapshot format; create a verified versioned snapshot before upgrading",
        ));
    }
    let binlog = inspect_binlog(&paths.binlog)?;
    if binlog.contains_checksumless_records {
        warn!(
            "Recovery accepted structurally valid checksumless ONX3 records; compact the dataset to replace this legacy recovery history"
        );
    }
    let snapshot_watermark = match snapshot_format {
        SnapshotFormat::Versioned { watermark } => watermark,
        SnapshotFormat::Missing => 0,
        SnapshotFormat::Legacy => unreachable!(),
    };
    if let Some(first_sequence) = binlog.min_sequence
        && first_sequence > snapshot_watermark.saturating_add(1)
    {
        return Err(PersistenceError::new(
            "Binlog begins after the snapshot recovery boundary",
        ));
    }

    let staging = ShardedStore::new();
    let snapshot_count = load_snapshot_entries(&staging, snapshot_path, snapshot_format)?;
    info!("Snapshot loaded: {} active entries", snapshot_count);

    let mut replayed = 0usize;
    for_each_binlog_record(&paths.binlog, |record| {
        let DecodedBinlogRecord::Versioned {
            sequence, effects, ..
        } = decode_binlog_record(record)?;
        if sequence <= snapshot_watermark {
            return Ok(());
        }
        let batch = decode_committed_batch(effects)?;
        apply_committed_batch(&staging, &batch);
        replayed += 1;
        Ok(())
    })?;

    if binlog.truncated_tail {
        warn!(
            "Truncating incomplete binlog tail at byte {}",
            binlog.valid_len
        );
        let file = OpenOptions::new().write(true).open(&paths.binlog)?;
        file.set_len(binlog.valid_len)?;
        file.sync_all()?;
    }
    store.engine.replace_all(staging.engine.snapshot_all());
    info!("Binlog replayed: {} commands", replayed);

    Ok(RecoveryState {
        last_sequence: snapshot_watermark.max(binlog.max_sequence),
        snapshot_watermark,
    })
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

    loop {
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

        // Normalizza il nome del comando in maiuscolo UNA VOLTA sola, in-place
        // (make_ascii_uppercase non alloca): prima il match in execute_command
        // era case-sensitive, quindi "set" minuscolo non funzionava — un vero
        // bug di compatibilità, non solo una questione di stile. Farlo qui,
        // prima di ogni uso, garantisce che dispatch, coda di EXEC, e stream
        // di replica vedano tutti la stessa versione normalizzata.
        args[0].make_ascii_uppercase();
        let cmd = args[0].as_str();

        // AUTH: sia `AUTH password` (utente "default") sia `AUTH utente password`
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
        // Autenticazione: da qui in poi, se serve login e non è stato fatto,
        // nessun altro comando è ammesso (nemmeno MULTI/EXEC/SYNC/SUBSCRIBE).
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

        // Se in transazione, accoda
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

        // PUBLISH — non richiede modalità speciale, un comando normale come
        // gli altri, solo che non passa da execute_command/persistenza (i
        // messaggi pub/sub sono effimeri, non finiscono nel binlog).
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

        // SUBSCRIBE — entra in "modalità pub/sub": da qui la connessione
        // smette di eseguire comandi normali e resta bloccata a inoltrare
        // messaggi dei canali sottoscritti, finché il client si disconnette.
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

            // Task dedicato a leggere ulteriori SUBSCRIBE/UNSUBSCRIBE dal
            // client mentre restiamo in ascolto dei messaggi pubblicati.
            // Stesso schema usato per gli ACK delle Repliche: un task
            // possiede la metà "lettura", il ciclo principale sceglie tra
            // canali cancel-safe (mpsc/broadcast), mai su read_line diretto.
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
                            // Altri comandi vengono ignorati finché si è in
                            // modalità pub/sub (limitazione nota, come nella
                            // sottoscrizione RESP2 di base di Redis).
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
                            None => break, // connessione chiusa dal client
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
                    let current_offset = persistence.repl_offset.load(Ordering::SeqCst);
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
                if buf_writer.write_all(marker.as_bytes()).await.is_err() {
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
                    (
                        persistence.repl_offset.load(Ordering::SeqCst),
                        store.engine.snapshot_all(),
                    )
                };
                let marker = format!(
                    "+FULLRESYNC3 {} {} {}\r\n",
                    repl_id(),
                    full_sync_offset,
                    snapshot_entries.len()
                );
                if buf_writer.write_all(marker.as_bytes()).await.is_err() {
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
            if buf_writer
                .write_all(syncdone_marker.as_bytes())
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
            let _ = buf_writer.flush().await;

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
                        if buf_writer.write_all(frame.as_bytes()).await.is_err()
                            || buf_writer.flush().await.is_err()
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
                                let _ = buf_writer.flush().await;
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
            let (mut resp, _is_write) = execute_ordered_command(&store, &persistence, &args).await;
            if cmd.eq_ignore_ascii_case("INFO")
                && let RESPValue::BulkString(Some(ref mut text)) = resp
            {
                let repl_offset = persistence.repl_offset.load(Ordering::SeqCst);
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
async fn time_updater_task() {
    loop {
        let now_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        CURRENT_TIME.store(now_sec, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> std::io::Result<()> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()
}

#[cfg(windows)]
fn sync_parent_directory(_path: &Path) -> std::io::Result<()> {
    // Windows metadata durability is provided by durable_rename using
    // MOVEFILE_WRITE_THROUGH. FlushFileBuffers on a directory handle is not
    // consistently supported and returns access denied on common systems.
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn sync_parent_directory(_path: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Directory synchronization is unsupported on this platform",
    ))
}

#[cfg(windows)]
fn durable_rename(from: &Path, to: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
    #[link(name = "Kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(existing: *const u16, replacement: *const u16, flags: u32) -> i32;
    }

    let existing: Vec<u16> = from.as_os_str().encode_wide().chain(Some(0)).collect();
    let replacement: Vec<u16> = to.as_os_str().encode_wide().chain(Some(0)).collect();
    let result = unsafe {
        MoveFileExW(
            existing.as_ptr(),
            replacement.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
fn durable_rename(from: &Path, to: &Path) -> std::io::Result<()> {
    fs::rename(from, to)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReplicaIdentity {
    replid: u64,
    baseline_sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DurableReplicaState {
    Detached,
    Installing,
    Ready(ReplicaIdentity),
}

fn load_durable_replica_state(
    paths: &PersistencePaths,
    snapshot_watermark: u64,
) -> Result<Option<DurableReplicaState>, PersistenceError> {
    if !paths.replica_state.exists() {
        return Ok(None);
    }
    let metadata = fs::metadata(&paths.replica_state)?;
    if metadata.len() > MAX_SNAPSHOT_METADATA_SIZE as u64 {
        return Err(PersistenceError::new(
            "Replica identity metadata exceeds the format limit",
        ));
    }
    let contents = fs::read_to_string(&paths.replica_state)?;
    let fields: Vec<&str> = contents.trim_end().split('\t').collect();
    if fields.len() != 5 || fields[0] != REPLICA_STATE_MAGIC {
        return Err(PersistenceError::new("Malformed durable replica state"));
    }
    let version = fields[1]
        .parse::<u8>()
        .map_err(|_| PersistenceError::new("Invalid durable replica state version"))?;
    if version != REPLICA_STATE_VERSION {
        return Err(PersistenceError::new(format!(
            "Unsupported durable replica state version: {}",
            version
        )));
    }
    let replid = fields[3]
        .parse::<u64>()
        .map_err(|_| PersistenceError::new("Invalid upstream replication ID"))?;
    let baseline_sequence = fields[4]
        .parse::<u64>()
        .map_err(|_| PersistenceError::new("Invalid replica baseline sequence"))?;
    match fields[2] {
        "DETACHED" if replid == 0 && baseline_sequence == 0 => {
            Ok(Some(DurableReplicaState::Detached))
        }
        "INSTALLING" if replid == 0 && baseline_sequence == 0 => {
            Ok(Some(DurableReplicaState::Installing))
        }
        "READY" if replid != 0 => {
            if baseline_sequence != snapshot_watermark {
                warn!(
                    "Replica identity does not match the installed snapshot; forcing a full synchronization"
                );
                Ok(Some(DurableReplicaState::Installing))
            } else {
                Ok(Some(DurableReplicaState::Ready(ReplicaIdentity {
                    replid,
                    baseline_sequence,
                })))
            }
        }
        _ => Err(PersistenceError::new(
            "Invalid durable replica state fields",
        )),
    }
}

fn write_durable_replica_state(
    paths: &PersistencePaths,
    state: DurableReplicaState,
) -> Result<(), PersistenceError> {
    let (status, replid, baseline_sequence) = match state {
        DurableReplicaState::Detached => ("DETACHED", 0, 0),
        DurableReplicaState::Installing => ("INSTALLING", 0, 0),
        DurableReplicaState::Ready(identity) => {
            ("READY", identity.replid, identity.baseline_sequence)
        }
    };
    let mut file = File::create(&paths.replica_state_temp)?;
    writeln!(
        file,
        "{}\t{}\t{}\t{}\t{}",
        REPLICA_STATE_MAGIC, REPLICA_STATE_VERSION, status, replid, baseline_sequence
    )?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    durable_rename(&paths.replica_state_temp, &paths.replica_state)?;
    sync_parent_directory(&paths.replica_state)?;
    Ok(())
}

fn write_replica_identity(
    paths: &PersistencePaths,
    identity: ReplicaIdentity,
) -> Result<(), PersistenceError> {
    write_durable_replica_state(paths, DurableReplicaState::Ready(identity))
}

fn write_replica_installing(paths: &PersistencePaths) -> Result<(), PersistenceError> {
    write_durable_replica_state(paths, DurableReplicaState::Installing)
}

fn write_replica_detached(paths: &PersistencePaths) -> Result<(), PersistenceError> {
    write_durable_replica_state(paths, DurableReplicaState::Detached)
}

#[cfg(test)]
fn load_replica_identity(
    paths: &PersistencePaths,
    snapshot_watermark: u64,
) -> Result<Option<ReplicaIdentity>, PersistenceError> {
    Ok(
        match load_durable_replica_state(paths, snapshot_watermark)? {
            Some(DurableReplicaState::Ready(identity)) => Some(identity),
            Some(DurableReplicaState::Detached | DurableReplicaState::Installing) | None => None,
        },
    )
}

fn prepare_replication_startup(
    paths: &PersistencePaths,
    snapshot_watermark: u64,
    configured_as_replica: bool,
) -> Result<Option<ReplicaIdentity>, PersistenceError> {
    let state = load_durable_replica_state(paths, snapshot_watermark)?;
    if configured_as_replica {
        return Ok(match state {
            Some(DurableReplicaState::Ready(identity)) => Some(identity),
            Some(DurableReplicaState::Detached | DurableReplicaState::Installing) | None => None,
        });
    }
    match state {
        Some(DurableReplicaState::Installing) => Err(PersistenceError::new(
            "Cannot start as master while replica baseline installation is incomplete",
        )),
        Some(DurableReplicaState::Ready(_)) => {
            write_replica_detached(paths)?;
            Ok(None)
        }
        Some(DurableReplicaState::Detached) | None => Ok(None),
    }
}

fn write_snapshot_file(
    entries: Vec<(Bytes, DataEntry)>,
    watermark: u64,
    paths: &PersistencePaths,
) -> Result<(), PersistenceError> {
    let file = File::create(&paths.snapshot_temp)?;
    let mut encoder = GzEncoder::new(BufWriter::new(file), Compression::default());
    writeln!(
        encoder,
        "{}\t{}\t{}",
        SNAPSHOT_MAGIC, SNAPSHOT_VERSION, watermark
    )?;
    for (key, entry) in entries {
        let record = encode_snapshot_entry(&key, &entry)?;
        let record_length = u32::try_from(record.len())
            .map_err(|_| PersistenceError::new("Snapshot entry exceeds the format limit"))?;
        encoder.write_all(&record_length.to_be_bytes())?;
        encoder.write_all(&record)?;
    }
    let mut writer = encoder.finish()?;
    writer.flush()?;
    let snapshot_file = writer
        .into_inner()
        .map_err(|error| PersistenceError::new(error.into_error().to_string()))?;
    snapshot_file.sync_all()?;
    drop(snapshot_file);

    if paths.snapshot.exists() {
        durable_rename(&paths.snapshot, &paths.snapshot_backup)?;
        sync_parent_directory(&paths.snapshot)?;
    }

    if let Err(error) = durable_rename(&paths.snapshot_temp, &paths.snapshot) {
        if !paths.snapshot.exists() && paths.snapshot_backup.exists() {
            let _ = durable_rename(&paths.snapshot_backup, &paths.snapshot);
            let _ = sync_parent_directory(&paths.snapshot);
        }
        return Err(error.into());
    }
    sync_parent_directory(&paths.snapshot)?;
    Ok(())
}

async fn request_log_flush(persistence: &Persistence) -> Result<(), PersistenceError> {
    let (completion_tx, completion_rx) = oneshot::channel();
    persistence
        .log_tx
        .send(LogMessage::Flush {
            completion: completion_tx,
        })
        .await
        .map_err(|_| PersistenceError::new("Binlog worker is unavailable"))?;
    completion_rx
        .await
        .map_err(|_| PersistenceError::new("Binlog flush completion was dropped"))?
        .map_err(PersistenceError::new)
}

async fn request_log_sync_data(persistence: &Persistence) -> Result<(), PersistenceError> {
    let (completion_tx, completion_rx) = oneshot::channel();
    persistence
        .log_tx
        .send(LogMessage::SyncData {
            completion: completion_tx,
        })
        .await
        .map_err(|_| PersistenceError::new("Binlog worker is unavailable"))?;
    completion_rx
        .await
        .map_err(|_| PersistenceError::new("Binlog sync completion was dropped"))?
        .map_err(PersistenceError::new)
}

async fn run_periodic_sync_once(persistence: &Persistence) -> Result<(), PersistenceError> {
    match request_log_sync_data(persistence).await {
        Ok(()) => Ok(()),
        Err(error) => {
            mark_persistence_failed(
                persistence,
                format!("Periodic binlog sync failed: {}", error),
            );
            Err(error)
        }
    }
}

async fn request_log_truncate(persistence: &Persistence) -> Result<(), PersistenceError> {
    let (completion_tx, completion_rx) = oneshot::channel();
    persistence
        .log_tx
        .send(LogMessage::Truncate {
            completion: completion_tx,
        })
        .await
        .map_err(|_| PersistenceError::new("Binlog worker is unavailable"))?;
    completion_rx
        .await
        .map_err(|_| PersistenceError::new("Binlog truncate completion was dropped"))?
        .map_err(PersistenceError::new)
}

async fn run_binlog_worker(
    mut receiver: mpsc::Receiver<LogMessage>,
    binlog: Arc<std::sync::Mutex<File>>,
) {
    while let Some(message) = receiver.recv().await {
        match message {
            LogMessage::Append {
                sequence,
                record,
                completion,
            } => {
                let result = (|| -> Result<(), PersistenceError> {
                    let encoded = encode_versioned_binlog_record(sequence, &record)?;
                    let length = u32::try_from(encoded.len()).map_err(|_| {
                        PersistenceError::new("Binlog record exceeds the format limit")
                    })?;
                    let mut file = binlog
                        .lock()
                        .map_err(|_| PersistenceError::new("Binlog file lock is poisoned"))?;
                    file.seek(SeekFrom::End(0))?;
                    file.write_all(&length.to_be_bytes())?;
                    file.write_all(&encoded)?;
                    file.flush()?;
                    if fsync_policy() == FsyncPolicy::Always {
                        file.sync_data()?;
                    }
                    Ok(())
                })();
                let _ = completion.send(result.map_err(|error| error.to_string()));
            }
            LogMessage::Flush { completion } => {
                let result = (|| -> Result<(), PersistenceError> {
                    let mut file = binlog
                        .lock()
                        .map_err(|_| PersistenceError::new("Binlog file lock is poisoned"))?;
                    file.flush()?;
                    file.sync_all()?;
                    Ok(())
                })();
                let _ = completion.send(result.map_err(|error| error.to_string()));
            }
            LogMessage::SyncData { completion } => {
                let result = (|| -> Result<(), PersistenceError> {
                    let file = binlog
                        .lock()
                        .map_err(|_| PersistenceError::new("Binlog file lock is poisoned"))?;
                    file.sync_data()?;
                    Ok(())
                })();
                let _ = completion.send(result.map_err(|error| error.to_string()));
            }
            LogMessage::Truncate { completion } => {
                let result = (|| -> Result<(), PersistenceError> {
                    let mut file = binlog
                        .lock()
                        .map_err(|_| PersistenceError::new("Binlog file lock is poisoned"))?;
                    file.flush()?;
                    file.set_len(0)?;
                    file.seek(SeekFrom::Start(0))?;
                    file.sync_all()?;
                    Ok(())
                })();
                let _ = completion.send(result.map_err(|error| error.to_string()));
            }
        }
    }
}

async fn compact_store(
    store: &Arc<ShardedStore>,
    persistence: &Arc<Persistence>,
) -> Result<u64, PersistenceError> {
    let _write_guard = persistence.write_gate.lock().await;
    request_log_flush(persistence)
        .await
        .map_err(|error| PersistenceError::new(format!("Binlog flush failed: {}", error)))?;

    let watermark = persistence.repl_offset.load(Ordering::SeqCst);
    let entries = store.engine.snapshot_all();
    let paths = persistence.paths.clone();
    tokio::task::spawn_blocking(move || write_snapshot_file(entries, watermark, &paths))
        .await
        .map_err(|error| PersistenceError::new(format!("Snapshot task failed: {}", error)))?
        .map_err(|error| {
            PersistenceError::new(format!("Snapshot installation failed: {}", error))
        })?;

    request_log_truncate(persistence)
        .await
        .map_err(|error| PersistenceError::new(format!("Binlog rotation failed: {}", error)))?;
    let upstream_replid = persistence.upstream_replid.load(Ordering::SeqCst);
    if upstream_replid != 0 {
        write_replica_identity(
            &persistence.paths,
            ReplicaIdentity {
                replid: upstream_replid,
                baseline_sequence: watermark,
            },
        )?;
    }
    persistence.write_count.store(0, Ordering::SeqCst);
    info!(
        "Compaction complete at sequence {}: snapshot installed and binlog truncated",
        watermark
    );
    Ok(watermark)
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

    let repl_offset = persistence.repl_offset.load(Ordering::SeqCst);
    let statuses = persistence.replica_status.lock().unwrap();
    let connected_replicas = statuses.len();
    let max_lag = statuses
        .values()
        .map(|s| repl_offset.saturating_sub(s.last_ack_offset))
        .max()
        .unwrap_or(0);
    drop(statuses);

    format!(
        "# HELP onyxdb_uptime_seconds Tempo di attivita' del server in secondi\n\
         # TYPE onyxdb_uptime_seconds counter\n\
         onyxdb_uptime_seconds {}\n\
         # HELP onyxdb_keys_total Numero di chiavi attualmente presenti\n\
        TYPE onyxdb_keys_total gauge\n\
         onyxdb_keys_total {}\n\
         # HELP onyxdb_active_connections Numero di connessioni client attive\n\
         # TYPE onyxdb_active_connections gauge\n\
         onyxdb_active_connections {}\n\
         # HELP onyxdb_commands_total Numero totale di comandi eseguiti\n\
         # TYPE onyxdb_commands_total counter\n\
         onyxdb_commands_total {}\n\
         # HELP onyxdb_cache_hits_total Numero di letture andate a buon fine\n\
         # TYPE onyxdb_cache_hits_total counter\n\
         onyxdb_cache_hits_total {}\n\
         # HELP onyxdb_cache_misses_total Numero di letture su chiavi inesistenti\n\
         # TYPE onyxdb_cache_misses_total counter\n\
         onyxdb_cache_misses_total {}\n\
         # HELP onyxdb_is_master 1 se questa istanza e' un Master, 0 se e' una Replica\n\
         # TYPE onyxdb_is_master gauge\n\
         onyxdb_is_master {}\n\
         # HELP onyxdb_replication_offset Offset di replicazione corrente (numero di comandi replicati)\n\
         # TYPE onyxdb_replication_offset counter\n\
         onyxdb_replication_offset {}\n\
         # HELP onyxdb_connected_replicas Numero di Replica attualmente connesse\n\
         # TYPE onyxdb_connected_replicas gauge\n\
         onyxdb_connected_replicas {}\n\
         # HELP onyxdb_max_replica_lag Quanto e' indietro la Replica piu' lenta, in numero di comandi\n\
         # TYPE onyxdb_max_replica_lag gauge\n\
         onyxdb_max_replica_lag {}\n\
         # HELP onyxdb_memory_bytes Byte occupati (stima approssimativa)\n\
         # TYPE onyxdb_memory_bytes gauge\n\
         onyxdb_memory_bytes {}\n",
        uptime,
        num_keys,
        active_conns,
        total_cmds,
        hits,
        misses,
        role_value,
        repl_offset,
        connected_replicas,
        max_lag,
        store.used_memory_bytes()
    )
}

async fn run_metrics_server(store: Arc<ShardedStore>, persistence: Arc<Persistence>, port: u16) {
    let addr = format!("127.0.0.1:{}", port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Unable to start metrics server on {}: {}", addr, e);
            return;
        }
    };
    info!(
        "Prometheus metrics server listening on http://{}/metrics",
        addr
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
// HANDLER OBP (Onyx Binary Protocol) - Listener parallelo
// ============================================================

async fn handle_obp_client(
    stream: TcpStream,
    store: Arc<ShardedStore>,
    persistence: Arc<Persistence>,
) {
    let _ = stream.set_nodelay(true);
    let peer_address = stream.peer_addr().ok();
    let (reader, writer) = stream.into_split();
    let mut buf_reader = TokioBufReader::with_capacity(65536, reader);
    let mut buf_writer = TokioBufWriter::with_capacity(8192, writer);
    let mut buf = bytes::BytesMut::with_capacity(4096);
    let mut read_buffer = [0u8; 8192];
    let mut authenticated = !auth_required();
    'connection: loop {
        match buf_reader.read(&mut read_buffer).await {
            Ok(0) => break,
            Ok(bytes_read) => {
                if buf.len().saturating_add(bytes_read) > MAX_OBP_FRAME_SIZE + read_buffer.len() {
                    warn!("Closing OBP connection with an oversized incomplete frame");
                    break;
                }
                buf.extend_from_slice(&read_buffer[..bytes_read]);
            }
            Err(_) => break,
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
            if response.encode(&mut out).is_err() || buf_writer.write_all(&out).await.is_err() {
                return;
            }
            wrote_response = true;
        }

        if wrote_response && buf_writer.flush().await.is_err() {
            return;
        }
    }

    let _ = buf_writer.flush().await;
}

async fn append_committed_batch(
    persistence: &Persistence,
    sequence: u64,
    batch: &CommittedBatch,
) -> Result<(), PersistenceError> {
    let effect_record = encode_committed_batch(batch)?;

    let (completion_tx, completion_rx) = oneshot::channel();
    persistence
        .log_tx
        .send(LogMessage::Append {
            sequence,
            record: effect_record,
            completion: completion_tx,
        })
        .await
        .map_err(|_| PersistenceError::new("Binlog worker is unavailable"))?;
    completion_rx
        .await
        .map_err(|_| PersistenceError::new("Binlog append completion was dropped"))?
        .map_err(PersistenceError::new)?;
    Ok(())
}

fn record_persisted_write(persistence: &Persistence) -> bool {
    persistence.write_count.fetch_add(1, Ordering::SeqCst) + 1 >= COMPACTION_THRESHOLD
        && persistence
            .compaction_pending
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
}

/// Persists and publishes one already-applied mutation at its assigned
/// sequence. The caller must hold the authoritative write gate.
async fn persist_ordered_mutation(
    persistence: &Persistence,
    sequence: u64,
    batch: &CommittedBatch,
) -> Result<bool, PersistenceError> {
    append_committed_batch(persistence, sequence, batch).await?;
    // The exact same committed batch is published to the backlog and live
    // replication only after its binlog append has been acknowledged.
    {
        let mut backlog = persistence.backlog.lock().unwrap();
        backlog.push_back((sequence, batch.clone()));
        while backlog.len() > BACKLOG_CAPACITY {
            backlog.pop_front();
        }
    }
    let _ = persistence.replica_tx.send((sequence, batch.clone()));

    Ok(record_persisted_write(persistence))
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
    let write_guard = persistence.write_gate.lock().await;
    let _visibility_guard = persistence.visibility_gate.write().await;
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
    let current = persistence.repl_offset.load(Ordering::SeqCst);
    let expected = current
        .checked_add(1)
        .ok_or_else(|| PersistenceError::new("Replica sequence is exhausted"))?;
    if sequence != expected {
        return Err(PersistenceError::new(format!(
            "Replication sequence mismatch: expected {}, received {}",
            expected, sequence
        )));
    }
    if let Err(error) = append_committed_batch(persistence, sequence, batch).await {
        mark_persistence_failed(
            persistence,
            format!(
                "Unable to persist replicated effect at sequence {}: {}",
                sequence, error
            ),
        );
        return Err(error);
    }
    apply_committed_batch(store, batch);
    persistence.repl_offset.store(sequence, Ordering::SeqCst);
    let should_compact = record_persisted_write(persistence);
    drop(write_guard);
    schedule_compaction(store, persistence, should_compact);
    Ok(())
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
    let _write_guard = persistence.write_gate.lock().await;
    let _visibility_guard = persistence.visibility_gate.write().await;
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
    request_log_flush(persistence).await?;
    // Invalidate promotability durably before truncating the old incremental
    // history. A crash from this point until the new identity is installed
    // must force another full synchronization rather than promote an older
    // snapshot whose post-boundary log may already be gone.
    write_replica_installing(&persistence.paths)?;
    request_log_truncate(persistence).await?;
    let entries = staging.engine.snapshot_all();
    let snapshot_entries = entries.clone();
    let paths = persistence.paths.clone();
    tokio::task::spawn_blocking(move || write_snapshot_file(snapshot_entries, sequence, &paths))
        .await
        .map_err(|error| {
            PersistenceError::new(format!("Replica snapshot task failed: {}", error))
        })??;
    write_replica_identity(
        &persistence.paths,
        ReplicaIdentity {
            replid,
            baseline_sequence: sequence,
        },
    )?;

    store.engine.replace_all(entries);
    persistence.repl_offset.store(sequence, Ordering::SeqCst);
    persistence.upstream_replid.store(replid, Ordering::SeqCst);
    persistence.replication_ready.store(true, Ordering::SeqCst);
    persistence.write_count.store(0, Ordering::SeqCst);
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
    let _write_guard = persistence.write_gate.lock().await;
    if !persistence.replication_ready.load(Ordering::SeqCst) {
        return Err(PersistenceError::new(
            "Replica is not durably synchronized and cannot be promoted",
        ));
    }
    request_log_flush(persistence).await?;
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
                .map(|args| execute_command(store, args).0)
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
                        execute_command(store, args).0
                    }
                })
                .collect(),
        );
    }

    let write_guard = persistence.write_gate.lock().await;
    let _visibility_guard = persistence.visibility_gate.write().await;
    if !persistence.accepting_writes.load(Ordering::SeqCst) {
        return RESPValue::Error(persistence_unavailable_message(persistence));
    }
    let current_sequence = persistence.repl_offset.load(Ordering::SeqCst);
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
            results.push(execute_command(store, args).0);
            continue;
        }

        let affected_keys = persistent_keys_for_command(args);
        let before = capture_entries(store, &affected_keys);
        for key in &affected_keys {
            if changed_key_set.insert(key.clone()) {
                changed_keys.push(key.clone());
                baseline.insert(key.clone(), before.get(key).cloned().flatten());
            }
        }
        let memory_before = store.used_memory_bytes();
        let key_count_before = store.engine.stats().total_keys;
        let (response, _) = execute_command(store, args);
        if derive_committed_batch(store, &affected_keys, &before, &[]).is_none() {
            results.push(response);
            continue;
        }

        let protected_keys = affected_keys.iter().cloned().collect::<HashSet<_>>();
        match enforce_write_admission(store, memory_before, key_count_before, &protected_keys) {
            Ok(evicted_entries) => {
                for (key, entry) in evicted_entries {
                    if changed_key_set.insert(key.clone()) {
                        changed_keys.push(key.clone());
                        baseline.insert(key, Some(entry));
                    }
                }
                results.push(response);
            }
            Err(error) => {
                rollback_attempted_mutation(store, &before, &[]);
                results.push(RESPValue::Error(error.message().to_string()));
            }
        }
    }

    changed_keys.sort();
    let Some(batch) = derive_committed_batch(store, &changed_keys, &baseline, &[]) else {
        drop(write_guard);
        return RESPValue::Array(results);
    };

    let sequence = current_sequence + 1;
    persistence.repl_offset.store(sequence, Ordering::SeqCst);
    match persist_ordered_mutation(persistence, sequence, &batch).await {
        Ok(should_compact) => {
            drop(write_guard);
            schedule_compaction(store, persistence, should_compact);
            RESPValue::Array(results)
        }
        Err(error) => {
            rollback_attempted_mutation(store, &baseline, &[]);
            persistence
                .repl_offset
                .store(current_sequence, Ordering::SeqCst);
            mark_persistence_failed(
                persistence,
                format!(
                    "Transaction persistence failed at sequence {}: {}",
                    sequence, error
                ),
            );
            drop(write_guard);
            RESPValue::Error(format!("MISCONF transaction persistence failed: {}", error))
        }
    }
}

async fn execute_ordered_command(
    store: &Arc<ShardedStore>,
    persistence: &Arc<Persistence>,
    args: &[String],
) -> (RESPValue, bool) {
    let command = args.first().map(|value| value.as_str()).unwrap_or("");
    if !is_write_command(command) {
        let _visibility_guard = persistence.visibility_gate.read().await;
        return execute_command(store, args);
    }

    let write_guard = persistence.write_gate.lock().await;
    let _visibility_guard = persistence.visibility_gate.write().await;
    if !persistence.accepting_writes.load(Ordering::SeqCst) {
        return (
            RESPValue::Error(persistence_unavailable_message(persistence)),
            false,
        );
    }
    let current_sequence = persistence.repl_offset.load(Ordering::SeqCst);
    if current_sequence == u64::MAX {
        mark_persistence_failed(persistence, "Persistence sequence is exhausted");
        return (
            RESPValue::Error("MISCONF persistence sequence is exhausted".to_string()),
            false,
        );
    }

    let affected_keys = persistent_keys_for_command(args);
    let before = capture_entries(store, &affected_keys);
    let memory_before = store.used_memory_bytes();
    let key_count_before = store.engine.stats().total_keys;
    let (response, _) = execute_command(store, args);
    if derive_committed_batch(store, &affected_keys, &before, &[]).is_none() {
        drop(write_guard);
        return (response, false);
    }

    let protected_keys = affected_keys.iter().cloned().collect::<HashSet<_>>();
    let evicted_entries =
        match enforce_write_admission(store, memory_before, key_count_before, &protected_keys) {
            Ok(entries) => entries,
            Err(error) => {
                rollback_attempted_mutation(store, &before, &[]);
                drop(write_guard);
                return (RESPValue::Error(error.message().to_string()), false);
            }
        };
    let committed_batch = derive_committed_batch(store, &affected_keys, &before, &evicted_entries);
    let is_write = committed_batch.is_some();
    let mut should_compact = false;
    if let Some(batch) = committed_batch {
        let sequence = current_sequence + 1;
        persistence.repl_offset.store(sequence, Ordering::SeqCst);
        match persist_ordered_mutation(persistence, sequence, &batch).await {
            Ok(value) => should_compact = value,
            Err(error) => {
                rollback_attempted_mutation(store, &before, &evicted_entries);
                persistence
                    .repl_offset
                    .store(current_sequence, Ordering::SeqCst);
                mark_persistence_failed(
                    persistence,
                    format!(
                        "Mutation persistence failed at sequence {}: {}",
                        sequence, error
                    ),
                );
                drop(write_guard);
                return (
                    RESPValue::Error(format!("MISCONF mutation persistence failed: {}", error)),
                    false,
                );
            }
        }
    }
    drop(write_guard);
    schedule_compaction(store, persistence, should_compact);
    (response, is_write)
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

    // AUTH via OBP (codice 0x10): arg[0]=password, oppure arg[0]=utente e arg[1]=password.
    if cmd == 0x10 {
        let (user, pass) = if args.len() >= 2 {
            (
                String::from_utf8_lossy(&args[0]).to_string(),
                String::from_utf8_lossy(&args[1]).to_string(),
            )
        } else {
            (
                "default".to_string(),
                args.first()
                    .map(|a| String::from_utf8_lossy(a).to_string())
                    .unwrap_or_default(),
            )
        };
        let ok = auth_required() && check_credentials(&user, &pass);
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

    // Se serve login e non è stato fatto, rifiuta qualsiasi altro comando.
    if !*authenticated {
        return OBPFrame {
            cmd: 0x00,
            flags: 0,
            correlation_id: frame.correlation_id,
            args: Vec::new(),
            payload: Some(Bytes::from("NOAUTH auth richiesta")),
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
                        .engine
                        .get(key)
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
                let write_guard = persistence.write_gate.lock().await;
                let _visibility_guard = persistence.visibility_gate.write().await;
                if !persistence.accepting_writes.load(Ordering::SeqCst) {
                    return OBPFrame {
                        cmd: 0x00,
                        flags: 0,
                        correlation_id: frame.correlation_id,
                        args: Vec::new(),
                        payload: Some(Bytes::from("MISCONF persistence is unavailable")),
                    };
                }
                let current_sequence = persistence.repl_offset.load(Ordering::SeqCst);
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
                let before =
                    std::collections::HashMap::from([(key.clone(), store.engine.peek(&key))]);
                let memory_before = store.used_memory_bytes();
                let key_count_before = store.engine.stats().total_keys;
                let value = OnyxValue::Blob(args[1].clone());
                store.engine.set(key.clone(), value, None);
                let protected_keys = HashSet::from([key.clone()]);
                let evicted_entries = match enforce_write_admission(
                    store,
                    memory_before,
                    key_count_before,
                    &protected_keys,
                ) {
                    Ok(entries) => entries,
                    Err(error) => {
                        rollback_attempted_mutation(store, &before, &[]);
                        return OBPFrame {
                            cmd: 0x00,
                            flags: 0,
                            correlation_id: frame.correlation_id,
                            args: Vec::new(),
                            payload: Some(Bytes::from(error.message())),
                        };
                    }
                };
                let entry = store
                    .engine
                    .peek(&key)
                    .map(PersistentEntry::from)
                    .expect("OBP SET must leave the committed key present");
                let mut effects = evicted_entries
                    .iter()
                    .map(|(evicted_key, _)| CommittedEffect::Delete {
                        key: evicted_key.clone(),
                    })
                    .collect::<Vec<_>>();
                effects.push(CommittedEffect::Put { key, entry });
                let batch = CommittedBatch { effects };
                let sequence = current_sequence + 1;
                persistence.repl_offset.store(sequence, Ordering::SeqCst);
                let persistence_result =
                    persist_ordered_mutation(persistence, sequence, &batch).await;
                drop(write_guard);
                match persistence_result {
                    Ok(should_compact) => {
                        schedule_compaction(store, persistence, should_compact);
                    }
                    Err(error) => {
                        rollback_attempted_mutation(store, &before, &evicted_entries);
                        persistence
                            .repl_offset
                            .store(current_sequence, Ordering::SeqCst);
                        mark_persistence_failed(
                            persistence,
                            format!(
                                "OBP mutation persistence failed at sequence {}: {}",
                                sequence, error
                            ),
                        );
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
            } else {
                (OnyxValue::Blob(Bytes::from("ERR")), false)
            }
        }
        0x03 => {
            if let Some(key) = args.first() {
                let write_guard = persistence.write_gate.lock().await;
                let _visibility_guard = persistence.visibility_gate.write().await;
                if !persistence.accepting_writes.load(Ordering::SeqCst) {
                    return OBPFrame {
                        cmd: 0x00,
                        flags: 0,
                        correlation_id: frame.correlation_id,
                        args: Vec::new(),
                        payload: Some(Bytes::from("MISCONF persistence is unavailable")),
                    };
                }
                let current_sequence = persistence.repl_offset.load(Ordering::SeqCst);
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
                let before =
                    std::collections::HashMap::from([(key.clone(), store.engine.peek(key))]);
                let deleted = store.engine.delete(key);
                if deleted {
                    let batch = CommittedBatch {
                        effects: vec![CommittedEffect::Delete { key: key.clone() }],
                    };
                    let sequence = current_sequence + 1;
                    persistence.repl_offset.store(sequence, Ordering::SeqCst);
                    let persistence_result =
                        persist_ordered_mutation(persistence, sequence, &batch).await;
                    drop(write_guard);
                    match persistence_result {
                        Ok(should_compact) => {
                            schedule_compaction(store, persistence, should_compact);
                        }
                        Err(error) => {
                            rollback_attempted_mutation(store, &before, &[]);
                            persistence
                                .repl_offset
                                .store(current_sequence, Ordering::SeqCst);
                            mark_persistence_failed(
                                persistence,
                                format!(
                                    "OBP mutation persistence failed at sequence {}: {}",
                                    sequence, error
                                ),
                            );
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
                } else {
                    drop(write_guard);
                }
                (OnyxValue::Int(if deleted { 1 } else { 0 }), true)
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
        // Non serve crittograficamente sicuro, solo "diverso ad ogni avvio
        // con probabilità di collisione trascurabile".
        nanos.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(pid)
    };
    REPL_ID.set(repl_id_val).ok();
    info!("This instance's replication ID: {}", repl_id_val);
    let args: Vec<String> = env::args().collect();
    let mut master_addr: Option<String> = None;
    let mut master_user: Option<String> = None;
    let mut master_password: Option<String> = None;
    let mut password: Option<String> = None;
    let mut appendfsync: Option<String> = None;
    let mut maxmemory_arg: Option<String> = None;
    let mut maxmemory_policy_arg: Option<String> = None;
    let mut users_map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut auto_failover = false;
    let mut failover_timeout_secs: u64 = 30;
    for i in 0..args.len() {
        if args[i] == "--replica-of" && i + 1 < args.len() {
            master_addr = Some(args[i + 1].clone());
        }
        if args[i] == "--masteruser" && i + 1 < args.len() {
            master_user = Some(args[i + 1].clone());
        }
        if args[i] == "--masterauth" && i + 1 < args.len() {
            master_password = Some(args[i + 1].clone());
        }
        if args[i] == "--requirepass" && i + 1 < args.len() {
            password = Some(args[i + 1].clone());
        }
        if args[i] == "--appendfsync" && i + 1 < args.len() {
            appendfsync = Some(args[i + 1].clone());
        }
        if args[i] == "--maxmemory" && i + 1 < args.len() {
            maxmemory_arg = Some(args[i + 1].clone());
        }
        if args[i] == "--maxmemory-policy" && i + 1 < args.len() {
            maxmemory_policy_arg = Some(args[i + 1].clone());
        }
        // --user nome:password — ripetibile, un utente per occorrenza.
        if args[i] == "--user" && i + 1 < args.len() {
            match args[i + 1].split_once(':') {
                Some((name, pw)) => {
                    users_map.insert(name.to_string(), pw.to_string());
                }
                None => warn!(
                    "Invalid format for --user ( expected name:password): '{}'",
                    args[i + 1]
                ),
            }
        }
        if args[i] == "--auto-failover" {
            auto_failover = true;
        }
        if args[i] == "--failover-timeout" && i + 1 < args.len() {
            failover_timeout_secs = args[i + 1].parse::<u64>().unwrap_or(30);
        }
    }
    if password.is_none() {
        password = env::var("ONYXDB_PASSWORD").ok();
    }
    if master_user.is_none() {
        master_user = env::var("ONYXDB_MASTER_USER").ok();
    }
    if master_password.is_none() {
        master_password = env::var("ONYXDB_MASTER_PASSWORD").ok();
    }
    if master_user.is_some() && master_password.is_none() {
        return Err(
            "upstream replication username requires --masterauth or ONYXDB_MASTER_PASSWORD".into(),
        );
    }
    if master_addr.is_none() && (master_user.is_some() || master_password.is_some()) {
        return Err("upstream replication credentials require --replica-of".into());
    }
    let upstream_credentials = master_password.map(|password| UpstreamCredentials {
        username: master_user.unwrap_or_else(|| "default".to_string()),
        password,
    });
    // Compatibilità con la vecchia modalità a password unica: diventa
    // l'utente "default", utilizzabile anche con `AUTH password` (senza
    // nome utente esplicito).
    if let Some(pw) = password {
        users_map.insert("default".to_string(), pw);
    }
    let num_users = users_map.len();
    USERS.set(users_map).ok();
    if num_users > 0 {
        info!("Authentication required: {} user(s) configured", num_users);
    }

    let policy = match appendfsync.as_deref() {
        Some(s) => FsyncPolicy::parse(s).unwrap_or_else(|| {
            warn!(
                "Invalid value for --appendfsync ('{}'), using 'everysec' as default",
                s
            );
            FsyncPolicy::EverySec
        }),
        None => FsyncPolicy::EverySec,
    };
    FSYNC_POLICY.set(policy).ok();
    info!("Binlog fsync policy: {:?}", policy);

    // maxmemory accepts suffixes such as 100mb and 1gb, or a raw byte count.
    let maxmemory_val: usize = match maxmemory_arg.as_deref() {
        Some(s) => parse_memory_size(s).unwrap_or_else(|| {
            warn!(
                "Invalid value for --maxmemory ('{}'); memory limiting is disabled",
                s
            );
            0
        }),
        None => 0,
    };
    let mm_policy = match maxmemory_policy_arg.as_deref() {
        Some(s) => EvictionPolicy::parse(s).unwrap_or_else(|| {
            warn!(
                "Invalid value for --maxmemory-policy ('{}'); using 'noeviction'",
                s
            );
            EvictionPolicy::NoEviction
        }),
        None => EvictionPolicy::NoEviction,
    };
    if maxmemory_val > 0 {
        info!(
            "Dataset memory limit: {} bytes, policy {:?}",
            maxmemory_val, mm_policy
        );
    }

    tokio::spawn(async {
        time_updater_task().await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let store = Arc::new(ShardedStore::with_maxmemory(maxmemory_val, mm_policy));
    let paths = PersistencePaths::default();
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
        log_tx: tx,
        write_count: AtomicUsize::new(0),
        compaction_pending: AtomicBool::new(false),
        replica_tx,
        promote_to_master: Arc::clone(&promote_flag),
        repl_offset: AtomicU64::new(recovery.last_sequence),
        backlog: std::sync::Mutex::new(std::collections::VecDeque::with_capacity(BACKLOG_CAPACITY)),
        next_replica_id: AtomicU64::new(0),
        replica_status: std::sync::Mutex::new(std::collections::HashMap::new()),
        pubsub_tx,
        next_subscriber_id: AtomicU64::new(0),
        subscriptions: std::sync::Mutex::new(std::collections::HashMap::new()),
        failure: std::sync::Mutex::new(None),
        upstream_replid: AtomicU64::new(
            recovered_replica_identity
                .map(|identity| identity.replid)
                .unwrap_or(0),
        ),
        replication_ready: AtomicBool::new(recovered_replica_identity.is_some()),
        replica_lifecycle,
        accepting_writes: AtomicBool::new(true),
        visibility_gate: tokio::sync::RwLock::new(()),
        write_gate: tokio::sync::Mutex::new(()),
        paths: paths.clone(),
    });

    let binlog_shared: Arc<std::sync::Mutex<File>> =
        Arc::new(std::sync::Mutex::new(open_binlog_file(&paths.binlog)));

    // Task periodico di fsync: solo se la policy e' "everysec" (il default,
    // come in Redis). Ogni secondo forza la scrittura fisica su disco del
    // binlog corrente, indipendentemente da quanto e' stato scritto nel
    // frattempo — se non c'e' nulla di nuovo l'fsync e' comunque economico.
    if fsync_policy() == FsyncPolicy::EverySec {
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
    tokio::spawn(run_binlog_worker(rx, binlog_writer));

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

    let mut port = "6380".to_string();
    for i in 0..args.len() {
        if args[i] == "--port" && i + 1 < args.len() {
            port = args[i + 1].clone();
        }
    }
    let bind_addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("Server listening on {}", bind_addr);
    let obp_port = port.parse::<u16>().unwrap_or(6380) + 1;
    let obp_addr = format!("127.0.0.1:{}", obp_port);
    let obp_listener = TcpListener::bind(&obp_addr).await?;
    info!("OBP (binary) server listening on {}", obp_addr);

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
    let metrics_port: u16 = port.parse::<u16>().unwrap_or(6380) + 1000;
    let store_metrics = Arc::clone(&store);
    let persistence_metrics = Arc::clone(&persistence);
    tokio::spawn(async move {
        run_metrics_server(store_metrics, persistence_metrics, metrics_port).await;
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
        _ = tokio::signal::ctrl_c() => {
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

struct UpstreamCredentials {
    username: String,
    password: String,
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
                    let Some(auth_write) =
                        await_or_stop(&mut stop_rx, writer.write_all(auth_command.as_bytes()))
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
                let Some(sync_write) =
                    await_or_stop(&mut stop_rx, writer.write_all(sync_cmd.as_bytes())).await
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
                        let Some(write_result) =
                            await_or_stop(&mut ack_stop_rx, writer.write_all(ack_cmd.as_bytes()))
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
                            staging.engine.apply_entry(key, entry);
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
            log_tx,
            write_count: AtomicUsize::new(0),
            compaction_pending: AtomicBool::new(false),
            replica_tx,
            promote_to_master: Arc::new(AtomicBool::new(false)),
            repl_offset: AtomicU64::new(initial_sequence),
            backlog: std::sync::Mutex::new(std::collections::VecDeque::new()),
            next_replica_id: AtomicU64::new(0),
            replica_status: std::sync::Mutex::new(std::collections::HashMap::new()),
            pubsub_tx,
            next_subscriber_id: AtomicU64::new(0),
            subscriptions: std::sync::Mutex::new(std::collections::HashMap::new()),
            failure: std::sync::Mutex::new(None),
            upstream_replid: AtomicU64::new(0),
            replication_ready: AtomicBool::new(false),
            replica_lifecycle: Arc::new(ReplicaLifecycle::new(true)),
            accepting_writes: AtomicBool::new(true),
            visibility_gate: tokio::sync::RwLock::new(()),
            write_gate: tokio::sync::Mutex::new(()),
            paths,
        })
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
        ));
        (persistence, worker)
    }

    async fn apply_test_command(
        store: &Arc<ShardedStore>,
        persistence: &Arc<Persistence>,
        args: &[&str],
    ) {
        let command: Vec<String> = args.iter().map(|value| (*value).to_string()).collect();
        let (response, is_write) = execute_ordered_command(store, persistence, &command).await;
        assert!(is_write, "command was not treated as a mutation: {args:?}");
        assert!(
            !matches!(response, RESPValue::Error(_)),
            "mutation failed: {response:?}"
        );
    }

    #[test]
    fn test_set_and_get() {
        let store = ShardedStore::new();
        store.set("key1".to_string(), "value1".to_string());
        assert_eq!(store.get("key1"), Ok(Some("value1".to_string())));
    }

    #[test]
    fn memory_size_parser_rejects_overflow() {
        assert_eq!(parse_memory_size(&format!("{}gb", usize::MAX)), None);
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
    fn test_lpush_e_lrange() {
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

        let (exists, _) = execute_command(&store, &["EXISTS".to_string(), "document".to_string()]);
        assert!(matches!(exists, RESPValue::Integer(1)));

        let (get, changed) = execute_command(&store, &["GET".to_string(), "document".to_string()]);
        assert!(matches!(
            get,
            RESPValue::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
        assert!(!changed);
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
            let (response, changed) = execute_command(&store, &args);
            assert!(matches!(
                response,
                RESPValue::Error(ref message) if message.starts_with("WRONGTYPE")
            ));
            assert!(!changed);
            assert_eq!(
                store.lrange("value", 0, -1),
                Ok(vec!["original".to_string()])
            );
        }
    }

    #[test]
    fn expired_entries_are_absent_to_conditional_and_collection_mutations() {
        let store = ShardedStore::new();
        store.engine.set(
            Bytes::from_static(b"conditional"),
            OnyxValue::Blob(Bytes::from_static(b"stale")),
            Some(now()),
        );
        assert!(store.setnx("conditional", "fresh"));
        assert_eq!(store.get("conditional"), Ok(Some("fresh".to_string())));

        store.engine.set(
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

        let (response, changed) = execute_command(
            &store,
            &[
                "JSON.SET".to_string(),
                "value".to_string(),
                "$".to_string(),
                r#"{"replacement":true}"#.to_string(),
            ],
        );
        assert!(matches!(
            response,
            RESPValue::Error(ref message) if message.starts_with("WRONGTYPE")
        ));
        assert!(!changed);
        assert_eq!(store.get("value"), Ok(Some("original".to_string())));
    }

    #[test]
    fn expired_entries_are_absent_to_all_presence_sensitive_primitives() {
        let store = ShardedStore::new();
        let expired = Some(now());

        for key in ["delete", "expire", "rename", "copy", "conditional"] {
            store.engine.set(
                Bytes::copy_from_slice(key.as_bytes()),
                OnyxValue::Blob(Bytes::from_static(b"stale")),
                expired,
            );
        }

        assert!(!store.delete("delete"));
        assert!(!store.expire("expire", 10));
        assert!(!store.rename("rename", "renamed"));
        assert!(!store.copy("copy", "copied"));
        assert!(!store.engine.set_conditional(
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
    fn empty_collection_deletion_holds_the_shard_lock_through_removal() {
        let store = Arc::new(ShardedStore::new());
        assert_eq!(store.lpush("list", "old".to_string()), Ok(1));
        let (mutation_started_tx, mutation_started_rx) = std::sync::mpsc::channel();
        let (release_mutation_tx, release_mutation_rx) = std::sync::mpsc::channel();
        let pop_store = Arc::clone(&store);
        let pop = std::thread::spawn(move || {
            pop_store
                .engine
                .update_if_exists_with_action(&Bytes::from_static(b"list"), |value| {
                    let OnyxValue::List(list) = value else {
                        panic!("test key changed type");
                    };
                    assert_eq!(list.pop(), Some(Bytes::from_static(b"old")));
                    mutation_started_tx.send(()).unwrap();
                    release_mutation_rx.recv().unwrap();
                    EntryMutation::Delete(())
                })
        });
        mutation_started_rx.recv().unwrap();

        let (push_started_tx, push_started_rx) = std::sync::mpsc::channel();
        let (push_complete_tx, push_complete_rx) = std::sync::mpsc::channel();
        let push_store = Arc::clone(&store);
        let push = std::thread::spawn(move || {
            push_started_tx.send(()).unwrap();
            let result = push_store.rpush("list", "new".to_string());
            push_complete_tx.send(result).unwrap();
        });
        push_started_rx.recv().unwrap();
        assert!(
            push_complete_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "a concurrent write entered the shard before deletion completed"
        );

        release_mutation_tx.send(()).unwrap();
        assert_eq!(pop.join().unwrap(), Some(()));
        assert_eq!(push_complete_rx.recv().unwrap(), Ok(1));
        push.join().unwrap();
        assert_eq!(store.lrange("list", 0, -1), Ok(vec!["new".to_string()]));
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
        let (set, changed) = execute_command(
            &store,
            &["SET".to_string(), "empty".to_string(), String::new()],
        );
        assert!(matches!(set, RESPValue::SimpleString(ref value) if value == "OK"));
        assert!(changed);
        assert_eq!(store.get("empty"), Ok(Some(String::new())));

        store.set("protected".to_string(), "original".to_string());
        let (invalid, changed) = execute_command(
            &store,
            &[
                "SET".to_string(),
                "protected".to_string(),
                "replacement".to_string(),
                "PX".to_string(),
                "0".to_string(),
            ],
        );
        assert!(matches!(invalid, RESPValue::Error(_)));
        assert!(!changed);
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
        store.engine.set(
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
    fn test_glob_match() {
        assert!(glob_match("user:*", "user:42"));
        assert!(glob_match("*", "any"));
        assert!(!glob_match("user:*", "product:1"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "different"));
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
    // Round-trip binlog binario: ogni comando che scrive deve sopravvivere
    // intatto a command_to_binary_record -> binary_record_to_args.
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
        store.engine.set(
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
            let (response, _) = execute_command(&snapshot_store, &args);
            assert!(!matches!(response, RESPValue::Error(_)));
        }
        write_snapshot_file(snapshot_store.engine.snapshot_all(), 6, &directory.paths).unwrap();

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
        write_snapshot_file(previous_store.engine.snapshot_all(), 1, &directory.paths).unwrap();
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
                    LogMessage::Flush { completion } => {
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
                    LogMessage::Flush { completion } => {
                        let _ = completion.send(Ok(()));
                    }
                    LogMessage::Truncate { completion } => {
                        let _ = completion.send(Err("injected rotation failure".to_string()));
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
        write_snapshot_file(store.engine.snapshot_all(), 3, &directory.paths).unwrap();
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
        store.engine.set(
            binary_key.clone(),
            OnyxValue::Blob(binary_value.clone()),
            Some(u64::MAX),
        );
        store.engine.set(
            Bytes::from_static(b"list"),
            OnyxValue::List(vec![
                Bytes::from_static(b"left|right"),
                Bytes::from_static(b"line\n\xff"),
            ]),
            None,
        );
        store
            .engine
            .set(Bytes::from_static(b"float"), OnyxValue::Float(-0.0), None);
        store.engine.set(
            Bytes::from_static(b"vector"),
            OnyxValue::Vector(vec![1.25, -3.5, f32::INFINITY]),
            None,
        );
        let mut hash = std::collections::HashMap::new();
        hash.insert(
            Bytes::from_static(b"field=|"),
            Bytes::from_static(b"value\n\xff"),
        );
        store.engine.set(
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
        store.engine.set(
            Bytes::from_static(b"set"),
            OnyxValue::Set(set.clone()),
            None,
        );

        write_snapshot_file(store.engine.snapshot_all(), 4, &directory.paths).unwrap();
        let recovered = ShardedStore::new();
        load_data_from_paths(&recovered, &directory.paths).unwrap();

        let binary_entry = recovered.engine.get(&binary_key).unwrap();
        assert_eq!(binary_entry.expires_at, Some(u64::MAX));
        assert!(matches!(binary_entry.value, OnyxValue::Blob(value) if value == binary_value));
        let list_entry = recovered.engine.get(&Bytes::from_static(b"list")).unwrap();
        assert!(matches!(
            list_entry.value,
            OnyxValue::List(values)
                if values == vec![Bytes::from_static(b"left|right"), Bytes::from_static(b"line\n\xff")]
        ));
        let float_entry = recovered.engine.get(&Bytes::from_static(b"float")).unwrap();
        assert!(matches!(
            float_entry.value,
            OnyxValue::Float(value) if value.to_bits() == (-0.0f64).to_bits()
        ));
        let vector_entry = recovered
            .engine
            .get(&Bytes::from_static(b"vector"))
            .unwrap();
        assert!(matches!(
            vector_entry.value,
            OnyxValue::Vector(values) if values == vec![1.25, -3.5, f32::INFINITY]
        ));
        let hash_entry = recovered.engine.get(&Bytes::from_static(b"hash")).unwrap();
        assert!(matches!(hash_entry.value, OnyxValue::Hash(values) if values == hash));
        let set_entry = recovered.engine.get(&Bytes::from_static(b"set")).unwrap();
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
        store.engine.set(
            Bytes::from_static(b"stale"),
            OnyxValue::Blob(Bytes::from_static(b"must disappear")),
            None,
        );

        let staging = ShardedStore::new();
        staging.engine.set(
            Bytes::from_static(b"binary\0\xff"),
            OnyxValue::Blob(Bytes::from_static(b"value\r\n\0\xff")),
            Some(u64::MAX),
        );
        staging
            .engine
            .set(Bytes::from_static(b"counter"), OnyxValue::Int(9), None);
        staging.engine.set(
            Bytes::from_static(b"document"),
            OnyxValue::Json(serde_json::json!({"visits": 2})),
            None,
        );

        install_full_sync(&store, &persistence, 71, 40, staging)
            .await
            .unwrap();
        assert!(store.engine.get(&Bytes::from_static(b"stale")).is_none());
        let binary = store
            .engine
            .get(&Bytes::from_static(b"binary\0\xff"))
            .unwrap();
        assert_eq!(binary.expires_at, Some(u64::MAX));
        assert!(matches!(
            binary.value,
            OnyxValue::Blob(value) if value == Bytes::from_static(b"value\r\n\0\xff")
        ));
        assert_eq!(persistence.repl_offset.load(Ordering::SeqCst), 40);
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
        assert_eq!(persistence.repl_offset.load(Ordering::SeqCst), 41);
        assert!(matches!(
            store
                .engine
                .get(&Bytes::from_static(b"counter"))
                .unwrap()
                .value,
            OnyxValue::Int(10)
        ));

        request_log_flush(&persistence).await.unwrap();
        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovery.snapshot_watermark, 40);
        assert_eq!(recovery.last_sequence, 41);
        assert!(
            recovered
                .engine
                .get(&Bytes::from_static(b"stale"))
                .is_none()
        );
        assert!(matches!(
            recovered
                .engine
                .get(&Bytes::from_static(b"counter"))
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
        store.engine.set(
            Bytes::from_static(b"first"),
            OnyxValue::Blob(Bytes::from_static(b"old-first")),
            None,
        );
        store.engine.set(
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
        replacement.engine.set(
            Bytes::from_static(b"first"),
            OnyxValue::Blob(Bytes::from_static(b"new-first")),
            None,
        );
        replacement.engine.set(
            Bytes::from_static(b"second"),
            OnyxValue::Blob(Bytes::from_static(b"new-second")),
            None,
        );
        store.engine.replace_all(replacement.engine.snapshot_all());
        drop(installation_guard);

        let (response, is_write) = read_task.await.unwrap();
        assert!(!is_write);
        let RESPValue::Array(values) = response else {
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
        staging
            .engine
            .set(Bytes::from_static(b"counter"), OnyxValue::Int(3), None);
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
                .engine
                .get(&Bytes::from_static(b"counter"))
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
                        let _ = completion.send(Err("injected replica append failure".to_string()));
                    }
                    LogMessage::Flush { completion }
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
        store
            .engine
            .set(Bytes::from_static(b"counter"), OnyxValue::Int(5), None);
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
        assert_eq!(persistence.repl_offset.load(Ordering::SeqCst), 5);
        assert!(!persistence.accepting_writes.load(Ordering::SeqCst));
        assert!(!persistence.replication_ready.load(Ordering::SeqCst));
        assert!(matches!(
            store
                .engine
                .get(&Bytes::from_static(b"counter"))
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
        old_disk_state.engine.set(
            Bytes::from_static(b"old"),
            OnyxValue::Blob(Bytes::from_static(b"durable")),
            None,
        );
        write_snapshot_file(old_disk_state.engine.snapshot_all(), 3, &directory.paths).unwrap();
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
        store.engine.set(
            Bytes::from_static(b"old"),
            OnyxValue::Blob(Bytes::from_static(b"live")),
            None,
        );
        let staging = ShardedStore::new();
        staging.engine.set(
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
        assert_eq!(persistence.repl_offset.load(Ordering::SeqCst), 1);
        {
            let backlog = persistence.backlog.lock().unwrap();
            assert_eq!(backlog.len(), 1);
            assert_eq!(backlog.front().unwrap().0, 1);
            assert_eq!(backlog.front().unwrap().1.effects.len(), 2);
        }

        request_log_flush(&persistence).await.unwrap();
        drop(persistence);
        worker.await.unwrap();

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(recovery.last_sequence, 1);
        assert_eq!(recovered.get("first"), Ok(Some("value".to_string())));
        assert_eq!(recovered.get("second"), Ok(Some("1".to_string())));
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
        assert_eq!(persistence.repl_offset.load(Ordering::SeqCst), 0);
        assert!(persistence.backlog.lock().unwrap().is_empty());

        drop(persistence);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn expired_collection_mutation_and_empty_delete_recover_faithfully() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let store = Arc::new(ShardedStore::new());
        store.engine.set(
            Bytes::from_static(b"list"),
            OnyxValue::List(vec![Bytes::from_static(b"stale")]),
            Some(now()),
        );

        apply_test_command(&store, &persistence, &["RPUSH", "list", "fresh"]).await;
        apply_test_command(&store, &persistence, &["LPOP", "list"]).await;
        assert!(!store.exists("list"));

        request_log_flush(&persistence).await.unwrap();
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
                    LogMessage::Flush { completion }
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
            .0
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
                        let _ = completion.send(Err("injected transaction failure".to_string()));
                    }
                    LogMessage::Flush { completion }
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
        assert_eq!(persistence.repl_offset.load(Ordering::SeqCst), 0);
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
                        let _ = completion.send(Err("injected transaction failure".to_string()));
                    }
                    LogMessage::Flush { completion }
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
    async fn binlog_append_failure_is_not_acknowledged_or_replicated() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(8);
        let worker = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    LogMessage::Append { completion, .. } => {
                        let _ = completion.send(Err("injected append failure".to_string()));
                    }
                    LogMessage::Flush { completion }
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
        let (response, is_write) = execute_ordered_command(&store, &persistence, &command).await;
        assert!(!is_write);
        assert!(matches!(response, RESPValue::Error(message) if message.starts_with("MISCONF")));
        assert!(!persistence.accepting_writes.load(Ordering::SeqCst));
        assert!(persistence.backlog.lock().unwrap().is_empty());
        assert!(live_receiver.try_recv().is_err());
        assert_eq!(persistence.repl_offset.load(Ordering::SeqCst), 0);
        assert_eq!(store.get("key"), Ok(None));

        let second_command = vec![
            "SET".to_string(),
            "second".to_string(),
            "rejected".to_string(),
        ];
        let (second_response, _) =
            execute_ordered_command(&store, &persistence, &second_command).await;
        assert!(matches!(second_response, RESPValue::Error(_)));
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
        assert_eq!(
            persistence.repl_offset.load(Ordering::SeqCst),
            MUTATION_COUNT
        );

        request_log_flush(&persistence).await.unwrap();
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
            store.engine.set(
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

        request_log_flush(&persistence).await.unwrap();
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
        // Un SET senza scadenza non deve risorgere con un EXAT fantasma.
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
        // EXPIRE (relativo) va persistito come EXPIREAT (assoluto): il
        // record binario stesso è già in forma assoluta.
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
            "campo".to_string(),
            "valore".to_string(),
        ];
        let record = command_to_binary_record("HSET", &args, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["HSET", "h", "campo", "valore"]
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
        let args = vec!["HDEL".to_string(), "h".to_string(), "campo".to_string()];
        let record = command_to_binary_record("HDEL", &args, None).unwrap();
        assert_eq!(
            binary_record_to_args(&record).unwrap(),
            vec!["HDEL", "h", "campo"]
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
        // Niente panic su un array troppo corto: solo None.
        let args = vec!["SET".to_string()];
        assert!(command_to_binary_record("SET", &args, None).is_none());
    }

    // ============================================================
    // Binlog corrotto: mai panic, solo None.
    // ============================================================

    #[test]
    fn test_record_vuoto_non_va_in_panic() {
        assert!(binary_record_to_args(&[]).is_none());
    }

    #[test]
    fn test_record_troncato_a_meta_chiave_non_va_in_panic() {
        // OP_SET, poi dichiara una chiave di 100 byte ma ne fornisce solo 2.
        let record = vec![OP_SET, 0x00, 0x64, b'a', b'b'];
        assert!(binary_record_to_args(&record).is_none());
    }

    #[test]
    fn truncated_record_mid_value_does_not_panic() {
        // Chiave valida "k", poi dichiara un valore di 1000 byte inesistente.
        let mut record = vec![OP_SET];
        record.extend_from_slice(&[0x00, 0x01]); // key_len = 1
        record.push(b'k');
        record.push(1); // tipo stringa
        record.extend_from_slice(&[0x00, 0x00, 0x03, 0xE8]); // val_len = 1000, ma non seguono byte
        assert!(binary_record_to_args(&record).is_none());
    }

    #[test]
    fn test_read_u16_be_su_buffer_troncato() {
        let buf = [0x00u8]; // solo 1 byte, ne servono 2
        let mut offset = 0;
        assert_eq!(read_u16_be(&buf, &mut offset), None);
    }

    #[test]
    fn test_read_u64_be_su_buffer_troncato() {
        let buf = [0x00u8, 0x01, 0x02]; // solo 3 byte, ne servono 8
        let mut offset = 0;
        assert_eq!(read_u64_be(&buf, &mut offset), None);
    }

    #[test]
    fn test_safe_slice_oltre_i_limiti() {
        let buf = [1u8, 2, 3];
        assert!(safe_slice(&buf, 0, 10).is_none());
        assert!(safe_slice(&buf, 5, 1).is_none());
        assert_eq!(safe_slice(&buf, 0, 3), Some(&buf[..]));
    }

    // ============================================================
    // Round-trip formato snapshot testuale (value_to_line / line_to_entry)
    // ============================================================

    #[test]
    fn test_snapshot_roundtrip_stringa() {
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
    fn test_snapshot_roundtrip_con_scadenza() {
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
    fn test_snapshot_riga_malformata_ritorna_none() {
        assert!(line_to_entry("questa non e' una riga valida").is_none());
        assert!(line_to_entry("").is_none());
    }
    // ============================================================
    // Logica di resync: qui vive il regression test del bug che ha
    // scoperto Marco (backlog vuoto dopo un riavvio del Master scambiato
    // per "già allineati").
    // ============================================================

    #[test]
    fn test_replid_stesso_id_permette_parziale() {
        assert!(replid_allows_partial(42, 42));
    }

    #[test]
    fn test_replid_diverso_richiede_full() {
        assert!(!replid_allows_partial(42, 99));
    }

    #[test]
    fn test_replid_sconosciuto_richiede_full() {
        // requested_replid = 0: la Replica non ha ancora mai visto un
        // replid valido (prima connessione in assoluto).
        assert!(!replid_allows_partial(0, 99));
    }

    #[test]
    fn test_partial_resync_backlog_copre_offset() {
        // Backlog parte da 5, richiesto offset 4 (il prossimo da mandare
        // sarebbe proprio il 5): nessun buco, parziale ammesso.
        assert!(partial_resync_possible(4, Some(5), 100));
    }

    #[test]
    fn test_partial_resync_gap_nel_backlog() {
        // Backlog parte da 20, ma si chiede di ripartire da 4: mancano i
        // comandi 5..19, non più recuperabili. Serve full resync.
        assert!(!partial_resync_possible(4, Some(20), 100));
    }

    #[test]
    fn test_partial_resync_backlog_vuoto_ma_davvero_allineati() {
        // Nessuna scrittura in backlog, ma l'offset richiesto combacia
        // esattamente con quello corrente del Master: qui è legittimo
        // concludere "non c'è nulla da rimandare".
        assert!(partial_resync_possible(9, None, 9));
    }

    #[test]
    fn test_partial_resync_backlog_vuoto_dopo_riavvio_master_e_il_bug_originale() {
        // Scenario esatto del bug: il Master è ripartito da zero
        // (repl_offset azzerato a 0, backlog vuoto perché nessuna
        // scrittura è ancora avvenuta nel nuovo processo), ma la Replica
        // si presenta con un offset "9" ereditato dal vecchio Master.
        // PRIMA del fix, backlog vuoto da solo bastava per concludere
        // "già allineati" — qui verifichiamo che NON sia più così: serve
        // che l'offset richiesto combaci con quello corrente (0), non con
        // un numero qualsiasi lasciato da un processo precedente.
        assert!(!partial_resync_possible(9, None, 0));
    }
    // ============================================================
    // JSON path: parser e navigazione
    // ============================================================

    #[test]
    fn test_parse_path_radice() {
        assert_eq!(parse_json_path("$"), Some(vec![]));
    }

    #[test]
    fn test_parse_path_campo_singolo() {
        assert_eq!(
            parse_json_path("$.name"),
            Some(vec![JsonPathSegment::Field("name".to_string())])
        );
    }

    #[test]
    fn test_parse_path_annidato() {
        assert_eq!(
            parse_json_path("$.address.city"),
            Some(vec![
                JsonPathSegment::Field("address".to_string()),
                JsonPathSegment::Field("city".to_string()),
            ])
        );
    }

    #[test]
    fn test_parse_path_indice_array() {
        assert_eq!(
            parse_json_path("$.tag[0]"),
            Some(vec![
                JsonPathSegment::Field("tag".to_string()),
                JsonPathSegment::Index(0)
            ])
        );
    }

    #[test]
    fn test_parse_path_misto_lungo() {
        assert_eq!(
            parse_json_path("$.a[1].b[2]"),
            Some(vec![
                JsonPathSegment::Field("a".to_string()),
                JsonPathSegment::Index(1),
                JsonPathSegment::Field("b".to_string()),
                JsonPathSegment::Index(2),
            ])
        );
    }

    #[test]
    fn test_parse_path_senza_dollaro_iniziale_none() {
        assert_eq!(parse_json_path("name"), None);
    }

    #[test]
    fn test_parse_path_doppio_punto_none() {
        assert_eq!(parse_json_path("$..name"), None);
    }

    #[test]
    fn test_parse_path_parentesi_non_chiusa_none() {
        assert_eq!(parse_json_path("$.tag[0"), None);
    }

    #[test]
    fn test_parse_path_indice_non_numerico_none() {
        assert_eq!(parse_json_path("$.tag[x]"), None);
    }

    #[test]
    fn test_get_json_path_campo_esistente() {
        let val: serde_json::Value = serde_json::json!({"name": "Marco", "age": 18});
        let path = parse_json_path("$.name").unwrap();
        assert_eq!(
            get_json_path(&val, &path),
            Some(&serde_json::json!("Marco"))
        );
    }

    #[test]
    fn test_get_json_path_annidato() {
        let val: serde_json::Value = serde_json::json!({"address": {"city": "Rome"}});
        let path = parse_json_path("$.address.city").unwrap();
        assert_eq!(get_json_path(&val, &path), Some(&serde_json::json!("Rome")));
    }

    #[test]
    fn test_get_json_path_campo_assente_none() {
        let val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.surname").unwrap();
        assert_eq!(get_json_path(&val, &path), None);
    }

    #[test]
    fn test_get_json_path_indice_array() {
        let val: serde_json::Value = serde_json::json!({"tag": ["dev", "rust"]});
        let path = parse_json_path("$.tag[1]").unwrap();
        assert_eq!(get_json_path(&val, &path), Some(&serde_json::json!("rust")));
    }

    #[test]
    fn test_get_json_path_indice_fuori_range_none() {
        let val: serde_json::Value = serde_json::json!({"tag": ["dev"]});
        let path = parse_json_path("$.tag[5]").unwrap();
        assert_eq!(get_json_path(&val, &path), None);
    }

    #[test]
    fn test_get_json_path_wrong_type_none() {
        // Indice su un oggetto (non un array): non ha senso, deve dare None.
        let val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.name[0]").unwrap();
        assert_eq!(get_json_path(&val, &path), None);
    }

    #[test]
    fn test_set_json_path_documento_intero() {
        let mut val: serde_json::Value = serde_json::json!({"old": true});
        let path = parse_json_path("$").unwrap();
        assert!(set_json_path(
            &mut val,
            &path,
            serde_json::json!({"new": true})
        ));
        assert_eq!(val, serde_json::json!({"new": true}));
    }

    #[test]
    fn test_set_json_path_campo_esistente() {
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.name").unwrap();
        assert!(set_json_path(&mut val, &path, serde_json::json!("Ahmed")));
        assert_eq!(val, serde_json::json!({"name": "Ahmed"}));
    }

    #[test]
    fn test_set_json_path_campo_nuovo_su_oggetto_esistente() {
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.age").unwrap();
        assert!(set_json_path(&mut val, &path, serde_json::json!(18)));
        assert_eq!(val, serde_json::json!({"name": "Marco", "age": 18}));
    }

    #[test]
    fn test_set_json_path_genitore_assente_fallisce() {
        // $.a.b.c ma "a" non esiste: niente auto-creazione, deve fallire.
        let mut val: serde_json::Value = serde_json::json!({});
        let path = parse_json_path("$.a.b.c").unwrap();
        assert!(!set_json_path(&mut val, &path, serde_json::json!(1)));
    }

    #[test]
    fn test_set_json_path_indice_array_esistente() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev", "rust"]});
        let path = parse_json_path("$.tag[0]").unwrap();
        assert!(set_json_path(&mut val, &path, serde_json::json!("go")));
        assert_eq!(val, serde_json::json!({"tag": ["go", "rust"]}));
    }

    #[test]
    fn test_set_json_path_append_in_coda_array() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev"]});
        let path = parse_json_path("$.tag[1]").unwrap();
        assert!(set_json_path(&mut val, &path, serde_json::json!("rust")));
        assert_eq!(val, serde_json::json!({"tag": ["dev", "rust"]}));
    }

    #[test]
    fn test_set_json_path_indice_troppo_avanti_fallisce() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev"]});
        let path = parse_json_path("$.tag[5]").unwrap();
        assert!(!set_json_path(&mut val, &path, serde_json::json!("x")));
    }

    #[test]
    fn test_delete_json_path_campo() {
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco", "age": 18});
        let path = parse_json_path("$.age").unwrap();
        assert!(delete_json_path(&mut val, &path));
        assert_eq!(val, serde_json::json!({"name": "Marco"}));
    }

    #[test]
    fn test_delete_json_path_indice_array() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev", "rust"]});
        let path = parse_json_path("$.tag[0]").unwrap();
        assert!(delete_json_path(&mut val, &path));
        assert_eq!(val, serde_json::json!({"tag": ["rust"]}));
    }

    #[test]
    fn test_delete_json_path_campo_assente_fallisce() {
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.surname").unwrap();
        assert!(!delete_json_path(&mut val, &path));
    }

    #[test]
    fn test_delete_json_path_radice_fallisce() {
        // DEL su "$" (documento intero) non passa da qui, va gestito
        // separatamente con un DEL normale sulla chiave.
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$").unwrap();
        assert!(!delete_json_path(&mut val, &path));
    }
    // ============================================================
    // JSON NUMINCRBY / ARRAPPEND
    // ============================================================

    #[test]
    fn test_numincrby_json_path_su_intero() {
        let mut val: serde_json::Value = serde_json::json!({"visits": 5});
        let path = parse_json_path("$.visits").unwrap();
        let result = numincrby_json_path(&mut val, &path, 3.0);
        assert_eq!(result, Ok(8.0));
        assert_eq!(val, serde_json::json!({"visits": 8}));
    }

    #[test]
    fn test_numincrby_json_path_with_negative_delta() {
        let mut val: serde_json::Value = serde_json::json!({"balance": 10});
        let path = parse_json_path("$.balance").unwrap();
        let result = numincrby_json_path(&mut val, &path, -3.0);
        assert_eq!(result, Ok(7.0));
    }

    #[test]
    fn test_numincrby_json_path_with_float_values() {
        let mut val: serde_json::Value = serde_json::json!({"price": 9.5});
        let path = parse_json_path("$.price").unwrap();
        let result = numincrby_json_path(&mut val, &path, 0.5);
        assert_eq!(result, Ok(10.0));
    }

    #[test]
    fn test_numincrby_json_path_with_string_value_fails() {
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.name").unwrap();
        assert!(numincrby_json_path(&mut val, &path, 1.0).is_err());
    }

    #[test]
    fn test_numincrby_json_path_assente_fallisce() {
        let mut val: serde_json::Value = serde_json::json!({});
        let path = parse_json_path("$.counter").unwrap();
        assert!(numincrby_json_path(&mut val, &path, 1.0).is_err());
    }

    #[test]
    fn test_arrappend_json_path_su_array_esistente() {
        let mut val: serde_json::Value = serde_json::json!({"tag": ["dev"]});
        let path = parse_json_path("$.tag").unwrap();
        let result = arrappend_json_path(&mut val, &path, serde_json::json!("rust"));
        assert_eq!(result, Ok(2));
        assert_eq!(val, serde_json::json!({"tag": ["dev", "rust"]}));
    }

    #[test]
    fn test_arrappend_json_path_su_array_vuoto() {
        let mut val: serde_json::Value = serde_json::json!({"tag": []});
        let path = parse_json_path("$.tag").unwrap();
        let result = arrappend_json_path(&mut val, &path, serde_json::json!("primo"));
        assert_eq!(result, Ok(1));
    }

    #[test]
    fn test_arrappend_json_path_su_non_array_fallisce() {
        let mut val: serde_json::Value = serde_json::json!({"name": "Marco"});
        let path = parse_json_path("$.name").unwrap();
        assert!(arrappend_json_path(&mut val, &path, serde_json::json!("x")).is_err());
    }

    #[test]
    fn test_arrappend_json_path_assente_fallisce() {
        let mut val: serde_json::Value = serde_json::json!({});
        let path = parse_json_path("$.tag").unwrap();
        assert!(arrappend_json_path(&mut val, &path, serde_json::json!("x")).is_err());
    }
    #[test]
    fn test_json_arrlen_e_objkeys_via_store() {
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
            .engine
            .snapshot_all()
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
        let (response, is_write) = execute_ordered_command(&store, &persistence, &command).await;
        assert!(matches!(response, RESPValue::Integer(0)));
        assert!(!is_write);
        assert_eq!(persistence.repl_offset.load(Ordering::SeqCst), 1);
        assert_eq!(persistence.backlog.lock().unwrap().len(), 1);

        request_log_flush(&persistence).await.unwrap();
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
        let offset_before_overflow = persistence.repl_offset.load(Ordering::SeqCst);
        let overflow = vec!["INCR".to_string(), "maximum".to_string()];
        let (response, is_write) = execute_ordered_command(&store, &persistence, &overflow).await;
        assert!(matches!(response, RESPValue::Error(message) if message.contains("overflow")));
        assert!(!is_write);
        assert_eq!(
            persistence.repl_offset.load(Ordering::SeqCst),
            offset_before_overflow
        );

        request_log_flush(&persistence).await.unwrap();
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
        write_snapshot_file(snapshot_store.engine.snapshot_all(), 1, &directory.paths).unwrap();

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
        request_log_flush(&persistence).await.unwrap();
        drop(persistence);
        worker.await.unwrap();

        let recovered = Arc::new(ShardedStore::new());
        let recovery = load_data_from_paths(&recovered, &directory.paths).unwrap();
        let entry = recovered.engine.peek(&key).unwrap();
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
        request_log_flush(&persistence).await.unwrap();
        drop(persistence);
        worker.await.unwrap();

        let recovered_after_delete = ShardedStore::new();
        load_data_from_paths(&recovered_after_delete, &directory.paths).unwrap();
        assert!(recovered_after_delete.engine.peek(&key).is_none());
    }

    #[tokio::test]
    async fn actual_eviction_victims_are_ordered_and_do_not_resurrect() {
        let directory = TestPersistenceDirectory::new();
        let (persistence, worker) = start_test_persistence(directory.paths.clone(), 0).await;
        let store = Arc::new(ShardedStore::new());
        apply_test_command(&store, &persistence, &["SET", "first", "aaaaaaaa"]).await;
        apply_test_command(&store, &persistence, &["SET", "second", "bbbbbbbb"]).await;

        let limit = store.engine.total_memory_bytes().saturating_sub(1);
        let evicted = store
            .engine
            .evict_to_fit(limit, EvictionPolicy::AllKeysLru, &HashSet::new());
        assert!(!evicted.is_empty());
        let written_key = Bytes::from_static(b"causing-write");
        store.engine.set(
            written_key.clone(),
            OnyxValue::Blob(Bytes::from_static(b"committed")),
            None,
        );
        let written_entry = store
            .engine
            .peek(&written_key)
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
        let sequence = persistence.repl_offset.load(Ordering::SeqCst) + 1;
        persistence.repl_offset.store(sequence, Ordering::SeqCst);
        persist_ordered_mutation(&persistence, sequence, &batch)
            .await
            .unwrap();
        let expected = persistent_state(&store);

        request_log_flush(&persistence).await.unwrap();
        drop(persistence);
        worker.await.unwrap();
        let recovered = ShardedStore::new();
        load_data_from_paths(&recovered, &directory.paths).unwrap();
        assert_eq!(persistent_state(&recovered), expected);
        for (key, _) in evicted {
            assert!(recovered.engine.peek(&key).is_none());
        }
    }

    #[test]
    fn evicted_target_recreated_with_same_value_is_replayed_as_delete_then_put() {
        let store = ShardedStore::new();
        let key = Bytes::from_static(b"target");
        store.engine.set(
            key.clone(),
            OnyxValue::Blob(Bytes::from_static(b"same-value")),
            None,
        );
        let keys = vec![key.clone()];
        let before = capture_entries(&store, &keys);
        let evicted_entry = store.engine.peek(&key).unwrap();
        assert!(store.engine.delete(&key));
        store.engine.set(
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
        replayed.engine.set(
            key.clone(),
            OnyxValue::Blob(Bytes::from_static(b"same-value")),
            None,
        );
        apply_committed_batch(&replayed, &batch);
        assert_eq!(persistent_state(&replayed), persistent_state(&store));
    }

    #[tokio::test]
    async fn periodic_sync_failure_disables_subsequent_writes() {
        let directory = TestPersistenceDirectory::new();
        let (log_tx, mut receiver) = mpsc::channel(4);
        let worker = tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    LogMessage::SyncData { completion } => {
                        let _ = completion.send(Err("injected sync failure".to_string()));
                    }
                    LogMessage::Append { completion, .. }
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
        assert!(
            persistence
                .failure
                .lock()
                .unwrap()
                .as_deref()
                .is_some_and(|message| message.contains("injected sync failure"))
        );

        let store = Arc::new(ShardedStore::new());
        let command = vec!["SET".to_string(), "key".to_string(), "value".to_string()];
        let (response, is_write) = execute_ordered_command(&store, &persistence, &command).await;
        assert!(!is_write);
        assert!(
            matches!(response, RESPValue::Error(message) if message.contains("injected sync failure"))
        );
        assert_eq!(store.get("key"), Ok(None));
        drop(persistence);
        worker.await.unwrap();
    }

    #[test]
    fn partial_resync_rejects_ahead_and_overflowing_offsets() {
        assert!(!partial_resync_possible(101, Some(1), 100));
        assert!(!partial_resync_possible(u64::MAX, Some(1), u64::MAX - 1));
        assert!(partial_resync_possible(u64::MAX, Some(u64::MAX), u64::MAX));
    }
}
