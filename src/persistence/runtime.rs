use super::{
    CommittedBatch, PersistenceError, ReplicaIdentity, encode_committed_batch,
    encode_versioned_binlog_record, write_replica_identity, write_snapshot_file,
};
use crate::config::FsyncPolicy;
use onyxdb::store::ShardedStore;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{Notify, mpsc, oneshot};
use tracing::{error, info};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageFailureDisposition {
    Rejected,
    Indeterminate,
}

#[derive(Debug)]
pub(crate) struct StorageFailure {
    disposition: StorageFailureDisposition,
    message: String,
}

impl StorageFailure {
    pub(crate) fn rejected(message: impl Into<String>) -> Self {
        Self {
            disposition: StorageFailureDisposition::Rejected,
            message: message.into(),
        }
    }

    pub(crate) fn indeterminate(message: impl Into<String>) -> Self {
        Self {
            disposition: StorageFailureDisposition::Indeterminate,
            message: message.into(),
        }
    }

    fn into_persistence_error(self) -> PersistenceError {
        match self.disposition {
            StorageFailureDisposition::Rejected => PersistenceError::new(self.message),
            StorageFailureDisposition::Indeterminate => {
                PersistenceError::indeterminate(self.message)
            }
        }
    }
}

impl std::fmt::Display for StorageFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

pub(crate) type StorageResult = Result<(), StorageFailure>;

#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum TruncateError {
    Unchanged(std::io::Error),
    Indeterminate(std::io::Error),
}

pub(crate) trait BinlogIo: Write + Seek + Send + 'static {
    fn sync_data(&mut self) -> std::io::Result<()>;
    fn sync_all(&mut self) -> std::io::Result<()>;
    fn truncate(&mut self, length: u64) -> Result<(), TruncateError>;
}

impl BinlogIo for File {
    fn sync_data(&mut self) -> std::io::Result<()> {
        File::sync_data(self)
    }

    fn sync_all(&mut self) -> std::io::Result<()> {
        File::sync_all(self)
    }

    fn truncate(&mut self, length: u64) -> Result<(), TruncateError> {
        self.set_len(length).map_err(TruncateError::Indeterminate)
    }
}

/// Authoritative runtime state for durable commit ordering and compaction.
///
/// Replication fan-out remains server-owned, but it can only publish a sequence
/// after this runtime has accepted the identical committed batch.
pub(crate) struct CommitRuntime {
    pub(crate) binlog: BinlogHandle,
    pub(crate) write_count: AtomicUsize,
    pub(crate) compaction_pending: AtomicBool,
    pub(crate) accepting_writes: AtomicBool,
    /// Readers hold a shared guard while observing state. Durable mutations and
    /// full-sync installation hold the exclusive guard. When both gates are
    /// required, `write_gate` must always be acquired first.
    pub(crate) visibility_gate: Arc<tokio::sync::RwLock<()>>,
    pub(crate) write_gate: Arc<tokio::sync::Mutex<()>>,
    pub(crate) paths: super::PersistencePaths,
    /// Last sequence durably accepted and made authoritative in live state.
    repl_offset: AtomicU64,
    pub(crate) failure: std::sync::Mutex<Option<String>>,
    fail_stop_started: AtomicBool,
    fail_stop_reason: std::sync::Mutex<Option<String>>,
    fail_stop_notify: Notify,
    fail_stop_visibility_guard: std::sync::Mutex<Option<tokio::sync::OwnedRwLockWriteGuard<()>>>,
}

impl CommitRuntime {
    pub(crate) fn new(
        binlog: BinlogHandle,
        initial_sequence: u64,
        paths: super::PersistencePaths,
    ) -> Self {
        Self {
            binlog,
            write_count: AtomicUsize::new(0),
            compaction_pending: AtomicBool::new(false),
            accepting_writes: AtomicBool::new(true),
            visibility_gate: Arc::new(tokio::sync::RwLock::new(())),
            write_gate: Arc::new(tokio::sync::Mutex::new(())),
            paths,
            repl_offset: AtomicU64::new(initial_sequence),
            failure: std::sync::Mutex::new(None),
            fail_stop_started: AtomicBool::new(false),
            fail_stop_reason: std::sync::Mutex::new(None),
            fail_stop_notify: Notify::new(),
            fail_stop_visibility_guard: std::sync::Mutex::new(None),
        }
    }

    fn record_persisted_write(&self, compaction_threshold: usize) -> bool {
        self.write_count.fetch_add(1, Ordering::SeqCst) + 1 >= compaction_threshold
            && self
                .compaction_pending
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }

    pub(crate) fn sequence(&self) -> u64 {
        self.repl_offset.load(Ordering::SeqCst)
    }

    pub(crate) fn next_sequence(&self) -> Result<u64, PersistenceError> {
        self.sequence()
            .checked_add(1)
            .ok_or_else(|| PersistenceError::new("Persistence sequence is exhausted"))
    }

    pub(crate) async fn accept_next_batch(
        &self,
        sequence: u64,
        batch: &CommittedBatch,
        compaction_threshold: usize,
    ) -> Result<bool, PersistenceError> {
        let expected = self.next_sequence()?;
        if sequence != expected {
            return Err(PersistenceError::new(format!(
                "Persistence sequence mismatch: expected {}, received {}",
                expected, sequence
            )));
        }
        self.binlog.append_batch(sequence, batch).await?;
        self.repl_offset.store(sequence, Ordering::SeqCst);
        Ok(self.record_persisted_write(compaction_threshold))
    }

    /// Accepts a contiguous logical sequence group through one storage outcome.
    /// The durable offset advances only after the complete physical append is
    /// acknowledged; individual batches remain separate recovery records.
    pub(crate) async fn accept_next_batches(
        &self,
        batches: &[(u64, CommittedBatch)],
        compaction_threshold: usize,
    ) -> Result<bool, PersistenceError> {
        if batches.is_empty() {
            return Err(PersistenceError::new(
                "A persistence group must contain at least one committed batch",
            ));
        }

        let mut expected = Some(self.next_sequence()?);
        for (sequence, _) in batches {
            let Some(expected_sequence) = expected else {
                return Err(PersistenceError::new("Persistence sequence is exhausted"));
            };
            if *sequence != expected_sequence {
                return Err(PersistenceError::new(format!(
                    "Persistence sequence mismatch: expected {}, received {}",
                    expected_sequence, sequence
                )));
            }
            expected = expected_sequence.checked_add(1);
        }

        self.binlog.append_batches(batches).await?;
        let last_sequence = batches
            .last()
            .expect("a non-empty persistence group has a last sequence")
            .0;
        self.repl_offset.store(last_sequence, Ordering::SeqCst);

        let mut should_compact = false;
        for _ in batches {
            should_compact |= self.record_persisted_write(compaction_threshold);
        }
        Ok(should_compact)
    }

    pub(crate) fn install_baseline(&self, sequence: u64) {
        self.repl_offset.store(sequence, Ordering::SeqCst);
        self.write_count.store(0, Ordering::SeqCst);
    }

    pub(crate) async fn acquire_commit_boundary(&self) -> CommitBoundary {
        CommitBoundary::acquire(&self.write_gate, &self.visibility_gate).await
    }

    pub(crate) fn enter_fail_stop_with_boundary(
        &self,
        boundary: CommitBoundary,
        message: impl Into<String>,
    ) {
        self.fail_stop_started.store(true, Ordering::SeqCst);
        let visibility_guard = boundary.into_visibility_guard();
        self.enter_fail_stop_with_visibility_guard(visibility_guard, message);
    }

    pub(crate) async fn enter_fail_stop(&self, message: impl Into<String>) {
        self.fail_stop_started.store(true, Ordering::SeqCst);
        let boundary = self.acquire_commit_boundary().await;
        self.enter_fail_stop_with_boundary(boundary, message);
    }

    fn enter_fail_stop_with_visibility_guard(
        &self,
        visibility_guard: tokio::sync::OwnedRwLockWriteGuard<()>,
        message: impl Into<String>,
    ) {
        let message = message.into();
        self.fail_stop_started.store(true, Ordering::SeqCst);
        let mut guard = self
            .fail_stop_visibility_guard
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.is_none() {
            *guard = Some(visibility_guard);
        }
        drop(guard);
        let mut reason = self
            .fail_stop_reason
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if reason.is_none() {
            *reason = Some(message.clone());
        }
        drop(reason);
        let mut failure = self
            .failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if failure.is_none() {
            *failure = Some(message.clone());
        }
        drop(failure);
        self.accepting_writes.store(false, Ordering::SeqCst);
        error!(
            "Persistence outcome is indeterminate; entering fail-stop: {}",
            message
        );
        self.fail_stop_notify.notify_waiters();
    }

    pub(crate) fn is_fail_stopped(&self) -> bool {
        self.fail_stop_started.load(Ordering::SeqCst)
    }

    pub(crate) async fn wait_for_fail_stop(&self) -> String {
        loop {
            let notified = self.fail_stop_notify.notified();
            if let Some(reason) = self
                .fail_stop_reason
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
            {
                return reason;
            }
            notified.await;
        }
    }

    pub(crate) async fn compact(
        &self,
        store: &Arc<ShardedStore>,
        upstream_replid: &AtomicU64,
    ) -> Result<u64, PersistenceError> {
        let _write_guard = self.write_gate.lock().await;
        if let Err(error) = self.binlog.flush().await {
            let error = error_with_context(error, "Binlog flush failed");
            if error.is_indeterminate() {
                let visibility_guard = Arc::clone(&self.visibility_gate).write_owned().await;
                self.enter_fail_stop_with_visibility_guard(visibility_guard, error.to_string());
            }
            return Err(error);
        }

        let watermark = self.sequence();
        let entries = store.raw_entries();
        let paths = self.paths.clone();
        tokio::task::spawn_blocking(move || write_snapshot_file(entries, watermark, &paths))
            .await
            .map_err(|error| PersistenceError::new(format!("Snapshot task failed: {}", error)))?
            .map_err(|error| {
                PersistenceError::new(format!("Snapshot installation failed: {}", error))
            })?;

        let replid = upstream_replid.load(Ordering::SeqCst);
        if replid != 0 {
            write_replica_identity(
                &self.paths,
                ReplicaIdentity {
                    replid,
                    baseline_sequence: watermark,
                },
            )?;
        }
        if let Err(error) = self.binlog.truncate().await {
            let error = error_with_context(error, "Binlog rotation failed");
            if error.is_indeterminate() {
                let visibility_guard = Arc::clone(&self.visibility_gate).write_owned().await;
                self.enter_fail_stop_with_visibility_guard(visibility_guard, error.to_string());
            }
            return Err(error);
        }
        self.write_count.store(0, Ordering::SeqCst);
        info!(
            "Compaction complete at sequence {}: snapshot installed and binlog truncated",
            watermark
        );
        Ok(watermark)
    }
}

fn error_with_context(error: PersistenceError, context: &str) -> PersistenceError {
    let message = format!("{}: {}", context, error);
    if error.is_indeterminate() {
        PersistenceError::indeterminate(message)
    } else {
        PersistenceError::new(message)
    }
}

pub(crate) enum LogMessage {
    Append {
        records: Vec<(u64, Vec<u8>)>,
        completion: oneshot::Sender<StorageResult>,
    },
    Flush {
        completion: oneshot::Sender<StorageResult>,
    },
    SyncData {
        completion: oneshot::Sender<StorageResult>,
    },
    Truncate {
        completion: oneshot::Sender<StorageResult>,
    },
}

#[derive(Clone)]
pub(crate) struct BinlogHandle {
    sender: mpsc::Sender<LogMessage>,
}

impl BinlogHandle {
    pub(crate) fn new(sender: mpsc::Sender<LogMessage>) -> Self {
        Self { sender }
    }

    async fn append(&self, sequence: u64, record: Vec<u8>) -> Result<(), PersistenceError> {
        let (completion_tx, completion_rx) = oneshot::channel();
        self.sender
            .send(LogMessage::Append {
                records: vec![(sequence, record)],
                completion: completion_tx,
            })
            .await
            .map_err(|_| PersistenceError::new("Binlog worker is unavailable"))?;
        completion_rx
            .await
            .map_err(|_| PersistenceError::indeterminate("Binlog append completion was dropped"))?
            .map_err(StorageFailure::into_persistence_error)
    }

    pub(crate) async fn append_batch(
        &self,
        sequence: u64,
        batch: &CommittedBatch,
    ) -> Result<(), PersistenceError> {
        self.append(sequence, encode_committed_batch(batch)?).await
    }

    pub(crate) async fn append_batches(
        &self,
        batches: &[(u64, CommittedBatch)],
    ) -> Result<(), PersistenceError> {
        if batches.is_empty() {
            return Err(PersistenceError::new(
                "A binlog append group must contain at least one committed batch",
            ));
        }
        let records = batches
            .iter()
            .map(|(sequence, batch)| Ok((*sequence, encode_committed_batch(batch)?)))
            .collect::<Result<Vec<_>, PersistenceError>>()?;
        let (completion_tx, completion_rx) = oneshot::channel();
        self.sender
            .send(LogMessage::Append {
                records,
                completion: completion_tx,
            })
            .await
            .map_err(|_| PersistenceError::new("Binlog worker is unavailable"))?;
        completion_rx
            .await
            .map_err(|_| PersistenceError::indeterminate("Binlog append completion was dropped"))?
            .map_err(StorageFailure::into_persistence_error)
    }

    pub(crate) async fn flush(&self) -> Result<(), PersistenceError> {
        let (completion_tx, completion_rx) = oneshot::channel();
        self.sender
            .send(LogMessage::Flush {
                completion: completion_tx,
            })
            .await
            .map_err(|_| {
                PersistenceError::indeterminate("Binlog worker is unavailable during flush")
            })?;
        completion_rx
            .await
            .map_err(|_| PersistenceError::indeterminate("Binlog flush completion was dropped"))?
            .map_err(StorageFailure::into_persistence_error)
    }

    pub(crate) async fn sync_data(&self) -> Result<(), PersistenceError> {
        let (completion_tx, completion_rx) = oneshot::channel();
        self.sender
            .send(LogMessage::SyncData {
                completion: completion_tx,
            })
            .await
            .map_err(|_| {
                PersistenceError::indeterminate("Binlog worker is unavailable during sync")
            })?;
        completion_rx
            .await
            .map_err(|_| PersistenceError::indeterminate("Binlog sync completion was dropped"))?
            .map_err(StorageFailure::into_persistence_error)
    }

    pub(crate) async fn truncate(&self) -> Result<(), PersistenceError> {
        let (completion_tx, completion_rx) = oneshot::channel();
        self.sender
            .send(LogMessage::Truncate {
                completion: completion_tx,
            })
            .await
            .map_err(|_| {
                PersistenceError::indeterminate("Binlog worker is unavailable during truncation")
            })?;
        completion_rx
            .await
            .map_err(|_| PersistenceError::indeterminate("Binlog truncate completion was dropped"))?
            .map_err(StorageFailure::into_persistence_error)
    }
}

fn rollback_binlog_tail<T: BinlogIo>(
    binlog: &mut T,
    previous_length: u64,
    original_error: std::io::Error,
) -> StorageFailure {
    let rollback_result = (|| -> Result<(), String> {
        binlog
            .truncate(previous_length)
            .map_err(|error| match error {
                TruncateError::Unchanged(error) | TruncateError::Indeterminate(error) => {
                    format!("tail truncation failed: {}", error)
                }
            })?;
        binlog
            .seek(SeekFrom::Start(previous_length))
            .map_err(|error| format!("tail seek failed: {}", error))?;
        binlog
            .flush()
            .map_err(|error| format!("rollback flush failed: {}", error))?;
        binlog
            .sync_all()
            .map_err(|error| format!("rollback sync failed: {}", error))?;
        Ok(())
    })();

    match rollback_result {
        Ok(()) => StorageFailure::rejected(format!(
            "Binlog append was durably rolled back after I/O failure: {}",
            original_error
        )),
        Err(rollback_error) => StorageFailure::indeterminate(format!(
            "Binlog append failed after possible modification ({}) and durable tail rollback could not be proven ({})",
            original_error, rollback_error
        )),
    }
}

fn encode_framed_binlog_record(
    sequence: u64,
    record: &[u8],
    output: &mut Vec<u8>,
) -> Result<(), StorageFailure> {
    let encoded = encode_versioned_binlog_record(sequence, record)
        .map_err(|error| StorageFailure::rejected(error.to_string()))?;
    let length = u32::try_from(encoded.len())
        .map_err(|_| StorageFailure::rejected("Binlog record exceeds the format limit"))?;
    let additional = 4usize
        .checked_add(encoded.len())
        .and_then(|additional| output.len().checked_add(additional).map(|_| additional))
        .ok_or_else(|| {
            StorageFailure::rejected("Binlog append group exceeds addressable memory")
        })?;
    output.try_reserve(additional).map_err(|_| {
        StorageFailure::rejected("Unable to allocate the encoded binlog append group")
    })?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(&encoded);
    Ok(())
}

fn append_binlog_tail<T: BinlogIo>(
    binlog: &mut T,
    encoded_tail: &[u8],
    fsync_policy: FsyncPolicy,
) -> StorageResult {
    let previous_length = binlog
        .seek(SeekFrom::End(0))
        .map_err(|error| StorageFailure::rejected(format!("Binlog seek failed: {}", error)))?;

    fn write_all_tracking_modification(
        writer: &mut impl Write,
        mut buffer: &[u8],
        modified: &mut bool,
    ) -> std::io::Result<()> {
        while !buffer.is_empty() {
            match writer.write(buffer) {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write the complete binlog record",
                    ));
                }
                Ok(written) => {
                    *modified = true;
                    buffer = &buffer[written..];
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    let mut modified = false;
    let write_result = (|| -> std::io::Result<()> {
        write_all_tracking_modification(binlog, encoded_tail, &mut modified)?;
        binlog.flush()?;
        if fsync_policy == FsyncPolicy::Always {
            binlog.sync_data()?;
        }
        Ok(())
    })();

    match write_result {
        Ok(()) => Ok(()),
        Err(error) if !modified => Err(StorageFailure::rejected(format!(
            "Binlog append failed before modifying the file: {}",
            error
        ))),
        Err(error) => Err(rollback_binlog_tail(binlog, previous_length, error)),
    }
}

fn append_binlog_records<T: BinlogIo>(
    binlog: &mut T,
    records: &[(u64, Vec<u8>)],
    fsync_policy: FsyncPolicy,
) -> StorageResult {
    if records.is_empty() {
        return Err(StorageFailure::rejected(
            "A binlog append group must contain at least one record",
        ));
    }
    let mut encoded_tail = Vec::new();
    for (sequence, record) in records {
        encode_framed_binlog_record(*sequence, record, &mut encoded_tail)?;
    }
    append_binlog_tail(binlog, &encoded_tail, fsync_policy)
}

pub(crate) async fn run_binlog_worker<T: BinlogIo>(
    mut receiver: mpsc::Receiver<LogMessage>,
    binlog: Arc<std::sync::Mutex<T>>,
    fsync_policy: FsyncPolicy,
) {
    while let Some(message) = receiver.recv().await {
        match message {
            LogMessage::Append {
                records,
                completion,
            } => {
                let result = binlog
                    .lock()
                    .map_err(|_| StorageFailure::indeterminate("Binlog file lock is poisoned"))
                    .and_then(|mut file| append_binlog_records(&mut *file, &records, fsync_policy));
                let _ = completion.send(result);
            }
            LogMessage::Flush { completion } => {
                let result = (|| -> StorageResult {
                    let mut file = binlog.lock().map_err(|_| {
                        StorageFailure::indeterminate("Binlog file lock is poisoned")
                    })?;
                    file.flush().map_err(|error| {
                        StorageFailure::indeterminate(format!("Binlog flush failed: {}", error))
                    })?;
                    file.sync_all().map_err(|error| {
                        StorageFailure::indeterminate(format!("Binlog sync failed: {}", error))
                    })?;
                    Ok(())
                })();
                let _ = completion.send(result);
            }
            LogMessage::SyncData { completion } => {
                let result = (|| -> StorageResult {
                    let mut file = binlog.lock().map_err(|_| {
                        StorageFailure::indeterminate("Binlog file lock is poisoned")
                    })?;
                    file.sync_data().map_err(|error| {
                        StorageFailure::indeterminate(format!("Binlog sync failed: {}", error))
                    })?;
                    Ok(())
                })();
                let _ = completion.send(result);
            }
            LogMessage::Truncate { completion } => {
                let result = (|| -> StorageResult {
                    let mut file = binlog.lock().map_err(|_| {
                        StorageFailure::indeterminate("Binlog file lock is poisoned")
                    })?;
                    file.flush().map_err(|error| {
                        StorageFailure::rejected(format!(
                            "Binlog was not truncated because the pre-truncation flush failed: {}",
                            error
                        ))
                    })?;
                    file.truncate(0).map_err(|error| match error {
                        TruncateError::Unchanged(error) => StorageFailure::rejected(format!(
                            "Binlog truncation did not modify the file: {}",
                            error
                        )),
                        TruncateError::Indeterminate(error) => StorageFailure::indeterminate(
                            format!("Binlog truncation outcome is indeterminate: {}", error),
                        ),
                    })?;
                    file.sync_all().map_err(|error| {
                        StorageFailure::indeterminate(format!(
                            "Binlog truncation completed but could not be durably synchronized: {}",
                            error
                        ))
                    })?;
                    Ok(())
                })();
                let _ = completion.send(result);
            }
        }
    }
}

/// Owns the ordering and visibility guards for a durable state transition.
///
/// An owned boundary can move into a finalizer task, allowing persistence to
/// complete even when the originating client connection is cancelled.
pub(crate) struct CommitBoundary {
    write_guard: tokio::sync::OwnedMutexGuard<()>,
    visibility_guard: tokio::sync::OwnedRwLockWriteGuard<()>,
}

impl CommitBoundary {
    async fn acquire(
        write_gate: &Arc<tokio::sync::Mutex<()>>,
        visibility_gate: &Arc<tokio::sync::RwLock<()>>,
    ) -> Self {
        let write_guard = Arc::clone(write_gate).lock_owned().await;
        let visibility_guard = Arc::clone(visibility_gate).write_owned().await;
        Self {
            write_guard,
            visibility_guard,
        }
    }

    fn into_visibility_guard(self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        let Self {
            write_guard,
            visibility_guard,
        } = self;
        drop(write_guard);
        visibility_guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{
        CommittedEffect, PersistencePaths, PersistentEntry, load_data_from_paths,
    };
    use bytes::Bytes;
    use onyxdb::engine::OnyxValue;
    use std::fs::OpenOptions;
    use std::io::Error;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let id = NEXT_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "onyxdb-runtime-fault-{}-{}",
                std::process::id(),
                id
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn paths(&self) -> PersistencePaths {
            PersistencePaths::in_directory(&self.path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[derive(Clone, Copy)]
    enum InjectedTruncateFailure {
        Unchanged,
        Indeterminate,
        PartiallyApplied,
    }

    #[derive(Default)]
    struct FaultPlan {
        short_write_on_call: Option<(usize, usize)>,
        write_error_on_call: Option<usize>,
        flush_error_on_call: Option<usize>,
        sync_data_error_on_call: Option<usize>,
        sync_all_error_on_call: Option<usize>,
        truncate_error_on_call: Option<(usize, InjectedTruncateFailure)>,
    }

    struct FaultInjectingFile {
        file: File,
        plan: FaultPlan,
        write_calls: usize,
        flush_calls: usize,
        sync_data_calls: usize,
        sync_all_calls: usize,
        truncate_calls: usize,
    }

    impl FaultInjectingFile {
        fn open(path: &Path, plan: FaultPlan) -> Self {
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            Self {
                file,
                plan,
                write_calls: 0,
                flush_calls: 0,
                sync_data_calls: 0,
                sync_all_calls: 0,
                truncate_calls: 0,
            }
        }

        fn injected_error(operation: &str) -> Error {
            Error::other(format!("injected {} failure", operation))
        }
    }

    impl Write for FaultInjectingFile {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.write_calls += 1;
            if self.plan.write_error_on_call == Some(self.write_calls) {
                return Err(Self::injected_error("write"));
            }
            if let Some((call, limit)) = self.plan.short_write_on_call
                && call == self.write_calls
            {
                return self.file.write(&buffer[..buffer.len().min(limit)]);
            }
            self.file.write(buffer)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.flush_calls += 1;
            if self.plan.flush_error_on_call == Some(self.flush_calls) {
                return Err(Self::injected_error("flush"));
            }
            self.file.flush()
        }
    }

    impl Seek for FaultInjectingFile {
        fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
            self.file.seek(position)
        }
    }

    impl BinlogIo for FaultInjectingFile {
        fn sync_data(&mut self) -> std::io::Result<()> {
            self.sync_data_calls += 1;
            if self.plan.sync_data_error_on_call == Some(self.sync_data_calls) {
                return Err(Self::injected_error("sync_data"));
            }
            self.file.sync_data()
        }

        fn sync_all(&mut self) -> std::io::Result<()> {
            self.sync_all_calls += 1;
            if self.plan.sync_all_error_on_call == Some(self.sync_all_calls) {
                return Err(Self::injected_error("sync_all"));
            }
            self.file.sync_all()
        }

        fn truncate(&mut self, length: u64) -> Result<(), TruncateError> {
            self.truncate_calls += 1;
            if let Some((call, disposition)) = self.plan.truncate_error_on_call
                && call == self.truncate_calls
            {
                let error = Self::injected_error("truncate");
                return Err(match disposition {
                    InjectedTruncateFailure::Unchanged => TruncateError::Unchanged(error),
                    InjectedTruncateFailure::Indeterminate => TruncateError::Indeterminate(error),
                    InjectedTruncateFailure::PartiallyApplied => {
                        let current_length = self.file.metadata().unwrap().len();
                        self.file.set_len(current_length / 2).unwrap();
                        TruncateError::Indeterminate(error)
                    }
                });
            }
            self.file
                .set_len(length)
                .map_err(TruncateError::Indeterminate)
        }
    }

    fn put_batch_for(key: &'static [u8], value: &'static [u8]) -> CommittedBatch {
        CommittedBatch::new(vec![CommittedEffect::Put {
            key: Bytes::from_static(key),
            entry: PersistentEntry {
                value: OnyxValue::Blob(Bytes::from_static(value)),
                expires_at: None,
            },
        }])
        .unwrap()
    }

    fn put_batch() -> CommittedBatch {
        put_batch_for(b"key", b"accepted")
    }

    async fn append_with_fault(plan: FaultPlan) -> (PersistenceError, TestDirectory) {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let io = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            plan,
        )));
        let (sender, receiver) = mpsc::channel(1);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::Always,
        ));
        let handle = BinlogHandle::new(sender);
        let error = handle.append_batch(1, &put_batch()).await.unwrap_err();
        drop(handle);
        worker.await.unwrap();
        drop(io);
        (error, directory)
    }

    fn recovered_value(directory: &TestDirectory) -> Option<OnyxValue> {
        let store = ShardedStore::new();
        load_data_from_paths(&store, &directory.paths()).unwrap();
        store
            .get_entry(&Bytes::from_static(b"key"))
            .map(|entry| entry.value)
    }

    #[tokio::test]
    async fn write_failure_before_bytes_is_definitively_rejected_without_truncation() {
        let (error, directory) = append_with_fault(FaultPlan {
            write_error_on_call: Some(1),
            truncate_error_on_call: Some((1, InjectedTruncateFailure::Indeterminate)),
            ..FaultPlan::default()
        })
        .await;

        assert!(!error.is_indeterminate());
        assert_eq!(recovered_value(&directory), None);
    }

    #[tokio::test]
    async fn partial_write_is_removed_before_rejection() {
        let (error, directory) = append_with_fault(FaultPlan {
            short_write_on_call: Some((1, 2)),
            write_error_on_call: Some(2),
            ..FaultPlan::default()
        })
        .await;

        assert!(!error.is_indeterminate());
        assert_eq!(
            std::fs::metadata(directory.paths().binlog).unwrap().len(),
            0
        );
        assert_eq!(recovered_value(&directory), None);
    }

    #[tokio::test]
    async fn flush_failure_is_removed_before_rejection() {
        let (error, directory) = append_with_fault(FaultPlan {
            flush_error_on_call: Some(1),
            ..FaultPlan::default()
        })
        .await;

        assert!(!error.is_indeterminate());
        assert_eq!(recovered_value(&directory), None);
    }

    #[tokio::test]
    async fn sync_failure_is_removed_before_rejection() {
        let (error, directory) = append_with_fault(FaultPlan {
            sync_data_error_on_call: Some(1),
            ..FaultPlan::default()
        })
        .await;

        assert!(!error.is_indeterminate());
        assert_eq!(recovered_value(&directory), None);
    }

    #[tokio::test]
    async fn rollback_truncate_failure_is_indeterminate_and_recovery_replays_record() {
        let (error, directory) = append_with_fault(FaultPlan {
            sync_data_error_on_call: Some(1),
            truncate_error_on_call: Some((1, InjectedTruncateFailure::Indeterminate)),
            ..FaultPlan::default()
        })
        .await;

        assert!(error.is_indeterminate());
        assert_eq!(
            recovered_value(&directory),
            Some(OnyxValue::Blob(Bytes::from_static(b"accepted")))
        );
    }

    #[tokio::test]
    async fn rollback_sync_failure_remains_indeterminate() {
        let (error, directory) = append_with_fault(FaultPlan {
            sync_data_error_on_call: Some(1),
            sync_all_error_on_call: Some(1),
            ..FaultPlan::default()
        })
        .await;

        assert!(error.is_indeterminate());
        assert_eq!(recovered_value(&directory), None);
    }

    async fn truncate_with_fault(plan: FaultPlan) -> (PersistenceError, u64) {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        std::fs::write(&paths.binlog, b"existing history").unwrap();
        let io = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            plan,
        )));
        let (sender, receiver) = mpsc::channel(1);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::Always,
        ));
        let handle = BinlogHandle::new(sender);
        let error = handle.truncate().await.unwrap_err();
        drop(handle);
        worker.await.unwrap();
        drop(io);
        let length = std::fs::metadata(&paths.binlog).unwrap().len();
        (error, length)
    }

    #[tokio::test]
    async fn truncate_failure_known_unchanged_is_rejected() {
        let (error, length) = truncate_with_fault(FaultPlan {
            truncate_error_on_call: Some((1, InjectedTruncateFailure::Unchanged)),
            ..FaultPlan::default()
        })
        .await;

        assert!(!error.is_indeterminate());
        assert_eq!(length, b"existing history".len() as u64);
    }

    #[tokio::test]
    async fn truncate_failure_with_unknown_effect_is_indeterminate() {
        let (error, length) = truncate_with_fault(FaultPlan {
            truncate_error_on_call: Some((1, InjectedTruncateFailure::Indeterminate)),
            ..FaultPlan::default()
        })
        .await;

        assert!(error.is_indeterminate());
        assert_eq!(length, b"existing history".len() as u64);
    }

    #[tokio::test]
    async fn partially_applied_truncate_is_indeterminate() {
        let original_length = b"existing history".len() as u64;
        let (error, length) = truncate_with_fault(FaultPlan {
            truncate_error_on_call: Some((1, InjectedTruncateFailure::PartiallyApplied)),
            ..FaultPlan::default()
        })
        .await;

        assert!(error.is_indeterminate());
        assert!(length > 0);
        assert!(length < original_length);
    }

    #[tokio::test]
    async fn partial_compaction_truncate_fail_stops_and_recovers_installed_snapshot() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let io = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            FaultPlan {
                truncate_error_on_call: Some((1, InjectedTruncateFailure::PartiallyApplied)),
                ..FaultPlan::default()
            },
        )));
        let (sender, receiver) = mpsc::channel(4);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::Always,
        ));
        let handle = BinlogHandle::new(sender);
        handle.append_batch(1, &put_batch()).await.unwrap();
        let runtime = CommitRuntime::new(handle, 7, paths.clone());
        let store = Arc::new(ShardedStore::new());
        store.set("key".to_string(), "snapshot".to_string());

        let error = runtime
            .compact(&store, &AtomicU64::new(0))
            .await
            .unwrap_err();

        assert!(error.is_indeterminate());
        assert!(runtime.is_fail_stopped());
        drop(runtime);
        worker.await.unwrap();
        drop(io);

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(state.snapshot_watermark, 7);
        assert_eq!(recovered.get("key"), Ok(Some("snapshot".to_string())));
    }

    #[tokio::test]
    async fn truncate_sync_failure_is_indeterminate() {
        let (error, length) = truncate_with_fault(FaultPlan {
            sync_all_error_on_call: Some(1),
            ..FaultPlan::default()
        })
        .await;

        assert!(error.is_indeterminate());
        assert_eq!(length, 0);
    }

    #[tokio::test]
    async fn unavailable_worker_before_enqueue_is_a_definitive_rejection() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let error = BinlogHandle::new(sender)
            .append_batch(1, &put_batch())
            .await
            .unwrap_err();

        assert!(!error.is_indeterminate());
    }

    #[tokio::test]
    async fn grouped_append_preserves_logical_records_and_syncs_once() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let io = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            FaultPlan::default(),
        )));
        let (sender, receiver) = mpsc::channel(1);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::Always,
        ));
        let handle = BinlogHandle::new(sender);

        handle
            .append_batches(&[
                (1, put_batch_for(b"first", b"one")),
                (2, put_batch_for(b"second", b"two")),
            ])
            .await
            .unwrap();

        {
            let io = io.lock().unwrap();
            assert_eq!(io.write_calls, 1);
            assert_eq!(io.flush_calls, 1);
            assert_eq!(io.sync_data_calls, 1);
        }
        drop(handle);
        worker.await.unwrap();
        drop(io);

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(recovery.last_sequence, 2);
        assert_eq!(recovered.get("first"), Ok(Some("one".to_string())));
        assert_eq!(recovered.get("second"), Ok(Some("two".to_string())));
    }

    #[tokio::test]
    async fn grouped_append_failure_rolls_back_the_entire_group_tail() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let first_batch = put_batch_for(b"first", b"one");
        let first_payload = encode_committed_batch(&first_batch).unwrap();
        let first_record_length = encode_versioned_binlog_record(1, &first_payload)
            .unwrap()
            .len()
            + 4;
        let io = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            FaultPlan {
                short_write_on_call: Some((1, first_record_length + 7)),
                write_error_on_call: Some(2),
                ..FaultPlan::default()
            },
        )));
        let (sender, receiver) = mpsc::channel(1);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::Always,
        ));
        let handle = BinlogHandle::new(sender);

        let error = handle
            .append_batches(&[(1, first_batch), (2, put_batch_for(b"second", b"two"))])
            .await
            .unwrap_err();

        assert!(!error.is_indeterminate());
        assert_eq!(std::fs::metadata(&paths.binlog).unwrap().len(), 0);
        drop(handle);
        worker.await.unwrap();
        drop(io);

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(recovery.last_sequence, 0);
        assert_eq!(recovered.get("first"), Ok(None));
        assert_eq!(recovered.get("second"), Ok(None));
    }

    #[tokio::test]
    async fn grouped_append_rejection_preserves_the_preexisting_durable_prefix() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let first_batch = put_batch_for(b"first", b"one");
        let first_payload = encode_committed_batch(&first_batch).unwrap();
        let first_record_length = encode_versioned_binlog_record(2, &first_payload)
            .unwrap()
            .len()
            + 4;
        let io = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            FaultPlan {
                short_write_on_call: Some((2, first_record_length + 7)),
                write_error_on_call: Some(3),
                ..FaultPlan::default()
            },
        )));
        let (sender, receiver) = mpsc::channel(2);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::Always,
        ));
        let handle = BinlogHandle::new(sender);
        handle
            .append_batch(1, &put_batch_for(b"durable", b"before"))
            .await
            .unwrap();

        let error = handle
            .append_batches(&[(2, first_batch), (3, put_batch_for(b"second", b"two"))])
            .await
            .unwrap_err();

        assert!(!error.is_indeterminate());
        drop(handle);
        worker.await.unwrap();
        drop(io);

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(recovery.last_sequence, 1);
        assert_eq!(recovered.get("durable"), Ok(Some("before".to_string())));
        assert_eq!(recovered.get("first"), Ok(None));
        assert_eq!(recovered.get("second"), Ok(None));
    }

    #[test]
    fn crash_torn_group_recovers_the_complete_logical_prefix_only() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let first = encode_committed_batch(&put_batch_for(b"first", b"one")).unwrap();
        let second = encode_committed_batch(&put_batch_for(b"second", b"two")).unwrap();
        let mut encoded_tail = Vec::new();
        encode_framed_binlog_record(1, &first, &mut encoded_tail).unwrap();
        let first_record_length = encoded_tail.len();
        encode_framed_binlog_record(2, &second, &mut encoded_tail).unwrap();
        let torn_length = first_record_length + 7;
        let mut file = File::create(&paths.binlog).unwrap();
        file.write_all(&encoded_tail[..torn_length]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &paths).unwrap();

        assert_eq!(recovery.last_sequence, 1);
        assert_eq!(recovered.get("first"), Ok(Some("one".to_string())));
        assert_eq!(recovered.get("second"), Ok(None));
        assert_eq!(
            std::fs::metadata(&paths.binlog).unwrap().len(),
            first_record_length as u64
        );
    }

    #[tokio::test]
    async fn dropped_completion_after_worker_ownership_is_indeterminate() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let file = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            FaultPlan::default(),
        )));
        let (sender, mut receiver) = mpsc::channel(1);
        let worker_file = Arc::clone(&file);
        let worker = tokio::spawn(async move {
            if let Some(LogMessage::Append {
                records,
                completion,
            }) = receiver.recv().await
            {
                append_binlog_records(
                    &mut *worker_file.lock().unwrap(),
                    &records,
                    FsyncPolicy::Always,
                )
                .unwrap();
                drop(completion);
            }
        });
        let error = BinlogHandle::new(sender)
            .append_batch(1, &put_batch())
            .await
            .unwrap_err();
        worker.await.unwrap();
        drop(file);

        assert!(error.is_indeterminate());
        assert_eq!(
            recovered_value(&directory),
            Some(OnyxValue::Blob(Bytes::from_static(b"accepted")))
        );
    }
}
