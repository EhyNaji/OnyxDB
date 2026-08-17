use super::{
    CommittedBatch, PersistenceError, ReplicaIdentity, durable_rename, encode_committed_batch,
    encode_versioned_binlog_record, framed_versioned_binlog_record_length, sync_parent_directory,
    write_replica_identity, write_snapshot_file,
};
use crate::config::FsyncPolicy;
use onyxdb::store::ShardedStore;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
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
pub(crate) type StoragePositionResult = Result<u64, StorageFailure>;

fn duration_nanoseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn observe_max(metric: &AtomicU64, value: u64) {
    let mut current = metric.load(Ordering::Relaxed);
    while value > current {
        match metric.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct BinlogMetricsSnapshot {
    pub(crate) append_attempts_total: u64,
    pub(crate) append_accepted_total: u64,
    pub(crate) append_rejected_total: u64,
    pub(crate) append_indeterminate_total: u64,
    pub(crate) records_accepted_total: u64,
    pub(crate) bytes_accepted_total: u64,
    pub(crate) records_per_append_max: u64,
    pub(crate) bytes_per_append_max: u64,
    pub(crate) append_ack_nanoseconds_total: u64,
    pub(crate) append_ack_nanoseconds_max: u64,
}

#[derive(Default)]
struct BinlogMetrics {
    append_attempts_total: AtomicU64,
    append_accepted_total: AtomicU64,
    append_rejected_total: AtomicU64,
    append_indeterminate_total: AtomicU64,
    records_accepted_total: AtomicU64,
    bytes_accepted_total: AtomicU64,
    records_per_append_max: AtomicU64,
    bytes_per_append_max: AtomicU64,
    append_ack_nanoseconds_total: AtomicU64,
    append_ack_nanoseconds_max: AtomicU64,
}

impl BinlogMetrics {
    fn record_append(
        &self,
        records: usize,
        bytes: usize,
        elapsed: Duration,
        disposition: Result<(), StorageFailureDisposition>,
    ) {
        self.append_attempts_total.fetch_add(1, Ordering::Relaxed);
        let elapsed = duration_nanoseconds(elapsed);
        self.append_ack_nanoseconds_total
            .fetch_add(elapsed, Ordering::Relaxed);
        observe_max(&self.append_ack_nanoseconds_max, elapsed);
        match disposition {
            Ok(()) => {
                self.append_accepted_total.fetch_add(1, Ordering::Relaxed);
                let records = u64::try_from(records).unwrap_or(u64::MAX);
                let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
                self.records_accepted_total
                    .fetch_add(records, Ordering::Relaxed);
                self.bytes_accepted_total
                    .fetch_add(bytes, Ordering::Relaxed);
                observe_max(&self.records_per_append_max, records);
                observe_max(&self.bytes_per_append_max, bytes);
            }
            Err(StorageFailureDisposition::Rejected) => {
                self.append_rejected_total.fetch_add(1, Ordering::Relaxed);
            }
            Err(StorageFailureDisposition::Indeterminate) => {
                self.append_indeterminate_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> BinlogMetricsSnapshot {
        BinlogMetricsSnapshot {
            append_attempts_total: self.append_attempts_total.load(Ordering::Relaxed),
            append_accepted_total: self.append_accepted_total.load(Ordering::Relaxed),
            append_rejected_total: self.append_rejected_total.load(Ordering::Relaxed),
            append_indeterminate_total: self.append_indeterminate_total.load(Ordering::Relaxed),
            records_accepted_total: self.records_accepted_total.load(Ordering::Relaxed),
            bytes_accepted_total: self.bytes_accepted_total.load(Ordering::Relaxed),
            records_per_append_max: self.records_per_append_max.load(Ordering::Relaxed),
            bytes_per_append_max: self.bytes_per_append_max.load(Ordering::Relaxed),
            append_ack_nanoseconds_total: self.append_ack_nanoseconds_total.load(Ordering::Relaxed),
            append_ack_nanoseconds_max: self.append_ack_nanoseconds_max.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CompactionMetricsSnapshot {
    pub(crate) attempts_total: u64,
    pub(crate) completed_total: u64,
    pub(crate) failed_total: u64,
    pub(crate) in_progress: u64,
    pub(crate) duration_nanoseconds_total: u64,
    pub(crate) duration_nanoseconds_last: u64,
    pub(crate) duration_nanoseconds_max: u64,
    pub(crate) gate_wait_nanoseconds_total: u64,
    pub(crate) gate_wait_nanoseconds_max: u64,
    pub(crate) serialization_wait_nanoseconds_total: u64,
    pub(crate) serialization_wait_nanoseconds_max: u64,
    pub(crate) write_pause_nanoseconds_total: u64,
    pub(crate) write_pause_nanoseconds_max: u64,
    pub(crate) checkpoint_nanoseconds_total: u64,
    pub(crate) checkpoint_nanoseconds_max: u64,
    pub(crate) snapshot_capture_nanoseconds_total: u64,
    pub(crate) snapshot_capture_nanoseconds_max: u64,
    pub(crate) snapshot_write_nanoseconds_total: u64,
    pub(crate) snapshot_write_nanoseconds_max: u64,
    pub(crate) suffix_prepare_nanoseconds_total: u64,
    pub(crate) suffix_prepare_nanoseconds_max: u64,
    pub(crate) rotation_nanoseconds_total: u64,
    pub(crate) rotation_nanoseconds_max: u64,
    pub(crate) retained_bytes_total: u64,
    pub(crate) retained_bytes_max: u64,
}

#[derive(Default)]
struct DurationMetric {
    total: AtomicU64,
    last: AtomicU64,
    max: AtomicU64,
}

impl DurationMetric {
    fn observe(&self, duration: Duration) {
        let duration = duration_nanoseconds(duration);
        self.total.fetch_add(duration, Ordering::Relaxed);
        self.last.store(duration, Ordering::Relaxed);
        observe_max(&self.max, duration);
    }
}

#[derive(Default)]
struct CompactionMetrics {
    attempts_total: AtomicU64,
    completed_total: AtomicU64,
    failed_total: AtomicU64,
    in_progress: AtomicU64,
    duration: DurationMetric,
    gate_wait: DurationMetric,
    serialization_wait: DurationMetric,
    write_pause: DurationMetric,
    checkpoint: DurationMetric,
    snapshot_capture: DurationMetric,
    snapshot_write: DurationMetric,
    suffix_prepare: DurationMetric,
    rotation: DurationMetric,
    retained_bytes_total: AtomicU64,
    retained_bytes_max: AtomicU64,
}

impl CompactionMetrics {
    fn snapshot(&self) -> CompactionMetricsSnapshot {
        CompactionMetricsSnapshot {
            attempts_total: self.attempts_total.load(Ordering::Relaxed),
            completed_total: self.completed_total.load(Ordering::Relaxed),
            failed_total: self.failed_total.load(Ordering::Relaxed),
            in_progress: self.in_progress.load(Ordering::Relaxed),
            duration_nanoseconds_total: self.duration.total.load(Ordering::Relaxed),
            duration_nanoseconds_last: self.duration.last.load(Ordering::Relaxed),
            duration_nanoseconds_max: self.duration.max.load(Ordering::Relaxed),
            gate_wait_nanoseconds_total: self.gate_wait.total.load(Ordering::Relaxed),
            gate_wait_nanoseconds_max: self.gate_wait.max.load(Ordering::Relaxed),
            serialization_wait_nanoseconds_total: self
                .serialization_wait
                .total
                .load(Ordering::Relaxed),
            serialization_wait_nanoseconds_max: self.serialization_wait.max.load(Ordering::Relaxed),
            write_pause_nanoseconds_total: self.write_pause.total.load(Ordering::Relaxed),
            write_pause_nanoseconds_max: self.write_pause.max.load(Ordering::Relaxed),
            checkpoint_nanoseconds_total: self.checkpoint.total.load(Ordering::Relaxed),
            checkpoint_nanoseconds_max: self.checkpoint.max.load(Ordering::Relaxed),
            snapshot_capture_nanoseconds_total: self.snapshot_capture.total.load(Ordering::Relaxed),
            snapshot_capture_nanoseconds_max: self.snapshot_capture.max.load(Ordering::Relaxed),
            snapshot_write_nanoseconds_total: self.snapshot_write.total.load(Ordering::Relaxed),
            snapshot_write_nanoseconds_max: self.snapshot_write.max.load(Ordering::Relaxed),
            suffix_prepare_nanoseconds_total: self.suffix_prepare.total.load(Ordering::Relaxed),
            suffix_prepare_nanoseconds_max: self.suffix_prepare.max.load(Ordering::Relaxed),
            rotation_nanoseconds_total: self.rotation.total.load(Ordering::Relaxed),
            rotation_nanoseconds_max: self.rotation.max.load(Ordering::Relaxed),
            retained_bytes_total: self.retained_bytes_total.load(Ordering::Relaxed),
            retained_bytes_max: self.retained_bytes_max.load(Ordering::Relaxed),
        }
    }

    fn observe_retained_bytes(&self, retained_bytes: u64) {
        self.retained_bytes_total
            .fetch_add(retained_bytes, Ordering::Relaxed);
        observe_max(&self.retained_bytes_max, retained_bytes);
    }
}

struct CompactionMeasurement<'a> {
    metrics: &'a CompactionMetrics,
    started_at: Instant,
    finished: bool,
}

impl<'a> CompactionMeasurement<'a> {
    fn start(metrics: &'a CompactionMetrics) -> Self {
        metrics.attempts_total.fetch_add(1, Ordering::Relaxed);
        metrics.in_progress.fetch_add(1, Ordering::Relaxed);
        Self {
            metrics,
            started_at: Instant::now(),
            finished: false,
        }
    }

    fn finish(mut self, success: bool) {
        self.finished = true;
        if success {
            self.metrics.completed_total.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.failed_total.fetch_add(1, Ordering::Relaxed);
        }
        self.record_end();
    }

    fn record_end(&self) {
        self.metrics.in_progress.fetch_sub(1, Ordering::Relaxed);
        self.metrics.duration.observe(self.started_at.elapsed());
    }
}

impl Drop for CompactionMeasurement<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.metrics.failed_total.fetch_add(1, Ordering::Relaxed);
            self.record_end();
        }
    }
}

type SnapshotWriteOperation = Box<
    dyn FnOnce(
            Vec<(bytes::Bytes, onyxdb::engine::DataEntry)>,
            u64,
            super::PersistencePaths,
        ) -> Result<(), PersistenceError>
        + Send
        + 'static,
>;

type SuffixPrepareOperation = Box<
    dyn FnOnce(super::PersistencePaths, u64, u64) -> Result<(), PersistenceError> + Send + 'static,
>;

#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum TruncateError {
    Unchanged(std::io::Error),
    Indeterminate(std::io::Error),
}

#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum BinlogCompactionError {
    Unchanged(std::io::Error),
    Indeterminate(std::io::Error),
}

pub(crate) trait BinlogIo: Write + Seek + Send + 'static {
    fn sync_data(&mut self) -> std::io::Result<()>;
    fn sync_all(&mut self) -> std::io::Result<()>;
    fn truncate(&mut self, length: u64) -> Result<(), TruncateError>;
    fn compact_suffix(
        &mut self,
        _retained_from: u64,
        _prepared_through: u64,
    ) -> Result<u64, BinlogCompactionError> {
        Err(BinlogCompactionError::Unchanged(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Binlog suffix compaction is unsupported by this storage backend",
        )))
    }
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

/// Owns the active binlog handle and its crash-recoverable replacement paths.
///
/// Suffix replacement is only requested while the authoritative commit
/// boundary is held. The old active file is renamed to the backup before the
/// synchronized temporary suffix becomes active, so every crash point leaves
/// either the original history or the replacement discoverable by recovery.
pub(crate) struct ManagedBinlogFile {
    file: Option<File>,
    paths: super::PersistencePaths,
}

impl ManagedBinlogFile {
    pub(crate) fn new(file: File, paths: super::PersistencePaths) -> Self {
        Self {
            file: Some(file),
            paths,
        }
    }

    fn file_mut(&mut self) -> std::io::Result<&mut File> {
        self.file.as_mut().ok_or_else(|| {
            std::io::Error::other("Active binlog handle is unavailable after rotation failure")
        })
    }

    fn reopen_active(&mut self) -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create(false)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.paths.binlog)?;
        file.seek(SeekFrom::End(0))?;
        self.file = Some(file);
        Ok(())
    }

    fn restore_backup_after_failed_rotation(
        &mut self,
        original_error: std::io::Error,
    ) -> Result<u64, BinlogCompactionError> {
        let restoration = (|| -> std::io::Result<()> {
            durable_rename(&self.paths.binlog_backup, &self.paths.binlog)?;
            sync_parent_directory(&self.paths.binlog)?;
            self.reopen_active()
        })();
        match restoration {
            Ok(()) => Err(BinlogCompactionError::Unchanged(original_error)),
            Err(restoration_error) => Err(BinlogCompactionError::Indeterminate(
                std::io::Error::other(format!(
                    "Binlog rotation failed ({original_error}) and the original active file could not be restored ({restoration_error})"
                )),
            )),
        }
    }

    fn remove_rotation_backup(&self) {
        match fs::remove_file(&self.paths.binlog_backup) {
            Ok(()) => {
                if let Err(error) = sync_parent_directory(&self.paths.binlog) {
                    tracing::warn!(
                        "Unable to synchronize redundant binlog backup cleanup: {}",
                        error
                    );
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => tracing::warn!(
                "Unable to remove redundant binlog backup {}: {}",
                self.paths.binlog_backup.display(),
                error
            ),
        }
    }
}

fn prepare_binlog_suffix(
    paths: &super::PersistencePaths,
    retained_from: u64,
    prepared_through: u64,
) -> Result<(), PersistenceError> {
    let retained_length = prepared_through.checked_sub(retained_from).ok_or_else(|| {
        PersistenceError::new(format!(
            "Binlog suffix preparation boundary {prepared_through} precedes checkpoint {retained_from}"
        ))
    })?;
    let mut active = OpenOptions::new().read(true).open(&paths.binlog)?;
    let active_length = active.metadata()?.len();
    if active_length < prepared_through {
        return Err(PersistenceError::new(format!(
            "Binlog suffix preparation boundary {prepared_through} exceeds active length {active_length}"
        )));
    }
    let mut temporary = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&paths.binlog_temp)?;
    active.seek(SeekFrom::Start(retained_from))?;
    let copied = std::io::copy(&mut active.take(retained_length), &mut temporary)?;
    if copied != retained_length {
        return Err(PersistenceError::new(format!(
            "Binlog suffix preparation copied {copied} bytes instead of {retained_length}"
        )));
    }
    temporary.flush()?;
    temporary.sync_all()?;
    drop(temporary);
    sync_parent_directory(&paths.binlog_temp)?;
    Ok(())
}

impl Write for ManagedBinlogFile {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.file_mut()?.write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file_mut()?.flush()
    }
}

impl Seek for ManagedBinlogFile {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.file_mut()?.seek(position)
    }
}

impl BinlogIo for ManagedBinlogFile {
    fn sync_data(&mut self) -> std::io::Result<()> {
        self.file_mut()?.sync_data()
    }

    fn sync_all(&mut self) -> std::io::Result<()> {
        self.file_mut()?.sync_all()
    }

    fn truncate(&mut self, length: u64) -> Result<(), TruncateError> {
        self.file_mut()
            .map_err(TruncateError::Indeterminate)?
            .set_len(length)
            .map_err(TruncateError::Indeterminate)
    }

    fn compact_suffix(
        &mut self,
        retained_from: u64,
        prepared_through: u64,
    ) -> Result<u64, BinlogCompactionError> {
        let active_length = self
            .file_mut()
            .map_err(BinlogCompactionError::Indeterminate)?
            .metadata()
            .map_err(BinlogCompactionError::Unchanged)?
            .len();
        let retained_bytes = active_length.checked_sub(retained_from).ok_or_else(|| {
            BinlogCompactionError::Unchanged(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Binlog checkpoint {retained_from} exceeds active length {active_length}"),
            ))
        })?;
        let prepared_bytes = prepared_through.checked_sub(retained_from).ok_or_else(|| {
            BinlogCompactionError::Unchanged(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Binlog prepared boundary {prepared_through} precedes checkpoint {retained_from}"
                ),
            ))
        })?;
        if prepared_through > active_length {
            return Err(BinlogCompactionError::Unchanged(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Binlog prepared boundary {prepared_through} exceeds active length {active_length}"
                ),
            )));
        }

        let temporary_result = (|| -> std::io::Result<()> {
            let mut temporary = OpenOptions::new()
                .create(false)
                .append(true)
                .read(true)
                .open(&self.paths.binlog_temp)?;
            let temporary_length = temporary.metadata()?.len();
            if temporary_length != prepared_bytes {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Prepared binlog suffix contains {temporary_length} bytes instead of {prepared_bytes}"
                    ),
                ));
            }
            let active = self.file_mut()?;
            active.seek(SeekFrom::Start(prepared_through))?;
            let remaining_bytes = active_length - prepared_through;
            let copied = std::io::copy(&mut active.take(remaining_bytes), &mut temporary)?;
            if copied != remaining_bytes {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "Binlog suffix finalization copied {copied} bytes instead of {remaining_bytes}"
                    ),
                ));
            }
            temporary.flush()?;
            temporary.sync_all()?;
            drop(temporary);
            sync_parent_directory(&self.paths.binlog_temp)?;
            Ok(())
        })();
        if let Err(error) = temporary_result {
            if let Ok(active) = self.file_mut() {
                let _ = active.seek(SeekFrom::End(0));
            }
            return Err(BinlogCompactionError::Unchanged(error));
        }

        drop(self.file.take());
        if let Err(error) = durable_rename(&self.paths.binlog, &self.paths.binlog_backup) {
            return match self.reopen_active() {
                Ok(()) => Err(BinlogCompactionError::Unchanged(error)),
                Err(reopen_error) => Err(BinlogCompactionError::Indeterminate(
                    std::io::Error::other(format!(
                        "Binlog backup rename failed ({error}) and the active file could not be reopened ({reopen_error})"
                    )),
                )),
            };
        }
        if let Err(error) = sync_parent_directory(&self.paths.binlog_backup) {
            return self.restore_backup_after_failed_rotation(error);
        }
        if let Err(error) = durable_rename(&self.paths.binlog_temp, &self.paths.binlog) {
            return self.restore_backup_after_failed_rotation(error);
        }
        if let Err(error) = sync_parent_directory(&self.paths.binlog) {
            let _ = self.reopen_active();
            return Err(BinlogCompactionError::Indeterminate(error));
        }
        self.reopen_active()
            .map_err(BinlogCompactionError::Indeterminate)?;
        self.remove_rotation_backup();
        Ok(retained_bytes)
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
    /// Serializes every snapshot/binlog baseline replacement, including
    /// automatic compaction, clean shutdown, and replica full synchronization.
    pub(crate) compaction_gate: Arc<tokio::sync::Mutex<()>>,
    pub(crate) paths: super::PersistencePaths,
    /// Last sequence durably accepted and made authoritative in live state.
    repl_offset: AtomicU64,
    pub(crate) failure: std::sync::Mutex<Option<String>>,
    fail_stop_started: AtomicBool,
    fail_stop_reason: std::sync::Mutex<Option<String>>,
    fail_stop_notify: Notify,
    fail_stop_visibility_guard: std::sync::Mutex<Option<tokio::sync::OwnedRwLockWriteGuard<()>>>,
    compaction_metrics: CompactionMetrics,
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
            compaction_gate: Arc::new(tokio::sync::Mutex::new(())),
            paths,
            repl_offset: AtomicU64::new(initial_sequence),
            failure: std::sync::Mutex::new(None),
            fail_stop_started: AtomicBool::new(false),
            fail_stop_reason: std::sync::Mutex::new(None),
            fail_stop_notify: Notify::new(),
            fail_stop_visibility_guard: std::sync::Mutex::new(None),
            compaction_metrics: CompactionMetrics::default(),
        }
    }

    fn record_persisted_write(&self, compaction_threshold: usize) -> bool {
        self.write_count.fetch_add(1, Ordering::SeqCst) + 1 >= compaction_threshold
            && self
                .compaction_pending
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }

    /// Releases automatic-compaction ownership and atomically reacquires it
    /// when writes accepted during the previous attempt already crossed the
    /// next threshold. A racing writer either schedules its own attempt or
    /// observes this worker's reacquisition; no threshold crossing is lost.
    pub(crate) fn finish_compaction_schedule_and_rearm(
        &self,
        compaction_threshold: usize,
        retry_immediately: bool,
    ) -> bool {
        self.compaction_pending.store(false, Ordering::SeqCst);
        retry_immediately
            && self.write_count.load(Ordering::SeqCst) >= compaction_threshold
            && self
                .compaction_pending
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }

    pub(crate) fn sequence(&self) -> u64 {
        self.repl_offset.load(Ordering::SeqCst)
    }

    pub(crate) fn binlog_metrics(&self) -> BinlogMetricsSnapshot {
        self.binlog.metrics.snapshot()
    }

    pub(crate) fn compaction_metrics(&self) -> CompactionMetricsSnapshot {
        self.compaction_metrics.snapshot()
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
        self: &Arc<Self>,
        store: &Arc<ShardedStore>,
        upstream_replid: &Arc<AtomicU64>,
    ) -> Result<u64, PersistenceError> {
        self.compact_with_writer(
            Arc::clone(store),
            Arc::clone(upstream_replid),
            Box::new(|entries, watermark, paths| write_snapshot_file(entries, watermark, &paths)),
        )
        .await
    }

    async fn compact_with_writer(
        self: &Arc<Self>,
        store: Arc<ShardedStore>,
        upstream_replid: Arc<AtomicU64>,
        snapshot_writer: SnapshotWriteOperation,
    ) -> Result<u64, PersistenceError> {
        self.compact_with_operations(
            store,
            upstream_replid,
            snapshot_writer,
            Box::new(|paths, retained_from, prepared_through| {
                prepare_binlog_suffix(&paths, retained_from, prepared_through)
            }),
        )
        .await
    }

    async fn compact_with_operations(
        self: &Arc<Self>,
        store: Arc<ShardedStore>,
        upstream_replid: Arc<AtomicU64>,
        snapshot_writer: SnapshotWriteOperation,
        suffix_preparer: SuffixPrepareOperation,
    ) -> Result<u64, PersistenceError> {
        let (completion_tx, completion_rx) = oneshot::channel();
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            let worker_runtime = Arc::clone(&runtime);
            let worker = tokio::spawn(async move {
                worker_runtime
                    .compact_owned(store, upstream_replid, snapshot_writer, suffix_preparer)
                    .await
            });
            let result = match worker.await {
                Ok(result) => result,
                Err(error) => {
                    let failure = PersistenceError::indeterminate(format!(
                        "Snapshot compaction task was interrupted: {}",
                        error
                    ));
                    if !runtime.is_fail_stopped() {
                        runtime.enter_fail_stop(failure.to_string()).await;
                    }
                    Err(failure)
                }
            };
            let _ = completion_tx.send(result);
        });
        match completion_rx.await {
            Ok(result) => result,
            Err(_) => {
                let failure = PersistenceError::indeterminate(
                    "Snapshot compaction supervisor dropped the outcome",
                );
                if !self.is_fail_stopped() {
                    self.enter_fail_stop(failure.to_string()).await;
                }
                Err(failure)
            }
        }
    }

    async fn compact_owned(
        self: Arc<Self>,
        store: Arc<ShardedStore>,
        upstream_replid: Arc<AtomicU64>,
        snapshot_writer: SnapshotWriteOperation,
        suffix_preparer: SuffixPrepareOperation,
    ) -> Result<u64, PersistenceError> {
        let measurement = CompactionMeasurement::start(&self.compaction_metrics);
        let serialization_started = Instant::now();
        let _compaction_guard = self.compaction_gate.lock().await;
        self.compaction_metrics
            .serialization_wait
            .observe(serialization_started.elapsed());

        let gate_started = Instant::now();
        let boundary = self.acquire_commit_boundary().await;
        self.compaction_metrics
            .gate_wait
            .observe(gate_started.elapsed());
        let capture_pause_started = Instant::now();
        let checkpoint_started = Instant::now();
        let checkpoint = match self.binlog.checkpoint().await {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                self.compaction_metrics
                    .checkpoint
                    .observe(checkpoint_started.elapsed());
                self.compaction_metrics
                    .write_pause
                    .observe(capture_pause_started.elapsed());
                let error = error_with_context(error, "Binlog compaction checkpoint failed");
                if error.is_indeterminate() {
                    self.enter_fail_stop_with_boundary(boundary, error.to_string());
                }
                measurement.finish(false);
                return Err(error);
            }
        };
        self.compaction_metrics
            .checkpoint
            .observe(checkpoint_started.elapsed());

        let watermark = self.sequence();
        let compacted_write_count = self.write_count.load(Ordering::SeqCst);
        let capture_started = Instant::now();
        let entries = store.raw_entries();
        self.compaction_metrics
            .snapshot_capture
            .observe(capture_started.elapsed());
        drop(boundary);
        self.compaction_metrics
            .write_pause
            .observe(capture_pause_started.elapsed());

        let paths = self.paths.clone();
        let snapshot_started = Instant::now();
        let snapshot_result =
            match tokio::task::spawn_blocking(move || snapshot_writer(entries, watermark, paths))
                .await
            {
                Ok(result) => result.map_err(|error| {
                    PersistenceError::new(format!("Snapshot installation failed: {}", error))
                }),
                Err(error) => Err(PersistenceError::new(format!(
                    "Snapshot task failed: {}",
                    error
                ))),
            };
        self.compaction_metrics
            .snapshot_write
            .observe(snapshot_started.elapsed());
        if let Err(error) = snapshot_result {
            measurement.finish(false);
            return Err(error);
        }

        let prepare_checkpoint_started = Instant::now();
        let prepared_through = match self.binlog.checkpoint().await {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                self.compaction_metrics
                    .checkpoint
                    .observe(prepare_checkpoint_started.elapsed());
                let error =
                    error_with_context(error, "Binlog suffix preparation checkpoint failed");
                if error.is_indeterminate() && !self.is_fail_stopped() {
                    self.enter_fail_stop(error.to_string()).await;
                }
                measurement.finish(false);
                return Err(error);
            }
        };
        self.compaction_metrics
            .checkpoint
            .observe(prepare_checkpoint_started.elapsed());
        let suffix_prepare_started = Instant::now();
        let prepare_paths = self.paths.clone();
        let suffix_prepare_result = tokio::task::spawn_blocking(move || {
            suffix_preparer(prepare_paths, checkpoint, prepared_through)
        })
        .await;
        self.compaction_metrics
            .suffix_prepare
            .observe(suffix_prepare_started.elapsed());
        match suffix_prepare_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                measurement.finish(false);
                return Err(error_with_context(
                    error,
                    "Binlog suffix preparation failed",
                ));
            }
            Err(error) => {
                measurement.finish(false);
                return Err(PersistenceError::new(format!(
                    "Binlog suffix preparation task failed: {error}"
                )));
            }
        }

        let final_gate_started = Instant::now();
        let boundary = self.acquire_commit_boundary().await;
        self.compaction_metrics
            .gate_wait
            .observe(final_gate_started.elapsed());
        let rotation_pause_started = Instant::now();
        let rotation_started = Instant::now();
        let replid = upstream_replid.load(Ordering::SeqCst);
        if replid != 0
            && let Err(error) = write_replica_identity(
                &self.paths,
                ReplicaIdentity {
                    replid,
                    baseline_sequence: watermark,
                },
            )
        {
            self.compaction_metrics
                .rotation
                .observe(rotation_started.elapsed());
            self.compaction_metrics
                .write_pause
                .observe(rotation_pause_started.elapsed());
            drop(boundary);
            measurement.finish(false);
            return Err(error);
        }
        let retained_bytes = match self
            .binlog
            .compact_suffix(checkpoint, prepared_through)
            .await
        {
            Ok(retained_bytes) => retained_bytes,
            Err(error) => {
                let error = error_with_context(error, "Binlog suffix replacement failed");
                self.compaction_metrics
                    .rotation
                    .observe(rotation_started.elapsed());
                self.compaction_metrics
                    .write_pause
                    .observe(rotation_pause_started.elapsed());
                if error.is_indeterminate() {
                    self.enter_fail_stop_with_boundary(boundary, error.to_string());
                } else {
                    drop(boundary);
                }
                measurement.finish(false);
                return Err(error);
            }
        };
        self.compaction_metrics
            .observe_retained_bytes(retained_bytes);
        self.compaction_metrics
            .rotation
            .observe(rotation_started.elapsed());
        self.write_count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                Some(count.saturating_sub(compacted_write_count))
            })
            .expect("compaction write-count update cannot fail");
        self.compaction_metrics
            .write_pause
            .observe(rotation_pause_started.elapsed());
        drop(boundary);
        info!(
            "Compaction complete at sequence {}: snapshot installed and {} post-boundary binlog bytes retained",
            watermark, retained_bytes
        );
        measurement.finish(true);
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
    Checkpoint {
        completion: oneshot::Sender<StoragePositionResult>,
    },
    CompactSuffix {
        retained_from: u64,
        prepared_through: u64,
        completion: oneshot::Sender<StoragePositionResult>,
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
    metrics: Arc<BinlogMetrics>,
}

impl BinlogHandle {
    pub(crate) fn new(sender: mpsc::Sender<LogMessage>) -> Self {
        Self {
            sender,
            metrics: Arc::new(BinlogMetrics::default()),
        }
    }

    async fn append(&self, sequence: u64, record: Vec<u8>) -> Result<(), PersistenceError> {
        self.append_records(vec![(sequence, record)]).await
    }

    async fn append_records(&self, records: Vec<(u64, Vec<u8>)>) -> Result<(), PersistenceError> {
        let record_count = records.len();
        let physical_bytes = records.iter().try_fold(0usize, |total, (_, record)| {
            total
                .checked_add(framed_versioned_binlog_record_length(record.len())?)
                .ok_or_else(|| PersistenceError::new("Binlog append group length overflow"))
        })?;
        let started_at = Instant::now();
        let (completion_tx, completion_rx) = oneshot::channel();
        if self
            .sender
            .send(LogMessage::Append {
                records,
                completion: completion_tx,
            })
            .await
            .is_err()
        {
            self.metrics.record_append(
                record_count,
                physical_bytes,
                started_at.elapsed(),
                Err(StorageFailureDisposition::Rejected),
            );
            return Err(PersistenceError::new("Binlog worker is unavailable"));
        }
        match completion_rx.await {
            Ok(Ok(())) => {
                self.metrics.record_append(
                    record_count,
                    physical_bytes,
                    started_at.elapsed(),
                    Ok(()),
                );
                Ok(())
            }
            Ok(Err(error)) => {
                let disposition = error.disposition;
                self.metrics.record_append(
                    record_count,
                    physical_bytes,
                    started_at.elapsed(),
                    Err(disposition),
                );
                Err(error.into_persistence_error())
            }
            Err(_) => {
                self.metrics.record_append(
                    record_count,
                    physical_bytes,
                    started_at.elapsed(),
                    Err(StorageFailureDisposition::Indeterminate),
                );
                Err(PersistenceError::indeterminate(
                    "Binlog append completion was dropped",
                ))
            }
        }
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
        self.append_records(records).await
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

    /// Returns the active binlog length after every preceding worker operation
    /// has completed. Callers establishing a snapshot watermark must hold the
    /// authoritative commit boundary while pairing it with this record boundary.
    pub(crate) async fn checkpoint(&self) -> Result<u64, PersistenceError> {
        let (completion_tx, completion_rx) = oneshot::channel();
        self.sender
            .send(LogMessage::Checkpoint {
                completion: completion_tx,
            })
            .await
            .map_err(|_| {
                PersistenceError::indeterminate(
                    "Binlog worker is unavailable during compaction checkpoint",
                )
            })?;
        completion_rx
            .await
            .map_err(|_| {
                PersistenceError::indeterminate(
                    "Binlog compaction checkpoint completion was dropped",
                )
            })?
            .map_err(StorageFailure::into_persistence_error)
    }

    /// Atomically replaces the active binlog with bytes written after the
    /// supplied checkpoint. The worker retains a crash-recoverable backup until
    /// the synchronized suffix is active.
    pub(crate) async fn compact_suffix(
        &self,
        retained_from: u64,
        prepared_through: u64,
    ) -> Result<u64, PersistenceError> {
        let (completion_tx, completion_rx) = oneshot::channel();
        self.sender
            .send(LogMessage::CompactSuffix {
                retained_from,
                prepared_through,
                completion: completion_tx,
            })
            .await
            .map_err(|_| {
                PersistenceError::indeterminate(
                    "Binlog worker is unavailable during suffix replacement",
                )
            })?;
        completion_rx
            .await
            .map_err(|_| {
                PersistenceError::indeterminate("Binlog suffix replacement completion was dropped")
            })?
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
            LogMessage::Checkpoint { completion } => {
                let result = (|| -> StoragePositionResult {
                    let mut file = binlog.lock().map_err(|_| {
                        StorageFailure::indeterminate("Binlog file lock is poisoned")
                    })?;
                    file.flush().map_err(|error| {
                        StorageFailure::indeterminate(format!(
                            "Binlog compaction checkpoint flush failed: {}",
                            error
                        ))
                    })?;
                    file.seek(SeekFrom::End(0)).map_err(|error| {
                        StorageFailure::indeterminate(format!(
                            "Binlog compaction checkpoint seek failed: {}",
                            error
                        ))
                    })
                })();
                let _ = completion.send(result);
            }
            LogMessage::CompactSuffix {
                retained_from,
                prepared_through,
                completion,
            } => {
                let result = binlog
                    .lock()
                    .map_err(|_| StorageFailure::indeterminate("Binlog file lock is poisoned"))
                    .and_then(|mut file| {
                        file.compact_suffix(retained_from, prepared_through)
                            .map_err(|error| match error {
                                BinlogCompactionError::Unchanged(error) => StorageFailure::rejected(
                                    format!(
                                        "Binlog suffix replacement did not modify history: {error}"
                                    ),
                                ),
                                BinlogCompactionError::Indeterminate(error) => {
                                    StorageFailure::indeterminate(format!(
                                        "Binlog suffix replacement outcome is indeterminate: {error}"
                                    ))
                                }
                            })
                    });
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
        compact_error_on_call: Option<(usize, InjectedTruncateFailure)>,
    }

    struct FaultInjectingFile {
        file: File,
        plan: FaultPlan,
        write_calls: usize,
        flush_calls: usize,
        sync_data_calls: usize,
        sync_all_calls: usize,
        truncate_calls: usize,
        compact_calls: usize,
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
                compact_calls: 0,
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

        fn compact_suffix(
            &mut self,
            retained_from: u64,
            _prepared_through: u64,
        ) -> Result<u64, BinlogCompactionError> {
            self.compact_calls += 1;
            if let Some((call, disposition)) = self.plan.compact_error_on_call
                && call == self.compact_calls
            {
                let error = Self::injected_error("compact");
                return Err(match disposition {
                    InjectedTruncateFailure::Unchanged => BinlogCompactionError::Unchanged(error),
                    InjectedTruncateFailure::Indeterminate => {
                        BinlogCompactionError::Indeterminate(error)
                    }
                    InjectedTruncateFailure::PartiallyApplied => {
                        let current_length = self.file.metadata().unwrap().len();
                        self.file.set_len(current_length / 2).unwrap();
                        BinlogCompactionError::Indeterminate(error)
                    }
                });
            }

            let active_length = self.file.metadata().unwrap().len();
            let retained_bytes = active_length.checked_sub(retained_from).ok_or_else(|| {
                BinlogCompactionError::Unchanged(Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "checkpoint exceeds test binlog length",
                ))
            })?;
            self.file.seek(SeekFrom::Start(retained_from)).unwrap();
            let mut suffix = Vec::new();
            std::io::Read::read_to_end(&mut self.file, &mut suffix).unwrap();
            self.file.set_len(0).unwrap();
            self.file.seek(SeekFrom::Start(0)).unwrap();
            self.file.write_all(&suffix).unwrap();
            self.flush().map_err(BinlogCompactionError::Indeterminate)?;
            self.sync_all()
                .map_err(BinlogCompactionError::Indeterminate)?;
            Ok(retained_bytes)
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

    #[test]
    fn automatic_compaction_rearms_when_live_writes_cross_the_next_threshold() {
        let directory = TestDirectory::new();
        let (sender, _receiver) = mpsc::channel(1);
        let runtime = CommitRuntime::new(BinlogHandle::new(sender), 0, directory.paths());
        runtime.compaction_pending.store(true, Ordering::SeqCst);
        runtime.write_count.store(11, Ordering::SeqCst);

        assert!(runtime.finish_compaction_schedule_and_rearm(10, true));
        assert!(runtime.compaction_pending.load(Ordering::SeqCst));

        runtime.write_count.store(9, Ordering::SeqCst);
        assert!(!runtime.finish_compaction_schedule_and_rearm(10, true));
        assert!(!runtime.compaction_pending.load(Ordering::SeqCst));

        runtime.compaction_pending.store(true, Ordering::SeqCst);
        runtime.write_count.store(10, Ordering::SeqCst);
        assert!(!runtime.finish_compaction_schedule_and_rearm(10, false));
        assert!(!runtime.compaction_pending.load(Ordering::SeqCst));
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
    async fn partial_suffix_replacement_fail_stops_and_recovers_installed_snapshot() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let io = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            FaultPlan {
                compact_error_on_call: Some((1, InjectedTruncateFailure::PartiallyApplied)),
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
        let runtime = Arc::new(CommitRuntime::new(handle, 7, paths.clone()));
        let upstream_replid = Arc::new(AtomicU64::new(0));
        let store = Arc::new(ShardedStore::new());
        store.set("key".to_string(), "snapshot".to_string());

        let error = runtime.compact(&store, &upstream_replid).await.unwrap_err();

        assert!(error.is_indeterminate());
        assert!(runtime.is_fail_stopped());
        let metrics = runtime.compaction_metrics();
        assert_eq!(metrics.attempts_total, 1);
        assert_eq!(metrics.completed_total, 0);
        assert_eq!(metrics.failed_total, 1);
        assert_eq!(metrics.in_progress, 0);
        assert!(metrics.snapshot_write_nanoseconds_total > 0);
        assert!(metrics.rotation_nanoseconds_total > 0);
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

        let metrics = handle.metrics.snapshot();
        assert_eq!(metrics.append_attempts_total, 1);
        assert_eq!(metrics.append_accepted_total, 1);
        assert_eq!(metrics.append_rejected_total, 0);
        assert_eq!(metrics.append_indeterminate_total, 0);
        assert_eq!(metrics.records_accepted_total, 2);
        assert_eq!(metrics.records_per_append_max, 2);
        assert_eq!(
            metrics.bytes_accepted_total,
            std::fs::metadata(&paths.binlog).unwrap().len()
        );
        assert_eq!(metrics.bytes_per_append_max, metrics.bytes_accepted_total);

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
        let metrics = handle.metrics.snapshot();
        assert_eq!(metrics.append_attempts_total, 1);
        assert_eq!(metrics.append_accepted_total, 0);
        assert_eq!(metrics.append_rejected_total, 1);
        assert_eq!(metrics.append_indeterminate_total, 0);
        assert_eq!(metrics.records_accepted_total, 0);
        assert_eq!(metrics.bytes_accepted_total, 0);
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
    async fn successful_compaction_records_phase_metrics() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let io = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            FaultPlan::default(),
        )));
        let (sender, receiver) = mpsc::channel(4);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::Always,
        ));
        let runtime = Arc::new(CommitRuntime::new(
            BinlogHandle::new(sender),
            7,
            paths.clone(),
        ));
        let upstream_replid = Arc::new(AtomicU64::new(0));
        let store = Arc::new(ShardedStore::new());
        store.set("key".to_string(), "snapshot".to_string());

        assert_eq!(runtime.compact(&store, &upstream_replid).await.unwrap(), 7);
        let metrics = runtime.compaction_metrics();
        assert_eq!(metrics.attempts_total, 1);
        assert_eq!(metrics.completed_total, 1);
        assert_eq!(metrics.failed_total, 0);
        assert_eq!(metrics.in_progress, 0);
        assert!(metrics.duration_nanoseconds_total > 0);
        assert_eq!(
            metrics.duration_nanoseconds_last,
            metrics.duration_nanoseconds_max
        );
        assert!(metrics.checkpoint_nanoseconds_total > 0);
        assert!(metrics.snapshot_capture_nanoseconds_total > 0);
        assert!(metrics.snapshot_write_nanoseconds_total > 0);
        assert!(metrics.suffix_prepare_nanoseconds_total > 0);
        assert!(metrics.rotation_nanoseconds_total > 0);
        assert!(metrics.write_pause_nanoseconds_total > 0);
        assert_eq!(metrics.retained_bytes_total, 0);
        {
            let io = io.lock().unwrap();
            assert_eq!(io.flush_calls, 3);
            assert_eq!(io.sync_data_calls, 0);
            assert_eq!(io.sync_all_calls, 1);
            assert_eq!(io.truncate_calls, 0);
            assert_eq!(io.compact_calls, 1);
        }

        drop(runtime);
        worker.await.unwrap();
        drop(io);
        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(recovery.snapshot_watermark, 7);
        assert_eq!(recovered.get("key"), Ok(Some("snapshot".to_string())));
    }

    #[tokio::test]
    async fn managed_suffix_replacement_retains_only_post_checkpoint_history() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&paths.binlog)
            .unwrap();
        let io = Arc::new(std::sync::Mutex::new(ManagedBinlogFile::new(
            file,
            paths.clone(),
        )));
        let (sender, receiver) = mpsc::channel(8);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::Always,
        ));
        let handle = BinlogHandle::new(sender);
        handle
            .append_batch(1, &put_batch_for(b"key", b"first"))
            .await
            .unwrap();
        let checkpoint = handle.checkpoint().await.unwrap();
        handle
            .append_batch(2, &put_batch_for(b"key", b"second"))
            .await
            .unwrap();
        let prepared_through = handle.checkpoint().await.unwrap();
        prepare_binlog_suffix(&paths, checkpoint, prepared_through).unwrap();

        let retained_bytes = handle
            .compact_suffix(checkpoint, prepared_through)
            .await
            .unwrap();

        assert!(retained_bytes > 0);
        let inspection = crate::persistence::inspect_binlog(&paths.binlog).unwrap();
        assert_eq!(inspection.min_sequence, Some(2));
        assert_eq!(inspection.max_sequence, 2);
        assert!(!paths.binlog_temp.exists());
        assert!(!paths.binlog_backup.exists());

        let snapshot = ShardedStore::new();
        snapshot.set("key".to_string(), "first".to_string());
        write_snapshot_file(snapshot.raw_entries(), 1, &paths).unwrap();
        drop(handle);
        worker.await.unwrap();
        drop(io);

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(state.last_sequence, 2);
        assert_eq!(state.snapshot_watermark, 1);
        assert_eq!(recovered.get("key"), Ok(Some("second".to_string())));
    }

    #[tokio::test]
    async fn mismatched_prepared_suffix_does_not_replace_full_history() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&paths.binlog)
            .unwrap();
        let io = Arc::new(std::sync::Mutex::new(ManagedBinlogFile::new(
            file,
            paths.clone(),
        )));
        let (sender, receiver) = mpsc::channel(8);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::Always,
        ));
        let handle = BinlogHandle::new(sender);
        handle
            .append_batch(1, &put_batch_for(b"key", b"first"))
            .await
            .unwrap();
        let checkpoint = handle.checkpoint().await.unwrap();
        handle
            .append_batch(2, &put_batch_for(b"key", b"second"))
            .await
            .unwrap();
        let prepared_through = handle.checkpoint().await.unwrap();
        prepare_binlog_suffix(&paths, checkpoint, prepared_through).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&paths.binlog_temp)
            .unwrap()
            .set_len(0)
            .unwrap();

        let error = handle
            .compact_suffix(checkpoint, prepared_through)
            .await
            .unwrap_err();

        assert!(!error.is_indeterminate());
        let inspection = crate::persistence::inspect_binlog(&paths.binlog).unwrap();
        assert_eq!(inspection.min_sequence, Some(1));
        assert_eq!(inspection.max_sequence, 2);
        assert!(!paths.binlog_backup.exists());
        drop(handle);
        worker.await.unwrap();
        drop(io);

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(state.last_sequence, 2);
        assert_eq!(recovered.get("key"), Ok(Some("second".to_string())));
    }

    #[tokio::test]
    async fn snapshot_and_suffix_preparation_release_commits_and_retain_live_writes() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&paths.binlog)
            .unwrap();
        let io = Arc::new(std::sync::Mutex::new(ManagedBinlogFile::new(
            file,
            paths.clone(),
        )));
        let (sender, receiver) = mpsc::channel(8);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::Always,
        ));
        let handle = BinlogHandle::new(sender);
        handle
            .append_batch(1, &put_batch_for(b"key", b"first"))
            .await
            .unwrap();
        let runtime = Arc::new(CommitRuntime::new(handle, 1, paths.clone()));
        runtime.write_count.store(7, Ordering::SeqCst);
        let store = Arc::new(ShardedStore::new());
        store.set("key".to_string(), "first".to_string());
        let upstream_replid = Arc::new(AtomicU64::new(0));
        let (snapshot_started_tx, snapshot_started_rx) = oneshot::channel();
        let (release_snapshot_tx, release_snapshot_rx) = std::sync::mpsc::channel();
        let (suffix_prepare_started_tx, suffix_prepare_started_rx) = oneshot::channel();
        let (release_suffix_prepare_tx, release_suffix_prepare_rx) = std::sync::mpsc::channel();
        let compact_runtime = Arc::clone(&runtime);
        let compact_store = Arc::clone(&store);
        let compact_replid = Arc::clone(&upstream_replid);
        let compact = tokio::spawn(async move {
            compact_runtime
                .compact_with_operations(
                    compact_store,
                    compact_replid,
                    Box::new(move |entries, watermark, paths| {
                        let _ = snapshot_started_tx.send(());
                        release_snapshot_rx.recv().unwrap();
                        write_snapshot_file(entries, watermark, &paths)
                    }),
                    Box::new(move |paths, retained_from, prepared_through| {
                        let _ = suffix_prepare_started_tx.send(());
                        release_suffix_prepare_rx.recv().unwrap();
                        prepare_binlog_suffix(&paths, retained_from, prepared_through)
                    }),
                )
                .await
        });
        snapshot_started_rx.await.unwrap();

        assert!(
            tokio::time::timeout(Duration::from_millis(50), runtime.compaction_gate.lock())
                .await
                .is_err(),
            "concurrent baseline replacement was not serialized while snapshot writing ran"
        );

        let boundary =
            tokio::time::timeout(Duration::from_secs(1), runtime.acquire_commit_boundary())
                .await
                .expect("snapshot serialization retained the authoritative commit boundary");
        store.set("key".to_string(), "second".to_string());
        runtime
            .accept_next_batch(2, &put_batch_for(b"key", b"second"), 1_000)
            .await
            .unwrap();
        drop(boundary);
        release_snapshot_tx.send(()).unwrap();

        suffix_prepare_started_rx.await.unwrap();
        let boundary =
            tokio::time::timeout(Duration::from_secs(1), runtime.acquire_commit_boundary())
                .await
                .expect("suffix preparation retained the authoritative commit boundary");
        store.set("key".to_string(), "third".to_string());
        runtime
            .accept_next_batch(3, &put_batch_for(b"key", b"third"), 1_000)
            .await
            .unwrap();
        drop(boundary);
        release_suffix_prepare_tx.send(()).unwrap();

        assert_eq!(compact.await.unwrap().unwrap(), 1);
        assert_eq!(runtime.sequence(), 3);
        assert_eq!(runtime.write_count.load(Ordering::SeqCst), 2);
        let metrics = runtime.compaction_metrics();
        assert_eq!(metrics.completed_total, 1);
        assert!(metrics.retained_bytes_total > 0);
        assert!(metrics.suffix_prepare_nanoseconds_total > 0);
        assert!(metrics.write_pause_nanoseconds_total < metrics.duration_nanoseconds_total);

        drop(runtime);
        worker.await.unwrap();
        drop(io);
        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(state.snapshot_watermark, 1);
        assert_eq!(state.last_sequence, 3);
        assert_eq!(recovered.get("key"), Ok(Some("third".to_string())));
    }

    #[tokio::test]
    async fn cancelling_compaction_waiter_does_not_cancel_owned_finalization() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&paths.binlog)
            .unwrap();
        let io = Arc::new(std::sync::Mutex::new(ManagedBinlogFile::new(
            file,
            paths.clone(),
        )));
        let (sender, receiver) = mpsc::channel(8);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::Always,
        ));
        let handle = BinlogHandle::new(sender);
        handle.append_batch(1, &put_batch()).await.unwrap();
        let runtime = Arc::new(CommitRuntime::new(handle, 1, paths.clone()));
        let store = Arc::new(ShardedStore::new());
        store.set("key".to_string(), "accepted".to_string());
        let upstream_replid = Arc::new(AtomicU64::new(0));
        let (snapshot_started_tx, snapshot_started_rx) = oneshot::channel();
        let (release_snapshot_tx, release_snapshot_rx) = std::sync::mpsc::channel();
        let compact_runtime = Arc::clone(&runtime);
        let compact_store = Arc::clone(&store);
        let compact_replid = Arc::clone(&upstream_replid);
        let waiter = tokio::spawn(async move {
            compact_runtime
                .compact_with_writer(
                    compact_store,
                    compact_replid,
                    Box::new(move |entries, watermark, paths| {
                        let _ = snapshot_started_tx.send(());
                        release_snapshot_rx.recv().unwrap();
                        write_snapshot_file(entries, watermark, &paths)
                    }),
                )
                .await
        });
        snapshot_started_rx.await.unwrap();
        waiter.abort();
        let _ = waiter.await;
        release_snapshot_tx.send(()).unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if runtime.compaction_metrics().completed_total == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned compaction finalization did not complete after waiter cancellation");
        assert!(paths.snapshot.exists());
        assert!(!runtime.is_fail_stopped());

        drop(runtime);
        worker.await.unwrap();
        drop(io);
        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(state.snapshot_watermark, 1);
        assert_eq!(state.last_sequence, 1);
        assert_eq!(recovered.get("key"), Ok(Some("accepted".to_string())));
    }

    #[tokio::test]
    async fn snapshot_capture_waits_for_the_full_visibility_boundary() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let io = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            FaultPlan::default(),
        )));
        let (sender, receiver) = mpsc::channel(4);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::Always,
        ));
        let handle = BinlogHandle::new(sender);
        handle.append_batch(1, &put_batch()).await.unwrap();
        let runtime = Arc::new(CommitRuntime::new(handle, 1, paths.clone()));
        let store = Arc::new(ShardedStore::new());
        store.set("key".to_string(), "accepted".to_string());
        let upstream_replid = Arc::new(AtomicU64::new(0));
        let visibility_guard = Arc::clone(&runtime.visibility_gate).write_owned().await;
        let (snapshot_started_tx, mut snapshot_started_rx) = oneshot::channel();
        let compact_runtime = Arc::clone(&runtime);
        let compact_store = Arc::clone(&store);
        let compact_replid = Arc::clone(&upstream_replid);
        let compact = tokio::spawn(async move {
            compact_runtime
                .compact_with_writer(
                    compact_store,
                    compact_replid,
                    Box::new(move |entries, watermark, paths| {
                        let _ = snapshot_started_tx.send(());
                        write_snapshot_file(entries, watermark, &paths)
                    }),
                )
                .await
        });

        loop {
            if runtime.write_gate.try_lock().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            snapshot_started_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        drop(visibility_guard);
        snapshot_started_rx.await.unwrap();
        assert_eq!(compact.await.unwrap().unwrap(), 1);
        assert!(!runtime.is_fail_stopped());

        drop(runtime);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn failed_suffix_preparation_keeps_the_snapshot_and_full_binlog_recoverable() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let io = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            FaultPlan::default(),
        )));
        let (sender, receiver) = mpsc::channel(4);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::Always,
        ));
        let handle = BinlogHandle::new(sender);
        handle.append_batch(1, &put_batch()).await.unwrap();
        let runtime = Arc::new(CommitRuntime::new(handle, 1, paths.clone()));
        let store = Arc::new(ShardedStore::new());
        store.set("key".to_string(), "accepted".to_string());
        let upstream_replid = Arc::new(AtomicU64::new(0));

        let error = runtime
            .compact_with_operations(
                Arc::clone(&store),
                upstream_replid,
                Box::new(|entries, watermark, paths| {
                    write_snapshot_file(entries, watermark, &paths)
                }),
                Box::new(|_, _, _| {
                    Err(PersistenceError::new(
                        "Injected binlog suffix preparation failure",
                    ))
                }),
            )
            .await
            .unwrap_err();

        assert!(!error.is_indeterminate());
        assert!(paths.snapshot.exists());
        assert!(!runtime.is_fail_stopped());
        assert_eq!(io.lock().unwrap().compact_calls, 0);
        drop(runtime);
        worker.await.unwrap();
        drop(io);

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(state.snapshot_watermark, 1);
        assert_eq!(state.last_sequence, 1);
        assert_eq!(recovered.get("key"), Ok(Some("accepted".to_string())));
    }

    #[tokio::test]
    async fn failed_compaction_checkpoint_fail_stops_without_installing_a_snapshot() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let io = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            FaultPlan {
                flush_error_on_call: Some(1),
                ..FaultPlan::default()
            },
        )));
        let (sender, receiver) = mpsc::channel(4);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::No,
        ));
        let runtime = Arc::new(CommitRuntime::new(
            BinlogHandle::new(sender),
            7,
            paths.clone(),
        ));
        let upstream_replid = Arc::new(AtomicU64::new(0));
        let store = Arc::new(ShardedStore::new());
        store.set("key".to_string(), "snapshot".to_string());

        let error = runtime.compact(&store, &upstream_replid).await.unwrap_err();

        assert!(error.is_indeterminate());
        assert!(runtime.is_fail_stopped());
        assert!(!paths.snapshot.exists());
        let metrics = runtime.compaction_metrics();
        assert_eq!(metrics.attempts_total, 1);
        assert_eq!(metrics.completed_total, 0);
        assert_eq!(metrics.failed_total, 1);
        assert_eq!(metrics.in_progress, 0);
        assert!(metrics.checkpoint_nanoseconds_total > 0);
        assert_eq!(metrics.snapshot_write_nanoseconds_total, 0);
        assert_eq!(io.lock().unwrap().truncate_calls, 0);

        drop(runtime);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn ordered_checkpoint_does_not_weaken_explicit_durable_flush() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let io = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            FaultPlan::default(),
        )));
        let (sender, receiver) = mpsc::channel(4);
        let worker = tokio::spawn(run_binlog_worker(
            receiver,
            Arc::clone(&io),
            FsyncPolicy::No,
        ));
        let handle = BinlogHandle::new(sender);

        assert_eq!(handle.checkpoint().await.unwrap(), 0);
        handle.flush().await.unwrap();

        {
            let io = io.lock().unwrap();
            assert_eq!(io.flush_calls, 2);
            assert_eq!(io.sync_all_calls, 1);
        }
        drop(handle);
        worker.await.unwrap();
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
