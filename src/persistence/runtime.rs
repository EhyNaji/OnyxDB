use super::{
    CommittedBatch, PersistenceError, ReplicaIdentity, durable_rename, encode_committed_batch,
    encode_versioned_binlog_record, framed_versioned_binlog_record_length, sync_parent_directory,
    write_replica_identity, write_snapshot_file,
};
use crate::config::FsyncPolicy;
use onyxdb::store::ShardedStore;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc, oneshot};
use tracing::{error, info, warn};

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
    pub(crate) generation_preflush_nanoseconds_total: u64,
    pub(crate) generation_preflush_nanoseconds_max: u64,
    pub(crate) write_pause_nanoseconds_total: u64,
    pub(crate) write_pause_nanoseconds_max: u64,
    pub(crate) checkpoint_nanoseconds_total: u64,
    pub(crate) checkpoint_nanoseconds_max: u64,
    pub(crate) snapshot_capture_nanoseconds_total: u64,
    pub(crate) snapshot_capture_nanoseconds_max: u64,
    pub(crate) snapshot_write_nanoseconds_total: u64,
    pub(crate) snapshot_write_nanoseconds_max: u64,
    pub(crate) rotation_nanoseconds_total: u64,
    pub(crate) rotation_nanoseconds_max: u64,
    pub(crate) segment_cleanup_nanoseconds_total: u64,
    pub(crate) segment_cleanup_nanoseconds_max: u64,
    pub(crate) sealed_bytes_total: u64,
    pub(crate) sealed_bytes_max: u64,
    pub(crate) preflushed_bytes_total: u64,
    pub(crate) preflushed_bytes_max: u64,
    pub(crate) retained_bytes_total: u64,
    pub(crate) retained_bytes_max: u64,
    pub(crate) cleanup_files_total: u64,
    pub(crate) cleanup_bytes_total: u64,
    pub(crate) cleanup_failures_total: u64,
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
    generation_preflush: DurationMetric,
    write_pause: DurationMetric,
    checkpoint: DurationMetric,
    snapshot_capture: DurationMetric,
    snapshot_write: DurationMetric,
    rotation: DurationMetric,
    segment_cleanup: DurationMetric,
    sealed_bytes_total: AtomicU64,
    sealed_bytes_max: AtomicU64,
    preflushed_bytes_total: AtomicU64,
    preflushed_bytes_max: AtomicU64,
    retained_bytes_total: AtomicU64,
    retained_bytes_max: AtomicU64,
    cleanup_files_total: AtomicU64,
    cleanup_bytes_total: AtomicU64,
    cleanup_failures_total: AtomicU64,
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
            generation_preflush_nanoseconds_total: self
                .generation_preflush
                .total
                .load(Ordering::Relaxed),
            generation_preflush_nanoseconds_max: self
                .generation_preflush
                .max
                .load(Ordering::Relaxed),
            write_pause_nanoseconds_total: self.write_pause.total.load(Ordering::Relaxed),
            write_pause_nanoseconds_max: self.write_pause.max.load(Ordering::Relaxed),
            checkpoint_nanoseconds_total: self.checkpoint.total.load(Ordering::Relaxed),
            checkpoint_nanoseconds_max: self.checkpoint.max.load(Ordering::Relaxed),
            snapshot_capture_nanoseconds_total: self.snapshot_capture.total.load(Ordering::Relaxed),
            snapshot_capture_nanoseconds_max: self.snapshot_capture.max.load(Ordering::Relaxed),
            snapshot_write_nanoseconds_total: self.snapshot_write.total.load(Ordering::Relaxed),
            snapshot_write_nanoseconds_max: self.snapshot_write.max.load(Ordering::Relaxed),
            rotation_nanoseconds_total: self.rotation.total.load(Ordering::Relaxed),
            rotation_nanoseconds_max: self.rotation.max.load(Ordering::Relaxed),
            segment_cleanup_nanoseconds_total: self.segment_cleanup.total.load(Ordering::Relaxed),
            segment_cleanup_nanoseconds_max: self.segment_cleanup.max.load(Ordering::Relaxed),
            sealed_bytes_total: self.sealed_bytes_total.load(Ordering::Relaxed),
            sealed_bytes_max: self.sealed_bytes_max.load(Ordering::Relaxed),
            preflushed_bytes_total: self.preflushed_bytes_total.load(Ordering::Relaxed),
            preflushed_bytes_max: self.preflushed_bytes_max.load(Ordering::Relaxed),
            retained_bytes_total: self.retained_bytes_total.load(Ordering::Relaxed),
            retained_bytes_max: self.retained_bytes_max.load(Ordering::Relaxed),
            cleanup_files_total: self.cleanup_files_total.load(Ordering::Relaxed),
            cleanup_bytes_total: self.cleanup_bytes_total.load(Ordering::Relaxed),
            cleanup_failures_total: self.cleanup_failures_total.load(Ordering::Relaxed),
        }
    }

    fn observe_sealed_bytes(&self, sealed_bytes: u64) {
        self.sealed_bytes_total
            .fetch_add(sealed_bytes, Ordering::Relaxed);
        observe_max(&self.sealed_bytes_max, sealed_bytes);
    }

    fn observe_preflush(&self, preflushed_bytes: u64, elapsed: Duration) {
        self.generation_preflush.observe(elapsed);
        self.preflushed_bytes_total
            .fetch_add(preflushed_bytes, Ordering::Relaxed);
        observe_max(&self.preflushed_bytes_max, preflushed_bytes);
    }

    fn observe_retained_bytes(&self, retained_bytes: u64) {
        self.retained_bytes_total
            .fetch_add(retained_bytes, Ordering::Relaxed);
        observe_max(&self.retained_bytes_max, retained_bytes);
    }

    fn observe_cleanup(&self, cleanup: BinlogSegmentCleanup, elapsed: Duration) {
        self.segment_cleanup.observe(elapsed);
        self.cleanup_files_total
            .fetch_add(cleanup.removed_files, Ordering::Relaxed);
        self.cleanup_bytes_total
            .fetch_add(cleanup.removed_bytes, Ordering::Relaxed);
        self.cleanup_failures_total
            .fetch_add(cleanup.failed_files, Ordering::Relaxed);
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

#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum TruncateError {
    Unchanged(std::io::Error),
    Indeterminate(std::io::Error),
}

#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum BinlogRotationError {
    Unchanged(std::io::Error),
    Indeterminate(std::io::Error),
}

pub(crate) trait BinlogIo: Write + Seek + Send + 'static {
    fn sync_data(&mut self) -> std::io::Result<()>;
    fn sync_all(&mut self) -> std::io::Result<()>;
    fn truncate(&mut self, length: u64) -> Result<(), TruncateError>;
    fn seal_active(&mut self, _end_sequence: u64) -> Result<u64, BinlogRotationError> {
        Err(BinlogRotationError::Unchanged(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Binlog generation sealing is unsupported by this storage backend",
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

/// Owns the active binlog handle and its immutable sealed generations.
///
/// Generation sealing is only requested while the authoritative commit
/// boundary is held. A non-empty active file is durably renamed to a path that
/// declares its final committed sequence before a new active file is created.
/// Recovery can therefore order and validate every crash state without a
/// separate mutable manifest.
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

    fn restore_segment_after_failed_seal(
        &mut self,
        segment: &Path,
        original_error: std::io::Error,
    ) -> Result<u64, BinlogRotationError> {
        if self.paths.binlog.exists() {
            return Err(BinlogRotationError::Indeterminate(std::io::Error::other(
                format!(
                    "Binlog generation sealing failed ({original_error}) after a new active path became visible"
                ),
            )));
        }
        let restoration = (|| -> std::io::Result<()> {
            durable_rename(segment, &self.paths.binlog)?;
            sync_parent_directory(&self.paths.binlog)?;
            self.reopen_active()
        })();
        match restoration {
            Ok(()) => Err(BinlogRotationError::Unchanged(original_error)),
            Err(restoration_error) => Err(BinlogRotationError::Indeterminate(
                std::io::Error::other(format!(
                    "Binlog generation sealing failed ({original_error}) and the original active file could not be restored ({restoration_error})"
                )),
            )),
        }
    }
}

fn list_binlog_segments(paths: &super::PersistencePaths) -> std::io::Result<Vec<(u64, PathBuf)>> {
    let directory = paths
        .binlog
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut segments = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let end_sequence =
            super::parse_binlog_segment_end_sequence(&entry.file_name()).map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
            })?;
        if let Some(end_sequence) = end_sequence {
            segments.push((end_sequence, entry.path()));
        }
    }
    segments.sort_by_key(|(end_sequence, _)| *end_sequence);
    Ok(segments)
}

fn remove_all_binlog_segments(paths: &super::PersistencePaths) -> std::io::Result<()> {
    let mut removed = false;
    for (_, path) in list_binlog_segments(paths)? {
        match fs::remove_file(path) {
            Ok(()) => removed = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    if removed {
        sync_parent_directory(&paths.binlog)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BinlogSegmentCleanup {
    removed_files: u64,
    removed_bytes: u64,
    failed_files: u64,
}

fn cleanup_binlog_segments_through(
    paths: &super::PersistencePaths,
    watermark: u64,
) -> BinlogSegmentCleanup {
    let mut cleanup = BinlogSegmentCleanup::default();
    let segments = match list_binlog_segments(paths) {
        Ok(segments) => segments,
        Err(error) => {
            warn!("Unable to enumerate snapshot-covered binlog segments: {error}");
            cleanup.failed_files = 1;
            return cleanup;
        }
    };
    for (end_sequence, path) in segments {
        if end_sequence > watermark {
            continue;
        }
        let length = fs::metadata(&path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        match fs::remove_file(&path) {
            Ok(()) => {
                cleanup.removed_files += 1;
                cleanup.removed_bytes = cleanup.removed_bytes.saturating_add(length);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                cleanup.failed_files += 1;
                warn!(
                    "Unable to remove snapshot-covered binlog segment {}: {}",
                    path.display(),
                    error
                );
            }
        }
    }
    if cleanup.removed_files > 0
        && let Err(error) = sync_parent_directory(&paths.binlog)
    {
        cleanup.failed_files += 1;
        warn!("Unable to synchronize binlog segment cleanup: {error}");
    }
    cleanup
}

fn preflush_active_generation(paths: &super::PersistencePaths) -> Result<u64, PersistenceError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&paths.binlog)?;
    let length = file.metadata()?.len();
    if length > 0 {
        file.sync_data()?;
    }
    Ok(length)
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
        let file = self
            .file_mut()
            .map_err(TruncateError::Indeterminate)?
            .set_len(length)
            .map_err(TruncateError::Indeterminate);
        file?;
        self.file_mut()
            .map_err(TruncateError::Indeterminate)?
            .seek(SeekFrom::Start(length))
            .map_err(TruncateError::Indeterminate)?;
        if length == 0 {
            remove_all_binlog_segments(&self.paths).map_err(TruncateError::Indeterminate)?;
        }
        Ok(())
    }

    fn seal_active(&mut self, end_sequence: u64) -> Result<u64, BinlogRotationError> {
        let active_length = self
            .file_mut()
            .map_err(BinlogRotationError::Indeterminate)?
            .metadata()
            .map_err(BinlogRotationError::Unchanged)?
            .len();
        if active_length == 0 {
            return Ok(0);
        }
        if end_sequence == 0 {
            return Err(BinlogRotationError::Unchanged(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "A non-empty binlog cannot be sealed at sequence zero",
            )));
        }
        let segment = self.paths.binlog_segment(end_sequence);
        if segment.exists() {
            return Err(BinlogRotationError::Unchanged(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("Binlog segment already exists: {}", segment.display()),
            )));
        }
        self.file_mut()
            .map_err(BinlogRotationError::Indeterminate)?
            .flush()
            .map_err(BinlogRotationError::Indeterminate)?;
        // Once commits can enter a different file, the previous generation
        // must be durable first. Otherwise a system crash could persist the new
        // active generation while losing its required predecessor.
        self.file_mut()
            .map_err(BinlogRotationError::Indeterminate)?
            .sync_all()
            .map_err(BinlogRotationError::Indeterminate)?;
        drop(self.file.take());
        if let Err(error) = durable_rename(&self.paths.binlog, &segment) {
            return match self.reopen_active() {
                Ok(()) => Err(BinlogRotationError::Unchanged(error)),
                Err(reopen_error) => Err(BinlogRotationError::Indeterminate(
                    std::io::Error::other(format!(
                        "Binlog generation rename failed ({error}) and the active file could not be reopened ({reopen_error})"
                    )),
                )),
            };
        }
        if let Err(error) = sync_parent_directory(&segment) {
            return self.restore_segment_after_failed_seal(&segment, error);
        }
        let mut active = match OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&self.paths.binlog)
        {
            Ok(active) => active,
            Err(error) => return self.restore_segment_after_failed_seal(&segment, error),
        };
        if let Err(error) = active.sync_all() {
            self.file = Some(active);
            return Err(BinlogRotationError::Indeterminate(error));
        }
        if let Err(error) = sync_parent_directory(&self.paths.binlog) {
            self.file = Some(active);
            return Err(BinlogRotationError::Indeterminate(error));
        }
        active
            .seek(SeekFrom::End(0))
            .map_err(BinlogRotationError::Indeterminate)?;
        self.file = Some(active);
        Ok(active_length)
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
        let (completion_tx, completion_rx) = oneshot::channel();
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            let worker_runtime = Arc::clone(&runtime);
            let worker = tokio::spawn(async move {
                worker_runtime
                    .compact_owned(store, upstream_replid, snapshot_writer)
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
    ) -> Result<u64, PersistenceError> {
        let measurement = CompactionMeasurement::start(&self.compaction_metrics);
        let serialization_started = Instant::now();
        let _compaction_guard = self.compaction_gate.lock().await;
        self.compaction_metrics
            .serialization_wait
            .observe(serialization_started.elapsed());

        let preflush_started = Instant::now();
        let preflush_paths = self.paths.clone();
        let preflushed_bytes =
            match tokio::task::spawn_blocking(move || preflush_active_generation(&preflush_paths))
                .await
            {
                Ok(Ok(preflushed_bytes)) => preflushed_bytes,
                Ok(Err(error)) => {
                    self.compaction_metrics
                        .observe_preflush(0, preflush_started.elapsed());
                    measurement.finish(false);
                    return Err(error_with_context(
                        error,
                        "Active binlog generation preflush failed",
                    ));
                }
                Err(error) => {
                    self.compaction_metrics
                        .observe_preflush(0, preflush_started.elapsed());
                    measurement.finish(false);
                    return Err(PersistenceError::new(format!(
                        "Active binlog generation preflush task failed: {error}"
                    )));
                }
            };
        self.compaction_metrics
            .observe_preflush(preflushed_bytes, preflush_started.elapsed());

        let gate_started = Instant::now();
        let boundary = self.acquire_commit_boundary().await;
        self.compaction_metrics
            .gate_wait
            .observe(gate_started.elapsed());
        let capture_pause_started = Instant::now();
        let watermark = self.sequence();
        let rotation_started = Instant::now();
        let sealed_bytes = match self.binlog.seal_active(watermark).await {
            Ok(sealed_bytes) => sealed_bytes,
            Err(error) => {
                self.compaction_metrics
                    .rotation
                    .observe(rotation_started.elapsed());
                self.compaction_metrics
                    .write_pause
                    .observe(capture_pause_started.elapsed());
                let error = error_with_context(error, "Binlog generation sealing failed");
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
            .rotation
            .observe(rotation_started.elapsed());
        self.compaction_metrics.observe_sealed_bytes(sealed_bytes);

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

        let final_gate_started = Instant::now();
        let boundary = self.acquire_commit_boundary().await;
        self.compaction_metrics
            .gate_wait
            .observe(final_gate_started.elapsed());
        let final_pause_started = Instant::now();
        let checkpoint_started = Instant::now();
        let retained_bytes = match self.binlog.checkpoint().await {
            Ok(retained_bytes) => retained_bytes,
            Err(error) => {
                self.compaction_metrics
                    .checkpoint
                    .observe(checkpoint_started.elapsed());
                self.compaction_metrics
                    .write_pause
                    .observe(final_pause_started.elapsed());
                let error = error_with_context(error, "Post-snapshot binlog checkpoint failed");
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
            .checkpoint
            .observe(checkpoint_started.elapsed());
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
                .write_pause
                .observe(final_pause_started.elapsed());
            drop(boundary);
            measurement.finish(false);
            return Err(error);
        }
        self.compaction_metrics
            .observe_retained_bytes(retained_bytes);
        self.write_count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                Some(count.saturating_sub(compacted_write_count))
            })
            .expect("compaction write-count update cannot fail");
        self.compaction_metrics
            .write_pause
            .observe(final_pause_started.elapsed());
        drop(boundary);

        let cleanup_started = Instant::now();
        let cleanup_paths = self.paths.clone();
        let cleanup = tokio::task::spawn_blocking(move || {
            cleanup_binlog_segments_through(&cleanup_paths, watermark)
        })
        .await
        .unwrap_or_else(|error| {
            warn!("Binlog segment cleanup task failed: {error}");
            BinlogSegmentCleanup {
                failed_files: 1,
                ..BinlogSegmentCleanup::default()
            }
        });
        self.compaction_metrics
            .observe_cleanup(cleanup, cleanup_started.elapsed());
        info!(
            "Compaction complete at sequence {}: {} bytes sealed, {} post-boundary bytes retained, {} covered segment(s) removed",
            watermark, sealed_bytes, retained_bytes, cleanup.removed_files
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
    SealActive {
        end_sequence: u64,
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

    /// Seals the active generation at the supplied authoritative sequence and
    /// creates a new empty active binlog. The returned value is the number of
    /// bytes moved into the immutable segment.
    pub(crate) async fn seal_active(&self, end_sequence: u64) -> Result<u64, PersistenceError> {
        let (completion_tx, completion_rx) = oneshot::channel();
        self.sender
            .send(LogMessage::SealActive {
                end_sequence,
                completion: completion_tx,
            })
            .await
            .map_err(|_| {
                PersistenceError::indeterminate(
                    "Binlog worker is unavailable during generation sealing",
                )
            })?;
        completion_rx
            .await
            .map_err(|_| {
                PersistenceError::indeterminate("Binlog generation sealing completion was dropped")
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
            LogMessage::SealActive {
                end_sequence,
                completion,
            } => {
                let result = binlog
                    .lock()
                    .map_err(|_| StorageFailure::indeterminate("Binlog file lock is poisoned"))
                    .and_then(|mut file| {
                        file.seal_active(end_sequence).map_err(|error| match error {
                            BinlogRotationError::Unchanged(error) => {
                                StorageFailure::rejected(format!(
                                    "Binlog generation sealing did not modify history: {error}"
                                ))
                            }
                            BinlogRotationError::Indeterminate(error) => {
                                StorageFailure::indeterminate(format!(
                                    "Binlog generation sealing outcome is indeterminate: {error}"
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
        seal_error_on_call: Option<(usize, InjectedTruncateFailure)>,
    }

    struct FaultInjectingFile {
        file: File,
        plan: FaultPlan,
        write_calls: usize,
        flush_calls: usize,
        sync_data_calls: usize,
        sync_all_calls: usize,
        truncate_calls: usize,
        seal_calls: usize,
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
                seal_calls: 0,
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

        fn seal_active(&mut self, _end_sequence: u64) -> Result<u64, BinlogRotationError> {
            self.seal_calls += 1;
            if let Some((call, disposition)) = self.plan.seal_error_on_call
                && call == self.seal_calls
            {
                let error = Self::injected_error("seal");
                return Err(match disposition {
                    InjectedTruncateFailure::Unchanged => BinlogRotationError::Unchanged(error),
                    InjectedTruncateFailure::Indeterminate => {
                        BinlogRotationError::Indeterminate(error)
                    }
                    InjectedTruncateFailure::PartiallyApplied => {
                        let current_length = self.file.metadata().unwrap().len();
                        self.file.set_len(current_length / 2).unwrap();
                        BinlogRotationError::Indeterminate(error)
                    }
                });
            }
            let active_length = self.file.metadata().unwrap().len();
            self.flush().map_err(BinlogRotationError::Indeterminate)?;
            self.sync_all()
                .map_err(BinlogRotationError::Indeterminate)?;
            Ok(active_length)
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
    async fn partially_applied_generation_seal_fail_stops_before_snapshot_installation() {
        let directory = TestDirectory::new();
        let paths = directory.paths();
        let io = Arc::new(std::sync::Mutex::new(FaultInjectingFile::open(
            &paths.binlog,
            FaultPlan {
                seal_error_on_call: Some((1, InjectedTruncateFailure::PartiallyApplied)),
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
        let runtime = Arc::new(CommitRuntime::new(handle, 1, paths.clone()));
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
        assert_eq!(metrics.snapshot_write_nanoseconds_total, 0);
        assert!(metrics.rotation_nanoseconds_total > 0);
        assert!(!paths.snapshot.exists());
        drop(runtime);
        worker.await.unwrap();
        drop(io);

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(state.snapshot_watermark, 0);
        assert_eq!(recovered.get("key"), Ok(None));
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
    async fn generation_sealing_synchronizes_the_predecessor_under_no_fsync_policy() {
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
        handle.append_batch(1, &put_batch()).await.unwrap();
        assert_eq!(io.lock().unwrap().sync_all_calls, 0);

        handle.seal_active(1).await.unwrap();

        assert_eq!(io.lock().unwrap().sync_all_calls, 1);
        drop(handle);
        worker.await.unwrap();
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
        assert!(metrics.generation_preflush_nanoseconds_total > 0);
        assert!(metrics.snapshot_capture_nanoseconds_total > 0);
        assert!(metrics.snapshot_write_nanoseconds_total > 0);
        assert!(metrics.rotation_nanoseconds_total > 0);
        assert!(metrics.write_pause_nanoseconds_total > 0);
        assert_eq!(metrics.retained_bytes_total, 0);
        {
            let io = io.lock().unwrap();
            assert_eq!(io.flush_calls, 2);
            assert_eq!(io.sync_data_calls, 0);
            assert_eq!(io.sync_all_calls, 1);
            assert_eq!(io.truncate_calls, 0);
            assert_eq!(io.seal_calls, 1);
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
    async fn managed_generation_seal_moves_history_and_reopens_the_active_binlog() {
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
        let sealed_bytes = handle.seal_active(1).await.unwrap();
        assert!(sealed_bytes > 0);
        let segment = paths.binlog_segment(1);
        let sealed = crate::persistence::inspect_binlog(&segment).unwrap();
        assert_eq!(sealed.min_sequence, Some(1));
        assert_eq!(sealed.max_sequence, 1);
        assert_eq!(std::fs::metadata(&paths.binlog).unwrap().len(), 0);
        handle
            .append_batch(2, &put_batch_for(b"key", b"second"))
            .await
            .unwrap();
        let inspection = crate::persistence::inspect_binlog(&paths.binlog).unwrap();
        assert_eq!(inspection.min_sequence, Some(2));
        assert_eq!(inspection.max_sequence, 2);

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
    async fn an_existing_generation_path_rejects_sealing_without_modifying_history() {
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
        let segment = paths.binlog_segment(1);
        std::fs::write(&segment, b"occupied").unwrap();

        let error = handle.seal_active(1).await.unwrap_err();

        assert!(!error.is_indeterminate());
        let inspection = crate::persistence::inspect_binlog(&paths.binlog).unwrap();
        assert_eq!(inspection.min_sequence, Some(1));
        assert_eq!(inspection.max_sequence, 1);
        assert_eq!(std::fs::read(&segment).unwrap(), b"occupied");
        std::fs::remove_file(segment).unwrap();
        drop(handle);
        worker.await.unwrap();
        drop(io);

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(state.last_sequence, 1);
        assert_eq!(recovered.get("key"), Ok(Some("first".to_string())));
    }

    #[tokio::test]
    async fn snapshot_writing_releases_commits_into_the_new_active_generation() {
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
                        release_snapshot_rx.recv().unwrap();
                        write_snapshot_file(entries, watermark, &paths)
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
        let boundary =
            tokio::time::timeout(Duration::from_secs(1), runtime.acquire_commit_boundary())
                .await
                .expect("snapshot writing retained the authoritative commit boundary");
        store.set("key".to_string(), "third".to_string());
        runtime
            .accept_next_batch(3, &put_batch_for(b"key", b"third"), 1_000)
            .await
            .unwrap();
        drop(boundary);
        release_snapshot_tx.send(()).unwrap();

        assert_eq!(compact.await.unwrap().unwrap(), 1);
        assert_eq!(runtime.sequence(), 3);
        assert_eq!(runtime.write_count.load(Ordering::SeqCst), 2);
        let metrics = runtime.compaction_metrics();
        assert_eq!(metrics.completed_total, 1);
        assert!(metrics.preflushed_bytes_total > 0);
        assert!(metrics.sealed_bytes_total > 0);
        assert!(metrics.retained_bytes_total > 0);
        assert_eq!(metrics.cleanup_files_total, 1);
        assert_eq!(metrics.cleanup_bytes_total, metrics.sealed_bytes_total);
        assert_eq!(metrics.cleanup_failures_total, 0);
        assert!(metrics.segment_cleanup_nanoseconds_total > 0);
        assert!(metrics.write_pause_nanoseconds_total < metrics.duration_nanoseconds_total);
        assert!(!paths.binlog_segment(1).exists());
        let active = crate::persistence::inspect_binlog(&paths.binlog).unwrap();
        assert_eq!(active.min_sequence, Some(2));
        assert_eq!(active.max_sequence, 3);

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
    async fn failed_snapshot_after_sealing_keeps_segmented_history_recoverable() {
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
            .compact_with_writer(
                Arc::clone(&store),
                upstream_replid,
                Box::new(|_, _, _| {
                    Err(PersistenceError::new(
                        "Injected snapshot installation failure",
                    ))
                }),
            )
            .await
            .unwrap_err();

        assert!(!error.is_indeterminate());
        assert!(!paths.snapshot.exists());
        assert!(!runtime.is_fail_stopped());
        assert!(paths.binlog_segment(1).exists());
        assert_eq!(std::fs::metadata(&paths.binlog).unwrap().len(), 0);
        drop(runtime);
        worker.await.unwrap();
        drop(io);

        let recovered = ShardedStore::new();
        let state = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(state.snapshot_watermark, 0);
        assert_eq!(state.last_sequence, 1);
        assert_eq!(recovered.get("key"), Ok(Some("accepted".to_string())));
    }

    #[tokio::test]
    async fn repeated_failed_snapshots_leave_an_ordered_recoverable_segment_history() {
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
        let store = Arc::new(ShardedStore::new());
        store.set("key".to_string(), "first".to_string());
        let upstream_replid = Arc::new(AtomicU64::new(0));

        for expected_segment in 1..=2 {
            let error = runtime
                .compact_with_writer(
                    Arc::clone(&store),
                    Arc::clone(&upstream_replid),
                    Box::new(|_, _, _| {
                        Err(PersistenceError::new(
                            "Injected snapshot installation failure",
                        ))
                    }),
                )
                .await
                .unwrap_err();
            assert!(!error.is_indeterminate());
            assert!(paths.binlog_segment(expected_segment).exists());
            if expected_segment == 1 {
                let boundary = runtime.acquire_commit_boundary().await;
                store.set("key".to_string(), "second".to_string());
                runtime
                    .accept_next_batch(2, &put_batch_for(b"key", b"second"), 1_000)
                    .await
                    .unwrap();
                drop(boundary);
            }
        }

        assert!(!runtime.is_fail_stopped());
        drop(runtime);
        worker.await.unwrap();
        drop(io);

        let recovered = ShardedStore::new();
        let recovery = load_data_from_paths(&recovered, &paths).unwrap();
        assert_eq!(recovery.snapshot_watermark, 0);
        assert_eq!(recovery.last_sequence, 2);
        assert_eq!(recovered.get("key"), Ok(Some("second".to_string())));
    }

    #[tokio::test]
    async fn failed_generation_seal_fail_stops_without_installing_a_snapshot() {
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
        assert_eq!(metrics.checkpoint_nanoseconds_total, 0);
        assert!(metrics.rotation_nanoseconds_total > 0);
        assert_eq!(metrics.snapshot_write_nanoseconds_total, 0);
        assert_eq!(io.lock().unwrap().truncate_calls, 0);

        drop(runtime);
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn failed_generation_preflush_does_not_seal_or_install_a_snapshot() {
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
        let mut unavailable_paths = paths.clone();
        unavailable_paths.binlog = directory.path.join("missing").join("onyx.binlog");
        let runtime = Arc::new(CommitRuntime::new(
            BinlogHandle::new(sender),
            0,
            unavailable_paths,
        ));

        let error = runtime
            .compact(&Arc::new(ShardedStore::new()), &Arc::new(AtomicU64::new(0)))
            .await
            .unwrap_err();

        assert!(!error.is_indeterminate());
        assert!(error.to_string().contains("generation preflush failed"));
        assert!(!runtime.is_fail_stopped());
        assert_eq!(io.lock().unwrap().seal_calls, 0);
        assert!(!paths.snapshot.exists());
        let metrics = runtime.compaction_metrics();
        assert_eq!(metrics.failed_total, 1);
        assert!(metrics.generation_preflush_nanoseconds_total > 0);

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
