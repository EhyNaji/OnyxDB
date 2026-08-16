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
use tokio::sync::{mpsc, oneshot};
use tracing::info;

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

    pub(crate) fn install_baseline(&self, sequence: u64) {
        self.repl_offset.store(sequence, Ordering::SeqCst);
        self.write_count.store(0, Ordering::SeqCst);
    }

    pub(crate) async fn acquire_commit_boundary(&self) -> CommitBoundary {
        CommitBoundary::acquire(&self.write_gate, &self.visibility_gate).await
    }

    pub(crate) async fn compact(
        &self,
        store: &Arc<ShardedStore>,
        upstream_replid: &AtomicU64,
    ) -> Result<u64, PersistenceError> {
        let _write_guard = self.write_gate.lock().await;
        self.binlog
            .flush()
            .await
            .map_err(|error| PersistenceError::new(format!("Binlog flush failed: {}", error)))?;

        let watermark = self.sequence();
        let entries = store.raw_entries();
        let paths = self.paths.clone();
        tokio::task::spawn_blocking(move || write_snapshot_file(entries, watermark, &paths))
            .await
            .map_err(|error| PersistenceError::new(format!("Snapshot task failed: {}", error)))?
            .map_err(|error| {
                PersistenceError::new(format!("Snapshot installation failed: {}", error))
            })?;

        self.binlog
            .truncate()
            .await
            .map_err(|error| PersistenceError::new(format!("Binlog rotation failed: {}", error)))?;
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
        self.write_count.store(0, Ordering::SeqCst);
        info!(
            "Compaction complete at sequence {}: snapshot installed and binlog truncated",
            watermark
        );
        Ok(watermark)
    }
}

pub(crate) enum LogMessage {
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
                sequence,
                record,
                completion: completion_tx,
            })
            .await
            .map_err(|_| PersistenceError::new("Binlog worker is unavailable"))?;
        completion_rx
            .await
            .map_err(|_| PersistenceError::new("Binlog append completion was dropped"))?
            .map_err(PersistenceError::new)
    }

    pub(crate) async fn append_batch(
        &self,
        sequence: u64,
        batch: &CommittedBatch,
    ) -> Result<(), PersistenceError> {
        self.append(sequence, encode_committed_batch(batch)?).await
    }

    pub(crate) async fn flush(&self) -> Result<(), PersistenceError> {
        let (completion_tx, completion_rx) = oneshot::channel();
        self.sender
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

    pub(crate) async fn sync_data(&self) -> Result<(), PersistenceError> {
        let (completion_tx, completion_rx) = oneshot::channel();
        self.sender
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

    pub(crate) async fn truncate(&self) -> Result<(), PersistenceError> {
        let (completion_tx, completion_rx) = oneshot::channel();
        self.sender
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
}

pub(crate) async fn run_binlog_worker(
    mut receiver: mpsc::Receiver<LogMessage>,
    binlog: Arc<std::sync::Mutex<File>>,
    fsync_policy: FsyncPolicy,
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
                    if fsync_policy == FsyncPolicy::Always {
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

/// Owns the ordering and visibility guards for a durable state transition.
///
/// An owned boundary can move into a finalizer task, allowing persistence to
/// complete even when the originating client connection is cancelled.
pub(crate) struct CommitBoundary {
    _write_guard: tokio::sync::OwnedMutexGuard<()>,
    _visibility_guard: tokio::sync::OwnedRwLockWriteGuard<()>,
}

impl CommitBoundary {
    async fn acquire(
        write_gate: &Arc<tokio::sync::Mutex<()>>,
        visibility_gate: &Arc<tokio::sync::RwLock<()>>,
    ) -> Self {
        let write_guard = Arc::clone(write_gate).lock_owned().await;
        let visibility_guard = Arc::clone(visibility_gate).write_owned().await;
        Self {
            _write_guard: write_guard,
            _visibility_guard: visibility_guard,
        }
    }
}
