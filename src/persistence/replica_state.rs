use super::{
    MAX_SNAPSHOT_METADATA_SIZE, PersistenceError, PersistencePaths, durable_rename,
    sync_parent_directory,
};
use std::fs::{self, File};
use std::io::Write;
use tracing::warn;

const REPLICA_STATE_MAGIC: &str = "ONYXREPL";
const REPLICA_STATE_VERSION: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReplicaIdentity {
    pub(crate) replid: u64,
    pub(crate) baseline_sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DurableReplicaState {
    Detached,
    Installing,
    Ready(ReplicaIdentity),
}

pub(crate) fn load_durable_replica_state(
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

pub(crate) fn write_replica_identity(
    paths: &PersistencePaths,
    identity: ReplicaIdentity,
) -> Result<(), PersistenceError> {
    write_durable_replica_state(paths, DurableReplicaState::Ready(identity))
}

pub(crate) fn write_replica_installing(paths: &PersistencePaths) -> Result<(), PersistenceError> {
    write_durable_replica_state(paths, DurableReplicaState::Installing)
}

pub(crate) fn write_replica_detached(paths: &PersistencePaths) -> Result<(), PersistenceError> {
    write_durable_replica_state(paths, DurableReplicaState::Detached)
}

#[cfg(test)]
pub(crate) fn load_replica_identity(
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

pub(crate) fn prepare_replication_startup(
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
