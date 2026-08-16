use std::path::{Path, PathBuf};

mod codec;
mod model;
mod recovery;
mod replica_state;
mod runtime;
pub(crate) use codec::*;
pub(crate) use model::*;
pub(crate) use recovery::*;
pub(crate) use replica_state::*;
pub(crate) use runtime::*;

const SNAPSHOT_PATH: &str = "onyx.snapshot";
const BINLOG_PATH: &str = "onyx.binlog";
const REPLICA_STATE_PATH: &str = "onyx.replica";

#[derive(Clone, Debug)]
pub(crate) struct PersistencePaths {
    pub(crate) snapshot: PathBuf,
    pub(crate) snapshot_temp: PathBuf,
    pub(crate) snapshot_backup: PathBuf,
    pub(crate) binlog: PathBuf,
    pub(crate) replica_state: PathBuf,
    pub(crate) replica_state_temp: PathBuf,
}

impl PersistencePaths {
    pub(crate) fn in_directory(directory: &Path) -> Self {
        Self {
            snapshot: directory.join(SNAPSHOT_PATH),
            snapshot_temp: directory.join(format!("{}.tmp", SNAPSHOT_PATH)),
            snapshot_backup: directory.join(format!("{}.previous", SNAPSHOT_PATH)),
            binlog: directory.join(BINLOG_PATH),
            replica_state: directory.join(REPLICA_STATE_PATH),
            replica_state_temp: directory.join(format!("{}.tmp", REPLICA_STATE_PATH)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct PersistenceError {
    message: String,
    upstream_unavailable: bool,
    indeterminate: bool,
}

impl PersistenceError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            upstream_unavailable: false,
            indeterminate: false,
        }
    }

    pub(crate) fn upstream_unavailable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            upstream_unavailable: true,
            indeterminate: false,
        }
    }

    pub(crate) fn indeterminate(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            upstream_unavailable: false,
            indeterminate: true,
        }
    }

    pub(crate) fn indicates_upstream_unavailable(&self) -> bool {
        self.upstream_unavailable
    }

    pub(crate) fn is_indeterminate(&self) -> bool {
        self.indeterminate
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
