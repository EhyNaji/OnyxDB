use std::sync::Arc;

/// Owns the ordering and visibility guards for a durable state transition.
///
/// An owned boundary can move into a finalizer task, allowing persistence to
/// complete even when the originating client connection is cancelled.
pub(crate) struct CommitBoundary {
    _write_guard: tokio::sync::OwnedMutexGuard<()>,
    _visibility_guard: tokio::sync::OwnedRwLockWriteGuard<()>,
}

impl CommitBoundary {
    pub(crate) async fn acquire(
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
