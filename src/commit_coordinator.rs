//! Bounded, cancellation-safe coordination for authoritative master mutations.
//!
//! The worker is the sole owner of speculative master execution. It holds the
//! global commit boundary while applying a FIFO group, assigns one sequence to
//! every logical committed batch, and exposes the tentative state only after a
//! single physical binlog append accepts the complete group. A definitive
//! storage rejection restores rollback state in reverse execution order; an
//! indeterminate outcome retains the boundary through persistence fail-stop.
//! Once a request enters the queue, dropping its client response receiver never
//! cancels the mutation or changes its authoritative order.

use super::*;
use std::time::Instant;

// Count and memory budgets jointly bound queued decoded protocol data. A group
// has smaller execution limits so one busy cohort cannot monopolize visibility.
const COMMIT_QUEUE_CAPACITY: usize = 1_024;
const COMMIT_QUEUE_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const COMMIT_QUEUE_MEMORY_UNIT_BYTES: usize = 4 * 1024;
const MAX_COMMIT_GROUP_REQUESTS: usize = 64;
const MAX_COMMIT_GROUP_LOGICAL_MUTATIONS: usize = 256;
const MAX_COMMIT_GROUP_INPUT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct CommitCoordinatorMetricsSnapshot {
    pub(super) requests_total: u64,
    pub(super) queue_depth: u64,
    pub(super) queue_depth_max: u64,
    pub(super) queue_wait_nanoseconds_total: u64,
    pub(super) queue_wait_nanoseconds_max: u64,
    pub(super) groups_total: u64,
    pub(super) groups_completed_total: u64,
    pub(super) groups_rejected_total: u64,
    pub(super) groups_indeterminate_total: u64,
    pub(super) groups_interrupted_total: u64,
    pub(super) groups_in_progress: u64,
    pub(super) group_requests_total: u64,
    pub(super) group_requests_max: u64,
    pub(super) group_input_bytes_total: u64,
    pub(super) group_input_bytes_max: u64,
    pub(super) logical_batches_total: u64,
    pub(super) group_duration_nanoseconds_total: u64,
    pub(super) group_duration_nanoseconds_max: u64,
    pub(super) storage_duration_nanoseconds_total: u64,
    pub(super) storage_duration_nanoseconds_max: u64,
}

#[derive(Default)]
struct CommitCoordinatorMetrics {
    requests_total: AtomicU64,
    queue_depth: AtomicU64,
    queue_depth_max: AtomicU64,
    queue_wait_nanoseconds_total: AtomicU64,
    queue_wait_nanoseconds_max: AtomicU64,
    groups_total: AtomicU64,
    groups_completed_total: AtomicU64,
    groups_rejected_total: AtomicU64,
    groups_indeterminate_total: AtomicU64,
    groups_interrupted_total: AtomicU64,
    groups_in_progress: AtomicU64,
    group_requests_total: AtomicU64,
    group_requests_max: AtomicU64,
    group_input_bytes_total: AtomicU64,
    group_input_bytes_max: AtomicU64,
    logical_batches_total: AtomicU64,
    group_duration_nanoseconds_total: AtomicU64,
    group_duration_nanoseconds_max: AtomicU64,
    storage_duration_nanoseconds_total: AtomicU64,
    storage_duration_nanoseconds_max: AtomicU64,
}

fn duration_nanoseconds(duration: std::time::Duration) -> u64 {
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

impl CommitCoordinatorMetrics {
    fn enter_queue(&self) {
        let depth = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        observe_max(&self.queue_depth_max, depth);
    }

    fn leave_queue(&self, wait: std::time::Duration) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
        let wait = duration_nanoseconds(wait);
        self.queue_wait_nanoseconds_total
            .fetch_add(wait, Ordering::Relaxed);
        observe_max(&self.queue_wait_nanoseconds_max, wait);
    }

    fn snapshot(&self) -> CommitCoordinatorMetricsSnapshot {
        CommitCoordinatorMetricsSnapshot {
            requests_total: self.requests_total.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_depth_max: self.queue_depth_max.load(Ordering::Relaxed),
            queue_wait_nanoseconds_total: self.queue_wait_nanoseconds_total.load(Ordering::Relaxed),
            queue_wait_nanoseconds_max: self.queue_wait_nanoseconds_max.load(Ordering::Relaxed),
            groups_total: self.groups_total.load(Ordering::Relaxed),
            groups_completed_total: self.groups_completed_total.load(Ordering::Relaxed),
            groups_rejected_total: self.groups_rejected_total.load(Ordering::Relaxed),
            groups_indeterminate_total: self.groups_indeterminate_total.load(Ordering::Relaxed),
            groups_interrupted_total: self.groups_interrupted_total.load(Ordering::Relaxed),
            groups_in_progress: self.groups_in_progress.load(Ordering::Relaxed),
            group_requests_total: self.group_requests_total.load(Ordering::Relaxed),
            group_requests_max: self.group_requests_max.load(Ordering::Relaxed),
            group_input_bytes_total: self.group_input_bytes_total.load(Ordering::Relaxed),
            group_input_bytes_max: self.group_input_bytes_max.load(Ordering::Relaxed),
            logical_batches_total: self.logical_batches_total.load(Ordering::Relaxed),
            group_duration_nanoseconds_total: self
                .group_duration_nanoseconds_total
                .load(Ordering::Relaxed),
            group_duration_nanoseconds_max: self
                .group_duration_nanoseconds_max
                .load(Ordering::Relaxed),
            storage_duration_nanoseconds_total: self
                .storage_duration_nanoseconds_total
                .load(Ordering::Relaxed),
            storage_duration_nanoseconds_max: self
                .storage_duration_nanoseconds_max
                .load(Ordering::Relaxed),
        }
    }
}

struct QueueMeasurement {
    metrics: Arc<CommitCoordinatorMetrics>,
    enqueued_at: Instant,
    queued: bool,
}

impl QueueMeasurement {
    fn new(metrics: Arc<CommitCoordinatorMetrics>) -> Self {
        metrics.enter_queue();
        Self {
            metrics,
            enqueued_at: Instant::now(),
            queued: true,
        }
    }

    fn leave_queue(&mut self) {
        if self.queued {
            self.queued = false;
            self.metrics.leave_queue(self.enqueued_at.elapsed());
        }
    }
}

impl Drop for QueueMeasurement {
    fn drop(&mut self) {
        self.leave_queue();
    }
}

#[derive(Clone, Copy)]
enum CommitGroupResult {
    Completed,
    Rejected,
    Indeterminate,
}

struct CommitGroupMeasurement {
    metrics: Arc<CommitCoordinatorMetrics>,
    started_at: Instant,
    finished: bool,
}

impl CommitGroupMeasurement {
    fn start(metrics: Arc<CommitCoordinatorMetrics>, requests: usize, input_bytes: usize) -> Self {
        let requests = u64::try_from(requests).unwrap_or(u64::MAX);
        let input_bytes = u64::try_from(input_bytes).unwrap_or(u64::MAX);
        metrics.groups_total.fetch_add(1, Ordering::Relaxed);
        metrics.groups_in_progress.fetch_add(1, Ordering::Relaxed);
        metrics
            .group_requests_total
            .fetch_add(requests, Ordering::Relaxed);
        observe_max(&metrics.group_requests_max, requests);
        metrics
            .group_input_bytes_total
            .fetch_add(input_bytes, Ordering::Relaxed);
        observe_max(&metrics.group_input_bytes_max, input_bytes);
        Self {
            metrics,
            started_at: Instant::now(),
            finished: false,
        }
    }

    fn observe_storage(&self, duration: std::time::Duration) {
        let duration = duration_nanoseconds(duration);
        self.metrics
            .storage_duration_nanoseconds_total
            .fetch_add(duration, Ordering::Relaxed);
        observe_max(&self.metrics.storage_duration_nanoseconds_max, duration);
    }

    fn finish(mut self, result: CommitGroupResult, logical_batches: usize) {
        self.finished = true;
        self.metrics.logical_batches_total.fetch_add(
            u64::try_from(logical_batches).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        match result {
            CommitGroupResult::Completed => &self.metrics.groups_completed_total,
            CommitGroupResult::Rejected => &self.metrics.groups_rejected_total,
            CommitGroupResult::Indeterminate => &self.metrics.groups_indeterminate_total,
        }
        .fetch_add(1, Ordering::Relaxed);
        self.record_end();
    }

    fn record_end(&self) {
        self.metrics
            .groups_in_progress
            .fetch_sub(1, Ordering::Relaxed);
        let duration = duration_nanoseconds(self.started_at.elapsed());
        self.metrics
            .group_duration_nanoseconds_total
            .fetch_add(duration, Ordering::Relaxed);
        observe_max(&self.metrics.group_duration_nanoseconds_max, duration);
    }
}

impl Drop for CommitGroupMeasurement {
    fn drop(&mut self) {
        if !self.finished {
            self.metrics
                .groups_interrupted_total
                .fetch_add(1, Ordering::Relaxed);
            self.record_end();
        }
    }
}

#[derive(Clone)]
pub(super) struct MasterCommitCoordinator {
    sender: mpsc::Sender<MasterCommitRequest>,
    memory: Arc<tokio::sync::Semaphore>,
    metrics: Arc<CommitCoordinatorMetrics>,
}

pub(super) enum ObpMutationResult {
    Value(OnyxValue),
    Error(String),
}

enum MasterCommitOperation {
    Command(Vec<String>),
    CommandGroup(Vec<Vec<String>>),
    Transaction(Vec<Vec<String>>),
    ObpSet {
        key: Bytes,
        value: Bytes,
    },
    ObpDelete {
        key: Bytes,
    },
    #[cfg(test)]
    PanicForTest,
}

impl MasterCommitOperation {
    fn estimated_bytes(&self) -> usize {
        fn arguments_size(arguments: &[String]) -> usize {
            arguments.iter().fold(64usize, |total, argument| {
                total.saturating_add(argument.len().saturating_add(24))
            })
        }

        match self {
            Self::Command(arguments) => arguments_size(arguments),
            Self::CommandGroup(commands) | Self::Transaction(commands) => {
                commands.iter().fold(64usize, |total, command| {
                    total.saturating_add(arguments_size(command))
                })
            }
            Self::ObpSet { key, value } => 64usize
                .saturating_add(key.len())
                .saturating_add(value.len()),
            Self::ObpDelete { key } => 64usize.saturating_add(key.len()),
            #[cfg(test)]
            Self::PanicForTest => 64,
        }
    }
}

enum MasterCommitResponse {
    Command(CommandOutcome),
    CommandGroup(Vec<CommandOutcome>),
    Transaction(RESPValue),
    Obp(ObpMutationResult),
}

impl MasterCommitResponse {
    fn mark_committed(&mut self) {
        if let Self::Command(outcome) = self {
            outcome.mutation = MutationState::Committed;
        } else if let Self::CommandGroup(outcomes) = self {
            for outcome in outcomes {
                if outcome.mutation == MutationState::Tentative {
                    outcome.mutation = MutationState::Committed;
                }
            }
        }
    }
}

struct MasterCommitRequest {
    operation: Option<MasterCommitOperation>,
    completion: tokio::sync::oneshot::Sender<Result<MasterCommitResponse, PersistenceError>>,
    _memory: tokio::sync::OwnedSemaphorePermit,
    queue_measurement: QueueMeasurement,
}

impl MasterCommitRequest {
    fn leave_queue(&mut self) {
        self.queue_measurement.leave_queue();
    }

    fn estimated_bytes(&self) -> usize {
        self.operation
            .as_ref()
            .expect("a queued commit request owns one operation")
            .estimated_bytes()
    }

    fn logical_mutation_upper_bound(&self) -> usize {
        match self
            .operation
            .as_ref()
            .expect("a queued commit request owns one operation")
        {
            MasterCommitOperation::CommandGroup(commands) => commands.len().max(1),
            _ => 1,
        }
    }
}

struct PreparedMasterCommit {
    response: MasterCommitResponse,
    batches: Vec<CommittedBatch>,
    rollbacks: Vec<MutationRollback>,
    completion: tokio::sync::oneshot::Sender<Result<MasterCommitResponse, PersistenceError>>,
    _memory: tokio::sync::OwnedSemaphorePermit,
}

struct PreparationFailure {
    error: PersistenceError,
    request: MasterCommitRequest,
}

type PreparationResult = Result<PreparedMasterCommit, Box<PreparationFailure>>;

fn preparation_failure(
    error: PersistenceError,
    request: MasterCommitRequest,
) -> Box<PreparationFailure> {
    Box::new(PreparationFailure { error, request })
}

impl PreparedMasterCommit {
    fn without_mutation(response: MasterCommitResponse, request: MasterCommitRequest) -> Self {
        Self {
            response,
            batches: Vec::new(),
            rollbacks: Vec::new(),
            completion: request.completion,
            _memory: request._memory,
        }
    }

    fn with_mutation(
        response: MasterCommitResponse,
        batch: CommittedBatch,
        rollback: MutationRollback,
        request: MasterCommitRequest,
    ) -> Self {
        Self {
            response,
            batches: vec![batch],
            rollbacks: vec![rollback],
            completion: request.completion,
            _memory: request._memory,
        }
    }

    fn with_mutations(
        response: MasterCommitResponse,
        batches: Vec<CommittedBatch>,
        rollbacks: Vec<MutationRollback>,
        request: MasterCommitRequest,
    ) -> Self {
        debug_assert_eq!(batches.len(), rollbacks.len());
        Self {
            response,
            batches,
            rollbacks,
            completion: request.completion,
            _memory: request._memory,
        }
    }
}

impl MasterCommitCoordinator {
    pub(super) fn start(store: Arc<ShardedStore>, persistence: &Arc<Persistence>) -> Self {
        let (sender, receiver) = mpsc::channel(COMMIT_QUEUE_CAPACITY);
        let memory_units = COMMIT_QUEUE_MEMORY_BYTES / COMMIT_QUEUE_MEMORY_UNIT_BYTES;
        let memory = Arc::new(tokio::sync::Semaphore::new(memory_units));
        let metrics = Arc::new(CommitCoordinatorMetrics::default());
        let persistence_weak = Arc::downgrade(persistence);
        let supervisor_persistence = persistence_weak.clone();
        let worker = tokio::spawn(run_master_commit_coordinator(
            store,
            persistence_weak,
            receiver,
            Arc::clone(&metrics),
        ));
        tokio::spawn(async move {
            let outcome = worker.await;
            let Some(persistence) = supervisor_persistence.upgrade() else {
                return;
            };
            if persistence.is_fail_stopped() || !persistence.accepting_writes.load(Ordering::SeqCst)
            {
                return;
            }
            let detail = match outcome {
                Ok(()) => "Master commit coordinator stopped unexpectedly".to_string(),
                Err(error) => format!("Master commit coordinator failed: {}", error),
            };
            enter_persistence_fail_stop(&persistence, detail).await;
        });

        Self {
            sender,
            memory,
            metrics,
        }
    }

    async fn submit(
        &self,
        operation: MasterCommitOperation,
    ) -> Result<MasterCommitResponse, PersistenceError> {
        if matches!(
            &operation,
            MasterCommitOperation::CommandGroup(commands)
                if commands.is_empty() || commands.len() > MAX_COMMIT_GROUP_LOGICAL_MUTATIONS
        ) {
            return Err(PersistenceError::new(
                "Commit command group exceeds the coordinator limit",
            ));
        }
        let bytes = operation.estimated_bytes().max(1);
        if bytes > COMMIT_QUEUE_MEMORY_BYTES {
            return Err(PersistenceError::new(
                "Commit request exceeds the coordinator memory budget",
            ));
        }
        let units = bytes
            .div_ceil(COMMIT_QUEUE_MEMORY_UNIT_BYTES)
            .try_into()
            .map_err(|_| PersistenceError::new("Commit request memory accounting overflow"))?;
        let memory = Arc::clone(&self.memory)
            .acquire_many_owned(units)
            .await
            .map_err(|_| PersistenceError::new("Master commit coordinator is unavailable"))?;
        let (completion, response) = tokio::sync::oneshot::channel();
        let queue_measurement = QueueMeasurement::new(Arc::clone(&self.metrics));
        self.sender
            .send(MasterCommitRequest {
                operation: Some(operation),
                completion,
                _memory: memory,
                queue_measurement,
            })
            .await
            .map_err(|_| PersistenceError::new("Master commit coordinator is unavailable"))?;
        self.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
        response.await.map_err(|_| {
            PersistenceError::indeterminate("Master commit coordinator dropped the outcome")
        })?
    }

    pub(super) async fn execute_command(
        &self,
        arguments: Vec<String>,
    ) -> Result<CommandOutcome, PersistenceError> {
        match self
            .submit(MasterCommitOperation::Command(arguments))
            .await?
        {
            MasterCommitResponse::Command(outcome) => Ok(outcome),
            _ => Err(PersistenceError::indeterminate(
                "Master commit coordinator returned a mismatched command outcome",
            )),
        }
    }

    pub(super) async fn execute_commands(
        &self,
        commands: Vec<Vec<String>>,
    ) -> Result<Vec<CommandOutcome>, PersistenceError> {
        match self
            .submit(MasterCommitOperation::CommandGroup(commands))
            .await?
        {
            MasterCommitResponse::CommandGroup(outcomes) => Ok(outcomes),
            _ => Err(PersistenceError::indeterminate(
                "Master commit coordinator returned a mismatched command group outcome",
            )),
        }
    }

    pub(super) async fn execute_transaction(
        &self,
        commands: Vec<Vec<String>>,
    ) -> Result<RESPValue, PersistenceError> {
        match self
            .submit(MasterCommitOperation::Transaction(commands))
            .await?
        {
            MasterCommitResponse::Transaction(response) => Ok(response),
            _ => Err(PersistenceError::indeterminate(
                "Master commit coordinator returned a mismatched transaction outcome",
            )),
        }
    }

    pub(super) async fn execute_obp_set(
        &self,
        key: Bytes,
        value: Bytes,
    ) -> Result<ObpMutationResult, PersistenceError> {
        match self
            .submit(MasterCommitOperation::ObpSet { key, value })
            .await?
        {
            MasterCommitResponse::Obp(response) => Ok(response),
            _ => Err(PersistenceError::indeterminate(
                "Master commit coordinator returned a mismatched OBP outcome",
            )),
        }
    }

    pub(super) async fn execute_obp_delete(
        &self,
        key: Bytes,
    ) -> Result<ObpMutationResult, PersistenceError> {
        match self
            .submit(MasterCommitOperation::ObpDelete { key })
            .await?
        {
            MasterCommitResponse::Obp(response) => Ok(response),
            _ => Err(PersistenceError::indeterminate(
                "Master commit coordinator returned a mismatched OBP outcome",
            )),
        }
    }

    #[cfg(test)]
    pub(super) fn pending_requests(&self) -> usize {
        self.sender.max_capacity() - self.sender.capacity()
    }

    pub(super) fn metrics_snapshot(&self) -> CommitCoordinatorMetricsSnapshot {
        self.metrics.snapshot()
    }

    #[cfg(test)]
    pub(super) async fn panic_worker_for_test(&self) -> PersistenceError {
        match self.submit(MasterCommitOperation::PanicForTest).await {
            Err(error) => error,
            Ok(_) => panic!("an injected coordinator panic cannot return a response"),
        }
    }
}

fn copy_persistence_error(error: &PersistenceError) -> PersistenceError {
    if error.is_indeterminate() {
        PersistenceError::indeterminate(error.to_string())
    } else {
        PersistenceError::new(error.to_string())
    }
}

struct PreparedCommandOperation {
    outcome: CommandOutcome,
    batch: Option<CommittedBatch>,
    rollback: Option<MutationRollback>,
}

fn prepare_command_operation(
    store: &ShardedStore,
    arguments: &[String],
) -> Result<PreparedCommandOperation, PersistenceError> {
    let affected_keys = persistent_keys_for_command(arguments);
    let mut attempt = store.begin_mutation(&affected_keys);
    let mut outcome = execute_command(store, arguments);
    if derive_committed_batch(store, &affected_keys, attempt.before_entries(), &[]).is_none() {
        attempt.commit();
        outcome.mutation = MutationState::NoChange;
        return Ok(PreparedCommandOperation {
            outcome,
            batch: None,
            rollback: None,
        });
    }

    if let Err(error) = attempt.admit(&affected_keys) {
        attempt.rollback();
        return Ok(PreparedCommandOperation {
            outcome: CommandOutcome {
                response: RESPValue::Error(error.message().to_string()),
                mutation: MutationState::NoChange,
            },
            batch: None,
            rollback: None,
        });
    }
    let Some(batch) = derive_committed_batch(
        store,
        &affected_keys,
        attempt.before_entries(),
        attempt.evicted_entries(),
    ) else {
        attempt.rollback();
        return Err(PersistenceError::new(
            "Committed effect derivation became empty after admission",
        ));
    };

    Ok(PreparedCommandOperation {
        outcome,
        batch: Some(batch),
        rollback: Some(attempt.into_rollback()),
    })
}

fn prepare_command(
    store: &ShardedStore,
    request: MasterCommitRequest,
    arguments: Vec<String>,
) -> PreparationResult {
    let prepared = match prepare_command_operation(store, &arguments) {
        Ok(prepared) => prepared,
        Err(error) => return Err(preparation_failure(error, request)),
    };
    let response = MasterCommitResponse::Command(prepared.outcome);
    let Some(batch) = prepared.batch else {
        return Ok(PreparedMasterCommit::without_mutation(response, request));
    };

    Ok(PreparedMasterCommit::with_mutation(
        response,
        batch,
        prepared
            .rollback
            .expect("a prepared command mutation owns rollback state"),
        request,
    ))
}

fn prepare_command_group(
    store: &ShardedStore,
    request: MasterCommitRequest,
    commands: Vec<Vec<String>>,
) -> PreparationResult {
    let mut outcomes = Vec::with_capacity(commands.len());
    let mut batches = Vec::new();
    let mut rollbacks: Vec<MutationRollback> = Vec::new();
    for arguments in &commands {
        let prepared = match prepare_command_operation(store, arguments) {
            Ok(prepared) => prepared,
            Err(error) => {
                for rollback in rollbacks.iter().rev() {
                    rollback.restore(store);
                }
                return Err(preparation_failure(error, request));
            }
        };
        outcomes.push(prepared.outcome);
        if let Some(batch) = prepared.batch {
            batches.push(batch);
            rollbacks.push(
                prepared
                    .rollback
                    .expect("a prepared command mutation owns rollback state"),
            );
        }
    }

    let response = MasterCommitResponse::CommandGroup(outcomes);
    if batches.is_empty() {
        return Ok(PreparedMasterCommit::without_mutation(response, request));
    }
    Ok(PreparedMasterCommit::with_mutations(
        response, batches, rollbacks, request,
    ))
}

fn prepare_transaction(
    store: &ShardedStore,
    request: MasterCommitRequest,
    commands: Vec<Vec<String>>,
) -> PreparedMasterCommit {
    let mut baseline = std::collections::HashMap::<Bytes, Option<DataEntry>>::new();
    let mut changed_keys = Vec::new();
    let mut changed_key_set = HashSet::new();
    let mut results = Vec::with_capacity(commands.len());

    for arguments in &commands {
        let command = arguments.first().map(String::as_str).unwrap_or("");
        if !is_write_command(command) {
            results.push(execute_command(store, arguments).into_response());
            continue;
        }

        let affected_keys = persistent_keys_for_command(arguments);
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
        let outcome = execute_command(store, arguments);
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
    let response = MasterCommitResponse::Transaction(RESPValue::Array(results));
    let Some(batch) = derive_committed_batch(store, &changed_keys, &baseline, &[]) else {
        return PreparedMasterCommit::without_mutation(response, request);
    };
    PreparedMasterCommit::with_mutation(
        response,
        batch,
        MutationRollback::from_baseline(baseline),
        request,
    )
}

fn prepare_obp_set(
    store: &ShardedStore,
    request: MasterCommitRequest,
    key: Bytes,
    value: Bytes,
) -> PreparationResult {
    let affected_keys = [key.clone()];
    let mut attempt = store.begin_mutation(&affected_keys);
    store.set_value(key, OnyxValue::Blob(value), None);
    if derive_committed_batch(store, &affected_keys, attempt.before_entries(), &[]).is_none() {
        attempt.commit();
        return Ok(PreparedMasterCommit::without_mutation(
            MasterCommitResponse::Obp(ObpMutationResult::Value(OnyxValue::Blob(
                Bytes::from_static(b"OK"),
            ))),
            request,
        ));
    }
    if let Err(error) = attempt.admit(&affected_keys) {
        attempt.rollback();
        return Ok(PreparedMasterCommit::without_mutation(
            MasterCommitResponse::Obp(ObpMutationResult::Error(error.message().to_string())),
            request,
        ));
    }
    let Some(batch) = derive_committed_batch(
        store,
        &affected_keys,
        attempt.before_entries(),
        attempt.evicted_entries(),
    ) else {
        attempt.rollback();
        return Err(preparation_failure(
            PersistenceError::new("OBP committed effect derivation failed after admission"),
            request,
        ));
    };
    Ok(PreparedMasterCommit::with_mutation(
        MasterCommitResponse::Obp(ObpMutationResult::Value(OnyxValue::Blob(
            Bytes::from_static(b"OK"),
        ))),
        batch,
        attempt.into_rollback(),
        request,
    ))
}

fn prepare_obp_delete(
    store: &ShardedStore,
    request: MasterCommitRequest,
    key: Bytes,
) -> PreparedMasterCommit {
    let affected_keys = [key.clone()];
    let attempt = store.begin_mutation(&affected_keys);
    let deleted = store.delete_bytes(&key);
    let response =
        MasterCommitResponse::Obp(ObpMutationResult::Value(OnyxValue::Int(i64::from(deleted))));
    if !deleted {
        attempt.commit();
        return PreparedMasterCommit::without_mutation(response, request);
    }
    PreparedMasterCommit::with_mutation(
        response,
        CommittedBatch {
            effects: vec![CommittedEffect::Delete { key }],
        },
        attempt.into_rollback(),
        request,
    )
}

fn prepare_request(store: &ShardedStore, mut request: MasterCommitRequest) -> PreparationResult {
    let operation = request
        .operation
        .take()
        .expect("a queued commit request owns one operation");
    match operation {
        MasterCommitOperation::Command(arguments) => prepare_command(store, request, arguments),
        MasterCommitOperation::CommandGroup(commands) => {
            prepare_command_group(store, request, commands)
        }
        MasterCommitOperation::Transaction(commands) => {
            Ok(prepare_transaction(store, request, commands))
        }
        MasterCommitOperation::ObpSet { key, value } => prepare_obp_set(store, request, key, value),
        MasterCommitOperation::ObpDelete { key } => Ok(prepare_obp_delete(store, request, key)),
        #[cfg(test)]
        MasterCommitOperation::PanicForTest => panic!("injected master commit coordinator panic"),
    }
}

fn reject_requests(requests: Vec<MasterCommitRequest>, error: &PersistenceError) {
    for request in requests {
        let _ = request.completion.send(Err(copy_persistence_error(error)));
    }
}

fn fail_prepared(prepared: Vec<PreparedMasterCommit>, error: &PersistenceError) {
    for pending in prepared {
        let _ = pending.completion.send(Err(copy_persistence_error(error)));
    }
}

fn rollback_prepared(store: &ShardedStore, prepared: &[PreparedMasterCommit]) {
    for pending in prepared.iter().rev() {
        for rollback in pending.rollbacks.iter().rev() {
            rollback.restore(store);
        }
    }
}

fn complete_prepared(mut prepared: Vec<PreparedMasterCommit>) {
    for mut pending in prepared.drain(..) {
        if !pending.batches.is_empty() {
            pending.response.mark_committed();
        }
        let _ = pending.completion.send(Ok(pending.response));
    }
}

async fn process_commit_group(
    store: &Arc<ShardedStore>,
    persistence: &Arc<Persistence>,
    requests: Vec<MasterCommitRequest>,
    metrics: Arc<CommitCoordinatorMetrics>,
) {
    let input_bytes = requests.iter().fold(0usize, |total, request| {
        total.saturating_add(request.estimated_bytes())
    });
    let measurement = CommitGroupMeasurement::start(metrics, requests.len(), input_bytes);
    if persistence.is_fail_stopped() || !persistence.accepting_writes.load(Ordering::SeqCst) {
        let error = PersistenceError::new(persistence_unavailable_message(persistence));
        reject_requests(requests, &error);
        measurement.finish(CommitGroupResult::Rejected, 0);
        return;
    }

    let boundary = persistence.acquire_commit_boundary().await;
    if !persistence.accepting_writes.load(Ordering::SeqCst) {
        drop(boundary);
        let error = PersistenceError::new(persistence_unavailable_message(persistence));
        reject_requests(requests, &error);
        measurement.finish(CommitGroupResult::Rejected, 0);
        return;
    }
    let commit_guard = PersistenceCommitGuard::new(
        Arc::clone(persistence),
        boundary,
        "Master commit coordinator group",
    );

    let mut prepared = Vec::with_capacity(requests.len());
    let mut requests = requests.into_iter();
    while let Some(request) = requests.next() {
        match prepare_request(store, request) {
            Ok(pending) => prepared.push(pending),
            Err(failure) => {
                let PreparationFailure {
                    error,
                    request: failed_request,
                } = *failure;
                rollback_prepared(store, &prepared);
                mark_persistence_failed(persistence, error.to_string());
                commit_guard.release();
                fail_prepared(prepared, &error);
                let mut rejected = vec![failed_request];
                rejected.extend(requests);
                reject_requests(rejected, &error);
                measurement.finish(CommitGroupResult::Rejected, 0);
                return;
            }
        }
    }

    let mut next_sequence = persistence.sequence();
    let mut batches = Vec::new();
    for pending in &prepared {
        for batch in &pending.batches {
            let Some(sequence) = next_sequence.checked_add(1) else {
                let error = PersistenceError::new("Persistence sequence is exhausted");
                rollback_prepared(store, &prepared);
                mark_persistence_failed(persistence, error.to_string());
                commit_guard.release();
                fail_prepared(prepared, &error);
                measurement.finish(CommitGroupResult::Rejected, 0);
                return;
            };
            next_sequence = sequence;
            batches.push((sequence, batch.clone()));
        }
    }

    if batches.is_empty() {
        commit_guard.release();
        complete_prepared(prepared);
        measurement.finish(CommitGroupResult::Completed, 0);
        return;
    }

    let first_sequence = batches[0].0;
    let last_sequence = batches
        .last()
        .expect("a non-empty commit group has a last batch")
        .0;
    let storage_started = Instant::now();
    let persistence_result = persist_and_publish_master_batches(persistence, &batches).await;
    measurement.observe_storage(storage_started.elapsed());
    match persistence_result {
        Ok(should_compact) => {
            commit_guard.release();
            complete_prepared(prepared);
            schedule_compaction(store, persistence, should_compact);
            measurement.finish(CommitGroupResult::Completed, batches.len());
        }
        Err(error) if error.is_indeterminate() => {
            commit_guard.fail_stop(format!(
                "Master commit group persistence is indeterminate for sequences {} through {}: {}",
                first_sequence, last_sequence, error
            ));
            fail_prepared(prepared, &error);
            measurement.finish(CommitGroupResult::Indeterminate, batches.len());
        }
        Err(error) => {
            rollback_prepared(store, &prepared);
            mark_persistence_failed(
                persistence,
                format!(
                    "Master commit group persistence failed for sequences {} through {}: {}",
                    first_sequence, last_sequence, error
                ),
            );
            commit_guard.release();
            fail_prepared(prepared, &error);
            measurement.finish(CommitGroupResult::Rejected, batches.len());
        }
    }
}

async fn run_master_commit_coordinator(
    store: Arc<ShardedStore>,
    persistence: std::sync::Weak<Persistence>,
    mut receiver: mpsc::Receiver<MasterCommitRequest>,
    metrics: Arc<CommitCoordinatorMetrics>,
) {
    let mut requests = Vec::with_capacity(MAX_COMMIT_GROUP_REQUESTS);
    let mut deferred = None;
    loop {
        requests.clear();
        let mut first = match deferred.take() {
            Some(request) => request,
            None => match receiver.recv().await {
                Some(request) => request,
                None => return,
            },
        };
        first.leave_queue();
        let mut logical_mutations = first.logical_mutation_upper_bound();
        let mut input_bytes = first.estimated_bytes();
        requests.push(first);
        tokio::task::yield_now().await;
        while requests.len() < MAX_COMMIT_GROUP_REQUESTS {
            let Ok(mut request) = receiver.try_recv() else {
                break;
            };
            let projected_mutations =
                logical_mutations.saturating_add(request.logical_mutation_upper_bound());
            let projected_bytes = input_bytes.saturating_add(request.estimated_bytes());
            if projected_mutations > MAX_COMMIT_GROUP_LOGICAL_MUTATIONS
                || projected_bytes > MAX_COMMIT_GROUP_INPUT_BYTES
            {
                deferred = Some(request);
                break;
            }
            logical_mutations = projected_mutations;
            input_bytes = projected_bytes;
            request.leave_queue();
            requests.push(request);
        }
        let Some(persistence) = persistence.upgrade() else {
            let error = PersistenceError::new("Master commit coordinator is shutting down");
            reject_requests(std::mem::take(&mut requests), &error);
            return;
        };
        process_commit_group(
            &store,
            &persistence,
            std::mem::take(&mut requests),
            Arc::clone(&metrics),
        )
        .await;
    }
}
