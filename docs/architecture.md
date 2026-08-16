# OnyxDB Architecture and Reliability Invariants

## Status and purpose

This document describes the current implementation, not a target architecture.
It records the boundaries that reliability work must preserve while the server
is decomposed incrementally.

OnyxDB is one process with three listeners on one validated bind address:

```text
RESP clients ─┐
OBP clients  ─┼─> command validation ─> engine ─> binlog ─> replicas
metrics HTTP ─┘                         │          │
                                        └─ snapshot compaction
```

The diagram is intentionally simplified. The ordering gates described below are
the authoritative boundary for state changes.

## Current modules

| Module | Responsibility | Current constraint |
| --- | --- | --- |
| `src/config.rs` | Startup argument/environment parsing, defaults, validation, secret-safe debug output | Pure configuration boundary; it does not start runtime services |
| `src/clock.rs` | Shared wall-clock source for TTL, recovery, and lifecycle timestamps | Wall-clock seconds; not a monotonic duration source |
| `src/client.rs` | Bounded RESP response parsing, command encoding, connections, and interactive argument tokenization | Tooling client; the server runtime does not depend on it |
| `src/command.rs` | Shared mutation and replica-read command classification | Command names must be normalized before classification |
| `src/engine.rs` | Sharded in-memory values, expiration, type-safe mutation primitives, logical memory accounting, eviction | 64 mutex-protected shards selected by FNV-1a |
| `src/store.rs` | Typed data operations plus tentative mutation capture, admission, and rollback | Does not assign sequences or decide durability |
| `src/store/json_path.rs` | Pure parsing and execution for the supported JSON field/index path subset | No persistence, protocol, or server dependencies |
| `src/resp.rs` | Bounded RESP command framing and response encoding | RESP command subset, not complete Redis compatibility |
| `src/protocol.rs` | Bounded OBP framing and encoding | Internal/experimental protocol with a small command subset |
| `src/main.rs` | Command dispatch, authoritative mutation ordering, committed effects, persistence, replication, networking, metrics, lifecycle | Still broad; durable ordering remains intentionally co-located |
| `src/onyx-cli.rs` | Minimal interactive RESP client | Not a complete shell parser or `redis-cli` replacement |
| `src/onyx-bench.rs` | Bounded RESP benchmark runner with repeatable workloads, percentiles, and error accounting | No OBP workload or coordinated-omission correction yet |

## Runtime ownership and network exposure

The configured data directory is created and canonicalized before recovery.
The process then acquires an exclusive operating-system lock on the persistent
`onyx.lock` file and holds it for the complete runtime lifetime. Every durable
path is derived from that locked canonical directory. Two processes must never
recover or mutate the same persistence files concurrently.

RESP, OBP, and metrics use the same numeric IPv4 or IPv6 bind address. The
default is IPv4 loopback. A non-loopback bind is an explicit trust-boundary
decision: OnyxDB has no TLS, and its metrics endpoint has no authentication.
All three listeners are bound before recovery and background task startup; a
partial listener set is not an accepted runtime state.

## Authoritative state transition

All client mutation paths, including RESP, OBP, and transactions, must converge
on one committed-effect model.

For a single mutation, the server:

1. Acquires the global write gate and the exclusive visibility gate.
2. Captures the affected state.
3. Applies the engine mutation tentatively.
4. Enforces key-count and projected-memory admission, including deterministic
   capture of eviction victims.
5. Derives a canonical batch of `Put` and `Delete` effects from the actual
   before/after state.
6. Assigns the next monotonic sequence and appends that exact batch to the
   binlog.
7. Only after append success, adds the batch to the partial-sync backlog and
   publishes it to connected replicas.
8. On append failure, restores the previous state, restores any eviction
   victims, restores the sequence, and makes persistence fail closed for later
   writes.

The visibility gate prevents clients from observing a tentative mutation or a
partial transaction. Code that needs both gates acquires the write gate before
the visibility gate.

### Mutation invariant

For every acknowledged state-changing batch, the in-memory state, binlog,
replication backlog, and live replication stream refer to the same canonical
effects at the same sequence. A failed or no-op command produces no committed
batch.

Transactions containing writes hold the same ordering boundary for the whole
execution. Successful effects are persisted and replicated as one batch. If
batch persistence fails, every effect is rolled back.

### Store mutation boundary

The store owns engine-facing tentative mutation mechanics. `MutationAttempt`
captures the exact affected entries plus pre-command key and logical-memory
usage. After command semantics run, it enforces projected admission and returns
ordered eviction victims. The server either derives canonical committed effects
from that state or asks the same attempt to restore its baseline and victims.

The store does not assign sequences, append the binlog, publish replication, or
decide when a tentative mutation is durable. Those responsibilities remain on
the server side of the boundary. Exact entry installation and full replacement
APIs exist for validated recovery and synchronization paths; normal RESP command
semantics use typed store operations.

## Engine semantics

### Logical presence and type safety

- A logically expired key is absent to every presence-sensitive primitive.
- Empty blobs are values, not absence markers.
- A command must not silently replace an incompatible value type.
- Removing the final member of a list, hash, or set deletes the key atomically
  under the shard lock.
- The store facade and engine primitives must agree on missing, present,
  expired, and wrong-type outcomes.

### Memory admission

The memory limit is evaluated against logical dataset size after the tentative
mutation. Existing-value growth is subject to the same limit as a new key.
When eviction is enabled, keys participating in the command are protected from
being selected as victims. Eviction deletes are included in the committed
effect batch.

A dataset already above its configured limit may perform a non-growing change;
new growth must fit or be rolled back. Replica recovery and synchronization
install authoritative state even when it exceeds the replica's local limit,
because local eviction would cause divergence.

## Persistence

### Files and formats

- Snapshots use the `ONYXSNAP` versioned format inside gzip framing and carry a
  committed-sequence watermark.
- New binlog records use `ONX4`, contain a non-zero sequence, encode canonical
  effects, and carry a CRC32 checksum over sequence and payload.
- Structurally valid legacy `ONX3` checksumless records remain readable with a
  warning. Ambiguous or unsafe legacy histories are rejected.
- Replica lifecycle state is stored separately in a versioned `ONYXREPL` file.
- Runtime ownership uses a persistent `onyx.lock` file; it is not durable data
  and contains no credentials or state.

All length and collection fields are bounded before allocation. Decoders reject
unknown types, trailing bytes, impossible counts, invalid checksums, and
non-contiguous sequences.

### Snapshot/binlog boundary invariant

A snapshot at watermark `W` represents the complete committed state through
sequence `W`. Recovery installs that snapshot and replays only binlog sequences
greater than `W`, in contiguous order. Therefore:

- already-snapshotted mutations are never replayed twice;
- post-boundary mutations remain recoverable;
- a binlog that begins after `W + 1` is rejected as a history gap.

Compaction holds the write gate, flushes the binlog, writes and synchronizes a
temporary snapshot, durably replaces the current snapshot while retaining a
previous snapshot, and only then truncates and synchronizes the binlog. A failed
snapshot installation never authorizes binlog truncation.

Recovery may truncate only a recognizable incomplete final record after a valid
history. Complete corruption and ambiguous framing fail startup. Recovery is
staged and replaces live state only after the entire snapshot and binlog have
validated.

### Durability policy

An append acknowledgement always follows a successful userspace write and
flush. Physical synchronization depends on `appendfsync`:

- `always`: synchronize each committed batch before acknowledging it;
- `everysec`: synchronize in a background task, with an expected system-crash
  loss window of approximately one second;
- `no`: rely on operating-system writeback after userspace flush.

A periodic synchronization failure disables subsequent writes.

## Replication

Replication transports the same canonical committed-effect batches used by
persistence. A batch sequence is valid only in the history identified by the
current master's non-zero replication ID.

### Partial synchronization

Partial synchronization is allowed only when:

- the replica names the current master replication ID;
- the requested sequence is not ahead of the master;
- the retained backlog covers every sequence after the requested boundary.

Otherwise the master sends a full synchronization snapshot. Live batches are
subscribed before the snapshot boundary is captured so writes cannot fall into
the transition gap.

### Full synchronization

A replica stages the complete dataset separately. Before accepting a new
baseline it durably marks its lifecycle as `INSTALLING`, which invalidates any
previous promotable identity. The staged state becomes visible only after the
snapshot, sequence, and upstream identity are durably installed. Incremental
batches are persisted before application and acknowledgement.

The durable states are:

- `READY`: a complete baseline and upstream identity are installed; promotion
  is allowed after lifecycle shutdown.
- `INSTALLING`: synchronization is incomplete; promotion is forbidden.
- `DETACHED`: no upstream history owns the local state; the process may act as
  a master.

### Liveness and promotion boundary

Upstream authentication completes before synchronization begins. Credentials
are not included in normal logs or debug representations. Sequence-bound
heartbeats detect a connected but silent master. Network reads, writes, and
snapshot transfers have finite idle/deadline behavior.

Promotion requests cancellation of the replica lifecycle and waits for its
reader and acknowledgement tasks to stop. Only then does the process flush
replicated history, durably write `DETACHED`, clear upstream identity, and
become writable. No old-master socket or task may mutate state after this
boundary.

This is lifecycle safety, not distributed consensus. Multiple replicas can
still promote independently during a partition unless an external coordinator
provides fencing.

## Protocol boundaries

RESP and OBP enforce deterministic limits on header size, argument count,
individual bulk size, aggregate frame size, and frame lifetime. Validation
happens before large payload allocation where possible. Malformed, ambiguous,
or timed-out frames close the connection after a protocol error; the parser does
not attempt lossy resynchronization.

Internal replication framing has separate bounded readers and transfer
deadlines. Client parser limits are not used as a substitute for replication
protocol validation.

## Architectural decision: incremental decomposition

### Context

The runtime and its strongest reliability tests remain concentrated in
`main.rs`. A wholesale rewrite would create a large semantic diff across the
mutation, persistence, and replication boundaries that currently protect data
integrity.

### Options considered

1. Leave the monolith unchanged. This minimizes immediate movement but preserves
   unclear ownership and makes later changes harder to review.
2. Rewrite the server around a new crate/module graph. This can produce a clean
   shape quickly, but it creates the highest regression and merge risk.
3. Extract cohesive boundaries in dependency order while preserving behavior
   and running the complete reliability suite at every checkpoint.

### Decision

Use option 3. Startup configuration was extracted first because it is mostly
pure and has no ownership of live tasks or durable state. Future extraction
must follow the actual dependency graph rather than target file size alone.

### Consequences

- `main.rs` remains large during the transition.
- Each extraction must preserve the invariants in this document and include
  focused tests for its new public/internal boundary.
- Persistence and replication should not be separated until shared sequence,
  lifecycle, and write-gate ownership has an explicit home.
- Temporary duplication is acceptable only inside one reviewable transition;
  permanent parallel implementations of command classification, framing, or
  committed-effect semantics are not.

## Next decomposition candidates

The preferred dependency order, subject to new evidence, is:

1. Extract command execution and its typed mutation outcome without moving the
   authoritative durability gate.
2. Extract committed effects plus persistence codecs/recovery as one cohesive
   subsystem.
3. Extract replication only after its ownership of durable identity, task
   cancellation, and sequence publication is explicit.
4. Extract listener ownership, connection supervision, metrics, and shutdown.
5. Reduce `main.rs` to process bootstrap and top-level error reporting.

Benchmark modernization should precede performance optimization. Public
performance claims require reproducible workloads, latency percentiles,
payload sizes, persistence settings, warmup, error accounting, and comparison
methodology.
