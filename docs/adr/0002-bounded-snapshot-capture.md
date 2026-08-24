# ADR 0002: Bounded copy-on-write snapshot capture

## Status

Accepted

## Context

Generational binlog compaction removes suffix copying from the final commit
pause, but cloning the complete store while holding the authoritative write and
visibility boundary still makes that pause proportional to dataset cardinality.
On a growing dataset, a fixed record threshold also repeats full snapshot work
too frequently. Separately, writes admitted while a predecessor preflush is in
progress can dirty data faster than storage synchronizes it, making the final
generation seal depend on an unbounded feedback loop.

The required invariant is that snapshot capture under the authoritative
boundary has work bounded by the fixed shard count, while the immutable image
still represents every committed effect through watermark `W` and no later
effect. Live writes after `W` must remain available in memory and recoverable
from later binlog history. Preflush concurrency must have an explicit bound and
must not deadlock compaction or fail-stop handling.

## Decision

At capture, each engine shard moves its current map into an immutable reference-
counted base and installs an initially empty per-key change map. This operation
does not scan or clone entries. The snapshot records one expiration timestamp,
so a key logically expired at the capture boundary is omitted even if physical
expiry cleanup occurs later.

Outside the commit boundary, snapshot materialization processes shards one at a
time. It clones the immutable entries for one shard into a contiguous image,
drops that shard's snapshot reference, and immediately folds the live change map
back into the original map. Releasing shards incrementally bounds the duration
for which reads and writes use the copy-on-write representation. The contiguous
image is then encoded with the gzip fast profile and installed through the
existing crash-safe snapshot replacement protocol.

Snapshot ownership is RAII-based. Cancellation, an encoding error, or task
unwinding drops the remaining immutable views and restores every unfinished
shard before another snapshot can begin. Complete dataset replacement fails
closed if it overlaps an active snapshot epoch.

Automatic compaction uses the greater of its configured minimum and the entry
count of the last successfully materialized snapshot. Recovery and full
synchronization initialize the same floor. Recovery also restores the count of
validated records after the snapshot watermark. This gives growing datasets an
amortized snapshot cadence without changing the recovery watermark invariant or
forgetting compaction debt across restart.

Before preflush starts, compaction opens a framed-byte admission budget. Normal
commit-boundary acquisition may proceed until accepted binlog bytes reach that
limit, after which it waits. Compaction owns an unthrottled internal boundary so
it can always seal the predecessor and release waiters. The budget is released
by an RAII guard on every exit path. One physical group already admitted at the
limit may finish; coordinator group bytes are independently bounded.

Blocking checkpoint, synchronization, rotation, flush, and truncation calls run
on Tokio's blocking pool while the single binlog worker awaits them. Message
ordering remains authoritative, but a slow filesystem call no longer occupies
an asynchronous runtime worker thread. Normal grouped appends remain on the
ordered worker because their userspace write is short under `everysec` and `no`,
and moving every group to the blocking pool would add a scheduling boundary to
the hot path.

## Alternatives considered

### Clone the entire store under the commit boundary

This is simple and preserves a precise image, but pause time and transient copy
work grow directly with the dataset. Measurements falsified it at high
cardinality.

### Stream the immutable hash maps directly into gzip

This makes capture short but holds the copy-on-write epoch through compression.
Unordered hash iteration also produced materially worse compression throughput
and prolonged live-write interference.

### Copy-on-write capture followed by one global materialization and merge

This preserves correctness, but all shards remain in the delta representation
until the largest snapshot copy finishes. Under sustained writes, delta growth
and the final global merge caused avoidable contention. Incremental shard
release retains the same boundary with a shorter interference window.

### Snapshot at independent shard boundaries

Without per-entry commit versions, independently timed shard captures can
include post-watermark effects that replay twice or omit pre-watermark effects
whose log is later removed. Adding MVCC solely for compaction would be a much
larger engine redesign.

### Allow unlimited writes during preflush

This maximizes instantaneous admission but cannot bound the final durability
pause when writeback is slower than new appends. Explicit backpressure makes the
tradeoff observable and bounds generation growth.

## Consequences

- Snapshot capture pause is proportional to 64 shards rather than live entries.
- Materialization still performs one full logical image copy and can consume
  substantial CPU and transient memory; it is outside the commit boundary.
- Writes touching an immutable-base key clone that entry once into the shard
  delta until that shard is released.
- Snapshot compression remains format-compatible, although compressed size may
  increase because the fast gzip profile trades ratio for lower CPU cost.
- Automatic compaction cadence depends on the previous snapshot cardinality;
  large mutation records can still make byte growth outpace the record count.
- Slow storage can intentionally delay admission after the preflush byte budget
  is exhausted. Metrics distinguish this backpressure from boundary hold time.
- ADR 0003 adds independent byte-triggered generation rollover while preserving
  the durable cross-generation prefix defined here.
