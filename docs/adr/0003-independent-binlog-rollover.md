# ADR 0003: Independent byte-bounded binlog rollover

## Status

Accepted

## Context

Snapshot compaction previously provided the only active-binlog rotation path.
Automatic maintenance was triggered by a count of committed mutation records,
so a small number of large records could grow the active generation without
bound. Rotating that generation also required materializing and installing a
complete dataset snapshot, even though the durability operation and the
baseline replacement have different costs and ownership requirements.

Recovery accepts at most 1,024 immutable segments to bound startup work. Any
independent rollover design must therefore bound both active-generation bytes
and the number of segments that can accumulate before a snapshot. It must also
retain the established cross-generation durability rule: a successor
generation cannot become visible until its complete predecessor is durable.

## Decision

OnyxDB separates active-generation rollover from snapshot compaction.

The runtime tracks framed ONX4 bytes in the active generation authoritatively.
The counter advances only after an append is accepted, resets only after a
successful seal or baseline replacement, and is reconstructed from the
validated active file during recovery. Reaching 16 MiB requests an independent
rollover. One already-admitted bounded commit group and the 8 MiB preflush
growth budget may overshoot the target. While rollover ownership is pending,
normal commit admission stops at 24 MiB; one group that acquired the boundary
below that limit may finish. The target is a scheduling threshold, not a
record-size limit.

Rollover preflushes through a separate file handle, takes the complete commit
and visibility boundary, synchronizes the predecessor, durably renames it to
`onyx.binlog.segment.<end-sequence>`, and creates the new active file. It does
not capture, encode, install, or delete a snapshot. A definitive failure keeps
the old generation and its accounting. An indeterminate outcome enters
fail-stop. Rollover completion is owned by a supervisor, so cancellation of its
initiating waiter cannot abandon an uncertain storage transition.

Snapshot compaction and replica full synchronization retain exclusive baseline
ownership. Active-generation rotation has a separate gate. Operations that
need both always acquire baseline ownership before rotation ownership.
Compaction releases rotation ownership immediately after sealing and capturing
the copy-on-write snapshot epoch, so a later independent rollover may proceed
while the captured snapshot is materialized and installed. Full
synchronization holds both gates while destroying and replacing the baseline.
After installing the snapshot, compaction reacquires rotation ownership for
snapshot-covered segment validation, deletion, and physical catalog accounting.
This excludes a concurrent rollover catalog refresh without holding the commit
boundary; ordinary commits can continue during catalog maintenance.

The runtime separately tracks immutable segments not covered by the installed
snapshot. Recovery reconstructs this count from validated segment end
sequences. Reaching 256 segments requests snapshot compaction even when the
record threshold has not been reached. A successful snapshot subtracts only
the segments captured at its watermark; segments created while it is written
remain counted and recoverable. The 256-segment trigger leaves substantial
headroom below the 1,024-segment recovery rejection limit for retriable
snapshot failures.

Snapshot and rollover scheduling use independent cancellation-safe ownership
flags. Clearing either flag is followed by an atomic recheck of authoritative
counters, so a threshold crossing concurrent with task completion cannot be
lost. Failures are not retried in a tight loop; later accepted work can request
another attempt, while unrelated maintenance remains schedulable. Clearing a
failed rollover also wakes admission waiters, so the byte bound cannot strand
the commit path behind an operation that is no longer owned.

## Alternatives considered

### Keep rotation coupled to snapshots and add a byte snapshot trigger

This bounds active bytes but turns every large-value workload into repeated
full-dataset serialization. It preserves unnecessary CPU, memory, and storage
interference.

### Synchronize and rename outside the commit boundary

This shortens the visible pause, but permits commits to append to a successor
whose predecessor is not yet proven durable. Preserving correctness would need
a different multi-generation commit protocol or manifest authority. This ADR
does not weaken the established crash invariant for latency.

### Use one maintenance mutex for rollover and snapshot lifetime

This is simple, but a slow snapshot installation prevents otherwise independent
generation control. Separate ordered gates express the actual resources and
allow safe overlap after snapshot capture.

### Rotate without a segment-count trigger

Byte rollover alone can eventually exceed the bounded recovery catalog when a
dataset has a high cardinality-derived record threshold. Segment pressure must
therefore be an independent snapshot signal.

## Consequences

- Large-value workloads no longer wait for the record threshold before active
  history is segmented.
- A normal byte rollover performs durability and metadata I/O but no snapshot
  materialization, compression, installation, or cleanup.
- Snapshot writing can coexist with later rollover, while full-sync baseline
  replacement remains exclusive with both.
- Operators can observe active bytes, target bytes, uncovered segment count,
  segment limit, rollover ownership, outcomes, and duration.
- The final predecessor synchronization remains inside the commit boundary.
  Its pause is bounded by generation growth and admission policy, but storage
  latency itself is not bounded.
- Repeated snapshot installation failures can continue to accumulate valid
  segments. ADR 0004 adds physical catalog accounting, reserved snapshot
  capacity, and bounded admission before the recovery limit can be exceeded.
