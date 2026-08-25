# ADR 0004: Preserve recoverable segment-catalog capacity

## Status

Accepted

## Context

Recovery deliberately accepts at most 1,024 immutable binlog segments. The
runtime previously tracked only segments newer than the installed snapshot.
If deletion of snapshot-covered segments repeatedly failed, those files
remained physically present but disappeared from authoritative maintenance
accounting. Later rollovers could therefore create more files than recovery
would accept, turning a cleanup problem into startup unavailability.

Snapshot failure and cleanup failure have different consequences. A failed
snapshot leaves required history in immutable segments. A successfully
installed snapshot makes segments ending at or before its watermark redundant,
but their deletion is not authoritative until the directory transition is
synchronized. A finite recovery catalog also needs a progress path when every
slot is occupied; requiring another seal before every snapshot would consume a
slot that no longer exists.

## Decision

OnyxDB treats the physical immutable-segment count as authoritative persistence
state in addition to the count not covered by the snapshot. Recovery reports
the conservative count that remains after validated cleanup, and the runtime
refreshes it under rotation ownership before every seal or snapshot capture.

Catalog maintenance validates the complete ONX4 contents and declared final
sequence of every covered segment before deletion. It then synchronizes the
parent directory and enumerates the remaining catalog. Capacity is reusable
only after that synchronization succeeds. Individual deletion failures retain
the affected files in the count and suppress immediate snapshot retry; a later
sealed generation authorizes one new cleanup attempt.

At 768 physical segments, snapshot compaction is requested even if logical
write debt and uncovered-segment debt are lower. Independent rollover may use
at most 1,023 slots, reserving the final recovery slot for snapshot progress.
If the catalog reaches 1,024 entries, compaction captures and installs a
snapshot without sealing the active generation. The active file may then
contain records on both sides of the snapshot watermark; recovery already
skips records at or below the watermark and replays the contiguous suffix.
After successful cleanup, normal byte rollover resumes.

When physical capacity is reserved, or catalog enumeration and directory
synchronization cannot establish an authoritative count, and the active file
reaches the 24 MiB admission bound, further commits wait. One already-admitted
bounded commit group may cross the limit. Successful catalog maintenance or
baseline replacement wakes the waiters. This keeps the current durable state
recoverable instead of extending the catalog or active generation without
bound.

The runtime exposes physical count, recovery limit, proactive pressure,
rollover reserve, cleanup-blocked and catalog-unavailable states, catalog
backpressure, capacity rejections, and full-catalog snapshots as permanent
metrics.

## Alternatives considered

### Continue tracking only uncovered segments

This preserves the smaller state model but assumes cleanup eventually succeeds.
A permanent permission, filesystem, or directory-synchronization failure can
then accumulate a catalog that recovery rejects.

### Raise or remove the recovery segment limit

This postpones the failure while weakening bounded startup work. It does not
solve unbounded file accumulation or make cleanup failures operationally
visible.

### Fail all writes as soon as the final slot is reserved

This is safe but unnecessarily sacrifices the bounded active generation and
cannot repair the catalog through a new snapshot. The no-seal snapshot path
preserves progress without adding a second recovery format.

### Delete covered files by filename alone

This is cheaper but trusts metadata that recovery otherwise verifies. A
misnamed or corrupted segment could be removed without detecting that its
contents cross the snapshot boundary. Catalog cleanup therefore validates the
same ONX4 identity before deletion.

## Consequences

- Cleanup failure cannot silently push newly created immutable history beyond
  the recovery catalog limit.
- Snapshot compaction remains possible at the hard limit without a manifest,
  suffix copy, or new on-disk record format.
- A full-catalog snapshot retains the covered prefix in the active file until a
  later rollover; this is correct but temporarily consumes additional disk
  space and recovery scan time.
- Covered-segment validation is linear in the bytes being removed. It runs
  outside the commit boundary but can still compete with commits for physical
  I/O, so segment-cleanup duration must remain part of compaction profiling.
- Persistent cleanup failure eventually applies bounded write backpressure and
  requires filesystem repair, a successful manual snapshot retry, or restart.
- Recovery still rejects a pre-existing catalog already larger than 1,024
  entries. Automatically deleting an unbounded legacy catalog before complete
  validation would weaken the fail-closed and bounded-recovery guarantees.
