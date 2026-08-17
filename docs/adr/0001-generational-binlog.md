# ADR 0001: Self-validating generational binlog

## Status

Accepted

## Context

Snapshot-first suffix compaction keeps snapshot compression and most suffix
copying outside the commit boundary, but finalization must still copy, flush,
and synchronize every record accepted after suffix preparation. Under sustained
write load, this produces a workload-dependent commit pause and duplicates
physical I/O for the retained suffix.

Compaction must preserve the following invariant: a snapshot at watermark `W`
contains every committed effect through `W`, while durable recovery history
contains every contiguous sequence after `W` exactly once. Every crash point
must leave enough durable evidence to prove that invariant without trusting
process memory.

## Decision

OnyxDB uses immutable, self-validating binlog segments plus one active
`onyx.binlog` file.

Before acquiring the commit boundary, compaction preflushes the active file
through a separate handle while commits continue. While holding the complete
boundary, it flushes and synchronizes the remaining delta, renames a non-empty active file to
`onyx.binlog.segment.<20-digit-end-sequence>`, and creates a new empty active
file. The segment end sequence is the capture watermark. New commits append
only to the new active file while the snapshot is written.

Recovery discovers segment files by their strict names, sorts them by declared
end sequence, inspects every record, and verifies that:

- each non-empty segment's final record matches its declared end sequence;
- adjacent segments and the active file form one contiguous sequence history;
- replay after the snapshot watermark begins exactly at `W + 1`;
- corruption, ambiguous framing, duplicate ranges, and interior incomplete
  tails fail closed.

Once a snapshot at `W` is durably installed, segments ending at or before `W`
are redundant and may be removed. A crash during cleanup is harmless because
recovery validates and skips records already represented by the snapshot.
Repeated crashes may leave multiple valid segments; no fixed previous/current
pair is assumed.

No separate manifest is used. The immutable filenames, ONX4 sequence fields,
record checksums, and versioned snapshot watermark are the catalog. This avoids
creating another mutable authority whose update would need to be atomic with
both log rotation and snapshot installation.

The previous `onyx.binlog.tmp` and `onyx.binlog.previous` recovery states remain
recognized for upgrade compatibility. New compactions do not create them.

## Alternatives considered

### Checksummed mutable manifest

A manifest can name active and sealed generations explicitly, but every
rotation introduces a three-object atomicity problem among the manifest, old
log, and new log. A double-buffered manifest reduces torn-write risk but does
not remove the state matrix. ONX4 records already contain the information
needed to validate ordering, so the additional authority is unnecessary.

### Fixed active and previous generation

Two files cover one interrupted compaction. Repeated crashes before cleanup can
legitimately require three or more generations, forcing either expensive
startup merging or another special recovery state.

### Snapshot-first prepared suffix

This is the previous design. It has a smaller recovery catalog but preserves a
suffix-size-dependent final commit pause and writes the retained suffix twice.

### Hold the commit boundary for snapshot creation

This is simplest to reason about but makes pause time proportional to dataset
serialization and storage latency.

## Consequences

- The capture pause contains final predecessor synchronization, metadata
  rotation, active-file creation, and store-image capture; the bulk preflush and
  all suffix copying remain outside that pause.
- Compaction establishes a durable cross-generation prefix even under
  `appendfsync everysec` and `appendfsync no`; normal append acknowledgements
  retain their configured policy.
- Normal post-watermark commits are written once.
- Recovery and full synchronization must understand and clean multiple segment
  files.
- Segment count is bounded during discovery to prevent unbounded startup
  resource use.
- Recognizable incomplete tails may be repaired only at the end of the complete
  physical history. An incomplete interior segment is a history gap and fails
  closed.
- Filesystem rename and directory durability remain platform-specific. Windows
  uses write-through rename; Unix synchronizes the parent directory.
