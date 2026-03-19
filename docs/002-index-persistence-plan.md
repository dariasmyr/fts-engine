# Index Persistence Plan (WAL + Checkpoint + Final Snapshot)

## Context

- Segment accepts writes while it is `open`.
- Segment sealing thresholds are controlled by the client layer (for example, `1M` records or `512MB`).
- After `seal`, writes stop and segment becomes immutable.
- FTS library must not own file naming/policy; it should write to provided `io.Writer` and read from `io.Reader`.

## Goal

Provide crash-tolerant indexing without full reindex after restart, while keeping immutable segment snapshots.

## Recommended Architecture

Use three artifacts with clear responsibilities:

1. **In-memory index**
   - primary structure for indexing/search throughput.

2. **WAL (append-only op-log)**
   - written during ingest (`key`, `docID`, `count`, `seq`, checksum).
   - guarantees progress survives process crash.

3. **Snapshot (checkpoint/final)**
   - full serialized index written to provided `io.Writer`.
   - checkpoint reduces WAL replay time.
   - final snapshot published on `seal`.

Snapshot format is internal serialization written to `io.Writer`; extension like `.fidx` is client convention only.

## Responsibility Boundaries

- **Client (Amber/ingest layer)**
  - decides when to seal segment,
  - provides destination (`io.Writer` / path / object key),
  - performs atomic publish policy (`.tmp` + rename / object commit),
  - stores manifest/metadata.

- **FTS library**
  - indexes documents in memory,
  - appends WAL records during ingest,
  - creates checkpoint/final snapshots,
  - serializes/deserializes index via `io.Writer` / `io.Reader`.

## Lifecycle

1. `open`:
   - ingest writes docs,
   - append operation to `segment.wal`,
   - apply `Index.Insert(...)` in memory.

2. periodic `checkpoint` (optional but recommended):
   - write full snapshot to temporary destination (`checkpoint.tmp`) using direct writer,
   - atomically publish checkpoint,
   - persist checkpoint metadata with WAL offset (`last_seq`).

3. `seal`:
   - stop accepting new docs,
   - flush/sync WAL,
   - write final full snapshot into temp destination (prefer direct stream, no full RAM copy),
   - atomic publish to final artifact (for example `segment-<id>.fidx`),
   - mark segment `ready`,
   - rotate/truncate WAL.

4. `ready`:
   - search uses final snapshot artifact.

5. startup/recovery:
   - if final snapshot exists: load it,
   - else if checkpoint exists: load checkpoint,
   - replay WAL from saved `last_seq + 1`,
   - on corrupted WAL tail: stop at last valid checksummed record,
   - if no snapshot/checkpoint: rebuild from segment source,
   - update manifest/status.

## Write Batching

- Use buffered writing for snapshot/WAL persistence to reduce syscall overhead.
- Tune per environment:
  - `buffer_size` (for example `1-8 MiB`),
  - `flush_threshold` (for example `256 KiB-1 MiB`),
  - file sync policy (`sync on seal/checkpoint` at minimum).
- Keep durability boundaries explicit: buffered data is not durable until flushed/synced.

## Why this approach

- Snapshot gives fast startup and immutable segment artifact.
- WAL preserves indexing progress across crashes.
- Checkpoints bound WAL replay time.
- `io.Writer`/`io.Reader` keeps storage backend pluggable (file/object storage/memory).
- No hard dependency on a specific extension or storage layout.
- WAL and snapshot are separated (no mixed file semantics).

## MVP Scope

1. Add WAL writer (append-only, `seq` + checksum, flush/sync policy).
2. Keep current in-memory indexing logic.
3. Keep persistence API storage-agnostic (`io.Writer`/`io.Reader`), no mandatory `.fidx` contract in library.
4. Add periodic checkpoint writer with atomic publish and saved WAL offset.
5. On `seal`, write final snapshot to temp destination and atomically publish.
6. Add startup recovery: load final/checkpoint and replay WAL tail.
7. Add manifest fields (`segment_id`, `status`, `snapshot_uri`, `checkpoint_uri`, `wal_uri`, `last_seq`, timestamps, version).

## Future Enhancements

- Snapshot compression (`zstd`/`gzip`) depending on CPU/IO budget.
- Compaction of multiple sealed segments.
- Metrics: `build_time`, `peak_ram`, `snapshot_size`, `checkpoint_time`, `wal_size`, `wal_replay_time`, `recovery_time`.
