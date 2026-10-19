# 0004: Commit log on the store; no separate WAL in v1

- Status: accepted; amended by 0009 (rows go into the log entry, group commit, memtable)
- Date: 2026-10-18

## Context
Notes sketch WAL + memtable + per-partition manifests. Batches often span many partitions
(10-day forecast horizon) and must commit atomically. The engine must work on object
storage, where the only atomic primitive is a conditional PUT.

## Decision
- One **commit log** per database: `log/<20-digit version>.json`, written with
  put-if-absent. Each entry lists segments added/removed, catalog changes, batch ids, and the
  writer's seq/system_time high-water mark. Periodic checkpoints (`<version>.checkpoint.json`)
  hold the full state so opening doesn't replay the whole log.
- A write = write immutable segment files (one per touched table-partition) → put-if-absent
  the next log entry. The log entry *is* the commit; a crash before it leaves orphan
  segment files that GC removes. No WAL, no memtable in v1.
- Conflicting writers are safe without a lease: the loser's put-if-absent fails, it reloads
  and errors (or retries). A lease is an availability concern only.
- Small segments from trickle ingest are merged by compaction (results never change).
- Segments are single files with column chunks at known offsets + footer (ranged reads on
  object stores, one GET for the footer).

## Consequences
Crash recovery is "read the log". Per-write latency is two PUTs — fine for batch ingest and
local disk; for high-rate trickle ingest an inline-rows-in-log-entry optimisation (a WAL in
all but name) can be added later without changing the format of segments.
