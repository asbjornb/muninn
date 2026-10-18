# 0006: Durability contract and GC safety

- Status: accepted
- Date: 2026-10-19

## Context
Review findings: GC on a lagging engine deleted another writer's live segments; orphan
age came from writer wall clocks in segment names; a lagging engine whose segments were
compacted and collected elsewhere stayed broken; a damaged checkpoint prevented opening.
Crash tests ran only on the in-memory store, so local-filesystem durability was untested.

## Decision
- **Store contract**: a mutating call that returns success is durable; one that fails
  may or may not have taken effect. `LocalFsStore` meets this with temp file → fsync →
  rename/link → directory fsync. The engine relies on nothing else: segments are written
  (durably) before the entry that references them, and the entry's create-if-absent is
  the commit. A crash "before" an op models its data never reaching disk; "after" models
  durable-but-unreported. Losing data from a call that *returned* would be a store bug,
  so no fsync-loss simulation is needed above the store; crash tests run on both
  `MemoryStore` and `LocalFsStore`.
- **GC** (FORMAT.md §3.5): decide against the log head and re-check before deleting;
  age files by GC's own observation, never by writer clocks; writers refuse to commit
  segments older than half the grace period.
- **Missing live segment** ⇒ the engine's state is stale: mark it, reload the head and
  retry the operation once.
- **Checkpoints are caches**: sealed with a checksum and bound to the CRC of the entry
  they summarise; any detectable mismatch falls back to an older checkpoint or a replay.

## Consequences
GC needs a grace period longer than any store stall; a write that stalls past half of
it fails with `io` (retry is safe: dedup or batch ids). After a restart GC re-observes
everything, delaying deletion by one grace period.
