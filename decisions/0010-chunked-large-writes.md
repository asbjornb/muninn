# 0010: Large writes in chunks — one commit, chunk-sized working set

- Status: accepted (amends 0009: how the leader processes a batch of more than
  `write_chunk_rows` rows)
- Date: 2026-10-25

## Context
After the write-path fixes of 2026-10-20, bulk loads were no longer quadratic, but 1M-row
writes were still ~1.6x slower than 100k-row writes (medium forecasts bulk on tmpfs:
10.1 s vs 6.1 s). A profile showed memory-bound work growing with the batch rather than
one hot spot: a million candidate versions (each with a heap-allocated value vector),
a million dedup keys to sort, partitions of ~80k rows to sort and encode, and the whole
batch to free at the end. The batch was also converted from Arrow in one piece before
the write started, and validation, dedup and encoding were serial apart from encoding
the partitions in parallel at the very end.

## Decision
A batch of more than `EngineOptions::write_chunk_rows` rows (default 100 000; 0 = never)
is processed **in chunks** of that many rows inside its group commit, and committed
**once**: one log entry, one `system_time`, one contiguous seq range in input order.
Only the encoded segment files (written to the store as they are produced, as for any
bulk write) and their records are held until the commit. Results are by construction
those of processing the batch whole:

- **Validation and errors (SPEC §4.1, §10.4).** Chunks are validated in input order, so
  the first invalid row decides, with its index in the batch, and nothing is committed
  (files already written are orphans for GC, as for any failed bulk write). Header
  checks (batch id, writer clock) still come first. A failure that is not a row error
  — reading stored versions, writing a file, an exhausted `system_time` — first
  validates the rest of the batch, so an invalid row still wins, exactly as when the
  whole batch was validated before anything else.
- **Dedup (SPEC §4.3).** A row is a duplicate iff it is content-identical to a stored
  version or to an *earlier candidate* of its batch (or a kept row of an earlier batch
  of the group). This is the same as "an earlier kept row" (content identity is an
  equivalence, and a dropped row is identical to what it was dropped for), but it makes
  each decision a function of the input alone, so chunks can be prepared ahead. Within a
  chunk: sorting content hashes, as before for a whole batch. Against stored versions:
  the partition received-time bounds, then the dedup index, then the stored versions,
  as before, against the same state. Across chunks: a `ContentIndex` (the hash index
  used across the batches of a group) over the earlier chunks' candidates, built
  lazily: candidates are recorded only as runs sharing `(series, received_time)` — a
  content-identical row has the same pair — behind a Bloom filter of the pairs, and a
  pair's rows are validated again from the input and indexed only when a later row has
  that pair, which fresh data never has. Later batches of the group consult the same
  structure.
- **Layout.** If the batch keeps at most `inline_max_rows` rows they go inline (the
  memtable), as before. Otherwise a partition's kept rows wait (across chunks) until
  they fill a file of `write_chunk_rows` rows; when more than `write_chunk_rows` rows
  wait in all, the partitions holding the most are written. So the rows held stay
  bounded, and a write still makes few files per partition: one file per chunk and
  partition (what 100k-row writes make) left compaction 1-3 s of merging at the end of
  the medium settlement and sparse loads that whole-batch processing did not need.
  Which rows share a file depends on row counts only. Query results do not depend on
  the layout.
- **Parallelism** (threads, no async). Preparing a chunk — validating it, hashing it,
  its in-chunk duplicates, the rows its partitions' stored received-time bounds do not
  rule out, its `(series, received_time)` runs, splitting it by partition — runs up to
  `max(2, io_threads / 2)` chunks ahead of the leader. The leader takes chunks in order
  and makes only the decisions that depend on earlier ones; seqs follow from a prefix
  count of kept rows. `max(1, io_threads / 2)` encoder threads (joined by the preparing
  threads for the last files) drop a partition's duplicates, assign seqs and
  `system_time`, and encode and write its file. The same input gives byte-identical
  segments whatever the thread count, listed in the entry in a fixed order.
- **Atomic `write_many`.** A call with such a batch is committed by the caller itself
  under the writer lock (the batch is read from the caller's input while it is
  processed, so it is not queued), still as one group: one commit for the whole call.
- **Input converted in chunks.** `WriteBatch::source` (a `ColumnSource`) holds columnar
  rows that are converted on demand; the Python binding gives Arrow input this way, so
  a large batch is converted piece by piece on the preparing threads instead of all at
  once up front. Structural errors (SPEC §10.3) are still raised by the binding for the
  whole input before the write starts.

Rejected: chunking by partition (needs the whole batch first; arrival-ordered input
interleaves partitions); holding more rows for bigger files (two chunks instead of one:
settlement and sparse a little faster, forecasts 15-20% slower); and smaller chunks
(without the waiting rows, medium forecasts with 50k-row chunks took 38 s to compact on
disk instead of 0.3 s: the files got small enough for compaction to merge everything).

## Results
bench/results/README.md (Ingest, 2026-10-25), medium bulk, before/after interleaved:
forecasts with 1M-row writes 11.7 → 6.0 s on tmpfs and 12.2 → 5.9 s on disk, faster than
100k-row writes (6.5 and 8.0 s); 1M-row writes are faster than 100k-row writes on every
dataset. The small datasets (da_prices, settlement) lose 0.1-0.5 s, all in the
end-of-load compaction of the extra files.

## Consequences
- A large write now holds about six chunks of rows at a time instead of the whole
  batch. With glibc's default per-thread arenas the process's peak RSS does not go down
  (freed chunk memory stays in the worker threads' arenas; it went up on some datasets);
  with `MALLOC_ARENA_MAX=4` it drops by a third on forecasts at the same speed, with one
  arena to 40% (slower). Choosing the allocator (or arena count) is left to the ingest
  process.
- A bulk write makes a few files per touched partition instead of one (at most about
  one per chunk); compaction merges them as before.
- Writer-lock hold time is unchanged in kind (a bulk write held it for its dedup and
  encoding before; now also for validating its rows, which runs in parallel).
- Not done: freeing memory back to the OS more eagerly (needs allocator control the
  engine does not have without `unsafe`), and a lazily converted form for dict input.
