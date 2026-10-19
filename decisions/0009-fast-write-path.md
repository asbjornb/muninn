# 0009: Fast write path — rows in the commit log, group commit, memtable, background flush

- Status: accepted (amends 0004: the log now carries rows; segments are written by flushes)
- Date: 2026-10-19

## Context
Expected load: hundreds of thousands of writes a day from hundreds of sources (ENTSO-E,
MISO, CAISO, Energinet, Meteologica, Volue, …), some updating every few seconds, with
bursts far above the average. Most writes are small: one series × 96 rows (a day at 15
minutes), or a 960-row forecast run spanning 10–11 day partitions.

Under 0004 a write is one segment file per touched partition plus a put-if-absent log
entry, all written before the call returns. On local disk every object creation is a file
fsync plus a directory fsync (`LocalFsStore`, 0006): measured 1.45 ms on this machine's
btrfs NVMe, against 0.72 ms for an append + `fdatasync` to a preallocated file. A forecast
write is 12 creations (~17–20 ms); every writer serialises on the writer lock and then on
the log head. Measured with `examples/ingest.rs` (local disk, 8 s per point):

| shape | producers | writes/s | p50 | p99 |
| --- | ---: | ---: | ---: | ---: |
| 1 × 96 rows | 1 | 316 | 3.0 ms | 8.3 ms |
| 1 × 96 rows | 8 | 306 | 26 ms | 34 ms |
| 1 × 96 rows | 64 | 280 | 225 ms | 340 ms |
| 1 × 960 rows | 1 | 49 | 19 ms | 31 ms |
| 1 × 960 rows | 8 | 54 | 147 ms | 162 ms |
| 1 × 960 rows | 64 | 50 | 1 264 ms | 1 387 ms |

Concurrency adds nothing: throughput is one writer's fsync rate. On an object store the
same write is 12 sequential PUTs. The benchmark's medium forecast load took 1 940 s.

## Decision

### One mechanism for both stores: rows go into the commit entry
The commit entry is the write-ahead log. A write entry (log format 3) carries its new
versions **inline**: per touched `(table, partition)` one *inline segment*, a complete
segment file (FORMAT.md §4, uncompressed) embedded base64 in the entry's segment record
(`"data"`, key `inline/<version>/<i>`). Committing a write is one put-if-absent: one PUT on
an object store, one file creation (file fsync + directory fsync) on local disk. Readers
in other processes see the rows as soon as the entry exists, exactly as before.

A dedicated local WAL file (append + `fdatasync`, ~0.7 ms instead of ~1.45 ms) was
rejected: it needs an exclusive writer lease, a second recovery path that merges WAL and
log, and it hides acknowledged writes from other processes until they are published. With
group commit the per-commit cost is amortised over all waiting writes, so the difference
matters only for a single producer writing back to back. If it ever does, a local log
sink can be added behind the same entry format.

### Group commit
`Engine::write` (and `Engine::write_many`) queue the batch; one of the waiting callers
becomes the *leader*, takes the writer lock, and commits every queued batch in one log
entry; the others wait for their result. While a commit's fsync or PUT is in flight new
batches queue up, so the group grows with load and no artificial delay is needed. Each
call returns only after the entry holding its batch is durable (put-if-absent returned
success, 0006). If the commit fails, every batch of the group gets the error (`io`:
unknown outcome, resend with the batch id; `conflict`: no effect).

Work is split so that the serial part is small: each producer validates its batch
(SPEC §4.1, against the catalog it sees; catalog changes are additive, so a batch that
validates against an older catalog is valid against every later one) in its own thread.
The leader does, per batch in queue order: the batch-id check (committed ids and earlier
batches of the group), deduplication (against stored versions — files and memtable — and
the rows kept by earlier batches of the group), `system_time = max(c, last + 1µs)` and
consecutive seqs, then encodes one inline segment per touched partition for the whole
group and commits. A group whose stored rows exceed `inline_max_rows` (bulk loads) is
written as ordinary segment files instead (in parallel threads), in the same one-entry
commit.

### Memtable = the live inline segments
The state lists inline segments like file segments (same statistics, same partitions), and
keeps their bytes and decoded footer in memory. The scanner reads them through the same
code as files — only the byte source differs — so **every** read path (latest, history,
replay, grid, asof_join, the lead filter, receipts, dedup lookups) serves recent rows with
the same pruning and the same results, by construction. Checkpoints include the inline
segments with their data, so opening is still "checkpoint + later entries", and log
truncation (0007) never drops a memtable row.

### Flush
A flush merges the live inline segments of each `(table, partition, knowledge period)`
(the compaction group of 0008) into one segment file, writes the files (in parallel, no
lock held), then commits a `flush` entry that adds the files and removes the inline
segments, atomically. The versions are the same before and after, so no query result can
change (SPEC §7.2); the entry is validated like a compaction (per partition: equal row
counts and seq hull). Flushes run

- in a background thread (default) when the memtable holds `flush_rows` rows or
  `flush_segments` inline segments, or its oldest segment is `flush_age` old;
- synchronously in a writer when the memtable exceeds `memtable_limit_rows`
  (back-pressure when flushing cannot keep up), or at the flush thresholds when the
  background thread is off;
- on `Engine::flush()` (and before CLI compaction).

Compaction ignores inline segments. A flush is a commit like any other: if another
process flushed or compacted meanwhile, it conflicts and retries later; its files become
orphans for GC.

### Ingest process and API
The ingest process is one `Engine` shared by all producer threads (`Arc<Engine>`):
`write` blocks until durable and is safe from any number of threads; `write_many` submits
several batches as one group (pipelining for a single producer). The Python binding
releases the GIL during writes, so Python threads share commits too, and exposes
`write_many` and `flush`. A network server (HTTP / Arrow Flight) is out of scope; it is a
thin layer that turns each request into `write`/`write_many` on its connection's thread —
the queue, validation in the caller's thread and per-batch results are what it needs.
Several processes may still write to one database (optimistic concurrency on the log head,
0004), but throughput comes from one process grouping its writes.

### Recovery, GC and truncation
There is no separate WAL replay: the state is still the fold of the log (checkpoint +
entries), and inline segments are part of it. Replay is idempotent because the fold is
deterministic and entries are immutable.

- Crash before the entry exists: the write was not acknowledged; nothing to undo (a bulk
  group's files are orphans, removed by GC).
- Crash after the entry exists, before or during a flush: the rows are in the log, hence in
  the state after reopening, and are flushed later; a half-done flush leaves orphan files
  and the inline segments live. An acknowledged write is never lost: it is acknowledged
  only after its entry is durable, and from then on it is part of every later state —
  inline until a flush entry replaces it by a file holding the same rows.
- Truncation (0007) removes entries only below a verified checkpoint, and checkpoints hold
  inline segments with their data. GC never deletes inline segments (they are not files)
  and deletes flushed files only once no state references them (0006 rules unchanged).

### Semantics (unchanged)
- Every batch gets its own `system_time`, strictly greater than every earlier batch's,
  and a contiguous seq range; batches of one group take them in queue order.
- Dedup (§4.3) and batch ids (§4.4) behave as if the group's batches were committed one
  by one in queue order. A rejected batch has no effect and does not affect its group.
- Atomicity per batch: a batch is wholly inside one entry. A group is atomic as a whole,
  which is stronger than needed and invisible.
- Receipts: `S = last_system_time` of the snapshot; later batches get greater system
  times; flushing does not change versions. Reproducibility (§7.3) holds as before.

### Format (FORMAT.md, log format 3)
Entries and checkpoints are written with `format: 3`; formats 1 and 2 are read unchanged,
and a format-2 reader rejects format 3 (it would not understand inline segments).

- Segment records may carry `"data"` (base64 of a segment file): an inline segment, key
  `inline/<20-digit version>/<index in add>`.
- `batch_ids` (array) replaces `batch_id` in format-3 entries (a group records several).
- A format-3 `write` entry may hold several batches: its rows' system times lie in
  `(previous last, last]` (like `restore`).
- New kind `flush`: removes inline segments and adds the files holding their rows.

## Consequences
- Small writes cost one object creation per *group*: on local disk the write rate is
  bounded by CPU, not fsyncs, once several producers write; on an object store by group
  size / PUT latency. A single producer writing back to back still pays one fsync pair
  (~1.5 ms) per write.
- Segment files are now written in large chunks (one per partition and knowledge period
  per flush) instead of one per partition per write, so compaction has less to do and
  segment counts stay low.
- Entries and checkpoints are bigger: an inline segment costs ~15–25 bytes per row
  (uncompressed, base64). The memtable is bounded by the flush thresholds, so checkpoints
  grow by at most a few MB.
- Reads scan the memtable's small segments (from memory) until they are flushed; the
  flush thresholds bound how many there are.
- Engines that predate format 3 cannot open a database once this engine has written to it.
- Not done: a local append-only log sink; a network server; compressing inline segments.
