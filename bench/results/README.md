# Benchmark results

One directory per run date and host; `<scale>.md` is the readable report, `<scale>.json`
everything measured (per-instance result hashes included). Regenerate with
`uv run --project bench python bench/run.py --scale <scale>`. Ingest has its own reports,
`ingest-<scale>.{md,json}` (`bench/ingest.py`), summarised in the `## Ingest` section of
`<scale>.md`.

## Ingest, 2026-10-20, omarchy-hp: write-path fixes (`post-fixes` build)

The ingest benchmark below (2026-10-19/20) found three problems in the write path. A new
build, `(post-fixes)`, fixes them; the build before it was re-measured in the same session
as `(post-0009 rerun)` so that the comparison is same-time (the machine was quiet: load
~1, nothing else using the disk; the rerun matches the earlier post-0009 numbers within
~10% on tmpfs, while the disk was ~3x faster than during the earlier sessions, e.g. small
forecasts per-batch 9.4 s vs 27 s). Same datasets (seed 1), `bench/ingest.py`, rows in
`2026-10-19-omarchy-hp/ingest-{small,medium}.{md,json}`. tmpfs = `/dev/shm`, so no disk
syncs (CPU only). Competitor numbers are unchanged from the earlier sessions.

| measurement | post-0009 rerun | post-fixes |
| --- | ---: | ---: |
| medium forecasts bulk, 1M-row writes, tmpfs (wall s; rows/s) | 67.1 (0.37M) | **10.1** (2.48M) |
| medium forecasts bulk, 100k-row writes, tmpfs | 24.4 (1.03M) | **6.1** (4.08M) |
| the same on btrfs: 1M / 100k | 65.2 / 24.6 | **9.8 / 8.1** |
| medium forecasts per-batch, tmpfs: wall s; CPU per `write` (ms) | 32.9; 0.99 | **22.2; 0.57** |
| medium forecasts per-batch `write_many` x64, tmpfs | 30.0 | **11.7** |
| medium da_prices per-batch, tmpfs: wall s; ms per `write` | 7.6; 0.16 | 7.0; 0.14 |
| small forecasts / settlement / da_prices / backfill bulk 1M, tmpfs | 6.37 / 0.88 / 0.44 / 0.33 | 1.01 / 0.40 / 0.12 / 0.12 |
| log after load + flush + compact + gc, small forecasts `write_many` x64 | 122.7 MB (8.2 MB segments) | **0.44 MB** |
| ... small settlement / backfill / da_prices `write_many` x64 | 10.0 / 14.5 / 3.1 MB | 0.03 / 0.01 / 0.02 MB |
| ... small per-batch (compact+gc every 500): forecasts / settlement / da_prices | 1.19 / 1.73 / 0.65 MB | 0.61 / 0.03 / 0.03 MB |
| ... medium forecasts per-batch / bulk 100k | 4.65 / 2.64 MB | 4.65 / 2.64 MB (75 / 71 MB segments) |

Bulk medium forecasts is now about ClickHouse's bulk time (9.2 s) and 1.2-2x DuckDB's
(4.9 s, which includes its sort), from 13x before. On disk, durable per-batch writes are
still bound by one file + directory fsync per commit (small forecasts 2.69 → 2.35 ms per
write, of which 1.7 ms log fsync), so the per-batch disk numbers move little; grouped
commits (`write_many`) move a lot (small forecasts on btrfs 3.65 → 1.48 s).

What changed, per problem:

1. **Log not truncated after a `write_many` load** (finding 5 below). Cause: GC truncates
   only below the *second-newest* checkpoint (ADR 0007), and a load with fewer than
   `checkpoint_interval` entries and no GC in between ends with a single checkpoint (GC's
   own, of the head), so nothing was truncated and every entry, with its inline rows,
   stayed. Not the inline rows as such, not checkpoints carrying the memtable (the final
   flush empties it), and flush entries were not special. Fix: GC *re-anchors* — after
   checkpointing the head, if the entries after the second-newest checkpoint hold more
   bytes than that checkpoint, it commits an empty `gc` entry and checkpoints it too, so
   the old head becomes the fallback checkpoint and everything below it goes (ADR 0007
   amendment, FORMAT.md §3.6; no format change). Regression test
   `log_is_small_after_a_write_many_load_flush_and_gc` asserts log < 5% of segments. What
   remains after a load is two checkpoints of the state (catalog and segment list), e.g.
   4.65 MB for the 4 400 segments of medium forecasts per-batch.
2. **Quadratic in-batch duplicate check** (finding 4). The in-batch check walked a chain of
   every kept row of the same point; the group commit compared rows with every stored and
   earlier-batch version of their point the same way. Now: `Version::content_hash` (exactly
   the fields `content_identical` compares, f64 by bit pattern) — in-batch duplicates by
   sorting `(hash, position)` and comparing equal hashes only; stored versions and rows of
   earlier batches of a group through a hash index (`ContentIndex`). Semantics unchanged
   (property tests against a literal SPEC §4.3 implementation and the model).
3. **Ingest CPU**. Arrow input is no longer turned into one `WriteRow` (a `String` and a
   `BTreeMap<String, InputValue>`) per row: the binding builds a `WriteColumns` (series
   names dictionary-encoded, times as µs, values as typed vectors), and
   `Catalog::validate_columns` resolves series, tables and column indexes once per batch
   and copies typed values into the versions. Any row the fast path does not accept is
   validated by the unchanged row path, which decides the error, so codes, the SPEC §10.4
   check order and messages are identical (differential tests in Rust and Python). In a
   py-spy profile of small forecasts per-batch on tmpfs, functions of input conversion
   and validation are in 32% of the samples inside `write` for the old build and 6% for
   the new one (native stacks are sampled approximately; the earlier 46% figure used
   another attribution). Bulk writes also no longer regrow their per-partition vectors.

Not solved: 1M-row writes are still ~1.6x slower than 100k-row writes (10.1 vs 6.1 s on
tmpfs). It is no longer quadratic; a profile shows memory-bound work that grows with the
batch — sorting a million dedup keys, a heap-allocated value vector per `Version` (built
and freed per row), dropping the batch — rather than one hot spot. Per-batch writes on
disk remain fsync-bound (1 file + directory sync per commit, ADR 0009). Checkpoints still
grow with the number of segments and series, and the log after a load is two of them.
Finding 6 below (sparse compaction during the load) was not looked at.

## Ingest, 2026-10-19/20, omarchy-hp

**Why a separate ingest benchmark.** The query runs load each system its own way: Muninn
writes one commit per original batch so that it reproduces `system_time` exactly (medium
forecasts: 26 030 commits), while DuckDB and ClickHouse load everything in one statement.
The "ingest s" columns of the earlier reports (Muninn 1 940 s vs DuckDB 4.4 s on medium
forecasts) therefore compared a durable commit per batch with one bulk insert, and said
nothing about either engine's ingest. `bench/ingest.py` now loads every dataset in two
modes for every system:

* **per-batch**: one transaction/commit per original batch (the dataset's arrival order;
  da_prices 31k batches at medium, forecasts 26k of ~960 rows, settlement 40k of 96 rows,
  backfill 3.7k, sparse 3.2M one-row batches). Muninn: `write` per batch, as in the
  query runs (compact+gc every 500 writes). DuckDB: `BEGIN; INSERT; COMMIT` on a database
  file (one WAL fsync per commit). ClickHouse: one synchronous `INSERT` per batch;
  `clickhouse` with default settings does **not** fsync an insert before acknowledging
  it, `clickhouse-fsync` sets `fsync_after_insert=1, fsync_part_directory=1`, which is
  the durable equivalent of the other two. For sparse, every system loads the same
  first 10 000 batches (a full per-batch load is days for some systems); rates are for
  that prefix and are not extrapolated.
* **bulk**: as few writes as each system allows: DuckDB/ClickHouse exactly as in the
  query runs; Muninn `write` calls of 1M rows (`muninn`) or 100k rows (`muninn-100k`),
  whose system_time values then differ from the dataset's (irrelevant for timing).

Every number includes the system's finalisation (Muninn flush+compact+gc, DuckDB
CHECKPOINT, ClickHouse OPTIMIZE FINAL). The per-batch DuckDB table stays in arrival order
rather than the sorted layout the query runs use. fsync calls and the time spent in them
are counted by an LD_PRELOAD shim (`bench/fsyncstat.c`) in the benchmark process (so not
for the ClickHouse server). Same laptop as below (btrfs on NVMe), and the same runs on
tmpfs (`/dev/shm`) for Muninn and DuckDB to show the floor without disk syncs.
`tests/test_ingest.py` checks every load path against the oracle.

**Two Muninn builds.** `(pre-0009)`: the engine these reports were made with, one segment
file per touched partition plus a log entry per write, every file created with a file and
a directory fsync. `(post-0009)`: main after ADR 0009 (rows inline in the commit entry,
group commit, memtable, background flush), measured after rebasing. Two more post-0009
per-batch variants use the group commit: `muninn-wm64` submits 64 original batches per
`write_many` call (one durable commit per 64 batches; each batch is still its own
transaction with its own system_time, results identical to the reference), `muninn-8p`
runs 8 producer threads calling `write` (commit order and so seq/system_time are then
nondeterministic). **DuckDB and ClickHouse were not measured with grouped or concurrent
commits**, so these two rows show what Muninn's API allows, not a like-for-like comparison.

The disk was shared with other agents' work: load average 2-6 during the pre-0009 session
(late 19 Oct), 1.3-2.8 during the post-0009 session (early 20 Oct). At small scale the
competitors' per-batch runs were repeated in the post-0009 session, so those rows are
same-time comparisons; at medium the competitors' numbers are from the earlier, noisier
session (a commit fsync then cost ~3.5 ms for DuckDB, ~7 ms for a Muninn file + directory
fsync pair). Treat ratios within ~1.5x as noise.

Per-batch, local disk, medium (wall s incl. finalisation; rows/s):

| dataset | Muninn pre-0009 | Muninn post-0009 | post, `write_many` x64 | post, 8 producers | DuckDB | ClickHouse (no fsync) | ClickHouse fsync |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices (31k commits) | 494 (3.7k) | 223 (8.1k) | 6.8 (267k) | 45 (41k) | 188 (9.6k) | 75 (24k) | 767 (2.4k) |
| forecasts (26k) | 2 099 (12k) | 274 (91k) | 32 (777k) | 90 (276k) | 162 (154k) | 99 (254k) | 645 (39k) |
| settlement (40k) | 579 (6.6k) | 325 (12k) | 8.1 (473k) | 69 (56k) | 196 (20k) | 103 (37k) | 890 (4.3k) |
| backfill (3.7k) | 56 (49k) | 30 (92k) | 7.6 (364k) | 8.2 (338k) | 15 (179k) | 9.7 (286k) | 84 (33k) |
| sparse, first 10k | 65 | 40 | 3.1 | 9.6 | 45 | 21 | 189 |

Small scale, same session for all (wall s): da_prices Muninn post 39 / DuckDB 38 /
ClickHouse 17 / ClickHouse fsync 150; forecasts 27 / 11 / 8.3 / 57; settlement 70 / 55 /
23 / 209; backfill 3.4 / 4.0 / 1.5 / 7.0 (pre-0009 Muninn: 69, 188, 142, 3.1).

Bulk, medium (wall s; rows/s): Muninn post-0009 1M-row writes da 1.8 (1.0M) / forecasts
62 (0.40M) / settlement 3.9 (0.97M) / backfill 3.5 (0.79M) / sparse 8.7 (0.37M);
100k-row writes forecasts 25 (1.0M); pre-0009 1M-row writes 3.0 / 60 / 5.9 / 4.8 / 9.9.
DuckDB 0.40 / 4.9 / 0.68 / 0.46 / 0.86 (4-6M rows/s, including the sort); ClickHouse
0.71 / 9.2 / 1.6 / 1.1 / 3.1 (2.5M rows/s; fsync makes no difference in bulk).

Where the time goes (Muninn, inside `write`, per write, medium, local disk):

| dataset | build | ms/write | fsyncs/write | segment fsync ms | log fsync ms | rest ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| forecasts | pre-0009 | 76 | 23.9 | 64 | 6.1 | 6.3 |
| forecasts | post-0009 | 8.7 | 2.5 | 2.6* | 7.4 | ~0 |
| da_prices | pre-0009 | 15.6 | 4.0 | 7.3 | 7.5 | 0.8 |
| da_prices | post-0009 | 7.0 | 2.0 | 0 | 6.7 | 0.3 |

\* the counters are process-wide: fsyncs of a background flush that overlap a write are
attributed to it. On tmpfs the same writes cost 2.0 ms (pre) and 0.93 ms (post) for
forecasts, 0.23 / 0.15 ms for da_prices: **fsync is 89-98% of a per-batch write on this
disk**, before and after 0009. Sampling the in-memory writes (py-spy, small forecasts,
tmpfs) splits the CPU part: pre-0009 31% converting Arrow input into per-row maps
(`BTreeMap<String, InputValue>`) and validating, 20% dedup, 11% file creation, 8% segment
encoding, <1% log entry; post-0009 46% input conversion and validation, ~15% inline
segment encoding (incl. base64), ~5% commit and file writes, 2% dedup.

Findings:

1. **Per-batch, durable, one client: Muninn post-0009 is 1.0-2.5x slower than DuckDB and
   2-4x faster than ClickHouse with fsync** (small, same session: da_prices 39 vs 38 s,
   settlement 70 vs 55, forecasts 27 vs 11), and 2-4x slower than ClickHouse's default,
   which does not sync. Pre-0009 it was 2.6-3.3x (da_prices) to 13-16x (forecasts)
   slower than DuckDB: ~24 file + directory fsyncs per forecast write. Post-0009 a write is one log file
   creation (file + directory fsync, ~7 ms on this disk at the time); DuckDB pays one WAL
   fsync per commit.
2. **Without disk syncs Muninn's per-batch path is the fastest measured**: tmpfs medium
   da_prices 7.1 s vs DuckDB 48 s, settlement 15 vs 68, forecasts 31 vs 57 (post-0009).
3. **Grouping commits removes the fsync bound**: `write_many` x64 loads medium forecasts
   in 32 s (777k rows/s) and settlement in 8 s with every batch still its own
   transaction; 8 producer threads give 3-5x over one. No competitor was given the same
   advantage here (ClickHouse async inserts or batched DuckDB transactions would be the
   counterparts).
4. **Bulk: Muninn ~0.4-1.4M rows/s vs DuckDB 4-6M and ClickHouse 1-3M** (fixed in the
   `post-fixes` build, see above: 2.5-4M rows/s). 1M-row writes
   are *slower* than 100k-row writes on forecasts (62 s vs 25 s at medium, both builds):
   in a 1M-row write each point has hundreds of versions in the same batch, and
   `Version::content_identical` (in-batch duplicate check, quadratic per point) takes
   ~48% of the samples (pre-0009 profile; post-0009 shows the same 1M-vs-100k gap).
   Input conversion is most of the rest.
5. **Size after a post-0009 load includes un-truncated log** (fixed in `post-fixes`, see
   above): e.g. small forecasts via
   `write_many` 125 MB on disk for 8 MB of segments (`log_bytes` in the JSON), da_prices
   per-batch 2.3 vs 1.8 MB. The inline rows stay in log entries and checkpoints until later
   checkpoints let GC truncate them; the end-of-load flush+compact+gc does not get there.
   Bulk loads and pre-0009 loads end with logs of tens of KB to a few MB.
6. The sparse dataset at medium (150k series) spends ~19 s of its 10k-write prefix in the
   compact+gc every 500 writes (the per-write cost is 0.12-1.7 ms), on tmpfs too.

## 2026-10-19 (later), omarchy-hp: Muninn after fixing the three problems below

The Muninn column of `2026-10-19-omarchy-hp/{small,medium}.{md,json}` was re-run after the
fixes (`run.py --systems muninn --merge`, ADRs 0007 and 0008) and merged into the reports;
DuckDB and ClickHouse numbers are from the original run on the same machine. All systems
still return identical results for all 361 (dataset, query, instance) combinations at
small and at medium scale — at medium now including Muninn on the 150k-series sparse
dataset, which could not be loaded before.

Medium (ms, warm p50; before → after, DuckDB native for comparison):

| query | Muninn before | Muninn after | DuckDB native | ClickHouse |
| --- | ---: | ---: | ---: | ---: |
| `fc_asof_single` | 5.3 | 1.0 | 4.8 | 8.7 |
| `da_asof_published` | 1.3 | 0.22 | 1.8 | 4.2 |
| `fc_grid_365` | 2 285 | **417** | 1 109 | 10 654 |
| `fc_grid_365_all` | 5 208 | **1 205** | 5 063 | 33 415 |
| `da_grid_365` | 2 379 | **658** | 843 | 1 747 |
| `st_grid_365` | 93 | 23 | 40 | 123 |
| `da_asof_join_signals` | 197 | 127 | 202 | 922 |
| `fc_asof_join_lead24` | 190 | 81 | 73 | 333 |
| `fc_replay_6h` | 103 | **8.0** | 5.6 | 15 |
| `fc_lead_24h` | 1 030 | **20** | 9.2 | 48 |
| `st_latest_now` | 53 | 27 | 11 | 33 |
| `fc_latest_now` | 59 | 14 | 15 | 44 |
| `fc_history_day` | 8.5 | 6.9 | 4.1 | 6.5 |
| `st_history_month` | 11 | 4.2 | 2.5 | 5.0 |
| `sp_latest_now` (150k series) | not run | 112 | 64 | 253 |
| `sp_replay_day` | not run | 53 | 16 | 136 |

Goals: the grid queries now beat DuckDB (`fc_grid_365` 2.7x, `da_grid_365` 1.3x faster;
at small scale 3.0x and 1.16x); `fc_replay_6h` is 1.4x DuckDB, `fc_lead_24h` 2.2x
(1.9x at small). Still slower than DuckDB: the sparse replay/latest queries (many tiny
series), `st_latest_now`, the history queries (~1.7x) and `fc_asof_join_lead24` (1.1x).

Bytes per stored version, total on disk (segments + log), medium: forecasts 22.8 → 3.16
(log 4.3 MB for 74.5 MB of segments, was 476 MB), da_prices 34.5 → 3.88, settlement
22.6 → 3.70, backfill 8.3 → 6.29, sparse (medium) 43.0 (87 MB segments, 52 MB log: two
checkpoints and the creation entries of a 150k-series catalog). Segment bytes alone grew
slightly (forecasts 2.79 → 2.98 B/version) from smaller, series-aligned blocks and
per-arrival-period segments.

Ingest. The engine work per write no longer depends on history (dedup index): a 960-row
forecast write costs ~1 ms in memory at any history depth (was 8.5 → 50 ms), and the whole
small forecasts load takes 7 s on tmpfs. On this machine's btrfs NVMe the load is bound by
`fsync` (~12 file creations per forecast write, each with a file and a directory sync)
and the disk got ~2x slower between the runs, so the bench ingest numbers (medium
forecasts 2 421 → 1 940 s, da_prices 145 → 456 s, settlement 291 → 509 s, sparse: not
loadable → 12 s, 150k series created in one batch) mix the fix with that drift. A
same-time A/B of the small da_prices load against the original build: 132 s → 95–99 s
(writes 13.7 → 12.3 ms each, fsync-bound; compaction + GC 27 → 2.2 s).

What changed, per problem:

1. **Write cost vs history** — an in-memory index of each point's stored received-time
   range per (table, partition), brought up to date with the writer's current state
   before every use (other writers, compaction elsewhere, restarts), so stored versions are
   compared only for rows that could be duplicates (re-sends, out-of-order backfills).
2. **Metadata** — `create_series_batch` (engine, Python, CLI `series import`, `restore`),
   a catalog name index, size-triggered checkpoints, and log truncation by GC below the
   older of the two newest checkpoints (log format 2, ADR 0007). 5 000 series one by one:
   2.9 s / 17.8 MB of log → 2.5 s / 2.6 MB; as one batch: 6 ms.
3. **Reads** — compaction keeps one segment per knowledge-time period per partition (the
   time statistics were already there; full compaction had made them useless), blocks end
   at series boundaries, streaming latest/grid reductions, a lead-time filter pushed into
   the engine (`history(lead=...)`), reused decode buffers, and segment format 2 with a
   nibble-length integer encoding (ADR 0008). Engine-column decoding: 58 → 23 ns/row.

Not solved: sparse/many-series replay and latest; history queries; skipping older
segments for points already answered by newer ones (would cut grid/latest further);
results are built row by row and then converted to Arrow; ingest on fsync-heavy
filesystems (one file per touched partition per write).

## 2026-10-19, omarchy-hp (laptop: Ryzen 5 PRO 5650U, 12 threads, 15 GB, NVMe) — original run

Muninn 0.1.0 (engine via the Python binding, release build, local directory) vs DuckDB 1.5.6
(native table, and over Parquet) vs ClickHouse 26.7 (MergeTree), 6 threads each (Muninn's
engine threads are not configurable), with other builds on the same machine: absolute
numbers are noisy, ratios are what matter. Every system matched the reference
implementation on the tiny workload and edge queries, and all systems returned identical
results for all 361 (dataset, query, instance) combinations at small and medium scale
(Muninn's sparse dataset was loaded with coalesced batches at small, so it was compared
without `system_time` there; it was not run at medium, see below).

Medium headline (`medium.md`; ms, warm p50):

| query | rows | Muninn | DuckDB native | DuckDB Parquet | ClickHouse |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fc_asof_single` (one series-day, ~230-version chains) | 96 | 5.3 | 4.8 | 12 | 8.7 |
| `da_asof_published` | 57 | 1.3 | 1.8 | 6.5 | 4.2 |
| `fc_grid_365` (365 frontiers x 36 h, one series) | 52k | 2 285 | 1 109 | 1 367 | 10 654 |
| `fc_grid_365_all` (3 series) | 157k | 5 208 | 5 063 | 5 134 | 33 415 |
| `da_grid_365` (40 zones, published clock) | 1.86M | 2 379 | 843 | 880 | 1 747 |
| `da_asof_join_signals` (10k rows, per-row frontier) | 10k | 197 | 202 | 225 | 922 |
| `fc_asof_join_lead24` | 2.9k | 190 | 73 | 85 | 333 |
| `fc_replay_6h` | 17k | 103 | 5.6 | 22 | 15 |
| `fc_lead_24h` (lead-time slice, 30 days) | 8.6k | 1 030 | 9.2 | 39 | 48 |
| `st_latest_now` | 27k | 53 | 11 | 57 | 33 |
| `sp_latest_now` (150k series) | 157k | not run | 64 | 133 | 253 |

Ingest (medium, s): Muninn da 145 / forecasts 2 421 / settlement 291 / backfill 24;
DuckDB 0.3-4.4; ClickHouse 0.6-9.3.

Bytes per stored version (medium):

| dataset | Muninn total (segments only) | DuckDB native | DuckDB Parquet | ClickHouse |
| --- | ---: | ---: | ---: | ---: |
| forecasts | 22.8 (**2.79**) | 23.9 | 3.58 | 2.99 |
| da_prices | 34.5 (**3.29**) | 10.3 | 5.68 | 3.51 |
| settlement | 22.6 (**3.37**) | 27.8 | 5.19 | 3.47 |
| backfill | 8.3 (**5.88**) | 17.9 | 13.0 | 7.72 |
| sparse (small) | 838 (**25.3**) | 36.9 | 32.2 | 31.1 |

### Where Muninn stands

* **Its segment format is already the most compact** on every dataset (2.79 B/version on
  forecasts vs 2.99 for ClickHouse), before any vintage-delta coding. The on-disk *total* is
  dominated by the commit log (see problem 2).
* **Point reads and as-of joins are on par or better**: `da_asof_published` 1.3 ms (best),
  `da_asof_join_signals` 197 ms (≈ DuckDB, 4.7x ClickHouse).
* **The single-pass grid does not win yet**: 2x slower than DuckDB on `fc_grid_365` and
  2.8x on `da_grid_365`; it beats ClickHouse only on forecasts.
* **Replay, history and the lead-time slice lose by 4-110x** (see problem 3).
* **Ingest is 100-500x slower** than both competitors, and the sparse dataset (150k series)
  cannot be loaded at medium scale at all.

### The three biggest Muninn problems (with evidence)

1. **Write cost grows with versions per point (dedup materialises history).**
   `Engine::write_once` → `load_points` scans and *materialises every stored version of
   every touched point* into a `HashMap<(SeriesId, Timestamp), Vec<Version>>` to apply
   §4.3. A 960-row forecast run costs 22 ms on an empty table and 70 ms once points hold
   ~100+ versions (54 ms in memory, so not I/O); py-spy: the scan/materialise/hash/decode
   path dominates. Medium forecasts: 26k writes, 2 421 s. A content-hash index (or a
   per-point fingerprint of the latest versions) would make dedup O(batch).
2. **Metadata grows quadratically.** Every `create_series` is its own commit, and every
   100th commit writes a checkpoint holding the whole catalog and segment list; old
   checkpoints are never pruned (not even by `gc`). 6 000 series: 10 s and 49 MB of log,
   each 1 000 slower than the last (0.2 s → 3.8 s); extrapolated to 150k series ≈ 1.7 h and
   ~30 GB. Small forecasts: 184 MB of checkpoints (29 of them, up to 12.8 MB each, listing
   30 640 segments) for 7.6 MB of data; medium forecasts: 476 MB of log for 66 MB of
   segments. Each write also creates one segment per touched partition (~11 for a 10-day
   forecast run) and ~2 files. Needed: a bulk catalog API, checkpoint truncation, and
   log GC.
3. **Reads are decode-bound and do not prune by t_clock.** py-spy on `fc_grid_365` and
   `fc_replay_6h`: 33-42% of samples in `muninn_format::codec` varint decoding
   (`uvarint`), plus `decode_engine_block` and ZSTD, i.e. whole segments are decoded per
   read. Replay (filter on t_clock across series) scans every segment: 103 ms vs 5.6 ms
   for DuckDB, which prunes row groups by min/max. The lead-time slice has no pushdown
   of value-column predicates, so the binding materialises 30 days of full history
   (~2M rows) into Arrow before the client filters it: 1 030 ms vs 9 ms. Faster integer
   codecs (bit-packed / FastLanes instead of varints), per-segment t_clock ranges for
   pruning, and late materialisation are the obvious next steps; the grid's single pass
   only pays off once decoding is cheap.

### Where a purpose-built engine could still win 10x

* **Frontier grid on many-version data.** DuckDB needs 1.1 s for `fc_grid_365` (sorts or
  hashes all ~8.4M versions). Muninn already does one merge pass but spends it decoding;
  at ~3 B/version that is ~25 MB of data, so tens of ms remains a realistic target.
* **As-of join.** Muninn is at DuckDB's level (197 ms for 10k lookups); with point lookups
  that don't decode whole segments, single-digit ms is plausible.
* **Compression.** Segments are already the smallest (2.79 B/version on forecasts);
  vintage-delta coding (notes/10) has further room (< 0.5 B/version would be ~6x ClickHouse).
* **Not** point reads: 1-8 ms is mostly client and planning overhead everywhere.

### Caveats

* Cold = system caches dropped / database reopened, OS page cache warm (no root); local NVMe.
* p99 over few samples is close to the maximum; heavy queries ran 3-9 times.
* Muninn load: one write per original batch with the writer clock set to its system_time,
  so system_time/seq are reproduced exactly (verified after load); compact+gc every 500
  writes. The sparse dataset has one message per batch (434k at small), so at small it is
  written in coalesced 200k-row batches (seq and knowledge order preserved, system_time
  values not); at medium it is skipped (problem 2).
* During development one ClickHouse correctness test (settlement) failed once and could
  not be reproduced in 10 later runs.
* DuckDB needed two correctness workarounds for `-0.0` (see
  `competitors/duckdb/README.md`); before them the medium run flagged a disagreement.
