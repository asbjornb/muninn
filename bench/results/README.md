# Benchmark results

One directory per run date and host; `<scale>.md` is the readable report, `<scale>.json`
everything measured (per-instance result hashes included). Regenerate with
`uv run --project bench python bench/run.py --scale <scale>`.

## 2026-10-19, omarchy-hp (laptop: Ryzen 5 PRO 5650U, 12 threads, 15 GB, NVMe)

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
