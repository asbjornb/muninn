# Benchmark results

One directory per run date and host; `<scale>.md` is the readable report, `<scale>.json`
everything measured (per-instance result hashes included). Regenerate with
`uv run --project bench python bench/run.py --scale <scale>`.

## 2026-10-19, omarchy-hp (laptop: Ryzen 5 PRO 5650U, 12 threads, 15 GB, NVMe)

DuckDB 1.5.6 (native table, and over Parquet) vs ClickHouse 26.7 (MergeTree), 6 threads
each, while another agent compiled Rust on the same machine: absolute numbers are noisy,
ratios are what matter. Every system matched the reference implementation on the tiny
workload and edge queries, and all systems returned identical results for all 361
(dataset, query, instance) combinations at small and at medium scale.

Medium headline (`medium.md`; ms, warm p50):

| query | rows | DuckDB native | DuckDB Parquet | ClickHouse |
| --- | ---: | ---: | ---: | ---: |
| `fc_asof_single` (one series-day, ~230-version chains) | 96 | 6 | 13 | 8 |
| `fc_grid_365` (365 frontiers x 36 h, one series) | 52k | 1 120 | 1 473 | 10 830 |
| `fc_grid_365_all` (3 series) | 157k | 6 300 | 5 535 | 33 291 |
| `da_grid_365` (40 zones, published clock) | 1.86M | 958 | 984 | 1 716 |
| `da_asof_join_signals` (10k rows, per-row frontier) | 10k | 227 | 240 | 926 |
| `fc_lead_24h` (lead-time slice, 30 days) | 8.6k | 11 | 40 | 47 |
| `sp_latest_now` (150k series) | 157k | 77 | 135 | 254 |

Bytes per stored version: forecasts 23.9 (DuckDB native) / 3.6 (DuckDB Parquet, zstd) /
3.0 (ClickHouse, Delta+ZSTD); sparse ~31-37 everywhere (tiny series, per-row overhead).

### Where a purpose-built engine could win 10x (from these measurements)

* **Frontier grid on many-version data.** `fc_grid_365` returns 52k rows but both systems
  have to sort or hash all ~8.4M versions of the series in range and join them against the
  (frontier, point) pairs: 1.1 s in DuckDB, 10.8 s in ClickHouse. With versions stored sorted
  by (series, value_time, t_clock, seq), one merge pass resolves every frontier per version
  chain; reading ~8.4M versions at ~3 B each is ~25 MB of decode, so tens of ms is a
  realistic target: 20-50x DuckDB, more against ClickHouse.
* **As-of join with per-row frontiers.** 10k lookups take 230 ms (DuckDB) / 930 ms
  (ClickHouse) because both build the ASOF structure over every version of the referenced
  series per query. Point lookups into a (series, value_time)-ordered layout should be
  single-digit ms: 30-100x.
* **Compression of forecast vintages.** 3.0-3.6 B/version with general-purpose codecs (24
  in DuckDB's native format, whose bit-packing has no delta coding for µs timestamps).
  Consecutive versions of a point differ by small, smooth amounts and their timestamps are
  near-periodic (hourly issue, value_time implied by position), so vintage-delta coding
  (notes/10) has room: < 0.5 B/version would be ~6x ClickHouse and ~50x DuckDB native.
* **Not** point reads: 2-8 ms is mostly client and planning overhead in both systems; an
  embedded engine wins there by removing overhead, not by an order of magnitude of work.
* **Replay across many series** (`sp_replay_day`, 19-136 ms) suffers from the
  series-major sort every system here chose; a commit-ordered log/index is the natural
  fix, but absolute times are small.

### Caveats

* Cold = system caches dropped, OS page cache warm (no root); disks are local NVMe.
* p99 over few samples is close to the maximum; heavy queries ran 3-9 times.
* During development one ClickHouse correctness test (settlement) failed once and could
  not be reproduced in 10 later runs; the cross-check at small/medium found no ClickHouse
  disagreement.
* DuckDB needed two correctness workarounds for `-0.0` (see
  `competitors/duckdb/README.md`); before them the medium run flagged a disagreement.
