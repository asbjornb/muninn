# Muninn benchmark — medium scale, seed 1

2026-10-19 on `omarchy-hp`: AMD Ryzen 5 PRO 5650U with Radeon Graphics, 12 logical CPUs, 14.9 GB RAM, Linux-7.2.3-arch1-3-x86_64-with-glibc2.44. Systems limited to 6 threads and 4096 MB. Latencies in ms, end to end through the Python client to a canonical Arrow table. p50/p99 over warm samples (few samples ⇒ p99 ≈ max).

## Datasets

| dataset | series | versions | points | versions/point (mean / p99 / max) | tombstones | null published | generator Parquet |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | 40 | 1,808,140 | 1,754,880 | 1.03 / 2 / 4 | 61 | 122,438 | 6.4 MB |
| forecasts | 3 | 24,975,472 | 107,988 | 231.28 / 240 / 241 | 112 | 8,322,352 | 83.3 MB |
| settlement | 40 | 3,840,000 | 1,401,600 | 2.74 / 3 / 3 | 707 | 663,936 | 15.3 MB |
| sparse | 150,000 | 3,246,139 | 1,793,926 | 1.81 / 6 / 21 | 80,052 | 64,760 | 78.7 MB |
| backfill | 60 | 2,760,303 | 2,629,440 | 1.05 / 2 / 2 | 0 | 1,498,817 | 31.2 MB |

## Ingest and size on disk

| dataset | muninn ingest s | muninn size (B/version) | duckdb-native ingest s | duckdb-native size (B/version) | duckdb-parquet ingest s | duckdb-parquet size (B/version) | clickhouse ingest s | clickhouse size (B/version) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | 144.7 | 59.4 MB (34.45) | 0.3 | 17.8 MB (10.3) | 0.3 | 9.8 MB (5.68) | 0.6 | 6.1 MB (3.51) |
| forecasts | 2420.6 | 543.1 MB (22.8) | 4.4 | 568.0 MB (23.85) | 4.3 | 85.3 MB (3.58) | 9.3 | 71.2 MB (2.99) |
| settlement | 291.0 | 82.6 MB (22.56) | 0.7 | 101.8 MB (27.79) | 0.6 | 19.0 MB (5.19) | 1.6 | 12.7 MB (3.47) |
| sparse | - | - | 0.9 | 113.0 MB (36.51) | 1.0 | 102.1 MB (32.97) | 3.1 | 97.7 MB (31.57) |
| backfill | 24.0 | 21.9 MB (8.3) | 0.4 | 47.0 MB (17.86) | 0.5 | 34.2 MB (13.0) | 1.1 | 20.3 MB (7.72) |

## Query latency (ms): p50 / p99 warm, cold

| query | rows | muninn | duckdb-native | duckdb-parquet | clickhouse |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fc_asof_single` | 96 | 5.27 / 6.74, 6.76 | 4.79 / 8.89, 9.64 | 12 / 15, 16 | 8.66 / 10, 11 |
| `da_asof_published` | 57 | 1.30 / 2.38, 1.52 | 1.77 / 4.31, 6.39 | 6.48 / 8.89, 9.96 | 4.17 / 5.07, 6.36 |
| `st_asof_system` | 96 | 3.11 / 5.05, 3.27 | 2.15 / 4.64, 7.06 | 8.03 / 11, 13 | 4.40 / 5.49, 6.24 |
| `sp_asof_single` | 6 | - | 1.70 / 4.61, 6.76 | 13 / 15, 16 | 4.84 / 6.58, 6.67 |
| `bf_asof_single` | 24 | 2.41 / 3.26, 3.50 | 1.78 / 3.74, 6.36 | 8.07 / 11, 13 | 4.15 / 6.53, 5.84 |
| `fc_grid_365` | 52,416 | 2285 / 2446, 2575 | 1109 / 1174, 1252 | 1367 / 1416, 1444 | 10654 / 10963, 10292 |
| `fc_grid_365_all` | 157,248 | 5208 / 5274, 5719 | 5063 / 5214, 4979 | 5134 / 5206, 5128 | 33415 / 33697, 33171 |
| `da_grid_365` | 1,863,360 | 2379 / 2422, 2489 | 843 / 853, 916 | 880 / 885, 935 | 1747 / 1770, 1571 |
| `st_grid_365` | 37,795 | 93 / 104, 127 | 40 / 42, 53 | 45 / 52, 86 | 123 / 134, 125 |
| `bf_grid_365` | 17,156 | 15 / 15, 15 | 12 / 14, 19 | 19 / 21, 26 | 19 / 32, 22 |
| `fc_history_day` | 22,837 | 8.51 / 13, 8.92 | 4.08 / 7.66, 11 | 11 / 15, 18 | 6.54 / 17, 12 |
| `st_history_month` | 8,198 | 11 / 15, 13 | 2.47 / 5.60, 7.55 | 8.80 / 13, 15 | 4.99 / 6.15, 5.98 |
| `fc_replay_6h` | 16,848 | 103 / 123, 101 | 5.58 / 13, 16 | 22 / 23, 27 | 15 / 50, 51 |
| `sp_replay_day` | 8,955 | - | 16 / 19, 47 | 85 / 89, 87 | 136 / 140, 130 |
| `fc_lead_24h` | 8,559 | 1030 / 1064, 1285 | 9.15 / 25, 28 | 39 / 41, 43 | 48 / 51, 48 |
| `st_latest_now` | 26,868 | 53 / 55, 53 | 11 / 12, 34 | 57 / 58, 63 | 33 / 35, 34 |
| `sp_latest_now` | 156,891 | - | 64 / 70, 115 | 133 / 139, 152 | 253 / 269, 271 |
| `fc_latest_now` | 2,868 | 59 / 60, 84 | 15 / 18, 23 | 26 / 30, 35 | 44 / 45, 47 |
| `da_asof_join_signals` | 10,000 | 197 / 204, 202 | 202 / 209, 226 | 225 / 238, 246 | 922 / 980, 853 |
| `fc_asof_join_lead24` | 2,880 | 190 / 192, 198 | 73 / 82, 107 | 85 / 88, 102 | 333 / 404, 423 |

## Agreement

All systems returned identical results for all 361 (dataset, query, instance) combinations.

## Systems

- **muninn** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable.
  - sparse: not run (150,000 series > 50,000: create_series is quadratic in the catalog size (see adapter))
- **duckdb-native** (1.5.6): DuckDB 1.5.6, threads=6, memory_limit=4096MB, layout=native, latest=argmax, grid=asof. Cold = fresh connection (DuckDB buffer pool and Parquet metadata empty); the OS page cache is not dropped (no root).
- **duckdb-parquet** (1.5.6): DuckDB 1.5.6, threads=6, memory_limit=4096MB, layout=parquet, latest=argmax, grid=asof. Cold = fresh connection (DuckDB buffer pool and Parquet metadata empty); the OS page cache is not dropped (no root).
- **clickhouse** (26.7.34.3): ClickHouse server (official static binary), max_threads=6, max_memory_usage=4096MB, MergeTree, query cache off. Cold = SYSTEM DROP MARK/UNCOMPRESSED/MMAP/QUERY CACHE (OS page cache not dropped).

## What each query stresses

- `fc_asof_single` (forecasts, latest): point read: one series, one value day, one frontier (10:00 the day before) over ~240-version chains
- `da_asof_published` (da_prices, latest): point read under the published clock (coalesce(published, received)), with corrections, published>received and null published
- `st_asof_system` (settlement, latest): system frontier binding before the knowledge frontier: 'what did the DB say 3 days after delivery' with D+5/M+3 already received later
- `sp_asof_single` (sparse, latest): tiny series among 100k+: catalog lookup and per-series segment overhead dominate
- `bf_asof_single` (backfill, latest): point read in bulk-loaded history where system_time is useless as a frontier; received is synthetic
- `fc_grid_365` (forecasts, grid): the quant's training-set query: 365 daily 10:00 decision times x next 36h, one series
- `fc_grid_365_all` (forecasts, grid): frontier grid over every forecast series (many providers)
- `da_grid_365` (da_prices, grid): grid under the published clock across all zones; decision at 12:00, just after the auction
- `st_grid_365` (settlement, grid): grid over correction chains looking back: latest actuals for the previous 36h at each 10:00
- `bf_grid_365` (backfill, grid): grid over the last year of a 5-year backfill (bulk-loaded, one system_time)
- `fc_history_day` (forecasts, history): all versions of one series-day (~23k rows at 240 versions/point)
- `st_history_month` (settlement, history): correction chains of one series-month
- `fc_replay_6h` (forecasts, replay): arrival-order scan across all series: (F, F+6h] on t_received; layout sorted by series hurts
- `sp_replay_day` (sparse, replay): arrival-order scan over 100k+ tiny series, published clock
- `fc_lead_24h` (forecasts, history): lead-time slice: every forecast made 24h ahead (value_time - issue_time in [24h, 25h)) over 30 days, all series
- `st_latest_now` (settlement, latest): current truth (unbounded frontier) for the last 7 days, all series
- `sp_latest_now` (sparse, latest): current truth over every sparse series, value range [now-30d, now+30d)
- `fc_latest_now` (forecasts, latest): current truth: newest forecast for the next 10 days, all series
- `da_asof_join_signals` (da_prices, asof_join): as-of join with a per-row frontier: 10k trading signals, each looking up one delivery hour 6-48h ahead
- `fc_asof_join_lead24` (forecasts, asof_join): lead-time slice via per-row frontier: every quarter-hour of 30 days, frontier = value_time - 24h
