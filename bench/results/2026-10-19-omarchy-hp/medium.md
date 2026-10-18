# Muninn benchmark — medium scale, seed 1

2026-10-19 on `omarchy-hp`: AMD Ryzen 5 PRO 5650U with Radeon Graphics, 12 logical CPUs, 14.9 GB RAM, Linux-7.2.3-arch1-3-x86_64-with-glibc2.44. Systems limited to 6 threads and 4096 MB. Latencies in ms, end to end through the Python client to a canonical Arrow table. p50/p99 over warm samples (few samples ⇒ p99 ≈ max).

## Datasets

| dataset | series | versions | points | versions/point (mean / p99 / max) | tombstones | null published | generator Parquet |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | 40 | 1,808,140 | 1,754,880 | 1.03 / 2 / 4 | 61 | 122,438 | 6.4 MB |
| forecasts | 3 | 24,975,472 | 107,988 | 231.28 / 240 / 241 | 112 | 8,322,352 | 83.3 MB |
| settlement | 40 | 3,840,000 | 1,401,600 | 2.74 / 3 / 3 | 707 | 663,936 | 15.3 MB |
| sparse | 150,000 | 3,246,139 | 1,793,926 | 1.81 / 6 / 21 | 80,052 | 64,760 | 78.1 MB |
| backfill | 60 | 2,760,303 | 2,629,440 | 1.05 / 2 / 2 | 0 | 1,498,817 | 31.2 MB |

## Ingest and size on disk

| dataset | duckdb-native ingest s | duckdb-native size (B/version) | duckdb-parquet ingest s | duckdb-parquet size (B/version) | clickhouse ingest s | clickhouse size (B/version) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | 0.4 | 17.8 MB (10.3) | 0.3 | 9.8 MB (5.68) | 0.6 | 6.1 MB (3.51) |
| forecasts | 5.2 | 568.0 MB (23.85) | 4.4 | 85.3 MB (3.58) | 9.5 | 71.2 MB (2.99) |
| settlement | 0.8 | 101.8 MB (27.79) | 0.6 | 19.0 MB (5.19) | 1.6 | 12.7 MB (3.47) |
| sparse | 1.0 | 113.0 MB (36.51) | 1.0 | 101.5 MB (32.79) | 3.2 | 97.1 MB (31.36) |
| backfill | 0.4 | 47.0 MB (17.86) | 0.4 | 34.2 MB (13.0) | 1.1 | 20.3 MB (7.72) |

## Query latency (ms): p50 / p99 warm, cold

| query | rows | duckdb-native | duckdb-parquet | clickhouse |
| --- | ---: | ---: | ---: | ---: |
| `fc_asof_single` | 96 | 6.08 / 9.75, 11 | 13 / 19, 18 | 8.47 / 9.69, 12 |
| `da_asof_published` | 57 | 2.60 / 5.33, 42 | 7.08 / 9.42, 9.89 | 4.20 / 5.50, 6.87 |
| `st_asof_system` | 96 | 2.62 / 5.84, 7.76 | 7.94 / 9.63, 9.41 | 4.43 / 5.51, 6.47 |
| `sp_asof_single` | 6 | 2.05 / 5.27, 5.78 | 13 / 15, 16 | 4.89 / 7.17, 6.76 |
| `bf_asof_single` | 24 | 1.98 / 4.45, 5.17 | 7.99 / 10, 12 | 4.11 / 4.98, 6.03 |
| `fc_grid_365` | 52,416 | 1120 / 1517, 1635 | 1473 / 1514, 1547 | 10830 / 10871, 10367 |
| `fc_grid_365_all` | 157,248 | 6300 / 8123, 8441 | 5535 / 5640, 5587 | 33291 / 33329, 33722 |
| `da_grid_365` | 1,863,360 | 958 / 959, 1119 | 984 / 990, 1027 | 1716 / 1822, 1652 |
| `st_grid_365` | 37,795 | 47 / 49, 65 | 45 / 50, 56 | 125 / 134, 121 |
| `bf_grid_365` | 17,156 | 15 / 17, 21 | 20 / 22, 25 | 19 / 32, 22 |
| `fc_history_day` | 22,837 | 8.57 / 13, 11 | 11 / 17, 21 | 7.14 / 9.77, 12 |
| `st_history_month` | 8,198 | 2.81 / 7.40, 6.79 | 8.46 / 12, 13 | 4.99 / 6.10, 5.89 |
| `fc_replay_6h` | 16,848 | 6.65 / 57, 33 | 23 / 26, 27 | 15 / 52, 48 |
| `sp_replay_day` | 8,955 | 19 / 22, 49 | 84 / 90, 88 | 136 / 140, 130 |
| `fc_lead_24h` | 8,559 | 11 / 32, 32 | 40 / 42, 45 | 47 / 49, 49 |
| `st_latest_now` | 26,868 | 12 / 14, 36 | 57 / 60, 64 | 33 / 37, 35 |
| `sp_latest_now` | 156,891 | 77 / 87, 122 | 135 / 139, 150 | 254 / 266, 272 |
| `fc_latest_now` | 2,868 | 40 / 74, 34 | 27 / 28, 30 | 44 / 45, 48 |
| `da_asof_join_signals` | 10,000 | 227 / 245, 243 | 240 / 251, 257 | 926 / 972, 871 |
| `fc_asof_join_lead24` | 2,880 | 96 / 325, 143 | 85 / 93, 104 | 363 / 403, 411 |

## Agreement

All systems returned identical results for all 361 (dataset, query, instance) combinations.

## Systems

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
