# Muninn benchmark — small scale, seed 1

2026-10-19 on `omarchy-hp`: AMD Ryzen 5 PRO 5650U with Radeon Graphics, 12 logical CPUs, 14.9 GB RAM, Linux-7.2.3-arch1-3-x86_64-with-glibc2.44. Systems limited to 6 threads and 4096 MB. Latencies in ms, end to end through the Python client to a canonical Arrow table. p50/p99 over warm samples (few samples ⇒ p99 ≈ max).

## Datasets

| dataset | series | versions | points | versions/point (mean / p99 / max) | tombstones | null published | generator Parquet |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | 20 | 453,060 | 440,160 | 1.03 / 2 / 3 | 14 | 29,234 | 1.8 MB |
| forecasts | 2 | 2,741,768 | 13,432 | 204.12 / 240 / 241 | 8 | 8 | 9.1 MB |
| settlement | 10 | 960,000 | 350,400 | 2.74 / 3 / 3 | 190 | 164,544 | 4.0 MB |
| sparse | 20,000 | 434,191 | 239,384 | 1.81 / 6 / 16 | 10,761 | 8,704 | 11.9 MB |
| backfill | 10 | 460,135 | 438,240 | 1.05 / 2 / 2 | 0 | 249,589 | 5.7 MB |

## Ingest and size on disk

| dataset | duckdb-native ingest s | duckdb-native size (B/version) | duckdb-parquet ingest s | duckdb-parquet size (B/version) | clickhouse ingest s | clickhouse size (B/version) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | 0.1 | 4.8 MB (11.02) | 0.1 | 2.2 MB (5.2) | 0.1 | 1.5 MB (3.55) |
| forecasts | 0.6 | 67.5 MB (25.82) | 0.5 | 9.5 MB (3.63) | 1.1 | 8.0 MB (3.06) |
| settlement | 0.3 | 25.5 MB (27.87) | 0.3 | 4.8 MB (5.26) | 0.5 | 3.2 MB (3.48) |
| sparse | 0.2 | 15.3 MB (36.86) | 0.2 | 13.3 MB (32.11) | 0.6 | 12.8 MB (30.98) |
| backfill | 0.2 | 8.3 MB (18.83) | 0.1 | 5.7 MB (13.07) | 0.2 | 3.4 MB (7.76) |

## Query latency (ms): p50 / p99 warm, cold

| query | rows | duckdb-native | duckdb-parquet | clickhouse |
| --- | ---: | ---: | ---: | ---: |
| `fc_asof_single` | 96 | 4.82 / 7.15, 8.94 | 8.49 / 12, 12 | 8.50 / 9.32, 10 |
| `da_asof_published` | 57 | 1.89 / 3.48, 34 | 6.09 / 7.87, 10 | 4.15 / 5.49, 7.05 |
| `st_asof_system` | 96 | 2.31 / 4.62, 5.68 | 7.69 / 11, 9.99 | 7.98 / 17, 8.26 |
| `sp_asof_single` | 6 | 1.82 / 5.08, 5.41 | 13 / 16, 13 | 8.40 / 20, 9.55 |
| `bf_asof_single` | 24 | 1.84 / 3.17, 5.30 | 7.96 / 17, 8.33 | 5.31 / 7.46, 7.96 |
| `fc_grid_365` | 8,496 | 181 / 186, 225 | 202 / 206, 232 | 631 / 663, 649 |
| `fc_grid_365_all` | 16,992 | 410 / 421, 446 | 431 / 449, 462 | 1457 / 2006, 1737 |
| `da_grid_365` | 658,080 | 271 / 282, 303 | 282 / 283, 307 | 624 / 631, 628 |
| `st_grid_365` | 37,795 | 38 / 41, 53 | 44 / 47, 54 | 221 / 288, 245 |
| `bf_grid_365` | 17,156 | 13 / 15, 19 | 18 / 21, 24 | 22 / 40, 25 |
| `fc_history_day` | 21,360 | 3.82 / 6.54, 8.96 | 8.39 / 13, 14 | 13 / 17, 18 |
| `st_history_month` | 8,198 | 2.48 / 5.48, 6.65 | 7.90 / 11, 13 | 9.41 / 14, 14 |
| `fc_replay_6h` | 11,376 | 4.33 / 8.52, 10 | 14 / 15, 18 | 22 / 30, 26 |
| `sp_replay_day` | 1,196 | 4.00 / 5.27, 12 | 16 / 20, 21 | 34 / 49, 105 |
| `fc_lead_24h` | 5,668 | 6.62 / 9.47, 17 | 24 / 29, 29 | 45 / 54, 45 |
| `st_latest_now` | 6,716 | 4.59 / 5.19, 12 | 19 / 21, 22 | 21 / 38, 39 |
| `sp_latest_now` | 20,754 | 11 / 12, 19 | 23 / 25, 28 | 51 / 54, 57 |
| `fc_latest_now` | 1,912 | 14 / 15, 18 | 20 / 22, 23 | 37 / 43, 35 |
| `da_asof_join_signals` | 10,000 | 52 / 60, 62 | 59 / 65, 70 | 253 / 285, 247 |
| `fc_asof_join_lead24` | 2,880 | 71 / 79, 88 | 82 / 85, 88 | 479 / 556, 539 |

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
