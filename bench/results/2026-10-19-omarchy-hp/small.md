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

| dataset | muninn ingest s | muninn size (B/version) | duckdb-native ingest s | duckdb-native size (B/version) | duckdb-parquet ingest s | duckdb-parquet size (B/version) | clickhouse ingest s | clickhouse size (B/version) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | 33.1 | 14.6 MB (33.82) | 0.1 | 4.8 MB (11.02) | 0.1 | 2.2 MB (5.2) | 0.2 | 1.5 MB (3.55) |
| forecasts | 252.7 | 56.6 MB (21.65) | 0.6 | 67.5 MB (25.82) | 0.5 | 9.5 MB (3.63) | 1.1 | 8.0 MB (3.06) |
| settlement | 56.6 | 20.3 MB (22.21) | 0.4 | 25.5 MB (27.87) | 0.3 | 4.8 MB (5.26) | 0.3 | 3.2 MB (3.48) |
| sparse | 351.1 | 347.1 MB (838.28) | 0.2 | 15.3 MB (36.86) | 0.2 | 13.3 MB (32.19) | 0.4 | 12.9 MB (31.07) |
| backfill | 3.2 | 3.7 MB (8.32) | 0.2 | 8.3 MB (18.83) | 0.1 | 5.7 MB (13.07) | 0.2 | 3.4 MB (7.76) |

## Query latency (ms): p50 / p99 warm, cold

| query | rows | muninn | duckdb-native | duckdb-parquet | clickhouse |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fc_asof_single` | 96 | 4.85 / 5.48, 5.59 | 4.75 / 8.04, 7.52 | 8.80 / 11, 12 | 8.42 / 9.32, 10 |
| `da_asof_published` | 57 | 1.28 / 2.36, 2.33 | 1.71 / 2.68, 5.22 | 6.02 / 8.23, 10 | 4.20 / 5.51, 6.19 |
| `st_asof_system` | 96 | 3.27 / 5.34, 5.14 | 2.23 / 3.93, 5.42 | 7.57 / 9.39, 11 | 4.45 / 6.67, 6.46 |
| `sp_asof_single` | 6 | 6.55 / 34, 4.48 | 1.82 / 3.74, 5.42 | 12 / 15, 13 | 4.76 / 5.71, 6.54 |
| `bf_asof_single` | 24 | 1.93 / 3.60, 1.34 | 1.76 / 3.00, 4.70 | 7.77 / 9.65, 9.79 | 4.11 / 5.38, 5.70 |
| `fc_grid_365` | 8,496 | 325 / 358, 332 | 181 / 188, 222 | 201 / 207, 229 | 620 / 663, 656 |
| `fc_grid_365_all` | 16,992 | 525 / 529, 563 | 409 / 431, 458 | 440 / 445, 473 | 1345 / 1438, 1638 |
| `da_grid_365` | 658,080 | 736 / 750, 763 | 277 / 284, 337 | 289 / 297, 315 | 608 / 642, 604 |
| `st_grid_365` | 37,795 | 98 / 99, 73 | 39 / 44, 55 | 44 / 47, 54 | 126 / 132, 123 |
| `bf_grid_365` | 17,156 | 12 / 13, 12 | 12 / 15, 18 | 18 / 20, 23 | 19 / 30, 20 |
| `fc_history_day` | 21,360 | 8.81 / 9.62, 10 | 3.63 / 6.71, 8.82 | 8.22 / 13, 13 | 6.59 / 12, 9.66 |
| `st_history_month` | 8,198 | 9.34 / 15, 14 | 2.50 / 5.30, 7.10 | 7.87 / 12, 13 | 4.85 / 6.36, 6.08 |
| `fc_replay_6h` | 11,376 | 74 / 76, 77 | 4.21 / 7.82, 10 | 14 / 15, 17 | 14 / 18, 15 |
| `sp_replay_day` | 1,196 | 20 / 24, 23 | 3.96 / 5.00, 9.79 | 16 / 19, 19 | 30 / 35, 30 |
| `fc_lead_24h` | 5,668 | 698 / 727, 732 | 6.08 / 9.35, 17 | 26 / 28, 30 | 30 / 33, 31 |
| `st_latest_now` | 6,716 | 13 / 13, 13 | 4.54 / 5.29, 13 | 18 / 19, 22 | 12 / 13, 14 |
| `sp_latest_now` | 20,754 | 20 / 21, 20 | 11 / 12, 19 | 24 / 25, 27 | 48 / 51, 53 |
| `fc_latest_now` | 1,912 | 43 / 44, 44 | 14 / 14, 18 | 20 / 21, 22 | 21 / 22, 26 |
| `da_asof_join_signals` | 10,000 | 63 / 66, 66 | 53 / 55, 65 | 59 / 61, 70 | 248 / 284, 277 |
| `fc_asof_join_lead24` | 2,880 | 191 / 197, 181 | 73 / 79, 90 | 82 / 89, 89 | 328 / 389, 355 |

## Agreement

All systems returned identical results for all 361 (dataset, query, instance) combinations.

Compared on every field except `system_time` (a system loaded them with coalesced batches, see its notes): sparse.

## Systems

- **muninn** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable.
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
