# Muninn benchmark — small scale, seed 1

2026-10-19 on `omarchy-hp`: AMD Ryzen 5 PRO 5650U with Radeon Graphics, 12 logical CPUs, 14.9 GB RAM, Linux-7.2.3-arch1-3-x86_64-with-glibc2.44. Systems limited to 6 threads and 4096 MB. Latencies in ms, end to end through the Python client to a canonical Arrow table. p50/p99 over warm samples (few samples ⇒ p99 ≈ max).

**muninn** was re-run on 2026-10-19 on `omarchy-hp` (datasets: da_prices, forecasts, settlement, sparse, backfill) and merged into this report; the other systems' numbers are from the original run.

## Datasets

| dataset | series | versions | points | versions/point (mean / p99 / max) | tombstones | null published | generator Parquet |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | 20 | 453,060 | 440,160 | 1.03 / 2 / 3 | 14 | 29,234 | 1.8 MB |
| forecasts | 2 | 2,741,768 | 13,432 | 204.12 / 240 / 241 | 8 | 8 | 9.1 MB |
| settlement | 10 | 960,000 | 350,400 | 2.74 / 3 / 3 | 190 | 164,544 | 4.0 MB |
| sparse | 20,000 | 434,191 | 239,384 | 1.81 / 6 / 16 | 10,761 | 8,704 | 11.9 MB |
| backfill | 10 | 460,135 | 438,240 | 1.05 / 2 / 2 | 0 | 249,589 | 5.7 MB |

## Ingest

Two modes, so that ingest compares like with like (details, per-write latency and fsync counts: [ingest-small.md](ingest-small.md)):

* **per-batch**: one commit per original write batch in arrival order (Muninn `write`, DuckDB `BEGIN/INSERT/COMMIT` on a file, ClickHouse one `INSERT` per batch; `clickhouse` does not fsync inserts by default, `clickhouse-fsync` does);
* **bulk**: as few writes as possible (DuckDB/ClickHouse one statement, Muninn 1M-row writes; `muninn-100k`: 100k-row writes).

* **Muninn builds**: `(pre-0009)` = before the fast write path (one segment file per touched partition plus a log entry per write, ~4-25 fsyncs); `(post-0009)` = ADR 0009 (rows inline in the commit entry, group commit, memtable, background flush; ~2 fsyncs per commit). Per-batch variants of the post-0009 build: `muninn-wm64` submits 64 original batches per `write_many` call (one durable commit per 64 batches, each batch still its own transaction and system_time), `muninn-8p` has 8 producer threads calling `write` (the engine groups concurrent writes). Neither has a competitor counterpart here: DuckDB and ClickHouse were measured with one client committing one batch at a time.

Cells: wall seconds incl. finalisation (rows/s). *prefix*: only the first N batches, the same N for every system.

Storage: **btrfs**

| dataset | mode | muninn (pre-0009) | muninn (post-0009) | muninn-wm64 (post-0009) | muninn-8p (post-0009) | muninn-100k (pre-0009) | muninn-100k (post-0009) | duckdb-native | clickhouse | clickhouse-fsync |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | per-batch | 69 (6,586) | 39 (12k) | 1.75 (258k) | 15 (30k) | - | - | 38 (12k) | 17 (27k) | 150 (3,014) |
| da_prices | bulk | 0.87 (521k) | 0.50 (904k) | - | - | 0.87 (523k) | 0.63 (719k) | 0.18 (2.50M) | 0.15 (2.96M) | 0.19 (2.34M) |
| forecasts | per-batch | 188 (15k) | 27 (102k) | 3.92 (699k) | 3.59 (763k) | - | - | 11 (240k) | 8.29 (331k) | 57 (48k) |
| forecasts | bulk | 8.93 (307k) | 6.26 (438k) | - | - | 4.43 (619k) | 2.89 (948k) | 0.67 (4.07M) | 1.35 (2.03M) | 1.33 (2.07M) |
| settlement | per-batch | 142 (6,737) | 70 (14k) | 3.02 (317k) | 5.54 (173k) | - | - | 55 (18k) | 23 (41k) | 209 (4,592) |
| settlement | bulk | 1.88 (510k) | 0.89 (1.08M) | - | - | 1.37 (702k) | 1.00 (959k) | 0.36 (2.63M) | 0.30 (3.17M) | 0.34 (2.86M) |
| sparse | per-batch (prefix 10,000 of 434,191) | 111 (90.0) | 59 (170) | 0.79 (13k) | 9.40 (1,063) | - | - | 30 (331) | 20 (492) | 203 (49.0) |
| sparse | bulk | 0.98 (445k) | 0.78 (554k) | - | - | 1.05 (412k) | 1.15 (379k) | 0.21 (2.03M) | 0.40 (1.10M) | 0.42 (1.04M) |
| backfill | per-batch | 3.08 (150k) | 3.43 (134k) | 1.26 (365k) | 0.83 (556k) | - | - | 4.02 (115k) | 1.52 (302k) | 6.98 (66k) |
| backfill | bulk | 0.58 (791k) | 0.33 (1.39M) | - | - | 0.72 (643k) | 0.53 (865k) | 0.16 (2.91M) | 0.21 (2.19M) | 0.22 (2.10M) |

Storage: **tmpfs**

| dataset | mode | muninn (pre-0009) | muninn (post-0009) | muninn-wm64 (post-0009) | muninn-8p (post-0009) | muninn-100k (pre-0009) | muninn-100k (post-0009) | duckdb-native |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | per-batch | 2.33 (194k) | 1.65 (274k) | 0.53 (855k) | 0.66 (685k) | - | - | 12 (37k) |
| da_prices | bulk | 0.67 (678k) | 0.44 (1.03M) | - | - | 0.50 (901k) | 0.36 (1.24M) | 0.15 (3.04M) |
| forecasts | per-batch | 10 (274k) | 3.21 (853k) | 3.35 (817k) | 1.74 (1.57M) | - | - | 6.91 (397k) |
| forecasts | bulk | 7.33 (374k) | 6.23 (440k) | - | - | 3.41 (803k) | 2.63 (1.04M) | 0.57 (4.83M) |
| settlement | per-batch | 4.03 (238k) | 2.95 (326k) | 1.36 (707k) | 1.12 (859k) | - | - | 18 (54k) |
| settlement | bulk | 1.59 (604k) | 0.90 (1.07M) | - | - | 1.27 (754k) | 0.95 (1.01M) | 0.36 (2.67M) |
| sparse | per-batch (prefix 10,000 of 434,191) | 4.55 (2,195) | 3.66 (2,735) | 0.52 (19k) | 0.95 (10k) | - | - | 16 (611) |
| sparse | bulk | 1.04 (419k) | 0.67 (650k) | - | - | 0.97 (446k) | 0.84 (519k) | 0.21 (2.04M) |
| backfill | per-batch | 0.84 (545k) | 0.80 (574k) | 1.18 (389k) | 0.44 (1.04M) | - | - | 1.30 (354k) |
| backfill | bulk | 0.81 (571k) | 0.33 (1.40M) | - | - | 0.70 (655k) | 0.49 (945k) | 0.14 (3.22M) |

## Load for the query runs, and size on disk

How each system was loaded for the query measurements below: Muninn one write per original batch (the per-batch mode, so `system_time` is reproduced exactly), DuckDB and ClickHouse in bulk. **These ingest times are not comparable across systems**; see Ingest for that.

| dataset | muninn load s | muninn size (B/version) | duckdb-native load s | duckdb-native size (B/version) | duckdb-parquet load s | duckdb-parquet size (B/version) | clickhouse load s | clickhouse size (B/version) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | 106.4 | 1.7 MB (4.04) | 0.1 | 4.8 MB (11.02) | 0.1 | 2.2 MB (5.2) | 0.2 | 1.5 MB (3.55) |
| forecasts | 202.2 | 10.7 MB (4.08) | 0.6 | 67.5 MB (25.82) | 0.5 | 9.5 MB (3.63) | 1.1 | 8.0 MB (3.06) |
| settlement | 136.7 | 3.6 MB (3.94) | 0.4 | 25.5 MB (27.87) | 0.3 | 4.8 MB (5.26) | 0.3 | 3.2 MB (3.48) |
| sparse | 1.4 | 17.4 MB (41.98) | 0.2 | 15.3 MB (36.86) | 0.2 | 13.3 MB (32.19) | 0.4 | 12.9 MB (31.07) |
| backfill | 4.9 | 2.8 MB (6.44) | 0.2 | 8.3 MB (18.83) | 0.1 | 5.7 MB (13.07) | 0.2 | 3.4 MB (7.76) |

## Query latency (ms): p50 / p99 warm, cold

| query | rows | muninn | duckdb-native | duckdb-parquet | clickhouse |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fc_asof_single` | 96 | 1.04 / 1.31, 1.34 | 4.75 / 8.04, 7.52 | 8.80 / 11, 12 | 8.42 / 9.32, 10 |
| `da_asof_published` | 57 | 0.21 / 0.30, 0.44 | 1.71 / 2.68, 5.22 | 6.02 / 8.23, 10 | 4.20 / 5.51, 6.19 |
| `st_asof_system` | 96 | 0.62 / 0.74, 0.90 | 2.23 / 3.93, 5.42 | 7.57 / 9.39, 11 | 4.45 / 6.67, 6.46 |
| `sp_asof_single` | 6 | 0.42 / 1.43, 1.11 | 1.82 / 3.74, 5.42 | 12 / 15, 13 | 4.76 / 5.71, 6.54 |
| `bf_asof_single` | 24 | 0.57 / 0.65, 0.81 | 1.76 / 3.00, 4.70 | 7.77 / 9.65, 9.79 | 4.11 / 5.38, 5.70 |
| `fc_grid_365` | 8,496 | 61 / 62, 68 | 181 / 188, 222 | 201 / 207, 229 | 620 / 663, 656 |
| `fc_grid_365_all` | 16,992 | 121 / 122, 129 | 409 / 431, 458 | 440 / 445, 473 | 1345 / 1438, 1638 |
| `da_grid_365` | 658,080 | 238 / 247, 241 | 277 / 284, 337 | 289 / 297, 315 | 608 / 642, 604 |
| `st_grid_365` | 37,795 | 23 / 25, 26 | 39 / 44, 55 | 44 / 47, 54 | 126 / 132, 123 |
| `bf_grid_365` | 17,156 | 3.98 / 4.56, 4.16 | 12 / 15, 18 | 18 / 20, 23 | 19 / 30, 20 |
| `fc_history_day` | 21,360 | 6.66 / 7.69, 7.88 | 3.63 / 6.71, 8.82 | 8.22 / 13, 13 | 6.59 / 12, 9.66 |
| `st_history_month` | 8,198 | 4.16 / 4.68, 5.03 | 2.50 / 5.30, 7.10 | 7.87 / 12, 13 | 4.85 / 6.36, 6.08 |
| `fc_replay_6h` | 11,376 | 5.31 / 8.04, 5.10 | 4.21 / 7.82, 10 | 14 / 15, 17 | 14 / 18, 15 |
| `sp_replay_day` | 1,196 | 8.55 / 9.41, 9.61 | 3.96 / 5.00, 9.79 | 16 / 19, 19 | 30 / 35, 30 |
| `fc_lead_24h` | 5,668 | 12 / 13, 16 | 6.08 / 9.35, 17 | 26 / 28, 30 | 30 / 33, 31 |
| `st_latest_now` | 6,716 | 6.63 / 6.87, 6.81 | 4.54 / 5.29, 13 | 18 / 19, 22 | 12 / 13, 14 |
| `sp_latest_now` | 20,754 | 10 / 12, 11 | 11 / 12, 19 | 24 / 25, 27 | 48 / 51, 53 |
| `fc_latest_now` | 1,912 | 10 / 10, 11 | 14 / 14, 18 | 20 / 21, 22 | 21 / 22, 26 |
| `da_asof_join_signals` | 10,000 | 45 / 49, 48 | 53 / 55, 65 | 59 / 61, 70 | 248 / 284, 277 |
| `fc_asof_join_lead24` | 2,880 | 89 / 98, 91 | 73 / 79, 90 | 82 / 89, 89 | 328 / 389, 355 |

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
