# Muninn benchmark — medium scale, seed 1

2026-10-19 on `omarchy-hp`: AMD Ryzen 5 PRO 5650U with Radeon Graphics, 12 logical CPUs, 14.9 GB RAM, Linux-7.2.3-arch1-3-x86_64-with-glibc2.44. Systems limited to 6 threads and 4096 MB. Latencies in ms, end to end through the Python client to a canonical Arrow table. p50/p99 over warm samples (few samples ⇒ p99 ≈ max).

**muninn** was re-run on 2026-10-19 on `omarchy-hp` (datasets: da_prices, forecasts, settlement, sparse, backfill) and merged into this report; the other systems' numbers are from the original run.

## Datasets

| dataset | series | versions | points | versions/point (mean / p99 / max) | tombstones | null published | generator Parquet |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | 40 | 1,808,140 | 1,754,880 | 1.03 / 2 / 4 | 61 | 122,438 | 6.4 MB |
| forecasts | 3 | 24,975,472 | 107,988 | 231.28 / 240 / 241 | 112 | 8,322,352 | 83.3 MB |
| settlement | 40 | 3,840,000 | 1,401,600 | 2.74 / 3 / 3 | 707 | 663,936 | 15.3 MB |
| sparse | 150,000 | 3,246,139 | 1,793,926 | 1.81 / 6 / 21 | 80,052 | 64,760 | 78.7 MB |
| backfill | 60 | 2,760,303 | 2,629,440 | 1.05 / 2 / 2 | 0 | 1,498,817 | 31.2 MB |

## Ingest

Two modes, so that ingest compares like with like (details, per-write latency and fsync counts: [ingest-medium.md](ingest-medium.md)):

* **per-batch**: one commit per original write batch in arrival order (Muninn `write`, DuckDB `BEGIN/INSERT/COMMIT` on a file, ClickHouse one `INSERT` per batch; `clickhouse` does not fsync inserts by default, `clickhouse-fsync` does);
* **bulk**: as few writes as possible (DuckDB/ClickHouse one statement, Muninn 1M-row writes; `muninn-100k`: 100k-row writes).

* **Muninn builds**: `(pre-0009)` = before the fast write path (one segment file per touched partition plus a log entry per write, ~4-25 fsyncs); `(post-0009)` = ADR 0009 (rows inline in the commit entry, group commit, memtable, background flush; ~2 fsyncs per commit); `(post-0009 rerun)` = the same build re-measured on 2026-10-20 in the session of the next one, for a same-time comparison; `(post-fixes)` = post-0009 plus the three write path fixes of 2026-10-20 (GC re-anchors log truncation after a memtable load, ADR 0007 amendment; linear in-batch dedup; columnar Arrow input and validation). Per-batch variants: `muninn-wm64` submits 64 original batches per `write_many` call (one durable commit per 64 batches, each batch still its own transaction and system_time), `muninn-8p` has 8 producer threads calling `write` (the engine groups concurrent writes). Neither has a competitor counterpart here: DuckDB and ClickHouse were measured with one client committing one batch at a time.

Cells: wall seconds incl. finalisation (rows/s). *prefix*: only the first N batches, the same N for every system.

Storage: **btrfs**

| dataset | mode | muninn (pre-0009) | muninn (post-0009) | muninn (post-0009 rerun) | muninn (post-fixes) | muninn-wm64 (post-0009) | muninn-8p (post-0009) | muninn-100k (pre-0009) | muninn-100k (post-0009) | muninn-100k (post-0009 rerun) | muninn-100k (post-fixes) | duckdb-native | clickhouse | clickhouse-fsync |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | per-batch | 494 (3,659) | 223 (8,094) | - | - | 6.77 (267k) | 45 (41k) | - | - | - | - | 188 (9,599) | 75 (24k) | 767 (2,357) |
| da_prices | bulk | 3.01 (600k) | 1.81 (997k) | - | - | - | - | 2.44 (740k) | 2.54 (713k) | - | - | 0.40 (4.55M) | 0.71 (2.53M) | 0.69 (2.61M) |
| forecasts | per-batch | 2,099 (12k) | 274 (91k) | - | - | 32 (777k) | 90 (276k) | - | - | - | - | 162 (154k) | 99 (254k) | 645 (39k) |
| forecasts | bulk | 60 (413k) | 62 (402k) | 65 (383k) | 9.83 (2.54M) | - | - | 36 (698k) | 25 (1.02M) | 25 (1.01M) | 8.13 (3.07M) | 4.87 (5.13M) | 9.18 (2.72M) | 9.42 (2.65M) |
| settlement | per-batch | 579 (6,627) | 325 (12k) | - | - | 8.12 (473k) | 69 (56k) | - | - | - | - | 196 (20k) | 103 (37k) | 890 (4,315) |
| settlement | bulk | 5.85 (656k) | 3.95 (973k) | - | - | - | - | 7.47 (514k) | 6.21 (619k) | - | - | 0.68 (5.67M) | 1.55 (2.48M) | 1.58 (2.43M) |
| sparse | per-batch (prefix 10,000 of 3,246,137) | 65 (153) | 40 (251) | - | - | 3.08 (3,245) | 9.56 (1,046) | - | - | - | - | 45 (222) | 21 (487) | 189 (53.0) |
| sparse | bulk | 9.87 (329k) | 8.69 (374k) | - | - | - | - | 13 (251k) | 12 (264k) | - | - | 0.86 (3.79M) | 3.13 (1.04M) | 3.23 (1.00M) |
| backfill | per-batch | 56 (49k) | 30 (92k) | - | - | 7.59 (364k) | 8.17 (338k) | - | - | - | - | 15 (179k) | 9.66 (286k) | 84 (33k) |
| backfill | bulk | 4.82 (573k) | 3.50 (788k) | - | - | - | - | 5.55 (497k) | 3.41 (809k) | - | - | 0.46 (6.00M) | 1.09 (2.52M) | 1.26 (2.18M) |

Storage: **tmpfs**

| dataset | mode | muninn (pre-0009) | muninn (post-0009) | muninn (post-0009 rerun) | muninn (post-fixes) | muninn-wm64 (post-0009) | muninn-wm64 (post-0009 rerun) | muninn-wm64 (post-fixes) | muninn-8p (post-0009) | muninn-100k (pre-0009) | muninn-100k (post-0009) | muninn-100k (post-0009 rerun) | muninn-100k (post-fixes) | duckdb-native |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | per-batch | 9.56 (189k) | 7.14 (253k) | 7.58 (238k) | 7.01 (258k) | 2.20 (822k) | 2.26 (800k) | 1.68 (1.07M) | 3.08 (586k) | - | - | - | - | 48 (38k) |
| da_prices | bulk | 2.40 (755k) | 1.67 (1.08M) | - | - | - | - | - | - | 2.25 (805k) | 1.77 (1.02M) | - | - | 0.33 (5.50M) |
| forecasts | per-batch | 69 (363k) | 31 (794k) | 33 (759k) | 22 (1.13M) | 30 (843k) | 30 (831k) | 12 (2.14M) | 19 (1.31M) | - | - | - | - | 57 (441k) |
| forecasts | bulk | 56 (446k) | 62 (401k) | 67 (372k) | 10 (2.48M) | - | - | - | - | 29 (848k) | 23 (1.10M) | 24 (1.03M) | 6.12 (4.08M) | 4.22 (5.92M) |
| settlement | per-batch | 19 (201k) | 15 (252k) | - | - | 6.19 (620k) | - | - | 5.66 (678k) | - | - | - | - | 68 (57k) |
| settlement | bulk | 5.14 (747k) | 3.75 (1.02M) | - | - | - | - | - | - | 6.35 (604k) | 5.43 (708k) | - | - | 0.63 (6.08M) |
| sparse | per-batch (prefix 10,000 of 3,246,137) | 24 (424) | 23 (441) | - | - | 2.82 (3,542) | - | - | 3.81 (2,625) | - | - | - | - | 16 (644) |
| sparse | bulk | 9.44 (344k) | 8.54 (380k) | - | - | - | - | - | - | 12 (263k) | 12 (270k) | - | - | 0.78 (4.14M) |
| backfill | per-batch | 11 (243k) | 11 (262k) | - | - | 7.09 (389k) | - | - | 6.17 (447k) | - | - | - | - | 6.97 (396k) |
| backfill | bulk | 4.51 (612k) | 3.36 (821k) | - | - | - | - | - | - | 4.04 (683k) | 3.29 (838k) | - | - | 0.37 (7.54M) |

## Load for the query runs, and size on disk

How each system was loaded for the query measurements below: Muninn one write per original batch (the per-batch mode, so `system_time` is reproduced exactly), DuckDB and ClickHouse in bulk. **These ingest times are not comparable across systems**; see Ingest for that.

| dataset | muninn load s | muninn size (B/version) | duckdb-native load s | duckdb-native size (B/version) | duckdb-parquet load s | duckdb-parquet size (B/version) | clickhouse load s | clickhouse size (B/version) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | 456.1 | 6.7 MB (3.88) | 0.3 | 17.8 MB (10.3) | 0.3 | 9.8 MB (5.68) | 0.6 | 6.1 MB (3.51) |
| forecasts | 1940.5 | 75.2 MB (3.16) | 4.4 | 568.0 MB (23.85) | 4.3 | 85.3 MB (3.58) | 9.3 | 71.2 MB (2.99) |
| settlement | 508.6 | 13.6 MB (3.7) | 0.7 | 101.8 MB (27.79) | 0.6 | 19.0 MB (5.19) | 1.6 | 12.7 MB (3.47) |
| sparse | 12.2 | 133.0 MB (42.97) | 0.9 | 113.0 MB (36.51) | 1.0 | 102.1 MB (32.97) | 3.1 | 97.7 MB (31.57) |
| backfill | 52.9 | 16.6 MB (6.29) | 0.4 | 47.0 MB (17.86) | 0.5 | 34.2 MB (13.0) | 1.1 | 20.3 MB (7.72) |

## Query latency (ms): p50 / p99 warm, cold

| query | rows | muninn | duckdb-native | duckdb-parquet | clickhouse |
| --- | ---: | ---: | ---: | ---: | ---: |
| `fc_asof_single` | 96 | 1.04 / 3.73, 4.54 | 4.79 / 8.89, 9.64 | 12 / 15, 16 | 8.66 / 10, 11 |
| `da_asof_published` | 57 | 0.22 / 0.33, 0.47 | 1.77 / 4.31, 6.39 | 6.48 / 8.89, 9.96 | 4.17 / 5.07, 6.36 |
| `st_asof_system` | 96 | 0.62 / 0.71, 0.92 | 2.15 / 4.64, 7.06 | 8.03 / 11, 13 | 4.40 / 5.49, 6.24 |
| `sp_asof_single` | 6 | 1.58 / 3.87, 5.19 | 1.70 / 4.61, 6.76 | 13 / 15, 16 | 4.84 / 6.58, 6.67 |
| `bf_asof_single` | 24 | 0.57 / 0.68, 0.89 | 1.78 / 3.74, 6.36 | 8.07 / 11, 13 | 4.15 / 6.53, 5.84 |
| `fc_grid_365` | 52,416 | 417 / 467, 1133 | 1109 / 1174, 1252 | 1367 / 1416, 1444 | 10654 / 10963, 10292 |
| `fc_grid_365_all` | 157,248 | 1205 / 1208, 1213 | 5063 / 5214, 4979 | 5134 / 5206, 5128 | 33415 / 33697, 33171 |
| `da_grid_365` | 1,863,360 | 658 / 682, 668 | 843 / 853, 916 | 880 / 885, 935 | 1747 / 1770, 1571 |
| `st_grid_365` | 37,795 | 23 / 24, 24 | 40 / 42, 53 | 45 / 52, 86 | 123 / 134, 125 |
| `bf_grid_365` | 17,156 | 4.36 / 4.76, 4.59 | 12 / 14, 19 | 19 / 21, 26 | 19 / 32, 22 |
| `fc_history_day` | 22,837 | 6.85 / 8.06, 8.41 | 4.08 / 7.66, 11 | 11 / 15, 18 | 6.54 / 17, 12 |
| `st_history_month` | 8,198 | 4.23 / 5.05, 28 | 2.47 / 5.60, 7.55 | 8.80 / 13, 15 | 4.99 / 6.15, 5.98 |
| `fc_replay_6h` | 16,848 | 7.96 / 12, 7.78 | 5.58 / 13, 16 | 22 / 23, 27 | 15 / 50, 51 |
| `sp_replay_day` | 8,955 | 53 / 62, 37 | 16 / 19, 47 | 85 / 89, 87 | 136 / 140, 130 |
| `fc_lead_24h` | 8,559 | 20 / 24, 25 | 9.15 / 25, 28 | 39 / 41, 43 | 48 / 51, 48 |
| `st_latest_now` | 26,868 | 27 / 28, 27 | 11 / 12, 34 | 57 / 58, 63 | 33 / 35, 34 |
| `sp_latest_now` | 156,891 | 112 / 114, 115 | 64 / 70, 115 | 133 / 139, 152 | 253 / 269, 271 |
| `fc_latest_now` | 2,868 | 14 / 15, 30 | 15 / 18, 23 | 26 / 30, 35 | 44 / 45, 47 |
| `da_asof_join_signals` | 10,000 | 127 / 133, 128 | 202 / 209, 226 | 225 / 238, 246 | 922 / 980, 853 |
| `fc_asof_join_lead24` | 2,880 | 81 / 84, 231 | 73 / 82, 107 | 85 / 88, 102 | 333 / 404, 423 |

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
