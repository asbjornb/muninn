# Muninn ingest benchmark — small scale

AMD Ryzen 5 PRO 5650U with Radeon Graphics, 12 logical CPUs, 14.9 GB RAM, Linux-7.2.3-arch1-3-x86_64-with-glibc2.44. Produced by `bench/ingest.py` (see its docstring and `bench/results/README.md`, section Ingest).

* **per-batch**: one transaction/commit per original write batch, in arrival order (Muninn `write`; DuckDB `BEGIN/INSERT/COMMIT` on a database file; ClickHouse one synchronous `INSERT` per batch, `clickhouse-fsync` with `fsync_after_insert=1`).
* **bulk**: as few writes as possible (DuckDB one `CREATE TABLE AS ... ORDER BY`, ClickHouse one `INSERT ... SELECT`, Muninn writes of 1M rows).
* Times include the system's finalisation (Muninn compact+gc, DuckDB CHECKPOINT, ClickHouse OPTIMIZE FINAL); **writes/s** is over the write phase, **rows/s** over the whole wall time. **prefix** = only the first N batches were loaded (same N for every system), rates are for that prefix. Size = on disk after finalisation.
* **fsyncs**: calls to fsync/fdatasync in the benchmark process and the time spent in them (`bench/fsyncstat.c`); ClickHouse runs as its own server process, so not counted. Summed over threads, so with background flushes the total can exceed the wall time.
* **Muninn builds**: `(pre-0009)` = before the fast write path (one segment file per touched partition plus a log entry per write, ~4-25 fsyncs); `(post-0009)` = ADR 0009 (rows inline in the commit entry, group commit, memtable, background flush; ~2 fsyncs per commit); `(post-0009 rerun)` = the same build re-measured on 2026-10-20 in the session of the next one, for a same-time comparison; `(post-fixes)` = post-0009 plus the three write path fixes of 2026-10-20 (GC re-anchors log truncation after a memtable load, ADR 0007 amendment; linear in-batch dedup; columnar Arrow input and validation). Per-batch variants: `muninn-wm64` submits 64 original batches per `write_many` call (one durable commit per 64 batches, each batch still its own transaction and system_time), `muninn-8p` has 8 producer threads calling `write` (the engine groups concurrent writes). Neither has a competitor counterpart here: DuckDB and ClickHouse were measured with one client committing one batch at a time.

## Storage: btrfs (`/home/abr/Projects/muninn/.claude/worktrees/agent-a52a9f0264c1178b5/bench/data/work`)

| dataset | mode | system | writes | rows | wall s | write s | finalize s | writes/s | rows/s | per write p50 / p99 ms | size (B/version) | fsyncs (time) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | per-batch | muninn (pre-0009) | 7,723 | 453,060 | 69 | 69 | 0.10 | 112 | 6,586 | 4.10 / 21.32 | 1.7 MB (4.04) | 31,399 (61 s, 4.1/write) |
| da_prices | per-batch | muninn (post-0009) | 7,723 | 453,060 | 39 | 39 | 0.11 | 197 | 12k | 1.81 / 12.49 | 2.2 MB (5.14) | 15,957 (37 s, 2.1/write) |
| da_prices | per-batch | muninn (post-0009 rerun) | 7,723 | 453,060 | 37 | 37 | 0.12 | 211 | 12k | 1.87 / 12.31 | 2.2 MB (5.14) | 15,957 (33 s, 2.1/write) |
| da_prices | per-batch | muninn (post-fixes) | 7,723 | 453,060 | 32 | 32 | 0.13 | 240 | 14k | 1.63 / 11.21 | 1.6 MB (3.76) | 16,053 (30 s, 2.1/write) |
| da_prices | per-batch | muninn-wm64 (post-0009) | 7,723 | 453,060 | 1.75 | 1.58 | 0.18 | 4,894 | 258k | 11.91 / 23.13 | 4.6 MB (10.58) | 326 (1.50 s, 0.0/write) |
| da_prices | per-batch | muninn-wm64 (post-0009 rerun) | 7,723 | 453,060 | 1.76 | 1.58 | 0.18 | 4,876 | 257k | 12.81 / 22.81 | 4.6 MB (10.58) | 326 (1.49 s, 0.0/write) |
| da_prices | per-batch | muninn-wm64 (post-fixes) | 7,723 | 453,060 | 1.61 | 1.41 | 0.20 | 5,485 | 281k | 10.84 / 18.48 | 1.6 MB (3.73) | 332 (1.50 s, 0.0/write) |
| da_prices | per-batch | muninn-8p (post-0009) | 7,723 | 453,060 | 15 | 15 | 0.26 | 529 | 30k | 16.35 / 25.60 | 4.5 MB (10.44) | 4,000 (14 s, 0.5/write) |
| da_prices | per-batch | duckdb-native | 7,723 | 453,060 | 38 | 38 | 0.05 | 202 | 12k | 5.65 / 6.61 | 4.5 MB (10.44) | 7,731 (24 s, 1.0/write) |
| da_prices | per-batch | clickhouse | 7,723 | 453,060 | 17 | 16 | 0.08 | 468 | 27k | 2.07 / 3.10 | 1.5 MB (3.54) | n/a |
| da_prices | per-batch | clickhouse-fsync | 7,723 | 453,060 | 150 | 150 | 0.08 | 51.4 | 3,014 | 22.72 / 38.87 | 1.5 MB (3.54) | n/a |
| da_prices | bulk | muninn (pre-0009) | 1 | 453,060 | 0.87 | 0.85 | 0.02 | - | 521k | - | 1.6 MB (3.6) | 51 (0.21 s) |
| da_prices | bulk | muninn (post-0009) | 1 | 453,060 | 0.50 | 0.48 | 0.02 | - | 904k | - | 1.6 MB (3.6) | 51 (0.33 s) |
| da_prices | bulk | muninn-100k (pre-0009) | 5 | 453,060 | 0.87 | 0.69 | 0.17 | 7.2 | 523k | 136.58 / 198.77 | 1.6 MB (3.62) | 89 (0.36 s, 17.8/write) |
| da_prices | bulk | muninn-100k (post-0009) | 5 | 453,060 | 0.63 | 0.47 | 0.17 | 10.8 | 719k | 94.78 / 103.41 | 1.6 MB (3.62) | 89 (0.45 s, 17.8/write) |
| da_prices | bulk | duckdb-native | 1 | 453,060 | 0.18 | 0.18 | 0.00 | - | 2.50M | - | 4.8 MB (11.02) | 6 (0.04 s) |
| da_prices | bulk | clickhouse | 1 | 453,060 | 0.15 | 0.15 | 0.00 | - | 2.96M | - | 1.5 MB (3.55) | n/a |
| da_prices | bulk | clickhouse-fsync | 1 | 453,060 | 0.19 | 0.19 | 0.00 | - | 2.34M | - | 1.5 MB (3.55) | n/a |
| forecasts | per-batch | muninn (pre-0009) | 2,857 | 2,741,768 | 188 | 186 | 2.13 | 15.4 | 15k | 28.05 / 122.29 | 10.7 MB (4.08) | 71,436 (164 s, 25.0/write) |
| forecasts | per-batch | muninn (post-0009) | 2,857 | 2,741,768 | 27 | 26 | 0.75 | 110 | 102k | 9.02 / 17.45 | 10.0 MB (3.84) | 8,934 (33 s, 3.1/write) |
| forecasts | per-batch | muninn (post-0009 rerun) | 2,857 | 2,741,768 | 9.42 | 9.21 | 0.21 | 310 | 291k | 2.50 / 6.22 | 9.9 MB (3.8) | 9,072 (8.23 s, 3.2/write) |
| forecasts | per-batch | muninn (post-fixes) | 2,857 | 2,741,768 | 8.43 | 8.21 | 0.22 | 348 | 325k | 2.16 / 5.65 | 8.5 MB (3.26) | 9,087 (8.31 s, 3.2/write) |
| forecasts | per-batch | muninn-wm64 (post-0009) | 2,857 | 2,741,768 | 3.92 | 3.81 | 0.11 | 749 | 699k | 82.69 / 98.44 | 124.8 MB (47.74) | 1,341 (6.60 s, 0.5/write) |
| forecasts | per-batch | muninn-wm64 (post-0009 rerun) | 2,857 | 2,741,768 | 3.65 | 3.51 | 0.14 | 813 | 751k | 73.67 / 93.73 | 124.8 MB (47.74) | 1,341 (1.73 s, 0.5/write) |
| forecasts | per-batch | muninn-wm64 (post-fixes) | 2,857 | 2,741,768 | 1.48 | 1.32 | 0.16 | 2,171 | 1.85M | 26.70 / 38.16 | 8.2 MB (3.14) | 1,350 (1.82 s, 0.5/write) |
| forecasts | per-batch | muninn-8p (post-0009) | 2,857 | 2,741,768 | 3.59 | 2.80 | 0.79 | 1,021 | 763k | 7.22 / 14.22 | 8.7 MB (3.33) | 3,884 (4.46 s, 1.4/write) |
| forecasts | per-batch | duckdb-native | 2,857 | 2,741,768 | 11 | 11 | 0.05 | 251 | 240k | 3.04 / 7.14 | 22.3 MB (8.51) | 2,889 (4.95 s, 1.0/write) |
| forecasts | per-batch | clickhouse | 2,857 | 2,741,768 | 8.29 | 7.78 | 0.51 | 367 | 331k | 2.63 / 4.14 | 8.0 MB (3.06) | n/a |
| forecasts | per-batch | clickhouse-fsync | 2,857 | 2,741,768 | 57 | 57 | 0.40 | 50.3 | 48k | 25.22 / 38.71 | 8.0 MB (3.06) | n/a |
| forecasts | bulk | muninn (pre-0009) | 3 | 2,741,768 | 8.93 | 8.88 | 0.05 | 0.3 | 307k | 2914.72 / 3560.20 | 7.3 MB (2.78) | 280 (0.34 s, 93.3/write) |
| forecasts | bulk | muninn (post-0009) | 3 | 2,741,768 | 6.26 | 6.23 | 0.03 | 0.5 | 438k | 2224.63 / 2371.35 | 7.3 MB (2.78) | 280 (0.39 s, 93.3/write) |
| forecasts | bulk | muninn-100k (pre-0009) | 28 | 2,741,768 | 4.43 | 4.25 | 0.18 | 6.6 | 619k | 150.12 / 192.07 | 7.7 MB (2.94) | 974 (0.75 s, 34.8/write) |
| forecasts | bulk | muninn-100k (post-0009) | 28 | 2,741,768 | 2.89 | 2.74 | 0.15 | 10.2 | 948k | 99.31 / 106.69 | 7.7 MB (2.94) | 974 (1.06 s, 34.8/write) |
| forecasts | bulk | duckdb-native | 1 | 2,741,768 | 0.67 | 0.67 | 0.00 | - | 4.07M | - | 67.5 MB (25.82) | 6 (0.02 s) |
| forecasts | bulk | clickhouse | 1 | 2,741,768 | 1.35 | 1.35 | 0.00 | - | 2.03M | - | 8.0 MB (3.06) | n/a |
| forecasts | bulk | clickhouse-fsync | 1 | 2,741,768 | 1.33 | 1.33 | 0.00 | - | 2.07M | - | 8.0 MB (3.06) | n/a |
| settlement | per-batch | muninn (pre-0009) | 10,000 | 960,000 | 142 | 142 | 0.00 | 70.2 | 6,737 | 15.85 / 30.55 | 3.6 MB (3.94) | 40,682 (131 s, 4.1/write) |
| settlement | per-batch | muninn (post-0009) | 10,000 | 960,000 | 70 | 70 | 0.00 | 143 | 14k | 8.02 / 13.71 | 5.0 MB (5.41) | 20,687 (65 s, 2.1/write) |
| settlement | per-batch | muninn-wm64 (post-0009) | 10,000 | 960,000 | 3.02 | 2.70 | 0.33 | 3,708 | 317k | 15.87 / 26.79 | 12.8 MB (14.0) | 464 (2.26 s, 0.0/write) |
| settlement | per-batch | muninn-8p (post-0009) | 10,000 | 960,000 | 5.54 | 5.21 | 0.32 | 1,919 | 173k | 3.70 / 16.03 | 4.3 MB (4.73) | 5,227 (4.58 s, 0.5/write) |
| settlement | per-batch | duckdb-native | 10,000 | 960,000 | 55 | 55 | 0.12 | 183 | 18k | 5.80 / 8.20 | 12.0 MB (13.12) | 10,014 (35 s, 1.0/write) |
| settlement | per-batch | clickhouse | 10,000 | 960,000 | 23 | 23 | 0.24 | 435 | 41k | 2.23 / 3.46 | 3.2 MB (3.47) | n/a |
| settlement | per-batch | clickhouse-fsync | 10,000 | 960,000 | 209 | 209 | 0.21 | 47.9 | 4,592 | 22.46 / 38.83 | 3.2 MB (3.47) | n/a |
| settlement | bulk | muninn (pre-0009) | 1 | 960,000 | 1.88 | 1.87 | 0.01 | - | 510k | - | 3.1 MB (3.35) | 51 (0.15 s) |
| settlement | bulk | muninn (post-0009) | 1 | 960,000 | 0.89 | 0.88 | 0.01 | - | 1.08M | - | 3.1 MB (3.35) | 51 (0.09 s) |
| settlement | bulk | muninn-100k (pre-0009) | 10 | 960,000 | 1.37 | 1.27 | 0.10 | 7.9 | 702k | 130.41 / 137.91 | 3.3 MB (3.56) | 140 (0.13 s, 14.0/write) |
| settlement | bulk | muninn-100k (post-0009) | 10 | 960,000 | 1.00 | 0.91 | 0.09 | 11.0 | 959k | 93.77 / 96.22 | 3.3 MB (3.56) | 140 (0.12 s, 14.0/write) |
| settlement | bulk | duckdb-native | 1 | 960,000 | 0.36 | 0.36 | 0.00 | - | 2.63M | - | 25.5 MB (27.87) | 6 (0.02 s) |
| settlement | bulk | clickhouse | 1 | 960,000 | 0.30 | 0.30 | 0.00 | - | 3.17M | - | 3.2 MB (3.48) | n/a |
| settlement | bulk | clickhouse-fsync | 1 | 960,000 | 0.34 | 0.34 | 0.00 | - | 2.86M | - | 3.2 MB (3.48) | n/a |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | muninn (pre-0009) | 10,000 | 10,000 | 111 | 111 | 0.06 | 90.1 | 90.0 | 15.38 / 22.10 | 7.3 MB (760.77) | 40,257 (101 s, 4.0/write) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | muninn (post-0009) | 10,000 | 10,000 | 59 | 59 | 0.07 | 170 | 170 | 7.55 / 9.28 | 7.4 MB (775.27) | 20,332 (53 s, 2.0/write) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | muninn-wm64 (post-0009) | 10,000 | 10,000 | 0.79 | 0.68 | 0.12 | 15k | 13k | 2.69 / 4.65 | 7.6 MB (793.29) | 342 (0.26 s, 0.0/write) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | muninn-8p (post-0009) | 10,000 | 10,000 | 9.40 | 9.20 | 0.20 | 1,086 | 1,063 | 3.76 / 19.27 | 9.7 MB (1020.53) | 5,039 (8.41 s, 0.5/write) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | duckdb-native | 10,000 | 10,000 | 30 | 30 | 0.02 | 331 | 331 | 2.42 / 6.23 | 780.0 KB (79.87) | 10,005 (14 s, 1.0/write) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | clickhouse | 10,000 | 10,000 | 20 | 20 | 0.02 | 493 | 492 | 1.97 / 2.89 | 326.2 KB (33.4) | n/a |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | clickhouse-fsync | 10,000 | 10,000 | 203 | 203 | 0.03 | 49.4 | 49.0 | 21.84 / 35.27 | 326.2 KB (33.4) | n/a |
| sparse | bulk | muninn (pre-0009) | 1 | 434,191 | 0.98 | 0.94 | 0.03 | - | 445k | - | 17.4 MB (41.93) | 54 (0.06 s) |
| sparse | bulk | muninn (post-0009) | 1 | 434,191 | 0.78 | 0.74 | 0.05 | - | 554k | - | 17.4 MB (41.93) | 54 (0.41 s) |
| sparse | bulk | muninn-100k (pre-0009) | 5 | 434,191 | 1.05 | 0.88 | 0.18 | 5.7 | 412k | 151.72 / 158.72 | 17.4 MB (41.99) | 116 (0.12 s, 23.2/write) |
| sparse | bulk | muninn-100k (post-0009) | 5 | 434,191 | 1.15 | 0.87 | 0.28 | 5.8 | 379k | 134.70 / 139.64 | 17.4 MB (41.99) | 116 (0.65 s, 23.2/write) |
| sparse | bulk | duckdb-native | 1 | 434,191 | 0.21 | 0.21 | 0.00 | - | 2.03M | - | 15.3 MB (36.86) | 6 (0.02 s) |
| sparse | bulk | clickhouse | 1 | 434,191 | 0.40 | 0.40 | 0.00 | - | 1.10M | - | 12.9 MB (31.07) | n/a |
| sparse | bulk | clickhouse-fsync | 1 | 434,191 | 0.42 | 0.42 | 0.00 | - | 1.04M | - | 12.9 MB (31.07) | n/a |
| backfill | per-batch | muninn (pre-0009) | 610 | 460,135 | 3.08 | 2.84 | 0.24 | 215 | 150k | 3.90 / 9.89 | 2.8 MB (6.44) | 2,656 (1.92 s, 4.4/write) |
| backfill | per-batch | muninn (post-0009) | 610 | 460,135 | 3.43 | 3.20 | 0.23 | 191 | 134k | 2.35 / 14.41 | 5.4 MB (12.31) | 1,448 (2.84 s, 2.4/write) |
| backfill | per-batch | muninn-wm64 (post-0009) | 610 | 460,135 | 1.26 | 1.03 | 0.23 | 591 | 365k | 106.72 / 126.70 | 16.6 MB (37.72) | 89 (0.13 s, 0.1/write) |
| backfill | per-batch | muninn-8p (post-0009) | 610 | 460,135 | 0.83 | 0.61 | 0.22 | 1,005 | 556k | 7.29 / 15.61 | 10.7 MB (24.28) | 446 (0.45 s, 0.7/write) |
| backfill | per-batch | duckdb-native | 610 | 460,135 | 4.02 | 3.98 | 0.03 | 153 | 115k | 6.18 / 10.01 | 7.8 MB (17.69) | 618 (2.62 s, 1.0/write) |
| backfill | per-batch | clickhouse | 610 | 460,135 | 1.52 | 1.44 | 0.09 | 425 | 302k | 2.21 / 3.98 | 3.4 MB (7.76) | n/a |
| backfill | per-batch | clickhouse-fsync | 610 | 460,135 | 6.98 | 6.89 | 0.09 | 88.5 | 66k | 7.07 / 31.55 | 3.4 MB (7.76) | n/a |
| backfill | bulk | muninn (pre-0009) | 1 | 460,135 | 0.58 | 0.58 | 0.00 | - | 791k | - | 2.7 MB (6.18) | 30 (0.03 s) |
| backfill | bulk | muninn (post-0009) | 1 | 460,135 | 0.33 | 0.33 | 0.00 | - | 1.39M | - | 2.7 MB (6.18) | 30 (0.03 s) |
| backfill | bulk | muninn-100k (pre-0009) | 5 | 460,135 | 0.72 | 0.52 | 0.19 | 9.6 | 643k | 112.10 / 115.75 | 2.7 MB (6.22) | 105 (0.07 s, 21.0/write) |
| backfill | bulk | muninn-100k (post-0009) | 5 | 460,135 | 0.53 | 0.35 | 0.18 | 14.2 | 865k | 74.24 / 78.16 | 2.7 MB (6.22) | 105 (0.14 s, 21.0/write) |
| backfill | bulk | duckdb-native | 1 | 460,135 | 0.16 | 0.16 | 0.00 | - | 2.91M | - | 8.3 MB (18.83) | 6 (0.01 s) |
| backfill | bulk | clickhouse | 1 | 460,135 | 0.21 | 0.21 | 0.00 | - | 2.19M | - | 3.4 MB (7.76) | n/a |
| backfill | bulk | clickhouse-fsync | 1 | 460,135 | 0.22 | 0.22 | 0.00 | - | 2.10M | - | 3.4 MB (7.76) | n/a |

Muninn per-batch, inside `write` only (excludes the periodic compact+gc): mean latency and the part of it spent in fsync, per write. The counters are process-wide: post-0009, fsyncs of a background flush that overlap a write are attributed to it (which can make 'other' negative).

| dataset | system | writes | mean ms/write | fsyncs/write | segment file+dir fsync ms | log file+dir fsync ms | other ms (encode, dedup, file writes, Python) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | muninn (pre-0009) | 7,723 | 8.58 | 4.0 | 3.83 | 3.94 | 0.82 |
| da_prices | muninn (post-0009) | 7,723 | 4.88 | 2.0 | 0.00 | 4.62 | 0.26 |
| da_prices | muninn (post-0009 rerun) | 7,723 | 4.53 | 2.0 | 0.00 | 4.20 | 0.34 |
| da_prices | muninn (post-fixes) | 7,723 | 3.97 | 2.0 | 0.00 | 3.73 | 0.24 |
| forecasts | muninn (pre-0009) | 2,857 | 60.78 | 24.0 | 49.42 | 4.80 | 6.57 |
| forecasts | muninn (post-0009) | 2,857 | 7.83 | 2.6 | 3.24 | 6.66 | -2.08 |
| forecasts | muninn (post-0009 rerun) | 2,857 | 2.69 | 2.6 | 0.71 | 1.70 | 0.28 |
| forecasts | muninn (post-fixes) | 2,857 | 2.35 | 2.6 | 0.72 | 1.73 | -0.10 |
| settlement | muninn (pre-0009) | 10,000 | 13.85 | 4.0 | 6.39 | 6.53 | 0.93 |
| settlement | muninn (post-0009) | 10,000 | 6.69 | 2.0 | 0.00 | 6.35 | 0.34 |
| sparse | muninn (pre-0009) | 10,000 | 10.61 | 4.0 | 4.96 | 5.03 | 0.62 |
| sparse | muninn (post-0009) | 10,000 | 5.50 | 2.0 | 0.00 | 5.17 | 0.33 |
| backfill | muninn (pre-0009) | 610 | 4.22 | 4.2 | 1.58 | 1.39 | 1.25 |
| backfill | muninn (post-0009) | 610 | 4.84 | 2.1 | 0.35 | 4.05 | 0.44 |

## Storage: tmpfs (`/dev/shm/muninn-ingest`)

| dataset | mode | system | writes | rows | wall s | write s | finalize s | writes/s | rows/s | per write p50 / p99 ms | size (B/version) | fsyncs (time) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | per-batch | muninn (pre-0009) | 7,723 | 453,060 | 2.33 | 2.31 | 0.02 | 3,340 | 194k | 0.24 / 0.49 | 1.7 MB (4.04) | 31,399 (0.02 s, 4.1/write) |
| da_prices | per-batch | muninn (post-0009) | 7,723 | 453,060 | 1.65 | 1.63 | 0.02 | 4,738 | 274k | 0.16 / 0.24 | 2.2 MB (5.14) | 15,957 (0.01 s, 2.1/write) |
| da_prices | per-batch | muninn (post-0009 rerun) | 7,723 | 453,060 | 1.64 | 1.61 | 0.03 | 4,794 | 277k | 0.16 / 0.23 | 2.2 MB (5.14) | 15,957 (0.01 s, 2.1/write) |
| da_prices | per-batch | muninn (post-fixes) | 7,723 | 453,060 | 1.50 | 1.48 | 0.03 | 5,229 | 302k | 0.13 / 0.18 | 1.6 MB (3.76) | 16,053 (0.01 s, 2.1/write) |
| da_prices | per-batch | muninn-wm64 (post-0009) | 7,723 | 453,060 | 0.53 | 0.47 | 0.06 | 17k | 855k | 3.01 / 9.76 | 4.6 MB (10.58) | 326 (0.00 s, 0.0/write) |
| da_prices | per-batch | muninn-wm64 (post-0009 rerun) | 7,723 | 453,060 | 0.51 | 0.45 | 0.06 | 17k | 887k | 2.80 / 8.26 | 4.6 MB (10.58) | 326 (0.00 s, 0.0/write) |
| da_prices | per-batch | muninn-wm64 (post-fixes) | 7,723 | 453,060 | 0.36 | 0.29 | 0.07 | 26k | 1.27M | 1.99 / 4.50 | 1.6 MB (3.73) | 332 (0.00 s, 0.0/write) |
| da_prices | per-batch | muninn-8p (post-0009) | 7,723 | 453,060 | 0.66 | 0.57 | 0.09 | 14k | 685k | 0.52 / 1.83 | 2.8 MB (6.54) | 7,103 (0.00 s, 0.9/write) |
| da_prices | per-batch | duckdb-native | 7,723 | 453,060 | 12 | 12 | 0.03 | 624 | 37k | 1.56 / 2.10 | 4.5 MB (10.44) | 7,731 (0.01 s, 1.0/write) |
| da_prices | bulk | muninn (pre-0009) | 1 | 453,060 | 0.67 | 0.67 | 0.00 | - | 678k | - | 1.6 MB (3.6) | 51 (0.00 s) |
| da_prices | bulk | muninn (post-0009) | 1 | 453,060 | 0.44 | 0.44 | 0.00 | - | 1.03M | - | 1.6 MB (3.6) | 51 (0.00 s) |
| da_prices | bulk | muninn (post-0009 rerun) | 1 | 453,060 | 0.44 | 0.44 | 0.00 | - | 1.03M | - | 1.6 MB (3.6) | 51 (0.00 s) |
| da_prices | bulk | muninn (post-fixes) | 1 | 453,060 | 0.12 | 0.12 | 0.00 | - | 3.68M | - | 1.6 MB (3.62) | 60 (0.00 s) |
| da_prices | bulk | muninn-100k (pre-0009) | 5 | 453,060 | 0.50 | 0.44 | 0.06 | 11.4 | 901k | 96.80 / 98.08 | 1.6 MB (3.62) | 89 (0.00 s, 17.8/write) |
| da_prices | bulk | muninn-100k (post-0009) | 5 | 453,060 | 0.36 | 0.30 | 0.07 | 16.7 | 1.24M | 66.41 / 67.62 | 1.6 MB (3.62) | 89 (0.00 s, 17.8/write) |
| da_prices | bulk | muninn-100k (post-0009 rerun) | 5 | 453,060 | 0.37 | 0.30 | 0.06 | 16.4 | 1.24M | 66.90 / 69.41 | 1.6 MB (3.62) | 89 (0.00 s, 17.8/write) |
| da_prices | bulk | muninn-100k (post-fixes) | 5 | 453,060 | 0.18 | 0.12 | 0.06 | 41.3 | 2.49M | 26.54 / 28.55 | 1.6 MB (3.61) | 98 (0.00 s, 19.6/write) |
| da_prices | bulk | duckdb-native | 1 | 453,060 | 0.15 | 0.15 | 0.00 | - | 3.04M | - | 4.8 MB (11.02) | 6 (0.00 s) |
| forecasts | per-batch | muninn (pre-0009) | 2,857 | 2,741,768 | 10 | 9.76 | 0.26 | 293 | 274k | 2.39 / 6.47 | 10.7 MB (4.08) | 71,436 (0.04 s, 25.0/write) |
| forecasts | per-batch | muninn (post-0009) | 2,857 | 2,741,768 | 3.21 | 3.13 | 0.08 | 913 | 853k | 0.85 / 2.70 | 9.2 MB (3.52) | 9,321 (0.01 s, 3.3/write) |
| forecasts | per-batch | muninn (post-0009 rerun) | 2,857 | 2,741,768 | 3.15 | 3.06 | 0.09 | 932 | 870k | 0.82 / 2.60 | 9.1 MB (3.48) | 9,328 (0.01 s, 3.3/write) |
| forecasts | per-batch | muninn (post-fixes) | 2,857 | 2,741,768 | 2.07 | 1.99 | 0.08 | 1,435 | 1.32M | 0.47 / 1.35 | 8.6 MB (3.27) | 9,057 (0.01 s, 3.2/write) |
| forecasts | per-batch | muninn-wm64 (post-0009) | 2,857 | 2,741,768 | 3.35 | 3.31 | 0.04 | 862 | 817k | 70.69 / 83.99 | 124.8 MB (47.74) | 1,341 (0.00 s, 0.5/write) |
| forecasts | per-batch | muninn-wm64 (post-0009 rerun) | 2,857 | 2,741,768 | 3.36 | 3.32 | 0.04 | 861 | 816k | 70.99 / 83.77 | 124.8 MB (47.74) | 1,341 (0.00 s, 0.5/write) |
| forecasts | per-batch | muninn-wm64 (post-fixes) | 2,857 | 2,741,768 | 1.24 | 1.16 | 0.09 | 2,467 | 2.20M | 22.25 / 35.06 | 8.2 MB (3.14) | 1,350 (0.00 s, 0.5/write) |
| forecasts | per-batch | muninn-8p (post-0009) | 2,857 | 2,741,768 | 1.74 | 1.48 | 0.26 | 1,925 | 1.57M | 3.71 / 13.45 | 8.7 MB (3.33) | 4,852 (0.00 s, 1.7/write) |
| forecasts | per-batch | duckdb-native | 2,857 | 2,741,768 | 6.91 | 6.85 | 0.05 | 417 | 397k | 2.17 / 3.44 | 22.3 MB (8.51) | 2,889 (0.00 s, 1.0/write) |
| forecasts | bulk | muninn (pre-0009) | 3 | 2,741,768 | 7.33 | 7.31 | 0.01 | 0.4 | 374k | 2485.64 / 3198.01 | 7.3 MB (2.78) | 280 (0.00 s, 93.3/write) |
| forecasts | bulk | muninn (post-0009) | 3 | 2,741,768 | 6.23 | 6.21 | 0.01 | 0.5 | 440k | 2228.64 / 2377.91 | 7.3 MB (2.78) | 280 (0.00 s, 93.3/write) |
| forecasts | bulk | muninn (post-0009 rerun) | 3 | 2,741,768 | 6.37 | 6.36 | 0.01 | 0.5 | 430k | 2298.80 / 2400.19 | 7.3 MB (2.78) | 280 (0.00 s, 93.3/write) |
| forecasts | bulk | muninn (post-fixes) | 3 | 2,741,768 | 1.01 | 1.00 | 0.01 | 3.0 | 2.71M | 364.62 / 367.78 | 7.3 MB (2.78) | 289 (0.00 s, 96.3/write) |
| forecasts | bulk | muninn-100k (pre-0009) | 28 | 2,741,768 | 3.41 | 3.35 | 0.07 | 8.4 | 803k | 118.53 / 169.08 | 7.7 MB (2.94) | 974 (0.00 s, 34.8/write) |
| forecasts | bulk | muninn-100k (post-0009) | 28 | 2,741,768 | 2.63 | 2.56 | 0.06 | 10.9 | 1.04M | 93.25 / 99.27 | 7.7 MB (2.94) | 974 (0.00 s, 34.8/write) |
| forecasts | bulk | muninn-100k (post-0009 rerun) | 28 | 2,741,768 | 2.63 | 2.56 | 0.07 | 10.9 | 1.04M | 93.27 / 99.85 | 7.7 MB (2.94) | 974 (0.00 s, 34.8/write) |
| forecasts | bulk | muninn-100k (post-fixes) | 28 | 2,741,768 | 0.71 | 0.64 | 0.06 | 43.5 | 3.87M | 23.45 / 24.52 | 7.7 MB (2.93) | 983 (0.00 s, 35.1/write) |
| forecasts | bulk | duckdb-native | 1 | 2,741,768 | 0.57 | 0.57 | 0.00 | - | 4.83M | - | 67.5 MB (25.82) | 6 (0.00 s) |
| settlement | per-batch | muninn (pre-0009) | 10,000 | 960,000 | 4.03 | 4.03 | 0.00 | 2,481 | 238k | 0.29 / 0.54 | 3.6 MB (3.94) | 40,682 (0.02 s, 4.1/write) |
| settlement | per-batch | muninn (post-0009) | 10,000 | 960,000 | 2.95 | 2.95 | 0.00 | 3,391 | 326k | 0.20 / 0.28 | 5.0 MB (5.41) | 20,687 (0.01 s, 2.1/write) |
| settlement | per-batch | muninn (post-0009 rerun) | 10,000 | 960,000 | 2.91 | 2.91 | 0.00 | 3,433 | 330k | 0.19 / 0.28 | 5.0 MB (5.41) | 20,687 (0.01 s, 2.1/write) |
| settlement | per-batch | muninn (post-fixes) | 10,000 | 960,000 | 2.54 | 2.54 | 0.00 | 3,945 | 379k | 0.15 / 0.20 | 3.3 MB (3.64) | 20,807 (0.01 s, 2.1/write) |
| settlement | per-batch | muninn-wm64 (post-0009) | 10,000 | 960,000 | 1.36 | 1.22 | 0.14 | 8,210 | 707k | 6.85 / 16.73 | 12.8 MB (14.0) | 464 (0.00 s, 0.0/write) |
| settlement | per-batch | muninn-wm64 (post-0009 rerun) | 10,000 | 960,000 | 1.31 | 1.17 | 0.14 | 8,525 | 732k | 6.52 / 16.86 | 12.8 MB (14.0) | 464 (0.00 s, 0.0/write) |
| settlement | per-batch | muninn-wm64 (post-fixes) | 10,000 | 960,000 | 0.82 | 0.68 | 0.14 | 15k | 1.18M | 3.69 / 9.45 | 3.4 MB (3.66) | 470 (0.00 s, 0.0/write) |
| settlement | per-batch | muninn-8p (post-0009) | 10,000 | 960,000 | 1.12 | 0.92 | 0.20 | 11k | 859k | 0.66 / 1.79 | 4.3 MB (4.64) | 9,377 (0.01 s, 0.9/write) |
| settlement | per-batch | duckdb-native | 10,000 | 960,000 | 18 | 18 | 0.09 | 566 | 54k | 1.70 / 2.57 | 12.0 MB (13.12) | 10,014 (0.01 s, 1.0/write) |
| settlement | bulk | muninn (pre-0009) | 1 | 960,000 | 1.59 | 1.59 | 0.00 | - | 604k | - | 3.1 MB (3.35) | 51 (0.00 s) |
| settlement | bulk | muninn (post-0009) | 1 | 960,000 | 0.90 | 0.90 | 0.00 | - | 1.07M | - | 3.1 MB (3.35) | 51 (0.00 s) |
| settlement | bulk | muninn (post-0009 rerun) | 1 | 960,000 | 0.88 | 0.88 | 0.00 | - | 1.09M | - | 3.1 MB (3.35) | 51 (0.00 s) |
| settlement | bulk | muninn (post-fixes) | 1 | 960,000 | 0.40 | 0.40 | 0.00 | - | 2.40M | - | 3.1 MB (3.35) | 60 (0.00 s) |
| settlement | bulk | muninn-100k (pre-0009) | 10 | 960,000 | 1.27 | 1.20 | 0.07 | 8.3 | 754k | 121.11 / 136.60 | 3.3 MB (3.56) | 140 (0.00 s, 14.0/write) |
| settlement | bulk | muninn-100k (post-0009) | 10 | 960,000 | 0.95 | 0.88 | 0.07 | 11.4 | 1.01M | 90.32 / 94.57 | 3.3 MB (3.56) | 140 (0.00 s, 14.0/write) |
| settlement | bulk | muninn-100k (post-0009 rerun) | 10 | 960,000 | 0.95 | 0.88 | 0.07 | 11.4 | 1.01M | 90.25 / 99.20 | 3.3 MB (3.56) | 140 (0.00 s, 14.0/write) |
| settlement | bulk | muninn-100k (post-fixes) | 10 | 960,000 | 0.42 | 0.35 | 0.07 | 28.9 | 2.30M | 35.95 / 38.47 | 3.3 MB (3.55) | 149 (0.00 s, 14.9/write) |
| settlement | bulk | duckdb-native | 1 | 960,000 | 0.36 | 0.36 | 0.00 | - | 2.67M | - | 25.5 MB (27.87) | 6 (0.00 s) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | muninn (pre-0009) | 10,000 | 10,000 | 4.55 | 4.49 | 0.07 | 2,229 | 2,195 | 0.17 / 0.29 | 7.3 MB (760.77) | 40,257 (0.02 s, 4.0/write) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | muninn (post-0009) | 10,000 | 10,000 | 3.66 | 3.59 | 0.07 | 2,785 | 2,735 | 0.12 / 0.16 | 7.4 MB (775.27) | 20,332 (0.01 s, 2.0/write) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | muninn-wm64 (post-0009) | 10,000 | 10,000 | 0.52 | 0.42 | 0.10 | 24k | 19k | 1.19 / 1.36 | 7.6 MB (793.29) | 342 (0.00 s, 0.0/write) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | muninn-8p (post-0009) | 10,000 | 10,000 | 0.95 | 0.82 | 0.14 | 12k | 10k | 0.41 / 1.47 | 9.9 MB (1040.6) | 10,447 (0.01 s, 1.0/write) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | duckdb-native | 10,000 | 10,000 | 16 | 16 | 0.01 | 611 | 611 | 1.60 / 2.14 | 780.0 KB (79.87) | 10,005 (0.01 s, 1.0/write) |
| sparse | bulk | muninn (pre-0009) | 1 | 434,191 | 1.04 | 1.00 | 0.03 | - | 419k | - | 17.4 MB (41.93) | 54 (0.00 s) |
| sparse | bulk | muninn (post-0009) | 1 | 434,191 | 0.67 | 0.64 | 0.03 | - | 650k | - | 17.4 MB (41.93) | 54 (0.00 s) |
| sparse | bulk | muninn-100k (pre-0009) | 5 | 434,191 | 0.97 | 0.84 | 0.14 | 6.0 | 446k | 140.98 / 144.79 | 17.4 MB (41.99) | 116 (0.00 s, 23.2/write) |
| sparse | bulk | muninn-100k (post-0009) | 5 | 434,191 | 0.84 | 0.70 | 0.13 | 7.1 | 519k | 108.40 / 110.32 | 17.4 MB (41.99) | 116 (0.00 s, 23.2/write) |
| sparse | bulk | duckdb-native | 1 | 434,191 | 0.21 | 0.21 | 0.00 | - | 2.04M | - | 15.3 MB (36.86) | 6 (0.00 s) |
| backfill | per-batch | muninn (pre-0009) | 610 | 460,135 | 0.84 | 0.69 | 0.16 | 891 | 545k | 0.82 / 2.01 | 2.8 MB (6.44) | 2,656 (0.00 s, 4.4/write) |
| backfill | per-batch | muninn (post-0009) | 610 | 460,135 | 0.80 | 0.63 | 0.17 | 967 | 574k | 0.64 / 2.22 | 5.4 MB (12.31) | 1,452 (0.00 s, 2.4/write) |
| backfill | per-batch | muninn (post-0009 rerun) | 610 | 460,135 | 0.73 | 0.56 | 0.17 | 1,093 | 631k | 0.62 / 1.62 | 5.4 MB (12.31) | 1,448 (0.00 s, 2.4/write) |
| backfill | per-batch | muninn (post-fixes) | 610 | 460,135 | 0.61 | 0.44 | 0.17 | 1,380 | 749k | 0.40 / 1.15 | 2.8 MB (6.28) | 1,456 (0.00 s, 2.4/write) |
| backfill | per-batch | muninn-wm64 (post-0009) | 610 | 460,135 | 1.18 | 0.99 | 0.19 | 614 | 389k | 103.16 / 124.45 | 16.6 MB (37.72) | 89 (0.00 s, 0.1/write) |
| backfill | per-batch | muninn-wm64 (post-0009 rerun) | 610 | 460,135 | 1.18 | 0.98 | 0.19 | 620 | 392k | 102.62 / 123.75 | 16.6 MB (37.72) | 89 (0.00 s, 0.1/write) |
| backfill | per-batch | muninn-wm64 (post-fixes) | 610 | 460,135 | 0.42 | 0.23 | 0.19 | 2,675 | 1.09M | 21.31 / 41.32 | 2.7 MB (6.19) | 98 (0.00 s, 0.2/write) |
| backfill | per-batch | muninn-8p (post-0009) | 610 | 460,135 | 0.44 | 0.27 | 0.17 | 2,259 | 1.04M | 3.05 / 9.52 | 10.8 MB (24.59) | 454 (0.00 s, 0.7/write) |
| backfill | per-batch | duckdb-native | 610 | 460,135 | 1.30 | 1.28 | 0.02 | 477 | 354k | 1.93 / 4.82 | 7.8 MB (17.69) | 618 (0.00 s, 1.0/write) |
| backfill | bulk | muninn (pre-0009) | 1 | 460,135 | 0.81 | 0.81 | 0.00 | - | 571k | - | 2.7 MB (6.18) | 30 (0.00 s) |
| backfill | bulk | muninn (post-0009) | 1 | 460,135 | 0.33 | 0.33 | 0.00 | - | 1.40M | - | 2.7 MB (6.18) | 30 (0.00 s) |
| backfill | bulk | muninn (post-0009 rerun) | 1 | 460,135 | 0.33 | 0.33 | 0.00 | - | 1.40M | - | 2.7 MB (6.18) | 30 (0.00 s) |
| backfill | bulk | muninn (post-fixes) | 1 | 460,135 | 0.12 | 0.12 | 0.00 | - | 3.90M | - | 2.7 MB (6.19) | 39 (0.00 s) |
| backfill | bulk | muninn-100k (pre-0009) | 5 | 460,135 | 0.70 | 0.54 | 0.16 | 9.2 | 655k | 104.97 / 139.55 | 2.7 MB (6.22) | 105 (0.00 s, 21.0/write) |
| backfill | bulk | muninn-100k (post-0009) | 5 | 460,135 | 0.49 | 0.33 | 0.16 | 15.1 | 945k | 70.08 / 76.13 | 2.7 MB (6.22) | 105 (0.00 s, 21.0/write) |
| backfill | bulk | muninn-100k (post-0009 rerun) | 5 | 460,135 | 0.48 | 0.33 | 0.16 | 15.3 | 953k | 71.13 / 75.16 | 2.7 MB (6.22) | 105 (0.00 s, 21.0/write) |
| backfill | bulk | muninn-100k (post-fixes) | 5 | 460,135 | 0.29 | 0.14 | 0.15 | 36.2 | 1.57M | 31.11 / 35.43 | 2.7 MB (6.19) | 114 (0.00 s, 22.8/write) |
| backfill | bulk | duckdb-native | 1 | 460,135 | 0.14 | 0.14 | 0.00 | - | 3.22M | - | 8.3 MB (18.83) | 6 (0.00 s) |

Muninn per-batch, inside `write` only (excludes the periodic compact+gc): mean latency and the part of it spent in fsync, per write. The counters are process-wide: post-0009, fsyncs of a background flush that overlap a write are attributed to it (which can make 'other' negative).

| dataset | system | writes | mean ms/write | fsyncs/write | segment file+dir fsync ms | log file+dir fsync ms | other ms (encode, dedup, file writes, Python) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | muninn (pre-0009) | 7,723 | 0.23 | 4.0 | 0.00 | 0.00 | 0.23 |
| da_prices | muninn (post-0009) | 7,723 | 0.15 | 2.0 | 0.00 | 0.00 | 0.15 |
| da_prices | muninn (post-0009 rerun) | 7,723 | 0.15 | 2.0 | 0.00 | 0.00 | 0.15 |
| da_prices | muninn (post-fixes) | 7,723 | 0.13 | 2.0 | 0.00 | 0.00 | 0.13 |
| forecasts | muninn (pre-0009) | 2,857 | 2.73 | 24.0 | 0.01 | 0.00 | 2.72 |
| forecasts | muninn (post-0009) | 2,857 | 0.93 | 2.6 | 0.00 | 0.00 | 0.93 |
| forecasts | muninn (post-0009 rerun) | 2,857 | 0.91 | 2.6 | 0.00 | 0.00 | 0.90 |
| forecasts | muninn (post-fixes) | 2,857 | 0.52 | 2.5 | 0.00 | 0.00 | 0.52 |
| settlement | muninn (pre-0009) | 10,000 | 0.30 | 4.0 | 0.00 | 0.00 | 0.30 |
| settlement | muninn (post-0009) | 10,000 | 0.20 | 2.0 | 0.00 | 0.00 | 0.20 |
| settlement | muninn (post-0009 rerun) | 10,000 | 0.20 | 2.0 | 0.00 | 0.00 | 0.20 |
| settlement | muninn (post-fixes) | 10,000 | 0.16 | 2.0 | 0.00 | 0.00 | 0.16 |
| sparse | muninn (pre-0009) | 10,000 | 0.18 | 4.0 | 0.00 | 0.00 | 0.18 |
| sparse | muninn (post-0009) | 10,000 | 0.12 | 2.0 | 0.00 | 0.00 | 0.12 |
| backfill | muninn (pre-0009) | 610 | 0.86 | 4.2 | 0.00 | 0.00 | 0.86 |
| backfill | muninn (post-0009) | 610 | 0.77 | 2.1 | 0.00 | 0.00 | 0.77 |
| backfill | muninn (post-0009 rerun) | 610 | 0.65 | 2.1 | 0.00 | 0.00 | 0.65 |
| backfill | muninn (post-fixes) | 610 | 0.47 | 2.1 | 0.00 | 0.00 | 0.47 |

## Runs

- **muninn (pre-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 1,000,000 rows, compact()+gc() at the end.
- **muninn-100k (pre-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 100,000 rows, compact()+gc() at the end.
- **duckdb-native** (1.5.6): DuckDB 1.5.6, threads=6, memory_limit=4096MB, layout=native, latest=argmax, grid=asof. Cold = fresh connection (DuckDB buffer pool and Parquet metadata empty); the OS page cache is not dropped (no root).
- **clickhouse** (26.7.34.3): ClickHouse server (official static binary), max_threads=6, max_memory_usage=4096MB, MergeTree, query cache off. Cold = SYSTEM DROP MARK/UNCOMPRESSED/MMAP/QUERY CACHE (OS page cache not dropped).
- **clickhouse-fsync** (26.7.34.3): ClickHouse server (official static binary), max_threads=6, max_memory_usage=4096MB, MergeTree, query cache off. Cold = SYSTEM DROP MARK/UNCOMPRESSED/MMAP/QUERY CACHE (OS page cache not dropped). Table setting fsync_after_insert=1, fsync_part_directory=1.
- **muninn (post-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 1,000,000 rows, compact()+gc() at the end.
- **muninn-wm64 (post-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 1,000,000 rows, compact()+gc() at the end.
- **muninn-8p (post-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 1,000,000 rows, compact()+gc() at the end.
- **muninn-100k (post-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 100,000 rows, compact()+gc() at the end.
- **muninn (post-0009 rerun)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 1,000,000 rows, compact()+gc() at the end.
- **muninn-wm64 (post-0009 rerun)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 1,000,000 rows, compact()+gc() at the end.
- **muninn-100k (post-0009 rerun)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 100,000 rows, compact()+gc() at the end.
- **muninn (post-fixes)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 1,000,000 rows, compact()+gc() at the end.
- **muninn-wm64 (post-fixes)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 1,000,000 rows, compact()+gc() at the end.
- **muninn-100k (post-fixes)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 100,000 rows, compact()+gc() at the end.

Run dates: 2026-10-19, 2026-10-20.
