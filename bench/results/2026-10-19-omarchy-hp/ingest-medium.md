# Muninn ingest benchmark — medium scale

AMD Ryzen 5 PRO 5650U with Radeon Graphics, 12 logical CPUs, 14.9 GB RAM, Linux-7.2.3-arch1-3-x86_64-with-glibc2.44. Produced by `bench/ingest.py` (see its docstring and `bench/results/README.md`, section Ingest).

* **per-batch**: one transaction/commit per original write batch, in arrival order (Muninn `write`; DuckDB `BEGIN/INSERT/COMMIT` on a database file; ClickHouse one synchronous `INSERT` per batch, `clickhouse-fsync` with `fsync_after_insert=1`).
* **bulk**: as few writes as possible (DuckDB one `CREATE TABLE AS ... ORDER BY`, ClickHouse one `INSERT ... SELECT`, Muninn writes of 1M rows).
* Times include the system's finalisation (Muninn compact+gc, DuckDB CHECKPOINT, ClickHouse OPTIMIZE FINAL); **writes/s** is over the write phase, **rows/s** over the whole wall time. **prefix** = only the first N batches were loaded (same N for every system), rates are for that prefix. Size = on disk after finalisation.
* **fsyncs**: calls to fsync/fdatasync in the benchmark process and the time spent in them (`bench/fsyncstat.c`); ClickHouse runs as its own server process, so not counted. Summed over threads, so with background flushes the total can exceed the wall time.
* **Muninn builds**: `(pre-0009)` = before the fast write path (one segment file per touched partition plus a log entry per write, ~4-25 fsyncs); `(post-0009)` = ADR 0009 (rows inline in the commit entry, group commit, memtable, background flush; ~2 fsyncs per commit). Per-batch variants of the post-0009 build: `muninn-wm64` submits 64 original batches per `write_many` call (one durable commit per 64 batches, each batch still its own transaction and system_time), `muninn-8p` has 8 producer threads calling `write` (the engine groups concurrent writes). Neither has a competitor counterpart here: DuckDB and ClickHouse were measured with one client committing one batch at a time.

## Storage: btrfs (`/home/abr/Projects/muninn/.claude/worktrees/agent-a52a9f0264c1178b5/bench/data/work`)

| dataset | mode | system | writes | rows | wall s | write s | finalize s | writes/s | rows/s | per write p50 / p99 ms | size (B/version) | fsyncs (time) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | per-batch | muninn (pre-0009) | 30,960 | 1,808,140 | 494 | 494 | 0.15 | 62.7 | 3,659 | 15.99 / 28.05 | 6.7 MB (3.88) | 125,590 (462 s, 4.1/write) |
| da_prices | per-batch | muninn (post-0009) | 30,960 | 1,808,140 | 223 | 223 | 0.14 | 139 | 8,094 | 7.78 / 13.49 | 7.7 MB (4.45) | 63,806 (211 s, 2.1/write) |
| da_prices | per-batch | muninn-wm64 (post-0009) | 30,960 | 1,808,140 | 6.77 | 6.22 | 0.55 | 4,974 | 267k | 12.77 / 24.27 | 16.5 MB (9.55) | 1,191 (5.32 s, 0.0/write) |
| da_prices | per-batch | muninn-8p (post-0009) | 30,960 | 1,808,140 | 45 | 44 | 1.08 | 711 | 41k | 15.71 / 27.48 | 7.9 MB (4.6) | 15,875 (42 s, 0.5/write) |
| da_prices | per-batch | duckdb-native | 30,960 | 1,808,140 | 188 | 188 | 0.02 | 164 | 9,599 | 5.78 / 24.12 | 15.5 MB (9.0) | 30,983 (110 s, 1.0/write) |
| da_prices | per-batch | clickhouse | 30,960 | 1,808,140 | 75 | 74 | 0.32 | 416 | 24k | 2.25 / 4.49 | 6.0 MB (3.51) | n/a |
| da_prices | per-batch | clickhouse-fsync | 30,960 | 1,808,140 | 767 | 767 | 0.30 | 40.4 | 2,357 | 23.72 / 65.25 | 6.0 MB (3.51) | n/a |
| da_prices | bulk | muninn (pre-0009) | 2 | 1,808,140 | 3.01 | 2.96 | 0.05 | 0.7 | 600k | 1475.24 / 1765.85 | 6.2 MB (3.59) | 98 (0.08 s, 49.0/write) |
| da_prices | bulk | muninn (post-0009) | 2 | 1,808,140 | 1.81 | 1.73 | 0.08 | 1.1 | 997k | 852.97 / 1050.65 | 6.2 MB (3.59) | 98 (0.68 s, 49.0/write) |
| da_prices | bulk | muninn-100k (pre-0009) | 19 | 1,808,140 | 2.44 | 1.92 | 0.53 | 9.9 | 740k | 104.56 / 113.70 | 6.2 MB (3.61) | 238 (0.18 s, 12.5/write) |
| da_prices | bulk | muninn-100k (post-0009) | 19 | 1,808,140 | 2.54 | 1.74 | 0.80 | 10.9 | 713k | 94.32 / 98.22 | 6.4 MB (3.7) | 240 (1.10 s, 12.6/write) |
| da_prices | bulk | duckdb-native | 1 | 1,808,140 | 0.40 | 0.40 | 0.00 | - | 4.55M | - | 17.5 MB (10.16) | 6 (0.02 s) |
| da_prices | bulk | clickhouse | 1 | 1,808,140 | 0.71 | 0.71 | 0.00 | - | 2.53M | - | 6.1 MB (3.51) | n/a |
| da_prices | bulk | clickhouse-fsync | 1 | 1,808,140 | 0.69 | 0.69 | 0.00 | - | 2.61M | - | 6.1 MB (3.51) | n/a |
| forecasts | per-batch | muninn (pre-0009) | 26,030 | 24,975,472 | 2,099 | 2,099 | 0.14 | 12.4 | 12k | 98.08 / 134.88 | 75.2 MB (3.16) | 643,322 (1,897 s, 24.7/write) |
| forecasts | per-batch | muninn (post-0009) | 26,030 | 24,975,472 | 274 | 274 | 0.34 | 95.1 | 91k | 8.95 / 16.53 | 75.5 MB (3.17) | 77,437 (311 s, 3.0/write) |
| forecasts | per-batch | muninn-wm64 (post-0009) | 26,030 | 24,975,472 | 32 | 31 | 1.08 | 838 | 777k | 72.11 / 106.10 | 74.8 MB (3.14) | 11,097 (15 s, 0.4/write) |
| forecasts | per-batch | muninn-8p (post-0009) | 26,030 | 24,975,472 | 90 | 65 | 25 | 399 | 276k | 20.89 / 36.29 | 76.4 MB (3.21) | 30,594 (144 s, 1.2/write) |
| forecasts | per-batch | duckdb-native | 26,030 | 24,975,472 | 162 | 162 | 0.09 | 161 | 154k | 6.58 / 11.79 | 207.0 MB (8.69) | 26,308 (97 s, 1.0/write) |
| forecasts | per-batch | clickhouse | 26,030 | 24,975,472 | 99 | 90 | 8.69 | 290 | 254k | 3.07 / 8.56 | 72.4 MB (3.04) | n/a |
| forecasts | per-batch | clickhouse-fsync | 26,030 | 24,975,472 | 645 | 641 | 3.62 | 40.6 | 39k | 25.92 / 41.31 | 72.4 MB (3.04) | n/a |
| forecasts | bulk | muninn (pre-0009) | 25 | 24,975,472 | 60 | 60 | 0.05 | 0.4 | 413k | 2394.79 / 2783.86 | 65.1 MB (2.73) | 1,717 (1.42 s, 68.7/write) |
| forecasts | bulk | muninn (post-0009) | 25 | 24,975,472 | 62 | 62 | 0.04 | 0.4 | 402k | 2479.10 / 2683.99 | 65.1 MB (2.73) | 1,717 (2.40 s, 68.7/write) |
| forecasts | bulk | muninn-100k (pre-0009) | 250 | 24,975,472 | 36 | 36 | 0.09 | 7.0 | 698k | 142.98 / 153.67 | 69.8 MB (2.93) | 7,133 (5.28 s, 28.5/write) |
| forecasts | bulk | muninn-100k (post-0009) | 250 | 24,975,472 | 25 | 24 | 0.08 | 10.2 | 1.02M | 97.26 / 109.89 | 69.8 MB (2.93) | 7,133 (8.29 s, 28.5/write) |
| forecasts | bulk | duckdb-native | 1 | 24,975,472 | 4.87 | 4.87 | 0.00 | - | 5.13M | - | 568.0 MB (23.85) | 6 (0.03 s) |
| forecasts | bulk | clickhouse | 1 | 24,975,472 | 9.18 | 9.18 | 0.00 | - | 2.72M | - | 71.2 MB (2.99) | n/a |
| forecasts | bulk | clickhouse-fsync | 1 | 24,975,472 | 9.42 | 9.42 | 0.00 | - | 2.65M | - | 71.2 MB (2.99) | n/a |
| settlement | per-batch | muninn (pre-0009) | 40,000 | 3,840,000 | 579 | 579 | 0.00 | 69.0 | 6,627 | 15.53 / 22.43 | 13.6 MB (3.7) | 162,167 (533 s, 4.1/write) |
| settlement | per-batch | muninn (post-0009) | 40,000 | 3,840,000 | 325 | 325 | 0.00 | 123 | 12k | 7.86 / 13.73 | 14.9 MB (4.07) | 82,430 (302 s, 2.1/write) |
| settlement | per-batch | muninn-wm64 (post-0009) | 40,000 | 3,840,000 | 8.12 | 6.51 | 1.60 | 6,140 | 473k | 8.53 / 21.69 | 18.6 MB (5.09) | 1,548 (2.05 s, 0.0/write) |
| settlement | per-batch | muninn-8p (post-0009) | 40,000 | 3,840,000 | 69 | 66 | 2.50 | 606 | 56k | 17.02 / 26.94 | 17.9 MB (4.89) | 20,564 (64 s, 0.5/write) |
| settlement | per-batch | duckdb-native | 40,000 | 3,840,000 | 196 | 196 | 0.07 | 204 | 20k | 5.66 / 7.28 | 35.3 MB (9.63) | 40,044 (119 s, 1.0/write) |
| settlement | per-batch | clickhouse | 40,000 | 3,840,000 | 103 | 102 | 0.82 | 392 | 37k | 2.38 / 5.09 | 12.7 MB (3.46) | n/a |
| settlement | per-batch | clickhouse-fsync | 40,000 | 3,840,000 | 890 | 889 | 0.84 | 45.0 | 4,315 | 22.46 / 41.10 | 12.6 MB (3.45) | n/a |
| settlement | bulk | muninn (pre-0009) | 4 | 3,840,000 | 5.85 | 5.71 | 0.14 | 0.7 | 656k | 1403.30 / 1674.20 | 12.6 MB (3.45) | 84 (0.41 s, 21.0/write) |
| settlement | bulk | muninn (post-0009) | 4 | 3,840,000 | 3.95 | 3.80 | 0.14 | 1.1 | 973k | 993.48 / 1002.86 | 12.6 MB (3.45) | 84 (0.53 s, 21.0/write) |
| settlement | bulk | muninn-100k (pre-0009) | 39 | 3,840,000 | 7.47 | 5.86 | 1.61 | 6.7 | 514k | 153.20 / 160.73 | 13.4 MB (3.65) | 367 (1.01 s, 9.4/write) |
| settlement | bulk | muninn-100k (post-0009) | 39 | 3,840,000 | 6.21 | 4.60 | 1.61 | 8.5 | 619k | 118.91 / 131.54 | 13.4 MB (3.65) | 367 (1.11 s, 9.4/write) |
| settlement | bulk | duckdb-native | 1 | 3,840,000 | 0.68 | 0.68 | 0.00 | - | 5.67M | - | 101.8 MB (27.79) | 6 (0.02 s) |
| settlement | bulk | clickhouse | 1 | 3,840,000 | 1.55 | 1.55 | 0.00 | - | 2.48M | - | 12.7 MB (3.47) | n/a |
| settlement | bulk | clickhouse-fsync | 1 | 3,840,000 | 1.58 | 1.58 | 0.00 | - | 2.43M | - | 12.7 MB (3.47) | n/a |
| sparse | per-batch (prefix: 10,000 of 3,246,137 batches) | muninn (pre-0009) | 10,000 | 10,000 | 65 | 65 | 0.63 | 155 | 153 | 3.15 / 17.28 | 50.4 MB (5283.01) | 40,260 (38 s, 4.0/write) |
| sparse | per-batch (prefix: 10,000 of 3,246,137 batches) | muninn (post-0009) | 10,000 | 10,000 | 40 | 39 | 0.64 | 255 | 251 | 1.61 / 2.48 | 50.5 MB (5297.75) | 20,333 (15 s, 2.0/write) |
| sparse | per-batch (prefix: 10,000 of 3,246,137 batches) | muninn-wm64 (post-0009) | 10,000 | 10,000 | 3.08 | 2.68 | 0.40 | 3,733 | 3,245 | 2.81 / 9.44 | 75.9 MB (7963.9) | 340 (0.31 s, 0.0/write) |
| sparse | per-batch (prefix: 10,000 of 3,246,137 batches) | muninn-8p (post-0009) | 10,000 | 10,000 | 9.56 | 8.52 | 1.04 | 1,174 | 1,046 | 3.46 / 18.19 | 52.9 MB (5544.79) | 5,040 (5.78 s, 0.5/write) |
| sparse | per-batch (prefix: 10,000 of 3,246,137 batches) | duckdb-native | 10,000 | 10,000 | 45 | 45 | 0.02 | 222 | 222 | 5.37 / 8.76 | 780.0 KB (79.87) | 10,005 (27 s, 1.0/write) |
| sparse | per-batch (prefix: 10,000 of 3,246,137 batches) | clickhouse | 10,000 | 10,000 | 21 | 21 | 0.02 | 488 | 487 | 2.00 / 2.80 | 330.2 KB (33.82) | n/a |
| sparse | per-batch (prefix: 10,000 of 3,246,137 batches) | clickhouse-fsync | 10,000 | 10,000 | 189 | 189 | 0.01 | 53.0 | 53.0 | 21.75 / 35.72 | 330.2 KB (33.82) | n/a |
| sparse | bulk | muninn (pre-0009) | 4 | 3,246,139 | 9.87 | 9.03 | 0.85 | 0.4 | 329k | 2008.36 / 2129.30 | 132.9 MB (42.94) | 99 (0.26 s, 24.8/write) |
| sparse | bulk | muninn (post-0009) | 4 | 3,246,139 | 8.69 | 7.73 | 0.96 | 0.5 | 374k | 1624.44 / 1721.17 | 132.9 MB (42.94) | 99 (0.95 s, 24.8/write) |
| sparse | bulk | muninn-100k (pre-0009) | 33 | 3,246,139 | 13 | 10 | 2.70 | 3.2 | 251k | 246.24 / 261.29 | 134.2 MB (43.36) | 485 (0.50 s, 14.7/write) |
| sparse | bulk | muninn-100k (post-0009) | 33 | 3,246,139 | 12 | 9.56 | 2.74 | 3.5 | 264k | 224.34 / 247.22 | 134.2 MB (43.36) | 485 (0.56 s, 14.7/write) |
| sparse | bulk | duckdb-native | 1 | 3,246,139 | 0.86 | 0.86 | 0.00 | - | 3.79M | - | 113.0 MB (36.51) | 6 (0.04 s) |
| sparse | bulk | clickhouse | 1 | 3,246,139 | 3.13 | 3.13 | 0.00 | - | 1.04M | - | 97.7 MB (31.57) | n/a |
| sparse | bulk | clickhouse-fsync | 1 | 3,246,139 | 3.23 | 3.23 | 0.00 | - | 1.00M | - | 97.7 MB (31.57) | n/a |
| backfill | per-batch | muninn (pre-0009) | 3,660 | 2,760,303 | 56 | 55 | 1.24 | 66.5 | 49k | 16.73 / 64.82 | 16.6 MB (6.29) | 15,640 (42 s, 4.3/write) |
| backfill | per-batch | muninn (post-0009) | 3,660 | 2,760,303 | 30 | 29 | 1.26 | 127 | 92k | 2.64 / 49.57 | 20.3 MB (7.71) | 8,352 (20 s, 2.3/write) |
| backfill | per-batch | muninn-wm64 (post-0009) | 3,660 | 2,760,303 | 7.59 | 6.52 | 1.07 | 562 | 364k | 109.75 / 154.82 | 99.2 MB (37.7) | 305 (0.93 s, 0.1/write) |
| backfill | per-batch | muninn-8p (post-0009) | 3,660 | 2,760,303 | 8.17 | 7.01 | 1.16 | 522 | 338k | 7.63 / 65.21 | 24.4 MB (9.28) | 2,306 (2.47 s, 0.6/write) |
| backfill | per-batch | duckdb-native | 3,660 | 2,760,303 | 15 | 15 | 0.04 | 237 | 179k | 3.00 / 7.70 | 46.0 MB (17.48) | 3,689 (7.80 s, 1.0/write) |
| backfill | per-batch | clickhouse | 3,660 | 2,760,303 | 9.66 | 9.19 | 0.46 | 398 | 286k | 2.42 / 3.87 | 20.3 MB (7.72) | n/a |
| backfill | per-batch | clickhouse-fsync | 3,660 | 2,760,303 | 84 | 83 | 0.45 | 44.0 | 33k | 25.93 / 38.82 | 20.3 MB (7.72) | n/a |
| backfill | bulk | muninn (pre-0009) | 3 | 2,760,303 | 4.82 | 3.66 | 1.16 | 0.8 | 573k | 1264.43 / 1417.89 | 16.2 MB (6.17) | 81 (0.45 s, 27.0/write) |
| backfill | bulk | muninn (post-0009) | 3 | 2,760,303 | 3.50 | 2.45 | 1.06 | 1.2 | 788k | 775.60 / 934.79 | 16.2 MB (6.17) | 81 (0.22 s, 27.0/write) |
| backfill | bulk | muninn-100k (pre-0009) | 28 | 2,760,303 | 5.55 | 4.37 | 1.18 | 6.4 | 497k | 153.54 / 180.31 | 16.3 MB (6.2) | 381 (1.57 s, 13.6/write) |
| backfill | bulk | muninn-100k (post-0009) | 28 | 2,760,303 | 3.41 | 2.34 | 1.07 | 11.9 | 809k | 83.35 / 106.81 | 16.3 MB (6.2) | 381 (0.44 s, 13.6/write) |
| backfill | bulk | duckdb-native | 1 | 2,760,303 | 0.46 | 0.46 | 0.00 | - | 6.00M | - | 47.0 MB (17.86) | 6 (0.10 s) |
| backfill | bulk | clickhouse | 1 | 2,760,303 | 1.09 | 1.09 | 0.00 | - | 2.52M | - | 20.3 MB (7.72) | n/a |
| backfill | bulk | clickhouse-fsync | 1 | 2,760,303 | 1.26 | 1.26 | 0.00 | - | 2.18M | - | 20.3 MB (7.72) | n/a |

Muninn per-batch, inside `write` only (excludes the periodic compact+gc): mean latency and the part of it spent in fsync, per write. The counters are process-wide: post-0009, fsyncs of a background flush that overlap a write are attributed to it (which can make 'other' negative).

| dataset | system | writes | mean ms/write | fsyncs/write | segment file+dir fsync ms | log file+dir fsync ms | other ms (encode, dedup, file writes, Python) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | muninn (pre-0009) | 30,960 | 15.61 | 4.0 | 7.32 | 7.47 | 0.82 |
| da_prices | muninn (post-0009) | 30,960 | 6.96 | 2.0 | 0.00 | 6.69 | 0.27 |
| forecasts | muninn (pre-0009) | 26,030 | 76.18 | 23.9 | 63.74 | 6.14 | 6.30 |
| forecasts | muninn (post-0009) | 26,030 | 8.68 | 2.5 | 2.56 | 7.43 | -1.30 |
| settlement | muninn (pre-0009) | 40,000 | 14.09 | 4.0 | 6.54 | 6.68 | 0.88 |
| settlement | muninn (post-0009) | 40,000 | 7.76 | 2.0 | 0.00 | 7.40 | 0.36 |
| sparse | muninn (pre-0009) | 10,000 | 4.25 | 4.0 | 1.86 | 1.92 | 0.47 |
| sparse | muninn (post-0009) | 10,000 | 1.70 | 2.0 | 0.00 | 1.45 | 0.25 |
| backfill | muninn (pre-0009) | 3,660 | 13.50 | 4.2 | 5.68 | 5.44 | 2.38 |
| backfill | muninn (post-0009) | 3,660 | 6.43 | 2.1 | 0.30 | 4.72 | 1.41 |

## Storage: tmpfs (`/dev/shm/muninn-ingest`)

| dataset | mode | system | writes | rows | wall s | write s | finalize s | writes/s | rows/s | per write p50 / p99 ms | size (B/version) | fsyncs (time) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | per-batch | muninn (pre-0009) | 30,960 | 1,808,140 | 9.56 | 9.51 | 0.05 | 3,256 | 189k | 0.23 / 0.48 | 6.7 MB (3.88) | 125,590 (0.06 s, 4.1/write) |
| da_prices | per-batch | muninn (post-0009) | 30,960 | 1,808,140 | 7.14 | 7.08 | 0.06 | 4,370 | 253k | 0.16 / 0.23 | 7.7 MB (4.44) | 63,806 (0.03 s, 2.1/write) |
| da_prices | per-batch | muninn-wm64 (post-0009) | 30,960 | 1,808,140 | 2.20 | 1.93 | 0.27 | 16k | 822k | 4.21 / 9.68 | 14.0 MB (8.13) | 1,186 (0.00 s, 0.0/write) |
| da_prices | per-batch | muninn-8p (post-0009) | 30,960 | 1,808,140 | 3.08 | 2.32 | 0.77 | 13k | 586k | 0.54 / 1.49 | 8.5 MB (4.94) | 27,223 (0.02 s, 0.9/write) |
| da_prices | per-batch | duckdb-native | 30,960 | 1,808,140 | 48 | 48 | 0.01 | 651 | 38k | 1.51 / 1.78 | 15.5 MB (9.0) | 30,983 (0.02 s, 1.0/write) |
| da_prices | bulk | muninn (pre-0009) | 2 | 1,808,140 | 2.40 | 2.36 | 0.03 | 0.8 | 755k | 1179.85 / 1444.32 | 6.2 MB (3.59) | 98 (0.00 s, 49.0/write) |
| da_prices | bulk | muninn (post-0009) | 2 | 1,808,140 | 1.67 | 1.64 | 0.04 | 1.2 | 1.08M | 819.10 / 1009.54 | 6.2 MB (3.59) | 98 (0.00 s, 49.0/write) |
| da_prices | bulk | muninn-100k (pre-0009) | 19 | 1,808,140 | 2.25 | 1.80 | 0.44 | 10.5 | 805k | 99.89 / 102.53 | 6.2 MB (3.61) | 238 (0.00 s, 12.5/write) |
| da_prices | bulk | muninn-100k (post-0009) | 19 | 1,808,140 | 1.77 | 1.33 | 0.45 | 14.3 | 1.02M | 74.91 / 80.98 | 6.4 MB (3.7) | 240 (0.00 s, 12.6/write) |
| da_prices | bulk | duckdb-native | 1 | 1,808,140 | 0.33 | 0.33 | 0.00 | - | 5.50M | - | 17.5 MB (10.16) | 6 (0.00 s) |
| forecasts | per-batch | muninn (pre-0009) | 26,030 | 24,975,472 | 69 | 69 | 0.06 | 379 | 363k | 1.95 / 2.81 | 75.2 MB (3.16) | 643,322 (0.30 s, 24.7/write) |
| forecasts | per-batch | muninn (post-0009) | 26,030 | 24,975,472 | 31 | 31 | 0.06 | 829 | 794k | 0.84 / 3.00 | 75.5 MB (3.17) | 78,809 (0.04 s, 3.0/write) |
| forecasts | per-batch | muninn-wm64 (post-0009) | 26,030 | 24,975,472 | 30 | 29 | 0.59 | 897 | 843k | 68.91 / 89.45 | 74.8 MB (3.14) | 11,097 (0.01 s, 0.4/write) |
| forecasts | per-batch | muninn-8p (post-0009) | 26,030 | 24,975,472 | 19 | 16 | 3.50 | 1,671 | 1.31M | 4.07 / 16.80 | 77.9 MB (3.27) | 41,776 (0.03 s, 1.6/write) |
| forecasts | per-batch | duckdb-native | 26,030 | 24,975,472 | 57 | 57 | 0.06 | 460 | 441k | 1.97 / 3.08 | 207.3 MB (8.7) | 26,308 (0.02 s, 1.0/write) |
| forecasts | bulk | muninn (pre-0009) | 25 | 24,975,472 | 56 | 56 | 0.03 | 0.5 | 446k | 2225.60 / 2497.28 | 65.1 MB (2.73) | 1,717 (0.00 s, 68.7/write) |
| forecasts | bulk | muninn (post-0009) | 25 | 24,975,472 | 62 | 62 | 0.03 | 0.4 | 401k | 2480.05 / 2606.68 | 65.1 MB (2.73) | 1,717 (0.00 s, 68.7/write) |
| forecasts | bulk | muninn-100k (pre-0009) | 250 | 24,975,472 | 29 | 29 | 0.03 | 8.5 | 848k | 117.32 / 123.70 | 69.8 MB (2.93) | 7,133 (0.00 s, 28.5/write) |
| forecasts | bulk | muninn-100k (post-0009) | 250 | 24,975,472 | 23 | 23 | 0.03 | 11.0 | 1.10M | 90.43 / 95.53 | 69.8 MB (2.93) | 7,133 (0.01 s, 28.5/write) |
| forecasts | bulk | duckdb-native | 1 | 24,975,472 | 4.22 | 4.22 | 0.00 | - | 5.92M | - | 568.0 MB (23.85) | 6 (0.00 s) |
| settlement | per-batch | muninn (pre-0009) | 40,000 | 3,840,000 | 19 | 19 | 0.00 | 2,091 | 201k | 0.30 / 0.58 | 13.6 MB (3.7) | 162,167 (0.08 s, 4.1/write) |
| settlement | per-batch | muninn (post-0009) | 40,000 | 3,840,000 | 15 | 15 | 0.00 | 2,627 | 252k | 0.20 / 0.27 | 14.9 MB (4.07) | 82,430 (0.04 s, 2.1/write) |
| settlement | per-batch | muninn-wm64 (post-0009) | 40,000 | 3,840,000 | 6.19 | 4.72 | 1.47 | 8,469 | 620k | 6.21 / 16.38 | 18.6 MB (5.09) | 1,548 (0.00 s, 0.0/write) |
| settlement | per-batch | muninn-8p (post-0009) | 40,000 | 3,840,000 | 5.66 | 3.96 | 1.70 | 10k | 678k | 0.70 / 1.87 | 17.3 MB (4.72) | 35,106 (0.02 s, 0.9/write) |
| settlement | per-batch | duckdb-native | 40,000 | 3,840,000 | 68 | 68 | 0.06 | 592 | 57k | 1.64 / 2.17 | 35.5 MB (9.7) | 40,044 (0.03 s, 1.0/write) |
| settlement | bulk | muninn (pre-0009) | 4 | 3,840,000 | 5.14 | 5.04 | 0.09 | 0.8 | 747k | 1299.82 / 1351.43 | 12.6 MB (3.45) | 84 (0.00 s, 21.0/write) |
| settlement | bulk | muninn (post-0009) | 4 | 3,840,000 | 3.75 | 3.66 | 0.10 | 1.1 | 1.02M | 950.16 / 987.33 | 12.6 MB (3.45) | 84 (0.00 s, 21.0/write) |
| settlement | bulk | muninn-100k (pre-0009) | 39 | 3,840,000 | 6.35 | 4.87 | 1.48 | 8.0 | 604k | 125.27 / 135.25 | 13.4 MB (3.65) | 367 (0.00 s, 9.4/write) |
| settlement | bulk | muninn-100k (post-0009) | 39 | 3,840,000 | 5.43 | 3.91 | 1.52 | 10.0 | 708k | 102.09 / 112.12 | 13.4 MB (3.65) | 367 (0.00 s, 9.4/write) |
| settlement | bulk | duckdb-native | 1 | 3,840,000 | 0.63 | 0.63 | 0.00 | - | 6.08M | - | 101.8 MB (27.79) | 6 (0.00 s) |
| sparse | per-batch (prefix: 10,000 of 3,246,137 batches) | muninn (pre-0009) | 10,000 | 10,000 | 24 | 23 | 0.63 | 436 | 424 | 0.17 / 0.22 | 50.4 MB (5283.01) | 40,260 (0.02 s, 4.0/write) |
| sparse | per-batch (prefix: 10,000 of 3,246,137 batches) | muninn (post-0009) | 10,000 | 10,000 | 23 | 22 | 0.64 | 454 | 441 | 0.12 / 0.17 | 50.5 MB (5297.75) | 20,332 (0.01 s, 2.0/write) |
| sparse | per-batch (prefix: 10,000 of 3,246,137 batches) | muninn-wm64 (post-0009) | 10,000 | 10,000 | 2.82 | 2.42 | 0.40 | 4,132 | 3,542 | 1.24 / 1.48 | 75.9 MB (7963.9) | 340 (0.00 s, 0.0/write) |
| sparse | per-batch (prefix: 10,000 of 3,246,137 batches) | muninn-8p (post-0009) | 10,000 | 10,000 | 3.81 | 2.84 | 0.97 | 3,526 | 2,625 | 0.44 / 1.39 | 54.8 MB (5749.13) | 9,248 (0.01 s, 0.9/write) |
| sparse | per-batch (prefix: 10,000 of 3,246,137 batches) | duckdb-native | 10,000 | 10,000 | 16 | 16 | 0.00 | 644 | 644 | 1.53 / 1.77 | 780.0 KB (79.87) | 10,005 (0.01 s, 1.0/write) |
| sparse | bulk | muninn (pre-0009) | 4 | 3,246,139 | 9.44 | 8.63 | 0.81 | 0.5 | 344k | 1947.51 / 2011.71 | 132.9 MB (42.94) | 99 (0.00 s, 24.8/write) |
| sparse | bulk | muninn (post-0009) | 4 | 3,246,139 | 8.54 | 7.71 | 0.83 | 0.5 | 380k | 1657.20 / 1669.97 | 132.9 MB (42.94) | 99 (0.00 s, 24.8/write) |
| sparse | bulk | muninn-100k (pre-0009) | 33 | 3,246,139 | 12 | 9.85 | 2.52 | 3.4 | 263k | 238.27 / 249.37 | 134.2 MB (43.36) | 485 (0.00 s, 14.7/write) |
| sparse | bulk | muninn-100k (post-0009) | 33 | 3,246,139 | 12 | 9.40 | 2.61 | 3.5 | 270k | 218.84 / 241.72 | 134.2 MB (43.36) | 485 (0.00 s, 14.7/write) |
| sparse | bulk | duckdb-native | 1 | 3,246,139 | 0.78 | 0.78 | 0.00 | - | 4.14M | - | 113.0 MB (36.51) | 6 (0.00 s) |
| backfill | per-batch | muninn (pre-0009) | 3,660 | 2,760,303 | 11 | 10 | 0.98 | 353 | 243k | 0.85 / 43.01 | 16.6 MB (6.29) | 15,640 (0.01 s, 4.3/write) |
| backfill | per-batch | muninn (post-0009) | 3,660 | 2,760,303 | 11 | 9.54 | 0.99 | 384 | 262k | 0.64 / 41.52 | 20.3 MB (7.71) | 8,376 (0.00 s, 2.3/write) |
| backfill | per-batch | muninn-wm64 (post-0009) | 3,660 | 2,760,303 | 7.09 | 6.05 | 1.04 | 605 | 389k | 103.77 / 144.92 | 99.2 MB (37.7) | 305 (0.00 s, 0.1/write) |
| backfill | per-batch | muninn-8p (post-0009) | 3,660 | 2,760,303 | 6.17 | 5.10 | 1.07 | 718 | 447k | 3.54 / 61.43 | 25.3 MB (9.62) | 2,326 (0.00 s, 0.6/write) |
| backfill | per-batch | duckdb-native | 3,660 | 2,760,303 | 6.97 | 6.96 | 0.02 | 526 | 396k | 1.83 / 2.33 | 46.3 MB (17.57) | 3,689 (0.00 s, 1.0/write) |
| backfill | bulk | muninn (pre-0009) | 3 | 2,760,303 | 4.51 | 3.51 | 0.99 | 0.8 | 612k | 1247.31 / 1330.62 | 16.2 MB (6.17) | 81 (0.00 s, 27.0/write) |
| backfill | bulk | muninn (post-0009) | 3 | 2,760,303 | 3.36 | 2.36 | 1.00 | 1.3 | 821k | 742.67 / 931.71 | 16.2 MB (6.17) | 81 (0.00 s, 27.0/write) |
| backfill | bulk | muninn-100k (pre-0009) | 28 | 2,760,303 | 4.04 | 3.03 | 1.01 | 9.2 | 683k | 108.42 / 131.80 | 16.3 MB (6.2) | 381 (0.00 s, 13.6/write) |
| backfill | bulk | muninn-100k (post-0009) | 28 | 2,760,303 | 3.29 | 2.26 | 1.04 | 12.4 | 838k | 79.36 / 113.50 | 16.3 MB (6.2) | 381 (0.00 s, 13.6/write) |
| backfill | bulk | duckdb-native | 1 | 2,760,303 | 0.37 | 0.37 | 0.00 | - | 7.54M | - | 47.0 MB (17.86) | 6 (0.00 s) |

Muninn per-batch, inside `write` only (excludes the periodic compact+gc): mean latency and the part of it spent in fsync, per write. The counters are process-wide: post-0009, fsyncs of a background flush that overlap a write are attributed to it (which can make 'other' negative).

| dataset | system | writes | mean ms/write | fsyncs/write | segment file+dir fsync ms | log file+dir fsync ms | other ms (encode, dedup, file writes, Python) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | muninn (pre-0009) | 30,960 | 0.23 | 4.0 | 0.00 | 0.00 | 0.23 |
| da_prices | muninn (post-0009) | 30,960 | 0.15 | 2.0 | 0.00 | 0.00 | 0.15 |
| forecasts | muninn (pre-0009) | 26,030 | 2.00 | 23.9 | 0.01 | 0.00 | 1.99 |
| forecasts | muninn (post-0009) | 26,030 | 0.93 | 2.4 | 0.00 | 0.00 | 0.93 |
| settlement | muninn (pre-0009) | 40,000 | 0.31 | 4.0 | 0.00 | 0.00 | 0.31 |
| settlement | muninn (post-0009) | 40,000 | 0.20 | 2.0 | 0.00 | 0.00 | 0.20 |
| sparse | muninn (pre-0009) | 10,000 | 0.20 | 4.0 | 0.00 | 0.00 | 0.20 |
| sparse | muninn (post-0009) | 10,000 | 0.12 | 2.0 | 0.00 | 0.00 | 0.12 |
| backfill | muninn (pre-0009) | 3,660 | 1.75 | 4.2 | 0.00 | 0.00 | 1.74 |
| backfill | muninn (post-0009) | 3,660 | 1.52 | 2.1 | 0.00 | 0.00 | 1.52 |

## Runs

- **muninn (pre-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 1,000,000 rows, compact()+gc() at the end.
- **duckdb-native** (1.5.6): DuckDB 1.5.6, threads=6, memory_limit=4096MB, layout=native, latest=argmax, grid=asof. Cold = fresh connection (DuckDB buffer pool and Parquet metadata empty); the OS page cache is not dropped (no root).
- **clickhouse** (26.7.34.3): ClickHouse server (official static binary), max_threads=6, max_memory_usage=4096MB, MergeTree, query cache off. Cold = SYSTEM DROP MARK/UNCOMPRESSED/MMAP/QUERY CACHE (OS page cache not dropped).
- **clickhouse-fsync** (26.7.34.3): ClickHouse server (official static binary), max_threads=6, max_memory_usage=4096MB, MergeTree, query cache off. Cold = SYSTEM DROP MARK/UNCOMPRESSED/MMAP/QUERY CACHE (OS page cache not dropped). Table setting fsync_after_insert=1, fsync_part_directory=1.
- **muninn-100k (pre-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 100,000 rows, compact()+gc() at the end.
- **muninn (post-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 1,000,000 rows, compact()+gc() at the end.
- **muninn-wm64 (post-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 1,000,000 rows, compact()+gc() at the end.
- **muninn-8p (post-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 1,000,000 rows, compact()+gc() at the end.
- **muninn-100k (post-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 100,000 rows, compact()+gc() at the end.

Run dates: 2026-10-19, 2026-10-20.
