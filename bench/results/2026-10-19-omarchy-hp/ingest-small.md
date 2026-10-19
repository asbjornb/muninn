# Muninn ingest benchmark — small scale

AMD Ryzen 5 PRO 5650U with Radeon Graphics, 12 logical CPUs, 14.9 GB RAM, Linux-7.2.3-arch1-3-x86_64-with-glibc2.44. Produced by `bench/ingest.py` (see its docstring and `bench/results/README.md`, section Ingest).

* **per-batch**: one transaction/commit per original write batch, in arrival order (Muninn `write`; DuckDB `BEGIN/INSERT/COMMIT` on a database file; ClickHouse one synchronous `INSERT` per batch, `clickhouse-fsync` with `fsync_after_insert=1`).
* **bulk**: as few writes as possible (DuckDB one `CREATE TABLE AS ... ORDER BY`, ClickHouse one `INSERT ... SELECT`, Muninn writes of 1M rows).
* Times include the system's finalisation (Muninn compact+gc, DuckDB CHECKPOINT, ClickHouse OPTIMIZE FINAL); **writes/s** is over the write phase, **rows/s** over the whole wall time. **prefix** = only the first N batches were loaded (same N for every system), rates are for that prefix. Size = on disk after finalisation.
* **fsyncs**: calls to fsync/fdatasync in the benchmark process and the time spent in them (`bench/fsyncstat.c`); ClickHouse runs as its own server process, so not counted.

## Storage: btrfs (`/home/abr/Projects/muninn/.claude/worktrees/agent-a52a9f0264c1178b5/bench/data/work`)

| dataset | mode | system | writes | rows | wall s | write s | finalize s | writes/s | rows/s | per write p50 / p99 ms | size (B/version) | fsyncs (time) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | per-batch | muninn (pre-0009) | 7,723 | 453,060 | 69 | 69 | 0.10 | 112 | 6,586 | 4.10 / 21.32 | 1.7 MB (4.04) | 31,399 (61 s, 4.1/write) |
| da_prices | per-batch | duckdb-native | 7,723 | 453,060 | 21 | 21 | 0.04 | 364 | 21k | 2.43 / 6.40 | 4.5 MB (10.44) | 7,731 (8.47 s, 1.0/write) |
| da_prices | per-batch | clickhouse | 7,723 | 453,060 | 17 | 17 | 0.08 | 456 | 27k | 2.13 / 3.23 | 1.5 MB (3.54) | n/a |
| da_prices | per-batch | clickhouse-fsync | 7,723 | 453,060 | 133 | 132 | 0.09 | 58.3 | 3,419 | 21.90 / 38.51 | 1.5 MB (3.54) | n/a |
| da_prices | bulk | muninn (pre-0009) | 1 | 453,060 | 0.87 | 0.85 | 0.02 | - | 521k | - | 1.6 MB (3.6) | 51 (0.21 s) |
| da_prices | bulk | muninn-100k (pre-0009) | 5 | 453,060 | 0.87 | 0.69 | 0.17 | 7.2 | 523k | 136.58 / 198.77 | 1.6 MB (3.62) | 89 (0.36 s, 17.8/write) |
| da_prices | bulk | duckdb-native | 1 | 453,060 | 0.18 | 0.18 | 0.00 | - | 2.50M | - | 4.8 MB (11.02) | 6 (0.04 s) |
| da_prices | bulk | clickhouse | 1 | 453,060 | 0.15 | 0.15 | 0.00 | - | 2.96M | - | 1.5 MB (3.55) | n/a |
| da_prices | bulk | clickhouse-fsync | 1 | 453,060 | 0.19 | 0.19 | 0.00 | - | 2.34M | - | 1.5 MB (3.55) | n/a |
| forecasts | per-batch | muninn (pre-0009) | 2,857 | 2,741,768 | 188 | 186 | 2.13 | 15.4 | 15k | 28.05 / 122.29 | 10.7 MB (4.08) | 71,436 (164 s, 25.0/write) |
| forecasts | per-batch | duckdb-native | 2,857 | 2,741,768 | 12 | 12 | 0.05 | 231 | 221k | 3.17 / 10.20 | 22.3 MB (8.51) | 2,889 (5.36 s, 1.0/write) |
| forecasts | per-batch | clickhouse | 2,857 | 2,741,768 | 9.22 | 8.81 | 0.41 | 324 | 297k | 2.90 / 6.03 | 8.0 MB (3.06) | n/a |
| forecasts | per-batch | clickhouse-fsync | 2,857 | 2,741,768 | 57 | 57 | 0.49 | 50.5 | 48k | 11.58 / 79.68 | 8.0 MB (3.06) | n/a |
| forecasts | bulk | muninn (pre-0009) | 3 | 2,741,768 | 8.93 | 8.88 | 0.05 | 0.3 | 307k | 2914.72 / 3560.20 | 7.3 MB (2.78) | 280 (0.34 s, 93.3/write) |
| forecasts | bulk | muninn-100k (pre-0009) | 28 | 2,741,768 | 4.43 | 4.25 | 0.18 | 6.6 | 619k | 150.12 / 192.07 | 7.7 MB (2.94) | 974 (0.75 s, 34.8/write) |
| forecasts | bulk | duckdb-native | 1 | 2,741,768 | 0.67 | 0.67 | 0.00 | - | 4.07M | - | 67.5 MB (25.82) | 6 (0.02 s) |
| forecasts | bulk | clickhouse | 1 | 2,741,768 | 1.35 | 1.35 | 0.00 | - | 2.03M | - | 8.0 MB (3.06) | n/a |
| forecasts | bulk | clickhouse-fsync | 1 | 2,741,768 | 1.33 | 1.33 | 0.00 | - | 2.07M | - | 8.0 MB (3.06) | n/a |
| settlement | per-batch | muninn (pre-0009) | 10,000 | 960,000 | 142 | 142 | 0.00 | 70.2 | 6,737 | 15.85 / 30.55 | 3.6 MB (3.94) | 40,682 (131 s, 4.1/write) |
| settlement | per-batch | duckdb-native | 10,000 | 960,000 | 59 | 59 | 0.15 | 170 | 16k | 5.93 / 14.76 | 12.0 MB (13.12) | 10,014 (35 s, 1.0/write) |
| settlement | per-batch | clickhouse | 10,000 | 960,000 | 36 | 35 | 0.25 | 282 | 27k | 2.96 / 9.09 | 3.2 MB (3.47) | n/a |
| settlement | per-batch | clickhouse-fsync | 10,000 | 960,000 | 210 | 209 | 0.22 | 47.7 | 4,578 | 22.91 / 46.34 | 3.2 MB (3.47) | n/a |
| settlement | bulk | muninn (pre-0009) | 1 | 960,000 | 1.88 | 1.87 | 0.01 | - | 510k | - | 3.1 MB (3.35) | 51 (0.15 s) |
| settlement | bulk | muninn-100k (pre-0009) | 10 | 960,000 | 1.37 | 1.27 | 0.10 | 7.9 | 702k | 130.41 / 137.91 | 3.3 MB (3.56) | 140 (0.13 s, 14.0/write) |
| settlement | bulk | duckdb-native | 1 | 960,000 | 0.36 | 0.36 | 0.00 | - | 2.63M | - | 25.5 MB (27.87) | 6 (0.02 s) |
| settlement | bulk | clickhouse | 1 | 960,000 | 0.30 | 0.30 | 0.00 | - | 3.17M | - | 3.2 MB (3.48) | n/a |
| settlement | bulk | clickhouse-fsync | 1 | 960,000 | 0.34 | 0.34 | 0.00 | - | 2.86M | - | 3.2 MB (3.48) | n/a |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | muninn (pre-0009) | 10,000 | 10,000 | 111 | 111 | 0.06 | 90.1 | 90.0 | 15.38 / 22.10 | 7.3 MB (760.77) | 40,257 (101 s, 4.0/write) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | duckdb-native | 10,000 | 10,000 | 57 | 57 | 0.03 | 174 | 174 | 5.65 / 17.33 | 780.0 KB (79.87) | 10,005 (34 s, 1.0/write) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | clickhouse | 10,000 | 10,000 | 26 | 26 | 0.02 | 387 | 387 | 2.14 / 7.97 | 326.2 KB (33.4) | n/a |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | clickhouse-fsync | 10,000 | 10,000 | 234 | 234 | 0.03 | 42.8 | 43.0 | 23.32 / 50.58 | 326.2 KB (33.4) | n/a |
| sparse | bulk | muninn (pre-0009) | 1 | 434,191 | 0.98 | 0.94 | 0.03 | - | 445k | - | 17.4 MB (41.93) | 54 (0.06 s) |
| sparse | bulk | muninn-100k (pre-0009) | 5 | 434,191 | 1.05 | 0.88 | 0.18 | 5.7 | 412k | 151.72 / 158.72 | 17.4 MB (41.99) | 116 (0.12 s, 23.2/write) |
| sparse | bulk | duckdb-native | 1 | 434,191 | 0.21 | 0.21 | 0.00 | - | 2.03M | - | 15.3 MB (36.86) | 6 (0.02 s) |
| sparse | bulk | clickhouse | 1 | 434,191 | 0.40 | 0.40 | 0.00 | - | 1.10M | - | 12.9 MB (31.07) | n/a |
| sparse | bulk | clickhouse-fsync | 1 | 434,191 | 0.42 | 0.42 | 0.00 | - | 1.04M | - | 12.9 MB (31.07) | n/a |
| backfill | per-batch | muninn (pre-0009) | 610 | 460,135 | 3.08 | 2.84 | 0.24 | 215 | 150k | 3.90 / 9.89 | 2.8 MB (6.44) | 2,656 (1.92 s, 4.4/write) |
| backfill | per-batch | duckdb-native | 610 | 460,135 | 2.01 | 1.99 | 0.02 | 307 | 229k | 3.06 / 4.50 | 7.8 MB (17.69) | 618 (0.64 s, 1.0/write) |
| backfill | per-batch | clickhouse | 610 | 460,135 | 1.69 | 1.60 | 0.08 | 381 | 273k | 2.47 / 4.24 | 3.4 MB (7.76) | n/a |
| backfill | per-batch | clickhouse-fsync | 610 | 460,135 | 4.91 | 4.82 | 0.09 | 126 | 94k | 7.58 / 15.15 | 3.4 MB (7.76) | n/a |
| backfill | bulk | muninn (pre-0009) | 1 | 460,135 | 0.58 | 0.58 | 0.00 | - | 791k | - | 2.7 MB (6.18) | 30 (0.03 s) |
| backfill | bulk | muninn-100k (pre-0009) | 5 | 460,135 | 0.72 | 0.52 | 0.19 | 9.6 | 643k | 112.10 / 115.75 | 2.7 MB (6.22) | 105 (0.07 s, 21.0/write) |
| backfill | bulk | duckdb-native | 1 | 460,135 | 0.16 | 0.16 | 0.00 | - | 2.91M | - | 8.3 MB (18.83) | 6 (0.01 s) |
| backfill | bulk | clickhouse | 1 | 460,135 | 0.21 | 0.21 | 0.00 | - | 2.19M | - | 3.4 MB (7.76) | n/a |
| backfill | bulk | clickhouse-fsync | 1 | 460,135 | 0.22 | 0.22 | 0.00 | - | 2.10M | - | 3.4 MB (7.76) | n/a |

Muninn per-batch, inside `write` only (excludes the periodic compact+gc): mean latency and the part of it spent in fsync, per write.

| dataset | system | writes | mean ms/write | fsyncs/write | segment file+dir fsync ms | log file+dir fsync ms | other ms (encode, dedup, file writes, Python) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | muninn (pre-0009) | 7,723 | 8.58 | 4.0 | 3.83 | 3.94 | 0.82 |
| forecasts | muninn (pre-0009) | 2,857 | 60.78 | 24.0 | 49.42 | 4.80 | 6.57 |
| settlement | muninn (pre-0009) | 10,000 | 13.85 | 4.0 | 6.39 | 6.53 | 0.93 |
| sparse | muninn (pre-0009) | 10,000 | 10.61 | 4.0 | 4.96 | 5.03 | 0.62 |
| backfill | muninn (pre-0009) | 610 | 4.22 | 4.2 | 1.58 | 1.39 | 1.25 |

## Storage: tmpfs (`/dev/shm/muninn-ingest`)

| dataset | mode | system | writes | rows | wall s | write s | finalize s | writes/s | rows/s | per write p50 / p99 ms | size (B/version) | fsyncs (time) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | per-batch | muninn (pre-0009) | 7,723 | 453,060 | 2.33 | 2.31 | 0.02 | 3,340 | 194k | 0.24 / 0.49 | 1.7 MB (4.04) | 31,399 (0.02 s, 4.1/write) |
| da_prices | per-batch | duckdb-native | 7,723 | 453,060 | 12 | 12 | 0.03 | 624 | 37k | 1.56 / 2.10 | 4.5 MB (10.44) | 7,731 (0.01 s, 1.0/write) |
| da_prices | bulk | muninn (pre-0009) | 1 | 453,060 | 0.67 | 0.67 | 0.00 | - | 678k | - | 1.6 MB (3.6) | 51 (0.00 s) |
| da_prices | bulk | muninn-100k (pre-0009) | 5 | 453,060 | 0.50 | 0.44 | 0.06 | 11.4 | 901k | 96.80 / 98.08 | 1.6 MB (3.62) | 89 (0.00 s, 17.8/write) |
| da_prices | bulk | duckdb-native | 1 | 453,060 | 0.15 | 0.15 | 0.00 | - | 3.04M | - | 4.8 MB (11.02) | 6 (0.00 s) |
| forecasts | per-batch | muninn (pre-0009) | 2,857 | 2,741,768 | 10 | 9.76 | 0.26 | 293 | 274k | 2.39 / 6.47 | 10.7 MB (4.08) | 71,436 (0.04 s, 25.0/write) |
| forecasts | per-batch | duckdb-native | 2,857 | 2,741,768 | 6.91 | 6.85 | 0.05 | 417 | 397k | 2.17 / 3.44 | 22.3 MB (8.51) | 2,889 (0.00 s, 1.0/write) |
| forecasts | bulk | muninn (pre-0009) | 3 | 2,741,768 | 7.33 | 7.31 | 0.01 | 0.4 | 374k | 2485.64 / 3198.01 | 7.3 MB (2.78) | 280 (0.00 s, 93.3/write) |
| forecasts | bulk | muninn-100k (pre-0009) | 28 | 2,741,768 | 3.41 | 3.35 | 0.07 | 8.4 | 803k | 118.53 / 169.08 | 7.7 MB (2.94) | 974 (0.00 s, 34.8/write) |
| forecasts | bulk | duckdb-native | 1 | 2,741,768 | 0.57 | 0.57 | 0.00 | - | 4.83M | - | 67.5 MB (25.82) | 6 (0.00 s) |
| settlement | per-batch | muninn (pre-0009) | 10,000 | 960,000 | 4.03 | 4.03 | 0.00 | 2,481 | 238k | 0.29 / 0.54 | 3.6 MB (3.94) | 40,682 (0.02 s, 4.1/write) |
| settlement | per-batch | duckdb-native | 10,000 | 960,000 | 18 | 18 | 0.09 | 566 | 54k | 1.70 / 2.57 | 12.0 MB (13.12) | 10,014 (0.01 s, 1.0/write) |
| settlement | bulk | muninn (pre-0009) | 1 | 960,000 | 1.59 | 1.59 | 0.00 | - | 604k | - | 3.1 MB (3.35) | 51 (0.00 s) |
| settlement | bulk | muninn-100k (pre-0009) | 10 | 960,000 | 1.27 | 1.20 | 0.07 | 8.3 | 754k | 121.11 / 136.60 | 3.3 MB (3.56) | 140 (0.00 s, 14.0/write) |
| settlement | bulk | duckdb-native | 1 | 960,000 | 0.36 | 0.36 | 0.00 | - | 2.67M | - | 25.5 MB (27.87) | 6 (0.00 s) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | muninn (pre-0009) | 10,000 | 10,000 | 4.55 | 4.49 | 0.07 | 2,229 | 2,195 | 0.17 / 0.29 | 7.3 MB (760.77) | 40,257 (0.02 s, 4.0/write) |
| sparse | per-batch (prefix: 10,000 of 434,191 batches) | duckdb-native | 10,000 | 10,000 | 16 | 16 | 0.01 | 611 | 611 | 1.60 / 2.14 | 780.0 KB (79.87) | 10,005 (0.01 s, 1.0/write) |
| sparse | bulk | muninn (pre-0009) | 1 | 434,191 | 1.04 | 1.00 | 0.03 | - | 419k | - | 17.4 MB (41.93) | 54 (0.00 s) |
| sparse | bulk | muninn-100k (pre-0009) | 5 | 434,191 | 0.97 | 0.84 | 0.14 | 6.0 | 446k | 140.98 / 144.79 | 17.4 MB (41.99) | 116 (0.00 s, 23.2/write) |
| sparse | bulk | duckdb-native | 1 | 434,191 | 0.21 | 0.21 | 0.00 | - | 2.04M | - | 15.3 MB (36.86) | 6 (0.00 s) |
| backfill | per-batch | muninn (pre-0009) | 610 | 460,135 | 0.84 | 0.69 | 0.16 | 891 | 545k | 0.82 / 2.01 | 2.8 MB (6.44) | 2,656 (0.00 s, 4.4/write) |
| backfill | per-batch | duckdb-native | 610 | 460,135 | 1.30 | 1.28 | 0.02 | 477 | 354k | 1.93 / 4.82 | 7.8 MB (17.69) | 618 (0.00 s, 1.0/write) |
| backfill | bulk | muninn (pre-0009) | 1 | 460,135 | 0.81 | 0.81 | 0.00 | - | 571k | - | 2.7 MB (6.18) | 30 (0.00 s) |
| backfill | bulk | muninn-100k (pre-0009) | 5 | 460,135 | 0.70 | 0.54 | 0.16 | 9.2 | 655k | 104.97 / 139.55 | 2.7 MB (6.22) | 105 (0.00 s, 21.0/write) |
| backfill | bulk | duckdb-native | 1 | 460,135 | 0.14 | 0.14 | 0.00 | - | 3.22M | - | 8.3 MB (18.83) | 6 (0.00 s) |

Muninn per-batch, inside `write` only (excludes the periodic compact+gc): mean latency and the part of it spent in fsync, per write.

| dataset | system | writes | mean ms/write | fsyncs/write | segment file+dir fsync ms | log file+dir fsync ms | other ms (encode, dedup, file writes, Python) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| da_prices | muninn (pre-0009) | 7,723 | 0.23 | 4.0 | 0.00 | 0.00 | 0.23 |
| forecasts | muninn (pre-0009) | 2,857 | 2.73 | 24.0 | 0.01 | 0.00 | 2.72 |
| settlement | muninn (pre-0009) | 10,000 | 0.30 | 4.0 | 0.00 | 0.00 | 0.30 |
| sparse | muninn (pre-0009) | 10,000 | 0.18 | 4.0 | 0.00 | 0.00 | 0.18 |
| backfill | muninn (pre-0009) | 610 | 0.86 | 4.2 | 0.00 | 0.00 | 0.86 |

## Runs

- **muninn (pre-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 1,000,000 rows, compact()+gc() at the end.
- **duckdb-native** (1.5.6): DuckDB 1.5.6, threads=6, memory_limit=4096MB, layout=native, latest=argmax, grid=asof. Cold = fresh connection (DuckDB buffer pool and Parquet metadata empty); the OS page cache is not dropped (no root).
- **clickhouse** (26.7.34.3): ClickHouse server (official static binary), max_threads=6, max_memory_usage=4096MB, MergeTree, query cache off. Cold = SYSTEM DROP MARK/UNCOMPRESSED/MMAP/QUERY CACHE (OS page cache not dropped).
- **clickhouse-fsync** (26.7.34.3): ClickHouse server (official static binary), max_threads=6, max_memory_usage=4096MB, MergeTree, query cache off. Cold = SYSTEM DROP MARK/UNCOMPRESSED/MMAP/QUERY CACHE (OS page cache not dropped). Table setting fsync_after_insert=1, fsync_part_directory=1.
- **muninn-100k (pre-0009)** (0.1.0): Muninn engine (Python binding, release build), local directory store; one write per original batch (coalesced for datasets with > 200,000 batches), compact()+gc() every 500 writes and at the end. Cold = database reopened (OS page cache not dropped). Engine thread count not configurable. Bulk ingest mode (bench/ingest.py): writes of 100,000 rows, compact()+gc() at the end.

Run dates: 2026-10-19.
