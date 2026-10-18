# Roadmap (rough)

Not started. Order is "each step is usable on its own and de-risks the next".

## 0. Prove the semantics before the engine

- Write the time model as an executable spec: a tiny reference implementation (Python or
  DuckDB SQL over Parquet) of "latest-known as of frontier", including corrections,
  tombstones, backfills, forecast issues, ties.
- A test-suite of scenarios with expected answers. Every later engine must pass it.
- Run it on real data (some energy forecasts with revisions) to see if the model holds.

This step might reveal that a DuckDB extension or a thin layer over Parquet is enough.
That would be a fine outcome.

## 1. On-disk format

- Column files, segment footer, manifests, WAL, commit markers.
- Readers: open + mmap + full scan.

## 2. Single-writer ingestion

- Append, idempotency, crash recovery, CSV/Parquet bulk import.

## 3. Pruning

- Partition pruning, zone maps on time columns, series bloom.

## 4. Point-in-time reads

- `AS OF` frontier(s), latest-known, all-versions, replay ordering.
- Benchmarks against the step-0 reference + ClickHouse/DuckDB doing the same queries.

## 5. As-of join

- Per-row frontier merge join on `(series, value_time)`.

## 6. Python bindings + Arrow results

## 7. Compaction + retention

- Proven not to change any query result (property tests against step-0 spec).

## 8. Later

- Derived current-truth views, rollups, server mode (HTTP/Flight), frontier-advancing
  cursor for backtest loops, latency/QA stats.
