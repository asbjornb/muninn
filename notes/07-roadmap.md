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

- Benchmark harness + competitor baselines (ClickHouse, DuckDB/Parquet, XTDB, ArcticDB,
  SQL Server/Postgres). See `09-benchmarks.md`. Sets the bar before writing the engine.

Spikes that can run in parallel with step 0 (see `10-ambition.md`):

- Format + spec as the product: would an open spec + conformance suite work, and give value?
- Vintage-delta compression on real forecast data vs. ClickHouse/Parquet.
- Single-pass frontier grid in the reference implementation.

## 1. On-disk format

- Column files, segment footer, manifests, WAL, commit markers.
- Readers: open + mmap + full scan.

## 2. Single-writer ingestion

- Append, idempotency, crash recovery, CSV/Parquet bulk import.

## 3. Pruning

- Partition pruning, zone maps on time columns, series bloom.

## 4. Point-in-time reads

- `AS OF` frontier(s), latest-known, all-versions, replay ordering.
- Run the step-0 benchmark suite; compare against the baselines.

## 5. As-of join + frontier grid

- Per-row frontier merge join on `(series, value_time)`.
- Frontier grid (many frontiers resolved in one pass).

## 6. Python bindings + Arrow results

## 7. Compaction + retention + purge

- Proven not to change any query result (property tests against step-0 spec).
- Purge: physically remove a series/range (data licence ends). See `faq.md`.

## 8. Later

- Derived current-truth views, rollups, server mode (HTTP/Flight), frontier-advancing
  cursor for backtest loops, latency/QA stats.
- Provably leak-free: noninterference fuzzing, formal spec of visibility, model-checked
  commit protocol, reproducibility receipts, explain-the-diff. See `10-ambition.md`.
- Massive parallel backtesting over the bucket; checkpoints + deltas.
- Point-in-time catalog reads (which series/tags existed at T — avoids survivorship bias).
- Derived series built at query time (frontier applied to all inputs), possibly
  materialized queries kept up to date as inputs get new versions. Until then users
  compute them outside and write them in.
