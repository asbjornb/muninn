# Roadmap (rough)

Status 2026-10-19: steps 0–4 and 6 done in v0.1 form (see below); 5 partly (grid done,
as-of join exact-match only); 7 partly (compaction + GC, no retention/purge). Order is "each step is usable on its own and de-risks the next".

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

- **Correction / anomaly flags** (from review of decision 0003, 2026-10-19). Versions are
  ordered by the query clock, so a later correction of a published time doesn't override.
  Add an explicit way to mark a version as superseded or anomalous (outlier, later
  corrected), so a reader can choose to see data with or without flagged values, as
  of a frontier. Flags are versions too: they need to be point-in-time correct themselves.
- **Duplicate-dropping write mode** (`skip_unchanged`). Today every re-send with a new
  `received_time` is a new version, the safe default, but duplicates can wreck performance.
  This is hard to get right: a 5 → 4 → 5 MW forecast sequence has a real second 5, not a
  duplicate, and without a source `published_time` it can look identical to a plain
  re-send. A safe rule likely compares to the *latest visible* version of the point (drop
  only if equal to the current latest), never to any historical version. Decide with real data.
- **Data strategy: synthetic first.** Don't commit real data in bulk. Pull public data
  (ENTSO-E, Energinet) locally to learn the shapes: revision patterns, issue cadence,
  publication delays, re-sends, A-B-A sequences. Turn those into the generators in
  `bench/datagen`, so tests and benchmarks stay reproducible from a seed. Revisit using
  real data directly once the generators are calibrated.
- Derived current-truth views, rollups, server mode (HTTP/Flight), frontier-advancing
  cursor for backtest loops, latency/QA stats.
- Provably leak-free: noninterference fuzzing, formal spec of visibility, model-checked
  commit protocol, reproducibility receipts, explain-the-diff. See `10-ambition.md`.
- Massive parallel backtesting over the bucket; checkpoints + deltas.
- Point-in-time catalog reads (which series/tags existed at T — avoids survivorship bias).
- Derived series built at query time (frontier applied to all inputs), possibly
  materialized queries kept up to date as inputs get new versions. Until then users
  compute them outside and write them in.
