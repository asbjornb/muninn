# Benchmarks

The model and API being right gives Muninn value on its own, but speed and compression are
what sell it. Build the benchmark harness **early** (roadmap step 0) and run competitors
first, so the bar is known before any engine code exists. If Muninn can't clearly beat the
best-tuned competitor on some workload, we should know that early.

## Datasets (synthetic, shaped like real energy data)

Deliberately different shapes, so no layout choice wins by accident:

| Dataset | Shape | Stresses |
| --- | --- | --- |
| Day-ahead prices | hourly → 15 min, few revisions | baseline, resolution change |
| Wind/solar forecasts | 15 min, hourly updates, 10-day horizon, many providers | version count (~240 per point), many partitions per write |
| Weather ensembles | 51 member columns, 6-hourly runs | wide rows |
| Forward curves | daily snapshots, delivery years ahead, month/quarter/year | writes touching years of partitions |
| Settlement / metered actuals | revised at D+1, D+5, M+3 | correction chains |
| UMMs / outages | irregular intervals, frequent revisions | irregular, sparse |
| Many small series | 100k+ sparse series | catalog, tiny segments |
| Backfill | 5 years bulk-loaded at once, synthetic received times | bulk ingest |

Plus **mixed loads**: trickle ingest (row-by-row, live) running concurrently with backtest
reads.

## Queries

- Single as-of read (one series, one day, one frontier).
- Frontier grid: for each of 365 decision times, latest known for the next 36 h.
- All versions of a range; replay in arrival order.
- Lead-time slice (all forecasts made ~24 h ahead).
- Latest known today (current truth).
- As-of join with per-row frontier (signals ↔ prices).

## Metrics

- Bytes on disk (compression), per dataset.
- Ingest: bulk throughput, trickle latency, write amplification.
- Query latency p50/p99, cold and warm cache; local disk and object storage.

## Competitors

Each with its best-practice schema, tuned honestly — a benchmark against a straw man is
worthless. Run in containers where possible. Keep competitor setups (schemas, settings,
queries) in the repo and **invite people who know each system to improve them**; credit
them and rerun when they do.

- **ClickHouse** — MergeTree with `argMax` / window reads; ReplacingMergeTree variant.
- **DuckDB** — native tables and over Parquet.
- **XTDB v2** — closest conceptual cousin (bitemporal, columnar).
- **ArcticDB** — versioned dataframes on S3, popular with quants.
- **SQL Server / Postgres (+Timescale)** — what many shops actually use today.
- Maybe QuestDB (`ASOF JOIN`, `LATEST ON`).

Check licences before *publishing* results: some commercial products (e.g. kdb+) restrict
publishing benchmarks.

## Rules

- Same datasets, same queries, same machine, scripted end to end (reproducible from one
  command).
- Every system's results checked against the step-0 reference implementation — fast but
  wrong doesn't count.
