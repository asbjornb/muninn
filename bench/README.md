# Muninn benchmark harness

Roadmap step 0 of [`notes/09-benchmarks.md`](../notes/09-benchmarks.md): know the bar before
the engine exists. Synthetic energy datasets, a declarative query workload, tuned
competitor setups (DuckDB, ClickHouse), an automated check that every system returns
*exactly* what the reference implementation returns, and one command that produces a
report.

```sh
# correctness: every system vs. spec/reference at tiny scale (~1-2 min)
uv run --project bench pytest

# a benchmark run (generates data if missing) -> bench/results/<date>-<host>/<scale>.{json,md}
uv run --project bench python bench/run.py --scale small
uv run --project bench python bench/run.py --scale medium --systems duckdb-native,clickhouse

# just the data
uv run --project bench python -m datagen --scale medium --out bench/data/medium-s1
```

(`cd bench` first if `uv run --project bench python -m datagen` cannot find the module;
`run.py` and pytest set up paths themselves.)

ClickHouse needs its binary once: `bench/competitors/clickhouse/fetch.sh` downloads the
official static build into `bench/tools/` (gitignored) and checks its sha512. Without it,
ClickHouse tests are skipped and the runner reports it as not run.

Large data, work directories and the ClickHouse binary live under `bench/data/` and
`bench/tools/` (gitignored). Reports under `bench/results/` are small and committed.

## Layout

| Path | What |
| --- | --- |
| `datagen/` | dataset generator (`python -m datagen`), scale knob, scenario export |
| `queries/workload.json` | the query templates (what, on which dataset, how parameterised, what it stresses) |
| `queries/params.py` | turns templates into concrete, deterministic queries per dataset |
| `queries/edges.py` | boundary queries derived from the data, correctness tests only |
| `competitors/base.py` | the `Adapter` interface and the canonical result shape |
| `competitors/reference/` | the oracle: `spec/reference/muninn_ref` behind the same interface |
| `competitors/duckdb/` | DuckDB native table and over-Parquet layouts ([tuning notes](competitors/duckdb/README.md)) |
| `competitors/clickhouse/` | ClickHouse MergeTree ([tuning notes](competitors/clickhouse/README.md)) |
| `competitors/muninn/` | placeholder for the engine; raises `Unavailable` until a binding/CLI exists |
| `competitors/registry.py` | system name -> adapter factory |
| `tests/` | correctness against the oracle, write-stream round trip through the reference |
| `run.py` | the end-to-end runner and report writer |
| `results/` | committed reports |

## Datasets

Each dataset is its own database with one table (SPEC §2), so series ids and `seq` start at 1
per dataset. The generator plays both writer and engine: dataset modules emit write batches
with a writer clock `c`; the writer applies SPEC §4.2 (`system_time = max(c, last)`, one
`seq` counter, input order within a batch) and §4.3 (exact re-sends are dropped and go to
`resends.parquet`). Batches are emitted in commit order one commit-day at a time, so a
late batch never pushes earlier arrivals' `system_time` forward. All randomness is keyed
by `(seed, dataset, series, day, ...)`: identical output regardless of chunking. Values are
rounded the way real feeds are (0.01 EUR/MWh, 0.1 MW, 0.001 MWh), so compression numbers
mean something.

| Dataset | Shape | Quirks | Stresses |
| --- | --- | --- | --- |
| `da_prices` | zones, hourly then 15-min half way through; published ~11:45 UTC D-1 | 5% null published, 1% published > received, price corrections at D+0, silent corrections (same published time), identical re-pushes with new received time, published-time corrections, retraction + re-issue, exact re-sends | baseline; resolution change; published clock |
| `forecasts` | provider x area x wind/solar; 15-min; hourly runs; 10-day horizon (~230 versions/point) | one provider stamps the run-cycle hour as "published", one tells the truth (and is sometimes > received), one never says; skipped and late runs; partial retractions (tombstones); exact re-sends. Errors are AR(1) between runs, so consecutive versions differ slightly, like real forecasts; solar is exactly 0 at night | version count, the headline compression and grid cases |
| `settlement` | metering-grid areas, 15-min; D+1 estimate, D+5 full re-send (40% changed), M+3 final | null published, rare D+5 tombstones re-issued at M+3, exact re-sends | correction chains, system frontier |
| `sparse` | REMIT-style unavailability messages; 100k+ series at medium; irregular times; `value_end` column | revisions keep the original published time sometimes, cancellations (tombstones), null published | catalog size, tiny segments, replay across many series |
| `backfill` | 5 years hourly, bulk-loaded on one day | synthetic received times; published unknown before 2021 and 20% after; corrections with later received times in the same load; re-loaded batches | bulk ingest; system_time useless as a frontier |

Scales (`datagen/datasets.py`): `tiny` (~35k versions in total; rare quirks fire 20x more
often so the oracle check sees all of them), `small` (~5M), `medium` (~36M, forecasts 25M
at 231 versions/point, sparse 150k series), `large` (~300M; streamed, never held in memory).
Medium generates in about a minute with a peak RSS under 2 GB.

Output per dataset: `versions.parquet` (canonical columns `series`, `series_id`,
`value_time`, `published_time`, `received_time`, `system_time`, `seq`, `deleted`, then the
value columns; timestamps µs UTC), `resends.parquet`, `catalog.json` (tables, columns,
series, generator config, stats). `--export-scenario` writes the SPEC §9 form.

## Queries

`queries/workload.json` lists them with what each stresses; parameters are deterministic
per dataset seed. In short:

| Kind (notes/09) | Queries |
| --- | --- |
| single as-of read | `fc_asof_single`, `da_asof_published` (published clock), `st_asof_system` (system frontier), `sp_asof_single`, `bf_asof_single` |
| frontier grid, 365 x next 36 h | `fc_grid_365`, `fc_grid_365_all`, `da_grid_365`, `st_grid_365` (look-back window), `bf_grid_365` |
| history of a range | `fc_history_day`, `st_history_month` |
| replay between two frontiers, `(from, to]` | `fc_replay_6h`, `sp_replay_day` |
| lead-time slice | `fc_lead_24h` (history filtered on `value_time - issue_time`), `fc_asof_join_lead24` (per-row frontier `vt - 24h`) |
| latest known today | `st_latest_now`, `sp_latest_now`, `fc_latest_now` |
| as-of join, per-row frontier | `da_asof_join_signals` (10k signals ↔ prices), `fc_asof_join_lead24` |

Replay uses the half-open interval `(from, to]` on t_clock (the spec change in progress).
The oracle adapter probes which interval the checked-out reference implements and adapts.

## Correctness ("fast but wrong doesn't count")

`uv run --project bench pytest` runs:

* **every system vs. the oracle** at tiny scale: all workload queries plus ~150 boundary
  queries per dataset (`queries/edges.py`: frontiers, system frontiers and replay bounds
  exactly on stored clock values and 1 µs before, all three clocks, duplicate grid
  frontiers, absolute grid windows, as-of rows that miss). Results are compared row by row
  under the SPEC §9 rules (complete list, in order, timestamps as integers, f64 bitwise).
  Injecting typical bugs (`<` for `<=`, `[from, to)` replay, dropping the tombstone filter,
  wrong tie-break, `published_time` without the received fallback) makes these tests fail;
* **write-stream round trip**: a prefix of each tiny dataset, including the exact re-sends,
  is exported as a SPEC §9 scenario and run through the reference's own scenario runner,
  so the generator's `system_time`/`seq`/dedup emulation is checked against
  `Database.write`;
* `datagen` asserts that no two stored versions are content-identical (§4.3).

At larger scales the oracle is too slow; `run.py` hashes every result and the report flags
any (query, instance) where systems disagree.

## Measurement

`run.py` per system and dataset: load (ingest time includes the system's sort/merge/
compression into its tuned layout; bytes on disk as the system reports them), then per
query template one **cold** run (after `Adapter.cold()`: fresh DuckDB connection; ClickHouse
cache drops; the OS page cache is *not* dropped, which needs root) and `--warm-runs` passes
over all instances (**warm** p50/p99; a per-template time budget caps slow ones; the sample
count is in the JSON). Latency is end to end through the Python client into the canonical
Arrow result (series names included). Threads are capped (default 6) so a concurrent build on
the same machine doesn't make results meaningless; numbers are for one laptop and are
meant for *relative* comparisons.

Not yet measured (notes/09): trickle ingest latency and write amplification, mixed
ingest+read load, object storage, weather ensembles, forward curves, UMM-style outages with
irregular intervals beyond `sparse`.

## Fairness rules (from notes/09)

* Same datasets, same queries, same machine, scripted end to end from one command.
* Every system's results checked against the reference implementation; a system that is
  fast but wrong on any query is not reported as faster.
* Each competitor gets its best-practice schema, tuned honestly: the choices and the
  alternatives tried are documented next to each adapter (`competitors/*/README.md`), and
  alternatives stay runnable (e.g. `duckdb-native-window`). People who know a system are
  invited to improve its setup; credit them and rerun.
* Competitors may use any property the spec guarantees (e.g. that `seq` order equals commit
  order); they may not use properties of the generator that the spec does not promise
  (e.g. that decision times are daily at 10:00).
* Licences: check before *publishing* results (some products restrict benchmark
  publication). DuckDB (MIT) and ClickHouse (Apache 2.0) do not.

## Adding a system

1. Write `competitors/<system>/adapter.py` with one `Adapter` subclass (see
   `competitors/base.py`): `load(dataset_dir, catalog)` ingests `versions.parquet` into the
   system's tuned layout and returns `{"ingest_seconds", "bytes_on_disk"}`; `run(query)`
   returns the canonical Arrow result; `cold()` drops caches; `close()`. Raise
   `Unavailable` if the runtime is missing.
2. Add one line to `competitors/registry.py`.
3. `MUNINN_BENCH_SYSTEMS=<system> uv run --project bench pytest` until it matches the oracle,
   then `bench/run.py --systems <system>`.
4. Document schema/tuning choices in `competitors/<system>/README.md`.

The Muninn engine plugs in the same way (`competitors/muninn/adapter.py` describes the
binding/CLI shape it expects).
