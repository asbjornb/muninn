# Muninn

*Named after Odin's raven of memory, who flies out over the world and reports
back what happened. Name may still change.*

A small, opinionated database for **point-in-time-correct time series** —
the kind of data you backtest trading/forecasting strategies against, where "what did we
know, and when could we act on it" matters as much as the value itself.

Think kdb+ or DuckDB in ambition (a focused engine you can hold in your head), not
Postgres or Oracle (a general-purpose RDBMS).

**Status:** early prototype (v0.1). The semantics are specified and the engine works end to
end: storage on a local directory, a CLI and Python bindings. Not production-ready yet: no
object-store backend, no vintage-delta compression, and the API may still change.

## What exists

| Path | What |
| --- | --- |
| [spec/SPEC.md](spec/SPEC.md) | **Normative semantics**: clocks, visibility, knowledge order, writes/dedup, reads, invariants, errors |
| [spec/scenarios/](spec/scenarios/) | Conformance suite: 51 hand-written + 150 generated scenarios (JSON), run against every implementation |
| [spec/reference/](spec/reference/) | Pure-Python reference implementation (the oracle) + property tests |
| [spec/FORMAT.md](spec/FORMAT.md), [spec/DUMP.md](spec/DUMP.md) | On-disk format (segments, commit log, checkpoints) and portable dump format |
| [crates/](crates/) | Rust: core types + in-memory model, store backends, segment format, engine, CLI, Python bindings |
| [bench/](bench/) | Synthetic energy datasets, query workload, DuckDB/ClickHouse baselines, results |
| [docs/cli.md](docs/cli.md) | CLI walkthrough |
| [decisions/](decisions/) | Decision records 0001–0010 |

```sh
task test       # Rust workspace: unit, conformance, differential fuzz vs model, crash tests
task lint       # rustfmt + clippy -D warnings
task py-test    # Python bindings: scenario corpus + differential test vs reference
uv run --project spec/reference pytest      # reference + invariants
python3 spec/reference/run_scenarios.py     # corpus against the reference
```

How correctness is checked: two independent implementations of the spec (Python reference,
Rust model) agree on the whole scenario corpus; the engine is differentially fuzzed against
the Rust model (with compaction, reopen and GC interleaved), crash-tested at every store
operation, and the Python bindings are fuzzed against the Python reference.

## Why build instead of choose?

Existing options each get part of the way:

- **kdb+** nails as-of joins and columnar speed, but is proprietary, expensive and q is an
  acquired taste. No first-class notion of corrections/knowledge time.
- **ClickHouse / Timescale / Postgres** can *model* point-in-time data, but every query
  becomes a `row_number() OVER (...)` / `argMax` dance and correctness depends on
  everyone writing that dance correctly every time.
- **DuckDB / Parquet** are great for analytics but have no append/correction model.

The bet: if "as of what we knew at time T" is the *native* semantics of the engine rather
than a query pattern, backtests become honest by default and the engine can be much
simpler than a general database because it refuses to do most things.

## Notes

| File | What it covers |
| --- | --- |
| [notes/00-vision.md](notes/00-vision.md) | Goals, non-goals, what "done enough" looks like |
| [notes/01-time-model.md](notes/01-time-model.md) | **The core.** Which timestamps exist and what they mean |
| [notes/02-data-model.md](notes/02-data-model.md) | Series, points, versions, corrections, extra metadata |
| [notes/03-storage.md](notes/03-storage.md) | On-disk layout, partitioning, segments, compression |
| [notes/04-ingestion.md](notes/04-ingestion.md) | Write path, WAL, idempotency, backfills, crash recovery |
| [notes/05-query.md](notes/05-query.md) | Query surface: AS OF, as-of joins, latest-known |
| [notes/06-prior-art.md](notes/06-prior-art.md) | Steal sheet: kdb+, Postgres, ClickHouse, Timescale, others |
| [notes/07-roadmap.md](notes/07-roadmap.md) | Rough MVP build order |
| [notes/08-deployment.md](notes/08-deployment.md) | Single-node vs. cloud, object storage, adoption, local testing |
| [notes/09-benchmarks.md](notes/09-benchmarks.md) | Datasets, queries, competitors, rules for performance tests |
| [notes/10-ambition.md](notes/10-ambition.md) | Where to be ambitious: 10× targets, spec-as-product, provable leak-freedom |
| [notes/faq.md](notes/faq.md) | "How do I model X?" — intervals, runs, ensembles, frontiers, trades |
| [notes/open-questions.md](notes/open-questions.md) | Unresolved questions, grouped by topic |
| [decisions/](decisions/) | Decision records |
| [inbox/](inbox/) | Raw, unedited input (earlier conversations etc.) |

Notes are **input, not dogma** — the inbox material in particular is an early brainstorm and
contradicts itself in places (see the naming discussion in `01-time-model.md`).
