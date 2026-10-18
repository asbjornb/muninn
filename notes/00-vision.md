# Vision

## One sentence

A database where time is always part of the read — most reads are "as of some moment
of knowledge" — and those reads are fast and correct by default, so backtests can't
leak future information by accident.

Not every read has to be as-of (latest-known-today, full version history and arrival-order
replay are all first-class too), but no read ignores time, and the as-of path is the one
that must be really fast.

## Primary use case

Energy / market time series and forecasts used in backtesting and live trading:

- Realized values (prices, volumes, production) that may be revised.
- Forecasts, where many versions exist for the same target time (issued at different times,
  by different models).
- Data that arrives late, gets backfilled from history, or comes from aggregators with
  questionable timestamps.

Typical questions:

- "What did the DK1 price forecast for tomorrow look like at 12:00 yesterday?"
- "Replay everything as it arrived during 2025-01-15, in arrival order."
- "Join my signals to realized prices, using only what was known when each signal fired."
- "How late does provider X usually publish? How often do they revise?"

## Goals

1. **Point-in-time correctness by default.** Leakage should require effort, not the
   other way round.
2. **Append-only truth.** Corrections are new facts. Nothing is rewritten in place.
3. **Deterministic answers.** Same query + same knowledge frontier = same result,
   regardless of compaction state, ingestion order of ties, etc.
4. **Fast and small enough to sell itself.** Never the bottleneck in a backtest loop, and
   aim for ~10× over the best-tuned competitor on some workloads (compression of forecast
   vintages, frontier-grid reads). See `10-ambition.md`.
5. **Small enough to understand.** One person should be able to hold the whole engine in
   their head. Simplicity beats features.
6. **Easy to get data in and out.** CSV/Parquet/Arrow in and out from day one; a
   dump/restore story early.

## Non-goals (at least for v1)

- General-purpose SQL / ANSI compliance.
- Multiple concurrent writers, distributed clustering, replication.
- Arbitrary secondary indexes, foreign keys, multi-statement transactions.
- Updates and deletes in place (a delete is a tombstone version).
- Being a replacement for the warehouse — this is the store backtests and live
  strategies read from.

## What "simpler than a real DB" buys us

- Single writer → no lock manager, trivial crash recovery, deterministic ordering.
- Append-only → no MVCC garbage, readers pin a snapshot by just listing segments.
- Fixed sort order → no query optimizer worth the name; pruning is the optimizer.
- Narrow query language → the planner is a handful of operators.

## Open framing questions

- Embedded library vs. server vs. both, and how this lives in a cloud-first world — see
  `08-deployment.md`.
- Implementation language. Candidates: Rust (ecosystem: Arrow, Parquet, DataFusion pieces),
  C# (familiar, good mmap/Span story, but weaker data-ecosystem interop), Zig/C++.
  See `open-questions.md`.
