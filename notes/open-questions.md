# Open questions

Move to `decisions/` once answered.

## Scope / framing

- Build an engine, or build a semantic layer (DuckDB extension / library over Parquet)?
  Roadmap step 0 should answer this with data rather than taste.
- Embedded library, server, or both? Object storage as the source of truth from day
  one? See `08-deployment.md`.
- Do trading firms want one central store for all teams? (Assumed yes.)
- How would this actually be deployed at work (cloud provider, k8s or not, .NET clients)?
- Implementation language: Rust vs. C# vs. something else. Trade-offs: Arrow/Parquet
  ecosystem and Python bindings (Rust strong) vs. familiarity (C#).
- What's the realistic data size? Rows per day, number of series, version churn. Drives
  segment size, partition granularity and whether any of this needs to be clever.

## Time model

- Is the default backtest frontier `received_time` or `published_time`? Per table? Per query?
- How to represent unknown published_time: NULL, provisional + confidence, or synthesized
  from a per-source latency model?
- Do we need `system_time` as a user-facing frontier from v1, or just internally for
  ordering? (Reproducing old backtests argues for v1.)
- When published_time is corrected, does that change the point's visibility under a
  published-time frontier retroactively? (It should, but only for queries whose system
  frontier is after the correction.)
- Is received_time "first time we saw this content" or "last time we were sent it"?
- Timestamp precision: ns (kdb, Arrow default) vs. µs (enough for this domain, more range).

## Data model

- ~~Forecast identity~~ → forecasts and actuals are one model; runs supersede.
- ~~Flat symbols vs. catalog~~ → integer `series_id` on rows + series catalog.
- ~~Catalog history~~ → yes, append-only change log for audit + rollback.
- Catalog storage: SQLite-like embedded store vs. own log?
- ~~Fixed-size array values~~ → separate scalar columns (see `faq.md`).
- ~~Interval-valued points~~ → start in `value_time`, length = catalog resolution;
  irregular intervals use a `value_end` value column (see `faq.md`).
- A series whose resolution changes (hourly → 15 min): new series, or catalog resolution
  with a valid-from?
- Trades and orders (own and market): in Muninn, or a separate system? Usage patterns,
  volume and compressibility differ; the combined picture is valuable. See `faq.md`.

## Storage

- Own column format vs. Parquet/Arrow IPC for segments. Row-by-row arrival is handled by
  WAL + memtable either way; the question is the *sealed* format. Experience (ClickHouse,
  Parquet, SQL Server) says revisions get expensive at insert or at read in standard
  formats. An own format allows encodings Parquet can't express, e.g. XOR/delta against
  the previous version of the same point. Let the benchmarks decide.
- Partition by value_date always, or by value_month for sparse tables?
- Sort key tail: frontier time before or after system_time?
- Is a per-series offset index in the footer worth it, or is bloom + zone maps enough?

## Query

- Write our own small SQL-ish language, a dataframe-style API, or ride on DuckDB SQL?
- As-of join: per-row frontier semantics, and how to make it fast. Joining series of
  different resolution (match by "contained in")? Left side from an external Arrow table?
- Reading a group of series as one, keeping the newest version per value_time (e.g. run
  cycles stored as separate series)?
- Frontier-advancing cursor API for backtest loops — what shape?
- Calendars & timezones (delivery days in local time, DST) — query layer only?

## QA / operations

- Freshness/gap metrics that stay PIT-aware when published times get corrected.
- Automatic vendor-timestamp QA flags (published after received, "published" that is
  really run time, silent corrections) — engine feature or external job? See `faq.md`.
- Backup = copy partition dirs + catalog; is that enough?
