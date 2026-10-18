# Query surface

Small, composable, predictable. The point-in-time semantics live in the clauses, not in
hand-written window functions.

## Core concepts

- **Frontier** — "as of" which moment of knowledge. Applies to one of the frontier
  clocks: `received` (default?), `published`, plus optionally `system` for
  reproducibility.
- **Value range** — which value_times we want (`VALID AT` / `VALID BETWEEN`, or just a
  `WHERE value_time BETWEEN ...`).
- **Latest-known** — per logical point, pick the newest version visible at the frontier.
  This should be the *default* behaviour of a select, not a function you remember to call.

## Sketch (syntax very much TBD)

```sql
-- What did we know at 12:00 on Jan 15 about Jan 16?
SELECT series, value_time, value, published_time
FROM forecasts
AS OF RECEIVED '2025-01-15T12:00:00Z'
WHERE series IN ('DK1', 'DK2')
  AND value_time >= '2025-01-16' AND value_time < '2025-01-17';

-- Same, but as the database looked on March 1 (reproduce an old backtest)
... AS OF RECEIVED '2025-01-15T12:00:00Z' SYSTEM '2025-03-01T00:00:00Z'

-- Full version history instead of latest-known
SELECT ... FROM forecasts ALL VERSIONS WHERE ...

-- Replay: everything in the order it arrived
SELECT ... FROM forecasts ALL VERSIONS
WHERE received_time BETWEEN ... ORDER BY received_time;
```

## As-of join

The kdb-style `aj`: for each left row, the latest right row with key match and time ≤
left time. In this world there are two layers:

- the **frontier** of each side (what was known when the left row's event happened), and
- the **value_time** alignment (match signal hour to price hour).

```sql
SELECT s.series, s.value_time, s.signal, p.price
FROM signals s
ASOF JOIN prices p USING (series, value_time)   -- p as known at s.received_time
```

Open: is the right side's frontier always the left row's own time (per-row frontier), or a
single global frontier? Per-row is what a real backtest needs and is the hard, interesting
operator.

## Rolling frontier / backtest loop

A backtest walks time forward and asks "what did I know now?" at each step. Re-running a
full query per step is wasteful. Worth a dedicated API: a **cursor that advances the
frontier** and yields deltas (new versions that became visible). Basically replay, but
consumable as state updates.

## Interfaces

- Embedded library API first (Python bindings matter most for backtesting).
- Arrow as the result format everywhere (zero-copy into pandas/polars).
- Later: HTTP and/or Arrow Flight server; maybe a DuckDB extension / table function so
  people can use DuckDB SQL on top of PIT-resolved scans. That last option could let us
  skip writing most of a SQL layer entirely.

## Derived views

- `current_truth` — latest version per point, today's knowledge. Incrementally
  maintained. For dashboards/analytics, not backtests.
- Rollups (daily averages etc.) parameterised by frontier — harder; maybe v2.
