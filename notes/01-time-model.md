# Time model

This is the heart of the project. Everything else is plumbing.

## The base model should be simple

Lots of series carry many timestamps: forecast issue time, model run time, time of the
last input data used by the model, delivery period start/end, etc. Those are real and
useful, but they are **series-specific metadata**, not engine concepts. The engine should
only know about a small fixed set of times that drive *visibility* and *versioning*.

## The timestamps that matter

| Name (working) | Question it answers | Who sets it | Can it be wrong / change? |
| --- | --- | --- | --- |
| `value_time` | What moment is this value *about*? (the delivery hour, the target time of a forecast) | Data | No — it's part of the point's identity |
| `published_time` | When did this become **publicly available in the market**? | Data / source | Yes — may be provisional, unknown, or later corrected |
| `received_time` | When did **we** get it and could have acted on it? | Our ingest | Usually exact for live data; synthetic for backfills |
| `system_time` | When was this version written into the database? | The engine | Never — the DB owns it, monotonic |

The two "very important" ones from a trading perspective are **published** and
**received**: when the market could know, and when we could act. Ideally they're close.
The gap between them is our latency, and it's interesting in itself.

`value_time` is the axis the data lives on (the "valid time" of classic bitemporal
literature). `system_time` is the classic "transaction time" — it exists mostly so that
the database itself is reproducible and auditable.

### Why four and not two?

The early input oscillated between `valid_time/available_time` and
`value_time/knowledge_time/published_time`. Untangling it:

- "Available time" in the first note conflated *published* and *received*.
- "Knowledge time" in the second note was really *system time* (when the row/version
  landed), used as the backtest frontier.

Keeping received and system time apart matters for **backfills**: if we backfill five
years of history today, every row has `system_time = today`. If system time were also the
backtest frontier, a backtest of 2023 would see no data at all. So:

- **Backtest frontier** = `received_time` (or `published_time`, see below) — "what could
  the strategy have seen at T".
- **Reproducibility frontier** = `system_time` — "what did the database say on date X" so
  that a backtest run last month can be re-run bit-for-bit even after later corrections.

These compose: *"as the DB looked on 2025-03-01, show me what we'd have received by
2024-06-10 12:00"*.

### Choosing the frontier: received vs. published

Both are legitimate, they answer different questions:

- Frontier on **received_time** → realistic backtest of *our* system including our latency.
- Frontier on **published_time** → "what if we had zero latency" / what the market as a
  whole could know. Useful for evaluating latency investments and for data where
  received_time is synthetic (backfills).

Probably the query names one frontier explicitly, and there's a per-table default.

## Versions and corrections

A logical point is identified by `(series, value_time)` plus, for forecasts, whatever
distinguishes issues (see `02-data-model.md`). Each logical point has a chain of versions.

A new version is appended when:

- the value is revised (provider correction),
- the published time is corrected (we learn the real publication time later),
- the point is retracted (tombstone).

Each version carries its own `published_time` / `received_time` / `system_time`. Reading
"as of frontier F" means: among versions visible at F, pick the latest. The published
time you get back is the one *believed at F*, not the one known today.

Example from the early notes: on March 5 we thought a value was published at 10:00; on
March 10 we learn it was really 08:37. A backtest reproducing March 5 knowledge sees
10:00; one using today's knowledge sees 08:37. No rows were rewritten.

### Late-bound published time

For backfills and aggregators we often don't know `published_time` at ingest. Options:

- `published_time = NULL` (unknown), with query-time fallback rules.
- Provisional value (`= received_time`, or the provider's claimed timestamp) plus a
  `pt_confidence` (`provisional | claimed | authoritative`) and `pt_source`.
- A **latency model** per source ("provider X publishes day-ahead forecasts at ~09:30")
  used to synthesize a plausible published/received time for backfilled history.

Leaning: store whatever is known plus confidence/source; never key physical layout on
published time, since it can move.

## Tie-breaking (determinism)

When two versions are equally "latest" under the frontier, break ties deterministically,
e.g. by `(frontier_time, system_time, insert_seq)`. `system_time` + a monotonic sequence
from the single writer makes this total.

## Representation

- All times UTC, int64 nanoseconds (or microseconds? see open questions).
- Calendars/time zones are a query-layer concern (delivery days in CET etc.), never storage.

## Open

See `open-questions.md` → *Time model*.
