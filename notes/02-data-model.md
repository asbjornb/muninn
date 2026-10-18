# Data model

## One kind of series

There's no fundamental difference between forecasts and actuals. Both record a value
about some moment (`value_time`), become known at some point, and can be corrected.
Actuals are mostly recorded *backwards* in time (value_time < published_time), forecasts
*forwards* (value_time > published_time), but the data and the semantics are the same.

What looked like a forecast-specific need — "what did the 06:00 run say?" — is just an
as-of read with the frontier just after that run was received. Each new forecast run is
a new version of the same `(series, value_time)` points, superseding the previous run.

Edge cases to keep an eye on (not reasons to split the model):

- A run that only covers part of the horizon: as-of reads then mix the new run with older
  values for the points it didn't cover. That's correct "latest known" semantics, but
  differs from "exactly the 12:00 run". If that's needed, `issue_time` as a normal column
  plus a filter gives it.
- Different models/providers for the same quantity are **different series**, not versions.

## Rows: engine columns + values

Every row has a small fixed set of columns the engine understands, plus one or more
value columns:

```
series_id       -- compact integer key into the series catalog (see below)
value_time      -- what moment the values are about
published_time  -- when it became public (may be provisional / corrected)
received_time   -- when we got it
system_time     -- when this version was written (engine-assigned) + seq for ties
deleted         -- tombstone flag
<values...>     -- user columns: one or many typed scalars
```

There's no real difference between "payload" and "meta" columns. A row is a
multivariate scalar record: `value`, or `p10, p50, p90`, or `price, volume`, or
`value, issue_time, model_run_time`. The engine treats them all the same — stores them
columnar, zone-maps them, returns them. Only the engine columns have visibility
semantics.

## Tombstones

A delete is just another version whose `deleted` flag is set. It doesn't need its own
"deleted_at" timestamp: like any version it already has `published_time`,
`received_time` and `system_time`, and those *are* when the deletion happened (and was
known). Reads at frontiers before it still see the value; reads after don't. Un-deleting
is one more version.

## Series catalog (metadata store)

Rows carry only `series_id` — a small integer (int32 is probably plenty). Everything
else about a series lives once, in a catalog:

- descriptive tags: provider, country, bidding zone, metric, model, unit, source...
- **time zone / calendar** (e.g. `Europe/Copenhagen` for a DK1 delivery day),
- **resolution** (15 min, hourly, daily, irregular),
- value column schema (names, types),
- default frontier (received vs. published), retention, etc.

Rows are always stored in UTC; the series' time zone is used for things like "delivery
day 2025-01-16" or daily rollups in local time, including DST days with 23/25 hours.

To consumers this is mostly invisible: they query by tags or names
(`area = 'DK1' AND metric = 'spot_price'`), the catalog resolves that to a set of
`series_id`s, and the scan runs on integers.

Open:

- Does catalog metadata change over time (provider renamed, unit fixed), and do those
  changes need to be versioned/as-of too? Probably yes for some fields, but simple
  versioning (a history table) is likely enough — not the full time model.
- Catalog storage: a tiny embedded store (SQLite?) vs. our own append-only log.
- Can one series' value schema evolve (adding p95 later)? Additive only, presumably.

## Schema evolution

- Additive only in v1: a new value column starts existing from some segment onwards;
  older segments read it as null.
- No type changes in place; add a new column instead.

## Provenance / QA (nice to have)

- Per-version: `pt_confidence` (`provisional | claimed | authoritative`), `pt_source`.
  Possibly engine-level, since they qualify an engine column.
- Derived stats: publication lateness (`published_time - value_time`, or relative to an
  expected schedule), our latency (`received_time - published_time`), correction delay.
