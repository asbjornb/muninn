# Data model

## Shape

Keep the engine-level model tiny:

```
series     -- interned symbol (e.g. 'DK1.spot.price', or (area, metric, model) tuple)
value_time -- what the value is about
<times>    -- published_time, received_time, system_time (see 01-time-model.md)
version    -- implicit, from system_time + insert sequence
payload    -- one or more typed columns (double, int, symbol, fixed-size vector...)
meta       -- optional extra columns: issue_time, model_run_time, last_input_time, ...
is_deleted -- tombstone flag
```

## Realized series vs. forecasts

These differ in what identifies a logical point:

- **Realized / observed** (prices, actuals): one logical value per `(series, value_time)`.
  Revisions supersede.
- **Forecasts**: many logical values per `(series, value_time)` — one per issue. A
  forecast issued at 06:00 and one issued at 12:00 are *different facts*, not revisions of
  each other. A revision of the 06:00 forecast is a new version of that issue.

Options:

1. Make `issue_time` (or a generic "issue key") part of the identity for forecast tables.
2. Treat forecasts as realized series where each issue supersedes the previous one, and
   rely on frontier queries to get "latest issue as of T". Simpler, but loses the ability to
   ask "what did the 06:00 run say" once the 12:00 run is in.

Leaning towards (1): a table declares its identity columns; the default is
`(series, value_time)`, forecast tables add `issue_time`.

## Keys and interning

- Series keys are interned into a global symbol dictionary (kdb-style). Fast compares,
  small storage.
- Low-cardinality descriptive fields (model, provider, source, pt_confidence) also interned.
- Unclear: is a series a flat symbol, or a structured tuple `(area, metric, model, ...)`
  with a separate series catalog holding the attributes? The catalog approach makes
  "all DK* price forecasts" queries natural without parsing strings.

## Extra timestamps and metadata

Things like forecast time, model run time, last input data time are plain typed
columns. The engine stores and returns them, may zone-map them, but doesn't give them
visibility semantics. If one of them turns out to be needed as a frontier, that's a
signal to revisit — not to add it up front.

## Schema evolution

- Additive only in v1: a new column starts existing from some segment onwards; older
  segments read it as null.
- No type changes in place; add a new column instead.

## Provenance / QA (nice to have)

- `provider`, `source_id`, `pt_source`, `pt_confidence`.
- Derived stats: publication lateness (`published_time - value_time`, or relative to an
  expected schedule), our latency (`received_time - published_time`), correction delay
  (time between first version and a correction).
