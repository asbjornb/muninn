# ClickHouse baseline — setup and tuning

`clickhouse` runs a local ClickHouse server from the official static binary
(`fetch.sh`: release tarball, sha512 verified, into `bench/tools/`; `$CLICKHOUSE_BIN`
overrides). It is the same build the `clickhouse/clickhouse-server` image ships; Docker
was not usable on the machine the first reports came from. The server config is generated
per dataset (`max_threads=6`, `max_memory_usage=4096MB`, server cap +1 GB, query cache off,
uncompressed cache off). The client is `clickhouse-connect` over HTTP, results as Arrow.
Version 26.7.34.3.

## Schema

```sql
CREATE TABLE v (
  series_id UInt32 CODEC(Delta, ZSTD(3)),
  value_time Int64 CODEC(Delta, ZSTD(3)),
  published_time Nullable(Int64) CODEC(Delta, ZSTD(3)),
  received_time Int64 CODEC(Delta, ZSTD(3)),
  system_time Int64 CODEC(Delta, ZSTD(3)),
  seq Int64 CODEC(Delta, ZSTD(3)),
  deleted Bool,
  value Nullable(Float64) CODEC(ZSTD(3)),          -- f64 value columns
  issue_time Nullable(Int64) CODEC(Delta, ZSTD(3)), -- timestamp value columns
  status LowCardinality(Nullable(String))           -- string value columns
) ENGINE = MergeTree ORDER BY (series_id, value_time, received_time, seq)
```

Loaded with `INSERT ... SELECT FROM file('versions.parquet')` then `OPTIMIZE TABLE v FINAL`
(one part; both counted as ingest). Same sort key as DuckDB, for the same reason (series +
value-time range pruning via the primary index).

Plain `MergeTree`, not `ReplacingMergeTree`: Muninn never replaces a version (history,
replay and every frontier other than "now" need all of them), so a Replacing engine would
only be valid for a "latest-now only" table. It is listed in notes/09 as a variant worth
adding for `*_latest_now`.

## Semantics in SQL

* `t_clock` and visibility as in the DuckDB notes; knowledge order `(t_clock, seq)` (seq
  order = commit order, §4.2).
* `latest`: `argMax(tuple(<output fields>, deleted), (t_clock, seq))` per point, then
  `WHERE NOT deleted`.
* `grid` / `asof_join`: `ASOF JOIN ... ON series_id, value_time AND frontier >= t`.
  ClickHouse ASOF takes one inequality column and picks an arbitrary row among equal
  `t`, so the right side is first reduced to one row per `(series, value_time, t_clock)`
  with `argMax(..., seq)`; then the ASOF match is unique and exact. As-of join left rows
  are sent as external data (Arrow stream) with the query.
* Grid pairs: each point's frontier index range is found with two ASOF joins against
  the sorted frontier list (ASOF needs an equality key: a constant column), expanded with
  `arrayJoin(range())`.

## Tuning log (small scale, warm, ms)

| Change | Effect |
| --- | --- |
| as-of join left rows as external data instead of `CREATE TABLE ... ENGINE=Memory` + insert per query | `da_asof_join_signals` (tiny) 70 → 12 |
| grid pairs via index range instead of `ARRAY JOIN arrayFilter(f -> window(f), frontiers)` (365 lambda evaluations per point) | `da_grid_365` 2741 → 522; `st_grid_365` 266 → 126; `bf_grid_365` 54 → 20 |
| grid via `ARRAY JOIN arrayFilter(f -> t <= f AND window, frontiers)` per *version* + `argMax` per (frontier, point) = `clickhouse-arrayjoin` | slower everywhere (`fc_grid_365` 1093 vs 587) |
| `optimize_aggregation_in_order=1` | grid slower (837 vs 668), point reads slightly faster (7 vs 11); not used |
| `join_algorithm='full_sorting_merge'` for the grid | slower (1239 vs 668) |

## Ideas for someone who knows ClickHouse better

* `ReplacingMergeTree(seq)` / a materialized "latest" table for current-truth queries.
* Projections ordered by `received_time` for replay.
* A tuned `index_granularity` for the many-tiny-series dataset.
* The native protocol (`clickhouse-driver`) instead of HTTP for small results: a constant
  ~3-4 ms per query here is client/HTTP overhead.
