# DuckDB baseline — setup and tuning

Two systems share one query generator (`adapter.py`):

| System | Storage | Load (counted as ingest) |
| --- | --- | --- |
| `duckdb-native` | DuckDB database file, table `v` | `CREATE TABLE v AS SELECT ... FROM read_parquet(raw) ORDER BY series_id, value_time, received_time, seq; CHECKPOINT` |
| `duckdb-parquet` | one zstd Parquet file, row groups of 122 880 rows, no dictionary encoding, queried via `read_parquet` | `COPY (same SELECT ... ORDER BY ...) TO ... (COMPRESSION zstd, ROW_GROUP_SIZE 122880, DICTIONARY_SIZE_LIMIT 0)` |
| `duckdb-native-window` | as `duckdb-native` | alternative query strategies (below), kept runnable |

Settings: `threads=6`, `memory_limit=4096MB`, `asof_loop_join_threshold=0`. DuckDB 1.5.6
via the `duckdb` Python package.

## Schema

`series_id UINTEGER, value_time, published_time, received_time, system_time BIGINT (µs),
seq BIGINT, deleted BOOLEAN`, then the value columns (`DOUBLE`, `VARCHAR`, timestamps as
`BIGINT` µs). Timestamps are BIGINT rather than `TIMESTAMP` only to keep the key arithmetic
below cast-free; the two are the same 64-bit integer physically. Series names live in the
catalog (the adapter maps ids to names on output, which is included in the timing).

**Sort order** `(series_id, value_time, received_time, seq)`: almost every query filters
one or a few series and a value-time range, and DuckDB prunes row groups with min/max
zone maps, so physical clustering by (series, value_time) is what makes point reads cheap.
Replay (filtering on t_clock across all series) is the query this order hurts; we
measured it anyway rather than pick a layout per query.

## Semantics in SQL

* `t_clock`: `received_time`, `coalesce(published_time, received_time)` (§5: unknown
  published falls back to received), or `system_time`.
* Visibility: `system_time <= S AND t_clock <= F`, both inclusive.
* Knowledge order: `(t_clock, system_time, seq)`. Because seq is assigned in commit order
  and system_time never decreases in commit order (§4.2), `(system_time, seq)` orders
  exactly like `seq`, so the key is `(t_clock, seq)`, packed into one HUGEINT
  `t_clock * 2^40 + seq` (seq < 2^40 is asserted at load). This is a property the spec
  guarantees, so any system may use it.
* `latest`: `arg_max_null(<field>, key)` for every output field (and `deleted`) grouped by
  point, then `WHERE NOT deleted` (tombstone filtering *after* picking the latest).
* `history` / `replay`: filtered scans ordered by `(series, value_time, key)` / `key`;
  replay is `t_clock > from AND t_clock <= to`.
* `grid`: `(frontier, point)` pairs, then `ASOF JOIN` versions on
  `(series_id, value_time)` and `frontier * 2^40 + (2^40 - 1) >= key` — the greatest key
  with `t_clock <= frontier` — then drop tombstones.
* `asof_join`: the left rows are registered as an Arrow table; `ASOF LEFT JOIN` as above
  with the per-row frontier; `found = coalesce(NOT deleted, false)`; right-side fields
  nulled when not found.
* lead-time filter: `issue_time IS NOT NULL AND value_time - issue_time` in `[min, max)`.

## Correctness issues found (DuckDB 1.5.6)

SPEC compares f64 bitwise, so `-0.0` and `0.0` are different values. Two DuckDB code
paths merge them:

* the **Parquet writer's dictionary encoding** treats them as one dictionary entry, so
  rows silently swap the sign of zero (found by the medium run's cross-check: 4 of 19
  zeros flipped). Fixed with `DICTIONARY_SIZE_LIMIT 0`, which costs ±5% in size here;
* **`arg_max(struct_pack(...), key)`** returns `0.0` where the winning row holds `-0.0`
  (plain `arg_max(value, key)` is fine). Fixed by one `arg_max_null` per column; note
  `arg_max` (without `_null`) skips NULL arguments, which is also wrong here.

Both are worth reporting upstream with a minimal repro; the generator now emits rare
`±0.0` prices so the tiny-scale oracle test covers this.

## Tuning log (small scale, warm, ms)

| Change | Effect |
| --- | --- |
| `asof_loop_join_threshold = 0`: the planner estimates the left side of an ASOF join from `unnest`/Arrow scans as 1-17 rows and switches to a nested-loop join below 64 | `da_asof_join_signals` ~1000 → 75; `st_grid_365` (tiny) 450 → 60 |
| Grid pairs: instead of joining points × frontiers on the window condition (DuckDB plans a nested loop + filter: 104M intermediate rows for `da_grid_365`), find each point's frontier *index range* with two ASOF joins against the sorted frontier list and expand it with `unnest(range())` — general for any sorted frontier list, linear in the output | `da_grid_365` 2900 → 300; `st_grid_365` 285 → 40; `bf_grid_365` 59 → 13 |
| Grid via window function (`row_number() OVER (PARTITION BY frontier, point ORDER BY key DESC)` over a versions × frontiers range join), = `duckdb-native-window` | after the change above: better only for `da_grid_365` (244 vs 304), worse on forecasts (241 vs 193) and settlement |
| `latest` via `arg_max` vs `QUALIFY row_number() = 1` (`duckdb-native-window`) | arg_max (then a packed struct) faster on many-version chains (`fc_latest_now` 9 vs 23), similar elsewhere; now per-column `arg_max_null`, see above |
| frontiers as a registered Arrow table instead of `unnest([...])` | no measurable difference for ASOF; small win for the window variant; kept |
| storage: `storage_compatibility_version='latest'`, `force_compression='zstd'` | no change: 25.8 B/version on forecasts either way. DuckDB's native format bit-packs (frame of reference) the µs timestamps at ~40 bits each and has no general-purpose codec; its own zstd Parquet of the same rows is 3.6 B/version |

Not done (judged unfair or not best practice): storing timestamps as deltas against
value_time, dropping columns the queries happen not to read, per-query layouts, result
caching.

## Ideas for someone who knows DuckDB better

* A second copy sorted by `received_time` for replay (costs ingest and bytes).
* Partitioned Parquet (hive layout by series/month) for very large datasets.
* An extension doing the per-point merge natively (that is what Muninn would be).
