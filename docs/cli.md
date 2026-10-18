# The `muninn` command-line tool

`muninn` works on a database in a local directory (`--db <dir>` or `MUNINN_DB`). Build it
with `cargo build --release -p muninn-cli`; the binary is `target/release/muninn`.
`muninn <command> --help` documents every flag.

| Area | Commands |
| --- | --- |
| Setup | `init <dir>` |
| Catalog | `table create/add-column/list/show`, `series create/list` |
| Import | `write [file]` (CSV or NDJSON, stdin by default) |
| Reads (SPEC §6) | `latest`, `history`, `replay`, `grid`, `asof-join`, `query` |
| Operations | `compact`, `gc`, `info`, `verify` |
| Export | `dump <dir>`, `restore <dump> <new-db>` (format: [`spec/DUMP.md`](../spec/DUMP.md)) |

Timestamps on the command line and in output are the SPEC §1 text form
(`2025-01-15T12:00:00Z`; offsets like `+01:00` are accepted and converted to UTC). Read
results print as an aligned table on a terminal and as CSV otherwise; `--format
csv|ndjson|table` chooses explicitly.

## Worked example: what did we know at 12:00?

A wind power forecast for DK1 is issued twice a day. Each run predicts the next day
hour by hour; we want to backtest with exactly what was known at a given moment.

```sh
export MUNINN_DB=./winddb
muninn init winddb
muninn table create wind_fc --column mw:f64 --column model:string   # default clock: received
muninn series create dk1_wind --table wind_fc --tag area=DK1 --tag kind=forecast --resolution 1h
```

Two runs as CSV: the 06:00 run (published 06:00, received 06:07) and the 12:00 run
(published 12:00, received 12:04).

```csv
series,value_time,published_time,received_time,mw,model
dk1_wind,2025-01-16T00:00:00Z,2025-01-15T06:00:00Z,2025-01-15T06:07:00Z,1510.0,ec
dk1_wind,2025-01-16T01:00:00Z,2025-01-15T06:00:00Z,2025-01-15T06:07:00Z,1625.5,ec
dk1_wind,2025-01-16T02:00:00Z,2025-01-15T06:00:00Z,2025-01-15T06:07:00Z,1702.0,ec
```

```sh
muninn write run_0600.csv --batch-id run-2025-01-15T06 --writer-clock 2025-01-15T06:08:00Z
# stored 3, duplicates 0, batches 1
muninn write run_1200.csv --batch-id run-2025-01-15T12 --writer-clock 2025-01-15T12:05:00Z
# stored 3, duplicates 0, batches 1
muninn write run_1200.csv --batch-id run-2025-01-15T12      # an accidental re-run is a no-op
# stored 0, duplicates 0, batches 1 (1 already committed, skipped)
```

(`--writer-clock` pins the commit clock so the example is reproducible; normally the
system clock is used and `system_time` is "now".)

**What did we know at 12:00?** The 12:00 run was only *received* at 12:04, so at
12:00 the 06:00 run is the latest knowledge:

```console
$ muninn latest --table wind_fc --frontier 2025-01-15T12:00:00Z
series    value_time            published_time        received_time         system_time           seq  mw      model
--------  --------------------  --------------------  --------------------  --------------------  ---  ------  -----
dk1_wind  2025-01-16T00:00:00Z  2025-01-15T06:00:00Z  2025-01-15T06:07:00Z  2025-01-15T06:08:00Z  1    1510.0  ec
dk1_wind  2025-01-16T01:00:00Z  2025-01-15T06:00:00Z  2025-01-15T06:07:00Z  2025-01-15T06:08:00Z  2    1625.5  ec
dk1_wind  2025-01-16T02:00:00Z  2025-01-15T06:00:00Z  2025-01-15T06:07:00Z  2025-01-15T06:08:00Z  3    1702.0  ec
(3 rows)
```

At 13:00 the 12:00 run (seq 4–6) has replaced it. Under the *published* clock
(`--clock published`) the 12:00 run is visible from 12:00 already, and `--system
2025-01-15T12:00:00Z` additionally restricts to what the database itself had stored by
then (a backtest that must not see later backfills).

The same question for many moments at once is a `grid`:

```console
$ muninn grid --table wind_fc --frontiers 2025-01-15T09:00:00Z,2025-01-15T13:00:00Z \
    --from 2025-01-16T00:00:00Z --to 2025-01-16T02:00:00Z
frontier              series    value_time            ...  seq  mw      model
--------------------  --------  --------------------  ...  ---  ------  -----
2025-01-15T09:00:00Z  dk1_wind  2025-01-16T00:00:00Z  ...  1    1510.0  ec
2025-01-15T09:00:00Z  dk1_wind  2025-01-16T01:00:00Z  ...  2    1625.5  ec
2025-01-15T13:00:00Z  dk1_wind  2025-01-16T00:00:00Z  ...  4    1388.0  ec
2025-01-15T13:00:00Z  dk1_wind  2025-01-16T01:00:00Z  ...  5    1490.0  ec
```

`--frontier-range 2025-01-15T00:00:00Z,2025-01-16T00:00:00Z,1h` generates frontiers, and
`--window 0h,36h` gives each frontier its own value window `[F, F + 36h)`.

Joining trading signals to the forecast known when each signal fired is an
`asof-join` with the left rows in CSV (`series,value_time,frontier`):

```console
$ muninn asof-join --table wind_fc --left signals.csv
series    value_time            frontier              found  ...  seq  mw      model
dk1_wind  2025-01-16T02:00:00Z  2025-01-15T06:00:00Z  false
dk1_wind  2025-01-16T02:00:00Z  2025-01-15T12:00:00Z  true   ...  3    1702.0  ec
dk1_wind  2025-01-16T02:00:00Z  2025-01-15T12:30:00Z  true   ...  6    1555.0  ec
```

`history` shows every version (tombstones included); `replay --since A --until B` lists
what became visible between two frontiers, in knowledge order.

## Importing

`muninn write [FILE|-]` reads CSV (default) or NDJSON (`--input ndjson`, or a `.ndjson` /
`.jsonl` file).

**CSV** has a header row. Recognised columns: `series` and `value_time` (required),
`published_time`, `received_time`, `deleted` (`true/false/1/0`); every other column is a
value column of the row's table. One file may mix tables: a value column that the row's
table lacks must be empty in that row.

- An empty field is null (so CSV cannot write an empty *string*; use NDJSON for that).
  Quoted fields may contain commas, quotes (`""`) and newlines.
- `f64`: any number, `NaN`, `inf`, `-inf` (`-0` keeps its sign). `i64`: integers in the
  full 64-bit range, no fraction or exponent. `bool`: `true/false/1/0`. `timestamp`:
  like every other timestamp in the file.
- `--time-format rfc3339|unix-us|unix-ms|unix-s` sets how the file's timestamps are
  written (`unix-ms` and `unix-s` accept up to µs precision fractions).
- `received_time` is required by the spec. Rows without one (no column, or an empty
  field) take `--received-time <timestamp>` or `--received-time now` (one reading for
  the whole import); otherwise the import fails.
- `system_time` and `seq` are assigned by the engine and are rejected in input (use
  `restore` to import them).

**NDJSON** has one object per line in the SPEC §9 write-row shape (`series`,
`value_time`, `published_time`, `received_time`, `deleted`, `values: {...}`); value
columns may also be given flat at the top level. Values use the SPEC §10.2 encodings
(`"NaN"`, big `i64` as decimal strings, …). The NDJSON output of `latest` without the
engine fields is valid input.

Errors name the input line (and column) and nothing is written:

```console
$ muninn write bad.csv
error: line 7, column "mw": type_mismatch: "12,5" is not an f64 (a number, NaN, inf or -inf)
```

**Atomicity and resuming.** Without `--chunk-rows` the whole file is one batch: it is
committed atomically or not at all. With `--chunk-rows N` the input is streamed and
committed in batches of `N` rows, so **atomicity is per chunk**: a failure (a bad line
or a crash) leaves the earlier chunks committed. Give the import a `--batch-id X` and
chunk `k` gets the id `X/k`; re-running the same command (after fixing the input) skips
the chunks that were committed (`batch_already_committed`) and writes the rest, so the
import resumes idempotently. Without a batch id a re-run relies on deduplication, which
only recognises rows with the same `received_time` (not `--received-time now`).
Re-running with a *different* chunk size or a modified input before the failure point
does not line up with the committed ids — keep both fixed.

## Reads

`latest`, `history`, `replay` and `grid` take `--table` (required), `--series a,b`
(default: all series of the table), the value range `--from` (inclusive) / `--to`
(exclusive), `--clock received|published|system` (default: the table's), and
`--system S`. `latest`/`history` take `--frontier F`; `replay` takes `--since`
(exclusive) and `--until` (inclusive); `grid` takes `--frontiers`, `--frontier-range`
and either `--window START,END` or `--from/--to`.

`muninn query '<step json>'` runs a SPEC §9 step (the `expect` field is ignored) through
the same JSON front end as the conformance suite and prints `{"rows": [...]}` exactly as
a scenario's `expect.rows` would be written. `muninn query @step.json` reads one step from
a file; `muninn query -` reads one step per line from stdin and prints one result per
line. Failures print `{"error": "<code>", "message": ...}` and make the exit code 1.

## Operations

- `compact [--table T]` merges each partition's segments into one; results never change.
- `gc [--grace 1h]` deletes segment files no committed state references (left by failed
  writes, or retired by compaction) once they are older than the grace period.
- `info` prints tables, series counts, versions, segments, bytes, log version, next seq.
- `verify` replays the whole log (checking every entry and every checkpoint against the
  replay), opens the database with the engine, and reads every live segment completely:
  header, footer and chunk checksums, statistics against the log, rows against the
  catalog, seq uniqueness. It prints `problem:` lines and exits 1 if anything is wrong.

## Dump and restore

```sh
muninn dump wind-dump               # manifest.json, catalog.json, batch_ids.ndjson, tables/*.ndjson
muninn restore wind-dump winddb2    # a new database, identical in every read
muninn --db winddb2 verify
```

The dump is plain NDJSON/JSON with checksums, documented in
[`spec/DUMP.md`](../spec/DUMP.md). The restored database returns the same rows for every
read under every clock, frontier and system bound — including `system_time` and `seq` —
and continues identically (same next seq, same batch ids, same dedup).

Limitations: `dump` holds one table's versions in memory at a time (to sort them by seq);
a restore is not atomic as a whole (delete the target and re-run on failure).
