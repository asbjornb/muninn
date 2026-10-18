# muninn (Python)

Python bindings for Muninn, a point-in-time-correct time series database. Every read answers
**"what did we know at this moment?"**. Versions that arrived later are invisible by
construction, so a backtest can't look ahead. Results are `pyarrow.Table`s
(`.to_pandas()`, `polars.from_arrow(...)`), and each one carries a reproducibility receipt.

This crate is a thin PyO3 layer (`muninn._native`) over `muninn-engine`, plus a small
pure-Python package (`python/muninn`) for input normalisation and receipts. The semantics
are those of [`spec/SPEC.md`](../../spec/SPEC.md); the bindings add nothing to them.

## Build and test

You need `uv` and a Rust toolchain. Python 3.10 or newer works (abi3 wheel), and 3.14 is tested.

```sh
cd crates/muninn-py
uv sync                                  # dev venv: maturin, pytest, hypothesis, pandas, polars
uv run maturin develop --uv --release    # build the extension, install it editable into .venv
uv run pytest                            # corpus + differential + API tests
```

Or from the repository root, `task py-test` (this builds and tests, with `CARGO_BUILD_JOBS=4`).
Run `uv run maturin build --release` to get a wheel in `target/wheels/`.
`MUNINN_PY_EXAMPLES=5000 uv run pytest tests/test_differential.py` runs a longer differential
campaign (the default is 300 examples).

## A backtest loop

```python
import pandas as pd
import muninn

db = muninn.open("data/muninn", create=True)        # or muninn.memory()
db.create_table("wind_fc", columns={"mw": "f64"}, default_clock="published")
db.create_series("wind_dk1", "wind_fc", tags={"area": "DK1"}, resolution="1h")

# Two forecast runs for the same delivery hours; run 2 (published 06:00) revises run 1.
hours = pd.date_range("2025-01-16", periods=24, freq="h", tz="UTC")
for published, level in [("2025-01-15T00:00Z", 100.0), ("2025-01-15T06:00Z", 120.0)]:
    pub = pd.Timestamp(published)
    db.write(pd.DataFrame({
        "series": "wind_dk1",
        "value_time": hours,                     # what the value is about
        "published_time": pub,                   # when it became public
        "received_time": pub + pd.Timedelta("5min"),   # when we got it
        "mw": level,
    }))

# For each decision time: what did I know about tomorrow's hours?
decisions = pd.date_range("2025-01-15T03:00Z", periods=2, freq="6h")
for t in decisions:
    known = db.latest("wind_fc", frontier=t,
                      value_from=t + pd.Timedelta("21h"),
                      value_to=t + pd.Timedelta("45h")).to_pandas()
    ...  # 03:00 sees run 1 only (mean 100.0); 09:00 sees run 2 (120.0)

# The same in one pass: one row per (decision time, delivery hour).
training = db.grid("wind_fc", decisions, window=("21h", "45h")).to_pandas()

# Per-row lookups: append "what was known at each row's frontier" to your own table.
signals = pd.DataFrame({"series": "wind_dk1", "value_time": hours[:3],
                        "frontier": decisions[0], "signal": [1, 0, 1]})
joined = db.asof_join("wind_fc", signals).to_pandas()   # + found, ..., mw
```

## Why this can't leak

A backtest leaks when a query at decision time `F` sees something that wasn't known at `F`.
Usually that's a window function, a `max(received_time)` or a join someone got slightly
wrong. Muninn doesn't depend on getting those right:

- **Visibility is the read.** `latest`, `history`, `grid` and `asof_join` only see versions
  with `t_clock <= F` (and `system_time <= S`). The engine applies that filter before it
  picks anything, and no option turns it off. If you leave out `frontier`, it is unbounded
  ("today's knowledge"), and that shows in the receipt.
- **Noninterference is tested.** SPEC §7.1: changing, adding or deleting anything known
  after `F` must not change a result at `F` in any bit. The engine is fuzzed for this, and
  these bindings are tested against the reference implementation (below).
- **Ambiguous times are rejected, not guessed.** Naive datetimes (`datetime(2025, 1, 1)`,
  naive pandas columns) and sub-microsecond timestamps raise `InvalidTimestampError`
  instead of being read as local time or truncated. `NaT` as a frontier is an error and
  never means "unbounded".
- **Results are reproducible.** Each result's schema metadata holds a receipt
  `(table, op, request, clock, F, S)`, where `S` is the snapshot's greatest `system_time`
  if you gave none. `muninn.replay_receipt(db, receipt)` re-runs the read and returns the
  same rows bit for bit, even after later writes and corrections (SPEC §7.3, §11).

So an agent or a quant writing `db.latest(table, frontier=t)` in a loop gets a query that
can't leak, whatever else they get wrong.

## API

| | |
| --- | --- |
| `muninn.open(path, *, create=False)` / `muninn.memory()` | open a local directory / an in-memory database |
| `db.create_table(name, columns={"value": "f64"}, default_clock="received", partition="day")` | types `f64 i64 bool string timestamp` (or `float int bool str datetime`) |
| `db.add_column(table, name, type)`, `db.create_series(name, table, tags=None, timezone=None, resolution=None)` | |
| `db.tables()`, `db.table(name)`, `db.series(table=None, **tag_filters)` | `TableInfo` / `SeriesInfo` dataclasses |
| `db.write(data, *, batch_id=None, clock=None, assume_utc=False)` | `WriteResult(stored, duplicates, batch_already_committed, system_time, first_seq)` |
| `latest(table, series=None, value_from=None, value_to=None, clock=None, frontier=None, system=None)` | SPEC §6.1 |
| `history(...)` (same arguments) | §6.2, every visible version, tombstones included (`deleted`) |
| `replay(table, from_, to, *, series, value_from, value_to, clock, system)` | §6.3, arrival order, `(from_, to]` |
| `grid(table, frontiers, *, window=("0h", "36h") \| value_from/value_to, series, clock, system)` | §6.4 |
| `asof_join(table, left, clock=None, system=None, *, assume_utc=False, suffix="_right")` | §6.5, appends to `left` |
| `db.snapshot()` | a read-only `Snapshot` with the same read methods, pinned to one commit |
| `db.compact(table=None, partition=None)`, `db.gc(grace=None)` | maintenance; neither changes a result |
| `muninn.receipt(result)`, `muninn.replay_receipt(db, receipt, *, left=None)` | SPEC §11 |

Every read on `db` takes a fresh snapshot. A `Snapshot` keeps seeing exactly the same data
whatever is written, compacted or garbage-collected meanwhile. The GIL is released for all
engine work, so threads can read concurrently.

**Write input.** `data` can be a pyarrow `Table` / `RecordBatch`, a pandas or polars
`DataFrame`, anything with `__arrow_c_stream__`, or a list of dicts. The columns are
`series`, `value_time` and `received_time` (required), `published_time` (optional; null means
unknown), `deleted` (optional bool), and one column per value. A dict row may instead use the
spec's nested form `{"values": {...}}`, and then it can't have any other keys.

- Timestamps can be Arrow `timestamp` in any unit and zone (converted to µs UTC), integers
  (µs since the epoch), or SPEC §1 strings (`2025-01-15T12:00:00Z`, `...+01:00`). Python
  `datetime` and `pandas.Timestamp` must carry a time zone. Naive values are rejected
  unless you pass `assume_utc=True`. `numpy.datetime64` is taken as UTC, since numpy has
  no time zones.
- pandas missing values (`NaN`, `None`, `NaT`, `pd.NA`) become nulls. In pyarrow, polars
  and dict input, `NaN` is a real NaN and `None` is null. Use pandas' nullable `Int64` for
  integer columns with gaps: a float column into an `i64` column is a `type_mismatch`, as in
  the spec.
- Integers keep their full i64 range. Values beyond it raise `TypeMismatchError`, never
  wrap. Integers written to `f64` columns are converted.
- In columnar input a null cell is the same as a missing value. That's how one batch can
  span tables with different columns.

**Read output.** You get a `pyarrow.Table` in spec order. It has `series` (name) and
`series_id`, then `value_time`, `published_time`, `received_time`, `system_time` (all
`timestamp[us, tz=UTC]`), `seq` (`uint64`), `deleted` (history/replay), and the value
columns (`float64`, `int64`, `bool`, `string`, `timestamp[us, tz=UTC]`). `grid` puts
`frontier` first. Time arguments accept `datetime`, `pandas.Timestamp`, `numpy.datetime64`,
`str` or `int` µs.

**Errors.** `muninn.MuninnError` is the base class. Each subclass has `.code` set to the
spec code: `UnknownTableError` (`unknown_table`), `UnknownSeriesError`, `UnknownColumnError`,
`TypeMismatchError`, `TombstoneWithValuesError`, `InvalidTimestampError`,
`InvalidRangeError`, `InvalidNameError`, `AlreadyExistsError`, `InvalidArgumentError` (also
a `TypeError`), `IoError` (`io`), `ConflictError` (`conflict`) and `InternalError`. A failed
call has no effect. The exception is `IoError` on a write, whose outcome is unknown: resend
with the same `batch_id`. Errors come in the spec's check order (§10.4); for example,
`unknown_table` wins over a malformed frontier.

## Arrow

Input goes through the Arrow PyCapsule interface (`__arrow_c_stream__`), and output is built
with `arrow-array` and returned as a `pyarrow.Table` through the C data interface.
The crate depends on `arrow-pyarrow`, `arrow-array`, `arrow-schema` and `arrow-buffer`, with
no compute, cast, CSV, JSON or IPC kernels. The bindings code has no `unsafe` of its own.
pyarrow (≥ 14) is the only runtime Python dependency; pandas and polars are optional.

## Tests

- `tests/test_corpus.py` runs the whole SPEC §9 corpus (`spec/scenarios/**/*.json`) through
  the Python API, using the reference's own comparison code (`muninn_ref.scenario`). It runs
  in memory for every scenario, and in four more variants for the hand-written ones and
  every fifth generated one: local directory, reopen before each read, compact before each
  read, and reads on a pinned snapshot. `tests/adapter.py` turns spec JSON into Python
  calls.
- `tests/test_differential.py` uses hypothesis to generate random catalogs, writes
  (duplicates, tombstones, batch ids, resends, invalid rows, schema evolution) and reads of
  all five kinds. It runs them through the bindings and through the pure-Python reference
  `spec/reference/muninn_ref`, and the rows, write results and error codes must be
  identical.
- `tests/test_api.py` covers the input forms, timestamp units and zones, naive rejection,
  NaN and None, big integers, output dtypes, receipt round trips, snapshot isolation, error
  codes and check order, persistence across reopen, and concurrent reads.

## Limitations

- Writes are converted row by row into the engine's write rows, which is fine for normal
  ingest batches but not zero-copy. Results are built column by column.
- `asof_join` matches `value_time` exactly (SPEC §6.5). Inexact alignment is future work.
- An `asof_join` receipt includes every left row.
- Only local directories and memory are supported (no object stores yet). One writer per
  database, and `refresh()` picks up another process's commits.
- A receipt taken on an empty database records `S` as "empty". Replaying it reads at
  `0001-01-01T00:00:00Z`, which matches only if no version was ever committed with that
  exact writer clock.
