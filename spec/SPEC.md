# Muninn semantics spec (v0.1)

This document is **normative**. It defines what every Muninn implementation must return
— the Python reference (`spec/reference/`), the Rust in-memory model
(`crates/muninn-core::model`) and the Rust engine (`crates/muninn-engine`). Physical
layout, compression, compaction and caching are *not* part of the spec; they must never
change a result.

The conformance suite (`spec/scenarios/*.json`, format in §9) is the executable form of
this document. If the prose and a scenario disagree, that is a bug in one of them — fix it,
don't paper over it.

Keywords MUST / MUST NOT / SHOULD are used in the RFC 2119 sense.

---

## 1. Timestamps and durations

- A **timestamp** is a signed 64-bit integer: **microseconds since the Unix epoch, UTC**.
  (Decision 0002.)
- Valid timestamps lie in `[0001-01-01T00:00:00Z, 9999-12-31T23:59:59.999999Z]`. Anything
  outside is rejected with `invalid_timestamp`.
- Text form (used by scenarios, CSV, CLI): RFC 3339 with a `Z` suffix and 0–6 fractional
  digits, e.g. `2025-01-15T12:00:00Z`, `2025-01-15T12:00:00.25Z`. Numeric offsets
  (`+01:00`) MUST be accepted on input and converted to UTC. Canonical output: no fraction
  if the sub-second part is zero, otherwise exactly 6 digits.
  Implementations compare timestamps as integers, never as strings.
- A **duration** in text form is `[-]<integer><unit>` with unit one of `us ms s m h d`
  (`d` = 86 400 s exactly; no calendars). E.g. `36h`, `-1d`, `90m`.

## 2. Catalog

### 2.1 Tables

A **table** groups series that share one value schema.

```
table:
  name            [a-z][a-z0-9_]*, unique
  columns         ordered list of (name, type); may be empty
  default_clock   received | published | system         (default: received)
  partition       day | month | year                     (default: day; physical only)
```

Column types: `f64`, `i64`, `bool`, `string`, `timestamp`. All value columns are nullable.

Column names match `[a-z][a-z0-9_]*`, are unique within the table, and MUST NOT be one of the
reserved names: `series`, `value_time`, `published_time`, `received_time`, `system_time`,
`seq`, `deleted`, `frontier`, `found`, `values`.

**Schema evolution** is additive only: `add_column(table, name, type)` appends a column.
Versions stored before the column existed read it as `null`. No drops, renames or type
changes in v1.

### 2.2 Series

```
series:
  id         u32, assigned by the engine: 1, 2, 3, ... in creation order, never reused
  name       non-empty string, unique across the whole database
  table      the table it belongs to (fixed for life)
  tags       string → string map (descriptive; no semantics)
  timezone   IANA name, default "UTC"  (metadata only in v0.1)
  resolution duration or null (irregular) (metadata only in v0.1)
```

Wherever results are ordered "by series", the order is by `id`.

### 2.3 Catalog history

Every catalog change (create table, add column, create series, change tags) is appended
to a change log with its `system_time`. v0.1 reads always use the *current* catalog;
point-in-time catalog reads are future work.

## 3. Versions

A logical **point** is identified by `(series, value_time)`. Each point has zero or more
**versions**. A version is:

| Field | Type | Set by | Notes |
| --- | --- | --- | --- |
| `series` | series id | writer input | |
| `value_time` | timestamp | writer input | what moment the values are about |
| `published_time` | timestamp or null | writer input | when it became public; null = unknown |
| `received_time` | timestamp | writer input | when we got it; **required** |
| `deleted` | bool | writer input | tombstone flag; default false |
| `values` | column → value or null | writer input | one entry per table column |
| `system_time` | timestamp | **engine** | when this version was committed |
| `seq` | u64 | **engine** | global, strictly increasing, unique |

Versions are immutable. Nothing in this spec ever modifies or removes a stored version
(purge and retention are explicit future operations that are allowed to break
reproducibility; they are not part of v0.1).

A **tombstone** (`deleted = true`) MUST have every value null; a write that supplies a
non-null value on a tombstone is rejected with `tombstone_with_values`.

## 4. Writes

`write(rows, batch_id?)` commits a **batch** atomically: either every row is applied or
none is (any validation error rejects the whole batch and changes nothing — `seq` and
`system_time` state included).

### 4.1 Validation (in this order, first failure wins per row; rows checked in input order)

1. Series name exists → else `unknown_series`. (All rows in a batch MAY belong to
   different tables.)
2. Every key in `values` is a column of the series' table → else `unknown_column`.
3. Every non-null value matches its column's type → else `type_mismatch`.
   - `f64` accepts any JSON number (integers are converted). Non-finite values are
     written as the strings `"NaN"`, `"inf"`, `"-inf"` in JSON and are legal.
   - `i64` accepts integral numbers in i64 range, or a decimal string (for values beyond
     2^53). `1.5` → `type_mismatch`.
   - `timestamp` accepts the text form (§1).
4. Tombstone with any non-null value → `tombstone_with_values`.
5. `value_time`, `received_time`, `published_time` (if non-null) are valid timestamps →
   else `invalid_timestamp`.

Missing value keys are null.

### 4.2 System time and seq

The writer has a clock. Let `c` be the clock reading at commit and `last` the greatest
`system_time` of any stored version (or −∞ if nothing is stored yet). Then

```
batch.system_time = max(c, last)        -- non-decreasing; all rows in a batch share it
```

`seq` is a single counter for the whole database starting at **1**. Each *stored* row
(after deduplication, §4.3) takes the next value, in batch input order. Deduplicated rows
and no-op batches consume no seq. Rejected batches consume nothing.

### 4.3 Deduplication (idempotency)

Two versions are **content-identical** if they agree on `series`, `value_time`,
`published_time`, `received_time`, `deleted` and every value (values compared by type:
`f64` **bitwise** — so `NaN` equals the same `NaN` bit pattern and `-0.0 ≠ 0.0` — others by
equality; null equals null). A column that did not exist when an old version was written
compares as null.

A row in a batch is **dropped as a duplicate** if it is content-identical to any already
stored version (any system_time) or to an earlier *kept* row of the same batch. The
existing version keeps its `system_time` and `seq`.

Consequence: resending a batch is harmless. A byte-identical value with a *new*
`received_time` is **not** a duplicate — it is a new version (it carries the information
"we were sent this again at t"). See decision 0003.

### 4.4 Batch ids

If `batch_id` (non-empty string) is given and a batch with that id was committed before,
the write is a **no-op** regardless of its rows (no validation, nothing stored), and the
result says so. Batch ids are remembered forever in v0.1.

### 4.5 Result

```
{ stored: <rows stored>, duplicates: <rows dropped by §4.3>, batch_already_committed: bool }
```

A batch where every row is a duplicate stores nothing. It still records its batch id, if
any, but it does not affect `last` in §4.2 (only stored versions do).

## 5. Clocks, visibility and knowledge order

There are three **clocks**. For a version `v`:

```
t_received(v)  = v.received_time
t_published(v) = v.published_time if not null else v.received_time
t_system(v)    = v.system_time
```

(Unknown published time falls back to received time. That is conservative: under a
published frontier, a version with unknown publication time becomes visible when we got it.)

A **read context** is:

```
clock      received | published | system      (default: the table's default_clock)
frontier   timestamp F, or unbounded           (default: unbounded)
system     timestamp S, or unbounded           (default: unbounded = the snapshot)
```

A version is **visible** in a context iff

```
v.system_time <= S   and   t_clock(v) <= F
```

Both bounds are **inclusive**.

The **knowledge order** under a clock is the total order by the key

```
k_clock(v) = (t_clock(v), v.system_time, v.seq)
```

`seq` is unique, so the order is total and results are deterministic. The latest version of
a point is the visible version with the greatest key. (Decision 0003 explains why the
frontier clock also orders versions.)

### 5.1 Snapshots

Every read runs against a **snapshot** — the set of committed batches at the moment the read
started. Concurrent writes never become partially visible. The system frontier `S` further
restricts *within* the snapshot.

## 6. Reads

All reads take a table, an optional series filter (list of names; omitted or empty = every
series in the table; a name that does not exist or belongs to another table →
`unknown_series`) and an optional **value range** `[value_from, value_to)` (either bound may
be omitted; `value_from > value_to` → `invalid_range`; equal bounds = empty range).

Unless stated otherwise, output rows carry:

```
series, value_time, published_time (stored, may be null), received_time,
system_time, seq, values (every current table column, null where absent)
```

### 6.1 `latest` — latest-known as of a frontier

For each point `(s, vt)` with `s` in the filter and `vt` in the value range: let `w` be its
latest visible version (§5). If there is none, or `w.deleted`, the point is absent.
Otherwise emit `w`.

Order: `(series, value_time)`.

This is *the* default read: "what did we know at F about these points".

### 6.2 `history` — all versions

Every version of every point in range that is visible in the context (frontier and system
both apply; leave them unbounded for full history). **Tombstones are included**; rows also
carry `deleted`.

Order: `(series, value_time, k_clock)`.

### 6.3 `replay` — arrival order

Every version with `series` in the filter, `value_time` in the value range,
`v.system_time <= S` and `t_clock(v)` in `[from, to)` (either bound optional).
Tombstones included; rows carry `deleted`.

Order: `k_clock` alone — i.e. `(t_clock, system_time, seq)` — across all series.

### 6.4 `grid` — many frontiers at once

Input: a list of frontiers `F_1..F_n` (duplicates removed, sorted ascending) and a value
window that is either

- **absolute**: `value_from`/`value_to` as in every read, or
- **relative**: `window_start`, `window_end` durations — for frontier `F_i` the value range
  is `[F_i + window_start, F_i + window_end)`. The sums are exact (no overflow, no
  clamping error); a bound outside the valid timestamp range simply admits or excludes
  everything on that side.

Result: for each `F_i`, exactly the rows of `latest` with `frontier = F_i` and that
window, each prefixed with `frontier = F_i`.

Order: `(frontier, series, value_time)`.

An implementation SHOULD compute this in one pass rather than `n` queries; the result MUST
equal the `n`-query definition.

### 6.5 `asof_join` — per-row frontier lookup

Input: a right table, a context (clock, system — the frontier comes from each row) and an
ordered list of left rows `(series, value_time, frontier)`. Each left row's series must
belong to the right table (`unknown_series` otherwise).

For each left row, in input order, emit:

```
series, value_time, frontier, found,
published_time, received_time, system_time, seq, values
```

where the right side is what `latest(right_table, [series], frontier = row.frontier)`
returns for the point `(series, value_time)` — exact `value_time` match (as a §6 half-open
value range: `[value_time, value_time + 1us)`). If there is no such version (or it is
a tombstone), `found = false` and all right-side fields are null (`values` itself is
`null`, not an object of nulls).

Order: left input order; one output row per left row.

(Future: inexact alignment — latest `value_time <= left value_time` and "contained in" for
different resolutions.)

## 7. Invariants (properties every implementation must satisfy)

These are tested by property/fuzz tests, not by fixed scenarios.

1. **Noninterference (no leakage).** For any context `(clock, F, S)`, the result of
   `latest`, `history`, `grid` and `asof_join` depends only on the versions with
   `system_time <= S` and `t_clock <= F` (per row for `asof_join`). Adding, changing or
   tombstoning anything outside that set MUST NOT change the result in any bit.
2. **Physical independence.** Results do not depend on segment boundaries, compaction,
   partition granularity, cache state, process restarts or the order in which segments are
   read.
3. **Snapshot stability.** A read against snapshot `X` returns the same result forever
   (until an explicit purge).
4. **Grid ≡ repeated latest.** As stated in §6.4.
5. **Dedup idempotency.** Writing any batch twice (without batch id) stores nothing the
   second time and changes no read.
6. **Write atomicity.** After a crash at any point during a write, the database is either
   as before the batch or as after it; never in between. Recovery never needs operator
   action.

## 8. Errors

Error codes, used in scenarios and APIs:

| Code | When |
| --- | --- |
| `unknown_table` | table name not in catalog |
| `unknown_series` | series name not in catalog / not in the read's table |
| `unknown_column` | write names a column the table doesn't have |
| `type_mismatch` | value doesn't match column type |
| `tombstone_with_values` | §3 |
| `invalid_timestamp` | out of range / unparsable timestamp |
| `invalid_range` | `value_from > value_to`, `from > to`, bad window |
| `invalid_name` | table/column/series name breaks the rules or is reserved |
| `already_exists` | duplicate table, column or series name |

A failed operation has no effect.

## 9. Scenario format (conformance suite)

One JSON file per scenario in `spec/scenarios/`, named `NNN-short-name.json`. Generated
(randomised) scenarios live in `spec/scenarios/generated/` and use the same format.

```jsonc
{
  "name": "corrected-published-time",
  "description": "Free text: what this scenario pins down and why.",
  "steps": [ <step>, ... ]
}
```

Steps run in order against a fresh, empty database. Every step may carry an `"expect"`;
a step without `expect` must simply succeed. `"expect": {"error": "<code>"}` means the step
must fail with that code (and have no effect).

### Catalog steps

```jsonc
{"op": "create_table", "name": "prices",
 "columns": [{"name": "value", "type": "f64"}],
 "default_clock": "received", "partition": "day"}          // last two optional

{"op": "add_column", "table": "prices", "name": "volume", "type": "f64"}

{"op": "create_series", "name": "DK1", "table": "prices",
 "tags": {"area": "DK1"}, "timezone": "Europe/Copenhagen", "resolution": "1h"}
                                                            // tags/timezone/resolution optional
```

### Write step

```jsonc
{"op": "write", "system_time": "2025-01-15T12:00:00Z", "batch_id": "b1",   // batch_id optional
 "rows": [
   {"series": "DK1", "value_time": "2025-01-16T00:00:00Z",
    "published_time": "2025-01-15T11:00:00Z",              // optional / null
    "received_time": "2025-01-15T11:05:00Z",
    "deleted": false,                                      // optional
    "values": {"value": 42.5}}                             // optional
 ],
 "expect": {"stored": 1, "duplicates": 0, "batch_already_committed": false}}
```

`system_time` is the writer clock reading `c` from §4.2 (the engine still applies the
`max(c, last)` rule).

### Read steps

Common fields: `"table"`, `"series"` (optional list of names), `"value_from"`,
`"value_to"` (optional), `"clock"` (optional), `"frontier"` (optional), `"system"`
(optional).

```jsonc
{"op": "latest",  ..., "expect": {"rows": [ <row>, ... ]}}
{"op": "history", ..., "expect": {"rows": [ ... ]}}
{"op": "replay",  ..., "from": "...", "to": "...", "expect": {"rows": [ ... ]}}   // no "frontier"
{"op": "grid",    ..., "frontiers": ["...", "..."],
                       "window_start": "0h", "window_end": "36h",               // or value_from/value_to
                       "expect": {"rows": [ ... ]}}                              // no "frontier"
{"op": "asof_join", "table": "...", "clock": "...", "system": "...",
                    "left": [{"series": "DK1", "value_time": "...", "frontier": "..."}],
                    "expect": {"rows": [ ... ]}}
```

Expected rows are objects with exactly the fields listed for the op in §6 (`values` as a
nested object containing every current column of the table, nulls included). Runners MUST
compare the complete row list exactly, in order (timestamps compared as integers, `f64`
bitwise except that a JSON number in the expectation is parsed to the nearest f64).
Values use the same JSON encodings as writes (§4.1): non-finite `f64` as `"NaN"`, `"inf"`,
`"-inf"`; `i64` as a JSON integer or a decimal string (compared as integers; generated
scenarios use strings beyond ±2^53); `timestamp` columns in the §1 text form (compared as
integers). A write's `expect` (other than `error`) is the complete §4.5 result object.

Optional `"expect"` sugar to keep hand-written scenarios readable: `"rows"` may be replaced
by `"values"` when the scenario only cares about which value won. Runners then compare just
these fields (still the full list, in order):

| op | `values` entry |
| --- | --- |
| `latest` | `[series, value_time, {values}]` |
| `history`, `replay` | `[series, value_time, {values}]`, or `[series, value_time, "deleted"]` for a tombstone |
| `grid` | `[frontier, series, value_time, {values}]` |
| `asof_join` | `[series, value_time, frontier, {values}]`, or `[series, value_time, frontier, null]` when not found |

In the sugar form `{values}` may list a subset of columns; only those are compared.
