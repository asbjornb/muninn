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
  if the sub-second part is zero, otherwise exactly 6 digits. The exact grammar is in
  §10.1. Implementations compare timestamps as integers, never as strings.
- A **duration** in text form is `[-]<integer><unit>` with unit one of `us ms s m h d`
  (`d` = 86 400 s exactly; no calendars). E.g. `36h`, `-1d`, `90m`. The value in
  microseconds must fit in i64.

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

0. Before any of the steps below, the **whole request** is checked for structure
   (§10.3) → else `invalid_argument`. This includes every row, and happens even when the
   batch id is already known (§4.4).
1. Series name exists → else `unknown_series`. (All rows in a batch MAY belong to
   different tables.)
2. Every key in `values` is a column of the series' table → else `unknown_column`.
3. Every non-null value matches its column's type → else `type_mismatch`.
   - `f64` accepts any JSON number (integers are converted). Non-finite values are
     written as the strings `"NaN"`, `"inf"`, `"-inf"` in JSON and are legal.
   - `i64` accepts integral numbers in i64 range, or a decimal string (for values beyond
     2^53). `1.5` → `type_mismatch`.
   - `timestamp` accepts the text form (§1).
   - Exact encodings: §10.2.
4. Tombstone with any non-null value → `tombstone_with_values`.
5. `value_time`, `received_time`, `published_time` (if non-null) are valid timestamps →
   else `invalid_timestamp`. `value_time` and `received_time` are required: missing or
   null is `invalid_timestamp`.

Missing value keys are null.

### 4.2 System time and seq

The writer has a clock. Let `c` be the clock reading at commit and `last` the greatest
`system_time` of any stored version (or −∞ if nothing is stored yet). Then

```
batch.system_time = max(c, last + 1us)  -- strictly increasing; all rows in a batch share it
```

(For the first stored batch, `batch.system_time = c`.) System times are therefore strictly
increasing across stored batches, so no later write can ever land at or below a system
bound `S` taken from an existing snapshot — this is what makes §7.3 hold. A batch that
stores nothing (all duplicates, empty, a batch-id no-op, rejected) does not change `last`.

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
the write is a **no-op** regardless of its rows (no §4.1 validation beyond the structural
step 0, nothing stored), and the result says so. Batch ids are remembered forever in v0.1.
A rejected batch does not record its id, so it can be retried with the same id. An empty
`rows` list is a valid batch (stores nothing, records its id).

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
`v.system_time <= S` and `t_clock(v)` in `(from, to]` — `from` exclusive, `to` inclusive,
either bound optional. `from > to` → `invalid_range`; `from == to` is empty.
Tombstones included; rows carry `deleted`.

Order: `k_clock` alone — i.e. `(t_clock, system_time, seq)` — across all series.

Because frontiers are inclusive, `replay(from = F1, to = F2)` is exactly the set of versions
that become visible when the frontier moves from `F1` to `F2` (same clock, `S`, filter and
value range), in knowledge order. Every such version has a greater key than anything
visible at `F1`, so applying them in order to `latest` at `F1` — a value version replaces
the point, a tombstone removes it — gives `latest` at `F2` (§7, invariant 7). An omitted
`from` means "from nothing"; an omitted `to` means unbounded.

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
3. **Reproducibility.** Let `T` be the greatest system_time in snapshot `X`. For any
   `S <= T`, every read with system bound `S` returns the same result when run against
   `X` and against every later snapshot (until an explicit purge). It follows from §4.2:
   later writes get system_time `> T >= S`, so they are invisible at `S` (§7.1). This is a
   guarantee about *results*, not physical snapshots: a pinned snapshot (log version) is
   only guaranteed readable while it is held; after compaction and garbage collection old
   log versions may become unreadable, but the same results stay available from any
   later snapshot with the same `S`.
4. **Grid ≡ repeated latest.** As stated in §6.4.
5. **Dedup idempotency.** Writing any batch twice (without batch id) stores nothing the
   second time and changes no read.
6. **Write atomicity.** After a crash at any point during a write, the database is either
   as before the batch or as after it; never in between. Recovery never needs operator
   action.
7. **Replay is the frontier delta.** For any clock, `S`, series filter, value range and
   `F1 <= F2` (`F1` possibly omitted, `F2` possibly unbounded): start from the rows of
   `latest` at `F1` and apply the rows of `replay(from = F1, to = F2)` in order (a value
   version replaces its point's row, a tombstone removes it). The result is exactly
   `latest` at `F2`.

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
| `invalid_argument` | structurally malformed request (§10.3): unknown column type, clock or partition; wrong JSON type for a container, name, flag or enumeration; empty or non-string `batch_id`; unknown request field |

A failed operation has no effect. When a request has several problems, the reported code
is fixed by the check order in §10.4.

### 8.1 Engine-only errors

Two further codes come from a storage engine, not from the semantics. They never appear
in scenarios.

| Code | When | Effect |
| --- | --- | --- |
| `conflict` | another writer committed first | no effect; safe to retry |
| `io` | storage failure | **unknown**: the write either fully happened or not at all (never partially, §7.6) |

The "no effect" rule applies to the §8 codes and `conflict`, not to `io`. A client
resolves an `io` failure on a write by resending it with the same `batch_id` (§4.4) or by
relying on deduplication (§4.3): either way the resend is a no-op if the first attempt
committed.

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
`max(c, last + 1us)` rule).

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

## 10. Input rules and check order

Requests are the JSON shapes of §9. This section fixes every input detail that two
implementations could otherwise decide differently.

### 10.1 Timestamp text grammar

```
YYYY-MM-DDTHH:MM:SS[.f{1,6}](Z|+HH:MM|-HH:MM)
```

- ASCII digits only; upper-case `T` and `Z` only; exactly 4-digit year and 2-digit
  fields. No space separator, no `+HHMM` or `+HH` offsets.
- 1–6 fraction digits; a bare `.` or 7+ digits is rejected (no rounding or truncation).
- Seconds `00..59` (no leap seconds); hours `00..23`; the date must exist in the proleptic
  Gregorian calendar (`2024-02-29` valid, `2025-02-29` invalid).
- Offset hours `00..23`, minutes `00..59`; `-00:00` means UTC.
- The §1 range check applies to the **UTC** value: `9999-12-31T23:30:00-01:00` is out of
  range; `0000-12-31T23:30:00-01:00` (= `0001-01-01T00:30:00Z`) is valid.
- A timestamp field holding anything but a string (e.g. a JSON number of microseconds) is
  `invalid_timestamp`.

### 10.2 Value encodings

- **f64.** Any JSON number; integer tokens are converted to the nearest f64, except that
  the integer token `-0` denotes `-0.0` (the sign is meaningful). `"NaN"`, `"inf"`,
  `"-inf"` (exactly these strings) are the non-finite values; `"NaN"` is the canonical
  quiet NaN `0x7FF8000000000000`, so all NaNs written through JSON are bitwise equal.
  Anything else — other strings (`"Infinity"`, `"nan"`, `"1.5"`), booleans — is
  `type_mismatch`.
- **i64.** A JSON number written without fraction or exponent (`2.0` and `1e3` are
  `type_mismatch`), or a decimal string matching `-?[0-9]+` (leading zeros allowed, no `+`,
  no whitespace). Outside `[-2^63, 2^63-1]` → `type_mismatch`. Output: a JSON integer when
  `|v| <= 2^53`, else a decimal string.
- **bool** takes JSON `true`/`false`; **string** takes a JSON string. Nothing else.
- **timestamp** columns take the §10.1 text form. Any value that is not a valid timestamp
  (unparsable, out of range, or not a string) is `type_mismatch` (step 3 of §4.1), not
  `invalid_timestamp` — that code is for `value_time`, `received_time`,
  `published_time` and read arguments.

### 10.3 Structure (`invalid_argument`)

A request is structurally valid when every field present has the right JSON shape:

- **No unknown fields** in a request (a step's fields other than `op` and `expect`), a
  write row, a column spec or an `asof_join` left row.
- **Catalog:** table, column and series names are strings; `columns` is a list of
  objects with exactly `name` (string) and `type` (one of §2.1's types); `default_clock`
  is one of the three clocks; `partition` is `day | month | year`; `tags` is an object of
  string values (or null); `timezone` is a string (not otherwise validated);
  `resolution` is null or a valid §1 duration.
- **Writes:** `rows` is a list of objects; each row's `series` is present and a string;
  `values` is absent, null or an object; `deleted` is absent or a bool (null is not
  allowed); `batch_id` is absent, null or a non-empty string.
- **Reads:** `clock` is absent, null or one of the three clocks; `series` is absent,
  null or a list of strings; `frontiers` (grid) is a required list; `left` (asof_join) is
  a required list of objects whose `series` is a string.

Scalars keep their own codes: a malformed timestamp is `invalid_timestamp`, a malformed
duration is `invalid_range`, a malformed value is `type_mismatch`, and a bad name that is
a string is `invalid_name`. A field given as JSON `null` is the same as omitting it,
except for `deleted`.

### 10.4 Check order

The first failing check decides the error.

- **Every operation:** structure (§10.3) of the whole request comes first.
- **Writes:** then the §4.4 batch-id no-op; then the writer clock (`invalid_timestamp`);
  then §4.1 steps 1–5 per row, rows in input order.
- **Reads:** then `unknown_table`; `unknown_series` (filter names in order, or for
  `asof_join` the left rows in order); `invalid_timestamp` — fields in the order
  `value_from`, `value_to`, `frontier`, `system`, `from`, `to`, then `frontiers` (grid),
  then each left row's `value_time` and `frontier` (asof_join); finally `invalid_range`
  (the value range, then `from`/`to` or the grid window).
- **`create_table`:** then table name syntax (`invalid_name`), table uniqueness
  (`already_exists`), then each column in order: name syntax or reserved name
  (`invalid_name`), uniqueness within the table (`already_exists`).
- **`add_column`:** then `unknown_table`, `invalid_name`, `already_exists`.
- **`create_series`:** then empty name (`invalid_name`), `unknown_table`,
  `already_exists`.

### 10.5 Grid windows

- A relative window needs both `window_start` and `window_end`; only one →
  `invalid_range`. A relative window together with `value_from`/`value_to` →
  `invalid_range`. Neither → absolute and unbounded.
- `window_start > window_end` → `invalid_range`; equal → an empty window.
- An empty `frontiers` list gives no rows.

### 10.6 Other rules

- Reserved names (§2.1) apply to columns only; a table may be called e.g. `values`.
- Series names are any non-empty string, unique across the database.
- Duplicate names in a read's series filter are harmless (the filter is a set).

## 11. Reproducibility receipt (informative)

A **receipt** records a read so it can be reproduced exactly later:

```
(table, op, request, clock, F, S)
```

`request` is the op's remaining arguments (series filter, value range, `from`/`to`,
frontiers and window, left rows); `clock` is the effective clock (after the table
default); `F` the frontier, if any; and `S` the system bound — when the read gave none,
the greatest system_time of the snapshot it ran against. By §7.3, replaying a receipt
against that snapshot or any later one returns the same rows, bit for bit (until an
explicit purge). A receipt therefore identifies a backtest's input data without copying
it.
