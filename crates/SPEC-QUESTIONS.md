# Spec questions (from the Rust foundation)

Places where `spec/SPEC.md` v0.1 is ambiguous or silent. In each case the Rust model and
conformance runner implement the reading given here (the most conservative one we could
find); the spec itself is unchanged. Each item should be either confirmed in the spec or
pinned by a scenario.

## Writes

1. **Integral floats in `i64` columns.** §4.1.3 says `i64` accepts "integral numbers" and
   that `1.5` is a mismatch, but not whether the JSON literal `2.0` (or `1e3`) counts.
   *Implemented:* only integer literals (and decimal strings) are accepted; any float
   literal is `type_mismatch`. This matches what a Python reader gets from `json.loads`
   (`int` vs `float`).
2. **JSON integers beyond the i64 range** (e.g. `18446744073709551615`) are treated as
   floats: accepted (rounded) by `f64` columns, `type_mismatch` for `i64` columns.
3. **Bad timestamp *values* in a `timestamp` column.** §4.1.3 ("accepts the text form")
   vs §8 (`invalid_timestamp`: "out of range / unparsable timestamp"). *Implemented:*
   `type_mismatch` (step 3); `invalid_timestamp` is reserved for `value_time`,
   `received_time`, `published_time` (step 5) and for timestamps in requests.
4. **Empty `batch_id`.** §4.4 says "non-empty string" but names no error. *Implemented:*
   `invalid_name`, checked before the already-committed lookup.
5. **Empty batch** (`rows: []`) is valid: stores nothing, records its batch id.
6. **Decimal strings for `i64`** accept exactly `-?[0-9]+` (no `+`, whitespace, `_`).
   **Non-finite `f64` strings** are exactly `"NaN"`, `"inf"`, `"-inf"`; `"NaN"` is the
   canonical quiet NaN `0x7ff8000000000000`. Because dedup and comparison are bitwise, a
   NaN with another payload (possible from binary ingest later) is a different value, and
   scenario expectations can only express the canonical one.

## Catalog

7. **Check order for catalog ops** is unspecified. *Implemented:* (1) referenced table
   exists (`unknown_table`), (2) every name is valid (`invalid_name`), (3) uniqueness
   (`already_exists`). For `create_table` all column names are validated before the
   table-exists and duplicate-column checks; a duplicate column within one
   `create_table` is `already_exists`.
8. **Catalog `system_time`** (§2.3): catalog steps carry no clock reading in scenarios.
   *Implemented:* the API takes an optional reading; without one the model uses the latest
   time it knows, kept non-decreasing across catalog changes. Catalog times never feed
   into `last` of §4.2 (only stored versions do). Unobservable in v0.1.
9. **"change tags"** is listed in §2.3 but has no operation or scenario step; not
   implemented.
10. **`timezone`** is not validated against the IANA database (metadata only). A
    malformed `resolution` duration is reported as `invalid_range` (the only
    duration-related code).
11. **Table names may be reserved words** (`seq`, `values`, ...): §2.1 only reserves
    *column* names.

## Reads

12. **Error precedence in reads.** *Implemented:* `unknown_table`, then `unknown_series`,
    then `invalid_range`. Unparsable timestamps in a read request are rejected while the
    request is built (by the runner, before the database sees it), so in a scenario they
    take precedence over everything else.
13. **Grid windows.** Neither window form given = unbounded absolute range. Both a
    relative (`window_start`/`window_end`) and an absolute (`value_from`/`value_to`)
    window, or only one of `window_start`/`window_end`, is `invalid_range` ("bad
    window"). `window_start > window_end` is `invalid_range`, checked even when the
    frontier list is empty. A relative window may extend past the valid timestamp range;
    that is not an error (the bounds are normalised without changing the result).
14. **`asof_join` lookup.** §6.5 writes the right side as `latest(..., [value_time,
    value_time], ...)`; with half-open ranges that would be empty. *Implemented:* exact
    `value_time` match.
15. **`asof_join` not-found rows** in the full `rows` form: `found: false` and
    `published_time`, `received_time`, `system_time`, `seq` **and `values`** are `null`
    (not an object of null columns). Errors for left rows are reported for the first
    offending row in input order.
16. **Repeated names in a series filter** are allowed and behave like a set.
17. **Output fields per op** follow §6/§9 literally: `deleted` only in `history` and
    `replay` rows; `grid` rows are `latest` rows prefixed with `frontier`. The runner
    rejects full-form expected rows with any other field set.

## Timestamps

18. **Text form edge cases.** Accepted: numeric offsets including `-00:00`; a local
    year `0000` whose UTC instant is in range (e.g. `0000-12-31T23:30:00-01:00`).
    Rejected (`invalid_timestamp`): lowercase `t`/`z`, a space instead of `T`, a `.`
    with no digits, more than 6 fractional digits (not truncated), leap second `:60`,
    missing offset, offsets beyond `±23:59`.
19. **Durations**: no `+` sign, no fractional or compound forms (`1h30m`); leading
    zeros are allowed; `-0h` is zero.

## Scenario format

20. The runner rejects unknown step keys (besides `comment`/`description`/`note`),
    unknown ops, unknown error codes, and `expect` objects with both `rows` and
    `values`. Write expectations may list a subset of `stored` / `duplicates` /
    `batch_already_committed`.
