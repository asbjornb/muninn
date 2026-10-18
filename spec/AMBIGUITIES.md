# Spec ambiguities (v0.1)

Places where `SPEC.md` leaves a real choice open. For each, the reference implementation
(`spec/reference/`) implements the most conservative reading given here. These are
*decisions waiting to be ratified*: either fold them into `SPEC.md` or change them — in
both cases update the reference and the scenarios together.

Clear gaps with only one sensible answer were fixed directly in `SPEC.md` instead (asof_join
exact-match range, `values: null` when not found, exact relative-window arithmetic,
expectation value encodings).

The generated corpus (`spec/scenarios/generated/`) never depends on A7/A8 (it never
produces a step that fails for two different reasons), so only the hand-written scenarios
named below pin these choices.

## A1. Timestamp text grammar (§1)

Accepted: `YYYY-MM-DDTHH:MM:SS[.f{1,6}](Z|+HH:MM|-HH:MM)`.

- ASCII digits only; upper-case `T` and `Z` only (RFC 3339 also allows lower case — rejected).
- Exactly 4-digit year, 2-digit fields. No space separator, no `+HHMM`, no `+HH`.
- 1–6 fraction digits; a bare `.` or 7+ digits is rejected (no truncation/rounding).
- Seconds `00..59`: leap seconds (`:60`) are rejected — µs-since-epoch has no leap seconds.
- Offset hours `00..23`, minutes `00..59`. `-00:00` is accepted and means UTC.
- The date must exist (proleptic Gregorian: `2024-02-29` ok, `2025-02-29` rejected).
- The §1 range check applies to the **UTC** result: `9999-12-31T23:30:00-01:00` is out of
  range, `0000-12-31T23:30:00-01:00` (= `0001-01-01T00:30:00Z`) is valid.

All failures are `invalid_timestamp`. Pinned by `044-timestamp-text-edge-cases`.

## A2. f64 JSON encodings (§4.1)

- `"NaN"` denotes the canonical quiet NaN (bits `0x7FF8000000000000`); JSON cannot carry
  other NaN payloads, so all `"NaN"` writes are bitwise equal and deduplicate.
- Only exactly `"NaN"`, `"inf"`, `"-inf"` are accepted as strings (`"Infinity"`, `"nan"`,
  `"1.5"` → `type_mismatch`). Bare `NaN`/`Infinity` tokens are not JSON and are rejected
  by the scenario loader.
- JSON `true`/`false` are not numbers → `type_mismatch`.
- The integer token `-0` is "an integer, converted" and so is `+0.0`, whereas `-0.0` is
  negative zero. JSON parsers disagree on this (serde_json yields `-0.0` for `-0`), so the
  corpus never contains the integer token `-0` in an f64 position; write `-0.0`.
- An integer literal too large for f64 (e.g. 400 digits) is `type_mismatch` (never
  generated).

## A3. i64 JSON encodings (§4.1)

- JSON numbers are accepted only when written without fraction or exponent: `2.0` and
  `1e3` are `type_mismatch` even though integral ("`1.5` → `type_mismatch`" generalised;
  parsers do not agree on whether `2.0` is an integer).
- Decimal strings match `-?[0-9]+` (leading zeros allowed, no `+`, no whitespace).
- Out of `[-2^63, 2^63-1]` (number or string) → `type_mismatch`.
- Output: JSON integer when `|v| <= 2^53`, else a decimal string.

## A4. Invalid value in a `timestamp` column (§4.1 step 3 vs step 5)

A `timestamp` column value that is not a valid §1 timestamp (unparsable *or* out of
range, or not a string at all) is `type_mismatch` — step 3, "timestamp accepts the text
form". `invalid_timestamp` (step 5) is reserved for `value_time` / `received_time` /
`published_time`. Pinned by `030-timestamp-columns`.

## A5. Missing required timestamps

A write row without `value_time` or `received_time` (or with `null`) fails with
`invalid_timestamp` at step 5 (a missing value is not a valid timestamp). Same for an
`asof_join` left row without `value_time` or `frontier`. Pinned by `026-validation-order-within-row`.

## A6. Grid window details (§6.4)

- Both `window_start` and `window_end` are required for a relative window; only one →
  `invalid_range`.
- Giving a relative window **and** `value_from`/`value_to` → `invalid_range`.
- Neither → absolute, unbounded.
- An unparsable duration → `invalid_range` (§8 "bad window"; no better code exists).
- A duration's value in µs must fit in i64, else `invalid_range`.
- `window_start > window_end` → `invalid_range`; equal → empty window.
- An empty `frontiers` list → empty result (not an error).

## A7. Order of argument checks in reads

When a read has several problems, the reported error is the first of:

1. `unknown_table`;
2. `unknown_series` (filter names in order; for `asof_join`, left rows in order);
3. `invalid_timestamp` — fields in this order: `value_from`, `value_to`, `frontier`,
   `system`, `from`, `to`, then `frontiers` (grid), then left rows' `value_time`,
   `frontier` (asof_join);
4. `invalid_range` — value range, then `from`/`to` (replay) or the window (grid).

This mirrors §4.1 (names before types before timestamps). Pinned by `045-read-errors`.

## A8. Order of checks in catalog operations

- `create_table`: table name syntax (`invalid_name`), table uniqueness (`already_exists`),
  then each column in order: name syntax/reserved (`invalid_name`), uniqueness
  (`already_exists`).
- `add_column`: `unknown_table`, `invalid_name`, `already_exists`.
- `create_series`: name non-empty (`invalid_name`), `unknown_table`, `already_exists`.

Table names are *not* checked against the reserved column names (§2.1 reserves column
names only), so a table may be called `values`. Pinned by `028-catalog-errors`.

## A9. Inputs with no error code

§8 has no code for: an unknown column type, an unknown clock (`default_clock` or a read's
`clock`), an unknown `partition`, an empty or non-string `batch_id`, `deleted` that is not
a bool, `values` that is not an object. The reference treats these as malformed input
(a caller bug, raised as `MalformedInput`, not a spec outcome) and no scenario exercises
them. Suggest adding `invalid_argument` to §8 for these.

`timezone` and `resolution` on series are metadata only and are not validated.

## A10. Catalog change log timestamps (§2.3)

Catalog steps carry no clock in the scenario format. The reference stamps each change
with `max(c, greatest system_time seen so far in versions or the catalog log)` (or none
before anything has a time). Catalog changes do **not** advance `last` in §4.2, which is
defined over stored versions. Not observable through any v0.1 read.

## A11. Odd but legal writes

- An empty `rows` list is a valid batch: `stored 0, duplicates 0`, records its batch id,
  does not change `last`.
- A rejected batch's `batch_id` is **not** recorded ("a failed operation has no effect");
  retrying with the same id after fixing the rows commits. Pinned by `022-batch-id-not-recorded-on-failure`.
- Duplicate names in a read's series filter are harmless (the filter is a set).
