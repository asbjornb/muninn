# Spec questions (from the Rust side)

Items still open after SPEC.md §10 (the earlier list was resolved by §8 `invalid_argument`
and §10). For each one, `muninn-core` does what the Python reference
(`spec/reference`) does, so both agree on the corpus.

1. **Catalog change log timestamps (§2.3)** — also open in `spec/AMBIGUITIES.md`. Catalog
   steps carry no clock. `ModelDb` stamps a change with the optional clock reading of
   the request, or else the latest time it knows, and never lets that time go backwards.
   Catalog times never feed into `last` (§4.2). Not observable in v0.1.
2. **f64 overflow from JSON numbers (§10.2).** "Any JSON number" doesn't say what
   happens when a number exceeds the f64 range. Implemented as in the reference
   (Python `json`): an *integer* token too large for f64 (e.g. 400 digits) is
   `type_mismatch`; a *float* token such as `1e400` reads as `+inf` and is accepted.
   This should be pinned down (rejecting both would be simpler).
3. **Duplicate keys in a JSON object** aren't covered by the spec. `wire::Wire::parse`
   keeps the last value (like Python), so `{"value": 1, "value": 2}` writes 2. Rejecting
   duplicates as `invalid_argument` would be stricter.
4. **A non-string `table` in a read** (e.g. `"table": 5`). §10.3 lists names as strings
   only for catalog requests. Implemented as in the reference: `unknown_table`.
5. **NaN payloads from non-JSON sources.** Every NaN written through JSON is the
   canonical `0x7FF8000000000000` (§10.2). The typed Rust API, and later binary ingest,
   can carry other NaN payloads. Because dedup and comparison are bitwise (§4.3), those
   are different values, and scenarios have no way to express them.
