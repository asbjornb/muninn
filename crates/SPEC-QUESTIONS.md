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

## From the storage engine

6. **Ambiguous storage failures vs §8 "a failed operation has no effect".** On an object
   store a commit whose request fails (timeout, crash) may still have happened. The
   engine reports `io` and, on its next operation, reloads from the log; the batch is
   then either fully there or absent (§7.6 holds), but the `io` result did not mean "no
   effect". Suggest §8 says the no-effect rule covers the §8 codes only, and that an
   engine-only `io` error leaves the outcome unknown (resend with a `batch_id`, or rely
   on dedup).
7. **Concurrent writers** are not in the spec. The engine detects a lost race
   (`put_if_absent` of the next log version fails) and returns an engine-only
   `conflict` error with no effect; the retry after the automatic reload succeeds.
8. **Snapshot lifetime (§7.3) vs garbage collection.** "A read against snapshot X returns
   the same result forever" holds for a snapshot pinned in the process (GC never deletes
   segments a live snapshot references), and the data of any old snapshot stays
   reachable from the *current* state through the `system` bound. But compaction retires
   segment files and GC deletes them after a grace period, so re-opening an old *log
   version* (time travel by version, or a reader in another process holding an old
   snapshot longer than the grace period) can fail with `io`. Suggest §7.3 defines
   reproducibility by `system` bound (always available) rather than by physical
   snapshot, or that retention of retired segments becomes an explicit setting.
9. **Catalog times on reopen.** Catalog changes are log entries carrying their
   `system_time`, so §2.3 history survives restarts and equals the model's; with no
   clock reading the engine uses the same "latest known time" rule as the model
   (item 1).
