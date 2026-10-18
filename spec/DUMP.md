# Muninn dump format (v1)

A **dump** is a portable, text-only export of a whole database, written by
`muninn dump <dir>` and read by `muninn restore <dir> <new-db>`. It exists so that data
can always get out of Muninn (vision goal 6) and back in, independently of the segment
format (`FORMAT.md`), which may change between versions.

**Guarantee.** Restoring a dump gives a database in which every read of SPEC §6 — under
any clock, frontier, system bound, series filter and value range — returns exactly what
the original returned at the moment of the dump, *including* `system_time` and `seq`.
The catalog change log (SPEC §2.3), the committed batch ids (SPEC §4.4), the next `seq`
and `last` (SPEC §4.2) are restored too, so later writes behave exactly as they would
have on the original (same dedup, same batch-id no-ops, same `system_time` and `seq`).

Physical layout is not preserved (segment boundaries, compaction state, retired files):
SPEC §7.2 says it never changes a result.

## Layout

```
<dir>/
  manifest.json        what the dump contains, with checksums (written last)
  catalog.json         the catalog change log
  batch_ids.ndjson     committed batch ids
  tables/<table>.ndjson  every version of one table, in seq order
```

All files are UTF-8. Timestamps are in the canonical SPEC §1 text form (UTC `Z`, no
fraction or exactly 6 digits). JSON objects are written with a fixed key order (shown
below), so the data files (everything but the manifest) of two databases that answer
every read identically are byte-identical; readers MUST NOT depend on key order or
whitespace.

## `manifest.json`

```jsonc
{
  "format": "muninn-dump",          // fixed
  "version": 1,                      // this document
  "muninn": "0.1.0",                 // version of the writer (informational)
  "dumped_at": "2025-01-15T12:00:00.123456Z",  // writer wall clock (informational)
  "log_version": 42,                 // log version of the dumped snapshot (informational)
  "next_seq": 1234,                  // next unassigned seq (1 for an empty database)
  "last_system_time": "2025-01-15T11:59:00Z",  // SPEC §4.2 `last`, or null
  "versions": 1233,                  // total versions over all tables
  "catalog":   {"file": "catalog.json",     "changes": 7, "crc32c": "1a2b3c4d"},
  "batch_ids": {"file": "batch_ids.ndjson", "count": 3,   "crc32c": "00000000"},
  "tables": [
    {"name": "wind_fc", "file": "tables/wind_fc.ndjson", "versions": 1233, "crc32c": "..."}
  ]
}
```

`crc32c` is the CRC-32C (Castagnoli) of the file's bytes as 8 lower-case hex digits. A
reader MUST verify every checksum before creating anything and reject the dump on a
mismatch. The manifest is written after every other file, so a dump without one is
incomplete. A dump is taken from one pinned snapshot: concurrent writers do not make it
inconsistent.

## `catalog.json`

`{"changes": [ <change>, ... ]}`, oldest first — the SPEC §2.3 change log. Each change
carries the `system_time` it was committed at:

```jsonc
{"op": "create_table", "system_time": "...", "name": "wind_fc",
 "columns": [{"name": "mw", "type": "f64"}],          // columns as created
 "default_clock": "received", "partition": "day"}
{"op": "add_column", "system_time": "...", "table": "wind_fc", "name": "model", "type": "string"}
{"op": "create_series", "system_time": "...", "id": 1, "name": "dk1_wind", "table": "wind_fc",
 "tags": {"area": "DK1"}, "timezone": "UTC", "resolution": "1h"}   // resolution: duration or null
```

Restore replays the changes in order with their original times. Series ids are assigned
1, 2, 3, … in creation order (SPEC §2.2), so replaying in order reproduces them; a reader
MUST check that each created series gets the `id` recorded here.

## `batch_ids.ndjson`

One JSON string per line: every batch id committed in the database, sorted. (Batch ids
are not tied to versions: an id may belong to a batch that stored nothing.)

## `tables/<table>.ndjson`

One JSON object per line, one line per **version** (tombstones included), in strictly
increasing `seq` order:

```jsonc
{"seq": 4, "system_time": "2025-01-15T12:05:00Z", "series": "dk1_wind",
 "value_time": "2025-01-16T00:00:00Z", "published_time": "2025-01-15T12:00:00Z",  // or null
 "received_time": "2025-01-15T12:04:00Z", "deleted": false,
 "values": {"mw": 1388.0, "model": "ec"}}
```

- `values` holds the non-null values only; a missing column is null. Its keys are
  columns of the table as of the dump (a version written before `add_column` simply has
  no entry for the new column, exactly as it reads in SPEC §2.1).
- Values use the SPEC §10.2 JSON encodings: `f64` as a JSON number (shortest
  round-trip form; `-0.0` keeps its sign), `"NaN"` (the canonical quiet NaN
  `0x7FF8000000000000`), `"inf"`, `"-inf"`; `i64` as a JSON integer when `|v| <= 2^53`,
  else a decimal string; `bool`, `string` as JSON; `timestamp` in the §1 text form.
- **Extension:** an `f64` NaN with any other bit pattern (possible through the typed
  API, see `crates/SPEC-QUESTIONS.md` item 5) is written as the string
  `"nan:0x<16 hex digits>"` — its exact bits — so that bitwise identity (SPEC §4.3)
  survives the round trip.

Within a table, `seq` is strictly increasing and `system_time` is non-decreasing. Seqs
are unique across all tables and, merged, contiguous from 1 to `next_seq - 1` (a
database never skips a seq); merging the table files by `seq` gives the database's
global commit order. Since batch system_times are strictly increasing (SPEC §4.2),
versions with equal `system_time` are exactly the versions of one original batch, and
in seq order `system_time` only stays equal within a batch or increases.

## Restoring

1. Read the manifest, check `format`/`version`, verify every checksum.
2. Create the new, empty database and replay `catalog.json`.
3. Merge the table files by `seq` and import the versions, in that order, with their
   original `system_time` and `seq` (engine API `Engine::restore`, which bypasses
   assignment and deduplication but checks that the seqs continue the counter without
   gaps, that `system_time` never decreases and that each call's first version is
   strictly after the stored `last` — i.e. a call holds whole original batches — and
   that every version is well-typed). Each call commits one log entry of kind `restore`
   (`FORMAT.md` §3.1). The reference tool imports in chunks of about `--chunk-rows`
   versions (default 100 000), cut only where `system_time` changes so that no batch is
   split, and records the batch ids along with the chunks.
4. Check that the restored version counts, `next_seq` and `last_system_time` equal the
   manifest's.

A restore is not atomic as a whole: if it fails, delete the new database and run it
again. Any tool that follows the rules above may produce or consume dumps; the files are
plain enough for `jq`, pandas or DuckDB (`read_json_auto('tables/*.ndjson')`).
