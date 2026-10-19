# Muninn storage format (segments v1, log format 2)

This document specifies the bytes a Muninn database leaves on a store, precisely enough
for a third party to write an independent reader. It complements `SPEC.md`, which defines
what queries return; nothing here may change a query result. The reference
implementations are `crates/muninn-format` (segment files) and `crates/muninn-engine`
(database layout). Keywords MUST / SHOULD are used as in RFC 2119.

A database is a set of objects in a key/value object store (a directory on a filesystem,
or a bucket prefix). Keys are `/`-separated. There are two kinds of object:

- the **commit log** (`log/…`, JSON): the single source of truth;
- **segments** (`segments/…`, binary): immutable column files holding versions.

## 1. Conventions

- Integers in binary structures are little-endian unless they are varints.
- `uvarint`: unsigned LEB128, at most 10 bytes, value < 2^64. Writers MUST emit the
  canonical (shortest) form; readers MUST reject a trailing zero byte (non-canonical),
  more than 10 bytes, or overflow.
- `ivarint`: a signed 64-bit integer zigzag-mapped (`(v << 1) ^ (v >> 63)`) and written
  as a `uvarint`.
- `string`: `uvarint` byte length, then that many bytes of UTF-8 (readers MUST reject
  invalid UTF-8).
- Timestamps are signed 64-bit microseconds since the Unix epoch, UTC (SPEC §1). Every
  timestamp in a valid file lies in `[-62135596800000000, 253402300799999999]`
  (`0001-01-01T00:00:00Z` … `9999-12-31T23:59:59.999999Z`); readers MUST reject others.
- CRC-32C is the Castagnoli CRC (polynomial `0x1EDC6F41`, reflected, init and final XOR
  `0xFFFFFFFF`), as used by iSCSI/ext4. `crc32c("123456789") = 0xE3069283`.
- Arithmetic in delta encodings is wrapping 64-bit two's complement.

## 2. Partitions

Every table has a partitioning granularity (`day`, `month` or `year`, SPEC §2.1). A
version belongs to the partition of its `value_time` in UTC:

| granularity | ordinal (`index`) | label |
| --- | --- | --- |
| `day` | days since 1970-01-01 (floor division) | `YYYY-MM-DD` |
| `month` | `year * 12 + month - 1` | `YYYY-MM` |
| `year` | `year` | `YYYY` |

Years are written with exactly four digits (`0001`–`9999`). A partition's value range is
the half-open interval from its first instant to the next partition's first instant.

## 3. Database layout

```
log/<v>.json                  commit entry for version v (v ≥ 1, 20 decimal digits, zero-padded)
log/<v>.checkpoint.json       full state as of version v (optional)
log/_last_checkpoint          {"version": v} — hint only
segments/<table>/<partition label>/<name>.seg
```

Version 0 is the empty database. **The state at version v is the fold of entries
`1..=v`.** Entries are created with an atomic create-if-absent; a writer only writes `v+1`
after reading or writing `v`, so the versions ever written form a gapless prefix `1..=N`.
A reader determines the current version by reading `N+1, N+2, …` until one is absent. A
segment file that no entry (still) references is garbage: it MUST be ignored.

Garbage collection may **truncate** the log (§3.6): entry 1 is then replaced by a marker
and the entries `2..T-1` are deleted, where a valid checkpoint at version `T` or later
summarises them. The entries present are then `1` (the marker) and the gapless range
`T..=N`. A reader that finds entry `N+1` absent MUST then also find entry `N` present
(and, for `N = 1`, not the marker) before concluding that `N` is the head; otherwise its
state is older than the truncation and it MUST reload from a checkpoint (§3.6).

Segment names written by the reference engine are
`<creation wall time µs, 20 digits>-<64-bit nonce, 16 lowercase hex>.seg`. Readers MUST
NOT depend on the name; it is used only by the garbage collector.

### 3.1 Commit entries

`log/<v>.json` is one JSON object. Unknown fields MUST be rejected (format evolution
bumps `format`).

| field | type | meaning |
| --- | --- | --- |
| `format` | int | `1` or `2` (written by engines that may truncate the log, §3.6; the fields are the same) |
| `version` | int | `v` (must equal the key's number) |
| `kind` | string | `catalog`, `write`, `restore`, `compact`, `gc` (constrains `add`/`remove`, below), or `truncated` for the marker of §3.6, which is not an applicable entry. Readers MUST reject other kinds |
| `wall_time` | int | writer wall clock, µs (used by GC only) |
| `catalog` | array, optional | catalog changes, applied in order (§3.2) |
| `batch_id` | string, optional | batch id recorded by this write (SPEC §4.4) |
| `add` | array, optional | segments added (§3.3) |
| `remove` | array of string, optional | keys of segments removed (compaction) |
| `purged` | array of string, optional | removed segment keys whose files were deleted |
| `next_seq` | int | the next unassigned `seq` after this entry (starts at 1) |
| `last_system_time` | int or null | greatest `system_time` of any stored version (SPEC §4.2) |

Applying an entry to state `S` (which MUST have version `v - 1`): apply the catalog
changes; record the batch id (it MUST not be recorded already); set the high-water marks
(they MUST NOT decrease); for each key in `remove`, drop that live segment (it MUST be
live) and remember it as *retired* with `wall_time`; add each segment of `add` (its key
MUST be new); forget retired keys listed in `purged`. The `kind` constrains `add` and
`remove`:

- `write`: nothing removed; every added segment's seq range lies in
  `[S.next_seq, next_seq)` and the added row counts sum to `next_seq - S.next_seq` (each
  new seq is stored exactly once, so no two segments of the entry share a seq). If rows
  are added, they all carry one `system_time`, which equals `last_system_time` and is
  strictly greater than `S.last_system_time` (SPEC §4.2); with no rows,
  `last_system_time` is unchanged;
- `restore` (dump/restore, `spec/DUMP.md`; it imports one or more *whole* original
  batches with their original `system_time` and `seq`): the same seq rules as `write`
  (contiguous, each new seq exactly once — no gaps) and at least one row; every added
  segment's `system_time` range lies in `(S.last_system_time, last_system_time]` and the
  greatest of them equals `last_system_time`. Rows of one entry may have several
  `system_time`s (one per original batch);
- `compact`: exactly one segment added, at least two removed, all of the added segment's
  table and partition; the row counts are equal, the seq range of the added segment is the
  hull of the removed ones, and the high-water marks are unchanged;
- `catalog`, `gc`: nothing added or removed.

A reader that finds an entry violating these rules MUST treat the database as corrupt.

The **live segments** of a state are the data. The versions of the database are exactly
the rows of its live segments; a version is never in two live segments (compaction
removes the sources in the same entry that adds the merged segment).

### 3.2 Catalog changes

Objects tagged by `op`:

```jsonc
{"op": "create_table", "system_time": 1735689600000000, "name": "prices",
 "columns": [{"name": "value", "type": "f64"}], "default_clock": "received", "partition": "day"}
{"op": "add_column", "system_time": 1735689600000000, "table": "prices",
 "column": {"name": "volume", "type": "f64"}}
{"op": "create_series", "system_time": 1735689600000000, "id": 1, "name": "DK1",
 "table": "prices", "tags": {"area": "DK1"}, "timezone": "UTC", "resolution": 3600000000}
```

`resolution` is µs or `null`. Series ids MUST be `1, 2, 3, …` in creation order. The
**column id** of a table column is its 0-based position in the table's column list
(columns are only appended, SPEC §2.1); segments refer to columns by id.

### 3.3 Segment records

Each element of `add` (and of a checkpoint's `segments`) describes a segment so readers
can prune without opening it:

```jsonc
{"key": "segments/prices/2025-01-16/00001760000000000000-0123456789abcdef.seg",
 "table": "prices", "partition": "2025-01-16", "size": 4096, "rows": 96,
 "value_time": [min, max], "received_time": [min, max], "published_time": [min, max],
 "system_time": [min, max], "seq": [min, max], "series": [min_id, max_id], "created": 0}
```

`published_time` bounds are of the *effective* published time
`coalesce(published_time, received_time)`. The key MUST start with
`segments/<table>/<partition>/`. These values MUST equal the segment footer's (§5);
a reader SHOULD verify that when it opens the footer.

### 3.4 Checkpoints and the hint

`log/<v>.checkpoint.json` holds the state after entry `v`:

```jsonc
{"format": 2, "version": v,                   // 1 or 2, as for entries
 "catalog": [<catalog change>, ...],   // the whole catalog change log, in order
 "batch_ids": ["..."], "segments": [<segment record>, ...],
 "retired": [["<key>", <removal wall time>], ...],
 "next_seq": n, "last_system_time": t,
 "entry_crc32c": c,                    // CRC-32C of the bytes of log/<v>.json (0 for v = 0)
 "checksum": k}                        // CRC-32C of this JSON serialised with "checksum": 0
```

The checksum is computed over the compact JSON serialisation with fields in the order
above and no whitespace (what the reference writer emits); readers recompute it the same
way.

Loading checkpoint `v` and then applying entries `v+1, …` MUST give the same state as
applying `1, …` from scratch. **The log is authoritative; a checkpoint is a cache.** A
reader MUST skip a checkpoint that is unparsable, fails its checksum, whose `version`
differs from its key, whose `entry_crc32c` does not match `log/<v>.json` (or that entry is
missing), or after which the later entries do not apply — and then try the next older
checkpoint, finally replaying from version 0. (A checkpoint that is internally
consistent, sealed and bound to the right entry but still wrong is not detectable without
a full replay; writers never produce one.) `log/_last_checkpoint` (`{"version": v}`)
names a recent checkpoint. It is only a hint: readers MUST tolerate it being absent,
stale, unparsable or naming a missing or invalid checkpoint.

When to write a checkpoint is the writer's choice and never changes a state. The
reference engine writes one when at least `checkpoint_interval` entries (default 100)
*and* at least `checkpoint_size_factor` (default 1) times the previous checkpoint's
bytes of entries were committed since it — so checkpoint bytes stay proportional to
entry bytes rather than to entries × state size — and GC writes one of the head (§3.6).

### 3.5 Garbage collection and the commit window

Segment files not referenced by the current state are deleted by GC. The rules that make
this safe with several writers and lagging readers:

1. GC decides against the **log head**: it first reads every entry after its own state,
   and re-reads the head immediately before deleting, sparing anything an entry committed
   meanwhile references.
2. GC deletes a file only after it has itself observed the file unreferenced for at
   least the grace period `G`, measured on its own clock. No writer clock — neither the
   creation time in segment names nor `wall_time` in entries — is trusted for ageing.
3. A writer MUST NOT commit an entry referencing segments it wrote more than `G / 2`
   ago (by its own elapsed-time clock); it abandons the write instead (the files become
   orphans).

Hence a segment is committed within `G / 2` of being written, while GC can delete it at
the earliest `G` after first seeing it — which is after it was written. Assumptions: the
store call that commits the entry completes within `G / 2`, and the writer's and GC's
clocks advance at comparable rates (no synchronisation is needed). A reader in another
process that holds a state no longer at the head can read its segments for at least `G`
after GC first sees them retired; after that it gets a "missing segment" error, on
which an engine reloads the head and retries once. `G = 0` disables rules 2–3 (single
process tests only).

### 3.6 Log truncation (log format 2)

Checkpoints make old entries redundant; GC may remove them so that the log does not grow
with every commit forever. The rules (decision 0007), with `G` the grace period of §3.5:

1. GC truncates only below the **second-newest** checkpoint `T` (by version, at most the
   head), and only once the **newest** checkpoint verifies (checksum, version, bound to its
   entry). The newest one keeps the head cheap to load; `T` with the entries `T+1..` is the
   fallback if the newest is damaged.
2. Removable are the entries `< T` and the checkpoints `< T`. GC removes each only after
   it has itself observed it removable for at least `G` (its own clock).
3. Entries are removed in **ascending** order, and only after every lower entry is gone:
   entry 1 is not deleted but overwritten with the **truncation marker**
   `{"format": 2, "version": 1, "kind": "truncated", "wall_time": t, "next_seq": 1,
   "last_system_time": null}`, entries `2, 3, …` are then deleted. Entry 1 therefore
   always exists once written: nobody can create it again, and a reader that replays
   from the start fails on the marker instead of seeing an empty database.
4. **Readers** detect a truncation with rule 3: having found entry `v+1` absent, they read
   entry `v`; if it is absent (or `v = 1` and it is the marker), the entries after their
   state were removed and they reload from a checkpoint. If `v` is present, `v+1` cannot
   have been deleted yet (ascending order) — it did not exist when they looked.
5. **Writers** commit entry `v+1` only after a recent proof that `v` is the head: their own
   successful commit of `v`, or a read that found `v+1` absent and then `v` present (rule
   4), younger than `G / 2` by their own clock. Otherwise they repeat that read first, and
   treat a present `v+1` or a removed `v` as a conflict. Since GC deletes `v+1` no earlier
   than `G` after it first saw it, which is after `v+1` was created, a lagging writer can
   never re-create a removed entry (assuming, as in §3.5, that a store call completes
   within `G / 2`). `G = 0` disables the timing (single process tests only); writers then
   check the head before every commit.

A reader that only knows format 1 does not know rules 3–5; a log that was ever truncated
contains format-2 entries and checkpoints, which such a reader rejects (§7).

## 4. Segment file

A segment holds versions of one table and one partition. Rows are sorted by
`(series, value_time, received_time, system_time, seq)`; since `seq` is unique the order
is strict.

```
offset 0            header (16 bytes)
offset 16           chunk data (referenced by the footer; any order, may have gaps)
footer_offset       footer (footer_len bytes)
file_size - 16      tail (16 bytes)
```

**Header**: `"MUNINNSG"` (8 bytes ASCII), `u16` format version `1`, `u16` flags `0`,
`u32` reserved `0`. A reader that reads the header MUST reject other versions or non-zero
flags (a tail-only reader relies on the footer format byte, §5.1).

**Tail**: `u32 footer_len`, `u32 footer_crc` (CRC-32C of the footer bytes), `"MUNINNSG"`.
`footer_offset = file_size - 16 - footer_len`, and MUST be ≥ 16.

To open a segment with one ranged read, read the last `k` bytes (the reference reader
uses 64 KiB, or the whole file if smaller). If `k < footer_len + 16`, read the last
`footer_len + 16` bytes. Verify the trailing magic and the footer CRC before decoding.

### 4.1 Rows, blocks and chunks

Rows are grouped into **blocks** of consecutive rows (1 … 1 048 576 rows each; the
reference writer uses 16 384). Each block stores one **chunk** per column:

| chunk | column | type | nullable | encodings |
| --- | --- | --- | --- | --- |
| 0 | `series` | u32 | no | integer |
| 1 | `value_time` | timestamp | no | integer |
| 2 | `published_time` | timestamp | **yes** | integer |
| 3 | `received_time` | timestamp | no | integer |
| 4 | `system_time` | timestamp | no | integer |
| 5 | `seq` | u64 (as i64 bits) | no | integer |
| 6 | `deleted` | bool | no | bitmap (4) |
| 7 + j | value column `j` of the footer's column list | per type | **yes** | per type |

Value chunk encodings by type: `f64` → 5, `i64` → integer, `bool` → 4, `string` → 6,
`timestamp` → integer. "Integer" means any of encodings 1, 2, 3, 7. Readers MUST reject
any other combination.

A chunk is stored as `stored = compress(raw)`. Its directory entry (§5.5) gives offset,
stored length, raw length, encoding, compression and the CRC-32C of the **stored** bytes,
which readers MUST verify before decompressing. Compression `0` = none (`raw_length`
MUST equal `length`), `1` = one Zstandard frame whose content is exactly `raw_length`
bytes.

A **non-nullable** raw chunk is exactly the encoding of the block's `n` values. A
**nullable** raw chunk starts with a mode byte:

- `0`: no nulls; the encoding of `n` values follows;
- `1`: all null; nothing follows;
- `2`: a validity bitmap of `n` bits (bit = 1: present) follows, then the encoding of the
  `k` present values only, in row order. A mode-2 bitmap MUST have `0 < k < n`.

Every raw chunk MUST be consumed exactly.

### 4.2 Encodings

| code | name | layout for `m` values |
| --- | --- | --- |
| 1 | run-length | pairs `(uvarint value, uvarint run ≥ 1)` until `m` values; runs MUST NOT overshoot |
| 2 | delta | `ivarint(v0)`, then `ivarint(v[i] - v[i-1])` |
| 3 | delta-of-delta | `ivarint(v0)`, `ivarint(v1 - v0)`, then `ivarint(d[i] - d[i-1])` with `d[i] = v[i] - v[i-1]` |
| 4 | bitmap | `ceil(m / 8)` bytes, value `i` in bit `i % 8` (LSB first) of byte `i / 8`; padding bits MUST be zero |
| 5 | plain f64 | 8 bytes per value, IEEE-754 binary64 little-endian (bit patterns are preserved, NaN payloads included) |
| 6 | length-prefixed | per value a `string` (§1) |
| 7 | zigzag | `ivarint(v)` per value |

Run-length values are read as `uvarint` and reinterpreted as `i64`. For the `series`
column values MUST fit in u32. The reference writer uses: series 1, value_time 3,
published/received/system time and seq 2, i64 values 7, timestamp values 2.

### 4.3 Values

`f64` values are compared and deduplicated bitwise (SPEC §4.3); a reader MUST return the
exact bit pattern. `bool` values present in a nullable chunk are the bits of an encoding-4
bitmap of `k` bits. A row with `deleted = 1` MUST have every value column null.

A table column whose id is not in the footer's column list (the segment predates
`add_column`) reads as null in every row.

## 5. Footer

The footer is a sequence of fields; every count MUST be plausible for the bytes that
remain, and the footer MUST be consumed exactly.

### 5.1 Header fields

```
u8      footer format = 1
string  table name
u8      partition granularity: 0 = day, 1 = month, 2 = year
ivarint partition ordinal (§2)
uvarint row_count (≥ 1)
uvarint column_count
column_count × { uvarint column id; u8 type: 0 f64, 1 i64, 2 bool, 3 string, 4 timestamp }
```

Column ids MUST be strictly increasing.

### 5.2 Segment statistics

```
4 × { ivarint min; ivarint max }   value_time, received_time, effective published_time, system_time
uvarint published_null_count        rows with null published_time
uvarint seq_min
uvarint seq_max
uvarint deleted_count
```

All bounds are exact (the min and max over all rows) and `min ≤ max`; value_time bounds
lie in the partition; `seq_max - seq_min ≥ row_count - 1`.

### 5.3 Series directory

```
uvarint series_count (≥ 1)
series_count × { uvarint id delta; uvarint row count (≥ 1) }
```

The first delta is the first id; later deltas are `id - previous id` (≥ 1). Series `i`
occupies rows `[start_i, start_i + count_i)` with `start_0 = 0` and `start_{i+1} =
start_i + count_i`; the counts MUST sum to `row_count`. Every row's `series` value MUST
equal the directory entry covering it. A reader that needs one series reads only the
blocks overlapping its row range.

### 5.4 Column statistics

```
column_count × { uvarint null_count; u8 has_min_max; if 1: 8 bytes min, 8 bytes max }
```

`min`/`max` are present only for `f64` (binary64 bits; over non-NaN values; neither may
be NaN), `i64` (two's complement) and `timestamp` columns, and only if the column has a
non-null (non-NaN) value. They are bounds; readers may use them only to skip data.

### 5.5 Blocks

```
uvarint block_count (≥ 1)
block_count × {
  uvarint row_count (1 … 1 048 576)
  4 × { ivarint min; ivarint max }   value_time, received_time, effective published_time, system_time
  (7 + column_count) × {
    uvarint offset; uvarint length; uvarint raw_length; u8 encoding; u8 compression; u32 crc32c
  }
}
```

Blocks tile the rows in order (row counts sum to `row_count`). Block bounds MUST lie
within the segment bounds and MUST contain every row of the block. Chunks MUST lie in
`[16, footer_offset)`. `raw_length` MUST NOT exceed what the encoding can need for the
block's `n` rows, so a reader never inflates more than that: `ceil(n/8)` for `deleted`;
`20n` for the other non-nullable engine chunks; for nullable chunks `1 + ceil(n/8)` plus
`20n` (integer types), `8n` (`f64`) or `ceil(n/8)` (`bool`); and 2^28 for `string`
chunks.

## 6. Reader obligations

A conforming reader MUST, before returning any row from a block, verify the CRCs of the
chunks it decoded, the series directory, the strict sort order and the block statistics,
and MUST report corruption (not return partial or guessed data) on any violation listed
in this document. Statistics (§3.3, §5.2–5.5) may be used to skip segments and blocks:
skipping is correct exactly when no row of the skipped unit can satisfy the query's
predicates (value range, series filter, `t_clock` bounds, `system_time ≤ S`).

## 7. Versioning

The header version, footer format byte and log `format` field are independent integers.
This document specifies segment header version `1`, footer format `1` and log formats `1`
and `2` (§3.6; format 2 entries and checkpoints have the same fields, and a format-2
writer may truncate the log). Readers of this document MUST accept log formats 1 and 2
and MUST reject versions they do not know.
