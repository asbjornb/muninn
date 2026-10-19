# 0008: Read path — knowledge-time compaction, series-aligned blocks, nibble encoding

- Status: accepted
- Date: 2026-10-19

## Context
The benchmark (bench/results/README.md, problem 3) found reads decode-bound (33–42% of
samples in varint decoding) and replay and the lead-time slice 18–110x slower than DuckDB.
Segments and blocks already carried `value_time`/received/published/`system_time` bounds,
in the log and in footers, and the scan already pruned by them. The bounds were useless:
compaction merged every segment of a partition into one, so a segment spanned every
knowledge time of its partition, and rows are sorted by `(series, value_time, …)`, so a
block spans them too. A one-series read decoded whole 16k-row blocks shared by all series.
Decoding cost ~58 ns per row, of which a third was allocation churn and the rest was
varint parsing and per-row checks.

## Decision
- **Compaction by knowledge-time period.** Compaction merges only segments of a partition
  whose greatest `t_clock` (table's default clock) falls in the same period of the table's
  partition granularity (`EngineOptions::compact_by_period`, default on). A forecast
  value day keeps about one segment per arrival day, so replay windows, frontiers, system
  bounds and lead filters skip whole segments from the log's statistics alone. Not a
  format change: a policy of the writer.
- **Series-aligned blocks.** The writer also ends a block where a new series starts once it
  holds 1 024 rows (`WriteOptions::series_block_rows`); a one-series read then decodes
  (almost) only that series. Block boundaries were always the writer's choice.
- **Streaming reductions.** `latest` and `grid` reduce each run of one point's versions
  from one segment to its possible winners and resolve points partition by partition (a
  partition's segments arrive together) instead of collecting every version and building
  per-point chains; grid output deduplicates winners per point and moves each version at
  its last use.
- **Segment format 2: encoding 8, nibble delta** (FORMAT.md §4.2). Zigzag differences as
  4-bit byte counts followed by the little-endian bytes. The value bytes stay
  byte-aligned and repeat like varints, so zstd compresses them as well (measured on the
  medium forecasts: 0.093 vs 0.090 B/row for received times), but decoding needs no varint
  parse per value (2.0 vs 3.9 ns per value). The header version and footer format byte
  are 2; readers accept 1 and 2, and encoding 8 only in format 2. A bit-packed
  frame-of-reference encoding was tried first: equally fast, but 4–5x larger after zstd
  (its fixed-width fields break the byte-level repetition), so it was dropped. Writers
  must use minimal byte counts, readers need not reject longer ones (they denote the same
  value; the CRC catches corruption): checking it cost a third of the decode time.
- **Decoder hygiene.** Per-thread reusable buffers (decompression scratch, integer
  scratch, one reused decoded block per scan) and one reused zstd context: allocating and
  freeing a block's worth of buffers per block made the allocator return pages to the
  system and fault them in again. Value columns are decoded only for the rows a read
  returns.
- **Lead-time filter pushdown.** `history_lead` / Python `history(lead=(column, min,
  max))`: versions with `min <= value_time - column < max`. Defined as `history` with the
  other rows removed (so not a semantic change), recorded in the receipt; segments whose
  column statistics cannot match are skipped, and a per-block row mask drops rows before
  anything but the filter column is decoded.

## Consequences
- Engine-column decoding: 58 → 23 ns/row on the medium forecasts (format 2, buffers
  reused). Segment sizes unchanged within a few percent (smaller blocks, more segments
  per partition, but the same per-row encoding density).
- A partition keeps one segment per arrival period: reads that need a whole partition
  (latest, grid, history) touch more, smaller segments; footers are cached and the
  per-segment cost is small next to decoding.
- Segment files written now are unreadable by engines that predate format 2 (they reject
  version 2 explicitly). `WriteOptions::format_version = 1` still writes format 1.
- Not done: skipping older segments for points already resolved by newer ones (latest and
  grid could skip most of a partition's segments), a columnar result path to the Python
  binding (results are built as rows, then converted to Arrow), vectorised decoding.
