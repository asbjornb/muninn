# Storage

Mostly borrowed from kdb+ (layout) and ClickHouse (segments + zone maps).

## Layout

Everything below is described as a directory tree, but it must work equally well on
object storage (S3 / Azure Blob / GCS) — see `08-deployment.md`. Segments are
immutable once written; the only mutable thing is the manifest, committed atomically.

```
db/
  catalog/            symbol dictionary, table schemas, series catalog
  wal/                write-ahead log segments
  <table>/
    2025.01.15/       partition per value_date (kdb-style)
      manifest        list of committed segments for the partition
      seg-000123/
        footer        min/max per column, bloom on series, time bounds, row count
        series.col    one file per column ("splayed")
        value_time.col
        received_time.col
        ...
```

- **Partition by value date** (daily dirs). Matches the most common access pattern
  (a range of delivery days) and makes backup/retention trivial (copy/drop a dir).
  Open: is daily right for all tables? Monthly for sparse data?
- **Within a partition, sort by** `(series, value_time, frontier_time, system_time, seq)`
  so "latest version as of F per (series, value_time)" is a single forward scan.
- **One file per column**, mmap'd locally. Opening a table is just reading manifests.
  On object storage, one-file-per-column means many small GETs; a single segment file
  with column chunks at known offsets (Parquet-like, ranged reads) may be better.
  mmap then applies to the local cache, not the source of truth.

## Segment footer

- Per-column min/max (zone maps) and null counts, per block (~64K rows).
- Series bloom filter / min-max of interned ids.
- Bounds on all time columns, notably the frontier columns — lets us skip whole segments
  whose earliest received_time is after the frontier.
- Maybe a tiny per-series index (offset ranges) so a single-series lookup doesn't scan.

## Pruning order

1. Partition by value_date range.
2. Segment/block by frontier time bounds (received/published/system).
3. Series bloom / min-max.
4. Scan the remaining blocks.

No B-trees in v1. If this isn't fast enough, the sort order is wrong.

## Compression

- Hot segments: LZ4 or none.
- Sealed/cold segments: ZSTD, plus column-specific encodings (delta / delta-of-delta for
  timestamps, dictionary for symbols, Gorilla/XOR or plain ZSTD for doubles).
- Target sealed segment size ~128–512 MB (to be tuned; may be far too big for this data).

## Compaction

- Background, single-threaded: merge many small segments into larger sorted ones.
- **Must not change query results** — compaction only reorganizes; it never collapses
  version chains in the base table (that would break point-in-time reads).
- Optional derived "current truth" tables (latest version only) for non-PIT analytics can
  be rebuilt freely.

## Retention / TTL

Two-dimensional: by value_time (drop old delivery days) and by system_time (drop
superseded versions older than N months, accepting that PIT reads that far back degrade).
Default: keep everything. Disk is cheap, wrong backtests are not.

## Open

- Own format vs. Parquet/Arrow IPC files with our own manifests on top. Parquet gets us
  tooling and interop for free; own format gets us mmap and control. Possibly: own hot
  format, Parquet for sealed segments. See `open-questions.md`.
