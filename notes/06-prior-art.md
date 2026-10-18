# Prior art — steal sheet

What to take as-is, adapt, or skip. Cleaned up from the early input in `inbox/`.

## kdb+

**Steal**
- Partition-per-day directories with small manifests → fast catalog loads.
- Splayed columns (one file per column) + mmap reads.
- Symbol interning for keys.
- As-of join semantics ("latest row not after X") — the mental model for backtests.
- Single writer / many readers.

**Skip**
- q. Proprietary licensing model. The tick architecture (tickerplant/RDB/HDB) as separate
  processes — fold into one engine.

## Postgres

**Steal (as concepts)**
- Range types / exclusion constraints: "no overlapping validity per key" thinking.
- Small, predictable SQL surface.
- `pg_dump`/restore mindset: a simple, text-friendly export/import path from day one.

## ClickHouse

**Steal**
- Per-block min/max (zone maps) for brutal pruning.
- `ORDER BY` tuple as the canonical physical sort.
- Compression by default, per-column codecs.
- Incremental materialized views for "current truth" / rollups.

**Avoid**
- `ReplacingMergeTree`-style semantics where results depend on whether a merge has
  happened yet (`FINAL`). Our results must not depend on compaction timing.

## Timescale

**Steal**
- Hot vs. cold chunks; policy jobs (retention, compression, downsampling).
- Ergonomic helpers (`create_hypertable` → something like `create_series_table`).

## Others worth reading up on

- **XTDB** — bitemporal database (valid time + transaction time), immutable log. Closest
  conceptual cousin; check how they handle as-of queries and indexing.
- **SQL:2011 temporal tables** (system-versioned + application-time periods) — the
  standard vocabulary, supported partly in MariaDB/SQL Server/Db2.
- **ArcticDB / Arctic (Man Group)** — versioned dataframes for quant research; versioning
  model and S3-backed storage.
- **QuestDB** — columnar time series DB with `ASOF JOIN` / `LATEST ON`.
- **DuckDB** — embedded architecture, extension API (possible host for us).
- **Gorilla paper (Facebook)** — float/timestamp compression.
- **Feature stores' point-in-time joins** (Feast, Tecton) — same leakage problem, ML flavour.

## Skip for v1 (from all of the above)

- Multi-writer concurrency, cross-partition transactions.
- Secondary indexes, FKs.
- Full ANSI SQL, window frames.
- Distribution/sharding (shard by market later if ever).
