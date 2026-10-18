# Early input (raw, 2026-10-18)

Unedited brainstorm from earlier conversations. Input, not dogma. Distilled into `notes/`.
Note the naming drift: part 1 uses valid_time/available_time, part 2 uses
value_time/knowledge_time/published_time — reconciled in `notes/01-time-model.md`.

---

## Part 1: steal sheet for a bitemporal backtesting DB

Focused "steal sheet" broken into what to take as-is, what to adapt, and what to skip (for v1)
from kdb+, Postgres, ClickHouse, and Timescale — shaped for a bitemporal backtesting DB
(dual time: available_time, valid_time).

### What to STEAL (as-is)

From kdb+
- Partition by (valid_date): directory-per-day; tiny manifest file per partition for fast catalog loads.
- Splayed columnar files + mmap reads: one file per column; cheap column scans, instant open.
- Symbol interning: global dictionary for high-cardinality keys (markets, models) → space + speed.
- As-of join semantics: "latest row not after X"; perfect mental model for backtests.
- Single-writer, many-reader process model: determinism and simple crash recovery.
- Append-only truth: corrections are new facts; older intervals get "closed," never rewritten.

From Postgres
- Exclusion constraints idea (concept, not literal): "no overlapping valid ranges per key."
- Range/period thinking: valid_[from,to) + available_[from,to).
- Simple SQL surface: keep syntax tiny, composable, and predictable.
- pg_dump/restore mindset: provide a simple, text-friendly export/import path early.

From ClickHouse
- Zone maps / min-max indexes per column chunk for brutal pruning.
- Materialized views (incremental, background) for "current truth" / rollups.
- Compression by default (LZ4/ZSTD), lightweight blocks, column-wise encoding.
- ORDER BY tuple: e.g. (key, valid_time, available_time, version) as the canonical sort.

From Timescale
- Time-chunking mental model: hot vs. cold chunks; retention policies.
- Policy jobs: automatic TTL, reindex, compress, and downsample tasks.
- User ergonomics: helpers like create_bitemporal_table(...) instead of hand-rolled DDL.

### What to ADAPT (bitemporalized)

Storage & layout
- Primary partitioning: by valid_date (daily dirs).
  Secondary organization: within a partition, sort by (key, valid_time, available_time, version).
- Segment metadata (footer):
  - per-column min/max, nullmaps
  - per-segment key bloom/filter
  - bitemporal bounds (min/max available_time, valid_time)
  - short "version chain" index per key-range for fast "latest-as-of".

Indexing & pruning
- Dual-time pruning: first by partition (valid-date); then zone-map prune by available_time; then key/bloom prune.
- No secondary B-trees (v1): rely on sort + zone maps + small per-key version index in footer.

Ingestion & consistency
- WAL → segmenter → column files; single writer fsyncs a tiny commit marker after segment write.
- Overwrite = new version: enforce deterministic tie-break: max(available_time), then max(version), then max(insert_id).
- Idempotency key: (key, valid_from, available_from, version).

Compaction & TTL
- Background compactor (single thread):
  - merge micro-chunks → macro-segments
  - close intervals deterministically
  - drop expired by valid_time or by available_time (two-dimensional TTL)
- Cold compression: rewrite to heavier codecs (ZSTD) once "sealed".

Query surface (tiny SQL+)
- Clauses:
  - AS OF 't_avail' (transaction/knowledge time)
  - VALID AT 't_valid' or VALID BETWEEN 'v1' AND 'v2'
- Functions:
  - LAST_KNOWN(value BY key, valid_time) AS OF t
  - As-of join: ... JOIN ... USING (key) AS OF 't_avail'
- Materialized views:
  - current_truth(model, key, valid_time) maintained incrementally
  - daily_rollup(...) AS OF t (parameterized by "knowledge time")
- Determinism promise: results are independent of compaction timing.

Schema & evolution
- Additive evolution: new column = new file from now on; readers default missing to null/zero.
- Typed columns: scalars + fixed-size vectors allowed; late bind types via columnar metadata.

Concurrency & crash recovery
- Single writer + many mmap readers; readers pin a catalog snapshot (list of committed segments).
- Recovery: on startup, scan WAL tail; finalize any half-written segment; replay idempotently.

Tooling
- SQL-ish + HTTP + Arrow Flight: keep it easy to adopt in Python/C#/Rust.
- COPY-like bulk ingest: CSV/Parquet import that produces aligned segments.
- Simple backup: partition-level copy (valid-date dirs) + manifest.

### What to SKIP (for v1)

- Multi-writer concurrency; cross-partition updates.
- Arbitrary secondary indexes and user-defined FKs.
- Full ANSI SQL surface; window frames beyond LAST_KNOWN.
- Distributed cluster; start single-node (shard-by-market later).
- Complex transaction semantics; stick to idempotent batch appends.

### Opinionated defaults (so it "just works")

- Types: key = interned symbol; model = smallint enum; times = UTC int64 ns.
- Sort: (key, valid_time, available_time, version); block size ~64K rows.
- Compression: LZ4 hot, ZSTD level 6 cold.
- Chunking: target 128–512 MB segments before sealing.
- TTL: default keep all VALID forever, prune by available_time after N months (config).
- Determinism: stable as-of answers without needing a FINAL/WITH TIES-style hint.

### Minimal API sketch

```sql
-- Create a bitemporal table with keys+scalars
CREATE BITEMPORAL TABLE forecasts (
  key           SYMBOL,
  model         SMALLINT,
  valid_time    TIMESTAMP NOT NULL,
  available_time TIMESTAMP NOT NULL,
  value         DOUBLE,
  version       INT DEFAULT 1
) PARTITION BY valid_date(valid_time);

-- Append (overwrites imply higher available_time or version)
INSERT INTO forecasts (...) VALUES (...);

-- Point-in-time replay: what we knew then about a future day
SELECT key, model, valid_time, LAST_KNOWN(value BY (key, model, valid_time))
FROM forecasts
AS OF '2025-01-15T12:00:00Z'
VALID BETWEEN '2025-01-16T00:00:00Z' AND '2025-01-16T23:59:59Z'
WHERE key IN ('DK1','DK2');

-- As-of join signals to realized prices
SELECT t.key, t.valid_time, t.pred, r.price
FROM ( SELECT ... FROM forecasts AS OF $t VALID AT $v ) t
AJOIN realized r USING (key, valid_time) AS OF $t;
```

### MVP checkboxes (build order)

1. On-disk format (+ WAL, commit markers, manifests).
2. Single-writer ingestion (append; idempotency; mmap readers).
3. Dual-time pruning (partition + zone maps).
4. AS OF / VALID filtering + LAST_KNOWN.
5. As-of join (nested merge on (key, valid_time) with per-key version pickers).
6. Materialized "current truth" (incremental, background).
7. TTL + compactor (deterministic interval closing).
8. Parquet import + HTTP/Flight endpoints.

---

## Part 2: late-bindable published time

To handle "we don't know the real published time at first" without painful rewrites,
design the store so published time is late-bindable. Separate what you knew when from
what the true published time is.

### Core idea: two clocks, one truth

- knowledge_time (KT) = when your system learned/accepted a version (first seen or later corrected).
- published_time (PT) = the "ground truth" publish/available time for the data as asserted
  by the best source you have at KT. PT can start provisional and later be corrected.

Backtests use KT as the frontier (no leakage), and within that frontier you use the
best-known PT as of that KT.

### How this solves the case

- Live stream? KT ≈ PT; fine.
- Backfills or aggregator with sketchy timestamps?
  1. Ingest rows with PT_provisional = ingest_time and pt_confidence='provisional' (or with
     the aggregator's claimed PT but low confidence). KT = now.
  2. When you later discover the authoritative PT, append a correction version for the same
     logical point, with KT = now(), PT = corrected_published_time, supersedes = prior_version_id.
  3. No rewrites. No updates in place. Queries "as of frontier" pick the latest version with
     KT ≤ frontier. That version brings along whichever PT was known at that time.

This preserves historical reality: if on March 5 you thought the row was published at
10:00, a backtest with frontier = March 5 sees 10:00; a backtest with frontier = March 10
(after you learned the truth) sees 08:37.

### Minimal schema sketch (ClickHouse-friendly)

```sql
dataset_points (
  dataset_id       String,
  value_time       DateTime64(3, 'UTC'),

  -- Versioning
  version_id       UUID,              -- immutable identity for this version
  supersedes       UUID DEFAULT null, -- links to previous version (if correction)
  knowledge_time   DateTime64(3, 'UTC'),  -- when THIS version became known

  -- The value and its publish time (late-bindable)
  value            Float64,
  published_time   DateTime64(3, 'UTC'),  -- provisional or corrected
  pt_confidence    LowCardinality(String),-- 'provisional' | 'authoritative' | 'claimed'
  pt_source        LowCardinality(String),-- 'ingest_time' | 'provider_field' | 'page_ts' | ...

  -- Provenance / QA
  provider         LowCardinality(String),
  source_id        LowCardinality(String),
  is_deleted       UInt8 DEFAULT 0
)
ENGINE MergeTree
PARTITION BY (dataset_id, toYYYYMM(value_time))
ORDER BY (dataset_id, value_time, knowledge_time, version_id);
```

### PIT read (frontier = KT)

```sql
WITH toDateTime64('2025-02-01 23:59:59', 3, 'UTC') AS F
SELECT
  value_time,
  anyLast(value)          AS value_asof,
  anyLast(published_time) AS pt_asof,
  anyLast(pt_confidence)  AS pt_conf_asof
FROM
(
  SELECT *,
         row_number() OVER (
           PARTITION BY dataset_id, value_time
           ORDER BY knowledge_time DESC, version_id DESC
         ) AS rn
  FROM dataset_points
  WHERE dataset_id = 'xyz'
    AND value_time BETWEEN ts_from AND ts_to
    AND knowledge_time <= F
) t
WHERE rn = 1
ORDER BY value_time;
```

### Replay

Order by knowledge_time (what you learned, when), not PT. That's the stream your systems would have seen.

### Storage & performance notes

- Keep PT out of the physical identity (don't key on PT). Cluster on (dataset_id, value_time, knowledge_time). PT can change between versions without reshuffling old data.
- Maintain a projection (or MV) sorted by (dataset_id, published_time, value_time) for analytics by true PT (latency histograms, SLA checks).
- Corrections are just new versions; compaction can keep only the latest version per (dataset_id, value_time) in an "authoritative view" table for non-PIT analytics.

### Quality & metadata

- Confidence & source: track pt_confidence and pt_source; alert on "too many provisional PTs".
- Watermarks: maintain both max(knowledge_time) and max(published_time) per dataset.
- Late arrival stats: lateness_sec = published_time - value_time; correction delay = KT(correction) - KT(initial).

### Backfill workflow

1. Initial backfill with provisional PT = ingest time (or claimed PT, low confidence); KT = now.
2. Reconciliation job parses better PTs → emits correction versions (KT = when produced) with authoritative PT.
3. (Optional) Tombstone versions with is_deleted=1 for spurious rows.
4. Nightly authoritative snapshot MV: latest version per (dataset_id, value_time) for non-PIT users.

### Questions for a DB-builder

- Identity: best stable key for a value_time point so we never "rewrite" when PT changes? (Version chain vs. indirection table.)
- Clustering: we filter on KT for PIT but also analyze by PT — what dual-projection layout gives best perf without doubling storage?
- Planner: fastest kernel for "pick latest version by KT ≤ F" under our sort key — argMaxIf vs. window?
- Compaction: when/if to collapse long correction chains in the authoritative view while keeping PIT correctness.
- APIs: should clients only send frontier = KT and never a PT filter? If PT filters are needed, how to ensure they use PT as of the KT frontier?
- QA semantics: gap/freshness with calendars when PT is provisional and later corrected — re-score historical windows? Keep metrics PIT-aware?

### TL;DR policy

- Frontier = knowledge_time.
- Published_time is late-bound, versioned metadata.
- Corrections append; never rewrite.
- Queries pick the latest version by KT ≤ frontier and use the PT known at that KT.

---

## Part 3: own note

There are generally two very important timestamps: when the knowledge was publicly
available in the market, and when we received it (and could act on it) — ideally close in
time. Many series have other timestamps (time of the underlying models used, last data
input into the forecast, forecast time, value time etc.), but the base model should be simple.
