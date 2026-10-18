# Ingestion

## Process model

- **Single writer, many readers** (kdb-style). The writer owns the WAL, assigns
  `system_time` and a monotonic `seq`, and publishes new segments by atomically updating
  manifests.
- Readers pin a snapshot = the set of manifests at query start. No locks.

## Write path

1. Batch arrives (API call, CSV/Parquet import, stream consumer).
2. Validate schema, intern symbols, stamp `system_time` + `seq`.
3. Append to WAL, fsync.
4. Buffer in memory; flush to a sorted segment when big enough or old enough.
5. Write segment files, fsync, then write commit marker / update manifest atomically
   (write-temp + rename).

## Idempotency

Re-sending a batch must be harmless. Idempotency key candidates:
`(series_id, value_time, received_time, values hash)` or a client-supplied
batch id. A retry with the same key is dropped; a different payload for the same key is a
new version (a correction).

Open: does a byte-identical re-send with a *new* received_time count as a new version?
Probably not — no information changed — but then received_time isn't "when we last got
it" but "when we first got this exact content".

## Corrections and tombstones

- Correction = new version, same identity, new payload and/or new published_time.
- Delete = tombstone version (`deleted = 1`). Its own times say when the deletion was
  published/received/written; invisible at frontiers after it.
- Optional `supersedes` pointer (from the early notes) is probably unnecessary when
  identity is explicit; versions are ordered by frontier/system time anyway.

## Backfills

The awkward case. Historical data loaded today has `system_time = today`, and an unknown
or unreliable `received_time`. Ingest should accept caller-supplied `received_time` /
`published_time` along with a confidence/source flag, so backfilled history is usable in
backtests, clearly marked as synthetic.

Possible helper: per-source latency/schedule model to synthesize times ("day-ahead
forecast from X usually lands ~09:45 CET").

## Bulk import

- `COPY`-like CSV/Parquet/Arrow import that produces aligned, sorted segments directly
  (bypassing the memtable for large loads).

## Crash recovery

On startup: read manifests, replay WAL entries newer than the last committed segment,
discard half-written segments (no commit marker). Idempotent replay means this is safe to
repeat.
