# 0003: Frontier clock orders versions; dedup on full content

- Status: accepted (revisit after real data)
- Date: 2026-10-18

## Context
"Latest version visible at frontier F" needs an order. Candidates:

1. Order by the frontier clock, then `system_time`, then `seq` (notes/01).
2. Always order by `received_time` (then system, seq) and use the clock only for visibility.
3. Order by write order (`system_time`, `seq`) — XTDB-style "last assertion wins".

(3) breaks out-of-order backfills and delayed ingestion: loading run A after run B would
make the older run win. (2) gets the published-time-correction example right (today's
knowledge reports 08:37), but under a published frontier a *silently corrected old run*
received late would override a newer run — wrong **values**. (1) reports slightly stale
**metadata** in the correction case (the 10:00 version stays latest-by-published-time after
10:00) but never picks wrong values in that scenario.

## Decision
- Visibility: `system_time <= S and t_clock(v) <= F`, both inclusive.
  `t_published = coalesce(published_time, received_time)`.
- Order: `(t_clock, system_time, seq)` — option (1). Pinned by scenario 002.
- Dedup: a row is dropped iff content-identical (all user fields, f64 bitwise) to an
  existing version. Same values with a new `received_time` is a new version. Plus optional
  client `batch_id` for whole-batch idempotency.

## Consequences
- Correcting a `received_time` to an *earlier* time doesn't override under the received
  clock (the older, later-received version still sorts last). Rare (synthetic backfill
  times); fix by purge + re-backfill, or a future explicit `supersedes`.
- "Received time = first time we got this content" is not what we store; re-sends create
  versions. A `skip_unchanged` write mode can be added later without changing the spec.
