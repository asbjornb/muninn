# FAQ — "how do I model X?"

Domain questions that the base model already answers, so they don't turn into engine
features. If an answer here starts needing engine support, it moves to `open-questions.md`.

## Delivery periods are intervals. Where's the end time?

Store the **start** in `value_time`; the period length is the series' **resolution** in the
catalog. Clients can present points as `[start, start + resolution)` without us storing it.

- **Normalise labels at ingest.** Some vendors label by period *end* (US "hour ending",
  HE1–HE24). Always convert to period start.
- **Overlapping products** (forward curves: Jan-27, Q1-27, Cal-27) → one series per product
  granularity (month / quarter / year curve). Within a series, periods never overlap.
- **Irregular intervals** (block products, REMIT UMMs / outage messages with arbitrary
  start/end) → put `value_end` in an ordinary value column.
- **Resolution changes mid-life** (e.g. day-ahead going from hourly to 15 min) → open: new
  series, or catalog resolution with a valid-from. See `open-questions.md`.

Where the engine *does* need interval semantics, eventually:

- As-of joining series of different resolution (hourly signal ↔ 15-min price) — match by
  "contained in", not equality.
- Resampling: sum vs. mean depends on the unit (MWh sums, MW and EUR/MWh average).

## How do I get "the 06z run" rather than "latest known"?

Three ways, pick per use case:

1. **One series per run cycle** (`ecmwf_wind_dk1_06z`, `..._12z`). Simple and explicit; good
   for models with fixed run cycles.
2. **`issue_time` as a value column**, filtered at read. Also gives lead-time analysis
   (`value_time - issue_time = 24h`).
3. **Frontier at the decision time** (e.g. 10:00 every day, before day-ahead gate closure).
   Often what a backtest actually wants: not "the 06z run" but "whatever we had at 10:00".

With (1), "latest across all cycles" means reading a *group* of series and keeping the newest
version per `value_time` — a small query feature, not a model change.

## Ensembles and quantiles?

Multiple value columns: `p10, p50, p90` or `m00 … m50`. Rows are multivariate scalars
already. Aggregating across members (ensemble mean, spread) happens client-side.

## Which frontier should a backtest use?

None is "the truth"; choose deliberately:

| Frontier | Includes | Answers |
| --- | --- | --- |
| `received_time` | our scraper latency, provider delays | "Would this have worked with *our* systems?" |
| `published_time` | vendor's claim (sometimes wrong) | "Would this work with zero latency?" — e.g. *is a better scraper worth building?* |
| `system_time` | ingest downtime, backloads | Reproducibility: "what did the DB say on date X?" |

- Vendors' published times are often wrong. Things to look for (candidates for automatic
  QA flags per source):
  - `published_time > received_time` — impossible, we can't receive before publication.
  - "Published time" that is really something else, e.g. the model run time or forecast
    issue time. Tell-tales: always lands exactly on run-cycle hours (00/06/12/18z), and the
    gap to our received time is consistently hours rather than minutes.
  - Silent corrections: values change but published time stays the same and no revision
    time is given. We see it as a new version with different content and an unchanged
    published time. Using that published time as the frontier would leak the corrected
    value back to the original publication — fall back to received time for such versions.
  - Published times identical across a whole history (backfill stamped with load time).
- Backfilled history has synthetic `received_time`; it is flagged as such
  (`pt_confidence` / source) and results should say how much rested on synthetic times.

## Derived series (residual load, spreads)?

Early on: compute outside Muninn and write them back as ordinary series. Give each derived
version the visibility times of its **latest** input, otherwise the derived series leaks.
When an input is revised, recomputing is the writer's job.

Later (roadmap, not early): build them at query time in the query language, where the
frontier applies to every input automatically, and possibly **materialized queries** that
the engine keeps up to date as inputs get new versions.

## Trades and orders?

**Undecided** — see `open-questions.md`. What we know so far:

- **Our own trades/orders**: small volume, need audit. Fit as ordinary series if wanted.
- **Market order books / public trades**: huge, compress worse, rarely revised, queried
  by event time (book reconstruction, VWAP). Different usage patterns; a separate tick
  store may fit better.
- The valuable bit is the *combined* picture: "for each trade, what forecasts were known at
  that moment". That's the as-of join; if its left side can be an external Arrow table,
  the combined picture doesn't require storing trades in Muninn.

## Can I get my data out if Muninn is abandoned?

Yes: the on-disk format is documented, and export to Parquet/Arrow/CSV exists from day one
(vision goal 6). A custom format is fine as long as a standalone reader is trivial.

## Licensed data must be removed when a contract ends?

A tombstone hides data at later frontiers but keeps it. **Purge** is different: physically
remove a series (or range) from all segments and rewrite manifests. Deliberately breaks
reproducibility for that data; logged in the catalog change log.
