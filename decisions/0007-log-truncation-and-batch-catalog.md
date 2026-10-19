# 0007: Log truncation, size-triggered checkpoints, batch series creation

- Status: accepted
- Date: 2026-10-19

## Context
The benchmark (bench/results/README.md, problem 2) found metadata growing quadratically:
every `create_series` was its own commit, every 100th commit wrote a checkpoint of the
whole state (catalog and segment list), and nothing was ever deleted from `log/`. 6 000
series took 10 s and 49 MB of log; small forecasts had 184 MB of checkpoints for 7.6 MB of
segments; 150k series were extrapolated to 1.7 h and ~30 GB. Even without checkpoints the
entries alone outgrow the data: a forecast write lists ~11 segment records (~4 KB), and
compaction retires those segments soon after, so most of the log describes files that no
longer exist.

## Decision
- **Batch catalog changes.** `Database::create_series_batch` creates many series in one
  entry: checked in order exactly like single calls (later requests see earlier ones,
  each gets the catalog time a single call would), atomically. The catalog has a name
  index, and replay applies entries in place, so catalog work is linear.
- **Size-triggered checkpoints.** A checkpoint is written when at least
  `checkpoint_interval` entries *and* at least `checkpoint_size_factor` × the previous
  checkpoint's size in entry bytes were committed since it. Every checkpoint is paid for
  by at least its predecessor's size in entries, so checkpoint bytes are linear in log
  bytes; recovery reads at most about one checkpoint's worth of entries.
- **Log truncation by GC** (FORMAT.md §3.6, log format 2). GC checkpoints the head, keeps
  the two newest checkpoints, and removes entries and checkpoints below the second-newest
  one once it has seen them removable for the grace period — in any order, batched
  (`Store::delete_many`, one directory sync per directory), entry 1 overwritten by a
  `truncated` marker instead of deleted. The newest checkpoint must verify first.
  Entry `T` and everything above it stay, so a reader that finds `v+1` absent lists
  `log/`: an entry beyond `v+1` means its state is stale (truncated past) and it reloads
  from a checkpoint. Writers commit `v+1` only within `G / 2` of a proof that `v` was the
  head, so a writer that lagged behind a truncation conflicts instead of re-creating a
  deleted entry. (A first version deleted entries in ascending order and checked that
  `v` was still present; that needs every delete to be durable before the next — one
  directory fsync per entry — so the listing rule replaced it.)
- **Version bump.** Entries and checkpoints are written with `format: 2`; format 1 is read
  unchanged. Truncation is the only semantic difference, and a format-1 reader rejects
  format-2 files rather than misreading a truncated log.

## Consequences
- The log is no longer the full history of commits: it is "a checkpoint plus the entries
  after it". Everything the spec exposes survives (the catalog change log, SPEC §2.3, and
  all batch ids, SPEC §4.4, live in every checkpoint), but per-commit metadata of
  truncated entries (writer wall times, which segments a write added) is gone. `verify`
  folds from the oldest usable checkpoint.
- Losing both retained checkpoints (bit rot) makes the database unrecoverable from the
  log alone, where before a full replay was possible. Both are sealed and verified; the
  segments carry the data either way.
- Detecting the head costs a `LIST` of `log/` whenever a reader reaches the end of the
  log (refresh, reload, GC), and a truncation-safe writer needs a `HEAD` + `LIST` before a
  commit when its last proof of the head is older than `G / 2` (every commit when
  `G = 0`). Truncation keeps `log/` short, so the listing stays small.
- `create_series` one at a time is still one commit each and copies the catalog per call
  (snapshots share it); bulk loads should use the batch API (Python
  `create_series_batch`, CLI `series import`, and `restore` batches runs of series).
