//! # muninn-engine
//!
//! The Muninn storage engine: an implementation of [`muninn_core::Database`] over any
//! [`muninn_store::Store`] (memory, local filesystem, later object stores).
//!
//! ## Architecture (decisions 0004–0009)
//!
//! - **Commit log.** `log/<20-digit version>.json`, one entry per committed change (catalog
//!   change, write batch, compaction, GC), written with `put_if_absent`. The entry *is* the
//!   commit. A full-state checkpoint `log/<version>.checkpoint.json` is written once the
//!   entries since the last one outweigh it (size-triggered), and `log/_last_checkpoint`
//!   names the newest one (a hint only: recovery verifies it and falls back to listing).
//!   GC truncates the log below the older of the two newest checkpoints (decision 0007).
//!   See [`log`].
//! - **Writes go into the log** (decision 0009). Concurrent writes are committed together
//!   (group commit): one log entry holds every queued batch, each with its own
//!   `system_time` and seqs, and its new versions *inline* — one small segment per touched
//!   `(table, partition)`, embedded in the entry. So a commit is one store write (one PUT,
//!   or one file creation on local disk), and each call returns once it is durable. Large
//!   groups (bulk loads) write segment files first instead.
//! - **Memtable.** The live inline segments are part of the state and are read exactly
//!   like segment files, from memory. A flush (background thread, threshold, or
//!   [`Engine::flush`]) merges them into segment files per partition and commits a
//!   `flush` entry that swaps them in atomically; results never change.
//! - **Segments.** Immutable segment files (`muninn-format`), one table partition each,
//!   written by flushes, bulk writes, compaction and restore before the entry that
//!   references them. A crash before the commit leaves orphan files that [`Engine::gc`]
//!   removes.
//! - **State.** The committed state is an immutable value ([`state::State`]) — catalog,
//!   batch ids, high-water marks, live segments with their statistics — derived from the
//!   log by one `apply` function used both when committing and when recovering.
//! - **Writes** validate with core's shared code, deduplicate (an in-memory index of each
//!   point's stored received-time range decides which rows could be duplicates; only
//!   those are compared with stored versions, see `dedup`), assign `system_time`/`seq`,
//!   write segments and commit. A lost race (`AlreadyExists`) is a
//!   `Conflict`; any failed commit marks the engine stale so it reloads from the log before
//!   serving anything else.
//! - **Reads** pin a snapshot ([`EngineSnapshot`], an `Arc` of the state). Each SPEC §6
//!   operation is a pruned scan (partition by value range; segment and block skipping by
//!   series, value time, `t_clock` and system time; per-series row slices) followed by a
//!   reduction under core's knowledge order. `grid` resolves all frontiers in one pass.
//! - **Compaction** merges the segments of a partition whose versions became known (on the
//!   table's default clock) in the same period into one, so segments keep narrow
//!   knowledge-time ranges for pruning; committed as a log entry that adds the new segment
//!   and retires the old ones; results never change.

#![forbid(unsafe_code)]

mod b64;
mod dedup;
mod engine;
mod fxhash;
pub mod log;
mod read;
mod segio;
pub mod state;

use std::sync::Arc;

pub use engine::{Engine, GcReport, MemtableStats};
pub use muninn_format::WriteOptions;
pub use read::{EngineSnapshot, LeadFilter};

/// A wall clock in µs since the epoch (used for segment names and GC grace periods only —
/// never for `system_time`, which comes from the write's clock reading).
pub type WallClock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Engine settings. None of them changes a query result.
#[derive(Clone)]
pub struct EngineOptions {
    /// Write a checkpoint at most every this many log versions (0 = never
    /// automatically) ...
    pub checkpoint_interval: u64,
    /// ... and only once the log entries since the last checkpoint add up to at least
    /// this many times its size (0 = by count alone). Keeps checkpoint bytes linear in
    /// the log bytes instead of quadratic, while recovery reads at most about this
    /// factor times a checkpoint's size of entries.
    pub checkpoint_size_factor: f64,
    /// Segment writer settings (block size, compression).
    pub segment: WriteOptions,
    /// Compaction merges only segments whose versions became known in the same period
    /// — of the table's partition granularity (day, month, year), on the table's
    /// default clock (latest received, published or system time) — instead of every
    /// segment of a partition. A compacted segment then covers a narrow range of
    /// knowledge times, so reads bounded by `t_clock` or `system_time` (replay,
    /// frontiers, system bounds, lead filters) skip the others; a partition keeps about
    /// one segment per period in which its versions arrived.
    pub compact_by_period: bool,
    /// GC deletes an unreferenced segment only when it is at least this old.
    pub gc_grace: std::time::Duration,
    /// Wall clock for segment names and GC.
    pub wall_clock: WallClock,
    /// Maximum number of cached segment footers.
    pub footer_cache_entries: usize,
    /// Bytes read from a segment's end to find its footer (one ranged read if the footer
    /// fits).
    pub tail_read: u64,
    /// Points `(series, value_time)` the write-side dedup index may hold in memory
    /// (about 50 bytes each); least recently written partitions are evicted beyond that
    /// and rebuilt from their segments when written again. 0 disables the index (every
    /// write then reads the stored versions of every point it touches).
    pub dedup_index_points: usize,
    /// A commit group storing at most this many rows keeps them inline in its log entry
    /// (the memtable, decision 0009); a larger one (a bulk load) writes segment files
    /// first. 0 = never inline (every write writes segment files, as before 0009).
    pub inline_max_rows: usize,
    /// Most candidate rows one group commit takes off the queue (at least one batch).
    pub group_max_rows: usize,
    /// Flush the memtable once it holds this many rows ...
    pub flush_rows: u64,
    /// ... or this many inline segments ...
    pub flush_segments: usize,
    /// ... or its oldest segment is this old (by the wall clock; checked by the
    /// background flusher only).
    pub flush_age: std::time::Duration,
    /// Writers flush synchronously (back-pressure) while the memtable holds more rows
    /// than this.
    pub memtable_limit_rows: u64,
    /// Run a background thread that flushes at the thresholds above. Without it, a
    /// writer whose write leaves the memtable at a row or segment threshold flushes
    /// before its call returns (the other writers of its group are not delayed), and
    /// `flush_age` is not checked.
    pub background_flush: bool,
    /// Threads that write segment files in parallel (flushes and bulk writes).
    pub io_threads: usize,
}

impl Default for EngineOptions {
    fn default() -> EngineOptions {
        EngineOptions {
            checkpoint_interval: 100,
            checkpoint_size_factor: 1.0,
            segment: WriteOptions::default(),
            compact_by_period: true,
            gc_grace: std::time::Duration::from_secs(3600),
            wall_clock: Arc::new(system_wall_clock),
            footer_cache_entries: 4096,
            tail_read: 64 * 1024,
            dedup_index_points: 2_000_000,
            inline_max_rows: 100_000,
            group_max_rows: 1_000_000,
            flush_rows: 200_000,
            flush_segments: 2048,
            flush_age: std::time::Duration::from_secs(60),
            memtable_limit_rows: 1_000_000,
            background_flush: true,
            io_threads: 8,
        }
    }
}

impl std::fmt::Debug for EngineOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineOptions")
            .field("checkpoint_interval", &self.checkpoint_interval)
            .field("checkpoint_size_factor", &self.checkpoint_size_factor)
            .field("segment", &self.segment)
            .field("compact_by_period", &self.compact_by_period)
            .field("gc_grace", &self.gc_grace)
            .field("footer_cache_entries", &self.footer_cache_entries)
            .field("tail_read", &self.tail_read)
            .field("dedup_index_points", &self.dedup_index_points)
            .field("inline_max_rows", &self.inline_max_rows)
            .field("group_max_rows", &self.group_max_rows)
            .field("flush_rows", &self.flush_rows)
            .field("flush_segments", &self.flush_segments)
            .field("flush_age", &self.flush_age)
            .field("memtable_limit_rows", &self.memtable_limit_rows)
            .field("background_flush", &self.background_flush)
            .field("io_threads", &self.io_threads)
            .finish_non_exhaustive()
    }
}

/// The system wall clock in µs since the epoch.
pub fn system_wall_clock() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}
