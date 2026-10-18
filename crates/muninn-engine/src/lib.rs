//! # muninn-engine
//!
//! The Muninn storage engine: an implementation of [`muninn_core::Database`] over any
//! [`muninn_store::Store`] (memory, local filesystem, later object stores).
//!
//! ## Architecture (decisions 0004, 0005)
//!
//! - **Commit log.** `log/<20-digit version>.json`, one entry per committed change (catalog
//!   change, write batch, compaction, GC), written with `put_if_absent`. The entry *is* the
//!   commit. Every few versions a full-state checkpoint `log/<version>.checkpoint.json` is
//!   written, and `log/_last_checkpoint` names the newest one (a hint only: recovery
//!   verifies it and falls back to listing). See [`log`].
//! - **Segments.** A write encodes its new versions as one immutable segment file per
//!   touched `(table, partition)` (`muninn-format`), writes them, then commits the log
//!   entry. A crash before the commit leaves orphan files that [`Engine::gc`] removes.
//! - **State.** The committed state is an immutable value ([`state::State`]) — catalog,
//!   batch ids, high-water marks, live segments with their statistics — derived from the
//!   log by one `apply` function used both when committing and when recovering.
//! - **Writes** validate with core's shared code, deduplicate against the stored versions
//!   of the touched points (pruned by partition, series and value time), assign
//!   `system_time`/`seq`, write segments and commit. A lost race (`AlreadyExists`) is a
//!   `Conflict`; any failed commit marks the engine stale so it reloads from the log before
//!   serving anything else.
//! - **Reads** pin a snapshot ([`EngineSnapshot`], an `Arc` of the state). Each SPEC §6
//!   operation is a pruned scan (partition by value range; segment and block skipping by
//!   series, value time, `t_clock` and system time; per-series row slices) followed by a
//!   reduction under core's knowledge order. `grid` resolves all frontiers in one pass.
//! - **Compaction** merges a partition's segments into one, committed as a log entry that
//!   adds the new segment and retires the old ones; results never change.

#![forbid(unsafe_code)]

mod engine;
pub mod log;
mod read;
mod segio;
pub mod state;

use std::sync::Arc;

pub use engine::{Engine, GcReport};
pub use muninn_format::WriteOptions;
pub use read::EngineSnapshot;

/// A wall clock in µs since the epoch (used for segment names and GC grace periods only —
/// never for `system_time`, which comes from the write's clock reading).
pub type WallClock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Engine settings. None of them changes a query result.
#[derive(Clone)]
pub struct EngineOptions {
    /// Write a checkpoint every this many log versions (0 = never automatically).
    pub checkpoint_interval: u64,
    /// Segment writer settings (block size, compression).
    pub segment: WriteOptions,
    /// GC deletes an unreferenced segment only when it is at least this old.
    pub gc_grace: std::time::Duration,
    /// Wall clock for segment names and GC.
    pub wall_clock: WallClock,
    /// Maximum number of cached segment footers.
    pub footer_cache_entries: usize,
    /// Bytes read from a segment's end to find its footer (one ranged read if the footer
    /// fits).
    pub tail_read: u64,
}

impl Default for EngineOptions {
    fn default() -> EngineOptions {
        EngineOptions {
            checkpoint_interval: 100,
            segment: WriteOptions::default(),
            gc_grace: std::time::Duration::from_secs(3600),
            wall_clock: Arc::new(system_wall_clock),
            footer_cache_entries: 4096,
            tail_read: 64 * 1024,
        }
    }
}

impl std::fmt::Debug for EngineOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineOptions")
            .field("checkpoint_interval", &self.checkpoint_interval)
            .field("segment", &self.segment)
            .field("gc_grace", &self.gc_grace)
            .field("footer_cache_entries", &self.footer_cache_entries)
            .field("tail_read", &self.tail_read)
            .finish_non_exhaustive()
    }
}

/// The system wall clock in µs since the epoch.
pub fn system_wall_clock() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}
