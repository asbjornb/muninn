//! [`Engine`]: open/recovery, commits, writes, catalog changes, compaction and GC.
//!
//! The engine's machinery lives in [`Core`], shared (`Arc`) between the [`Engine`] handle
//! and the background flusher thread; [`Engine`] forwards to it and stops the flusher when
//! dropped.

use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::thread::JoinHandle;
use std::time::Instant;

use muninn_core::api::{
    AddColumn, AsofJoinRequest, AsofRow, CreateSeries, CreateTable, Database, GridRequest, GridRow,
    HistoryRequest, LatestRequest, ReadResult, Reader, ReplayRequest, VersionRow, WriteBatch,
    WriteResult,
};
use muninn_core::catalog::{CatalogChange, CatalogChangeKind, catalog_time};
use muninn_core::types::{SeriesDef, SeriesId, TableDef, Version};
use muninn_core::{Error, ErrorKind, Result, TimeRange, Timestamp};
use muninn_format::{Partition, SegmentFile, ValueColumn, write_segment};
use muninn_store::{Key, PutOutcome, Store, StoreErrorKind};

use crate::EngineOptions;
use crate::dedup::DedupIndex;
use crate::fxhash::{FxHashMap, FxHashSet};
use crate::log::{
    self, CatalogChangeJson, Checkpoint, Hint, LOG_FORMAT, LogEntry, from_json, is_truncated,
    to_json, truncated,
};
use crate::read::{EngineSnapshot, ScanSpec, Scanner};
use crate::segio::{Shared, check_columns, is_missing_segment, store_err};
use crate::state::{SegmentRef, State};

mod chunked;
mod flush;
mod restore;
mod write;

/// What [`Engine::gc`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Segment files deleted.
    pub deleted: Vec<Key>,
    /// Unreferenced files kept because they are younger than the grace period or a live
    /// snapshot of this process still uses them.
    pub kept: usize,
    /// Log entries and checkpoints deleted (log truncation, decision 0007; entry 1 is
    /// replaced by the truncation marker instead and not counted).
    pub log_deleted: Vec<Key>,
}

/// Bookkeeping for size-triggered checkpoints: the newest checkpoint this engine knows
/// of, and the log written or read after it.
#[derive(Copy, Clone, Debug, Default)]
struct CkTrack {
    version: u64,
    bytes: u64,
    since_entries: u64,
    since_bytes: u64,
}

impl CkTrack {
    fn due(&self, opts: &EngineOptions) -> bool {
        let every = opts.checkpoint_interval;
        every > 0
            && self.since_entries >= every
            && self.since_bytes as f64 >= opts.checkpoint_size_factor * self.bytes as f64
    }
}

/// The result of reading the log: a state, the checkpoint it started from, what was
/// applied after it, and when the state was last known to be the head.
struct Loaded {
    state: State,
    ck: CkTrack,
    at_head: Instant,
}

/// The current state plus every snapshot handed out (for GC), under one lock so that a
/// snapshot is registered atomically with reading the current state.
struct Published {
    state: Arc<State>,
    live: Vec<Weak<State>>,
}

/// The writer lock, held for the duration of every mutating operation.
struct Writer;

/// Memtable figures of [`Engine::memtable`].
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct MemtableStats {
    /// Live inline segments (not yet flushed).
    pub segments: usize,
    /// Rows in them.
    pub rows: u64,
    /// Their bytes (as held in memory and in the log, before base64).
    pub bytes: u64,
    /// Flushes this engine committed since it was opened.
    pub flushes: u64,
}

/// The engine's machinery (see the module docs).
pub(crate) struct Core<S: Store> {
    shared: Arc<Shared<S>>,
    published: Mutex<Published>,
    writer: Mutex<Writer>,
    nonce: RandomState,
    counter: AtomicU64,
    /// GC: when this engine first saw each unreferenced segment file (its own wall
    /// clock). A file is deleted only `gc_grace` after that, so no writer's clock is
    /// trusted.
    unreferenced_since: Mutex<HashMap<Key, i64>>,
    /// GC: when this engine first saw each log entry or checkpoint as truncatable.
    log_truncatable_since: Mutex<HashMap<Key, i64>>,
    /// Write-side dedup index (only used under the writer lock).
    dedup: Mutex<DedupIndex>,
    /// Size-triggered checkpoints.
    ck: Mutex<CkTrack>,
    /// The last time this engine knew its state to be the log head: `(version, when)`
    /// (decision 0007: a writer commits only from a recently confirmed head).
    head_seen: Mutex<Option<(u64, Instant)>>,
    /// Group commit (decision 0009).
    queue: Mutex<write::GroupQueue>,
    queue_cv: Condvar,
    /// Held for the duration of a flush (one at a time).
    flush_lock: Mutex<()>,
    flush_signal: flush::FlushSignal,
    /// A checkpoint the commit path left to the background thread.
    ck_pending: Mutex<Option<Arc<State>>>,
    flushes: AtomicU64,
    /// This engine committed a write (only a writing engine flushes in the background:
    /// a process that only reads never commits anything).
    wrote: AtomicBool,
}

/// The Muninn storage engine over a [`Store`] (decisions 0004, 0005, 0009).
///
/// One `Engine` is one writer process's handle: share it between threads (`Arc<Engine>`);
/// concurrent [`Database::write`] calls are committed together (group commit), and any
/// number of threads may read through it or through snapshots concurrently. All methods
/// take `&self`. Unless [`EngineOptions::background_flush`] is off, a background thread
/// flushes the memtable; it stops when the engine is dropped.
pub struct Engine<S: Store> {
    core: Arc<Core<S>>,
    flusher: Option<JoinHandle<()>>,
}

impl<S: Store> std::fmt::Debug for Engine<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("version", &self.core.published().state.version)
            .finish_non_exhaustive()
    }
}

impl<S: Store> Drop for Engine<S> {
    fn drop(&mut self) {
        if let Some(h) = self.flusher.take() {
            self.core.flush_signal.stop();
            let _ = h.join();
        }
    }
}

impl<S: Store + 'static> Engine<S> {
    /// Opens the database on `store`, creating an empty one if the log is empty.
    /// Recovery: latest checkpoint (found via the hint, else by listing `log/`) plus every
    /// later entry — including the memtable, whose rows live in the log. Segment files no
    /// entry references are ignored. Starts the background flusher unless
    /// [`EngineOptions::background_flush`] is off.
    pub fn open(store: S, opts: EngineOptions) -> Result<Engine<S>> {
        let background = opts.background_flush;
        let core = Arc::new(Core::open(store, opts)?);
        let flusher = if background {
            core.flush_signal.start();
            let c = Arc::clone(&core);
            Some(
                std::thread::Builder::new()
                    .name("muninn-flush".into())
                    .spawn(move || flush::flusher_loop(c))
                    .map_err(|e| Error::internal(format!("starting the flusher: {e}")))?,
            )
        } else {
            None
        };
        Ok(Engine { core, flusher })
    }
}

impl<S: Store> Engine<S> {
    /// The underlying store.
    pub fn store(&self) -> &S {
        &self.core.shared.store
    }

    /// The options.
    pub fn options(&self) -> &EngineOptions {
        &self.core.shared.opts
    }

    /// The current log version.
    pub fn version(&self) -> u64 {
        self.core.published().state.version
    }

    /// The catalog change log (SPEC §2.3), oldest first.
    pub fn catalog_log(&self) -> Result<Vec<CatalogChange>> {
        self.core.catalog_log()
    }

    /// Number of live segments (files and memtable segments) per `(table, partition
    /// label)`.
    pub fn segment_counts(&self) -> BTreeMap<(String, String), usize> {
        self.core.segment_counts()
    }

    /// [`EngineSnapshot::history_lead`] on the current state.
    pub fn history_lead(
        &self,
        req: &HistoryRequest,
        lead: &crate::read::LeadFilter,
    ) -> Result<ReadResult<VersionRow>> {
        self.core.history_lead(req, lead)
    }

    /// Keys of every segment file the current state references (not the memtable's
    /// inline segments, which are not files).
    pub fn live_segment_keys(&self) -> BTreeSet<Key> {
        self.core.live_segment_keys()
    }

    /// Keys of segments retired by compaction and not yet purged by GC.
    pub fn retired_segment_keys(&self) -> BTreeSet<Key> {
        self.core.retired_segment_keys()
    }

    /// Picks up entries committed by other writers (or by an ambiguous failure of ours).
    pub fn refresh(&self) -> Result<u64> {
        self.core.refresh()
    }

    /// Catches up with the log head and writes a checkpoint of it now (normally
    /// automatic, see [`EngineOptions::checkpoint_interval`]).
    pub fn checkpoint(&self) -> Result<()> {
        self.core.checkpoint()
    }

    /// Merges the segment files of one partition, by compaction group (see
    /// [`EngineOptions::compact_by_period`]): the files of each group with at least two
    /// become one. Returns whether anything was compacted. Never changes a query result
    /// (SPEC §7.2): the new segments hold exactly the same versions. The memtable is not
    /// compacted (see [`Engine::flush`]).
    pub fn compact(&self, table: &str, partition_label: &str) -> Result<bool> {
        self.core.compact(table, partition_label)
    }

    /// Compacts every partition that has a compaction group of more than one segment
    /// file; returns how many partitions were compacted.
    pub fn compact_all(&self) -> Result<usize> {
        self.core.compact_all()
    }

    /// Deletes segment files that no committed state references and truncates the log;
    /// see [`Engine::gc_with_grace`].
    pub fn gc(&self) -> Result<GcReport> {
        self.core.gc()
    }

    /// Deletes segment files that no committed state references: orphans of failed
    /// writes (ours or another writer's) and segments retired by compaction.
    ///
    /// Guarantees (FORMAT.md §3.5): the decision is made against the **log head** (GC
    /// first reads every entry it has not seen, so a lagging engine never mistakes
    /// another writer's segments for orphans), and re-checked against the head again just
    /// before deleting. A file is deleted only after *this engine* has seen it
    /// unreferenced for `grace` (by its own clock — no writer clock or segment name is
    /// trusted), and never while a live snapshot of this process references it. Writers
    /// refuse to commit segments older than half the grace period, so with a grace period
    /// longer than any store-call stall, GC cannot delete a segment that is about to be
    /// committed. Readers in other processes are protected by the grace period only.
    pub fn gc_with_grace(&self, grace: std::time::Duration) -> Result<GcReport> {
        self.core.gc_with_grace(grace)
    }

    /// Imports `versions` (in seq order) with their `system_time` and `seq` exactly as
    /// given, atomically, optionally recording `batch_id` (SPEC §4.4) in the same commit
    /// (dump/restore, `spec/DUMP.md`).
    ///
    /// Errors (nothing is changed): `invalid_argument` for an empty or already committed
    /// batch id, a seq other than the next unassigned one, or a `system_time` that
    /// decreases or is not after the latest stored one (a split batch);
    /// `unknown_series`, `unknown_column` (more values than the table has columns),
    /// `type_mismatch` and `tombstone_with_values` for malformed versions.
    ///
    /// Unlike [`Database::write`] there is no deduplication: the caller vouches that the
    /// versions come from a valid database (a dump). An empty `versions` with a batch id
    /// only records the id; with neither it is a no-op.
    pub fn restore(&self, versions: &[Version], batch_id: Option<&str>) -> Result<WriteResult> {
        self.core.restore(versions, batch_id)
    }

    /// Writes several batches as one group commit: each batch is validated, deduplicated
    /// and assigned its own `system_time` and seqs exactly as if written by
    /// [`Database::write`] one after the other, and is atomic; the results are in order.
    /// One durable commit (one store write) holds them all, together with whatever other
    /// threads queued meanwhile (unless a batch has more than
    /// [`EngineOptions::write_chunk_rows`] rows: then the call commits alone, processing
    /// that batch in chunks, decision 0010).
    pub fn write_many(&self, batches: &[WriteBatch]) -> Vec<Result<WriteResult>> {
        self.core.write_many(batches)
    }

    /// Flushes the memtable now: its rows move from the log into segment files (one per
    /// partition and compaction group), atomically. Returns whether anything was
    /// flushed. Never changes a query result. Normally automatic (see
    /// [`EngineOptions::flush_rows`]).
    pub fn flush(&self) -> Result<bool> {
        self.core.flush()
    }

    /// The memtable's size (rows written to the log and not yet flushed).
    pub fn memtable(&self) -> MemtableStats {
        let st = Arc::clone(&self.core.published().state);
        MemtableStats {
            segments: st.memtable.len(),
            rows: st.memtable_rows(),
            bytes: st.memtable_bytes(),
            flushes: self.core.flushes.load(Ordering::Relaxed),
        }
    }
}

macro_rules! forward_reads {
    ($($name:ident($req:ty) -> $out:ty;)*) => {$(
        fn $name(&self, req: $req) -> Result<$out> {
            self.core.$name(req)
        }
    )*};
}

impl<S: Store> Reader for Engine<S> {
    fn table(&self, name: &str) -> Result<TableDef> {
        Reader::table(&*self.core, name)
    }

    fn tables(&self) -> Result<Vec<TableDef>> {
        Reader::tables(&*self.core)
    }

    fn series(&self, name: &str) -> Result<SeriesDef> {
        Reader::series(&*self.core, name)
    }

    fn table_series(&self, table: &str) -> Result<Vec<SeriesDef>> {
        Reader::table_series(&*self.core, table)
    }

    forward_reads! {
        latest(&LatestRequest) -> ReadResult<VersionRow>;
        history(&HistoryRequest) -> ReadResult<VersionRow>;
        replay(&ReplayRequest) -> ReadResult<VersionRow>;
        grid(&GridRequest) -> ReadResult<GridRow>;
        asof_join(&AsofJoinRequest) -> ReadResult<AsofRow>;
    }
}

impl<S: Store> Database for Engine<S> {
    type Snapshot = EngineSnapshot<S>;

    fn snapshot(&self) -> Result<EngineSnapshot<S>> {
        self.core.snapshot()
    }

    fn create_table(&self, req: &CreateTable) -> Result<TableDef> {
        self.core.create_table(req)
    }

    fn add_column(&self, req: &AddColumn) -> Result<TableDef> {
        self.core.add_column(req)
    }

    fn create_series(&self, req: &CreateSeries) -> Result<SeriesDef> {
        self.core.create_series(req)
    }

    fn create_series_batch(&self, reqs: &[CreateSeries]) -> Result<Vec<SeriesDef>> {
        self.core.create_series_batch(reqs)
    }

    /// SPEC §4. Concurrent calls from several threads are committed together (group
    /// commit, decision 0009); each returns once its batch is durable.
    fn write(&self, batch: &WriteBatch) -> Result<WriteResult> {
        self.core.write_one(batch)
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section either completes or leaves the protected value untouched
    // (states are swapped whole), so a poisoned lock is safe to keep using.
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// `f(i, &items[i])` for every item, on up to `threads` threads; results in order.
fn parallel_map<T: Sync, R: Send>(
    items: &[T],
    threads: usize,
    f: impl Fn(usize, &T) -> R + Sync,
) -> Vec<R> {
    let threads = threads.clamp(1, items.len().max(1));
    if threads == 1 {
        return items.iter().enumerate().map(|(i, x)| f(i, x)).collect();
    }
    let next = AtomicU64::new(0);
    let done: Vec<Vec<(usize, R)>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let mut mine = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed) as usize;
                        let Some(x) = items.get(i) else { break };
                        mine.push((i, f(i, x)));
                    }
                    mine
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
            .collect()
    });
    let mut out: Vec<Option<R>> = (0..items.len()).map(|_| None).collect();
    for (i, r) in done.into_iter().flatten() {
        out[i] = Some(r);
    }
    out.into_iter()
        .map(|r| r.expect("every item was mapped"))
        .collect()
}

fn table_columns(t: &TableDef) -> Vec<ValueColumn> {
    t.columns
        .iter()
        .enumerate()
        .map(|(i, c)| ValueColumn {
            id: i as u32,
            ty: c.ty,
        })
        .collect()
}

impl<S: Store> Core<S> {
    fn open(store: S, opts: EngineOptions) -> Result<Core<S>> {
        let dedup = DedupIndex::new(opts.dedup_index_points);
        let shared = Arc::new(Shared::new(store, opts));
        let loaded = load(&shared)?;
        Ok(Core {
            dedup: Mutex::new(dedup),
            shared,
            head_seen: Mutex::new(Some((loaded.state.version, loaded.at_head))),
            ck: Mutex::new(loaded.ck),
            published: Mutex::new(Published {
                state: Arc::new(loaded.state),
                live: Vec::new(),
            }),
            writer: Mutex::new(Writer),
            nonce: RandomState::new(),
            counter: AtomicU64::new(0),
            unreferenced_since: Mutex::new(HashMap::new()),
            log_truncatable_since: Mutex::new(HashMap::new()),
            queue: Mutex::new(write::GroupQueue::default()),
            queue_cv: Condvar::new(),
            flush_lock: Mutex::new(()),
            flush_signal: flush::FlushSignal::default(),
            ck_pending: Mutex::new(None),
            flushes: AtomicU64::new(0),
            wrote: AtomicBool::new(false),
        })
    }

    fn published(&self) -> MutexGuard<'_, Published> {
        lock(&self.published)
    }

    /// Marks the state as possibly behind the log (reloaded before the next operation).
    fn mark_stale(&self) {
        self.shared.stale.store(true, Ordering::SeqCst);
    }

    /// The current state, caught up first if a commit failed (unlike [`Core::current`]
    /// not registered as a snapshot: for writers and the flusher, which hold it briefly
    /// and only read what the state itself holds or segments they then commit against).
    fn current_state(&self) -> Result<Arc<State>> {
        if self.shared.stale.load(Ordering::SeqCst) {
            let mut w = lock(&self.writer);
            return self.fresh(&mut w);
        }
        Ok(Arc::clone(&self.published().state))
    }

    /// The catalog change log (SPEC §2.3), oldest first.
    pub fn catalog_log(&self) -> Result<Vec<CatalogChange>> {
        Ok(self.current()?.catalog_log.to_vec())
    }

    /// Number of live segments, overall and per `(table, partition label)`.
    pub fn segment_counts(&self) -> BTreeMap<(String, String), usize> {
        let st = Arc::clone(&self.published().state);
        st.partitions
            .values()
            .filter_map(|v| {
                v.first()
                    .map(|s| ((s.table.clone(), s.partition.label()), v.len()))
            })
            .collect()
    }

    /// [`EngineSnapshot::history_lead`] on the current state.
    pub fn history_lead(
        &self,
        req: &HistoryRequest,
        lead: &crate::read::LeadFilter,
    ) -> Result<ReadResult<VersionRow>> {
        self.retry_missing(|| self.snapshot()?.history_lead(req, lead))
    }

    /// Keys of every segment file the current state references.
    pub fn live_segment_keys(&self) -> BTreeSet<Key> {
        let st = Arc::clone(&self.published().state);
        st.file_segments().map(|s| s.key.clone()).collect()
    }

    /// Keys of segments retired by compaction and not yet purged by GC.
    pub fn retired_segment_keys(&self) -> BTreeSet<Key> {
        let st = Arc::clone(&self.published().state);
        st.retired.iter().map(|(k, _)| k.clone()).collect()
    }

    fn publish(&self, state: Arc<State>) {
        self.published().state = state;
    }

    /// Catches up with the log if a previous commit failed ambiguously. Must hold the
    /// writer lock.
    fn fresh(&self, _w: &mut Writer) -> Result<Arc<State>> {
        let cur = Arc::clone(&self.published().state);
        if !self.shared.stale.load(Ordering::SeqCst) {
            return Ok(cur);
        }
        let next = match catch_up(&self.shared, (*cur).clone()) {
            Ok(c) => {
                let mut t = lock(&self.ck);
                t.since_entries += c.entries;
                t.since_bytes += c.bytes;
                (c.state, c.at_head)
            }
            // The entries after our version were truncated away (we lagged behind a GC
            // by more than its grace period): reload from a checkpoint.
            Err(e) if is_truncated(&e) => {
                let l = load(&self.shared)?;
                *lock(&self.ck) = l.ck;
                (l.state, l.at_head)
            }
            Err(e) => return Err(e),
        };
        let (next, at_head) = (Arc::new(next.0), next.1);
        *lock(&self.head_seen) = Some((next.version, at_head));
        self.publish(Arc::clone(&next));
        self.shared.stale.store(false, Ordering::SeqCst);
        Ok(next)
    }

    /// Runs `f`; if it failed because a live segment was missing (another writer compacted
    /// and collected it while our view lagged), the engine is marked stale, so running
    /// `f` once more reloads from the log first.
    fn retry_missing<T>(&self, f: impl Fn() -> Result<T>) -> Result<T> {
        match f() {
            Err(e) if is_missing_segment(&e) => f(),
            r => r,
        }
    }

    /// The state at the log head (reads every entry after ours). Holds the writer lock.
    fn head(&self, w: &mut Writer) -> Result<Arc<State>> {
        self.shared.stale.store(true, Ordering::SeqCst);
        self.fresh(w)
    }

    /// Fails (without committing) if segments written at `since` might already be
    /// considered orphans by a GC: writers must commit within half the grace period of
    /// writing their segments (FORMAT.md §3.5). A zero grace period disables the check.
    fn check_commit_age(&self, since: Instant) -> Result<()> {
        let grace = self.shared.opts.gc_grace;
        if !grace.is_zero() && since.elapsed() >= grace / 2 {
            return Err(Error::new(
                ErrorKind::Io,
                format!(
                    "segments were written {:?} ago, more than half the GC grace period; \
                     not committing (retry the operation)",
                    since.elapsed()
                ),
            ));
        }
        Ok(())
    }

    /// Picks up entries committed by other writers (or by an ambiguous failure of ours).
    pub fn refresh(&self) -> Result<u64> {
        let mut w = lock(&self.writer);
        self.shared.stale.store(true, Ordering::SeqCst);
        Ok(self.fresh(&mut w)?.version)
    }

    /// The current state (catching up first if a commit failed).
    fn current(&self) -> Result<Arc<State>> {
        if self.shared.stale.load(Ordering::SeqCst) {
            let mut w = lock(&self.writer);
            self.fresh(&mut w)?;
        }
        let mut p = self.published();
        let st = Arc::clone(&p.state);
        if p.live.len() >= 64 {
            p.live.retain(|w| w.strong_count() > 0);
        }
        p.live.push(Arc::downgrade(&st));
        Ok(st)
    }

    fn snapshot_of(&self, state: Arc<State>) -> EngineSnapshot<S> {
        EngineSnapshot {
            state,
            shared: Arc::clone(&self.shared),
        }
    }

    fn wall(&self) -> i64 {
        (self.shared.opts.wall_clock)()
    }

    fn new_entry(&self, base: &State, kind: &str) -> LogEntry {
        LogEntry {
            format: LOG_FORMAT,
            version: base.version + 1,
            kind: kind.to_owned(),
            wall_time: self.wall(),
            catalog: Vec::new(),
            batch_id: None,
            batch_ids: Vec::new(),
            add: Vec::new(),
            remove: Vec::new(),
            purged: Vec::new(),
            next_seq: base.next_seq,
            last_system_time: base.last_system_time.map(Timestamp::as_micros),
        }
    }

    /// Commits `entry` on top of `base`: validates it by applying it, writes it with
    /// put-if-absent, and publishes the new state. On any failure to write the entry the
    /// engine is marked stale (it does not know whether the log moved) and reloads before
    /// its next operation; `AlreadyExists` (another writer won) is a `Conflict`.
    fn commit(&self, w: &mut Writer, base: &State, entry: LogEntry) -> Result<Arc<State>> {
        self.commit_prepared(w, base, entry, &[])
    }

    /// [`Core::commit`] with the records of the entry's inline segments already decoded
    /// (`prepared`, matched by key), so that applying the entry does not decode them
    /// again.
    fn commit_prepared(
        &self,
        _w: &mut Writer,
        base: &State,
        entry: LogEntry,
        prepared: &[SegmentRef],
    ) -> Result<Arc<State>> {
        let next = Arc::new(base.apply_prepared(&entry, prepared)?);
        let bytes = to_json(&entry)?;
        self.confirm_head(base)?;
        let attempt = Instant::now();
        match self
            .shared
            .store
            .put_if_absent(&log::entry_key(entry.version), &bytes)
        {
            Ok(PutOutcome::Created) => {}
            Ok(PutOutcome::AlreadyExists) => {
                self.shared.stale.store(true, Ordering::SeqCst);
                return Err(Error::new(
                    ErrorKind::Conflict,
                    format!(
                        "log version {} was committed by another writer; reload and retry",
                        entry.version
                    ),
                ));
            }
            Err(e) => {
                self.shared.stale.store(true, Ordering::SeqCst);
                return Err(store_err(
                    &format!("committing log version {}", entry.version),
                    e,
                ));
            }
        }
        // Nobody can have written the next entry before ours existed.
        *lock(&self.head_seen) = Some((next.version, attempt));
        self.publish(Arc::clone(&next));
        let due = {
            let mut t = lock(&self.ck);
            t.since_entries += 1;
            t.since_bytes += bytes.len() as u64;
            t.due(&self.shared.opts)
        };
        if due {
            // An optimisation only: failures are ignored (recovery replays more entries).
            // With a background thread it writes the checkpoint, off the commit path
            // (a checkpoint holds the whole state, memtable included).
            if self.has_flusher() {
                *lock(&self.ck_pending) = Some(Arc::clone(&next));
                self.kick_flusher();
            } else {
                let _ = self.write_checkpoint(&next);
            }
        }
        Ok(next)
    }

    /// Writes the checkpoint a commit left to the background thread, if any. A
    /// checkpoint holds the memtable's rows too, so if the memtable is half way to a
    /// flush it is flushed first and the checkpoint taken after it: checkpoints then
    /// rarely copy many memtable rows, at the price of an occasional earlier flush.
    pub(crate) fn write_pending_checkpoint(&self) {
        if lock(&self.ck_pending).take().is_none() {
            return;
        }
        let Ok(st) = self.current_state() else {
            return;
        };
        let st = if st.memtable_rows() >= self.shared.opts.flush_rows / 2 && self.flush().is_ok() {
            match self.current_state() {
                Ok(st) => st,
                Err(_) => return,
            }
        } else {
            st
        };
        if lock(&self.ck).due(&self.shared.opts) {
            let _ = self.write_checkpoint(&st);
        }
    }

    /// Decision 0007: a commit of entry `v + 1` must follow a recent proof that `v` is
    /// the head — otherwise a writer that lagged behind a log truncation could re-create
    /// a deleted entry. Our own commit of `v` or a read that found `v + 1` absent and
    /// then `v` present are such proofs; with a grace period `G`, one is recent if it is
    /// younger than `G / 2` (GC deletes an entry only `G` after it saw it truncatable).
    /// With `G = 0` (single-process use) the head is checked before every commit.
    fn confirm_head(&self, base: &State) -> Result<()> {
        let grace = self.shared.opts.gc_grace;
        if !grace.is_zero()
            && matches!(*lock(&self.head_seen),
                Some((v, at)) if v == base.version && at.elapsed() < grace / 2)
        {
            return Ok(());
        }
        let probe = Instant::now();
        match head_probe(&self.shared, base.version) {
            Ok(()) => {
                *lock(&self.head_seen) = Some((base.version, probe));
                Ok(())
            }
            Err(e) => {
                self.shared.stale.store(true, Ordering::SeqCst);
                if e.kind() == ErrorKind::Conflict || is_truncated(&e) {
                    Err(Error::new(
                        ErrorKind::Conflict,
                        format!(
                            "log version {} is not the head any more; reload and retry",
                            base.version
                        ),
                    ))
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Writes a checkpoint of `st` (and the hint), and records it as the newest one.
    fn write_checkpoint(&self, st: &State) -> Result<()> {
        let store = &self.shared.store;
        let mut ck = st.to_checkpoint();
        if st.version > 0 {
            let entry = store
                .get(&log::entry_key(st.version))
                .map_err(|e| store_err("reading the checkpointed entry", e))?;
            ck.entry_crc32c = crc32c::crc32c(&entry);
        }
        let bytes = ck.seal()?;
        store
            .put_if_absent(&log::checkpoint_key(st.version), &bytes)
            .map_err(|e| store_err("writing checkpoint", e))?;
        {
            let mut t = lock(&self.ck);
            if st.version >= t.version {
                *t = CkTrack {
                    version: st.version,
                    bytes: bytes.len() as u64,
                    ..CkTrack::default()
                };
            }
        }
        store
            .put(
                &log::hint_key(),
                &to_json(&Hint {
                    version: st.version,
                })?,
            )
            .map_err(|e| store_err("writing checkpoint hint", e))
    }

    /// Catches up with the log head and writes a checkpoint of it now (normally
    /// automatic, see [`EngineOptions::checkpoint_interval`]).
    pub fn checkpoint(&self) -> Result<()> {
        let mut w = lock(&self.writer);
        let st = self.head(&mut w)?;
        self.write_checkpoint(&st)
    }

    // -----------------------------------------------------------------------------------
    // Segments
    // -----------------------------------------------------------------------------------

    /// Writes several segment files (`(table, partition, versions)` each) with up to
    /// [`EngineOptions::io_threads`] threads; returns their records in order.
    fn put_segments(&self, jobs: &[(&TableDef, Partition, &[Version])]) -> Result<Vec<SegmentRef>> {
        parallel_map(jobs, self.shared.opts.io_threads, |_, (t, p, vs)| {
            self.put_segment(t, *p, vs)
        })
        .into_iter()
        .collect()
    }
    /// Writes `versions` (one table partition) as a new segment file; returns its log
    /// record. The file is an orphan until a log entry references it.
    fn put_segment(
        &self,
        table: &TableDef,
        partition: Partition,
        versions: &[Version],
    ) -> Result<SegmentRef> {
        let bytes = write_segment(
            &table.name,
            partition,
            &table_columns(table),
            versions,
            &self.shared.opts.segment,
        )
        .map_err(|e| Error::internal(format!("encoding segment: {e}")))?;
        let footer = SegmentFile::open(&bytes)
            .map_err(|e| Error::internal(format!("segment we just wrote is invalid: {e}")))?
            .footer()
            .clone();
        let created = self.wall();
        let label = partition.label();
        for _ in 0..8 {
            let mut h = self.nonce.build_hasher();
            h.write_u64(self.counter.fetch_add(1, Ordering::Relaxed));
            h.write_i64(created);
            let key = log::segment_key(&table.name, &label, created, h.finish());
            match self.shared.store.put_if_absent(&key, &bytes) {
                Ok(PutOutcome::Created) => {
                    let seg = SegmentRef::from_footer(
                        key.clone(),
                        &table.name,
                        &footer,
                        bytes.len() as u64,
                        created,
                        None,
                    );
                    let mut f = footer;
                    f.file_size = bytes.len() as u64;
                    self.shared.cache_footer(&key, Arc::new(f));
                    return Ok(seg);
                }
                Ok(PutOutcome::AlreadyExists) => continue,
                Err(e) => return Err(store_err(&format!("writing segment {key}"), e)),
            }
        }
        Err(Error::internal("could not find a free segment name"))
    }

    /// The stored versions of the given points (for dedup) whose received time is one of
    /// the given ones (only those can be content-identical to a row), indexed by content.
    fn load_points(
        &self,
        st: &State,
        points: &BTreeMap<String, FxHashMap<SeriesId, BTreeSet<Timestamp>>>,
        rts: &FxHashSet<(SeriesId, Timestamp, Timestamp)>,
    ) -> Result<write::StoredVersions> {
        let mut out = write::StoredVersions::default();
        for (table, pts) in points {
            let table = st.catalog.table(table)?;
            let ids: BTreeSet<SeriesId> = pts.keys().copied().collect();
            let lo = pts.values().filter_map(|s| s.first()).min().copied();
            let hi = pts.values().filter_map(|s| s.last()).max().copied();
            let range = TimeRange::new(
                lo,
                hi.and_then(|t| Timestamp::from_micros(t.as_micros().saturating_add(1))),
            );
            // Only versions with one of the rows' received times matter: prune segments
            // and blocks by received time too.
            let rt_of = |c: &(SeriesId, Timestamp, Timestamp)| {
                pts.contains_key(&c.0).then_some(c.2.as_micros())
            };
            let tc_lo = rts.iter().filter_map(rt_of).min().unwrap_or(i64::MAX);
            let tc_hi = rts.iter().filter_map(rt_of).max().unwrap_or(i64::MIN);
            let mut scanner = Scanner::new(&self.shared, st);
            let mut cands = scanner.scan(&ScanSpec {
                table,
                ids: &ids,
                range,
                clock: muninn_core::Clock::Received,
                tc_lo,
                tc_hi,
                system: None,
                points: Some(pts),
                keep_segment: None,
                row_mask: None,
            })?;
            cands.retain(|c| rts.contains(&(c.series, c.vt, c.rt)));
            for v in scanner.materialize(table.columns.len(), &cands)? {
                out.push(v);
            }
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------------------
    // Compaction and GC
    // -----------------------------------------------------------------------------------

    /// Merges the segments of one partition, by compaction group (see
    /// [`EngineOptions::compact_by_period`]): the segments of each group with at
    /// least two become one. Returns whether anything was compacted. Never changes a
    /// query result (SPEC §7.2): the new segments hold exactly the same versions.
    pub fn compact(&self, table: &str, partition_label: &str) -> Result<bool> {
        self.retry_missing(|| self.compact_once(table, partition_label))
    }

    fn compact_once(&self, table: &str, partition_label: &str) -> Result<bool> {
        let mut w = lock(&self.writer);
        let st = self.fresh(&mut w)?;
        let t = st.catalog.table(table)?.clone();
        let p = Partition::parse_label(t.partition, partition_label)
            .map_err(|e| Error::new(ErrorKind::InvalidRange, e.message().to_owned()))?;
        self.compact_locked(&mut w, st, &t, p)
    }

    /// Compacts every partition that has a compaction group of more than one segment;
    /// returns how many partitions were compacted.
    pub fn compact_all(&self) -> Result<usize> {
        self.retry_missing(|| self.compact_all_once())
    }

    fn compact_all_once(&self) -> Result<usize> {
        let mut w = lock(&self.writer);
        let mut n = 0;
        let targets: Vec<(String, i64)> = {
            let st = self.fresh(&mut w)?;
            st.partitions
                .iter()
                .filter(|((table, _), v)| {
                    v.len() > 1
                        && st.catalog.table(table).is_ok_and(|t| {
                            let mut groups = BTreeMap::new();
                            v.iter().filter(|s| !s.is_inline()).any(|s| {
                                let c = groups.entry(self.compaction_group(t, s)).or_insert(0);
                                *c += 1;
                                *c > 1
                            })
                        })
                })
                .map(|(k, _)| k.clone())
                .collect()
        };
        for (table, index) in targets {
            let st = self.fresh(&mut w)?;
            let t = st.catalog.table(&table)?.clone();
            let p = Partition {
                granularity: t.partition,
                index,
            };
            if self.compact_locked(&mut w, st, &t, p)? {
                n += 1;
            }
        }
        Ok(n)
    }

    /// The compaction group of a segment: with [`EngineOptions::compact_by_period`],
    /// the period of the table's partition granularity (day, month, year) containing
    /// its greatest `t_clock` under the table's default clock; else one group per
    /// partition.
    fn compaction_group(&self, t: &TableDef, seg: &SegmentRef) -> i64 {
        if self.shared.opts.compact_by_period {
            Partition::of(t.partition, seg.t_clock(t.default_clock).max).index
        } else {
            0
        }
    }

    fn compact_locked(
        &self,
        w: &mut Writer,
        mut st: Arc<State>,
        t: &TableDef,
        p: Partition,
    ) -> Result<bool> {
        let Some(segs) = st.partitions.get(&(t.name.clone(), p.index)).cloned() else {
            return Ok(false);
        };
        let mut groups: BTreeMap<i64, Vec<Arc<SegmentRef>>> = BTreeMap::new();
        for seg in segs.iter().filter(|s| !s.is_inline()) {
            groups
                .entry(self.compaction_group(t, seg))
                .or_default()
                .push(Arc::clone(seg));
        }
        let mut any = false;
        for group in groups.into_values().filter(|g| g.len() > 1) {
            st = self.merge_segments(w, &st, t, p, &group)?;
            any = true;
        }
        Ok(any)
    }

    /// Replaces `segs` (of partition `p`, at least two) by one segment with their rows.
    fn merge_segments(
        &self,
        w: &mut Writer,
        st: &Arc<State>,
        t: &TableDef,
        p: Partition,
        segs: &[Arc<SegmentRef>],
    ) -> Result<Arc<State>> {
        let mut versions = Vec::new();
        for seg in segs {
            let bytes = self.shared.store.get(&seg.key).map_err(|e| {
                if e.kind() == StoreErrorKind::NotFound {
                    self.shared.stale.store(true, Ordering::SeqCst);
                    Error::new(
                        ErrorKind::Io,
                        format!("{}: {}", crate::segio::MISSING_SEGMENT, seg.key),
                    )
                } else {
                    store_err(&format!("reading {}", seg.key), e)
                }
            })?;
            let corrupt = |e: muninn_format::Error| {
                Error::new(ErrorKind::Io, format!("corrupt segment {}: {e}", seg.key))
            };
            let file = SegmentFile::open(&bytes).map_err(corrupt)?;
            check_columns(&seg.key, file.footer(), t)?;
            if file.footer().row_count != seg.rows || file.footer().partition != p {
                return Err(Error::new(
                    ErrorKind::Io,
                    format!("corrupt segment {}: footer disagrees with the log", seg.key),
                ));
            }
            versions.extend(file.versions_within(t.columns.len()).map_err(corrupt)?);
        }
        let started = Instant::now();
        let new = self.put_segment(t, p, &versions)?;
        self.check_commit_age(started)?;
        let mut entry = self.new_entry(st, "compact");
        entry.add.push(new.to_json());
        entry.remove = segs.iter().map(|s| s.key.as_str().to_owned()).collect();
        let next = self.commit(w, st, entry)?;
        let removed: Vec<Key> = segs.iter().map(|s| s.key.clone()).collect();
        lock(&self.dedup).record_compaction(&t.name, p, &removed, &new.key);
        Ok(next)
    }

    /// Deletes segment files that no committed state references: orphans of failed
    /// writes (ours or another writer's) and segments retired by compaction.
    ///
    /// Guarantees (FORMAT.md §3.5): the decision is made against the **log head** (GC
    /// first reads every entry it has not seen, so a lagging engine never mistakes
    /// another writer's segments for orphans), and re-checked against the head again just
    /// before deleting. A file is deleted only after *this engine* has seen it
    /// unreferenced for [`EngineOptions::gc_grace`] (by its own clock — no writer clock
    /// or segment name is trusted), and never while a live snapshot of this process
    /// references it. Writers refuse to commit segments older than half the grace
    /// period, so with a grace period longer than any store-call stall, GC cannot delete
    /// a segment that is about to be committed. Readers in other processes are protected
    /// by the grace period only.
    pub fn gc(&self) -> Result<GcReport> {
        self.gc_with_grace(self.shared.opts.gc_grace)
    }

    /// [`Engine::gc`] with an explicit grace period instead of
    /// [`EngineOptions::gc_grace`].
    pub fn gc_with_grace(&self, grace: std::time::Duration) -> Result<GcReport> {
        self.retry_missing(|| self.gc_once(grace))
    }

    fn gc_once(&self, grace: std::time::Duration) -> Result<GcReport> {
        // A flush writes its segment files before taking the writer lock; until its entry
        // commits they look like orphans. Not running during a flush keeps them safe in
        // this process even without a grace period (other processes are protected by the
        // grace period, FORMAT.md §3.5).
        let _flush = lock(&self.flush_lock);
        let mut w = lock(&self.writer);
        let st = self.head(&mut w)?;
        let referenced = |st: &State| -> BTreeSet<Key> {
            let mut out: BTreeSet<Key> = st.segments().map(|x| x.key.clone()).collect();
            let mut p = self.published();
            p.live.retain(|s| s.strong_count() > 0);
            for s in p.live.iter().filter_map(Weak::upgrade) {
                out.extend(s.segments().map(|x| x.key.clone()));
            }
            out
        };
        let refs = referenced(&st);
        let current: BTreeSet<&Key> = st.segments().map(|s| &s.key).collect();
        let retired: BTreeSet<&Key> = st.retired.iter().map(|(k, _)| k).collect();
        let now = self.wall();
        let grace = i64::try_from(grace.as_micros()).unwrap_or(i64::MAX);
        let listed = self
            .shared
            .store
            .list(log::SEGMENTS_PREFIX)
            .map_err(|e| store_err("listing segments", e))?;
        let listed_set: BTreeSet<&Key> = listed.iter().collect();
        let mut report = GcReport::default();
        let mut candidates = Vec::new();
        {
            let mut since = lock(&self.unreferenced_since);
            since.retain(|k, _| listed_set.contains(k) && !refs.contains(k));
            for key in &listed {
                if refs.contains(key) {
                    if !current.contains(key) {
                        report.kept += 1; // retired, but a live snapshot still reads it
                    }
                    continue;
                }
                // Files whose name we do not recognise are not ours to delete.
                if !retired.contains(key) && log::segment_created(key.as_str()).is_none() {
                    continue;
                }
                let first = *since.entry(key.clone()).or_insert(now);
                if now.saturating_sub(first) < grace {
                    report.kept += 1;
                    continue;
                }
                candidates.push(key.clone());
            }
        }
        // Close the window between deciding and deleting: anything an entry committed
        // meanwhile references is spared.
        let st = if candidates.is_empty() {
            st
        } else {
            let head = self.head(&mut w)?;
            let refs = referenced(&head);
            candidates.retain(|k| !refs.contains(k));
            head
        };
        let mut purged = Vec::new();
        self.shared
            .store
            .delete_many(&candidates)
            .map_err(|e| store_err("deleting unreferenced segments", e))?;
        {
            let retired: BTreeSet<&Key> = st.retired.iter().map(|(k, _)| k).collect();
            let mut since = lock(&self.unreferenced_since);
            for key in candidates {
                since.remove(&key);
                if retired.contains(&key) {
                    purged.push(key.as_str().to_owned());
                }
                report.deleted.push(key);
            }
        }
        // Retired entries whose file is already gone (deleted by an earlier GC that
        // crashed before committing, or by another writer's GC) can be forgotten too.
        let refs = referenced(&st);
        for (k, _) in st.retired.iter() {
            if !listed_set.contains(k)
                && !refs.contains(k)
                && !purged.iter().any(|p| p == k.as_str())
            {
                purged.push(k.as_str().to_owned());
            }
        }
        let mut st = st;
        if !purged.is_empty() {
            let mut entry = self.new_entry(&st, "gc");
            entry.purged = purged;
            match self.commit(&mut w, &st, entry) {
                // Losing the race only delays forgetting the retired keys.
                Err(e) if e.kind() == ErrorKind::Conflict => {}
                r => st = r?,
            }
        }
        report.log_deleted = self.gc_log(&mut w, st, now, grace)?;
        Ok(report)
    }

    /// Log truncation (decision 0007, FORMAT.md §3.6): checkpoints the head if the newest
    /// checkpoint is older, then removes everything only needed to replay to the
    /// *second*-newest checkpoint `T` — entries below `T` (entry 1 is replaced by the
    /// truncation marker, the others deleted) and checkpoints below `T` — once this
    /// engine has seen them truncatable for the grace period. The newest checkpoint must
    /// verify first; the second-newest one stays as a fallback.
    ///
    /// If the entries after the second-newest checkpoint (all of them, if there is only
    /// one) hold more bytes than the newest checkpoint, GC *re-anchors*: it commits an
    /// empty `gc` entry and checkpoints it too, so that the two newest checkpoints are
    /// the old head and the new one and everything before the old head can go. Without
    /// that, a load through the memtable (rows inline in the entries) followed by a flush
    /// kept every entry it wrote until two later checkpoints existed (bench: 125 MB of
    /// log for 8 MB of segments). The cost is one checkpoint, paid only when it removes
    /// more than it writes; a second GC finds the tail small and does nothing.
    fn gc_log(&self, w: &mut Writer, st: Arc<State>, now: i64, grace: i64) -> Result<Vec<Key>> {
        let store = &self.shared.store;
        let listed = store
            .list("log/")
            .map_err(|e| store_err("listing the log", e))?;
        let mut cks: Vec<u64> = listed
            .iter()
            .filter_map(|k| log::parse_checkpoint_key(k.as_str()))
            .filter(|&v| v <= st.version)
            .collect();
        cks.sort_unstable();
        let mut entries: Vec<u64> = listed
            .iter()
            .filter_map(|k| log::parse_entry_key(k.as_str()))
            .collect();
        entries.sort_unstable();
        if st.version > 0 && cks.last() != Some(&st.version) && self.write_checkpoint(&st).is_ok() {
            cks.push(st.version);
        }
        let Some(&verified) = cks.last() else {
            lock(&self.log_truncatable_since).clear();
            return Ok(Vec::new());
        };
        let Some((_, newest_bytes)) = checkpoint_state(&self.shared, verified)? else {
            lock(&self.log_truncatable_since).clear();
            return Ok(Vec::new());
        };
        if verified == st.version {
            let anchor = cks.len().checked_sub(2).map_or(0, |i| cks[i]);
            let mut tail = 0u64;
            for &v in entries.iter().filter(|&&v| v > anchor && v <= st.version) {
                match store.head(&log::entry_key(v)) {
                    Ok(m) => tail += m.size,
                    Err(e) if e.kind() == StoreErrorKind::NotFound => {}
                    Err(e) => return Err(store_err("reading the log", e)),
                }
            }
            if tail > newest_bytes {
                let entry = self.new_entry(&st, "gc");
                match self.commit(w, &st, entry) {
                    // Losing the race only delays the truncation.
                    Err(e) if e.kind() == ErrorKind::Conflict => {}
                    Err(e) => return Err(e),
                    Ok(next) => {
                        entries.push(next.version);
                        if self.write_checkpoint(&next).is_ok() {
                            cks.push(next.version);
                        }
                    }
                }
            }
        }
        let mut since = lock(&self.log_truncatable_since);
        let (Some(&newest), Some(&keep_from)) =
            (cks.last(), cks.len().checked_sub(2).map(|i| &cks[i]))
        else {
            since.clear();
            return Ok(Vec::new());
        };
        if newest != verified && checkpoint_state(&self.shared, newest)?.is_none() {
            since.clear();
            return Ok(Vec::new());
        }
        let candidates: BTreeSet<Key> = entries
            .iter()
            .filter(|&&v| v < keep_from)
            .map(|&v| log::entry_key(v))
            .chain(
                cks.iter()
                    .filter(|&&v| v < keep_from)
                    .map(|&v| log::checkpoint_key(v)),
            )
            .collect();
        // Age every candidate by our own observation (FORMAT.md §3.5 rule 2).
        since.retain(|k, _| candidates.contains(k));
        for k in &candidates {
            since.entry(k.clone()).or_insert(now);
        }
        let ripe = |since: &HashMap<Key, i64>, k: &Key| {
            since
                .get(k)
                .is_some_and(|&first| now.saturating_sub(first) >= grace)
        };
        // Entry 1 becomes the truncation marker (never deleted, so it cannot be created
        // again and a replay from the start fails instead of seeing an empty database);
        // the other ripe entries and checkpoints below `keep_from` are deleted, in any
        // order: readers recognise a truncation by checkpoint `keep_from` and its entry,
        // which stay (`after_head`).
        let one = log::entry_key(1);
        if entries.first() == Some(&1) && keep_from > 1 && ripe(&since, &one) {
            let bytes = store
                .get(&one)
                .map_err(|e| store_err("reading log entry 1", e))?;
            let marked = from_json::<LogEntry>(&bytes, "log entry 1")
                .is_ok_and(|e| e.is_truncation_marker());
            if !marked {
                store
                    .put(&one, &to_json(&LogEntry::truncation_marker(now))?)
                    .map_err(|e| store_err("marking log entry 1", e))?;
            }
        }
        let deleted: Vec<Key> = candidates
            .into_iter()
            .filter(|k| *k != one && ripe(&since, k))
            .collect();
        store
            .delete_many(&deleted)
            .map_err(|e| store_err("truncating the log", e))?;
        for k in &deleted {
            since.remove(k);
        }
        Ok(deleted)
    }
}

/// Reads the state from the log: the newest *valid* checkpoint (the hint's, else the
/// newest listed) plus every later entry. A checkpoint is only an optimisation: one that
/// is missing, unparsable, fails its checksum, does not match the log entry it claims to
/// summarise, or from which the later entries do not apply is skipped in favour of an
/// older one, and finally of a full replay of the log. A log truncated below every valid
/// checkpoint (decision 0007) cannot be replayed and is an error.
fn load<S: Store>(shared: &Shared<S>) -> Result<Loaded> {
    // A GC elsewhere may truncate the log while we read it: start again then.
    let mut last = None;
    for _ in 0..4 {
        match load_once(shared) {
            Err(e) if is_truncated(&e) => last = Some(e),
            r => return r,
        }
    }
    Err(last.expect("at least one attempt"))
}

fn load_once<S: Store>(shared: &Shared<S>) -> Result<Loaded> {
    let store = &shared.store;
    let mut hint: Option<u64> = None;
    match store.get(&log::hint_key()) {
        // An unreadable hint is ignored.
        Ok(bytes) => {
            hint = serde_json::from_slice::<Hint>(&bytes)
                .ok()
                .map(|h| h.version)
        }
        Err(e) if e.kind() == StoreErrorKind::NotFound => {}
        Err(e) => return Err(store_err("reading checkpoint hint", e)),
    }
    let mut tried = BTreeSet::new();
    let mut listed: Option<Vec<u64>> = None;
    loop {
        let next = match hint.take() {
            Some(v) => Some(v),
            None => {
                let all = match &mut listed {
                    Some(l) => l,
                    None => {
                        let keys = store
                            .list("log/")
                            .map_err(|e| store_err("listing the log", e))?;
                        let mut v: Vec<u64> = keys
                            .iter()
                            .filter_map(|k| log::parse_checkpoint_key(k.as_str()))
                            .collect();
                        v.sort_unstable();
                        listed.insert(v)
                    }
                };
                all.retain(|v| !tried.contains(v));
                all.pop()
            }
        };
        let Some(v) = next else { break };
        if !tried.insert(v) {
            continue;
        }
        let Some((state, bytes)) = checkpoint_state(shared, v)? else {
            continue;
        };
        match catch_up(shared, state) {
            Ok(c) => {
                return Ok(Loaded {
                    ck: CkTrack {
                        version: v,
                        bytes,
                        since_entries: c.entries,
                        since_bytes: c.bytes,
                    },
                    state: c.state,
                    at_head: c.at_head,
                });
            }
            Err(e) if log::is_corrupt(&e) => continue,
            Err(e) => return Err(e),
        }
    }
    let c = catch_up(shared, State::empty())?;
    Ok(Loaded {
        ck: CkTrack {
            since_entries: c.entries,
            since_bytes: c.bytes,
            ..CkTrack::default()
        },
        state: c.state,
        at_head: c.at_head,
    })
}

/// Checkpoint `v` (and its size) if it exists and verifies against the log; `Ok(None)` if
/// it should be skipped; `Err` only for store failures.
fn checkpoint_state<S: Store>(shared: &Shared<S>, v: u64) -> Result<Option<(State, u64)>> {
    let store = &shared.store;
    let bytes = match store.get(&log::checkpoint_key(v)) {
        Ok(b) => b,
        Err(e) if e.kind() == StoreErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(store_err("reading checkpoint", e)),
    };
    let Ok(c) = Checkpoint::open(&bytes, v) else {
        return Ok(None);
    };
    let entry_crc = if v == 0 {
        0
    } else {
        match store.get(&log::entry_key(v)) {
            Ok(b) => crc32c::crc32c(&b),
            Err(e) if e.kind() == StoreErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(store_err("reading log entry", e)),
        }
    };
    if entry_crc != c.entry_crc32c {
        return Ok(None);
    }
    Ok(State::from_checkpoint(&c)
        .ok()
        .map(|s| (s, bytes.len() as u64)))
}

/// What [`catch_up`] read.
struct CaughtUp {
    state: State,
    entries: u64,
    bytes: u64,
    /// Just before the read that found the next entry absent.
    at_head: Instant,
}

/// Applies every entry after `state.version` until the first missing one, which must be
/// the head and not a truncation (see [`after_head`]).
fn catch_up<S: Store>(shared: &Shared<S>, mut state: State) -> Result<CaughtUp> {
    let (mut entries, mut bytes) = (0, 0);
    loop {
        let v = state.version + 1;
        let probe = Instant::now();
        let raw = match shared.store.get(&log::entry_key(v)) {
            Ok(b) => b,
            Err(e) if e.kind() == StoreErrorKind::NotFound => match after_head(shared, v)? {
                Beyond::Nothing => {
                    return Ok(CaughtUp {
                        state,
                        entries,
                        bytes,
                        at_head: probe,
                    });
                }
                Beyond::Next => continue, // committed meanwhile
                Beyond::Truncated(why) => return Err(truncated(why)),
            },
            Err(e) => return Err(store_err(&format!("reading log version {v}"), e)),
        };
        let entry: LogEntry = from_json(&raw, &format!("log entry {v}"))?;
        if entry.is_truncation_marker() {
            return Err(truncated(format!(
                "entry {v} is the truncation marker (no valid checkpoint was usable)"
            )));
        }
        state = state.apply_owned(&entry)?;
        entries += 1;
        bytes += raw.len() as u64;
    }
}

/// What lies beyond a missing entry `v` (see [`after_head`]).
enum Beyond {
    /// The log ends before `v`: `v - 1` was the head when we looked.
    Nothing,
    /// Entry `v` exists now (written since we looked).
    Next,
    /// Entry `v` was removed by log truncation (our state is too old).
    Truncated(String),
}

/// Having found entry `v` absent, tells a log that ends there from one that was truncated
/// past it (decision 0007, FORMAT.md §3.6). GC removes entries only below a checkpoint
/// `T` whose entry it keeps, and never removes an entry at or above the newest such `T`
/// — so whenever `v` has been removed, some entry `> v` is present. Listed after `v` was
/// found absent, no entry `> v` can exist if `v` had not been written yet (entries are
/// created in order). Deletions therefore need no order and no individual sync.
fn after_head<S: Store>(shared: &Shared<S>, v: u64) -> Result<Beyond> {
    let listed = shared
        .store
        .list("log/")
        .map_err(|e| store_err("listing the log", e))?;
    let mut beyond = None;
    for k in &listed {
        match log::parse_entry_key(k.as_str()) {
            Some(e) if e == v => return Ok(Beyond::Next),
            Some(e) if e > v => beyond = Some(beyond.map_or(e, |b: u64| b.min(e))),
            _ => {}
        }
    }
    Ok(match beyond {
        Some(e) => Beyond::Truncated(format!("entry {v} is missing but entry {e} exists")),
        None => Beyond::Nothing,
    })
}

/// Proves that `v` is the head now: entry `v + 1` absent and nothing beyond it (see
/// [`after_head`]). `Conflict` if the log moved past `v`.
fn head_probe<S: Store>(shared: &Shared<S>, v: u64) -> Result<()> {
    match shared.store.head(&log::entry_key(v + 1)) {
        Ok(_) => {
            return Err(Error::new(
                ErrorKind::Conflict,
                format!("log version {} exists", v + 1),
            ));
        }
        Err(e) if e.kind() == StoreErrorKind::NotFound => {}
        Err(e) => return Err(store_err(&format!("reading log version {}", v + 1), e)),
    }
    match after_head(shared, v + 1)? {
        Beyond::Nothing => Ok(()),
        Beyond::Next => Err(Error::new(
            ErrorKind::Conflict,
            format!("log version {} exists", v + 1),
        )),
        Beyond::Truncated(why) => Err(truncated(why)),
    }
}

// ---------------------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------------------

macro_rules! read_via_snapshot {
    ($($name:ident($req:ty) -> $out:ty;)*) => {$(
        fn $name(&self, req: $req) -> Result<$out> {
            self.retry_missing(|| self.snapshot()?.$name(req))
        }
    )*};
}

impl<S: Store> Reader for Core<S> {
    fn table(&self, name: &str) -> Result<TableDef> {
        self.snapshot()?.table(name)
    }

    fn tables(&self) -> Result<Vec<TableDef>> {
        self.snapshot()?.tables()
    }

    fn series(&self, name: &str) -> Result<SeriesDef> {
        self.snapshot()?.series(name)
    }

    fn table_series(&self, table: &str) -> Result<Vec<SeriesDef>> {
        self.snapshot()?.table_series(table)
    }

    read_via_snapshot! {
        latest(&LatestRequest) -> ReadResult<VersionRow>;
        history(&HistoryRequest) -> ReadResult<VersionRow>;
        replay(&ReplayRequest) -> ReadResult<VersionRow>;
        grid(&GridRequest) -> ReadResult<GridRow>;
        asof_join(&AsofJoinRequest) -> ReadResult<AsofRow>;
    }
}

impl<S: Store> Core<S> {
    fn catalog_change(
        &self,
        clock: Option<Timestamp>,
        check: impl FnOnce(&State) -> Result<CatalogChangeKind>,
    ) -> Result<Arc<State>> {
        let mut w = lock(&self.writer);
        let st = self.fresh(&mut w)?;
        let change = check(&st)?;
        let mut entry = self.new_entry(&st, "catalog");
        entry
            .catalog
            .push(CatalogChangeJson::from_change(&CatalogChange {
                system_time: catalog_time(st.last_catalog_time(), st.last_system_time, clock),
                change,
            }));
        self.commit(&mut w, &st, entry)
    }
}

impl<S: Store> Database for Core<S> {
    type Snapshot = EngineSnapshot<S>;

    fn snapshot(&self) -> Result<EngineSnapshot<S>> {
        Ok(self.snapshot_of(self.current()?))
    }

    fn create_table(&self, req: &CreateTable) -> Result<TableDef> {
        let st = self.catalog_change(req.clock, |st| {
            st.catalog
                .check_create_table(req)
                .map(CatalogChangeKind::CreateTable)
        })?;
        st.catalog.table(&req.name).cloned()
    }

    fn add_column(&self, req: &AddColumn) -> Result<TableDef> {
        let st = self.catalog_change(req.clock, |st| {
            let column = st.catalog.check_add_column(req)?;
            Ok(CatalogChangeKind::AddColumn {
                table: req.table.clone(),
                column,
            })
        })?;
        st.catalog.table(&req.table).cloned()
    }

    fn create_series(&self, req: &CreateSeries) -> Result<SeriesDef> {
        let st = self.catalog_change(req.clock, |st| {
            st.catalog
                .check_create_series(req)
                .map(CatalogChangeKind::CreateSeries)
        })?;
        st.catalog.series_by_name(&req.name).cloned()
    }

    fn create_series_batch(&self, reqs: &[CreateSeries]) -> Result<Vec<SeriesDef>> {
        if reqs.is_empty() {
            return Ok(Vec::new());
        }
        let mut w = lock(&self.writer);
        let st = self.fresh(&mut w)?;
        // Each request is checked against the catalog including the earlier ones, with
        // the catalog time a sequence of single creations would get.
        let mut catalog = (*st.catalog).clone();
        let mut prev = st.last_catalog_time();
        let mut entry = self.new_entry(&st, "catalog");
        for req in reqs {
            let def = catalog.check_create_series(req)?;
            let change = CatalogChangeKind::CreateSeries(def);
            catalog.apply(&change)?;
            let system_time = catalog_time(prev, st.last_system_time, req.clock);
            prev = Some(system_time);
            entry
                .catalog
                .push(CatalogChangeJson::from_change(&CatalogChange {
                    system_time,
                    change,
                }));
        }
        let st = self.commit(&mut w, &st, entry)?;
        reqs.iter()
            .map(|r| st.catalog.series_by_name(&r.name).cloned())
            .collect()
    }

    fn write(&self, batch: &WriteBatch) -> Result<WriteResult> {
        self.write_one(batch)
    }
}
