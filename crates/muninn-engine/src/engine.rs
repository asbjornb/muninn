//! [`Engine`]: open/recovery, commits, writes, catalog changes, compaction and GC.

use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use muninn_core::api::{
    AddColumn, AsofJoinRequest, AsofRow, CreateSeries, CreateTable, Database, GridRequest, GridRow,
    HistoryRequest, LatestRequest, ReadResult, Reader, ReplayRequest, VersionRow, WriteBatch,
    WriteResult,
};
use muninn_core::catalog::{
    BatchPlan, CatalogChange, CatalogChangeKind, batch_system_time, catalog_time, plan_dedup,
};
use muninn_core::types::{SeriesDef, SeriesId, TableDef, Version};
use muninn_core::{Error, ErrorKind, Result, TimeRange, Timestamp};
use muninn_format::{Partition, SegmentFile, ValueColumn, write_segment};
use muninn_store::{Key, PutOutcome, Store, StoreErrorKind};

use crate::EngineOptions;
use crate::log::{
    self, CatalogChangeJson, Checkpoint, Hint, LOG_FORMAT, LogEntry, from_json, to_json,
};
use crate::read::{EngineSnapshot, ScanSpec, Scanner};
use crate::segio::{Shared, store_err};
use crate::state::{SegmentRef, State};

mod restore;

/// What [`Engine::gc`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Segment files deleted.
    pub deleted: Vec<Key>,
    /// Unreferenced files kept because they are younger than the grace period or a live
    /// snapshot of this process still uses them.
    pub kept: usize,
}

/// The current state plus every snapshot handed out (for GC), under one lock so that a
/// snapshot is registered atomically with reading the current state.
struct Published {
    state: Arc<State>,
    live: Vec<Weak<State>>,
}

/// The writer lock, held for the duration of every mutating operation.
struct Writer;

/// The Muninn storage engine over a [`Store`] (decisions 0004, 0005).
///
/// One `Engine` is one writer; any number of threads may read through it or through
/// snapshots concurrently. All methods take `&self`.
pub struct Engine<S: Store> {
    shared: Arc<Shared<S>>,
    published: Mutex<Published>,
    writer: Mutex<Writer>,
    /// Set when a commit failed in a way that leaves it unknown whether the log moved
    /// (ambiguous store error, or another writer won the version). The next operation
    /// reloads from the log before doing anything else.
    stale: AtomicBool,
    nonce: RandomState,
    counter: AtomicU64,
}

impl<S: Store> std::fmt::Debug for Engine<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("version", &self.published().state.version)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section either completes or leaves the protected value untouched
    // (states are swapped whole), so a poisoned lock is safe to keep using.
    m.lock().unwrap_or_else(PoisonError::into_inner)
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

impl<S: Store> Engine<S> {
    /// Opens the database on `store`, creating an empty one if the log is empty.
    /// Recovery: latest checkpoint (found via the hint, else by listing `log/`) plus every
    /// later entry. Segment files no entry references are ignored.
    pub fn open(store: S, opts: EngineOptions) -> Result<Engine<S>> {
        let shared = Arc::new(Shared::new(store, opts));
        let state = load(&shared)?;
        Ok(Engine {
            shared,
            published: Mutex::new(Published {
                state: Arc::new(state),
                live: Vec::new(),
            }),
            writer: Mutex::new(Writer),
            stale: AtomicBool::new(false),
            nonce: RandomState::new(),
            counter: AtomicU64::new(0),
        })
    }

    /// The underlying store.
    pub fn store(&self) -> &S {
        &self.shared.store
    }

    /// The options.
    pub fn options(&self) -> &EngineOptions {
        &self.shared.opts
    }

    fn published(&self) -> MutexGuard<'_, Published> {
        lock(&self.published)
    }

    /// The current log version.
    pub fn version(&self) -> u64 {
        self.published().state.version
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

    /// Keys of every segment the current state references.
    pub fn live_segment_keys(&self) -> BTreeSet<Key> {
        let st = Arc::clone(&self.published().state);
        st.segments().map(|s| s.key.clone()).collect()
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
        if !self.stale.load(Ordering::SeqCst) {
            return Ok(cur);
        }
        let next = Arc::new(catch_up(&self.shared, (*cur).clone())?);
        self.publish(Arc::clone(&next));
        self.stale.store(false, Ordering::SeqCst);
        Ok(next)
    }

    /// Picks up entries committed by other writers (or by an ambiguous failure of ours).
    pub fn refresh(&self) -> Result<u64> {
        let mut w = lock(&self.writer);
        self.stale.store(true, Ordering::SeqCst);
        Ok(self.fresh(&mut w)?.version)
    }

    /// The current state (catching up first if a commit failed).
    fn current(&self) -> Result<Arc<State>> {
        if self.stale.load(Ordering::SeqCst) {
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
    fn commit(&self, _w: &mut Writer, base: &State, entry: LogEntry) -> Result<Arc<State>> {
        let next = Arc::new(base.apply(&entry)?);
        let bytes = to_json(&entry)?;
        match self
            .shared
            .store
            .put_if_absent(&log::entry_key(entry.version), &bytes)
        {
            Ok(PutOutcome::Created) => {}
            Ok(PutOutcome::AlreadyExists) => {
                self.stale.store(true, Ordering::SeqCst);
                return Err(Error::new(
                    ErrorKind::Conflict,
                    format!(
                        "log version {} was committed by another writer; reload and retry",
                        entry.version
                    ),
                ));
            }
            Err(e) => {
                self.stale.store(true, Ordering::SeqCst);
                return Err(store_err(
                    &format!("committing log version {}", entry.version),
                    e,
                ));
            }
        }
        self.publish(Arc::clone(&next));
        let every = self.shared.opts.checkpoint_interval;
        if every > 0 && next.version % every == 0 {
            // An optimisation only: failures are ignored (recovery replays more entries).
            let _ = self.write_checkpoint(&next);
        }
        Ok(next)
    }

    fn write_checkpoint(&self, st: &State) -> Result<()> {
        let bytes = to_json(&st.to_checkpoint())?;
        let store = &self.shared.store;
        store
            .put_if_absent(&log::checkpoint_key(st.version), &bytes)
            .map_err(|e| store_err("writing checkpoint", e))?;
        store
            .put(
                &log::hint_key(),
                &to_json(&Hint {
                    version: st.version,
                })?,
            )
            .map_err(|e| store_err("writing checkpoint hint", e))
    }

    /// Writes a checkpoint of the current state now (normally automatic every
    /// [`EngineOptions::checkpoint_interval`] versions).
    pub fn checkpoint(&self) -> Result<()> {
        let mut w = lock(&self.writer);
        let st = self.fresh(&mut w)?;
        self.write_checkpoint(&st)
    }

    // -----------------------------------------------------------------------------------
    // Segments
    // -----------------------------------------------------------------------------------

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
                    let st = &footer.stats;
                    let seg = SegmentRef {
                        key: key.clone(),
                        table: table.name.clone(),
                        partition,
                        size: bytes.len() as u64,
                        rows: footer.row_count,
                        value_time: st.value_time,
                        received_time: st.received_time,
                        published_time: st.published_time,
                        system_time: st.system_time,
                        seq: (st.seq_min, st.seq_max),
                        series: (
                            footer.series.first().expect("non-empty").id,
                            footer.series.last().expect("non-empty").id,
                        ),
                        created,
                    };
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

    /// Every stored version of the given points (for dedup), keyed by point.
    fn load_points(
        &self,
        st: &State,
        points: &BTreeMap<String, HashMap<SeriesId, BTreeSet<Timestamp>>>,
    ) -> Result<HashMap<(SeriesId, Timestamp), Vec<Version>>> {
        let mut out: HashMap<(SeriesId, Timestamp), Vec<Version>> = HashMap::new();
        for (table, pts) in points {
            let table = st.catalog.table(table)?;
            let ids: BTreeSet<SeriesId> = pts.keys().copied().collect();
            let lo = pts.values().filter_map(|s| s.first()).min().copied();
            let hi = pts.values().filter_map(|s| s.last()).max().copied();
            let range = TimeRange::new(
                lo,
                hi.and_then(|t| Timestamp::from_micros(t.as_micros().saturating_add(1))),
            );
            let mut scanner = Scanner::new(&self.shared, st);
            let cands = scanner.scan(&ScanSpec {
                table,
                ids: &ids,
                range,
                clock: muninn_core::Clock::System,
                tc_lo: i64::MIN,
                tc_hi: i64::MAX,
                system: None,
                points: Some(pts),
            })?;
            for v in scanner.materialize(table.columns.len(), &cands)? {
                out.entry((v.series, v.value_time)).or_default().push(v);
            }
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------------------
    // Compaction and GC
    // -----------------------------------------------------------------------------------

    /// Merges every segment of one partition into one (no-op with fewer than two).
    /// Returns whether anything was compacted. Never changes a query result (SPEC §7.2):
    /// the new segment holds exactly the same versions.
    pub fn compact(&self, table: &str, partition_label: &str) -> Result<bool> {
        let mut w = lock(&self.writer);
        let st = self.fresh(&mut w)?;
        let t = st.catalog.table(table)?.clone();
        let p = Partition::parse_label(t.partition, partition_label)
            .map_err(|e| Error::new(ErrorKind::InvalidRange, e.message().to_owned()))?;
        self.compact_locked(&mut w, &st, &t, p)
    }

    /// Compacts every partition that has more than one segment; returns how many were
    /// compacted.
    pub fn compact_all(&self) -> Result<usize> {
        let mut w = lock(&self.writer);
        let mut n = 0;
        let targets: Vec<(String, i64)> = {
            let st = self.fresh(&mut w)?;
            st.partitions
                .iter()
                .filter(|(_, v)| v.len() > 1)
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
            if self.compact_locked(&mut w, &st, &t, p)? {
                n += 1;
            }
        }
        Ok(n)
    }

    fn compact_locked(
        &self,
        w: &mut Writer,
        st: &Arc<State>,
        t: &TableDef,
        p: Partition,
    ) -> Result<bool> {
        let Some(segs) = st.partitions.get(&(t.name.clone(), p.index)) else {
            return Ok(false);
        };
        if segs.len() < 2 {
            return Ok(false);
        }
        let mut versions = Vec::new();
        for seg in segs.iter() {
            let bytes = self
                .shared
                .store
                .get(&seg.key)
                .map_err(|e| store_err(&format!("reading {}", seg.key), e))?;
            let file = SegmentFile::open(&bytes).map_err(|e| {
                Error::new(ErrorKind::Io, format!("corrupt segment {}: {e}", seg.key))
            })?;
            versions.extend(file.versions().map_err(|e| {
                Error::new(ErrorKind::Io, format!("corrupt segment {}: {e}", seg.key))
            })?);
        }
        let new = self.put_segment(t, p, &versions)?;
        let mut entry = self.new_entry(st, "compact");
        entry.add.push(new.to_json());
        entry.remove = segs.iter().map(|s| s.key.as_str().to_owned()).collect();
        self.commit(w, st, entry)?;
        Ok(true)
    }

    /// Deletes segment files that no committed state references: orphans of failed
    /// writes, and segments retired by compaction. A file is only deleted if it is older
    /// than [`EngineOptions::gc_grace`] (by its creation time, or for retired segments its
    /// removal time) and no live snapshot of this process references it. Readers in other
    /// processes are protected by the grace period only.
    pub fn gc(&self) -> Result<GcReport> {
        let mut w = lock(&self.writer);
        let st = self.fresh(&mut w)?;
        let mut referenced: BTreeSet<Key> = BTreeSet::new();
        {
            let mut p = self.published();
            p.live.retain(|s| s.strong_count() > 0);
            let live: Vec<Arc<State>> = p.live.iter().filter_map(Weak::upgrade).collect();
            for s in live.iter().chain([&p.state, &st]) {
                referenced.extend(s.segments().map(|x| x.key.clone()));
            }
        }
        let retired: HashMap<&Key, i64> = st.retired.iter().map(|(k, t)| (k, *t)).collect();
        let now = self.wall();
        let grace = i64::try_from(self.shared.opts.gc_grace.as_micros()).unwrap_or(i64::MAX);
        let mut report = GcReport::default();
        let mut purged = Vec::new();
        let listed = self
            .shared
            .store
            .list(log::SEGMENTS_PREFIX)
            .map_err(|e| store_err("listing segments", e))?;
        let listed_set: BTreeSet<&Key> = listed.iter().collect();
        let current: BTreeSet<&Key> = st.segments().map(|s| &s.key).collect();
        for key in &listed {
            if referenced.contains(key) {
                if !current.contains(key) {
                    report.kept += 1; // retired, but a live snapshot still reads it
                }
                continue;
            }
            let since = match retired.get(key) {
                Some(&t) => Some(t),
                None => log::segment_created(key.as_str()),
            };
            // Files whose name we do not recognise are not ours to delete.
            let Some(since) = since else { continue };
            if now.saturating_sub(since) < grace {
                report.kept += 1;
                continue;
            }
            self.shared
                .store
                .delete(key)
                .map_err(|e| store_err(&format!("deleting {key}"), e))?;
            if retired.contains_key(key) {
                purged.push(key.as_str().to_owned());
            }
            report.deleted.push(key.clone());
        }
        // Retired entries whose file is already gone (e.g. deleted by an earlier GC that
        // crashed before committing) can be forgotten too.
        for (k, t) in st.retired.iter() {
            if !listed_set.contains(k)
                && !referenced.contains(k)
                && now.saturating_sub(*t) >= grace
                && !purged.iter().any(|p| p == k.as_str())
            {
                purged.push(k.as_str().to_owned());
            }
        }
        if !purged.is_empty() {
            let mut entry = self.new_entry(&st, "gc");
            entry.purged = purged;
            self.commit(&mut w, &st, entry)?;
        }
        Ok(report)
    }
}

/// Reads the state from the log: checkpoint (via hint, else listing) + later entries.
fn load<S: Store>(shared: &Shared<S>) -> Result<State> {
    let store = &shared.store;
    let mut start: Option<u64> = None;
    match store.get(&log::hint_key()) {
        Ok(bytes) => {
            // The hint is an optimisation: if it is unreadable, fall back to listing.
            if let Ok(h) = serde_json::from_slice::<Hint>(&bytes) {
                start = Some(h.version);
            }
        }
        Err(e) if e.kind() == StoreErrorKind::NotFound => {}
        Err(e) => return Err(store_err("reading checkpoint hint", e)),
    }
    let mut state = None;
    if let Some(v) = start {
        match store.get(&log::checkpoint_key(v)) {
            Ok(bytes) => state = Some(checkpoint_state(&bytes, v)?),
            Err(e) if e.kind() == StoreErrorKind::NotFound => {}
            Err(e) => return Err(store_err("reading checkpoint", e)),
        }
    }
    if state.is_none() {
        let keys = store
            .list("log/")
            .map_err(|e| store_err("listing the log", e))?;
        if let Some(v) = keys
            .iter()
            .filter_map(|k| log::parse_checkpoint_key(k.as_str()))
            .max()
        {
            let bytes = store
                .get(&log::checkpoint_key(v))
                .map_err(|e| store_err("reading checkpoint", e))?;
            state = Some(checkpoint_state(&bytes, v)?);
        }
    }
    catch_up(shared, state.unwrap_or_else(State::empty))
}

fn checkpoint_state(bytes: &[u8], v: u64) -> Result<State> {
    let c: Checkpoint = from_json(bytes, "checkpoint")?;
    if c.version != v {
        return Err(log::corrupt(format!(
            "checkpoint {v} says it is version {}",
            c.version
        )));
    }
    State::from_checkpoint(&c)
}

/// Applies every entry after `state.version` until the first missing one.
fn catch_up<S: Store>(shared: &Shared<S>, mut state: State) -> Result<State> {
    loop {
        let v = state.version + 1;
        let bytes = match shared.store.get(&log::entry_key(v)) {
            Ok(b) => b,
            Err(e) if e.kind() == StoreErrorKind::NotFound => return Ok(state),
            Err(e) => return Err(store_err(&format!("reading log version {v}"), e)),
        };
        let entry: LogEntry = from_json(&bytes, &format!("log entry {v}"))?;
        state = state.apply(&entry)?;
    }
}

// ---------------------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------------------

macro_rules! read_via_snapshot {
    ($($name:ident($req:ty) -> $out:ty;)*) => {$(
        fn $name(&self, req: $req) -> Result<$out> {
            self.snapshot()?.$name(req)
        }
    )*};
}

impl<S: Store> Reader for Engine<S> {
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

impl<S: Store> Engine<S> {
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

impl<S: Store> Database for Engine<S> {
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

    fn write(&self, batch: &WriteBatch) -> Result<WriteResult> {
        let mut w = lock(&self.writer);
        let st = self.fresh(&mut w)?;
        // SPEC §4.4, §4.1 (shared with the model).
        let (clock, candidates) = match st
            .catalog
            .validate_batch(batch, |id| st.batch_ids.contains(id))?
        {
            BatchPlan::AlreadyCommitted => {
                return Ok(WriteResult {
                    batch_already_committed: true,
                    ..WriteResult::default()
                });
            }
            BatchPlan::Candidates { clock, rows } => (clock, rows),
        };
        // SPEC §4.3 against stored versions: read every stored version of the touched
        // points (pruned by partition, series and value time).
        let mut points: BTreeMap<String, HashMap<SeriesId, BTreeSet<Timestamp>>> = BTreeMap::new();
        for v in &candidates {
            let s = st
                .catalog
                .series_by_id(v.series)
                .ok_or_else(|| Error::internal("validated row of unknown series"))?;
            points
                .entry(s.table.clone())
                .or_default()
                .entry(v.series)
                .or_default()
                .insert(v.value_time);
        }
        let stored = self.load_points(&st, &points)?;
        let system_time = batch_system_time(clock, st.last_system_time);
        let plan = plan_dedup(candidates, system_time, st.next_seq, |v| {
            Ok(stored
                .get(&(v.series, v.value_time))
                .is_some_and(|old| old.iter().any(|o| o.content_identical(v))))
        })?;
        let result = plan.result();
        if plan.kept.is_empty() && batch.batch_id.is_none() {
            return Ok(result);
        }
        // One segment per touched (table, partition).
        let mut groups: BTreeMap<(String, Partition), Vec<Version>> = BTreeMap::new();
        for v in &plan.kept {
            let s = st
                .catalog
                .series_by_id(v.series)
                .ok_or_else(|| Error::internal("kept row of unknown series"))?;
            let t = st.catalog.table(&s.table)?;
            groups
                .entry((t.name.clone(), Partition::of(t.partition, v.value_time)))
                .or_default()
                .push(v.clone());
        }
        let mut entry = self.new_entry(&st, "write");
        entry.batch_id = batch.batch_id.clone();
        for ((table, partition), versions) in &groups {
            let t = st.catalog.table(table)?;
            entry
                .add
                .push(self.put_segment(t, *partition, versions)?.to_json());
        }
        if !plan.kept.is_empty() {
            entry.next_seq = st.next_seq + plan.kept.len() as u64;
            entry.last_system_time = Some(system_time.as_micros());
        }
        self.commit(&mut w, &st, entry)?;
        Ok(result)
    }
}
