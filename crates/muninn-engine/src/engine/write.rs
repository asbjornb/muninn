//! The write path (decision 0009): validation in the caller's thread, group commit of
//! everything queued into one log entry, rows stored inline in the entry (the memtable)
//! or, for large groups, as segment files.
//!
//! A caller validates its batches (SPEC §4.1, §10.4: structure, batch-id no-op, writer
//! clock, rows) against the state it sees, then queues them. Whoever finds no leader
//! becomes the leader: it takes a group off the queue, commits it under the writer lock,
//! posts every batch's result, and hands leadership on. Validation against an older
//! catalog stays valid (catalog changes only add tables, columns and series), and an
//! error has no effect, so it is linearised at validation time.
//!
//! The leader treats the group's batches as if they were committed one by one, in queue
//! order: the batch-id check sees the committed ids and those of earlier batches of the
//! group; deduplication (SPEC §4.3) compares with stored versions (segment files and
//! memtable, via the dedup index) and with the rows kept by earlier batches; each batch
//! that stores rows takes `system_time = max(c, last + 1µs)` and the next seqs. The whole
//! group is one entry, so each batch is atomic (and so is the group). A call returns only
//! once the entry holding its batch is durable; if the commit fails, every batch of the
//! group gets the error.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use muninn_core::api::{WriteBatch, WriteResult};
use muninn_core::catalog::{BatchPlan, batch_system_time, plan_dedup};
use muninn_core::types::{SeriesId, TableDef, Version};
use muninn_core::{Error, ErrorKind, Partitioning, Result, TimeRange, Timestamp};
use muninn_format::{Partition, WriteOptions, write_segment};
use muninn_store::Store;

use super::{Core, lock, table_columns};
use crate::fxhash::{FxHashMap, FxHashSet};
use crate::log::inline_key;
use crate::segio::is_missing_segment;
use crate::state::{InlineData, SegmentRef, State};

/// A validated batch waiting for a commit.
pub(crate) struct Request {
    id: u64,
    clock: Timestamp,
    batch_id: Option<String>,
    rows: Vec<Version>,
    /// The batch failed validation, but an earlier batch of the same `write_many` call
    /// has its batch id: if that one commits, this one is a no-op instead (SPEC §10.4:
    /// the batch-id check comes first), which only the leader can tell.
    rejected: Option<Error>,
}

/// The group-commit queue.
#[derive(Default)]
pub(crate) struct GroupQueue {
    next_id: u64,
    pending: VecDeque<Request>,
    done: FxHashMap<u64, Result<WriteResult>>,
    /// A caller is committing a group.
    leading: bool,
}

impl GroupQueue {
    /// Takes the next group: at least one request, then more while the group stays
    /// within `max_rows` candidate rows.
    fn take_group(&mut self, max_rows: usize) -> Vec<Request> {
        let mut out = Vec::new();
        let mut rows = 0;
        while let Some(r) = self.pending.front() {
            if !out.is_empty() && rows + r.rows.len() > max_rows {
                break;
            }
            rows += r.rows.len();
            out.push(self.pending.pop_front().expect("front exists"));
        }
        out
    }
}

/// Segment writer settings for inline segments: no compression (they are written once,
/// kept in memory and read often until flushed).
fn inline_options(base: &WriteOptions) -> WriteOptions {
    WriteOptions {
        compress: false,
        ..*base
    }
}

/// The table and partition of rows, caching the last series and partition (rows come in
/// runs of one series and partition).
struct Placer<'a> {
    st: &'a State,
    series: Option<(SeriesId, &'a TableDef)>,
    part: Option<(Partitioning, Partition, TimeRange)>,
}

impl<'a> Placer<'a> {
    fn new(st: &'a State) -> Placer<'a> {
        Placer {
            st,
            series: None,
            part: None,
        }
    }

    fn place(&mut self, v: &Version) -> Result<(&'a TableDef, Partition)> {
        let t = match self.series {
            Some((id, t)) if id == v.series => t,
            _ => {
                let s = self
                    .st
                    .catalog
                    .series_by_id(v.series)
                    .ok_or_else(|| Error::internal("validated row of unknown series"))?;
                let t = self.st.catalog.table(&s.table)?;
                self.series = Some((v.series, t));
                t
            }
        };
        let p = match self.part {
            Some((g, p, r)) if g == t.partition && r.contains(v.value_time) => p,
            _ => {
                let p = Partition::of(t.partition, v.value_time);
                self.part = Some((t.partition, p, p.range()));
                p
            }
        };
        Ok((t, p))
    }
}

/// `[min, max]` received time (µs) of a partition's live segments, `None` if it has none.
type RtHull = Option<(i64, i64)>;

/// What the leader decided for one batch before the commit.
enum Outcome {
    /// Final whatever happens to the commit (resolved against the committed state only).
    Final(Result<WriteResult>),
    /// Valid only if the group commits (stores rows, records a batch id, or depends on an
    /// earlier batch of the group).
    IfCommitted(WriteResult),
}

impl<S: Store> Core<S> {
    /// Several batches as one group (each atomic, results in order); see the module docs.
    pub(crate) fn write_many(&self, batches: &[WriteBatch]) -> Vec<Result<WriteResult>> {
        let st = match self.current_state() {
            Ok(st) => st,
            Err(e) => return batches.iter().map(|_| Err(e.clone())).collect(),
        };
        let mut out: Vec<Option<Result<WriteResult>>> = vec![None; batches.len()];
        let mut queued: Vec<(usize, u64)> = Vec::new();
        {
            let mut reqs = Vec::new();
            // Batch ids of the batches of this call queued so far.
            let mut queued_ids: FxHashSet<&str> = FxHashSet::default();
            for (i, b) in batches.iter().enumerate() {
                match st.catalog.validate_batch(b, |id| st.batch_ids.contains(id)) {
                    Err(e) => match b.batch_id.as_deref() {
                        Some(id) if queued_ids.contains(id) => {
                            reqs.push((
                                i,
                                Timestamp::UNIX_EPOCH,
                                Some(id.to_owned()),
                                Vec::new(),
                                Some(e),
                            ));
                        }
                        _ => out[i] = Some(Err(e)),
                    },
                    Ok(BatchPlan::AlreadyCommitted) => {
                        out[i] = Some(Ok(WriteResult {
                            batch_already_committed: true,
                            ..WriteResult::default()
                        }));
                    }
                    Ok(BatchPlan::Candidates { clock, rows }) => {
                        if let Some(id) = b.batch_id.as_deref() {
                            queued_ids.insert(id);
                        }
                        reqs.push((i, clock, b.batch_id.clone(), rows, None));
                    }
                }
            }
            if !reqs.is_empty() {
                let mut q = lock(&self.queue);
                for (i, clock, batch_id, rows, rejected) in reqs {
                    let id = q.next_id;
                    q.next_id += 1;
                    q.pending.push_back(Request {
                        id,
                        clock,
                        batch_id,
                        rows,
                        rejected,
                    });
                    queued.push((i, id));
                }
            }
        }
        drop(st);
        if !queued.is_empty() {
            for (i, r) in queued.iter().map(|&(i, _)| i).zip(self.wait_for(&queued)) {
                out[i] = Some(r);
            }
            self.after_write();
        }
        out.into_iter()
            .map(|r| r.expect("every batch has a result"))
            .collect()
    }

    /// Waits for the results of `ids` (in order), leading groups whenever nobody else
    /// does.
    fn wait_for(&self, ids: &[(usize, u64)]) -> Vec<Result<WriteResult>> {
        let mut results: Vec<Option<Result<WriteResult>>> = vec![None; ids.len()];
        let mut missing = ids.len();
        let mut q = lock(&self.queue);
        loop {
            for (slot, &(_, id)) in results.iter_mut().zip(ids) {
                if slot.is_none()
                    && let Some(r) = q.done.remove(&id)
                {
                    *slot = Some(r);
                    missing -= 1;
                }
            }
            if missing == 0 {
                break;
            }
            if q.leading {
                q = self
                    .queue_cv
                    .wait(q)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                continue;
            }
            q.leading = true;
            let group = q.take_group(self.shared.opts.group_max_rows.max(1));
            drop(q);
            let ids_of: Vec<u64> = group.iter().map(|r| r.id).collect();
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.commit_group(group)));
            q = lock(&self.queue);
            q.leading = false;
            let panic = match outcome {
                Ok(rs) => {
                    q.done.extend(rs);
                    None
                }
                Err(p) => {
                    // Nothing is known about the commit: report `io` (unknown outcome)
                    // and reload before the next operation.
                    self.mark_stale();
                    for id in ids_of {
                        q.done.insert(
                            id,
                            Err(Error::new(
                                ErrorKind::Io,
                                "the commit panicked; outcome unknown",
                            )),
                        );
                    }
                    Some(p)
                }
            };
            self.queue_cv.notify_all();
            if let Some(p) = panic {
                drop(q);
                std::panic::resume_unwind(p);
            }
        }
        results.into_iter().map(|r| r.expect("collected")).collect()
    }

    /// Commits one group; returns every request's result.
    fn commit_group(&self, group: Vec<Request>) -> Vec<(u64, Result<WriteResult>)> {
        let ids: Vec<u64> = group.iter().map(|r| r.id).collect();
        let fail_all = |e: Error| ids.iter().map(|&id| (id, Err(e.clone()))).collect();
        let mut w = lock(&self.writer);
        // Dedup lookups read stored versions; if a segment vanished (compacted and
        // collected by another process), reload and try once more.
        let mut attempt = 0;
        let (st, stored) = loop {
            let st = match self.fresh(&mut w) {
                Ok(st) => st,
                Err(e) => return fail_all(e),
            };
            match self.stored_duplicates(&st, &group) {
                Ok(stored) => break (st, stored),
                Err(e) if is_missing_segment(&e) && attempt == 0 => attempt += 1,
                Err(e) => return fail_all(e),
            }
        };

        // Each batch in queue order, as if committed one by one.
        let mut next_seq = st.next_seq;
        let mut last = st.last_system_time;
        let mut batch_ids: Vec<String> = Vec::new();
        let mut taken: FxHashSet<String> = FxHashSet::default();
        let mut kept: Vec<Version> = Vec::new();
        // Ranges of `kept` holding rows of each series. Only a series that an earlier
        // batch of the group kept rows of can make a row an in-group duplicate; batches
        // of different series (the usual case) never build a per-point index.
        let mut series_runs: FxHashMap<SeriesId, Vec<(u32, u32)>> = FxHashMap::default();
        let mut outcomes: Vec<(u64, Outcome)> = Vec::with_capacity(group.len());
        for req in group {
            if let Some(id) = &req.batch_id
                && (st.batch_ids.contains(id) || taken.contains(id))
            {
                let r = WriteResult {
                    batch_already_committed: true,
                    ..WriteResult::default()
                };
                outcomes.push((
                    req.id,
                    if taken.contains(id) {
                        Outcome::IfCommitted(r)
                    } else {
                        Outcome::Final(Ok(r))
                    },
                ));
                continue;
            }
            if let Some(e) = req.rejected {
                outcomes.push((req.id, Outcome::Final(Err(e))));
                continue;
            }
            let system_time = match batch_system_time(req.clock, last) {
                Ok(t) => t,
                Err(e) => {
                    outcomes.push((req.id, Outcome::Final(Err(e))));
                    continue;
                }
            };
            let mut earlier: FxHashMap<(SeriesId, Timestamp), Vec<u32>> = FxHashMap::default();
            if !series_runs.is_empty() {
                let mut done: FxHashSet<SeriesId> = FxHashSet::default();
                let mut prev = None;
                for v in &req.rows {
                    if prev == Some(v.series) || !done.insert(v.series) {
                        continue;
                    }
                    prev = Some(v.series);
                    for &(a, b) in series_runs.get(&v.series).into_iter().flatten() {
                        for i in a..b {
                            let k = &kept[i as usize];
                            earlier.entry((k.series, k.value_time)).or_default().push(i);
                        }
                    }
                }
            }
            let mut uses_group = false;
            let plan = plan_dedup(req.rows, system_time, next_seq, |v| {
                let point = (v.series, v.value_time);
                if !stored.is_empty()
                    && stored
                        .get(&point)
                        .is_some_and(|old| old.iter().any(|o| o.content_identical(v)))
                {
                    return Ok(true);
                }
                let in_group = !earlier.is_empty()
                    && earlier.get(&point).is_some_and(|ix| {
                        ix.iter().any(|&i| kept[i as usize].content_identical(v))
                    });
                uses_group |= in_group;
                Ok(in_group)
            });
            drop(earlier);
            let plan = match plan {
                Ok(p) => p,
                Err(e) => {
                    outcomes.push((req.id, Outcome::Final(Err(e))));
                    continue;
                }
            };
            let result = plan.result();
            if plan.kept.is_empty() && req.batch_id.is_none() {
                outcomes.push((
                    req.id,
                    if uses_group {
                        Outcome::IfCommitted(result)
                    } else {
                        Outcome::Final(Ok(result))
                    },
                ));
                continue;
            }
            if let Some(id) = req.batch_id {
                taken.insert(id.clone());
                batch_ids.push(id);
            }
            if !plan.kept.is_empty() {
                next_seq += plan.kept.len() as u64;
                last = Some(system_time);
            }
            for v in plan.kept {
                let i = kept.len() as u32;
                let runs = series_runs.entry(v.series).or_default();
                match runs.last_mut() {
                    Some(r) if r.1 == i => r.1 = i + 1,
                    _ => runs.push((i, i + 1)),
                }
                kept.push(v);
            }
            outcomes.push((req.id, Outcome::IfCommitted(result)));
        }
        drop(series_runs);

        let committed = if kept.is_empty() && batch_ids.is_empty() {
            Ok(())
        } else {
            self.commit_rows(&mut w, &st, kept, batch_ids, next_seq, last)
        };
        outcomes
            .into_iter()
            .map(|(id, o)| {
                (
                    id,
                    match (o, &committed) {
                        (Outcome::Final(r), _) => r,
                        (Outcome::IfCommitted(r), Ok(())) => Ok(r),
                        (Outcome::IfCommitted(_), Err(e)) => Err(e.clone()),
                    },
                )
            })
            .collect()
    }

    /// The stored versions content-identical candidates of `group` could duplicate,
    /// keyed by point: only rows whose received time the dedup index cannot rule out are
    /// looked up (re-sends, out-of-order backfills).
    fn stored_duplicates(
        &self,
        st: &State,
        group: &[Request],
    ) -> Result<FxHashMap<(SeriesId, Timestamp), Vec<Version>>> {
        // A stored version content-identical to a row has the row's received time and
        // lies in the row's partition. So a row whose received time is outside the
        // received-time bounds of its partition's live segments (log statistics, no I/O)
        // cannot be a duplicate: fresh data (a new run, a correction, received now) stops
        // here. Only the other rows consult the dedup index (per point), and the rows it
        // cannot rule out are compared with stored versions.
        let mut hulls: FxHashMap<(&str, i64), RtHull> = FxHashMap::default();
        let mut placer = Placer::new(st);
        let mut cur: Option<((&str, Partition), RtHull)> = None;
        let mut touched: BTreeSet<(String, Partition)> = BTreeSet::new();
        let mut maybe: Vec<(&Version, &str, Partition)> = Vec::new();
        for v in group.iter().flat_map(|r| &r.rows) {
            let (t, p) = placer.place(v)?;
            let key = (t.name.as_str(), p);
            let hull = match cur {
                Some((k, h)) if k == key => h,
                _ => {
                    let h = *hulls.entry((key.0, p.index)).or_insert_with(|| {
                        st.partitions
                            .get(&(t.name.clone(), p.index))
                            .and_then(|segs| {
                                let lo = segs.iter().map(|s| s.received_time.min).min()?;
                                let hi = segs.iter().map(|s| s.received_time.max).max()?;
                                Some((lo.as_micros(), hi.as_micros()))
                            })
                    });
                    cur = Some((key, h));
                    h
                }
            };
            let rt = v.received_time.as_micros();
            if hull.is_some_and(|(lo, hi)| lo <= rt && rt <= hi) {
                if maybe.last().is_none_or(|m| (m.1, m.2) != key) {
                    touched.insert((t.name.clone(), p));
                }
                maybe.push((v, key.0, p));
            }
        }
        if maybe.is_empty() {
            return Ok(FxHashMap::default());
        }
        let mut dedup = lock(&self.dedup);
        dedup.prepare(&self.shared, st, &touched)?;
        let mut points: BTreeMap<String, FxHashMap<SeriesId, BTreeSet<Timestamp>>> =
            BTreeMap::new();
        let mut rts: FxHashSet<(SeriesId, Timestamp, Timestamp)> = FxHashSet::default();
        for (v, table, p) in maybe {
            if dedup.may_have(table, p, v.series, v.value_time, v.received_time) {
                points
                    .entry(table.to_owned())
                    .or_default()
                    .entry(v.series)
                    .or_default()
                    .insert(v.value_time);
                rts.insert((v.series, v.value_time, v.received_time));
            }
        }
        drop(dedup);
        if points.is_empty() {
            return Ok(FxHashMap::default());
        }
        self.load_points(st, &points, &rts)
    }

    /// Commits the kept rows of a group (and its batch ids) as one `write` entry: inline
    /// segments, one per touched partition, unless there are more than
    /// [`crate::EngineOptions::inline_max_rows`] rows, in which case segment files are
    /// written first (in parallel).
    fn commit_rows(
        &self,
        w: &mut super::Writer,
        st: &State,
        kept: Vec<Version>,
        batch_ids: Vec<String>,
        next_seq: u64,
        last: Option<Timestamp>,
    ) -> Result<()> {
        let mut groups: BTreeMap<(&str, Partition), Vec<Version>> = BTreeMap::new();
        let rows = kept.len();
        let mut placer = Placer::new(st);
        let mut last_key: Option<(&str, Partition)> = None;
        let mut run: Vec<Version> = Vec::new();
        for v in kept {
            let (t, p) = placer.place(&v)?;
            let key = (t.name.as_str(), p);
            if last_key != Some(key) {
                if let Some(k) = last_key {
                    groups.entry(k).or_default().append(&mut run);
                }
                last_key = Some(key);
            }
            run.push(v);
        }
        if let Some(k) = last_key {
            groups.entry(k).or_default().append(&mut run);
        }
        let mut entry = self.new_entry(st, "write");
        entry.batch_ids = batch_ids;
        entry.next_seq = next_seq;
        entry.last_system_time = last.map(Timestamp::as_micros);
        let inline = rows <= self.shared.opts.inline_max_rows;
        let started = Instant::now();
        let segs: Vec<SegmentRef> = if inline {
            let opts = inline_options(&self.shared.opts.segment);
            let created = self.wall();
            let version = entry.version;
            let jobs: Vec<_> = groups.iter().collect();
            // Encoding is CPU work: spread big groups over threads.
            let threads = if rows >= 2048 {
                self.shared.opts.io_threads
            } else {
                1
            };
            super::parallel_map(&jobs, threads, |i, ((table, p), vs)| {
                let t = st.catalog.table(table)?;
                let bytes = write_segment(&t.name, *p, &table_columns(t), vs, &opts)
                    .map_err(|e| Error::internal(format!("encoding segment: {e}")))?;
                let size = bytes.len() as u64;
                let d = InlineData::open(bytes).map_err(|e| {
                    Error::internal(format!("inline segment we just wrote is invalid: {e}"))
                })?;
                let footer = Arc::clone(&d.footer);
                Ok(SegmentRef::from_footer(
                    inline_key(version, i),
                    &t.name,
                    &footer,
                    size,
                    created,
                    Some(Arc::new(d)),
                ))
            })
            .into_iter()
            .collect::<Result<_>>()?
        } else {
            let jobs: Vec<_> = groups
                .iter()
                .map(|((table, p), vs)| Ok((st.catalog.table(table)?, *p, vs.as_slice())))
                .collect::<Result<_>>()?;
            self.put_segments(&jobs)?
        };
        entry.add = segs.iter().map(SegmentRef::to_json).collect();
        if !inline {
            self.check_commit_age(started)?;
        }
        self.commit_prepared(w, st, entry, &segs)?;
        let mut dedup = lock(&self.dedup);
        for (((table, p), vs), seg) in groups.iter().zip(&segs) {
            dedup.record(
                table,
                *p,
                &seg.key,
                vs.iter().map(|v| (v.series, v.value_time, v.received_time)),
            );
        }
        Ok(())
    }

    /// After a write returned: start a flush if the memtable reached a threshold — in
    /// the background thread if there is one, else here — and flush here, waiting for
    /// any running flush, when it exceeds the hard limit (back-pressure).
    fn after_write(&self) {
        let opts = &self.shared.opts;
        let Ok(st) = self.current_state() else {
            return;
        };
        if st.memtable.is_empty() {
            return;
        }
        let rows = st.memtable_rows();
        drop(st);
        if rows > opts.memtable_limit_rows {
            let _ = self.flush();
            return;
        }
        if self.flush_due(self.has_flusher()) {
            if self.has_flusher() {
                self.kick_flusher();
            } else {
                let _ = self.try_flush();
            }
        }
    }
}

impl<S: Store> Core<S> {
    /// A single batch (the [`muninn_core::Database::write`] of the engine).
    pub(crate) fn write_one(&self, batch: &WriteBatch) -> Result<WriteResult> {
        self.write_many(std::slice::from_ref(batch))
            .pop()
            .expect("one result per batch")
    }
}
