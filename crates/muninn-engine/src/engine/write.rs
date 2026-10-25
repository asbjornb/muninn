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
use muninn_core::types::{ContentIndex, SeriesId, TableDef, Version};
use muninn_core::{Error, ErrorKind, Partitioning, Result, TimeRange, Timestamp};
use muninn_format::{Partition, WriteOptions, write_segment};
use muninn_store::Store;

use super::chunked::{Chunked, EarlierRows, InGroup, Layout, Written};
use super::{Core, lock, table_columns};
use crate::fxhash::{FxHashMap, FxHashSet};
use crate::log::inline_key;
use crate::segio::is_missing_segment;
use crate::state::{InlineData, SegmentRef, State};

/// Stored versions a group's rows could duplicate (SPEC §4.3), indexed by content so that
/// a lookup costs O(1) however many versions a point has.
#[derive(Default)]
pub(crate) struct StoredVersions {
    items: Vec<Version>,
    index: ContentIndex,
}

impl StoredVersions {
    pub(crate) fn push(&mut self, v: Version) {
        self.index.push(v.content_hash());
        self.items.push(v);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Is a stored version content-identical to `v` (whose content hash is `hash`)?
    pub(crate) fn contains(&self, v: &Version, hash: u64) -> bool {
        !self.is_empty() && self.index.find(&self.items, v, hash).is_some()
    }
}

/// The rows of a batch waiting for a commit.
pub(crate) enum Rows<'b> {
    /// Validated: the candidate versions in input order.
    Validated(Vec<Version>),
    /// A batch of more than [`crate::EngineOptions::write_chunk_rows`] rows, validated
    /// in chunks by the commit itself (`chunked.rs`); never queued.
    Chunked(&'b WriteBatch),
}

impl Rows<'_> {
    fn len(&self) -> usize {
        match self {
            Rows::Validated(v) => v.len(),
            Rows::Chunked(b) => b.len(),
        }
    }

    fn validated(&self) -> &[Version] {
        match self {
            Rows::Validated(v) => v,
            Rows::Chunked(_) => &[],
        }
    }
}

/// A batch waiting for a commit (header checked, rows validated unless chunked).
pub(crate) struct Request<'b> {
    id: u64,
    clock: Timestamp,
    batch_id: Option<String>,
    rows: Rows<'b>,
    /// The batch failed validation, but an earlier batch of the same `write_many` call
    /// has its batch id: if that one commits, this one is a no-op instead (SPEC §10.4:
    /// the batch-id check comes first), which only the leader can tell.
    rejected: Option<Error>,
}

/// The group-commit queue.
#[derive(Default)]
pub(crate) struct GroupQueue {
    next_id: u64,
    pending: VecDeque<Request<'static>>,
    done: FxHashMap<u64, Result<WriteResult>>,
    /// A caller is committing a group.
    leading: bool,
}

impl GroupQueue {
    /// Takes the next group: at least one request, then more while the group stays
    /// within `max_rows` candidate rows.
    fn take_group(&mut self, max_rows: usize) -> Vec<Request<'static>> {
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
pub(crate) struct Placer<'a> {
    st: &'a State,
    series: Option<(SeriesId, &'a TableDef)>,
    part: Option<(Partitioning, Partition, TimeRange)>,
}

impl<'a> Placer<'a> {
    pub(crate) fn new(st: &'a State) -> Placer<'a> {
        Placer {
            st,
            series: None,
            part: None,
        }
    }

    pub(crate) fn place(&mut self, v: &Version) -> Result<(&'a TableDef, Partition)> {
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
pub(crate) type RtHull = Option<(i64, i64)>;

/// The received-time bounds of the live segments of `table`/`p` in `st`: a stored version
/// content-identical to a row of the partition has the row's received time, so a row
/// outside them cannot be a duplicate.
pub(crate) fn rt_hull(st: &State, table: &str, p: Partition) -> RtHull {
    st.partitions
        .get(&(table.to_owned(), p.index))
        .and_then(|segs| {
            let lo = segs.iter().map(|s| s.received_time.min).min()?;
            let hi = segs.iter().map(|s| s.received_time.max).max()?;
            Some((lo.as_micros(), hi.as_micros()))
        })
}

/// Rows by `(table, partition)`.
type Groups<'a> = BTreeMap<(&'a str, Partition), Vec<Version>>;

/// What the leader decided for one batch before the commit.
enum Outcome {
    /// Final whatever happens to the commit (resolved against the committed state only).
    Final(Result<WriteResult>),
    /// Valid only if the group commits (stores rows, records a batch id, or depends on an
    /// earlier batch of the group).
    IfCommitted(WriteResult),
}

/// How [`Core::apply_group`] ended.
enum GroupResult {
    /// Every request's result.
    Done(Vec<(u64, Result<WriteResult>)>),
    /// A stored segment vanished while a chunked batch read it (compacted and collected
    /// by another process); nothing was committed: reload and try again.
    Retry,
}

/// The segment files of a group commit besides its inline rows.
struct Files<'a> {
    /// Ranges of the kept rows to write as files (one per partition) before the entry.
    bulk: &'a [(u32, u32)],
    /// Files chunked batches have written already.
    written: Vec<Written>,
    /// When the first of `written` was written.
    started: Option<Instant>,
}

impl Request<'_> {
    /// The request as queued (only validated requests are).
    fn detach(self) -> Request<'static> {
        Request {
            id: self.id,
            clock: self.clock,
            batch_id: self.batch_id,
            rows: match self.rows {
                Rows::Validated(v) => Rows::Validated(v),
                Rows::Chunked(_) => unreachable!("chunked batches are not queued"),
            },
            rejected: self.rejected,
        }
    }
}

/// The checks of a call's batches that come before the commit (SPEC §10.4: the header,
/// then the rows), against `st`: the results decided here, and the requests to commit
/// (in order). A batch of more than `chunk` rows (`chunk > 0`) only has its header
/// checked: its rows are validated in chunks by the commit.
#[allow(clippy::type_complexity)]
fn prepare_requests<'b>(
    st: &State,
    batches: &'b [WriteBatch],
    chunk: usize,
) -> (Vec<Option<Result<WriteResult>>>, Vec<(usize, Request<'b>)>) {
    let mut out: Vec<Option<Result<WriteResult>>> = vec![None; batches.len()];
    let mut reqs = Vec::new();
    // Batch ids of the batches of this call queued so far.
    let mut queued_ids: FxHashSet<&str> = FxHashSet::default();
    for (i, b) in batches.iter().enumerate() {
        let is_committed = |id: &str| st.batch_ids.contains(id);
        let checked = if chunk > 0 && b.len() > chunk {
            st.catalog
                .check_batch(b, is_committed)
                .map(|c| c.map(|clock| (clock, Rows::Chunked(b))))
        } else {
            st.catalog.validate_batch(b, is_committed).map(|p| match p {
                BatchPlan::AlreadyCommitted => None,
                BatchPlan::Candidates { clock, rows } => Some((clock, Rows::Validated(rows))),
            })
        };
        match checked {
            Err(e) => match b.batch_id.as_deref() {
                Some(id) if queued_ids.contains(id) => reqs.push((
                    i,
                    Request {
                        id: i as u64,
                        clock: Timestamp::UNIX_EPOCH,
                        batch_id: Some(id.to_owned()),
                        rows: Rows::Validated(Vec::new()),
                        rejected: Some(e),
                    },
                )),
                _ => out[i] = Some(Err(e)),
            },
            Ok(None) => {
                out[i] = Some(Ok(WriteResult {
                    batch_already_committed: true,
                    ..WriteResult::default()
                }));
            }
            Ok(Some((clock, rows))) => {
                if let Some(id) = b.batch_id.as_deref() {
                    queued_ids.insert(id);
                }
                reqs.push((
                    i,
                    Request {
                        id: i as u64,
                        clock,
                        batch_id: b.batch_id.clone(),
                        rows,
                        rejected: None,
                    },
                ));
            }
        }
    }
    (out, reqs)
}

impl<S: Store> Core<S> {
    /// Several batches as one group (each atomic, results in order); see the module docs.
    /// A call with a batch of more than [`crate::EngineOptions::write_chunk_rows`] rows
    /// is committed by the caller itself (the large batch is validated and encoded in
    /// chunks, `chunked.rs`), still as one group: one commit for the whole call.
    pub(crate) fn write_many(&self, batches: &[WriteBatch]) -> Vec<Result<WriteResult>> {
        let chunk = self.shared.opts.write_chunk_rows;
        if chunk > 0 && batches.iter().any(|b| b.len() > chunk) {
            return self.write_many_direct(batches, chunk);
        }
        let st = match self.current_state() {
            Ok(st) => st,
            Err(e) => return batches.iter().map(|_| Err(e.clone())).collect(),
        };
        let (mut out, reqs) = prepare_requests(&st, batches, 0);
        drop(st);
        let mut queued: Vec<(usize, u64)> = Vec::new();
        if !reqs.is_empty() {
            let mut q = lock(&self.queue);
            for (i, req) in reqs {
                let mut req = req.detach();
                req.id = q.next_id;
                q.next_id += 1;
                queued.push((i, req.id));
                q.pending.push_back(req);
            }
        }
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

    /// [`Core::write_many`] for a call with a batch of more than `chunk` rows: the caller
    /// commits its batches as one group under the writer lock (not through the queue:
    /// the large batch is read from the caller's input while it is processed).
    fn write_many_direct(&self, batches: &[WriteBatch], chunk: usize) -> Vec<Result<WriteResult>> {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut w = lock(&self.writer);
            let mut attempt = 0;
            loop {
                let st = self.fresh(&mut w)?;
                let (mut out, reqs) = prepare_requests(&st, batches, chunk);
                let index: Vec<usize> = reqs.iter().map(|(i, _)| *i).collect();
                let group: Vec<Request<'_>> = reqs.into_iter().map(|(_, r)| r).collect();
                let stored = match self
                    .stored_duplicates(&st, group.iter().flat_map(|r| r.rows.validated()))
                {
                    Ok(s) => s,
                    Err(e) if is_missing_segment(&e) && attempt == 0 => {
                        attempt += 1;
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                match self.apply_group(&mut w, &st, stored, group, attempt == 0) {
                    GroupResult::Retry => attempt += 1,
                    GroupResult::Done(rs) => {
                        for (i, (_, r)) in index.into_iter().zip(rs) {
                            out[i] = Some(r);
                        }
                        return Ok(out);
                    }
                }
            }
        }));
        let out = match outcome {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => return batches.iter().map(|_| Err(e.clone())).collect(),
            Err(p) => {
                // Nothing is known about the commit: reload before the next operation.
                self.mark_stale();
                std::panic::resume_unwind(p);
            }
        };
        self.after_write();
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
    fn commit_group(&self, group: Vec<Request<'static>>) -> Vec<(u64, Result<WriteResult>)> {
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
            match self.stored_duplicates(&st, group.iter().flat_map(|r| r.rows.validated())) {
                Ok(stored) => break (st, stored),
                Err(e) if is_missing_segment(&e) && attempt == 0 => attempt += 1,
                Err(e) => return fail_all(e),
            }
        };
        match self.apply_group(&mut w, &st, stored, group, false) {
            GroupResult::Done(rs) => rs,
            GroupResult::Retry => unreachable!("no retry asked for"),
        }
    }

    /// Decides and commits a group against `st` (the writer lock is held): each batch in
    /// order, as if committed one by one. `stored` are the stored versions the validated
    /// rows could duplicate. With `may_retry`, a stored segment that vanished while a
    /// chunked batch read it gives [`GroupResult::Retry`] (nothing was committed).
    fn apply_group<'b>(
        &self,
        w: &mut super::Writer,
        st: &'b State,
        stored: StoredVersions,
        group: Vec<Request<'b>>,
        may_retry: bool,
    ) -> GroupResult {
        let opts = &self.shared.opts;
        let mut next_seq = st.next_seq;
        let mut last = st.last_system_time;
        let mut batch_ids: Vec<String> = Vec::new();
        let mut taken: FxHashSet<String> = FxHashSet::default();
        let mut kept: Vec<Version> = Vec::new();
        // Ranges of `kept` holding the rows of batches too large to keep inline.
        let mut bulk: Vec<(u32, u32)> = Vec::new();
        // The files chunked batches wrote, and when the first was written.
        let mut written: Vec<Written> = Vec::new();
        let mut started: Option<Instant> = None;
        // The rows kept by earlier batches of the group, by content (only built when a
        // later batch could consult it), and the series they belong to: a batch none of
        // whose series an earlier batch kept rows of (the usual case) cannot hold an
        // in-group duplicate and skips the lookups.
        let mut group_index = ContentIndex::default();
        let mut group_series: FxHashSet<SeriesId> = FxHashSet::default();
        // The candidates of earlier chunked batches of the group (their rows are not
        // kept in memory).
        let mut earlier_chunked: Vec<EarlierRows<'b>> = Vec::new();
        let mut remaining = group.len();
        let mut outcomes: Vec<(u64, Outcome)> = Vec::with_capacity(group.len());
        // A chunked batch failed other than by validation: the group fails with it.
        let mut failed: Option<Error> = None;
        let mut unprocessed: Vec<u64> = Vec::new();
        for req in group {
            if failed.is_some() {
                unprocessed.push(req.id);
                continue;
            }
            remaining -= 1;
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
            let rows = match req.rows {
                Rows::Validated(rows) => rows,
                Rows::Chunked(b) => {
                    let system_time = batch_system_time(req.clock, last);
                    if !group_series.is_empty() {
                        for v in &kept[group_index.len()..] {
                            group_index.push(v.content_hash());
                        }
                    }
                    let mut uses_group = false;
                    let check = !group_series.is_empty() || !earlier_chunked.is_empty();
                    let out = {
                        let mut in_group = |v: &Version, hash: u64| -> Result<bool> {
                            let mut hit = group_series.contains(&v.series)
                                && group_index.find(&kept, v, hash).is_some();
                            for e in &mut earlier_chunked {
                                if hit {
                                    break;
                                }
                                hit = e.contains(v, hash)?;
                            }
                            uses_group |= hit;
                            Ok(hit)
                        };
                        let in_group: InGroup<'_> = if check { Some(&mut in_group) } else { None };
                        self.chunked_batch(st, b, system_time.clone(), next_seq, in_group)
                    };
                    let c = match out {
                        Chunked::Rejected(e) => {
                            outcomes.push((req.id, Outcome::Final(Err(e))));
                            continue;
                        }
                        Chunked::Failed(e) => {
                            if may_retry && is_missing_segment(&e) {
                                return GroupResult::Retry;
                            }
                            outcomes.push((req.id, Outcome::IfCommitted(WriteResult::default())));
                            failed = Some(e);
                            continue;
                        }
                        Chunked::Applied(c) => c,
                    };
                    let result = WriteResult {
                        stored: c.stored,
                        duplicates: c.duplicates,
                        batch_already_committed: false,
                        system_time: system_time.as_ref().ok().copied().filter(|_| c.stored > 0),
                        first_seq: (c.stored > 0).then_some(next_seq),
                    };
                    if c.stored == 0 && req.batch_id.is_none() {
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
                    if c.stored > 0 {
                        next_seq += c.stored;
                        last = system_time.ok();
                    }
                    match c.layout {
                        Layout::Inline(rows) => kept.extend(rows),
                        Layout::Files(files, at) => {
                            written.extend(files);
                            started = Some(started.map_or(at, |s| s.min(at)));
                        }
                    }
                    if remaining > 0 {
                        earlier_chunked.push(c.earlier);
                    }
                    outcomes.push((req.id, Outcome::IfCommitted(result)));
                    continue;
                }
            };
            let system_time = match batch_system_time(req.clock, last) {
                Ok(t) => t,
                Err(e) => {
                    outcomes.push((req.id, Outcome::Final(Err(e))));
                    continue;
                }
            };
            let check_group = !group_series.is_empty() && {
                let mut prev = None;
                rows.iter().any(|v| {
                    let new = prev != Some(v.series);
                    prev = Some(v.series);
                    new && group_series.contains(&v.series)
                })
            };
            let mut uses_group = false;
            let plan = plan_dedup(rows, system_time, next_seq, |v| {
                if stored.is_empty() && !check_group && earlier_chunked.is_empty() {
                    return Ok(false);
                }
                let hash = v.content_hash();
                if stored.contains(v, hash) {
                    return Ok(true);
                }
                let mut in_group = check_group && group_index.find(&kept, v, hash).is_some();
                for e in &mut earlier_chunked {
                    if in_group {
                        break;
                    }
                    in_group = e.contains(v, hash)?;
                }
                uses_group |= in_group;
                Ok(in_group)
            });
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
            if plan.kept.len() > opts.inline_max_rows {
                let a = kept.len() as u32;
                bulk.push((a, a + plan.kept.len() as u32));
            }
            if remaining > 0 {
                // Later batches of the group may duplicate these rows.
                let mut prev = None;
                for v in &plan.kept {
                    if prev != Some(v.series) {
                        prev = Some(v.series);
                        group_series.insert(v.series);
                    }
                }
                // Index every kept row so far (`kept` and the index stay aligned).
                for v in &kept[group_index.len()..] {
                    group_index.push(v.content_hash());
                }
                for v in &plan.kept {
                    group_index.push(v.content_hash());
                }
            }
            if kept.is_empty() {
                kept = plan.kept; // no copy (a bulk write is one big batch)
            } else {
                kept.extend(plan.kept);
            }
            outcomes.push((req.id, Outcome::IfCommitted(result)));
        }
        drop(group_index);
        drop(earlier_chunked);

        let committed = if let Some(e) = failed {
            Err(e)
        } else if kept.is_empty() && batch_ids.is_empty() && written.is_empty() {
            Ok(())
        } else {
            let files = Files {
                bulk: &bulk,
                written,
                started,
            };
            self.commit_rows(w, st, kept, files, batch_ids, next_seq, last)
        };
        let mut out: Vec<(u64, Result<WriteResult>)> = outcomes
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
            .collect();
        if let Err(e) = &committed {
            out.extend(unprocessed.into_iter().map(|id| (id, Err(e.clone()))));
        }
        GroupResult::Done(out)
    }

    /// The stored versions content-identical candidates `rows` could duplicate, indexed by
    /// content: only rows whose received time the dedup index cannot rule out are looked
    /// up (re-sends, out-of-order backfills).
    pub(crate) fn stored_duplicates<'v>(
        &self,
        st: &State,
        rows: impl IntoIterator<Item = &'v Version>,
    ) -> Result<StoredVersions> {
        // A stored version content-identical to a row has the row's received time and
        // lies in the row's partition. So a row whose received time is outside the
        // received-time bounds of its partition's live segments (log statistics, no I/O)
        // cannot be a duplicate: fresh data (a new run, a correction, received now) stops
        // here. Only the other rows consult the dedup index (per point), and the rows it
        // cannot rule out are compared with stored versions.
        let mut hulls: FxHashMap<(&str, i64), RtHull> = FxHashMap::default();
        let mut placer = Placer::new(st);
        let mut cur: Option<((&str, Partition), RtHull)> = None;
        let mut maybe: Vec<(&Version, &str, Partition)> = Vec::new();
        for v in rows {
            let (t, p) = placer.place(v)?;
            let key = (t.name.as_str(), p);
            let hull = match cur {
                Some((k, h)) if k == key => h,
                _ => {
                    let h = *hulls
                        .entry((key.0, p.index))
                        .or_insert_with(|| rt_hull(st, &t.name, p));
                    cur = Some((key, h));
                    h
                }
            };
            let rt = v.received_time.as_micros();
            if hull.is_some_and(|(lo, hi)| lo <= rt && rt <= hi) {
                maybe.push((v, key.0, p));
            }
        }
        self.stored_of(st, &maybe)
    }

    /// [`Core::stored_duplicates`] for the rows that the received-time bounds of their
    /// partition do not rule out, with their table and partition.
    pub(crate) fn stored_of(
        &self,
        st: &State,
        maybe: &[(&Version, &str, Partition)],
    ) -> Result<StoredVersions> {
        if maybe.is_empty() {
            return Ok(StoredVersions::default());
        }
        let mut touched: BTreeSet<(String, Partition)> = BTreeSet::new();
        let mut prev = None;
        for &(_, t, p) in maybe {
            if prev != Some((t, p)) {
                prev = Some((t, p));
                touched.insert((t.to_owned(), p));
            }
        }
        let mut dedup = lock(&self.dedup);
        dedup.prepare(&self.shared, st, &touched)?;
        let mut points: BTreeMap<String, FxHashMap<SeriesId, BTreeSet<Timestamp>>> =
            BTreeMap::new();
        let mut rts: FxHashSet<(SeriesId, Timestamp, Timestamp)> = FxHashSet::default();
        for &(v, table, p) in maybe {
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
            return Ok(StoredVersions::default());
        }
        self.load_points(st, &points, &rts)
    }

    /// Commits the kept rows of a group (and its batch ids) as one `write` entry: the rows
    /// of batches with at most [`crate::EngineOptions::inline_max_rows`] rows as inline
    /// segments, one per touched partition; the rows of larger batches (`files.bulk`,
    /// ranges of `kept`) as segment files written first (in parallel), one per partition;
    /// and the files chunked batches already wrote (`files.written`).
    #[allow(clippy::too_many_arguments)]
    fn commit_rows(
        &self,
        w: &mut super::Writer,
        st: &State,
        kept: Vec<Version>,
        files: Files<'_>,
        batch_ids: Vec<String>,
        next_seq: u64,
        last: Option<Timestamp>,
    ) -> Result<()> {
        let mut inline: Groups<'_> = BTreeMap::new();
        let mut bulk_files: Groups<'_> = BTreeMap::new();
        let mut inline_rows = 0;
        let mut placer = Placer::new(st);
        let mut bulk_at = files.bulk.iter().peekable();
        // Two passes, so that every group's vector is allocated once at its final size
        // (a bulk write moves a million versions; growing the vectors copied them over
        // and over): the group of each row, then the moves.
        type Key<'a> = (bool, &'a str, Partition);
        let mut keys: Vec<Key<'_>> = Vec::new();
        let mut index: FxHashMap<Key<'_>, u32> = FxHashMap::default();
        let mut counts: Vec<usize> = Vec::new();
        let mut of_row: Vec<u32> = Vec::with_capacity(kept.len());
        let mut prev: Option<(Key<'_>, u32)> = None;
        for (i, v) in kept.iter().enumerate() {
            let i = i as u32;
            while bulk_at.peek().is_some_and(|r| r.1 <= i) {
                bulk_at.next();
            }
            let is_bulk = bulk_at.peek().is_some_and(|r| r.0 <= i);
            if !is_bulk {
                inline_rows += 1;
            }
            let (t, p) = placer.place(v)?;
            let key = (is_bulk, t.name.as_str(), p);
            let k = match prev {
                Some((lk, k)) if lk == key => k,
                _ => *index.entry(key).or_insert_with(|| {
                    keys.push(key);
                    counts.push(0);
                    (keys.len() - 1) as u32
                }),
            };
            prev = Some((key, k));
            counts[k as usize] += 1;
            of_row.push(k);
        }
        let mut groups: Vec<Vec<Version>> = counts.iter().map(|&n| Vec::with_capacity(n)).collect();
        for (v, k) in kept.into_iter().zip(of_row) {
            groups[k as usize].push(v);
        }
        for ((is_bulk, t, p), vs) in keys.into_iter().zip(groups) {
            let to = if is_bulk {
                &mut bulk_files
            } else {
                &mut inline
            };
            to.insert((t, p), vs);
        }
        let mut entry = self.new_entry(st, "write");
        entry.batch_ids = batch_ids;
        entry.next_seq = next_seq;
        entry.last_system_time = last.map(Timestamp::as_micros);
        let started = files.started.unwrap_or_else(Instant::now);
        // Files first (they must be durable before the entry), then inline segments,
        // keyed by their position in `add`.
        let mut segs: Vec<SegmentRef> = if bulk_files.is_empty() {
            Vec::new()
        } else {
            let jobs: Vec<_> = bulk_files
                .iter()
                .map(|((table, p), vs)| Ok((st.catalog.table(table)?, *p, vs.as_slice())))
                .collect::<Result<_>>()?;
            self.put_segments(&jobs)?
        };
        let n_bulk = segs.len();
        segs.extend(files.written.iter().map(|f| f.3.clone()));
        let any_files = !segs.is_empty();
        let opts = inline_options(&self.shared.opts.segment);
        let created = self.wall();
        let (version, offset) = (entry.version, segs.len());
        let jobs: Vec<_> = inline.iter().collect();
        // Encoding is CPU work: spread big groups over threads.
        let threads = if inline_rows >= 2048 {
            self.shared.opts.io_threads
        } else {
            1
        };
        let encoded: Vec<Result<SegmentRef>> =
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
                    inline_key(version, offset + i),
                    &t.name,
                    &footer,
                    size,
                    created,
                    Some(Arc::new(d)),
                ))
            });
        for s in encoded {
            segs.push(s?);
        }
        entry.add = segs.iter().map(SegmentRef::to_json).collect();
        if any_files {
            self.check_commit_age(started)?;
        }
        self.commit_prepared(w, st, entry, &segs)?;
        self.wrote.store(true, std::sync::atomic::Ordering::Relaxed);
        let mut dedup = lock(&self.dedup);
        let n_written = files.written.len();
        let rows_of = bulk_files
            .iter()
            .zip(&segs[..n_bulk])
            .chain(inline.iter().zip(&segs[n_bulk + n_written..]));
        for (((table, p), vs), seg) in rows_of {
            dedup.record(
                table,
                *p,
                &seg.key,
                vs.iter().map(|v| (v.series, v.value_time, v.received_time)),
            );
        }
        for (_, table, p, seg, ranges) in &files.written {
            if let Some(r) = ranges {
                dedup.record_ranges(table, *p, &seg.key, r.iter().copied());
            }
        }
        Ok(())
    }

    /// After a write returned: start a flush if the memtable reached a threshold — in
    /// the background thread if there is one, else here — and flush here, waiting for
    /// any running flush, when it exceeds the hard limit (back-pressure).
    pub(crate) fn after_write(&self) {
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
