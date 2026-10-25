//! Large writes in chunks: a batch of more than [`crate::EngineOptions::write_chunk_rows`]
//! rows is validated, deduplicated and encoded chunk by chunk, inside its group commit.
//!
//! Processing a million rows at once is memory-bound (a million candidate versions with
//! their value vectors, a million dedup keys to sort, partitions of ~100k rows to sort
//! and encode, the whole batch to free at the end). In chunks, the per-row intermediates
//! of a chunk are released once its rows are encoded into segment files; only the files
//! (written to the store at once, like a bulk write's files, decision 0009) and their
//! records are held until the commit. The write is still **one** commit — the group's
//! log entry — with one `system_time` and one contiguous seq range in input order, and
//! every result is that of processing the batch whole:
//!
//! * **Validation** (SPEC §4.1, §10.4): chunks are validated in input order, so the first
//!   invalid row of the batch decides, with its index in the batch; any error rejects
//!   the batch and nothing of it is committed. A batch that fails for another reason
//!   (reading stored versions, writing a file) is first validated to its end: an
//!   invalid row reports its validation error, exactly as when validation preceded
//!   everything. The same goes for an exhausted `system_time` (SPEC §4.2).
//! * **Deduplication** (SPEC §4.3): a row is a duplicate iff it is content-identical to a
//!   stored version or to an *earlier candidate* of its batch or a kept row of an earlier
//!   batch of the group — the same as "an earlier kept row", since content identity is an
//!   equivalence and a dropped row is identical to what it was dropped for. So each
//!   row's decision depends on the input alone, not on other decisions: within its chunk
//!   by sorting content hashes (as for a whole batch), against stored versions through
//!   the dedup index (as for a whole batch, against the same state), and against earlier
//!   chunks through a [`ContentIndex`] over their candidates ([`EarlierRows`]). That index
//!   is built lazily: earlier candidates are recorded as runs of rows sharing `(series,
//!   received_time)` — a content-identical row has the same pair — and the rows of a pair
//!   are hashed (validated again from the input) only when a later row has that pair,
//!   which fresh data (later received times) never has. Later batches of the group
//!   consult the same structure.
//! * **Seqs**: kept rows take consecutive seqs in input order, chunk after chunk.
//! * **Layout**: rows go inline (the memtable) if the batch keeps at most
//!   [`crate::EngineOptions::inline_max_rows`] rows, else to segment files: kept rows are
//!   buffered until they exceed that, then every chunk writes one file per touched
//!   partition. Query results do not depend on the layout (SPEC §7.2).
//!
//! Parallelism (threads, no async), in three stages. *Preparing* a chunk (up to
//! `max(2, io_threads / 2)` chunks ahead, each on its own thread) validates it, hashes
//! its rows, finds its in-chunk duplicates, the rows its partitions' stored received-time
//! bounds do not rule out, its `(series, received_time)` runs, and splits it by
//! partition. The *leader* (the calling thread, holding the writer lock) takes the
//! chunks in order and makes the decisions that depend on earlier ones: stored versions
//! (through the dedup index), earlier chunks, earlier batches of the group; then the seq
//! of every kept row follows from a prefix count. *Encoders* (`max(1, io_threads / 2)`
//! threads) drop the duplicates of a partition's rows, assign seqs and `system_time`,
//! and encode and write the file. The bytes of every file depend only on its chunk's
//! rows, never on the thread count or timing: the same input gives byte-identical
//! segments, listed in the entry in input order.

use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use muninn_core::api::WriteBatch;
use muninn_core::catalog::{BatchRows, distinct_points, in_batch_duplicates};
use muninn_core::types::{ContentIndex, SeriesId, TableDef, Version};
use muninn_core::{Error, Result, Timestamp};
use muninn_format::Partition;
use muninn_store::Store;

use super::write::{Placer, rt_hull};
use super::{Core, lock};
use crate::fxhash::FxHashMap;
use crate::state::{SegmentRef, State};

/// The rows of one `(table, partition)` of a chunk, with their positions in the chunk.
struct Part<'s> {
    table: &'s TableDef,
    partition: Partition,
    rows: Vec<Version>,
    pos: Vec<u32>,
}

/// A prepared chunk (see the module docs); per-position vectors are indexed by the
/// row's position in the chunk.
struct Prepared<'s> {
    /// By `(table, partition)`.
    parts: Vec<Part<'s>>,
    /// Position → (part, index in the part).
    at: Vec<(u32, u32)>,
    hashes: Vec<u64>,
    /// Content-identical to an earlier row of the chunk.
    dup: Vec<bool>,
    /// Positions of rows within the stored received-time bounds of their partition.
    maybe: Vec<u32>,
    /// Runs `[start, end)` of positions sharing `(series, received_time µs)`.
    runs: Vec<(SeriesId, i64, u32, u32)>,
}

fn prepare<'s>(st: &'s State, input: &BatchRows<'_>, range: Range<usize>) -> Result<Prepared<'s>> {
    let vs = input.validate(range)?;
    let n = vs.len();
    let hashes: Vec<u64> = vs.iter().map(Version::content_hash).collect();
    let dup = if distinct_points(&vs) {
        vec![false; n]
    } else {
        in_batch_duplicates(&vs, &hashes)
    };
    let mut runs = Vec::new();
    let mut i = 0;
    while i < n {
        let key = (vs[i].series, vs[i].received_time.as_micros());
        let mut j = i + 1;
        while j < n && (vs[j].series, vs[j].received_time.as_micros()) == key {
            j += 1;
        }
        runs.push((key.0, key.1, i as u32, j as u32));
        i = j;
    }
    // The part of each row (rows come in runs of one partition), parts in key order.
    let mut placer = Placer::new(st);
    let mut index: FxHashMap<(&'s str, Partition), u32> = FxHashMap::default();
    let mut found: Vec<(&'s TableDef, Partition, usize)> = Vec::new();
    let mut of_row: Vec<u32> = Vec::with_capacity(n);
    let mut prev: Option<((&'s str, Partition), u32)> = None;
    for v in &vs {
        let (t, p) = placer.place(v)?;
        let key = (t.name.as_str(), p);
        let g = match prev {
            Some((k, g)) if k == key => g,
            _ => *index.entry(key).or_insert_with(|| {
                found.push((t, p, 0));
                (found.len() - 1) as u32
            }),
        };
        prev = Some((key, g));
        found[g as usize].2 += 1;
        of_row.push(g);
    }
    let mut order: Vec<u32> = (0..found.len() as u32).collect();
    order.sort_by(|&a, &b| {
        let (ta, pa, _) = found[a as usize];
        let (tb, pb, _) = found[b as usize];
        (ta.name.as_str(), pa).cmp(&(tb.name.as_str(), pb))
    });
    let mut rank = vec![0u32; found.len()];
    for (r, &g) in order.iter().enumerate() {
        rank[g as usize] = r as u32;
    }
    let mut parts: Vec<Part<'s>> = order
        .iter()
        .map(|&g| {
            let (table, partition, count) = found[g as usize];
            Part {
                table,
                partition,
                rows: Vec::with_capacity(count),
                pos: Vec::with_capacity(count),
            }
        })
        .collect();
    let hulls: Vec<Option<(i64, i64)>> = parts
        .iter()
        .map(|p| rt_hull(st, &p.table.name, p.partition))
        .collect();
    let mut at = Vec::with_capacity(n);
    let mut maybe = Vec::new();
    for (i, (v, g)) in vs.into_iter().zip(of_row).enumerate() {
        let g = rank[g as usize] as usize;
        let rt = v.received_time.as_micros();
        if hulls[g].is_some_and(|(lo, hi)| lo <= rt && rt <= hi) {
            maybe.push(i as u32);
        }
        let part = &mut parts[g];
        at.push((g as u32, part.rows.len() as u32));
        part.rows.push(v);
        part.pos.push(i as u32);
    }
    Ok(Prepared {
        parts,
        at,
        hashes,
        dup,
        maybe,
        runs,
    })
}

/// Candidates of a chunked batch recorded so far (its earlier chunks; once it is done,
/// all of it, for the later batches of its group), for duplicate checks without keeping
/// its rows (see the module docs).
pub(crate) struct EarlierRows<'a> {
    input: BatchRows<'a>,
    pairs: FxHashMap<(SeriesId, i64), Pair>,
}

/// The recorded candidates with one `(series, received_time)`.
#[derive(Default)]
struct Pair {
    /// Runs `[start, end)` of batch row indices.
    runs: Vec<(u32, u32)>,
    /// `runs[..indexed]` are in `index`/`ids`.
    indexed: usize,
    index: ContentIndex,
    /// Item `k` of `index` is batch row `ids[k]`.
    ids: Vec<u32>,
}

impl<'a> EarlierRows<'a> {
    fn new(input: BatchRows<'a>) -> EarlierRows<'a> {
        EarlierRows {
            input,
            pairs: FxHashMap::default(),
        }
    }

    /// Records the runs of a chunk starting at batch row `base` as candidates.
    fn add(&mut self, base: usize, runs: &[(SeriesId, i64, u32, u32)]) {
        let base = base as u32;
        for &(s, rt, a, b) in runs {
            self.pairs
                .entry((s, rt))
                .or_default()
                .runs
                .push((base + a, base + b));
        }
    }

    /// May a recorded candidate have this `(series, received_time µs)`?
    fn has_pair(&self, series: SeriesId, rt: i64) -> bool {
        self.pairs.contains_key(&(series, rt))
    }

    /// Is a recorded candidate content-identical to `v` (hash `hash`)?
    pub(crate) fn contains(&mut self, v: &Version, hash: u64) -> Result<bool> {
        let Some(p) = self.pairs.get_mut(&(v.series, v.received_time.as_micros())) else {
            return Ok(false);
        };
        let input = &self.input;
        while p.indexed < p.runs.len() {
            let (a, b) = p.runs[p.indexed];
            for (j, w) in (a..b).zip(input.validate(a as usize..b as usize)?) {
                p.index.push(w.content_hash());
                p.ids.push(j);
            }
            p.indexed += 1;
        }
        let mut err = None;
        let found = p
            .index
            .find_with(hash, |k| match input.row(p.ids[k] as usize) {
                Ok(w) => w.content_identical(v),
                Err(e) => {
                    err = Some(e);
                    false
                }
            })
            .is_some();
        match err {
            Some(e) => Err(e),
            None => Ok(found),
        }
    }
}

/// How an encoder turns a part's candidates into stored rows: drop the duplicates,
/// assign seqs (`seq_base` + the number of kept rows before the position) and the
/// `system_time`.
struct Finish {
    pos: Vec<u32>,
    dup: Arc<Vec<bool>>,
    before: Arc<Vec<u32>>,
    seq_base: u64,
    system_time: Timestamp,
}

impl Finish {
    fn apply(self, rows: Vec<Version>) -> Vec<Version> {
        let mut out = Vec::with_capacity(rows.len());
        for (mut v, p) in rows.into_iter().zip(self.pos) {
            let p = p as usize;
            if self.dup[p] {
                continue;
            }
            v.seq = self.seq_base + u64::from(self.before[p]);
            v.system_time = self.system_time;
            out.push(v);
        }
        out
    }
}

/// Rows of one partition to be written as a segment file.
struct Job<'a> {
    /// Position among the batch's files.
    no: usize,
    table: &'a TableDef,
    partition: Partition,
    rows: Vec<Version>,
    /// `None`: the rows are final.
    finish: Option<Finish>,
    /// The partition is in the dedup index: also return the rows' per-point received
    /// time ranges, to fold in once the entry is committed.
    ranges: bool,
}

/// Per-point `(series, value_time, min rt, max rt)` (µs) of a written file's rows.
pub(crate) type Ranges = Vec<(SeriesId, i64, i64, i64)>;

/// A written segment file: (position, table, partition, record, ranges if the partition
/// is indexed for dedup).
pub(crate) type Written = (usize, String, Partition, SegmentRef, Option<Ranges>);

/// Where a chunked batch's kept rows went.
pub(crate) enum Layout {
    /// At most `inline_max_rows` rows, to be stored inline with the group's.
    Inline(Vec<Version>),
    /// Segment files (in `add` order), the first written at the instant given.
    Files(Vec<Written>, Instant),
}

/// What a chunked batch did.
pub(crate) struct ChunkedRows<'a> {
    pub(crate) stored: u64,
    pub(crate) duplicates: u64,
    pub(crate) layout: Layout,
    /// Its candidates, for the later batches of the group.
    pub(crate) earlier: EarlierRows<'a>,
}

/// The outcome of [`Core::chunked_batch`].
pub(crate) enum Chunked<'a> {
    /// Rejected: an invalid row (the first), else an exhausted `system_time`. The batch
    /// has no effect; the group goes on.
    Rejected(Error),
    /// Failed other than by validation (the batch is valid): the group fails with it.
    Failed(Error),
    /// Deduplicated and stored (files) or ready to store (inline).
    Applied(ChunkedRows<'a>),
}

/// What the leader is doing with the chunks.
enum Phase {
    /// Kept rows are buffered: inline if the batch keeps at most `inline_max_rows`.
    Buffer(Vec<Version>),
    /// Kept rows go to segment files, chunk by chunk.
    Files,
    /// Something other than a row failed: the rest of the batch is only validated (an
    /// invalid row's error wins), then this error is returned (`true`: as a rejection).
    Failed(Error, bool),
}

/// Answers whether a row duplicates a row kept by an earlier batch of the group.
pub(crate) type InGroup<'f> = Option<&'f mut dyn FnMut(&Version, u64) -> Result<bool>>;

impl<S: Store> Core<S> {
    /// Processes a batch of more than `write_chunk_rows` rows in chunks, inside a group
    /// commit against `st` (the writer lock is held; see the module docs). Its kept rows
    /// take `system_time` and seqs from `first_seq`; `in_group(v, hash)`, if given,
    /// answers whether `v` duplicates a row kept by an earlier batch of the group.
    pub(crate) fn chunked_batch<'a>(
        &self,
        st: &'a State,
        batch: &'a WriteBatch,
        system_time: Result<Timestamp>,
        first_seq: u64,
        mut in_group: InGroup<'_>,
    ) -> Chunked<'a> {
        let opts = &self.shared.opts;
        let input = BatchRows::new(&st.catalog, batch);
        let n = input.len();
        let chunk = opts.write_chunk_rows.max(1);
        let chunks = n.div_ceil(chunk);
        let range = |c: usize| c * chunk..((c + 1) * chunk).min(n);
        let threads = opts.io_threads.max(1);
        let lookahead = (threads / 2).max(2);
        let encoders = (threads / 2).max(1);

        let (mut phase, system_time) = match system_time {
            Ok(t) => (Phase::Buffer(Vec::new()), t),
            Err(e) => (Phase::Failed(e, true), Timestamp::MIN),
        };
        let mut next_seq = first_seq;
        let mut duplicates = 0u64;
        let mut earlier = EarlierRows::new(BatchRows::new(&st.catalog, batch));
        let mut jobs_sent = 0usize;
        let mut started: Option<Instant> = None;
        let cancel = AtomicBool::new(false);

        let (done_tx, done_rx) = mpsc::channel::<Result<Option<Written>>>();
        let mut written: Vec<Result<Option<Written>>> = Vec::new();
        let (job_tx, job_rx) = mpsc::sync_channel::<Job<'a>>(2 * encoders);
        let job_rx = Mutex::new(job_rx);
        let invalid: Option<Error> = std::thread::scope(|s| {
            // Encoders (none with one thread: the leader encodes). The sender is owned
            // here, so it is dropped on every way out and the encoders then stop.
            let job_tx = job_tx;
            if threads > 1 {
                for _ in 0..encoders {
                    let (job_rx, done_tx, cancel) = (&job_rx, done_tx.clone(), &cancel);
                    s.spawn(move || {
                        loop {
                            let job = lock(job_rx).recv();
                            let Ok(job) = job else { break };
                            if cancel.load(Ordering::Relaxed) {
                                continue;
                            }
                            let r = self.encode_job(job);
                            if r.is_err() {
                                cancel.store(true, Ordering::Relaxed);
                            }
                            let _ = done_tx.send(r);
                        }
                    });
                }
            }
            // Preparation ahead of the leader: up to `lookahead` chunks in flight.
            let mut pending: VecDeque<std::thread::ScopedJoinHandle<'_, Result<Prepared<'a>>>> =
                VecDeque::new();
            let mut next_chunk = 0;
            let input = &input;
            for c in 0..chunks {
                let prepared = if threads > 1 {
                    while next_chunk < chunks && pending.len() < lookahead {
                        let r = range(next_chunk);
                        pending.push_back(s.spawn(move || prepare(st, input, r)));
                        next_chunk += 1;
                    }
                    let h = pending.pop_front().expect("chunk in flight");
                    h.join().unwrap_or_else(|p| std::panic::resume_unwind(p))
                } else {
                    prepare(st, input, range(c))
                };
                let mut p = match prepared {
                    Ok(p) => p,
                    Err(e) => {
                        cancel.store(true, Ordering::Relaxed);
                        return Some(e); // the first invalid row of the batch
                    }
                };
                if matches!(phase, Phase::Failed(..)) {
                    continue;
                }
                if cancel.load(Ordering::Relaxed) {
                    // An encoder failed (its error is collected below).
                    phase = Phase::Failed(Error::internal("an encoder failed"), false);
                    continue;
                }
                let base = c * chunk;
                if let Err(e) = self.chunk_dedup(st, &mut earlier, &mut in_group, base, &mut p) {
                    cancel.store(true, Ordering::Relaxed);
                    phase = Phase::Failed(e, false);
                    continue;
                }
                // Seqs: the kept rows before each position.
                let mut before = Vec::with_capacity(p.dup.len());
                let mut kept = 0u32;
                for &d in &p.dup {
                    before.push(kept);
                    kept += u32::from(!d);
                }
                duplicates += p.dup.len() as u64 - u64::from(kept);
                let seq_base = next_seq;
                next_seq += u64::from(kept);
                let (dup, before) = (Arc::new(p.dup), Arc::new(before));
                let finish = |pos: Vec<u32>| Finish {
                    pos,
                    dup: Arc::clone(&dup),
                    before: Arc::clone(&before),
                    seq_base,
                    system_time,
                };
                let mut jobs: Vec<Job<'a>> = Vec::new();
                match &mut phase {
                    Phase::Buffer(buf) if buf.len() + kept as usize <= opts.inline_max_rows => {
                        for part in p.parts {
                            buf.extend(finish(part.pos).apply(part.rows));
                        }
                        continue;
                    }
                    Phase::Buffer(buf) => {
                        // More than `inline_max_rows`: files from now on, starting with
                        // the buffered rows.
                        let buffered = std::mem::take(buf);
                        phase = Phase::Files;
                        match self.final_jobs(st, buffered, &mut jobs_sent) {
                            Ok(j) => jobs.extend(j),
                            Err(e) => {
                                cancel.store(true, Ordering::Relaxed);
                                phase = Phase::Failed(e, false);
                                continue;
                            }
                        }
                    }
                    Phase::Files => {}
                    Phase::Failed(..) => unreachable!("skipped above"),
                }
                let dedup = lock(&self.dedup);
                for part in p.parts {
                    if part.pos.iter().all(|&i| dup[i as usize]) {
                        continue;
                    }
                    jobs.push(Job {
                        no: jobs_sent,
                        table: part.table,
                        partition: part.partition,
                        ranges: dedup.is_indexed(&part.table.name, part.partition),
                        finish: Some(finish(part.pos)),
                        rows: part.rows,
                    });
                    jobs_sent += 1;
                }
                drop(dedup);
                if jobs.is_empty() {
                    continue;
                }
                started.get_or_insert_with(Instant::now);
                for job in jobs {
                    if threads > 1 {
                        if job_tx.send(job).is_err() {
                            phase = Phase::Failed(Error::internal("encoder threads exited"), false);
                            break;
                        }
                    } else {
                        match self.encode_job(job) {
                            Ok(f) => written.push(Ok(f)),
                            Err(e) => {
                                phase = Phase::Failed(e, false);
                                break;
                            }
                        }
                    }
                }
            }
            None
        });
        drop(done_tx);
        written.extend(done_rx.iter());
        if let Some(e) = invalid {
            return Chunked::Rejected(e);
        }
        let mut files: Vec<Written> = Vec::with_capacity(written.len());
        for r in written {
            match r {
                Ok(f) => files.extend(f),
                Err(e) => return Chunked::Failed(e),
            }
        }
        let layout = match phase {
            Phase::Failed(e, true) => return Chunked::Rejected(e),
            Phase::Failed(e, false) => return Chunked::Failed(e),
            Phase::Buffer(buf) => Layout::Inline(buf),
            Phase::Files => {
                files.sort_by_key(|f| f.0);
                Layout::Files(files, started.unwrap_or_else(Instant::now))
            }
        };
        Chunked::Applied(ChunkedRows {
            stored: next_seq - first_seq,
            duplicates,
            layout,
            earlier,
        })
    }

    /// The decisions for one chunk (rows `base..` of the batch) that depend on what came
    /// before it — stored versions, earlier chunks, earlier batches of the group — added
    /// to `p.dup`; then records the chunk's candidates in `earlier`.
    fn chunk_dedup(
        &self,
        st: &State,
        earlier: &mut EarlierRows<'_>,
        in_group: &mut InGroup<'_>,
        base: usize,
        p: &mut Prepared<'_>,
    ) -> Result<()> {
        let Prepared {
            parts,
            at,
            hashes,
            dup,
            maybe,
            runs,
        } = p;
        let row = |i: usize| {
            let (g, k) = at[i];
            &parts[g as usize].rows[k as usize]
        };
        if !maybe.is_empty() {
            let rows: Vec<(&Version, &str, Partition)> = maybe
                .iter()
                .map(|&i| {
                    let part = &parts[at[i as usize].0 as usize];
                    (row(i as usize), part.table.name.as_str(), part.partition)
                })
                .collect();
            let stored = self.stored_of(st, &rows)?;
            if !stored.is_empty() {
                for &i in maybe.iter() {
                    let i = i as usize;
                    if !dup[i] && stored.contains(row(i), hashes[i]) {
                        dup[i] = true;
                    }
                }
            }
        }
        if !earlier.pairs.is_empty() {
            for &(s, rt, a, b) in runs.iter() {
                if !earlier.has_pair(s, rt) {
                    continue;
                }
                for i in a as usize..b as usize {
                    if !dup[i] && earlier.contains(row(i), hashes[i])? {
                        dup[i] = true;
                    }
                }
            }
        }
        if let Some(f) = in_group {
            for i in 0..dup.len() {
                if !dup[i] && f(row(i), hashes[i])? {
                    dup[i] = true;
                }
            }
        }
        earlier.add(base, runs);
        Ok(())
    }

    /// Jobs for rows whose seqs are assigned already: one per touched partition, in key
    /// order.
    fn final_jobs<'s>(
        &self,
        st: &'s State,
        rows: Vec<Version>,
        jobs_sent: &mut usize,
    ) -> Result<Vec<Job<'s>>> {
        let mut placer = Placer::new(st);
        let mut parts: BTreeMap<(&'s str, Partition), (&'s TableDef, Vec<Version>)> =
            BTreeMap::new();
        for v in rows {
            let (t, p) = placer.place(&v)?;
            parts
                .entry((t.name.as_str(), p))
                .or_insert_with(|| (t, Vec::new()))
                .1
                .push(v);
        }
        let dedup = lock(&self.dedup);
        Ok(parts
            .into_iter()
            .map(|((table, partition), (def, rows))| {
                let no = *jobs_sent;
                *jobs_sent += 1;
                Job {
                    no,
                    table: def,
                    partition,
                    rows,
                    finish: None,
                    ranges: dedup.is_indexed(table, partition),
                }
            })
            .collect())
    }

    /// Finishes, encodes and writes one job's file (`None` if no row is kept); its rows
    /// are freed here.
    fn encode_job(&self, job: Job<'_>) -> Result<Option<Written>> {
        let rows = match job.finish {
            Some(f) => f.apply(job.rows),
            None => job.rows,
        };
        if rows.is_empty() {
            return Ok(None);
        }
        let seg = self.put_segment(job.table, job.partition, &rows)?;
        let ranges = job.ranges.then(|| {
            let mut m: FxHashMap<(SeriesId, i64), (i64, i64)> = FxHashMap::default();
            for v in &rows {
                let rt = v.received_time.as_micros();
                m.entry((v.series, v.value_time.as_micros()))
                    .and_modify(|r| {
                        r.0 = r.0.min(rt);
                        r.1 = r.1.max(rt);
                    })
                    .or_insert((rt, rt));
            }
            m.into_iter()
                .map(|((s, vt), (lo, hi))| (s, vt, lo, hi))
                .collect()
        });
        Ok(Some((
            job.no,
            job.table.name.clone(),
            job.partition,
            seg,
            ranges,
        )))
    }
}
