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
//!   is built lazily: earlier candidates are recorded only as runs of rows sharing
//!   `(series, received_time)` — a content-identical row has the same pair — behind a
//!   Bloom filter of the pairs, and the rows of a pair are validated again from the input
//!   and indexed only when a later row has that pair, which fresh data (later received
//!   times) never has. Later batches of the group consult the same structure.
//! * **Seqs**: kept rows take consecutive seqs in input order, chunk after chunk.
//! * **Layout**: rows go inline (the memtable) if the batch keeps at most
//!   [`crate::EngineOptions::inline_max_rows`] rows, else to segment files: kept rows are
//!   buffered until they exceed that; from then on a partition's kept rows wait until they
//!   fill a file of `write_chunk_rows` rows, or, when more than `write_chunk_rows` rows
//!   wait in all, the partitions holding the most are written ([`Pending`]): a large
//!   write makes few files per partition (fewer for compaction to merge) while the rows
//!   it holds stay bounded. Query results do not depend on the layout (SPEC §7.2).
//!
//! Parallelism (threads, no async), in three stages. *Preparing* a chunk (up to
//! `max(2, io_threads / 2)` chunks ahead, each on its own thread) validates it, hashes
//! its rows, finds its in-chunk duplicates, the rows its partitions' stored received-time
//! bounds do not rule out, its `(series, received_time)` runs, and splits it by
//! partition. The *leader* (the calling thread, holding the writer lock) takes the
//! chunks in order and makes the decisions that depend on earlier ones: stored versions
//! (through the dedup index), earlier chunks, earlier batches of the group; then the seq
//! of every kept row follows from a prefix count. *Encoders* (`max(1, io_threads / 2)`
//! threads, joined by the preparing threads for the last files) drop the duplicates of a
//! partition's rows, assign seqs and `system_time`, and encode and write the file. Which
//! rows go into which file depends only on the input (row counts, never timing), and a
//! file's bytes only on its rows: the same input gives byte-identical segments whatever
//! the thread count, listed in the entry in a fixed order.

use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use muninn_core::api::WriteBatch;
use muninn_core::catalog::{BatchRows, in_batch_duplicates_by};
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
    /// The runs for [`EarlierRows`].
    keys: Vec<(u64, u32, u32)>,
}

/// Kept rows of a chunked batch that may wait to be written, in chunks (see
/// [`Pending`]).
const MAX_PENDING_CHUNKS: usize = 1;

/// Rows validated (and converted) at a time while preparing a chunk: rows go straight
/// into their partition's vector, so no allocation is chunk-sized (large allocations make
/// the system allocator keep correspondingly large free areas per thread).
const PIECE_ROWS: usize = 8192;

fn prepare<'s>(st: &'s State, input: &BatchRows<'_>, range: Range<usize>) -> Result<Prepared<'s>> {
    let n = range.len();
    let mut hashes: Vec<u64> = Vec::with_capacity(n);
    let mut runs: Vec<(SeriesId, i64, u32, u32)> = Vec::new();
    let mut distinct = true;
    let mut prev_point: Option<(SeriesId, Timestamp)> = None;
    // The part of each row (rows come in runs of one partition), in order of appearance.
    let mut placer = Placer::new(st);
    let mut index: FxHashMap<(&'s str, Partition), u32> = FxHashMap::default();
    let mut parts: Vec<Part<'s>> = Vec::new();
    let mut hulls: Vec<Option<(i64, i64)>> = Vec::new();
    let mut prev: Option<((&'s str, Partition), u32)> = None;
    let mut at: Vec<(u32, u32)> = Vec::with_capacity(n);
    let mut maybe = Vec::new();
    let mut i = 0u32;
    let mut a = range.start;
    while a < range.end {
        let b = (a + PIECE_ROWS).min(range.end);
        for v in input.validate(a..b)? {
            hashes.push(v.content_hash());
            let point = (v.series, v.value_time);
            distinct &= prev_point.is_none_or(|p| p < point);
            prev_point = Some(point);
            let rt = v.received_time.as_micros();
            match runs.last_mut() {
                Some(r) if (r.0, r.1) == (v.series, rt) => r.3 = i + 1,
                _ => runs.push((v.series, rt, i, i + 1)),
            }
            let (t, p) = placer.place(&v)?;
            let key = (t.name.as_str(), p);
            let g = match prev {
                Some((k, g)) if k == key => g,
                _ => *index.entry(key).or_insert_with(|| {
                    parts.push(Part {
                        table: t,
                        partition: p,
                        rows: Vec::new(),
                        pos: Vec::new(),
                    });
                    hulls.push(rt_hull(st, &t.name, p));
                    (parts.len() - 1) as u32
                }),
            };
            prev = Some((key, g));
            if hulls[g as usize].is_some_and(|(lo, hi)| lo <= rt && rt <= hi) {
                maybe.push(i);
            }
            let part = &mut parts[g as usize];
            at.push((g, part.rows.len() as u32));
            part.rows.push(v);
            part.pos.push(i);
            i += 1;
        }
        a = b;
    }
    // Parts in key order (so files are listed in a fixed order).
    let mut order: Vec<u32> = (0..parts.len() as u32).collect();
    order.sort_by(|&x, &y| {
        let (px, py) = (&parts[x as usize], &parts[y as usize]);
        (px.table.name.as_str(), px.partition).cmp(&(py.table.name.as_str(), py.partition))
    });
    let mut rank = vec![0u32; parts.len()];
    for (r, &g) in order.iter().enumerate() {
        rank[g as usize] = r as u32;
    }
    for x in &mut at {
        x.0 = rank[x.0 as usize];
    }
    let mut slots: Vec<Option<Part<'s>>> = parts.into_iter().map(Some).collect();
    let parts: Vec<Part<'s>> = order
        .iter()
        .map(|&g| slots[g as usize].take().expect("each part once"))
        .collect();
    let dup = if distinct {
        vec![false; n]
    } else {
        in_batch_duplicates_by(&hashes, |i| {
            let (g, k) = at[i];
            &parts[g as usize].rows[k as usize]
        })
    };
    let keys = run_keys(&runs);
    Ok(Prepared {
        parts,
        at,
        hashes,
        dup,
        maybe,
        runs,
        keys,
    })
}

/// Candidates of a chunked batch recorded so far (its earlier chunks; once it is done,
/// all of it, for the later batches of its group), for duplicate checks without keeping
/// its rows (see the module docs).
///
/// Per chunk only its `(series, received_time)` runs are kept, as `(pair hash, start,
/// end)` sorted by hash (sorted while the chunk is prepared), plus a Bloom filter of the
/// pairs over all chunks: a row whose pair the filter rules out (fresh data: every pair
/// is new) costs two bit tests. A pair the filter lets through is looked up in each
/// chunk's runs, and its rows are validated again from the input and indexed by content
/// ([`ContentIndex`]), once.
pub(crate) struct EarlierRows<'a> {
    input: BatchRows<'a>,
    /// Per recorded chunk: its first batch row and its runs.
    chunks: Vec<(u32, Vec<RunKey>)>,
    bloom: Vec<u64>,
    /// `bloom` has `mask + 1` bits.
    mask: u64,
    /// Pairs looked up so far.
    pairs: FxHashMap<(SeriesId, i64), Pair>,
}

/// The recorded candidates with one `(series, received_time)` (of `chunks[..upto]`).
#[derive(Default)]
struct Pair {
    upto: usize,
    index: ContentIndex,
    rows: Vec<Version>,
}

/// A run of rows sharing a `(series, received_time)`: `(pair hash, start, end)`.
type RunKey = (u64, u32, u32);

/// The hash of a `(series, received_time µs)` pair (splitmix64 finaliser).
fn pair_hash(series: SeriesId, rt: i64) -> u64 {
    let mut x = (rt as u64) ^ u64::from(series.0).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// A chunk's runs as `(pair hash, start, end)`, sorted (see [`EarlierRows`]).
fn run_keys(runs: &[(SeriesId, i64, u32, u32)]) -> Vec<(u64, u32, u32)> {
    let mut keys: Vec<(u64, u32, u32)> = runs
        .iter()
        .map(|&(s, rt, a, b)| (pair_hash(s, rt), a, b))
        .collect();
    keys.sort_unstable();
    keys
}

impl<'a> EarlierRows<'a> {
    /// For a batch of `rows` rows (sizes the filter: ~16 bits per row, at most 2^28).
    fn new(input: BatchRows<'a>, rows: usize) -> EarlierRows<'a> {
        let bits = (rows.saturating_mul(16))
            .next_power_of_two()
            .clamp(1 << 10, 1 << 28);
        EarlierRows {
            input,
            chunks: Vec::new(),
            bloom: vec![0; bits / 64],
            mask: bits as u64 - 1,
            pairs: FxHashMap::default(),
        }
    }

    fn bits(&self, h: u64) -> [u64; 2] {
        [h & self.mask, h.rotate_left(32) & self.mask]
    }

    /// Nothing recorded yet?
    fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Records the runs of a chunk starting at batch row `base` as candidates.
    fn add(&mut self, base: usize, keys: Vec<(u64, u32, u32)>) {
        for &(h, _, _) in &keys {
            for b in self.bits(h) {
                self.bloom[(b / 64) as usize] |= 1 << (b % 64);
            }
        }
        self.chunks.push((base as u32, keys));
    }

    /// May a recorded candidate have this `(series, received_time µs)`?
    fn may_have(&self, series: SeriesId, rt: i64) -> bool {
        self.bits(pair_hash(series, rt))
            .iter()
            .all(|&b| self.bloom[(b / 64) as usize] & (1 << (b % 64)) != 0)
    }

    /// Is a recorded candidate content-identical to `v` (hash `hash`)?
    pub(crate) fn contains(&mut self, v: &Version, hash: u64) -> Result<bool> {
        let rt = v.received_time.as_micros();
        if self.chunks.is_empty() || !self.may_have(v.series, rt) {
            return Ok(false);
        }
        let h = pair_hash(v.series, rt);
        let p = self.pairs.entry((v.series, rt)).or_default();
        for (base, keys) in &self.chunks[p.upto..] {
            let from = keys.partition_point(|k| k.0 < h);
            for &(_, a, b) in keys[from..].iter().take_while(|k| k.0 == h) {
                let range = (base + a) as usize..(base + b) as usize;
                for w in self.input.validate(range)? {
                    // Another pair with the same hash: not a candidate for `v`.
                    if (w.series, w.received_time.as_micros()) == (v.series, rt) {
                        p.index.push(w.content_hash());
                        p.rows.push(w);
                    }
                }
            }
        }
        p.upto = self.chunks.len();
        Ok(p.index.find(&p.rows, v, hash).is_some())
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

/// Rows of one partition, as candidates with what turns them into stored rows (`None`:
/// final already).
type Segs = Vec<(Vec<Version>, Option<Finish>)>;

/// Rows of one partition to be written as a segment file.
struct Job<'a> {
    /// Position among the batch's files.
    no: usize,
    table: &'a TableDef,
    partition: Partition,
    segs: Segs,
    /// The partition is in the dedup index: also return the rows' per-point received
    /// time ranges, to fold in once the entry is committed.
    ranges: bool,
}

/// A partition's kept rows of the chunks so far, not yet written: rows of several
/// chunks go into one file (fewer, larger files; see [`Pending::flush`]).
struct Pend<'a> {
    table: &'a TableDef,
    segs: Segs,
    /// Kept rows in `segs`.
    rows: usize,
}

/// The pending rows of all partitions.
struct Pending<'a> {
    parts: BTreeMap<(&'a str, Partition), Pend<'a>>,
    /// Kept rows over all partitions.
    rows: usize,
}

impl<'a> Pending<'a> {
    fn add(
        &mut self,
        table: &'a TableDef,
        partition: Partition,
        rows: Vec<Version>,
        finish: Option<Finish>,
        kept: usize,
    ) {
        let p = self
            .parts
            .entry((table.name.as_str(), partition))
            .or_insert_with(|| Pend {
                table,
                segs: Vec::new(),
                rows: 0,
            });
        p.segs.push((rows, finish));
        p.rows += kept;
        self.rows += kept;
    }

    /// The partitions to write now: every one holding at least `file_rows` kept rows,
    /// then the largest ones while more than `max_rows` are pending (`all`: every one).
    /// Deterministic: it depends on the row counts only.
    fn flush(
        &mut self,
        file_rows: usize,
        max_rows: usize,
        all: bool,
    ) -> Vec<((&'a str, Partition), Pend<'a>)> {
        let mut keys: Vec<(&'a str, Partition)> = self
            .parts
            .iter()
            .filter(|(_, p)| all || p.rows >= file_rows)
            .map(|(k, _)| *k)
            .collect();
        let mut out: Vec<_> = keys
            .drain(..)
            .map(|k| (k, self.parts.remove(&k).expect("listed")))
            .collect();
        self.rows -= out.iter().map(|(_, p)| p.rows).sum::<usize>();
        // Largest first, so that the encoders finish together.
        out.sort_by(|a, b| b.1.rows.cmp(&a.1.rows).then(a.0.cmp(&b.0)));
        while self.rows > max_rows {
            let k = *self
                .parts
                .iter()
                .max_by(|a, b| a.1.rows.cmp(&b.1.rows).then(b.0.cmp(a.0)))
                .expect("rows pending")
                .0;
            let p = self.parts.remove(&k).expect("listed");
            self.rows -= p.rows;
            out.push((k, p));
        }
        out
    }
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
        let mut earlier = EarlierRows::new(BatchRows::new(&st.catalog, batch), n);
        let mut jobs_sent = 0usize;
        let mut started: Option<Instant> = None;
        let cancel = AtomicBool::new(false);
        // Kept rows waiting to be written; a partition's file takes `chunk` rows, and at
        // most `max_pending` rows wait over all partitions.
        let mut waiting = Pending {
            parts: BTreeMap::new(),
            rows: 0,
        };
        let max_pending = MAX_PENDING_CHUNKS * chunk;

        let (done_tx, done_rx) = mpsc::channel::<Result<Option<Written>>>();
        let mut written: Vec<Result<Option<Written>>> = Vec::new();
        let (job_tx, job_rx) = mpsc::sync_channel::<Job<'a>>(2 * encoders);
        let job_rx = Mutex::new(job_rx);
        let invalid: Option<Error> = std::thread::scope(|s| {
            // Encoders (none with one thread: the leader encodes). The sender is owned
            // here, so it is dropped on every way out and the encoders then stop.
            let job_tx = job_tx;
            let spawn_encoders = |k: usize| {
                for _ in 0..k {
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
            };
            if threads > 1 {
                spawn_encoders(encoders);
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
                        match self.pend_final(st, buffered, &mut waiting) {
                            Ok(()) => {}
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
                for part in p.parts {
                    let kept = part.pos.iter().filter(|&&i| !dup[i as usize]).count();
                    if kept > 0 {
                        let f = finish(part.pos);
                        waiting.add(part.table, part.partition, part.rows, Some(f), kept);
                    }
                }
                let ready = waiting.flush(chunk, max_pending, false);
                if let Err(e) = self.send_jobs(
                    ready,
                    &mut jobs_sent,
                    &mut started,
                    &job_tx,
                    threads,
                    &mut written,
                ) {
                    phase = Phase::Failed(e, false);
                }
            }
            if matches!(phase, Phase::Files) {
                // The rows still waiting: preparation is over, so its threads encode too.
                if threads > 1 {
                    spawn_encoders(lookahead);
                }
                let rest = waiting.flush(chunk, max_pending, true);
                if let Err(e) = self.send_jobs(
                    rest,
                    &mut jobs_sent,
                    &mut started,
                    &job_tx,
                    threads,
                    &mut written,
                ) {
                    phase = Phase::Failed(e, false);
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
            keys,
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
        if !earlier.is_empty() {
            for &(s, rt, a, b) in runs.iter() {
                if !earlier.may_have(s, rt) {
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
        earlier.add(base, std::mem::take(keys));
        Ok(())
    }

    /// Adds rows whose seqs are assigned already to `pending`, by partition.
    fn pend_final<'s>(
        &self,
        st: &'s State,
        rows: Vec<Version>,
        pending: &mut Pending<'s>,
    ) -> Result<()> {
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
        for ((_, p), (t, rows)) in parts {
            let n = rows.len();
            pending.add(t, p, rows, None, n);
        }
        Ok(())
    }

    /// Hands partitions ready to be written to the encoders (or encodes them here with
    /// one thread), numbering the files in order.
    #[allow(clippy::too_many_arguments)]
    fn send_jobs<'s>(
        &self,
        ready: Vec<((&'s str, Partition), Pend<'s>)>,
        jobs_sent: &mut usize,
        started: &mut Option<Instant>,
        job_tx: &mpsc::SyncSender<Job<'s>>,
        threads: usize,
        written: &mut Vec<Result<Option<Written>>>,
    ) -> Result<()> {
        if ready.is_empty() {
            return Ok(());
        }
        started.get_or_insert_with(Instant::now);
        let jobs: Vec<Job<'s>> = {
            let dedup = lock(&self.dedup);
            ready
                .into_iter()
                .map(|((table, partition), p)| {
                    let no = *jobs_sent;
                    *jobs_sent += 1;
                    Job {
                        no,
                        table: p.table,
                        partition,
                        segs: p.segs,
                        ranges: dedup.is_indexed(table, partition),
                    }
                })
                .collect()
        };
        for job in jobs {
            if threads > 1 {
                job_tx
                    .send(job)
                    .map_err(|_| Error::internal("encoder threads exited"))?;
            } else {
                written.push(Ok(self.encode_job(job)?));
            }
        }
        Ok(())
    }

    /// Finishes, encodes and writes one job's file (`None` if no row is kept); its rows
    /// are freed here.
    fn encode_job(&self, job: Job<'_>) -> Result<Option<Written>> {
        let mut rows: Vec<Version> = Vec::new();
        for (seg, finish) in job.segs {
            let seg = match finish {
                Some(f) => f.apply(seg),
                None => seg,
            };
            if rows.is_empty() {
                rows = seg;
            } else {
                rows.extend(seg);
            }
        }
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
