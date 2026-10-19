//! The fast write path (decision 0009): group commit, rows in the log (memtable), flushes.
//!
//! - concurrent writers (with background flushes, compaction, GC and readers running
//!   alongside) end up exactly where the model ends up when their batches are replayed in
//!   commit order; every snapshot a reader took shows whole batches only, and its
//!   receipt reproduces;
//! - every acknowledged write survives a crash at every store operation (sequential
//!   workload, exhaustively; concurrent writers, at sampled points), on `MemoryStore` and
//!   `LocalFsStore`, and a crash never leaves part of a batch;
//! - the memtable survives log truncation and reopening; flush entries are validated;
//! - concurrent writes share commits.
//!
//! The random interleavings of writes, group commits, flushes, reads, compaction, GC,
//! reopen and crashes against the model are in `differential.rs` and `crash.rs` (their
//! workloads include `Op::WriteMany` and `Op::Flush`).

mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use common::*;
use muninn_core::api::*;
use muninn_core::{ColumnDef, ColumnType, Database, ErrorKind, ModelDb, Reader, Timestamp, Value};
use muninn_engine::{Engine, EngineOptions, log};
use muninn_store::{FaultyStore, Key, LocalFsStore, MemoryStore, Store};
use proptest::prelude::*;

const PRODUCERS: usize = 6;

fn setup<D: Database>(db: &D) {
    db.create_table(&CreateTable::new(
        "t",
        vec![ColumnDef::new("value", ColumnType::F64)],
    ))
    .unwrap();
    let mut reqs: Vec<CreateSeries> = (0..PRODUCERS)
        .map(|p| CreateSeries::new(format!("s{p}"), "t"))
        .collect();
    reqs.push(CreateSeries::new("hot", "t"));
    db.create_series_batch(&reqs).unwrap();
}

fn all() -> LatestRequest {
    LatestRequest {
        table: "t".into(),
        ..LatestRequest::default()
    }
}

/// A small deterministic generator (SplitMix64).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Batch `k` of producer `p`: rows of its own series spread over a few day partitions
/// (received at a time unique to the batch), sometimes rows of the shared `hot` series
/// that other producers write identically (only one copy may be stored), sometimes a
/// batch id, sometimes a resend of an earlier batch (same id: a no-op; no id: all
/// duplicates).
fn producer_batch(rng: &mut Rng, p: usize, k: usize, sent: &[WriteBatch]) -> WriteBatch {
    producer_batch_with(rng, p, k, sent, true)
}

fn producer_batch_with(
    rng: &mut Rng,
    p: usize,
    k: usize,
    sent: &[WriteBatch],
    resends: bool,
) -> WriteBatch {
    if resends && !sent.is_empty() && rng.below(8) == 0 {
        let mut b = sent[rng.below(sent.len() as u64) as usize].clone();
        b.clock = h(rng.below(40) as i64).into();
        return b;
    }
    let n = 1 + rng.below(12) as i64;
    let rt = 1000 + (p * 1000 + k) as i64;
    let mut rows: Vec<WriteRow> = (0..n)
        .map(|j| {
            WriteRow::new(format!("s{p}"), h(j * 7 % 96), h(rt))
                .value("value", Value::F64((k as i64 * 31 + j) as f64))
        })
        .collect();
    if rng.below(3) == 0 {
        let x = rng.below(4) as i64;
        rows.push(WriteRow::new("hot", h(x), h(x)).value("value", Value::F64(x as f64)));
    }
    if rng.below(10) == 0 {
        // A tombstone of an earlier point.
        rows.push(WriteRow::new(format!("s{p}"), h(rng.below(96) as i64), h(rt)).tombstone());
    }
    WriteBatch {
        rows,
        batch_id: (rng.below(3) == 0).then(|| format!("p{p}-{k}")),
        clock: h(rng.below(40) as i64).into(),
    }
}

/// What a reader saw in one snapshot: its `S`, rows per system_time, and the history.
struct Seen {
    last: Option<Timestamp>,
    per_batch: BTreeMap<Timestamp, usize>,
    history: ReadResult<VersionRow>,
}

fn options_for_concurrency() -> EngineOptions {
    EngineOptions {
        // Small thresholds: the background thread flushes all the time.
        flush_rows: 40,
        flush_segments: 6,
        background_flush: true,
        io_threads: 4,
        ..test_options(4, 5, true)
    }
}

/// Writers (single writes and `write_many`), readers, and a maintenance thread
/// (compaction, GC, explicit flushes) on one engine; then the checks of the module docs.
fn concurrent_writers<S: Store + Clone + 'static>(store: S, seed: u64, batches: usize) {
    let engine = Arc::new(Engine::open(store.clone(), options_for_concurrency()).unwrap());
    setup(&*engine);
    let done = Arc::new(AtomicBool::new(false));
    let seen: Arc<Mutex<Vec<Seen>>> = Arc::new(Mutex::new(Vec::new()));
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let (engine, done, seen) = (Arc::clone(&engine), Arc::clone(&done), Arc::clone(&seen));
            std::thread::spawn(move || {
                while !done.load(Ordering::SeqCst) {
                    let snap = engine.snapshot().unwrap();
                    let history = snap.history(&all()).unwrap();
                    // Seqs are exactly 1..=n: commits are whole and in seq order.
                    let seqs: BTreeSet<u64> = history.rows.iter().map(|r| r.seq).collect();
                    assert_eq!(seqs.len(), history.rows.len());
                    assert_eq!(
                        seqs.iter().copied().collect::<Vec<_>>(),
                        (1..=seqs.len() as u64).collect::<Vec<_>>()
                    );
                    let mut per_batch = BTreeMap::new();
                    for r in &history.rows {
                        *per_batch.entry(r.system_time).or_insert(0) += 1;
                    }
                    let mut seen = seen.lock().unwrap();
                    if seen.len() < 400 {
                        seen.push(Seen {
                            last: snap.last_system_time(),
                            per_batch,
                            history,
                        });
                    }
                }
            })
        })
        .collect();
    let maintenance = {
        let (engine, done) = (Arc::clone(&engine), Arc::clone(&done));
        std::thread::spawn(move || {
            let mut i = 0u64;
            while !done.load(Ordering::SeqCst) {
                match i % 3 {
                    0 => drop(engine.compact_all().unwrap()),
                    1 => drop(engine.gc().unwrap()),
                    _ => drop(engine.flush().unwrap()),
                }
                i += 1;
                std::thread::yield_now();
            }
        })
    };
    let producers: Vec<_> = (0..PRODUCERS)
        .map(|p| {
            let engine = Arc::clone(&engine);
            std::thread::spawn(move || {
                let mut rng = Rng(seed ^ (p as u64 * 0x1234_5678));
                let mut sent: Vec<WriteBatch> = Vec::new();
                let mut results: Vec<(WriteBatch, WriteResult)> = Vec::new();
                let mut k = 0;
                while k < batches {
                    // Sometimes several batches in one call.
                    let n = if rng.below(4) == 0 {
                        1 + rng.below(4) as usize
                    } else {
                        1
                    };
                    let group: Vec<WriteBatch> = (0..n.min(batches - k))
                        .map(|j| {
                            let b = producer_batch(&mut rng, p, k + j, &sent);
                            sent.push(b.clone());
                            b
                        })
                        .collect();
                    let rs = if group.len() == 1 {
                        vec![engine.write(&group[0])]
                    } else {
                        engine.write_many(&group)
                    };
                    for (b, r) in group.into_iter().zip(rs) {
                        results.push((b, r.unwrap()));
                    }
                    k += n;
                }
                results
            })
        })
        .collect();
    let mut results: Vec<(usize, usize, WriteBatch, WriteResult)> = Vec::new();
    for (p, h) in producers.into_iter().enumerate() {
        for (k, (b, r)) in h.join().unwrap().into_iter().enumerate() {
            results.push((p, k, b, r));
        }
    }
    done.store(true, Ordering::SeqCst);
    maintenance.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }

    // Commit order: the batches that stored rows, by seq; contiguous seqs, strictly
    // increasing system times.
    let mut stored: Vec<&(usize, usize, WriteBatch, WriteResult)> =
        results.iter().filter(|x| x.3.stored > 0).collect();
    stored.sort_by_key(|x| x.3.first_seq);
    let mut next = 1;
    for w in &stored {
        assert_eq!(w.3.first_seq, Some(next));
        next += w.3.stored;
    }
    assert!(
        stored
            .windows(2)
            .all(|w| w[0].3.system_time < w[1].3.system_time)
    );
    // Replay into the model in commit order (each at its system_time): every result is
    // the engine's. Then the batches that stored nothing, in each producer's order
    // (they only record batch ids; resends come after their originals).
    let model = ModelDb::new();
    setup(&model);
    for (_, _, b, r) in stored.iter().map(|x| (x.0, x.1, &x.2, &x.3)) {
        let mut b = b.clone();
        b.clock = r.system_time.unwrap().into();
        assert_eq!(&model.write(&b).unwrap(), r);
    }
    let mut rest: Vec<&(usize, usize, WriteBatch, WriteResult)> =
        results.iter().filter(|x| x.3.stored == 0).collect();
    rest.sort_by_key(|x| (x.0, x.1));
    for (_, _, b, r) in rest.iter().map(|x| (x.0, x.1, &x.2, &x.3)) {
        let m = model.write(b).unwrap();
        assert_eq!((m.stored, m.duplicates), (0, r.duplicates), "{b:?}");
        assert_eq!(
            m.batch_already_committed, r.batch_already_committed,
            "{b:?}"
        );
    }
    assert_eq!(fingerprint(&*engine), fingerprint(&model));

    // Every snapshot showed whole batches only, and its receipt (S = its last
    // system_time) reproduces now.
    let size: HashMap<Timestamp, u64> = stored
        .iter()
        .map(|x| (x.3.system_time.unwrap(), x.3.stored))
        .collect();
    let seen = seen.lock().unwrap();
    assert!(!seen.is_empty());
    for (i, s) in seen.iter().enumerate() {
        for (t, n) in &s.per_batch {
            assert_eq!(
                Some(*n as u64),
                size.get(t).copied(),
                "a partial batch at {t}"
            );
        }
        if i % 7 == 0 {
            let req = LatestRequest {
                context: ReadContext {
                    system: Some(s.last.unwrap_or(Timestamp::MIN)),
                    ..ReadContext::default()
                },
                ..all()
            };
            let now = engine.history(&req).unwrap();
            if s.last.is_some() {
                assert_eq!(now, s.history, "receipt of snapshot {i} does not reproduce");
            }
        }
    }
    drop(seen);
    assert!(
        engine.memtable().flushes > 0,
        "the background flusher never ran"
    );
    // A fresh process sees the same database.
    let fp = fingerprint(&*engine);
    drop(engine);
    let reopened = Engine::open(store, options_for_concurrency()).unwrap();
    assert_eq!(fingerprint(&reopened), fp);
}

#[test]
fn concurrent_writers_match_the_model_in_commit_order() {
    for seed in 0..4 {
        concurrent_writers(Arc::new(MemoryStore::new()), seed, 60);
    }
}

#[test]
fn concurrent_writers_match_the_model_on_local_fs() {
    let dir = tempfile::tempdir().unwrap();
    concurrent_writers(Arc::new(LocalFsStore::open(dir.path()).unwrap()), 99, 40);
}

// ---------------------------------------------------------------------------------------
// Crashes
// ---------------------------------------------------------------------------------------

/// Sequential workload steps for the exhaustive crash test.
#[derive(Clone, Debug)]
enum Step {
    Write(WriteBatch),
    Many(Vec<WriteBatch>),
    Flush,
    Compact,
    Gc,
    Checkpoint,
}

fn crash_workload() -> Vec<Step> {
    let mut rng = Rng(7);
    let mut steps = Vec::new();
    let mut sent: Vec<WriteBatch> = Vec::new();
    for k in 0..14 {
        let b = producer_batch(&mut rng, k % 3, k, &sent);
        sent.push(b.clone());
        steps.push(Step::Write(b));
        match k % 6 {
            1 => {
                let group: Vec<WriteBatch> = (0..3)
                    .map(|j| {
                        let b = producer_batch(&mut rng, (k + j) % 3, 100 + k * 3 + j, &sent);
                        sent.push(b.clone());
                        b
                    })
                    .collect();
                steps.push(Step::Many(group));
            }
            2 => steps.push(Step::Flush),
            3 => steps.push(Step::Checkpoint),
            4 => steps.push(Step::Compact),
            5 => steps.push(Step::Gc),
            _ => {}
        }
    }
    steps
}

/// Inline writes with frequent threshold flushes, checkpoints and log truncation.
fn crash_options() -> EngineOptions {
    EngineOptions {
        flush_rows: 30,
        flush_segments: 5,
        ..test_options(3, 3, true)
    }
}

/// Runs `steps` until the store fails; returns the acknowledged batches (with results)
/// and the batches whose outcome is unknown (the failed step's).
fn run_until_crash<S: Store>(
    engine: &Engine<S>,
    steps: &[Step],
) -> (Vec<(WriteBatch, WriteResult)>, Vec<WriteBatch>) {
    let mut acked = Vec::new();
    for step in steps {
        let failed: Option<Vec<WriteBatch>> = match step {
            Step::Write(b) => match engine.write(b) {
                Ok(r) => {
                    acked.push((b.clone(), r));
                    None
                }
                Err(_) => Some(vec![b.clone()]),
            },
            Step::Many(bs) => {
                let rs = engine.write_many(bs);
                let mut unknown = Vec::new();
                for (b, r) in bs.iter().zip(rs) {
                    match r {
                        Ok(r) => acked.push((b.clone(), r)),
                        Err(e) => {
                            assert!(
                                matches!(e.kind(), ErrorKind::Io | ErrorKind::Conflict),
                                "{e}"
                            );
                            unknown.push(b.clone());
                        }
                    }
                }
                (!unknown.is_empty()).then_some(unknown)
            }
            Step::Flush => engine.flush().err().map(|_| Vec::new()),
            Step::Compact => engine.compact_all().err().map(|_| Vec::new()),
            Step::Gc => engine.gc().err().map(|_| Vec::new()),
            Step::Checkpoint => engine.checkpoint().err().map(|_| Vec::new()),
        };
        if let Some(unknown) = failed {
            return (acked, unknown);
        }
    }
    (acked, Vec::new())
}

/// After a crash: the reopened database holds every acknowledged batch, and is exactly
/// the model of the acknowledged batches, or of those plus the batches whose outcome was
/// unknown (a group commits whole).
fn check_after_crash<S: Store>(
    reopened: &Engine<S>,
    acked: &[(WriteBatch, WriteResult)],
    unknown: &[WriteBatch],
) {
    let fp = fingerprint(reopened);
    let history = reopened.history(&all()).unwrap();
    for (b, r) in acked {
        let Some(t) = r.system_time else { continue };
        let rows: Vec<&VersionRow> = history.rows.iter().filter(|x| x.system_time == t).collect();
        assert_eq!(
            rows.len() as u64,
            r.stored,
            "acknowledged batch lost: {b:?}"
        );
        let first = r.first_seq.unwrap();
        assert!(
            rows.iter()
                .all(|x| (first..first + r.stored).contains(&x.seq))
        );
    }
    let model_of = |extra: &[WriteBatch]| {
        let m = ModelDb::new();
        setup(&m);
        for (b, r) in acked {
            let got = m.write(b).unwrap();
            assert_eq!((got.stored, got.duplicates), (r.stored, r.duplicates));
        }
        for b in extra {
            let _ = m.write(b);
        }
        fingerprint(&m)
    };
    let before = model_of(&[]);
    if fp != before {
        assert_eq!(
            fp,
            model_of(unknown),
            "neither before nor after the failed step"
        );
    }
}

/// One crash point: `n` store operations into the workload (after setup), with or
/// without the operation taking effect.
fn crash_at<S: Store + 'static>(inner: Arc<S>, steps: &[Step], n: u64, effect: bool) {
    let store = Arc::new(FaultyStore::new(Arc::clone(&inner), n));
    let engine = Engine::open(Arc::clone(&store), crash_options()).unwrap();
    setup(&engine);
    store.set_crash_at(store.op_count() + n, effect);
    let (acked, unknown) = run_until_crash(&engine, steps);
    drop(engine);
    store.restart();
    let reopened = Engine::open(Arc::clone(&inner), crash_options()).unwrap();
    check_after_crash(&reopened, &acked, &unknown);
    // And it keeps working: a write, a flush, a GC, a reopen (at some crash points:
    // the test runs every point).
    if n % 5 != 0 {
        return;
    }
    let more = WriteBatch::new(
        h(999),
        vec![WriteRow::new("s0", h(5), h(5_000)).value("value", Value::F64(1.0))],
    );
    assert_eq!(reopened.write(&more).unwrap().stored, 1);
    reopened.flush().unwrap();
    reopened.gc().unwrap();
    let fp = fingerprint(&reopened);
    drop(reopened);
    assert_eq!(
        fingerprint(&Engine::open(inner, crash_options()).unwrap()),
        fp
    );
}

/// Store operations of the whole workload (after setup), fault-free.
fn workload_ops(steps: &[Step]) -> u64 {
    let store = Arc::new(FaultyStore::new(Arc::new(MemoryStore::new()), 0));
    let engine = Engine::open(Arc::clone(&store), crash_options()).unwrap();
    setup(&engine);
    let start = store.op_count();
    let (acked, unknown) = run_until_crash(&engine, steps);
    assert!(unknown.is_empty() && !acked.is_empty());
    assert!(engine.memtable().flushes > 0);
    store.op_count() - start
}

#[test]
fn acknowledged_writes_survive_a_crash_at_every_store_operation() {
    let steps = crash_workload();
    let total = workload_ops(&steps);
    for n in 0..=total {
        for effect in [false, true] {
            crash_at(Arc::new(MemoryStore::new()), &steps, n, effect);
        }
    }
}

#[test]
fn acknowledged_writes_survive_a_crash_at_every_store_operation_on_local_fs() {
    let steps = crash_workload();
    let total = workload_ops(&steps);
    for n in 0..=total {
        for effect in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            crash_at(
                Arc::new(LocalFsStore::open(dir.path()).unwrap()),
                &steps,
                n,
                effect,
            );
        }
    }
}

/// Concurrent writers and a crash at a sampled store operation: every acknowledged batch
/// survives, and no batch is partly there.
#[test]
fn concurrent_writers_keep_every_acknowledged_write_across_a_crash() {
    for (round, local) in [(0u64, false), (1, false), (2, true), (3, false)] {
        for n in [3u64, 10, 25, 60, 140] {
            for effect in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let inner: Arc<dyn Store> = if local {
                    Arc::new(LocalFsStore::open(dir.path()).unwrap())
                } else {
                    Arc::new(MemoryStore::new())
                };
                let store = Arc::new(FaultyStore::new(Arc::clone(&inner), n));
                let opts = EngineOptions {
                    background_flush: true,
                    ..crash_options()
                };
                let engine = Arc::new(Engine::open(Arc::clone(&store), opts.clone()).unwrap());
                setup(&*engine);
                store.set_crash_at(store.op_count() + n, effect);
                let handles: Vec<_> = (0..4)
                    .map(|p| {
                        let engine = Arc::clone(&engine);
                        std::thread::spawn(move || {
                            let mut rng = Rng(round * 100 + p as u64);
                            let mut sent = Vec::new();
                            let mut acked = Vec::new();
                            let mut unknown = Vec::new();
                            for k in 0..30 {
                                // Unique rows per batch (no hot rows, no resends), so
                                // that every stored row names its batch.
                                let mut b = producer_batch_with(&mut rng, p, k, &sent, false);
                                b.rows.retain(|r| r.series != "hot" && !r.deleted);
                                b.batch_id = None;
                                if b.rows.is_empty() {
                                    continue;
                                }
                                sent.push(b.clone());
                                match engine.write(&b) {
                                    Ok(r) => acked.push((b, r)),
                                    Err(_) => unknown.push(b),
                                }
                            }
                            (acked, unknown)
                        })
                    })
                    .collect();
                let mut acked = Vec::new();
                let mut unknown = Vec::new();
                for h in handles {
                    let (a, u) = h.join().unwrap();
                    acked.extend(a);
                    unknown.extend(u);
                }
                drop(engine);
                store.restart();
                let reopened = Engine::open(Arc::clone(&inner), opts).unwrap();
                let history = reopened.history(&all()).unwrap();
                // Rows by batch (series + received time identify it).
                let mut present: BTreeMap<(String, Timestamp), usize> = BTreeMap::new();
                for r in &history.rows {
                    *present
                        .entry((r.series.to_string(), r.received_time))
                        .or_default() += 1;
                }
                let key = |b: &WriteBatch| {
                    let r = &b.rows[0];
                    let rt = match &r.received_time {
                        TimeInput::Micros(us) => Timestamp::from_micros(*us).unwrap(),
                        other => panic!("{other:?}"),
                    };
                    (r.series.clone(), rt)
                };
                for (b, r) in &acked {
                    assert_eq!(
                        present.get(&key(b)).copied(),
                        Some(r.stored as usize),
                        "lost {b:?}"
                    );
                }
                for b in &unknown {
                    let n = present.get(&key(b)).copied().unwrap_or(0);
                    let expected = ModelDb::new();
                    setup(&expected);
                    let want = expected.write(b).unwrap().stored as usize;
                    assert!(n == 0 || n == want, "partial batch {b:?}: {n} of {want}");
                }
                let seqs: Vec<u64> = {
                    let mut s: Vec<u64> = history.rows.iter().map(|r| r.seq).collect();
                    s.sort_unstable();
                    s
                };
                assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// Memtable, truncation, validation
// ---------------------------------------------------------------------------------------

/// Rows that live only in the log (never flushed) survive log truncation — the
/// checkpoints carry them — and replaying is idempotent (reopen, reopen again).
#[test]
fn memtable_survives_log_truncation_and_reopening() {
    let store = Arc::new(MemoryStore::new());
    let opts = EngineOptions {
        flush_rows: u64::MAX,
        flush_segments: usize::MAX,
        ..test_options(4, 2, false)
    };
    let e = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
    setup(&e);
    for k in 0..12 {
        e.write(&WriteBatch::new(
            h(k),
            vec![WriteRow::new("s0", h(k), h(k)).value("value", Value::F64(k as f64))],
        ))
        .unwrap();
        e.gc().unwrap();
    }
    assert_eq!(e.memtable().rows, 12);
    assert!(store.list("segments/").unwrap().is_empty());
    // The entries that held the first rows are gone.
    assert!(store.get(&log::entry_key(4)).is_err());
    let fp = fingerprint(&e);
    drop(e);
    for _ in 0..2 {
        let e = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
        assert_eq!(fingerprint(&e), fp);
        assert_eq!(e.memtable().rows, 12);
    }
    let e = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
    assert!(e.flush().unwrap());
    assert_eq!(e.memtable().rows, 0);
    assert_eq!(fingerprint(&e), fp);
    assert_eq!(store.list("segments/").unwrap().len(), 1);
    drop(e);
    assert_eq!(fingerprint(&Engine::open(store, opts).unwrap()), fp);
}

fn rewrite_entry(store: &MemoryStore, v: u64, f: impl FnOnce(&mut serde_json::Value)) {
    let k = log::entry_key(v);
    let mut j: serde_json::Value = serde_json::from_slice(&store.get(&k).unwrap()).unwrap();
    f(&mut j);
    store.put(&k, &serde_json::to_vec(&j).unwrap()).unwrap();
}

fn opens(store: &Arc<MemoryStore>) -> bool {
    Engine::open(Arc::clone(store), test_options(4, 0, false)).is_ok()
}

/// A database with two inline writes in two partitions and then a flush (the last
/// entry); returns the store, a pristine copy maker and the flush entry's version.
fn flushed_db() -> (Arc<MemoryStore>, u64) {
    let store = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), test_options(4, 0, false)).unwrap();
    setup(&e);
    for k in 0..2 {
        e.write(&WriteBatch::new(
            h(k),
            vec![
                WriteRow::new("s0", h(k), h(k)).value("value", Value::F64(1.0)),
                WriteRow::new("s0", h(30 + k), h(k)).value("value", Value::F64(2.0)),
            ],
        ))
        .unwrap();
    }
    assert!(e.flush().unwrap());
    (store, e.version())
}

#[test]
fn flush_entries_are_validated() {
    let (store, v) = flushed_db();
    assert!(opens(&store));
    let tamper = |f: &dyn Fn(&mut serde_json::Value)| {
        let (s, v) = flushed_db();
        rewrite_entry(&s, v, f);
        opens(&s)
    };
    assert!(tamper(&|_| {}));
    // Rows that do not add up.
    assert!(!tamper(&|j| j["add"][0]["rows"] = 3.into()));
    // Removing something that is not a live inline segment.
    assert!(!tamper(
        &|j| j["remove"][0] = "inline/00000000000000000099/0".into()
    ));
    // Removing the same inline segment twice.
    assert!(!tamper(&|j| {
        let first = j["remove"][0].clone();
        j["remove"][1] = first;
    }));
    // Moving the high-water mark.
    assert!(!tamper(&|j| j["next_seq"] = 99.into()));
    // A flush that adds an inline segment.
    assert!(!tamper(&|j| j["add"][0]["data"] = "AAAA".into()));
    // Not in format 2.
    assert!(!tamper(&|j| j["format"] = 2.into()));
    let _ = v;
}

#[test]
fn inline_segments_are_validated() {
    let store = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), test_options(4, 0, false)).unwrap();
    setup(&e);
    e.write(&WriteBatch::new(
        h(1),
        vec![WriteRow::new("s0", h(1), h(1)).value("value", Value::F64(1.0))],
    ))
    .unwrap();
    let v = e.version();
    drop(e);
    let tamper = |f: &dyn Fn(&mut serde_json::Value)| {
        let s = Arc::new(MemoryStore::new());
        for k in store.list("").unwrap() {
            s.put(&k, &store.get(&k).unwrap()).unwrap();
        }
        rewrite_entry(&s, v, f);
        opens(&s)
    };
    assert!(tamper(&|_| {}));
    // The key must be inline/<version>/<index>.
    assert!(!tamper(
        &|j| j["add"][0]["key"] = "inline/00000000000000000001/0".into()
    ));
    // Statistics must agree with the data.
    assert!(!tamper(&|j| j["add"][0]["received_time"][1] = 5.into()));
    assert!(!tamper(&|j| j["add"][0]["size"] = 7.into()));
    // Data that is not a segment, or not base64.
    assert!(!tamper(&|j| j["add"][0]["data"] = "AAAA".into()));
    assert!(!tamper(&|j| j["add"][0]["data"] = "!!!!".into()));
    // Inline data on a key of a segment file.
    assert!(!tamper(
        &|j| j["add"][0]["key"] = "segments/t/2025-01-01/x.seg".into()
    ));
    // Only format-3 write entries carry inline segments.
    assert!(!tamper(&|j| j["format"] = 2.into()));
    assert!(!tamper(&|j| j["kind"] = "restore".into()));
}

/// Databases written by format-2 engines (single-batch writes with `batch_id`, segment
/// files) open and keep working.
#[test]
fn format_2_logs_are_read() {
    let store = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), file_options(4, 3, false)).unwrap();
    setup(&e);
    for k in 0..7 {
        let mut b = WriteBatch::new(
            h(k),
            vec![WriteRow::new("s1", h(k), h(k)).value("value", Value::F64(k as f64))],
        );
        b.batch_id = Some(format!("b{k}"));
        e.write(&b).unwrap();
    }
    let fp = fingerprint(&e);
    let version = e.version();
    drop(e);
    // Downgrade every entry and drop the checkpoints (format-3 ones).
    for k in store.list("log/").unwrap() {
        if log::parse_checkpoint_key(k.as_str()).is_some()
            || k.as_str().ends_with("_last_checkpoint")
        {
            store.delete(&k).unwrap();
        }
    }
    for v in 1..=version {
        rewrite_entry(&store, v, |j| {
            j["format"] = 2.into();
            if let Some(ids) = j.as_object_mut().unwrap().remove("batch_ids") {
                j["batch_id"] = ids[0].clone();
            }
        });
    }
    let e = Engine::open(Arc::clone(&store), test_options(4, 3, false)).unwrap();
    assert_eq!(fingerprint(&e), fp);
    let mut b = WriteBatch::new(h(50), vec![]);
    b.batch_id = Some("b3".into());
    assert!(e.write(&b).unwrap().batch_already_committed);
    e.write(&WriteBatch::new(
        h(51),
        vec![WriteRow::new("s1", h(51), h(51))],
    ))
    .unwrap();
    e.flush().unwrap();
    let fp = fingerprint(&e);
    drop(e);
    assert_eq!(
        fingerprint(&Engine::open(store, test_options(4, 3, false)).unwrap()),
        fp
    );
}

// ---------------------------------------------------------------------------------------
// Group commit
// ---------------------------------------------------------------------------------------

/// A store whose log puts take a while (a slow disk or an object store).
struct SlowLog(Arc<MemoryStore>, std::time::Duration, Arc<AtomicU64>);

impl Store for SlowLog {
    fn get(&self, k: &Key) -> muninn_store::Result<Vec<u8>> {
        self.0.get(k)
    }
    fn get_range(&self, k: &Key, r: std::ops::Range<u64>) -> muninn_store::Result<Vec<u8>> {
        self.0.get_range(k, r)
    }
    fn head(&self, k: &Key) -> muninn_store::Result<muninn_store::ObjectMeta> {
        self.0.head(k)
    }
    fn put(&self, k: &Key, d: &[u8]) -> muninn_store::Result<()> {
        self.0.put(k, d)
    }
    fn put_if_absent(&self, k: &Key, d: &[u8]) -> muninn_store::Result<muninn_store::PutOutcome> {
        if log::parse_entry_key(k.as_str()).is_some() {
            self.2.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(self.1);
        }
        self.0.put_if_absent(k, d)
    }
    fn list(&self, p: &str) -> muninn_store::Result<Vec<Key>> {
        self.0.list(p)
    }
    fn delete(&self, k: &Key) -> muninn_store::Result<()> {
        self.0.delete(k)
    }
}

#[test]
fn concurrent_writes_share_commits() {
    let puts = Arc::new(AtomicU64::new(0));
    let store = SlowLog(
        Arc::new(MemoryStore::new()),
        std::time::Duration::from_millis(20),
        Arc::clone(&puts),
    );
    let engine = Arc::new(Engine::open(store, test_options(4, 0, false)).unwrap());
    setup(&*engine);
    let before = puts.load(Ordering::SeqCst);
    let threads = 24;
    let barrier = Arc::new(std::sync::Barrier::new(threads));
    let handles: Vec<_> = (0..threads)
        .map(|i| {
            let (engine, barrier) = (Arc::clone(&engine), Arc::clone(&barrier));
            std::thread::spawn(move || {
                barrier.wait();
                let b = WriteBatch::new(
                    h(1),
                    vec![WriteRow::new(
                        format!("s{}", i % PRODUCERS),
                        h(i as i64),
                        h(i as i64),
                    )],
                );
                engine.write(&b).unwrap()
            })
        })
        .collect();
    let results: Vec<WriteResult> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let commits = puts.load(Ordering::SeqCst) - before;
    // One commit for the first writer, then the rest queue up behind it.
    assert!(
        commits < threads as u64 / 2,
        "{commits} commits for {threads} writes"
    );
    let mut times: Vec<Timestamp> = results.iter().map(|r| r.system_time.unwrap()).collect();
    times.sort();
    times.dedup();
    assert_eq!(times.len(), threads, "every batch has its own system_time");
    assert_eq!(engine.history(&all()).unwrap().rows.len(), threads);
}

/// Group semantics in one call: batch ids and duplicates against earlier batches of the
/// group, per-batch errors that leave the others alone, strictly increasing times.
#[test]
fn write_many_is_writes_one_after_another() {
    let e = Engine::open(Arc::new(MemoryStore::new()), test_options(4, 0, false)).unwrap();
    let m = ModelDb::new();
    setup(&e);
    setup(&m);
    let r =
        |s: &str, vt: i64, rt: i64| WriteRow::new(s, h(vt), h(rt)).value("value", Value::F64(1.0));
    let with_id = |mut b: WriteBatch, id: &str| {
        b.batch_id = Some(id.into());
        b
    };
    let batches = vec![
        WriteBatch::new(h(5), vec![r("s0", 1, 1), r("s0", 2, 1)]),
        WriteBatch::new(h(3), vec![r("s0", 1, 1), r("s1", 1, 1)]), // one duplicate; clock goes back
        with_id(WriteBatch::new(h(9), vec![r("s2", 1, 1)]), "x"),
        with_id(WriteBatch::new(h(9), vec![r("s2", 7, 1)]), "x"), // no-op
        with_id(WriteBatch::new(h(9), vec![r("nope", 1, 1)]), "x"), // no-op, not an error
        with_id(WriteBatch::new(h(9), vec![r("nope", 1, 1)]), "y"), // unknown_series
        WriteBatch::new(h(9), vec![]),
        WriteBatch::new(h(9), vec![r("s0", 2, 1)]), // all duplicates
        WriteBatch::new(h(1), vec![WriteRow::new("s3", h(1), h(1)).tombstone()]),
    ];
    let got = e.write_many(&batches);
    let want: Vec<_> = batches.iter().map(|b| m.write(b)).collect();
    let kinds = |v: &[muninn_core::Result<WriteResult>]| -> Vec<_> {
        v.iter()
            .map(|r| r.as_ref().map_err(|e| e.kind()).cloned())
            .collect()
    };
    assert_eq!(kinds(&got), kinds(&want));
    assert_eq!(
        got[5].as_ref().unwrap_err().kind(),
        ErrorKind::UnknownSeries
    );
    assert_eq!(fingerprint(&e), fingerprint(&m));
    assert_eq!(e.memtable().rows, 5);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(64), ..ProptestConfig::default() })]

    /// The differential workload with other inline/flush settings: never inline, always
    /// inline, a flush after almost every write, and an index that is always cold.
    #[test]
    fn engine_matches_model_with_any_memtable_setting(
        ops in arb_ops(ops_len(40)),
        inline in prop_oneof![Just(0usize), Just(2), Just(1_000_000)],
        flush_rows in prop_oneof![Just(1u64), Just(5), Just(u64::MAX)],
        flush_segments in prop_oneof![Just(1usize), Just(3), Just(usize::MAX)],
        index in prop_oneof![Just(0usize), Just(2_000_000)],
    ) {
        let o = EngineOptions {
            inline_max_rows: inline,
            flush_rows,
            flush_segments,
            dedup_index_points: index,
            ..test_options(2, 3, true)
        };
        run_differential(Arc::new(MemoryStore::new()), o, &ops)?;
    }
}
