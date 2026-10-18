//! Crash and fault injection (SPEC §7.6 write atomicity): random workloads of catalog
//! changes, writes, compactions and GCs run on a `FaultyStore`; at a crash point (every
//! store op index, or a sample) the store fails — with or without the op taking effect —
//! and stays down. After a restart a fresh engine must show exactly the database before
//! or after the in-flight operation, and the rest of the workload must then behave
//! exactly like the model. A second mode injects random ambiguous failures into a
//! long-lived engine, which must reload instead of serving stale state.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::*;
use muninn_core::{Database, ErrorKind, ModelDb};
use muninn_engine::{Engine, EngineOptions};
use muninn_store::{FaultyStore, Key, MemoryStore, Store};
use proptest::prelude::*;

type Faulty = Arc<FaultyStore<Arc<MemoryStore>>>;

/// Mutations plus compaction and GC (no reads: the store is the subject).
fn arb_workload(len: usize) -> impl Strategy<Value = Vec<Op>> {
    let op = prop_oneof![
        12 => arb_mutation(),
        2 => Just(Op::Compact),
        1 => Just(Op::Gc),
    ];
    prop::collection::vec(op, 1..len)
}

/// Replays `ops` on a fresh model; returns it with the resend history.
fn build_model(ops: &[Op]) -> (ModelDb, Vec<Vec<GenRow>>) {
    let model = ModelDb::new();
    let mut history = Vec::new();
    for op in ops {
        if let Some(Ok(_)) = apply_mutation(&model, op, &history)
            && let Some((_, rows)) = batch_of(op, &history)
        {
            history.push(rows);
        }
    }
    (model, history)
}

/// Runs one physical op on the engine. `Err` only for engine errors.
fn run_engine<S: Store>(
    engine: &Engine<S>,
    op: &Op,
    history: &[Vec<GenRow>],
) -> Result<Option<String>, muninn_core::Error> {
    match op {
        Op::Compact => engine.compact_all().map(|_| None),
        Op::Gc => engine.gc().map(|_| None),
        _ => match apply_mutation(engine, op, history) {
            Some(Ok(s)) => Ok(Some(s)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        },
    }
}

/// Which pool batch ids are committed: probed by an empty write with each id (on a
/// throwaway database, so nothing is changed).
fn batch_ids_of<D: Database>(db: &D) -> Vec<bool> {
    (0..4u8)
        .map(|b| {
            let op = Op::Write {
                clock: 0,
                batch_id: Some(b),
                rows: vec![],
            };
            let (batch, _) = batch_of(&op, &[]).unwrap();
            db.write(&batch).unwrap().batch_already_committed
        })
        .collect()
}

/// A copy of a memory store's objects.
fn copy_store(s: &MemoryStore) -> Arc<MemoryStore> {
    let out = Arc::new(MemoryStore::new());
    for k in s.list("").unwrap() {
        out.put(&k, &s.get(&k).unwrap()).unwrap();
    }
    out
}

/// Fingerprint plus committed batch ids of the database in `store`.
fn observe(store: &MemoryStore, opts: &EngineOptions) -> String {
    let e = Engine::open(copy_store(store), opts.clone()).unwrap();
    let fp = fingerprint(&e);
    format!("{fp}{:?}", batch_ids_of(&e))
}

fn observe_model(ops: &[Op]) -> String {
    let fp = fingerprint(&build_model(ops).0);
    format!("{fp}{:?}", batch_ids_of(&build_model(ops).0))
}

/// After a fault in `op`: the database must equal the model before or after it. Returns
/// whether it was "after".
fn resolve(seen: &str, applied: &[Op], op: &Op) -> Result<bool, TestCaseError> {
    if seen == observe_model(applied) {
        return Ok(false);
    }
    let mut with = applied.to_vec();
    with.push(op.clone());
    let fa = observe_model(&with);
    prop_assert_eq!(
        seen,
        fa.as_str(),
        "neither before nor after the in-flight {:?}",
        op
    );
    Ok(true)
}

/// Total store ops of a fault-free run (to choose crash points).
fn dry_run(ops: &[Op], opts: &EngineOptions) -> u64 {
    let store: Faulty = Arc::new(FaultyStore::new(Arc::new(MemoryStore::new()), 0));
    let engine = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
    let mut applied: Vec<Op> = Vec::new();
    for op in ops {
        let (_, history) = build_model(&applied);
        run_engine(&engine, op, &history).ok();
        if op.is_mutation() {
            applied.push(op.clone());
        }
    }
    store.op_count()
}

/// Asserts that, with faults off, GC leaves exactly the referenced segments.
fn assert_no_orphans_after_gc<S: Store>(engine: &Engine<S>) -> Result<(), TestCaseError> {
    engine.gc().unwrap();
    let listed: BTreeSet<Key> = engine
        .store()
        .list("segments/")
        .unwrap()
        .into_iter()
        .collect();
    prop_assert_eq!(listed, engine.live_segment_keys());
    prop_assert!(engine.retired_segment_keys().is_empty());
    Ok(())
}

/// Crash at store op `crash_at`, restart, recover, finish the workload.
fn crash_run(
    ops: &[Op],
    opts: &EngineOptions,
    crash_at: u64,
    effect: bool,
) -> Result<(), TestCaseError> {
    let inner = Arc::new(MemoryStore::new());
    let store: Faulty = Arc::new(FaultyStore::new(Arc::clone(&inner), crash_at));
    store.set_crash_at(crash_at, effect);
    let mut engine = Engine::open(Arc::clone(&store), opts.clone()).ok();
    let mut applied: Vec<Op> = Vec::new();
    let mut i = 0;
    let mut recovered = false;
    while i < ops.len() {
        let op = &ops[i];
        let (model, history) = build_model(&applied);
        let outcome = match &engine {
            Some(e) => run_engine(e, op, &history),
            None => Err(muninn_core::Error::new(ErrorKind::Io, "open failed")),
        };
        match outcome {
            Err(e) if e.kind() == ErrorKind::Io && !recovered => {
                prop_assert!(store.is_crashed(), "Io error without a crash: {}", e);
                // Restart the "process": a new engine over the same durable state.
                store.restart();
                drop(engine.take());
                let e2 = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
                let seen = format!("{}{:?}", fingerprint(&e2), {
                    let probe = Engine::open(copy_store(&inner), opts.clone()).unwrap();
                    batch_ids_of(&probe)
                });
                let after = resolve(&seen, &applied, op)?;
                if op.is_mutation() && after {
                    applied.push(op.clone());
                }
                engine = Some(e2);
                recovered = true;
                if after || !op.is_mutation() {
                    i += 1;
                }
                // else: retry the op (what a client would do).
            }
            outcome => {
                let expected = if op.is_mutation() {
                    apply_mutation(&model, op, &history)
                        .map_or(Ok(None), |r| r.map(Some))
                        .map_err(|e| e.kind())
                } else {
                    Ok(None)
                };
                prop_assert_eq!(outcome.map_err(|e| e.kind()), expected, "op {} {:?}", i, op);
                if op.is_mutation() {
                    applied.push(op.clone());
                }
                i += 1;
            }
        }
    }
    if !recovered {
        store.restart();
    }
    let (model, _) = build_model(&applied);
    let engine = Engine::open(Arc::clone(&inner), opts.clone()).unwrap();
    prop_assert_eq!(fingerprint(&engine), fingerprint(&model));
    assert_no_orphans_after_gc(&engine)?;
    let reopened = Engine::open(Arc::clone(&inner), opts.clone()).unwrap();
    prop_assert_eq!(fingerprint(&reopened), fingerprint(&model));
    // seq / system_time / batch-id state survived: one more identical write on both.
    let probe = Op::Write {
        clock: 0,
        batch_id: Some(0),
        rows: vec![GenRow {
            series: 0,
            vt: GenTime::Hours(1),
            pt: None,
            rt: GenTime::Hours(99),
            deleted: false,
            values: vec![],
        }],
    };
    let (_, history) = build_model(&applied);
    let a = apply_mutation(&model, &probe, &history)
        .unwrap()
        .map_err(|e| e.kind());
    let b = apply_mutation(&reopened, &probe, &history)
        .unwrap()
        .map_err(|e| e.kind());
    prop_assert_eq!(b, a);
    prop_assert_eq!(fingerprint(&reopened), fingerprint(&model));
    Ok(())
}

fn workload_with_setup(ops: Vec<Op>) -> Vec<Op> {
    let mut all = setup_ops();
    all.extend(ops);
    all
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(96), ..ProptestConfig::default() })]

    /// Crash at a sample of store-op indices (with and without the op taking effect).
    #[test]
    fn crash_anywhere_recovers_before_or_after(
        ops in arb_workload(ops_len(14)),
        block_rows in 1usize..4,
        checkpoint in 1u64..4,
        picks in prop::collection::vec((any::<prop::sample::Index>(), any::<bool>()), 12),
    ) {
        let ops = workload_with_setup(ops);
        let opts = test_options(block_rows, checkpoint, false);
        let total = dry_run(&ops, &opts);
        for (ix, effect) in picks {
            crash_run(&ops, &opts, ix.index(total as usize + 1) as u64, effect)?;
        }
    }

    /// Random ambiguous failures (fail-before and fail-after) on a long-lived engine:
    /// after each failure it must reload and show the state before or after the op.
    #[test]
    fn ambiguous_failures_never_serve_stale_state(
        ops in arb_workload(ops_len(20)),
        seed in any::<u64>(),
        p_before in 0.0f64..0.15,
        p_after in 0.0f64..0.15,
    ) {
        let ops = workload_with_setup(ops);
        let opts = test_options(2, 2, false);
        let inner = Arc::new(MemoryStore::new());
        let store: Faulty = Arc::new(FaultyStore::new(Arc::clone(&inner), seed));
        let engine = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
        store.set_fail_before(p_before);
        store.set_fail_after(p_after);
        let mut applied: Vec<Op> = Vec::new();
        let mut i = 0;
        let mut attempts = 0;
        while i < ops.len() {
            let op = &ops[i];
            let (model, history) = build_model(&applied);
            match run_engine(&engine, op, &history) {
                Err(e) if e.kind() == ErrorKind::Io => {
                    // Look at the database through the same engine, faults paused.
                    store.set_fail_before(0.0);
                    store.set_fail_after(0.0);
                    let fp = fingerprint(&engine);
                    prop_assert_eq!(&observe(&inner, &opts)[..fp.len()], fp.as_str());
                    let after = resolve(&observe(&inner, &opts), &applied, op)?;
                    store.set_fail_before(p_before);
                    store.set_fail_after(p_after);
                    attempts += 1;
                    if op.is_mutation() && after {
                        applied.push(op.clone());
                    }
                    if after || !op.is_mutation() || attempts > 5 {
                        i += 1;
                        attempts = 0;
                    }
                }
                outcome => {
                    let expected = if op.is_mutation() {
                        apply_mutation(&model, op, &history).map_or(Ok(None), |r| r.map(Some)).map_err(|e| e.kind())
                    } else {
                        Ok(None)
                    };
                    prop_assert_eq!(outcome.map_err(|e| e.kind()), expected, "op {} {:?}", i, op);
                    if op.is_mutation() {
                        applied.push(op.clone());
                    }
                    i += 1;
                    attempts = 0;
                }
            }
        }
        store.set_fail_before(0.0);
        store.set_fail_after(0.0);
        let (model, _) = build_model(&applied);
        prop_assert_eq!(fingerprint(&engine), fingerprint(&model));
        let reopened = Engine::open(Arc::clone(&inner), opts).unwrap();
        prop_assert_eq!(fingerprint(&reopened), fingerprint(&model));
        assert_no_orphans_after_gc(&reopened)?;
    }
}

/// Exhaustive: one fixed workload, a crash at *every* store op, both effects.
#[test]
fn crash_at_every_op_of_a_fixed_workload() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::{Config, TestRunner};
    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        proptest::test_runner::TestRng::deterministic_rng(
            proptest::test_runner::RngAlgorithm::ChaCha,
        ),
    );
    for _ in 0..3 {
        let ops = workload_with_setup(arb_workload(12).new_tree(&mut runner).unwrap().current());
        let opts = test_options(2, 3, true);
        let total = dry_run(&ops, &opts);
        for n in 0..=total {
            for effect in [false, true] {
                crash_run(&ops, &opts, n, effect).unwrap_or_else(|e| {
                    panic!("crash at op {n} (effect {effect}) of {ops:#?}: {e}")
                });
            }
        }
    }
}
