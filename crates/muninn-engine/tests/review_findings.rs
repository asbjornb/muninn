//! Adversarial review findings: reproducers for bugs found while reviewing the storage
//! engine, plus extra differential fuzzing over shapes the main generator never produces
//! (extreme timestamps, many partitions per batch, many blocks per segment, sparse
//! series).
//!
//! Tests that reproduce a confirmed, still-open bug are `#[ignore = "review finding N"]`
//! so the suite stays green. Run them with
//!
//! ```sh
//! cargo test -p muninn-engine --test review_findings -- --ignored
//! ```

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::*;
use muninn_core::api::*;
use muninn_core::{
    Clock, ColumnDef, ColumnType, Database, Duration, ModelDb, Partitioning, Reader, TimeRange,
    Timestamp, Value,
};
use muninn_engine::{Engine, EngineOptions};
use muninn_store::{Key, MemoryStore, Store};
use proptest::prelude::*;

type Mem = Arc<MemoryStore>;

fn setup_t(e: &impl Database) {
    e.create_table(&CreateTable::new(
        "t",
        vec![ColumnDef::new("value", ColumnType::F64)],
    ))
    .unwrap();
    for s in ["a", "b"] {
        e.create_series(&CreateSeries::new(s, "t")).unwrap();
    }
}

fn row(series: &str, vt: i64, rt: i64, v: f64) -> WriteRow {
    WriteRow::new(series, h(vt), h(rt)).value("value", Value::F64(v))
}

fn all(table: &str) -> LatestRequest {
    LatestRequest {
        table: table.into(),
        ..LatestRequest::default()
    }
}

// ---------------------------------------------------------------------------------------
// Finding 1: GC on an engine whose view of the log is behind (another writer committed,
// which decision 0004 / SPEC-QUESTIONS 7 say is supported) treats the other writer's live
// segments as orphans and deletes them. `gc` only catches up when *its own* last commit
// failed (`fresh`), and when it has nothing to purge it commits nothing, so the lost race
// is never even detected.
// ---------------------------------------------------------------------------------------

#[test]
fn gc_on_lagging_engine_deletes_other_writers_live_segments() {
    let store: Mem = Arc::new(MemoryStore::new());
    let opts = file_options(4, 0, false); // gc_grace = 0 (stands in for "older than grace")
    let a = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
    setup_t(&a);
    // b opens after setup and writes; a never hears about it.
    let b = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
    b.write(&WriteBatch::new(h(1), vec![row("a", 0, 0, 1.0)]))
        .unwrap();
    let live_b = b.live_segment_keys();
    assert_eq!(live_b.len(), 1);

    // a (e.g. a maintenance process, or a writer that just hasn't written since) runs GC.
    let report = a.gc().unwrap();

    // The committed database must be intact.
    let fresh = Engine::open(Arc::clone(&store), opts).unwrap();
    let listed: BTreeSet<Key> = store.list("segments/").unwrap().into_iter().collect();
    assert!(
        live_b.is_subset(&listed),
        "gc deleted live segments {:?} (report {report:?})",
        live_b.difference(&listed).collect::<Vec<_>>()
    );
    assert_eq!(fresh.latest(&all("t")).unwrap().rows.len(), 1);
}

// ---------------------------------------------------------------------------------------
// Extra differential fuzzing: extreme timestamps, all partition granularities, batches
// spanning many partitions, many blocks per segment, sparse series ids.
// ---------------------------------------------------------------------------------------

const DAY: i64 = 86_400_000_000;

/// Timestamps around every boundary the engine does arithmetic on.
fn interesting_times() -> Vec<i64> {
    let min = Timestamp::MIN.as_micros();
    let max = Timestamp::MAX.as_micros();
    let iso = |s: &str| Timestamp::parse(s).unwrap().as_micros();
    let mut v = vec![
        min,
        min + 1,
        min + DAY - 1,
        min + DAY,
        iso("0001-02-01T00:00:00Z") - 1,
        iso("0001-02-01T00:00:00Z"),
        iso("0002-01-01T00:00:00Z") - 1,
        iso("0002-01-01T00:00:00Z"),
        iso("1969-12-01T00:00:00Z"),
        -DAY - 1,
        -DAY,
        -1,
        0,
        1,
        DAY - 1,
        DAY,
        iso("1970-02-01T00:00:00Z") - 1,
        iso("1970-02-01T00:00:00Z"),
        iso("1971-01-01T00:00:00Z") - 1,
        iso("1971-01-01T00:00:00Z"),
        iso("2024-02-29T23:59:59.999999Z"),
        iso("2024-03-01T00:00:00Z"),
        iso("2025-01-01T00:00:00Z"),
        iso("9999-12-01T00:00:00Z") - 1,
        iso("9999-12-31T00:00:00Z"),
        max - 1,
        max,
    ];
    v.sort();
    v.dedup();
    v
}

#[derive(Clone, Debug)]
enum XOp {
    Write {
        clock: usize,
        rows: Vec<(usize, usize, Option<usize>, usize, bool, u8)>,
    },
    Latest {
        table: usize,
        series: Vec<usize>,
        range: (Option<usize>, Option<usize>),
        clock: Option<Clock>,
        frontier: Option<usize>,
        system: Option<usize>,
        history: bool,
    },
    Replay {
        table: usize,
        range: (Option<usize>, Option<usize>),
        clock: Option<Clock>,
        arrival: (Option<usize>, Option<usize>),
    },
    Grid {
        table: usize,
        frontiers: Vec<usize>,
        window: Option<(i64, i64)>,
        range: (Option<usize>, Option<usize>),
        clock: Option<Clock>,
    },
    Asof {
        table: usize,
        left: Vec<(usize, usize, usize)>,
        clock: Option<Clock>,
    },
    Compact,
    Reopen,
    Gc,
    AddColumn,
}

const XTABLES: [(&str, Partitioning); 3] = [
    ("d", Partitioning::Day),
    ("m", Partitioning::Month),
    ("y", Partitioning::Year),
];
/// Series per table; ids interleave across tables (sparse within a table).
const XSERIES: [(&str, usize); 9] = [
    ("d1", 0),
    ("m1", 1),
    ("y1", 2),
    ("d2", 0),
    ("m2", 1),
    ("y2", 2),
    ("d3", 0),
    ("pad", 1),
    ("d4", 0),
];

fn xsetup(db: &impl Database) {
    for (name, p) in XTABLES {
        db.create_table(&CreateTable {
            name: name.into(),
            columns: vec![ColumnDef::new("value", ColumnType::F64)],
            default_clock: Clock::Received,
            partition: p,
            clock: None,
        })
        .unwrap();
    }
    for (s, t) in XSERIES {
        db.create_series(&CreateSeries::new(s, XTABLES[t].0))
            .unwrap();
    }
}

fn arb_xop(nt: usize) -> impl Strategy<Value = XOp> {
    let t = 0..nt;
    let ix = 0..nt;
    let clock = prop::option::of(prop::sample::select(vec![
        Clock::Received,
        Clock::Published,
        Clock::System,
    ]));
    let range = (prop::option::of(ix.clone()), prop::option::of(ix.clone()));
    let write = (
        // Writer clock: mostly early, so system bounds remain interesting.
        prop_oneof![10 => 0usize..14, 1 => t.clone()],
        prop::collection::vec(
            (
                0usize..XSERIES.len(),
                t.clone(),
                prop::option::weighted(0.5, t.clone()),
                t.clone(),
                prop::bool::weighted(0.15),
                0u8..3,
            ),
            0..40,
        ),
    )
        .prop_map(|(clock, rows)| XOp::Write { clock, rows });
    let latest = (
        0usize..3,
        prop::collection::vec(0usize..XSERIES.len(), 0..3),
        range.clone(),
        clock.clone(),
        prop::option::of(ix.clone()),
        prop::option::of(ix.clone()),
        any::<bool>(),
    )
        .prop_map(
            |(table, series, range, clock, frontier, system, history)| XOp::Latest {
                table,
                series,
                range,
                clock,
                frontier,
                system,
                history,
            },
        );
    let replay = (
        0usize..3,
        range.clone(),
        clock.clone(),
        (prop::option::of(ix.clone()), prop::option::of(ix.clone())),
    )
        .prop_map(|(table, range, clock, arrival)| XOp::Replay {
            table,
            range,
            clock,
            arrival,
        });
    let dur = prop_oneof![
        Just(0i64),
        Just(-DAY),
        Just(DAY),
        Just(36 * 3_600_000_000),
        Just(i64::MAX / 2),
        Just(-(i64::MAX / 2)),
        Just(i64::MAX),
        Just(i64::MIN + 1),
        -400 * DAY..400 * DAY,
    ];
    let grid = (
        0usize..3,
        prop::collection::vec(ix.clone(), 0..6),
        prop::option::of((dur.clone(), dur)),
        range,
        clock.clone(),
    )
        .prop_map(|(table, frontiers, window, range, clock)| XOp::Grid {
            table,
            frontiers,
            window: window.map(|(a, b)| (a.min(b), a.max(b))),
            range,
            clock,
        });
    let asof = (
        0usize..3,
        prop::collection::vec((0usize..XSERIES.len(), ix.clone(), ix), 0..8),
        clock,
    )
        .prop_map(|(table, left, clock)| XOp::Asof { table, left, clock });
    prop_oneof![
        10 => write,
        5 => latest,
        2 => replay,
        3 => grid,
        3 => asof,
        1 => Just(XOp::Compact),
        1 => Just(XOp::Reopen),
        1 => Just(XOp::Gc),
        1 => Just(XOp::AddColumn),
    ]
}

fn tr(times: &[i64], r: (Option<usize>, Option<usize>)) -> TimeRange {
    let t = |i: usize| Timestamp::from_micros(times[i]).unwrap();
    let (a, b) = (r.0.map(t), r.1.map(t));
    // Keep most ranges valid.
    match (a, b) {
        (Some(a), Some(b)) => TimeRange::new(Some(a.min(b)), Some(a.max(b))),
        _ => TimeRange::new(a, b),
    }
}

fn run_extreme(ops: &[XOp], opts: EngineOptions) -> Result<(), TestCaseError> {
    let times = interesting_times();
    let ts = |i: usize| Timestamp::from_micros(times[i]).unwrap();
    let store: Mem = Arc::new(MemoryStore::new());
    let model = ModelDb::new();
    let mut engine = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
    xsetup(&model);
    xsetup(&engine);
    let mut ncols = 1;
    for (step, op) in ops.iter().enumerate() {
        let series_of_table = |t: usize, pick: &[usize]| -> Vec<String> {
            pick.iter()
                .filter(|&&s| XSERIES[s].1 == t)
                .map(|&s| XSERIES[s].0.to_owned())
                .collect()
        };
        let render = |r: muninn_core::Result<String>| r.map_err(|e| e.kind());
        match op {
            XOp::Write { clock, rows } => {
                let rows = rows
                    .iter()
                    .map(|&(s, vt, pt, rt, del, v)| {
                        let mut w = WriteRow::new(XSERIES[s].0, ts(vt), ts(rt));
                        if let Some(p) = pt {
                            w = w.published(ts(p));
                        }
                        if del {
                            w = w.tombstone();
                        } else {
                            w = w.value("value", Value::F64(f64::from(v)));
                            if ncols > 1 && v == 2 {
                                w = w.value("extra", Value::F64(-0.0));
                            }
                        }
                        w
                    })
                    .collect();
                let b = WriteBatch::new(ts(*clock), rows);
                let x = render(model.write(&b).map(|r| format!("{r:?}")));
                let y = render(engine.write(&b).map(|r| format!("{r:?}")));
                prop_assert_eq!(y, x, "step {} write", step);
            }
            XOp::Latest {
                table,
                series,
                range,
                clock,
                frontier,
                system,
                history,
            } => {
                let req = LatestRequest {
                    table: XTABLES[*table].0.into(),
                    series: series_of_table(*table, series),
                    range: tr(&times, *range),
                    context: ReadContext {
                        clock: *clock,
                        frontier: frontier.map(ts),
                        system: system.map(ts),
                    },
                };
                let (x, y) = if *history {
                    (
                        render(model.history(&req).map(|r| format!("{r:?}"))),
                        render(engine.history(&req).map(|r| format!("{r:?}"))),
                    )
                } else {
                    (
                        render(model.latest(&req).map(|r| format!("{r:?}"))),
                        render(engine.latest(&req).map(|r| format!("{r:?}"))),
                    )
                };
                prop_assert_eq!(y, x, "step {} {:?}", step, req);
            }
            XOp::Replay {
                table,
                range,
                clock,
                arrival,
            } => {
                let req = ReplayRequest {
                    table: XTABLES[*table].0.into(),
                    series: vec![],
                    range: tr(&times, *range),
                    clock: *clock,
                    system: None,
                    arrival: tr(&times, *arrival),
                };
                let x = render(model.replay(&req).map(|r| format!("{r:?}")));
                let y = render(engine.replay(&req).map(|r| format!("{r:?}")));
                prop_assert_eq!(y, x, "step {} {:?}", step, req);
            }
            XOp::Grid {
                table,
                frontiers,
                window,
                range,
                clock,
            } => {
                let req = GridRequest {
                    table: XTABLES[*table].0.into(),
                    series: vec![],
                    frontiers: frontiers.iter().map(|&i| ts(i)).collect(),
                    window: match window {
                        Some((a, b)) => GridWindow::Relative {
                            start: Duration::from_micros(*a),
                            end: Duration::from_micros(*b),
                        },
                        None => GridWindow::Absolute(tr(&times, *range)),
                    },
                    clock: *clock,
                    system: None,
                };
                let x = render(model.grid(&req).map(|r| format!("{r:?}")));
                let y = render(engine.grid(&req).map(|r| format!("{r:?}")));
                // Finding 2 (fixed): relative windows past MAX are checked too.
                prop_assert_eq!(&y, &x, "step {} {:?}", step, req);
                let snap = engine.snapshot().unwrap();
                let z = render(snap.grid_by_repeated_latest(&req).map(|r| format!("{r:?}")));
                prop_assert_eq!(&z, &x, "step {} by definition {:?}", step, req);
            }
            XOp::Asof { table, left, clock } => {
                let left: Vec<AsofLeftRow> = left
                    .iter()
                    .filter(|(s, _, _)| XSERIES[*s].1 == *table)
                    .map(|&(s, vt, f)| AsofLeftRow {
                        series: XSERIES[s].0.into(),
                        value_time: ts(vt),
                        frontier: ts(f),
                    })
                    .collect();
                let req = AsofJoinRequest {
                    table: XTABLES[*table].0.into(),
                    clock: *clock,
                    system: None,
                    left,
                };
                let x = render(model.asof_join(&req).map(|r| format!("{r:?}")));
                let y = render(engine.asof_join(&req).map(|r| format!("{r:?}")));
                prop_assert_eq!(y, x, "step {} {:?}", step, req);
            }
            XOp::Compact => {
                engine.compact_all().unwrap();
            }
            XOp::Reopen => {
                drop(engine);
                engine = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
            }
            XOp::Gc => {
                engine.gc().unwrap();
            }
            XOp::AddColumn => {
                if ncols == 1 {
                    for (t, _) in XTABLES {
                        let req = AddColumn {
                            table: t.into(),
                            column: ColumnDef::new("extra", ColumnType::F64),
                            clock: None,
                        };
                        model.add_column(&req).unwrap();
                        engine.add_column(&req).unwrap();
                    }
                    ncols = 2;
                }
            }
        }
    }
    let reopened = Engine::open(Arc::clone(&store), opts).unwrap();
    prop_assert_eq!(fingerprint(&reopened), fingerprint(&model));
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(64), max_shrink_iters: 4000, ..ProptestConfig::default() })]

    /// Engine ≡ model on extreme timestamps (year 0001 / 9999, pre-1970, partition
    /// boundaries ± 1µs) across day / month / year tables, with relative grid windows
    /// whose sums overflow the valid range.
    #[test]
    fn extreme_timestamps_match_model(
        ops in prop::collection::vec(arb_xop(interesting_times().len()), 1..ops_len(30)),
        block_rows in 1usize..4,
        checkpoint in 0u64..4,
        compress in any::<bool>(),
        tail_read in prop::sample::select(vec![16u64, 17, 40, 200, 64 * 1024]),
        footer_cache in prop::sample::select(vec![0usize, 1, 2, 4096]),
    ) {
        let opts = EngineOptions {
            tail_read,
            footer_cache_entries: footer_cache,
            ..test_options(block_rows, checkpoint, compress)
        };
        run_extreme(&ops, opts)?;
    }
}

// ---------------------------------------------------------------------------------------
// Finding 2: single-pass grid with a relative window drops rows when the *last*
// frontier's window lies entirely past 9999-12-31 (F + window_start > MAX). Then
// `relative_window` returns the empty sentinel `[MAX, MAX)`, and `grid` builds the union
// scan range as `[first.from, last.to)` = `[.., MAX)`, which excludes value_time = MAX even
// though earlier frontiers' windows contain it (read.rs, `grid`, union range).
// ---------------------------------------------------------------------------------------

#[test]
fn grid_relative_window_past_max_drops_rows_of_earlier_frontiers() {
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Engine::open(store, test_options(4, 0, false)).unwrap();
    let m = ModelDb::new();
    for db in [&e as &dyn DynDb, &m] {
        db.setup();
    }
    let max = Timestamp::MAX;
    let w = WriteBatch::new(
        h(0),
        vec![WriteRow::new("a", max, h(0)).value("value", Value::F64(1.0))],
    );
    e.write(&w).unwrap();
    m.write(&w).unwrap();
    let req = GridRequest {
        table: "t".into(),
        series: vec![],
        // F1: window [F1 + 1h, ∞) contains MAX. F2: F2 + 1h > MAX, window empty.
        frontiers: vec![
            h(0),
            Timestamp::from_micros(max.as_micros() - 1000).unwrap(),
        ],
        window: GridWindow::Relative {
            start: Duration::from_micros(3_600_000_000),
            end: Duration::from_micros(i64::MAX),
        },
        clock: None,
        system: None,
    };
    let expected = m.grid(&req).unwrap();
    assert_eq!(expected.rows.len(), 1);
    let by_def = e.snapshot().unwrap().grid_by_repeated_latest(&req).unwrap();
    assert_eq!(format!("{by_def:?}"), format!("{expected:?}"));
    assert_eq!(
        format!("{:?}", e.grid(&req).unwrap()),
        format!("{expected:?}")
    );
}

/// Object-safe helper so one setup runs on both implementations.
trait DynDb {
    fn setup(&self);
}
impl<D: Database> DynDb for D {
    fn setup(&self) {
        setup_t(self);
    }
}

// ---------------------------------------------------------------------------------------
// Two writers on one store (decision 0004: "conflicting writers are safe without a
// lease"). Random interleavings of writes, compactions, checkpoints and refreshes on two
// engines; every committed write must be in the final database exactly once, as in the
// model. GC runs only right after a refresh of the engine running it (finding 1 covers the
// lagging case).
// ---------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum TwoOp {
    Write(bool, Vec<(usize, i64, i64, u8)>, Option<u8>),
    Compact(bool),
    Checkpoint(bool),
    Refresh(bool),
    GcAfterRefresh(bool),
    Reopen(bool),
}

fn arb_two_op() -> impl Strategy<Value = TwoOp> {
    prop_oneof![
        8 => (any::<bool>(), prop::collection::vec((0usize..2, 0i64..72, 0i64..20, 0u8..3), 0..8),
              prop::option::weighted(0.3, 0u8..4))
            .prop_map(|(w, r, b)| TwoOp::Write(w, r, b)),
        2 => any::<bool>().prop_map(TwoOp::Compact),
        1 => any::<bool>().prop_map(TwoOp::Checkpoint),
        2 => any::<bool>().prop_map(TwoOp::Refresh),
        1 => any::<bool>().prop_map(TwoOp::GcAfterRefresh),
        1 => any::<bool>().prop_map(TwoOp::Reopen),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(64), ..ProptestConfig::default() })]

    #[test]
    fn two_writers_never_lose_or_duplicate_commits(
        ops in prop::collection::vec(arb_two_op(), 1..ops_len(40)),
        block_rows in 1usize..4,
        checkpoint in 0u64..4,
    ) {
        let store: Mem = Arc::new(MemoryStore::new());
        let opts = test_options(block_rows, checkpoint, false);
        let model = ModelDb::new();
        setup_t(&model);
        let mut engines = {
            let a = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
            setup_t(&a);
            [a, Engine::open(Arc::clone(&store), opts.clone()).unwrap()]
        };
        for (step, op) in ops.iter().enumerate() {
            match op {
                TwoOp::Write(which, rows, batch) => {
                    let e = &engines[usize::from(*which)];
                    let batch = WriteBatch {
                        batch_id: batch.map(|b| format!("b{b}")),
                        ..WriteBatch::new(
                            h(step as i64),
                            rows.iter()
                                .map(|&(s, vt, rt, v)| row(["a", "b"][s], vt, rt, f64::from(v)))
                                .collect(),
                        )
                    };
                    let got = match e.write(&batch) {
                        Err(err) if err.kind() == muninn_core::ErrorKind::Conflict => e.write(&batch),
                        other => other,
                    };
                    let want = model.write(&batch);
                    // system_time / seq may legitimately differ only if the engines
                    // disagree with the model about `last`; they must not.
                    prop_assert_eq!(
                        got.map(|r| format!("{r:?}")).map_err(|e| e.kind()),
                        want.map(|r| format!("{r:?}")).map_err(|e| e.kind()),
                        "step {}", step
                    );
                }
                TwoOp::Compact(which) => {
                    let e = &engines[usize::from(*which)];
                    let r = e.compact_all();
                    match r {
                        Err(err) if err.kind() == muninn_core::ErrorKind::Conflict => {}
                        r => { r.unwrap(); }
                    }
                }
                TwoOp::Checkpoint(which) => engines[usize::from(*which)].checkpoint().unwrap(),
                TwoOp::Refresh(which) => { engines[usize::from(*which)].refresh().unwrap(); }
                // Finding 1 (fixed): GC on a possibly lagging engine must establish the
                // log head itself, so no refresh first.
                TwoOp::GcAfterRefresh(which) => {
                    let e = &engines[usize::from(*which)];
                    match e.gc() {
                        Err(err) if err.kind() == muninn_core::ErrorKind::Conflict => {}
                        r => { r.unwrap(); }
                    }
                }
                TwoOp::Reopen(which) => {
                    let i = usize::from(*which);
                    engines[i] = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
                }
            }
        }
        for e in &engines {
            e.refresh().unwrap();
            prop_assert_eq!(fingerprint(e), fingerprint(&model));
        }
        let fresh = Engine::open(Arc::clone(&store), opts).unwrap();
        prop_assert_eq!(fingerprint(&fresh), fingerprint(&model));
    }
}

// ---------------------------------------------------------------------------------------
// Finding 3: an engine whose view lags behind another writer's compaction + GC (by more
// than the grace period; grace 0 here) fails every read and write with `io` ("reading
// segments/...: not found") forever: a missing *live* segment is not treated as "my state
// is stale", so `fresh()` never reloads. Only an explicit `refresh()` or reopen recovers
// (SPEC §7.6: recovery never needs operator action).
// ---------------------------------------------------------------------------------------

#[test]
fn lagging_engine_recovers_after_its_segments_were_compacted_and_collected() {
    let store: Mem = Arc::new(MemoryStore::new());
    let opts = file_options(4, 0, false);
    let a = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
    setup_t(&a);
    let b = Engine::open(Arc::clone(&store), opts).unwrap();
    b.write(&WriteBatch::new(h(1), vec![row("a", 9, 0, 1.0)]))
        .unwrap();
    a.refresh().unwrap();
    a.write(&WriteBatch::new(h(2), vec![row("a", 0, 0, 1.0)]))
        .unwrap();
    assert_eq!(a.compact_all().unwrap(), 1);
    a.gc().unwrap(); // deletes b's (now retired) segment
    // b lags: its state still lists the deleted segment as live. The read notices the
    // missing segment, reloads from the log and retries once, so it succeeds.
    let first = b
        .latest(&all("t"))
        .expect("reload-and-retry on a missing segment");
    assert_eq!(first.rows.len(), 2);
    let again = b.latest(&all("t")).unwrap();
    assert_eq!(again.rows.len(), 2);
}

// ---------------------------------------------------------------------------------------
// Finding 5: checkpoints are an optimisation (the log is the source of truth), but a
// damaged checkpoint — the one the hint names, or the newest one found by listing — makes
// `Engine::open` fail outright instead of falling back to an older checkpoint or a full
// replay of the intact log (engine.rs `load`: `checkpoint_state(..)?`).
// ---------------------------------------------------------------------------------------

#[test]
fn damaged_checkpoint_falls_back_to_the_log() {
    let store: Mem = Arc::new(MemoryStore::new());
    let opts = file_options(4, 0, false);
    let e = Engine::open(Arc::clone(&store), opts.clone()).unwrap();
    setup_t(&e);
    e.write(&WriteBatch::new(h(1), vec![row("a", 0, 0, 1.0)]))
        .unwrap();
    e.checkpoint().unwrap();
    let expected = fingerprint(&e);
    let ck = muninn_engine::log::checkpoint_key(e.version());
    store.delete(&ck).unwrap();
    store.put(&ck, b"{\"format\": 1, \"version\"").unwrap(); // torn / bit-rotted
    let reopened = Engine::open(Arc::clone(&store), opts).expect("log is intact");
    assert_eq!(fingerprint(&reopened), expected);
}
