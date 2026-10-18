//! Shared test harness: a random workload generator (catalog ops, batches with
//! duplicates, tombstones, null published times, batch ids, clock regressions and invalid
//! rows; every read kind with random contexts; compaction, reopen, GC and pinned
//! snapshots) and a driver that runs a workload against the engine and `ModelDb` in
//! lockstep, comparing every result and error.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use muninn_core::api::*;
use muninn_core::model::ModelSnapshot;
use muninn_core::{
    Clock, ColumnDef, ColumnType, Database, Duration, Error, ErrorKind, ModelDb, Partitioning,
    Reader, TimeRange, Timestamp, Value,
};
use muninn_engine::{Engine, EngineOptions, EngineSnapshot, WriteOptions};
use muninn_store::{MemoryStore, Store};
use proptest::prelude::*;
use proptest::sample::Index;

pub const HOUR: i64 = 3_600_000_000;
pub const BASE: i64 = 1_735_689_600_000_000; // 2025-01-01T00:00:00Z

pub fn h(x: i64) -> Timestamp {
    Timestamp::from_micros(BASE + x * HOUR).unwrap()
}

/// Number of proptest cases: `MUNINN_FUZZ_CASES` or `default`.
pub fn cases(default: u32) -> u32 {
    std::env::var("MUNINN_FUZZ_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

pub const TABLES: [&str; 4] = ["t", "u", "w", "Bad"];
/// Column names with the type they usually get.
pub const COLUMNS: [(&str, ColumnType); 7] = [
    ("value", ColumnType::F64),
    ("note", ColumnType::String),
    ("n", ColumnType::I64),
    ("flag", ColumnType::Bool),
    ("at", ColumnType::Timestamp),
    ("x", ColumnType::F64),
    ("seq", ColumnType::I64), // reserved → invalid_name
];
pub const SERIES: [&str; 7] = ["a", "b", "c", "d", "e", "", "zz"];
const TYPES: [ColumnType; 5] = [
    ColumnType::F64,
    ColumnType::I64,
    ColumnType::Bool,
    ColumnType::String,
    ColumnType::Timestamp,
];
const CLOCKS: [Clock; 3] = [Clock::Received, Clock::Published, Clock::System];
const PARTS: [Partitioning; 3] = [Partitioning::Day, Partitioning::Month, Partitioning::Year];

#[derive(Clone, Debug)]
pub enum GenTime {
    Hours(i64),
    OutOfRange,
    Garbage,
}

impl GenTime {
    fn input(&self) -> TimeInput {
        match self {
            GenTime::Hours(x) => h(*x).into(),
            GenTime::OutOfRange => TimeInput::Micros(i64::MAX),
            GenTime::Garbage => TimeInput::Text("2025-13-01T00:00:00Z".into()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct GenRow {
    pub series: usize,
    pub vt: GenTime,
    pub pt: Option<i64>,
    pub rt: GenTime,
    pub deleted: bool,
    pub values: Vec<(usize, Option<Value>)>,
}

#[derive(Clone, Debug)]
pub enum Op {
    CreateTable {
        name: usize,
        columns: Vec<(usize, ColumnType)>,
        clock: Clock,
        partition: Partitioning,
    },
    AddColumn {
        table: usize,
        column: usize,
        ty: ColumnType,
    },
    CreateSeries {
        name: usize,
        table: usize,
    },
    Write {
        clock: i64,
        batch_id: Option<u8>,
        rows: Vec<GenRow>,
    },
    /// Re-send the rows of an earlier write (by index among executed writes).
    Resend {
        which: Index,
        clock: i64,
        batch_id: Option<u8>,
    },
    Latest {
        req: LatestRequest,
        history: bool,
    },
    Replay(ReplayRequest),
    Grid(GridRequest),
    Asof(AsofJoinRequest),
    Compact,
    Reopen,
    Gc,
    /// Pin a snapshot of both implementations.
    Pin,
    /// Read every table's history on a pinned snapshot and compare.
    ReadPinned(Index),
}

impl Op {
    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            Op::CreateTable { .. }
                | Op::AddColumn { .. }
                | Op::CreateSeries { .. }
                | Op::Write { .. }
                | Op::Resend { .. }
        )
    }
}

fn arb_value(ty: ColumnType) -> BoxedStrategy<Value> {
    match ty {
        ColumnType::F64 => prop_oneof![
            (0i32..3).prop_map(|x| Value::F64(f64::from(x))),
            Just(Value::F64(f64::NAN)),
            Just(Value::F64(-0.0)),
            Just(Value::F64(f64::INFINITY)),
            (0i64..3).prop_map(Value::I64),
            Just(Value::String("NaN".into())),
        ]
        .boxed(),
        ColumnType::I64 => prop_oneof![
            (-2i64..3).prop_map(Value::I64),
            Just(Value::String("9223372036854775807".into())),
        ]
        .boxed(),
        ColumnType::Bool => any::<bool>().prop_map(Value::Bool).boxed(),
        ColumnType::String => prop_oneof![Just("x"), Just("y"), Just("é")]
            .prop_map(|s| Value::String(s.into()))
            .boxed(),
        ColumnType::Timestamp => prop_oneof![
            (0i64..3).prop_map(|x| Value::Timestamp(h(x))),
            Just(Value::String("2025-01-01T01:00:00+01:00".into())),
        ]
        .boxed(),
    }
}

/// Columns usually written for a series: t's for a/b (and the created d), u's for c (and e).
fn usual_columns(series: usize) -> &'static [usize] {
    match series {
        0 | 1 | 3 => &[0, 1],
        2 | 4 => &[2, 3, 4],
        _ => &[0, 2],
    }
}

/// Values for a row of `series`: usually the right columns with the right types; rarely
/// an unknown column, a wrong type or an explicit null.
fn arb_values(series: usize) -> BoxedStrategy<Vec<(usize, Option<Value>)>> {
    let cols = usual_columns(series).to_vec();
    let one = (prop::sample::select(cols), 0u8..40).prop_flat_map(|(c, roll)| {
        let v: BoxedStrategy<Option<Value>> = match roll {
            0 => prop::sample::select(TYPES.to_vec())
                .prop_flat_map(arb_value)
                .prop_map(Some)
                .boxed(),
            1..=3 => Just(None).boxed(),
            _ => arb_value(COLUMNS[c].1).prop_map(Some).boxed(),
        };
        let c = if roll == 39 { 5 } else { c }; // "x": usually unknown
        (Just(c), v)
    });
    prop::collection::vec(one, 0..3).boxed()
}

fn arb_time(max: i64, bad: f64) -> impl Strategy<Value = GenTime> {
    prop_oneof![
        ((1.0 - bad) * 1000.0) as u32 => (0..max).prop_map(GenTime::Hours),
        (bad * 500.0) as u32 + 1 => Just(GenTime::OutOfRange),
        (bad * 500.0) as u32 + 1 => Just(GenTime::Garbage),
    ]
}

fn arb_row() -> impl Strategy<Value = GenRow> {
    (
        prop_oneof![150 => 0usize..3, 2 => 3usize..5, 1 => 5usize..7],
        0u8..60,
    )
        .prop_flat_map(|(series, roll)| {
            (
                Just(series),
                arb_time(72, 0.005),
                prop::option::weighted(0.6, 0i64..20),
                arb_time(20, 0.005),
                Just(roll < 8),
                arb_values(series),
                Just(roll == 59),
            )
        })
        .prop_map(
            |(series, vt, pt, rt, deleted, values, bad_tombstone)| GenRow {
                series,
                vt,
                pt,
                rt,
                deleted,
                // Tombstones without values (rarely with: tombstone_with_values).
                values: if deleted && !bad_tombstone {
                    vec![]
                } else {
                    values
                },
            },
        )
}

fn arb_range(max: i64) -> impl Strategy<Value = TimeRange> {
    (
        prop::option::of(0..max),
        prop::option::of(0..max),
        prop::bool::weighted(0.05),
    )
        .prop_map(|(a, len, invalid)| {
            let to = match (a, len) {
                (Some(a), Some(l)) if invalid => Some(a - l - 1),
                (Some(a), Some(l)) => Some(a + l),
                (None, l) => l,
                (Some(_), None) => None,
            };
            TimeRange::new(a.map(h), to.map(h))
        })
}

fn arb_filter(table: usize) -> BoxedStrategy<Vec<String>> {
    let own: Vec<usize> = match table {
        0 => vec![0, 1],
        1 => vec![2],
        _ => vec![0, 2, 3, 4],
    };
    prop_oneof![
        8 => Just(vec![]),
        8 => prop::collection::vec(prop::sample::select(own), 1..3)
            .prop_map(|v| v.into_iter().map(|i| SERIES[i].to_owned()).collect()),
        1 => prop::collection::vec(0usize..7, 1..3)
            .prop_map(|v| v.into_iter().map(|i| SERIES[i].to_owned()).collect()),
    ]
    .boxed()
}

/// A table index (usually t or u) and a matching series filter.
fn arb_table() -> impl Strategy<Value = (String, Vec<String>)> {
    prop_oneof![40 => 0usize..2, 2 => 2usize..3, 1 => 3usize..4]
        .prop_flat_map(|t| (Just(TABLES[t].to_owned()), arb_filter(t)))
}

fn arb_clock() -> impl Strategy<Value = Option<Clock>> {
    prop::option::of(prop::sample::select(CLOCKS.to_vec()))
}

fn arb_read() -> impl Strategy<Value = Op> {
    let latest = (
        arb_table(),
        arb_range(80),
        arb_clock(),
        prop::option::of(0i64..30),
        prop::option::of(0i64..40),
        any::<bool>(),
    )
        .prop_map(
            |((table, series), range, clock, frontier, system, history)| Op::Latest {
                req: LatestRequest {
                    table,
                    series,
                    range,
                    context: ReadContext {
                        clock,
                        frontier: frontier.map(h),
                        system: system.map(h),
                    },
                },
                history,
            },
        );
    let replay = (
        arb_table(),
        arb_range(80),
        arb_clock(),
        prop::option::of(0i64..40),
        arb_range(40),
    )
        .prop_map(|((table, series), range, clock, system, arrival)| {
            Op::Replay(ReplayRequest {
                table,
                series,
                range,
                clock,
                system: system.map(h),
                arrival,
            })
        });
    let window = prop_oneof![
        arb_range(80).prop_map(GridWindow::Absolute),
        (-30i64..30, prop_oneof![1 => -5i64..0, 20 => 0i64..40]).prop_map(|(s, len)| {
            GridWindow::Relative {
                start: Duration::from_micros(s * HOUR),
                end: Duration::from_micros((s + len) * HOUR),
            }
        }),
    ];
    let grid = (
        arb_table(),
        prop::collection::vec(0i64..40, 0..6),
        window,
        arb_clock(),
        prop::option::of(0i64..40),
    )
        .prop_map(|((table, series), fs, window, clock, system)| {
            Op::Grid(GridRequest {
                table,
                series,
                frontiers: fs.into_iter().map(h).collect(),
                window,
                clock,
                system: system.map(h),
            })
        });
    let asof = prop_oneof![20 => 0usize..2, 1 => 2usize..4].prop_flat_map(|t| {
        let own: Vec<usize> = match t {
            0 => vec![0, 1],
            1 => vec![2],
            _ => vec![0, 2],
        };
        let series = prop_oneof![60 => prop::sample::select(own), 1 => 0usize..7];
        (
            Just(TABLES[t].to_owned()),
            arb_clock(),
            prop::option::of(0i64..40),
            prop::collection::vec((series, 0i64..72, 0i64..30), 0..6),
        )
            .prop_map(|(table, clock, system, left)| {
                Op::Asof(AsofJoinRequest {
                    table,
                    clock,
                    system: system.map(h),
                    left: left
                        .into_iter()
                        .map(|(s, vt, f)| AsofLeftRow {
                            series: SERIES[s].to_owned(),
                            value_time: h(vt),
                            frontier: h(f),
                        })
                        .collect(),
                })
            })
    });
    prop_oneof![4 => latest, 2 => replay, 2 => grid, 2 => asof]
}

pub fn arb_mutation() -> impl Strategy<Value = Op> {
    let create_table = (
        0usize..4,
        prop::collection::vec(
            (
                0usize..COLUMNS.len(),
                prop_oneof![9 => Just(None), 1 => prop::sample::select(TYPES.to_vec()).prop_map(Some)],
            ),
            0..4,
        ),
        prop::sample::select(CLOCKS.to_vec()),
        prop::sample::select(PARTS.to_vec()),
    )
        .prop_map(|(name, cols, clock, partition)| Op::CreateTable {
            name,
            columns: cols
                .into_iter()
                .map(|(c, ty)| (c, ty.unwrap_or(COLUMNS[c].1)))
                .collect(),
            clock,
            partition,
        });
    let add_column = (0usize..4, 0usize..COLUMNS.len()).prop_map(|(table, column)| Op::AddColumn {
        table,
        column,
        ty: COLUMNS[column].1,
    });
    let create_series = (0usize..SERIES.len(), 0usize..4)
        .prop_map(|(name, table)| Op::CreateSeries { name, table });
    let write = (
        0i64..30,
        prop::option::weighted(0.3, 0u8..4),
        prop::collection::vec(arb_row(), 0..10),
    )
        .prop_map(|(clock, batch_id, rows)| Op::Write {
            clock,
            batch_id,
            rows,
        });
    let resend = (
        any::<Index>(),
        0i64..30,
        prop::option::weighted(0.2, 0u8..4),
    )
        .prop_map(|(which, clock, batch_id)| Op::Resend {
            which,
            clock,
            batch_id,
        });
    prop_oneof![
        1 => create_table,
        1 => add_column,
        1 => create_series,
        12 => write,
        3 => resend,
    ]
}

/// A workload: mutations, reads and physical operations.
pub fn arb_ops(len: usize) -> impl Strategy<Value = Vec<Op>> {
    let op = prop_oneof![
        10 => arb_mutation(),
        8 => arb_read(),
        1 => Just(Op::Compact),
        1 => Just(Op::Reopen),
        1 => Just(Op::Gc),
        1 => Just(Op::Pin),
        1 => any::<Index>().prop_map(Op::ReadPinned),
    ];
    prop::collection::vec(op, 1..len)
}

/// The fixed start of every workload: `t(value f64, note string)` by day, `u(n i64, flag
/// bool, at timestamp)` by month (default clock published), series a, b in t, c in u.
pub fn setup_ops() -> Vec<Op> {
    vec![
        Op::CreateTable {
            name: 0,
            columns: vec![(0, ColumnType::F64), (1, ColumnType::String)],
            clock: Clock::Received,
            partition: Partitioning::Day,
        },
        Op::CreateTable {
            name: 1,
            columns: vec![
                (2, ColumnType::I64),
                (3, ColumnType::Bool),
                (4, ColumnType::Timestamp),
            ],
            clock: Clock::Published,
            partition: Partitioning::Month,
        },
        Op::CreateSeries { name: 0, table: 0 },
        Op::CreateSeries { name: 1, table: 0 },
        Op::CreateSeries { name: 2, table: 1 },
    ]
}

/// Engine options for tests: tiny blocks, frequent checkpoints, no GC grace.
pub fn test_options(block_rows: usize, checkpoint_interval: u64, compress: bool) -> EngineOptions {
    EngineOptions {
        checkpoint_interval,
        segment: WriteOptions {
            block_rows,
            compress,
            zstd_level: 1,
        },
        gc_grace: std::time::Duration::ZERO,
        ..EngineOptions::default()
    }
}

/// Builds a write batch from a `Write`/`Resend` op (resolving resends against
/// `history`, the rows of executed writes).
pub fn batch_of(op: &Op, history: &[Vec<GenRow>]) -> Option<(WriteBatch, Vec<GenRow>)> {
    let (clock, batch_id, rows) = match op {
        Op::Write {
            clock,
            batch_id,
            rows,
        } => (*clock, *batch_id, rows.clone()),
        Op::Resend {
            which,
            clock,
            batch_id,
        } => {
            if history.is_empty() {
                return None;
            }
            (
                *clock,
                *batch_id,
                history[which.index(history.len())].clone(),
            )
        }
        _ => return None,
    };
    let write_rows = rows
        .iter()
        .map(|r| {
            let mut w = WriteRow {
                series: SERIES[r.series].to_owned(),
                value_time: r.vt.input(),
                published_time: r.pt.map(|p| h(p).into()),
                received_time: r.rt.input(),
                deleted: r.deleted,
                values: BTreeMap::new(),
            };
            for (c, v) in &r.values {
                w.values.insert(COLUMNS[*c].0.to_owned(), v.clone());
            }
            w
        })
        .collect();
    Some((
        WriteBatch {
            rows: write_rows,
            batch_id: batch_id.map(|b| format!("batch-{b}")),
            clock: h(clock),
        },
        rows,
    ))
}

/// Applies a catalog or write op to any database; `Ok(None)` for non-mutations.
pub fn apply_mutation<D: Database>(
    db: &D,
    op: &Op,
    history: &[Vec<GenRow>],
) -> Option<Result<String, Error>> {
    Some(match op {
        Op::CreateTable {
            name,
            columns,
            clock,
            partition,
        } => db
            .create_table(&CreateTable {
                name: TABLES[*name].to_owned(),
                columns: columns
                    .iter()
                    .map(|(c, ty)| ColumnDef::new(COLUMNS[*c].0, *ty))
                    .collect(),
                default_clock: *clock,
                partition: *partition,
                clock: None,
            })
            .map(|d| format!("{d:?}")),
        Op::AddColumn { table, column, ty } => db
            .add_column(&AddColumn {
                table: TABLES[*table].to_owned(),
                column: ColumnDef::new(COLUMNS[*column].0, *ty),
                clock: None,
            })
            .map(|d| format!("{d:?}")),
        Op::CreateSeries { name, table } => db
            .create_series(&CreateSeries::new(SERIES[*name], TABLES[*table]))
            .map(|d| format!("{d:?}")),
        Op::Write { .. } | Op::Resend { .. } => {
            let (batch, _) = batch_of(op, history)?;
            db.write(&batch).map(|r| format!("{r:?}"))
        }
        _ => return None,
    })
}

/// Runs a read op against a reader, rendering the outcome comparably (errors by kind).
pub fn read<R: Reader>(r: &R, op: &Op) -> Option<Result<String, ErrorKind>> {
    let out = match op {
        Op::Latest {
            req,
            history: false,
        } => r.latest(req).map(|x| format!("{x:?}")),
        Op::Latest { req, history: true } => r.history(req).map(|x| format!("{x:?}")),
        Op::Replay(req) => r.replay(req).map(|x| format!("{x:?}")),
        Op::Grid(req) => r.grid(req).map(|x| format!("{x:?}")),
        Op::Asof(req) => r.asof_join(req).map(|x| format!("{x:?}")),
        _ => return None,
    };
    Some(out.map_err(|e| e.kind()))
}

/// Everything observable: the catalog and the full history of every table.
pub fn fingerprint<R: Reader>(r: &R) -> String {
    let mut out = String::new();
    for t in r.tables().unwrap() {
        out += &format!("{t:?}\n{:?}\n", r.table_series(&t.name).unwrap());
        let req = LatestRequest {
            table: t.name.clone(),
            ..LatestRequest::default()
        };
        out += &format!("{:?}\n", r.history(&req).unwrap());
    }
    out
}

pub type MemEngine = Engine<Arc<MemoryStore>>;

/// Runs `ops` (after [`setup_ops`]) against a fresh engine on `store` and a model,
/// comparing every outcome. Returns the engine for further checks.
pub fn run_differential<S: Store + Clone>(
    store: S,
    opts: EngineOptions,
    ops: &[Op],
) -> Result<Engine<S>, TestCaseError> {
    let model = ModelDb::new();
    let mut engine = Engine::open(store.clone(), opts.clone()).unwrap();
    let mut history: Vec<Vec<GenRow>> = Vec::new();
    let mut pinned: Vec<(EngineSnapshot<S>, ModelSnapshot, String)> = Vec::new();
    for (i, op) in setup_ops().iter().chain(ops).enumerate() {
        match op {
            Op::Compact => {
                engine.compact_all().unwrap();
            }
            Op::Reopen => {
                // Snapshots of the old instance are invisible to the new one's GC
                // (cross-process readers are protected by the grace period only, which
                // is zero here), so they are dropped with it.
                pinned.clear();
                drop(engine);
                engine = Engine::open(store.clone(), opts.clone()).unwrap();
            }
            Op::Gc => {
                engine.gc().unwrap();
            }
            Op::Pin => {
                let (e, m) = (engine.snapshot().unwrap(), model.snapshot().unwrap());
                let fp = fingerprint(&m);
                prop_assert_eq!(&fingerprint(&e), &fp, "step {}: pinned snapshot differs", i);
                pinned.push((e, m, fp));
            }
            Op::ReadPinned(ix) => {
                if !pinned.is_empty() {
                    let (e, m, fp) = &pinned[ix.index(pinned.len())];
                    prop_assert_eq!(&fingerprint(m), fp);
                    prop_assert_eq!(&fingerprint(e), fp, "step {}: snapshot changed", i);
                }
            }
            _ if op.is_mutation() => {
                let a = apply_mutation(&model, op, &history);
                let b = apply_mutation(&engine, op, &history);
                let norm = |r: Option<Result<String, Error>>| r.map(|r| r.map_err(|e| e.kind()));
                let (a, b) = (norm(a), norm(b));
                prop_assert_eq!(&b, &a, "step {}: {:?}", i, op);
                if let Some((_, rows)) = batch_of(op, &history)
                    && matches!(a, Some(Ok(_)))
                {
                    history.push(rows);
                }
            }
            _ => {
                let a = read(&model, op);
                let b = read(&engine, op);
                prop_assert_eq!(&b, &a, "step {}: {:?}", i, op);
                // Grid: the single-pass implementation equals the repeated-latest
                // definition (SPEC §7.4).
                if let Op::Grid(req) = op {
                    let snap = engine.snapshot().unwrap();
                    let by_def = snap.grid_by_repeated_latest(req).map(|x| format!("{x:?}"));
                    prop_assert_eq!(
                        by_def.map_err(|e| e.kind()),
                        snap.grid(req)
                            .map(|x| format!("{x:?}"))
                            .map_err(|e| e.kind())
                    );
                }
            }
        }
    }
    prop_assert_eq!(
        fingerprint(&engine),
        fingerprint(&model),
        "final state differs"
    );
    prop_assert_eq!(engine.catalog_log().unwrap(), model.catalog_log());
    // A reopened engine sees exactly the same database.
    let reopened = Engine::open(store, opts).unwrap();
    prop_assert_eq!(
        fingerprint(&reopened),
        fingerprint(&model),
        "reopened state differs"
    );
    Ok(engine)
}
