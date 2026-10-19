//! Skipping data must never change a result: compaction by system-time period (segments
//! with narrow knowledge-time ranges, so frontiers, replay windows and system bounds skip
//! them), the streaming latest/grid reductions, and the lead-time filter of
//! `history_lead` (segments skipped by their column statistics). Differential against
//! the model, plus checks that the skipping actually happens.

mod common;

use std::sync::Arc;

use common::*;
use muninn_core::api::*;
use muninn_core::{ColumnDef, ColumnType, Database, Duration, Reader, TimeRange, Value};
use muninn_engine::{Engine, EngineOptions, LeadFilter};
use muninn_store::{FaultyStore, MemoryStore, OpKind};
use proptest::prelude::*;

type Mem = Arc<MemoryStore>;
const DAY: i64 = 24;

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(300), ..ProptestConfig::default() })]

    /// The random workload (every read kind, compaction, reopen, GC) with either
    /// compaction policy and tiny blocks.
    #[test]
    fn engine_matches_model_with_either_compaction(
        ops in arb_ops(ops_len(40)),
        by_system_time in any::<bool>(),
        block_rows in 1usize..4,
    ) {
        let o = EngineOptions {
            compact_by_system_time: by_system_time,
            ..test_options(block_rows, 3, false)
        };
        run_differential(Arc::new(MemoryStore::new()), o, &ops)?;
    }

    /// `history_lead` is `history` with the failing rows removed, for random workloads
    /// and filters on table `u`'s timestamp column `at` (after the differential run has
    /// checked `history` itself against the model).
    #[test]
    fn history_lead_is_filtered_history(
        ops in arb_ops(ops_len(40)),
        filters in prop::collection::vec((-80i64..80, 0i64..60, prop::option::of(0i64..40)), 1..6),
        block_rows in 1usize..4,
    ) {
        let e = run_differential(
            Arc::new(MemoryStore::new()),
            test_options(block_rows, 3, false),
            &ops,
        )?;
        e.compact_all().unwrap();
        let Ok(u) = e.table("u") else { return Ok(()) };
        let Some(ci) = u.column_index("at") else { return Ok(()) };
        if u.columns[ci].ty != ColumnType::Timestamp {
            return Ok(());
        }
        for (lo, len, frontier) in filters {
            let lead = LeadFilter {
                column: "at".into(),
                min: Duration::from_micros(lo * HOUR),
                max: Duration::from_micros((lo + len) * HOUR),
            };
            let req = LatestRequest {
                table: "u".into(),
                context: ReadContext { frontier: frontier.map(h), ..ReadContext::default() },
                ..LatestRequest::default()
            };
            let mut want = e.history(&req).unwrap();
            want.rows.retain(|r| lead.accepts(r.value_time, r.values[ci].as_ref()));
            let got = e.history_lead(&req, &lead).unwrap();
            prop_assert_eq!(format!("{got:?}"), format!("{want:?}"));
        }
    }
}

fn setup(e: &impl Database) {
    e.create_table(&CreateTable::new(
        "fc",
        vec![
            ColumnDef::new("value", ColumnType::F64),
            ColumnDef::new("issue", ColumnType::Timestamp),
        ],
    ))
    .unwrap();
    e.create_series_batch(&[CreateSeries::new("a", "fc"), CreateSeries::new("b", "fc")])
        .unwrap();
}

/// Forecast runs every 6 h for 4 days, each for the next 3 days hourly, all series.
fn forecast(e: &impl Database) {
    for run in 0..16 {
        let issued = run * 6;
        let rows = ["a", "b"]
            .iter()
            .flat_map(|s| {
                (0..3 * DAY).map(move |k| {
                    WriteRow::new(*s, h(issued + k), h(issued))
                        .value("value", Value::F64((run * 100 + k) as f64))
                        .value("issue", Value::Timestamp(h(issued)))
                })
            })
            .collect();
        e.write(&WriteBatch::new(h(issued), rows)).unwrap();
    }
}

fn opts(by_system_time: bool) -> EngineOptions {
    EngineOptions {
        compact_by_system_time: by_system_time,
        ..test_options(16, 0, true)
    }
}

/// Keys of the segments a read fetched (footers or chunks).
fn fetched(store: &FaultyStore<Mem>, since: u64) -> std::collections::BTreeSet<String> {
    store.op_log()[since as usize..]
        .iter()
        .filter(|r| r.op == OpKind::GetRange)
        .map(|r| r.key.clone())
        .collect()
}

#[test]
fn compaction_keeps_system_time_periods_apart() {
    let tiered = Engine::open(Arc::new(MemoryStore::new()), opts(true)).unwrap();
    let full = Engine::open(Arc::new(MemoryStore::new()), opts(false)).unwrap();
    for e in [&tiered, &full] {
        setup(e);
        forecast(e);
        e.compact_all().unwrap();
    }
    assert_eq!(fingerprint(&tiered), fingerprint(&full));
    let counts = |e: &Engine<Mem>| {
        e.segment_counts()
            .into_iter()
            .map(|((_, p), n)| (p, n))
            .collect::<Vec<_>>()
    };
    // Value day 3 (2025-01-04) was written by runs on days 1-4: four groups vs one.
    assert!(
        counts(&full).iter().all(|(_, n)| *n == 1),
        "{:?}",
        counts(&full)
    );
    assert!(
        counts(&tiered).contains(&("2025-01-04".to_owned(), 4)),
        "{:?}",
        counts(&tiered)
    );
    // Compacting again changes nothing.
    assert_eq!(tiered.compact_all().unwrap(), 0);
}

#[test]
fn knowledge_bounded_reads_skip_other_periods() {
    let inner: Mem = Arc::new(MemoryStore::new());
    let store = Arc::new(FaultyStore::new(Arc::clone(&inner), 0));
    let e = Engine::open(Arc::clone(&store), opts(true)).unwrap();
    setup(&e);
    forecast(&e);
    e.compact_all().unwrap();
    let e = Engine::open(Arc::clone(&store), opts(true)).unwrap();
    let model = muninn_core::ModelDb::new();
    setup(&model);
    forecast(&model);
    let all = e.live_segment_keys().len();
    // Replay of (day 1 06:00, day 1 12:00]: the 12:00 run of day 1 (issued at 36 h).
    let replay = ReplayRequest {
        table: "fc".into(),
        arrival: TimeRange::new(Some(h(DAY + 6)), Some(h(DAY + 12))),
        ..ReplayRequest::default()
    };
    let start = store.op_count();
    let got = e.replay(&replay).unwrap();
    assert_eq!(
        format!("{got:?}"),
        format!("{:?}", model.replay(&replay).unwrap())
    );
    assert_eq!(got.rows.len(), 2 * 3 * DAY as usize); // the 12:00 run of both series
    let read = fetched(&store, start);
    // The run wrote into value days 1-4; in each of those partitions only the segment
    // of system day 1 can hold its arrivals; every other segment is skipped from the
    // log's statistics without reading even its footer.
    assert_eq!(
        read.len(),
        4,
        "{} of {all} segments read: {read:#?}",
        read.len()
    );
    // A system bound within day 0 skips the segments of later days: latest as of 23:00
    // reads only the day-0 segments of value days 0-3.
    let latest = LatestRequest {
        table: "fc".into(),
        context: ReadContext {
            system: Some(h(DAY - 1)),
            ..ReadContext::default()
        },
        ..LatestRequest::default()
    };
    let start = store.op_count();
    let got = e.latest(&latest).unwrap();
    assert_eq!(
        format!("{got:?}"),
        format!("{:?}", model.latest(&latest).unwrap())
    );
    let read = fetched(&store, start);
    // Runs of day 1 wrote into value days 1-4: 4 partitions, one day-1 segment each.
    assert_eq!(read.len(), 4, "{} segments read: {read:#?}", read.len());
}

#[test]
fn lead_filter_skips_segments_by_column_statistics() {
    let inner: Mem = Arc::new(MemoryStore::new());
    let store = Arc::new(FaultyStore::new(Arc::clone(&inner), 0));
    let e = Engine::open(Arc::clone(&store), opts(true)).unwrap();
    setup(&e);
    forecast(&e);
    e.compact_all().unwrap();
    let e = Engine::open(Arc::clone(&store), opts(true)).unwrap();
    let req = LatestRequest {
        table: "fc".into(),
        range: TimeRange::new(Some(h(2 * DAY)), Some(h(3 * DAY))),
        ..LatestRequest::default()
    };
    let lead = LeadFilter {
        column: "issue".into(),
        min: Duration::from_micros(24 * HOUR),
        max: Duration::from_micros(30 * HOUR),
    };
    let start = store.op_count();
    let got = e.history_lead(&req, &lead).unwrap();
    let mut reads = std::collections::BTreeMap::<String, usize>::new();
    for r in &store.op_log()[start as usize..] {
        if r.op == OpKind::GetRange {
            *reads.entry(r.key.clone()).or_default() += 1;
        }
    }
    let mut want = e.history(&req).unwrap();
    want.rows
        .retain(|r| lead.accepts(r.value_time, r.values[1].as_ref()));
    assert_eq!(format!("{got:?}"), format!("{want:?}"));
    assert_eq!(got.rows.len(), 2 * DAY as usize); // one run per hour of the day, per series
    // Value day 2 (48-72 h) was written on system days 0, 1 and 2 (three segments);
    // issue times 24-30 h before it lie in (18 h, 48 h): only the day-1 segment's issue
    // times (24-42 h) can match. The others are read for their footer (statistics) only.
    assert_eq!(reads.len(), 3, "{reads:#?}");
    assert_eq!(reads.values().filter(|&&n| n > 1).count(), 1, "{reads:#?}");
    // Errors: unknown column, non-timestamp column, min > max.
    let bad = |column: &str, min: i64, max: i64| {
        e.history_lead(
            &req,
            &LeadFilter {
                column: column.into(),
                min: Duration::from_micros(min),
                max: Duration::from_micros(max),
            },
        )
        .unwrap_err()
        .kind()
    };
    use muninn_core::ErrorKind::*;
    assert_eq!(bad("nope", 0, 1), UnknownColumn);
    assert_eq!(bad("value", 0, 1), TypeMismatch);
    assert_eq!(bad("issue", 1, 0), InvalidRange);
    // An empty lead range is valid and matches nothing.
    let empty = e
        .history_lead(
            &req,
            &LeadFilter {
                column: "issue".into(),
                min: Duration::from_micros(5),
                max: Duration::from_micros(5),
            },
        )
        .unwrap();
    assert!(empty.rows.is_empty());
}
