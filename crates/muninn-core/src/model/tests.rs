//! Scenario-style tests of the tricky rules, plus property tests of the SPEC §7
//! invariants that are checkable on the model alone.

use std::collections::BTreeMap;

use proptest::prelude::*;

use super::*;
use crate::api::*;
use crate::error::ErrorKind;
use crate::time::Duration;
use crate::types::{Clock, ColumnDef, ColumnType, TimeRange, Value};

fn ts(s: &str) -> Timestamp {
    Timestamp::parse(s).unwrap()
}

/// `2025-01-01T00:00:00Z + h hours`.
fn h(hours: i64) -> Timestamp {
    Timestamp::from_micros(ts("2025-01-01T00:00:00Z").as_micros() + hours * 3_600_000_000).unwrap()
}

fn f(x: f64) -> Option<Value> {
    Some(Value::F64(x))
}

/// A database with table `t(value f64, note string)` and series `a`, `b`; table `u(value
/// f64)` with series `c`.
fn setup() -> ModelDb {
    let db = ModelDb::new();
    db.create_table(&CreateTable::new(
        "t",
        vec![
            ColumnDef::new("value", ColumnType::F64),
            ColumnDef::new("note", ColumnType::String),
        ],
    ))
    .unwrap();
    db.create_table(&CreateTable::new(
        "u",
        vec![ColumnDef::new("value", ColumnType::F64)],
    ))
    .unwrap();
    for (s, t) in [("a", "t"), ("b", "t"), ("c", "u")] {
        db.create_series(&CreateSeries::new(s, t)).unwrap();
    }
    db
}

fn row(series: &str, vt: Timestamp, rt: Timestamp, value: Option<Value>) -> WriteRow {
    WriteRow::new(series, vt, rt).value("value", value)
}

fn write(db: &ModelDb, clock: Timestamp, rows: Vec<WriteRow>) -> Result<WriteResult> {
    db.write(&WriteBatch::new(clock, rows))
}

fn latest(db: &impl Reader, table: &str, frontier: Option<Timestamp>) -> Vec<VersionRow> {
    db.latest(&LatestRequest {
        table: table.into(),
        context: ReadContext {
            frontier,
            ..ReadContext::default()
        },
        ..LatestRequest::default()
    })
    .unwrap()
    .rows
}

fn history(db: &impl Reader, table: &str) -> Vec<VersionRow> {
    db.history(&HistoryRequest {
        table: table.into(),
        ..HistoryRequest::default()
    })
    .unwrap()
    .rows
}

fn kind<T: std::fmt::Debug>(r: Result<T>) -> ErrorKind {
    r.unwrap_err().kind()
}

// ---------------------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------------------

#[test]
fn catalog_ids_and_errors() {
    let db = setup();
    assert_eq!(db.series("a").unwrap().id, SeriesId(1));
    assert_eq!(db.series("c").unwrap().id, SeriesId(3));
    assert_eq!(db.series("c").unwrap().timezone, "UTC");
    // Failed creates do not consume ids.
    assert_eq!(
        kind(db.create_series(&CreateSeries::new("a", "u"))),
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        kind(db.create_series(&CreateSeries::new("", "u"))),
        ErrorKind::InvalidName
    );
    assert_eq!(
        kind(db.create_series(&CreateSeries::new("d", "nope"))),
        ErrorKind::UnknownTable
    );
    assert_eq!(
        db.create_series(&CreateSeries::new("d", "u")).unwrap().id,
        SeriesId(4)
    );

    assert_eq!(
        kind(db.create_table(&CreateTable::new("t", vec![]))),
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        kind(db.create_table(&CreateTable::new("T", vec![]))),
        ErrorKind::InvalidName
    );
    let reserved = CreateTable::new("v", vec![ColumnDef::new("seq", ColumnType::I64)]);
    assert_eq!(kind(db.create_table(&reserved)), ErrorKind::InvalidName);
    let dup = CreateTable::new(
        "v",
        vec![
            ColumnDef::new("x", ColumnType::I64),
            ColumnDef::new("x", ColumnType::F64),
        ],
    );
    assert_eq!(kind(db.create_table(&dup)), ErrorKind::AlreadyExists);
    assert_eq!(
        kind(db.table("v")),
        ErrorKind::UnknownTable,
        "failed create has no effect"
    );
    db.create_table(&CreateTable::new("v", vec![])).unwrap();
    assert!(db.table("v").unwrap().columns.is_empty());

    let add = |table: &str, name: &str| AddColumn {
        table: table.into(),
        column: ColumnDef::new(name, ColumnType::Bool),
        clock: None,
    };
    assert_eq!(
        kind(db.add_column(&add("nope", "x"))),
        ErrorKind::UnknownTable
    );
    assert_eq!(
        kind(db.add_column(&add("t", "value"))),
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        kind(db.add_column(&add("t", "found"))),
        ErrorKind::InvalidName
    );
    assert_eq!(db.add_column(&add("t", "flag")).unwrap().columns.len(), 3);
    assert_eq!(db.table_series("t").unwrap().len(), 2);
    assert_eq!(db.tables().unwrap().len(), 3);
    // Every successful catalog change is logged, in order.
    let log = db.catalog_log();
    assert_eq!(log.len(), 2 + 3 + 1 + 1 + 1);
    assert!(log.windows(2).all(|w| w[0].system_time <= w[1].system_time));
}

#[test]
fn added_column_reads_null_and_compares_null() {
    let db = setup();
    write(&db, h(1), vec![row("a", h(0), h(1), f(1.0))]).unwrap();
    db.add_column(&AddColumn {
        table: "t".into(),
        column: ColumnDef::new("volume", ColumnType::I64),
        clock: None,
    })
    .unwrap();
    let rows = latest(&db, "t", None);
    assert_eq!(rows[0].values, vec![f(1.0), None, None]);
    // Same content with the new column explicitly null is a duplicate (SPEC §4.3).
    let r = write(
        &db,
        h(2),
        vec![row("a", h(0), h(1), f(1.0)).value("volume", None)],
    )
    .unwrap();
    assert_eq!((r.stored, r.duplicates), (0, 1));
    let r = write(
        &db,
        h(2),
        vec![row("a", h(0), h(1), f(1.0)).value("volume", Some(Value::I64(5)))],
    )
    .unwrap();
    assert_eq!((r.stored, r.duplicates), (1, 0));
}

// ---------------------------------------------------------------------------------------
// Validation order and atomicity (SPEC §4.1)
// ---------------------------------------------------------------------------------------

#[test]
fn validation_order_within_a_row() {
    let db = setup();
    let base = || row("a", h(0), h(1), f(1.0));
    // unknown_series beats everything else in the row.
    let mut r = base().value("nope", Some(Value::Bool(true))).tombstone();
    r.series = "zzz".into();
    r.value_time = TimeInput::Text("garbage".into());
    assert_eq!(kind(write(&db, h(1), vec![r])), ErrorKind::UnknownSeries);
    // unknown_column (step 2) beats type_mismatch (step 3) regardless of key order.
    let r = base()
        .value("aaa_unknown", None)
        .value("note", Some(Value::I64(1)));
    assert_eq!(kind(write(&db, h(1), vec![r])), ErrorKind::UnknownColumn);
    let r = base()
        .value("value", Some(Value::Bool(true)))
        .value("zzz_unknown", None);
    assert_eq!(kind(write(&db, h(1), vec![r])), ErrorKind::UnknownColumn);
    // type_mismatch (3) beats tombstone_with_values (4).
    let r = base().value("value", Some(Value::Bool(true))).tombstone();
    assert_eq!(kind(write(&db, h(1), vec![r])), ErrorKind::TypeMismatch);
    // tombstone_with_values (4) beats invalid_timestamp (5).
    let mut r = base().tombstone();
    r.received_time = TimeInput::Micros(i64::MAX);
    assert_eq!(
        kind(write(&db, h(1), vec![r])),
        ErrorKind::TombstoneWithValues
    );
    // invalid_timestamp for each time field.
    let mut r = base();
    r.value_time = TimeInput::Text("2025-02-30T00:00:00Z".into());
    assert_eq!(kind(write(&db, h(1), vec![r])), ErrorKind::InvalidTimestamp);
    let mut r = base();
    r.received_time = TimeInput::Micros(Timestamp::MIN.as_micros() - 1);
    assert_eq!(kind(write(&db, h(1), vec![r])), ErrorKind::InvalidTimestamp);
    let r = base().published("0000-06-01T00:00:00Z");
    assert_eq!(kind(write(&db, h(1), vec![r])), ErrorKind::InvalidTimestamp);
    // A tombstone with explicit nulls is fine.
    write(
        &db,
        h(1),
        vec![base().value("value", None).value("note", None).tombstone()],
    )
    .unwrap();
}

#[test]
fn validation_rows_in_input_order() {
    let db = setup();
    let ok = row("a", h(0), h(1), f(1.0));
    let mismatch = row("a", h(1), h(1), Some(Value::String("x".into())));
    let unknown = row("zzz", h(1), h(1), None);
    assert_eq!(
        kind(write(
            &db,
            h(1),
            vec![ok.clone(), mismatch.clone(), unknown.clone()]
        )),
        ErrorKind::TypeMismatch
    );
    assert_eq!(
        kind(write(&db, h(1), vec![ok, unknown, mismatch])),
        ErrorKind::UnknownSeries
    );
}

#[test]
fn rows_may_span_tables() {
    let db = setup();
    let r = write(
        &db,
        h(1),
        vec![row("a", h(0), h(1), f(1.0)), row("c", h(0), h(1), f(2.0))],
    )
    .unwrap();
    assert_eq!(r.stored, 2);
    assert_eq!(latest(&db, "u", None)[0].values, vec![f(2.0)]);
}

#[test]
fn rejected_batch_changes_nothing() {
    let db = setup();
    write(&db, h(5), vec![row("a", h(0), h(1), f(1.0))]).unwrap();
    let before = db.versions();
    let mut batch = WriteBatch::new(
        h(10),
        vec![
            row("a", h(1), h(9), f(2.0)),
            row("b", h(1), h(9), Some(Value::Bool(true))),
        ],
    );
    batch.batch_id = Some("b1".into());
    assert_eq!(kind(db.write(&batch)), ErrorKind::TypeMismatch);
    assert_eq!(db.versions(), before);
    // Nothing was consumed: not the seq, not `last`, not the batch id.
    batch.rows[1] = row("b", h(1), h(9), f(3.0));
    batch.clock = h(6).into();
    let r = db.write(&batch).unwrap();
    assert_eq!(
        (r.stored, r.first_seq, r.system_time),
        (2, Some(2), Some(h(6)))
    );
    assert!(!r.batch_already_committed);
}

// ---------------------------------------------------------------------------------------
// system_time, seq, dedup, batch ids (SPEC §4.2–§4.5)
// ---------------------------------------------------------------------------------------

#[test]
fn system_time_max_rule_and_seq() {
    let db = setup();
    let r = write(
        &db,
        h(10),
        vec![row("a", h(0), h(1), f(1.0)), row("b", h(0), h(1), f(1.0))],
    )
    .unwrap();
    assert_eq!((r.system_time, r.first_seq), (Some(h(10)), Some(1)));
    // Clock went backwards: system_time becomes `last + 1us` (strictly increasing).
    let after_10 = Timestamp::from_micros(h(10).as_micros() + 1).unwrap();
    let r = write(&db, h(3), vec![row("a", h(1), h(2), f(1.0))]).unwrap();
    assert_eq!((r.system_time, r.first_seq), (Some(after_10), Some(3)));
    let r = write(&db, h(11), vec![row("a", h(2), h(2), f(1.0))]).unwrap();
    assert_eq!((r.system_time, r.first_seq), (Some(h(11)), Some(4)));
    let v = db.versions();
    assert_eq!(
        v.iter().map(|v| v.seq).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert_eq!(
        v.iter().map(|v| v.system_time).collect::<Vec<_>>(),
        vec![h(10), h(10), after_10, h(11)]
    );
}

#[test]
fn duplicate_only_batch_does_not_advance_last() {
    let db = setup();
    write(&db, h(10), vec![row("a", h(0), h(1), f(1.0))]).unwrap();
    let r = write(&db, h(20), vec![row("a", h(0), h(1), f(1.0))]).unwrap();
    assert_eq!((r.stored, r.duplicates, r.system_time), (0, 1, None));
    // `last` is still h(10), so a clock reading of h(15) is used as is.
    let r = write(&db, h(15), vec![row("a", h(1), h(1), f(1.0))]).unwrap();
    assert_eq!((r.system_time, r.first_seq), (Some(h(15)), Some(2)));
    // The duplicate kept the original system_time and seq.
    assert_eq!(
        (db.versions()[0].system_time, db.versions()[0].seq),
        (h(10), 1)
    );
}

#[test]
fn empty_batch_is_a_noop() {
    let db = setup();
    let r = write(&db, h(20), vec![]).unwrap();
    assert_eq!(r, WriteResult::default());
    let r = write(&db, h(5), vec![row("a", h(0), h(1), f(1.0))]).unwrap();
    assert_eq!(r.system_time, Some(h(5)));
}

#[test]
fn dedup_is_bitwise_for_f64() {
    let db = setup();
    let nan_a = f64::from_bits(0x7ff8_0000_0000_0000);
    let nan_b = f64::from_bits(0x7ff8_0000_0000_0001);
    write(
        &db,
        h(1),
        vec![row("a", h(0), h(1), f(nan_a)), row("a", h(1), h(1), f(0.0))],
    )
    .unwrap();
    let r = write(
        &db,
        h(2),
        vec![
            row("a", h(0), h(1), f(nan_a)), // same NaN bits: duplicate
            row("a", h(0), h(1), f(nan_b)), // different NaN payload: new version
            row("a", h(1), h(1), f(-0.0)),  // -0.0 != 0.0: new version
            row("a", h(1), h(1), f(0.0)),   // duplicate
            row("a", h(1), h(1), f(-0.0)),  // duplicate of the kept row above
        ],
    )
    .unwrap();
    assert_eq!((r.stored, r.duplicates, r.first_seq), (2, 3, Some(3)));
    // NaN via the spec string form is the canonical NaN.
    let r = write(
        &db,
        h(3),
        vec![row("a", h(0), h(1), Some(Value::String("NaN".into())))],
    )
    .unwrap();
    assert_eq!((r.stored, r.duplicates), (0, 1));
}

#[test]
fn dedup_considers_every_user_field() {
    let db = setup();
    let base = row("a", h(0), h(1), f(1.0));
    write(&db, h(1), vec![base.clone()]).unwrap();
    let variants = vec![
        row("b", h(0), h(1), f(1.0)),                                // series
        row("a", h(9), h(1), f(1.0)),                                // value_time
        base.clone().published(h(1)),                                // published_time null vs set
        row("a", h(0), h(2), f(1.0)), // received_time: re-send is a new version
        row("a", h(0), h(1), None).tombstone(), // deleted
        base.clone().value("note", Some(Value::String("x".into()))), // another column
        row("a", h(0), h(1), Some(Value::I64(1))), // coerces to 1.0: identical!
    ];
    let r = write(&db, h(2), variants).unwrap();
    assert_eq!((r.stored, r.duplicates), (6, 1));
    // Writing the very same batch again stores nothing (SPEC §7.5).
    let again = write(&db, h(3), vec![base]).unwrap();
    assert_eq!((again.stored, again.duplicates), (0, 1));
}

#[test]
fn batch_ids() {
    let db = setup();
    let mut b = WriteBatch::new(h(1), vec![row("a", h(0), h(1), f(1.0))]);
    b.batch_id = Some("b1".into());
    assert_eq!(db.write(&b).unwrap().stored, 1);
    // Replay: no-op regardless of rows, without validation.
    let mut replay = WriteBatch::new(h(2), vec![row("zzz", h(0), h(1), f(2.0))]);
    replay.batch_id = Some("b1".into());
    let r = db.write(&replay).unwrap();
    assert_eq!(
        r,
        WriteResult {
            batch_already_committed: true,
            ..WriteResult::default()
        }
    );
    assert_eq!(db.versions().len(), 1);
    // An all-duplicate batch still records its id.
    let mut dup = WriteBatch::new(h(3), vec![row("a", h(0), h(1), f(1.0))]);
    dup.batch_id = Some("b2".into());
    assert_eq!(db.write(&dup).unwrap().duplicates, 1);
    dup.rows = vec![row("a", h(5), h(5), f(5.0))];
    assert!(db.write(&dup).unwrap().batch_already_committed);
    // An empty batch with an id records it too.
    let mut empty = WriteBatch::new(h(3), vec![]);
    empty.batch_id = Some("b3".into());
    assert!(!db.write(&empty).unwrap().batch_already_committed);
    assert!(db.write(&empty).unwrap().batch_already_committed);
    // Empty batch ids are rejected (SPEC §10.3), even before the known-id no-op.
    empty.batch_id = Some(String::new());
    assert_eq!(kind(db.write(&empty)), ErrorKind::InvalidArgument);
}

// ---------------------------------------------------------------------------------------
// Visibility and knowledge order (SPEC §5)
// ---------------------------------------------------------------------------------------

#[test]
fn published_time_fallback() {
    let db = ModelDb::new();
    let mut ct = CreateTable::new("p", vec![ColumnDef::new("value", ColumnType::F64)]);
    ct.default_clock = Clock::Published;
    db.create_table(&ct).unwrap();
    db.create_series(&CreateSeries::new("s", "p")).unwrap();
    // v1: published 01:00, received 05:00. v2: unknown publication, received 03:00.
    write(
        &db,
        h(5),
        vec![row("s", h(0), h(5), f(1.0)).published(h(1))],
    )
    .unwrap();
    write(&db, h(6), vec![row("s", h(0), h(3), f(2.0))]).unwrap();
    let at = |fr: i64| {
        latest(&db, "p", Some(h(fr)))
            .first()
            .map(|r| r.values[0].clone())
    };
    assert_eq!(at(0), None);
    assert_eq!(at(1), Some(f(1.0)));
    assert_eq!(at(2), Some(f(1.0)));
    // At 03:00 v2 becomes visible under its fallback (received) time and sorts last.
    assert_eq!(at(3), Some(f(2.0)));
    // Output carries the *stored* published_time (null), not the fallback.
    assert_eq!(latest(&db, "p", Some(h(3)))[0].published_time, None);
    // Under the received clock v1 (received 05:00) is the latest.
    let rows = db
        .latest(&LatestRequest {
            table: "p".into(),
            context: ReadContext {
                clock: Some(Clock::Received),
                ..ReadContext::default()
            },
            ..LatestRequest::default()
        })
        .unwrap()
        .rows;
    assert_eq!(rows[0].values[0], f(1.0));
}

#[test]
fn ties_break_by_system_time_then_seq() {
    let db = setup();
    // Same received time, written in two batches: the later system_time wins.
    write(&db, h(10), vec![row("a", h(0), h(1), f(1.0))]).unwrap();
    write(&db, h(11), vec![row("a", h(0), h(1), f(2.0))]).unwrap();
    assert_eq!(latest(&db, "t", None)[0].values[0], f(2.0));
    // Same received time, same batch (same system_time): the later seq wins.
    write(
        &db,
        h(12),
        vec![row("b", h(0), h(1), f(3.0)), row("b", h(0), h(1), f(4.0))],
    )
    .unwrap();
    let rows = latest(&db, "t", None);
    assert_eq!((rows[1].values[0].clone(), rows[1].seq), (f(4.0), 4));
    // Under the system clock with a frontier between the batches.
    let rows = db
        .latest(&LatestRequest {
            table: "t".into(),
            context: ReadContext {
                clock: Some(Clock::System),
                frontier: Some(h(10)),
                system: None,
            },
            ..LatestRequest::default()
        })
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], f(1.0));
    // History is ordered by the same key.
    let seqs: Vec<u64> = history(&db, "t").iter().map(|r| r.seq).collect();
    assert_eq!(seqs, vec![1, 2, 3, 4]);
}

#[test]
fn late_backfill_of_an_older_run_does_not_win() {
    let db = setup();
    // Run B (received 12:00) is loaded first, then the older run A (received 06:00).
    write(&db, h(13), vec![row("a", h(24), h(12), f(90.0))]).unwrap();
    write(&db, h(14), vec![row("a", h(24), h(6), f(100.0))]).unwrap();
    assert_eq!(latest(&db, "t", None)[0].values[0], f(90.0));
    assert_eq!(latest(&db, "t", Some(h(7)))[0].values[0], f(100.0));
}

#[test]
fn system_bound_reproduces_past_knowledge() {
    let db = setup();
    write(&db, h(10), vec![row("a", h(0), h(1), f(1.0))]).unwrap();
    write(&db, h(20), vec![row("a", h(0), h(2), f(2.0))]).unwrap();
    let at_system = |s: Timestamp| {
        db.latest(&LatestRequest {
            table: "t".into(),
            context: ReadContext {
                system: Some(s),
                ..ReadContext::default()
            },
            ..LatestRequest::default()
        })
        .unwrap()
        .rows
    };
    assert!(at_system(h(9)).is_empty());
    assert_eq!(at_system(h(10))[0].values[0], f(1.0));
    assert_eq!(at_system(h(19))[0].values[0], f(1.0));
    assert_eq!(at_system(h(20))[0].values[0], f(2.0));
}

// ---------------------------------------------------------------------------------------
// Reads (SPEC §6)
// ---------------------------------------------------------------------------------------

#[test]
fn read_errors() {
    let db = setup();
    let req = |table: &str, series: &[&str], from: i64, to: i64| LatestRequest {
        table: table.into(),
        series: series.iter().map(|s| s.to_string()).collect(),
        range: TimeRange::new(Some(h(from)), Some(h(to))),
        ..LatestRequest::default()
    };
    assert_eq!(
        kind(db.latest(&req("nope", &["zzz"], 1, 0))),
        ErrorKind::UnknownTable
    );
    assert_eq!(
        kind(db.latest(&req("t", &["zzz"], 1, 0))),
        ErrorKind::UnknownSeries
    );
    assert_eq!(
        kind(db.latest(&req("t", &["a", "c"], 0, 1))),
        ErrorKind::UnknownSeries
    );
    assert_eq!(
        kind(db.history(&req("t", &["a"], 1, 0))),
        ErrorKind::InvalidRange
    );
    assert!(db.latest(&req("t", &["a"], 1, 1)).unwrap().rows.is_empty());
    let replay = ReplayRequest {
        table: "t".into(),
        arrival: TimeRange::new(Some(h(2)), Some(h(1))),
        ..ReplayRequest::default()
    };
    assert_eq!(kind(db.replay(&replay)), ErrorKind::InvalidRange);
    let grid = GridRequest {
        table: "t".into(),
        window: GridWindow::Relative {
            start: Duration::from_micros(1),
            end: Duration::ZERO,
        },
        ..GridRequest::default()
    };
    assert_eq!(kind(db.grid(&grid)), ErrorKind::InvalidRange);
    let asof = AsofJoinRequest {
        table: "t".into(),
        left: vec![AsofLeftRow {
            series: "c".into(),
            value_time: h(0),
            frontier: h(0),
        }],
        ..AsofJoinRequest::default()
    };
    assert_eq!(kind(db.asof_join(&asof)), ErrorKind::UnknownSeries);
}

#[test]
fn history_and_replay_include_tombstones() {
    let db = setup();
    write(&db, h(1), vec![row("b", h(0), h(1), f(1.0))]).unwrap();
    write(&db, h(2), vec![row("a", h(0), h(2), f(2.0))]).unwrap();
    write(&db, h(3), vec![WriteRow::new("b", h(0), h(3)).tombstone()]).unwrap();
    assert_eq!(latest(&db, "t", None).len(), 1);
    assert_eq!(latest(&db, "t", Some(h(2))).len(), 2);
    let hist = history(&db, "t");
    // Ordered by series id first: a (seq 2), then b (seq 1, 3).
    assert_eq!(
        hist.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![2, 1, 3]
    );
    assert!(hist[2].deleted && hist[2].values == vec![None, None]);
    let replay = |from: Option<i64>, to: Option<i64>| {
        db.replay(&ReplayRequest {
            table: "t".into(),
            arrival: TimeRange::new(from.map(h), to.map(h)),
            ..ReplayRequest::default()
        })
        .unwrap()
        .rows
        .iter()
        .map(|r| r.seq)
        .collect::<Vec<_>>()
    };
    assert_eq!(replay(None, None), vec![1, 2, 3]);
    // (from, to]: `from` exclusive, `to` inclusive (SPEC §6.3).
    assert_eq!(replay(Some(2), None), vec![3]);
    assert_eq!(replay(Some(1), Some(3)), vec![2, 3]);
    assert_eq!(replay(None, Some(1)), vec![1]);
    assert_eq!(replay(Some(2), Some(2)), Vec::<u64>::new());
    // Replay respects the system bound but not a frontier.
    let r = db
        .replay(&ReplayRequest {
            table: "t".into(),
            system: Some(h(2)),
            ..ReplayRequest::default()
        })
        .unwrap();
    assert_eq!(r.rows.len(), 2);
}

#[test]
fn grid_relative_window() {
    let db = setup();
    // Forecast runs every 6h, each covering value times [run, run+12h) hourly.
    for run in [0i64, 6, 12] {
        let rows = (0..12)
            .map(|k| row("a", h(run + k), h(run), f((run * 100 + k) as f64)))
            .collect();
        write(&db, h(run), rows).unwrap();
    }
    let grid = db
        .grid(&GridRequest {
            table: "t".into(),
            frontiers: vec![h(12), h(6), h(6), h(7)],
            window: GridWindow::Relative {
                start: Duration::parse("0h").unwrap(),
                end: Duration::parse("2h").unwrap(),
            },
            ..GridRequest::default()
        })
        .unwrap();
    let got: Vec<(Timestamp, Timestamp, Option<Value>)> = grid
        .rows
        .iter()
        .map(|g| (g.frontier, g.row.value_time, g.row.values[0].clone()))
        .collect();
    assert_eq!(
        got,
        vec![
            (h(6), h(6), f(600.0)),
            (h(6), h(7), f(601.0)),
            (h(7), h(7), f(601.0)),
            (h(7), h(8), f(602.0)),
            (h(12), h(12), f(1200.0)),
            (h(12), h(13), f(1201.0)),
        ]
    );
}

#[test]
fn asof_join_exact_match() {
    let db = setup();
    write(
        &db,
        h(1),
        vec![row("a", h(0), h(1), f(1.0)), row("b", h(0), h(1), f(5.0))],
    )
    .unwrap();
    write(&db, h(2), vec![WriteRow::new("a", h(0), h(2)).tombstone()]).unwrap();
    let left = |s: &str, vt: i64, fr: i64| AsofLeftRow {
        series: s.into(),
        value_time: h(vt),
        frontier: h(fr),
    };
    let r = db
        .asof_join(&AsofJoinRequest {
            table: "t".into(),
            left: vec![
                left("a", 0, 1),
                left("a", 0, 2),
                left("a", 0, 0),
                left("b", 1, 5),
                left("b", 0, 9),
                left("a", 0, 1),
            ],
            ..AsofJoinRequest::default()
        })
        .unwrap();
    let found: Vec<bool> = r.rows.iter().map(AsofRow::found).collect();
    assert_eq!(found, vec![true, false, false, false, true, true]);
    assert_eq!(r.rows[4].right.as_ref().unwrap().values[0], f(5.0));
    assert_eq!(r.rows[1].frontier, h(2));
}

#[test]
fn snapshot_is_stable() {
    let db = setup();
    write(&db, h(1), vec![row("a", h(0), h(1), f(1.0))]).unwrap();
    let snap = db.snapshot().unwrap();
    let before = latest(&snap, "t", None);
    write(
        &db,
        h(2),
        vec![row("a", h(0), h(2), f(2.0)), row("a", h(1), h(2), f(3.0))],
    )
    .unwrap();
    db.add_column(&AddColumn {
        table: "t".into(),
        column: ColumnDef::new("z", ColumnType::Bool),
        clock: None,
    })
    .unwrap();
    assert_eq!(latest(&snap, "t", None), before);
    assert_eq!(snap.table("t").unwrap().columns.len(), 2);
    assert_eq!(latest(&db, "t", None).len(), 2);
}

// ---------------------------------------------------------------------------------------
// Property tests of SPEC §7 invariants
// ---------------------------------------------------------------------------------------

/// A compact random write: (series index, value hour, received hour, published hour or
/// none, tombstone, value).
type GenRow = (usize, i64, i64, Option<i64>, bool, Option<i8>);

fn gen_row() -> impl Strategy<Value = GenRow> {
    (
        0usize..2,
        0i64..4,
        0i64..12,
        prop::option::of(0i64..12),
        prop::bool::weighted(0.15),
        prop::option::of(-2i8..3),
    )
}

fn gen_batches() -> impl Strategy<Value = Vec<(i64, Vec<GenRow>)>> {
    prop::collection::vec((0i64..24, prop::collection::vec(gen_row(), 0..5)), 0..8)
}

fn to_write_row(g: &GenRow) -> WriteRow {
    let (s, vt, rt, pt, del, val) = *g;
    let mut r = WriteRow::new(["a", "b"][s], h(vt), h(rt));
    r.published_time = pt.map(|p| h(p).into());
    if del {
        r.deleted = true;
    } else {
        r.values
            .insert("value".into(), val.map(|v| Value::F64(f64::from(v))).into());
    }
    r
}

fn apply(db: &ModelDb, batches: &[(i64, Vec<GenRow>)]) {
    for (clock, rows) in batches {
        write(db, h(*clock), rows.iter().map(to_write_row).collect()).unwrap();
    }
}

fn clock_of(i: u8) -> Clock {
    [Clock::Received, Clock::Published, Clock::System][usize::from(i % 3)]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// SPEC §7.4: grid ≡ repeated latest.
    #[test]
    fn grid_equals_repeated_latest(
        batches in gen_batches(),
        frontiers in prop::collection::vec(0i64..30, 0..5),
        clock in 0u8..3, system in prop::option::of(0i64..30),
        relative in any::<bool>(), a in -3i64..3, len in 0i64..4,
    ) {
        let db = setup();
        apply(&db, &batches);
        let clock = clock_of(clock);
        let window = if relative {
            GridWindow::Relative { start: Duration::from_micros(a * 3_600_000_000), end: Duration::from_micros((a + len) * 3_600_000_000) }
        } else {
            GridWindow::Absolute(TimeRange::new(Some(h(a.max(0))), Some(h(a.max(0) + len))))
        };
        let grid = db.grid(&GridRequest {
            table: "t".into(), frontiers: frontiers.iter().map(|&x| h(x)).collect(), window,
            clock: Some(clock), system: system.map(h), ..GridRequest::default()
        }).unwrap();
        let mut expected = Vec::new();
        let mut fs: Vec<i64> = frontiers.clone();
        fs.sort();
        fs.dedup();
        for fr in fs {
            let range = match window {
                GridWindow::Absolute(r) => r,
                GridWindow::Relative { start, end } => TimeRange::relative_window(h(fr), start, end).unwrap(),
            };
            let rows = db.latest(&LatestRequest {
                table: "t".into(), range,
                context: ReadContext { clock: Some(clock), frontier: Some(h(fr)), system: system.map(h) },
                ..LatestRequest::default()
            }).unwrap().rows;
            expected.extend(rows.into_iter().map(|row| GridRow { frontier: h(fr), row }));
        }
        prop_assert_eq!(grid.rows, expected);
    }

    /// SPEC §7.1: versions outside the visible set never change a result.
    #[test]
    fn noninterference(
        batches in gen_batches(),
        extra in prop::collection::vec(gen_row(), 0..6),
        clock in 0u8..3, frontier in 0i64..14, system in 0i64..26,
    ) {
        let clock = clock_of(clock);
        let ctx = ReadContext { clock: Some(clock), frontier: Some(h(frontier)), system: Some(h(system)) };
        let read = |db: &ModelDb| {
            let req = LatestRequest { table: "t".into(), context: ctx, ..LatestRequest::default() };
            let left: Vec<AsofLeftRow> = (0..4).flat_map(|vt| ["a", "b"].map(|s| AsofLeftRow {
                series: s.into(), value_time: h(vt), frontier: h(frontier),
            })).collect();
            (
                db.latest(&req).unwrap(),
                db.history(&req).unwrap(),
                db.asof_join(&AsofJoinRequest { table: "t".into(), clock: Some(clock), system: Some(h(system)), left }).unwrap(),
            )
        };
        let db = setup();
        apply(&db, &batches);
        let before = read(&db);
        // Write versions that are invisible in ctx: a system_time after S (clock >= 27 >
        // system; max rule only increases it), and for the received/published clocks also
        // a t_clock after F.
        let rows: Vec<WriteRow> = extra.iter().map(|g| {
            let mut g = *g;
            g.2 += frontier + 1;
            g.3 = g.3.map(|p| frontier + 1 + p);
            to_write_row(&g)
        }).collect();
        write(&db, h(27), rows).unwrap();
        prop_assert_eq!(read(&db), before);
    }

    /// SPEC §7.5: writing any batch twice stores nothing the second time.
    #[test]
    fn dedup_idempotency(batches in gen_batches(), clock in 0i64..40) {
        let db = setup();
        apply(&db, &batches);
        let before = (history(&db, "t"), db.versions().len());
        for (_, rows) in &batches {
            let r = write(&db, h(clock), rows.iter().map(to_write_row).collect()).unwrap();
            prop_assert_eq!(r.stored, 0);
            prop_assert_eq!(r.duplicates as usize, rows.len());
        }
        prop_assert_eq!((history(&db, "t"), db.versions().len()), before);
    }

    /// seq is 1..=n with no gaps, and system_time is non-decreasing in seq order.
    #[test]
    fn seq_and_system_time_monotone(batches in gen_batches()) {
        let db = setup();
        apply(&db, &batches);
        let v = db.versions();
        for (i, x) in v.iter().enumerate() {
            prop_assert_eq!(x.seq, i as u64 + 1);
        }
        prop_assert!(v.windows(2).all(|w| w[0].system_time <= w[1].system_time));
        // No two stored versions are content-identical.
        for (i, a) in v.iter().enumerate() {
            for b in &v[i + 1..] {
                prop_assert!(!a.content_identical(b));
            }
        }
    }

    /// latest = last element per point of history (when not a tombstone).
    #[test]
    fn latest_is_last_of_history(batches in gen_batches(), clock in 0u8..3, frontier in prop::option::of(0i64..14)) {
        let db = setup();
        apply(&db, &batches);
        let req = LatestRequest {
            table: "t".into(),
            context: ReadContext { clock: Some(clock_of(clock)), frontier: frontier.map(h), system: None },
            ..LatestRequest::default()
        };
        let mut last: BTreeMap<(SeriesId, Timestamp), VersionRow> = BTreeMap::new();
        for r in db.history(&req).unwrap().rows {
            last.insert((r.series_id, r.value_time), r);
        }
        let expected: Vec<VersionRow> = last.into_values().filter(|r| !r.deleted).collect();
        prop_assert_eq!(db.latest(&req).unwrap().rows, expected);
    }
}

// ---------------------------------------------------------------------------------------
// SPEC §10.4 check order for the typed API
// ---------------------------------------------------------------------------------------

#[test]
fn writer_clock_is_checked_after_the_batch_id() {
    let db = setup();
    let mut b = WriteBatch::new(h(1), vec![row("a", h(0), h(1), f(1.0))]);
    b.batch_id = Some("b1".into());
    db.write(&b).unwrap();
    // Known batch id: a no-op even with an invalid clock and invalid rows.
    let mut replay = WriteBatch::new("not a time", vec![row("zzz", h(0), h(1), f(1.0))]);
    replay.batch_id = Some("b1".into());
    assert!(db.write(&replay).unwrap().batch_already_committed);
    // Unknown batch id: the clock is checked before the rows.
    replay.batch_id = Some("b2".into());
    assert_eq!(kind(db.write(&replay)), ErrorKind::InvalidTimestamp);
    replay.clock = TimeInput::Invalid("missing".into());
    assert_eq!(kind(db.write(&replay)), ErrorKind::InvalidTimestamp);
    replay.clock = h(2).into();
    assert_eq!(kind(db.write(&replay)), ErrorKind::UnknownSeries);
    // Missing value_time / received_time are invalid_timestamp (step 5).
    let mut r = row("a", h(0), h(1), f(1.0));
    r.received_time = TimeInput::Invalid("missing".into());
    assert_eq!(kind(write(&db, h(2), vec![r])), ErrorKind::InvalidTimestamp);
}

#[test]
fn catalog_check_order() {
    let db = setup();
    // create_table: table uniqueness before column names.
    let req = CreateTable::new("t", vec![ColumnDef::new("Bad", ColumnType::F64)]);
    assert_eq!(kind(db.create_table(&req)), ErrorKind::AlreadyExists);
    // ... and columns in order: a duplicate before a later bad name.
    let req = CreateTable::new(
        "v",
        vec![
            ColumnDef::new("x", ColumnType::F64),
            ColumnDef::new("x", ColumnType::F64),
            ColumnDef::new("Bad", ColumnType::F64),
        ],
    );
    assert_eq!(kind(db.create_table(&req)), ErrorKind::AlreadyExists);
    let req = CreateTable::new(
        "v",
        vec![
            ColumnDef::new("Bad", ColumnType::F64),
            ColumnDef::new("Bad", ColumnType::F64),
        ],
    );
    assert_eq!(kind(db.create_table(&req)), ErrorKind::InvalidName);
    // create_series: empty name before unknown table.
    assert_eq!(
        kind(db.create_series(&CreateSeries::new("", "nope"))),
        ErrorKind::InvalidName
    );
    assert_eq!(
        kind(db.create_series(&CreateSeries::new("a", "nope"))),
        ErrorKind::UnknownTable
    );
}

#[test]
fn input_values_follow_the_json_encodings() {
    use crate::types::InputValue as I;
    let db = ModelDb::new();
    db.create_table(&CreateTable::new(
        "j",
        vec![
            ColumnDef::new("x", ColumnType::F64),
            ColumnDef::new("n", ColumnType::I64),
        ],
    ))
    .unwrap();
    db.create_series(&CreateSeries::new("s", "j")).unwrap();
    let w = |x: I, n: I, vt: i64| {
        write(
            &db,
            h(1),
            vec![WriteRow::new("s", h(vt), h(1)).value("x", x).value("n", n)],
        )
    };
    let num = |t: &str| I::Number(t.into());
    w(num("-0"), num("-0"), 0).unwrap();
    let v = &db.versions()[0];
    assert_eq!(v.values, vec![Some(Value::F64(-0.0)), Some(Value::I64(0))]);
    w(num("18446744073709551616"), num("9223372036854775807"), 1).unwrap();
    assert_eq!(
        db.versions()[1].values[0],
        Some(Value::F64(18446744073709551616.0))
    );
    w(num("1e400"), I::Null, 2).unwrap();
    assert_eq!(db.versions()[2].values[0], Some(Value::F64(f64::INFINITY)));
    for (x, n) in [
        (I::Null, num("2.0")),
        (I::Null, num("1e3")),
        (I::Null, num("9223372036854775808")),
        (num(&"9".repeat(400)), I::Null),
        (I::Unsupported("[1]".into()), I::Null),
        (I::Null, I::Unsupported("{}".into())),
        (I::Value(Value::Bool(true)), I::Null),
    ] {
        assert_eq!(kind(w(x, n, 3)), ErrorKind::TypeMismatch);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// SPEC §7 invariant 7: latest at F1 plus replay(F1, F2), applied in order, is latest
    /// at F2.
    #[test]
    fn replay_is_the_frontier_delta(
        batches in gen_batches(),
        clock in 0u8..3, system in prop::option::of(0i64..30),
        f1 in prop::option::of(0i64..14), len in prop::option::of(0i64..14),
        filter in prop::sample::select(vec![vec![], vec!["a"], vec!["b"], vec!["a", "b"]]),
        range in prop::option::of((0i64..4, 0i64..3)),
    ) {
        let db = setup();
        apply(&db, &batches);
        let clock = Some(clock_of(clock));
        let f2 = match (f1, len) {
            (_, None) => None,
            (Some(a), Some(l)) => Some(a + l),
            (None, Some(l)) => Some(l),
        };
        let series: Vec<String> = filter.iter().map(|s| s.to_string()).collect();
        let range = range.map_or(TimeRange::ALL, |(a, l)| TimeRange::new(Some(h(a)), Some(h(a + l))));
        let latest_at = |frontier: Option<i64>| db.latest(&LatestRequest {
            table: "t".into(), series: series.clone(), range,
            context: ReadContext { clock, frontier: frontier.map(h), system: system.map(h) },
        }).unwrap().rows;
        let mut state: BTreeMap<(SeriesId, Timestamp), VersionRow> = BTreeMap::new();
        if f1.is_some() {
            for r in latest_at(f1) {
                state.insert((r.series_id, r.value_time), r);
            }
        }
        let delta = db.replay(&ReplayRequest {
            table: "t".into(), series: series.clone(), range, clock, system: system.map(h),
            arrival: TimeRange::new(f1.map(h), f2.map(h)),
        }).unwrap().rows;
        for r in delta {
            if r.deleted {
                state.remove(&(r.series_id, r.value_time));
            } else {
                state.insert((r.series_id, r.value_time), r);
            }
        }
        prop_assert_eq!(state.into_values().collect::<Vec<_>>(), latest_at(f2));
    }
}
