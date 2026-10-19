//! The write-side dedup index (engine `dedup` module): SPEC §4.3 must hold exactly as
//! without it, across reopen, eviction, other writers' commits and compactions, and for
//! bitwise f64 identity and columns added later.

mod common;

use std::sync::Arc;

use common::*;
use muninn_core::api::*;
use muninn_core::{ColumnDef, ColumnType, Database, ErrorKind, ModelDb, Value};
use muninn_engine::{Engine, EngineOptions};
use muninn_store::MemoryStore;
use proptest::prelude::*;

type Mem = Arc<MemoryStore>;

fn setup<D: Database>(db: &D) {
    db.create_table(&CreateTable::new(
        "t",
        vec![ColumnDef::new("value", ColumnType::F64)],
    ))
    .unwrap();
    for s in ["a", "b"] {
        db.create_series(&CreateSeries::new(s, "t")).unwrap();
    }
}

fn row(series: &str, vt: i64, rt: i64, v: f64) -> WriteRow {
    WriteRow::new(series, h(vt), h(rt)).value("value", Value::F64(v))
}

fn write<D: Database>(db: &D, clock: i64, rows: Vec<WriteRow>) -> (u64, u64) {
    let r = db.write(&WriteBatch::new(h(clock), rows)).unwrap();
    (r.stored, r.duplicates)
}

fn opts(index: usize) -> EngineOptions {
    EngineOptions {
        dedup_index_points: index,
        ..file_options(2, 3, false)
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(200), ..ProptestConfig::default() })]

    /// The random differential workload (re-sends, reopen, compaction, GC, add_column)
    /// with the index disabled, evicting constantly (1 point) and large.
    #[test]
    fn engine_matches_model_with_any_index_size(
        ops in arb_ops(ops_len(40)),
        index in prop_oneof![Just(0usize), Just(1), Just(3), Just(2_000_000)],
        block_rows in 1usize..4,
    ) {
        let o = EngineOptions { dedup_index_points: index, ..test_options(block_rows, 3, false) };
        run_differential(Arc::new(MemoryStore::new()), o, &ops)?;
    }
}

#[test]
fn reopened_engine_builds_its_index_from_segments() {
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), opts(1000)).unwrap();
    setup(&e);
    assert_eq!(
        write(&e, 1, vec![row("a", 0, 0, 1.0), row("a", 30, 0, 2.0)]),
        (2, 0)
    );
    drop(e);
    let e = Engine::open(Arc::clone(&store), opts(1000)).unwrap();
    // Same content: duplicates, in two partitions (day partitioning: vt 0 h and 30 h).
    assert_eq!(
        write(&e, 2, vec![row("a", 0, 0, 1.0), row("a", 30, 0, 2.0)]),
        (0, 2)
    );
    // New received time: a new version even with identical values.
    assert_eq!(write(&e, 3, vec![row("a", 0, 1, 1.0)]), (1, 0));
    // An older received time inside the stored range, different value: new.
    assert_eq!(write(&e, 4, vec![row("a", 0, 0, 9.0)]), (1, 0));
    // ... and the same again: a duplicate of that version.
    assert_eq!(write(&e, 5, vec![row("a", 0, 0, 9.0)]), (0, 1));
}

/// Engine `b` built its index before `a` committed rows at the same points; `b` must not
/// store them again (it reloads on the conflict and folds `a`'s segment first).
#[test]
fn stale_writer_sees_other_writers_rows() {
    let store: Mem = Arc::new(MemoryStore::new());
    let a = Engine::open(Arc::clone(&store), opts(1000)).unwrap();
    setup(&a);
    let b = Engine::open(Arc::clone(&store), opts(1000)).unwrap();
    assert_eq!(write(&b, 1, vec![row("a", 0, 0, 1.0)]), (1, 0));
    a.refresh().unwrap();
    assert_eq!(write(&a, 2, vec![row("a", 0, 5, 2.0)]), (1, 0));
    // b's view lags: its commit conflicts; the retry reloads and finds a's version.
    let batch = WriteBatch::new(h(3), vec![row("a", 0, 5, 2.0), row("a", 0, 0, 1.0)]);
    let r = match b.write(&batch) {
        Err(e) if e.kind() == ErrorKind::Conflict => b.write(&batch).unwrap(),
        other => other.unwrap(),
    };
    assert_eq!((r.stored, r.duplicates), (0, 2));
    // Also when b catches up explicitly instead of conflicting.
    assert_eq!(write(&a, 4, vec![row("a", 0, 7, 3.0)]), (1, 0));
    b.refresh().unwrap();
    assert_eq!(write(&b, 5, vec![row("a", 0, 7, 3.0)]), (0, 1));
}

/// Another engine compacts (and collects) the segments this engine's index covers; the
/// merged segment is new to us, so it is folded again from the file. Results stay right.
#[test]
fn compaction_elsewhere_keeps_the_index_correct() {
    let store: Mem = Arc::new(MemoryStore::new());
    let a = Engine::open(Arc::clone(&store), opts(1000)).unwrap();
    setup(&a);
    for k in 0..4 {
        assert_eq!(
            write(&a, k, vec![row("a", 0, k, f64::from(k as u8))]),
            (1, 0)
        );
    }
    let b = Engine::open(Arc::clone(&store), opts(1000)).unwrap();
    assert_eq!(b.compact_all().unwrap(), 1);
    b.gc().unwrap();
    a.refresh().unwrap();
    for k in 0..4 {
        assert_eq!(
            write(&a, 10 + k, vec![row("a", 0, k, f64::from(k as u8))]),
            (0, 1)
        );
    }
    assert_eq!(write(&a, 20, vec![row("a", 0, 9, 9.0)]), (1, 0));
    // b lags behind a's writes: its first attempt conflicts, the retry reloads.
    let batch = WriteBatch::new(h(21), vec![row("a", 0, 9, 9.0)]);
    let r = match b.write(&batch) {
        Err(e) if e.kind() == ErrorKind::Conflict => b.write(&batch).unwrap(),
        other => other.unwrap(),
    };
    assert_eq!((r.stored, r.duplicates), (0, 1));
}

#[test]
fn f64_identity_is_bitwise_and_new_columns_compare_as_null() {
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), opts(1000)).unwrap();
    let m = ModelDb::new();
    setup(&e);
    setup(&m);
    let both = |clock: i64, rows: Vec<WriteRow>| {
        let x = write(&e, clock, rows.clone());
        assert_eq!(x, write(&m, clock, rows));
        x
    };
    assert_eq!(both(1, vec![row("a", 0, 0, 0.0)]), (1, 0));
    assert_eq!(both(2, vec![row("a", 0, 0, -0.0)]), (1, 0)); // -0.0 != 0.0
    assert_eq!(
        both(3, vec![row("a", 0, 0, -0.0), row("a", 0, 0, 0.0)]),
        (0, 2)
    );
    let nan = f64::from_bits(0x7ff8_0000_0000_0001);
    assert_eq!(both(4, vec![row("a", 1, 0, nan)]), (1, 0));
    assert_eq!(both(5, vec![row("a", 1, 0, nan)]), (0, 1));
    assert_eq!(both(6, vec![row("a", 1, 0, f64::NAN)]), (1, 0)); // other payload
    // A column added later reads as null in old versions: an explicit null matches.
    let add = AddColumn {
        table: "t".into(),
        column: ColumnDef::new("extra", ColumnType::I64),
        clock: None,
    };
    e.add_column(&add).unwrap();
    m.add_column(&add).unwrap();
    let with_null = |v: f64| {
        let mut r = row("a", 0, 0, v);
        r.values
            .insert("extra".into(), muninn_core::types::InputValue::Null);
        r
    };
    assert_eq!(both(7, vec![with_null(0.0)]), (0, 1));
    let with_one = WriteRow::new("a", h(0), h(0))
        .value("value", Value::F64(0.0))
        .value("extra", Value::I64(1));
    assert_eq!(both(8, vec![with_one]), (1, 0));
    assert_eq!(fingerprint(&e), fingerprint(&m));
}
