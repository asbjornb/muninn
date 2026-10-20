//! Columnar write batches ([`WriteBatch::columns`], what the Python binding builds from
//! Arrow input) are validated straight from the columns. They must behave exactly like
//! the same rows given as [`WriteRow`]s: the same stored versions, the same results, and
//! for invalid input the same error code *and message* (SPEC §4.1 / §10.4 check order:
//! rows in input order, steps 1–5 within a row).

mod common;

use std::sync::Arc;

use common::*;
use muninn_core::api::*;
use muninn_core::{ColumnDef, ColumnType, Database, InputValue, ModelDb, Timestamp, Value};
use muninn_engine::Engine;
use muninn_store::MemoryStore;
use proptest::prelude::*;

fn setup<D: Database>(db: &D) {
    db.create_table(&CreateTable::new(
        "t",
        vec![
            ColumnDef::new("a", ColumnType::F64),
            ColumnDef::new("b", ColumnType::I64),
            ColumnDef::new("c", ColumnType::Timestamp),
            ColumnDef::new("d", ColumnType::String),
            ColumnDef::new("e", ColumnType::Bool),
        ],
    ))
    .unwrap();
    db.create_table(&CreateTable::new(
        "u",
        vec![ColumnDef::new("x", ColumnType::F64)],
    ))
    .unwrap();
    db.create_series_batch(&[
        CreateSeries::new("s1", "t"),
        CreateSeries::new("s2", "t"),
        CreateSeries::new("s3", "u"),
    ])
    .unwrap();
}

const COLS: [&str; 7] = ["a", "b", "c", "d", "e", "x", "zzz"];

fn arb_time_column(n: usize, nullable: bool) -> impl Strategy<Value = TimeColumn> {
    let micro = prop_oneof![
        8 => (0i64..6).prop_map(|k| h(k).as_micros()),
        1 => Just(Timestamp::MAX.as_micros() + 1),
        1 => Just(i64::MIN),
    ];
    let input = prop_oneof![
        4 => (0i64..6).prop_map(|k| TimeInput::Micros(h(k).as_micros())),
        2 => (0i64..6).prop_map(|k| TimeInput::Text(h(k).to_string())),
        1 => Just(TimeInput::Text("2025-02-30T00:00:00Z".into())),
        1 => Just(TimeInput::Invalid("a bool".into())),
    ];
    let null_weight = if nullable { 3 } else { 1 };
    prop_oneof![
        prop::collection::vec(
            prop_oneof![10 => micro.prop_map(Some), null_weight => Just(None)],
            n
        )
        .prop_map(TimeColumn::Micros),
        prop::collection::vec(
            prop_oneof![10 => input.prop_map(Some), null_weight => Just(None)],
            n
        )
        .prop_map(TimeColumn::Inputs),
    ]
}

fn arb_input_value() -> impl Strategy<Value = InputValue> {
    prop_oneof![
        3 => Just(InputValue::Null),
        2 => prop::sample::select(vec![0.0, -0.0, 1.5, f64::NAN]).prop_map(|x| Value::F64(x).into()),
        2 => (-2i64..3).prop_map(|x| Value::I64(x).into()),
        1 => any::<bool>().prop_map(|x| Value::Bool(x).into()),
        2 => prop::sample::select(vec!["", "NaN", "7", "2025-01-01T00:00:00Z", "x"])
            .prop_map(|s| Value::String(s.into()).into()),
        1 => (0i64..3).prop_map(|k| Value::Timestamp(h(k)).into()),
        1 => prop::sample::select(vec!["-0", "2.0", "18446744073709551616", "1e3"])
            .prop_map(|s| InputValue::Number(s.into())),
        1 => Just(InputValue::Unsupported("[1]".into())),
    ]
}

fn arb_value_column(n: usize) -> impl Strategy<Value = ValueColumn> {
    fn cells<T: std::fmt::Debug + Clone + 'static>(
        n: usize,
        s: impl Strategy<Value = T> + 'static,
    ) -> impl Strategy<Value = Vec<Option<T>>> {
        prop::collection::vec(prop_oneof![3 => s.prop_map(Some), 1 => Just(None)], n)
    }
    prop_oneof![
        3 => cells(n, prop::sample::select(vec![0.0, -0.0, 2.5, f64::NAN, f64::INFINITY]))
            .prop_map(ValueColumn::F64),
        2 => cells(n, -3i64..3).prop_map(ValueColumn::I64),
        1 => cells(n, any::<bool>()).prop_map(ValueColumn::Bool),
        1 => cells(n, (0i64..3).prop_map(h)).prop_map(ValueColumn::Timestamp),
        2 => prop::collection::vec(arb_input_value(), n).prop_map(ValueColumn::Inputs),
    ]
}

fn arb_columns() -> impl Strategy<Value = WriteColumns> {
    (0usize..8).prop_flat_map(|n| {
        (
            prop::collection::vec(prop_oneof![10 => 0u32..3, 1 => Just(3u32)], n),
            prop::option::weighted(0.95, arb_time_column(n, false)),
            prop::option::weighted(0.95, arb_time_column(n, false)),
            prop::option::of(arb_time_column(n, true)),
            prop::option::of(prop::collection::vec(prop::bool::weighted(0.2), n)),
            prop::sample::subsequence(COLS.to_vec(), 0..=4),
            prop::collection::vec(arb_value_column(n), 4),
        )
            .prop_map(
                move |(codes, vt, rt, pt, deleted, names, mut cols)| WriteColumns {
                    len: n,
                    series_names: vec!["s1".into(), "s2".into(), "s3".into(), "nope".into()],
                    series_codes: codes,
                    value_time: vt,
                    received_time: rt,
                    published_time: pt,
                    deleted,
                    values: names
                        .into_iter()
                        .map(|name| (name.to_owned(), cols.pop().expect("4 columns")))
                        .collect(),
                },
            )
    })
}

fn outcome(
    r: muninn_core::Result<WriteResult>,
) -> Result<WriteResult, (muninn_core::ErrorKind, String)> {
    r.map_err(|e| (e.kind(), e.message().to_owned()))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(512), ..ProptestConfig::default() })]

    #[test]
    fn columnar_batches_behave_like_rows(
        batches in prop::collection::vec((arb_columns(), prop::collection::vec(any::<u8>(), 0..2)), 1..6),
    ) {
        let e = Engine::open(Arc::new(MemoryStore::new()), test_options(4, 0, false)).unwrap();
        let m = ModelDb::new();
        setup(&e);
        setup(&m);
        for (k, (cols, prefix)) in batches.into_iter().enumerate() {
            let clock = h(10 + k as i64);
            // Sometimes some ordinary rows first (the columnar rows follow them).
            let lead: Vec<WriteRow> = prefix
                .iter()
                .map(|&b| {
                    WriteRow::new(["s1", "s3"][usize::from(b % 2)], h(i64::from(b % 4)), h(1))
                })
                .collect();
            let columnar = WriteBatch {
                rows: lead.clone(),
                ..WriteBatch::from_columns(clock, cols)
            };
            let rows = WriteBatch::new(clock, columnar.all_rows());
            prop_assert_eq!(rows.len(), columnar.len());
            let got = outcome(e.write(&columnar));
            let want = outcome(m.write(&rows));
            prop_assert_eq!(got, want);
        }
        prop_assert_eq!(fingerprint(&e), fingerprint(&m));
    }
}

/// The columnar fast path itself: a large valid batch is stored exactly like rows.
#[test]
fn large_valid_columnar_batch() {
    let n = 5_000;
    let cols = WriteColumns {
        len: n,
        series_names: vec!["s1".into(), "s3".into(), "s2".into()],
        series_codes: (0..n).map(|i| (i % 3) as u32).collect(),
        value_time: Some(TimeColumn::Micros(
            (0..n).map(|i| Some(h(i as i64 / 3).as_micros())).collect(),
        )),
        received_time: Some(TimeColumn::Micros(vec![Some(h(0).as_micros()); n])),
        published_time: Some(TimeColumn::Micros(
            (0..n)
                .map(|i| (i % 2 == 0).then(|| h(-1).as_micros()))
                .collect(),
        )),
        deleted: None,
        values: vec![
            (
                "a".into(),
                ValueColumn::F64((0..n).map(|i| (i % 3 != 1).then_some(i as f64)).collect()),
            ),
            (
                "x".into(),
                ValueColumn::I64((0..n).map(|i| (i % 3 == 1).then_some(i as i64)).collect()),
            ),
            (
                "d".into(),
                ValueColumn::Inputs(
                    (0..n)
                        .map(|i| {
                            if i % 3 == 1 {
                                InputValue::Null
                            } else {
                                Value::String(format!("v{i}")).into()
                            }
                        })
                        .collect(),
                ),
            ),
        ],
    };
    let e = Engine::open(Arc::new(MemoryStore::new()), test_options(64, 0, false)).unwrap();
    let m = ModelDb::new();
    setup(&e);
    setup(&m);
    let b = WriteBatch::from_columns(h(100), cols);
    let r = e.write(&b).unwrap();
    assert_eq!(r.stored, n as u64);
    assert_eq!(m.write(&WriteBatch::new(h(100), b.all_rows())).unwrap(), r);
    assert_eq!(fingerprint(&e), fingerprint(&m));
}
