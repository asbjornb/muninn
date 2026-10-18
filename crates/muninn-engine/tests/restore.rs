//! `Engine::restore`: copying a database version by version (with the original
//! `system_time` and `seq`) gives a database that answers every read identically, and
//! malformed restores are rejected without effect.

mod common;

use std::sync::Arc;

use common::*;
use muninn_core::api::*;
use muninn_core::catalog::CatalogChangeKind;
use muninn_core::types::{SeriesId, Version};
use muninn_core::{ColumnDef, ColumnType, Database, ErrorKind, Reader, Timestamp, Value};
use muninn_engine::{Engine, EngineOptions};
use muninn_store::MemoryStore;
use proptest::prelude::*;

fn opts() -> EngineOptions {
    test_options(3, 7, false)
}

fn fresh() -> MemEngine {
    Engine::open(Arc::new(MemoryStore::new()), opts()).unwrap()
}

/// Every version of every table, in seq order, from one snapshot.
fn all_versions<R: Reader>(r: &R) -> Vec<Version> {
    let mut out = Vec::new();
    for t in r.tables().unwrap() {
        let rows = r
            .history(&LatestRequest {
                table: t.name.clone(),
                ..LatestRequest::default()
            })
            .unwrap()
            .rows;
        out.extend(rows.into_iter().map(|r| Version {
            series: r.series_id,
            value_time: r.value_time,
            published_time: r.published_time,
            received_time: r.received_time,
            deleted: r.deleted,
            values: r.values,
            system_time: r.system_time,
            seq: r.seq,
        }));
    }
    out.sort_by_key(|v| v.seq);
    out
}

/// Copies `src` into a fresh engine via catalog replay + `restore` in chunks of `chunk`.
fn copy(src: &MemEngine, chunk: usize) -> MemEngine {
    let snap = src.snapshot().unwrap();
    let dst = fresh();
    for c in snap.catalog_log() {
        let clock = Some(c.system_time);
        match c.change {
            CatalogChangeKind::CreateTable(def) => {
                dst.create_table(&CreateTable {
                    name: def.name,
                    columns: def.columns,
                    default_clock: def.default_clock,
                    partition: def.partition,
                    clock,
                })
                .unwrap();
            }
            CatalogChangeKind::AddColumn { table, column } => {
                dst.add_column(&AddColumn {
                    table,
                    column,
                    clock,
                })
                .unwrap();
            }
            CatalogChangeKind::CreateSeries(def) => {
                dst.create_series(&CreateSeries {
                    name: def.name.to_string(),
                    table: def.table,
                    tags: def.tags,
                    timezone: Some(def.timezone),
                    resolution: def.resolution,
                    clock,
                })
                .unwrap();
            }
        }
    }
    let versions = all_versions(&snap);
    let mut ids = snap.batch_ids().into_iter();
    for part in versions.chunks(chunk.max(1)) {
        dst.restore(part, ids.next().as_deref()).unwrap();
    }
    for id in ids {
        dst.restore(&[], Some(&id)).unwrap();
    }
    dst
}

fn assert_same(a: &MemEngine, b: &MemEngine, ops: &[Op]) {
    assert_eq!(fingerprint(a), fingerprint(b));
    let (sa, sb) = (a.snapshot().unwrap(), b.snapshot().unwrap());
    assert_eq!(sa.catalog_log(), sb.catalog_log());
    assert_eq!(sa.batch_ids(), sb.batch_ids());
    assert_eq!(sa.next_seq(), sb.next_seq());
    assert_eq!(sa.last_system_time(), sb.last_system_time());
    for op in ops {
        assert_eq!(read(a, op), read(b, op), "{op:?}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(24), ..ProptestConfig::default() })]

    #[test]
    fn restore_copy_answers_every_read_identically(
        ops in arb_ops(ops_len(40)),
        chunk in 1usize..12,
    ) {
        let src = run_differential(Arc::new(MemoryStore::new()), opts(), &ops)?;
        let dst = copy(&src, chunk);
        assert_same(&src, &dst, &ops);
        // The copy survives a reopen and compaction.
        dst.compact_all().unwrap();
        let dst = Engine::open(Arc::clone(dst.store()), opts()).unwrap();
        assert_same(&src, &dst, &ops);
        // Later writes behave identically (same system_time, seq, dedup, batch ids).
        for op in ops.iter().filter(|o| o.is_mutation()) {
            let ra = apply_mutation(&src, op, &[]).map(|r| r.map_err(|e| e.kind()));
            let rb = apply_mutation(&dst, op, &[]).map(|r| r.map_err(|e| e.kind()));
            prop_assert_eq!(ra, rb);
        }
        assert_same(&src, &dst, &ops);
    }
}

fn setup(e: &MemEngine) {
    e.create_table(&CreateTable::new(
        "t",
        vec![
            ColumnDef::new("value", ColumnType::F64),
            ColumnDef::new("n", ColumnType::I64),
        ],
    ))
    .unwrap();
    e.create_series(&CreateSeries::new("a", "t")).unwrap();
}

fn version(seq: u64, system: i64) -> Version {
    Version {
        series: SeriesId(1),
        value_time: h(0),
        published_time: None,
        received_time: h(1),
        deleted: false,
        values: vec![Some(Value::F64(1.5))],
        system_time: h(system),
        seq,
    }
}

#[test]
fn restore_keeps_times_and_continues_after_them() {
    let e = fresh();
    setup(&e);
    let r = e
        .restore(
            &[version(5, 10), version(9, 10), version(10, 12)],
            Some("b"),
        )
        .unwrap();
    assert_eq!((r.stored, r.first_seq), (3, Some(5)));
    let rows = e
        .history(&LatestRequest {
            table: "t".into(),
            ..LatestRequest::default()
        })
        .unwrap()
        .rows;
    let got: Vec<(u64, Timestamp)> = rows.iter().map(|r| (r.seq, r.system_time)).collect();
    assert_eq!(got, vec![(5, h(10)), (9, h(10)), (10, h(12))]);
    // A later write continues at seq 11 and never goes below last = h(12).
    let w = e
        .write(&WriteBatch::new(
            h(3),
            vec![WriteRow::new("a", h(0), h(2)).value("value", Value::F64(2.0))],
        ))
        .unwrap();
    assert_eq!((w.first_seq, w.system_time), (Some(11), Some(h(12))));
    // The batch id is committed.
    let mut b = WriteBatch::new(h(20), vec![]);
    b.batch_id = Some("b".into());
    assert!(e.write(&b).unwrap().batch_already_committed);
}

#[test]
fn malformed_restores_are_rejected_without_effect() {
    let e = fresh();
    setup(&e);
    e.restore(&[version(3, 10)], Some("x")).unwrap();
    let before = fingerprint(&e);
    let version_before = e.version();
    let mut tomb = version(4, 10);
    tomb.deleted = true;
    let mut wide = version(4, 10);
    wide.values = vec![None, None, Some(Value::F64(1.0))];
    let mut wrong = version(4, 10);
    wrong.values = vec![None, Some(Value::F64(1.0))];
    let mut unknown = version(4, 10);
    unknown.series = SeriesId(7);
    let cases: Vec<(Vec<Version>, Option<&str>, ErrorKind)> = vec![
        (vec![version(3, 11)], None, ErrorKind::InvalidArgument), // seq not new
        (vec![version(2, 11)], None, ErrorKind::InvalidArgument), // seq below next
        (
            vec![version(5, 11), version(5, 11)],
            None,
            ErrorKind::InvalidArgument,
        ),
        (vec![version(5, 9)], None, ErrorKind::InvalidArgument), // before last
        (
            vec![version(5, 12), version(6, 11)],
            None,
            ErrorKind::InvalidArgument,
        ),
        (vec![version(5, 12)], Some("x"), ErrorKind::InvalidArgument), // batch id known
        (vec![version(5, 12)], Some(""), ErrorKind::InvalidArgument),
        (vec![tomb], None, ErrorKind::TombstoneWithValues),
        (vec![wide], None, ErrorKind::UnknownColumn),
        (vec![wrong], None, ErrorKind::TypeMismatch),
        (vec![unknown], None, ErrorKind::UnknownSeries),
        (
            vec![version(5, 12), version(4, 12)],
            None,
            ErrorKind::InvalidArgument,
        ),
    ];
    for (vs, id, kind) in cases {
        let err = e.restore(&vs, id).unwrap_err();
        assert_eq!(err.kind(), kind, "{vs:?} {err}");
        assert_eq!(fingerprint(&e), before);
        assert_eq!(e.version(), version_before);
    }
    // A no-op restore commits nothing.
    e.restore(&[], None).unwrap();
    assert_eq!(e.version(), version_before);
}
