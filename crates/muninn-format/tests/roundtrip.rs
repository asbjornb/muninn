//! Property tests of the segment format: round trips, footer statistics, corruption
//! detection and decoder robustness.

use muninn_core::types::{ColumnType, SeriesId, Version};
use muninn_core::{Clock, Partitioning, Timestamp, Value};
use muninn_format::{
    ErrorKind, Partition, SegmentFile, Tail, ValueColumn, WriteOptions, parse_tail, write_segment,
};
use proptest::prelude::*;

const DAY: i64 = 86_400_000_000;
const BASE: i64 = 1_735_689_600_000_000; // 2025-01-01T00:00:00Z

fn columns() -> Vec<ValueColumn> {
    vec![
        ValueColumn {
            id: 0,
            ty: ColumnType::F64,
        },
        ValueColumn {
            id: 1,
            ty: ColumnType::I64,
        },
        ValueColumn {
            id: 2,
            ty: ColumnType::Bool,
        },
        ValueColumn {
            id: 3,
            ty: ColumnType::String,
        },
        ValueColumn {
            id: 5,
            ty: ColumnType::Timestamp,
        },
    ]
}

fn ts(us: i64) -> Timestamp {
    Timestamp::from_micros(us).unwrap()
}

fn arb_value(ty: ColumnType) -> BoxedStrategy<Option<Value>> {
    let v: BoxedStrategy<Value> = match ty {
        ColumnType::F64 => prop_oneof![
            any::<f64>().prop_map(Value::F64),
            Just(Value::F64(f64::NAN)),
            Just(Value::F64(-0.0)),
            (-5i32..5).prop_map(|x| Value::F64(f64::from(x))),
        ]
        .boxed(),
        ColumnType::I64 => prop_oneof![any::<i64>(), -3i64..3]
            .prop_map(Value::I64)
            .boxed(),
        ColumnType::Bool => any::<bool>().prop_map(Value::Bool).boxed(),
        ColumnType::String => "[a-zé€]{0,6}".prop_map(Value::String).boxed(),
        ColumnType::Timestamp => (Timestamp::MIN.as_micros()..=Timestamp::MAX.as_micros())
            .prop_map(|t| Value::Timestamp(ts(t)))
            .boxed(),
    };
    prop::option::weighted(0.7, v).boxed()
}

/// Random versions of partition 2025-01-01 (day), unique seqs.
fn arb_versions() -> impl Strategy<Value = Vec<Version>> {
    let row = (
        1u32..6,
        0i64..DAY,
        prop::option::of(-1000i64..1000),
        -1000i64..1000,
        0i64..5,
        any::<bool>(),
        arb_value(ColumnType::F64),
        arb_value(ColumnType::I64),
        arb_value(ColumnType::Bool),
        arb_value(ColumnType::String),
        arb_value(ColumnType::Timestamp),
    );
    prop::collection::vec(row, 1..60).prop_map(|rows| {
        rows.into_iter()
            .enumerate()
            .map(|(i, (s, vt, pt, rt, st, del, a, b, c, d, e))| {
                let mut values = if del {
                    vec![]
                } else {
                    vec![a, b, c, d, None, e]
                };
                while values.last().is_some_and(Option::is_none) {
                    values.pop();
                }
                Version {
                    series: SeriesId(s * 3),
                    value_time: ts(BASE + vt),
                    published_time: pt.map(|p| ts(BASE + p * 1_000_000)),
                    received_time: ts(BASE + rt * 1_000_000),
                    deleted: del,
                    values,
                    system_time: ts(BASE + st * 1_000_000),
                    seq: (i as u64) * 7 + 1,
                }
            })
            .collect()
    })
}

fn partition() -> Partition {
    Partition::of(Partitioning::Day, ts(BASE))
}

fn sorted(mut v: Vec<Version>) -> Vec<Version> {
    v.sort_by_key(|v| {
        (
            v.series,
            v.value_time,
            v.received_time,
            v.system_time,
            v.seq,
        )
    });
    v
}

fn opts(block_rows: usize, compress: bool) -> WriteOptions {
    WriteOptions {
        block_rows,
        compress,
        zstd_level: 1,
    }
}

fn bit_identical(a: &[Version], b: &[Version]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x == y && x.seq == y.seq)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn round_trip(versions in arb_versions(), block_rows in 1usize..20, compress in any::<bool>()) {
        let file = write_segment("t", partition(), &columns(), &versions, &opts(block_rows, compress)).unwrap();
        let seg = SegmentFile::open(&file).unwrap();
        let got = seg.versions().unwrap();
        prop_assert!(bit_identical(&got, &sorted(versions.clone())));

        // Footer statistics are exact.
        let f = seg.footer();
        let st = &f.stats;
        let eff = |v: &Version| v.published_time.unwrap_or(v.received_time);
        prop_assert_eq!(f.row_count as usize, versions.len());
        prop_assert_eq!(st.value_time.min, versions.iter().map(|v| v.value_time).min().unwrap());
        prop_assert_eq!(st.value_time.max, versions.iter().map(|v| v.value_time).max().unwrap());
        prop_assert_eq!(st.published_time.min, versions.iter().map(eff).min().unwrap());
        prop_assert_eq!(st.received_time.max, versions.iter().map(|v| v.received_time).max().unwrap());
        prop_assert_eq!(st.system_time.min, versions.iter().map(|v| v.system_time).min().unwrap());
        prop_assert_eq!(st.t_clock(Clock::Published), st.published_time);
        prop_assert_eq!(st.seq_min, versions.iter().map(|v| v.seq).min().unwrap());
        prop_assert_eq!(st.deleted_count as usize, versions.iter().filter(|v| v.deleted).count());
        prop_assert_eq!(st.published_null_count as usize, versions.iter().filter(|v| v.published_time.is_none()).count());
        let ids: Vec<SeriesId> = { let mut s: Vec<_> = versions.iter().map(|v| v.series).collect(); s.sort(); s.dedup(); s };
        prop_assert_eq!(f.series.iter().map(|e| e.id).collect::<Vec<_>>(), ids.clone());
        let all = sorted(versions.clone());
        for id in ids {
            let r = f.series_rows(id).unwrap();
            prop_assert!(all[r.start as usize..r.end as usize].iter().all(|v| v.series == id));
            prop_assert_eq!((r.end - r.start) as usize, all.iter().filter(|v| v.series == id).count());
            // Blocks for the slice cover it.
            let bs = f.blocks_for_rows(r.clone());
            prop_assert!(f.blocks[bs.start].row_start <= r.start);
            prop_assert!(f.blocks[bs.end - 1].rows().end >= r.end);
        }
        for (p, c) in f.columns.iter().enumerate() {
            let vals: Vec<&Value> = versions.iter().filter_map(|v| v.value(c.id as usize)).collect();
            prop_assert_eq!(f.column_stats[p].null_count as usize, versions.len() - vals.len());
            if let Some((lo, hi)) = &f.column_stats[p].min_max {
                for v in vals {
                    match (v, lo, hi) {
                        (Value::F64(x), Value::F64(a), Value::F64(b)) => prop_assert!(x.is_nan() || (a <= x && x <= b)),
                        (Value::I64(x), Value::I64(a), Value::I64(b)) => prop_assert!(a <= x && x <= b),
                        (Value::Timestamp(x), Value::Timestamp(a), Value::Timestamp(b)) => prop_assert!(a <= x && x <= b),
                        _ => prop_assert!(false, "min/max on wrong type"),
                    }
                }
            }
        }
        for b in &f.blocks {
            prop_assert!(b.row_count as usize <= block_rows);
        }

        // Opening from a short tail asks for more bytes, then succeeds.
        let n = file.len();
        match parse_tail(&file[n - 16..], n as u64).unwrap() {
            Tail::NeedBytes(k) => {
                let Tail::Footer(f2) = parse_tail(&file[n - k as usize..], n as u64).unwrap() else { panic!() };
                prop_assert_eq!(&*f2, f);
            }
            Tail::Footer(_) => prop_assert!(false, "footer cannot fit in the tail"),
        }
    }

    /// Any single bit flip anywhere in the file is detected by a full read.
    #[test]
    fn bit_flips_are_detected(versions in arb_versions(), block_rows in 1usize..8, compress in any::<bool>(), pos in any::<prop::sample::Index>(), bit in 0u8..8) {
        let mut file = write_segment("t", partition(), &columns(), &versions, &opts(block_rows, compress)).unwrap();
        let i = pos.index(file.len());
        file[i] ^= 1 << bit;
        let res = SegmentFile::open(&file).and_then(|s| s.versions());
        prop_assert!(res.is_err(), "flip at byte {} bit {} went undetected", i, bit);
    }

    /// Truncation is always detected.
    #[test]
    fn truncation_is_detected(versions in arb_versions(), cut in any::<prop::sample::Index>()) {
        let file = write_segment("t", partition(), &columns(), &versions, &opts(4, true)).unwrap();
        let n = cut.index(file.len());
        let res = SegmentFile::open(&file[..n]).and_then(|s| s.versions());
        prop_assert!(res.is_err());
        // A tail read of a truncated file never panics either.
        if n >= 16 {
            let _ = parse_tail(&file[..n], n as u64);
        }
    }

    /// Random garbage, and random overwrites of a valid file, never panic.
    #[test]
    fn garbage_never_panics(
        versions in arb_versions(),
        junk in prop::collection::vec(any::<u8>(), 0..300),
        at in any::<prop::sample::Index>(),
    ) {
        let _ = SegmentFile::open(&junk).and_then(|s| s.versions());
        let mut file = write_segment("t", partition(), &columns(), &versions, &opts(3, false)).unwrap();
        let start = at.index(file.len());
        for (k, b) in junk.iter().enumerate() {
            if let Some(x) = file.get_mut(start + k) { *x = *b; }
        }
        let _ = SegmentFile::open(&file).and_then(|s| s.versions());
        let n = file.len();
        let _ = parse_tail(&file[n.saturating_sub(64)..], n as u64);
    }
}

#[test]
fn writer_rejects_bad_input() {
    let p = partition();
    let v = Version {
        series: SeriesId(1),
        value_time: ts(BASE),
        published_time: None,
        received_time: ts(BASE),
        deleted: false,
        values: vec![Some(Value::I64(1))],
        system_time: ts(BASE),
        seq: 1,
    };
    let o = WriteOptions::default();
    let err = |r: muninn_format::Result<Vec<u8>>| r.unwrap_err().kind();
    assert_eq!(
        err(write_segment("t", p, &columns(), &[], &o)),
        ErrorKind::InvalidInput
    );
    // Wrong type for column 0 (f64).
    assert_eq!(
        err(write_segment(
            "t",
            p,
            &columns(),
            std::slice::from_ref(&v),
            &o
        )),
        ErrorKind::InvalidInput
    );
    // Outside the partition.
    let mut w = v.clone();
    w.values = vec![];
    w.value_time = ts(BASE + DAY);
    assert_eq!(
        err(write_segment("t", p, &columns(), &[w], &o)),
        ErrorKind::InvalidInput
    );
    // Duplicate seq.
    let mut a = v.clone();
    a.values = vec![];
    let mut b = a.clone();
    b.received_time = ts(BASE + 1);
    assert_eq!(
        err(write_segment("t", p, &columns(), &[a, b], &o)),
        ErrorKind::InvalidInput
    );
}

#[test]
fn schema_evolution_missing_columns_read_null() {
    // A segment written before column 1 existed: only column 0 stored.
    let v = Version {
        series: SeriesId(1),
        value_time: ts(BASE),
        published_time: None,
        received_time: ts(BASE),
        deleted: false,
        values: vec![Some(Value::F64(1.5))],
        system_time: ts(BASE),
        seq: 1,
    };
    let cols = [ValueColumn {
        id: 0,
        ty: ColumnType::F64,
    }];
    let file = write_segment(
        "t",
        partition(),
        &cols,
        std::slice::from_ref(&v),
        &WriteOptions::default(),
    )
    .unwrap();
    let seg = SegmentFile::open(&file).unwrap();
    assert_eq!(seg.footer().column_position(0), Some(0));
    assert_eq!(seg.footer().column_position(1), None);
    assert_eq!(seg.versions().unwrap(), vec![v]);
}

#[test]
fn large_segment_compresses_and_needs_more_tail() {
    let versions: Vec<Version> = (0..20_000u64)
        .map(|i| Version {
            series: SeriesId((i % 50) as u32 + 1),
            value_time: ts(BASE + (i as i64 / 50) * 900_000_000 % DAY),
            published_time: Some(ts(BASE - 3_600_000_000)),
            received_time: ts(BASE - 3_600_000_000 + i as i64),
            deleted: false,
            values: vec![Some(Value::F64(i as f64 * 0.25))],
            system_time: ts(BASE),
            seq: i + 1,
        })
        .collect();
    let cols = [ValueColumn {
        id: 0,
        ty: ColumnType::F64,
    }];
    let file = write_segment(
        "t",
        partition(),
        &cols,
        &versions,
        &WriteOptions {
            block_rows: 512,
            ..WriteOptions::default()
        },
    )
    .unwrap();
    let seg = SegmentFile::open(&file).unwrap();
    assert_eq!(seg.versions().unwrap().len(), versions.len());
    assert!(seg.footer().blocks.len() >= 39);
    assert!(file.len() < versions.len() * 20, "{} bytes", file.len());
}
