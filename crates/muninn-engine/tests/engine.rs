//! Targeted engine tests: recovery paths (checkpoints, hint fallback), writer conflicts,
//! GC grace and snapshot protection, I/O pruning, corruption handling, and snapshot
//! stability under concurrent writes, compaction and GC.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::*;
use muninn_core::api::*;
use muninn_core::{Clock, ColumnDef, ColumnType, Database, ErrorKind, Reader, TimeRange, Value};
use muninn_engine::{Engine, EngineOptions, log};
use muninn_store::{FaultyStore, Key, MemoryStore, OpKind, Store};

type Mem = Arc<MemoryStore>;

fn setup(e: &Engine<Mem>) {
    e.create_table(&CreateTable::new(
        "t",
        vec![ColumnDef::new("value", ColumnType::F64)],
    ))
    .unwrap();
    for s in ["a", "b", "c"] {
        e.create_series(&CreateSeries::new(s, "t")).unwrap();
    }
}

fn row(series: &str, vt: i64, rt: i64, v: f64) -> WriteRow {
    WriteRow::new(series, h(vt), h(rt)).value("value", Value::F64(v))
}

fn write(e: &Engine<Mem>, clock: i64, rows: Vec<WriteRow>) -> WriteResult {
    e.write(&WriteBatch::new(h(clock), rows)).unwrap()
}

fn all(table: &str) -> LatestRequest {
    LatestRequest {
        table: table.into(),
        ..LatestRequest::default()
    }
}

fn opts() -> EngineOptions {
    test_options(4, 5, true)
}

#[test]
fn checkpoint_recovery_equals_full_replay() {
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), opts()).unwrap();
    setup(&e);
    for i in 0..23 {
        write(
            &e,
            i,
            vec![row("a", i % 7, i, i as f64), row("b", i % 5, i, 1.0)],
        );
        if i % 6 == 0 {
            e.compact_all().unwrap();
        }
    }
    let expected = fingerprint(&e);
    assert!(
        store
            .list("log/")
            .unwrap()
            .iter()
            .any(|k| k.as_str().ends_with(".checkpoint.json"))
    );
    // Via the hint.
    let a = Engine::open(Arc::clone(&store), opts()).unwrap();
    assert_eq!(fingerprint(&a), expected);
    assert_eq!(a.version(), e.version());
    // A garbage hint, and a hint naming a missing checkpoint, fall back to listing.
    store.put(&log::hint_key(), b"not json").unwrap();
    assert_eq!(
        fingerprint(&Engine::open(Arc::clone(&store), opts()).unwrap()),
        expected
    );
    store
        .put(&log::hint_key(), br#"{"version": 999999}"#)
        .unwrap();
    assert_eq!(
        fingerprint(&Engine::open(Arc::clone(&store), opts()).unwrap()),
        expected
    );
    // A stale hint (older checkpoint) just replays more.
    store.put(&log::hint_key(), br#"{"version": 5}"#).unwrap();
    assert_eq!(
        fingerprint(&Engine::open(Arc::clone(&store), opts()).unwrap()),
        expected
    );
    // Without any checkpoint: full replay from version 1.
    for k in store.list("log/").unwrap() {
        if k.as_str().contains("checkpoint") {
            store.delete(&k).unwrap();
        }
    }
    let b = Engine::open(Arc::clone(&store), opts()).unwrap();
    assert_eq!(fingerprint(&b), expected);
    assert_eq!(b.catalog_log().unwrap(), e.catalog_log().unwrap());
}

#[test]
fn second_writer_conflicts_and_recovers() {
    let store: Mem = Arc::new(MemoryStore::new());
    let a = Engine::open(Arc::clone(&store), opts()).unwrap();
    setup(&a);
    let b = Engine::open(Arc::clone(&store), opts()).unwrap();
    write(&a, 1, vec![row("a", 0, 0, 1.0)]);
    // b has not seen a's write: its commit loses the race.
    let err = b
        .write(&WriteBatch::new(h(1), vec![row("b", 0, 0, 2.0)]))
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Conflict);
    // b reloads before its next operation; the retry succeeds and sees a's data.
    let r = b
        .write(&WriteBatch::new(h(1), vec![row("b", 0, 0, 2.0)]))
        .unwrap();
    assert_eq!(r.first_seq, Some(2));
    assert_eq!(b.latest(&all("t")).unwrap().rows.len(), 2);
    a.refresh().unwrap();
    assert_eq!(fingerprint(&a), fingerprint(&b));
    // The losing writer's segment is an orphan that GC removes.
    a.gc().unwrap();
    let listed: std::collections::BTreeSet<Key> =
        store.list("segments/").unwrap().into_iter().collect();
    assert_eq!(listed, a.live_segment_keys());
}

#[test]
fn gc_respects_grace_and_live_snapshots() {
    let store: Mem = Arc::new(MemoryStore::new());
    let now = Arc::new(std::sync::atomic::AtomicI64::new(1_000_000_000));
    let clock = Arc::clone(&now);
    let o = EngineOptions {
        gc_grace: std::time::Duration::from_secs(10),
        wall_clock: Arc::new(move || clock.load(Ordering::SeqCst)),
        ..opts()
    };
    let e = Engine::open(Arc::clone(&store), o).unwrap();
    setup(&e);
    write(&e, 1, vec![row("a", 0, 0, 1.0)]);
    write(&e, 2, vec![row("a", 1, 1, 2.0)]);
    let snap = e.snapshot().unwrap();
    let before = fingerprint(&snap);
    assert_eq!(e.compact_all().unwrap(), 1);
    // Within the grace period nothing is deleted.
    let r = e.gc().unwrap();
    assert!(r.deleted.is_empty() && r.kept == 2, "{r:?}");
    // After it, the snapshot still protects its segments.
    now.fetch_add(11_000_000, Ordering::SeqCst);
    assert!(e.gc().unwrap().deleted.is_empty());
    assert_eq!(fingerprint(&snap), before);
    drop(snap);
    // Unreferenced from now on: deleted one grace period after GC first sees that.
    assert!(e.gc().unwrap().deleted.is_empty());
    now.fetch_add(11_000_000, Ordering::SeqCst);
    assert_eq!(e.gc().unwrap().deleted.len(), 2);
    assert!(e.retired_segment_keys().is_empty());
    assert_eq!(fingerprint(&e), before);
    // Unknown files under segments/ are never touched.
    let foreign = Key::new("segments/t/2025-01-01/readme.txt").unwrap();
    store.put(&foreign, b"hi").unwrap();
    e.gc().unwrap();
    assert!(store.head(&foreign).is_ok());
}

#[test]
fn gc_with_grace_overrides_option_and_snapshot_reports_last_system_time() {
    let store: Mem = Arc::new(MemoryStore::new());
    let o = EngineOptions {
        gc_grace: std::time::Duration::from_secs(3600),
        ..opts()
    };
    let e = Engine::open(Arc::clone(&store), o).unwrap();
    setup(&e);
    assert_eq!(e.snapshot().unwrap().last_system_time(), None);
    write(&e, 1, vec![row("a", 0, 0, 1.0)]);
    write(&e, 2, vec![row("a", 1, 1, 2.0)]);
    let snap = e.snapshot().unwrap();
    assert_eq!(snap.last_system_time(), Some(h(2)));
    // Equal clock reading: strictly increasing system_time (SPEC §4.2).
    write(&e, 2, vec![row("a", 2, 2, 3.0)]);
    assert_eq!(snap.last_system_time(), Some(h(2)));
    let next = e.snapshot().unwrap().last_system_time().unwrap();
    assert_eq!(next.as_micros(), h(2).as_micros() + 1);
    drop(snap);
    assert_eq!(e.compact_all().unwrap(), 1);
    // The configured hour-long grace keeps the retired segments; a zero grace does not.
    assert!(e.gc().unwrap().deleted.is_empty());
    assert_eq!(
        e.gc_with_grace(std::time::Duration::ZERO)
            .unwrap()
            .deleted
            .len(),
        3
    );
}

/// A single-series read of one partition touches only that partition's segments and
/// only the blocks of that series.
#[test]
fn reads_are_pruned() {
    let inner: Mem = Arc::new(MemoryStore::new());
    let store = Arc::new(FaultyStore::new(Arc::clone(&inner), 0));
    let e = Engine::open(Arc::clone(&store), test_options(8, 0, false)).unwrap();
    e.create_table(&CreateTable::new(
        "t",
        vec![ColumnDef::new("value", ColumnType::F64)],
    ))
    .unwrap();
    for i in 0..50 {
        e.create_series(&CreateSeries::new(format!("s{i}"), "t"))
            .unwrap();
    }
    // 10 days × 50 series × 24 hours in one batch: 10 segments, many blocks each.
    let mut rows = Vec::new();
    for s in 0..50 {
        for vt in 0..240 {
            rows.push(
                WriteRow::new(format!("s{s}"), h(vt), h(0)).value("value", Value::F64(vt as f64)),
            );
        }
    }
    e.write(&WriteBatch::new(h(0), rows)).unwrap();
    assert_eq!(e.live_segment_keys().len(), 10);
    let reopened = Engine::open(Arc::clone(&store), test_options(8, 0, false)).unwrap();
    let start = store.op_count();
    let res = reopened
        .latest(&LatestRequest {
            table: "t".into(),
            series: vec!["s7".into()],
            range: TimeRange::new(Some(h(48)), Some(h(72))),
            ..LatestRequest::default()
        })
        .unwrap();
    assert_eq!(res.rows.len(), 24);
    let reads: Vec<_> = store.op_log()[start as usize..]
        .iter()
        .filter(|r| r.op == OpKind::GetRange)
        .cloned()
        .collect();
    // One footer read, then engine + value chunks of the 3–4 blocks holding s7's 24 rows.
    assert!(
        reads.iter().all(|r| r.key.contains("/2025-01-03/")),
        "{reads:#?}"
    );
    assert!(
        reads.len() <= 1 + 2 * 4,
        "{} reads: {reads:#?}",
        reads.len()
    );
    // A frontier before every version skips the segment from log stats alone.
    let start = store.op_count();
    let res = reopened
        .latest(&LatestRequest {
            table: "t".into(),
            context: ReadContext {
                frontier: Some(h(-1)),
                ..ReadContext::default()
            },
            ..LatestRequest::default()
        })
        .unwrap();
    assert!(res.rows.is_empty());
    assert_eq!(store.op_count(), start);
}

#[test]
fn corrupt_segment_is_an_error_not_a_wrong_answer() {
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), opts()).unwrap();
    setup(&e);
    write(&e, 1, vec![row("a", 0, 0, 1.0), row("b", 0, 0, 2.0)]);
    let key = e.live_segment_keys().into_iter().next().unwrap();
    let mut bytes = store.get(&key).unwrap();
    for i in (16..bytes.len() - 16).step_by(7) {
        let mut b = bytes.clone();
        b[i] ^= 0x10;
        store.put(&key, &b).unwrap();
        let fresh = Engine::open(Arc::clone(&store), opts()).unwrap();
        match fresh.history(&all("t")) {
            Err(err) => assert_eq!(err.kind(), ErrorKind::Io),
            Ok(_) => panic!("corruption at byte {i} went unnoticed"),
        }
    }
    bytes.truncate(bytes.len() / 2);
    store.put(&key, &bytes).unwrap();
    let fresh = Engine::open(Arc::clone(&store), opts()).unwrap();
    assert_eq!(fresh.latest(&all("t")).unwrap_err().kind(), ErrorKind::Io);
}

#[test]
fn schema_evolution_old_segments_read_null() {
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), opts()).unwrap();
    setup(&e);
    write(&e, 1, vec![row("a", 0, 0, 1.0)]);
    e.add_column(&AddColumn {
        table: "t".into(),
        column: ColumnDef::new("note", ColumnType::String),
        clock: None,
    })
    .unwrap();
    // Content-identical to the old version (note null) → duplicate.
    assert_eq!(write(&e, 2, vec![row("a", 0, 0, 1.0)]).duplicates, 1);
    write(
        &e,
        2,
        vec![row("a", 1, 0, 1.0).value("note", Value::String("x".into()))],
    );
    e.compact_all().unwrap();
    let rows = Engine::open(Arc::clone(&store), opts())
        .unwrap()
        .latest(&all("t"))
        .unwrap()
        .rows;
    assert_eq!(rows[0].values, vec![Some(Value::F64(1.0)), None]);
    assert_eq!(
        rows[1].values,
        vec![Some(Value::F64(1.0)), Some(Value::String("x".into()))]
    );
}

/// Readers on pinned snapshots keep getting identical results while another thread
/// writes, compacts and collects garbage (grace 0).
#[test]
fn snapshots_are_stable_under_concurrent_writes_compaction_and_gc() {
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Arc::new(Engine::open(Arc::clone(&store), test_options(3, 4, true)).unwrap());
    setup(&e);
    for i in 0..10 {
        write(
            &e,
            i,
            vec![row("a", i % 4, i, i as f64), row("b", i, i, 0.5)],
        );
    }
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (e, stop) = (Arc::clone(&e), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut i = 10;
            while !stop.load(Ordering::SeqCst) {
                write(
                    &e,
                    i,
                    vec![row("a", i % 4, i, i as f64), row("c", i % 9, i, 1.0)],
                );
                if i % 3 == 0 {
                    e.compact_all().unwrap();
                    e.gc().unwrap();
                }
                i += 1;
            }
            i
        })
    };
    let readers: Vec<_> = (0..3)
        .map(|k| {
            let e = Arc::clone(&e);
            std::thread::spawn(move || {
                for _ in 0..15 {
                    let snap = e.snapshot().unwrap();
                    let grid = GridRequest {
                        table: "t".into(),
                        frontiers: (0..12).map(h).collect(),
                        window: GridWindow::Relative {
                            start: muninn_core::Duration::from_micros(-5 * HOUR),
                            end: muninn_core::Duration::from_micros(5 * HOUR),
                        },
                        clock: Some(Clock::Received),
                        ..GridRequest::default()
                    };
                    let first = (fingerprint(&snap), snap.grid(&grid).unwrap());
                    for _ in 0..4 {
                        std::thread::yield_now();
                        assert_eq!(fingerprint(&snap), first.0, "reader {k}");
                        assert_eq!(snap.grid(&grid).unwrap(), first.1, "reader {k}");
                    }
                }
            })
        })
        .collect();
    for r in readers {
        r.join().unwrap();
    }
    stop.store(true, Ordering::SeqCst);
    let n = writer.join().unwrap();
    assert!(n > 10);
    // Everything committed is there after a reopen.
    let reopened = Engine::open(Arc::clone(&store), opts()).unwrap();
    assert_eq!(fingerprint(&reopened), fingerprint(&*e));
}

#[test]
fn ambiguous_commit_is_visible_after_reload() {
    // A commit that took effect but reported failure: the engine must reload (not reuse
    // the seq range) and the retry is a harmless duplicate.
    let inner: Mem = Arc::new(MemoryStore::new());
    let store = Arc::new(FaultyStore::new(Arc::clone(&inner), 7));
    // A grace period, so the commit needs no head probe (the previous commit confirmed
    // the head, decision 0007).
    let opts = EngineOptions {
        gc_grace: std::time::Duration::from_secs(3600),
        ..test_options(4, 0, false)
    };
    let e = Engine::open(Arc::clone(&store), opts).unwrap();
    e.create_table(&CreateTable::new(
        "t",
        vec![ColumnDef::new("value", ColumnType::F64)],
    ))
    .unwrap();
    e.create_series(&CreateSeries::new("a", "t")).unwrap();
    // The write does: segment put_if_absent, then the log put_if_absent. Crash after the
    // log put took effect.
    let at = store.op_count() + 1;
    store.set_crash_at(at, true);
    let batch = WriteBatch::new(h(1), vec![WriteRow::new("a", h(0), h(0))]);
    assert_eq!(e.write(&batch).unwrap_err().kind(), ErrorKind::Io);
    store.restart();
    let r = e.write(&batch).unwrap();
    assert_eq!((r.stored, r.duplicates), (0, 1));
    let r = e
        .write(&WriteBatch::new(h(1), vec![WriteRow::new("a", h(1), h(0))]))
        .unwrap();
    assert_eq!(r.first_seq, Some(2));
}

/// Rough timings on a forecast-like workload (`cargo test --release -- --ignored
/// perf_smoke --nocapture`). Not a benchmark; a guard against accidental cliffs.
#[test]
#[ignore]
fn perf_smoke() {
    use std::time::Instant;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(muninn_store::LocalFsStore::open(dir.path()).unwrap());
    let e = Engine::open(Arc::clone(&store), EngineOptions::default()).unwrap();
    e.create_table(&CreateTable::new(
        "t",
        vec![ColumnDef::new("value", ColumnType::F64)],
    ))
    .unwrap();
    let nseries = 20;
    for s in 0..nseries {
        e.create_series(&CreateSeries::new(format!("s{s}"), "t"))
            .unwrap();
    }
    // Every 6 h a forecast run for the next 3 days at 15-minute resolution, 30 days.
    let q = HOUR / 4;
    let t0 = Instant::now();
    let mut rows_total = 0;
    for run in 0..120i64 {
        let issued = run * 6 * HOUR;
        let mut rows = Vec::new();
        for s in 0..nseries {
            for k in 0..(72 * 4) {
                let vt = issued + k * q;
                rows.push(
                    WriteRow::new(
                        format!("s{s}"),
                        muninn_core::Timestamp::from_micros(BASE + vt).unwrap(),
                        muninn_core::Timestamp::from_micros(BASE + issued).unwrap(),
                    )
                    .value("value", Value::F64((run * 7 + k + s) as f64 * 0.1)),
                );
            }
        }
        rows_total += rows.len();
        e.write(&WriteBatch::new(
            muninn_core::Timestamp::from_micros(BASE + issued).unwrap(),
            rows,
        ))
        .unwrap();
    }
    eprintln!("write {rows_total} rows in 120 batches: {:?}", t0.elapsed());
    let t = Instant::now();
    e.compact_all().unwrap();
    eprintln!("compact_all: {:?}", t.elapsed());
    let e = Engine::open(Arc::clone(&store), EngineOptions::default()).unwrap();
    let t = Instant::now();
    let r = e.latest(&all("t")).unwrap();
    eprintln!("latest all ({} rows): {:?}", r.rows.len(), t.elapsed());
    let t = Instant::now();
    let r = e
        .latest(&LatestRequest {
            table: "t".into(),
            series: vec!["s3".into()],
            range: TimeRange::new(Some(h(240)), Some(h(264))),
            ..LatestRequest::default()
        })
        .unwrap();
    eprintln!(
        "latest one series one day ({} rows): {:?}",
        r.rows.len(),
        t.elapsed()
    );
    let t = Instant::now();
    let g = e
        .grid(&GridRequest {
            table: "t".into(),
            frontiers: (0..30).map(|d| h(d * 24 + 10)).collect(),
            window: GridWindow::Relative {
                start: muninn_core::Duration::from_micros(14 * HOUR),
                end: muninn_core::Duration::from_micros(38 * HOUR),
            },
            ..GridRequest::default()
        })
        .unwrap();
    eprintln!(
        "grid 30 frontiers ({} rows): {:?}",
        g.rows.len(),
        t.elapsed()
    );
    let t = Instant::now();
    let hst = e.history(&all("t")).unwrap();
    eprintln!("history all ({} rows): {:?}", hst.rows.len(), t.elapsed());
}

/// A checkpoint is never trusted over the log: tampered content (valid JSON, wrong
/// checksum) or a checkpoint that does not belong to this log's entry is skipped.
#[test]
fn tampered_or_foreign_checkpoints_fall_back_to_the_log() {
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), opts()).unwrap();
    setup(&e);
    write(&e, 1, vec![row("a", 0, 0, 1.0)]);
    e.checkpoint().unwrap();
    write(&e, 2, vec![row("a", 1, 1, 2.0)]);
    let expected = fingerprint(&e);
    let v = e.version() - 1;
    let ck = log::checkpoint_key(v);
    let good = store.get(&ck).unwrap();
    // Tamper: drop a series from the checkpoint but keep it valid JSON.
    let text = String::from_utf8(good.clone()).unwrap();
    let tampered = text.replacen("\"next_seq\":2", "\"next_seq\":9", 1);
    assert_ne!(tampered, text);
    store.put(&ck, tampered.as_bytes()).unwrap();
    assert_eq!(
        fingerprint(&Engine::open(Arc::clone(&store), opts()).unwrap()),
        expected
    );
    // A checkpoint (correctly sealed) from a different database at the same version.
    let other: Mem = Arc::new(MemoryStore::new());
    let o = Engine::open(Arc::clone(&other), opts()).unwrap();
    setup(&o);
    write(&o, 1, vec![row("b", 5, 5, 7.0)]);
    o.checkpoint().unwrap();
    assert_eq!(o.version(), v);
    store.put(&ck, &other.get(&ck).unwrap()).unwrap();
    store
        .put(&log::hint_key(), format!("{{\"version\": {v}}}").as_bytes())
        .unwrap();
    let reopened = Engine::open(Arc::clone(&store), opts()).unwrap();
    assert_eq!(fingerprint(&reopened), expected);
    // The genuine checkpoint is used again once restored.
    store.put(&ck, &good).unwrap();
    assert_eq!(
        fingerprint(&Engine::open(Arc::clone(&store), opts()).unwrap()),
        expected
    );
}

fn rewrite_entry(store: &MemoryStore, v: u64, f: impl FnOnce(&mut serde_json::Value)) {
    let k = log::entry_key(v);
    let mut j: serde_json::Value = serde_json::from_slice(&store.get(&k).unwrap()).unwrap();
    f(&mut j);
    store.put(&k, &serde_json::to_vec(&j).unwrap()).unwrap();
}

/// A write entry whose segments do not hold exactly its seq range is corruption.
#[test]
fn inconsistent_log_entries_are_rejected() {
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), test_options(4, 0, false)).unwrap();
    setup(&e);
    write(&e, 1, vec![row("a", 0, 0, 1.0), row("a", 30, 0, 1.0)]); // two partitions
    let v = e.version();
    // Claim one more seq than the segments hold (as if two segments shared a seq).
    rewrite_entry(&store, v, |j| j["next_seq"] = serde_json::json!(4));
    let err = Engine::open(Arc::clone(&store), test_options(4, 0, false)).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Io);
    assert!(err.message().contains("corrupt"), "{err}");
    rewrite_entry(&store, v, |j| {
        j["next_seq"] = serde_json::json!(3);
        j["kind"] = serde_json::json!("gc");
    });
    assert!(Engine::open(Arc::clone(&store), test_options(4, 0, false)).is_err());
}

/// A segment whose value column has a type the catalog does not give that column is
/// reported as corruption, not decoded.
#[test]
fn footer_column_types_are_checked_against_the_catalog() {
    use muninn_format::{Partition, ValueColumn, write_segment};
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), test_options(4, 0, false)).unwrap();
    setup(&e);
    write(&e, 1, vec![WriteRow::new("a", h(0), h(0))]);
    let key = e.live_segment_keys().into_iter().next().unwrap();
    let file = muninn_format::SegmentFile::open(&store.get(&key).unwrap())
        .unwrap()
        .versions()
        .unwrap();
    let bytes = write_segment(
        "t",
        Partition::of(muninn_core::Partitioning::Day, h(0)),
        &[ValueColumn {
            id: 0,
            ty: ColumnType::I64,
        }],
        &file,
        &muninn_format::WriteOptions::default(),
    )
    .unwrap();
    store.put(&key, &bytes).unwrap();
    rewrite_entry(&store, e.version(), |j| {
        j["add"][0]["size"] = serde_json::json!(bytes.len())
    });
    let fresh = Engine::open(Arc::clone(&store), test_options(4, 0, false)).unwrap();
    let err = fresh.latest(&all("t")).unwrap_err();
    assert!(err.message().contains("is not a column of table"), "{err}");
}

/// A store whose segment puts stall.
struct SlowSegments(Mem, std::time::Duration);

impl Store for SlowSegments {
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
        let r = self.0.put_if_absent(k, d);
        if k.as_str().starts_with("segments/") {
            std::thread::sleep(self.1);
        }
        r
    }
    fn list(&self, p: &str) -> muninn_store::Result<Vec<Key>> {
        self.0.list(p)
    }
    fn delete(&self, k: &Key) -> muninn_store::Result<()> {
        self.0.delete(k)
    }
}

/// A writer that stalled for more than half the GC grace period after writing its
/// segments does not commit them (a GC elsewhere may already treat them as orphans).
#[test]
fn stalled_writer_does_not_commit_old_segments() {
    let inner: Mem = Arc::new(MemoryStore::new());
    let o = EngineOptions {
        gc_grace: std::time::Duration::from_millis(40),
        ..test_options(4, 0, false)
    };
    let e = Engine::open(
        SlowSegments(Arc::clone(&inner), std::time::Duration::from_millis(30)),
        o,
    )
    .unwrap();
    e.create_table(&CreateTable::new(
        "t",
        vec![ColumnDef::new("value", ColumnType::F64)],
    ))
    .unwrap();
    e.create_series(&CreateSeries::new("a", "t")).unwrap();
    let v = e.version();
    let err = e
        .write(&WriteBatch::new(h(1), vec![WriteRow::new("a", h(0), h(0))]))
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Io);
    assert_eq!(e.version(), v);
    assert!(e.live_segment_keys().is_empty());
}
