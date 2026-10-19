//! Log truncation by GC and size-triggered checkpoints (decision 0007, FORMAT.md §3.6),
//! and batch series creation.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use common::*;
use muninn_core::api::*;
use muninn_core::{ColumnDef, ColumnType, Database, ErrorKind, ModelDb, Value};
use muninn_engine::log::{self, LogEntry};
use muninn_engine::{Engine, EngineOptions};
use muninn_store::{MemoryStore, Store};
use proptest::prelude::*;

type Mem = Arc<MemoryStore>;

fn setup<D: Database>(db: &D) {
    db.create_table(&CreateTable::new(
        "t",
        vec![ColumnDef::new("value", ColumnType::F64)],
    ))
    .unwrap();
    db.create_series_batch(&[CreateSeries::new("a", "t"), CreateSeries::new("b", "t")])
        .unwrap();
}

fn row(series: &str, vt: i64, rt: i64, v: f64) -> WriteRow {
    WriteRow::new(series, h(vt), h(rt)).value("value", Value::F64(v))
}

fn write_retrying<D: Database>(db: &D, batch: &WriteBatch) -> WriteResult {
    match db.write(batch) {
        Err(e) if e.kind() == ErrorKind::Conflict => db.write(batch).unwrap(),
        r => r.unwrap(),
    }
}

fn entries(store: &MemoryStore) -> Vec<u64> {
    let mut v: Vec<u64> = store
        .list("log/")
        .unwrap()
        .iter()
        .filter_map(|k| log::parse_entry_key(k.as_str()))
        .collect();
    v.sort_unstable();
    v
}

fn checkpoints(store: &MemoryStore) -> Vec<u64> {
    let mut v: Vec<u64> = store
        .list("log/")
        .unwrap()
        .iter()
        .filter_map(|k| log::parse_checkpoint_key(k.as_str()))
        .collect();
    v.sort_unstable();
    v
}

fn entry_1_is_marker(store: &MemoryStore) -> bool {
    let b = store.get(&log::entry_key(1)).unwrap();
    log::from_json::<LogEntry>(&b, "e1")
        .unwrap()
        .is_truncation_marker()
}

fn opts() -> EngineOptions {
    test_options(4, 0, false) // no automatic checkpoints: GC writes them
}

#[test]
fn gc_truncates_the_log_and_reopen_recovers() {
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), opts()).unwrap();
    setup(&e);
    for i in 0..30 {
        e.write(&WriteBatch::new(h(i), vec![row("a", i % 5, i, i as f64)]))
            .unwrap();
        if i % 10 == 9 {
            e.compact_all().unwrap();
            let r = e.gc().unwrap();
            if i > 9 {
                assert!(!r.log_deleted.is_empty(), "gc {i} truncated nothing");
            }
        }
    }
    let head = e.version();
    let cks = checkpoints(&store);
    assert_eq!(cks.len(), 2, "keeps exactly two checkpoints: {cks:?}");
    assert_eq!(*cks.last().unwrap(), head);
    let es = entries(&store);
    assert_eq!(es[0], 1);
    assert!(entry_1_is_marker(&store));
    // Entries 2..T are gone, T..=head remain (T = the older checkpoint).
    assert_eq!(es[1..].to_vec(), (cks[0]..=head).collect::<Vec<_>>());
    let expected = fingerprint(&e);
    let catalog = e.catalog_log().unwrap();
    let r = Engine::open(Arc::clone(&store), opts()).unwrap();
    assert_eq!(fingerprint(&r), expected);
    assert_eq!(r.catalog_log().unwrap(), catalog);
    assert_eq!(r.version(), head);
    // The newest checkpoint damaged: the older one plus the retained entries suffice.
    store
        .put(&log::checkpoint_key(head), b"{\"format\": 2, \"vers")
        .unwrap();
    store.delete(&log::hint_key()).unwrap();
    let r = Engine::open(Arc::clone(&store), opts()).unwrap();
    assert_eq!(fingerprint(&r), expected);
    // Both damaged: replay from the start is impossible, which is an error — never an
    // empty database.
    store
        .put(&log::checkpoint_key(cks[0]), b"{\"format\": 2, \"vers")
        .unwrap();
    let err = Engine::open(Arc::clone(&store), opts()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Io);
    assert!(err.message().contains("truncat"), "{err}");
}

/// A writer whose state lags behind a truncation must not re-create a deleted entry: its
/// commit conflicts, the retry reloads from a checkpoint, and nothing is lost.
#[test]
fn lagging_writer_after_truncation_conflicts_and_recovers() {
    let store: Mem = Arc::new(MemoryStore::new());
    let model = ModelDb::new();
    let a = Engine::open(Arc::clone(&store), opts()).unwrap();
    setup(&a);
    setup(&model);
    let b = Engine::open(Arc::clone(&store), opts()).unwrap();
    let lag = b.version();
    for i in 0..12 {
        let batch = WriteBatch::new(h(i), vec![row("a", i, i, 1.0)]);
        a.write(&batch).unwrap();
        model.write(&batch).unwrap();
        if i % 4 == 3 {
            a.gc().unwrap();
        }
    }
    assert!(
        !entries(&store).contains(&(lag + 1)),
        "b's next entry was truncated"
    );
    // b's reads are of its own (old) snapshot until it refreshes; its write conflicts.
    let batch = WriteBatch::new(h(20), vec![row("b", 0, 0, 2.0)]);
    let got = write_retrying(&b, &batch);
    let want = model.write(&batch).unwrap();
    assert_eq!(format!("{got:?}"), format!("{want:?}"));
    assert!(
        !entries(&store).contains(&(lag + 1)),
        "re-created a deleted entry"
    );
    assert_eq!(fingerprint(&b), fingerprint(&model));
    a.refresh().unwrap();
    assert_eq!(fingerprint(&a), fingerprint(&model));
    let fresh = Engine::open(Arc::clone(&store), opts()).unwrap();
    assert_eq!(fingerprint(&fresh), fingerprint(&model));
}

/// The same for a reader: `refresh` notices the truncation and reloads.
#[test]
fn lagging_reader_refreshes_across_a_truncation() {
    let store: Mem = Arc::new(MemoryStore::new());
    let a = Engine::open(Arc::clone(&store), opts()).unwrap();
    setup(&a);
    let b = Engine::open(Arc::clone(&store), opts()).unwrap();
    for i in 0..9 {
        a.write(&WriteBatch::new(h(i), vec![row("a", i, i, 1.0)]))
            .unwrap();
        if i % 3 == 2 {
            a.gc().unwrap();
        }
    }
    assert_eq!(b.refresh().unwrap(), a.version());
    assert_eq!(fingerprint(&b), fingerprint(&a));
}

/// An engine opened on an empty store (version 0) writes after another engine created and
/// truncated the log: entry 1 is the marker, so it can neither be re-created nor mistaken
/// for an empty log.
#[test]
fn writer_at_version_zero_after_truncation() {
    let store: Mem = Arc::new(MemoryStore::new());
    let z = Engine::open(Arc::clone(&store), opts()).unwrap();
    let a = Engine::open(Arc::clone(&store), opts()).unwrap();
    setup(&a);
    for i in 0..6 {
        a.write(&WriteBatch::new(h(i), vec![row("a", i, i, 1.0)]))
            .unwrap();
        a.gc().unwrap();
    }
    assert!(entry_1_is_marker(&store));
    let req = CreateTable::new("u", vec![]);
    let r = match z.create_table(&req) {
        Err(e) if e.kind() == ErrorKind::Conflict => z.create_table(&req),
        r => r,
    };
    r.unwrap();
    assert_eq!(z.table_series("t").unwrap().len(), 2);
    a.refresh().unwrap();
    assert_eq!(fingerprint(&a), fingerprint(&z));
}

/// GC removes log files only after it has seen them truncatable for the grace period
/// (its own clock), like segments.
#[test]
fn log_truncation_waits_for_the_grace_period() {
    let clock = Arc::new(AtomicI64::new(1_000_000_000));
    let c = Arc::clone(&clock);
    let o = EngineOptions {
        gc_grace: std::time::Duration::from_secs(60),
        wall_clock: Arc::new(move || c.load(Ordering::SeqCst)),
        ..opts()
    };
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), o).unwrap();
    setup(&e);
    for i in 0..4 {
        e.write(&WriteBatch::new(h(i), vec![row("a", i, i, 1.0)]))
            .unwrap();
        assert!(e.gc().unwrap().log_deleted.is_empty());
    }
    assert!(!entry_1_is_marker(&store));
    let before = entries(&store).len();
    clock.fetch_add(61_000_000, Ordering::SeqCst);
    let r = e.gc().unwrap();
    assert!(!r.log_deleted.is_empty());
    assert!(entry_1_is_marker(&store));
    assert!(entries(&store).len() < before);
    let reopened = Engine::open(Arc::clone(&store), opts()).unwrap();
    assert_eq!(fingerprint(&reopened), fingerprint(&e));
}

/// Databases written before log format 2 (entries and checkpoints with `format: 1`)
/// are read unchanged.
#[test]
fn format_1_logs_are_read() {
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), opts()).unwrap();
    setup(&e);
    for i in 0..5 {
        e.write(&WriteBatch::new(h(i), vec![row("a", i, i, 1.0)]))
            .unwrap();
    }
    let expected = fingerprint(&e);
    for v in entries(&store) {
        let key = log::entry_key(v);
        let mut j: serde_json::Value = serde_json::from_slice(&store.get(&key).unwrap()).unwrap();
        j["format"] = 1.into();
        store.put(&key, &serde_json::to_vec(&j).unwrap()).unwrap();
    }
    let r = Engine::open(Arc::clone(&store), opts()).unwrap();
    assert_eq!(fingerprint(&r), expected);
    // ... and continue them.
    r.write(&WriteBatch::new(h(9), vec![row("b", 1, 1, 1.0)]))
        .unwrap();
    r.checkpoint().unwrap();
    let r2 = Engine::open(Arc::clone(&store), opts()).unwrap();
    assert_eq!(fingerprint(&r2), fingerprint(&r));
}

/// With the size factor, checkpoint bytes stay proportional to the log written, not
/// quadratic in it (the old fixed interval wrote the whole state every 100 commits).
#[test]
fn checkpoints_are_size_triggered() {
    let store: Mem = Arc::new(MemoryStore::new());
    let o = EngineOptions {
        checkpoint_interval: 1,
        checkpoint_size_factor: 1.0,
        ..test_options(16, 1, false)
    };
    let e = Engine::open(Arc::clone(&store), o.clone()).unwrap();
    setup(&e);
    // Growing state (many series), small entries.
    let names: Vec<CreateSeries> = (0..2000)
        .map(|i| CreateSeries::new(format!("s{i}"), "t"))
        .collect();
    e.create_series_batch(&names).unwrap();
    for i in 0..200 {
        e.write(&WriteBatch::new(
            h(i),
            vec![row(&format!("s{}", i % 2000), i, i, 1.0)],
        ))
        .unwrap();
    }
    let size = |k: muninn_store::Key| store.get(&k).unwrap().len();
    let entry_bytes: usize = entries(&store)
        .into_iter()
        .map(|v| size(log::entry_key(v)))
        .sum();
    let cks: Vec<usize> = checkpoints(&store)
        .into_iter()
        .map(|v| size(log::checkpoint_key(v)))
        .collect();
    assert!(
        cks.len() <= 4,
        "{} checkpoints for 200 small writes",
        cks.len()
    );
    // Each checkpoint is written only after the entries since the previous one reach
    // that one's size: all checkpoints but the newest are paid for by log entries.
    let paid: usize = cks[..cks.len() - 1].iter().sum();
    assert!(
        paid <= entry_bytes,
        "{paid} checkpoint bytes vs {entry_bytes} entry bytes"
    );
    let r = Engine::open(Arc::clone(&store), o).unwrap();
    assert_eq!(fingerprint(&r), fingerprint(&e));
}

// ---------------------------------------------------------------------------------------
// create_series_batch
// ---------------------------------------------------------------------------------------

#[test]
fn create_series_batch_is_one_atomic_commit() {
    let store: Mem = Arc::new(MemoryStore::new());
    let e = Engine::open(Arc::clone(&store), opts()).unwrap();
    let m = ModelDb::new();
    for db in [&e as &dyn DbLike, &m] {
        db.make_tables();
    }
    let v0 = e.version();
    let reqs: Vec<CreateSeries> = (0..1000)
        .map(|i| CreateSeries::new(format!("s{i}"), "t"))
        .collect();
    let got = e.create_series_batch(&reqs).unwrap();
    assert_eq!(got, m.create_series_batch(&reqs).unwrap());
    assert_eq!(e.version(), v0 + 1);
    assert_eq!(got[999].id.0, 1000);
    // A failure anywhere (here: the last request repeats a name) creates nothing.
    let bad = vec![
        CreateSeries::new("x1", "t"),
        CreateSeries::new("x2", "t"),
        CreateSeries::new("x1", "t"),
    ];
    let v1 = e.version();
    assert_eq!(
        e.create_series_batch(&bad).unwrap_err().kind(),
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        m.create_series_batch(&bad).unwrap_err().kind(),
        ErrorKind::AlreadyExists
    );
    assert_eq!(e.version(), v1);
    assert!(e.series("x1").is_err());
    assert!(e.create_series_batch(&[]).unwrap().is_empty());
    assert_eq!(e.version(), v1);
    assert_eq!(e.catalog_log().unwrap(), m.catalog_log());
    let r = Engine::open(Arc::clone(&store), opts()).unwrap();
    assert_eq!(fingerprint(&r), fingerprint(&m));
}

trait DbLike {
    fn make_tables(&self);
}
impl<D: Database> DbLike for D {
    fn make_tables(&self) {
        self.create_table(&CreateTable::new("t", vec![])).unwrap();
        self.create_table(&CreateTable::new("u", vec![])).unwrap();
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(200), ..ProptestConfig::default() })]

    /// Random batches (unknown tables, empty and repeated names, catalog clocks) give the
    /// engine and the model the same results, the same catalog log and atomicity.
    #[test]
    fn create_series_batch_matches_model(
        batches in prop::collection::vec(
            prop::collection::vec(
                (0usize..6, prop_oneof![8 => Just("t"), 1 => Just("u"), 1 => Just("nope")],
                 prop::option::of(0i64..10)),
                0..5),
            1..6),
    ) {
        let e = Engine::open(Arc::new(MemoryStore::new()), opts()).unwrap();
        let m = ModelDb::new();
        e.make_tables();
        m.make_tables();
        for b in &batches {
            let reqs: Vec<CreateSeries> = b
                .iter()
                .map(|(n, t, c)| CreateSeries {
                    clock: c.map(h),
                    ..CreateSeries::new(["", "a", "b", "c", "d", "a b"][*n], *t)
                })
                .collect();
            let x = e.create_series_batch(&reqs).map_err(|e| e.kind());
            let y = m.create_series_batch(&reqs).map_err(|e| e.kind());
            prop_assert_eq!(x, y);
        }
        prop_assert_eq!(e.catalog_log().unwrap(), m.catalog_log());
        prop_assert_eq!(fingerprint(&e), fingerprint(&m));
    }
}
