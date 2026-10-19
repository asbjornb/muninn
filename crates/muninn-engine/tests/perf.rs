//! Rough performance probes (ignored by default; run with
//! `cargo test --release -p muninn-engine --test perf -- --ignored --nocapture`).
//! They print timings and assert nothing about speed, only about results.

mod common;

use std::sync::Arc;
use std::time::Instant;

use common::*;
use muninn_core::api::*;
use muninn_core::{ColumnDef, ColumnType, Database, Duration, Reader, TimeRange, Timestamp, Value};
use muninn_engine::{Engine, EngineOptions};
use muninn_store::{LocalFsStore, MemoryStore, Store};

const Q: i64 = HOUR / 4;

fn ts(us: i64) -> Timestamp {
    Timestamp::from_micros(BASE + us).unwrap()
}

/// One forecast run: `series` issued at `issued` (µs after BASE) for the next 10 days at
/// 15-minute resolution (960 rows), values rounded to 0.1 like a real feed.
fn run_rows(series: &str, issued: i64, salt: i64) -> Vec<WriteRow> {
    (0..960)
        .map(|k| {
            let v = ((issued / HOUR * 7 + k * 13 + salt) % 1000) as f64 / 10.0;
            WriteRow::new(series, ts(issued + k * Q), ts(issued))
                .value("value", Value::F64(v))
                .value("issue_time", Value::Timestamp(ts(issued)))
        })
        .collect()
}

fn setup<D: Database>(db: &D, nseries: usize) {
    db.create_table(&CreateTable::new(
        "fc",
        vec![
            ColumnDef::new("value", ColumnType::F64),
            ColumnDef::new("issue_time", ColumnType::Timestamp),
        ],
    ))
    .unwrap();
    for s in 0..nseries {
        db.create_series(&CreateSeries::new(format!("s{s}"), "fc"))
            .unwrap();
    }
}

fn dir_bytes(p: &std::path::Path, sub: &str) -> u64 {
    fn walk(p: &std::path::Path) -> u64 {
        let Ok(rd) = std::fs::read_dir(p) else {
            return 0;
        };
        rd.flatten()
            .map(|e| {
                let m = e.metadata().unwrap();
                if m.is_dir() { walk(&e.path()) } else { m.len() }
            })
            .sum()
    }
    walk(&p.join(sub))
}

/// Writes `runs` hourly runs of 2 series (one 960-row write per series and run), with
/// compaction every 200 writes; prints the mean write latency per window of runs.
fn write_history<S: Store>(e: &Engine<S>, runs: i64) {
    let mut window = Vec::new();
    let mut writes = 0;
    for run in 0..runs {
        for s in 0..2 {
            let rows = run_rows(&format!("s{s}"), run * HOUR, s);
            let t = Instant::now();
            let r = e.write(&WriteBatch::new(ts(run * HOUR), rows)).unwrap();
            window.push(t.elapsed());
            assert_eq!(r.stored, 960);
            writes += 1;
            if writes % 200 == 0 {
                e.compact_all().unwrap();
                e.gc().unwrap();
            }
        }
        if (run + 1) % 50 == 0 {
            let mean = window.iter().sum::<std::time::Duration>() / window.len() as u32;
            eprintln!(
                "  runs {:>4}..{:>4}: mean write {:>8.2?} ({} versions/point max)",
                run + 1 - 50,
                run + 1,
                mean,
                (run + 1).min(240)
            );
            window.clear();
        }
    }
    // A re-send of the newest run is entirely duplicates; of an old run too.
    for run in [runs - 1, runs / 2] {
        let t = Instant::now();
        let r = e
            .write(&WriteBatch::new(
                ts(runs * HOUR),
                run_rows("s0", run * HOUR, 0),
            ))
            .unwrap();
        assert_eq!((r.stored, r.duplicates), (0, 960));
        eprintln!("  re-send of run {run}: {:?}", t.elapsed());
    }
}

#[test]
#[ignore]
fn perf_write_history() {
    let runs: i64 = std::env::var("PERF_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);
    let opts = EngineOptions {
        gc_grace: std::time::Duration::ZERO,
        ..EngineOptions::default()
    };
    eprintln!("memory store:");
    let e = Engine::open(Arc::new(MemoryStore::new()), opts.clone()).unwrap();
    setup(&e, 2);
    write_history(&e, runs);
    eprintln!("local fs:");
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(Arc::new(LocalFsStore::open(dir.path()).unwrap()), opts).unwrap();
    setup(&e, 2);
    let t = Instant::now();
    write_history(&e, runs);
    eprintln!(
        "  total {:?}; segments {} B, log {} B",
        t.elapsed(),
        dir_bytes(dir.path(), "segments"),
        dir_bytes(dir.path(), "log")
    );
}

#[test]
#[ignore]
fn perf_create_series() {
    let n: usize = std::env::var("PERF_SERIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000);
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(
        Arc::new(LocalFsStore::open(dir.path()).unwrap()),
        EngineOptions::default(),
    )
    .unwrap();
    setup(&e, 0);
    let t = Instant::now();
    for s in 0..n {
        e.create_series(&CreateSeries::new(format!("series-{s:06}"), "fc"))
            .unwrap();
        if (s + 1) % 1000 == 0 {
            eprintln!("  {} series: {:?}", s + 1, t.elapsed());
        }
    }
    eprintln!(
        "one by one: {n} series in {:?}; log {} B",
        t.elapsed(),
        dir_bytes(dir.path(), "log")
    );
    let t = Instant::now();
    let e = Engine::open(
        Arc::new(LocalFsStore::open(dir.path()).unwrap()),
        EngineOptions::default(),
    )
    .unwrap();
    eprintln!("reopen: {:?}", t.elapsed());
    assert_eq!(e.table_series("fc").unwrap().len(), n);
    drop(e);
    // The same as one batch per 10 000 series.
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(
        Arc::new(LocalFsStore::open(dir.path()).unwrap()),
        EngineOptions::default(),
    )
    .unwrap();
    setup(&e, 0);
    let t = Instant::now();
    let reqs: Vec<CreateSeries> = (0..n)
        .map(|s| CreateSeries::new(format!("series-{s:06}"), "fc"))
        .collect();
    for chunk in reqs.chunks(10_000) {
        e.create_series_batch(chunk).unwrap();
    }
    eprintln!(
        "batched: {n} series in {:?}; log {} B",
        t.elapsed(),
        dir_bytes(dir.path(), "log")
    );
    e.gc().unwrap();
    eprintln!("after gc: log {} B", dir_bytes(dir.path(), "log"));
    let t = Instant::now();
    let e = Engine::open(
        Arc::new(LocalFsStore::open(dir.path()).unwrap()),
        EngineOptions::default(),
    )
    .unwrap();
    eprintln!("reopen: {:?}", t.elapsed());
    assert_eq!(e.table_series("fc").unwrap().len(), n);
}

/// A forecast-like table: `runs` hourly runs of `nseries` series, compacted.
fn forecast_db(dir: &std::path::Path, nseries: usize, runs: i64) -> Engine<Arc<LocalFsStore>> {
    let opts = EngineOptions {
        gc_grace: std::time::Duration::ZERO,
        ..EngineOptions::default()
    };
    let e = Engine::open(Arc::new(LocalFsStore::open(dir).unwrap()), opts.clone()).unwrap();
    setup(&e, nseries);
    for run in 0..runs {
        let mut rows = Vec::new();
        for s in 0..nseries {
            rows.extend(run_rows(&format!("s{s}"), run * HOUR, s as i64));
        }
        e.write(&WriteBatch::new(ts(run * HOUR), rows)).unwrap();
        if (run + 1) % 250 == 0 {
            e.compact_all().unwrap();
            e.gc().unwrap();
        }
    }
    e.compact_all().unwrap();
    e.gc().unwrap();
    drop(e);
    Engine::open(Arc::new(LocalFsStore::open(dir).unwrap()), opts).unwrap()
}

fn time<T>(what: &str, reps: u32, mut f: impl FnMut() -> T) -> T {
    let mut out = f();
    let t = Instant::now();
    for _ in 0..reps {
        out = f();
    }
    eprintln!("  {what}: {:?}", t.elapsed() / reps);
    out
}

/// Engine-column decode throughput over every `.seg` file under `$MUNINN_PERF_SEGMENTS`
/// (e.g. a bench database's `segments/` directory).
#[test]
#[ignore]
fn perf_decode() {
    let Ok(dir) = std::env::var("MUNINN_PERF_SEGMENTS") else {
        eprintln!("set MUNINN_PERF_SEGMENTS");
        return;
    };
    fn files(p: &std::path::Path, out: &mut Vec<Vec<u8>>) {
        for e in std::fs::read_dir(p).unwrap().flatten() {
            let path = e.path();
            if path.is_dir() {
                files(&path, out);
            } else if path.extension().is_some_and(|x| x == "seg") {
                out.push(std::fs::read(&path).unwrap());
            }
        }
    }
    let mut segs = Vec::new();
    files(std::path::Path::new(&dir), &mut segs);
    let opened: Vec<muninn_format::SegmentFile<'_>> = segs
        .iter()
        .map(|b| muninn_format::SegmentFile::open(b).unwrap())
        .collect();
    let rows: u64 = opened.iter().map(|f| f.footer().row_count).sum();
    let bytes: usize = segs.iter().map(Vec::len).sum();
    for pass in 0..3 {
        let t = Instant::now();
        let mut n = 0;
        for f in &opened {
            for b in 0..f.footer().blocks.len() {
                n += f.engine_block(b).unwrap().len();
            }
        }
        let el = t.elapsed();
        eprintln!(
            "pass {pass}: {} segments, {n} rows ({bytes} B): {el:?}, {:.1} ns/row",
            opened.len(),
            el.as_nanos() as f64 / rows as f64
        );
    }
    let t = Instant::now();
    let mut n = 0;
    for f in &opened {
        for b in 0..f.footer().blocks.len() {
            for p in 0..f.footer().columns.len() {
                n += f.value_chunk(b, p).unwrap().len();
            }
        }
    }
    eprintln!("value columns: {n} values in {:?}", t.elapsed());
}

#[test]
#[ignore]
fn perf_reads() {
    let runs: i64 = std::env::var("PERF_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(24 * 60);
    let dir = tempfile::tempdir().unwrap();
    let t = Instant::now();
    let e = forecast_db(dir.path(), 2, runs);
    eprintln!(
        "built {runs} runs x 2 series in {:?}; segments {} B, log {} B",
        t.elapsed(),
        dir_bytes(dir.path(), "segments"),
        dir_bytes(dir.path(), "log")
    );
    let days = runs / 24;
    let grid = GridRequest {
        table: "fc".into(),
        series: vec!["s0".into()],
        frontiers: (1..days - 1)
            .map(|d| ts(d * 24 * HOUR + 10 * HOUR))
            .collect(),
        window: GridWindow::Relative {
            start: Duration::from_micros(0),
            end: Duration::from_micros(36 * HOUR),
        },
        ..GridRequest::default()
    };
    let g = time("grid one series", 3, || e.grid(&grid).unwrap());
    eprintln!("    ({} rows)", g.rows.len());
    let snap = e.snapshot().unwrap();
    assert_eq!(
        format!("{:?}", snap.grid_by_repeated_latest(&grid).unwrap()),
        format!("{g:?}")
    );
    let from = days / 2 * 24 * HOUR + 3 * HOUR;
    let replay = ReplayRequest {
        table: "fc".into(),
        arrival: TimeRange::new(Some(ts(from)), Some(ts(from + 6 * HOUR))),
        ..ReplayRequest::default()
    };
    let r = time("replay 6h", 10, || e.replay(&replay).unwrap());
    eprintln!("    ({} rows)", r.rows.len());
    let hist = LatestRequest {
        table: "fc".into(),
        series: vec!["s1".into()],
        range: TimeRange::new(
            Some(ts(days / 2 * 24 * HOUR)),
            Some(ts((days / 2 + 1) * 24 * HOUR)),
        ),
        ..LatestRequest::default()
    };
    let r = time("history one series-day", 10, || e.history(&hist).unwrap());
    eprintln!("    ({} rows)", r.rows.len());
    let latest = LatestRequest {
        table: "fc".into(),
        range: TimeRange::new(Some(ts(days * 24 * HOUR - 24 * HOUR)), None),
        ..LatestRequest::default()
    };
    let r = time("latest now, all series", 10, || e.latest(&latest).unwrap());
    eprintln!("    ({} rows)", r.rows.len());
    let lead = LatestRequest {
        table: "fc".into(),
        range: TimeRange::new(Some(ts(24 * HOUR)), Some(ts(31 * 24 * HOUR))),
        ..LatestRequest::default()
    };
    let r = time("history 30 days, all series", 3, || {
        e.history(&lead).unwrap()
    });
    eprintln!("    ({} rows)", r.rows.len());
}
