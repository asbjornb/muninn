//! Ingest benchmark: small writes from concurrent producers, and a bulk load.
//!
//! ```text
//! cargo run --release -p muninn-engine --example ingest -- --dir /path/on/disk \
//!     [--secs 5] [--producers 1,8,64] [--shapes 96,960] [--bulk-rows 2000000] [--memory]
//! ```
//!
//! Every scenario runs on a fresh database in its own subdirectory of `--dir` (a
//! `LocalFsStore`, so every acknowledged write is durable; `--memory` uses a `MemoryStore`
//! instead to show the CPU cost alone). A table `fc(value f64, issue_time timestamp)` with
//! day partitions and 2 000 series is created first.
//!
//! - **Small writes**: `P` producer threads share one `Engine` and write back to back for
//!   `--secs` seconds. Shape 96 is one series × 96 rows (one day at 15 minutes, one
//!   partition); shape 960 is a forecast run, one series × 960 rows (10 days, 10–11
//!   partitions). Every write has a new received time, so nothing is deduplicated.
//!   Reported: writes/s, rows/s, and p50 / p99 / max latency of `write` (from the call to
//!   its return, i.e. until the write is durable and acknowledged).
//! - **Bulk**: one producer loads `--bulk-rows` rows in batches of 100 000 rows (100
//!   series × 1 000 rows); reported in rows/s.
//!
//! Throughput and latency are measured on the engine's API; building the rows is not
//! timed. After each small-write run the memtable's size is printed with the time of a
//! `latest` over one series and over the whole table, before and after a flush.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use muninn_core::api::{CreateSeries, CreateTable, LatestRequest, WriteBatch, WriteRow};
use muninn_core::{ColumnDef, ColumnType, Database, Reader, Timestamp, Value};
use muninn_engine::{Engine, EngineOptions};
use muninn_store::{LocalFsStore, MemoryStore, Store};

const BASE: i64 = 1_735_689_600_000_000; // 2025-01-01T00:00:00Z
const MIN: i64 = 60_000_000;
const DAY: i64 = 1440 * MIN;
const QUARTER: i64 = 15 * MIN;
const SERIES: usize = 2000;

struct Args {
    dir: Option<std::path::PathBuf>,
    secs: f64,
    producers: Vec<usize>,
    shapes: Vec<usize>,
    bulk_rows: usize,
    memory: bool,
}

fn parse_args() -> Args {
    let mut a = Args {
        dir: None,
        secs: 5.0,
        producers: vec![1, 8, 64],
        shapes: vec![96, 960],
        bulk_rows: 2_000_000,
        memory: false,
    };
    let list = |s: String| -> Vec<usize> {
        s.split(',')
            .filter(|x| !x.is_empty())
            .map(|x| x.parse().expect("a number"))
            .collect()
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| panic!("{arg} needs a value"));
        match arg.as_str() {
            "--dir" => a.dir = Some(val().into()),
            "--secs" => a.secs = val().parse().expect("seconds"),
            "--producers" => a.producers = list(val()),
            "--shapes" => a.shapes = list(val()),
            "--bulk-rows" => a.bulk_rows = val().parse().expect("rows"),
            "--memory" => a.memory = true,
            other => panic!("unknown argument {other}"),
        }
    }
    assert!(
        a.memory || a.dir.is_some(),
        "--dir <path> is required (or --memory)"
    );
    a
}

fn ts(us: i64) -> Timestamp {
    Timestamp::from_micros(BASE + us).expect("valid timestamp")
}

fn setup<D: Database>(db: &D) {
    db.create_table(&CreateTable::new(
        "fc",
        vec![
            ColumnDef::new("value", ColumnType::F64),
            ColumnDef::new("issue_time", ColumnType::Timestamp),
        ],
    ))
    .expect("create table");
    let reqs: Vec<CreateSeries> = (0..SERIES)
        .map(|s| CreateSeries::new(format!("s{s}"), "fc"))
        .collect();
    db.create_series_batch(&reqs).expect("create series");
}

/// `n` rows of one series from `start` at 15 minutes, all received at `received`.
fn rows(series: usize, start: i64, n: usize, received: i64, salt: u64) -> Vec<WriteRow> {
    (0..n as i64)
        .map(|k| {
            let v = ((salt as i64 * 7 + k * 13) % 1000) as f64 / 10.0;
            WriteRow::new(format!("s{series}"), ts(start + k * QUARTER), ts(received))
                .value("value", Value::F64(v))
                .value("issue_time", Value::Timestamp(ts(received)))
        })
        .collect()
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = ((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[i]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn small_writes<S: Store + 'static>(
    engine: Arc<Engine<S>>,
    producers: usize,
    shape: usize,
    secs: f64,
) {
    // Received times: one global counter, so every write is new data.
    let clock = Arc::new(AtomicU64::new(1));
    let deadline = Instant::now() + Duration::from_secs_f64(secs);
    let started = Instant::now();
    let handles: Vec<_> = (0..producers)
        .map(|p| {
            let engine = Arc::clone(&engine);
            let clock = Arc::clone(&clock);
            std::thread::spawn(move || {
                let mut lat = Vec::new();
                let mut i = 0usize;
                while Instant::now() < deadline {
                    let n = clock.fetch_add(1, Ordering::Relaxed);
                    let received = 400 * DAY + n as i64 * 1_000;
                    let series = (p * 7919 + i * 31) % SERIES;
                    // Spread the value days over a month of partitions.
                    let day = ((n as i64) * 7) % 30;
                    let batch =
                        WriteBatch::new(ts(received), rows(series, day * DAY, shape, received, n));
                    let t = Instant::now();
                    let r = engine.write(&batch).expect("write");
                    lat.push(t.elapsed());
                    assert_eq!(r.stored as usize, shape);
                    i += 1;
                }
                lat
            })
        })
        .collect();
    let mut lat: Vec<Duration> = handles
        .into_iter()
        .flat_map(|h| h.join().expect("producer"))
        .collect();
    let wall = started.elapsed().as_secs_f64();
    lat.sort_unstable();
    let writes = lat.len() as f64;
    println!(
        "small  shape={shape:>4} producers={producers:>3}  writes={:>7}  {:>9.0} writes/s  {:>11.0} rows/s  p50={:>8.3} ms  p99={:>8.3} ms  max={:>8.3} ms",
        lat.len(),
        writes / wall,
        writes * shape as f64 / wall,
        ms(pct(&lat, 0.50)),
        ms(pct(&lat, 0.99)),
        ms(*lat.last().unwrap_or(&Duration::ZERO)),
    );
    reads(&engine);
}

/// Memtable size, and the cost of reads over it (one series, the whole table) before
/// and after flushing it.
fn reads<S: Store>(engine: &Engine<S>) {
    let time = |req: &LatestRequest| {
        let t = Instant::now();
        let n = engine.latest(req).expect("latest").rows.len();
        (n, t.elapsed())
    };
    let one = LatestRequest {
        table: "fc".into(),
        series: vec!["s7".into()],
        ..LatestRequest::default()
    };
    let all = LatestRequest {
        table: "fc".into(),
        ..LatestRequest::default()
    };
    let m = engine.memtable();
    let (n1, t1) = time(&one);
    let (na, ta) = time(&all);
    engine.flush().expect("flush");
    let (n1f, t1f) = time(&one);
    let (naf, taf) = time(&all);
    assert_eq!((n1, na), (n1f, naf));
    println!(
        "       memtable: {} rows in {} segments ({} flushes so far); latest one series {:.2} ms, all {:.1} ms ({na} rows); after flush {:.2} / {:.1} ms",
        m.rows,
        m.segments,
        m.flushes,
        ms(t1),
        ms(ta),
        ms(t1f),
        ms(taf),
    );
}

fn bulk<S: Store>(engine: &Engine<S>, total: usize) {
    const PER_SERIES: usize = 1000;
    const SERIES_PER_BATCH: usize = 100;
    let batches = total.div_ceil(PER_SERIES * SERIES_PER_BATCH);
    let mut timed = Duration::ZERO;
    let mut stored = 0u64;
    for b in 0..batches {
        let received = 500 * DAY + b as i64 * MIN;
        let mut batch_rows = Vec::with_capacity(PER_SERIES * SERIES_PER_BATCH);
        for s in 0..SERIES_PER_BATCH {
            let series = (b * SERIES_PER_BATCH + s) % SERIES;
            batch_rows.extend(rows(series, 0, PER_SERIES, received, b as u64));
        }
        let batch = WriteBatch::new(ts(received), batch_rows);
        let t = Instant::now();
        stored += engine.write(&batch).expect("bulk write").stored;
        timed += t.elapsed();
    }
    println!(
        "bulk   rows={stored:>9}  batches={batches}  {:.0} rows/s  ({:.2} s)",
        stored as f64 / timed.as_secs_f64(),
        timed.as_secs_f64()
    );
}

fn open_store(args: &Args, name: &str) -> Arc<dyn Store> {
    if args.memory {
        return Arc::new(MemoryStore::new());
    }
    let dir = args.dir.as_ref().expect("dir").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    Arc::new(LocalFsStore::open(&dir).expect("open store"))
}

/// Lets the filesystem settle between scenarios (writeback of the previous one would
/// otherwise slow the next one's fsyncs down).
fn settle(args: &Args) {
    if !args.memory {
        let _ = std::process::Command::new("sync").status();
        std::thread::sleep(Duration::from_secs(2));
    }
}

fn main() {
    let args = parse_args();
    for &shape in &args.shapes {
        for &p in &args.producers {
            settle(&args);
            let store = open_store(&args, &format!("small-{shape}-{p}"));
            let engine = Arc::new(Engine::open(store, EngineOptions::default()).expect("open"));
            setup(&*engine);
            small_writes(Arc::clone(&engine), p, shape, args.secs);
        }
    }
    if args.bulk_rows > 0 {
        settle(&args);
        let store = open_store(&args, "bulk");
        let engine = Engine::open(store, EngineOptions::default()).expect("open");
        setup(&engine);
        bulk(&engine, args.bulk_rows);
    }
}
