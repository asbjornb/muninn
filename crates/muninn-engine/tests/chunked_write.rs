//! Large writes processed in chunks (`EngineOptions::write_chunk_rows`) must be
//! indistinguishable from processing them whole: the same results (stored, duplicates,
//! `system_time`, first seq), the same stored versions and seqs, the same dedup
//! decisions — duplicates straddling chunk boundaries included — and for invalid input
//! the same error, message included, with nothing committed (SPEC §4, §10.4). Chunk
//! sizes are forced tiny (1, 7, 64 rows) so that every batch spans many chunks. The
//! encoded segments must not depend on the thread count.

mod common;

use std::sync::Arc;

use common::*;
use muninn_core::api::*;
use muninn_core::{
    ColumnDef, ColumnType, Database, ErrorKind, InputValue, ModelDb, Timestamp, Value,
};
use muninn_engine::{Engine, EngineOptions};
use muninn_store::{MemoryStore, Store};
use proptest::prelude::*;

fn opts(chunk: usize, threads: usize, inline_max_rows: usize) -> EngineOptions {
    EngineOptions {
        write_chunk_rows: chunk,
        io_threads: threads,
        inline_max_rows,
        ..test_options(4, 3, false)
    }
}

fn setup<D: Database>(db: &D) {
    db.create_table(&CreateTable::new(
        "t",
        vec![
            ColumnDef::new("a", ColumnType::F64),
            ColumnDef::new("d", ColumnType::String),
        ],
    ))
    .unwrap();
    db.create_table(&CreateTable {
        partition: muninn_core::Partitioning::Month,
        ..CreateTable::new("u", vec![ColumnDef::new("x", ColumnType::I64)])
    })
    .unwrap();
    db.create_series_batch(&[
        CreateSeries::new("s1", "t"),
        CreateSeries::new("s2", "t"),
        CreateSeries::new("s3", "u"),
    ])
    .unwrap();
}

const SERIES_NAMES: [&str; 4] = ["s1", "s2", "s3", "nope"];

/// One generated row from a small domain, so that duplicates (within a batch, across
/// chunks, and against earlier writes) are frequent.
#[derive(Clone, Debug)]
struct Row {
    series: usize,
    vt: i64,
    rt: i64,
    pt: Option<i64>,
    deleted: bool,
    a: Option<f64>,
    d: Option<&'static str>,
    x: Option<i64>,
}

/// An invalid row to put into a batch, at some position.
#[derive(Clone, Copy, Debug)]
enum Bad {
    UnknownSeries,
    UnknownColumn,
    TypeMismatch,
    InvalidTime,
    TombstoneWithValue,
}

fn arb_row() -> impl Strategy<Value = Row> {
    (
        prop_oneof![3 => Just(0usize), 2 => Just(1), 2 => Just(2)],
        0i64..60, // hours: spans three day partitions
        0i64..3,
        prop::option::weighted(0.3, 0i64..3),
        prop::bool::weighted(0.1),
        prop::option::weighted(0.8, prop::sample::select(vec![0.0, -0.0, 1.0, f64::NAN])),
        prop::option::weighted(0.3, prop::sample::select(vec!["", "x"])),
        prop::option::weighted(0.8, -1i64..2),
    )
        .prop_map(|(series, vt, rt, pt, deleted, a, d, x)| Row {
            series,
            vt,
            rt,
            pt,
            deleted,
            a: a.filter(|_| !deleted && series < 2),
            d: d.filter(|_| !deleted && series < 2),
            x: x.filter(|_| !deleted && series == 2),
        })
}

fn write_row(r: &Row) -> WriteRow {
    let mut w = WriteRow::new(SERIES_NAMES[r.series], h(r.vt), h(r.rt));
    w.published_time = r.pt.map(|p| h(p).into());
    w.deleted = r.deleted;
    if let Some(a) = r.a {
        w.values.insert("a".into(), Value::F64(a).into());
    }
    if let Some(d) = r.d {
        w.values.insert("d".into(), Value::String(d.into()).into());
    }
    if let Some(x) = r.x {
        w.values.insert("x".into(), Value::I64(x).into());
    }
    w
}

fn bad_row(bad: Bad) -> WriteRow {
    let mut w = WriteRow::new("s1", h(1), h(1));
    match bad {
        Bad::UnknownSeries => w.series = "nope".into(),
        Bad::UnknownColumn => {
            w.values.insert("zzz".into(), Value::F64(1.0).into());
        }
        Bad::TypeMismatch => {
            w.values
                .insert("a".into(), Value::String("1.5".into()).into());
        }
        Bad::InvalidTime => w.received_time = TimeInput::Text("2025-02-30T00:00:00Z".into()),
        Bad::TombstoneWithValue => {
            w.deleted = true;
            w.values.insert("a".into(), Value::F64(1.0).into());
        }
    }
    w
}

/// How a batch's rows are given.
#[derive(Clone, Copy, Debug)]
enum Form {
    /// [`WriteRow`]s.
    Rows,
    /// [`WriteColumns`] (after `split` leading rows).
    Columns,
    /// A [`ColumnSource`] converting range by range (the Python binding's Arrow input),
    /// after `split` leading rows.
    Source,
    /// Leading rows, then columns, then a source.
    Mixed,
}

const FORMS: [Form; 4] = [Form::Rows, Form::Columns, Form::Source, Form::Mixed];

fn to_columns(rows: &[WriteRow]) -> WriteColumns {
    let mut names: Vec<String> = Vec::new();
    let codes = rows
        .iter()
        .map(|r| match names.iter().position(|s| *s == r.series) {
            Some(k) => k as u32,
            None => {
                names.push(r.series.clone());
                (names.len() - 1) as u32
            }
        })
        .collect();
    let time = |f: &dyn Fn(&WriteRow) -> Option<TimeInput>| {
        TimeColumn::Inputs(rows.iter().map(f).collect())
    };
    let mut keys: Vec<String> = rows.iter().flat_map(|r| r.values.keys().cloned()).collect();
    keys.sort();
    keys.dedup();
    let values = keys
        .into_iter()
        .map(|k| {
            let col = ValueColumn::Inputs(
                rows.iter()
                    .map(|r| r.values.get(&k).cloned().unwrap_or(InputValue::Null))
                    .collect(),
            );
            (k, col)
        })
        .collect();
    WriteColumns {
        len: rows.len(),
        series_names: names,
        series_codes: codes,
        value_time: Some(time(&|r| Some(r.value_time.clone()))),
        received_time: Some(time(&|r| Some(r.received_time.clone()))),
        published_time: Some(time(&|r| r.published_time.clone())),
        deleted: Some(rows.iter().map(|r| r.deleted).collect()),
        values,
    }
}

/// Rows converted to columns range by range.
struct RowsSource(Vec<WriteRow>);

impl ColumnSource for RowsSource {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn columns(&self, range: std::ops::Range<usize>) -> muninn_core::Result<WriteColumns> {
        Ok(to_columns(&self.0[range]))
    }
}

/// The rows as a batch of the given form; `split` leading rows stay [`WriteRow`]s (a
/// batch is its rows, then its columns, then its source rows).
fn batch_of_rows(rows: Vec<WriteRow>, form: Form, split: usize) -> WriteBatch {
    let split = split.min(rows.len());
    let (lead, rest) = rows.split_at(split);
    let (cols, src) = match form {
        Form::Rows => return WriteBatch::new(h(100), rows),
        Form::Columns => (rest, &[][..]),
        Form::Source => (&[][..], rest),
        Form::Mixed => rest.split_at(rest.len() / 2),
    };
    WriteBatch {
        rows: lead.to_vec(),
        columns: (!cols.is_empty()).then(|| Arc::new(to_columns(cols))),
        source: (!src.is_empty()).then(|| SourceRef(Arc::new(RowsSource(src.to_vec())))),
        ..WriteBatch::new(h(100), Vec::new())
    }
}

/// A generated write: rows (optionally a resend of earlier ones first), an optional
/// invalid row, a batch id from a small pool, the input form and the writer clock.
#[derive(Clone, Debug)]
struct GenWrite {
    rows: Vec<Row>,
    resend: Option<prop::sample::Index>,
    bad: Option<(prop::sample::Index, Bad)>,
    batch_id: Option<u8>,
    form: Form,
    split: usize,
    clock: i64,
}

fn arb_write() -> impl Strategy<Value = GenWrite> {
    (
        prop::collection::vec(arb_row(), 0..160),
        prop::option::weighted(0.3, any::<prop::sample::Index>()),
        prop::option::weighted(
            0.25,
            (
                any::<prop::sample::Index>(),
                prop::sample::select(vec![
                    Bad::UnknownSeries,
                    Bad::UnknownColumn,
                    Bad::TypeMismatch,
                    Bad::InvalidTime,
                    Bad::TombstoneWithValue,
                ]),
            ),
        ),
        prop::option::weighted(0.3, 0u8..3),
        prop::sample::select(FORMS.to_vec()),
        0usize..4,
        prop_oneof![4 => 90i64..110, 1 => Just(-1i64)],
    )
        .prop_map(
            |(rows, resend, bad, batch_id, form, split, clock)| GenWrite {
                rows,
                resend,
                bad,
                batch_id,
                form,
                split,
                clock,
            },
        )
}

/// The batch of a generated write; `earlier` holds the rows of earlier writes.
fn build(w: &GenWrite, earlier: &[Vec<WriteRow>]) -> WriteBatch {
    let mut rows: Vec<WriteRow> = Vec::new();
    if let (Some(ix), false) = (w.resend, earlier.is_empty()) {
        rows.extend(earlier[ix.index(earlier.len())].iter().cloned());
    }
    rows.extend(w.rows.iter().map(write_row));
    if let Some((ix, bad)) = w.bad {
        rows.insert(ix.index(rows.len() + 1), bad_row(bad));
    }
    let mut b = batch_of_rows(rows, w.form, w.split);
    b.batch_id = w.batch_id.map(|k| format!("b{k}"));
    b.clock = if w.clock < 0 {
        TimeInput::Text("not a time".into())
    } else {
        h(w.clock).into()
    };
    b
}

type Outcome = Result<WriteResult, (ErrorKind, String)>;

fn outcome(r: muninn_core::Result<WriteResult>) -> Outcome {
    r.map_err(|e| (e.kind(), e.message().to_owned()))
}

type Mem = Engine<Arc<MemoryStore>>;

/// Runs `writes` (single writes, or `write_many` groups of consecutive writes when
/// `group > 1`) on `db`; returns each outcome and the fingerprint after each step.
fn run(db: &Mem, writes: &[GenWrite], group: usize) -> (Vec<Outcome>, Vec<String>) {
    setup(db);
    let mut earlier: Vec<Vec<WriteRow>> = Vec::new();
    let (mut outs, mut fps) = (Vec::new(), Vec::new());
    for ws in writes.chunks(group.max(1)) {
        let batches: Vec<WriteBatch> = ws.iter().map(|w| build(w, &earlier)).collect();
        let before = db.version();
        let results: Vec<Outcome> = if group > 1 {
            db.write_many(&batches).into_iter().map(outcome).collect()
        } else {
            batches.iter().map(|b| outcome(db.write(b))).collect()
        };
        if results.iter().all(|r| r.is_err()) {
            // Nothing committed (no log entry), whatever the chunks did before failing.
            assert_eq!(db.version(), before, "a rejected write committed something");
        }
        for (b, r) in batches.into_iter().zip(&results) {
            if r.is_ok() {
                earlier.push(b.all_rows());
            }
        }
        outs.extend(results);
        fps.push(fingerprint(db));
    }
    (outs, fps)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(96), ..ProptestConfig::default() })]

    /// Chunked processing (tiny chunks, one or several threads, inline or file layout)
    /// gives exactly the outcomes, error messages and stored versions of unchunked
    /// processing, write after write.
    #[test]
    fn chunked_writes_equal_unchunked(
        writes in prop::collection::vec(arb_write(), 1..6),
        chunk in prop::sample::select(vec![1usize, 2, 7, 64]),
        threads in prop::sample::select(vec![1usize, 3]),
        inline_max in prop::sample::select(vec![0usize, 10, 20_000]),
        group in prop::sample::select(vec![1usize, 1, 3]),
    ) {
        let whole = Engine::open(Arc::new(MemoryStore::new()), opts(0, 1, inline_max)).unwrap();
        let chunked =
            Engine::open(Arc::new(MemoryStore::new()), opts(chunk, threads, inline_max)).unwrap();
        let (want, want_fp) = run(&whole, &writes, group);
        let (got, got_fp) = run(&chunked, &writes, group);
        prop_assert_eq!(got, want);
        prop_assert_eq!(got_fp, want_fp);
        // And the result survives reopening.
        let reopened = Engine::open(chunked.store().clone(), opts(chunk, threads, inline_max)).unwrap();
        prop_assert_eq!(fingerprint(&reopened), fingerprint(&whole));
    }

    /// The general differential workload (catalog changes, writes, resends, group
    /// commits, reads, flushes, compaction, GC, reopen) against the model, with every
    /// batch of more than `chunk` rows processed in chunks.
    #[test]
    fn chunked_engine_matches_model(
        ops in arb_ops(ops_len(30)),
        chunk in 1usize..4,
        threads in prop::sample::select(vec![1usize, 3]),
        inline_max in prop::sample::select(vec![0usize, 3, 8]),
    ) {
        run_differential(Arc::new(MemoryStore::new()), opts(chunk, threads, inline_max), &ops)?;
    }
}

/// `n` distinct fresh rows of `s1` (one point each), received at hour `rt`.
fn fresh(n: usize, rt: i64) -> Vec<WriteRow> {
    (0..n)
        .map(|i| {
            let mut w = WriteRow::new("s1", h(i as i64 % 48), h(rt));
            w.values
                .insert("a".into(), Value::F64((i / 48) as f64).into());
            w
        })
        .collect()
}

fn pair(chunk: usize, threads: usize, inline_max: usize) -> (Mem, Mem, ModelDb) {
    let whole = Engine::open(Arc::new(MemoryStore::new()), opts(0, 1, inline_max)).unwrap();
    let chunked = Engine::open(
        Arc::new(MemoryStore::new()),
        opts(chunk, threads, inline_max),
    )
    .unwrap();
    let model = ModelDb::new();
    setup(&whole);
    setup(&chunked);
    setup(&model);
    (whole, chunked, model)
}

/// Duplicates exactly at chunk boundaries (the last row of a chunk repeated as the first
/// of the next), far apart (first chunk vs last), and of rows that were themselves
/// dropped as duplicates of stored versions.
#[test]
fn duplicates_straddling_chunk_boundaries() {
    for chunk in [1usize, 7, 64] {
        for threads in [1usize, 4] {
            for inline_max in [0usize, 20_000] {
                let (whole, chunked, model) = pair(chunk, threads, inline_max);
                let stored = fresh(30, 0);
                let first = WriteBatch::new(h(1), stored.clone());
                whole.write(&first).unwrap();
                chunked.write(&first).unwrap();
                model.write(&first).unwrap();
                let mut rows = fresh(5 * chunk + 3, 1);
                for b in 1..=4 {
                    let at = b * chunk;
                    rows.insert(at, rows[at - 1].clone()); // straddles boundary b
                }
                let first = rows[0].clone();
                rows.push(first); // the very first row again, at the end
                rows.insert(chunk / 2, stored[3].clone()); // a stored version
                rows.push(stored[3].clone()); // ... twice
                for form in FORMS {
                    let b = batch_of_rows(rows.clone(), form, 3);
                    let want = model.write(&b).unwrap();
                    assert_eq!(outcome(whole.write(&b)), Ok(want.clone()));
                    assert_eq!(outcome(chunked.write(&b)), Ok(want.clone()));
                    assert_eq!(fingerprint(&chunked), fingerprint(&model));
                }
                assert_eq!(fingerprint(&whole), fingerprint(&model));
            }
        }
    }
}

/// An invalid row in the last chunk rejects the whole write with the error unchunked
/// processing gives (same code and message, row index in the batch), commits nothing
/// (log version, seqs, rows, batch ids), and leaves the batch id free for a retry.
#[test]
fn invalid_row_in_a_late_chunk_rejects_the_write() {
    for chunk in [1usize, 7, 64] {
        for threads in [1usize, 4] {
            for inline_max in [0usize, 20_000] {
                for bad in [
                    Bad::UnknownSeries,
                    Bad::UnknownColumn,
                    Bad::TypeMismatch,
                    Bad::InvalidTime,
                    Bad::TombstoneWithValue,
                ] {
                    let (whole, chunked, model) = pair(chunk, threads, inline_max);
                    let mut rows = fresh(10 * chunk + 5, 2);
                    let at = rows.len() - 2;
                    rows.insert(at, bad_row(bad));
                    for form in FORMS {
                        let mut b = batch_of_rows(rows.clone(), form, 1);
                        b.batch_id = Some("retry-me".into());
                        let before = (chunked.version(), fingerprint(&chunked));
                        let want = outcome(whole.write(&b));
                        let got = outcome(chunked.write(&b));
                        assert!(want.is_err(), "{bad:?}");
                        assert_eq!(got, want, "{bad:?} chunk {chunk}");
                        assert!(got.unwrap_err().1.starts_with(&format!("row {at}:")));
                        assert_eq!((chunked.version(), fingerprint(&chunked)), before);
                        assert_eq!(chunked.live_segment_keys().len(), 0);
                    }
                    // The id was not recorded: the corrected batch goes through.
                    rows.remove(at);
                    let mut b = batch_of_rows(rows, Form::Source, 0);
                    b.batch_id = Some("retry-me".into());
                    let want = model.write(&b).unwrap();
                    assert!(!want.batch_already_committed && want.stored > 0);
                    assert_eq!(chunked.write(&b).unwrap(), want);
                    assert_eq!(chunked.write(&b).unwrap(), model.write(&b).unwrap());
                    assert_eq!(fingerprint(&chunked), fingerprint(&model));
                }
            }
        }
    }
}

/// Once the last stored batch is at the maximum timestamp, a write fails with the
/// `system_time` error — unless it has an invalid row (anywhere), whose error comes
/// first, as when the whole batch is validated before anything else.
#[test]
fn validation_errors_come_before_system_time_exhaustion() {
    for chunk in [1usize, 7] {
        let (whole, chunked, _) = pair(chunk, 2, 0);
        let top = WriteBatch::new(Timestamp::MAX, fresh(1, 0));
        whole.write(&top).unwrap();
        chunked.write(&top).unwrap();
        let mut rows = fresh(5 * chunk, 1);
        let b = WriteBatch::new(h(1), rows.clone());
        let want = outcome(whole.write(&b));
        assert_eq!(want.as_ref().unwrap_err().0, ErrorKind::InvalidTimestamp);
        assert_eq!(outcome(chunked.write(&b)), want);
        rows.push(bad_row(Bad::TypeMismatch));
        let b = WriteBatch::new(h(1), rows);
        let want = outcome(whole.write(&b));
        assert_eq!(want.as_ref().unwrap_err().0, ErrorKind::TypeMismatch);
        assert_eq!(outcome(chunked.write(&b)), want);
    }
}

/// One write, one commit: a chunked write is one log entry with one `system_time` and a
/// contiguous seq range in input order, and a batch that keeps few rows stays inline.
#[test]
fn a_chunked_write_is_one_commit() {
    let (_, chunked, model) = pair(7, 3, 0);
    let rows = fresh(500, 0);
    let before = chunked.version();
    let r = chunked.write(&WriteBatch::new(h(5), rows.clone())).unwrap();
    assert_eq!(chunked.version(), before + 1);
    assert_eq!(r, model.write(&WriteBatch::new(h(5), rows)).unwrap());
    assert_eq!(
        (r.stored, r.first_seq, r.system_time),
        (500, Some(1), Some(h(5)))
    );
    let hist = chunked
        .history(&LatestRequest {
            table: "t".into(),
            ..LatestRequest::default()
        })
        .unwrap();
    let mut seqs: Vec<u64> = hist.rows.iter().map(|v| v.seq).collect();
    seqs.sort_unstable();
    assert_eq!(seqs, (1..=500).collect::<Vec<_>>());
    assert!(hist.rows.iter().all(|v| v.system_time == h(5)));
    // 500 rows (two day partitions) in chunks of 7: files of about 7 rows of a partition
    // (a partition's rows of consecutive chunks are written together).
    assert_eq!(chunked.memtable().rows, 0);
    let files = chunked.live_segment_keys().len();
    assert!((500 / 14..=500 / 7 + 2).contains(&files), "{files} files");

    // A batch that keeps at most `inline_max_rows` rows (here: all but 3 are re-sends)
    // goes inline, as unchunked.
    let (_, chunked, _) = pair(7, 3, 8);
    chunked.write(&WriteBatch::new(h(1), fresh(40, 0))).unwrap();
    let files = chunked.live_segment_keys().len();
    let mut rows = fresh(40, 0);
    rows.extend(fresh(3, 9));
    let r = chunked.write(&WriteBatch::new(h(2), rows)).unwrap();
    assert_eq!((r.stored, r.duplicates), (3, 40));
    assert_eq!(chunked.live_segment_keys().len(), files);
    assert_eq!(chunked.memtable().rows, 3);
}

/// The same input gives byte-identical segment files whatever the number of threads.
#[test]
fn segments_do_not_depend_on_the_thread_count() {
    fn segment_bytes(threads: usize) -> Vec<Vec<u8>> {
        let e = Engine::open(Arc::new(MemoryStore::new()), opts(64, threads, 0)).unwrap();
        setup(&e);
        let mut rows: Vec<WriteRow> = Vec::new();
        for run in 0..40i64 {
            for vt in 0..72 {
                let mut w = WriteRow::new(["s1", "s2"][(run % 2) as usize], h(run + vt), h(run));
                w.values
                    .insert("a".into(), Value::F64((run * vt) as f64).into());
                rows.push(w);
                if vt % 9 == 0 {
                    let mut w = WriteRow::new("s3", h(run * 24 + vt), h(run));
                    w.values.insert("x".into(), Value::I64(vt).into());
                    rows.push(w);
                }
            }
        }
        rows.extend(rows[..200].to_vec()); // duplicates
        e.write(&batch_of_rows(rows, Form::Source, 0)).unwrap();
        let mut out: Vec<Vec<u8>> = e
            .live_segment_keys()
            .iter()
            .map(|k| e.store().get(k).unwrap())
            .collect();
        out.sort();
        out
    }
    let one = segment_bytes(1);
    assert!(one.len() > 40, "{} segments", one.len());
    for threads in [2, 4, 8] {
        assert!(segment_bytes(threads) == one, "{threads} threads");
    }
}
