//! `dump` and `restore` (format: `spec/DUMP.md`).

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use muninn_core::api::{AddColumn, CreateSeries, CreateTable, LatestRequest};
use muninn_core::catalog::{CatalogChange, CatalogChangeKind};
use muninn_core::types::{SeriesDef, TableDef, Version};
use muninn_core::wire::{Wire, encode_value};
use muninn_core::{
    Clock, ColumnDef, ColumnType, Database, Duration, Partitioning, Reader, Timestamp, Value,
};
use muninn_engine::{Engine, EngineOptions};

use crate::db::{self, Context};
use crate::output;
use crate::{CliError, Res};

/// `format` field of the manifest.
pub const DUMP_FORMAT: &str = "muninn-dump";
/// Dump format version.
pub const DUMP_VERSION: u64 = 1;

const CANONICAL_NAN: u64 = 0x7FF8_0000_0000_0000;

fn err(m: impl Into<String>) -> CliError {
    CliError::new(m)
}

/// A writer that keeps a CRC-32C and a line count of everything written.
struct Crc<W> {
    inner: W,
    crc: u32,
}

impl<W: Write> Write for Crc<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.crc = crc32c::crc32c_append(self.crc, &buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn create(path: &Path) -> Res<Crc<BufWriter<File>>> {
    let f = File::create(path)
        .map_err(|e| err(format!("io: cannot create {}: {e}", path.display())))?;
    Ok(Crc {
        inner: BufWriter::new(f),
        crc: 0,
    })
}

fn finish(mut w: Crc<BufWriter<File>>) -> Res<String> {
    w.flush()?;
    w.inner
        .into_inner()
        .map_err(|e| err(format!("io: {e}")))?
        .sync_all()?;
    Ok(format!("{:08x}", w.crc))
}

fn file_crc(path: &Path) -> Res<String> {
    let mut f =
        File::open(path).map_err(|e| err(format!("io: cannot open {}: {e}", path.display())))?;
    let mut crc = 0u32;
    let mut buf = vec![0u8; 1 << 16];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        crc = crc32c::crc32c_append(crc, &buf[..n]);
    }
    Ok(format!("{crc:08x}"))
}

fn ensure_empty_dir(dir: &Path) -> Res<()> {
    if dir.exists() && (!dir.is_dir() || fs::read_dir(dir)?.next().is_some()) {
        return Err(err(format!(
            "{} already exists and is not an empty directory",
            dir.display()
        )));
    }
    Ok(())
}

fn ws(s: impl Into<String>) -> Wire {
    Wire::String(s.into())
}

fn wn(n: impl ToString) -> Wire {
    Wire::Number(n.to_string())
}

fn obj(fields: Vec<(&str, Wire)>) -> Wire {
    Wire::Object(fields.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
}

/// A value in dump form: SPEC §10.2 JSON, except that a NaN other than the canonical
/// quiet NaN is written as `"nan:0x<16 hex digits>"` (its exact bits).
fn dump_value(v: &Value) -> Wire {
    match v {
        Value::F64(x) if x.is_nan() && x.to_bits() != CANONICAL_NAN => {
            ws(format!("nan:0x{:016x}", x.to_bits()))
        }
        other => encode_value(Some(other)),
    }
}

fn change_json(c: &CatalogChange) -> Wire {
    let t = ws(c.system_time.to_string());
    let columns = |cols: &[ColumnDef]| {
        Wire::Array(
            cols.iter()
                .map(|c| obj(vec![("name", ws(&c.name)), ("type", ws(c.ty.name()))]))
                .collect(),
        )
    };
    match &c.change {
        CatalogChangeKind::CreateTable(def) => obj(vec![
            ("op", ws("create_table")),
            ("system_time", t),
            ("name", ws(&def.name)),
            ("columns", columns(&def.columns)),
            ("default_clock", ws(def.default_clock.name())),
            ("partition", ws(def.partition.name())),
        ]),
        CatalogChangeKind::AddColumn { table, column } => obj(vec![
            ("op", ws("add_column")),
            ("system_time", t),
            ("table", ws(table)),
            ("name", ws(&column.name)),
            ("type", ws(column.ty.name())),
        ]),
        CatalogChangeKind::CreateSeries(s) => obj(vec![
            ("op", ws("create_series")),
            ("system_time", t),
            ("id", wn(s.id.0)),
            ("name", ws(&*s.name)),
            ("table", ws(&s.table)),
            (
                "tags",
                Wire::Object(s.tags.iter().map(|(k, v)| (k.clone(), ws(v))).collect()),
            ),
            ("timezone", ws(&s.timezone)),
            (
                "resolution",
                s.resolution.map_or(Wire::Null, |r| ws(r.to_string())),
            ),
        ]),
    }
}

/// `dump`.
pub fn dump(ctx: &Context, out: &Path) -> Res<()> {
    let db = ctx.open()?;
    // Everything comes from one pinned state, so the dump is consistent.
    let snap = db.snapshot()?;
    ensure_empty_dir(out)?;
    fs::create_dir_all(out.join("tables"))?;

    let log = snap.catalog_log();
    let mut w = create(&out.join("catalog.json"))?;
    writeln!(w, "{{\"changes\": [")?;
    for (i, c) in log.iter().enumerate() {
        let sep = if i + 1 < log.len() { "," } else { "" };
        writeln!(w, "  {}{sep}", change_json(c))?;
    }
    writeln!(w, "]}}")?;
    let catalog_crc = finish(w)?;

    let ids = snap.batch_ids();
    let mut w = create(&out.join("batch_ids.ndjson"))?;
    for id in &ids {
        writeln!(w, "{}", ws(id))?;
    }
    let ids_crc = finish(w)?;

    let mut tables = Vec::new();
    let mut total = 0u64;
    for t in snap.tables()? {
        let res = snap.history(&LatestRequest {
            table: t.name.clone(),
            ..LatestRequest::default()
        })?;
        let mut rows = res.rows;
        rows.sort_unstable_by_key(|r| r.seq);
        let rel = format!("tables/{}.ndjson", t.name);
        let mut w = create(&out.join(&rel))?;
        for r in &rows {
            let values = Wire::Object(
                res.columns
                    .iter()
                    .zip(&r.values)
                    .filter_map(|(c, v)| v.as_ref().map(|v| (c.name.clone(), dump_value(v))))
                    .collect(),
            );
            let line = obj(vec![
                ("seq", wn(r.seq)),
                ("system_time", ws(r.system_time.to_string())),
                ("series", ws(&*r.series)),
                ("value_time", ws(r.value_time.to_string())),
                (
                    "published_time",
                    r.published_time.map_or(Wire::Null, |p| ws(p.to_string())),
                ),
                ("received_time", ws(r.received_time.to_string())),
                ("deleted", Wire::Bool(r.deleted)),
                ("values", values),
            ]);
            writeln!(w, "{line}")?;
        }
        total += rows.len() as u64;
        tables.push(obj(vec![
            ("name", ws(&t.name)),
            ("file", ws(rel)),
            ("versions", wn(rows.len())),
            ("crc32c", ws(finish(w)?)),
        ]));
    }

    let manifest = obj(vec![
        ("format", ws(DUMP_FORMAT)),
        ("version", wn(DUMP_VERSION)),
        ("muninn", ws(env!("CARGO_PKG_VERSION"))),
        ("dumped_at", ws(ctx.now().to_string())),
        ("log_version", wn(snap.version())),
        ("next_seq", wn(snap.next_seq())),
        (
            "last_system_time",
            snap.last_system_time()
                .map_or(Wire::Null, |t| ws(t.to_string())),
        ),
        ("versions", wn(total)),
        (
            "catalog",
            obj(vec![
                ("file", ws("catalog.json")),
                ("changes", wn(log.len())),
                ("crc32c", ws(catalog_crc)),
            ]),
        ),
        (
            "batch_ids",
            obj(vec![
                ("file", ws("batch_ids.ndjson")),
                ("count", wn(ids.len())),
                ("crc32c", ws(ids_crc)),
            ]),
        ),
        ("tables", Wire::Array(tables)),
    ]);
    // The manifest is written last: its presence marks a complete dump.
    let mut w = create(&out.join("manifest.json"))?;
    writeln!(w, "{manifest}")?;
    finish(w)?;
    output::line(&format!(
        "dumped {} table(s), {} series, {total} version(s), {} batch id(s) to {}",
        snap.tables()?.len(),
        log.iter()
            .filter(|c| matches!(c.change, CatalogChangeKind::CreateSeries(_)))
            .count(),
        ids.len(),
        out.display()
    ))
}

// ---------------------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------------------

fn get<'a>(w: &'a Wire, key: &str, what: &str) -> Res<&'a Wire> {
    w.get(key)
        .ok_or_else(|| err(format!("{what}: missing field {key:?}")))
}

fn get_str<'a>(w: &'a Wire, key: &str, what: &str) -> Res<&'a str> {
    get(w, key, what)?
        .as_str()
        .ok_or_else(|| err(format!("{what}: {key:?} must be a string")))
}

fn get_u64(w: &Wire, key: &str, what: &str) -> Res<u64> {
    get(w, key, what)?
        .as_number()
        .and_then(|n| n.parse().ok())
        .ok_or_else(|| err(format!("{what}: {key:?} must be a non-negative integer")))
}

fn get_ts(w: &Wire, key: &str, what: &str) -> Res<Timestamp> {
    let s = get_str(w, key, what)?;
    Timestamp::parse(s).map_err(|e| err(format!("{what}: {key}: {e}")))
}

fn opt_ts(w: &Wire, key: &str, what: &str) -> Res<Option<Timestamp>> {
    match w.get(key) {
        None | Some(Wire::Null) => Ok(None),
        Some(_) => get_ts(w, key, what).map(Some),
    }
}

fn read_json(path: &Path) -> Res<Wire> {
    let text = fs::read_to_string(path)
        .map_err(|e| err(format!("io: cannot read {}: {e}", path.display())))?;
    Wire::parse(&text).map_err(|e| err(format!("{}: invalid JSON: {e}", path.display())))
}

/// A data file of the dump, verified against the manifest's checksum.
fn data_file(dir: &Path, entry: &Wire, what: &str) -> Res<PathBuf> {
    let rel = get_str(entry, "file", what)?;
    if rel.contains("..") || rel.starts_with('/') {
        return Err(err(format!("{what}: bad file name {rel:?}")));
    }
    let path = dir.join(rel);
    let want = get_str(entry, "crc32c", what)?;
    let got = file_crc(&path)?;
    if got != want {
        return Err(err(format!(
            "{}: checksum {got} does not match the manifest ({want}); the dump is damaged",
            path.display()
        )));
    }
    Ok(path)
}

fn columns(w: &Wire, what: &str) -> Res<Vec<ColumnDef>> {
    get(w, "columns", what)?
        .as_array()
        .ok_or_else(|| err(format!("{what}: columns must be a list")))?
        .iter()
        .map(|c| {
            let ty = get_str(c, "type", what)?;
            Ok(ColumnDef::new(
                get_str(c, "name", what)?,
                ColumnType::parse(ty).ok_or_else(|| err(format!("{what}: type {ty:?}")))?,
            ))
        })
        .collect()
}

/// Replays the catalog change log with the original change times.
fn restore_catalog<D: Database>(db: &D, catalog: &Wire) -> Res<usize> {
    let changes = get(catalog, "changes", "catalog.json")?
        .as_array()
        .ok_or_else(|| err("catalog.json: changes must be a list"))?;
    for (i, c) in changes.iter().enumerate() {
        let what = format!("catalog.json change {i}");
        let what = what.as_str();
        let clock = Some(get_ts(c, "system_time", what)?);
        match get_str(c, "op", what)? {
            "create_table" => {
                let dc = get_str(c, "default_clock", what)?;
                let p = get_str(c, "partition", what)?;
                db.create_table(&CreateTable {
                    name: get_str(c, "name", what)?.to_owned(),
                    columns: columns(c, what)?,
                    default_clock: Clock::parse(dc)
                        .ok_or_else(|| err(format!("{what}: clock {dc:?}")))?,
                    partition: Partitioning::parse(p)
                        .ok_or_else(|| err(format!("{what}: partition {p:?}")))?,
                    clock,
                })?;
            }
            "add_column" => {
                let ty = get_str(c, "type", what)?;
                db.add_column(&AddColumn {
                    table: get_str(c, "table", what)?.to_owned(),
                    column: ColumnDef::new(
                        get_str(c, "name", what)?,
                        ColumnType::parse(ty).ok_or_else(|| err(format!("{what}: type {ty:?}")))?,
                    ),
                    clock,
                })?;
            }
            "create_series" => {
                let tags = match c.get("tags") {
                    None | Some(Wire::Null) => Default::default(),
                    Some(Wire::Object(o)) => o
                        .iter()
                        .map(|(k, v)| {
                            v.as_str()
                                .map(|v| (k.clone(), v.to_owned()))
                                .ok_or_else(|| err(format!("{what}: tag values are strings")))
                        })
                        .collect::<Res<_>>()?,
                    Some(_) => return Err(err(format!("{what}: tags must be an object"))),
                };
                let resolution = match c.get("resolution") {
                    None | Some(Wire::Null) => None,
                    Some(Wire::String(r)) => Some(
                        Duration::parse(r).map_err(|e| err(format!("{what}: resolution: {e}")))?,
                    ),
                    Some(_) => return Err(err(format!("{what}: bad resolution"))),
                };
                let s = db.create_series(&CreateSeries {
                    name: get_str(c, "name", what)?.to_owned(),
                    table: get_str(c, "table", what)?.to_owned(),
                    tags,
                    timezone: Some(get_str(c, "timezone", what)?.to_owned()),
                    resolution,
                    clock,
                })?;
                let want = get_u64(c, "id", what)?;
                if u64::from(s.id.0) != want {
                    return Err(err(format!(
                        "{what}: series {:?} got id {} but the dump says {want}",
                        s.name, s.id.0
                    )));
                }
            }
            other => return Err(err(format!("{what}: unknown op {other:?}"))),
        }
    }
    Ok(changes.len())
}

fn restore_value(v: &Wire, ty: ColumnType) -> Option<Value> {
    Some(match (ty, v) {
        (ColumnType::F64, Wire::Number(n)) => {
            if n == "-0" {
                Value::F64(-0.0)
            } else {
                Value::F64(n.parse().ok()?)
            }
        }
        (ColumnType::F64, Wire::String(s)) => match s.as_str() {
            "NaN" => Value::F64(f64::from_bits(CANONICAL_NAN)),
            "inf" => Value::F64(f64::INFINITY),
            "-inf" => Value::F64(f64::NEG_INFINITY),
            s => {
                let hex = s.strip_prefix("nan:0x")?;
                let bits = u64::from_str_radix(hex, 16).ok()?;
                let x = f64::from_bits(bits);
                if hex.len() != 16 || !x.is_nan() {
                    return None;
                }
                Value::F64(x)
            }
        },
        (ColumnType::I64, Wire::Number(n) | Wire::String(n)) => Value::I64(n.parse().ok()?),
        (ColumnType::Bool, Wire::Bool(b)) => Value::Bool(*b),
        (ColumnType::String, Wire::String(s)) => Value::String(s.clone()),
        (ColumnType::Timestamp, Wire::String(s)) => Value::Timestamp(Timestamp::parse(s).ok()?),
        _ => return None,
    })
}

/// Reads one table file's versions in order.
struct TableReader {
    name: String,
    lines: std::io::Lines<BufReader<File>>,
    line: u64,
    table: TableDef,
    series: Arc<HashMap<String, SeriesDef>>,
    last_seq: u64,
}

impl TableReader {
    fn next_version(&mut self) -> Res<Option<Version>> {
        let text = loop {
            match self.lines.next() {
                None => return Ok(None),
                Some(l) => {
                    self.line += 1;
                    let l = l?;
                    if !l.trim().is_empty() {
                        break l;
                    }
                }
            }
        };
        let what = format!("{} line {}", self.name, self.line);
        let what = what.as_str();
        let w = Wire::parse(&text).map_err(|e| err(format!("{what}: invalid JSON: {e}")))?;
        let series = get_str(&w, "series", what)?;
        let s = self
            .series
            .get(series)
            .filter(|s| s.table == self.table.name)
            .ok_or_else(|| err(format!("{what}: series {series:?} is not in this table")))?;
        let seq = get_u64(&w, "seq", what)?;
        if seq <= self.last_seq {
            return Err(err(format!("{what}: seq {seq} is not increasing")));
        }
        self.last_seq = seq;
        let mut values: Vec<Option<Value>> = vec![None; self.table.columns.len()];
        match w.get("values") {
            None | Some(Wire::Null) => {}
            Some(Wire::Object(o)) => {
                for (k, v) in o {
                    let i = self
                        .table
                        .column_index(k)
                        .ok_or_else(|| err(format!("{what}: unknown column {k:?}")))?;
                    if v.is_null() {
                        continue;
                    }
                    values[i] =
                        Some(restore_value(v, self.table.columns[i].ty).ok_or_else(|| {
                            err(format!(
                                "{what}: {v} is not a {} value",
                                self.table.columns[i].ty
                            ))
                        })?);
                }
            }
            Some(_) => return Err(err(format!("{what}: values must be an object"))),
        }
        let deleted = match w.get("deleted") {
            None | Some(Wire::Null) => false,
            Some(Wire::Bool(b)) => *b,
            Some(_) => return Err(err(format!("{what}: deleted must be a bool"))),
        };
        Ok(Some(Version {
            series: s.id,
            value_time: get_ts(&w, "value_time", what)?,
            published_time: opt_ts(&w, "published_time", what)?,
            received_time: get_ts(&w, "received_time", what)?,
            deleted,
            values,
            system_time: get_ts(&w, "system_time", what)?,
            seq,
        }))
    }
}

/// `restore`.
pub fn restore(
    _ctx: &Context,
    dump_dir: &Path,
    new_db: &Path,
    chunk_rows: usize,
    compact: bool,
) -> Res<()> {
    let chunk_rows = chunk_rows.max(1);
    let manifest = read_json(&dump_dir.join("manifest.json")).map_err(|e| {
        err(format!(
            "{} (is {} a complete dump?)",
            e.message,
            dump_dir.display()
        ))
    })?;
    let what = "manifest.json";
    if get_str(&manifest, "format", what)? != DUMP_FORMAT {
        return Err(err("manifest.json: not a muninn dump"));
    }
    let version = get_u64(&manifest, "version", what)?;
    if version != DUMP_VERSION {
        return Err(err(format!(
            "dump format version {version} is not supported (this muninn reads {DUMP_VERSION})"
        )));
    }
    // Verify every checksum before creating anything.
    let catalog_path = data_file(dump_dir, get(&manifest, "catalog", what)?, "catalog")?;
    let ids_path = data_file(dump_dir, get(&manifest, "batch_ids", what)?, "batch_ids")?;
    let tables = get(&manifest, "tables", what)?
        .as_array()
        .ok_or_else(|| err("manifest.json: tables must be a list"))?;
    let mut table_files = Vec::new();
    for t in tables {
        let name = get_str(t, "name", what)?.to_owned();
        let path = data_file(dump_dir, t, &format!("table {name}"))?;
        table_files.push((name, path, get_u64(t, "versions", what)?));
    }
    let catalog = read_json(&catalog_path)?;
    let mut ids = Vec::new();
    for (i, l) in BufReader::new(File::open(&ids_path)?).lines().enumerate() {
        let l = l?;
        if l.trim().is_empty() {
            continue;
        }
        match Wire::parse(&l) {
            Ok(Wire::String(s)) => ids.push(s),
            _ => {
                return Err(err(format!(
                    "batch_ids.ndjson line {}: not a JSON string",
                    i + 1
                )));
            }
        }
    }

    ensure_empty_dir(new_db)?;
    let store = db::init(new_db)?;
    let engine = Engine::open(store, EngineOptions::default())?;
    let changes = restore_catalog(&engine, &catalog)?;
    let snap = engine.snapshot()?;
    let series: Arc<HashMap<String, SeriesDef>> = Arc::new({
        let mut m = HashMap::new();
        for t in snap.tables()? {
            for s in snap.table_series(&t.name)? {
                m.insert(s.name.to_string(), s);
            }
        }
        m
    });
    let mut readers = Vec::new();
    for (name, path, _) in &table_files {
        let table = snap.table(name)?;
        readers.push(TableReader {
            name: path
                .strip_prefix(dump_dir)
                .unwrap_or(path)
                .display()
                .to_string(),
            lines: BufReader::new(File::open(path)?).lines(),
            line: 0,
            table,
            series: Arc::clone(&series),
            last_seq: 0,
        });
    }
    // Merge the tables by seq.
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> = BinaryHeap::new();
    let mut heads: Vec<Option<Version>> = Vec::new();
    for (i, r) in readers.iter_mut().enumerate() {
        let v = r.next_version()?;
        if let Some(v) = &v {
            heap.push(Reverse((v.seq, i)));
        }
        heads.push(v);
    }
    let mut ids_iter = ids.iter();
    let mut chunk: Vec<Version> = Vec::with_capacity(chunk_rows.min(1 << 20));
    let mut total = 0u64;
    let mut commits = 0u64;
    let flush = |chunk: &mut Vec<Version>, ids: &mut std::slice::Iter<'_, String>| -> Res<()> {
        engine
            .restore(chunk, ids.next().map(String::as_str))
            .map_err(|e| err(format!("restoring: {}: {}", e.kind(), e.message())))?;
        chunk.clear();
        Ok(())
    };
    while let Some(Reverse((_, i))) = heap.pop() {
        let v = heads[i].take().expect("heap entry has a head");
        // Cut chunks only between original batches (system_time changes): the engine
        // requires each restore to start a new batch (strictly increasing system_time).
        if chunk.len() >= chunk_rows && chunk.last().is_some_and(|l| l.system_time != v.system_time)
        {
            flush(&mut chunk, &mut ids_iter)?;
            commits += 1;
        }
        chunk.push(v);
        total += 1;
        if let Some(next) = readers[i].next_version()? {
            heap.push(Reverse((next.seq, i)));
            heads[i] = Some(next);
        }
    }
    if !chunk.is_empty() {
        flush(&mut chunk, &mut ids_iter)?;
        commits += 1;
    }
    for id in ids_iter {
        engine.restore(&[], Some(id))?;
    }
    // Cross-check against the manifest.
    let snap = engine.snapshot()?;
    let want_total = get_u64(&manifest, "versions", what)?;
    let want_next = get_u64(&manifest, "next_seq", what)?;
    let want_last = opt_ts(&manifest, "last_system_time", what)?;
    for (name, _, want) in &table_files {
        let got = engine
            .history(&LatestRequest {
                table: name.clone(),
                ..LatestRequest::default()
            })?
            .rows
            .len() as u64;
        if got != *want {
            return Err(err(format!(
                "table {name}: restored {got} versions, the manifest says {want}"
            )));
        }
    }
    if total != want_total || snap.last_system_time() != want_last {
        return Err(err(format!(
            "restored {total} versions (last system_time {:?}); the manifest says {want_total} ({want_last:?})",
            snap.last_system_time()
        )));
    }
    if snap.next_seq() != want_next {
        return Err(err(format!(
            "restored next seq {} but the dump says {want_next}",
            snap.next_seq()
        )));
    }
    if compact {
        engine.compact_all()?;
    }
    output::line(&format!(
        "restored {changes} catalog change(s), {total} version(s) in {commits} commit(s), {} batch id(s) into {}",
        ids.len(),
        new_db.display()
    ))
}
