//! `write`: CSV / NDJSON import with line-numbered errors and optional chunking.
//!
//! Rows are checked here first (series, columns, types, timestamps) so that errors point
//! at the input line; the engine then re-validates every batch in SPEC §4.1 order.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use muninn_core::api::{TimeInput, WriteBatch, WriteRow};
use muninn_core::types::TableDef;
use muninn_core::wire::Wire;
use muninn_core::{ColumnType, Database, InputValue, Reader, Timestamp, Value};

use crate::db::{Context, Db, parse_ts};
use crate::output;
use crate::timefmt::TimeFormat;
use crate::{CliError, InputFormat, Res, WriteArgs};

/// Opens a file argument (`None` or `-` = stdin).
pub fn open_input(file: Option<&Path>) -> Res<Box<dyn Read>> {
    match file {
        None => Ok(Box::new(std::io::stdin())),
        Some(p) if p.as_os_str() == "-" => Ok(Box::new(std::io::stdin())),
        Some(p) => Ok(Box::new(File::open(p).map_err(|e| {
            CliError::new(format!("io: cannot open {}: {e}", p.display()))
        })?)),
    }
}

fn at(line: u64, col: Option<&str>, msg: impl std::fmt::Display) -> CliError {
    match col {
        Some(c) => CliError::new(format!("line {line}, column {c:?}: {msg}")),
        None => CliError::new(format!("line {line}: {msg}")),
    }
}

/// Catalog lookups for rows: series name → its table (cached).
struct Lookup<'a> {
    db: &'a Db,
    tables: HashMap<String, TableDef>,
    series: HashMap<String, String>,
}

impl<'a> Lookup<'a> {
    fn new(db: &'a Db) -> Lookup<'a> {
        Lookup {
            db,
            tables: HashMap::new(),
            series: HashMap::new(),
        }
    }

    fn table_of(&mut self, series: &str) -> muninn_core::Result<&TableDef> {
        if !self.series.contains_key(series) {
            let s = self.db.series(series)?;
            if !self.tables.contains_key(&s.table) {
                let t = self.db.table(&s.table)?;
                self.tables.insert(s.table.clone(), t);
            }
            self.series.insert(series.to_owned(), s.table);
        }
        Ok(&self.tables[&self.series[series]])
    }
}

/// Parses one CSV cell for a column of type `ty`; empty = null.
pub fn csv_value(ty: ColumnType, cell: &str, tf: TimeFormat) -> Result<Option<Value>, String> {
    if cell.is_empty() {
        return Ok(None);
    }
    let bad = |what: &str| Err(format!("type_mismatch: {cell:?} is not {what}"));
    Ok(Some(match ty {
        ColumnType::F64 => match cell {
            "NaN" => Value::F64(f64::NAN),
            "inf" => Value::F64(f64::INFINITY),
            "-inf" => Value::F64(f64::NEG_INFINITY),
            _ => {
                let numeric = cell
                    .bytes()
                    .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'+' | b'e' | b'E'))
                    && cell.bytes().any(|b| b.is_ascii_digit());
                match cell.parse::<f64>() {
                    Ok(x) if numeric => Value::F64(x),
                    _ => return bad("an f64 (a number, NaN, inf or -inf)"),
                }
            }
        },
        ColumnType::I64 => {
            let digits = cell.strip_prefix('-').unwrap_or(cell);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return bad("an i64 (an integer without fraction or exponent)");
            }
            match cell.parse::<i64>() {
                Ok(n) => Value::I64(n),
                Err(_) => return bad("an i64 (out of range)"),
            }
        }
        ColumnType::Bool => match cell {
            "true" | "TRUE" | "True" | "1" => Value::Bool(true),
            "false" | "FALSE" | "False" | "0" => Value::Bool(false),
            _ => return bad("a bool (true/false/1/0)"),
        },
        ColumnType::String => Value::String(cell.to_owned()),
        ColumnType::Timestamp => match tf.parse(cell) {
            Ok(t) => Value::Timestamp(t),
            Err(e) => return Err(format!("type_mismatch: {e}")),
        },
    }))
}

fn resolve_time(line: u64, col: &str, input: TimeInput) -> Res<TimeInput> {
    input.resolve().map(TimeInput::from).map_err(|e| {
        at(
            line,
            Some(col),
            format!("invalid_timestamp: {}", e.message()),
        )
    })
}

fn parse_flag(cell: &str) -> Option<bool> {
    match cell {
        "" | "false" | "FALSE" | "False" | "0" => Some(false),
        "true" | "TRUE" | "True" | "1" => Some(true),
        _ => None,
    }
}

const ENGINE_FIELDS: [&str; 2] = ["system_time", "seq"];

/// A source of `(line, row)` pairs.
type RowIter<'a> = Box<dyn Iterator<Item = Res<(u64, WriteRow)>> + 'a>;

struct Defaults {
    received: Option<Timestamp>,
    tf: TimeFormat,
}

fn csv_rows<'a>(
    reader: Box<dyn Read>,
    lookup: &'a mut Lookup<'_>,
    d: &'a Defaults,
) -> Res<RowIter<'a>> {
    let mut r = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .from_reader(reader);
    let header: Vec<String> = r
        .headers()
        .map_err(|e| CliError::new(format!("line 1: cannot read the CSV header: {e}")))?
        .iter()
        .map(str::to_owned)
        .collect();
    for (i, h) in header.iter().enumerate() {
        if header[..i].contains(h) {
            return Err(at(
                1,
                None,
                format!("column {h:?} appears twice in the header"),
            ));
        }
        if ENGINE_FIELDS.contains(&h.as_str()) {
            return Err(at(
                1,
                None,
                format!("{h} is assigned by the engine; use `muninn restore` to import it"),
            ));
        }
    }
    let pos = |name: &str| header.iter().position(|h| h == name);
    let (series_i, vt_i) = match (pos("series"), pos("value_time")) {
        (Some(s), Some(v)) => (s, v),
        _ => {
            return Err(at(
                1,
                None,
                "the header needs at least `series` and `value_time` columns",
            ));
        }
    };
    let (pt_i, rt_i, del_i) = (pos("published_time"), pos("received_time"), pos("deleted"));
    let fixed = [Some(series_i), Some(vt_i), pt_i, rt_i, del_i];
    let value_cols: Vec<(usize, String)> = header
        .iter()
        .enumerate()
        .filter(|(i, _)| !fixed.contains(&Some(*i)))
        .map(|(i, h)| (i, h.clone()))
        .collect();
    let mut records = r.into_records();
    Ok(Box::new(std::iter::from_fn(move || {
        let rec = match records.next()? {
            Ok(rec) => rec,
            Err(e) => {
                let line = e.position().map_or(0, |p| p.line());
                let msg = match e.kind() {
                    csv::ErrorKind::UnequalLengths {
                        expected_len, len, ..
                    } => format!("the row has {len} fields but the header has {expected_len}"),
                    _ => e.to_string(),
                };
                return Some(Err(at(line, None, msg)));
            }
        };
        let line = rec.position().map_or(0, |p| p.line());
        Some((|| {
            let series = &rec[series_i];
            if series.is_empty() {
                return Err(at(line, Some("series"), "empty series name"));
            }
            let table = lookup.table_of(series).map_err(|e| {
                at(
                    line,
                    Some("series"),
                    format!("{}: {}", e.kind(), e.message()),
                )
            })?;
            let vt = &rec[vt_i];
            if vt.is_empty() {
                return Err(at(line, Some("value_time"), "invalid_timestamp: empty"));
            }
            let value_time = resolve_time(line, "value_time", d.tf.input(vt))?;
            let published_time = match pt_i.map(|i| &rec[i]) {
                None | Some("") => None,
                Some(p) => Some(resolve_time(line, "published_time", d.tf.input(p))?),
            };
            let received_time = match (rt_i.map(|i| &rec[i]), d.received) {
                (Some(c), _) if !c.is_empty() => {
                    resolve_time(line, "received_time", d.tf.input(c))?
                }
                (_, Some(t)) => TimeInput::from(t),
                (_, None) => {
                    return Err(at(
                        line,
                        Some("received_time"),
                        "invalid_timestamp: missing (add a received_time column or pass \
                         --received-time now|<timestamp>)",
                    ));
                }
            };
            let deleted = match del_i.map(|i| &rec[i]) {
                None => false,
                Some(c) => parse_flag(c).ok_or_else(|| {
                    at(
                        line,
                        Some("deleted"),
                        format!("{c:?} is not true/false/1/0"),
                    )
                })?,
            };
            let mut values = BTreeMap::new();
            for (i, name) in &value_cols {
                let cell = &rec[*i];
                match table.column_index(name) {
                    Some(c) => {
                        let v = csv_value(table.columns[c].ty, cell, d.tf)
                            .map_err(|m| at(line, Some(name), m))?;
                        if v.is_some() {
                            values.insert(name.clone(), InputValue::from(v));
                        }
                    }
                    // A column of another table (multi-table files): must be empty here.
                    None if cell.is_empty() => {}
                    None => {
                        return Err(at(
                            line,
                            Some(name),
                            format!("unknown_column: table {:?} has no such column", table.name),
                        ));
                    }
                }
            }
            if deleted && !values.is_empty() {
                return Err(at(
                    line,
                    None,
                    "tombstone_with_values: a deleted row must leave every value empty",
                ));
            }
            Ok((
                line,
                WriteRow {
                    series: series.to_owned(),
                    value_time,
                    published_time,
                    received_time,
                    deleted,
                    values,
                },
            ))
        })())
    })))
}

fn json_time(line: u64, key: &str, v: &Wire, tf: TimeFormat) -> Res<TimeInput> {
    let input = match (v, tf) {
        (Wire::String(s), TimeFormat::Rfc3339) => tf.input(s),
        (Wire::Number(n), f) if f != TimeFormat::Rfc3339 => f.input(n),
        (Wire::String(s), f) => f.input(s),
        (other, _) => TimeInput::Invalid(other.to_string()),
    };
    resolve_time(line, key, input)
}

fn json_value(v: &Wire, ty: ColumnType, tf: TimeFormat) -> InputValue {
    match v {
        Wire::Null => InputValue::Null,
        Wire::Bool(b) => InputValue::Value(Value::Bool(*b)),
        Wire::Number(n) if ty == ColumnType::Timestamp && tf != TimeFormat::Rfc3339 => {
            match tf.parse(n) {
                Ok(t) => InputValue::Value(Value::Timestamp(t)),
                Err(_) => InputValue::Unsupported(n.clone()),
            }
        }
        Wire::Number(n) => InputValue::Number(n.clone()),
        Wire::String(s) if ty == ColumnType::Timestamp => match tf.parse(s) {
            Ok(t) => InputValue::Value(Value::Timestamp(t)),
            Err(_) => InputValue::Unsupported(format!("{s:?}")),
        },
        Wire::String(s) => InputValue::Value(Value::String(s.clone())),
        other => InputValue::Unsupported(other.to_string()),
    }
}

fn ndjson_row(line: u64, text: &str, lookup: &mut Lookup<'_>, d: &Defaults) -> Res<WriteRow> {
    let obj = Wire::parse(text).map_err(|e| at(line, None, format!("invalid JSON: {e}")))?;
    let fields = obj
        .as_object()
        .ok_or_else(|| at(line, None, "each line must be a JSON object"))?;
    let series = match obj.get("series") {
        Some(Wire::String(s)) if !s.is_empty() => s.clone(),
        _ => return Err(at(line, Some("series"), "must be a non-empty string")),
    };
    let table = lookup
        .table_of(&series)
        .map_err(|e| {
            at(
                line,
                Some("series"),
                format!("{}: {}", e.kind(), e.message()),
            )
        })?
        .clone();
    let value_time = match obj.get("value_time") {
        Some(v) if !v.is_null() => json_time(line, "value_time", v, d.tf)?,
        _ => return Err(at(line, Some("value_time"), "invalid_timestamp: missing")),
    };
    let published_time = match obj.get("published_time") {
        Some(v) if !v.is_null() => Some(json_time(line, "published_time", v, d.tf)?),
        _ => None,
    };
    let received_time = match (obj.get("received_time"), d.received) {
        (Some(v), _) if !v.is_null() => json_time(line, "received_time", v, d.tf)?,
        (_, Some(t)) => TimeInput::from(t),
        _ => {
            return Err(at(
                line,
                Some("received_time"),
                "invalid_timestamp: missing (pass --received-time now|<timestamp>)",
            ));
        }
    };
    let deleted = match obj.get("deleted") {
        None | Some(Wire::Null) => false,
        Some(Wire::Bool(b)) => *b,
        Some(other) => return Err(at(line, Some("deleted"), format!("{other} is not a bool"))),
    };
    let mut raw: Vec<(&str, &Wire)> = Vec::new();
    for (k, v) in fields {
        match k.as_str() {
            "series" | "value_time" | "published_time" | "received_time" | "deleted" => {}
            "values" => match v {
                Wire::Null => {}
                Wire::Object(m) => raw.extend(m.iter().map(|(k, v)| (k.as_str(), v))),
                other => {
                    return Err(at(
                        line,
                        Some("values"),
                        format!("{other} is not an object"),
                    ));
                }
            },
            k if ENGINE_FIELDS.contains(&k) => {
                return Err(at(
                    line,
                    Some(k),
                    "assigned by the engine; use `muninn restore` to import it",
                ));
            }
            k => raw.push((k, v)),
        }
    }
    let mut values = BTreeMap::new();
    for (name, v) in raw {
        let c = table.column_index(name).ok_or_else(|| {
            at(
                line,
                Some(name),
                format!("unknown_column: table {:?} has no such column", table.name),
            )
        })?;
        let iv = json_value(v, table.columns[c].ty, d.tf);
        let coerced = iv
            .coerce_to(table.columns[c].ty)
            .map_err(|e| at(line, Some(name), format!("{}: {}", e.kind(), e.message())))?;
        if coerced.is_some() {
            values.insert(name.to_owned(), InputValue::from(coerced));
        }
    }
    if deleted && !values.is_empty() {
        return Err(at(
            line,
            None,
            "tombstone_with_values: a deleted row must have every value null",
        ));
    }
    Ok(WriteRow {
        series,
        value_time,
        published_time,
        received_time,
        deleted,
        values,
    })
}

fn ndjson_rows<'a>(
    reader: Box<dyn Read>,
    lookup: &'a mut Lookup<'_>,
    d: &'a Defaults,
) -> RowIter<'a> {
    let mut lines = BufReader::new(reader).lines().enumerate();
    Box::new(std::iter::from_fn(move || {
        loop {
            let (i, l) = lines.next()?;
            let line = i as u64 + 1;
            let text = match l {
                Ok(t) => t,
                Err(e) => return Some(Err(at(line, None, format!("io: {e}")))),
            };
            if text.trim().is_empty() {
                continue;
            }
            return Some(ndjson_row(line, &text, lookup, d).map(|r| (line, r)));
        }
    }))
}

/// Totals over an import.
#[derive(Default)]
struct Totals {
    stored: u64,
    duplicates: u64,
    batches: u64,
    already: u64,
}

fn commit(
    db: &Db,
    ctx: &Context,
    rows: Vec<WriteRow>,
    lines: &[u64],
    batch_id: Option<String>,
    totals: &mut Totals,
) -> Res<()> {
    let batch = WriteBatch {
        rows,
        batch_id,
        clock: ctx.now().into(),
    };
    let r = db.write(&batch).map_err(|e| {
        // Engine messages name rows by batch index ("row 3: ..."); point at the line.
        let msg = e.message();
        let rewritten = msg
            .strip_prefix("row ")
            .and_then(|rest| rest.split_once(": "))
            .and_then(|(i, m)| i.parse::<usize>().ok().map(|i| (i, m)))
            .and_then(|(i, m)| lines.get(i).map(|l| format!("line {l}: {}: {m}", e.kind())));
        CliError::new(rewritten.unwrap_or_else(|| format!("{}: {msg}", e.kind())))
    })?;
    totals.stored += r.stored;
    totals.duplicates += r.duplicates;
    totals.batches += 1;
    if r.batch_already_committed {
        totals.already += 1;
    }
    Ok(())
}

/// `write`.
pub fn write(ctx: &Context, args: &WriteArgs) -> Res<()> {
    if args.chunk_rows == Some(0) {
        return Err(CliError::new("--chunk-rows must be at least 1"));
    }
    if args.batch_id.as_deref() == Some("") {
        return Err(CliError::new(
            "invalid_argument: --batch-id must be non-empty",
        ));
    }
    let received = match args.received_time.as_deref() {
        None => None,
        Some("now") => Some(ctx.now()),
        Some(t) => Some(parse_ts(t, "--received-time")?),
    };
    let d = Defaults {
        received,
        tf: args.time_format,
    };
    let format = args.input.unwrap_or_else(|| {
        match args
            .file
            .as_deref()
            .and_then(Path::extension)
            .and_then(|e| e.to_str())
        {
            Some("ndjson" | "jsonl" | "json") => InputFormat::Ndjson,
            _ => InputFormat::Csv,
        }
    });
    let db = ctx.open()?;
    let reader = open_input(args.file.as_deref())?;
    let mut lookup = Lookup::new(&db);
    let rows = match format {
        InputFormat::Csv => csv_rows(reader, &mut lookup, &d)?,
        InputFormat::Ndjson => ndjson_rows(reader, &mut lookup, &d),
    };
    let chunk = args.chunk_rows.unwrap_or(usize::MAX);
    let mut totals = Totals::default();
    let mut buf: Vec<WriteRow> = Vec::new();
    let mut lines: Vec<u64> = Vec::new();
    let mut k = 0u64;
    let id_for = |k: u64| {
        args.batch_id.as_ref().map(|id| match args.chunk_rows {
            Some(_) => format!("{id}/{k}"),
            None => id.clone(),
        })
    };
    let mut pending_err = None;
    for item in rows {
        match item {
            Ok((line, row)) => {
                buf.push(row);
                lines.push(line);
            }
            Err(e) => {
                pending_err = Some(e);
                break;
            }
        }
        if buf.len() >= chunk {
            commit(
                &db,
                ctx,
                std::mem::take(&mut buf),
                &lines,
                id_for(k),
                &mut totals,
            )?;
            lines.clear();
            k += 1;
        }
    }
    if let Some(e) = pending_err {
        if totals.batches > 0 {
            return Err(CliError::new(format!(
                "{}\n{} earlier chunk(s) were committed; fix the input and re-run{}",
                e.message,
                totals.batches,
                if args.batch_id.is_some() {
                    " with the same --batch-id to resume"
                } else {
                    " (duplicates are dropped if received_time is fixed)"
                }
            )));
        }
        return Err(e);
    }
    if !buf.is_empty() || totals.batches == 0 {
        commit(&db, ctx, buf, &lines, id_for(k), &mut totals)?;
    }
    output::line(&format!(
        "stored {}, duplicates {}, batches {}{}",
        totals.stored,
        totals.duplicates,
        totals.batches,
        if totals.already > 0 {
            format!(" ({} already committed, skipped)", totals.already)
        } else {
            String::new()
        }
    ))
}
