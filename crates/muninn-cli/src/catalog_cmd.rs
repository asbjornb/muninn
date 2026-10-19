//! `table` and `series` commands.

use std::collections::BTreeMap;

use muninn_core::api::{AddColumn, CreateSeries, CreateTable};
use muninn_core::types::{SeriesDef, TableDef};
use muninn_core::wire::Wire;
use muninn_core::{Clock, ColumnDef, ColumnType, Database, Duration, Partitioning, Reader};

use crate::db::Context;
use crate::output::{self, Rows, n, s};
use crate::{CliError, InputFormat, Res, SeriesCmd, TableCmd};

/// Parses `name:type`.
pub fn parse_column(spec: &str) -> Res<ColumnDef> {
    let (name, ty) = spec
        .split_once(':')
        .ok_or_else(|| CliError::new(format!("column {spec:?} must be written NAME:TYPE")))?;
    let ty = ColumnType::parse(ty).ok_or_else(|| {
        CliError::new(format!(
            "unknown column type {ty:?} (use f64, i64, bool, string or timestamp)"
        ))
    })?;
    Ok(ColumnDef::new(name, ty))
}

/// Parses `key=value`.
pub fn parse_tag(spec: &str) -> Res<(String, String)> {
    spec.split_once('=')
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .ok_or_else(|| CliError::new(format!("tag {spec:?} must be written KEY=VALUE")))
}

/// Parses a clock name.
pub fn parse_clock(s: &str) -> Res<Clock> {
    Clock::parse(s).ok_or_else(|| {
        CliError::new(format!(
            "invalid_argument: unknown clock {s:?} (use received, published or system)"
        ))
    })
}

fn columns_text(t: &TableDef) -> String {
    t.columns
        .iter()
        .map(|c| format!("{}:{}", c.name, c.ty))
        .collect::<Vec<_>>()
        .join(",")
}

fn tags_text(tags: &BTreeMap<String, String>) -> String {
    tags.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn series_rows(series: &[SeriesDef]) -> Rows {
    let mut rows = Rows::flat(&["id", "name", "table", "tags", "timezone", "resolution"]);
    for x in series {
        rows.push(vec![
            n(x.id.0),
            s(&*x.name),
            s(&x.table),
            s(tags_text(&x.tags)),
            s(&x.timezone),
            x.resolution.map_or(Wire::Null, |r| s(r.to_string())),
        ]);
    }
    rows
}

/// `table ...`.
pub fn table(ctx: &Context, cmd: TableCmd) -> Res<()> {
    match cmd {
        TableCmd::Create {
            name,
            columns,
            clock,
            partition,
        } => {
            let columns = columns
                .iter()
                .map(|c| parse_column(c))
                .collect::<Res<Vec<_>>>()?;
            let default_clock = parse_clock(&clock)?;
            let partition = Partitioning::parse(&partition).ok_or_else(|| {
                CliError::new(format!(
                    "invalid_argument: unknown partitioning {partition:?} (use day, month or year)"
                ))
            })?;
            let db = ctx.open()?;
            let t = db.create_table(&CreateTable {
                name,
                columns,
                default_clock,
                partition,
                clock: Some(ctx.now()),
            })?;
            output::line(&format!("created table {} ({})", t.name, columns_text(&t)))
        }
        TableCmd::AddColumn { table, column } => {
            let column = parse_column(&column)?;
            let db = ctx.open()?;
            let t = db.add_column(&AddColumn {
                table,
                column,
                clock: Some(ctx.now()),
            })?;
            output::line(&format!("table {} now has ({})", t.name, columns_text(&t)))
        }
        TableCmd::List(f) => {
            let db = ctx.open()?;
            let snap = db.snapshot()?;
            let mut rows = Rows::flat(&["name", "columns", "default_clock", "partition", "series"]);
            for t in snap.tables()? {
                let count = snap.table_series(&t.name)?.len();
                rows.push(vec![
                    s(&t.name),
                    s(columns_text(&t)),
                    s(t.default_clock.name()),
                    s(t.partition.name()),
                    n(count),
                ]);
            }
            output::print(&rows, f.resolve())
        }
        TableCmd::Show { name, format } => {
            let db = ctx.open()?;
            let snap = db.snapshot()?;
            let t = snap.table(&name)?;
            let format = format.resolve();
            let mut rows = Rows::flat(&["column", "type"]);
            for c in &t.columns {
                rows.push(vec![s(&c.name), s(c.ty.name())]);
            }
            if format == output::Format::Table {
                output::line(&format!(
                    "table {}  (default clock {}, partition {})",
                    t.name, t.default_clock, t.partition
                ))?;
                output::print(&rows, format)?;
                output::line("")?;
            }
            output::print(&series_rows(&snap.table_series(&name)?), format)
        }
    }
}

/// `series ...`.
pub fn series(ctx: &Context, cmd: SeriesCmd) -> Res<()> {
    match cmd {
        SeriesCmd::Create {
            name,
            table,
            tags,
            timezone,
            resolution,
        } => {
            let tags = tags
                .iter()
                .map(|t| parse_tag(t))
                .collect::<Res<BTreeMap<_, _>>>()?;
            let resolution = resolution
                .map(|r| {
                    Duration::parse(&r).map_err(|e| CliError::new(format!("invalid_range: {e}")))
                })
                .transpose()?;
            let db = ctx.open()?;
            let x = db.create_series(&CreateSeries {
                name,
                table,
                tags,
                timezone,
                resolution,
                clock: Some(ctx.now()),
            })?;
            output::line(&format!(
                "created series {} (id {}) in table {}",
                x.name, x.id.0, x.table
            ))
        }
        SeriesCmd::Import { file, input, table } => {
            let format = input.unwrap_or_else(|| {
                match file
                    .as_deref()
                    .and_then(std::path::Path::extension)
                    .and_then(|e| e.to_str())
                {
                    Some("ndjson" | "jsonl" | "json") => InputFormat::Ndjson,
                    _ => InputFormat::Csv,
                }
            });
            let reader = crate::input::open_input(file.as_deref())?;
            let mut reqs = match format {
                InputFormat::Csv => series_from_csv(reader, table.as_deref())?,
                InputFormat::Ndjson => series_from_ndjson(reader, table.as_deref())?,
            };
            let now = ctx.now();
            for r in &mut reqs {
                r.1.clock = Some(now);
            }
            let db = ctx.open()?;
            let only: Vec<CreateSeries> = reqs.iter().map(|(_, r)| r.clone()).collect();
            let created = match db.create_series_batch(&only) {
                Ok(c) => c,
                Err(e) => {
                    // Point at the line of the first request that fails: replay the
                    // engine's checks on the catalog.
                    let st = crate::db::load_state(&ctx.store()?)?;
                    let mut cat = (*st.catalog).clone();
                    for (line, r) in &reqs {
                        match cat.check_create_series(r) {
                            Ok(d) => cat
                                .apply(&muninn_core::catalog::CatalogChangeKind::CreateSeries(d))
                                .map_err(CliError::from)?,
                            Err(_) => return Err(CliError::new(format!("line {line}: {e}"))),
                        }
                    }
                    return Err(CliError::from(e));
                }
            };
            output::line(&format!(
                "created {} series (ids {}..={})",
                created.len(),
                created.first().map_or(0, |x| x.id.0),
                created.last().map_or(0, |x| x.id.0)
            ))
        }
        SeriesCmd::List {
            table,
            tags,
            format,
        } => {
            let tags = tags.iter().map(|t| parse_tag(t)).collect::<Res<Vec<_>>>()?;
            let db = ctx.open()?;
            let snap = db.snapshot()?;
            let mut all = Vec::new();
            match &table {
                Some(t) => all = snap.table_series(t)?,
                None => {
                    for t in snap.tables()? {
                        all.extend(snap.table_series(&t.name)?);
                    }
                    all.sort_by_key(|x| x.id);
                }
            }
            all.retain(|x| tags.iter().all(|(k, v)| x.tags.get(k) == Some(v)));
            output::print(&series_rows(&all), format.resolve())
        }
    }
}

fn parse_resolution(r: &str, line: u64) -> Res<Duration> {
    Duration::parse(r)
        .map_err(|e| CliError::new(format!("line {line}: invalid_range: resolution: {e}")))
}

/// `series import` CSV: `(line, request)` per record.
fn series_from_csv(
    reader: Box<dyn std::io::Read>,
    table: Option<&str>,
) -> Res<Vec<(u64, CreateSeries)>> {
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
            return Err(CliError::new(format!(
                "line 1: column {h:?} appears twice in the header"
            )));
        }
    }
    let pos = |n: &str| header.iter().position(|h| h == n);
    let name_i = pos("name")
        .ok_or_else(|| CliError::new("line 1: the header needs a `name` column".to_owned()))?;
    let (table_i, tz_i, res_i) = (pos("table"), pos("timezone"), pos("resolution"));
    if table_i.is_none() && table.is_none() {
        return Err(CliError::new(
            "line 1: no `table` column; pass --table".to_owned(),
        ));
    }
    let mut out = Vec::new();
    for rec in r.records() {
        let rec = rec.map_err(|e| CliError::new(format!("invalid CSV: {e}")))?;
        let line = rec.position().map_or(0, |p| p.line());
        let cell = |i: usize| rec.get(i).unwrap_or("");
        let tbl = match table_i.map(cell).filter(|t| !t.is_empty()) {
            Some(t) => t.to_owned(),
            None => table
                .ok_or_else(|| CliError::new(format!("line {line}: no table (pass --table)")))?
                .to_owned(),
        };
        let mut req = CreateSeries::new(cell(name_i), tbl);
        req.timezone = tz_i.map(cell).filter(|t| !t.is_empty()).map(str::to_owned);
        req.resolution = res_i
            .map(cell)
            .filter(|t| !t.is_empty())
            .map(|t| parse_resolution(t, line))
            .transpose()?;
        for (i, h) in header.iter().enumerate() {
            if [Some(name_i), table_i, tz_i, res_i].contains(&Some(i)) || cell(i).is_empty() {
                continue;
            }
            req.tags.insert(h.clone(), cell(i).to_owned());
        }
        out.push((line, req));
    }
    Ok(out)
}

/// `series import` NDJSON: `(line, request)` per non-empty line.
fn series_from_ndjson(
    reader: Box<dyn std::io::Read>,
    table: Option<&str>,
) -> Res<Vec<(u64, CreateSeries)>> {
    use std::io::BufRead;
    let mut out = Vec::new();
    for (i, text) in std::io::BufReader::new(reader).lines().enumerate() {
        let line = i as u64 + 1;
        let text = text.map_err(|e| CliError::new(format!("io: line {line}: {e}")))?;
        let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
        if text.trim().is_empty() {
            continue;
        }
        let bad = |m: String| CliError::new(format!("line {line}: {m}"));
        let obj = Wire::parse(text).map_err(|e| bad(format!("invalid JSON: {e}")))?;
        let fields = obj
            .as_object()
            .ok_or_else(|| bad("expected a JSON object".to_owned()))?;
        let mut req = CreateSeries::new("", table.unwrap_or_default());
        let mut has_table = table.is_some();
        let mut has_name = false;
        for (k, v) in fields {
            let text = |what: &str| {
                v.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| bad(format!("invalid_argument: {what} must be a string")))
            };
            match k.as_str() {
                "name" => {
                    req.name = text("name")?;
                    has_name = true;
                }
                "table" => {
                    req.table = text("table")?;
                    has_table = true;
                }
                "timezone" => {
                    if !matches!(v, Wire::Null) {
                        req.timezone = Some(text("timezone")?);
                    }
                }
                "resolution" => {
                    if !matches!(v, Wire::Null) {
                        req.resolution = Some(parse_resolution(&text("resolution")?, line)?);
                    }
                }
                "tags" => match v {
                    Wire::Null => {}
                    Wire::Object(tags) => {
                        for (tk, tv) in tags {
                            let tv = tv.as_str().ok_or_else(|| {
                                bad("invalid_argument: tag values must be strings".to_owned())
                            })?;
                            req.tags.insert(tk.clone(), tv.to_owned());
                        }
                    }
                    _ => return Err(bad("invalid_argument: tags must be an object".to_owned())),
                },
                other => return Err(bad(format!("invalid_argument: unknown field {other:?}"))),
            }
        }
        if !has_name {
            return Err(bad("invalid_argument: missing `name`".to_owned()));
        }
        if !has_table {
            return Err(bad("no `table` (pass --table)".to_owned()));
        }
        out.push((line, req));
    }
    Ok(out)
}
