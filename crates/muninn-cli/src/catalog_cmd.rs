//! `table` and `series` commands.

use std::collections::BTreeMap;

use muninn_core::api::{AddColumn, CreateSeries, CreateTable};
use muninn_core::types::{SeriesDef, TableDef};
use muninn_core::wire::Wire;
use muninn_core::{Clock, ColumnDef, ColumnType, Database, Duration, Partitioning, Reader};

use crate::db::Context;
use crate::output::{self, Rows, n, s};
use crate::{CliError, Res, SeriesCmd, TableCmd};

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
