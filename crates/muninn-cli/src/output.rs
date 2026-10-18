//! Printing rows as CSV, NDJSON or an aligned table.

use std::io::{IsTerminal, Write};

use clap::ValueEnum;
use muninn_core::api::{AsofRow, GridRow, ReadResult, VersionRow};
use muninn_core::wire::{Wire, encode_value};
use muninn_core::{Timestamp, Value};

use crate::{CliError, FormatArg, Res};

/// Output formats.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// RFC 4180 CSV with a header row; null is an empty field.
    Csv,
    /// One JSON object per line (value columns nested under `values`, as in SPEC §9).
    Ndjson,
    /// Aligned columns for humans.
    Table,
}

impl FormatArg {
    /// The chosen format, defaulting to `table` on a terminal and `csv` otherwise.
    pub fn resolve(self) -> Format {
        self.format.unwrap_or_else(|| {
            if std::io::stdout().is_terminal() {
                Format::Table
            } else {
                Format::Csv
            }
        })
    }
}

/// A result table: named columns of JSON-encoded cells.
pub struct Rows {
    /// Column names.
    pub columns: Vec<String>,
    /// Index of the first value column (NDJSON nests columns from here under `values`);
    /// `None` = flat.
    pub values_from: Option<usize>,
    /// Rows; each has one cell per column.
    pub rows: Vec<Row>,
}

/// One output row.
pub struct Row {
    /// Cells, one per column.
    pub cells: Vec<Wire>,
    /// NDJSON: write `"values": null` instead of an object (asof-join, not found).
    pub values_null: bool,
}

impl Rows {
    /// An empty, flat result with these columns.
    pub fn flat(columns: &[&str]) -> Rows {
        Rows {
            columns: columns.iter().map(|s| (*s).to_owned()).collect(),
            values_from: None,
            rows: Vec::new(),
        }
    }

    /// Appends a flat row.
    pub fn push(&mut self, cells: Vec<Wire>) {
        self.rows.push(Row {
            cells,
            values_null: false,
        });
    }
}

/// A string cell.
pub fn s(v: impl Into<String>) -> Wire {
    Wire::String(v.into())
}

/// A number cell.
pub fn n(v: impl ToString) -> Wire {
    Wire::Number(v.to_string())
}

/// A timestamp cell (canonical text form).
pub fn ts(t: Timestamp) -> Wire {
    Wire::String(t.to_string())
}

/// An optional timestamp cell.
pub fn ots(t: Option<Timestamp>) -> Wire {
    t.map_or(Wire::Null, ts)
}

/// The text of a cell in CSV / table output.
fn text(w: &Wire) -> String {
    match w {
        Wire::Null => String::new(),
        Wire::Bool(b) => b.to_string(),
        Wire::Number(n) => n.clone(),
        Wire::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn io(e: std::io::Error) -> CliError {
    if e.kind() == std::io::ErrorKind::BrokenPipe {
        CliError {
            message: String::new(),
            code: 0,
        }
    } else {
        CliError::new(format!("io: writing output: {e}"))
    }
}

/// Prints `rows` to stdout in `format`.
pub fn print(rows: &Rows, format: Format) -> Res<()> {
    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());
    write(&mut out, rows, format).map_err(io)?;
    out.flush().map_err(io)
}

/// Prints a line of text to stdout.
pub fn line(text: &str) -> Res<()> {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    writeln!(out, "{text}").map_err(io)
}

/// One NDJSON object for `row`.
pub fn json_object(rows: &Rows, row: &Row) -> Wire {
    let split = rows.values_from.unwrap_or(rows.columns.len());
    let mut fields: Vec<(String, Wire)> = rows.columns[..split]
        .iter()
        .cloned()
        .zip(row.cells[..split].iter().cloned())
        .collect();
    if rows.values_from.is_some() {
        let values = if row.values_null {
            Wire::Null
        } else {
            Wire::Object(
                rows.columns[split..]
                    .iter()
                    .cloned()
                    .zip(row.cells[split..].iter().cloned())
                    .collect(),
            )
        };
        fields.push(("values".to_owned(), values));
    }
    Wire::Object(fields)
}

fn write(out: &mut impl Write, rows: &Rows, format: Format) -> std::io::Result<()> {
    match format {
        Format::Csv => {
            let mut w = csv::Writer::from_writer(out);
            w.write_record(&rows.columns)?;
            for r in &rows.rows {
                w.write_record(r.cells.iter().map(text))?;
            }
            w.flush()?;
        }
        Format::Ndjson => {
            for r in &rows.rows {
                writeln!(out, "{}", json_object(rows, r))?;
            }
        }
        Format::Table => {
            let cells: Vec<Vec<String>> = rows
                .rows
                .iter()
                .map(|r| {
                    r.cells
                        .iter()
                        .map(text)
                        .map(|t| t.replace('\n', "\\n"))
                        .collect()
                })
                .collect();
            let mut width: Vec<usize> = rows.columns.iter().map(|c| c.chars().count()).collect();
            for r in &cells {
                for (w, c) in width.iter_mut().zip(r) {
                    *w = (*w).max(c.chars().count());
                }
            }
            let line = |out: &mut dyn Write, items: &[String]| -> std::io::Result<()> {
                let mut s = String::new();
                for (i, (c, w)) in items.iter().zip(&width).enumerate() {
                    if i > 0 {
                        s.push_str("  ");
                    }
                    s.push_str(c);
                    if i + 1 < items.len() {
                        s.extend(std::iter::repeat_n(' ', w - c.chars().count()));
                    }
                }
                writeln!(out, "{}", s.trim_end())
            };
            line(out, &rows.columns)?;
            let rule: Vec<String> = width.iter().map(|w| "-".repeat(*w)).collect();
            line(out, &rule)?;
            for r in &cells {
                line(out, r)?;
            }
            writeln!(
                out,
                "({} row{})",
                cells.len(),
                if cells.len() == 1 { "" } else { "s" }
            )?;
        }
    }
    Ok(())
}

/// A value cell. Non-finite f64 use the spec strings; i64 beyond ±2^53 are strings.
pub fn value(v: Option<&Value>) -> Wire {
    encode_value(v)
}

const VERSION_COLS: [&str; 6] = [
    "series",
    "value_time",
    "published_time",
    "received_time",
    "system_time",
    "seq",
];

fn version_cells(r: &VersionRow, deleted: bool) -> Vec<Wire> {
    let mut c = vec![
        s(&*r.series),
        ts(r.value_time),
        ots(r.published_time),
        ts(r.received_time),
        ts(r.system_time),
        n(r.seq),
    ];
    if deleted {
        c.push(Wire::Bool(r.deleted));
    }
    c.extend(r.values.iter().map(|v| value(v.as_ref())));
    c
}

/// Rows of `latest` (`deleted = false`) or `history` / `replay` (`deleted = true`).
pub fn versions(res: &ReadResult<VersionRow>, deleted: bool) -> Rows {
    let mut columns: Vec<String> = VERSION_COLS.iter().map(|s| (*s).to_owned()).collect();
    if deleted {
        columns.push("deleted".into());
    }
    let values_from = columns.len();
    columns.extend(res.columns.iter().map(|c| c.name.clone()));
    Rows {
        columns,
        values_from: Some(values_from),
        rows: res
            .rows
            .iter()
            .map(|r| Row {
                cells: version_cells(r, deleted),
                values_null: false,
            })
            .collect(),
    }
}

/// Rows of `grid`.
pub fn grid(res: &ReadResult<GridRow>) -> Rows {
    let mut columns = vec!["frontier".to_owned()];
    columns.extend(VERSION_COLS.iter().map(|s| (*s).to_owned()));
    let values_from = columns.len();
    columns.extend(res.columns.iter().map(|c| c.name.clone()));
    Rows {
        columns,
        values_from: Some(values_from),
        rows: res
            .rows
            .iter()
            .map(|g| {
                let mut cells = vec![ts(g.frontier)];
                cells.extend(version_cells(&g.row, false));
                Row {
                    cells,
                    values_null: false,
                }
            })
            .collect(),
    }
}

/// Rows of `asof_join`.
pub fn asof(res: &ReadResult<AsofRow>) -> Rows {
    let mut columns: Vec<String> = [
        "series",
        "value_time",
        "frontier",
        "found",
        "published_time",
        "received_time",
        "system_time",
        "seq",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    let values_from = columns.len();
    columns.extend(res.columns.iter().map(|c| c.name.clone()));
    let width = res.columns.len();
    Rows {
        columns,
        values_from: Some(values_from),
        rows: res
            .rows
            .iter()
            .map(|a| {
                let mut cells = vec![
                    s(&*a.series),
                    ts(a.value_time),
                    ts(a.frontier),
                    Wire::Bool(a.found()),
                ];
                match &a.right {
                    Some(r) => {
                        cells.extend([
                            ots(r.published_time),
                            ts(r.received_time),
                            ts(r.system_time),
                            n(r.seq),
                        ]);
                        cells.extend(r.values.iter().map(|v| value(v.as_ref())));
                    }
                    None => cells.extend(std::iter::repeat_n(Wire::Null, 4 + width)),
                }
                Row {
                    cells,
                    values_null: a.right.is_none(),
                }
            })
            .collect(),
    }
}
