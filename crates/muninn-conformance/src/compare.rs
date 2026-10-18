//! Typed comparison of actual results with scenario expectations (SPEC §9), and readable
//! rendering of both sides for diffs.

use std::sync::Arc;

use muninn_core::api::{AsofRow, GridRow, ReadResult, VersionRow};
use muninn_core::wire::{Wire, encode_value};
use muninn_core::{ColumnDef, ColumnType, Timestamp, Value};

/// The read operations, which determine the output fields (SPEC §6, §9).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReadOp {
    Latest,
    History,
    Replay,
    Grid,
    AsofJoin,
}

impl ReadOp {
    pub fn from_op(op: &str) -> Option<ReadOp> {
        Some(match op {
            "latest" => ReadOp::Latest,
            "history" => ReadOp::History,
            "replay" => ReadOp::Replay,
            "grid" => ReadOp::Grid,
            "asof_join" => ReadOp::AsofJoin,
            _ => return None,
        })
    }

    /// Exactly the fields of a full expected row, in spec order.
    fn fields(self) -> &'static [&'static str] {
        const LATEST: &[&str] = &[
            "series",
            "value_time",
            "published_time",
            "received_time",
            "system_time",
            "seq",
            "values",
        ];
        const HISTORY: &[&str] = &[
            "series",
            "value_time",
            "published_time",
            "received_time",
            "system_time",
            "seq",
            "deleted",
            "values",
        ];
        const GRID: &[&str] = &[
            "frontier",
            "series",
            "value_time",
            "published_time",
            "received_time",
            "system_time",
            "seq",
            "values",
        ];
        const ASOF: &[&str] = &[
            "series",
            "value_time",
            "frontier",
            "found",
            "published_time",
            "received_time",
            "system_time",
            "seq",
            "values",
        ];
        match self {
            ReadOp::Latest => LATEST,
            ReadOp::History | ReadOp::Replay => HISTORY,
            ReadOp::Grid => GRID,
            ReadOp::AsofJoin => ASOF,
        }
    }

    /// Leading fields of a `values`-sugar entry (the last element is the values slot).
    fn sugar_fields(self) -> &'static [&'static str] {
        match self {
            ReadOp::Latest | ReadOp::History | ReadOp::Replay => &["series", "value_time"],
            ReadOp::Grid => &["frontier", "series", "value_time"],
            ReadOp::AsofJoin => &["series", "value_time", "frontier"],
        }
    }
}

/// One field of an actual output row.
#[derive(Clone, Debug)]
enum Field {
    Name(Arc<str>),
    Time(Option<Timestamp>),
    Seq(Option<u64>),
    Flag(bool),
    /// One entry per current column; `None` = the whole right side is null (asof miss).
    Values(Option<Vec<Option<Value>>>),
}

/// An actual output row as named fields.
#[derive(Clone, Debug)]
pub(crate) struct ActualRow {
    fields: Vec<(&'static str, Field)>,
}

impl ActualRow {
    fn get(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|(n, _)| *n == name).map(|(_, f)| f)
    }

    fn deleted(&self) -> bool {
        matches!(self.get("deleted"), Some(Field::Flag(true)))
    }
}

/// Actual rows plus the table's current columns.
pub(crate) struct Actual {
    pub op: ReadOp,
    pub columns: Vec<ColumnDef>,
    pub rows: Vec<ActualRow>,
}

fn version_fields(op: ReadOp, r: &VersionRow) -> Vec<(&'static str, Field)> {
    let mut f = vec![
        ("series", Field::Name(Arc::clone(&r.series))),
        ("value_time", Field::Time(Some(r.value_time))),
        ("published_time", Field::Time(r.published_time)),
        ("received_time", Field::Time(Some(r.received_time))),
        ("system_time", Field::Time(Some(r.system_time))),
        ("seq", Field::Seq(Some(r.seq))),
    ];
    if matches!(op, ReadOp::History | ReadOp::Replay) {
        f.push(("deleted", Field::Flag(r.deleted)));
    }
    f.push(("values", Field::Values(Some(r.values.clone()))));
    f
}

impl Actual {
    pub fn versions(op: ReadOp, res: ReadResult<VersionRow>) -> Actual {
        let rows = res
            .rows
            .iter()
            .map(|r| ActualRow {
                fields: version_fields(op, r),
            })
            .collect();
        Actual {
            op,
            columns: res.columns,
            rows,
        }
    }

    pub fn grid(res: ReadResult<GridRow>) -> Actual {
        let rows = res
            .rows
            .iter()
            .map(|g| {
                let mut fields = vec![("frontier", Field::Time(Some(g.frontier)))];
                fields.extend(version_fields(ReadOp::Grid, &g.row));
                ActualRow { fields }
            })
            .collect();
        Actual {
            op: ReadOp::Grid,
            columns: res.columns,
            rows,
        }
    }

    pub fn asof(res: ReadResult<AsofRow>) -> Actual {
        let rows = res
            .rows
            .iter()
            .map(|a| {
                let r = a.right.as_ref();
                ActualRow {
                    fields: vec![
                        ("series", Field::Name(Arc::clone(&a.series))),
                        ("value_time", Field::Time(Some(a.value_time))),
                        ("frontier", Field::Time(Some(a.frontier))),
                        ("found", Field::Flag(r.is_some())),
                        (
                            "published_time",
                            Field::Time(r.and_then(|r| r.published_time)),
                        ),
                        ("received_time", Field::Time(r.map(|r| r.received_time))),
                        ("system_time", Field::Time(r.map(|r| r.system_time))),
                        ("seq", Field::Seq(r.map(|r| r.seq))),
                        ("values", Field::Values(r.map(|r| r.values.clone()))),
                    ],
                }
            })
            .collect();
        Actual {
            op: ReadOp::AsofJoin,
            columns: res.columns,
            rows,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Rendering (for diffs)
// ---------------------------------------------------------------------------------------

fn render_values(
    columns: &[ColumnDef],
    values: &Option<Vec<Option<Value>>>,
    only: Option<&[String]>,
) -> String {
    let Some(values) = values else {
        return "null".into();
    };
    let fields: Vec<(String, Wire)> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| only.is_none_or(|o| o.contains(&c.name)))
        .map(|(i, c)| {
            (
                c.name.clone(),
                encode_value(values.get(i).and_then(Option::as_ref)),
            )
        })
        .collect();
    Wire::Object(fields).to_string()
}

fn render_field(columns: &[ColumnDef], f: &Field) -> String {
    match f {
        Field::Name(s) => Wire::String(s.to_string()).to_string(),
        Field::Time(Some(t)) => format!("\"{t}\""),
        Field::Time(None) | Field::Seq(None) => "null".into(),
        Field::Seq(Some(n)) => n.to_string(),
        Field::Flag(b) => b.to_string(),
        Field::Values(v) => render_values(columns, v, None),
    }
}

/// Renders an actual row in the full `rows` form.
pub(crate) fn render_full(a: &Actual, row: &ActualRow) -> String {
    let parts: Vec<String> = row
        .fields
        .iter()
        .map(|(n, f)| format!("\"{n}\": {}", render_field(&a.columns, f)))
        .collect();
    format!("{{{}}}", parts.join(", "))
}

/// Renders an actual row in the `values` sugar form (only the columns `expected` names,
/// when it is a values object).
pub(crate) fn render_sugar(a: &Actual, row: &ActualRow, expected: Option<&Wire>) -> String {
    let mut parts: Vec<String> =
        a.op.sugar_fields()
            .iter()
            .map(|n| {
                row.get(n)
                    .map_or("?".into(), |f| render_field(&a.columns, f))
            })
            .collect();
    let only: Option<Vec<String>> = expected
        .and_then(Wire::as_array)
        .and_then(<[Wire]>::last)
        .and_then(Wire::as_object)
        .map(|o| o.iter().map(|(k, _)| k.clone()).collect());
    let last = match (a.op, row.get("values")) {
        (ReadOp::History | ReadOp::Replay, _) if row.deleted() => "\"deleted\"".into(),
        (_, Some(Field::Values(v))) => render_values(&a.columns, v, only.as_deref()),
        _ => "?".into(),
    };
    parts.push(last);
    format!("[{}]", parts.join(", "))
}

// ---------------------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------------------

fn parse_expected_time(j: &Wire) -> Result<Option<Timestamp>, String> {
    match j {
        Wire::Null => Ok(None),
        Wire::String(s) => Timestamp::parse(s)
            .map(Some)
            .map_err(|e| format!("bad expected timestamp: {e}")),
        other => Err(format!(
            "expected a timestamp string or null, found {other}"
        )),
    }
}

/// SPEC §9/§10.2: f64 bitwise, a JSON number in the expectation parsed to the nearest
/// f64 (the integer token `-0` is -0.0); non-finite values are "NaN", "inf", "-inf".
fn expected_f64(j: &Wire) -> Option<f64> {
    match j {
        Wire::Number(n) if n == "-0" => Some(-0.0),
        Wire::Number(n) => n.parse().ok(),
        Wire::String(s) => match s.as_str() {
            "NaN" => Some(f64::NAN),
            "inf" => Some(f64::INFINITY),
            "-inf" => Some(f64::NEG_INFINITY),
            _ => None,
        },
        _ => None,
    }
}

/// i64 expectations: an integer token or a decimal string, compared as integers.
fn expected_i64(j: &Wire) -> Option<i64> {
    let text = match j {
        Wire::Number(n) if j.is_integer_token() => n.as_str(),
        Wire::String(s) => s.as_str(),
        _ => return None,
    };
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// Compares one column value; `Err` describes the mismatch.
fn compare_value(ty: ColumnType, expected: &Wire, actual: Option<&Value>) -> Result<(), String> {
    let mismatch = || Err(format!("expected {expected}, got {}", encode_value(actual)));
    let Some(actual) = actual else {
        return if expected.is_null() {
            Ok(())
        } else {
            mismatch()
        };
    };
    let equal = match (ty, actual) {
        (ColumnType::F64, Value::F64(x)) => {
            expected_f64(expected).is_some_and(|e| e.to_bits() == x.to_bits())
        }
        (ColumnType::I64, Value::I64(x)) => expected_i64(expected) == Some(*x),
        (ColumnType::Bool, Value::Bool(x)) => *expected == Wire::Bool(*x),
        (ColumnType::String, Value::String(x)) => expected.as_str() == Some(x.as_str()),
        (ColumnType::Timestamp, Value::Timestamp(x)) => {
            expected.as_str().and_then(|s| Timestamp::parse(s).ok()) == Some(*x)
        }
        (ty, v) => return Err(format!("implementation returned {v:?} for a {ty} column")),
    };
    if equal { Ok(()) } else { mismatch() }
}

/// Compares a values object. `full`: the object must name exactly the current columns.
fn compare_values(
    columns: &[ColumnDef],
    expected: &Wire,
    actual: &Option<Vec<Option<Value>>>,
    full: bool,
) -> Result<(), String> {
    let Some(actual) = actual else {
        return if expected.is_null() {
            Ok(())
        } else {
            Err(format!("expected values {expected}, got null"))
        };
    };
    let Some(obj) = expected.as_object() else {
        return Err(format!("expected values {expected}, got an object"));
    };
    for (k, _) in obj {
        if !columns.iter().any(|c| &c.name == k) {
            return Err(format!(
                "expectation names column `{k}`, which the table does not have"
            ));
        }
    }
    if full {
        let missing: Vec<&str> = columns
            .iter()
            .map(|c| c.name.as_str())
            .filter(|n| expected.get(n).is_none())
            .collect();
        if !missing.is_empty() {
            return Err(format!(
                "full-row expectation must list every current column; missing {missing:?}"
            ));
        }
    }
    for (i, c) in columns.iter().enumerate() {
        if let Some(e) = expected.get(&c.name) {
            compare_value(c.ty, e, actual.get(i).and_then(Option::as_ref))
                .map_err(|m| format!("values.{}: {m}", c.name))?;
        }
    }
    Ok(())
}

fn compare_field(
    columns: &[ColumnDef],
    name: &str,
    expected: &Wire,
    actual: &Field,
    full: bool,
) -> Result<(), String> {
    let r = match actual {
        Field::Name(s) => {
            if expected.as_str() == Some(s) {
                Ok(())
            } else {
                Err(format!("expected {expected}, got {:?}", s.as_ref()))
            }
        }
        Field::Time(t) => {
            let e = parse_expected_time(expected)?;
            // Compared as integers (SPEC §9).
            if e.map(Timestamp::as_micros) == t.map(Timestamp::as_micros) {
                Ok(())
            } else {
                Err(format!(
                    "expected {expected}, got {}",
                    t.map_or("null".into(), |t| format!("\"{t}\""))
                ))
            }
        }
        Field::Seq(s) => {
            let e = match expected {
                Wire::Null => Some(None),
                Wire::Number(n) if expected.is_integer_token() => n.parse::<u64>().ok().map(Some),
                _ => None,
            };
            if e == Some(*s) {
                Ok(())
            } else {
                Err(format!(
                    "expected {expected}, got {}",
                    s.map_or("null".into(), |s| s.to_string())
                ))
            }
        }
        Field::Flag(b) => {
            if *expected == Wire::Bool(*b) {
                Ok(())
            } else {
                Err(format!("expected {expected}, got {b}"))
            }
        }
        Field::Values(v) => return compare_values(columns, expected, v, full),
    };
    r.map_err(|m| format!("{name}: {m}"))
}

/// Compares one row in the full `rows` form.
pub(crate) fn compare_full(a: &Actual, expected: &Wire, row: &ActualRow) -> Result<(), String> {
    let Some(obj) = expected.as_object() else {
        return Err(format!("expected row must be an object, found {expected}"));
    };
    let want = a.op.fields();
    let extra: Vec<&String> = obj
        .iter()
        .map(|(k, _)| k)
        .filter(|k| !want.contains(&k.as_str()))
        .collect();
    let missing: Vec<&&str> = want.iter().filter(|k| expected.get(k).is_none()).collect();
    if !extra.is_empty() || !missing.is_empty() {
        return Err(format!(
            "expected row must have exactly the fields {want:?} (SPEC §9); extra {extra:?}, missing {missing:?}"
        ));
    }
    for (name, field) in &row.fields {
        let e = expected.get(name).expect("checked above");
        compare_field(&a.columns, name, e, field, true)?;
    }
    Ok(())
}

/// Compares one row in the `values` sugar form.
pub(crate) fn compare_sugar(a: &Actual, expected: &Wire, row: &ActualRow) -> Result<(), String> {
    let lead = a.op.sugar_fields();
    let Some(arr) = expected
        .as_array()
        .filter(|arr| arr.len() == lead.len() + 1)
    else {
        return Err(format!(
            "`values` entry must be an array of {} elements, found {expected}",
            lead.len() + 1
        ));
    };
    for (i, name) in lead.iter().enumerate() {
        let field = row.get(name).expect("field exists for op");
        compare_field(&a.columns, name, &arr[i], field, false)?;
    }
    let last = &arr[lead.len()];
    let values = match row.get("values") {
        Some(Field::Values(v)) => v,
        _ => unreachable!("every op has values"),
    };
    match a.op {
        ReadOp::History | ReadOp::Replay => match (last.as_str(), row.deleted()) {
            (Some("deleted"), true) => Ok(()),
            (Some("deleted"), false) => Err("expected a tombstone, got a value version".into()),
            (Some(other), _) => Err(format!("unknown sugar marker {other:?} (only \"deleted\")")),
            (None, true) => Err(format!("expected values {last}, got a tombstone")),
            (None, false) => compare_values(&a.columns, last, values, false),
        },
        ReadOp::AsofJoin => match (last.is_null(), values.is_some()) {
            (true, true) => Err("expected not found (null), got a match".into()),
            (true, false) => Ok(()),
            (false, false) => Err(format!("expected values {last}, got not found")),
            (false, true) => compare_values(&a.columns, last, values, false),
        },
        ReadOp::Latest | ReadOp::Grid => compare_values(&a.columns, last, values, false),
    }
}
