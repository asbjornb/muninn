//! Python / Arrow inputs → the loose request types of `muninn_core::api`.
//!
//! Everything here is *structural* (SPEC §10.3, `invalid_argument`) or produces loose
//! values ([`TimeInput`], [`InputValue`]) that the database validates itself in SPEC §4.1 /
//! §10.4 order. The Python layer (`muninn/_convert.py`) has already turned datetimes,
//! pandas / numpy scalars and timedeltas into ints (µs), strings, or an "invalid" marker;
//! anything that is neither an int nor a string here is therefore an invalid time.

use std::collections::BTreeMap;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use muninn_core::api::{TimeInput, WriteRow};
use muninn_core::{Clock, Duration, Error, ErrorKind, InputValue, Result, Value};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

pub(crate) fn invalid(msg: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidArgument, msg)
}

fn describe(obj: &Bound<'_, PyAny>) -> String {
    obj.repr()
        .map(|r| r.to_string())
        .unwrap_or_else(|_| "<object>".into())
}

// ---------------------------------------------------------------------------------------
// Scalars
// ---------------------------------------------------------------------------------------

/// A Python time argument (already normalised by the Python layer): `int` = µs since the
/// epoch, `str` = SPEC §1 text, anything else = invalid (`invalid_timestamp` when resolved).
pub(crate) fn time_input(obj: &Bound<'_, PyAny>) -> TimeInput {
    if obj.is_instance_of::<PyBool>() {
        return TimeInput::Invalid(format!("{} (a bool)", describe(obj)));
    }
    if let Ok(i) = obj.cast::<PyInt>() {
        return match i.extract::<i64>() {
            Ok(us) => TimeInput::Micros(us),
            Err(_) => TimeInput::Invalid(format!("{} µs is out of range", describe(obj))),
        };
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return match s.to_str() {
            Ok(s) => TimeInput::Text(s.to_owned()),
            Err(_) => TimeInput::Invalid("not valid UTF-8".into()),
        };
    }
    // The `_Invalid` marker of `muninn._convert` (or anything unexpected).
    TimeInput::Invalid(obj.str().map_or_else(|_| describe(obj), |s| s.to_string()))
}

/// An optional time argument: `None` = absent.
pub(crate) fn opt_time(obj: Option<&Bound<'_, PyAny>>) -> Option<TimeInput> {
    obj.filter(|o| !o.is_none()).map(time_input)
}

/// Resolves a read-side time argument (`invalid_timestamp` names the argument).
pub(crate) fn resolve_time(
    t: &Option<TimeInput>,
    what: &str,
) -> Result<Option<muninn_core::Timestamp>> {
    t.as_ref()
        .map(|t| {
            t.resolve()
                .map_err(|e| Error::new(e.kind(), format!("{what}: {}", e.message())))
        })
        .transpose()
}

/// A duration argument (grid windows): `int` = µs, `str` = SPEC §1 duration text.
#[derive(Clone, Debug)]
pub(crate) enum DurInput {
    Micros(i64),
    Text(String),
    Invalid(String),
}

impl DurInput {
    pub(crate) fn from_py(obj: &Bound<'_, PyAny>) -> DurInput {
        if obj.is_instance_of::<PyBool>() {
            return DurInput::Invalid(describe(obj));
        }
        if let Ok(i) = obj.cast::<PyInt>() {
            return i
                .extract::<i64>()
                .map_or_else(|_| DurInput::Invalid(describe(obj)), DurInput::Micros);
        }
        if let Ok(s) = obj.extract::<String>() {
            return DurInput::Text(s);
        }
        DurInput::Invalid(obj.str().map_or_else(|_| describe(obj), |s| s.to_string()))
    }

    /// SPEC §8: a malformed window bound is a "bad window" (`invalid_range`).
    pub(crate) fn resolve(&self, what: &str) -> Result<Duration> {
        match self {
            DurInput::Micros(us) => Ok(Duration::from_micros(*us)),
            DurInput::Text(s) => Duration::parse(s)
                .map_err(|e| Error::new(ErrorKind::InvalidRange, format!("{what}: {e}"))),
            DurInput::Invalid(d) => Err(Error::new(
                ErrorKind::InvalidRange,
                format!("{what}: not a duration: {d}"),
            )),
        }
    }
}

pub(crate) fn opt_clock(obj: Option<&Bound<'_, PyAny>>) -> Result<Option<Clock>> {
    match obj.filter(|o| !o.is_none()) {
        None => Ok(None),
        Some(o) => o
            .extract::<String>()
            .ok()
            .and_then(|s| Clock::parse(&s))
            .map(Some)
            .ok_or_else(|| {
                invalid(format!(
                    "clock must be 'received', 'published' or 'system', not {}",
                    describe(o)
                ))
            }),
    }
}

/// A series filter: `None` or a list of names (the Python layer turns a single name or
/// any iterable into a list).
pub(crate) fn series_list(obj: Option<&Bound<'_, PyAny>>) -> Result<Vec<String>> {
    match obj.filter(|o| !o.is_none()) {
        None => Ok(Vec::new()),
        Some(o) => {
            let list = o.cast::<PyList>().map_err(|_| {
                invalid(format!(
                    "series must be a name or a list of names, not {}",
                    describe(o)
                ))
            })?;
            list.iter()
                .map(|s| {
                    s.extract::<String>().map_err(|_| {
                        invalid(format!(
                            "series names must be strings, not {}",
                            describe(&s)
                        ))
                    })
                })
                .collect()
        }
    }
}

/// The table argument of a read: a non-string is `unknown_table` (as in `muninn_core::wire`).
pub(crate) fn table_name(obj: &Bound<'_, PyAny>) -> Result<String> {
    obj.extract::<String>().map_err(|_| {
        Error::new(
            ErrorKind::UnknownTable,
            format!("no table {}", describe(obj)),
        )
    })
}

/// A write value cell from a Python object (list-of-dicts input).
fn input_value(obj: &Bound<'_, PyAny>, helpers: &Helpers<'_>) -> PyResult<InputValue> {
    if obj.is_none() {
        return Ok(InputValue::Null);
    }
    if let Ok(b) = obj.cast::<PyBool>() {
        return Ok(InputValue::Value(Value::Bool(b.is_true())));
    }
    if let Ok(i) = obj.cast::<PyInt>() {
        return Ok(match i.extract::<i64>() {
            Ok(n) => InputValue::Value(Value::I64(n)),
            // Beyond i64: keep the exact digits (an i64 mismatch, or the nearest f64).
            Err(_) => InputValue::Number(i.str()?.to_string()),
        });
    }
    if let Ok(f) = obj.cast::<PyFloat>() {
        return Ok(InputValue::Value(Value::F64(f.value())));
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return Ok(InputValue::Value(Value::String(s.to_str()?.to_owned())));
    }
    // numpy / pandas scalars, datetimes: normalised by the Python helper into a
    // (kind, payload) tuple.
    let t = helpers.value_arg.call1((obj, helpers.assume_utc))?;
    let t = t.cast::<PyTuple>()?;
    let kind: String = t.get_item(0)?.extract()?;
    let p = t.get_item(1)?;
    Ok(match kind.as_str() {
        "null" => InputValue::Null,
        "bool" => InputValue::Value(Value::Bool(p.extract()?)),
        "int" => match p.extract::<i64>() {
            Ok(n) => InputValue::Value(Value::I64(n)),
            Err(_) => InputValue::Number(p.str()?.to_string()),
        },
        "float" => InputValue::Value(Value::F64(p.extract()?)),
        "str" => InputValue::Value(Value::String(p.extract()?)),
        "ts" => match muninn_core::Timestamp::from_micros(p.extract()?) {
            Some(t) => InputValue::Value(Value::Timestamp(t)),
            None => InputValue::Unsupported("timestamp out of range".into()),
        },
        _ => InputValue::Unsupported(p.str()?.to_string()),
    })
}

/// Python helpers from `muninn._convert`, looked up once per call.
pub(crate) struct Helpers<'py> {
    time_arg: Bound<'py, PyAny>,
    value_arg: Bound<'py, PyAny>,
    assume_utc: bool,
}

impl<'py> Helpers<'py> {
    pub(crate) fn new(py: Python<'py>, assume_utc: bool) -> PyResult<Helpers<'py>> {
        let m = py.import("muninn._convert")?;
        Ok(Helpers {
            time_arg: m.getattr("time_arg")?,
            value_arg: m.getattr("value_arg")?,
            assume_utc,
        })
    }

    /// A time cell of a dict row: `None` (or NaT) = missing.
    fn time(&self, obj: &Bound<'py, PyAny>) -> PyResult<Option<TimeInput>> {
        if obj.is_none() {
            return Ok(None);
        }
        if obj.is_instance_of::<PyString>()
            || (obj.is_instance_of::<PyInt>() && !obj.is_instance_of::<PyBool>())
        {
            return Ok(Some(time_input(obj)));
        }
        let n = self.time_arg.call1((obj, self.assume_utc, true))?;
        Ok(if n.is_none() {
            None
        } else {
            Some(time_input(&n))
        })
    }
}

/// An error that is either a Muninn error (structure) or a Python exception raised while
/// converting (propagated as is).
pub(crate) enum ConvError {
    Muninn(Error),
    Py(PyErr),
}

impl From<Error> for ConvError {
    fn from(e: Error) -> ConvError {
        ConvError::Muninn(e)
    }
}

impl From<PyErr> for ConvError {
    fn from(e: PyErr) -> ConvError {
        ConvError::Py(e)
    }
}

pub(crate) type ConvResult<T> = std::result::Result<T, ConvError>;

const FIXED_ROW_KEYS: [&str; 5] = [
    "series",
    "value_time",
    "published_time",
    "received_time",
    "deleted",
];

/// Write rows from a list of dicts. A row is either *flat* (every key other than the five
/// fixed fields is a value column) or *nested* (spec form: a `values` dict, and then no
/// other keys are allowed).
pub(crate) fn rows_from_dicts(
    list: &Bound<'_, PyList>,
    h: &Helpers<'_>,
) -> ConvResult<Vec<WriteRow>> {
    let mut out = Vec::with_capacity(list.len());
    for (i, row) in list.iter().enumerate() {
        let d = row.cast::<PyDict>().map_err(|_| {
            invalid(format!(
                "row {i}: a row must be a dict, not {}",
                describe(&row)
            ))
        })?;
        let mut series = None;
        let mut value_time = None;
        let mut received_time = None;
        let mut published_time = None;
        let mut deleted = false;
        let mut values: BTreeMap<String, InputValue> = BTreeMap::new();
        let nested = d.contains("values")?;
        for (k, v) in d.iter() {
            let key: String = k.extract().map_err(|_| {
                invalid(format!(
                    "row {i}: keys must be strings, not {}",
                    describe(&k)
                ))
            })?;
            match key.as_str() {
                "series" => {
                    series = Some(v.extract::<String>().map_err(|_| {
                        invalid(format!(
                            "row {i}: series must be a string, not {}",
                            describe(&v)
                        ))
                    })?);
                }
                "value_time" => value_time = h.time(&v)?,
                "received_time" => received_time = h.time(&v)?,
                "published_time" => published_time = h.time(&v)?,
                "deleted" => {
                    // SPEC §10.3: `deleted` must be a bool when present (null is not).
                    deleted = if let Ok(b) = v.cast::<PyBool>() {
                        b.is_true()
                    } else if v
                        .get_type()
                        .name()
                        .is_ok_and(|n| n == "bool" || n == "bool_")
                    {
                        v.is_truthy()? // numpy.bool_
                    } else {
                        return Err(invalid(format!(
                            "row {i}: deleted must be a bool, not {}",
                            describe(&v)
                        ))
                        .into());
                    };
                }
                "values" => {
                    if v.is_none() {
                        continue;
                    }
                    let vd = v.cast::<PyDict>().map_err(|_| {
                        invalid(format!(
                            "row {i}: values must be a dict, not {}",
                            describe(&v)
                        ))
                    })?;
                    for (ck, cv) in vd.iter() {
                        let col: String = ck.extract().map_err(|_| {
                            invalid(format!(
                                "row {i}: column names must be strings, not {}",
                                describe(&ck)
                            ))
                        })?;
                        values.insert(col, input_value(&cv, h)?);
                    }
                }
                _ if nested => {
                    return Err(invalid(format!(
                        "row {i}: unknown field {key:?} (a row with a `values` dict takes \
                         no other value keys)"
                    ))
                    .into());
                }
                _ => {
                    values.insert(key, input_value(&v, h)?);
                }
            }
        }
        let series = series.ok_or_else(|| invalid(format!("row {i}: series is required")))?;
        out.push(WriteRow {
            series,
            value_time: value_time.unwrap_or_else(|| TimeInput::Invalid("missing".into())),
            published_time,
            received_time: received_time.unwrap_or_else(|| TimeInput::Invalid("missing".into())),
            deleted,
            values,
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------
// Arrow
// ---------------------------------------------------------------------------------------

/// Microseconds from a timestamp of `unit`, or why not (sub-µs part, overflow).
fn scale_to_micros(v: i64, unit: TimeUnit) -> std::result::Result<i64, String> {
    match unit {
        TimeUnit::Second => v
            .checked_mul(1_000_000)
            .ok_or_else(|| format!("{v} s is out of range")),
        TimeUnit::Millisecond => v
            .checked_mul(1_000)
            .ok_or_else(|| format!("{v} ms is out of range")),
        TimeUnit::Microsecond => Ok(v),
        TimeUnit::Nanosecond => {
            if v % 1_000 == 0 {
                Ok(v / 1_000)
            } else {
                Err(format!(
                    "{v} ns has a sub-microsecond part (Muninn stores microseconds)"
                ))
            }
        }
    }
}

/// One Arrow cell as a time (`None` = null).
fn arrow_time(arr: &dyn Array, i: usize, assume_utc: bool) -> Option<TimeInput> {
    if arr.is_null(i) {
        return None;
    }
    Some(match arr.data_type() {
        DataType::Timestamp(unit, tz) => {
            if tz.is_none() && !assume_utc {
                return Some(TimeInput::Invalid(
                    "naive timestamp (no time zone); localize it or pass assume_utc=True".into(),
                ));
            }
            let v = match unit {
                TimeUnit::Second => arr.as_primitive::<TimestampSecondType>().value(i),
                TimeUnit::Millisecond => arr.as_primitive::<TimestampMillisecondType>().value(i),
                TimeUnit::Microsecond => arr.as_primitive::<TimestampMicrosecondType>().value(i),
                TimeUnit::Nanosecond => arr.as_primitive::<TimestampNanosecondType>().value(i),
            };
            match scale_to_micros(v, *unit) {
                Ok(us) => TimeInput::Micros(us),
                Err(why) => TimeInput::Invalid(why),
            }
        }
        DataType::Int8 => TimeInput::Micros(arr.as_primitive::<Int8Type>().value(i).into()),
        DataType::Int16 => TimeInput::Micros(arr.as_primitive::<Int16Type>().value(i).into()),
        DataType::Int32 => TimeInput::Micros(arr.as_primitive::<Int32Type>().value(i).into()),
        DataType::Int64 => TimeInput::Micros(arr.as_primitive::<Int64Type>().value(i)),
        DataType::UInt8 => TimeInput::Micros(arr.as_primitive::<UInt8Type>().value(i).into()),
        DataType::UInt16 => TimeInput::Micros(arr.as_primitive::<UInt16Type>().value(i).into()),
        DataType::UInt32 => TimeInput::Micros(arr.as_primitive::<UInt32Type>().value(i).into()),
        DataType::UInt64 => {
            let v = arr.as_primitive::<UInt64Type>().value(i);
            i64::try_from(v).map_or_else(
                |_| TimeInput::Invalid(format!("{v} µs is out of range")),
                TimeInput::Micros,
            )
        }
        DataType::Utf8 => TimeInput::Text(arr.as_string::<i32>().value(i).to_owned()),
        DataType::LargeUtf8 => TimeInput::Text(arr.as_string::<i64>().value(i).to_owned()),
        DataType::Utf8View => TimeInput::Text(arr.as_string_view().value(i).to_owned()),
        other => TimeInput::Invalid(format!("a {other} value is not a timestamp")),
    })
}

/// One Arrow cell as a write value.
fn arrow_value(arr: &dyn Array, i: usize, assume_utc: bool) -> InputValue {
    if arr.is_null(i) {
        return InputValue::Null;
    }
    let v = match arr.data_type() {
        DataType::Float64 => Value::F64(arr.as_primitive::<Float64Type>().value(i)),
        DataType::Float32 => Value::F64(arr.as_primitive::<Float32Type>().value(i).into()),
        DataType::Float16 => Value::F64(arr.as_primitive::<Float16Type>().value(i).to_f64()),
        DataType::Int8 => Value::I64(arr.as_primitive::<Int8Type>().value(i).into()),
        DataType::Int16 => Value::I64(arr.as_primitive::<Int16Type>().value(i).into()),
        DataType::Int32 => Value::I64(arr.as_primitive::<Int32Type>().value(i).into()),
        DataType::Int64 => Value::I64(arr.as_primitive::<Int64Type>().value(i)),
        DataType::UInt8 => Value::I64(arr.as_primitive::<UInt8Type>().value(i).into()),
        DataType::UInt16 => Value::I64(arr.as_primitive::<UInt16Type>().value(i).into()),
        DataType::UInt32 => Value::I64(arr.as_primitive::<UInt32Type>().value(i).into()),
        DataType::UInt64 => {
            let v = arr.as_primitive::<UInt64Type>().value(i);
            match i64::try_from(v) {
                Ok(n) => Value::I64(n),
                Err(_) => return InputValue::Number(v.to_string()),
            }
        }
        DataType::Boolean => Value::Bool(arr.as_boolean().value(i)),
        DataType::Utf8 => Value::String(arr.as_string::<i32>().value(i).to_owned()),
        DataType::LargeUtf8 => Value::String(arr.as_string::<i64>().value(i).to_owned()),
        DataType::Utf8View => Value::String(arr.as_string_view().value(i).to_owned()),
        DataType::Timestamp(_, _) => match arrow_time(arr, i, assume_utc) {
            Some(TimeInput::Micros(us)) => match muninn_core::Timestamp::from_micros(us) {
                Some(t) => Value::Timestamp(t),
                None => return InputValue::Unsupported(format!("{us} µs is out of range")),
            },
            Some(TimeInput::Invalid(why)) => return InputValue::Unsupported(why),
            _ => return InputValue::Null,
        },
        DataType::Null => return InputValue::Null,
        other => return InputValue::Unsupported(format!("a {other} value")),
    };
    InputValue::Value(v)
}

/// An input column with dictionary encoding resolved once (keys normalised up front, so
/// each cell is a constant-time lookup into the dictionary values).
struct Col<'a> {
    arr: &'a dyn Array,
    dict: Option<(Vec<usize>, &'a dyn Array)>,
}

impl<'a> Col<'a> {
    fn new(arr: &'a dyn Array) -> Col<'a> {
        let dict = arr.as_any_dictionary_opt().map(|d| {
            let keys = if d.values().is_empty() {
                Vec::new()
            } else {
                d.normalized_keys()
            };
            (keys, d.values().as_ref())
        });
        Col { arr, dict }
    }

    /// The array and index holding row `i`, or `None` if the cell is null.
    fn at(&self, i: usize) -> Option<(&'a dyn Array, usize)> {
        if self.arr.is_null(i) {
            return None;
        }
        Some(match &self.dict {
            Some((keys, values)) => (*values, keys[i]),
            None => (self.arr, i),
        })
    }

    fn time(&self, i: usize, assume_utc: bool) -> Option<TimeInput> {
        self.at(i).and_then(|(a, j)| arrow_time(a, j, assume_utc))
    }

    fn value(&self, i: usize, assume_utc: bool) -> InputValue {
        self.at(i)
            .map_or(InputValue::Null, |(a, j)| arrow_value(a, j, assume_utc))
    }
}

/// The `series` column of a batch as names (non-null strings).
fn series_column(b: &RecordBatch, what: &str) -> Result<Vec<String>> {
    let col = b
        .column_by_name("series")
        .ok_or_else(|| invalid(format!("{what} needs a `series` column")))?;
    let c = Col::new(col.as_ref());
    (0..b.num_rows())
        .map(|i| match c.at(i).and_then(|(a, j)| arrow_string(a, j)) {
            Some(s) => Ok(s),
            None if col.is_null(i) => Err(invalid(format!("{what} row {i}: series is null"))),
            None => Err(invalid(format!(
                "{what}: series must be a string column, not {}",
                col.data_type()
            ))),
        })
        .collect()
}

fn arrow_string(arr: &dyn Array, i: usize) -> Option<String> {
    if arr.is_null(i) {
        return None;
    }
    match arr.data_type() {
        DataType::Utf8 => Some(arr.as_string::<i32>().value(i).to_owned()),
        DataType::LargeUtf8 => Some(arr.as_string::<i64>().value(i).to_owned()),
        DataType::Utf8View => Some(arr.as_string_view().value(i).to_owned()),
        _ => None,
    }
}

/// Write rows from Arrow record batches. Columns: `series`, `value_time`,
/// `received_time`, optional `published_time` and `deleted`, the rest are value columns.
/// Null cells of value columns are omitted (a null is the same as a missing value, and in a
/// columnar batch that spans several tables each row only has its own table's columns).
pub(crate) fn rows_from_arrow(batches: &[RecordBatch], assume_utc: bool) -> Result<Vec<WriteRow>> {
    let mut out = Vec::new();
    for b in batches {
        let series = series_column(b, "write data")?;
        let schema = b.schema();
        let get = |name: &str| b.column_by_name(name).map(|c| Col::new(c.as_ref()));
        let vt = get("value_time");
        let rt = get("received_time");
        let pt = get("published_time");
        let del = b.column_by_name("deleted");
        if let Some(d) = del
            && d.data_type() != &DataType::Boolean
        {
            return Err(invalid(format!(
                "the deleted column must be bool, not {}",
                d.data_type()
            )));
        }
        let value_cols: Vec<(String, Col<'_>)> = schema
            .fields()
            .iter()
            .zip(b.columns())
            .filter(|(f, _)| !FIXED_ROW_KEYS.contains(&f.name().as_str()))
            .map(|(f, c)| (f.name().clone(), Col::new(c.as_ref())))
            .collect();
        for (i, series) in series.into_iter().enumerate() {
            let missing = || TimeInput::Invalid("missing".into());
            let deleted = match del {
                None => false,
                Some(d) if d.is_null(i) => {
                    return Err(invalid(format!(
                        "row {i}: deleted is null (it must be true or false)"
                    )));
                }
                Some(d) => d.as_boolean().value(i),
            };
            let mut values = BTreeMap::new();
            for (name, col) in &value_cols {
                let v = col.value(i, assume_utc);
                if v != InputValue::Null {
                    values.insert(name.clone(), v);
                }
            }
            out.push(WriteRow {
                series,
                value_time: vt
                    .as_ref()
                    .and_then(|c| c.time(i, assume_utc))
                    .unwrap_or_else(missing),
                published_time: pt.as_ref().and_then(|c| c.time(i, assume_utc)),
                received_time: rt
                    .as_ref()
                    .and_then(|c| c.time(i, assume_utc))
                    .unwrap_or_else(missing),
                deleted,
                values,
            });
        }
    }
    Ok(out)
}

/// An `asof_join` left row before its times are resolved.
pub(crate) struct LeftRow {
    pub series: String,
    pub value_time: TimeInput,
    pub frontier: TimeInput,
}

/// Left rows from Arrow batches (`series`, `value_time`, `frontier` columns; other columns
/// are carried through by the Python layer).
pub(crate) fn left_from_arrow(batches: &[RecordBatch], assume_utc: bool) -> Result<Vec<LeftRow>> {
    let mut out = Vec::new();
    for b in batches {
        let series = series_column(b, "left")?;
        let vt = b.column_by_name("value_time").map(|c| Col::new(c.as_ref()));
        let f = b.column_by_name("frontier").map(|c| Col::new(c.as_ref()));
        for (i, series) in series.into_iter().enumerate() {
            let missing = || TimeInput::Invalid("missing".into());
            out.push(LeftRow {
                series,
                value_time: vt
                    .as_ref()
                    .and_then(|c| c.time(i, assume_utc))
                    .unwrap_or_else(missing),
                frontier: f
                    .as_ref()
                    .and_then(|c| c.time(i, assume_utc))
                    .unwrap_or_else(missing),
            });
        }
    }
    Ok(out)
}

/// Left rows from a list of dicts with exactly the keys `series`, `value_time`, `frontier`.
pub(crate) fn left_from_dicts(
    list: &Bound<'_, PyList>,
    h: &Helpers<'_>,
) -> ConvResult<Vec<LeftRow>> {
    let mut out = Vec::with_capacity(list.len());
    for (i, row) in list.iter().enumerate() {
        let d = row.cast::<PyDict>().map_err(|_| {
            invalid(format!(
                "left row {i} must be a dict, not {}",
                describe(&row)
            ))
        })?;
        let mut series = None;
        let mut vt = None;
        let mut f = None;
        for (k, v) in d.iter() {
            let key: String = k
                .extract()
                .map_err(|_| invalid(format!("left row {i}: keys must be strings")))?;
            match key.as_str() {
                "series" => {
                    series = Some(v.extract::<String>().map_err(|_| {
                        invalid(format!(
                            "left row {i}: series must be a string, not {}",
                            describe(&v)
                        ))
                    })?);
                }
                "value_time" => vt = h.time(&v)?,
                "frontier" => f = h.time(&v)?,
                other => {
                    return Err(invalid(format!("left row {i}: unknown field {other:?}")).into());
                }
            }
        }
        out.push(LeftRow {
            series: series.ok_or_else(|| invalid(format!("left row {i}: series is required")))?,
            value_time: vt.unwrap_or_else(|| TimeInput::Invalid("missing".into())),
            frontier: f.unwrap_or_else(|| TimeInput::Invalid("missing".into())),
        });
    }
    Ok(out)
}
