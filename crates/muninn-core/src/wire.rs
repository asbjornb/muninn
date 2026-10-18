//! JSON requests (SPEC §9 shapes, §10 input rules) on top of any [`Database`].
//!
//! The typed [`crate::api`] cannot even represent most malformed inputs (an unknown clock
//! string, a number where a series name belongs, an array as a value). Front ends that
//! receive JSON — the conformance runner, and later bindings and servers — go through this
//! module instead, which
//!
//! 1. checks the **structure** of the whole request (§10.3, `invalid_argument`) first,
//! 2. then performs the remaining checks in the §10.4 order — catalog lookups via the
//!    [`Reader`] methods (reads have no effect, so looking up early is safe), timestamp
//!    parsing in field order, windows — and
//! 3. hands everything else to the typed API, passing loose write inputs
//!    ([`TimeInput`], [`InputValue`]) through so that the database itself reports
//!    `invalid_timestamp` / `type_mismatch` in §4.1 order.
//!
//! Because it is generic over [`Database`], every implementation reports the same codes
//! for the same JSON. [`Wire`] is a minimal JSON value with a strict parser that keeps
//! number tokens verbatim (so `-0`, `2.0` and huge integers keep their §10.2 meaning).

use std::collections::BTreeMap;
use std::fmt;

use crate::api::{
    AddColumn, AsofJoinRequest, AsofLeftRow, AsofRow, CreateSeries, CreateTable, GridRequest,
    GridRow, GridWindow, LatestRequest, ReadContext, ReadResult, ReplayRequest, TimeInput,
    VersionRow, WriteBatch, WriteResult, WriteRow,
};
use crate::error::{Error, ErrorKind, Result};
use crate::time::{Duration, Timestamp};
use crate::types::{
    Clock, ColumnDef, ColumnType, InputValue, Partitioning, TableDef, TimeRange, Value,
};
use crate::{Database, Reader};

// ---------------------------------------------------------------------------------------
// The JSON value
// ---------------------------------------------------------------------------------------

/// A JSON value. Numbers keep their exact token text; objects keep their key order (a
/// repeated key keeps its first position and its last value, like Python's `json`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Wire {
    /// `null`.
    Null,
    /// `true` / `false`.
    Bool(bool),
    /// A number token, verbatim (valid JSON number grammar).
    Number(String),
    /// A string.
    String(String),
    /// An array.
    Array(Vec<Wire>),
    /// An object.
    Object(Vec<(String, Wire)>),
}

/// Error from [`Wire::parse`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireParseError {
    /// Byte offset of the problem.
    pub offset: usize,
    /// What is wrong.
    pub message: String,
}

impl fmt::Display for WireParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid JSON at byte {}: {}", self.offset, self.message)
    }
}

impl std::error::Error for WireParseError {}

const MAX_DEPTH: usize = 256;

struct Parser<'a> {
    s: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    fn err<T>(&self, msg: &str) -> std::result::Result<T, WireParseError> {
        Err(WireParseError {
            offset: self.pos,
            message: msg.to_owned(),
        })
    }

    fn ws(&mut self) {
        while matches!(self.s.get(self.pos), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn lit(&mut self, word: &str, v: Wire) -> std::result::Result<Wire, WireParseError> {
        if self.s[self.pos..].starts_with(word.as_bytes()) {
            self.pos += word.len();
            Ok(v)
        } else {
            self.err("unexpected token")
        }
    }

    fn value(&mut self, depth: usize) -> std::result::Result<Wire, WireParseError> {
        if depth > MAX_DEPTH {
            return self.err("nesting too deep");
        }
        self.ws();
        match self.s.get(self.pos) {
            None => self.err("unexpected end of input"),
            Some(b'n') => self.lit("null", Wire::Null),
            Some(b't') => self.lit("true", Wire::Bool(true)),
            Some(b'f') => self.lit("false", Wire::Bool(false)),
            Some(b'"') => Ok(Wire::String(self.string()?)),
            Some(b'[') => {
                self.pos += 1;
                let mut items = Vec::new();
                self.ws();
                if self.s.get(self.pos) == Some(&b']') {
                    self.pos += 1;
                    return Ok(Wire::Array(items));
                }
                loop {
                    items.push(self.value(depth + 1)?);
                    self.ws();
                    match self.s.get(self.pos) {
                        Some(b',') => self.pos += 1,
                        Some(b']') => {
                            self.pos += 1;
                            return Ok(Wire::Array(items));
                        }
                        _ => return self.err("expected ',' or ']'"),
                    }
                }
            }
            Some(b'{') => {
                self.pos += 1;
                let mut fields: Vec<(String, Wire)> = Vec::new();
                self.ws();
                if self.s.get(self.pos) == Some(&b'}') {
                    self.pos += 1;
                    return Ok(Wire::Object(fields));
                }
                loop {
                    self.ws();
                    if self.s.get(self.pos) != Some(&b'"') {
                        return self.err("expected a string key");
                    }
                    let key = self.string()?;
                    self.ws();
                    if self.s.get(self.pos) != Some(&b':') {
                        return self.err("expected ':'");
                    }
                    self.pos += 1;
                    let v = self.value(depth + 1)?;
                    match fields.iter_mut().find(|(k, _)| *k == key) {
                        Some(slot) => slot.1 = v,
                        None => fields.push((key, v)),
                    }
                    self.ws();
                    match self.s.get(self.pos) {
                        Some(b',') => self.pos += 1,
                        Some(b'}') => {
                            self.pos += 1;
                            return Ok(Wire::Object(fields));
                        }
                        _ => return self.err("expected ',' or '}'"),
                    }
                }
            }
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(_) => self.err("unexpected character"),
        }
    }

    fn digits(&mut self) -> usize {
        let start = self.pos;
        while matches!(self.s.get(self.pos), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
        self.pos - start
    }

    fn number(&mut self) -> std::result::Result<Wire, WireParseError> {
        let start = self.pos;
        if self.s[self.pos] == b'-' {
            self.pos += 1;
        }
        match self.s.get(self.pos) {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => {
                self.digits();
            }
            _ => return self.err("bad number"),
        }
        if self.s.get(self.pos) == Some(&b'.') {
            self.pos += 1;
            if self.digits() == 0 {
                return self.err("bad number fraction");
            }
        }
        if matches!(self.s.get(self.pos), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.s.get(self.pos), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if self.digits() == 0 {
                return self.err("bad number exponent");
            }
        }
        let tok = std::str::from_utf8(&self.s[start..self.pos]).expect("ASCII");
        Ok(Wire::Number(tok.to_owned()))
    }

    fn hex4(&mut self) -> std::result::Result<u32, WireParseError> {
        let Some(h) = self.s.get(self.pos..self.pos + 4) else {
            return self.err("bad \\u escape");
        };
        let Some(n) = std::str::from_utf8(h)
            .ok()
            .and_then(|h| u32::from_str_radix(h, 16).ok())
        else {
            return self.err("bad \\u escape");
        };
        self.pos += 4;
        Ok(n)
    }

    fn string(&mut self) -> std::result::Result<String, WireParseError> {
        self.pos += 1; // opening quote
        let mut out = String::new();
        loop {
            let start = self.pos;
            while matches!(self.s.get(self.pos), Some(&c) if c != b'"' && c != b'\\' && c >= 0x20) {
                self.pos += 1;
            }
            out.push_str(std::str::from_utf8(&self.s[start..self.pos]).expect("input is str"));
            match self.s.get(self.pos) {
                Some(b'"') => {
                    self.pos += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.pos += 1;
                    let Some(&e) = self.s.get(self.pos) else {
                        return self.err("bad escape");
                    };
                    self.pos += 1;
                    match e {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let hi = self.hex4()?;
                            let c = if (0xD800..0xDC00).contains(&hi) {
                                if !self.s[self.pos..].starts_with(b"\\u") {
                                    return self.err("unpaired surrogate");
                                }
                                self.pos += 2;
                                let lo = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return self.err("unpaired surrogate");
                                }
                                0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                            } else {
                                hi
                            };
                            match char::from_u32(c) {
                                Some(c) => out.push(c),
                                None => return self.err("unpaired surrogate"),
                            }
                        }
                        _ => return self.err("bad escape"),
                    }
                }
                Some(_) => return self.err("control character in string"),
                None => return self.err("unterminated string"),
            }
        }
    }
}

impl Wire {
    /// Parses strict JSON (RFC 8259: no `NaN`/`Infinity`, no trailing commas, no
    /// comments, no leading zeros).
    pub fn parse(text: &str) -> std::result::Result<Wire, WireParseError> {
        let mut p = Parser {
            s: text.as_bytes(),
            pos: 0,
        };
        let v = p.value(0)?;
        p.ws();
        if p.pos != p.s.len() {
            return p.err("trailing characters");
        }
        Ok(v)
    }

    /// Field `key` of an object (`None` for non-objects).
    pub fn get(&self, key: &str) -> Option<&Wire> {
        match self {
            Wire::Object(fields) => fields.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// The string, if this is one.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Wire::String(s) => Some(s),
            _ => None,
        }
    }

    /// The fields, if this is an object.
    pub fn as_object(&self) -> Option<&[(String, Wire)]> {
        match self {
            Wire::Object(f) => Some(f),
            _ => None,
        }
    }

    /// The items, if this is an array.
    pub fn as_array(&self) -> Option<&[Wire]> {
        match self {
            Wire::Array(a) => Some(a),
            _ => None,
        }
    }

    /// Is this `null`?
    pub fn is_null(&self) -> bool {
        matches!(self, Wire::Null)
    }

    /// The number token, if this is a number.
    pub fn as_number(&self) -> Option<&str> {
        match self {
            Wire::Number(n) => Some(n),
            _ => None,
        }
    }

    /// Is this a number token without fraction or exponent?
    pub fn is_integer_token(&self) -> bool {
        self.as_number()
            .is_some_and(|n| !n.contains(['.', 'e', 'E']))
    }
}

fn write_json_string(f: &mut fmt::Formatter<'_>, s: &str) -> fmt::Result {
    f.write_str("\"")?;
    for c in s.chars() {
        match c {
            '"' => f.write_str("\\\"")?,
            '\\' => f.write_str("\\\\")?,
            '\n' => f.write_str("\\n")?,
            '\r' => f.write_str("\\r")?,
            '\t' => f.write_str("\\t")?,
            c if (c as u32) < 0x20 => write!(f, "\\u{:04x}", c as u32)?,
            c => write!(f, "{c}")?,
        }
    }
    f.write_str("\"")
}

impl fmt::Display for Wire {
    /// Compact JSON.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Wire::Null => f.write_str("null"),
            Wire::Bool(b) => write!(f, "{b}"),
            Wire::Number(n) => f.write_str(n),
            Wire::String(s) => write_json_string(f, s),
            Wire::Array(items) => {
                f.write_str("[")?;
                for (i, v) in items.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{v}")?;
                }
                f.write_str("]")
            }
            Wire::Object(fields) => {
                f.write_str("{")?;
                for (i, (k, v)) in fields.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write_json_string(f, k)?;
                    write!(f, ": {v}")?;
                }
                f.write_str("}")
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// JSON form of output values (SPEC §9, §10.2)
// ---------------------------------------------------------------------------------------

/// The JSON encoding of a column value: non-finite `f64` as `"NaN"`/`"inf"`/`"-inf"`,
/// `i64` as an integer when `|v| <= 2^53` else a decimal string, timestamps in canonical
/// text form.
pub fn encode_value(v: Option<&Value>) -> Wire {
    match v {
        None => Wire::Null,
        Some(Value::F64(x)) if x.is_nan() => Wire::String("NaN".into()),
        Some(Value::F64(x)) if x.is_infinite() => {
            Wire::String(if *x > 0.0 { "inf" } else { "-inf" }.into())
        }
        Some(Value::F64(x)) => Wire::Number(format!("{x:?}")),
        Some(Value::I64(x)) if x.unsigned_abs() <= 1 << 53 => Wire::Number(x.to_string()),
        Some(Value::I64(x)) => Wire::String(x.to_string()),
        Some(Value::Bool(b)) => Wire::Bool(*b),
        Some(Value::String(s)) => Wire::String(s.clone()),
        Some(Value::Timestamp(t)) => Wire::String(t.to_string()),
    }
}

// ---------------------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------------------

/// The operations of SPEC §9.
pub const OPS: [&str; 9] = [
    "create_table",
    "add_column",
    "create_series",
    "write",
    "latest",
    "history",
    "replay",
    "grid",
    "asof_join",
];

/// What a successful [`execute`] produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// A catalog change succeeded.
    Done,
    /// A write result.
    Write(WriteResult),
    /// Rows of `latest`, `history` or `replay`.
    Versions(ReadResult<VersionRow>),
    /// Rows of `grid`.
    Grid(ReadResult<GridRow>),
    /// Rows of `asof_join`.
    Asof(ReadResult<AsofRow>),
}

/// The request fields of a step: every field except `op` and `expect`.
type Fields = [(String, Wire)];

fn invalid(msg: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidArgument, msg)
}

fn arg(ok: bool, msg: impl FnOnce() -> String) -> Result<()> {
    if ok { Ok(()) } else { Err(invalid(msg())) }
}

/// A field; JSON `null` counts as absent (SPEC §10.3).
fn field<'a>(f: &'a Fields, key: &str) -> Option<&'a Wire> {
    f.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v)
        .filter(|v| !v.is_null())
}

fn check_keys(f: &Fields, allowed: &[&str], what: &str) -> Result<()> {
    for (k, _) in f {
        arg(allowed.contains(&k.as_str()), || {
            format!("unknown field `{k}` in {what}")
        })?;
    }
    Ok(())
}

fn req_string<'a>(f: &'a Fields, key: &str) -> Result<&'a str> {
    field(f, key)
        .and_then(Wire::as_str)
        .ok_or_else(|| invalid(format!("`{key}` must be a string")))
}

fn opt_string<'a>(f: &'a Fields, key: &str) -> Result<Option<&'a str>> {
    match field(f, key) {
        None => Ok(None),
        Some(Wire::String(s)) => Ok(Some(s)),
        Some(v) => Err(invalid(format!("`{key}` must be a string, found {v}"))),
    }
}

fn opt_clock(f: &Fields) -> Result<Option<Clock>> {
    match field(f, "clock") {
        None => Ok(None),
        Some(v) => v
            .as_str()
            .and_then(Clock::parse)
            .map(Some)
            .ok_or_else(|| invalid(format!("bad clock {v}"))),
    }
}

fn column_type(v: Option<&Wire>) -> Result<ColumnType> {
    v.and_then(Wire::as_str)
        .and_then(ColumnType::parse)
        .ok_or_else(|| {
            invalid(format!(
                "bad column type {}",
                v.map_or("(missing)".into(), Wire::to_string)
            ))
        })
}

fn series_list(f: &Fields) -> Result<Vec<String>> {
    match field(f, "series") {
        None => Ok(Vec::new()),
        Some(Wire::Array(items)) => items
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| invalid(format!("series names must be strings, found {v}")))
            })
            .collect(),
        Some(v) => Err(invalid(format!(
            "`series` must be a list of names, found {v}"
        ))),
    }
}

/// A timestamp argument of a read: absent/null = none; anything but a valid §10.1 string
/// is `invalid_timestamp`.
fn opt_time(f: &Fields, key: &str) -> Result<Option<Timestamp>> {
    field(f, key).map(|v| parse_time(v, key)).transpose()
}

fn parse_time(v: &Wire, key: &str) -> Result<Timestamp> {
    match v {
        Wire::String(s) => Timestamp::parse(s)
            .map_err(|e| Error::new(ErrorKind::InvalidTimestamp, format!("{key}: {e}"))),
        other => Err(Error::new(
            ErrorKind::InvalidTimestamp,
            format!("{key}: not a timestamp string: {other}"),
        )),
    }
}

/// A write-side timestamp, passed through for the database to validate.
fn time_input(v: Option<&Wire>) -> TimeInput {
    match v {
        Some(Wire::String(s)) => TimeInput::Text(s.clone()),
        None | Some(Wire::Null) => TimeInput::Invalid("missing".into()),
        Some(other) => TimeInput::Invalid(other.to_string()),
    }
}

fn input_value(v: &Wire) -> InputValue {
    match v {
        Wire::Null => InputValue::Null,
        Wire::Bool(b) => InputValue::Value(Value::Bool(*b)),
        Wire::Number(n) => InputValue::Number(n.clone()),
        Wire::String(s) => InputValue::Value(Value::String(s.clone())),
        other => InputValue::Unsupported(other.to_string()),
    }
}

fn duration_arg(v: &Wire, key: &str) -> Result<Duration> {
    match v {
        Wire::String(s) => Duration::parse(s)
            .map_err(|e| Error::new(ErrorKind::InvalidRange, format!("{key}: {e}"))),
        other => Err(Error::new(
            ErrorKind::InvalidRange,
            format!("{key}: not a duration: {other}"),
        )),
    }
}

/// §10.4 read order after structure: `unknown_table`, then `unknown_series` (names in
/// order). Lookups only; the typed read repeats them.
fn resolve_read<R: Reader + ?Sized>(db: &R, f: &Fields, series: &[String]) -> Result<TableDef> {
    let table = match field(f, "table") {
        Some(Wire::String(t)) => db.table(t)?,
        other => {
            return Err(Error::new(
                ErrorKind::UnknownTable,
                format!(
                    "no table {}",
                    other.map_or("(missing)".into(), Wire::to_string)
                ),
            ));
        }
    };
    for name in series {
        check_series(db, &table, name)?;
    }
    Ok(table)
}

fn check_series<R: Reader + ?Sized>(db: &R, table: &TableDef, name: &str) -> Result<()> {
    let s = db.series(name)?;
    if s.table != table.name {
        return Err(Error::new(
            ErrorKind::UnknownSeries,
            format!(
                "series {name:?} belongs to table {:?}, not {:?}",
                s.table, table.name
            ),
        ));
    }
    Ok(())
}

const READ_KEYS: &[&str] = &[
    "table",
    "series",
    "value_from",
    "value_to",
    "clock",
    "frontier",
    "system",
];
const ROW_KEYS: &[&str] = &[
    "series",
    "value_time",
    "published_time",
    "received_time",
    "deleted",
    "values",
];

/// Executes one JSON request against `db`: `op` is one of [`OPS`], `fields` the request
/// fields (the step without `op` and `expect`). Errors carry the SPEC §8 code that §10.4
/// assigns; an unknown `op` is `invalid_argument`.
pub fn execute<D: Database>(db: &D, op: &str, fields: &Fields) -> Result<Outcome> {
    match op {
        "create_table" => {
            check_keys(
                fields,
                &["name", "columns", "default_clock", "partition"],
                op,
            )?;
            let name = req_string(fields, "name")?;
            let columns = match field(fields, "columns") {
                None => Vec::new(),
                Some(Wire::Array(items)) => items
                    .iter()
                    .map(|c| {
                        let o = c
                            .as_object()
                            .ok_or_else(|| invalid(format!("bad column spec {c}")))?;
                        arg(
                            o.len() == 2 && c.get("name").is_some() && c.get("type").is_some(),
                            || format!("a column spec has exactly `name` and `type`: {c}"),
                        )?;
                        let name = c
                            .get("name")
                            .and_then(Wire::as_str)
                            .ok_or_else(|| invalid(format!("column name must be a string: {c}")))?;
                        Ok(ColumnDef::new(name, column_type(c.get("type"))?))
                    })
                    .collect::<Result<_>>()?,
                Some(v) => return Err(invalid(format!("`columns` must be a list, found {v}"))),
            };
            let default_clock = match field(fields, "default_clock") {
                None => Clock::default(),
                Some(v) => v
                    .as_str()
                    .and_then(Clock::parse)
                    .ok_or_else(|| invalid(format!("bad clock {v}")))?,
            };
            let partition = match field(fields, "partition") {
                None => Partitioning::default(),
                Some(v) => v
                    .as_str()
                    .and_then(Partitioning::parse)
                    .ok_or_else(|| invalid(format!("bad partition {v}")))?,
            };
            let req = CreateTable {
                name: name.to_owned(),
                columns,
                default_clock,
                partition,
                clock: None,
            };
            db.create_table(&req).map(|_| Outcome::Done)
        }
        "add_column" => {
            check_keys(fields, &["table", "name", "type"], op)?;
            let table = req_string(fields, "table")?;
            let name = req_string(fields, "name")?;
            let ty = column_type(field(fields, "type"))?;
            let req = AddColumn {
                table: table.to_owned(),
                column: ColumnDef::new(name, ty),
                clock: None,
            };
            db.add_column(&req).map(|_| Outcome::Done)
        }
        "create_series" => {
            check_keys(
                fields,
                &["name", "table", "tags", "timezone", "resolution"],
                op,
            )?;
            let name = req_string(fields, "name")?;
            let table = req_string(fields, "table")?;
            let mut tags = BTreeMap::new();
            match field(fields, "tags") {
                None => {}
                Some(Wire::Object(o)) => {
                    for (k, v) in o {
                        let v = v.as_str().ok_or_else(|| {
                            invalid(format!("tag values must be strings: {k} = {v}"))
                        })?;
                        tags.insert(k.clone(), v.to_owned());
                    }
                }
                Some(v) => return Err(invalid(format!("`tags` must be an object, found {v}"))),
            }
            let timezone = opt_string(fields, "timezone")?.map(str::to_owned);
            let resolution = match field(fields, "resolution") {
                None => None,
                Some(v) => Some(
                    v.as_str()
                        .and_then(|s| Duration::parse(s).ok())
                        .ok_or_else(|| invalid(format!("bad resolution {v}")))?,
                ),
            };
            let req = CreateSeries {
                name: name.to_owned(),
                table: table.to_owned(),
                tags,
                timezone,
                resolution,
                clock: None,
            };
            db.create_series(&req).map(|_| Outcome::Done)
        }
        "write" => {
            check_keys(fields, &["system_time", "batch_id", "rows"], op)?;
            let batch_id = match field(fields, "batch_id") {
                None => None,
                Some(Wire::String(s)) if !s.is_empty() => Some(s.clone()),
                Some(v) => {
                    return Err(invalid(format!(
                        "batch_id must be a non-empty string, found {v}"
                    )));
                }
            };
            let rows = match field(fields, "rows") {
                None => Vec::new(),
                Some(Wire::Array(rows)) => rows.iter().map(write_row).collect::<Result<_>>()?,
                Some(v) => return Err(invalid(format!("`rows` must be a list, found {v}"))),
            };
            let batch = WriteBatch {
                rows,
                batch_id,
                clock: time_input(field(fields, "system_time")),
            };
            db.write(&batch).map(Outcome::Write)
        }
        "latest" | "history" => {
            check_keys(fields, READ_KEYS, op)?;
            let clock = opt_clock(fields)?;
            let series = series_list(fields)?;
            let table = resolve_read(db, fields, &series)?;
            let range = TimeRange::new(
                opt_time(fields, "value_from")?,
                opt_time(fields, "value_to")?,
            );
            let context = ReadContext {
                clock,
                frontier: opt_time(fields, "frontier")?,
                system: opt_time(fields, "system")?,
            };
            let req = LatestRequest {
                table: table.name,
                series,
                range,
                context,
            };
            if op == "latest" {
                db.latest(&req)
            } else {
                db.history(&req)
            }
            .map(Outcome::Versions)
        }
        "replay" => {
            check_keys(
                fields,
                &[
                    "table",
                    "series",
                    "value_from",
                    "value_to",
                    "clock",
                    "system",
                    "from",
                    "to",
                ],
                op,
            )?;
            let clock = opt_clock(fields)?;
            let series = series_list(fields)?;
            let table = resolve_read(db, fields, &series)?;
            let range = TimeRange::new(
                opt_time(fields, "value_from")?,
                opt_time(fields, "value_to")?,
            );
            let system = opt_time(fields, "system")?;
            let arrival = TimeRange::new(opt_time(fields, "from")?, opt_time(fields, "to")?);
            let req = ReplayRequest {
                table: table.name,
                series,
                range,
                clock,
                system,
                arrival,
            };
            db.replay(&req).map(Outcome::Versions)
        }
        "grid" => {
            check_keys(
                fields,
                &[
                    "table",
                    "series",
                    "value_from",
                    "value_to",
                    "clock",
                    "system",
                    "frontiers",
                    "window_start",
                    "window_end",
                ],
                op,
            )?;
            let clock = opt_clock(fields)?;
            let series = series_list(fields)?;
            let frontiers = match field(fields, "frontiers") {
                Some(Wire::Array(items)) => items,
                other => {
                    return Err(invalid(format!(
                        "`frontiers` must be a list, found {}",
                        other.map_or("(missing)".into(), Wire::to_string)
                    )));
                }
            };
            let table = resolve_read(db, fields, &series)?;
            let range = TimeRange::new(
                opt_time(fields, "value_from")?,
                opt_time(fields, "value_to")?,
            );
            let system = opt_time(fields, "system")?;
            let frontiers = frontiers
                .iter()
                .map(|f| parse_time(f, "frontiers"))
                .collect::<Result<Vec<_>>>()?;
            // §10.5 window rules.
            let absolute = range.from.is_some() || range.to.is_some();
            let (ws, we) = (field(fields, "window_start"), field(fields, "window_end"));
            let window = match (ws, we) {
                (None, None) => GridWindow::Absolute(range),
                _ if absolute => {
                    return Err(Error::new(
                        ErrorKind::InvalidRange,
                        "grid window is either absolute or relative, not both",
                    ));
                }
                (Some(ws), Some(we)) => GridWindow::Relative {
                    start: duration_arg(ws, "window_start")?,
                    end: duration_arg(we, "window_end")?,
                },
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidRange,
                        "a relative window needs both window_start and window_end",
                    ));
                }
            };
            let req = GridRequest {
                table: table.name,
                series,
                frontiers,
                window,
                clock,
                system,
            };
            db.grid(&req).map(Outcome::Grid)
        }
        "asof_join" => {
            check_keys(fields, &["table", "clock", "system", "left"], op)?;
            let clock = opt_clock(fields)?;
            let left = match field(fields, "left") {
                Some(Wire::Array(items)) => items,
                other => {
                    return Err(invalid(format!(
                        "`left` must be a list, found {}",
                        other.map_or("(missing)".into(), Wire::to_string)
                    )));
                }
            };
            for l in left {
                let o = l
                    .as_object()
                    .ok_or_else(|| invalid(format!("left rows must be objects, found {l}")))?;
                check_keys(o, &["series", "value_time", "frontier"], "a left row")?;
                arg(l.get("series").and_then(Wire::as_str).is_some(), || {
                    format!("left row series must be a string: {l}")
                })?;
            }
            let table = resolve_read(db, fields, &[])?;
            for l in left {
                check_series(
                    db,
                    &table,
                    l.get("series").and_then(Wire::as_str).expect("checked"),
                )?;
            }
            let system = opt_time(fields, "system")?;
            let mut rows = Vec::with_capacity(left.len());
            for l in left {
                let none = Wire::Null;
                rows.push(AsofLeftRow {
                    series: l
                        .get("series")
                        .and_then(Wire::as_str)
                        .expect("checked")
                        .to_owned(),
                    value_time: parse_time(l.get("value_time").unwrap_or(&none), "value_time")?,
                    frontier: parse_time(l.get("frontier").unwrap_or(&none), "frontier")?,
                });
            }
            let req = AsofJoinRequest {
                table: table.name,
                clock,
                system,
                left: rows,
            };
            db.asof_join(&req).map(Outcome::Asof)
        }
        other => Err(invalid(format!("unknown op {other:?}"))),
    }
}

/// §10.3 structure of one write row, then conversion to loose typed inputs.
fn write_row(row: &Wire) -> Result<WriteRow> {
    let o = row
        .as_object()
        .ok_or_else(|| invalid(format!("a row must be an object, found {row}")))?;
    check_keys(o, ROW_KEYS, "a write row")?;
    let series = row
        .get("series")
        .and_then(Wire::as_str)
        .ok_or_else(|| invalid(format!("row series must be a string: {row}")))?;
    let values = match row.get("values") {
        None | Some(Wire::Null) => BTreeMap::new(),
        Some(Wire::Object(m)) => m.iter().map(|(k, v)| (k.clone(), input_value(v))).collect(),
        Some(v) => return Err(invalid(format!("`values` must be an object, found {v}"))),
    };
    // `deleted` is the one field where null is not the same as absent (§10.3).
    let deleted = match row.get("deleted") {
        None => false,
        Some(Wire::Bool(b)) => *b,
        Some(v) => return Err(invalid(format!("`deleted` must be a bool, found {v}"))),
    };
    let published_time = match row.get("published_time") {
        None | Some(Wire::Null) => None,
        v => Some(time_input(v)),
    };
    Ok(WriteRow {
        series: series.to_owned(),
        value_time: time_input(row.get("value_time")),
        published_time,
        received_time: time_input(row.get("received_time")),
        deleted,
        values,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_display() {
        let v = Wire::parse(
            r#" {"a": [1, -0, 2.50, 1e3, true, null, "x\"\u00e9\ud83d\ude00"], "a": 2, "b": {}} "#,
        )
        .unwrap();
        assert_eq!(
            v.get("a"),
            Some(&Wire::Number("2".into())),
            "last duplicate wins"
        );
        assert_eq!(v.as_object().unwrap().len(), 2);
        let w =
            Wire::parse(r#"[1, -0, 2.50, 1e3, true, null, "x\"\u00e9\ud83d\ude00\n"]"#).unwrap();
        assert_eq!(
            w.to_string(),
            "[1, -0, 2.50, 1e3, true, null, \"x\\\"é😀\\n\"]"
        );
        assert_eq!(Wire::parse(&w.to_string()).unwrap(), w);
        for bad in [
            "",
            "[1,]",
            "{\"a\":1,}",
            "01",
            "-",
            "1.",
            ".5",
            "1e",
            "NaN",
            "Infinity",
            "[1] x",
            "\"\\x\"",
            "\"a\nb\"",
            "{1: 2}",
            "\"\\ud800\"",
            "tru",
            "+1",
        ] {
            assert!(Wire::parse(bad).is_err(), "{bad:?}");
        }
        assert!(Wire::parse(&"[".repeat(1000)).is_err());
    }

    #[test]
    fn value_encoding() {
        assert_eq!(encode_value(Some(&Value::F64(-0.0))).to_string(), "-0.0");
        assert_eq!(
            encode_value(Some(&Value::F64(f64::NAN))).to_string(),
            "\"NaN\""
        );
        assert_eq!(
            encode_value(Some(&Value::I64(1 << 53))).to_string(),
            "9007199254740992"
        );
        assert_eq!(
            encode_value(Some(&Value::I64((1 << 53) + 1))).to_string(),
            "\"9007199254740993\""
        );
        assert_eq!(
            encode_value(Some(&Value::I64(i64::MIN))).to_string(),
            "\"-9223372036854775808\""
        );
        assert_eq!(encode_value(None).to_string(), "null");
    }
}
