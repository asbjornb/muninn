//! Shared vocabulary: column types and values (SPEC §2.1, §4.1), catalog definitions
//! (§2), versions (§3), content identity (§4.3), clocks, visibility and knowledge order
//! (§5), value ranges (§6).
//!
//! The visibility and ordering rules are small pure functions so that the storage engine
//! can reuse exactly the code the model is checked with.

use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::error::{Error, ErrorKind, Result};
use crate::time::{Duration, Timestamp};

// ---------------------------------------------------------------------------------------
// Column types and values
// ---------------------------------------------------------------------------------------

/// A value column type (SPEC §2.1). All value columns are nullable.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ColumnType {
    /// IEEE-754 double; NaN and ±inf are legal values.
    F64,
    /// Signed 64-bit integer.
    I64,
    /// Boolean.
    Bool,
    /// UTF-8 string.
    String,
    /// A [`Timestamp`] (µs UTC).
    Timestamp,
}

impl ColumnType {
    /// The spec name: `f64`, `i64`, `bool`, `string`, `timestamp`.
    pub const fn name(self) -> &'static str {
        match self {
            ColumnType::F64 => "f64",
            ColumnType::I64 => "i64",
            ColumnType::Bool => "bool",
            ColumnType::String => "string",
            ColumnType::Timestamp => "timestamp",
        }
    }

    /// Parses a spec name; `None` if unknown.
    pub fn parse(s: &str) -> Option<ColumnType> {
        Some(match s {
            "f64" => ColumnType::F64,
            "i64" => ColumnType::I64,
            "bool" => ColumnType::Bool,
            "string" => ColumnType::String,
            "timestamp" => ColumnType::Timestamp,
            _ => return None,
        })
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A non-null value. Null is represented as `Option<Value>::None` everywhere.
///
/// **Equality is content identity (SPEC §4.3):** `F64` compares *bitwise*, so a NaN equals
/// a NaN with the same bit pattern and `-0.0 != 0.0`. That makes `Value` `Eq` and `Hash`
/// consistently. Values of different variants are never equal.
///
/// On input (writes), a value is *coerced* to its column's type with
/// [`Value::coerce_to`], which implements the SPEC §4.1 step 3 acceptance rules (e.g. an
/// `I64` into an `f64` column, a decimal `String` into an `i64` column). Stored and
/// returned values always have exactly their column's type.
#[derive(Clone, Debug)]
pub enum Value {
    /// An `f64` value.
    F64(f64),
    /// An `i64` value.
    I64(i64),
    /// A `bool` value.
    Bool(bool),
    /// A `string` value.
    String(String),
    /// A `timestamp` value.
    Timestamp(Timestamp),
}

impl PartialEq for Value {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(),
            (Value::I64(a), Value::I64(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::F64(x) => x.to_bits().hash(state),
            Value::I64(x) => x.hash(state),
            Value::Bool(x) => x.hash(state),
            Value::String(x) => x.hash(state),
            Value::Timestamp(x) => x.hash(state),
        }
    }
}

impl fmt::Display for Value {
    /// Human-readable form; non-finite floats print as the spec strings `NaN`, `inf`, `-inf`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::F64(x) if x.is_nan() => f.write_str("NaN"),
            Value::F64(x) if x.is_infinite() => f.write_str(if *x > 0.0 { "inf" } else { "-inf" }),
            Value::F64(x) => write!(f, "{x:?}"),
            Value::I64(x) => write!(f, "{x}"),
            Value::Bool(x) => write!(f, "{x}"),
            Value::String(x) => write!(f, "{x:?}"),
            Value::Timestamp(x) => write!(f, "{x}"),
        }
    }
}

impl Value {
    /// The natural column type of this value (no coercion).
    pub fn column_type(&self) -> ColumnType {
        match self {
            Value::F64(_) => ColumnType::F64,
            Value::I64(_) => ColumnType::I64,
            Value::Bool(_) => ColumnType::Bool,
            Value::String(_) => ColumnType::String,
            Value::Timestamp(_) => ColumnType::Timestamp,
        }
    }

    /// Coerces an input value to column type `ty` per SPEC §4.1 step 3, or fails with
    /// `type_mismatch`.
    ///
    /// | column | accepts |
    /// | --- | --- |
    /// | `f64` | `F64`; `I64` (converted, round-to-nearest); `String` `"NaN"`, `"inf"`, `"-inf"` |
    /// | `i64` | `I64`; `String` matching `-?[0-9]+` within i64 range |
    /// | `bool` | `Bool` |
    /// | `string` | `String` |
    /// | `timestamp` | `Timestamp`; `String` in the SPEC §1 text form (incl. range) |
    ///
    /// `F64` into an `i64` column is always a mismatch, even when integral (`2.0`): JSON
    /// readers distinguish integer and float literals, and `1.5` is called out as a
    /// mismatch. See `crates/SPEC-QUESTIONS.md`.
    pub fn coerce_to(&self, ty: ColumnType) -> Result<Value> {
        let ok = match (ty, self) {
            (ColumnType::F64, Value::F64(x)) => Some(Value::F64(*x)),
            (ColumnType::F64, Value::I64(n)) => Some(Value::F64(*n as f64)),
            (ColumnType::F64, Value::String(s)) => match s.as_str() {
                "NaN" => Some(Value::F64(f64::NAN)),
                "inf" => Some(Value::F64(f64::INFINITY)),
                "-inf" => Some(Value::F64(f64::NEG_INFINITY)),
                _ => None,
            },
            (ColumnType::I64, Value::I64(n)) => Some(Value::I64(*n)),
            (ColumnType::I64, Value::String(s)) => parse_decimal_i64(s).map(Value::I64),
            (ColumnType::Bool, Value::Bool(b)) => Some(Value::Bool(*b)),
            (ColumnType::String, Value::String(s)) => Some(Value::String(s.clone())),
            (ColumnType::Timestamp, Value::Timestamp(t)) => Some(Value::Timestamp(*t)),
            (ColumnType::Timestamp, Value::String(s)) => {
                Timestamp::parse(s).ok().map(Value::Timestamp)
            }
            _ => None,
        };
        ok.ok_or_else(|| {
            Error::new(
                ErrorKind::TypeMismatch,
                format!("value {self} does not match column type {ty}"),
            )
        })
    }
}

/// A write input value as a client sent it, before SPEC §4.1 step 3 coercion.
///
/// Typed Rust callers use [`InputValue::Value`]; JSON front ends (the conformance runner,
/// bindings, servers) use [`InputValue::Number`] for number tokens — so that `-0`, big
/// integers and `2.0` keep the meaning SPEC §10.2 gives the exact token — and
/// [`InputValue::Unsupported`] for JSON shapes that are never a value (arrays, objects),
/// which coerce to nothing (`type_mismatch`).
#[derive(Clone, Debug, PartialEq)]
pub enum InputValue {
    /// JSON `null` / absent: the column is null.
    Null,
    /// A typed value, coerced with [`Value::coerce_to`].
    Value(Value),
    /// A JSON number token, exactly as written (e.g. `"-0"`, `"2.0"`, `"1e3"`,
    /// `"18446744073709551616"`).
    Number(String),
    /// Any other JSON shape (array, object); the text is for error messages only.
    Unsupported(String),
}

impl From<Value> for InputValue {
    fn from(v: Value) -> InputValue {
        InputValue::Value(v)
    }
}

impl From<Option<Value>> for InputValue {
    fn from(v: Option<Value>) -> InputValue {
        v.map_or(InputValue::Null, InputValue::Value)
    }
}

impl InputValue {
    /// Coerces to column type `ty` per SPEC §4.1 step 3 / §10.2: `Ok(None)` for null,
    /// `Err(type_mismatch)` if the value is not acceptable for the column.
    pub fn coerce_to(&self, ty: ColumnType) -> Result<Option<Value>> {
        let mismatch = |what: &str| {
            Err(Error::new(
                ErrorKind::TypeMismatch,
                format!("value {what} does not match column type {ty}"),
            ))
        };
        match self {
            InputValue::Null => Ok(None),
            InputValue::Value(v) => v.coerce_to(ty).map(Some),
            InputValue::Unsupported(text) => mismatch(text),
            InputValue::Number(tok) => {
                let integer_token = !tok.contains(['.', 'e', 'E']);
                match ty {
                    ColumnType::F64 => {
                        // §10.2: the integer token -0 denotes -0.0.
                        if tok == "-0" {
                            return Ok(Some(Value::F64(-0.0)));
                        }
                        match tok.parse::<f64>() {
                            // An integer token too large for f64 is a mismatch; a
                            // float token like 1e400 is +inf, as JSON readers give it.
                            Ok(x) if integer_token && x.is_infinite() => mismatch(tok),
                            Ok(x) => Ok(Some(Value::F64(x))),
                            Err(_) => mismatch(tok),
                        }
                    }
                    // §10.2: only tokens without fraction or exponent, in i64 range.
                    ColumnType::I64 if integer_token => match parse_decimal_i64(tok) {
                        Some(n) => Ok(Some(Value::I64(n))),
                        None => mismatch(tok),
                    },
                    _ => mismatch(tok),
                }
            }
        }
    }
}

/// Parses `-?[0-9]+` into an `i64` (no `+`, no whitespace, no fraction).
fn parse_decimal_i64(s: &str) -> Option<i64> {
    let digits = s.strip_prefix('-').unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// Bitwise content identity of two optional values (null equals null; SPEC §4.3).
pub fn values_identical(a: Option<&Value>, b: Option<&Value>) -> bool {
    a == b
}

/// Content identity of two value vectors where a missing trailing entry is null (a column
/// added after a version was written reads, and compares, as null — SPEC §2.1, §4.3).
pub fn value_lists_identical(a: &[Option<Value>], b: &[Option<Value>]) -> bool {
    let n = a.len().max(b.len());
    (0..n).all(|i| {
        values_identical(
            a.get(i).and_then(Option::as_ref),
            b.get(i).and_then(Option::as_ref),
        )
    })
}

// ---------------------------------------------------------------------------------------
// Names (SPEC §2.1, §2.2)
// ---------------------------------------------------------------------------------------

/// Column names that MUST NOT be used (SPEC §2.1): they are the fixed output fields.
pub const RESERVED_COLUMN_NAMES: [&str; 10] = [
    "series",
    "value_time",
    "published_time",
    "received_time",
    "system_time",
    "seq",
    "deleted",
    "frontier",
    "found",
    "values",
];

/// Does `name` match `[a-z][a-z0-9_]*`?
pub fn is_identifier(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && bytes.all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

/// Validates a table name: `[a-z][a-z0-9_]*` (reserved names are not banned for tables).
pub fn validate_table_name(name: &str) -> Result<()> {
    if is_identifier(name) {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::InvalidName,
            format!("table name {name:?} must match [a-z][a-z0-9_]*"),
        ))
    }
}

/// Validates a column name: `[a-z][a-z0-9_]*` and not in [`RESERVED_COLUMN_NAMES`].
pub fn validate_column_name(name: &str) -> Result<()> {
    if !is_identifier(name) {
        return Err(Error::new(
            ErrorKind::InvalidName,
            format!("column name {name:?} must match [a-z][a-z0-9_]*"),
        ));
    }
    if RESERVED_COLUMN_NAMES.contains(&name) {
        return Err(Error::new(
            ErrorKind::InvalidName,
            format!("column name {name:?} is reserved"),
        ));
    }
    Ok(())
}

/// Validates a series name: any non-empty string (SPEC §2.2).
pub fn validate_series_name(name: &str) -> Result<()> {
    if name.is_empty() {
        Err(Error::new(
            ErrorKind::InvalidName,
            "series name must be non-empty",
        ))
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------
// Catalog (SPEC §2)
// ---------------------------------------------------------------------------------------

/// Which of the three clocks a read uses (SPEC §5).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum Clock {
    /// `t_received(v) = v.received_time`.
    #[default]
    Received,
    /// `t_published(v) = v.published_time`, falling back to `received_time` when null.
    Published,
    /// `t_system(v) = v.system_time`.
    System,
}

impl Clock {
    /// The spec name: `received`, `published`, `system`.
    pub const fn name(self) -> &'static str {
        match self {
            Clock::Received => "received",
            Clock::Published => "published",
            Clock::System => "system",
        }
    }

    /// Parses a spec name; `None` if unknown.
    pub fn parse(s: &str) -> Option<Clock> {
        Some(match s {
            "received" => Clock::Received,
            "published" => Clock::Published,
            "system" => Clock::System,
            _ => return None,
        })
    }
}

impl fmt::Display for Clock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Physical partition granularity (SPEC §2.1). Never affects a result.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum Partitioning {
    /// One partition per value day (default).
    #[default]
    Day,
    /// One partition per value month.
    Month,
    /// One partition per value year.
    Year,
}

impl Partitioning {
    /// The spec name: `day`, `month`, `year`.
    pub const fn name(self) -> &'static str {
        match self {
            Partitioning::Day => "day",
            Partitioning::Month => "month",
            Partitioning::Year => "year",
        }
    }

    /// Parses a spec name; `None` if unknown.
    pub fn parse(s: &str) -> Option<Partitioning> {
        Some(match s {
            "day" => Partitioning::Day,
            "month" => Partitioning::Month,
            "year" => Partitioning::Year,
            _ => return None,
        })
    }
}

impl fmt::Display for Partitioning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A value column definition.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColumnDef {
    /// Column name (validated by [`validate_column_name`]).
    pub name: String,
    /// Column type.
    pub ty: ColumnType,
}

impl ColumnDef {
    /// Convenience constructor.
    pub fn new(name: impl Into<String>, ty: ColumnType) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            ty,
        }
    }
}

/// A table definition (SPEC §2.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableDef {
    /// Table name, unique.
    pub name: String,
    /// Ordered value columns; schema evolution only appends.
    pub columns: Vec<ColumnDef>,
    /// Clock used by reads that do not name one.
    pub default_clock: Clock,
    /// Physical partitioning (no semantic effect).
    pub partition: Partitioning,
}

impl TableDef {
    /// Index of the column called `name`, if any.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

/// A series id: 1, 2, 3, … in creation order, never reused (SPEC §2.2).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SeriesId(pub u32);

impl fmt::Display for SeriesId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// A series definition (SPEC §2.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeriesDef {
    /// Engine-assigned id.
    pub id: SeriesId,
    /// Name, unique across the whole database.
    pub name: Arc<str>,
    /// Owning table (fixed for life).
    pub table: String,
    /// Descriptive tags (no semantics).
    pub tags: BTreeMap<String, String>,
    /// IANA timezone name; metadata only in v0.1.
    pub timezone: String,
    /// Nominal resolution, `None` = irregular; metadata only in v0.1.
    pub resolution: Option<Duration>,
}

// ---------------------------------------------------------------------------------------
// Versions (SPEC §3) and content identity (SPEC §4.3)
// ---------------------------------------------------------------------------------------

/// One immutable version of a point `(series, value_time)` — every field of SPEC §3.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version {
    /// The series.
    pub series: SeriesId,
    /// What moment the values are about.
    pub value_time: Timestamp,
    /// When it became public; `None` = unknown.
    pub published_time: Option<Timestamp>,
    /// When we got it (required).
    pub received_time: Timestamp,
    /// Tombstone flag; a tombstone has every value null.
    pub deleted: bool,
    /// One entry per table column *at write time*, in column order; a shorter vector
    /// means later-added columns, which read as null.
    pub values: Vec<Option<Value>>,
    /// Engine-assigned commit time (SPEC §4.2).
    pub system_time: Timestamp,
    /// Engine-assigned global sequence number, unique and strictly increasing.
    pub seq: u64,
}

impl Version {
    /// `t_clock(v)` for this version (SPEC §5).
    pub fn t_clock(&self, clock: Clock) -> Timestamp {
        t_clock(
            clock,
            self.published_time,
            self.received_time,
            self.system_time,
        )
    }

    /// The knowledge-order key `k_clock(v)` (SPEC §5).
    pub fn knowledge_key(&self, clock: Clock) -> KnowledgeKey {
        KnowledgeKey {
            t_clock: self.t_clock(clock),
            system_time: self.system_time,
            seq: self.seq,
        }
    }

    /// Value of column `i`, null if the column did not exist when this was written.
    pub fn value(&self, i: usize) -> Option<&Value> {
        self.values.get(i).and_then(Option::as_ref)
    }

    /// SPEC §4.3 content identity: equal `series`, `value_time`, `published_time`,
    /// `received_time`, `deleted` and every value (bitwise `f64`, missing = null).
    /// `system_time` and `seq` are deliberately ignored.
    pub fn content_identical(&self, other: &Version) -> bool {
        self.series == other.series
            && self.value_time == other.value_time
            && self.published_time == other.published_time
            && self.received_time == other.received_time
            && self.deleted == other.deleted
            && value_lists_identical(&self.values, &other.values)
    }

    /// A 64-bit hash of exactly the fields [`Version::content_identical`] compares:
    /// content-identical versions have equal hashes (`f64` by bit pattern, trailing nulls
    /// and missing trailing values hash alike), so duplicate detection can look a version
    /// up by hash and compare only on a hash match. Not stable across releases; never
    /// persisted.
    pub fn content_hash(&self) -> u64 {
        let mut h = ContentHasher::default();
        h.add(u64::from(self.series.0));
        h.add(self.value_time.as_micros() as u64);
        match self.published_time {
            Some(t) => {
                h.add(1);
                h.add(t.as_micros() as u64);
            }
            None => h.add(0),
        }
        h.add(self.received_time.as_micros() as u64);
        h.add(u64::from(self.deleted));
        let n = self
            .values
            .iter()
            .rposition(Option::is_some)
            .map_or(0, |i| i + 1);
        for v in &self.values[..n] {
            match v {
                None => h.add(0),
                Some(Value::F64(x)) => {
                    h.add(1);
                    h.add(x.to_bits());
                }
                Some(Value::I64(x)) => {
                    h.add(2);
                    h.add(*x as u64);
                }
                Some(Value::Bool(x)) => h.add(3 + u64::from(*x)),
                Some(Value::String(s)) => {
                    h.add(5);
                    h.add(s.len() as u64);
                    let mut chunks = s.as_bytes().chunks_exact(8);
                    for c in &mut chunks {
                        h.add(u64::from_le_bytes(c.try_into().expect("8 bytes")));
                    }
                    let mut rest = [0u8; 8];
                    let r = chunks.remainder();
                    rest[..r.len()].copy_from_slice(r);
                    h.add(u64::from_le_bytes(rest));
                }
                Some(Value::Timestamp(t)) => {
                    h.add(6);
                    h.add(t.as_micros() as u64);
                }
            }
        }
        h.finish()
    }
}

/// The word-at-a-time multiply-rotate hash behind [`Version::content_hash`], with a final
/// avalanche so that the result can index a hash table directly.
#[derive(Default)]
struct ContentHasher(u64);

impl ContentHasher {
    #[inline]
    fn add(&mut self, x: u64) {
        self.0 = (self.0.rotate_left(26) ^ x).wrapping_mul(0xf135_7aea_2e62_a9c5);
    }

    #[inline]
    fn finish(&self) -> u64 {
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// A [`std::hash::Hasher`] for keys that already are well-mixed `u64` hashes (such as
/// [`Version::content_hash`]): it passes them through.
#[derive(Clone, Copy, Default)]
pub struct PrehashedHasher(u64);

impl std::hash::Hasher for PrehashedHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.rotate_left(8) ^ u64::from(b)).wrapping_mul(0xf135_7aea_2e62_a9c5);
        }
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

/// A hash map keyed by prehashed `u64`s (see [`PrehashedHasher`]).
pub type PrehashedMap<V> =
    std::collections::HashMap<u64, V, std::hash::BuildHasherDefault<PrehashedHasher>>;

/// An index of versions by content (SPEC §4.3): `find` returns a stored item
/// content-identical to a version in expected O(1), whatever the number of versions per
/// point. Items are indices into a caller-owned slice; equal hashes are chained.
#[derive(Default)]
pub struct ContentIndex {
    first: PrehashedMap<u32>,
    /// `next[i]`: the next item with the same hash as item `i`, or `u32::MAX`.
    next: Vec<u32>,
}

impl ContentIndex {
    /// An empty index with room for `n` items.
    pub fn with_capacity(n: usize) -> ContentIndex {
        ContentIndex {
            first: PrehashedMap::with_capacity_and_hasher(n, Default::default()),
            next: Vec::with_capacity(n),
        }
    }

    /// Number of items.
    pub fn len(&self) -> usize {
        self.next.len()
    }

    /// No items?
    pub fn is_empty(&self) -> bool {
        self.next.is_empty()
    }

    /// Adds the next item (index `self.len()` of the caller's slice) with content hash
    /// `hash`.
    pub fn push(&mut self, hash: u64) {
        let i = u32::try_from(self.next.len()).expect("fewer than 2^32 items");
        let prev = self.first.insert(hash, i).unwrap_or(u32::MAX);
        self.next.push(prev);
    }

    /// The index of an item of `items` (the caller's slice this index was built over)
    /// content-identical to `v`, whose content hash is `hash`.
    pub fn find(&self, items: &[Version], v: &Version, hash: u64) -> Option<usize> {
        self.find_with(hash, |i| items[i].content_identical(v))
    }

    /// The first item (most recently pushed first) with content hash `hash` for which
    /// `same(item)` holds: [`ContentIndex::find`] for items the caller does not keep as
    /// a slice of versions (`same` compares item `i` with the version looked up).
    pub fn find_with(&self, hash: u64, mut same: impl FnMut(usize) -> bool) -> Option<usize> {
        let mut at = self.first.get(&hash).copied().unwrap_or(u32::MAX);
        while at != u32::MAX {
            let i = at as usize;
            if same(i) {
                return Some(i);
            }
            at = self.next[i];
        }
        None
    }
}

// ---------------------------------------------------------------------------------------
// Clocks, visibility and knowledge order (SPEC §5)
// ---------------------------------------------------------------------------------------

/// `t_clock(v)` (SPEC §5): received → `received_time`; published → `published_time`, or
/// `received_time` if unknown; system → `system_time`.
pub fn t_clock(
    clock: Clock,
    published_time: Option<Timestamp>,
    received_time: Timestamp,
    system_time: Timestamp,
) -> Timestamp {
    match clock {
        Clock::Received => received_time,
        Clock::Published => published_time.unwrap_or(received_time),
        Clock::System => system_time,
    }
}

/// The knowledge-order key `k_clock(v) = (t_clock(v), system_time, seq)` (SPEC §5).
/// The derived `Ord` is lexicographic in field order, which is exactly the spec order.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KnowledgeKey {
    /// `t_clock(v)`.
    pub t_clock: Timestamp,
    /// `v.system_time`.
    pub system_time: Timestamp,
    /// `v.seq` (unique, so the order is total).
    pub seq: u64,
}

/// A resolved read context (SPEC §5): a clock and two inclusive upper bounds.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Visibility {
    /// The clock that both filters (`frontier`) and orders versions.
    pub clock: Clock,
    /// Frontier `F` on `t_clock`, inclusive; `None` = unbounded.
    pub frontier: Option<Timestamp>,
    /// System bound `S` on `system_time`, inclusive; `None` = unbounded.
    pub system: Option<Timestamp>,
}

impl Visibility {
    /// Unbounded context under `clock`.
    pub fn unbounded(clock: Clock) -> Visibility {
        Visibility {
            clock,
            frontier: None,
            system: None,
        }
    }

    /// Is a version with these clock readings visible? (`system_time <= S` and
    /// `t_clock <= F`, both inclusive.)
    pub fn admits(&self, t_clock: Timestamp, system_time: Timestamp) -> bool {
        within(Some(system_time), self.system) && within(Some(t_clock), self.frontier)
    }

    /// [`Visibility::admits`] for a whole version.
    pub fn admits_version(&self, v: &Version) -> bool {
        self.admits(v.t_clock(self.clock), v.system_time)
    }
}

fn within(t: Option<Timestamp>, bound: Option<Timestamp>) -> bool {
    match (t, bound) {
        (Some(t), Some(b)) => t <= b,
        _ => true,
    }
}

// ---------------------------------------------------------------------------------------
// Ranges (SPEC §6)
// ---------------------------------------------------------------------------------------

/// A half-open timestamp range `[from, to)`; either bound may be omitted (unbounded).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct TimeRange {
    /// Inclusive lower bound; `None` = unbounded.
    pub from: Option<Timestamp>,
    /// Exclusive upper bound; `None` = unbounded.
    pub to: Option<Timestamp>,
}

impl TimeRange {
    /// The unbounded range.
    pub const ALL: TimeRange = TimeRange {
        from: None,
        to: None,
    };

    /// A range with the given optional bounds (not validated; see [`TimeRange::validate`]).
    pub fn new(from: Option<Timestamp>, to: Option<Timestamp>) -> TimeRange {
        TimeRange { from, to }
    }

    /// `from > to` is `invalid_range` (SPEC §6); equal bounds are a valid empty range.
    pub fn validate(&self) -> Result<()> {
        match (self.from, self.to) {
            (Some(f), Some(t)) if f > t => Err(Error::new(
                ErrorKind::InvalidRange,
                format!("range start {f} is after range end {t}"),
            )),
            _ => Ok(()),
        }
    }

    /// Does the range contain `t` (`from <= t < to`)? This is the value-range reading
    /// of SPEC §6.
    pub fn contains(&self, t: Timestamp) -> bool {
        self.from.is_none_or(|f| f <= t) && self.to.is_none_or(|e| t < e)
    }

    /// Does the open-closed interval `(from, to]` contain `t`? This is the reading of
    /// `replay`'s arrival bounds (SPEC §6.3), matching inclusive frontiers.
    pub fn contains_open_closed(&self, t: Timestamp) -> bool {
        self.from.is_none_or(|f| f < t) && self.to.is_none_or(|e| t <= e)
    }

    /// The SPEC §6.4 relative window `[frontier + start, frontier + end)`.
    ///
    /// Fails with `invalid_range` if `start > end`. Bounds that fall outside the valid
    /// timestamp range are normalised without changing which valid timestamps the range
    /// contains: a lower bound `<= MIN` becomes unbounded, an upper bound `> MAX` becomes
    /// unbounded, and a window entirely above `MAX` becomes the empty `[MAX, MAX)`.
    pub fn relative_window(
        frontier: Timestamp,
        start: Duration,
        end: Duration,
    ) -> Result<TimeRange> {
        if start > end {
            return Err(Error::new(
                ErrorKind::InvalidRange,
                format!("window start {start} is after window end {end}"),
            ));
        }
        let min = i128::from(Timestamp::MIN.as_micros());
        let max = i128::from(Timestamp::MAX.as_micros());
        let f = i128::from(frontier.as_micros());
        let lo = f + i128::from(start.as_micros());
        let hi = f + i128::from(end.as_micros());
        if lo > max {
            return Ok(TimeRange::new(Some(Timestamp::MAX), Some(Timestamp::MAX)));
        }
        let clamp = |x: i128| Timestamp::from_micros(x.clamp(min, max) as i64);
        let from = if lo <= min { None } else { clamp(lo) };
        let to = if hi > max { None } else { clamp(hi) };
        Ok(TimeRange { from, to })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn ts(s: &str) -> Timestamp {
        Timestamp::parse(s).unwrap()
    }

    #[test]
    fn column_type_names() {
        for t in [
            ColumnType::F64,
            ColumnType::I64,
            ColumnType::Bool,
            ColumnType::String,
            ColumnType::Timestamp,
        ] {
            assert_eq!(ColumnType::parse(t.name()), Some(t));
        }
        assert_eq!(ColumnType::parse("f32"), None);
        assert_eq!(Clock::parse("published"), Some(Clock::Published));
        assert_eq!(Clock::default(), Clock::Received);
        assert_eq!(Partitioning::default(), Partitioning::Day);
        assert_eq!(Partitioning::parse("year"), Some(Partitioning::Year));
        assert_eq!(Partitioning::parse("week"), None);
    }

    #[test]
    fn value_identity_is_bitwise() {
        assert_eq!(Value::F64(f64::NAN), Value::F64(f64::NAN));
        assert_ne!(Value::F64(f64::NAN), Value::F64(-f64::NAN));
        assert_ne!(
            Value::F64(f64::NAN),
            Value::F64(f64::from_bits(0x7ff8_0000_0000_0001))
        );
        assert_ne!(Value::F64(0.0), Value::F64(-0.0));
        assert_eq!(Value::F64(1.5), Value::F64(1.5));
        assert_ne!(Value::F64(1.0), Value::I64(1));
        assert_ne!(Value::String("1".into()), Value::I64(1));
        assert!(values_identical(None, None));
        assert!(!values_identical(None, Some(&Value::Bool(false))));
        assert!(value_lists_identical(
            &[Some(Value::I64(1))],
            &[Some(Value::I64(1)), None]
        ));
        assert!(!value_lists_identical(
            &[Some(Value::I64(1))],
            &[Some(Value::I64(1)), Some(Value::I64(2))]
        ));
        assert!(value_lists_identical(&[], &[None, None]));
    }

    #[test]
    fn coercion_rules() {
        use ColumnType as T;
        let ok = |v: Value, t: T| v.coerce_to(t).unwrap();
        let bad = |v: Value, t: T| {
            assert_eq!(v.coerce_to(t).unwrap_err().kind(), ErrorKind::TypeMismatch)
        };
        assert_eq!(ok(Value::I64(3), T::F64), Value::F64(3.0));
        assert_eq!(
            ok(Value::I64(9_007_199_254_740_993), T::F64),
            Value::F64(9_007_199_254_740_992.0)
        );
        assert_eq!(
            ok(Value::String("NaN".into()), T::F64),
            Value::F64(f64::NAN)
        );
        assert_eq!(
            ok(Value::String("inf".into()), T::F64),
            Value::F64(f64::INFINITY)
        );
        assert_eq!(
            ok(Value::String("-inf".into()), T::F64),
            Value::F64(f64::NEG_INFINITY)
        );
        assert_eq!(ok(Value::F64(-0.0), T::F64), Value::F64(-0.0));
        bad(Value::String("nan".into()), T::F64);
        bad(Value::String("Infinity".into()), T::F64);
        bad(Value::String("1.5".into()), T::F64);
        bad(Value::Bool(true), T::F64);
        assert_eq!(ok(Value::I64(-7), T::I64), Value::I64(-7));
        assert_eq!(
            ok(Value::String("9223372036854775807".into()), T::I64),
            Value::I64(i64::MAX)
        );
        assert_eq!(
            ok(Value::String("-9223372036854775808".into()), T::I64),
            Value::I64(i64::MIN)
        );
        assert_eq!(ok(Value::String("-0".into()), T::I64), Value::I64(0));
        bad(Value::String("9223372036854775808".into()), T::I64);
        bad(Value::String("+1".into()), T::I64);
        bad(Value::String("1.0".into()), T::I64);
        bad(Value::String("".into()), T::I64);
        bad(Value::String("-".into()), T::I64);
        bad(Value::String(" 1".into()), T::I64);
        bad(Value::F64(1.5), T::I64);
        bad(Value::F64(2.0), T::I64);
        bad(Value::Bool(true), T::I64);
        assert_eq!(ok(Value::Bool(false), T::Bool), Value::Bool(false));
        bad(Value::I64(0), T::Bool);
        bad(Value::String("true".into()), T::Bool);
        assert_eq!(
            ok(Value::String("x".into()), T::String),
            Value::String("x".into())
        );
        bad(Value::I64(1), T::String);
        assert_eq!(
            ok(
                Value::String("2025-01-15T13:00:00+01:00".into()),
                T::Timestamp
            ),
            Value::Timestamp(ts("2025-01-15T12:00:00Z"))
        );
        assert_eq!(
            ok(Value::Timestamp(Timestamp::MIN), T::Timestamp),
            Value::Timestamp(Timestamp::MIN)
        );
        bad(Value::String("2025-01-15".into()), T::Timestamp);
        bad(Value::String("0000-01-01T00:00:00Z".into()), T::Timestamp);
        bad(Value::I64(0), T::Timestamp);
    }

    #[test]
    fn names() {
        for good in ["a", "abc", "a1", "a_b", "x_1_"] {
            assert!(validate_table_name(good).is_ok());
            assert!(validate_column_name(good).is_ok());
        }
        for bad in ["", "A", "1a", "_a", "a-b", "a b", "aB", "é", "a.b"] {
            assert_eq!(
                validate_table_name(bad).unwrap_err().kind(),
                ErrorKind::InvalidName
            );
            assert_eq!(
                validate_column_name(bad).unwrap_err().kind(),
                ErrorKind::InvalidName
            );
        }
        for r in RESERVED_COLUMN_NAMES {
            assert_eq!(
                validate_column_name(r).unwrap_err().kind(),
                ErrorKind::InvalidName
            );
            assert!(validate_table_name(r).is_ok());
        }
        assert!(validate_series_name("DK1 spot / €").is_ok());
        assert_eq!(
            validate_series_name("").unwrap_err().kind(),
            ErrorKind::InvalidName
        );
    }

    fn version(published: Option<&str>, received: &str, system: &str, seq: u64) -> Version {
        Version {
            series: SeriesId(1),
            value_time: ts("2025-01-01T00:00:00Z"),
            published_time: published.map(ts),
            received_time: ts(received),
            deleted: false,
            values: vec![Some(Value::F64(1.0))],
            system_time: ts(system),
            seq,
        }
    }

    #[test]
    fn clocks_and_published_fallback() {
        let v = version(None, "2025-01-02T00:00:00Z", "2025-01-03T00:00:00Z", 1);
        assert_eq!(v.t_clock(Clock::Received), ts("2025-01-02T00:00:00Z"));
        assert_eq!(v.t_clock(Clock::Published), ts("2025-01-02T00:00:00Z"));
        assert_eq!(v.t_clock(Clock::System), ts("2025-01-03T00:00:00Z"));
        let w = version(
            Some("2025-01-01T12:00:00Z"),
            "2025-01-02T00:00:00Z",
            "2025-01-03T00:00:00Z",
            2,
        );
        assert_eq!(w.t_clock(Clock::Published), ts("2025-01-01T12:00:00Z"));
    }

    #[test]
    fn visibility_bounds_are_inclusive() {
        let v = version(None, "2025-01-02T00:00:00Z", "2025-01-03T00:00:00Z", 1);
        let ctx = |f: Option<&str>, s: Option<&str>| Visibility {
            clock: Clock::Received,
            frontier: f.map(ts),
            system: s.map(ts),
        };
        assert!(ctx(None, None).admits_version(&v));
        assert!(ctx(Some("2025-01-02T00:00:00Z"), Some("2025-01-03T00:00:00Z")).admits_version(&v));
        assert!(!ctx(Some("2025-01-01T23:59:59.999999Z"), None).admits_version(&v));
        assert!(!ctx(None, Some("2025-01-02T23:59:59.999999Z")).admits_version(&v));
    }

    #[test]
    fn knowledge_order_ties() {
        // Same t_clock: system_time breaks the tie, then seq.
        let a = version(None, "2025-01-02T00:00:00Z", "2025-01-03T00:00:00Z", 5);
        let b = version(None, "2025-01-02T00:00:00Z", "2025-01-04T00:00:00Z", 2);
        let c = version(None, "2025-01-02T00:00:00Z", "2025-01-04T00:00:00Z", 3);
        assert!(a.knowledge_key(Clock::Received) < b.knowledge_key(Clock::Received));
        assert!(b.knowledge_key(Clock::Received) < c.knowledge_key(Clock::Received));
        // Different t_clock dominates system_time.
        let d = version(None, "2025-01-01T00:00:00Z", "2025-01-09T00:00:00Z", 9);
        assert!(d.knowledge_key(Clock::Received) < a.knowledge_key(Clock::Received));
        assert!(d.knowledge_key(Clock::System) > a.knowledge_key(Clock::System));
    }

    #[test]
    fn content_identity_ignores_engine_fields() {
        let a = version(None, "2025-01-02T00:00:00Z", "2025-01-03T00:00:00Z", 1);
        let mut b = a.clone();
        b.system_time = ts("2026-01-01T00:00:00Z");
        b.seq = 99;
        assert!(a.content_identical(&b));
        b.values.push(None);
        assert!(a.content_identical(&b));
        b.received_time = ts("2025-01-02T00:00:00.000001Z");
        assert!(!a.content_identical(&b));
        let mut c = a.clone();
        c.published_time = Some(c.received_time);
        assert!(
            !a.content_identical(&c),
            "null published_time differs from an explicit one"
        );
        let mut d = a.clone();
        d.values = vec![Some(Value::F64(-0.0))];
        let mut e = a.clone();
        e.values = vec![Some(Value::F64(0.0))];
        assert!(!d.content_identical(&e));
    }

    #[test]
    fn ranges() {
        let r = TimeRange::new(
            Some(ts("2025-01-01T00:00:00Z")),
            Some(ts("2025-01-02T00:00:00Z")),
        );
        assert!(r.validate().is_ok());
        assert!(r.contains(ts("2025-01-01T00:00:00Z")));
        assert!(!r.contains(ts("2025-01-02T00:00:00Z")));
        let empty = TimeRange::new(r.from, r.from);
        assert!(empty.validate().is_ok());
        assert!(!empty.contains(ts("2025-01-01T00:00:00Z")));
        assert_eq!(
            TimeRange::new(r.to, r.from).validate().unwrap_err().kind(),
            ErrorKind::InvalidRange
        );
        assert!(TimeRange::ALL.contains(Timestamp::MIN) && TimeRange::ALL.contains(Timestamp::MAX));
        let d = |s: &str| Duration::parse(s).unwrap();
        assert_eq!(
            TimeRange::relative_window(ts("2025-01-15T12:00:00Z"), d("0h"), d("36h")).unwrap(),
            TimeRange::new(
                Some(ts("2025-01-15T12:00:00Z")),
                Some(ts("2025-01-17T00:00:00Z"))
            )
        );
        assert_eq!(
            TimeRange::relative_window(ts("2025-01-15T12:00:00Z"), d("1h"), d("0h"))
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidRange
        );
    }

    proptest! {
        #[test]
        fn relative_window_matches_integer_definition(
            f in Timestamp::MIN.as_micros()..=Timestamp::MAX.as_micros(),
            a in any::<i64>(), b in any::<i64>(),
            probe in prop::sample::select(vec![0u8, 1, 2, 3, 4, 5]),
            t in Timestamp::MIN.as_micros()..=Timestamp::MAX.as_micros(),
        ) {
            let (start, end) = (a.min(b), a.max(b));
            let frontier = Timestamp::from_micros(f).unwrap();
            let w = TimeRange::relative_window(frontier, Duration::from_micros(start), Duration::from_micros(end)).unwrap();
            prop_assert!(w.validate().is_ok());
            let lo = i128::from(f) + i128::from(start);
            let hi = i128::from(f) + i128::from(end);
            // Probe near the interesting points as well as at random.
            let candidates = [t as i128, lo, hi, hi - 1, i128::from(Timestamp::MIN.as_micros()), i128::from(Timestamp::MAX.as_micros())];
            let x = candidates[probe as usize];
            if let Some(ts) = i64::try_from(x).ok().and_then(Timestamp::from_micros) {
                prop_assert_eq!(w.contains(ts), lo <= x && x < hi);
            }
        }
    }

    // ---------------------------------------------------------------------------------
    // Content hash (dedup index)
    // ---------------------------------------------------------------------------------

    /// Small domains so that equal contents (and near misses) are common.
    fn arb_value() -> impl Strategy<Value = Option<Value>> {
        prop_oneof![
            3 => Just(None),
            3 => prop::sample::select(vec![
                0.0f64, -0.0, 1.0, f64::NAN, -f64::NAN, f64::from_bits(0x7FF8_0000_0000_0001),
                f64::INFINITY,
            ]).prop_map(|x| Some(Value::F64(x))),
            1 => (-2i64..2).prop_map(|x| Some(Value::I64(x))),
            1 => any::<bool>().prop_map(|x| Some(Value::Bool(x))),
            1 => prop::sample::select(vec!["", "a", "abcdefgh", "abcdefghi"])
                .prop_map(|s| Some(Value::String(s.to_owned()))),
            1 => (0i64..2).prop_map(|x| Some(Value::Timestamp(Timestamp::from_micros(x).unwrap()))),
        ]
    }

    fn arb_version() -> impl Strategy<Value = Version> {
        (
            1u32..3,
            0i64..2,
            prop::option::of(0i64..2),
            0i64..2,
            any::<bool>(),
            prop::collection::vec(arb_value(), 0..4),
            0i64..3,
            0u64..3,
        )
            .prop_map(|(s, vt, p, rt, deleted, values, st, seq)| Version {
                series: SeriesId(s),
                value_time: Timestamp::from_micros(vt).unwrap(),
                published_time: p.map(|p| Timestamp::from_micros(p).unwrap()),
                received_time: Timestamp::from_micros(rt).unwrap(),
                deleted,
                values,
                system_time: Timestamp::from_micros(st).unwrap(),
                seq,
            })
    }

    #[test]
    fn content_hash_follows_content_identity_edge_cases() {
        let v = version(None, "2025-01-02T00:00:00Z", "2025-01-03T00:00:00Z", 1);
        let with = |values: Vec<Option<Value>>| Version {
            values,
            ..v.clone()
        };
        // Missing trailing values and trailing nulls are the same content.
        let a = with(vec![Some(Value::F64(1.0))]);
        let b = with(vec![Some(Value::F64(1.0)), None, None]);
        assert!(a.content_identical(&b));
        assert_eq!(a.content_hash(), b.content_hash());
        // system_time and seq are not content.
        let c = Version {
            seq: 99,
            system_time: ts("2030-01-01T00:00:00Z"),
            ..a.clone()
        };
        assert_eq!(a.content_hash(), c.content_hash());
        // f64 bitwise: -0.0 differs from 0.0, NaN equals the same NaN.
        let z = with(vec![Some(Value::F64(0.0))]);
        let nz = with(vec![Some(Value::F64(-0.0))]);
        assert!(!z.content_identical(&nz));
        assert_ne!(z.content_hash(), nz.content_hash());
        let n1 = with(vec![Some(Value::F64(f64::NAN))]);
        let n2 = with(vec![Some(Value::F64(f64::NAN))]);
        assert_eq!(n1.content_hash(), n2.content_hash());
        // A null in the middle is not a shift of the values.
        let m1 = with(vec![None, Some(Value::I64(1))]);
        let m2 = with(vec![Some(Value::I64(1))]);
        assert_ne!(m1.content_hash(), m2.content_hash());
    }

    proptest! {
        /// Content-identical versions hash alike, and a [`ContentIndex`] finds exactly
        /// the items a linear scan with `content_identical` finds.
        #[test]
        fn content_index_matches_linear_scan(
            items in prop::collection::vec(arb_version(), 0..40),
            probes in prop::collection::vec(arb_version(), 1..20),
        ) {
            let mut ix = ContentIndex::default();
            for v in &items {
                ix.push(v.content_hash());
            }
            for p in probes.iter().chain(&items) {
                let linear = items.iter().any(|v| v.content_identical(p));
                let found = ix.find(&items, p, p.content_hash());
                prop_assert_eq!(found.is_some(), linear);
                if let Some(i) = found {
                    prop_assert!(items[i].content_identical(p));
                }
                for v in &items {
                    if v.content_identical(p) {
                        prop_assert_eq!(v.content_hash(), p.content_hash());
                    }
                }
            }
        }
    }
}
