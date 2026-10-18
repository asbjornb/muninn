//! Request/response types for every operation, and the [`Database`] / [`Reader`] traits.
//!
//! This module is the seam between implementations (the in-memory [`crate::model`], the
//! storage engine) and their users (the conformance runner, differential fuzzers, bindings).
//! Design rules:
//!
//! - Everything is synchronous (decision 0005) and takes `&self`: implementations use
//!   interior mutability, so one handle can be shared by a writer and many readers.
//! - Every request is passed by reference, so the same request can be replayed against
//!   several implementations (differential testing).
//! - Reads run against a [`Reader`]. A [`Database`] is itself a `Reader` over its latest
//!   snapshot (each call takes a fresh snapshot, SPEC §5.1); [`Database::snapshot`] pins
//!   one so that several reads see the same committed state (SPEC §7.3).
//! - Writes take the writer clock reading explicitly ([`WriteBatch::clock`]); the
//!   implementation applies the `max(c, last)` rule of SPEC §4.2 itself.
//! - Write inputs are *loose* (they carry what a client sent: [`TimeInput`] and
//!   un-coerced [`InputValue`]s) so that the implementation — not the caller — performs the
//!   SPEC §4.1 validation in the normative order.
//! - Results are plain rows with typed values; no Arrow here.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::Result;
use crate::time::{Duration, Timestamp};
use crate::types::{
    Clock, ColumnDef, InputValue, Partitioning, SeriesDef, SeriesId, TableDef, TimeRange, Value,
};

// ---------------------------------------------------------------------------------------
// Catalog requests (SPEC §2)
// ---------------------------------------------------------------------------------------

/// `create_table` (SPEC §2.1).
#[derive(Clone, Debug, PartialEq)]
pub struct CreateTable {
    /// Table name, `[a-z][a-z0-9_]*`.
    pub name: String,
    /// Ordered columns (may be empty).
    pub columns: Vec<ColumnDef>,
    /// Default read clock (spec default: received).
    pub default_clock: Clock,
    /// Physical partitioning (spec default: day).
    pub partition: Partitioning,
    /// Writer clock reading for the catalog change log (SPEC §2.3); `None` lets the
    /// implementation use its own clock. Not observable through any v0.1 read.
    pub clock: Option<Timestamp>,
}

impl CreateTable {
    /// A table with spec defaults for clock and partitioning.
    pub fn new(name: impl Into<String>, columns: Vec<ColumnDef>) -> CreateTable {
        CreateTable {
            name: name.into(),
            columns,
            default_clock: Clock::default(),
            partition: Partitioning::default(),
            clock: None,
        }
    }
}

/// `add_column` (SPEC §2.1): appends a column; existing versions read it as null.
#[derive(Clone, Debug, PartialEq)]
pub struct AddColumn {
    /// Table to extend.
    pub table: String,
    /// The new column.
    pub column: ColumnDef,
    /// Writer clock reading for the catalog change log; see [`CreateTable::clock`].
    pub clock: Option<Timestamp>,
}

/// `create_series` (SPEC §2.2).
#[derive(Clone, Debug, PartialEq)]
pub struct CreateSeries {
    /// Series name: non-empty, unique across the database.
    pub name: String,
    /// Owning table.
    pub table: String,
    /// Descriptive tags.
    pub tags: BTreeMap<String, String>,
    /// IANA timezone name; `None` = `"UTC"`.
    pub timezone: Option<String>,
    /// Nominal resolution; `None` = irregular.
    pub resolution: Option<Duration>,
    /// Writer clock reading for the catalog change log; see [`CreateTable::clock`].
    pub clock: Option<Timestamp>,
}

impl CreateSeries {
    /// A series with no tags, UTC timezone and irregular resolution.
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> CreateSeries {
        CreateSeries {
            name: name.into(),
            table: table.into(),
            tags: BTreeMap::new(),
            timezone: None,
            resolution: None,
            clock: None,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Writes (SPEC §4)
// ---------------------------------------------------------------------------------------

/// A timestamp as supplied by a writer, validated by the implementation in SPEC §4.1
/// step 5 (so that an invalid time loses to earlier validation failures).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TimeInput {
    /// Raw microseconds since the epoch (may be out of range).
    Micros(i64),
    /// SPEC §1 text form (may be unparsable).
    Text(String),
    /// Missing, null where a time is required, or not a string (SPEC §10.1): always
    /// `invalid_timestamp`. The text describes the input for error messages.
    Invalid(String),
}

impl TimeInput {
    /// Resolves to a valid [`Timestamp`] or fails with `invalid_timestamp`.
    pub fn resolve(&self) -> Result<Timestamp> {
        Ok(match self {
            TimeInput::Micros(us) => Timestamp::try_from_micros(*us)?,
            TimeInput::Text(s) => Timestamp::parse(s)?,
            TimeInput::Invalid(what) => {
                return Err(crate::Error::new(
                    crate::ErrorKind::InvalidTimestamp,
                    format!("not a timestamp: {what}"),
                ));
            }
        })
    }
}

impl From<Timestamp> for TimeInput {
    fn from(t: Timestamp) -> TimeInput {
        TimeInput::Micros(t.as_micros())
    }
}

impl From<&str> for TimeInput {
    fn from(s: &str) -> TimeInput {
        TimeInput::Text(s.to_owned())
    }
}

impl From<String> for TimeInput {
    fn from(s: String) -> TimeInput {
        TimeInput::Text(s)
    }
}

/// One row of a write batch: the writer-supplied fields of a version (SPEC §3).
#[derive(Clone, Debug, PartialEq)]
pub struct WriteRow {
    /// Series *name*.
    pub series: String,
    /// What moment the values are about.
    pub value_time: TimeInput,
    /// When it became public; `None` = unknown.
    pub published_time: Option<TimeInput>,
    /// When we got it (required).
    pub received_time: TimeInput,
    /// Tombstone flag.
    pub deleted: bool,
    /// Column name → value; missing keys and [`InputValue::Null`] are null. Values are
    /// coerced to the column type per SPEC §4.1 step 3 ([`InputValue::coerce_to`]).
    pub values: BTreeMap<String, InputValue>,
}

impl WriteRow {
    /// A non-deleted row with no published time and no values.
    pub fn new(
        series: impl Into<String>,
        value_time: impl Into<TimeInput>,
        received_time: impl Into<TimeInput>,
    ) -> WriteRow {
        WriteRow {
            series: series.into(),
            value_time: value_time.into(),
            published_time: None,
            received_time: received_time.into(),
            deleted: false,
            values: BTreeMap::new(),
        }
    }

    /// Builder: sets the published time.
    pub fn published(mut self, t: impl Into<TimeInput>) -> WriteRow {
        self.published_time = Some(t.into());
        self
    }

    /// Builder: sets a column value (a [`Value`], an `Option<Value>` with `None` = explicit
    /// null, or any [`InputValue`]).
    pub fn value(mut self, column: impl Into<String>, v: impl Into<InputValue>) -> WriteRow {
        self.values.insert(column.into(), v.into());
        self
    }

    /// Builder: marks the row as a tombstone.
    pub fn tombstone(mut self) -> WriteRow {
        self.deleted = true;
        self
    }
}

/// `write(rows, batch_id?)` (SPEC §4): an atomic batch.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteBatch {
    /// Rows, in input order (validation order and seq order follow it).
    pub rows: Vec<WriteRow>,
    /// Optional whole-batch idempotency key (SPEC §4.4); an empty string is
    /// `invalid_argument`.
    pub batch_id: Option<String>,
    /// The writer clock reading `c` at commit (SPEC §4.2). Validated (`invalid_timestamp`)
    /// only after the batch-id check (SPEC §10.4), hence a [`TimeInput`].
    pub clock: TimeInput,
}

impl WriteBatch {
    /// A batch without batch id.
    pub fn new(clock: impl Into<TimeInput>, rows: Vec<WriteRow>) -> WriteBatch {
        WriteBatch {
            rows,
            batch_id: None,
            clock: clock.into(),
        }
    }
}

/// Result of a write (SPEC §4.5), plus engine-assigned commit metadata.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct WriteResult {
    /// Rows stored.
    pub stored: u64,
    /// Rows dropped as content-identical duplicates (SPEC §4.3).
    pub duplicates: u64,
    /// The batch id had been committed before; nothing was done (SPEC §4.4).
    pub batch_already_committed: bool,
    /// The batch `system_time` (SPEC §4.2); `None` if nothing was stored.
    pub system_time: Option<Timestamp>,
    /// `seq` of the first stored row (the rest follow consecutively); `None` if nothing
    /// was stored.
    pub first_seq: Option<u64>,
}

// ---------------------------------------------------------------------------------------
// Reads (SPEC §5, §6)
// ---------------------------------------------------------------------------------------

/// Read context of SPEC §5 as requested (clock may default to the table's).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct ReadContext {
    /// Clock; `None` = the table's `default_clock`.
    pub clock: Option<Clock>,
    /// Frontier `F`, inclusive; `None` = unbounded.
    pub frontier: Option<Timestamp>,
    /// System bound `S`, inclusive; `None` = unbounded (the whole snapshot).
    pub system: Option<Timestamp>,
}

/// `latest` (SPEC §6.1).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct LatestRequest {
    /// Table name.
    pub table: String,
    /// Series filter by name; empty = every series of the table.
    pub series: Vec<String>,
    /// Value range `[from, to)` on `value_time`.
    pub range: TimeRange,
    /// Read context.
    pub context: ReadContext,
}

/// `history` (SPEC §6.2). Same fields as [`LatestRequest`].
pub type HistoryRequest = LatestRequest;

/// `replay` (SPEC §6.3).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ReplayRequest {
    /// Table name.
    pub table: String,
    /// Series filter by name; empty = every series of the table.
    pub series: Vec<String>,
    /// Value range `[from, to)` on `value_time`.
    pub range: TimeRange,
    /// Clock; `None` = the table's default.
    pub clock: Option<Clock>,
    /// System bound `S`, inclusive.
    pub system: Option<Timestamp>,
    /// Arrival bounds on `t_clock`, read as the **open-closed** interval `(from, to]`
    /// (SPEC §6.3; see [`TimeRange::contains_open_closed`]). `from > to` is
    /// `invalid_range`.
    pub arrival: TimeRange,
}

/// Value window of a `grid` read (SPEC §6.4).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum GridWindow {
    /// The same value range for every frontier.
    Absolute(TimeRange),
    /// `[F_i + start, F_i + end)` for frontier `F_i`.
    Relative {
        /// Window start offset.
        start: Duration,
        /// Window end offset (exclusive).
        end: Duration,
    },
}

impl Default for GridWindow {
    fn default() -> GridWindow {
        GridWindow::Absolute(TimeRange::ALL)
    }
}

/// `grid` (SPEC §6.4).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct GridRequest {
    /// Table name.
    pub table: String,
    /// Series filter by name; empty = every series of the table.
    pub series: Vec<String>,
    /// Frontiers; duplicates are removed and the list sorted by the implementation.
    pub frontiers: Vec<Timestamp>,
    /// Value window.
    pub window: GridWindow,
    /// Clock; `None` = the table's default.
    pub clock: Option<Clock>,
    /// System bound `S`, inclusive.
    pub system: Option<Timestamp>,
}

/// One left row of an `asof_join`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AsofLeftRow {
    /// Series name (must belong to the right table).
    pub series: String,
    /// Exact value time to look up.
    pub value_time: Timestamp,
    /// Frontier for this row.
    pub frontier: Timestamp,
}

/// `asof_join` (SPEC §6.5).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct AsofJoinRequest {
    /// Right table name.
    pub table: String,
    /// Clock; `None` = the table's default.
    pub clock: Option<Clock>,
    /// System bound `S`, inclusive.
    pub system: Option<Timestamp>,
    /// Left rows, in output order.
    pub left: Vec<AsofLeftRow>,
}

/// A version as returned by `latest`, `history`, `replay` and (nested) `grid` /
/// `asof_join`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionRow {
    /// Series id (results are ordered "by series" = by id).
    pub series_id: SeriesId,
    /// Series name.
    pub series: Arc<str>,
    /// Value time.
    pub value_time: Timestamp,
    /// Stored published time (may be null — *not* the fallback).
    pub published_time: Option<Timestamp>,
    /// Received time.
    pub received_time: Timestamp,
    /// Commit time.
    pub system_time: Timestamp,
    /// Sequence number.
    pub seq: u64,
    /// Tombstone flag. Always `false` in `latest`/`grid`/`asof_join` results (which never
    /// return tombstones); part of the spec output only for `history` and `replay`.
    pub deleted: bool,
    /// One entry per *current* table column ([`ReadResult::columns`]), null where absent.
    pub values: Vec<Option<Value>>,
}

/// A `grid` output row: a `latest` row prefixed with its frontier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GridRow {
    /// The frontier `F_i` this row belongs to.
    pub frontier: Timestamp,
    /// The `latest` row at that frontier.
    pub row: VersionRow,
}

/// An `asof_join` output row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AsofRow {
    /// Left series id.
    pub series_id: SeriesId,
    /// Left series name.
    pub series: Arc<str>,
    /// Left value time.
    pub value_time: Timestamp,
    /// Left frontier.
    pub frontier: Timestamp,
    /// The matching right version, or `None` (`found = false`, right side all null).
    pub right: Option<VersionRow>,
}

impl AsofRow {
    /// The spec's `found` field.
    pub fn found(&self) -> bool {
        self.right.is_some()
    }
}

/// Rows of a read plus the table's current columns (the order of every `values` vector).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadResult<R> {
    /// The table's columns at read time.
    pub columns: Vec<ColumnDef>,
    /// Result rows in spec order.
    pub rows: Vec<R>,
}

// ---------------------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------------------

/// Read operations against one snapshot (SPEC §5.1, §6). Every method has no effect.
pub trait Reader {
    /// Table definition by name (`unknown_table`).
    fn table(&self, name: &str) -> Result<TableDef>;
    /// All tables, in creation order.
    fn tables(&self) -> Result<Vec<TableDef>>;
    /// Series definition by name (`unknown_series`).
    fn series(&self, name: &str) -> Result<SeriesDef>;
    /// All series of `table`, ordered by id (`unknown_table`).
    fn table_series(&self, table: &str) -> Result<Vec<SeriesDef>>;

    /// SPEC §6.1. Order `(series, value_time)`.
    fn latest(&self, req: &LatestRequest) -> Result<ReadResult<VersionRow>>;
    /// SPEC §6.2. Order `(series, value_time, k_clock)`; tombstones included.
    fn history(&self, req: &HistoryRequest) -> Result<ReadResult<VersionRow>>;
    /// SPEC §6.3. Order `k_clock`; tombstones included.
    fn replay(&self, req: &ReplayRequest) -> Result<ReadResult<VersionRow>>;
    /// SPEC §6.4. Order `(frontier, series, value_time)`.
    fn grid(&self, req: &GridRequest) -> Result<ReadResult<GridRow>>;
    /// SPEC §6.5. One row per left row, in input order.
    fn asof_join(&self, req: &AsofJoinRequest) -> Result<ReadResult<AsofRow>>;
}

/// A Muninn database: catalog changes, atomic writes, snapshots, and reads on the latest
/// snapshot. Implemented by [`crate::model::ModelDb`] and the storage engine.
///
/// Contract: every method that returns `Err` has no effect (SPEC §8); a successful write
/// is atomic and immediately visible to subsequent reads and snapshots.
pub trait Database: Reader {
    /// A pinned, immutable view of the committed state.
    type Snapshot: Reader;

    /// Pins the current committed state (SPEC §5.1).
    fn snapshot(&self) -> Result<Self::Snapshot>;

    /// SPEC §2.1. Check order (§10.4): table name `invalid_name`, `already_exists`, then
    /// per column `invalid_name`, `already_exists`.
    fn create_table(&self, req: &CreateTable) -> Result<TableDef>;
    /// SPEC §2.1. Errors: `unknown_table`, `invalid_name`, `already_exists`.
    fn add_column(&self, req: &AddColumn) -> Result<TableDef>;
    /// SPEC §2.2. Check order (§10.4): empty name `invalid_name`, `unknown_table`,
    /// `already_exists`.
    fn create_series(&self, req: &CreateSeries) -> Result<SeriesDef>;
    /// SPEC §4. Check order per §10.4: empty batch id (`invalid_argument`), known batch id
    /// (no-op), writer clock (`invalid_timestamp`), then §4.1 steps 1–5 per row.
    fn write(&self, batch: &WriteBatch) -> Result<WriteResult>;
}
