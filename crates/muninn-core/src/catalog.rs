//! The catalog (SPEC §2) and the semantic steps every implementation shares: catalog
//! checks, write validation and planning (SPEC §4), and read-request preparation (SPEC §6).
//!
//! These are the parts of the spec that do not depend on how versions are stored. They
//! live here — not in [`crate::model`] — so that the in-memory model and the storage engine
//! run *literally the same code* for them; differential tests between the two then
//! exercise what actually differs (storage, merging, pruning). The model itself is a thin
//! layer over this module plus naive linear scans.
//!
//! Check orders the spec leaves open are documented in `crates/SPEC-QUESTIONS.md`.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::api::{
    AddColumn, AsofJoinRequest, CreateSeries, CreateTable, GridRequest, GridWindow, LatestRequest,
    ReplayRequest, VersionRow, WriteBatch, WriteResult, WriteRow,
};
use crate::error::{Error, ErrorKind, Result};
use crate::time::Timestamp;
use crate::types::{
    Clock, ColumnDef, SeriesDef, SeriesId, TableDef, TimeRange, Version, Visibility,
    validate_column_name, validate_series_name, validate_table_name,
};

/// One entry of the catalog change log (SPEC §2.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogChange {
    /// When the change was committed.
    pub system_time: Timestamp,
    /// What changed.
    pub change: CatalogChangeKind,
}

/// The kinds of catalog change in v0.1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogChangeKind {
    /// A table was created (definition as created).
    CreateTable(TableDef),
    /// A column was appended to a table.
    AddColumn {
        /// Table name.
        table: String,
        /// The new column.
        column: ColumnDef,
    },
    /// A series was created.
    CreateSeries(SeriesDef),
}

/// Tables and series (SPEC §2.1, §2.2).
///
/// Invariant: `series[i].id == SeriesId(i + 1)` and every series' table exists.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Catalog {
    tables: Vec<TableDef>,
    series: Vec<SeriesDef>,
}

/// Outcome of [`Catalog::validate_batch`].
#[derive(Clone, Debug, PartialEq)]
pub enum BatchPlan {
    /// The batch id was committed before: the write is a no-op (SPEC §4.4).
    AlreadyCommitted,
    /// The writer clock and every row validated: the candidate versions in input order
    /// (`system_time` and `seq` not yet assigned).
    Candidates {
        /// The resolved writer clock reading `c` (SPEC §4.2).
        clock: Timestamp,
        /// The candidate versions.
        rows: Vec<Version>,
    },
}

impl Catalog {
    /// An empty catalog.
    pub fn new() -> Catalog {
        Catalog::default()
    }

    /// Tables in creation order.
    pub fn tables(&self) -> &[TableDef] {
        &self.tables
    }

    /// Series in creation (= id) order.
    pub fn all_series(&self) -> &[SeriesDef] {
        &self.series
    }

    /// Table by name (`unknown_table`).
    pub fn table(&self, name: &str) -> Result<&TableDef> {
        self.tables
            .iter()
            .find(|t| t.name == name)
            .ok_or_else(|| Error::new(ErrorKind::UnknownTable, format!("no table {name:?}")))
    }

    /// Series by name (`unknown_series`).
    pub fn series_by_name(&self, name: &str) -> Result<&SeriesDef> {
        self.series
            .iter()
            .find(|s| &*s.name == name)
            .ok_or_else(|| Error::new(ErrorKind::UnknownSeries, format!("no series {name:?}")))
    }

    /// Series by id.
    pub fn series_by_id(&self, id: SeriesId) -> Option<&SeriesDef> {
        (id.0 as usize)
            .checked_sub(1)
            .and_then(|i| self.series.get(i))
    }

    /// All series of `table`, ordered by id (`unknown_table`).
    pub fn table_series(&self, table: &str) -> Result<Vec<SeriesDef>> {
        self.table(table)?;
        Ok(self
            .series
            .iter()
            .filter(|s| s.table == table)
            .cloned()
            .collect())
    }

    // -----------------------------------------------------------------------------------
    // Catalog changes
    // -----------------------------------------------------------------------------------
    //
    // Check order: SPEC §10.4. Structure (§10.3) is guaranteed by the typed API, or
    // checked by `wire` for JSON input.

    /// Checks a `create_table` and returns the definition it would create.
    pub fn check_create_table(&self, req: &CreateTable) -> Result<TableDef> {
        // §10.4: table name syntax, table uniqueness, then each column in order (name
        // syntax / reserved name, uniqueness within the table).
        validate_table_name(&req.name)?;
        if self.tables.iter().any(|t| t.name == req.name) {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("table {:?} already exists", req.name),
            ));
        }
        for (i, c) in req.columns.iter().enumerate() {
            validate_column_name(&c.name)?;
            if req.columns[..i].iter().any(|p| p.name == c.name) {
                return Err(Error::new(
                    ErrorKind::AlreadyExists,
                    format!("duplicate column {:?} in table {:?}", c.name, req.name),
                ));
            }
        }
        Ok(TableDef {
            name: req.name.clone(),
            columns: req.columns.clone(),
            default_clock: req.default_clock,
            partition: req.partition,
        })
    }

    /// Checks an `add_column`; returns the column to append.
    pub fn check_add_column(&self, req: &AddColumn) -> Result<ColumnDef> {
        let table = self.table(&req.table)?;
        validate_column_name(&req.column.name)?;
        if table.column_index(&req.column.name).is_some() {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!(
                    "column {:?} already exists in table {:?}",
                    req.column.name, req.table
                ),
            ));
        }
        Ok(req.column.clone())
    }

    /// Checks a `create_series` and returns the definition it would create (with the next
    /// id).
    pub fn check_create_series(&self, req: &CreateSeries) -> Result<SeriesDef> {
        // §10.4: empty name, unknown_table, uniqueness.
        validate_series_name(&req.name)?;
        self.table(&req.table)?;
        if self.series.iter().any(|s| *s.name == *req.name) {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("series {:?} already exists", req.name),
            ));
        }
        let id = u32::try_from(self.series.len() + 1)
            .map(SeriesId)
            .map_err(|_| Error::internal("series id space exhausted"))?;
        Ok(SeriesDef {
            id,
            name: Arc::from(req.name.as_str()),
            table: req.table.clone(),
            tags: req.tags.clone(),
            timezone: req.timezone.clone().unwrap_or_else(|| "UTC".to_owned()),
            resolution: req.resolution,
        })
    }

    /// Applies a change produced by one of the `check_*` methods (or replayed from a
    /// durable log). Re-checks the invariants, so replaying a corrupt or reordered log
    /// fails instead of producing a wrong catalog.
    pub fn apply(&mut self, change: &CatalogChangeKind) -> Result<()> {
        match change {
            CatalogChangeKind::CreateTable(def) => {
                let checked = self.check_create_table(&CreateTable {
                    name: def.name.clone(),
                    columns: def.columns.clone(),
                    default_clock: def.default_clock,
                    partition: def.partition,
                    clock: None,
                })?;
                self.tables.push(checked);
            }
            CatalogChangeKind::AddColumn { table, column } => {
                let col = self.check_add_column(&AddColumn {
                    table: table.clone(),
                    column: column.clone(),
                    clock: None,
                })?;
                let t = self
                    .tables
                    .iter_mut()
                    .find(|t| t.name == *table)
                    .ok_or_else(|| Error::internal("table vanished"))?;
                t.columns.push(col);
            }
            CatalogChangeKind::CreateSeries(def) => {
                let checked = self.check_create_series(&CreateSeries {
                    name: def.name.to_string(),
                    table: def.table.clone(),
                    tags: def.tags.clone(),
                    timezone: Some(def.timezone.clone()),
                    resolution: def.resolution,
                    clock: None,
                })?;
                if checked.id != def.id {
                    return Err(Error::internal(format!(
                        "series {:?} has id {} but the next id is {}",
                        def.name, def.id, checked.id
                    )));
                }
                self.series.push(checked);
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------------------
    // Writes (SPEC §4)
    // -----------------------------------------------------------------------------------

    /// SPEC §4.1: validates one row, checks in the normative order (first failure wins).
    /// Returns the version content (`system_time`/`seq` are placeholders).
    pub fn validate_row(&self, row: &WriteRow) -> Result<Version> {
        // 1. Series name exists.
        let series = self.series_by_name(&row.series)?;
        let table = self.table(&series.table)?;
        // 2. Every key in `values` is a column of the series' table.
        for name in row.values.keys() {
            if table.column_index(name).is_none() {
                return Err(Error::new(
                    ErrorKind::UnknownColumn,
                    format!("table {:?} has no column {name:?}", table.name),
                ));
            }
        }
        // 3. Every non-null value matches its column's type. Missing keys are null.
        let mut values: Vec<Option<_>> = vec![None; table.columns.len()];
        for (name, value) in &row.values {
            let i = table.column_index(name).expect("checked in step 2");
            values[i] = value
                .coerce_to(table.columns[i].ty)
                .map_err(|e| Error::new(e.kind(), format!("column {name:?}: {}", e.message())))?;
        }
        // 4. Tombstone with any non-null value.
        if row.deleted && values.iter().any(Option::is_some) {
            return Err(Error::new(
                ErrorKind::TombstoneWithValues,
                "a tombstone must have every value null",
            ));
        }
        // 5. value_time, received_time, published_time (if non-null) are valid.
        let value_time = row.value_time.resolve()?;
        let received_time = row.received_time.resolve()?;
        let published_time = row
            .published_time
            .as_ref()
            .map(|t| t.resolve())
            .transpose()?;
        Ok(Version {
            series: series.id,
            value_time,
            published_time,
            received_time,
            deleted: row.deleted,
            values,
            system_time: Timestamp::MIN, // placeholder, set by `plan_dedup`
            seq: 0,                      // placeholder, set by `plan_dedup`
        })
    }

    /// SPEC §10.4 order: an empty batch id (`invalid_argument`), the §4.4 no-op for a
    /// known batch id (no further validation), the writer clock (`invalid_timestamp`),
    /// then §4.1 steps 1–5 per row in input order (any failure rejects the whole batch). `is_committed` answers whether a batch id was
    /// committed before.
    pub fn validate_batch(
        &self,
        batch: &WriteBatch,
        is_committed: impl FnOnce(&str) -> bool,
    ) -> Result<BatchPlan> {
        if let Some(id) = &batch.batch_id {
            if id.is_empty() {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "batch_id must be non-empty",
                ));
            }
            if is_committed(id) {
                return Ok(BatchPlan::AlreadyCommitted);
            }
        }
        let clock = batch
            .clock
            .resolve()
            .map_err(|e| Error::new(e.kind(), format!("writer clock: {}", e.message())))?;
        let mut rows = Vec::with_capacity(batch.rows.len());
        for (i, row) in batch.rows.iter().enumerate() {
            let v = self
                .validate_row(row)
                .map_err(|e| Error::new(e.kind(), format!("row {i}: {}", e.message())))?;
            rows.push(v);
        }
        Ok(BatchPlan::Candidates { clock, rows })
    }

    // -----------------------------------------------------------------------------------
    // Reads (SPEC §6)
    // -----------------------------------------------------------------------------------
    //
    // Check order for reads: table, then series filter, then ranges/windows.

    /// Resolves a series filter (SPEC §6): empty = every series of the table; a name that
    /// does not exist or belongs to another table is `unknown_series`.
    pub fn series_filter(&self, table: &TableDef, names: &[String]) -> Result<BTreeSet<SeriesId>> {
        if names.is_empty() {
            return Ok(self
                .series
                .iter()
                .filter(|s| s.table == table.name)
                .map(|s| s.id)
                .collect());
        }
        let mut ids = BTreeSet::new();
        for name in names {
            let s = self.series_by_name(name)?;
            if s.table != table.name {
                return Err(Error::new(
                    ErrorKind::UnknownSeries,
                    format!(
                        "series {name:?} belongs to table {:?}, not {:?}",
                        s.table, table.name
                    ),
                ));
            }
            ids.insert(s.id);
        }
        Ok(ids)
    }

    /// Validates a `latest`/`history` request and resolves it.
    pub fn prepare_latest(&self, req: &LatestRequest) -> Result<PreparedRead<'_>> {
        let table = self.table(&req.table)?;
        let ids = self.series_filter(table, &req.series)?;
        req.range.validate()?;
        Ok(PreparedRead {
            table,
            ids,
            range: req.range,
            vis: Visibility {
                clock: req.context.clock.unwrap_or(table.default_clock),
                frontier: req.context.frontier,
                system: req.context.system,
            },
        })
    }

    /// Validates a `replay` request and resolves it. The returned visibility carries the
    /// system bound only (the arrival range, not a frontier, bounds `t_clock`).
    pub fn prepare_replay(&self, req: &ReplayRequest) -> Result<PreparedRead<'_>> {
        let table = self.table(&req.table)?;
        let ids = self.series_filter(table, &req.series)?;
        req.range.validate()?;
        req.arrival.validate()?;
        Ok(PreparedRead {
            table,
            ids,
            range: req.range,
            vis: Visibility {
                clock: req.clock.unwrap_or(table.default_clock),
                frontier: None,
                system: req.system,
            },
        })
    }

    /// Validates a `grid` request and resolves it.
    pub fn prepare_grid(&self, req: &GridRequest) -> Result<PreparedGrid<'_>> {
        let table = self.table(&req.table)?;
        let ids = self.series_filter(table, &req.series)?;
        match req.window {
            GridWindow::Absolute(r) => r.validate()?,
            GridWindow::Relative { start, end } => {
                // Validate even when there are no frontiers.
                TimeRange::relative_window(Timestamp::UNIX_EPOCH, start, end)?;
            }
        }
        let frontiers: BTreeSet<Timestamp> = req.frontiers.iter().copied().collect();
        Ok(PreparedGrid {
            table,
            ids,
            frontiers: frontiers.into_iter().collect(),
            window: req.window,
            clock: req.clock.unwrap_or(table.default_clock),
            system: req.system,
        })
    }

    /// Validates an `asof_join` request: the table, then each left row's series in input
    /// order (the first offending row is reported). Returns the table, the clock and each
    /// left row's series.
    pub fn prepare_asof(&self, req: &AsofJoinRequest) -> Result<PreparedAsof<'_>> {
        let table = self.table(&req.table)?;
        let mut series = Vec::with_capacity(req.left.len());
        for left in &req.left {
            let ids = self.series_filter(table, std::slice::from_ref(&left.series))?;
            debug_assert_eq!(ids.len(), 1);
            series.push(self.series_by_name(&left.series)?);
        }
        Ok(PreparedAsof {
            table,
            clock: req.clock.unwrap_or(table.default_clock),
            system: req.system,
            series,
        })
    }

    /// An output row for version `v` of `table`: every *current* column, null where the
    /// version predates the column.
    pub fn version_row(&self, table: &TableDef, v: &Version) -> Result<VersionRow> {
        let s = self
            .series_by_id(v.series)
            .ok_or_else(|| Error::internal(format!("version of unknown series {}", v.series)))?;
        Ok(VersionRow {
            series_id: v.series,
            series: Arc::clone(&s.name),
            value_time: v.value_time,
            published_time: v.published_time,
            received_time: v.received_time,
            system_time: v.system_time,
            seq: v.seq,
            deleted: v.deleted,
            values: (0..table.columns.len())
                .map(|i| v.value(i).cloned())
                .collect(),
        })
    }
}

/// A validated `latest` / `history` / `replay` request.
#[derive(Clone, Debug)]
pub struct PreparedRead<'a> {
    /// The table.
    pub table: &'a TableDef,
    /// The series filter, resolved to ids.
    pub ids: BTreeSet<SeriesId>,
    /// The value range.
    pub range: TimeRange,
    /// The resolved context.
    pub vis: Visibility,
}

/// A validated `grid` request.
#[derive(Clone, Debug)]
pub struct PreparedGrid<'a> {
    /// The table.
    pub table: &'a TableDef,
    /// The series filter, resolved to ids.
    pub ids: BTreeSet<SeriesId>,
    /// Frontiers, deduplicated and sorted ascending.
    pub frontiers: Vec<Timestamp>,
    /// The (validated) value window.
    pub window: GridWindow,
    /// The clock.
    pub clock: Clock,
    /// The system bound.
    pub system: Option<Timestamp>,
}

/// A validated `asof_join` request.
#[derive(Clone, Debug)]
pub struct PreparedAsof<'a> {
    /// The right table.
    pub table: &'a TableDef,
    /// The clock.
    pub clock: Clock,
    /// The system bound.
    pub system: Option<Timestamp>,
    /// The series of each left row, in input order.
    pub series: Vec<&'a SeriesDef>,
}

impl PreparedGrid<'_> {
    /// The value range for frontier `f` (SPEC §6.4).
    pub fn range_for(&self, f: Timestamp) -> Result<TimeRange> {
        match self.window {
            GridWindow::Absolute(r) => Ok(r),
            GridWindow::Relative { start, end } => TimeRange::relative_window(f, start, end),
        }
    }
}

/// SPEC §4.2: `batch.system_time = max(c, last + 1us)` — strictly increasing across
/// stored batches, so no later write can land at or below a system bound taken from an
/// existing snapshot (§7.3). Fails with `invalid_timestamp` once `last` is the maximum
/// timestamp (there is no `last + 1us`).
pub fn batch_system_time(clock: Timestamp, last: Option<Timestamp>) -> Result<Timestamp> {
    match last {
        None => Ok(clock),
        Some(last) => Timestamp::from_micros(last.as_micros() + 1)
            .map(|next| clock.max(next))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidTimestamp,
                    "system_time exhausted: the last stored batch is at the maximum timestamp",
                )
            }),
    }
}

/// Catalog change timestamps (SPEC §2.3, crates/SPEC-QUESTIONS.md 8): the supplied clock
/// reading (or, without one, the latest timestamp known: `prev` catalog time or `last`
/// version time), never before the previous catalog change `prev`. Catalog times never
/// feed into version `system_time`.
pub fn catalog_time(
    prev: Option<Timestamp>,
    last: Option<Timestamp>,
    clock: Option<Timestamp>,
) -> Timestamp {
    let base = clock
        .or_else(|| prev.max(last))
        .unwrap_or(Timestamp::UNIX_EPOCH);
    base.max(prev.unwrap_or(Timestamp::MIN))
}

/// The outcome of [`plan_dedup`].
#[derive(Clone, Debug, PartialEq)]
pub struct DedupPlan {
    /// The rows to store, in input order, with `system_time` and `seq` assigned.
    pub kept: Vec<Version>,
    /// Rows dropped as duplicates.
    pub duplicates: u64,
}

impl DedupPlan {
    /// The SPEC §4.5 result for this plan.
    pub fn result(&self) -> WriteResult {
        WriteResult {
            stored: self.kept.len() as u64,
            duplicates: self.duplicates,
            batch_already_committed: false,
            system_time: self.kept.first().map(|v| v.system_time),
            first_seq: self.kept.first().map(|v| v.seq),
        }
    }
}

/// SPEC §4.3 + §4.2: drops candidates content-identical to a stored version
/// (`is_stored`) or to an earlier *kept* row of the batch; kept rows take
/// `system_time` and consecutive seqs from `next_seq` in input order.
///
/// `is_stored(v)` must answer whether any already stored version is content-identical to
/// `v` (see [`Version::content_identical`]).
pub fn plan_dedup(
    candidates: Vec<Version>,
    system_time: Timestamp,
    next_seq: u64,
    mut is_stored: impl FnMut(&Version) -> Result<bool>,
) -> Result<DedupPlan> {
    let mut kept: Vec<Version> = Vec::new();
    // Kept rows by point: only versions of the same point can be content-identical.
    let mut by_point: HashMap<(SeriesId, Timestamp), Vec<usize>> = HashMap::new();
    let mut duplicates = 0u64;
    for mut v in candidates {
        let point = (v.series, v.value_time);
        let dup_in_batch = by_point
            .get(&point)
            .is_some_and(|ix| ix.iter().any(|&i| kept[i].content_identical(&v)));
        if dup_in_batch || is_stored(&v)? {
            duplicates += 1;
            continue;
        }
        v.system_time = system_time;
        v.seq = next_seq + kept.len() as u64;
        by_point.entry(point).or_default().push(kept.len());
        kept.push(v);
    }
    Ok(DedupPlan { kept, duplicates })
}
