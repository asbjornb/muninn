//! `ModelDb`: a naive, obviously-correct in-memory implementation of SPEC v0.1.
//!
//! This is the Rust oracle the storage engine is differentially tested against, so it
//! optimises for being *checkably* right, not fast:
//!
//! - All state lives in one immutable [`State`] value behind an `Arc`. A write builds a
//!   complete new `State` and publishes it with a single pointer swap only after every
//!   check has passed — so a failed operation has no effect (SPEC §4, §8) and readers
//!   never see a partial batch (SPEC §5.1). A snapshot is just a clone of the `Arc`.
//! - Versions are kept in one `Vec` in commit order; every read is a linear scan.
//! - Derived quantities are recomputed instead of cached: the next `seq` is
//!   `versions.len() + 1` (every stored row takes the next value, SPEC §4.2) and `last` is
//!   the max `system_time` over stored versions.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, PoisonError, RwLock};

use crate::api::{
    AddColumn, AsofJoinRequest, AsofRow, CreateSeries, CreateTable, Database, GridRequest, GridRow,
    GridWindow, HistoryRequest, LatestRequest, ReadResult, Reader, ReplayRequest, VersionRow,
    WriteBatch, WriteResult, WriteRow,
};
use crate::error::{Error, ErrorKind, Result};
use crate::time::Timestamp;
use crate::types::{
    ColumnDef, SeriesDef, SeriesId, TableDef, TimeRange, Version, Visibility, validate_column_name,
    validate_series_name, validate_table_name,
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

/// The complete database state. Immutable once published.
#[derive(Clone, Debug, Default)]
struct State {
    /// Tables in creation order.
    tables: Vec<TableDef>,
    /// Series in creation order; `series[i].id == SeriesId(i + 1)` (SPEC §2.2).
    series: Vec<SeriesDef>,
    /// Every stored version, in commit (= seq) order: `versions[i].seq == i + 1`.
    versions: Vec<Version>,
    /// Every batch id ever committed (SPEC §4.4: remembered forever).
    batch_ids: BTreeSet<String>,
    /// Catalog change log (SPEC §2.3).
    catalog_log: Vec<CatalogChange>,
}

/// The naive in-memory model. See the module docs.
#[derive(Debug, Default)]
pub struct ModelDb {
    state: RwLock<Arc<State>>,
}

/// A pinned snapshot of a [`ModelDb`].
#[derive(Clone, Debug)]
pub struct ModelSnapshot {
    state: Arc<State>,
}

impl ModelDb {
    /// An empty database.
    pub fn new() -> ModelDb {
        ModelDb::default()
    }

    fn current(&self) -> Arc<State> {
        // A panic while holding the lock cannot leave a half-updated State (we only ever
        // swap a fully built Arc), so a poisoned lock is safe to keep using.
        Arc::clone(&self.state.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Runs `f` on the current state; if it succeeds, publishes the new state atomically.
    fn commit<T>(&self, f: impl FnOnce(&State) -> Result<(Option<State>, T)>) -> Result<T> {
        let mut guard = self.state.write().unwrap_or_else(PoisonError::into_inner);
        let (next, out) = f(&guard)?;
        if let Some(next) = next {
            *guard = Arc::new(next);
        }
        Ok(out)
    }

    /// The catalog change log (SPEC §2.3), oldest first.
    pub fn catalog_log(&self) -> Vec<CatalogChange> {
        self.current().catalog_log.clone()
    }

    /// Every stored version in commit order (for tests and differential checks).
    pub fn versions(&self) -> Vec<Version> {
        self.current().versions.clone()
    }
}

impl State {
    fn table(&self, name: &str) -> Result<&TableDef> {
        self.tables
            .iter()
            .find(|t| t.name == name)
            .ok_or_else(|| Error::new(ErrorKind::UnknownTable, format!("no table {name:?}")))
    }

    fn table_mut(&mut self, name: &str) -> Result<&mut TableDef> {
        self.tables
            .iter_mut()
            .find(|t| t.name == name)
            .ok_or_else(|| Error::new(ErrorKind::UnknownTable, format!("no table {name:?}")))
    }

    fn series_by_name(&self, name: &str) -> Result<&SeriesDef> {
        self.series
            .iter()
            .find(|s| &*s.name == name)
            .ok_or_else(|| Error::new(ErrorKind::UnknownSeries, format!("no series {name:?}")))
    }

    fn series_by_id(&self, id: SeriesId) -> &SeriesDef {
        &self.series[id.0 as usize - 1]
    }

    /// `last` of SPEC §4.2: the greatest `system_time` of any stored version.
    fn last_system_time(&self) -> Option<Timestamp> {
        self.versions.iter().map(|v| v.system_time).max()
    }

    /// Catalog change timestamps: the supplied clock reading (or, without one, the latest
    /// timestamp known to the model), never before the previous catalog change. The
    /// catalog time never feeds into version `system_time` (SPEC §4.2 `last` counts only
    /// stored versions).
    fn catalog_time(&self, clock: Option<Timestamp>) -> Timestamp {
        let prev = self.catalog_log.last().map(|c| c.system_time);
        let base = clock
            .or_else(|| prev.max(self.last_system_time()))
            .unwrap_or(Timestamp::UNIX_EPOCH);
        base.max(prev.unwrap_or(Timestamp::MIN))
    }

    // -----------------------------------------------------------------------------------
    // Catalog
    // -----------------------------------------------------------------------------------
    //
    // Check order for catalog operations (the spec does not fix one; see
    // crates/SPEC-QUESTIONS.md): (1) referenced table exists, (2) every name is valid,
    // (3) uniqueness.

    fn create_table(&self, req: &CreateTable) -> Result<(State, TableDef)> {
        validate_table_name(&req.name)?;
        for c in &req.columns {
            validate_column_name(&c.name)?;
        }
        if self.tables.iter().any(|t| t.name == req.name) {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("table {:?} already exists", req.name),
            ));
        }
        for (i, c) in req.columns.iter().enumerate() {
            if req.columns[..i].iter().any(|p| p.name == c.name) {
                return Err(Error::new(
                    ErrorKind::AlreadyExists,
                    format!("duplicate column {:?} in table {:?}", c.name, req.name),
                ));
            }
        }
        let def = TableDef {
            name: req.name.clone(),
            columns: req.columns.clone(),
            default_clock: req.default_clock,
            partition: req.partition,
        };
        let mut next = self.clone();
        next.catalog_log.push(CatalogChange {
            system_time: self.catalog_time(req.clock),
            change: CatalogChangeKind::CreateTable(def.clone()),
        });
        next.tables.push(def.clone());
        Ok((next, def))
    }

    fn add_column(&self, req: &AddColumn) -> Result<(State, TableDef)> {
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
        let mut next = self.clone();
        next.catalog_log.push(CatalogChange {
            system_time: self.catalog_time(req.clock),
            change: CatalogChangeKind::AddColumn {
                table: req.table.clone(),
                column: req.column.clone(),
            },
        });
        let t = next.table_mut(&req.table)?;
        t.columns.push(req.column.clone());
        let def = t.clone();
        Ok((next, def))
    }

    fn create_series(&self, req: &CreateSeries) -> Result<(State, SeriesDef)> {
        self.table(&req.table)?;
        validate_series_name(&req.name)?;
        if self.series.iter().any(|s| *s.name == *req.name) {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("series {:?} already exists", req.name),
            ));
        }
        let id = u32::try_from(self.series.len() + 1)
            .map(SeriesId)
            .map_err(|_| Error::internal("series id space exhausted"))?;
        let def = SeriesDef {
            id,
            name: Arc::from(req.name.as_str()),
            table: req.table.clone(),
            tags: req.tags.clone(),
            timezone: req.timezone.clone().unwrap_or_else(|| "UTC".to_owned()),
            resolution: req.resolution,
        };
        let mut next = self.clone();
        next.catalog_log.push(CatalogChange {
            system_time: self.catalog_time(req.clock),
            change: CatalogChangeKind::CreateSeries(def.clone()),
        });
        next.series.push(def.clone());
        Ok((next, def))
    }

    // -----------------------------------------------------------------------------------
    // Writes (SPEC §4)
    // -----------------------------------------------------------------------------------

    /// SPEC §4.1: validates one row, checks in the normative order (first failure wins).
    /// Returns the version content (system_time/seq are filled in later).
    fn validate_row(&self, row: &WriteRow) -> Result<Version> {
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
            if let Some(v) = value {
                let i = table.column_index(name).expect("checked in step 2");
                let coerced = v.coerce_to(table.columns[i].ty).map_err(|e| {
                    Error::new(e.kind(), format!("column {name:?}: {}", e.message()))
                })?;
                values[i] = Some(coerced);
            }
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
            system_time: Timestamp::MIN, // placeholder, set in `write`
            seq: 0,                      // placeholder, set in `write`
        })
    }

    fn write(&self, batch: &WriteBatch) -> Result<(Option<State>, WriteResult)> {
        // SPEC §4.4: a batch id must be non-empty; a known one makes the write a no-op
        // without any validation.
        if let Some(id) = &batch.batch_id {
            if id.is_empty() {
                return Err(Error::new(
                    ErrorKind::InvalidName,
                    "batch_id must be non-empty",
                ));
            }
            if self.batch_ids.contains(id) {
                return Ok((
                    None,
                    WriteResult {
                        batch_already_committed: true,
                        ..WriteResult::default()
                    },
                ));
            }
        }

        // SPEC §4.1: rows checked in input order; any failure rejects the whole batch.
        let mut candidates = Vec::with_capacity(batch.rows.len());
        for (i, row) in batch.rows.iter().enumerate() {
            let v = self
                .validate_row(row)
                .map_err(|e| Error::new(e.kind(), format!("row {i}: {}", e.message())))?;
            candidates.push(v);
        }

        // SPEC §4.2: batch.system_time = max(c, last); all rows share it.
        let system_time = match self.last_system_time() {
            Some(last) => batch.clock.max(last),
            None => batch.clock,
        };

        // SPEC §4.3: drop rows content-identical to any stored version or to an earlier
        // *kept* row of this batch. Kept rows take consecutive seqs in input order.
        let mut kept: Vec<Version> = Vec::new();
        let mut duplicates = 0u64;
        for mut v in candidates {
            let dup = self
                .versions
                .iter()
                .chain(kept.iter())
                .any(|old| old.content_identical(&v));
            if dup {
                duplicates += 1;
                continue;
            }
            v.system_time = system_time;
            v.seq = (self.versions.len() + kept.len() + 1) as u64;
            kept.push(v);
        }

        let stored = kept.len() as u64;
        let result = WriteResult {
            stored,
            duplicates,
            batch_already_committed: false,
            system_time: (stored > 0).then_some(system_time),
            first_seq: kept.first().map(|v| v.seq),
        };
        if kept.is_empty() && batch.batch_id.is_none() {
            // Nothing to record: state unchanged (no seq consumed, `last` unaffected).
            return Ok((None, result));
        }
        let mut next = self.clone();
        next.versions.extend(kept);
        if let Some(id) = &batch.batch_id {
            // SPEC §4.5: an all-duplicate batch still records its batch id.
            next.batch_ids.insert(id.clone());
        }
        Ok((Some(next), result))
    }

    // -----------------------------------------------------------------------------------
    // Reads (SPEC §6)
    // -----------------------------------------------------------------------------------
    //
    // Check order for reads: table, then series filter, then ranges/windows.

    /// Resolves a series filter (SPEC §6): empty = every series of the table; a name that
    /// does not exist or belongs to another table is `unknown_series`.
    fn series_filter(&self, table: &TableDef, names: &[String]) -> Result<BTreeSet<SeriesId>> {
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

    fn to_row(&self, table: &TableDef, v: &Version) -> VersionRow {
        VersionRow {
            series_id: v.series,
            series: Arc::clone(&self.series_by_id(v.series).name),
            value_time: v.value_time,
            published_time: v.published_time,
            received_time: v.received_time,
            system_time: v.system_time,
            seq: v.seq,
            deleted: v.deleted,
            // Every *current* column; columns added after the version was written are null.
            values: (0..table.columns.len())
                .map(|i| v.value(i).cloned())
                .collect(),
        }
    }

    /// The core of SPEC §6.1: for each point with series in `ids` and value time accepted
    /// by `in_range`, its latest visible version unless that is a tombstone. Sorted by
    /// `(series, value_time)`.
    fn latest_rows(
        &self,
        table: &TableDef,
        ids: &BTreeSet<SeriesId>,
        in_range: impl Fn(Timestamp) -> bool,
        vis: Visibility,
    ) -> Vec<VersionRow> {
        let mut best: BTreeMap<(SeriesId, Timestamp), &Version> = BTreeMap::new();
        for v in &self.versions {
            if !(ids.contains(&v.series) && in_range(v.value_time) && vis.admits_version(v)) {
                continue;
            }
            best.entry((v.series, v.value_time))
                .and_modify(|w| {
                    if v.knowledge_key(vis.clock) > w.knowledge_key(vis.clock) {
                        *w = v;
                    }
                })
                .or_insert(v);
        }
        best.values()
            .filter(|w| !w.deleted)
            .map(|w| self.to_row(table, w))
            .collect()
    }

    fn latest(&self, req: &LatestRequest) -> Result<ReadResult<VersionRow>> {
        let table = self.table(&req.table)?;
        let ids = self.series_filter(table, &req.series)?;
        req.range.validate()?;
        let vis = Visibility {
            clock: req.context.clock.unwrap_or(table.default_clock),
            frontier: req.context.frontier,
            system: req.context.system,
        };
        let rows = self.latest_rows(table, &ids, |t| req.range.contains(t), vis);
        Ok(ReadResult {
            columns: table.columns.clone(),
            rows,
        })
    }

    fn history(&self, req: &HistoryRequest) -> Result<ReadResult<VersionRow>> {
        let table = self.table(&req.table)?;
        let ids = self.series_filter(table, &req.series)?;
        req.range.validate()?;
        let vis = Visibility {
            clock: req.context.clock.unwrap_or(table.default_clock),
            frontier: req.context.frontier,
            system: req.context.system,
        };
        let mut hits: Vec<&Version> = self
            .versions
            .iter()
            .filter(|v| ids.contains(&v.series) && req.range.contains(v.value_time))
            .filter(|v| vis.admits_version(v))
            .collect();
        hits.sort_by_key(|v| (v.series, v.value_time, v.knowledge_key(vis.clock)));
        let rows = hits.into_iter().map(|v| self.to_row(table, v)).collect();
        Ok(ReadResult {
            columns: table.columns.clone(),
            rows,
        })
    }

    fn replay(&self, req: &ReplayRequest) -> Result<ReadResult<VersionRow>> {
        let table = self.table(&req.table)?;
        let ids = self.series_filter(table, &req.series)?;
        req.range.validate()?;
        req.arrival.validate()?;
        let clock = req.clock.unwrap_or(table.default_clock);
        // System bound applies; the frontier does not — `arrival` bounds t_clock instead.
        let vis = Visibility {
            clock,
            frontier: None,
            system: req.system,
        };
        let mut hits: Vec<&Version> = self
            .versions
            .iter()
            .filter(|v| ids.contains(&v.series) && req.range.contains(v.value_time))
            .filter(|v| vis.admits_version(v) && req.arrival.contains(v.t_clock(clock)))
            .collect();
        hits.sort_by_key(|v| v.knowledge_key(clock));
        let rows = hits.into_iter().map(|v| self.to_row(table, v)).collect();
        Ok(ReadResult {
            columns: table.columns.clone(),
            rows,
        })
    }

    fn grid(&self, req: &GridRequest) -> Result<ReadResult<GridRow>> {
        let table = self.table(&req.table)?;
        let ids = self.series_filter(table, &req.series)?;
        match req.window {
            GridWindow::Absolute(r) => r.validate()?,
            GridWindow::Relative { start, end } => {
                // Validate even when there are no frontiers.
                TimeRange::relative_window(Timestamp::UNIX_EPOCH, start, end)?;
            }
        }
        let clock = req.clock.unwrap_or(table.default_clock);
        // SPEC §6.4 is defined as n `latest` queries; the model does exactly that.
        let frontiers: BTreeSet<Timestamp> = req.frontiers.iter().copied().collect();
        let mut rows = Vec::new();
        for f in frontiers {
            let range = match req.window {
                GridWindow::Absolute(r) => r,
                GridWindow::Relative { start, end } => TimeRange::relative_window(f, start, end)?,
            };
            let vis = Visibility {
                clock,
                frontier: Some(f),
                system: req.system,
            };
            for row in self.latest_rows(table, &ids, |t| range.contains(t), vis) {
                rows.push(GridRow { frontier: f, row });
            }
        }
        Ok(ReadResult {
            columns: table.columns.clone(),
            rows,
        })
    }

    fn asof_join(&self, req: &AsofJoinRequest) -> Result<ReadResult<AsofRow>> {
        let table = self.table(&req.table)?;
        let clock = req.clock.unwrap_or(table.default_clock);
        let mut rows = Vec::with_capacity(req.left.len());
        for left in &req.left {
            let ids = self.series_filter(table, std::slice::from_ref(&left.series))?;
            let vis = Visibility {
                clock,
                frontier: Some(left.frontier),
                system: req.system,
            };
            // Exact value_time match (SPEC §6.5); at most one point qualifies.
            let mut hits = self.latest_rows(table, &ids, |t| t == left.value_time, vis);
            debug_assert!(hits.len() <= 1);
            let s = self.series_by_name(&left.series)?;
            rows.push(AsofRow {
                series_id: s.id,
                series: Arc::clone(&s.name),
                value_time: left.value_time,
                frontier: left.frontier,
                right: hits.pop(),
            });
        }
        Ok(ReadResult {
            columns: table.columns.clone(),
            rows,
        })
    }
}

impl Reader for ModelSnapshot {
    fn table(&self, name: &str) -> Result<TableDef> {
        self.state.table(name).cloned()
    }

    fn tables(&self) -> Result<Vec<TableDef>> {
        Ok(self.state.tables.clone())
    }

    fn series(&self, name: &str) -> Result<SeriesDef> {
        self.state.series_by_name(name).cloned()
    }

    fn table_series(&self, table: &str) -> Result<Vec<SeriesDef>> {
        self.state.table(table)?;
        Ok(self
            .state
            .series
            .iter()
            .filter(|s| s.table == table)
            .cloned()
            .collect())
    }

    fn latest(&self, req: &LatestRequest) -> Result<ReadResult<VersionRow>> {
        self.state.latest(req)
    }

    fn history(&self, req: &HistoryRequest) -> Result<ReadResult<VersionRow>> {
        self.state.history(req)
    }

    fn replay(&self, req: &ReplayRequest) -> Result<ReadResult<VersionRow>> {
        self.state.replay(req)
    }

    fn grid(&self, req: &GridRequest) -> Result<ReadResult<GridRow>> {
        self.state.grid(req)
    }

    fn asof_join(&self, req: &AsofJoinRequest) -> Result<ReadResult<AsofRow>> {
        self.state.asof_join(req)
    }
}

impl Reader for ModelDb {
    fn table(&self, name: &str) -> Result<TableDef> {
        self.snapshot()?.table(name)
    }

    fn tables(&self) -> Result<Vec<TableDef>> {
        self.snapshot()?.tables()
    }

    fn series(&self, name: &str) -> Result<SeriesDef> {
        self.snapshot()?.series(name)
    }

    fn table_series(&self, table: &str) -> Result<Vec<SeriesDef>> {
        self.snapshot()?.table_series(table)
    }

    fn latest(&self, req: &LatestRequest) -> Result<ReadResult<VersionRow>> {
        self.snapshot()?.latest(req)
    }

    fn history(&self, req: &HistoryRequest) -> Result<ReadResult<VersionRow>> {
        self.snapshot()?.history(req)
    }

    fn replay(&self, req: &ReplayRequest) -> Result<ReadResult<VersionRow>> {
        self.snapshot()?.replay(req)
    }

    fn grid(&self, req: &GridRequest) -> Result<ReadResult<GridRow>> {
        self.snapshot()?.grid(req)
    }

    fn asof_join(&self, req: &AsofJoinRequest) -> Result<ReadResult<AsofRow>> {
        self.snapshot()?.asof_join(req)
    }
}

impl Database for ModelDb {
    type Snapshot = ModelSnapshot;

    fn snapshot(&self) -> Result<ModelSnapshot> {
        Ok(ModelSnapshot {
            state: self.current(),
        })
    }

    fn create_table(&self, req: &CreateTable) -> Result<TableDef> {
        self.commit(|s| s.create_table(req).map(|(n, d)| (Some(n), d)))
    }

    fn add_column(&self, req: &AddColumn) -> Result<TableDef> {
        self.commit(|s| s.add_column(req).map(|(n, d)| (Some(n), d)))
    }

    fn create_series(&self, req: &CreateSeries) -> Result<SeriesDef> {
        self.commit(|s| s.create_series(req).map(|(n, d)| (Some(n), d)))
    }

    fn write(&self, batch: &WriteBatch) -> Result<WriteResult> {
        self.commit(|s| s.write(batch))
    }
}

#[cfg(test)]
mod tests;
