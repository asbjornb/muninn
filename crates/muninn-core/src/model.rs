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
    HistoryRequest, LatestRequest, ReadResult, Reader, ReplayRequest, VersionRow, WriteBatch,
    WriteResult,
};
use crate::catalog::{BatchPlan, Catalog, batch_system_time, catalog_time, plan_dedup};
pub use crate::catalog::{CatalogChange, CatalogChangeKind};
use crate::error::Result;
use crate::time::Timestamp;
use crate::types::{SeriesDef, SeriesId, TableDef, Version, Visibility};

/// The complete database state. Immutable once published.
#[derive(Clone, Debug, Default)]
struct State {
    /// Tables and series.
    catalog: Catalog,
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
    /// `last` of SPEC §4.2: the greatest `system_time` of any stored version.
    fn last_system_time(&self) -> Option<Timestamp> {
        self.versions.iter().map(|v| v.system_time).max()
    }

    /// Records a catalog change (validated by the caller) with its SPEC §2.3 time.
    fn with_change(&self, clock: Option<Timestamp>, change: CatalogChangeKind) -> Result<State> {
        let prev = self.catalog_log.last().map(|c| c.system_time);
        let mut next = self.clone();
        next.catalog.apply(&change)?;
        next.catalog_log.push(CatalogChange {
            system_time: catalog_time(prev, self.last_system_time(), clock),
            change,
        });
        Ok(next)
    }

    // -----------------------------------------------------------------------------------
    // Catalog (checks shared with the engine: crate::catalog)
    // -----------------------------------------------------------------------------------

    fn create_table(&self, req: &CreateTable) -> Result<(State, TableDef)> {
        let def = self.catalog.check_create_table(req)?;
        let next = self.with_change(req.clock, CatalogChangeKind::CreateTable(def.clone()))?;
        Ok((next, def))
    }

    fn add_column(&self, req: &AddColumn) -> Result<(State, TableDef)> {
        let column = self.catalog.check_add_column(req)?;
        let next = self.with_change(
            req.clock,
            CatalogChangeKind::AddColumn {
                table: req.table.clone(),
                column,
            },
        )?;
        let def = next.catalog.table(&req.table)?.clone();
        Ok((next, def))
    }

    fn create_series(&self, req: &CreateSeries) -> Result<(State, SeriesDef)> {
        let def = self.catalog.check_create_series(req)?;
        let next = self.with_change(req.clock, CatalogChangeKind::CreateSeries(def.clone()))?;
        Ok((next, def))
    }

    // -----------------------------------------------------------------------------------
    // Writes (SPEC §4)
    // -----------------------------------------------------------------------------------

    fn write(&self, batch: &WriteBatch) -> Result<(Option<State>, WriteResult)> {
        // SPEC §4.4 then §4.1 (shared with the engine).
        let (clock, candidates) = match self
            .catalog
            .validate_batch(batch, |id| self.batch_ids.contains(id))?
        {
            BatchPlan::AlreadyCommitted => {
                return Ok((
                    None,
                    WriteResult {
                        batch_already_committed: true,
                        ..WriteResult::default()
                    },
                ));
            }
            BatchPlan::Candidates { clock, rows } => (clock, rows),
        };

        // SPEC §4.2: batch.system_time = max(c, last + 1us); all rows share it.
        let system_time = batch_system_time(clock, self.last_system_time())?;

        // SPEC §4.3: drop rows content-identical to any stored version (a linear scan
        // here) or to an earlier kept row of this batch.
        let plan = plan_dedup(
            candidates,
            system_time,
            self.versions.len() as u64 + 1,
            |v| Ok(self.versions.iter().any(|old| old.content_identical(v))),
        )?;
        let result = plan.result();
        if plan.kept.is_empty() && batch.batch_id.is_none() {
            // Nothing to record: state unchanged (no seq consumed, `last` unaffected).
            return Ok((None, result));
        }
        let mut next = self.clone();
        next.versions.extend(plan.kept);
        if let Some(id) = &batch.batch_id {
            // SPEC §4.5: an all-duplicate batch still records its batch id.
            next.batch_ids.insert(id.clone());
        }
        Ok((Some(next), result))
    }

    // -----------------------------------------------------------------------------------
    // Reads (SPEC §6): request checks shared with the engine, then naive scans.
    // -----------------------------------------------------------------------------------

    /// The core of SPEC §6.1: for each point with series in `ids` and value time accepted
    /// by `in_range`, its latest visible version unless that is a tombstone. Sorted by
    /// `(series, value_time)`.
    fn latest_rows(
        &self,
        table: &TableDef,
        ids: &BTreeSet<SeriesId>,
        in_range: impl Fn(Timestamp) -> bool,
        vis: Visibility,
    ) -> Result<Vec<VersionRow>> {
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
            .map(|w| self.catalog.version_row(table, w))
            .collect()
    }

    fn latest(&self, req: &LatestRequest) -> Result<ReadResult<VersionRow>> {
        let p = self.catalog.prepare_latest(req)?;
        let rows = self.latest_rows(p.table, &p.ids, |t| p.range.contains(t), p.vis)?;
        Ok(ReadResult {
            columns: p.table.columns.clone(),
            rows,
        })
    }

    fn history(&self, req: &HistoryRequest) -> Result<ReadResult<VersionRow>> {
        let p = self.catalog.prepare_latest(req)?;
        let mut hits: Vec<&Version> = self
            .versions
            .iter()
            .filter(|v| p.ids.contains(&v.series) && p.range.contains(v.value_time))
            .filter(|v| p.vis.admits_version(v))
            .collect();
        hits.sort_by_key(|v| (v.series, v.value_time, v.knowledge_key(p.vis.clock)));
        let rows = hits
            .into_iter()
            .map(|v| self.catalog.version_row(p.table, v))
            .collect::<Result<_>>()?;
        Ok(ReadResult {
            columns: p.table.columns.clone(),
            rows,
        })
    }

    fn replay(&self, req: &ReplayRequest) -> Result<ReadResult<VersionRow>> {
        let p = self.catalog.prepare_replay(req)?;
        let clock = p.vis.clock;
        // System bound applies; the frontier does not. `arrival` bounds t_clock as the
        // open-closed interval (from, to] (SPEC §6.3): exactly the versions that become
        // visible when the frontier moves from `from` to `to`.
        let mut hits: Vec<&Version> = self
            .versions
            .iter()
            .filter(|v| p.ids.contains(&v.series) && p.range.contains(v.value_time))
            .filter(|v| {
                p.vis.admits_version(v) && req.arrival.contains_open_closed(v.t_clock(clock))
            })
            .collect();
        hits.sort_by_key(|v| v.knowledge_key(clock));
        let rows = hits
            .into_iter()
            .map(|v| self.catalog.version_row(p.table, v))
            .collect::<Result<_>>()?;
        Ok(ReadResult {
            columns: p.table.columns.clone(),
            rows,
        })
    }

    fn grid(&self, req: &GridRequest) -> Result<ReadResult<GridRow>> {
        let p = self.catalog.prepare_grid(req)?;
        // SPEC §6.4 is defined as n `latest` queries; the model does exactly that.
        let mut rows = Vec::new();
        for &f in &p.frontiers {
            let range = p.range_for(f)?;
            let vis = Visibility {
                clock: p.clock,
                frontier: Some(f),
                system: p.system,
            };
            for row in self.latest_rows(p.table, &p.ids, |t| range.contains(t), vis)? {
                rows.push(GridRow { frontier: f, row });
            }
        }
        Ok(ReadResult {
            columns: p.table.columns.clone(),
            rows,
        })
    }

    fn asof_join(&self, req: &AsofJoinRequest) -> Result<ReadResult<AsofRow>> {
        let p = self.catalog.prepare_asof(req)?;
        let mut rows = Vec::with_capacity(req.left.len());
        for (left, s) in req.left.iter().zip(&p.series) {
            let vis = Visibility {
                clock: p.clock,
                frontier: Some(left.frontier),
                system: p.system,
            };
            // Exact value_time match (SPEC §6.5); at most one point qualifies.
            let ids = BTreeSet::from([s.id]);
            let mut hits = self.latest_rows(p.table, &ids, |t| t == left.value_time, vis)?;
            debug_assert!(hits.len() <= 1);
            rows.push(AsofRow {
                series_id: s.id,
                series: Arc::clone(&s.name),
                value_time: left.value_time,
                frontier: left.frontier,
                right: hits.pop(),
            });
        }
        Ok(ReadResult {
            columns: p.table.columns.clone(),
            rows,
        })
    }
}

impl Reader for ModelSnapshot {
    fn table(&self, name: &str) -> Result<TableDef> {
        self.state.catalog.table(name).cloned()
    }

    fn tables(&self) -> Result<Vec<TableDef>> {
        Ok(self.state.catalog.tables().to_vec())
    }

    fn series(&self, name: &str) -> Result<SeriesDef> {
        self.state.catalog.series_by_name(name).cloned()
    }

    fn table_series(&self, table: &str) -> Result<Vec<SeriesDef>> {
        self.state.catalog.table_series(table)
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

    fn create_series_batch(&self, reqs: &[CreateSeries]) -> Result<Vec<SeriesDef>> {
        self.commit(|s| {
            // Literally the sequence of single creations, published only if all succeed.
            let mut next: Option<State> = None;
            let mut defs = Vec::with_capacity(reqs.len());
            for req in reqs {
                let (n, d) = next.as_ref().unwrap_or(s).create_series(req)?;
                next = Some(n);
                defs.push(d);
            }
            Ok((next, defs))
        })
    }

    fn write(&self, batch: &WriteBatch) -> Result<WriteResult> {
        self.commit(|s| s.write(batch))
    }
}

#[cfg(test)]
mod tests;
