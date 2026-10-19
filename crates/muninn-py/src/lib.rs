//! # muninn-py
//!
//! The native half of the `muninn` Python package (`muninn._native`): a thin PyO3 layer
//! over [`muninn_engine::Engine`]. The Python package (`python/muninn`) owns the public API,
//! input normalisation (pandas / polars / numpy / datetime) and receipts replay; this crate
//! owns everything that touches the engine:
//!
//! - structural checks (SPEC §10.3) and the §10.4 check order for reads,
//! - Arrow input (any object with `__arrow_c_stream__`, via the Arrow PyCapsule interface)
//!   and Arrow output (`pyarrow.Table`), with `arrow-array` / `arrow-schema` only,
//! - reproducibility receipts (SPEC §11) in the result's schema metadata,
//! - releasing the GIL around every engine call.

#![forbid(unsafe_code)]

mod input;
mod output;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_pyarrow::{FromPyArrow, IntoPyArrow, Table};
use muninn_core::api::{
    AddColumn, AsofJoinRequest, AsofLeftRow, CreateSeries, CreateTable, GridRequest, GridWindow,
    LatestRequest, ReadContext, ReplayRequest, TimeInput, WriteBatch,
};
use muninn_core::types::{SeriesDef, TableDef};
use muninn_core::{
    Clock, ColumnDef, ColumnType, Database as _, Duration, Error, ErrorKind, Partitioning, Reader,
    Result, TimeRange, Timestamp,
};
use muninn_engine::{Engine, EngineOptions, EngineSnapshot};
use muninn_store::{LocalFsStore, MemoryStore, Store};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use serde_json::{Value as Json, json};

use input::{ConvError, DurInput, Helpers, LeftRow, invalid};

type DynStore = Arc<dyn Store>;
type Snap = EngineSnapshot<DynStore>;
/// `(stored, duplicates, batch_already_committed, system_time_us, first_seq)`.
type WriteTuple = (u64, u64, bool, Option<i64>, Option<u64>);

// ---------------------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------------------

/// A Muninn error as the matching `muninn.MuninnError` subclass.
fn py_err(py: Python<'_>, e: &Error) -> PyErr {
    let make = || -> PyResult<PyErr> {
        let exc = py
            .import("muninn._errors")?
            .getattr("error_for_code")?
            .call1((e.kind().name(), e.message()))?;
        Ok(PyErr::from_value(exc))
    };
    make().unwrap_or_else(|err| err)
}

fn conv_err(py: Python<'_>, e: ConvError) -> PyErr {
    match e {
        ConvError::Muninn(e) => py_err(py, &e),
        ConvError::Py(e) => e,
    }
}

trait OrPy<T> {
    fn or_py(self, py: Python<'_>) -> PyResult<T>;
}

impl<T> OrPy<T> for Result<T> {
    fn or_py(self, py: Python<'_>) -> PyResult<T> {
        self.map_err(|e| py_err(py, &e))
    }
}

fn store_err(e: muninn_store::StoreError) -> Error {
    Error::new(ErrorKind::Io, e.to_string())
}

// ---------------------------------------------------------------------------------------
// Catalog → Python
// ---------------------------------------------------------------------------------------

fn table_dict<'py>(py: Python<'py>, t: &TableDef) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("name", &t.name)?;
    let cols = PyList::empty(py);
    for c in &t.columns {
        cols.append((c.name.as_str(), c.ty.name()))?;
    }
    d.set_item("columns", cols)?;
    d.set_item("default_clock", t.default_clock.name())?;
    d.set_item("partition", t.partition.name())?;
    Ok(d)
}

fn series_dict<'py>(py: Python<'py>, s: &SeriesDef) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("id", s.id.0)?;
    d.set_item("name", &*s.name)?;
    d.set_item("table", &s.table)?;
    let tags = PyDict::new(py);
    for (k, v) in &s.tags {
        tags.set_item(k, v)?;
    }
    d.set_item("tags", tags)?;
    d.set_item("timezone", &s.timezone)?;
    d.set_item("resolution", s.resolution.map(Duration::as_micros))?;
    Ok(d)
}

// ---------------------------------------------------------------------------------------
// Receipts (SPEC §11)
// ---------------------------------------------------------------------------------------

fn ts_json(t: Option<Timestamp>) -> Json {
    t.map_or(Json::Null, |t| Json::String(t.to_string()))
}

fn dur_json(d: Duration) -> Json {
    Json::String(format!("{}us", d.as_micros()))
}

/// How a read is shaped for a receipt replay: the value columns as of the receipt, and
/// whether the receipt was taken on an empty snapshot (SPEC §11 `EMPTY`: no version is
/// visible, whatever has been written since).
#[derive(Clone, Debug, Default)]
struct AsOf {
    columns: Option<Vec<String>>,
    empty: bool,
}

/// The marker for the system bound of a receipt taken on an empty snapshot.
const EMPTY: &str = "EMPTY";

impl AsOf {
    /// The output columns: the table's current ones, or exactly the receipt's (in its
    /// order; the catalog is additive, so they all still exist).
    fn project(&self, table: &TableDef) -> Result<(Vec<ColumnDef>, Vec<usize>)> {
        match &self.columns {
            None => Ok((table.columns.clone(), (0..table.columns.len()).collect())),
            Some(names) => {
                let mut defs = Vec::with_capacity(names.len());
                let mut idx = Vec::with_capacity(names.len());
                for n in names {
                    let i = table.column_index(n).ok_or_else(|| {
                        invalid(format!(
                            "receipt column {n:?} is not in table {:?}",
                            table.name
                        ))
                    })?;
                    defs.push(table.columns[i].clone());
                    idx.push(i);
                }
                Ok((defs, idx))
            }
        }
    }
}

fn pick(values: &mut Vec<Option<muninn_core::Value>>, idx: &[usize]) {
    if idx.iter().enumerate().all(|(k, &i)| k == i) {
        values.resize(idx.len(), None); // the table's own columns, in order
        return;
    }
    *values = idx
        .iter()
        .map(|&i| values.get(i).cloned().flatten())
        .collect();
}

/// `(table, op, request, clock, F, S)` with `S` the effective system bound of SPEC §11:
/// `min(S_given, L)`, `L` if none was given, `EMPTY` if the snapshot is empty (`L` its
/// greatest system_time). The output columns are recorded too, so that a replay returns
/// the same schema after later `add_column`s.
#[allow(clippy::too_many_arguments)]
fn receipt(
    snap: &Snap,
    table: &TableDef,
    columns: &[ColumnDef],
    as_of: &AsOf,
    op: &str,
    clock: Option<Clock>,
    frontier: Option<Timestamp>,
    system: Option<Timestamp>,
    request: Json,
) -> String {
    let s = match (as_of.empty, system, snap.last_system_time()) {
        (true, _, _) | (false, _, None) => Json::String(EMPTY.into()),
        (false, Some(s), Some(l)) => ts_json(Some(s.min(l))),
        (false, None, Some(l)) => ts_json(Some(l)),
    };
    let r = json!({
        "muninn_receipt": 1,
        "table": table.name,
        "op": op,
        "request": request,
        "clock": clock.unwrap_or(table.default_clock).name(),
        "frontier": ts_json(frontier),
        "system": s,
        "columns": columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>(),
    });
    r.to_string()
}

/// Projects a `latest` / `history` / `replay` result to the output columns (and empties
/// it for an `EMPTY` receipt).
fn shape_versions(
    res: &mut muninn_core::api::ReadResult<muninn_core::api::VersionRow>,
    as_of: &AsOf,
    cols: (Vec<ColumnDef>, Vec<usize>),
) {
    if as_of.empty {
        res.rows.clear();
    }
    for r in &mut res.rows {
        pick(&mut r.values, &cols.1);
    }
    res.columns = cols.0;
}

// ---------------------------------------------------------------------------------------
// Reads: the §10.4 order after structure
// ---------------------------------------------------------------------------------------

/// Structure-checked arguments common to every read.
struct Common {
    table: Result<String>,
    series: Vec<String>,
    clock: Option<Clock>,
    value_from: Option<TimeInput>,
    value_to: Option<TimeInput>,
    system: Option<TimeInput>,
}

impl Common {
    fn extract(
        table: &Bound<'_, PyAny>,
        series: Option<&Bound<'_, PyAny>>,
        clock: Option<&Bound<'_, PyAny>>,
        value_from: Option<&Bound<'_, PyAny>>,
        value_to: Option<&Bound<'_, PyAny>>,
        system: Option<&Bound<'_, PyAny>>,
    ) -> Result<Common> {
        Ok(Common {
            clock: input::opt_clock(clock)?,
            series: input::series_list(series)?,
            table: input::table_name(table),
            value_from: input::opt_time(value_from),
            value_to: input::opt_time(value_to),
            system: input::opt_time(system),
        })
    }

    /// `unknown_table`, then `unknown_series` (filter names in order).
    fn resolve(&self, snap: &Snap) -> Result<TableDef> {
        let t = snap.table(self.table.as_ref().map_err(Clone::clone)?)?;
        for name in &self.series {
            check_series(snap, &t, name)?;
        }
        Ok(t)
    }

    fn range(&self) -> Result<TimeRange> {
        Ok(TimeRange::new(
            input::resolve_time(&self.value_from, "value_from")?,
            input::resolve_time(&self.value_to, "value_to")?,
        ))
    }

    fn base_request(&self, range: &TimeRange) -> serde_json::Map<String, Json> {
        let mut m = serde_json::Map::new();
        m.insert("series".into(), json!(self.series));
        m.insert("value_from".into(), ts_json(range.from));
        m.insert("value_to".into(), ts_json(range.to));
        m
    }
}

fn check_series(snap: &Snap, table: &TableDef, name: &str) -> Result<()> {
    let s = snap.series(name)?;
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

fn to_table(py: Python<'_>, batch: RecordBatch) -> PyResult<Py<PyAny>> {
    let schema = batch.schema();
    let t = Table::try_new(vec![batch], schema)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(t.into_pyarrow(py)?.unbind())
}

/// A lead-time filter from Python: `(column, min, max)`.
type LeadArg = (Result<String>, DurInput, DurInput);

fn lead_arg(obj: Option<&Bound<'_, PyAny>>) -> Result<Option<LeadArg>> {
    let Some(o) = obj.filter(|o| !o.is_none()) else {
        return Ok(None);
    };
    let t = o
        .cast::<PyTuple>()
        .map_err(|_| invalid("lead must be a (column, min, max) tuple"))?;
    if t.len() != 3 {
        return Err(invalid("lead must be a (column, min, max) tuple"));
    }
    let item = |i: usize| t.get_item(i).map_err(|_| invalid("bad lead"));
    Ok(Some((
        req_str(&item(0)?, "lead column"),
        DurInput::from_py(&item(1)?),
        DurInput::from_py(&item(2)?),
    )))
}

fn latest_or_history(
    snap: &Snap,
    c: &Common,
    frontier: &Option<TimeInput>,
    history: bool,
    as_of: &AsOf,
    lead: Option<&LeadArg>,
) -> Result<RecordBatch> {
    let t = c.resolve(snap)?;
    let cols = as_of.project(&t)?;
    let range = c.range()?;
    let frontier = input::resolve_time(frontier, "frontier")?;
    let system = input::resolve_time(&c.system, "system")?;
    let req = LatestRequest {
        table: t.name.clone(),
        series: c.series.clone(),
        range,
        context: ReadContext {
            clock: c.clock,
            frontier,
            system,
        },
    };
    let lead = match lead {
        None => None,
        Some((column, lo, hi)) => Some(muninn_engine::LeadFilter {
            column: column.clone()?,
            min: lo.resolve("lead min")?,
            max: hi.resolve("lead max")?,
        }),
    };
    let mut res = match (&lead, history) {
        (Some(l), true) => snap.history_lead(&req, l)?,
        (None, true) => snap.history(&req)?,
        (_, false) => snap.latest(&req)?,
    };
    shape_versions(&mut res, as_of, cols);
    let op = if history { "history" } else { "latest" };
    let mut request = c.base_request(&range);
    if let Some(l) = &lead {
        request.insert(
            "lead".into(),
            json!([l.column, dur_json(l.min), dur_json(l.max)]),
        );
    }
    let r = receipt(
        snap,
        &t,
        &res.columns,
        as_of,
        op,
        c.clock,
        frontier,
        system,
        Json::Object(request),
    );
    output::versions_batch(&res, history, r)
}

fn replay(
    snap: &Snap,
    c: &Common,
    from: &Option<TimeInput>,
    to: &Option<TimeInput>,
    as_of: &AsOf,
) -> Result<RecordBatch> {
    let t = c.resolve(snap)?;
    let cols = as_of.project(&t)?;
    let range = c.range()?;
    let system = input::resolve_time(&c.system, "system")?;
    let arrival = TimeRange::new(
        input::resolve_time(from, "from")?,
        input::resolve_time(to, "to")?,
    );
    let req = ReplayRequest {
        table: t.name.clone(),
        series: c.series.clone(),
        range,
        clock: c.clock,
        system,
        arrival,
    };
    let mut res = snap.replay(&req)?;
    shape_versions(&mut res, as_of, cols);
    let mut m = c.base_request(&range);
    m.insert("from".into(), ts_json(arrival.from));
    m.insert("to".into(), ts_json(arrival.to));
    let r = receipt(
        snap,
        &t,
        &res.columns,
        as_of,
        "replay",
        c.clock,
        None,
        system,
        Json::Object(m),
    );
    output::versions_batch(&res, true, r)
}

fn grid(
    snap: &Snap,
    c: &Common,
    frontiers: &[TimeInput],
    window: (Option<DurInput>, Option<DurInput>),
    as_of: &AsOf,
) -> Result<RecordBatch> {
    let t = c.resolve(snap)?;
    let cols = as_of.project(&t)?;
    let range = c.range()?;
    let system = input::resolve_time(&c.system, "system")?;
    let fs = frontiers
        .iter()
        .map(|f| {
            f.resolve()
                .map_err(|e| Error::new(e.kind(), format!("frontiers: {}", e.message())))
        })
        .collect::<Result<Vec<_>>>()?;
    // SPEC §10.5.
    let absolute = range.from.is_some() || range.to.is_some();
    let w = match window {
        (None, None) => GridWindow::Absolute(range),
        _ if absolute => {
            return Err(Error::new(
                ErrorKind::InvalidRange,
                "grid window is either absolute (value_from/value_to) or relative (window), not both",
            ));
        }
        (Some(s), Some(e)) => GridWindow::Relative {
            start: s.resolve("window start")?,
            end: e.resolve("window end")?,
        },
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidRange,
                "a relative window needs both a start and an end",
            ));
        }
    };
    let req = GridRequest {
        table: t.name.clone(),
        series: c.series.clone(),
        frontiers: fs.clone(),
        window: w,
        clock: c.clock,
        system,
    };
    let mut res = snap.grid(&req)?;
    if as_of.empty {
        res.rows.clear();
    }
    for r in &mut res.rows {
        pick(&mut r.row.values, &cols.1);
    }
    res.columns = cols.0;
    let mut m = c.base_request(&range);
    m.insert(
        "frontiers".into(),
        Json::Array(fs.iter().map(|f| ts_json(Some(*f))).collect()),
    );
    if let GridWindow::Relative { start, end } = w {
        m.insert("window".into(), json!([dur_json(start), dur_json(end)]));
    } else {
        m.insert("window".into(), Json::Null);
    }
    let r = receipt(
        snap,
        &t,
        &res.columns,
        as_of,
        "grid",
        c.clock,
        None,
        system,
        Json::Object(m),
    );
    output::grid_batch(&res, r)
}

fn asof_join(
    snap: &Snap,
    table: &Result<String>,
    clock: Option<Clock>,
    system: &Option<TimeInput>,
    left: &[LeftRow],
    as_of: &AsOf,
) -> Result<RecordBatch> {
    let t = snap.table(table.as_ref().map_err(Clone::clone)?)?;
    let cols = as_of.project(&t)?;
    for l in left {
        check_series(snap, &t, &l.series)?;
    }
    let system = input::resolve_time(system, "system")?;
    let mut rows = Vec::with_capacity(left.len());
    for l in left {
        rows.push(AsofLeftRow {
            series: l.series.clone(),
            value_time: l
                .value_time
                .resolve()
                .map_err(|e| Error::new(e.kind(), format!("left value_time: {}", e.message())))?,
            frontier: l
                .frontier
                .resolve()
                .map_err(|e| Error::new(e.kind(), format!("left frontier: {}", e.message())))?,
        });
    }
    let req = AsofJoinRequest {
        table: t.name.clone(),
        clock,
        system,
        left: rows,
    };
    let mut res = snap.asof_join(&req)?;
    for r in &mut res.rows {
        if as_of.empty {
            r.right = None;
        }
        if let Some(v) = &mut r.right {
            pick(&mut v.values, &cols.1);
        }
    }
    res.columns = cols.0;
    let left_json: Vec<Json> = req
        .left
        .iter()
        .map(|l| json!([l.series, l.value_time.to_string(), l.frontier.to_string()]))
        .collect();
    let r = receipt(
        snap,
        &t,
        &res.columns,
        as_of,
        "asof_join",
        clock,
        None,
        system,
        json!({ "left": left_json }),
    );
    output::asof_batch(&res, r)
}

/// Record batches from any object with `__arrow_c_stream__` (collected with the GIL held:
/// the producer's callbacks may need it).
fn arrow_batches(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    let t = Table::from_pyarrow_bound(obj).map_err(|e| {
        py_err(
            py,
            &invalid(format!("cannot read the input as Arrow data: {e}")),
        )
    })?;
    Ok(t.into_inner().0)
}

// ---------------------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------------------

/// A pinned, read-only view of one committed state (SPEC §5.1).
#[pyclass(module = "muninn._native", frozen)]
struct Snapshot {
    snap: Snap,
}

#[pymethods]
impl Snapshot {
    /// The log version this snapshot reflects.
    fn version(&self) -> u64 {
        self.snap.version()
    }

    /// The greatest `system_time` stored in this snapshot (µs), or `None`.
    fn last_system_time(&self) -> Option<i64> {
        self.snap.last_system_time().map(Timestamp::as_micros)
    }

    fn tables<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let ts = self.snap.tables().or_py(py)?;
        ts.iter().map(|t| table_dict(py, t)).collect()
    }

    fn table<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyDict>> {
        table_dict(py, &self.snap.table(name).or_py(py)?)
    }

    /// Series of `table` (or of every table), ordered by id.
    #[pyo3(signature = (table=None))]
    fn series_list<'py>(
        &self,
        py: Python<'py>,
        table: Option<&str>,
    ) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let mut all = Vec::new();
        match table {
            Some(t) => all = self.snap.table_series(t).or_py(py)?,
            None => {
                for t in self.snap.tables().or_py(py)? {
                    all.extend(self.snap.table_series(&t.name).or_py(py)?);
                }
                all.sort_by_key(|s| s.id);
            }
        }
        all.iter().map(|s| series_dict(py, s)).collect()
    }

    #[pyo3(signature = (table, series, value_from, value_to, clock, frontier, system, *, as_of_columns=None, empty=false))]
    #[allow(clippy::too_many_arguments)]
    fn latest(
        &self,
        py: Python<'_>,
        table: &Bound<'_, PyAny>,
        series: Option<&Bound<'_, PyAny>>,
        value_from: Option<&Bound<'_, PyAny>>,
        value_to: Option<&Bound<'_, PyAny>>,
        clock: Option<&Bound<'_, PyAny>>,
        frontier: Option<&Bound<'_, PyAny>>,
        system: Option<&Bound<'_, PyAny>>,
        as_of_columns: Option<Vec<String>>,
        empty: bool,
    ) -> PyResult<Py<PyAny>> {
        let as_of = AsOf {
            columns: as_of_columns,
            empty,
        };
        let c = Common::extract(table, series, clock, value_from, value_to, system).or_py(py)?;
        let f = input::opt_time(frontier);
        let b = py
            .detach(|| latest_or_history(&self.snap, &c, &f, false, &as_of, None))
            .or_py(py)?;
        to_table(py, b)
    }

    #[pyo3(signature = (table, series, value_from, value_to, clock, frontier, system, *, as_of_columns=None, empty=false, lead=None))]
    #[allow(clippy::too_many_arguments)]
    fn history(
        &self,
        py: Python<'_>,
        table: &Bound<'_, PyAny>,
        series: Option<&Bound<'_, PyAny>>,
        value_from: Option<&Bound<'_, PyAny>>,
        value_to: Option<&Bound<'_, PyAny>>,
        clock: Option<&Bound<'_, PyAny>>,
        frontier: Option<&Bound<'_, PyAny>>,
        system: Option<&Bound<'_, PyAny>>,
        as_of_columns: Option<Vec<String>>,
        empty: bool,
        lead: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let as_of = AsOf {
            columns: as_of_columns,
            empty,
        };
        let c = Common::extract(table, series, clock, value_from, value_to, system).or_py(py)?;
        let f = input::opt_time(frontier);
        let lead = lead_arg(lead).or_py(py)?;
        let b = py
            .detach(|| latest_or_history(&self.snap, &c, &f, true, &as_of, lead.as_ref()))
            .or_py(py)?;
        to_table(py, b)
    }

    #[pyo3(signature = (table, series, value_from, value_to, clock, system, from_, to, *, as_of_columns=None, empty=false))]
    #[allow(clippy::too_many_arguments)]
    fn replay(
        &self,
        py: Python<'_>,
        table: &Bound<'_, PyAny>,
        series: Option<&Bound<'_, PyAny>>,
        value_from: Option<&Bound<'_, PyAny>>,
        value_to: Option<&Bound<'_, PyAny>>,
        clock: Option<&Bound<'_, PyAny>>,
        system: Option<&Bound<'_, PyAny>>,
        from_: Option<&Bound<'_, PyAny>>,
        to: Option<&Bound<'_, PyAny>>,
        as_of_columns: Option<Vec<String>>,
        empty: bool,
    ) -> PyResult<Py<PyAny>> {
        let as_of = AsOf {
            columns: as_of_columns,
            empty,
        };
        let c = Common::extract(table, series, clock, value_from, value_to, system).or_py(py)?;
        let (from, to) = (input::opt_time(from_), input::opt_time(to));
        let b = py
            .detach(|| replay(&self.snap, &c, &from, &to, &as_of))
            .or_py(py)?;
        to_table(py, b)
    }

    #[pyo3(signature = (table, frontiers, series, value_from, value_to, window_start, window_end, clock, system, *, as_of_columns=None, empty=false))]
    #[allow(clippy::too_many_arguments)]
    fn grid(
        &self,
        py: Python<'_>,
        table: &Bound<'_, PyAny>,
        frontiers: Option<&Bound<'_, PyAny>>,
        series: Option<&Bound<'_, PyAny>>,
        value_from: Option<&Bound<'_, PyAny>>,
        value_to: Option<&Bound<'_, PyAny>>,
        window_start: Option<&Bound<'_, PyAny>>,
        window_end: Option<&Bound<'_, PyAny>>,
        clock: Option<&Bound<'_, PyAny>>,
        system: Option<&Bound<'_, PyAny>>,
        as_of_columns: Option<Vec<String>>,
        empty: bool,
    ) -> PyResult<Py<PyAny>> {
        let as_of = AsOf {
            columns: as_of_columns,
            empty,
        };
        let c = Common::extract(table, series, clock, value_from, value_to, system).or_py(py)?;
        let fs: Vec<TimeInput> = match frontiers.map(|f| f.cast::<PyList>()) {
            Some(Ok(l)) => l.iter().map(|f| input::time_input(&f)).collect(),
            _ => {
                return Err(py_err(
                    py,
                    &invalid("grid needs `frontiers`: a list of timestamps"),
                ));
            }
        };
        let w = (
            window_start.filter(|o| !o.is_none()).map(DurInput::from_py),
            window_end.filter(|o| !o.is_none()).map(DurInput::from_py),
        );
        let b = py
            .detach(|| grid(&self.snap, &c, &fs, w, &as_of))
            .or_py(py)?;
        to_table(py, b)
    }

    #[pyo3(signature = (table, left, clock, system, assume_utc, *, as_of_columns=None, empty=false))]
    #[allow(clippy::too_many_arguments)]
    fn asof_join(
        &self,
        py: Python<'_>,
        table: &Bound<'_, PyAny>,
        left: &Bound<'_, PyAny>,
        clock: Option<&Bound<'_, PyAny>>,
        system: Option<&Bound<'_, PyAny>>,
        assume_utc: bool,
        as_of_columns: Option<Vec<String>>,
        empty: bool,
    ) -> PyResult<Py<PyAny>> {
        let as_of = AsOf {
            columns: as_of_columns,
            empty,
        };
        let clock = input::opt_clock(clock).or_py(py)?;
        let rows = if let Ok(list) = left.cast::<PyList>() {
            let h = Helpers::new(py, assume_utc)?;
            input::left_from_dicts(list, &h).map_err(|e| conv_err(py, e))?
        } else if left.hasattr("__arrow_c_stream__")? {
            let batches = arrow_batches(py, left)?;
            py.detach(|| input::left_from_arrow(&batches, assume_utc))
                .or_py(py)?
        } else {
            return Err(py_err(
                py,
                &invalid("asof_join `left` must be a list of dicts or Arrow data"),
            ));
        };
        let table = input::table_name(table);
        let system = input::opt_time(system);
        let b = py
            .detach(|| asof_join(&self.snap, &table, clock, &system, &rows, &as_of))
            .or_py(py)?;
        to_table(py, b)
    }
}

// ---------------------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------------------

/// A Muninn database (one writer; reads go through snapshots).
#[pyclass(module = "muninn._native", frozen)]
struct Database {
    engine: Engine<DynStore>,
}

fn column_type(obj: &Bound<'_, PyAny>) -> Result<ColumnType> {
    obj.extract::<String>()
        .ok()
        .and_then(|s| ColumnType::parse(&s))
        .ok_or_else(|| {
            invalid(format!(
                "unknown column type {} (f64, i64, bool, string, timestamp)",
                obj.repr().map_or_else(|_| "?".into(), |r| r.to_string())
            ))
        })
}

fn req_str(obj: &Bound<'_, PyAny>, what: &str) -> Result<String> {
    obj.extract::<String>()
        .map_err(|_| invalid(format!("{what} must be a string")))
}

#[pymethods]
impl Database {
    /// Pins the current committed state.
    fn snapshot(&self, py: Python<'_>) -> PyResult<Snapshot> {
        let snap = py.detach(|| self.engine.snapshot()).or_py(py)?;
        Ok(Snapshot { snap })
    }

    /// The current log version.
    fn version(&self) -> u64 {
        self.engine.version()
    }

    /// Picks up entries committed by another writer process.
    fn refresh(&self, py: Python<'_>) -> PyResult<u64> {
        py.detach(|| self.engine.refresh()).or_py(py)
    }

    #[pyo3(signature = (name, columns, default_clock, partition))]
    fn create_table<'py>(
        &self,
        py: Python<'py>,
        name: &Bound<'py, PyAny>,
        columns: &Bound<'py, PyList>,
        default_clock: Option<&Bound<'py, PyAny>>,
        partition: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let req = (|| -> Result<CreateTable> {
            let name = req_str(name, "table name")?;
            let mut cols = Vec::new();
            for c in columns.iter() {
                let t = c
                    .cast::<PyTuple>()
                    .map_err(|_| invalid("columns must be (name, type) pairs"))?;
                if t.len() != 2 {
                    return Err(invalid("columns must be (name, type) pairs"));
                }
                let n = req_str(
                    &t.get_item(0).map_err(|_| invalid("bad column"))?,
                    "column name",
                )?;
                let ty = column_type(&t.get_item(1).map_err(|_| invalid("bad column"))?)?;
                cols.push(ColumnDef::new(n, ty));
            }
            let default_clock = match default_clock.filter(|o| !o.is_none()) {
                None => Clock::default(),
                Some(v) => v
                    .extract::<String>()
                    .ok()
                    .and_then(|s| Clock::parse(&s))
                    .ok_or_else(|| {
                        invalid("default_clock must be received, published or system")
                    })?,
            };
            let partition = match partition.filter(|o| !o.is_none()) {
                None => Partitioning::default(),
                Some(v) => v
                    .extract::<String>()
                    .ok()
                    .and_then(|s| Partitioning::parse(&s))
                    .ok_or_else(|| invalid("partition must be day, month or year"))?,
            };
            Ok(CreateTable {
                name,
                columns: cols,
                default_clock,
                partition,
                clock: None,
            })
        })()
        .or_py(py)?;
        let t = py.detach(|| self.engine.create_table(&req)).or_py(py)?;
        table_dict(py, &t)
    }

    fn add_column<'py>(
        &self,
        py: Python<'py>,
        table: &Bound<'py, PyAny>,
        name: &Bound<'py, PyAny>,
        r#type: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let req = (|| -> Result<AddColumn> {
            Ok(AddColumn {
                table: req_str(table, "table")?,
                column: ColumnDef::new(req_str(name, "column name")?, column_type(r#type)?),
                clock: None,
            })
        })()
        .or_py(py)?;
        let t = py.detach(|| self.engine.add_column(&req)).or_py(py)?;
        table_dict(py, &t)
    }

    #[pyo3(signature = (name, table, tags, timezone, resolution))]
    fn create_series<'py>(
        &self,
        py: Python<'py>,
        name: &Bound<'py, PyAny>,
        table: &Bound<'py, PyAny>,
        tags: Option<&Bound<'py, PyAny>>,
        timezone: Option<&Bound<'py, PyAny>>,
        resolution: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let req = series_request(name, table, tags, timezone, resolution).or_py(py)?;
        let s = py.detach(|| self.engine.create_series(&req)).or_py(py)?;
        series_dict(py, &s)
    }

    /// Several series in one atomic commit: `items` is a list of
    /// `(name, table, tags, timezone, resolution)` tuples.
    fn create_series_batch<'py>(
        &self,
        py: Python<'py>,
        items: &Bound<'py, PyList>,
    ) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let reqs = items
            .iter()
            .map(|it| {
                let t = it.cast::<PyTuple>().map_err(|_| {
                    invalid("series must be (name, table, tags, timezone, resolution)")
                })?;
                if t.len() != 5 {
                    return Err(invalid(
                        "series must be (name, table, tags, timezone, resolution)",
                    ));
                }
                let item = |i: usize| t.get_item(i).map_err(|_| invalid("bad series item"));
                let opt = |b: Bound<'py, PyAny>| (!b.is_none()).then_some(b);
                series_request(
                    &item(0)?,
                    &item(1)?,
                    opt(item(2)?).as_ref(),
                    opt(item(3)?).as_ref(),
                    opt(item(4)?).as_ref(),
                )
            })
            .collect::<Result<Vec<CreateSeries>>>()
            .or_py(py)?;
        let made = py
            .detach(|| self.engine.create_series_batch(&reqs))
            .or_py(py)?;
        made.iter().map(|s| series_dict(py, s)).collect()
    }

    /// Writes one atomic batch from a list of dicts (`rows`) or Arrow data (`arrow`).
    /// Returns `(stored, duplicates, batch_already_committed, system_time_us, first_seq)`.
    #[pyo3(signature = (rows, arrow, batch_id, clock, assume_utc))]
    fn write(
        &self,
        py: Python<'_>,
        rows: Option<&Bound<'_, PyAny>>,
        arrow: Option<&Bound<'_, PyAny>>,
        batch_id: Option<&Bound<'_, PyAny>>,
        clock: Option<&Bound<'_, PyAny>>,
        assume_utc: bool,
    ) -> PyResult<WriteTuple> {
        let batch_id = match batch_id.filter(|o| !o.is_none()) {
            None => None,
            Some(b) => match b.cast::<PyString>() {
                Ok(s) => Some(s.to_str()?.to_owned()),
                Err(_) => {
                    return Err(py_err(py, &invalid("batch_id must be a non-empty string")));
                }
            },
        };
        let rows = match (rows, arrow) {
            (Some(r), _) => {
                let list = r
                    .cast::<PyList>()
                    .map_err(|_| py_err(py, &invalid("rows must be a list of dicts")))?;
                let h = Helpers::new(py, assume_utc)?;
                input::rows_from_dicts(list, &h).map_err(|e| conv_err(py, e))?
            }
            (None, Some(a)) => {
                let batches = arrow_batches(py, a)?;
                py.detach(|| input::rows_from_arrow(&batches, assume_utc))
                    .or_py(py)?
            }
            (None, None) => Vec::new(),
        };
        let clock = match clock.filter(|o| !o.is_none()) {
            None => TimeInput::Micros(muninn_engine::system_wall_clock()),
            Some(c) => input::time_input(c),
        };
        let batch = WriteBatch {
            rows,
            batch_id,
            clock,
        };
        let r = py.detach(|| self.engine.write(&batch)).or_py(py)?;
        Ok((
            r.stored,
            r.duplicates,
            r.batch_already_committed,
            r.system_time.map(Timestamp::as_micros),
            r.first_seq,
        ))
    }

    /// Compacts one partition (`table` + `partition` label) or, with neither, every
    /// partition with more than one segment. Returns how many partitions were compacted.
    #[pyo3(signature = (table=None, partition=None))]
    fn compact(
        &self,
        py: Python<'_>,
        table: Option<String>,
        partition: Option<String>,
    ) -> PyResult<usize> {
        match (table, partition) {
            (None, None) => py.detach(|| self.engine.compact_all()).or_py(py),
            (Some(t), Some(p)) => py
                .detach(|| self.engine.compact(&t, &p))
                .map(usize::from)
                .or_py(py),
            _ => Err(py_err(
                py,
                &invalid("compact takes both table and partition, or neither"),
            )),
        }
    }

    /// Deletes unreferenced segment files older than the grace period (µs; `None` = the
    /// engine default) and truncates the log. Returns `(deleted keys, kept count, log
    /// files deleted)`.
    #[pyo3(signature = (grace_us=None))]
    fn gc(&self, py: Python<'_>, grace_us: Option<i64>) -> PyResult<(Vec<String>, usize, usize)> {
        let r = match grace_us {
            None => py.detach(|| self.engine.gc()),
            Some(us) => {
                let us = u64::try_from(us)
                    .map_err(|_| py_err(py, &invalid("grace must not be negative")))?;
                py.detach(|| {
                    self.engine
                        .gc_with_grace(std::time::Duration::from_micros(us))
                })
            }
        }
        .or_py(py)?;
        Ok((
            r.deleted.iter().map(|k| k.as_str().to_owned()).collect(),
            r.kept,
            r.log_deleted.len(),
        ))
    }
}

/// A `create_series` request from Python arguments.
fn series_request(
    name: &Bound<'_, PyAny>,
    table: &Bound<'_, PyAny>,
    tags: Option<&Bound<'_, PyAny>>,
    timezone: Option<&Bound<'_, PyAny>>,
    resolution: Option<&Bound<'_, PyAny>>,
) -> Result<CreateSeries> {
    let name = req_str(name, "series name")?;
    let table = req_str(table, "table")?;
    let mut tag_map = BTreeMap::new();
    if let Some(t) = tags.filter(|o| !o.is_none()) {
        let d = t
            .cast::<PyDict>()
            .map_err(|_| invalid("tags must be a dict of strings"))?;
        for (k, v) in d.iter() {
            tag_map.insert(req_str(&k, "tag names")?, req_str(&v, "tag values")?);
        }
    }
    let timezone = match timezone.filter(|o| !o.is_none()) {
        None => None,
        Some(t) => Some(req_str(t, "timezone")?),
    };
    let resolution = match resolution.filter(|o| !o.is_none()) {
        None => None,
        Some(r) => Some(match DurInput::from_py(r) {
            DurInput::Micros(us) => Duration::from_micros(us),
            DurInput::Text(s) => {
                Duration::parse(&s).map_err(|e| invalid(format!("resolution: {e}")))?
            }
            DurInput::Invalid(d) => {
                return Err(invalid(format!("resolution: not a duration: {d}")));
            }
        }),
    };
    Ok(CreateSeries {
        name,
        table,
        tags: tag_map,
        timezone,
        resolution,
        clock: None,
    })
}

/// Opens (or creates) a database in a local directory.
#[pyfunction]
fn open_local(py: Python<'_>, path: String) -> PyResult<Database> {
    let engine = py
        .detach(|| -> Result<Engine<DynStore>> {
            let store: DynStore = Arc::new(LocalFsStore::open(&path).map_err(store_err)?);
            Engine::open(store, EngineOptions::default())
        })
        .or_py(py)?;
    Ok(Database { engine })
}

/// A fresh in-memory database.
#[pyfunction]
fn memory(py: Python<'_>) -> PyResult<Database> {
    let store: DynStore = Arc::new(MemoryStore::new());
    let engine = Engine::open(store, EngineOptions::default()).or_py(py)?;
    Ok(Database { engine })
}

/// `RECEIPT_KEY`: the schema metadata key of the receipt.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add_class::<Snapshot>()?;
    m.add_function(wrap_pyfunction!(open_local, m)?)?;
    m.add_function(wrap_pyfunction!(memory, m)?)?;
    m.add("RECEIPT_KEY", output::RECEIPT_KEY)?;
    Ok(())
}
