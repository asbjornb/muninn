//! The read path: pruned scans over a snapshot's segments, and the SPEC §6 operations on
//! top of them.
//!
//! A scan visits the partitions overlapping the value range, skips segments (from log
//! statistics) and blocks (from footer statistics) that cannot hold a qualifying row,
//! reads only the series slices it needs, and returns *candidates*: the engine columns of
//! every qualifying version plus its location. The operations reduce candidates by the
//! knowledge order of core ([`KnowledgeKey`]) and only then decode value columns, for the
//! rows actually returned. Results never depend on segment boundaries or order: every
//! reduction is over a total order and every output is sorted by the spec order.

use std::collections::{BTreeMap, BTreeSet};

use crate::fxhash::FxHashMap as HashMap;
use std::sync::Arc;

use muninn_core::api::{
    AsofJoinRequest, AsofRow, GridRequest, GridRow, GridWindow, HistoryRequest, LatestRequest,
    ReadResult, Reader, ReplayRequest, VersionRow,
};
use muninn_core::types::{KnowledgeKey, SeriesDef, SeriesId, TableDef, Version};
use muninn_core::{Clock, Result, TimeRange, Timestamp, Value};
use muninn_format::{Footer, Partition};
use muninn_store::Store;

use crate::segio::Shared;
use crate::state::{SegmentRef, State};

/// A pinned, immutable view of the database at one log version (SPEC §5.1).
///
/// Cheap to clone; reads never block writers and always see exactly the committed state
/// of their version (SPEC §7.3).
pub struct EngineSnapshot<S> {
    pub(crate) state: Arc<State>,
    pub(crate) shared: Arc<Shared<S>>,
}

impl<S> Clone for EngineSnapshot<S> {
    fn clone(&self) -> Self {
        EngineSnapshot {
            state: Arc::clone(&self.state),
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<S> std::fmt::Debug for EngineSnapshot<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineSnapshot")
            .field("version", &self.state.version)
            .finish_non_exhaustive()
    }
}

impl<S> EngineSnapshot<S> {
    /// The log version this snapshot reflects.
    pub fn version(&self) -> u64 {
        self.state.version
    }

    /// The greatest `system_time` of any version in this snapshot (`last` of SPEC §4.2),
    /// or `None` if nothing is stored. This is the `S` of a reproducibility receipt for a
    /// read without a system bound (SPEC §11).
    pub fn last_system_time(&self) -> Option<muninn_core::Timestamp> {
        self.state.last_system_time
    }
}

/// A qualifying version found by a scan (engine columns only).
#[derive(Copy, Clone, Debug)]
pub(crate) struct Cand {
    pub series: SeriesId,
    pub vt: Timestamp,
    pub pt: Option<Timestamp>,
    pub rt: Timestamp,
    pub st: Timestamp,
    pub seq: u64,
    pub deleted: bool,
    /// `t_clock` under the scan's clock.
    pub tc: Timestamp,
    seg: u32,
    block: u32,
    row: u32,
}

impl Cand {
    pub fn key(&self) -> KnowledgeKey {
        KnowledgeKey {
            t_clock: self.tc,
            system_time: self.st,
            seq: self.seq,
        }
    }

    fn point(&self) -> (SeriesId, Timestamp) {
        (self.series, self.vt)
    }
}

/// What [`Scanner::scan_each`] reports.
pub(crate) enum Scanned {
    /// A qualifying version.
    Row(Cand),
    /// Every version of the partition scanned so far was reported (also sent before the
    /// first partition and between partitions; may repeat).
    PartitionEnd,
}

/// A segment predicate of [`ScanSpec::keep_segment`].
pub(crate) type SegmentFilter<'a> = dyn Fn(&SegmentRef, &Footer) -> bool + 'a;

/// What a scan looks for.
pub(crate) struct ScanSpec<'a> {
    pub table: &'a TableDef,
    pub ids: &'a BTreeSet<SeriesId>,
    /// Value range.
    pub range: TimeRange,
    pub clock: Clock,
    /// Inclusive bounds on `t_clock`, in µs (`i64::MIN` / `i64::MAX` = unbounded).
    pub tc_lo: i64,
    pub tc_hi: i64,
    /// System bound `S`.
    pub system: Option<Timestamp>,
    /// Restrict to these exact points (asof_join, dedup).
    pub points: Option<&'a HashMap<SeriesId, BTreeSet<Timestamp>>>,
    /// Skip segments for which this returns `false` (it may only return `false` when no
    /// row of the segment can be part of the result).
    pub keep_segment: Option<&'a SegmentFilter<'a>>,
}

/// Runs scans against one state and later materialises their candidates.
pub(crate) struct Scanner<'a, S> {
    shared: &'a Shared<S>,
    state: &'a State,
    segs: Vec<(Arc<SegmentRef>, Arc<Footer>)>,
}

fn range_overlaps(r: &TimeRange, min: Timestamp, max: Timestamp) -> bool {
    r.from.is_none_or(|f| f <= max) && r.to.is_none_or(|t| min < t)
}

impl<'a, S: Store> Scanner<'a, S> {
    pub fn new(shared: &'a Shared<S>, state: &'a State) -> Scanner<'a, S> {
        Scanner {
            shared,
            state,
            segs: Vec::new(),
        }
    }

    /// Every version matching `spec`, in no particular order.
    pub fn scan(&mut self, spec: &ScanSpec<'_>) -> Result<Vec<Cand>> {
        let mut out = Vec::new();
        self.scan_each(spec, |s| {
            if let Scanned::Row(c) = s {
                out.push(c);
            }
        })?;
        Ok(out)
    }

    /// Calls `f` with every version matching `spec`, partition by partition (each
    /// followed by [`Scanned::PartitionEnd`]); within a partition segment by segment, and
    /// within a segment in row order, i.e. sorted by `(series, value_time)`.
    pub fn scan_each(&mut self, spec: &ScanSpec<'_>, mut f: impl FnMut(Scanned)) -> Result<()> {
        let (Some(&id_min), Some(&id_max)) = (spec.ids.first(), spec.ids.last()) else {
            return Ok(());
        };
        if spec.tc_lo > spec.tc_hi {
            return Ok(());
        }
        let Some((p_lo, p_hi)) = Partition::covering(spec.table.partition, spec.range) else {
            return Ok(());
        };
        let s_max = spec.system.map_or(i64::MAX, Timestamp::as_micros);
        let name = spec.table.name.clone();
        let tc_ok = |min: Timestamp, max: Timestamp| {
            min.as_micros() <= spec.tc_hi && max.as_micros() >= spec.tc_lo
        };
        for segs in self
            .state
            .partitions
            .range((name.clone(), p_lo)..=(name, p_hi))
            .map(|(_, v)| v)
        {
            f(Scanned::PartitionEnd);
            for seg in segs.iter() {
                // Segment pruning from the log's statistics.
                if seg.series.1 < id_min
                    || seg.series.0 > id_max
                    || spec.ids.range(seg.series.0..=seg.series.1).next().is_none()
                    || !range_overlaps(&spec.range, seg.value_time.min, seg.value_time.max)
                    || seg.system_time.min.as_micros() > s_max
                {
                    continue;
                }
                let tcb = seg.t_clock(spec.clock);
                if !tc_ok(tcb.min, tcb.max) {
                    continue;
                }
                let footer = self.shared.footer(seg, spec.table)?;
                if spec.keep_segment.is_some_and(|k| !k(seg, &footer)) {
                    continue;
                }
                // Series slices: footer series ∩ filter (∩ points).
                let mut by_block: BTreeMap<usize, Vec<(u64, u64)>> = BTreeMap::new();
                for e in &footer.series {
                    if !spec.ids.contains(&e.id)
                        || spec.points.is_some_and(|p| !p.contains_key(&e.id))
                    {
                        continue;
                    }
                    let rows = e.row_start..e.row_end;
                    for b in footer.blocks_for_rows(rows.clone()) {
                        let br = footer.blocks[b].rows();
                        by_block
                            .entry(b)
                            .or_default()
                            .push((rows.start.max(br.start), rows.end.min(br.end)));
                    }
                }
                if by_block.is_empty() {
                    continue;
                }
                let seg_idx = self.segs.len() as u32;
                self.segs.push((Arc::clone(seg), Arc::clone(&footer)));
                for (b, slices) in by_block {
                    let bm = &footer.blocks[b];
                    let btc = bm.t_clock(spec.clock);
                    if !range_overlaps(&spec.range, bm.value_time.min, bm.value_time.max)
                        || bm.system_time.min.as_micros() > s_max
                        || !tc_ok(btc.min, btc.max)
                    {
                        continue;
                    }
                    let eb = self.shared.engine_block(seg, &footer, b)?;
                    for (lo, hi) in slices {
                        for row in lo..hi {
                            let i = (row - bm.row_start) as usize;
                            let vt = eb.value_time[i];
                            if !spec.range.contains(vt) {
                                continue;
                            }
                            if let Some(p) = spec.points
                                && !p.get(&eb.series[i]).is_some_and(|s| s.contains(&vt))
                            {
                                continue;
                            }
                            let st = eb.system_time[i];
                            let tc = eb.t_clock(spec.clock, i);
                            if st.as_micros() > s_max
                                || tc.as_micros() > spec.tc_hi
                                || tc.as_micros() < spec.tc_lo
                            {
                                continue;
                            }
                            f(Scanned::Row(Cand {
                                series: eb.series[i],
                                vt,
                                pt: eb.published_time[i],
                                rt: eb.received_time[i],
                                st,
                                seq: eb.seq[i],
                                deleted: eb.deleted[i],
                                tc,
                                seg: seg_idx,
                                block: b as u32,
                                row: i as u32,
                            }));
                        }
                    }
                }
            }
        }
        f(Scanned::PartitionEnd);
        Ok(())
    }

    /// Full versions (values for table columns `0..ncols`) for `cands`, in order.
    pub fn materialize(&self, ncols: usize, cands: &[Cand]) -> Result<Vec<Version>> {
        type Cols = Vec<Option<Vec<Option<Value>>>>;
        let mut cache: HashMap<(u32, u32), Cols> = HashMap::default();
        let mut out = Vec::with_capacity(cands.len());
        for c in cands {
            let cols = match cache.entry((c.seg, c.block)) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    let (seg, footer) = &self.segs[c.seg as usize];
                    e.insert(
                        self.shared
                            .value_columns(seg, footer, c.block as usize, ncols)?,
                    )
                }
            };
            let values = cols
                .iter()
                .map(|col| col.as_ref().and_then(|v| v[c.row as usize].clone()))
                .collect();
            out.push(Version {
                series: c.series,
                value_time: c.vt,
                published_time: c.pt,
                received_time: c.rt,
                deleted: c.deleted,
                values,
                system_time: c.st,
                seq: c.seq,
            });
        }
        Ok(out)
    }
}

/// Calls `f` with the candidates of each point in `buf` (one partition's), in
/// `(series, value_time)` order, and empties `buf`. Each segment's candidates arrive sorted
/// by `(series, value_time)`, so the stable sort only merges a few sorted runs.
fn by_point(buf: &mut Vec<Cand>, mut f: impl FnMut(&mut [Cand])) {
    buf.sort_by_key(Cand::point);
    for g in buf.chunk_by_mut(|a, b| a.point() == b.point()) {
        f(g);
    }
    buf.clear();
}

/// `latest` as a streaming reduction: per point the candidate with the greatest
/// knowledge key; points whose winner is a tombstone are absent. A point's versions all
/// lie in one partition, which the scan delivers in one piece.
#[derive(Default)]
struct LatestReducer {
    buf: Vec<Cand>,
    /// The best version so far of the current run (consecutive versions of one point
    /// from one segment): only it can win, so only it is buffered.
    run: Option<Cand>,
    out: Vec<Cand>,
}

impl LatestReducer {
    fn push(&mut self, s: Scanned) {
        match s {
            Scanned::Row(c) => match &mut self.run {
                Some(r) if r.point() == c.point() => {
                    if c.key() > r.key() {
                        *r = c;
                    }
                }
                _ => {
                    if let Some(r) = self.run.replace(c) {
                        self.buf.push(r);
                    }
                }
            },
            Scanned::PartitionEnd => {
                if let Some(r) = self.run.take() {
                    self.buf.push(r);
                }
                let out = &mut self.out;
                by_point(&mut self.buf, |g| {
                    let w = g.iter().max_by_key(|c| c.key()).expect("non-empty group");
                    if !w.deleted {
                        out.push(*w);
                    }
                });
            }
        }
    }

    /// The winners, ordered by `(series, value_time)`.
    fn finish(mut self) -> Vec<Cand> {
        self.push(Scanned::PartitionEnd);
        // Sorted within each partition: a stable sort merges the partitions' runs.
        self.out.sort_by_key(Cand::point);
        self.out
    }
}

/// `grid` as a streaming reduction (SPEC §6.4 in one pass): per point and frontier `F_i`
/// whose window contains the point, the candidate with the greatest knowledge key among
/// those with `t_clock <= F_i`. A point's versions (all in one partition) are sorted by
/// knowledge key; since the key starts with `t_clock`, the versions visible at `F` are a
/// prefix, so the sorted frontiers and the versions are walked together.
struct GridReducer<'a> {
    frontiers: &'a [Timestamp],
    /// `frontiers` in µs, widened for exact window arithmetic.
    fs: Vec<i128>,
    window: GridWindow,
    buf: Vec<Cand>,
    /// The current run (consecutive versions of one point from one segment); reduced to
    /// its winners per frontier before it is buffered.
    run: Vec<Cand>,
    /// Distinct winning versions (a version belongs to one point, so deduplicating per
    /// point is enough).
    versions: Vec<Cand>,
    /// `(frontier index, index into versions)`.
    hits: Vec<(u32, u32)>,
}

/// Frontier indices `[a, b)` whose window contains `vt` (`F + start <= vt < F + end`).
fn window_frontiers(fs: &[i128], window: GridWindow, vt: Timestamp) -> (usize, usize) {
    let v = i128::from(vt.as_micros());
    match window {
        GridWindow::Absolute(_) => (0, fs.len()),
        GridWindow::Relative { start, end } => {
            let (s, e) = (i128::from(start.as_micros()), i128::from(end.as_micros()));
            (
                fs.partition_point(|&f| f + e <= v),
                fs.partition_point(|&f| f + s <= v),
            )
        }
    }
}

/// For versions `g` of one point sorted by knowledge key, calls `f(i, winner)` for every
/// frontier `i` in `[a, b)` at which some version is visible.
fn sweep(frontiers: &[Timestamp], a: usize, b: usize, g: &[Cand], mut f: impl FnMut(usize, &Cand)) {
    let mut j = 0;
    for (i, fr) in frontiers.iter().enumerate().take(b).skip(a) {
        while j < g.len() && g[j].tc <= *fr {
            j += 1;
        }
        if j > 0 {
            f(i, &g[j - 1]);
        }
    }
}

impl<'a> GridReducer<'a> {
    fn new(frontiers: &'a [Timestamp], window: GridWindow) -> GridReducer<'a> {
        GridReducer {
            frontiers,
            fs: frontiers
                .iter()
                .map(|f| i128::from(f.as_micros()))
                .collect(),
            window,
            buf: Vec::new(),
            run: Vec::new(),
            versions: Vec::new(),
            hits: Vec::new(),
        }
    }

    /// Buffers the versions of the current run that win at some frontier within the run
    /// (the point's overall winner at a frontier is the best of the runs' winners there).
    fn flush_run(&mut self) {
        let Some(first) = self.run.first() else {
            return;
        };
        let (a, b) = window_frontiers(&self.fs, self.window, first.vt);
        if a < b {
            self.run.sort_unstable_by_key(Cand::key);
            let buf = &mut self.buf;
            let mut last = None;
            sweep(self.frontiers, a, b, &self.run, |_, c| {
                if last != Some(c.seq) {
                    last = Some(c.seq);
                    buf.push(*c);
                }
            });
        }
        self.run.clear();
    }

    fn push(&mut self, s: Scanned) {
        match s {
            Scanned::Row(c) => {
                if self.run.first().is_some_and(|r| r.point() != c.point()) {
                    self.flush_run();
                }
                self.run.push(c);
            }
            Scanned::PartitionEnd => {
                self.flush_run();
                let (frontiers, fs, window, versions, hits) = (
                    self.frontiers,
                    &self.fs,
                    self.window,
                    &mut self.versions,
                    &mut self.hits,
                );
                by_point(&mut self.buf, |g| {
                    let (a, b) = window_frontiers(fs, window, g[0].vt);
                    g.sort_unstable_by_key(Cand::key);
                    let mut last = None;
                    sweep(frontiers, a, b, g, |i, c| {
                        if !c.deleted {
                            if last != Some(c.seq) {
                                last = Some(c.seq);
                                versions.push(*c);
                            }
                            hits.push((i as u32, (versions.len() - 1) as u32));
                        }
                    });
                });
            }
        }
    }

    /// The distinct winning versions, and `(frontier index, version index)` for every
    /// point and frontier whose winner is not a tombstone, ordered by `(frontier, series,
    /// value_time)`.
    fn finish(mut self) -> (Vec<Cand>, Vec<(u32, u32)>) {
        self.push(Scanned::PartitionEnd);
        let (versions, hits) = (self.versions, self.hits);
        // Counting sort by frontier (stable), then by point within each frontier: there
        // the hits already come in runs sorted by (series, value_time), one run per
        // partition, which the stable sort merges.
        let mut start = vec![0usize; self.frontiers.len() + 1];
        for &(i, _) in &hits {
            start[i as usize + 1] += 1;
        }
        for i in 1..start.len() {
            start[i] += start[i - 1];
        }
        let mut at = start.clone();
        let mut out = vec![(0u32, 0u32); hits.len()];
        for &h in &hits {
            out[at[h.0 as usize]] = h;
            at[h.0 as usize] += 1;
        }
        for w in start.windows(2) {
            out[w[0]..w[1]].sort_by_key(|&(_, v)| versions[v as usize].point());
        }
        (versions, out)
    }
}

/// Candidates grouped per point, each chain sorted by knowledge key.
fn chains(cands: Vec<Cand>) -> HashMap<(SeriesId, Timestamp), Vec<Cand>> {
    let mut m: HashMap<(SeriesId, Timestamp), Vec<Cand>> = HashMap::default();
    for c in cands {
        m.entry(c.point()).or_default().push(c);
    }
    for v in m.values_mut() {
        v.sort_by_key(Cand::key);
    }
    m
}

fn tc_hi(frontier: Option<Timestamp>) -> i64 {
    frontier.map_or(i64::MAX, Timestamp::as_micros)
}

impl<S: Store> EngineSnapshot<S> {
    fn rows(
        &self,
        scanner: &Scanner<'_, S>,
        table: &TableDef,
        cands: &[Cand],
    ) -> Result<Vec<VersionRow>> {
        scanner
            .materialize(table.columns.len(), cands)?
            .iter()
            .map(|v| self.state.catalog.version_row(table, v))
            .collect()
    }

    /// The `latest` rows for an already-validated request.
    fn latest_impl(
        &self,
        table: &TableDef,
        ids: &BTreeSet<SeriesId>,
        range: TimeRange,
        vis: muninn_core::types::Visibility,
    ) -> Result<Vec<VersionRow>> {
        let mut scanner = Scanner::new(&self.shared, &self.state);
        let mut red = LatestReducer::default();
        scanner.scan_each(
            &ScanSpec {
                table,
                ids,
                range,
                clock: vis.clock,
                tc_lo: i64::MIN,
                tc_hi: tc_hi(vis.frontier),
                system: vis.system,
                points: None,
                keep_segment: None,
            },
            |c| red.push(c),
        )?;
        let win = red.finish();
        self.rows(&scanner, table, &win)
    }

    /// SPEC §6.4 by its definition: one `latest` per frontier. Used by tests to check the
    /// single-pass [`Reader::grid`] (SPEC §7.4).
    pub fn grid_by_repeated_latest(&self, req: &GridRequest) -> Result<ReadResult<GridRow>> {
        let p = self.state.catalog.prepare_grid(req)?;
        let mut rows = Vec::new();
        for &f in &p.frontiers {
            let vis = muninn_core::types::Visibility {
                clock: p.clock,
                frontier: Some(f),
                system: p.system,
            };
            for row in self.latest_impl(p.table, &p.ids, p.range_for(f)?, vis)? {
                rows.push(GridRow { frontier: f, row });
            }
        }
        Ok(ReadResult {
            columns: p.table.columns.clone(),
            rows,
        })
    }
}

/// A lead-time filter for [`EngineSnapshot::history_lead`]: keep the versions whose
/// timestamp column `column` holds a value `c` with `min <= value_time - c < max`
/// (exact arithmetic; null values never match). Not part of SPEC §6: the result is by
/// definition that of `history` with the rows that fail the filter removed — it only lets
/// the engine skip data (segments by their column statistics, values of rejected rows).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeadFilter {
    /// A `timestamp` column of the table (e.g. the issue time of a forecast).
    pub column: String,
    /// Smallest lead, inclusive.
    pub min: muninn_core::Duration,
    /// Largest lead, exclusive.
    pub max: muninn_core::Duration,
}

impl LeadFilter {
    /// Does a version with this `value_time` and column value pass?
    pub fn accepts(&self, vt: Timestamp, value: Option<&Value>) -> bool {
        match value {
            Some(Value::Timestamp(c)) => {
                let lead = i128::from(vt.as_micros()) - i128::from(c.as_micros());
                i128::from(self.min.as_micros()) <= lead && lead < i128::from(self.max.as_micros())
            }
            _ => false,
        }
    }
}

impl<S: Store> EngineSnapshot<S> {
    /// `history` (SPEC §6.2) restricted by a [`LeadFilter`]. Errors as `history`, then
    /// `unknown_column` if the table has no such column, `type_mismatch` if it is not a
    /// `timestamp` column, `invalid_range` if `min > max`.
    pub fn history_lead(
        &self,
        req: &HistoryRequest,
        lead: &LeadFilter,
    ) -> Result<ReadResult<VersionRow>> {
        let p = self.state.catalog.prepare_latest(req)?;
        let ci = p.table.column_index(&lead.column).ok_or_else(|| {
            muninn_core::Error::new(
                muninn_core::ErrorKind::UnknownColumn,
                format!("table {:?} has no column {:?}", p.table.name, lead.column),
            )
        })?;
        if p.table.columns[ci].ty != muninn_core::ColumnType::Timestamp {
            return Err(muninn_core::Error::new(
                muninn_core::ErrorKind::TypeMismatch,
                format!(
                    "lead filter column {:?} is not a timestamp column",
                    lead.column
                ),
            ));
        }
        if lead.min > lead.max {
            return Err(muninn_core::Error::new(
                muninn_core::ErrorKind::InvalidRange,
                "lead filter: min > max",
            ));
        }
        let (lo, hi) = (
            i128::from(lead.min.as_micros()),
            i128::from(lead.max.as_micros()),
        );
        // A segment can hold a match only if some value c in its column bounds and some
        // value time v in its bounds have lo <= v - c < hi, i.e. c in (v_min - hi,
        // v_max - lo]. Without values (column absent, all null) nothing matches.
        let keep = |seg: &SegmentRef, f: &Footer| -> bool {
            let Some(pos) = f.column_position(ci as u32) else {
                return false;
            };
            match &f.column_stats[pos].min_max {
                Some((Value::Timestamp(cmin), Value::Timestamp(cmax))) => {
                    let (vmin, vmax) = (
                        i128::from(seg.value_time.min.as_micros()),
                        i128::from(seg.value_time.max.as_micros()),
                    );
                    i128::from(cmax.as_micros()) > vmin - hi
                        && i128::from(cmin.as_micros()) <= vmax - lo
                }
                Some(_) => true,
                None => f.column_stats[pos].null_count < f.row_count && lo < hi,
            }
        };
        let mut scanner = Scanner::new(&self.shared, &self.state);
        let cands = if lo < hi {
            scanner.scan(&ScanSpec {
                table: p.table,
                ids: &p.ids,
                range: p.range,
                clock: p.vis.clock,
                tc_lo: i64::MIN,
                tc_hi: tc_hi(p.vis.frontier),
                system: p.vis.system,
                points: None,
                keep_segment: Some(&keep),
            })?
        } else {
            Vec::new()
        };
        // Decode only the filter column of the candidates' blocks, then everything else
        // only for the survivors.
        let mut cols: HashMap<(u32, u32), Option<Vec<Option<Value>>>> = HashMap::default();
        let mut kept = Vec::with_capacity(cands.len() / 8);
        for c in cands {
            let col = match cols.entry((c.seg, c.block)) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    let (seg, footer) = &scanner.segs[c.seg as usize];
                    e.insert(
                        self.shared
                            .value_column(seg, footer, c.block as usize, ci)?,
                    )
                }
            };
            let v = col.as_ref().and_then(|v| v[c.row as usize].as_ref());
            if lead.accepts(c.vt, v) {
                kept.push(c);
            }
        }
        drop(cols);
        kept.sort_unstable_by_key(|c| (c.series, c.vt, c.key()));
        let rows = self.rows(&scanner, p.table, &kept)?;
        Ok(ReadResult {
            columns: p.table.columns.clone(),
            rows,
        })
    }
}

impl<S: Store> Reader for EngineSnapshot<S> {
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
        let p = self.state.catalog.prepare_latest(req)?;
        let rows = self.latest_impl(p.table, &p.ids, p.range, p.vis)?;
        Ok(ReadResult {
            columns: p.table.columns.clone(),
            rows,
        })
    }

    fn history(&self, req: &HistoryRequest) -> Result<ReadResult<VersionRow>> {
        let p = self.state.catalog.prepare_latest(req)?;
        let mut scanner = Scanner::new(&self.shared, &self.state);
        let mut cands = scanner.scan(&ScanSpec {
            table: p.table,
            ids: &p.ids,
            range: p.range,
            clock: p.vis.clock,
            tc_lo: i64::MIN,
            tc_hi: tc_hi(p.vis.frontier),
            system: p.vis.system,
            points: None,
            keep_segment: None,
        })?;
        cands.sort_by_key(|c| (c.series, c.vt, c.key()));
        let rows = self.rows(&scanner, p.table, &cands)?;
        Ok(ReadResult {
            columns: p.table.columns.clone(),
            rows,
        })
    }

    fn replay(&self, req: &ReplayRequest) -> Result<ReadResult<VersionRow>> {
        let p = self.state.catalog.prepare_replay(req)?;
        let mut scanner = Scanner::new(&self.shared, &self.state);
        // Arrival range (from, to] on t_clock (SPEC §6.3) as inclusive µs bounds.
        let tc_lo = req
            .arrival
            .from
            .map_or(i64::MIN, |t| t.as_micros().saturating_add(1));
        let tc_hi = req.arrival.to.map_or(i64::MAX, Timestamp::as_micros);
        let mut cands = scanner.scan(&ScanSpec {
            table: p.table,
            ids: &p.ids,
            range: p.range,
            clock: p.vis.clock,
            tc_lo,
            tc_hi,
            system: p.vis.system,
            points: None,
            keep_segment: None,
        })?;
        cands.sort_by_key(Cand::key);
        let rows = self.rows(&scanner, p.table, &cands)?;
        Ok(ReadResult {
            columns: p.table.columns.clone(),
            rows,
        })
    }

    /// Single pass (SPEC §6.4 SHOULD): one scan up to the last frontier; per point, the
    /// version chain sorted by knowledge order. Because the order's first component is
    /// `t_clock`, the versions visible at `F` are a prefix of the chain, so walking the
    /// sorted frontiers and the chain together resolves every frontier in
    /// `O(chain + frontiers)`.
    fn grid(&self, req: &GridRequest) -> Result<ReadResult<GridRow>> {
        let p = self.state.catalog.prepare_grid(req)?;
        let columns = p.table.columns.clone();
        let (Some(&f_first), Some(&f_last)) = (p.frontiers.first(), p.frontiers.last()) else {
            return Ok(ReadResult {
                columns,
                rows: Vec::new(),
            });
        };
        // The union of all frontiers' windows: `[F_1 + start, F_n + end)` in exact
        // arithmetic (windows move monotonically with F), clamped to valid timestamps.
        // Built directly rather than from the normalised per-frontier windows: the last
        // frontier's window may be the empty sentinel `[MAX, MAX)` while earlier ones still
        // contain MAX.
        let range = match p.window {
            GridWindow::Absolute(r) => r,
            GridWindow::Relative { start, end } => {
                let (min, max) = (
                    i128::from(Timestamp::MIN.as_micros()),
                    i128::from(Timestamp::MAX.as_micros()),
                );
                let lo = i128::from(f_first.as_micros()) + i128::from(start.as_micros());
                let hi = i128::from(f_last.as_micros()) + i128::from(end.as_micros());
                if lo > max || hi <= min || lo >= hi {
                    return Ok(ReadResult {
                        columns,
                        rows: Vec::new(),
                    });
                }
                let at = |x: i128| Timestamp::from_micros(x as i64);
                TimeRange::new(
                    if lo <= min { None } else { at(lo) },
                    if hi > max { None } else { at(hi) },
                )
            }
        };
        let mut scanner = Scanner::new(&self.shared, &self.state);
        let mut red = GridReducer::new(&p.frontiers, p.window);
        scanner.scan_each(
            &ScanSpec {
                table: p.table,
                ids: &p.ids,
                range,
                clock: p.clock,
                tc_lo: i64::MIN,
                tc_hi: f_last.as_micros(),
                system: p.system,
                points: None,
                keep_segment: None,
            },
            |c| red.push(c),
        )?;
        // Materialise each distinct version once (a version is often the answer at
        // several frontiers), and hand it out by move at its last use.
        let (distinct, hits) = red.finish();
        let mut uses = vec![0u32; distinct.len()];
        for &(_, v) in &hits {
            uses[v as usize] += 1;
        }
        let mut versions: Vec<Option<VersionRow>> = self
            .rows(&scanner, p.table, &distinct)?
            .into_iter()
            .map(Some)
            .collect();
        let rows = hits
            .iter()
            .map(|&(i, v)| {
                let v = v as usize;
                uses[v] -= 1;
                let row = if uses[v] == 0 {
                    versions[v].take()
                } else {
                    versions[v].clone()
                };
                GridRow {
                    frontier: p.frontiers[i as usize],
                    row: row.expect("each version is used as often as counted"),
                }
            })
            .collect();
        Ok(ReadResult { columns, rows })
    }

    fn asof_join(&self, req: &AsofJoinRequest) -> Result<ReadResult<AsofRow>> {
        let p = self.state.catalog.prepare_asof(req)?;
        let columns = p.table.columns.clone();
        let mut points: HashMap<SeriesId, BTreeSet<Timestamp>> = HashMap::default();
        for (left, s) in req.left.iter().zip(&p.series) {
            points.entry(s.id).or_default().insert(left.value_time);
        }
        let mut found: HashMap<(SeriesId, Timestamp), Vec<Cand>> = HashMap::default();
        let mut scanner = Scanner::new(&self.shared, &self.state);
        if let (Some(vt_min), Some(vt_max), Some(f_max)) = (
            req.left.iter().map(|l| l.value_time).min(),
            req.left.iter().map(|l| l.value_time).max(),
            req.left.iter().map(|l| l.frontier).max(),
        ) {
            let ids: BTreeSet<SeriesId> = points.keys().copied().collect();
            let range = TimeRange::new(
                Some(vt_min),
                Timestamp::from_micros(vt_max.as_micros().saturating_add(1)),
            );
            let cands = scanner.scan(&ScanSpec {
                table: p.table,
                ids: &ids,
                range,
                clock: p.clock,
                tc_lo: i64::MIN,
                tc_hi: f_max.as_micros(),
                system: p.system,
                points: Some(&points),
                keep_segment: None,
            })?;
            found = chains(cands);
        }
        // Per left row: the last version of the chain with t_clock <= frontier.
        let picks: Vec<Option<Cand>> = req
            .left
            .iter()
            .zip(&p.series)
            .map(|(left, s)| {
                let chain = found.get(&(s.id, left.value_time))?;
                let n = chain.partition_point(|c| c.tc <= left.frontier);
                (n > 0 && !chain[n - 1].deleted).then(|| chain[n - 1])
            })
            .collect();
        let mut distinct: Vec<Cand> = picks.iter().flatten().copied().collect();
        distinct.sort_by_key(|c| c.seq);
        distinct.dedup_by_key(|c| c.seq);
        let rows = self.rows(&scanner, p.table, &distinct)?;
        let by_seq: HashMap<u64, &VersionRow> = rows.iter().map(|r| (r.seq, r)).collect();
        let rows = req
            .left
            .iter()
            .zip(&p.series)
            .zip(picks)
            .map(|((left, s), pick)| AsofRow {
                series_id: s.id,
                series: Arc::clone(&s.name),
                value_time: left.value_time,
                frontier: left.frontier,
                right: pick.map(|c| by_seq[&c.seq].clone()),
            })
            .collect();
        Ok(ReadResult { columns, rows })
    }
}
