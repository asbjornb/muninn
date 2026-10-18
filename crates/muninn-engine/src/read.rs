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

use std::collections::{BTreeMap, BTreeSet, HashMap};
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
        let (Some(&id_min), Some(&id_max)) = (spec.ids.first(), spec.ids.last()) else {
            return Ok(out);
        };
        if spec.tc_lo > spec.tc_hi {
            return Ok(out);
        }
        let Some((p_lo, p_hi)) = Partition::covering(spec.table.partition, spec.range) else {
            return Ok(out);
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
                            out.push(Cand {
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
                            });
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// Full versions (values for table columns `0..ncols`) for `cands`, in order.
    pub fn materialize(&self, ncols: usize, cands: &[Cand]) -> Result<Vec<Version>> {
        type Cols = Vec<Option<Vec<Option<Value>>>>;
        let mut cache: HashMap<(u32, u32), Cols> = HashMap::new();
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

/// For each point, the candidate with the greatest knowledge key.
fn winners(cands: Vec<Cand>) -> HashMap<(SeriesId, Timestamp), Cand> {
    let mut best: HashMap<(SeriesId, Timestamp), Cand> = HashMap::with_capacity(cands.len());
    for c in cands {
        best.entry(c.point())
            .and_modify(|w| {
                if c.key() > w.key() {
                    *w = c;
                }
            })
            .or_insert(c);
    }
    best
}

/// Candidates grouped per point, each chain sorted by knowledge key.
fn chains(cands: Vec<Cand>) -> HashMap<(SeriesId, Timestamp), Vec<Cand>> {
    let mut m: HashMap<(SeriesId, Timestamp), Vec<Cand>> = HashMap::new();
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
        let cands = scanner.scan(&ScanSpec {
            table,
            ids,
            range,
            clock: vis.clock,
            tc_lo: i64::MIN,
            tc_hi: tc_hi(vis.frontier),
            system: vis.system,
            points: None,
        })?;
        let mut win: Vec<Cand> = winners(cands)
            .into_values()
            .filter(|c| !c.deleted)
            .collect();
        win.sort_by_key(Cand::point);
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
        let cands = scanner.scan(&ScanSpec {
            table: p.table,
            ids: &p.ids,
            range,
            clock: p.clock,
            tc_lo: i64::MIN,
            tc_hi: f_last.as_micros(),
            system: p.system,
            points: None,
        })?;
        let fs: Vec<i128> = p
            .frontiers
            .iter()
            .map(|f| i128::from(f.as_micros()))
            .collect();
        // (frontier index, point, candidate)
        let mut hits: Vec<(usize, Cand)> = Vec::new();
        for chain in chains(cands).into_values() {
            let vt = i128::from(chain[0].vt.as_micros());
            // Frontiers whose window contains vt: F + start <= vt < F + end.
            let (a, b) = match p.window {
                GridWindow::Absolute(_) => (0, fs.len()),
                GridWindow::Relative { start, end } => {
                    let (s, e) = (i128::from(start.as_micros()), i128::from(end.as_micros()));
                    (
                        fs.partition_point(|&f| f + e <= vt),
                        fs.partition_point(|&f| f + s <= vt),
                    )
                }
            };
            let mut j = 0;
            for (i, f) in p.frontiers.iter().enumerate().take(b).skip(a) {
                while j < chain.len() && chain[j].tc <= *f {
                    j += 1;
                }
                if j > 0 && !chain[j - 1].deleted {
                    hits.push((i, chain[j - 1]));
                }
            }
        }
        hits.sort_by_key(|(i, c)| (*i, c.series, c.vt));
        // Materialise each distinct version once.
        let mut distinct: Vec<Cand> = hits.iter().map(|(_, c)| *c).collect();
        distinct.sort_by_key(|c| c.seq);
        distinct.dedup_by_key(|c| c.seq);
        let rows = self.rows(&scanner, p.table, &distinct)?;
        let by_seq: HashMap<u64, &VersionRow> = rows.iter().map(|r| (r.seq, r)).collect();
        let rows = hits
            .into_iter()
            .map(|(i, c)| GridRow {
                frontier: p.frontiers[i],
                row: by_seq[&c.seq].clone(),
            })
            .collect();
        Ok(ReadResult { columns, rows })
    }

    fn asof_join(&self, req: &AsofJoinRequest) -> Result<ReadResult<AsofRow>> {
        let p = self.state.catalog.prepare_asof(req)?;
        let columns = p.table.columns.clone();
        let mut points: HashMap<SeriesId, BTreeSet<Timestamp>> = HashMap::new();
        for (left, s) in req.left.iter().zip(&p.series) {
            points.entry(s.id).or_default().insert(left.value_time);
        }
        let mut found: HashMap<(SeriesId, Timestamp), Vec<Cand>> = HashMap::new();
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
