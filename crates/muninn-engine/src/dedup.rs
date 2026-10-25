//! The write-side deduplication index (SPEC §4.3).
//!
//! A row is a duplicate only if it is content-identical to a stored version, and content
//! includes `received_time`. So a row whose `received_time` lies outside the range of
//! received times stored for its point cannot be a duplicate, whatever the values are.
//! The index keeps, per point `(series, value_time)`, the minimum and maximum
//! `received_time` of its stored versions; a write consults it in `O(rows)` and compares
//! values (by reading stored versions) only for rows whose received time falls inside
//! that range — re-sends and out-of-order backfills. Fresh data (a new forecast run, a
//! correction) never touches the stored history.
//!
//! **Correctness rule:** the index may *over*-approximate a point's range (that only sends
//! more rows down the exact path), never under-approximate it. It is built per
//! `(table, partition)` from segments and records exactly which segment files it has
//! folded in (`covered`). Before every use it is brought up to date with the live
//! segments of the writer's *current* state: any live segment it has not folded (written
//! by another writer, by compaction elsewhere, by restore, or before this process
//! started) is read and folded first. Versions are immutable and never removed, so
//! folding a segment that is later retired, or folding the same rows twice (a compacted
//! segment and its sources), only widens ranges. Rows of our own writes are folded only
//! after their commit succeeded; an ambiguous failure leaves the segment unfolded, and if
//! it did commit, the reloaded state lists it and it is folded from the file.
//!
//! Memory is bounded: whole partitions are evicted least-recently-used first when the
//! total number of indexed points exceeds the limit; an evicted partition is rebuilt from
//! its segments on next use.

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::fxhash::FxHashMap;

use muninn_core::types::{SeriesId, TableDef};
use muninn_core::{Result, Timestamp};
use muninn_format::Partition;
use muninn_store::{Key, Store};

use crate::segio::Shared;
use crate::state::State;

/// `[min, max]` received time (µs) of a point's stored versions.
type RtRange = (i64, i64);

#[derive(Default)]
struct PartIndex {
    /// Segments whose every row is folded into `points`.
    covered: HashSet<Key>,
    /// Per point `(series, value_time µs)`.
    points: FxHashMap<(SeriesId, i64), RtRange>,
    /// LRU clock of the last use.
    used: u64,
}

impl PartIndex {
    fn fold(&mut self, series: SeriesId, vt: i64, rt: i64) {
        self.points
            .entry((series, vt))
            .and_modify(|r| {
                r.0 = r.0.min(rt);
                r.1 = r.1.max(rt);
            })
            .or_insert((rt, rt));
    }
}

/// See the module docs. Only used under the engine's writer lock.
pub(crate) struct DedupIndex {
    /// Table name → partition index → index.
    parts: HashMap<String, FxHashMap<i64, PartIndex>>,
    /// Evict partitions beyond this many points in total (0 = no index: every row is
    /// checked against the stored versions).
    limit: usize,
    tick: u64,
}

impl DedupIndex {
    pub fn new(limit: usize) -> DedupIndex {
        DedupIndex {
            parts: HashMap::new(),
            limit,
            tick: 0,
        }
    }

    fn part(&self, table: &str, p: Partition) -> Option<&PartIndex> {
        self.parts.get(table)?.get(&p.index)
    }

    fn part_mut(&mut self, table: &str, p: Partition) -> Option<&mut PartIndex> {
        self.parts.get_mut(table)?.get_mut(&p.index)
    }

    /// Brings the index of each partition in `touched` up to date with the live segments
    /// of `st`, reading the segments it has not folded yet.
    pub fn prepare<S: Store>(
        &mut self,
        shared: &Shared<S>,
        st: &State,
        touched: &BTreeSet<(String, Partition)>,
    ) -> Result<()> {
        if self.limit == 0 {
            return Ok(());
        }
        self.tick += 1;
        for (table, p) in touched {
            let part = self
                .parts
                .entry(table.clone())
                .or_default()
                .entry(p.index)
                .or_default();
            part.used = self.tick;
            let Some(segs) = st.partitions.get(&(table.clone(), p.index)) else {
                continue;
            };
            let def: &TableDef = st.catalog.table(table)?;
            for seg in segs.iter() {
                if part.covered.contains(&seg.key) {
                    continue;
                }
                let footer = shared.footer(seg, def)?;
                let mut eb = muninn_format::EngineBlock::default();
                for b in 0..footer.blocks.len() {
                    shared.engine_block_into(seg, &footer, b, &mut eb)?;
                    for i in 0..eb.len() {
                        part.fold(
                            eb.series[i],
                            eb.value_time[i].as_micros(),
                            eb.received_time[i].as_micros(),
                        );
                    }
                }
                // Only now: a segment is covered when *all* its rows are folded.
                part.covered.insert(seg.key.clone());
            }
            // Forget retired keys once they dominate (their rows stay folded, which is
            // safe: versions are never removed).
            if part.covered.len() > 2 * segs.len() + 16 {
                let live: HashSet<&Key> = segs.iter().map(|s| &s.key).collect();
                part.covered.retain(|k| live.contains(k));
            }
        }
        self.evict(touched);
        Ok(())
    }

    /// Could a stored version of `(series, vt)` in `table`/`p` have received time `rt`?
    /// `true` unless the index proves otherwise (call [`DedupIndex::prepare`] with the
    /// state the write is planned against first).
    pub fn may_have(
        &self,
        table: &str,
        p: Partition,
        series: SeriesId,
        vt: Timestamp,
        rt: Timestamp,
    ) -> bool {
        if self.limit == 0 {
            return true;
        }
        let Some(part) = self.part(table, p) else {
            return true;
        };
        match part.points.get(&(series, vt.as_micros())) {
            None => false,
            Some(&(lo, hi)) => (lo..=hi).contains(&rt.as_micros()),
        }
    }

    /// Records a committed segment `key` of `table`/`p` holding rows with these
    /// `(series, value_time, received_time)`.
    pub fn record(
        &mut self,
        table: &str,
        p: Partition,
        key: &Key,
        rows: impl Iterator<Item = (SeriesId, Timestamp, Timestamp)>,
    ) {
        // Only partitions that are indexed: an unindexed one is built from the segment
        // files when it is next needed (and then includes this one).
        let Some(part) = self.part_mut(table, p) else {
            return;
        };
        for (s, vt, rt) in rows {
            part.fold(s, vt.as_micros(), rt.as_micros());
        }
        part.covered.insert(key.clone());
    }

    /// Is `table`/`p` indexed now? (Only then does [`DedupIndex::record`] fold rows.)
    pub fn is_indexed(&self, table: &str, p: Partition) -> bool {
        self.limit > 0 && self.part(table, p).is_some()
    }

    /// [`DedupIndex::record`] for rows given as per-point received-time ranges
    /// `(series, value_time µs, min, max received_time µs)`.
    pub fn record_ranges(
        &mut self,
        table: &str,
        p: Partition,
        key: &Key,
        ranges: impl Iterator<Item = (SeriesId, i64, i64, i64)>,
    ) {
        let Some(part) = self.part_mut(table, p) else {
            return;
        };
        for (s, vt, lo, hi) in ranges {
            part.fold(s, vt, lo);
            part.fold(s, vt, hi);
        }
        part.covered.insert(key.clone());
    }

    /// Records a committed compaction of `table`/`p`: `new` holds exactly the rows of
    /// `removed`, so it is covered if they all were.
    pub fn record_compaction(&mut self, table: &str, p: Partition, removed: &[Key], new: &Key) {
        if let Some(part) = self.part_mut(table, p)
            && removed.iter().all(|k| part.covered.contains(k))
        {
            part.covered.insert(new.clone());
        }
    }

    /// Points indexed over all partitions.
    pub fn len(&self) -> usize {
        self.parts
            .values()
            .flat_map(|m| m.values())
            .map(|p| p.points.len())
            .sum()
    }

    fn evict(&mut self, keep: &BTreeSet<(String, Partition)>) {
        let mut total = self.len();
        while total > self.limit {
            let victim = self
                .parts
                .iter()
                .flat_map(|(t, m)| m.iter().map(move |(i, p)| (t, *i, p)))
                .filter(|(t, i, _)| !keep.iter().any(|(kt, kp)| kt == *t && kp.index == *i))
                .min_by_key(|(_, _, p)| p.used)
                .map(|(t, i, _)| (t.clone(), i));
            let Some((t, i)) = victim else { break };
            if let Some(m) = self.parts.get_mut(&t)
                && let Some(p) = m.remove(&i)
            {
                total -= p.points.len();
            }
        }
    }
}
