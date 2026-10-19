//! The engine's in-memory state: an immutable value derived from the log (a fold of
//! [`State::apply`] over the entries, or a checkpoint plus the entries after it).
//!
//! Every commit computes the next state with the *same* `apply` that recovery uses, before
//! the entry is written; so the state an engine serves after a commit is exactly the state a
//! fresh process reconstructs from the log.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use muninn_core::catalog::{Catalog, CatalogChange};
use muninn_core::types::SeriesId;
use muninn_core::{Result, Timestamp};
use muninn_format::{Partition, TimeBounds};
use muninn_store::Key;

use crate::log::{
    CatalogChangeJson, Checkpoint, LOG_FORMAT, LogEntry, SegmentJson, corrupt, format_supported,
};

/// A live segment: its key and the statistics recorded in the log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentRef {
    /// Store key.
    pub key: Key,
    /// Table name.
    pub table: String,
    /// Partition.
    pub partition: Partition,
    /// File size.
    pub size: u64,
    /// Rows.
    pub rows: u64,
    /// `value_time` bounds.
    pub value_time: TimeBounds,
    /// `received_time` bounds.
    pub received_time: TimeBounds,
    /// Effective published time bounds.
    pub published_time: TimeBounds,
    /// `system_time` bounds.
    pub system_time: TimeBounds,
    /// `[min, max]` seq.
    pub seq: (u64, u64),
    /// `[min, max]` series id.
    pub series: (SeriesId, SeriesId),
    /// Wall clock at creation (µs).
    pub created: i64,
}

impl SegmentRef {
    /// `t_clock` bounds.
    pub fn t_clock(&self, clock: muninn_core::Clock) -> TimeBounds {
        match clock {
            muninn_core::Clock::Received => self.received_time,
            muninn_core::Clock::Published => self.published_time,
            muninn_core::Clock::System => self.system_time,
        }
    }

    pub(crate) fn to_json(&self) -> SegmentJson {
        let b = |t: TimeBounds| [t.min.as_micros(), t.max.as_micros()];
        SegmentJson {
            key: self.key.as_str().to_owned(),
            table: self.table.clone(),
            partition: self.partition.label(),
            size: self.size,
            rows: self.rows,
            value_time: b(self.value_time),
            received_time: b(self.received_time),
            published_time: b(self.published_time),
            system_time: b(self.system_time),
            seq: [self.seq.0, self.seq.1],
            series: [self.series.0.0, self.series.1.0],
            created: self.created,
        }
    }

    fn from_json(j: &SegmentJson, catalog: &Catalog) -> Result<SegmentRef> {
        let table = catalog
            .table(&j.table)
            .map_err(|_| corrupt(format!("segment {} of unknown table {:?}", j.key, j.table)))?;
        let partition = Partition::parse_label(table.partition, &j.partition)
            .map_err(|e| corrupt(format!("segment {}: {}", j.key, e.message())))?;
        let key = Key::new(j.key.clone()).map_err(|e| corrupt(format!("segment key: {e}")))?;
        let expected_prefix = format!("segments/{}/{}/", j.table, j.partition);
        if !j.key.starts_with(&expected_prefix) {
            return Err(corrupt(format!(
                "segment key {} outside {expected_prefix}",
                j.key
            )));
        }
        let b = |a: [i64; 2]| -> Result<TimeBounds> {
            let min = Timestamp::from_micros(a[0]).ok_or_else(|| corrupt("segment bound"))?;
            let max = Timestamp::from_micros(a[1]).ok_or_else(|| corrupt("segment bound"))?;
            if min > max {
                return Err(corrupt(format!("segment {}: bounds min > max", j.key)));
            }
            Ok(TimeBounds { min, max })
        };
        let s = SegmentRef {
            key,
            table: j.table.clone(),
            partition,
            size: j.size,
            rows: j.rows,
            value_time: b(j.value_time)?,
            received_time: b(j.received_time)?,
            published_time: b(j.published_time)?,
            system_time: b(j.system_time)?,
            seq: (j.seq[0], j.seq[1]),
            series: (SeriesId(j.series[0]), SeriesId(j.series[1])),
            created: j.created,
        };
        if s.rows == 0 || s.seq.0 > s.seq.1 || s.series.0 > s.series.1 {
            return Err(corrupt(format!("segment {}: bad statistics", j.key)));
        }
        let r = partition.range();
        if !(r.contains(s.value_time.min) && r.contains(s.value_time.max)) {
            return Err(corrupt(format!("segment {} outside its partition", j.key)));
        }
        Ok(s)
    }
}

/// Batch ids in chunks, so that cloning a [`State`] (once per commit) costs
/// `O(ids / CHUNK)` instead of `O(ids)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BatchIds {
    chunks: Vec<Arc<BTreeSet<String>>>,
}

const BATCH_ID_CHUNK: usize = 4096;

impl BatchIds {
    /// Has `id` been committed?
    pub fn contains(&self, id: &str) -> bool {
        self.chunks.iter().any(|c| c.contains(id))
    }

    fn insert(&mut self, id: String) {
        match self.chunks.last_mut() {
            Some(c) if c.len() < BATCH_ID_CHUNK => {
                Arc::make_mut(c).insert(id);
            }
            _ => self.chunks.push(Arc::new(BTreeSet::from([id]))),
        }
    }

    fn iter(&self) -> impl Iterator<Item = &String> {
        self.chunks.iter().flat_map(|c| c.iter())
    }
}

/// Segments of one `(table, partition)`.
pub type PartitionSegments = Arc<Vec<Arc<SegmentRef>>>;

/// The complete committed state at one log version. Immutable once published.
#[derive(Clone, Debug, Default)]
pub struct State {
    /// The log version this state reflects (0 = empty database).
    pub version: u64,
    /// Tables and series.
    pub catalog: Arc<Catalog>,
    /// The catalog change log (SPEC §2.3).
    pub catalog_log: Arc<Vec<CatalogChange>>,
    /// Every committed batch id.
    pub batch_ids: BatchIds,
    /// Next unassigned seq (seqs start at 1).
    pub next_seq: u64,
    /// `last` of SPEC §4.2.
    pub last_system_time: Option<Timestamp>,
    /// Live segments by `(table, partition index)`.
    pub partitions: BTreeMap<(String, i64), PartitionSegments>,
    /// Removed but not yet deleted segments, with their removal wall time.
    pub retired: Arc<Vec<(Key, i64)>>,
}

impl State {
    /// The empty database.
    pub fn empty() -> State {
        State {
            next_seq: 1,
            ..State::default()
        }
    }

    /// The latest catalog change time.
    pub fn last_catalog_time(&self) -> Option<Timestamp> {
        self.catalog_log.last().map(|c| c.system_time)
    }

    /// Every live segment.
    pub fn segments(&self) -> impl Iterator<Item = &Arc<SegmentRef>> {
        self.partitions.values().flat_map(|v| v.iter())
    }

    /// Applies the next log entry, validating it against this state. Used both to commit
    /// and to recover, so a successful `apply` is the definition of a valid entry.
    pub fn apply(&self, e: &LogEntry) -> Result<State> {
        self.clone().apply_owned(e)
    }

    /// [`State::apply`] consuming the state, so that parts nobody else shares (the
    /// catalog, during a replay) are updated in place instead of copied.
    pub fn apply_owned(self, e: &LogEntry) -> Result<State> {
        let mut next = self;
        let (prev_version, prev_next_seq, prev_last) =
            (next.version, next.next_seq, next.last_system_time);
        if !format_supported(e.format) {
            return Err(corrupt(format!("log entry format {}", e.format)));
        }
        if e.is_truncation_marker() {
            return Err(corrupt(format!(
                "entry {} is the truncation marker: the log was truncated and no valid \
                 checkpoint covers the missing entries",
                e.version
            )));
        }
        if e.version != prev_version + 1 {
            return Err(corrupt(format!(
                "log entry {} follows version {prev_version}",
                e.version
            )));
        }
        next.version = e.version;
        if !e.catalog.is_empty() {
            let catalog = Arc::make_mut(&mut next.catalog);
            let log = Arc::make_mut(&mut next.catalog_log);
            for c in &e.catalog {
                let change = c.to_change()?;
                catalog
                    .apply(&change.change)
                    .map_err(|err| corrupt(format!("entry {}: {err}", e.version)))?;
                if log
                    .last()
                    .is_some_and(|p| p.system_time > change.system_time)
                {
                    return Err(corrupt(format!(
                        "entry {}: catalog time went back",
                        e.version
                    )));
                }
                log.push(change);
            }
        }
        if let Some(id) = &e.batch_id {
            if id.is_empty() || next.batch_ids.contains(id) {
                return Err(corrupt(format!(
                    "entry {}: batch id {id:?} repeated",
                    e.version
                )));
            }
            next.batch_ids.insert(id.clone());
        }
        // High-water marks only move forward.
        let last = e.last_system_time.map(Timestamp::from_micros);
        if last == Some(None) {
            return Err(corrupt("last_system_time out of range"));
        }
        let last = last.flatten();
        if e.next_seq < prev_next_seq || last < prev_last {
            return Err(corrupt(format!(
                "entry {}: high-water marks went back",
                e.version
            )));
        }
        next.next_seq = e.next_seq;
        next.last_system_time = last;

        // What the entry kind allows: a write adds exactly its stored rows (each seq of
        // `[prev next_seq, next_seq)` once, so segment seq sets cannot overlap); a
        // compaction replaces segments of one partition by one holding the same rows;
        // other entries add or remove nothing.
        //
        // A write's rows all share the batch system_time, which is the new `last` and
        // strictly greater than the previous one (SPEC §4.2). A restore (dump/restore,
        // `Engine::restore`) imports one or more whole batches with their original times:
        // its rows lie in `(previous last, last]` and the greatest of them is `last`.
        match e.kind.as_str() {
            "write" | "restore" => {
                let rows: u64 = e.add.iter().map(|j| j.rows).sum();
                if !e.remove.is_empty() || rows != e.next_seq - prev_next_seq {
                    return Err(corrupt(format!(
                        "entry {}: {} adds {rows} rows for {} seqs",
                        e.version,
                        e.kind,
                        e.next_seq - prev_next_seq
                    )));
                }
                if rows > 0 {
                    let after_prev = |t: i64| prev_last.is_none_or(|p| t > p.as_micros());
                    let last_us = last.map(Timestamp::as_micros);
                    let ok = after_prev(last_us.unwrap_or(i64::MIN))
                        && e.add.iter().all(|j| {
                            let [lo, hi] = j.system_time;
                            Some(hi) <= last_us
                                && after_prev(lo)
                                && (e.kind == "restore" || (lo == hi && Some(hi) == last_us))
                        })
                        && e.add.iter().map(|j| j.system_time[1]).max() == last_us;
                    if !ok {
                        return Err(corrupt(format!(
                            "entry {}: {} rows have system_times inconsistent with the batch",
                            e.version, e.kind
                        )));
                    }
                } else if last != prev_last {
                    return Err(corrupt(format!(
                        "entry {}: {} without rows moves last_system_time",
                        e.version, e.kind
                    )));
                }
            }
            "compact" => {
                // Segments of the added one's partition (a key lies under its own
                // table/partition prefix, so removed ones of that partition are there).
                let part: &[Arc<SegmentRef>] = match e.add.as_slice() {
                    [new] => next
                        .catalog
                        .table(&new.table)
                        .ok()
                        .and_then(|t| Partition::parse_label(t.partition, &new.partition).ok())
                        .and_then(|p| next.partitions.get(&(new.table.clone(), p.index)))
                        .map_or(&[], |v| v.as_slice()),
                    _ => &[],
                };
                let removed: Vec<&Arc<SegmentRef>> = e
                    .remove
                    .iter()
                    .filter_map(|k| part.iter().find(|s| s.key.as_str() == k))
                    .collect();
                let ok = matches!(e.add.as_slice(), [new] if
                    removed.len() == e.remove.len()
                    && removed.len() >= 2
                    && removed.iter().all(|s| s.table == new.table && s.partition.label() == new.partition)
                    && removed.iter().map(|s| s.rows).sum::<u64>() == new.rows
                    && removed.iter().map(|s| s.seq.0).min() == Some(new.seq[0])
                    && removed.iter().map(|s| s.seq.1).max() == Some(new.seq[1]))
                    && e.next_seq == prev_next_seq
                    && last == prev_last;
                if !ok {
                    return Err(corrupt(format!("entry {}: invalid compaction", e.version)));
                }
            }
            "catalog" | "gc" => {
                if !e.add.is_empty() || !e.remove.is_empty() {
                    return Err(corrupt(format!(
                        "entry {}: a {:?} entry cannot add or remove segments",
                        e.version, e.kind
                    )));
                }
            }
            other => {
                return Err(corrupt(format!(
                    "entry {}: unknown kind {other:?}",
                    e.version
                )));
            }
        }

        let mut retired = if e.remove.is_empty() && e.purged.is_empty() {
            None
        } else {
            Some((*next.retired).clone())
        };
        for k in &e.remove {
            let key = Key::new(k.clone()).map_err(|err| corrupt(format!("removed key: {err}")))?;
            // Only a compaction removes, and its removed segments are in the added one's
            // partition (checked above).
            let pk = e
                .add
                .first()
                .and_then(|new| {
                    let t = next.catalog.table(&new.table).ok()?;
                    let p = Partition::parse_label(t.partition, &new.partition).ok()?;
                    Some((new.table.clone(), p.index))
                })
                .ok_or_else(|| corrupt(format!("entry {} removes without adding", e.version)))?;
            let found = next.partitions.get_mut(&pk).and_then(|segs| {
                segs.iter()
                    .position(|s| s.key == key)
                    .map(|i| Arc::make_mut(segs).remove(i))
            });
            if found.is_none() {
                return Err(corrupt(format!(
                    "entry {} removes unknown segment {k}",
                    e.version
                )));
            }
            retired
                .as_mut()
                .expect("cloned when removing")
                .push((key, e.wall_time));
        }
        // Keys are unique: a key lies under its own table/partition prefix (checked by
        // `from_json`), so a re-added key could only be in that partition's live list or
        // among the retired ones.
        let retired_keys: std::collections::HashSet<&str> = if e.add.is_empty() {
            Default::default()
        } else {
            retired
                .as_deref()
                .unwrap_or(&next.retired)
                .iter()
                .map(|(k, _)| k.as_str())
                .collect()
        };
        let mut added: Vec<Arc<SegmentRef>> = Vec::with_capacity(e.add.len());
        for j in &e.add {
            let s = SegmentRef::from_json(j, &next.catalog)?;
            if (e.kind == "write" || e.kind == "restore")
                && !(prev_next_seq <= s.seq.0 && s.seq.1 < e.next_seq)
            {
                return Err(corrupt(format!(
                    "entry {}: segment seqs outside the batch",
                    e.version
                )));
            }
            let pk = (s.table.clone(), s.partition.index);
            if next
                .partitions
                .get(&pk)
                .is_some_and(|v| v.iter().any(|x| x.key == s.key))
                || retired_keys.contains(s.key.as_str())
                || added.iter().any(|x| x.key == s.key)
            {
                return Err(corrupt(format!(
                    "entry {} re-adds segment {}",
                    e.version, s.key
                )));
            }
            added.push(Arc::new(s));
        }
        drop(retired_keys);
        for s in added {
            let segs = next
                .partitions
                .entry((s.table.clone(), s.partition.index))
                .or_default();
            Arc::make_mut(segs).push(s);
        }
        next.partitions.retain(|_, v| !v.is_empty());
        if let Some(mut retired) = retired {
            if !e.purged.is_empty() {
                let purged: std::collections::HashSet<&str> =
                    e.purged.iter().map(String::as_str).collect();
                retired.retain(|(r, _)| !purged.contains(r.as_str()));
            }
            next.retired = Arc::new(retired);
        }
        Ok(next)
    }

    /// The checkpoint of this state.
    pub fn to_checkpoint(&self) -> Checkpoint {
        Checkpoint {
            format: LOG_FORMAT,
            version: self.version,
            catalog: self
                .catalog_log
                .iter()
                .map(CatalogChangeJson::from_change)
                .collect(),
            batch_ids: self.batch_ids.iter().cloned().collect(),
            segments: self.segments().map(|s| s.to_json()).collect(),
            retired: self
                .retired
                .iter()
                .map(|(k, t)| (k.as_str().to_owned(), *t))
                .collect(),
            next_seq: self.next_seq,
            last_system_time: self.last_system_time.map(Timestamp::as_micros),
            entry_crc32c: 0,
            checksum: 0,
        }
    }

    /// Rebuilds a state from a checkpoint.
    pub fn from_checkpoint(c: &Checkpoint) -> Result<State> {
        if !format_supported(c.format) {
            return Err(corrupt(format!("checkpoint format {}", c.format)));
        }
        let mut catalog = Catalog::new();
        let mut log = Vec::with_capacity(c.catalog.len());
        for j in &c.catalog {
            let change = j.to_change()?;
            catalog
                .apply(&change.change)
                .map_err(|e| corrupt(format!("checkpoint {}: {e}", c.version)))?;
            log.push(change);
        }
        let mut s = State {
            version: c.version,
            catalog: Arc::new(catalog),
            catalog_log: Arc::new(log),
            next_seq: c.next_seq,
            last_system_time: match c.last_system_time {
                None => None,
                Some(t) => Some(Timestamp::from_micros(t).ok_or_else(|| corrupt("last time"))?),
            },
            ..State::default()
        };
        for id in &c.batch_ids {
            s.batch_ids.insert(id.clone());
        }
        for j in &c.segments {
            let seg = SegmentRef::from_json(j, &s.catalog)?;
            Arc::make_mut(
                s.partitions
                    .entry((seg.table.clone(), seg.partition.index))
                    .or_default(),
            )
            .push(Arc::new(seg));
        }
        s.retired = Arc::new(
            c.retired
                .iter()
                .map(|(k, t)| {
                    Key::new(k.clone())
                        .map(|k| (k, *t))
                        .map_err(|e| corrupt(format!("retired key: {e}")))
                })
                .collect::<Result<_>>()?,
        );
        Ok(s)
    }
}
