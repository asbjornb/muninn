//! Flushing the memtable (decision 0009): the live inline segments of each
//! `(table, partition, compaction group)` are merged into one segment file, and a `flush`
//! entry swaps the files in for the inline segments atomically. The versions are the same
//! before and after, so no read result changes.
//!
//! The files are written without holding the writer lock (inline segments are immutable);
//! only the commit takes it, against the then-current state. If the flushed inline
//! segments are no longer all live (another process flushed them), nothing is committed
//! and the files are orphans for GC.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, TryLockError};
use std::time::{Duration, Instant};

use muninn_core::types::{TableDef, Version};
use muninn_core::{Error, ErrorKind, Result};
use muninn_format::{Partition, SegmentFile};
use muninn_store::{Key, Store};

use super::{Core, lock};
use crate::state::SegmentRef;

/// Control of the background flusher thread.
#[derive(Default)]
pub(crate) struct FlushSignal {
    ctl: Mutex<FlushCtl>,
    cv: Condvar,
}

#[derive(Default)]
struct FlushCtl {
    /// A flusher thread runs.
    running: bool,
    /// Stop the thread.
    stop: bool,
    /// Check the thresholds now.
    kick: bool,
}

impl FlushSignal {
    pub(crate) fn start(&self) {
        lock(&self.ctl).running = true;
    }

    pub(crate) fn stop(&self) {
        let mut c = lock(&self.ctl);
        c.stop = true;
        self.cv.notify_all();
    }
}

/// How often the background flusher looks at the memtable's age without being kicked.
const FLUSH_TICK: Duration = Duration::from_millis(100);

/// The background flusher: flushes whenever a threshold is reached (kicked by writers) or
/// the memtable's oldest segment is old enough; exits when the engine is dropped.
pub(crate) fn flusher_loop<S: Store>(core: Arc<Core<S>>) {
    loop {
        {
            let sig = &core.flush_signal;
            let mut c = lock(&sig.ctl);
            if !c.stop && !c.kick {
                c = sig
                    .cv
                    .wait_timeout(c, FLUSH_TICK)
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .0;
            }
            if c.stop {
                return;
            }
            c.kick = false;
        }
        core.write_pending_checkpoint();
        if core.flush_due(true) {
            // Failures are retried on the next tick (the engine reloads if it must).
            let _ = core.flush();
        }
    }
}

impl<S: Store> Core<S> {
    /// Is a background flusher running?
    pub(crate) fn has_flusher(&self) -> bool {
        lock(&self.flush_signal.ctl).running
    }

    /// Asks the background flusher to check the thresholds now.
    pub(crate) fn kick_flusher(&self) {
        let mut c = lock(&self.flush_signal.ctl);
        c.kick = true;
        self.flush_signal.cv.notify_all();
    }

    /// Has the memtable reached a flush threshold (rows, segments, or — if `with_age` —
    /// age)?
    pub(crate) fn flush_due(&self, with_age: bool) -> bool {
        let opts = &self.shared.opts;
        let st = Arc::clone(&self.published().state);
        if st.memtable.is_empty() {
            return false;
        }
        let age_us = i64::try_from(opts.flush_age.as_micros()).unwrap_or(i64::MAX);
        st.memtable_rows() >= opts.flush_rows
            || st.memtable.len() >= opts.flush_segments
            || with_age
                && st
                    .memtable_oldest()
                    .is_some_and(|t| self.wall().saturating_sub(t) >= age_us)
    }

    /// Flushes unless another flush is running.
    pub(crate) fn try_flush(&self) -> Result<bool> {
        match self.flush_lock.try_lock() {
            Ok(g) => self.flush_locked(g),
            Err(TryLockError::Poisoned(p)) => self.flush_locked(p.into_inner()),
            Err(TryLockError::WouldBlock) => Ok(false),
        }
    }

    /// Flushes the whole memtable now (waiting for a running flush first). Returns
    /// whether anything was flushed.
    pub(crate) fn flush(&self) -> Result<bool> {
        let g = lock(&self.flush_lock);
        self.flush_locked(g)
    }

    fn flush_locked(&self, _guard: std::sync::MutexGuard<'_, ()>) -> Result<bool> {
        let st = self.current_state()?;
        if st.memtable.is_empty() {
            return Ok(false);
        }
        // Group by (table, partition, compaction group), keeping the table definition
        // the inline segments were read with.
        type Group = (TableDef, Partition, Vec<Arc<SegmentRef>>);
        let mut groups: BTreeMap<(String, i64, i64), Group> = BTreeMap::new();
        for seg in st.memtable.iter() {
            let t = st.catalog.table(&seg.table)?;
            let g = self.compaction_group(t, seg);
            groups
                .entry((seg.table.clone(), seg.partition.index, g))
                .or_insert_with(|| (t.clone(), seg.partition, Vec::new()))
                .2
                .push(Arc::clone(seg));
        }
        drop(st);
        let mut versions: Vec<Vec<Version>> = Vec::with_capacity(groups.len());
        for (t, _, segs) in groups.values() {
            let mut vs = Vec::new();
            for seg in segs {
                let d = seg.inline.as_ref().expect("memtable segments are inline");
                let file = SegmentFile::open(&d.bytes)
                    .and_then(|f| f.versions_within(t.columns.len()))
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Io,
                            format!("corrupt inline segment {}: {e}", seg.key),
                        )
                    })?;
                vs.extend(file);
            }
            versions.push(vs);
        }
        let started = Instant::now();
        let jobs: Vec<(&TableDef, Partition, &[Version])> = groups
            .values()
            .zip(&versions)
            .map(|((t, p, _), vs)| (t, *p, vs.as_slice()))
            .collect();
        let files = self.put_segments(&jobs)?;
        drop(jobs);
        drop(versions);

        let mut w = lock(&self.writer);
        let cur = self.fresh(&mut w)?;
        let live: HashSet<&Key> = cur.memtable.iter().map(|s| &s.key).collect();
        if !groups
            .values()
            .flat_map(|(_, _, segs)| segs)
            .all(|s| live.contains(&s.key))
        {
            // Flushed elsewhere meanwhile; our files are orphans.
            return Ok(false);
        }
        drop(live);
        self.check_commit_age(started)?;
        let mut entry = self.new_entry(&cur, "flush");
        entry.add = files.iter().map(SegmentRef::to_json).collect();
        entry.remove = groups
            .values()
            .flat_map(|(_, _, segs)| segs.iter().map(|s| s.key.as_str().to_owned()))
            .collect();
        self.commit(&mut w, &cur, entry)?;
        let mut dedup = lock(&self.dedup);
        for ((t, p, segs), file) in groups.values().zip(&files) {
            let removed: Vec<Key> = segs.iter().map(|s| s.key.clone()).collect();
            dedup.record_compaction(&t.name, *p, &removed, &file.key);
        }
        self.flushes.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }
}
