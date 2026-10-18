//! [`Engine::restore`]: import versions with their original `system_time` and `seq`.
//!
//! This is the engine half of dump/restore (`spec/DUMP.md`): a restored database must
//! answer every read — under any clock, frontier and system bound — exactly like the
//! original, so the engine-assigned fields cannot be re-assigned. `restore` therefore
//! bypasses SPEC §4.2 assignment and §4.3 deduplication, but keeps every invariant the
//! log and the readers rely on:
//!
//! - `seq` is strictly increasing within the call and at least the database's next seq
//!   (gaps are allowed; later writes continue after the greatest restored seq);
//! - `system_time` is non-decreasing in seq order and never below the database's `last`
//!   (SPEC §4.2), so later writes still satisfy `system_time = max(c, last)`;
//! - every version is well-typed for its table (series exists, values match column
//!   types, tombstones carry no values);
//! - the result is ordinary segments plus one ordinary `write` log entry, so recovery,
//!   compaction, GC and readers need nothing new.
//!
//! A failed restore has no effect.

use std::collections::BTreeMap;

use muninn_core::api::WriteResult;
use muninn_core::catalog::CatalogChange;
use muninn_core::types::Version;
use muninn_core::{Error, ErrorKind, Result, Timestamp};
use muninn_format::Partition;
use muninn_store::Store;

use super::{Engine, lock};
use crate::read::EngineSnapshot;

/// The non-version state a dump needs, read from the *same* pinned state as the
/// snapshot's rows (so a dump is consistent even while another process writes).
impl<S> EngineSnapshot<S> {
    /// The catalog change log (SPEC §2.3) as of this snapshot, oldest first.
    pub fn catalog_log(&self) -> Vec<CatalogChange> {
        self.state.catalog_log.to_vec()
    }

    /// Every batch id committed as of this snapshot (SPEC §4.4), sorted.
    pub fn batch_ids(&self) -> Vec<String> {
        let mut ids = self.state.to_checkpoint().batch_ids;
        ids.sort();
        ids
    }

    /// The next unassigned seq (1 for an empty database).
    pub fn next_seq(&self) -> u64 {
        self.state.next_seq
    }

    /// `last` of SPEC §4.2: the greatest `system_time` of any stored version.
    pub fn last_system_time(&self) -> Option<Timestamp> {
        self.state.last_system_time
    }
}

fn invalid(msg: String) -> Error {
    Error::new(ErrorKind::InvalidArgument, msg)
}

impl<S: Store> Engine<S> {
    /// Imports `versions` (in seq order) with their `system_time` and `seq` exactly as
    /// given, atomically, optionally recording `batch_id` (SPEC §4.4) in the same commit.
    ///
    /// Errors (nothing is changed): `invalid_argument` for an empty or already committed
    /// batch id, a seq that is not strictly increasing or is below the next unassigned
    /// seq, or a `system_time` that decreases or is below the latest stored one;
    /// `unknown_series`, `unknown_column` (more values than the table has columns),
    /// `type_mismatch` and `tombstone_with_values` for malformed versions.
    ///
    /// Unlike [`muninn_core::Database::write`] there is no deduplication: the caller
    /// vouches that the versions come from a valid database (a dump). An empty
    /// `versions` with a batch id only records the id; with neither it is a no-op.
    pub fn restore(&self, versions: &[Version], batch_id: Option<&str>) -> Result<WriteResult> {
        let mut w = lock(&self.writer);
        let st = self.fresh(&mut w)?;
        if let Some(id) = batch_id {
            if id.is_empty() {
                return Err(invalid("batch_id must be non-empty".into()));
            }
            if st.batch_ids.contains(id) {
                return Err(invalid(format!("batch id {id:?} is already committed")));
            }
        }
        let mut next_seq = st.next_seq;
        let mut last = st.last_system_time;
        let mut groups: BTreeMap<(String, Partition), Vec<Version>> = BTreeMap::new();
        for (i, v) in versions.iter().enumerate() {
            let at = |m: String| format!("version {i} (seq {}): {m}", v.seq);
            let s = st.catalog.series_by_id(v.series).ok_or_else(|| {
                Error::new(
                    ErrorKind::UnknownSeries,
                    at(format!("no series with id {}", v.series.0)),
                )
            })?;
            let t = st.catalog.table(&s.table)?;
            if v.values.len() > t.columns.len() {
                return Err(Error::new(
                    ErrorKind::UnknownColumn,
                    at(format!(
                        "{} values but table {:?} has {} columns",
                        v.values.len(),
                        t.name,
                        t.columns.len()
                    )),
                ));
            }
            for (c, val) in t.columns.iter().zip(&v.values) {
                if let Some(val) = val {
                    if val.column_type() != c.ty {
                        return Err(Error::new(
                            ErrorKind::TypeMismatch,
                            at(format!("value {val} does not match column {:?}", c.name)),
                        ));
                    }
                }
            }
            if v.deleted && v.values.iter().any(Option::is_some) {
                return Err(Error::new(
                    ErrorKind::TombstoneWithValues,
                    at("a tombstone must have every value null".into()),
                ));
            }
            if v.seq < next_seq {
                return Err(invalid(at(format!(
                    "seq must be strictly increasing and at least {next_seq}"
                ))));
            }
            if last.is_some_and(|l| v.system_time < l) {
                return Err(invalid(at(format!(
                    "system_time {} is before {}",
                    v.system_time,
                    last.expect("checked")
                ))));
            }
            next_seq = v
                .seq
                .checked_add(1)
                .ok_or_else(|| invalid(at("seq overflows".into())))?;
            last = Some(v.system_time);
            let mut v = v.clone();
            while v.values.last().is_some_and(Option::is_none) {
                v.values.pop();
            }
            groups
                .entry((t.name.clone(), Partition::of(t.partition, v.value_time)))
                .or_default()
                .push(v);
        }
        if versions.is_empty() && batch_id.is_none() {
            return Ok(WriteResult::default());
        }
        let mut entry = self.new_entry(&st, "write");
        entry.batch_id = batch_id.map(str::to_owned);
        for ((table, partition), vs) in &groups {
            let t = st.catalog.table(table)?;
            entry
                .add
                .push(self.put_segment(t, *partition, vs)?.to_json());
        }
        entry.next_seq = next_seq;
        entry.last_system_time = last.map(Timestamp::as_micros);
        self.commit(&mut w, &st, entry)?;
        Ok(WriteResult {
            stored: versions.len() as u64,
            duplicates: 0,
            batch_already_committed: false,
            system_time: versions.first().map(|v| v.system_time),
            first_seq: versions.first().map(|v| v.seq),
        })
    }
}
