//! The durable layout on the store (decision 0004) and its JSON encodings.
//!
//! ```text
//! log/<20-digit version>.json             commit entries, written with put_if_absent
//! log/<20-digit version>.checkpoint.json  full state as of that version (optimisation)
//! log/_last_checkpoint                    {"version": N} hint (optimisation, never trusted)
//! segments/<table>/<partition>/<created µs, 20 digits>-<nonce, 16 hex>.seg
//! ```
//!
//! Version 0 is the empty database; the first commit is version 1. The log is the only
//! source of truth: a segment file that no committed entry references is an orphan.

use std::collections::BTreeMap;
use std::sync::Arc;

use muninn_core::catalog::{CatalogChange, CatalogChangeKind};
use muninn_core::types::{SeriesDef, SeriesId, TableDef};
use muninn_core::{
    Clock, ColumnDef, ColumnType, Duration, Error, ErrorKind, Partitioning, Result, Timestamp,
};
use muninn_store::Key;
use serde::{Deserialize, Serialize};

/// Format version of log entries and checkpoints.
pub const LOG_FORMAT: u32 = 1;

/// Key of commit entry `version`.
pub fn entry_key(version: u64) -> Key {
    Key::new(format!("log/{version:020}.json")).expect("valid key")
}

/// Key of the checkpoint for `version`.
pub fn checkpoint_key(version: u64) -> Key {
    Key::new(format!("log/{version:020}.checkpoint.json")).expect("valid key")
}

/// Key of the last-checkpoint hint.
pub fn hint_key() -> Key {
    Key::new("log/_last_checkpoint").expect("valid key")
}

/// Parses `log/<20 digits>.checkpoint.json` into its version.
pub fn parse_checkpoint_key(key: &str) -> Option<u64> {
    let digits = key.strip_prefix("log/")?.strip_suffix(".checkpoint.json")?;
    (digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

/// Prefix of every segment key.
pub const SEGMENTS_PREFIX: &str = "segments/";

/// Key of a new segment.
pub fn segment_key(table: &str, partition_label: &str, created: i64, nonce: u64) -> Key {
    Key::new(format!(
        "{SEGMENTS_PREFIX}{table}/{partition_label}/{:020}-{nonce:016x}.seg",
        created.max(0)
    ))
    .expect("valid key")
}

/// The creation time (wall µs) encoded in a segment key, if it has the engine's shape.
pub fn segment_created(key: &str) -> Option<i64> {
    let name = key.strip_prefix(SEGMENTS_PREFIX)?.rsplit('/').next()?;
    let stem = name.strip_suffix(".seg")?;
    let (created, nonce) = stem.split_once('-')?;
    if created.len() != 20 || nonce.len() != 16 || !nonce.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    created.parse().ok()
}

pub(crate) fn corrupt(what: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Io, format!("corrupt database: {what}"))
}

// ---------------------------------------------------------------------------------------
// Commit entries
// ---------------------------------------------------------------------------------------

/// One commit entry: `log/<version>.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogEntry {
    /// [`LOG_FORMAT`].
    pub format: u32,
    /// This entry's version (= the previous version + 1).
    pub version: u64,
    /// What kind of change (informational): `catalog`, `write`, `compact`, `gc`.
    pub kind: String,
    /// The committing writer's wall clock (µs since the epoch); used for GC grace periods.
    pub wall_time: i64,
    /// Catalog changes (SPEC §2.3), applied in order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub catalog: Vec<CatalogChangeJson>,
    /// The batch id recorded by this write (SPEC §4.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<String>,
    /// Segments added.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub add: Vec<SegmentJson>,
    /// Segment keys removed (compaction). They become *retired*: no longer part of the
    /// current state, deleted by GC after the grace period.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remove: Vec<String>,
    /// Retired segment keys whose files GC deleted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub purged: Vec<String>,
    /// The next unassigned `seq` after this entry.
    pub next_seq: u64,
    /// `last` of SPEC §4.2 after this entry (µs), or null if nothing is stored.
    pub last_system_time: Option<i64>,
}

/// A segment as recorded in the log: its key plus the statistics needed to prune it
/// without reading its footer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentJson {
    /// Store key.
    pub key: String,
    /// Table.
    pub table: String,
    /// Partition label (`YYYY-MM-DD`, `YYYY-MM`, `YYYY`).
    pub partition: String,
    /// File size.
    pub size: u64,
    /// Rows.
    pub rows: u64,
    /// `[min, max]` of value_time (µs).
    pub value_time: [i64; 2],
    /// `[min, max]` of received_time.
    pub received_time: [i64; 2],
    /// `[min, max]` of the effective published time.
    pub published_time: [i64; 2],
    /// `[min, max]` of system_time.
    pub system_time: [i64; 2],
    /// `[min, max]` of seq.
    pub seq: [u64; 2],
    /// `[min, max]` of series id.
    pub series: [u32; 2],
    /// Wall clock at creation (µs).
    pub created: i64,
}

/// A catalog change as recorded in the log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum CatalogChangeJson {
    /// SPEC §2.1 create table.
    CreateTable {
        /// Commit time (µs).
        system_time: i64,
        /// Table name.
        name: String,
        /// Columns.
        columns: Vec<ColumnJson>,
        /// `received` / `published` / `system`.
        default_clock: String,
        /// `day` / `month` / `year`.
        partition: String,
    },
    /// SPEC §2.1 add column.
    AddColumn {
        /// Commit time (µs).
        system_time: i64,
        /// Table name.
        table: String,
        /// The column.
        column: ColumnJson,
    },
    /// SPEC §2.2 create series.
    CreateSeries {
        /// Commit time (µs).
        system_time: i64,
        /// Assigned id.
        id: u32,
        /// Name.
        name: String,
        /// Table.
        table: String,
        /// Tags.
        tags: BTreeMap<String, String>,
        /// Timezone.
        timezone: String,
        /// Resolution in µs, or null.
        resolution: Option<i64>,
    },
}

/// A column definition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnJson {
    /// Name.
    pub name: String,
    /// `f64` / `i64` / `bool` / `string` / `timestamp`.
    #[serde(rename = "type")]
    pub ty: String,
}

fn ts(us: i64) -> Result<Timestamp> {
    Timestamp::from_micros(us).ok_or_else(|| corrupt(format!("timestamp {us} out of range")))
}

impl ColumnJson {
    fn from_def(c: &ColumnDef) -> ColumnJson {
        ColumnJson {
            name: c.name.clone(),
            ty: c.ty.name().to_owned(),
        }
    }

    fn to_def(&self) -> Result<ColumnDef> {
        let ty = ColumnType::parse(&self.ty)
            .ok_or_else(|| corrupt(format!("column type {:?}", self.ty)))?;
        Ok(ColumnDef::new(self.name.clone(), ty))
    }
}

impl CatalogChangeJson {
    /// Encodes a core change.
    pub fn from_change(c: &CatalogChange) -> CatalogChangeJson {
        let system_time = c.system_time.as_micros();
        match &c.change {
            CatalogChangeKind::CreateTable(t) => CatalogChangeJson::CreateTable {
                system_time,
                name: t.name.clone(),
                columns: t.columns.iter().map(ColumnJson::from_def).collect(),
                default_clock: t.default_clock.name().to_owned(),
                partition: t.partition.name().to_owned(),
            },
            CatalogChangeKind::AddColumn { table, column } => CatalogChangeJson::AddColumn {
                system_time,
                table: table.clone(),
                column: ColumnJson::from_def(column),
            },
            CatalogChangeKind::CreateSeries(s) => CatalogChangeJson::CreateSeries {
                system_time,
                id: s.id.0,
                name: s.name.to_string(),
                table: s.table.clone(),
                tags: s.tags.clone(),
                timezone: s.timezone.clone(),
                resolution: s.resolution.map(Duration::as_micros),
            },
        }
    }

    /// Decodes into a core change.
    pub fn to_change(&self) -> Result<CatalogChange> {
        Ok(match self {
            CatalogChangeJson::CreateTable {
                system_time,
                name,
                columns,
                default_clock,
                partition,
            } => CatalogChange {
                system_time: ts(*system_time)?,
                change: CatalogChangeKind::CreateTable(TableDef {
                    name: name.clone(),
                    columns: columns
                        .iter()
                        .map(ColumnJson::to_def)
                        .collect::<Result<_>>()?,
                    default_clock: Clock::parse(default_clock)
                        .ok_or_else(|| corrupt(format!("clock {default_clock:?}")))?,
                    partition: Partitioning::parse(partition)
                        .ok_or_else(|| corrupt(format!("partitioning {partition:?}")))?,
                }),
            },
            CatalogChangeJson::AddColumn {
                system_time,
                table,
                column,
            } => CatalogChange {
                system_time: ts(*system_time)?,
                change: CatalogChangeKind::AddColumn {
                    table: table.clone(),
                    column: column.to_def()?,
                },
            },
            CatalogChangeJson::CreateSeries {
                system_time,
                id,
                name,
                table,
                tags,
                timezone,
                resolution,
            } => CatalogChange {
                system_time: ts(*system_time)?,
                change: CatalogChangeKind::CreateSeries(SeriesDef {
                    id: SeriesId(*id),
                    name: Arc::from(name.as_str()),
                    table: table.clone(),
                    tags: tags.clone(),
                    timezone: timezone.clone(),
                    resolution: resolution.map(Duration::from_micros),
                }),
            },
        })
    }
}

// ---------------------------------------------------------------------------------------
// Checkpoints
// ---------------------------------------------------------------------------------------

/// A full-state checkpoint: `log/<version>.checkpoint.json`. Equivalent to replaying
/// entries `1..=version`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    /// [`LOG_FORMAT`].
    pub format: u32,
    /// The version this checkpoint reflects.
    pub version: u64,
    /// The whole catalog change log; the catalog is its replay.
    pub catalog: Vec<CatalogChangeJson>,
    /// Every committed batch id.
    pub batch_ids: Vec<String>,
    /// Live segments.
    pub segments: Vec<SegmentJson>,
    /// Retired (removed but not yet deleted) segment keys with their removal wall time.
    pub retired: Vec<(String, i64)>,
    /// Next unassigned seq.
    pub next_seq: u64,
    /// `last` of SPEC §4.2 (µs).
    pub last_system_time: Option<i64>,
}

/// The `_last_checkpoint` hint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hint {
    /// Version of the latest checkpoint known when the hint was written.
    pub version: u64,
}

/// Serialises to JSON bytes.
pub fn to_json<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(v).map_err(|e| Error::internal(format!("serialising: {e}")))
}

/// Parses JSON bytes (`what` for errors).
pub fn from_json<T: for<'de> Deserialize<'de>>(bytes: &[u8], what: &str) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|e| corrupt(format!("{what}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys() {
        assert_eq!(entry_key(7).as_str(), "log/00000000000000000007.json");
        assert_eq!(parse_checkpoint_key(checkpoint_key(42).as_str()), Some(42));
        assert_eq!(parse_checkpoint_key(entry_key(42).as_str()), None);
        assert_eq!(parse_checkpoint_key("log/_last_checkpoint"), None);
        let k = segment_key("t", "2025-01-01", 123, 0xabc);
        assert_eq!(
            k.as_str(),
            "segments/t/2025-01-01/00000000000000000123-0000000000000abc.seg"
        );
        assert_eq!(segment_created(k.as_str()), Some(123));
        assert_eq!(segment_created("segments/t/x/foo.seg"), None);
    }
}
