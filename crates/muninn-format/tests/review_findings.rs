//! Review findings in the segment format (see crates/muninn-engine/tests/review_findings.rs
//! for the engine ones). Confirmed, still-open bugs are `#[ignore = "review finding N"]`:
//!
//! ```sh
//! cargo test -p muninn-format --test review_findings -- --ignored
//! ```

use muninn_core::types::{ColumnType, SeriesId, Version};
use muninn_core::{Partitioning, Timestamp};
use muninn_format::{Partition, SegmentFile, ValueColumn, WriteOptions, write_segment};

/// Finding 4: `SegmentFile::versions` sizes every row's value vector by the *largest
/// column id in the footer* (`vec![None; last_id + 1]`), and the trailing-null `pop`s keep
/// that capacity. Column ids are only required to be strictly increasing, so a tiny,
/// CRC-valid file whose single column has id 2^16 makes every decoded row hold a
/// 65 537-entry allocation (with id ~2^32: ~100 GB per row → abort). Compaction calls
/// `versions()` on stored files.
#[test]
fn versions_allocation_is_bounded_by_input_not_by_column_id() {
    let t = Timestamp::from_micros(0).unwrap();
    let versions: Vec<Version> = (0..4u64)
        .map(|i| Version {
            series: SeriesId(1),
            value_time: t,
            published_time: None,
            received_time: Timestamp::from_micros(i as i64).unwrap(),
            deleted: false,
            values: vec![],
            system_time: t,
            seq: i + 1,
        })
        .collect();
    let cols = [ValueColumn {
        id: 1 << 16,
        ty: ColumnType::F64,
    }];
    let bytes = write_segment(
        "t",
        Partition::of(Partitioning::Day, t),
        &cols,
        &versions,
        &WriteOptions::default(),
    )
    .unwrap();
    assert!(bytes.len() < 1024);
    let file = SegmentFile::open(&bytes).unwrap();
    let decoded = file.versions().unwrap();
    let held: usize = decoded.iter().map(|v| v.values.capacity()).sum();
    assert!(
        held <= 64,
        "{} byte file decoded into rows holding {held} value slots",
        bytes.len()
    );
}
