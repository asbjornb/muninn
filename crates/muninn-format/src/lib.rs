//! # muninn-format
//!
//! Immutable **segment files**: the on-disk unit of a Muninn database (decision 0004). A
//! segment holds versions of one table and one partition (UTC day / month / year of
//! `value_time`), sorted by `(series, value_time, received_time, system_time, seq)`, as
//! column chunks grouped into row blocks, followed by a footer with a column directory and
//! statistics. The byte layout is specified normatively in `spec/FORMAT.md`; this crate is
//! its reference implementation.
//!
//! - [`write_segment`] encodes versions into a segment file.
//! - [`parse_tail`] decodes the footer from the last bytes of a file, so a reader can
//!   open a segment with one ranged read and then fetch only the chunks it needs.
//! - [`Footer::decode_engine_block`] / [`Footer::decode_value_chunk`] decode chunks; every
//!   chunk is CRC-32C-checked before it is decoded.
//! - [`SegmentFile`] is a convenience reader over a whole file held in memory.
//!
//! Decoding is total: corrupt, truncated or hostile input yields [`Error`], never a panic,
//! an unbounded allocation or a silently wrong row.

#![forbid(unsafe_code)]

mod codec;
mod partition;
mod segment;

use std::fmt;

pub use codec::Encoding;
pub use partition::Partition;
pub use segment::{
    BlockMeta, ChunkMeta, ColumnStats, Compression, ENGINE_COLUMNS, EngineBlock, FORMAT_VERSION,
    Footer, HEADER_LEN, MAGIC, MAX_BLOCK_ROWS, SegmentFile, SegmentStats, SeriesEntry, TAIL_LEN,
    Tail, TimeBounds, ValueColumn, WriteOptions, parse_tail, write_segment,
};

/// A format error: the input is not a valid segment (or a write was given bad input).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    kind: ErrorKind,
    message: String,
}

/// What kind of [`Error`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// The bytes are not a valid segment: bad magic, checksum mismatch, truncation,
    /// impossible values.
    Corrupt,
    /// A valid-looking segment using a format version or feature this reader does not
    /// know.
    Unsupported,
    /// [`write_segment`] was given input that cannot be written (e.g. no rows, a row
    /// outside the partition, a value of the wrong type).
    InvalidInput,
}

impl Error {
    pub(crate) fn corrupt(message: impl Into<String>) -> Error {
        Error {
            kind: ErrorKind::Corrupt,
            message: message.into(),
        }
    }

    pub(crate) fn unsupported(message: impl Into<String>) -> Error {
        Error {
            kind: ErrorKind::Unsupported,
            message: message.into(),
        }
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Error {
        Error {
            kind: ErrorKind::InvalidInput,
            message: message.into(),
        }
    }

    /// The kind.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "segment {:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for Error {}

/// `Result` with [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;
