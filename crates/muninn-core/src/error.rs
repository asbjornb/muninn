//! Errors (SPEC §8).
//!
//! [`ErrorKind`] has one variant per SPEC §8 code, plus a few engine-only kinds that are
//! never produced by the spec semantics (I/O failures, internal invariant violations,
//! commit conflicts between writers). Conformance runners compare
//! [`ErrorKind::spec_code`] with the scenario's `expect.error`.

use std::fmt;

use crate::time::{ParseDurationError, ParseTimestampError};

/// The kind of an [`Error`]. The first nine variants map 1:1 to SPEC §8 codes.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// `unknown_table`: table name not in catalog.
    UnknownTable,
    /// `unknown_series`: series name not in catalog / not in the read's table.
    UnknownSeries,
    /// `unknown_column`: write names a column the table doesn't have.
    UnknownColumn,
    /// `type_mismatch`: value doesn't match the column type.
    TypeMismatch,
    /// `tombstone_with_values`: a tombstone carries a non-null value (SPEC §3).
    TombstoneWithValues,
    /// `invalid_timestamp`: out of range / unparsable timestamp.
    InvalidTimestamp,
    /// `invalid_range`: `value_from > value_to`, `from > to`, bad window.
    InvalidRange,
    /// `invalid_name`: table/column/series name breaks the rules or is reserved.
    InvalidName,
    /// `already_exists`: duplicate table, column or series name.
    AlreadyExists,
    /// Engine only: the underlying storage failed. Never a spec outcome.
    Io,
    /// Engine only: another writer committed first (decision 0004). Never a spec outcome.
    Conflict,
    /// Engine only: an internal invariant was violated (a bug). Never a spec outcome.
    Internal,
}

impl ErrorKind {
    /// All SPEC §8 kinds, in table order.
    pub const SPEC_KINDS: [ErrorKind; 9] = [
        ErrorKind::UnknownTable,
        ErrorKind::UnknownSeries,
        ErrorKind::UnknownColumn,
        ErrorKind::TypeMismatch,
        ErrorKind::TombstoneWithValues,
        ErrorKind::InvalidTimestamp,
        ErrorKind::InvalidRange,
        ErrorKind::InvalidName,
        ErrorKind::AlreadyExists,
    ];

    /// The SPEC §8 code, or `None` for engine-only kinds.
    pub const fn spec_code(self) -> Option<&'static str> {
        Some(match self {
            ErrorKind::UnknownTable => "unknown_table",
            ErrorKind::UnknownSeries => "unknown_series",
            ErrorKind::UnknownColumn => "unknown_column",
            ErrorKind::TypeMismatch => "type_mismatch",
            ErrorKind::TombstoneWithValues => "tombstone_with_values",
            ErrorKind::InvalidTimestamp => "invalid_timestamp",
            ErrorKind::InvalidRange => "invalid_range",
            ErrorKind::InvalidName => "invalid_name",
            ErrorKind::AlreadyExists => "already_exists",
            ErrorKind::Io | ErrorKind::Conflict | ErrorKind::Internal => return None,
        })
    }

    /// Inverse of [`ErrorKind::spec_code`].
    pub fn from_spec_code(code: &str) -> Option<ErrorKind> {
        Self::SPEC_KINDS
            .into_iter()
            .find(|k| k.spec_code() == Some(code))
    }

    /// A stable name for every kind (the spec code, or `io` / `conflict` / `internal`).
    pub const fn name(self) -> &'static str {
        match self.spec_code() {
            Some(code) => code,
            None => match self {
                ErrorKind::Io => "io",
                ErrorKind::Conflict => "conflict",
                _ => "internal",
            },
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A Muninn error: a [`ErrorKind`] plus a human-readable message.
///
/// SPEC §8: a failed operation has no effect. Every implementation of
/// [`crate::api::Database`] must uphold that for every error it returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    kind: ErrorKind,
    message: String,
}

impl Error {
    /// Creates an error of `kind` with `message`.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Error {
        Error {
            kind,
            message: message.into(),
        }
    }

    /// The error kind.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The human-readable message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Shorthand for an [`ErrorKind::Internal`] error.
    pub fn internal(message: impl Into<String>) -> Error {
        Error::new(ErrorKind::Internal, message)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

impl std::error::Error for Error {}

impl From<ParseTimestampError> for Error {
    fn from(e: ParseTimestampError) -> Error {
        Error::new(ErrorKind::InvalidTimestamp, e.to_string())
    }
}

impl From<ParseDurationError> for Error {
    /// Durations only appear as window bounds (and series metadata); a malformed one is a
    /// "bad window" (`invalid_range`, SPEC §8).
    fn from(e: ParseDurationError) -> Error {
        Error::new(ErrorKind::InvalidRange, e.to_string())
    }
}

/// `Result` with [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_codes_roundtrip() {
        for k in ErrorKind::SPEC_KINDS {
            let code = k.spec_code().unwrap();
            assert_eq!(ErrorKind::from_spec_code(code), Some(k));
            assert_eq!(k.name(), code);
        }
        assert_eq!(ErrorKind::Io.spec_code(), None);
        assert_eq!(ErrorKind::Internal.spec_code(), None);
        assert_eq!(ErrorKind::Conflict.spec_code(), None);
        assert_eq!(ErrorKind::from_spec_code("io"), None);
        assert_eq!(ErrorKind::from_spec_code("nope"), None);
    }

    #[test]
    fn display() {
        let e = Error::new(ErrorKind::UnknownSeries, "no series \"x\"");
        assert_eq!(e.to_string(), "unknown_series: no series \"x\"");
        assert_eq!(Error::internal("boom").kind(), ErrorKind::Internal);
    }
}
