//! # muninn-core
//!
//! The shared vocabulary of Muninn, a point-in-time-correct time series database, and an
//! in-memory model of its semantics (`spec/SPEC.md`, v0.1).
//!
//! - [`time`]: µs-UTC [`Timestamp`]s and [`Duration`]s with their RFC 3339 / duration text
//!   forms (SPEC §1).
//! - [`types`]: column types, values, catalog definitions, versions, content identity,
//!   clocks, visibility and knowledge order (SPEC §2–§5) as small pure functions.
//! - [`error`]: [`Error`] with one kind per SPEC §8 code.
//! - [`api`]: request/response types and the [`Database`] / [`Reader`] traits that every
//!   implementation provides.
//! - [`model`]: [`ModelDb`], the naive, obviously-correct oracle.

#![forbid(unsafe_code)]

pub mod api;
pub mod error;
pub mod model;
pub mod time;
pub mod types;

pub use api::{Database, Reader};
pub use error::{Error, ErrorKind, Result};
pub use model::ModelDb;
pub use time::{Duration, Timestamp};
pub use types::{Clock, ColumnDef, ColumnType, Partitioning, SeriesId, TimeRange, Value};
