//! # muninn-store
//!
//! The storage interface of decision 0005: a tiny *synchronous* object-store trait that the
//! engine is written against, with three backends:
//!
//! - [`MemoryStore`]: thread-safe in-memory map (tests, simulation).
//! - [`LocalFsStore`]: a directory on a local filesystem with crash-safe atomic puts.
//! - [`FaultyStore`]: a deterministic fault-injecting wrapper around any store.
//!
//! [`conformance`] holds a backend test suite that every implementation must pass, so that
//! all backends are observably identical (including edge cases such as key/prefix
//! conflicts that object stores would allow but a filesystem cannot represent).
//!
//! ## Keys
//!
//! Keys are `/`-separated relative paths ([`Key`]). Components must be non-empty, must not
//! start with `.` (that excludes `.` and `..`, and reserves dot-names for backend temp
//! files) and must not contain `\` or NUL. Because a filesystem cannot hold both `a` and
//! `a/b`, no backend allows an object key to be a path-prefix of another object key
//! ([`StoreErrorKind::Conflict`]).

#![forbid(unsafe_code)]

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

pub mod conformance;
mod faulty;
mod local;
mod memory;

pub use faulty::{FaultKind, FaultyStore, OpKind, OpRecord};
pub use local::LocalFsStore;
pub use memory::MemoryStore;

/// A validated object key: a relative `/`-separated path. See the crate docs for the rules.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(String);

impl Key {
    /// Validates and wraps `key`.
    pub fn new(key: impl Into<String>) -> Result<Key> {
        let key = key.into();
        validate_key(&key)?;
        Ok(Key(key))
    }

    /// The key text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The `/`-separated components.
    pub fn components(&self) -> impl Iterator<Item = &str> {
        self.0.split('/')
    }

    /// Every proper ancestor path of this key (`a/b/c` → `a`, `a/b`).
    pub fn ancestors(&self) -> impl Iterator<Item = &str> {
        self.0
            .char_indices()
            .filter(|&(_, c)| c == '/')
            .map(|(i, _)| &self.0[..i])
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Key({:?})", self.0)
    }
}

impl AsRef<str> for Key {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for Key {
    type Error = StoreError;
    fn try_from(s: &str) -> Result<Key> {
        Key::new(s)
    }
}

fn validate_key(key: &str) -> Result<()> {
    let bad = |why: &str| {
        Err(StoreError::new(
            StoreErrorKind::InvalidKey,
            format!("invalid key {key:?}: {why}"),
        ))
    };
    if key.is_empty() {
        return bad("empty");
    }
    if key.starts_with('/') {
        return bad("leading '/'");
    }
    for c in key.split('/') {
        if c.is_empty() {
            return bad("empty component");
        }
        if c.starts_with('.') {
            return bad("component starts with '.' ('.', '..' and dot-names are reserved)");
        }
        if c.contains(['\\', '\0']) {
            return bad("contains '\\' or NUL");
        }
    }
    Ok(())
}

/// Validates a `list` prefix: any string that is a prefix of some valid key, i.e. a valid
/// key, the empty string, or a valid key followed by `/`, or a valid key with a partial
/// last component.
fn validate_prefix(prefix: &str) -> Result<()> {
    if prefix.is_empty() {
        return Ok(());
    }
    // `a/b/` and `a/b/x` are prefixes of `a/b/xy`; check what would be a valid key.
    let probe = if prefix.ends_with('/') {
        format!("{prefix}x")
    } else {
        prefix.to_owned()
    };
    validate_key(&probe).map_err(|_| {
        StoreError::new(
            StoreErrorKind::InvalidKey,
            format!("invalid list prefix {prefix:?}"),
        )
    })
}

/// Size metadata from [`Store::head`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectMeta {
    /// Object size in bytes.
    pub size: u64,
}

/// Outcome of [`Store::put_if_absent`].
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum PutOutcome {
    /// The object did not exist and now holds the given bytes.
    Created,
    /// An object already existed under the key; nothing was written.
    AlreadyExists,
}

/// What went wrong in a store operation.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum StoreErrorKind {
    /// No object under the key.
    NotFound,
    /// The key (or list prefix) is malformed.
    InvalidKey,
    /// `get_range` with `start > end` or `end > size`.
    InvalidRange,
    /// The key is a path-prefix of an existing key, or has an existing key as a prefix.
    Conflict,
    /// Any other failure (I/O, injected faults). The operation may or may not have taken
    /// effect.
    Io,
}

/// A store error.
#[derive(Debug)]
pub struct StoreError {
    kind: StoreErrorKind,
    message: String,
    injected: bool,
    source: Option<std::io::Error>,
}

impl StoreError {
    /// Creates an error.
    pub fn new(kind: StoreErrorKind, message: impl Into<String>) -> StoreError {
        StoreError {
            kind,
            message: message.into(),
            injected: false,
            source: None,
        }
    }

    /// An `Io` error wrapping `source`.
    pub fn io(context: impl Into<String>, source: std::io::Error) -> StoreError {
        StoreError {
            kind: StoreErrorKind::Io,
            message: format!("{}: {source}", context.into()),
            injected: false,
            source: Some(source),
        }
    }

    pub(crate) fn injected(message: impl Into<String>) -> StoreError {
        StoreError {
            kind: StoreErrorKind::Io,
            message: message.into(),
            injected: true,
            source: None,
        }
    }

    pub(crate) fn not_found(key: &Key) -> StoreError {
        StoreError::new(StoreErrorKind::NotFound, format!("no object {key}"))
    }

    /// The error kind.
    pub fn kind(&self) -> StoreErrorKind {
        self.kind
    }

    /// The message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Was this produced by [`FaultyStore`]? (Callers must not special-case this; it exists
    /// for test assertions.)
    pub fn is_injected(&self) -> bool {
        self.injected
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|e| e as _)
    }
}

/// `Result` with [`StoreError`].
pub type Result<T, E = StoreError> = std::result::Result<T, E>;

/// A minimal synchronous object store (decision 0005).
///
/// Semantics every backend must provide (checked by [`conformance`]):
///
/// - Objects are immutable byte strings; `put` replaces an object atomically (readers see
///   the old or the new bytes, never a mix), `put_if_absent` creates atomically and never
///   overwrites.
/// - A successful mutating call is durable when it returns (for persistent backends).
/// - Any `Io` error from a mutating call is *ambiguous*: the change may or may not have
///   happened. Callers recover by reading back.
pub trait Store: Send + Sync {
    /// The whole object. `NotFound` if absent.
    fn get(&self, key: &Key) -> Result<Vec<u8>>;

    /// Bytes `range.start..range.end` of the object. Requires
    /// `start <= end <= size` (else `InvalidRange`); `size..size` is a valid empty read.
    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>>;

    /// Object metadata. `NotFound` if absent.
    fn head(&self, key: &Key) -> Result<ObjectMeta>;

    /// Creates or atomically replaces the object.
    fn put(&self, key: &Key, data: &[u8]) -> Result<()>;

    /// Creates the object only if no object exists under `key`. Exactly one of several
    /// concurrent callers for the same absent key gets `Created`.
    fn put_if_absent(&self, key: &Key, data: &[u8]) -> Result<PutOutcome>;

    /// All keys that start with `prefix` (plain string prefix, like S3: `"a"` matches
    /// `"a/b"` and `"ab"`), sorted by byte order. `""` lists everything.
    fn list(&self, prefix: &str) -> Result<Vec<Key>>;

    /// Removes the object. Deleting an absent key succeeds (idempotent).
    fn delete(&self, key: &Key) -> Result<()>;

    /// Removes several objects, in no particular order; like [`Store::delete`] for each
    /// (durable once it returns; on an error some may have been removed). Backends may
    /// do this more cheaply than one `delete` each (one directory sync per directory).
    fn delete_many(&self, keys: &[Key]) -> Result<()> {
        keys.iter().try_for_each(|k| self.delete(k))
    }
}

macro_rules! forward_store {
    ($($ty:ty),*) => {$(
        impl<S: Store + ?Sized> Store for $ty {
            fn get(&self, key: &Key) -> Result<Vec<u8>> { (**self).get(key) }
            fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>> { (**self).get_range(key, range) }
            fn head(&self, key: &Key) -> Result<ObjectMeta> { (**self).head(key) }
            fn put(&self, key: &Key, data: &[u8]) -> Result<()> { (**self).put(key, data) }
            fn put_if_absent(&self, key: &Key, data: &[u8]) -> Result<PutOutcome> { (**self).put_if_absent(key, data) }
            fn list(&self, prefix: &str) -> Result<Vec<Key>> { (**self).list(prefix) }
            fn delete(&self, key: &Key) -> Result<()> { (**self).delete(key) }
            fn delete_many(&self, keys: &[Key]) -> Result<()> { (**self).delete_many(keys) }
        }
    )*};
}

forward_store!(Arc<S>, Box<S>, &S);

/// Checks `range` against an object of `size` bytes (shared by all backends).
pub(crate) fn check_range(key: &Key, range: &Range<u64>, size: u64) -> Result<()> {
    if range.start > range.end || range.end > size {
        return Err(StoreError::new(
            StoreErrorKind::InvalidRange,
            format!(
                "range {}..{} out of bounds for {key} ({size} bytes)",
                range.start, range.end
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_validation() {
        for good in [
            "a",
            "a/b",
            "log/00000000000000000001.json",
            "seg-1/x_y.col",
            "a b",
            "ü/€",
        ] {
            assert!(Key::new(good).is_ok(), "{good:?}");
        }
        for bad in [
            "", "/a", "a/", "a//b", ".", "..", "a/../b", "a/./b", ".hidden", "a/.tmp", "a\\b",
            "a\0b",
        ] {
            assert_eq!(
                Key::new(bad).unwrap_err().kind(),
                StoreErrorKind::InvalidKey,
                "{bad:?}"
            );
        }
        let k = Key::new("a/b/c").unwrap();
        assert_eq!(k.components().collect::<Vec<_>>(), ["a", "b", "c"]);
        assert_eq!(k.ancestors().collect::<Vec<_>>(), ["a", "a/b"]);
        assert_eq!(Key::new("a").unwrap().ancestors().count(), 0);
    }

    #[test]
    fn prefix_validation() {
        for good in ["", "a", "a/", "a/b", "log/000"] {
            assert!(validate_prefix(good).is_ok(), "{good:?}");
        }
        for bad in ["/", "/a", "a//", "../", "a/../", ".", "a/."] {
            assert!(validate_prefix(bad).is_err(), "{bad:?}");
        }
    }
}
