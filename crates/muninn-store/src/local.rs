//! Local filesystem backend.
//!
//! Crash safety: an object file only ever appears under its final name by an atomic
//! directory operation on a fully written, fsynced temp file in the same directory:
//!
//! - `put`: write temp (`O_EXCL`) → `fsync(temp)` → `rename(temp, key)` → `fsync(dir)`.
//! - `put_if_absent`: write temp → `fsync(temp)` → `link(temp, key)` (fails with `EEXIST`
//!   if the key exists, atomically) → `unlink(temp)` → `fsync(dir)`.
//!
//! So a crash can never expose a torn object; at worst it leaves a temp file behind.
//! Temp files are named `.muninn-tmp-*` (keys cannot start with `.`), are never listed,
//! and are removed by [`LocalFsStore::remove_stale_temp_files`] (call it from the single
//! writer at startup — not from readers, which could race with a live writer).
//! Newly created directories are made durable by fsyncing their parents. Requires a
//! filesystem with hard links (any local POSIX filesystem).

use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    Key, ObjectMeta, PutOutcome, Result, Store, StoreError, StoreErrorKind, check_range,
    validate_key, validate_prefix,
};

const TEMP_PREFIX: &str = ".muninn-tmp-";
/// How often a put retries when a concurrent `delete` prunes the directory under it.
const DIR_RACE_RETRIES: usize = 8;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A [`Store`] rooted at a local directory. Key `a/b/c` is the file `<root>/a/b/c`.
#[derive(Debug, Clone)]
pub struct LocalFsStore {
    root: PathBuf,
}

fn io_err(what: &str, path: &Path, e: io::Error) -> StoreError {
    StoreError::io(format!("{what} {}", path.display()), e)
}

fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io_err("fsync dir", dir, e))
}

/// Errors that mean "there is no object at this path".
fn is_absent(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        ErrorKind::NotFound | ErrorKind::IsADirectory | ErrorKind::NotADirectory
    )
}

fn conflict(key: &Key) -> StoreError {
    StoreError::new(
        StoreErrorKind::Conflict,
        format!("key {key} conflicts with an existing key (one is a path-prefix of the other)"),
    )
}

/// Removes `dir` if it (recursively) contains only empty directories. Returns whether it
/// is gone.
fn prune_empty_dirs(dir: &Path) -> bool {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                prune_empty_dirs(&entry.path());
            }
        }
    }
    fs::remove_dir(dir).is_ok()
}

impl LocalFsStore {
    /// Opens (creating if needed) a store rooted at `root`.
    pub fn open(root: impl Into<PathBuf>) -> Result<LocalFsStore> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|e| io_err("create root", &root, e))?;
        if let Some(parent) = root.parent().filter(|p| !p.as_os_str().is_empty()) {
            sync_dir(parent)?;
        }
        Ok(LocalFsStore { root })
    }

    /// The root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, key: &Key) -> PathBuf {
        let mut p = self.root.clone();
        p.extend(key.components());
        p
    }

    /// Creates the parent directories of `key` durably; `Conflict` if a file is in the way.
    fn ensure_parent(&self, key: &Key) -> Result<PathBuf> {
        let mut dir = self.root.clone();
        for component in key.ancestors().map(|a| a.rsplit('/').next().unwrap_or(a)) {
            let parent = dir.clone();
            dir.push(component);
            match fs::create_dir(&dir) {
                Ok(()) => sync_dir(&parent)?,
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    if !dir.is_dir() {
                        return Err(conflict(key));
                    }
                }
                Err(e) if e.kind() == ErrorKind::NotADirectory => return Err(conflict(key)),
                Err(e) => return Err(io_err("create dir", &dir, e)),
            }
        }
        Ok(dir)
    }

    /// Writes `data` to a fresh, fsynced temp file in `dir`.
    fn write_temp(dir: &Path, data: &[u8]) -> io::Result<PathBuf> {
        let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = dir.join(format!("{TEMP_PREFIX}{}-{n}", std::process::id()));
        let result = (|| {
            let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
            f.write_all(data)?;
            f.sync_all()
        })();
        match result {
            Ok(()) => Ok(tmp),
            Err(e) => {
                let _ = fs::remove_file(&tmp);
                Err(e)
            }
        }
    }

    /// Shared skeleton of `put` / `put_if_absent`: `install` moves the temp file into
    /// place and returns `Ok(Some(outcome))`, or `Ok(None)` when the target turned out to
    /// be an (empty, prunable) directory and the attempt should be retried.
    fn install(
        &self,
        key: &Key,
        data: &[u8],
        install: impl Fn(&Path, &Path) -> io::Result<()>,
    ) -> Result<PutOutcome> {
        let target = self.path(key);
        for _ in 0..DIR_RACE_RETRIES {
            let dir = self.ensure_parent(key)?;
            let tmp = match Self::write_temp(&dir, data) {
                Ok(tmp) => tmp,
                // A concurrent delete pruned the directory between ensure_parent and here.
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(io_err("write temp in", &dir, e)),
            };
            let installed = install(&tmp, &target);
            let _ = fs::remove_file(&tmp); // no-op after a successful rename
            match installed {
                Ok(()) => {
                    sync_dir(&dir)?;
                    return Ok(PutOutcome::Created);
                }
                Err(e) => {
                    if target.is_dir() {
                        // Leftover empty directories from deleted keys are not objects.
                        if prune_empty_dirs(&target) {
                            continue;
                        }
                        return Err(conflict(key));
                    }
                    if e.kind() == ErrorKind::AlreadyExists {
                        return Ok(PutOutcome::AlreadyExists);
                    }
                    if e.kind() == ErrorKind::NotFound {
                        continue; // directory pruned concurrently
                    }
                    return Err(io_err("install", &target, e));
                }
            }
        }
        Err(StoreError::new(
            StoreErrorKind::Io,
            format!("put {key}: gave up after concurrent directory changes"),
        ))
    }

    /// Removes leftover `.muninn-tmp-*` files (from crashed puts). Returns how many were
    /// removed. Only the single writer should call this, at startup.
    pub fn remove_stale_temp_files(&self) -> Result<usize> {
        fn walk(dir: &Path, removed: &mut usize) -> Result<()> {
            let entries = match fs::read_dir(dir) {
                Ok(e) => e,
                Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(io_err("read dir", dir, e)),
            };
            for entry in entries {
                let entry = entry.map_err(|e| io_err("read dir", dir, e))?;
                let path = entry.path();
                let ty = entry.file_type().map_err(|e| io_err("stat", &path, e))?;
                if ty.is_dir() {
                    walk(&path, removed)?;
                } else if entry
                    .file_name()
                    .to_str()
                    .is_some_and(|n| n.starts_with(TEMP_PREFIX))
                {
                    match fs::remove_file(&path) {
                        Ok(()) => *removed += 1,
                        Err(e) if e.kind() == ErrorKind::NotFound => {}
                        Err(e) => return Err(io_err("remove temp", &path, e)),
                    }
                }
            }
            Ok(())
        }
        let mut removed = 0;
        walk(&self.root, &mut removed)?;
        Ok(removed)
    }
}

impl Store for LocalFsStore {
    fn get(&self, key: &Key) -> Result<Vec<u8>> {
        let path = self.path(key);
        fs::read(&path).map_err(|e| {
            if is_absent(&e) {
                StoreError::not_found(key)
            } else {
                io_err("read", &path, e)
            }
        })
    }

    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>> {
        let path = self.path(key);
        let absent = |e: io::Error| {
            if is_absent(&e) {
                StoreError::not_found(key)
            } else {
                io_err("read", &path, e)
            }
        };
        let mut f = File::open(&path).map_err(absent)?;
        let meta = f.metadata().map_err(absent)?;
        if !meta.is_file() {
            return Err(StoreError::not_found(key));
        }
        check_range(key, &range, meta.len())?;
        let mut buf = vec![0u8; (range.end - range.start) as usize];
        f.seek(SeekFrom::Start(range.start)).map_err(absent)?;
        f.read_exact(&mut buf).map_err(absent)?;
        Ok(buf)
    }

    fn head(&self, key: &Key) -> Result<ObjectMeta> {
        let path = self.path(key);
        match fs::metadata(&path) {
            Ok(m) if m.is_file() => Ok(ObjectMeta { size: m.len() }),
            Ok(_) => Err(StoreError::not_found(key)),
            Err(e) if is_absent(&e) => Err(StoreError::not_found(key)),
            Err(e) => Err(io_err("stat", &path, e)),
        }
    }

    fn put(&self, key: &Key, data: &[u8]) -> Result<()> {
        self.install(key, data, |tmp, target| fs::rename(tmp, target))
            .map(|_| ())
    }

    fn put_if_absent(&self, key: &Key, data: &[u8]) -> Result<PutOutcome> {
        self.install(key, data, |tmp, target| fs::hard_link(tmp, target))
    }

    fn list(&self, prefix: &str) -> Result<Vec<Key>> {
        validate_prefix(prefix)?;
        // Start the walk at the deepest directory fully named by the prefix.
        let dir_part = prefix.rfind('/').map_or("", |i| &prefix[..i]);
        let mut start = self.root.clone();
        if !dir_part.is_empty() {
            start.extend(dir_part.split('/'));
        }
        let mut keys = Vec::new();
        fn walk(dir: &Path, rel: &str, out: &mut Vec<String>) -> Result<()> {
            let entries = match fs::read_dir(dir) {
                Ok(e) => e,
                Err(e) if is_absent(&e) => return Ok(()),
                Err(e) => return Err(io_err("read dir", dir, e)),
            };
            for entry in entries {
                let entry = entry.map_err(|e| io_err("read dir", dir, e))?;
                let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                    continue; // not UTF-8: cannot be a key
                };
                if name.starts_with('.') {
                    continue; // temp files (and anything else that cannot be a key)
                }
                let key = if rel.is_empty() {
                    name
                } else {
                    format!("{rel}/{name}")
                };
                let ty = entry
                    .file_type()
                    .map_err(|e| io_err("stat", &entry.path(), e))?;
                if ty.is_dir() {
                    walk(&entry.path(), &key, out)?;
                } else if ty.is_file() && validate_key(&key).is_ok() {
                    out.push(key);
                }
            }
            Ok(())
        }
        walk(&start, dir_part, &mut keys)?;
        keys.retain(|k| k.starts_with(prefix));
        keys.sort();
        Ok(keys.into_iter().map(Key).collect())
    }

    fn delete(&self, key: &Key) -> Result<()> {
        let path = self.path(key);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if is_absent(&e) => return Ok(()),
            Err(e) => return Err(io_err("remove", &path, e)),
        }
        let parent = path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.root.clone());
        sync_dir(&parent)?;
        // Prune now-empty parent directories (best effort; never the root) so that the
        // freed path can hold an object again, as in the other backends.
        let mut dir = parent;
        while dir != self.root && fs::remove_dir(&dir).is_ok() {
            match dir.parent() {
                Some(p) => dir = p.to_path_buf(),
                None => break,
            }
        }
        Ok(())
    }
}
