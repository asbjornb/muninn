//! In-memory backend.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, PoisonError, RwLock};

use crate::{
    Key, ObjectMeta, PutOutcome, Result, Store, StoreError, StoreErrorKind, check_range,
    validate_prefix,
};

/// A thread-safe in-memory [`Store`]. Every operation is atomic under one lock.
#[derive(Debug, Default)]
pub struct MemoryStore {
    objects: RwLock<BTreeMap<String, Arc<[u8]>>>,
}

impl MemoryStore {
    /// An empty store.
    pub fn new() -> MemoryStore {
        MemoryStore::default()
    }

    /// Number of objects (for tests).
    pub fn len(&self) -> usize {
        self.read().len()
    }

    /// Is the store empty?
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, Arc<[u8]>>> {
        // Mutations are single map operations, so a poisoned lock holds a consistent map.
        self.objects.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<String, Arc<[u8]>>> {
        self.objects.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn object(&self, key: &Key) -> Result<Arc<[u8]>> {
        self.read()
            .get(key.as_str())
            .cloned()
            .ok_or_else(|| StoreError::not_found(key))
    }
}

/// Mirrors the filesystem constraint: an object key may not be a path-prefix of another.
fn check_conflict(map: &BTreeMap<String, Arc<[u8]>>, key: &Key) -> Result<()> {
    let conflict = || {
        Err(StoreError::new(
            StoreErrorKind::Conflict,
            format!("key {key} conflicts with an existing key (one is a path-prefix of the other)"),
        ))
    };
    if key.ancestors().any(|a| map.contains_key(a)) {
        return conflict();
    }
    let dir = format!("{key}/");
    if map
        .range(dir.clone()..)
        .next()
        .is_some_and(|(k, _)| k.starts_with(&dir))
    {
        return conflict();
    }
    Ok(())
}

impl Store for MemoryStore {
    fn get(&self, key: &Key) -> Result<Vec<u8>> {
        Ok(self.object(key)?.to_vec())
    }

    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>> {
        let obj = self.object(key)?;
        check_range(key, &range, obj.len() as u64)?;
        Ok(obj[range.start as usize..range.end as usize].to_vec())
    }

    fn head(&self, key: &Key) -> Result<ObjectMeta> {
        Ok(ObjectMeta {
            size: self.object(key)?.len() as u64,
        })
    }

    fn put(&self, key: &Key, data: &[u8]) -> Result<()> {
        let mut map = self.write();
        check_conflict(&map, key)?;
        map.insert(key.as_str().to_owned(), Arc::from(data));
        Ok(())
    }

    fn put_if_absent(&self, key: &Key, data: &[u8]) -> Result<PutOutcome> {
        let mut map = self.write();
        if map.contains_key(key.as_str()) {
            return Ok(PutOutcome::AlreadyExists);
        }
        check_conflict(&map, key)?;
        map.insert(key.as_str().to_owned(), Arc::from(data));
        Ok(PutOutcome::Created)
    }

    fn list(&self, prefix: &str) -> Result<Vec<Key>> {
        validate_prefix(prefix)?;
        let map = self.read();
        Ok(map
            .range(prefix.to_owned()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| Key(k.clone()))
            .collect())
    }

    fn delete(&self, key: &Key) -> Result<()> {
        self.write().remove(key.as_str());
        Ok(())
    }
}
