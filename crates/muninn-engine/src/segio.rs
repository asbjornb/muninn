//! Segment I/O: footers (one ranged tail read, cached), engine blocks and value chunks
//! (ranged reads of only the chunks a query needs).

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use muninn_core::types::TableDef;
use muninn_core::{Error, ErrorKind, Result, Value};
use muninn_format::{ENGINE_COLUMNS, EngineBlock, Footer, Tail, parse_tail};
use muninn_store::{Key, Store, StoreError, StoreErrorKind};

use crate::EngineOptions;
use crate::state::SegmentRef;

pub(crate) fn store_err(what: &str, e: StoreError) -> Error {
    Error::new(ErrorKind::Io, format!("{what}: {e}"))
}

/// Message prefix of the error for a live segment that is not in the store.
pub(crate) const MISSING_SEGMENT: &str = "live segment missing from the store";

/// Is `e` the error of a missing live segment (the state may be stale)?
pub(crate) fn is_missing_segment(e: &Error) -> bool {
    e.kind() == ErrorKind::Io && e.message().starts_with(MISSING_SEGMENT)
}

fn format_err(key: &Key, e: muninn_format::Error) -> Error {
    Error::new(ErrorKind::Io, format!("corrupt segment {key}: {e}"))
}

/// What every engine handle and snapshot shares: the store, options and caches.
pub(crate) struct Shared<S> {
    pub store: S,
    pub opts: EngineOptions,
    /// Set when the engine's state may be behind the log: a commit failed ambiguously or
    /// lost a race, or a live segment was missing (another writer compacted and collected
    /// it). The next operation reloads from the log first.
    pub stale: AtomicBool,
    footers: Mutex<HashMap<Key, Arc<Footer>>>,
}

/// Every value column of a segment must be a column of its table, with the same type
/// (columns are only appended, so ids are positions).
pub(crate) fn check_columns(key: &Key, f: &Footer, table: &TableDef) -> Result<()> {
    for c in &f.columns {
        match table.columns.get(c.id as usize) {
            Some(def) if def.ty == c.ty => {}
            _ => {
                return Err(Error::new(
                    ErrorKind::Io,
                    format!(
                        "corrupt segment {key}: column {} ({}) is not a column of table {:?}",
                        c.id, c.ty, table.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

impl<S: Store> Shared<S> {
    pub fn new(store: S, opts: EngineOptions) -> Shared<S> {
        Shared {
            store,
            opts,
            stale: AtomicBool::new(false),
            footers: Mutex::new(HashMap::new()),
        }
    }

    fn get_range(&self, key: &Key, r: Range<u64>) -> Result<Vec<u8>> {
        self.store.get_range(key, r).map_err(|e| {
            if e.kind() == StoreErrorKind::NotFound {
                self.stale.store(true, Ordering::SeqCst);
                Error::new(ErrorKind::Io, format!("{MISSING_SEGMENT}: {key}"))
            } else {
                store_err(&format!("reading {key}"), e)
            }
        })
    }

    /// Caches a footer we already have (e.g. of a segment we just wrote).
    pub fn cache_footer(&self, key: &Key, f: Arc<Footer>) {
        let mut c = self.footers.lock().unwrap_or_else(PoisonError::into_inner);
        if c.len() >= self.opts.footer_cache_entries {
            c.clear();
        }
        c.insert(key.clone(), f);
    }

    /// The footer of `seg`: from the cache, or by reading the file's tail (one ranged
    /// read, a second one if the footer is larger than the first read).
    pub fn footer(&self, seg: &SegmentRef, table: &TableDef) -> Result<Arc<Footer>> {
        let cached = self
            .footers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&seg.key)
            .cloned();
        let footer = match cached {
            Some(f) => f,
            None => self.read_footer(seg)?,
        };
        check_columns(&seg.key, &footer, table)?;
        Ok(footer)
    }

    fn read_footer(&self, seg: &SegmentRef) -> Result<Arc<Footer>> {
        let size = seg.size;
        let first = self.opts.tail_read.min(size);
        let tail = self.get_range(&seg.key, size - first..size)?;
        let footer = match parse_tail(&tail, size).map_err(|e| format_err(&seg.key, e))? {
            Tail::Footer(f) => f,
            Tail::NeedBytes(n) => {
                let tail = self.get_range(&seg.key, size - n.min(size)..size)?;
                match parse_tail(&tail, size).map_err(|e| format_err(&seg.key, e))? {
                    Tail::Footer(f) => f,
                    Tail::NeedBytes(_) => {
                        return Err(format_err(
                            &seg.key,
                            muninn_format::Error::corrupt_message("footer keeps growing"),
                        ));
                    }
                }
            }
        };
        // The footer must agree with what the log says about the segment.
        let st = &footer.stats;
        if footer.row_count != seg.rows
            || footer.partition != seg.partition
            || footer.table != seg.table
            || (st.seq_min, st.seq_max) != seg.seq
            || st.value_time != seg.value_time
            || st.received_time != seg.received_time
            || st.published_time != seg.published_time
            || st.system_time != seg.system_time
            || footer.series.first().map(|s| s.id) != Some(seg.series.0)
            || footer.series.last().map(|s| s.id) != Some(seg.series.1)
        {
            return Err(Error::new(
                ErrorKind::Io,
                format!("corrupt segment {}: footer disagrees with the log", seg.key),
            ));
        }
        let footer: Arc<Footer> = Arc::from(footer);
        self.cache_footer(&seg.key, Arc::clone(&footer));
        Ok(footer)
    }

    /// Reads the chunks at `ranges` of one file, coalescing ranges that are adjacent.
    fn read_chunks(&self, key: &Key, ranges: &[Range<u64>]) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::with_capacity(ranges.len());
        let mut i = 0;
        while i < ranges.len() {
            let mut j = i + 1;
            while j < ranges.len() && ranges[j].start == ranges[j - 1].end {
                j += 1;
            }
            let span = ranges[i].start..ranges[j - 1].end;
            let bytes = self.get_range(key, span.clone())?;
            if bytes.len() as u64 != span.end - span.start {
                return Err(Error::new(ErrorKind::Io, format!("short read of {key}")));
            }
            for r in &ranges[i..j] {
                let a = (r.start - span.start) as usize;
                let b = (r.end - span.start) as usize;
                out.push(bytes[a..b].to_vec());
            }
            i = j;
        }
        Ok(out)
    }

    /// Decodes the engine columns of block `b`.
    pub fn engine_block(&self, seg: &SegmentRef, f: &Footer, b: usize) -> Result<EngineBlock> {
        let ranges = f.engine_chunk_ranges(b);
        let chunks = self.read_chunks(&seg.key, &ranges)?;
        let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
        f.decode_engine_block(b, &refs)
            .map_err(|e| format_err(&seg.key, e))
    }

    /// Decodes table column `id` of block `b`; `None` when the segment predates it.
    pub fn value_column(
        &self,
        seg: &SegmentRef,
        f: &Footer,
        b: usize,
        id: usize,
    ) -> Result<Option<Vec<Option<Value>>>> {
        let Some(p) = f.column_position(id as u32) else {
            return Ok(None);
        };
        let r = f.blocks[b].chunks[ENGINE_COLUMNS + p].range();
        let bytes = self.read_chunks(&seg.key, &[r])?;
        f.decode_value_chunk(b, p, &bytes[0])
            .map(Some)
            .map_err(|e| format_err(&seg.key, e))
    }

    /// Decodes the value columns of block `b` for table columns `0..ncols`: entry `i` is
    /// `None` when the segment predates column `i` (every row reads null).
    pub fn value_columns(
        &self,
        seg: &SegmentRef,
        f: &Footer,
        b: usize,
        ncols: usize,
    ) -> Result<Vec<Option<Vec<Option<Value>>>>> {
        let present: Vec<(usize, usize)> = (0..ncols)
            .filter_map(|id| f.column_position(id as u32).map(|p| (id, p)))
            .collect();
        let ranges: Vec<Range<u64>> = present
            .iter()
            .map(|&(_, p)| f.blocks[b].chunks[ENGINE_COLUMNS + p].range())
            .collect();
        let chunks = self.read_chunks(&seg.key, &ranges)?;
        let mut out: Vec<Option<Vec<Option<Value>>>> = vec![None; ncols];
        for ((id, p), bytes) in present.into_iter().zip(chunks) {
            out[id] = Some(
                f.decode_value_chunk(b, p, &bytes)
                    .map_err(|e| format_err(&seg.key, e))?,
            );
        }
        Ok(out)
    }
}
