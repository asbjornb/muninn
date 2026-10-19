//! Segment files: writer, footer, tail parsing and chunk decoding (FORMAT.md §3–§6).

use std::ops::Range;

use muninn_core::types::{Clock, ColumnType, SeriesId, Version};
use muninn_core::{Partitioning, Timestamp, Value};

use crate::codec::{
    Encoding, Reader, decode_bitmap, decode_ints, encode_bitmap, encode_ints, put_ivarint,
    put_string, put_uvarint,
};
use crate::partition::Partition;
use crate::{Error, Result};

/// First and last 8 bytes of every segment file.
pub const MAGIC: [u8; 8] = *b"MUNINNSG";
/// The format version written in the header (and the footer format byte).
pub const FORMAT_VERSION: u16 = 1;
/// Header size: magic, `u16` version, `u16` flags, `u32` reserved.
pub const HEADER_LEN: u64 = 16;
/// Tail size: `u32` footer length, `u32` footer CRC-32C, magic.
pub const TAIL_LEN: u64 = 16;
/// Upper bound on rows per block (readers reject larger blocks).
pub const MAX_BLOCK_ROWS: usize = 1 << 20;
/// Upper bound on the decoded size of a string chunk (readers reject larger chunks). Other
/// chunk types have exact per-row bounds, see [`max_raw_length`].
pub const MAX_STRING_CHUNK_RAW: u64 = 1 << 28;
/// Number of engine columns; value column `j` is chunk `ENGINE_COLUMNS + j` of a block.
pub const ENGINE_COLUMNS: usize = 7;

// Engine column positions within a block's chunk list.
const C_SERIES: usize = 0;
const C_VALUE_TIME: usize = 1;
const C_PUBLISHED: usize = 2;
const C_RECEIVED: usize = 3;
const C_SYSTEM: usize = 4;
const C_SEQ: usize = 5;
const C_DELETED: usize = 6;

/// A value column stored in a segment: the table column's stable id (its position in the
/// table's column list — columns are only ever appended) and type.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ValueColumn {
    /// Stable column id.
    pub id: u32,
    /// Column type.
    pub ty: ColumnType,
}

fn type_code(ty: ColumnType) -> u8 {
    match ty {
        ColumnType::F64 => 0,
        ColumnType::I64 => 1,
        ColumnType::Bool => 2,
        ColumnType::String => 3,
        ColumnType::Timestamp => 4,
    }
}

fn type_from_code(c: u8) -> Option<ColumnType> {
    Some(match c {
        0 => ColumnType::F64,
        1 => ColumnType::I64,
        2 => ColumnType::Bool,
        3 => ColumnType::String,
        4 => ColumnType::Timestamp,
        _ => return None,
    })
}

fn granularity_code(g: Partitioning) -> u8 {
    match g {
        Partitioning::Day => 0,
        Partitioning::Month => 1,
        Partitioning::Year => 2,
    }
}

fn value_encoding(ty: ColumnType) -> Encoding {
    match ty {
        ColumnType::F64 => Encoding::PlainF64,
        ColumnType::I64 => Encoding::Zigzag,
        ColumnType::Bool => Encoding::Bitmap,
        ColumnType::String => Encoding::LengthPrefixed,
        ColumnType::Timestamp => Encoding::Delta,
    }
}

/// Is `enc` a legal encoding for chunk `pos` (`ty` for value columns)?
fn encoding_allowed(pos: usize, ty: Option<ColumnType>, enc: Encoding) -> bool {
    match (pos, ty) {
        (C_DELETED, None) => enc == Encoding::Bitmap,
        (_, None) => enc.is_integer(),
        (_, Some(ColumnType::F64)) => enc == Encoding::PlainF64,
        (_, Some(ColumnType::I64 | ColumnType::Timestamp)) => enc.is_integer(),
        (_, Some(ColumnType::Bool)) => enc == Encoding::Bitmap,
        (_, Some(ColumnType::String)) => enc == Encoding::LengthPrefixed,
    }
}

/// The largest raw (decoded) size a valid chunk at position `pos` (value type `ty`) of
/// an `n`-row block can have (FORMAT.md §5.5). Bounds decompression output, so a corrupt or
/// hostile directory cannot make a reader inflate more than the encoding allows.
pub fn max_raw_length(pos: usize, ty: Option<ColumnType>, n: u64) -> u64 {
    let bitmap = n.div_ceil(8);
    // Integer encodings need at most 10 bytes per value (run-length: 20 per run).
    let ints = 20 * n;
    let nullable = |values: u64| 1 + bitmap + values;
    match (pos, ty) {
        (C_DELETED, None) => bitmap,
        (C_PUBLISHED, None) => nullable(ints),
        (_, None) => ints,
        (_, Some(ColumnType::F64)) => nullable(8 * n),
        (_, Some(ColumnType::I64 | ColumnType::Timestamp)) => nullable(ints),
        (_, Some(ColumnType::Bool)) => nullable(bitmap),
        (_, Some(ColumnType::String)) => MAX_STRING_CHUNK_RAW,
    }
}

/// Chunk compression.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Compression {
    /// `0`: stored as is.
    None,
    /// `1`: one Zstandard frame.
    Zstd,
}

thread_local! {
    /// One zstd decompression context per thread, reused for every chunk (creating one
    /// per chunk dominated the decoding of small chunks).
    static DECOMPRESSOR: std::cell::RefCell<Option<zstd::bulk::Decompressor<'static>>> =
        const { std::cell::RefCell::new(None) };
}

/// Directory entry of one column chunk.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ChunkMeta {
    /// Absolute file offset of the stored bytes.
    pub offset: u64,
    /// Stored (possibly compressed) length.
    pub length: u64,
    /// Length after decompression.
    pub raw_length: u64,
    /// Encoding of the (present) values.
    pub encoding: Encoding,
    /// Compression of the stored bytes.
    pub compression: Compression,
    /// CRC-32C (Castagnoli) of the stored bytes.
    pub crc32c: u32,
}

impl ChunkMeta {
    /// The byte range of the stored chunk in the file.
    pub fn range(&self) -> Range<u64> {
        self.offset..self.offset + self.length
    }

    /// Verifies the checksum of `stored` and decompresses it.
    fn open<'a>(&self, stored: &'a [u8]) -> Result<std::borrow::Cow<'a, [u8]>> {
        if stored.len() as u64 != self.length {
            return Err(Error::corrupt(format!(
                "chunk at {} has {} bytes, directory says {}",
                self.offset,
                stored.len(),
                self.length
            )));
        }
        let crc = crc32c::crc32c(stored);
        if crc != self.crc32c {
            return Err(Error::corrupt(format!(
                "checksum mismatch in chunk at offset {} (stored {:08x}, computed {crc:08x})",
                self.offset, self.crc32c
            )));
        }
        match self.compression {
            Compression::None => Ok(std::borrow::Cow::Borrowed(stored)),
            Compression::Zstd => {
                // Decompress into exactly `raw_length` bytes of capacity (a frame that
                // inflates to more is an error). A decoded footer bounds raw_length by
                // what the chunk's encoding can need (`max_raw_length`), so this
                // allocation is bounded; the cap guards hand-built directories.
                if self.raw_length > MAX_STRING_CHUNK_RAW {
                    return Err(Error::corrupt(format!(
                        "chunk at {} claims {} raw bytes",
                        self.offset, self.raw_length
                    )));
                }
                let mut raw = Vec::with_capacity(self.raw_length as usize);
                DECOMPRESSOR
                    .with(|d| {
                        let mut d = d.borrow_mut();
                        if d.is_none() {
                            *d = Some(zstd::bulk::Decompressor::new()?);
                        }
                        d.as_mut()
                            .expect("just set")
                            .decompress_to_buffer(stored, &mut raw)
                    })
                    .map_err(|e| Error::corrupt(format!("zstd chunk at {}: {e}", self.offset)))?;
                if raw.len() as u64 != self.raw_length {
                    return Err(Error::corrupt(format!(
                        "chunk at {} decompressed to {} bytes, directory says {}",
                        self.offset,
                        raw.len(),
                        self.raw_length
                    )));
                }
                Ok(std::borrow::Cow::Owned(raw))
            }
        }
    }
}

/// Inclusive bounds of a timestamp column.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct TimeBounds {
    /// Smallest value.
    pub min: Timestamp,
    /// Largest value.
    pub max: Timestamp,
}

impl TimeBounds {
    fn of(mut it: impl Iterator<Item = Timestamp>) -> Option<TimeBounds> {
        let first = it.next()?;
        Some(it.fold(
            TimeBounds {
                min: first,
                max: first,
            },
            |b, t| TimeBounds {
                min: b.min.min(t),
                max: b.max.max(t),
            },
        ))
    }

    /// Does `t` lie within the bounds?
    pub fn contains(&self, t: Timestamp) -> bool {
        self.min <= t && t <= self.max
    }

    fn contains_bounds(&self, inner: &TimeBounds) -> bool {
        self.min <= inner.min && inner.max <= self.max
    }

    fn put(&self, out: &mut Vec<u8>) {
        put_ivarint(out, self.min.as_micros());
        put_ivarint(out, self.max.as_micros());
    }

    fn read(r: &mut Reader<'_>) -> Result<TimeBounds> {
        let min = read_ts(r)?;
        let max = read_ts(r)?;
        if min > max {
            return Err(r.err("time bounds min > max"));
        }
        Ok(TimeBounds { min, max })
    }
}

fn read_ts(r: &mut Reader<'_>) -> Result<Timestamp> {
    let v = r.ivarint()?;
    Timestamp::from_micros(v).ok_or_else(|| r.err(format!("timestamp {v} out of range")))
}

fn ts_checked(v: i64, what: &str) -> Result<Timestamp> {
    Timestamp::from_micros(v)
        .ok_or_else(|| Error::corrupt(format!("{what}: timestamp {v}us out of range")))
}

/// Segment-level statistics (FORMAT.md §5.3).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct SegmentStats {
    /// `value_time` bounds.
    pub value_time: TimeBounds,
    /// `received_time` bounds.
    pub received_time: TimeBounds,
    /// Bounds of the *effective* published time `coalesce(published_time,
    /// received_time)` — `t_published` of SPEC §5.
    pub published_time: TimeBounds,
    /// `system_time` bounds.
    pub system_time: TimeBounds,
    /// Rows with a null `published_time`.
    pub published_null_count: u64,
    /// Smallest `seq`.
    pub seq_min: u64,
    /// Largest `seq`.
    pub seq_max: u64,
    /// Tombstone rows.
    pub deleted_count: u64,
}

impl SegmentStats {
    /// Bounds of `t_clock` (SPEC §5) over the segment's rows.
    pub fn t_clock(&self, clock: Clock) -> TimeBounds {
        match clock {
            Clock::Received => self.received_time,
            Clock::Published => self.published_time,
            Clock::System => self.system_time,
        }
    }
}

/// A series' contiguous row range in the segment.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct SeriesEntry {
    /// Series id.
    pub id: SeriesId,
    /// First row (inclusive).
    pub row_start: u64,
    /// Last row (exclusive).
    pub row_end: u64,
}

/// Statistics of one value column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnStats {
    /// Null rows.
    pub null_count: u64,
    /// Min and max of non-null values, for `f64` (ignoring NaN), `i64` and `timestamp`
    /// columns; `None` if there are none (or the type has no order here).
    pub min_max: Option<(Value, Value)>,
}

/// One row block: a contiguous row range with one chunk per column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// First row (inclusive) of the block.
    pub row_start: u64,
    /// Rows in the block (1..=[`MAX_BLOCK_ROWS`]).
    pub row_count: u32,
    /// `value_time` bounds within the block.
    pub value_time: TimeBounds,
    /// `received_time` bounds.
    pub received_time: TimeBounds,
    /// Effective published time bounds.
    pub published_time: TimeBounds,
    /// `system_time` bounds.
    pub system_time: TimeBounds,
    /// Chunks: the [`ENGINE_COLUMNS`] engine columns, then one per value column.
    pub chunks: Vec<ChunkMeta>,
}

impl BlockMeta {
    /// The block's rows.
    pub fn rows(&self) -> Range<u64> {
        self.row_start..self.row_start + u64::from(self.row_count)
    }

    /// Bounds of `t_clock` within the block.
    pub fn t_clock(&self, clock: Clock) -> TimeBounds {
        match clock {
            Clock::Received => self.received_time,
            Clock::Published => self.published_time,
            Clock::System => self.system_time,
        }
    }
}

/// A decoded segment footer (FORMAT.md §5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Footer {
    /// The table name (informational; the engine also knows it from the path).
    pub table: String,
    /// The partition.
    pub partition: Partition,
    /// Total rows (≥ 1).
    pub row_count: u64,
    /// Value columns present, by ascending id. Table columns not listed read as null.
    pub columns: Vec<ValueColumn>,
    /// Segment statistics.
    pub stats: SegmentStats,
    /// Series directory, by ascending id; the row ranges tile `0..row_count`.
    pub series: Vec<SeriesEntry>,
    /// Per value column statistics (parallel to `columns`).
    pub column_stats: Vec<ColumnStats>,
    /// Row blocks; their row ranges tile `0..row_count`.
    pub blocks: Vec<BlockMeta>,
    /// File offset where the footer starts.
    pub footer_offset: u64,
    /// File size.
    pub file_size: u64,
}

/// The engine columns of one block, decoded.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct EngineBlock {
    /// Series ids.
    pub series: Vec<SeriesId>,
    /// Value times.
    pub value_time: Vec<Timestamp>,
    /// Published times (stored; null = unknown).
    pub published_time: Vec<Option<Timestamp>>,
    /// Received times.
    pub received_time: Vec<Timestamp>,
    /// System times.
    pub system_time: Vec<Timestamp>,
    /// Sequence numbers.
    pub seq: Vec<u64>,
    /// Tombstone flags.
    pub deleted: Vec<bool>,
}

impl EngineBlock {
    /// Number of rows.
    pub fn len(&self) -> usize {
        self.series.len()
    }

    /// Is the block empty? (Never true for a decoded block.)
    pub fn is_empty(&self) -> bool {
        self.series.is_empty()
    }

    /// `t_clock` of row `i`.
    pub fn t_clock(&self, clock: Clock, i: usize) -> Timestamp {
        muninn_core::types::t_clock(
            clock,
            self.published_time[i],
            self.received_time[i],
            self.system_time[i],
        )
    }
}

impl Footer {
    /// Position of value column `id` in [`Footer::columns`], if the segment has it.
    pub fn column_position(&self, id: u32) -> Option<usize> {
        self.columns.binary_search_by_key(&id, |c| c.id).ok()
    }

    /// Rows of series `id`, if present.
    pub fn series_rows(&self, id: SeriesId) -> Option<Range<u64>> {
        self.series
            .binary_search_by_key(&id, |s| s.id)
            .ok()
            .map(|i| self.series[i].row_start..self.series[i].row_end)
    }

    /// Indices of the blocks overlapping `rows`.
    pub fn blocks_for_rows(&self, rows: Range<u64>) -> Range<usize> {
        if rows.start >= rows.end {
            return 0..0;
        }
        let first = self.blocks.partition_point(|b| b.rows().end <= rows.start);
        let last = self.blocks.partition_point(|b| b.row_start < rows.end);
        first..last.max(first)
    }

    /// The byte ranges of the engine chunks of block `b` (in chunk order).
    pub fn engine_chunk_ranges(&self, b: usize) -> Vec<Range<u64>> {
        self.blocks[b].chunks[..ENGINE_COLUMNS]
            .iter()
            .map(ChunkMeta::range)
            .collect()
    }

    /// Decodes the engine columns of block `b` from their stored bytes (`chunks[i]` =
    /// the file bytes of engine chunk `i`), verifying checksums, the series directory,
    /// the sort order and the block statistics.
    pub fn decode_engine_block(&self, b: usize, chunks: &[&[u8]]) -> Result<EngineBlock> {
        let block = self
            .blocks
            .get(b)
            .ok_or_else(|| Error::invalid(format!("no block {b}")))?;
        if chunks.len() != ENGINE_COLUMNS {
            return Err(Error::invalid("decode_engine_block needs 7 chunks"));
        }
        let n = block.row_count as usize;
        let open = |i: usize| block.chunks[i].open(chunks[i]);
        let ints = |i: usize, what: &'static str| -> Result<Vec<i64>> {
            let raw = open(i)?;
            let mut r = Reader::new(&raw, what);
            let v = decode_ints(block.chunks[i].encoding, n, &mut r)?;
            r.finish()?;
            Ok(v)
        };

        let series_raw = ints(C_SERIES, "series chunk")?;
        let value_time = ints(C_VALUE_TIME, "value_time chunk")?;
        let received_time = ints(C_RECEIVED, "received_time chunk")?;
        let system_time = ints(C_SYSTEM, "system_time chunk")?;
        let seq = ints(C_SEQ, "seq chunk")?;
        let (present, published) = {
            let raw = open(C_PUBLISHED)?;
            let mut r = Reader::new(&raw, "published_time chunk");
            let present = read_validity(n, &mut r)?;
            let k = present.iter().filter(|&&p| p).count();
            let vals = decode_ints(block.chunks[C_PUBLISHED].encoding, k, &mut r)?;
            r.finish()?;
            (present, vals)
        };
        let deleted = {
            let raw = open(C_DELETED)?;
            let mut r = Reader::new(&raw, "deleted chunk");
            let v = decode_bitmap(n, &mut r)?;
            r.finish()?;
            v
        };

        // Series ids must agree with the directory for this block's rows: one tight
        // comparison per directory entry overlapping the block (the entries tile the
        // rows, checked when the footer was decoded).
        let mut series = Vec::with_capacity(n);
        let end = block.row_start + n as u64;
        let mut e = self
            .series
            .partition_point(|s| s.row_end <= block.row_start);
        while (series.len() as u64) < n as u64 {
            let entry = self
                .series
                .get(e)
                .ok_or_else(|| Error::corrupt("row beyond the series directory"))?;
            let lo = (entry.row_start.max(block.row_start) - block.row_start) as usize;
            let hi = (entry.row_end.min(end) - block.row_start) as usize;
            if lo != series.len() || hi <= lo {
                return Err(Error::corrupt("series directory does not tile the block"));
            }
            let id = i64::from(entry.id.0);
            if let Some(off) = series_raw[lo..hi].iter().position(|&x| x != id) {
                return Err(Error::corrupt(format!(
                    "row {} has series {}, directory says {}",
                    block.row_start + (lo + off) as u64,
                    series_raw[lo + off],
                    entry.id.0
                )));
            }
            series.resize(hi, entry.id);
            e += 1;
        }

        // Every row within its block statistics (whose bounds are valid timestamps, so
        // this also range-checks every value) and in strict sort order (seq is unique).
        let b_vt = (
            block.value_time.min.as_micros(),
            block.value_time.max.as_micros(),
        );
        let b_rt = (
            block.received_time.min.as_micros(),
            block.received_time.max.as_micros(),
        );
        let b_pt = (
            block.published_time.min.as_micros(),
            block.published_time.max.as_micros(),
        );
        let b_st = (
            block.system_time.min.as_micros(),
            block.system_time.max.as_micros(),
        );
        let inside = |b: (i64, i64), x: i64| b.0 <= x && x <= b.1;
        let mut published_time = Vec::with_capacity(n);
        let mut pv = published.iter();
        let mut prev: Option<(SeriesId, i64, i64, i64, u64)> = None;
        for i in 0..n {
            let pt = if present[i] {
                Some(*pv.next().expect("k values"))
            } else {
                None
            };
            let (vt, rt, st, sq) = (
                value_time[i],
                received_time[i],
                system_time[i],
                seq[i] as u64,
            );
            if !(inside(b_vt, vt)
                && inside(b_rt, rt)
                && inside(b_pt, pt.unwrap_or(rt))
                && inside(b_st, st)
                && (self.stats.seq_min..=self.stats.seq_max).contains(&sq))
            {
                return Err(Error::corrupt(format!(
                    "row {} lies outside its block statistics",
                    block.row_start + i as u64
                )));
            }
            let key = (series[i], vt, rt, st, sq);
            if prev.is_some_and(|p| p >= key) {
                return Err(Error::corrupt(format!(
                    "rows {} and {} are out of order",
                    block.row_start + i as u64 - 1,
                    block.row_start + i as u64
                )));
            }
            prev = Some(key);
            published_time.push(pt.map(|p| ts_checked(p, "published_time")).transpose()?);
        }
        let ts = |v: Vec<i64>, what: &str| {
            Timestamp::from_micros_vec(v)
                .map_err(|i| Error::corrupt(format!("{what}: row {i} out of range")))
        };
        Ok(EngineBlock {
            series,
            value_time: ts(value_time, "value_time")?,
            published_time,
            received_time: ts(received_time, "received_time")?,
            system_time: ts(system_time, "system_time")?,
            seq: seq.into_iter().map(|x| x as u64).collect(),
            deleted,
        })
    }

    /// Decodes value column at position `pos` (in [`Footer::columns`]) of block `b` from
    /// its stored bytes.
    pub fn decode_value_chunk(
        &self,
        b: usize,
        pos: usize,
        stored: &[u8],
    ) -> Result<Vec<Option<Value>>> {
        let block = self
            .blocks
            .get(b)
            .ok_or_else(|| Error::invalid(format!("no block {b}")))?;
        let col = self
            .columns
            .get(pos)
            .ok_or_else(|| Error::invalid(format!("no value column at position {pos}")))?;
        let meta = &block.chunks[ENGINE_COLUMNS + pos];
        let n = block.row_count as usize;
        let raw = meta.open(stored)?;
        let mut r = Reader::new(&raw, "value chunk");
        let present = read_validity(n, &mut r)?;
        let k = present.iter().filter(|&&p| p).count();
        let vals: Vec<Value> = match col.ty {
            ColumnType::F64 => {
                if k > r.remaining() / 8 {
                    return Err(r.err("f64 chunk truncated"));
                }
                let mut v = Vec::with_capacity(k);
                for _ in 0..k {
                    v.push(Value::F64(f64::from_bits(r.u64_le()?)));
                }
                v
            }
            ColumnType::I64 => decode_ints(meta.encoding, k, &mut r)?
                .into_iter()
                .map(Value::I64)
                .collect(),
            ColumnType::Timestamp => decode_ints(meta.encoding, k, &mut r)?
                .into_iter()
                .map(|x| ts_checked(x, "timestamp value").map(Value::Timestamp))
                .collect::<Result<_>>()?,
            ColumnType::Bool => decode_bitmap(k, &mut r)?
                .into_iter()
                .map(Value::Bool)
                .collect(),
            ColumnType::String => {
                if k > r.remaining() {
                    return Err(r.err("string chunk truncated"));
                }
                let mut v = Vec::with_capacity(k);
                for _ in 0..k {
                    v.push(Value::String(r.string()?.to_owned()));
                }
                v
            }
        };
        r.finish()?;
        let mut vals = vals.into_iter();
        Ok(present
            .into_iter()
            .map(|p| if p { vals.next() } else { None })
            .collect())
    }
}

/// The nullable-chunk prefix (FORMAT.md §4.1): mode byte, then a validity bitmap for mode
/// 2. Returns one presence flag per row.
fn read_validity(n: usize, r: &mut Reader<'_>) -> Result<Vec<bool>> {
    match r.u8()? {
        0 => Ok(vec![true; n]),
        1 => Ok(vec![false; n]),
        2 => {
            let bits = decode_bitmap(n, r)?;
            let k = bits.iter().filter(|&&b| b).count();
            if k == 0 || k == n {
                return Err(r.err("validity bitmap used for an all-null or no-null chunk"));
            }
            Ok(bits)
        }
        m => Err(r.err(format!("unknown null mode {m}"))),
    }
}

fn put_validity(present: &[bool], out: &mut Vec<u8>) {
    let k = present.iter().filter(|&&p| p).count();
    if k == present.len() {
        out.push(0);
    } else if k == 0 {
        out.push(1);
    } else {
        out.push(2);
        encode_bitmap(present.iter().copied(), out);
    }
}

// ---------------------------------------------------------------------------------------
// Footer encoding
// ---------------------------------------------------------------------------------------

const FOOTER_FORMAT: u8 = 1;

fn encode_footer(f: &Footer) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(FOOTER_FORMAT);
    put_string(&mut out, &f.table);
    out.push(granularity_code(f.partition.granularity));
    put_ivarint(&mut out, f.partition.index);
    put_uvarint(&mut out, f.row_count);
    put_uvarint(&mut out, f.columns.len() as u64);
    for c in &f.columns {
        put_uvarint(&mut out, u64::from(c.id));
        out.push(type_code(c.ty));
    }
    let s = &f.stats;
    for b in [
        s.value_time,
        s.received_time,
        s.published_time,
        s.system_time,
    ] {
        b.put(&mut out);
    }
    put_uvarint(&mut out, s.published_null_count);
    put_uvarint(&mut out, s.seq_min);
    put_uvarint(&mut out, s.seq_max);
    put_uvarint(&mut out, s.deleted_count);
    put_uvarint(&mut out, f.series.len() as u64);
    let mut prev = 0u32;
    for e in &f.series {
        put_uvarint(&mut out, u64::from(e.id.0 - prev));
        put_uvarint(&mut out, e.row_end - e.row_start);
        prev = e.id.0;
    }
    for cs in &f.column_stats {
        put_uvarint(&mut out, cs.null_count);
        match &cs.min_max {
            None => out.push(0),
            Some((lo, hi)) => {
                out.push(1);
                for v in [lo, hi] {
                    let bits = match v {
                        Value::F64(x) => x.to_bits(),
                        Value::I64(x) => *x as u64,
                        Value::Timestamp(t) => t.as_micros() as u64,
                        _ => unreachable!("min/max only for numeric columns"),
                    };
                    out.extend_from_slice(&bits.to_le_bytes());
                }
            }
        }
    }
    put_uvarint(&mut out, f.blocks.len() as u64);
    for b in &f.blocks {
        put_uvarint(&mut out, u64::from(b.row_count));
        for t in [
            b.value_time,
            b.received_time,
            b.published_time,
            b.system_time,
        ] {
            t.put(&mut out);
        }
        for c in &b.chunks {
            put_uvarint(&mut out, c.offset);
            put_uvarint(&mut out, c.length);
            put_uvarint(&mut out, c.raw_length);
            out.push(c.encoding.code());
            out.push(match c.compression {
                Compression::None => 0,
                Compression::Zstd => 1,
            });
            out.extend_from_slice(&c.crc32c.to_le_bytes());
        }
    }
    out
}

fn decode_footer(bytes: &[u8], footer_offset: u64, file_size: u64) -> Result<Footer> {
    let mut r = Reader::new(bytes, "footer");
    let fmt = r.u8()?;
    if fmt != FOOTER_FORMAT {
        return Err(Error::unsupported(format!("footer format {fmt}")));
    }
    let table = r.string()?.to_owned();
    let granularity = match r.u8()? {
        0 => Partitioning::Day,
        1 => Partitioning::Month,
        2 => Partitioning::Year,
        g => return Err(r.err(format!("unknown partition granularity {g}"))),
    };
    let partition = Partition {
        granularity,
        index: r.ivarint()?,
    };
    if !partition.is_valid() {
        return Err(r.err("partition outside the valid timestamp range"));
    }
    let row_count = r.uvarint()?;
    if row_count == 0 {
        return Err(r.err("segment has no rows"));
    }
    let ncols = r.count(r.remaining() / 2, "column count")?;
    let mut columns = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let id = u32::try_from(r.uvarint()?).map_err(|_| r.err("column id exceeds u32"))?;
        let ty = type_from_code(r.u8()?).ok_or_else(|| r.err("unknown column type"))?;
        if columns.last().is_some_and(|c: &ValueColumn| c.id >= id) {
            return Err(r.err("column ids not strictly increasing"));
        }
        columns.push(ValueColumn { id, ty });
    }
    let stats = SegmentStats {
        value_time: TimeBounds::read(&mut r)?,
        received_time: TimeBounds::read(&mut r)?,
        published_time: TimeBounds::read(&mut r)?,
        system_time: TimeBounds::read(&mut r)?,
        published_null_count: r.uvarint()?,
        seq_min: r.uvarint()?,
        seq_max: r.uvarint()?,
        deleted_count: r.uvarint()?,
    };
    let prange = partition.range();
    if !(prange.contains(stats.value_time.min) && prange.contains(stats.value_time.max)) {
        return Err(r.err("value_time bounds outside the partition"));
    }
    if stats.seq_min > stats.seq_max
        || stats.published_null_count > row_count
        || stats.deleted_count > row_count
        || stats.seq_max - stats.seq_min < row_count - 1
    {
        return Err(r.err("inconsistent segment statistics"));
    }
    let nseries = r.count(r.remaining() / 2, "series count")?;
    if nseries == 0 {
        return Err(r.err("empty series directory"));
    }
    let mut series = Vec::with_capacity(nseries);
    let (mut prev, mut row) = (0u64, 0u64);
    for i in 0..nseries {
        let delta = r.uvarint()?;
        if i > 0 && delta == 0 {
            return Err(r.err("series ids not strictly increasing"));
        }
        let id = prev
            .checked_add(delta)
            .filter(|&id| id <= u64::from(u32::MAX))
            .ok_or_else(|| r.err("series id exceeds u32"))?;
        let rows = r.uvarint()?;
        let end = row
            .checked_add(rows)
            .filter(|&e| rows > 0 && e <= row_count)
            .ok_or_else(|| r.err("bad series row count"))?;
        series.push(SeriesEntry {
            id: SeriesId(id as u32),
            row_start: row,
            row_end: end,
        });
        prev = id;
        row = end;
    }
    if row != row_count {
        return Err(r.err("series directory does not cover every row"));
    }
    let mut column_stats = Vec::with_capacity(ncols);
    for c in &columns {
        let null_count = r.uvarint()?;
        if null_count > row_count {
            return Err(r.err("null count exceeds row count"));
        }
        let min_max = match r.u8()? {
            0 => None,
            1 => {
                let (lo, hi) = (r.u64_le()?, r.u64_le()?);
                Some(match c.ty {
                    ColumnType::F64 => {
                        let (lo, hi) = (f64::from_bits(lo), f64::from_bits(hi));
                        if lo.is_nan() || hi.is_nan() || lo > hi {
                            return Err(r.err("bad f64 min/max"));
                        }
                        (Value::F64(lo), Value::F64(hi))
                    }
                    ColumnType::I64 => {
                        if (lo as i64) > (hi as i64) {
                            return Err(r.err("bad i64 min/max"));
                        }
                        (Value::I64(lo as i64), Value::I64(hi as i64))
                    }
                    ColumnType::Timestamp => {
                        let lo = ts_checked(lo as i64, "column min")?;
                        let hi = ts_checked(hi as i64, "column max")?;
                        if lo > hi {
                            return Err(r.err("bad timestamp min/max"));
                        }
                        (Value::Timestamp(lo), Value::Timestamp(hi))
                    }
                    _ => return Err(r.err("min/max on a non-numeric column")),
                })
            }
            m => return Err(r.err(format!("bad min/max flag {m}"))),
        };
        column_stats.push(ColumnStats {
            null_count,
            min_max,
        });
    }
    let nchunks = ENGINE_COLUMNS + ncols;
    let nblocks = r.count(r.remaining() / (8 + 8 * nchunks), "block count")?;
    if nblocks == 0 {
        return Err(r.err("no blocks"));
    }
    let mut blocks = Vec::with_capacity(nblocks);
    let mut row = 0u64;
    for _ in 0..nblocks {
        let n = r.count(MAX_BLOCK_ROWS, "block row count")?;
        if n == 0 || row + n as u64 > row_count {
            return Err(r.err("bad block row count"));
        }
        let (vt, rt, pt, st) = (
            TimeBounds::read(&mut r)?,
            TimeBounds::read(&mut r)?,
            TimeBounds::read(&mut r)?,
            TimeBounds::read(&mut r)?,
        );
        if !(stats.value_time.contains_bounds(&vt)
            && stats.received_time.contains_bounds(&rt)
            && stats.published_time.contains_bounds(&pt)
            && stats.system_time.contains_bounds(&st))
        {
            return Err(r.err("block statistics outside segment statistics"));
        }
        let mut chunks = Vec::with_capacity(nchunks);
        for pos in 0..nchunks {
            let offset = r.uvarint()?;
            let length = r.uvarint()?;
            let raw_length = r.uvarint()?;
            let encoding = Encoding::from_code(r.u8()?).ok_or_else(|| r.err("unknown encoding"))?;
            let compression = match r.u8()? {
                0 => Compression::None,
                1 => Compression::Zstd,
                c => return Err(r.err(format!("unknown compression {c}"))),
            };
            let crc32c = r.u32_le()?;
            let ty = pos.checked_sub(ENGINE_COLUMNS).map(|j| columns[j].ty);
            if !encoding_allowed(pos, ty, encoding) {
                return Err(r.err(format!("encoding {encoding:?} not allowed for chunk {pos}")));
            }
            let end = offset.checked_add(length);
            if offset < HEADER_LEN || end.is_none_or(|e| e > footer_offset) {
                return Err(r.err("chunk outside the data region"));
            }
            if raw_length > max_raw_length(pos, ty, n as u64)
                || (compression == Compression::None && raw_length != length)
            {
                return Err(r.err("bad chunk raw length"));
            }
            chunks.push(ChunkMeta {
                offset,
                length,
                raw_length,
                encoding,
                compression,
                crc32c,
            });
        }
        blocks.push(BlockMeta {
            row_start: row,
            row_count: n as u32,
            value_time: vt,
            received_time: rt,
            published_time: pt,
            system_time: st,
            chunks,
        });
        row += n as u64;
    }
    if row != row_count {
        return Err(r.err("blocks do not cover every row"));
    }
    r.finish()?;
    Ok(Footer {
        table,
        partition,
        row_count,
        columns,
        stats,
        series,
        column_stats,
        blocks,
        footer_offset,
        file_size,
    })
}

/// Result of [`parse_tail`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Tail {
    /// The footer was complete in the given bytes.
    Footer(Box<Footer>),
    /// The footer is larger: read (at least) this many bytes from the end of the file and
    /// call again.
    NeedBytes(u64),
}

/// Parses the footer from `tail`, the last `tail.len()` bytes of a file of `file_size`
/// bytes (FORMAT.md §6). Read e.g. the last 64 KiB; if the footer is bigger the result
/// says how many bytes to read.
pub fn parse_tail(tail: &[u8], file_size: u64) -> Result<Tail> {
    if file_size < HEADER_LEN + TAIL_LEN {
        return Err(Error::corrupt(format!(
            "{file_size} bytes is too small for a segment"
        )));
    }
    let k = tail.len() as u64;
    if !(TAIL_LEN..=file_size).contains(&k) {
        return Err(Error::invalid(format!(
            "tail of {k} bytes for a {file_size}-byte file"
        )));
    }
    let mut r = Reader::new(&tail[tail.len() - TAIL_LEN as usize..], "tail");
    let footer_len = u64::from(r.u32_le()?);
    let crc = r.u32_le()?;
    if r.bytes(8)? != MAGIC {
        return Err(Error::corrupt(
            "bad trailing magic (not a segment, or truncated)",
        ));
    }
    if HEADER_LEN + footer_len + TAIL_LEN > file_size {
        return Err(Error::corrupt("footer length exceeds the file"));
    }
    if k < footer_len + TAIL_LEN {
        return Ok(Tail::NeedBytes(footer_len + TAIL_LEN));
    }
    if k == file_size {
        check_header(tail)?;
    }
    let end = tail.len() - TAIL_LEN as usize;
    let bytes = &tail[end - footer_len as usize..end];
    let computed = crc32c::crc32c(bytes);
    if computed != crc {
        return Err(Error::corrupt(format!(
            "footer checksum mismatch (stored {crc:08x}, computed {computed:08x})"
        )));
    }
    let footer_offset = file_size - TAIL_LEN - footer_len;
    Ok(Tail::Footer(Box::new(decode_footer(
        bytes,
        footer_offset,
        file_size,
    )?)))
}

fn check_header(file: &[u8]) -> Result<()> {
    let mut r = Reader::new(file, "header");
    if r.bytes(8)? != MAGIC {
        return Err(Error::corrupt("bad leading magic"));
    }
    let version = r.u16_le()?;
    if version != FORMAT_VERSION {
        return Err(Error::unsupported(format!("format version {version}")));
    }
    let (flags, reserved) = (r.u16_le()?, r.u32_le()?);
    if flags != 0 || reserved != 0 {
        return Err(Error::unsupported("unknown header flags"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------------------

/// Writer settings. Never affect what a reader decodes.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct WriteOptions {
    /// Maximum rows per block (1..=[`MAX_BLOCK_ROWS`]).
    pub block_rows: usize,
    /// Compress chunks with zstd when that makes them smaller.
    pub compress: bool,
    /// zstd level.
    pub zstd_level: i32,
}

impl Default for WriteOptions {
    fn default() -> WriteOptions {
        WriteOptions {
            block_rows: 16_384,
            compress: true,
            zstd_level: 3,
        }
    }
}

/// Encodes `versions` of one table partition into a segment file.
///
/// `columns` are the value columns to store (normally every current table column, by
/// ascending id); `versions[i].values[id]` is the value of column `id` (missing = null).
/// Rows are sorted by `(series, value_time, received_time, system_time, seq)` here.
/// Fails with `InvalidInput` if there are no rows, a row lies outside `partition`, two
/// rows share a seq, a value has the wrong type, or a column id repeats.
pub fn write_segment(
    table: &str,
    partition: Partition,
    columns: &[ValueColumn],
    versions: &[Version],
    opts: &WriteOptions,
) -> Result<Vec<u8>> {
    if versions.is_empty() {
        return Err(Error::invalid("a segment needs at least one row"));
    }
    if !(1..=MAX_BLOCK_ROWS).contains(&opts.block_rows) {
        return Err(Error::invalid("block_rows out of range"));
    }
    if columns.windows(2).any(|w| w[0].id >= w[1].id) {
        return Err(Error::invalid("column ids must be strictly increasing"));
    }
    let prange = partition.range();
    let mut order: Vec<&Version> = versions.iter().collect();
    order.sort_by_key(|v| {
        (
            v.series,
            v.value_time,
            v.received_time,
            v.system_time,
            v.seq,
        )
    });
    for (i, v) in order.iter().enumerate() {
        if !prange.contains(v.value_time) {
            return Err(Error::invalid(format!(
                "value_time {} is outside partition {}",
                v.value_time,
                partition.label()
            )));
        }
        if i > 0 && order[i - 1].seq == v.seq {
            return Err(Error::invalid(format!("duplicate seq {}", v.seq)));
        }
        for (id, val) in v.values.iter().enumerate() {
            if let Some(val) = val {
                match columns.iter().find(|c| c.id as usize == id) {
                    Some(c) if val.column_type() == c.ty => {}
                    Some(c) => {
                        return Err(Error::invalid(format!(
                            "value {val} does not match column {id} of type {}",
                            c.ty
                        )));
                    }
                    None => {
                        return Err(Error::invalid(format!(
                            "row has a value for column {id}, which is not written"
                        )));
                    }
                }
            }
        }
    }

    let eff = |v: &Version| v.published_time.unwrap_or(v.received_time);
    let bounds = |rows: &[&Version], f: &dyn Fn(&Version) -> Timestamp| {
        TimeBounds::of(rows.iter().map(|v| f(v))).expect("non-empty")
    };

    let mut file = Vec::new();
    file.extend_from_slice(&MAGIC);
    file.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    file.extend_from_slice(&0u16.to_le_bytes());
    file.extend_from_slice(&0u32.to_le_bytes());

    let put_chunk = |file: &mut Vec<u8>, raw: Vec<u8>, encoding: Encoding| -> ChunkMeta {
        let raw_length = raw.len() as u64;
        let (stored, compression) = if opts.compress && raw.len() >= 64 {
            match zstd::bulk::compress(&raw, opts.zstd_level) {
                Ok(c) if c.len() + c.len() / 8 < raw.len() => (c, Compression::Zstd),
                _ => (raw, Compression::None),
            }
        } else {
            (raw, Compression::None)
        };
        let meta = ChunkMeta {
            offset: file.len() as u64,
            length: stored.len() as u64,
            raw_length,
            encoding,
            compression,
            crc32c: crc32c::crc32c(&stored),
        };
        file.extend_from_slice(&stored);
        meta
    };

    let mut blocks = Vec::new();
    for (bi, rows) in order.chunks(opts.block_rows).enumerate() {
        let mut chunks = Vec::with_capacity(ENGINE_COLUMNS + columns.len());
        let ints = |f: &dyn Fn(&Version) -> i64, enc: Encoding| {
            let vals: Vec<i64> = rows.iter().map(|v| f(v)).collect();
            let mut raw = Vec::new();
            encode_ints(enc, &vals, &mut raw);
            raw
        };
        let raw = ints(&|v| i64::from(v.series.0), Encoding::RunLength);
        chunks.push(put_chunk(&mut file, raw, Encoding::RunLength));
        let raw = ints(&|v| v.value_time.as_micros(), Encoding::DeltaOfDelta);
        chunks.push(put_chunk(&mut file, raw, Encoding::DeltaOfDelta));
        {
            let present: Vec<bool> = rows.iter().map(|v| v.published_time.is_some()).collect();
            let mut raw = Vec::new();
            put_validity(&present, &mut raw);
            let vals: Vec<i64> = rows
                .iter()
                .filter_map(|v| v.published_time.map(Timestamp::as_micros))
                .collect();
            encode_ints(Encoding::Delta, &vals, &mut raw);
            chunks.push(put_chunk(&mut file, raw, Encoding::Delta));
        }
        let raw = ints(&|v| v.received_time.as_micros(), Encoding::Delta);
        chunks.push(put_chunk(&mut file, raw, Encoding::Delta));
        let raw = ints(&|v| v.system_time.as_micros(), Encoding::Delta);
        chunks.push(put_chunk(&mut file, raw, Encoding::Delta));
        let raw = ints(&|v| v.seq as i64, Encoding::Delta);
        chunks.push(put_chunk(&mut file, raw, Encoding::Delta));
        {
            let mut raw = Vec::new();
            encode_bitmap(rows.iter().map(|v| v.deleted), &mut raw);
            chunks.push(put_chunk(&mut file, raw, Encoding::Bitmap));
        }
        for c in columns {
            let vals: Vec<Option<&Value>> = rows.iter().map(|v| v.value(c.id as usize)).collect();
            let present: Vec<bool> = vals.iter().map(Option::is_some).collect();
            let mut raw = Vec::new();
            put_validity(&present, &mut raw);
            let enc = value_encoding(c.ty);
            let some = vals.iter().flatten();
            match c.ty {
                ColumnType::F64 => {
                    for v in some {
                        if let Value::F64(x) = v {
                            raw.extend_from_slice(&x.to_bits().to_le_bytes());
                        }
                    }
                }
                ColumnType::I64 | ColumnType::Timestamp => {
                    let ints: Vec<i64> = some
                        .map(|v| match v {
                            Value::I64(x) => *x,
                            Value::Timestamp(t) => t.as_micros(),
                            _ => unreachable!("type checked above"),
                        })
                        .collect();
                    encode_ints(enc, &ints, &mut raw);
                }
                ColumnType::Bool => {
                    let bits: Vec<bool> = some.map(|v| matches!(v, Value::Bool(true))).collect();
                    encode_bitmap(bits.into_iter(), &mut raw);
                }
                ColumnType::String => {
                    for v in some {
                        if let Value::String(s) = v {
                            put_string(&mut raw, s);
                        }
                    }
                }
            }
            chunks.push(put_chunk(&mut file, raw, enc));
        }
        blocks.push(BlockMeta {
            row_start: (bi * opts.block_rows) as u64,
            row_count: rows.len() as u32,
            value_time: bounds(rows, &|v| v.value_time),
            received_time: bounds(rows, &|v| v.received_time),
            published_time: bounds(rows, &eff),
            system_time: bounds(rows, &|v| v.system_time),
            chunks,
        });
    }

    let mut series: Vec<SeriesEntry> = Vec::new();
    for (i, v) in order.iter().enumerate() {
        match series.last_mut() {
            Some(e) if e.id == v.series => e.row_end = i as u64 + 1,
            _ => series.push(SeriesEntry {
                id: v.series,
                row_start: i as u64,
                row_end: i as u64 + 1,
            }),
        }
    }
    let column_stats = columns
        .iter()
        .map(|c| {
            let vals: Vec<&Value> = order
                .iter()
                .filter_map(|v| v.value(c.id as usize))
                .collect();
            let null_count = (order.len() - vals.len()) as u64;
            let min_max = match c.ty {
                ColumnType::F64 => {
                    let xs = vals.iter().filter_map(|v| match v {
                        Value::F64(x) if !x.is_nan() => Some(*x),
                        _ => None,
                    });
                    xs.fold(None, |acc: Option<(f64, f64)>, x| match acc {
                        None => Some((x, x)),
                        Some((lo, hi)) => {
                            Some((if x < lo { x } else { lo }, if x > hi { x } else { hi }))
                        }
                    })
                    .map(|(lo, hi)| (Value::F64(lo), Value::F64(hi)))
                }
                ColumnType::I64 => {
                    let xs: Vec<i64> = vals
                        .iter()
                        .filter_map(|v| match v {
                            Value::I64(x) => Some(*x),
                            _ => None,
                        })
                        .collect();
                    xs.iter()
                        .min()
                        .zip(xs.iter().max())
                        .map(|(a, b)| (Value::I64(*a), Value::I64(*b)))
                }
                ColumnType::Timestamp => {
                    let xs: Vec<Timestamp> = vals
                        .iter()
                        .filter_map(|v| match v {
                            Value::Timestamp(x) => Some(*x),
                            _ => None,
                        })
                        .collect();
                    xs.iter()
                        .min()
                        .zip(xs.iter().max())
                        .map(|(a, b)| (Value::Timestamp(*a), Value::Timestamp(*b)))
                }
                _ => None,
            };
            ColumnStats {
                null_count,
                min_max,
            }
        })
        .collect();
    let footer = Footer {
        table: table.to_owned(),
        partition,
        row_count: order.len() as u64,
        columns: columns.to_vec(),
        stats: SegmentStats {
            value_time: bounds(&order, &|v| v.value_time),
            received_time: bounds(&order, &|v| v.received_time),
            published_time: bounds(&order, &eff),
            system_time: bounds(&order, &|v| v.system_time),
            published_null_count: order.iter().filter(|v| v.published_time.is_none()).count()
                as u64,
            seq_min: order.iter().map(|v| v.seq).min().expect("non-empty"),
            seq_max: order.iter().map(|v| v.seq).max().expect("non-empty"),
            deleted_count: order.iter().filter(|v| v.deleted).count() as u64,
        },
        series,
        column_stats,
        blocks,
        footer_offset: file.len() as u64,
        file_size: 0,
    };
    let fbytes = encode_footer(&footer);
    let flen = u32::try_from(fbytes.len()).map_err(|_| Error::invalid("footer exceeds 4 GiB"))?;
    file.extend_from_slice(&fbytes);
    file.extend_from_slice(&flen.to_le_bytes());
    file.extend_from_slice(&crc32c::crc32c(&fbytes).to_le_bytes());
    file.extend_from_slice(&MAGIC);
    Ok(file)
}

// ---------------------------------------------------------------------------------------
// Whole-file reader
// ---------------------------------------------------------------------------------------

/// A segment held entirely in memory (tests, tools, compaction).
#[derive(Clone, Debug)]
pub struct SegmentFile<'a> {
    bytes: &'a [u8],
    footer: Footer,
}

impl<'a> SegmentFile<'a> {
    /// Opens a complete segment file, verifying header, tail and footer.
    pub fn open(bytes: &'a [u8]) -> Result<SegmentFile<'a>> {
        check_header(bytes)?;
        match parse_tail(bytes, bytes.len() as u64)? {
            Tail::Footer(footer) => Ok(SegmentFile {
                bytes,
                footer: *footer,
            }),
            Tail::NeedBytes(_) => Err(Error::corrupt("footer larger than the file")),
        }
    }

    /// The footer.
    pub fn footer(&self) -> &Footer {
        &self.footer
    }

    fn slice(&self, r: Range<u64>) -> &'a [u8] {
        // Footer validation guarantees chunk ranges lie within the file.
        &self.bytes[r.start as usize..r.end as usize]
    }

    /// Decodes the engine columns of block `b`.
    pub fn engine_block(&self, b: usize) -> Result<EngineBlock> {
        let ranges = self.footer.engine_chunk_ranges(b);
        let chunks: Vec<&[u8]> = ranges.into_iter().map(|r| self.slice(r)).collect();
        self.footer.decode_engine_block(b, &chunks)
    }

    /// Decodes value column at position `pos` of block `b`.
    pub fn value_chunk(&self, b: usize, pos: usize) -> Result<Vec<Option<Value>>> {
        let meta = self
            .footer
            .blocks
            .get(b)
            .and_then(|bl| bl.chunks.get(ENGINE_COLUMNS + pos))
            .ok_or_else(|| Error::invalid("no such chunk"))?;
        self.footer
            .decode_value_chunk(b, pos, self.slice(meta.range()))
    }

    /// Every row as a [`Version`], in file order. `values` is indexed by column id (with
    /// nulls for ids the segment does not store), trimmed of trailing nulls: each row's
    /// vector is only as long as its last non-null value requires.
    pub fn versions(&self) -> Result<Vec<Version>> {
        self.versions_within(usize::MAX)
    }

    /// [`SegmentFile::versions`], but a column id `>= width` (a column the table does not
    /// have) is reported as corruption. Engines pass the table's column count, which also
    /// bounds every row's value vector.
    pub fn versions_within(&self, width: usize) -> Result<Vec<Version>> {
        if let Some(c) = self.footer.columns.iter().find(|c| c.id as usize >= width) {
            return Err(Error::corrupt(format!(
                "column id {} beyond the table's {width} columns",
                c.id
            )));
        }
        let mut out = Vec::with_capacity(self.footer.row_count as usize);
        for b in 0..self.footer.blocks.len() {
            let e = self.engine_block(b)?;
            let cols: Vec<Vec<Option<Value>>> = (0..self.footer.columns.len())
                .map(|p| self.value_chunk(b, p))
                .collect::<Result<_>>()?;
            #[allow(clippy::needless_range_loop)]
            for i in 0..e.len() {
                let last = (0..cols.len()).rev().find(|&p| cols[p][i].is_some());
                let mut values = Vec::new();
                if let Some(last) = last {
                    values.resize(self.footer.columns[last].id as usize + 1, None);
                    for (p, c) in self.footer.columns.iter().enumerate().take(last + 1) {
                        values[c.id as usize] = cols[p][i].clone();
                    }
                }
                if e.deleted[i] && !values.is_empty() {
                    return Err(Error::corrupt("tombstone row with values"));
                }
                out.push(Version {
                    series: e.series[i],
                    value_time: e.value_time[i],
                    published_time: e.published_time[i],
                    received_time: e.received_time[i],
                    deleted: e.deleted[i],
                    values,
                    system_time: e.system_time[i],
                    seq: e.seq[i],
                });
            }
        }
        Ok(out)
    }
}
