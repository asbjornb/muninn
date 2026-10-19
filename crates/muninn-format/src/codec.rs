//! Low-level byte codecs: LEB128 varints, zigzag, a bounds-checked reader, and the column
//! encodings of `spec/FORMAT.md` §4.
//!
//! Every decoder is total: on malformed input it returns [`Error`], never panics, and never
//! allocates more than a small multiple of its input (counts are checked against the bytes
//! that remain before anything is reserved).

use crate::{Error, Result};

/// Appends `v` as an unsigned LEB128 varint.
pub fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Zigzag-maps a signed integer to unsigned (`0, -1, 1, -2, …` → `0, 1, 2, 3, …`).
pub const fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// Inverse of [`zigzag`].
pub const fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// Appends `v` zigzag-mapped as a varint.
pub fn put_ivarint(out: &mut Vec<u8>, v: i64) {
    put_uvarint(out, zigzag(v));
}

/// Decodes a canonical varint of at most 8 bytes from the first 8 bytes of `buf` in a
/// few word operations: `None` if fewer than 8 bytes are available, the varint is longer,
/// or it is not canonical (callers fall back to the byte-wise decoder, which reports
/// errors).
#[inline(always)]
fn varint_word(buf: &[u8]) -> Option<(u64, usize)> {
    let w = u64::from_le_bytes(*buf.first_chunk::<8>()?);
    // Bit 7 of byte k is clear where the varint ends.
    let stops = !w & 0x8080_8080_8080_8080;
    if stops == 0 {
        return None;
    }
    let len = (stops.trailing_zeros() as usize >> 3) + 1;
    let w = if len == 8 {
        w
    } else {
        w & ((1u64 << (8 * len)) - 1)
    };
    // Canonical: a multi-byte varint does not end in a zero byte.
    if len > 1 && w >> (8 * (len - 1)) == 0 {
        return None;
    }
    // Squeeze the 7-bit groups together (8 x 7 = 56 bits, no overflow possible).
    let x = w & 0x7f7f_7f7f_7f7f_7f7f;
    let x = ((x & 0x7f00_7f00_7f00_7f00) >> 1) | (x & 0x007f_007f_007f_007f);
    let x = ((x & 0x3fff_0000_3fff_0000) >> 2) | (x & 0x0000_3fff_0000_3fff);
    let x = ((x & 0x0fff_ffff_0000_0000) >> 4) | (x & 0x0000_0000_0fff_ffff);
    Some((x, len))
}

/// A bounds-checked cursor over a byte slice.
#[derive(Clone, Debug)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    what: &'static str,
}

impl<'a> Reader<'a> {
    /// A reader over `buf`; `what` names the structure in error messages.
    pub fn new(buf: &'a [u8], what: &'static str) -> Reader<'a> {
        Reader { buf, pos: 0, what }
    }

    /// Bytes not yet consumed.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Fails unless every byte was consumed.
    pub fn finish(&self) -> Result<()> {
        if self.remaining() != 0 {
            return Err(self.err(format!("{} trailing bytes", self.remaining())));
        }
        Ok(())
    }

    /// A corruption error mentioning the structure and offset.
    pub fn err(&self, msg: impl std::fmt::Display) -> Error {
        Error::corrupt(format!("{} at byte {}: {msg}", self.what, self.pos))
    }

    /// The next `n` bytes.
    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.remaining() {
            return Err(self.err(format!(
                "need {n} bytes, {} remain (truncated)",
                self.remaining()
            )));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// One byte.
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    /// A little-endian `u16`.
    pub fn u16_le(&mut self) -> Result<u16> {
        let b = self.bytes(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    /// A little-endian `u32`.
    pub fn u32_le(&mut self) -> Result<u32> {
        let b = self.bytes(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// A little-endian `u64`.
    pub fn u64_le(&mut self) -> Result<u64> {
        let b = self.bytes(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_le_bytes(a))
    }

    /// An unsigned LEB128 varint of at most 10 bytes whose value fits in 64 bits.
    pub fn uvarint(&mut self) -> Result<u64> {
        let mut v: u64 = 0;
        for i in 0..10 {
            let b = self.u8()?;
            let payload = u64::from(b & 0x7f);
            if i == 9 && payload > 1 {
                return Err(self.err("varint overflows 64 bits"));
            }
            v |= payload << (7 * i);
            if b & 0x80 == 0 {
                if i > 0 && b == 0 {
                    return Err(self.err("non-canonical varint (trailing zero byte)"));
                }
                return Ok(v);
            }
        }
        Err(self.err("varint longer than 10 bytes"))
    }

    /// A zigzag varint.
    pub fn ivarint(&mut self) -> Result<i64> {
        Ok(unzigzag(self.uvarint()?))
    }

    /// [`Reader::uvarint`] with a fast path: the same values accepted and the same
    /// inputs rejected, but without a bounds check per byte while at least 10 bytes
    /// remain (the longest varint). Used by the column decoders.
    #[inline]
    pub fn uvarint_fast(&mut self) -> Result<u64> {
        let rest = &self.buf[self.pos..];
        if let Some(&b0) = rest.first()
            && b0 < 0x80
        {
            self.pos += 1;
            return Ok(u64::from(b0));
        }
        match varint_word(rest) {
            Some((v, len)) => {
                self.pos += len;
                Ok(v)
            }
            // Fewer than 8 bytes left, longer than 8 bytes, or non-canonical: the
            // byte-wise path decodes or reports it.
            None => self.uvarint(),
        }
    }

    /// A varint that must fit in `usize` and be at most `max`.
    pub fn count(&mut self, max: usize, what: &str) -> Result<usize> {
        let v = self.uvarint()?;
        if v > max as u64 {
            return Err(self.err(format!("{what} {v} exceeds limit {max}")));
        }
        Ok(v as usize)
    }

    /// A varint-length-prefixed UTF-8 string.
    pub fn string(&mut self) -> Result<&'a str> {
        let n = self.count(self.remaining(), "string length")?;
        let b = self.bytes(n)?;
        std::str::from_utf8(b).map_err(|_| self.err("invalid UTF-8"))
    }
}

/// Appends a varint-length-prefixed string.
pub fn put_string(out: &mut Vec<u8>, s: &str) {
    put_uvarint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

// ---------------------------------------------------------------------------------------
// Column encodings (FORMAT.md §4.2)
// ---------------------------------------------------------------------------------------

/// The encoding of a chunk's (present) values.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Encoding {
    /// `1`: runs of `(uvarint value, uvarint run_length ≥ 1)`.
    RunLength,
    /// `2`: zigzag varint of the first value, then zigzag varints of successive
    /// differences (wrapping 64-bit arithmetic).
    Delta,
    /// `3`: zigzag varint of `v0`, of `v1 - v0`, then of each second difference.
    DeltaOfDelta,
    /// `4`: one bit per value, least-significant bit first, padding bits zero.
    Bitmap,
    /// `5`: 8 bytes little-endian IEEE-754 binary64 per value.
    PlainF64,
    /// `6`: per value a uvarint byte length followed by that many UTF-8 bytes.
    LengthPrefixed,
    /// `7`: zigzag varint per value.
    Zigzag,
}

impl Encoding {
    /// The on-disk code.
    pub const fn code(self) -> u8 {
        match self {
            Encoding::RunLength => 1,
            Encoding::Delta => 2,
            Encoding::DeltaOfDelta => 3,
            Encoding::Bitmap => 4,
            Encoding::PlainF64 => 5,
            Encoding::LengthPrefixed => 6,
            Encoding::Zigzag => 7,
        }
    }

    /// Parses an on-disk code.
    pub fn from_code(c: u8) -> Option<Encoding> {
        Some(match c {
            1 => Encoding::RunLength,
            2 => Encoding::Delta,
            3 => Encoding::DeltaOfDelta,
            4 => Encoding::Bitmap,
            5 => Encoding::PlainF64,
            6 => Encoding::LengthPrefixed,
            7 => Encoding::Zigzag,
            _ => return None,
        })
    }

    /// Is this one of the integer encodings ([`decode_ints`] accepts it)?
    pub const fn is_integer(self) -> bool {
        matches!(
            self,
            Encoding::RunLength | Encoding::Delta | Encoding::DeltaOfDelta | Encoding::Zigzag
        )
    }
}

/// Decodes `n` zigzag varints, passing each to `sink`: the column decoders' inner loop.
/// Works on the slice directly (word-at-a-time, see `varint_word`) and falls back to the
/// checked byte-wise decoder near the end of the buffer and for long or non-canonical
/// varints, so it accepts and rejects exactly what [`Reader::ivarint`] does.
#[inline]
fn zigzags(r: &mut Reader<'_>, n: usize, mut sink: impl FnMut(i64)) -> Result<()> {
    let buf = r.buf;
    let mut pos = r.pos;
    for _ in 0..n {
        match varint_word(&buf[pos..]) {
            Some((v, len)) => {
                pos += len;
                sink(unzigzag(v));
            }
            None => {
                r.pos = pos;
                let v = r.uvarint()?;
                pos = r.pos;
                sink(unzigzag(v));
            }
        }
    }
    r.pos = pos;
    Ok(())
}

/// Encodes 64-bit integers with an integer encoding.
pub fn encode_ints(enc: Encoding, values: &[i64], out: &mut Vec<u8>) {
    match enc {
        Encoding::RunLength => {
            let mut i = 0;
            while i < values.len() {
                let v = values[i];
                let mut j = i + 1;
                while j < values.len() && values[j] == v {
                    j += 1;
                }
                put_uvarint(out, v as u64);
                put_uvarint(out, (j - i) as u64);
                i = j;
            }
        }
        Encoding::Delta => {
            let mut prev = 0i64;
            for &v in values {
                put_ivarint(out, v.wrapping_sub(prev));
                prev = v;
            }
        }
        Encoding::DeltaOfDelta => {
            let (mut prev, mut prev_d) = (0i64, 0i64);
            for (i, &v) in values.iter().enumerate() {
                if i == 0 {
                    put_ivarint(out, v);
                } else {
                    let d = v.wrapping_sub(prev);
                    put_ivarint(out, d.wrapping_sub(prev_d));
                    prev_d = d;
                }
                prev = v;
            }
        }
        Encoding::Zigzag => {
            for &v in values {
                put_ivarint(out, v);
            }
        }
        _ => unreachable!("encode_ints called with non-integer encoding {enc:?}"),
    }
}

/// Decodes exactly `n` integers.
pub fn decode_ints(enc: Encoding, n: usize, r: &mut Reader<'_>) -> Result<Vec<i64>> {
    // Every encoding needs at least one byte per value except run-length.
    if enc != Encoding::RunLength && n > r.remaining() {
        return Err(r.err(format!("{n} values cannot fit in {} bytes", r.remaining())));
    }
    // Callers bound `n` (at most MAX_BLOCK_ROWS), so reserving it is safe.
    let mut out = Vec::with_capacity(n);
    match enc {
        Encoding::RunLength => {
            while out.len() < n {
                let v = r.uvarint_fast()? as i64;
                let run = r.uvarint_fast()?;
                if run == 0 || run > (n - out.len()) as u64 {
                    return Err(r.err(format!("bad run length {run}")));
                }
                out.extend(std::iter::repeat_n(v, run as usize));
            }
        }
        Encoding::Delta => {
            let mut prev = 0i64;
            zigzags(r, n, |x| {
                prev = prev.wrapping_add(x);
                out.push(prev);
            })?;
        }
        Encoding::DeltaOfDelta => {
            let (mut prev, mut prev_d, mut first) = (0i64, 0i64, true);
            zigzags(r, n, |x| {
                if first {
                    prev = x;
                    first = false;
                } else {
                    prev_d = prev_d.wrapping_add(x);
                    prev = prev.wrapping_add(prev_d);
                }
                out.push(prev);
            })?;
        }
        Encoding::Zigzag => zigzags(r, n, |x| out.push(x))?,
        _ => return Err(r.err(format!("{enc:?} is not an integer encoding"))),
    }
    Ok(out)
}

/// Appends a bitmap (LSB first, zero padding).
pub fn encode_bitmap(bits: impl ExactSizeIterator<Item = bool>, out: &mut Vec<u8>) {
    let n = bits.len();
    let start = out.len();
    out.resize(start + n.div_ceil(8), 0);
    for (i, b) in bits.enumerate() {
        if b {
            out[start + i / 8] |= 1 << (i % 8);
        }
    }
}

/// Decodes a bitmap of exactly `n` bits; padding bits must be zero.
pub fn decode_bitmap(n: usize, r: &mut Reader<'_>) -> Result<Vec<bool>> {
    let bytes = r.bytes(n.div_ceil(8))?;
    if n % 8 != 0 && bytes[n / 8] >> (n % 8) != 0 {
        return Err(r.err("bitmap padding bits are not zero"));
    }
    let mut out = Vec::with_capacity(n);
    for &b in bytes {
        for k in 0..8 {
            out.push(b & (1 << k) != 0);
        }
    }
    out.truncate(n);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn varint_edges() {
        for v in [0u64, 1, 127, 128, 300, u64::MAX] {
            let mut b = Vec::new();
            put_uvarint(&mut b, v);
            let mut r = Reader::new(&b, "t");
            assert_eq!(r.uvarint().unwrap(), v);
            r.finish().unwrap();
        }
        // Overlong / overflowing / non-canonical encodings are rejected.
        assert!(Reader::new(&[0xff; 11], "t").uvarint().is_err());
        assert!(
            Reader::new(
                &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02],
                "t"
            )
            .uvarint()
            .is_err()
        );
        assert!(Reader::new(&[0x80, 0x00], "t").uvarint().is_err());
        assert!(Reader::new(&[0x80], "t").uvarint().is_err());
        for v in [0i64, -1, 1, i64::MIN, i64::MAX] {
            assert_eq!(unzigzag(zigzag(v)), v);
        }
    }

    /// The fast varint path accepts and rejects exactly what the plain one does, at every
    /// position relative to the end of the buffer.
    #[test]
    fn fast_varint_matches_plain() {
        let mut cases: Vec<Vec<u8>> = vec![
            vec![0x80, 0x00],
            vec![0xff; 11],
            vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02],
            vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01],
            vec![0x80; 10],
            vec![0x81, 0x80, 0x00],
        ];
        for v in [0u64, 1, 127, 128, 300, 1 << 35, u64::MAX] {
            let mut b = Vec::new();
            put_uvarint(&mut b, v);
            cases.push(b);
        }
        for c in cases {
            for pad in 0..12 {
                let mut buf = c.clone();
                buf.extend(std::iter::repeat_n(0x05u8, pad));
                let (mut a, mut b) = (Reader::new(&buf, "t"), Reader::new(&buf, "t"));
                let (x, y) = (a.uvarint(), b.uvarint_fast());
                assert_eq!(x.is_ok(), y.is_ok(), "{buf:?}");
                if let (Ok(x), Ok(y)) = (x, y) {
                    assert_eq!(x, y);
                    assert_eq!(a.remaining(), b.remaining());
                }
            }
        }
    }

    /// Decoder speed on forecast-like columns (`cargo test --release -p muninn-format
    /// --lib -- --ignored --nocapture decode_speed`).
    #[test]
    #[ignore]
    fn decode_speed() {
        let n = 16_384;
        // Received times: hourly runs (3.6e9 µs steps) with jumps back between points.
        let rt: Vec<i64> = (0..n as i64)
            .map(|i| 1_700_000_000_000_000 + (i % 230) * 3_600_000_000 + (i % 7) * 1_000_000)
            .collect();
        let seq: Vec<i64> = (0..n as i64)
            .map(|i| 1000 + (i % 230) * 4000 + i / 230)
            .collect();
        for (name, enc, vals) in [
            ("delta rt", Encoding::Delta, &rt),
            ("delta seq", Encoding::Delta, &seq),
            ("dod rt", Encoding::DeltaOfDelta, &rt),
        ] {
            let mut b = Vec::new();
            encode_ints(enc, vals, &mut b);
            let t = std::time::Instant::now();
            let reps = 200;
            for _ in 0..reps {
                let mut r = Reader::new(&b, "t");
                let v = decode_ints(enc, n, &mut r).unwrap();
                assert_eq!(v.len(), n);
            }
            let ns = t.elapsed().as_nanos() as f64 / (reps * n) as f64;
            eprintln!(
                "{name}: {:.2} B/value, {ns:.2} ns/value",
                b.len() as f64 / n as f64
            );
        }
    }

    proptest! {
        #[test]
        fn fast_varint_matches_plain_random(bytes in prop::collection::vec(any::<u8>(), 0..24)) {
            let (mut a, mut b) = (Reader::new(&bytes, "t"), Reader::new(&bytes, "t"));
            loop {
                let (x, y) = (a.uvarint(), b.uvarint_fast());
                prop_assert_eq!(x.is_ok(), y.is_ok());
                match (x, y) {
                    (Ok(x), Ok(y)) => {
                        prop_assert_eq!(x, y);
                        prop_assert_eq!(a.remaining(), b.remaining());
                    }
                    _ => break,
                }
            }
        }

        #[test]
        fn ints_round_trip(values in prop::collection::vec(any::<i64>(), 0..50), small in prop::collection::vec(-3i64..3, 0..50)) {
            for enc in [Encoding::RunLength, Encoding::Delta, Encoding::DeltaOfDelta, Encoding::Zigzag] {
                for vs in [&values, &small] {
                    let mut b = Vec::new();
                    encode_ints(enc, vs, &mut b);
                    let mut r = Reader::new(&b, "t");
                    prop_assert_eq!(&decode_ints(enc, vs.len(), &mut r).unwrap(), vs);
                    prop_assert!(r.finish().is_ok());
                }
            }
        }

        #[test]
        fn bitmap_round_trip(bits in prop::collection::vec(any::<bool>(), 0..40)) {
            let mut b = Vec::new();
            encode_bitmap(bits.iter().copied(), &mut b);
            let mut r = Reader::new(&b, "t");
            prop_assert_eq!(decode_bitmap(bits.len(), &mut r).unwrap(), bits);
        }

        #[test]
        fn decoders_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..40), n in 0usize..100) {
            for enc in [Encoding::RunLength, Encoding::Delta, Encoding::DeltaOfDelta, Encoding::Zigzag] {
                let _ = decode_ints(enc, n, &mut Reader::new(&bytes, "t"));
            }
            let _ = decode_bitmap(n, &mut Reader::new(&bytes, "t"));
            let _ = Reader::new(&bytes, "t").string();
        }
    }
}
