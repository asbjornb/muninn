//! Decode cost breakdown (ignored by default): `MUNINN_PERF_SEGMENTS=<dir> cargo test
//! --release -p muninn-format --test perf -- --ignored --nocapture`. Prints, for every
//! `.seg` file under the directory: raw/stored bytes per engine chunk and the time of
//! decompression alone versus full engine-block decoding.

use std::time::Instant;

use muninn_format::{Compression, ENGINE_COLUMNS, SegmentFile};

fn files(p: &std::path::Path, out: &mut Vec<Vec<u8>>) {
    for e in std::fs::read_dir(p).unwrap().flatten() {
        let path = e.path();
        if path.is_dir() {
            files(&path, out);
        } else if path.extension().is_some_and(|x| x == "seg") {
            out.push(std::fs::read(&path).unwrap());
        }
    }
}

#[test]
#[ignore]
fn perf_decode_breakdown() {
    let Ok(dir) = std::env::var("MUNINN_PERF_SEGMENTS") else {
        eprintln!("set MUNINN_PERF_SEGMENTS");
        return;
    };
    let mut segs = Vec::new();
    files(std::path::Path::new(&dir), &mut segs);
    let opened: Vec<SegmentFile<'_>> = segs.iter().map(|b| SegmentFile::open(b).unwrap()).collect();
    let rows: u64 = opened.iter().map(|f| f.footer().row_count).sum();
    let mut raw = [0u64; ENGINE_COLUMNS];
    let mut stored = [0u64; ENGINE_COLUMNS];
    let mut zstd_chunks = [0u64; ENGINE_COLUMNS];
    for f in &opened {
        for b in &f.footer().blocks {
            for (i, c) in b.chunks[..ENGINE_COLUMNS].iter().enumerate() {
                raw[i] += c.raw_length;
                stored[i] += c.length;
                zstd_chunks[i] += u64::from(c.compression == Compression::Zstd);
            }
        }
    }
    let names = [
        "series",
        "value_time",
        "published",
        "received",
        "system",
        "seq",
        "deleted",
    ];
    for i in 0..ENGINE_COLUMNS {
        eprintln!(
            "{:>10}: raw {:6.2} B/row, stored {:6.2} B/row, {} zstd chunks, {:?}",
            names[i],
            raw[i] as f64 / rows as f64,
            stored[i] as f64 / rows as f64,
            zstd_chunks[i],
            opened[0].footer().blocks[0].chunks[i].encoding
        );
    }
    let mut d = zstd::bulk::Decompressor::new().unwrap();
    let t = Instant::now();
    let mut out = 0usize;
    for (f, bytes) in opened.iter().zip(&segs) {
        for b in &f.footer().blocks {
            for c in &b.chunks[..ENGINE_COLUMNS] {
                if c.compression == Compression::Zstd {
                    let s = &bytes[c.offset as usize..(c.offset + c.length) as usize];
                    let mut v = Vec::with_capacity(c.raw_length as usize);
                    out += d.decompress_to_buffer(s, &mut v).unwrap();
                }
            }
        }
    }
    let z = t.elapsed();
    let t = Instant::now();
    let mut n = 0;
    for f in &opened {
        for b in 0..f.footer().blocks.len() {
            n += f.engine_block(b).unwrap().len();
        }
    }
    let full = t.elapsed();
    eprintln!(
        "{rows} rows: zstd alone {z:?} ({:.1} ns/row, {out} B); full engine decode {full:?} \
         ({:.1} ns/row, {n} rows)",
        z.as_nanos() as f64 / rows as f64,
        full.as_nanos() as f64 / rows as f64
    );
}
