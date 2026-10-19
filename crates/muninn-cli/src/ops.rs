//! Operations: `compact`, `gc`, `info`, `verify`.

use std::collections::{BTreeMap, HashMap, HashSet};

use muninn_core::wire::Wire;
use muninn_core::{Duration, Timestamp};
use muninn_engine::EngineOptions;
use muninn_engine::log::{self, Checkpoint};
use muninn_engine::state::{SegmentRef, State};
use muninn_format::{SegmentFile, TimeBounds};
use muninn_store::{Key, Store, StoreErrorKind};

use crate::db::{Context, load_state, log_listing, read_checkpoint, read_entry, store_err};
use crate::output::{self, Format, Rows, n, ots, s};
use crate::{CliError, FormatArg, Res};

/// `compact`.
pub fn compact(ctx: &Context, table: Option<&str>) -> Res<()> {
    let db = ctx.open()?;
    let done = match table {
        None => db.compact_all()?,
        Some(t) => {
            muninn_core::Reader::table(&db, t)?;
            let mut done = 0;
            for ((tt, label), count) in db.segment_counts() {
                if tt == t && count > 1 && db.compact(&tt, &label)? {
                    done += 1;
                }
            }
            done
        }
    };
    output::line(&format!("compacted {done} partition(s)"))
}

/// `gc`.
pub fn gc(ctx: &Context, grace: &str) -> Res<()> {
    let d = Duration::parse(grace).map_err(|e| CliError::new(format!("--grace: {e}")))?;
    if d.as_micros() < 0 {
        return Err(CliError::new("--grace must not be negative"));
    }
    let db = ctx.open_with(EngineOptions {
        gc_grace: std::time::Duration::from_micros(d.as_micros() as u64),
        ..EngineOptions::default()
    })?;
    let r = db.gc()?;
    output::line(&format!(
        "deleted {} file(s), kept {} unreferenced file(s) within the grace period or in use",
        r.deleted.len(),
        r.kept
    ))
}

/// `info`.
pub fn info(ctx: &Context, f: FormatArg) -> Res<()> {
    let store = ctx.store()?;
    let st = load_state(&store)?;
    let format = f.resolve();
    let mut per_table: BTreeMap<&str, (usize, u64, u64, HashSet<i64>)> = BTreeMap::new();
    for seg in st.segments() {
        let e = per_table.entry(seg.table.as_str()).or_default();
        e.0 += 1;
        e.1 += seg.rows;
        e.2 += seg.size;
        e.3.insert(seg.partition.index);
    }
    let mut total_bytes = 0u64;
    let mut log_bytes = 0u64;
    for k in store.list("").map_err(|e| store_err("listing", e))? {
        let size = store.head(&k).map_err(|e| store_err("head", e))?.size;
        total_bytes += size;
        if k.as_str().starts_with("log/") {
            log_bytes += size;
        }
    }
    let cp = st.to_checkpoint();
    let mut db_rows = Rows::flat(&["key", "value"]);
    let kv: Vec<(&str, Wire)> = vec![
        ("database", s(ctx.dir()?.display().to_string())),
        ("log_version", n(st.version)),
        ("tables", n(st.catalog.tables().len())),
        ("series", n(st.catalog.all_series().len())),
        ("versions", n(st.segments().map(|x| x.rows).sum::<u64>())),
        ("segments", n(st.segments().count())),
        (
            "segment_bytes",
            n(st.segments().map(|x| x.size).sum::<u64>()),
        ),
        ("retired_segments", n(st.retired.len())),
        ("store_bytes", n(total_bytes)),
        ("log_bytes", n(log_bytes)),
        ("next_seq", n(st.next_seq)),
        ("last_system_time", ots(st.last_system_time)),
        ("batch_ids", n(cp.batch_ids.len())),
        ("catalog_changes", n(st.catalog_log.len())),
    ];
    for (k, v) in kv {
        db_rows.push(vec![s(k), v]);
    }
    let mut t_rows = Rows::flat(&[
        "table",
        "columns",
        "series",
        "partitions",
        "segments",
        "versions",
        "bytes",
    ]);
    for t in st.catalog.tables() {
        let series = st
            .catalog
            .all_series()
            .iter()
            .filter(|x| x.table == t.name)
            .count();
        let (segs, rows, bytes, parts) = per_table
            .get(t.name.as_str())
            .map(|(a, b, c, d)| (*a, *b, *c, d.len()))
            .unwrap_or_default();
        t_rows.push(vec![
            s(&t.name),
            n(t.columns.len()),
            n(series),
            n(parts),
            n(segs),
            n(rows),
            n(bytes),
        ]);
    }
    match format {
        Format::Table => {
            output::print(&db_rows, format)?;
            output::line("")?;
            output::print(&t_rows, format)
        }
        Format::Ndjson => {
            let obj = Wire::Object(
                db_rows
                    .rows
                    .iter()
                    .map(|r| {
                        let key = r.cells[0].as_str().unwrap_or_default().to_owned();
                        (key, r.cells[1].clone())
                    })
                    .chain([(
                        "per_table".to_owned(),
                        Wire::Array(
                            t_rows
                                .rows
                                .iter()
                                .map(|r| output::json_object(&t_rows, r))
                                .collect(),
                        ),
                    )])
                    .collect(),
            );
            output::line(&obj.to_string())
        }
        Format::Csv => output::print(&t_rows, format),
    }
}

fn content_hash(v: &muninn_core::types::Version) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    (
        v.series,
        v.value_time,
        v.published_time,
        v.received_time,
        v.deleted,
        &v.values,
    )
        .hash(&mut h);
    h.finish()
}

/// Order-insensitive form of a checkpoint, for comparing states.
fn normalise(mut c: Checkpoint) -> Checkpoint {
    // The seal binds a checkpoint to its bytes and entry; a fold has neither (checked
    // separately).
    c.entry_crc32c = 0;
    c.checksum = 0;
    c.batch_ids.sort();
    c.segments.sort_by(|a, b| a.key.cmp(&b.key));
    c.retired.sort();
    c
}

/// Checks one live segment against its log record and the catalog; returns its seqs.
fn check_segment<S: Store>(
    store: &S,
    st: &State,
    seg: &SegmentRef,
    seqs: &mut HashSet<u64>,
    contents: &mut HashMap<u64, u64>,
    problems: &mut Vec<String>,
) -> u64 {
    let key = seg.key.as_str();
    let mut bad = |m: String| problems.push(format!("segment {key}: {m}"));
    let bytes = match store.get(&seg.key) {
        Ok(b) => b,
        Err(e) => {
            bad(format!("cannot read: {e}"));
            return 0;
        }
    };
    if bytes.len() as u64 != seg.size {
        bad(format!(
            "file has {} bytes, the log says {}",
            bytes.len(),
            seg.size
        ));
    }
    let file = match SegmentFile::open(&bytes) {
        Ok(f) => f,
        Err(e) => {
            bad(format!("bad header/footer: {e}"));
            return 0;
        }
    };
    let f = file.footer();
    let stats = f.stats;
    if f.table != seg.table || f.partition != seg.partition {
        bad(format!(
            "footer says table {:?} partition {}, the log says {:?} {}",
            f.table,
            f.partition.label(),
            seg.table,
            seg.partition.label()
        ));
    }
    if f.row_count != seg.rows
        || stats.value_time != seg.value_time
        || stats.received_time != seg.received_time
        || stats.published_time != seg.published_time
        || stats.system_time != seg.system_time
        || (stats.seq_min, stats.seq_max) != seg.seq
    {
        bad("footer statistics differ from the log record".into());
    }
    let versions = match file.versions() {
        Ok(v) => v,
        Err(e) => {
            bad(format!("cannot decode rows (checksum or encoding): {e}"));
            return 0;
        }
    };
    if versions.len() as u64 != f.row_count {
        bad(format!(
            "{} rows decoded, footer says {}",
            versions.len(),
            f.row_count
        ));
    }
    let table = match st.catalog.table(&seg.table) {
        Ok(t) => t,
        Err(_) => {
            bad("unknown table".into());
            return versions.len() as u64;
        }
    };
    for c in &f.columns {
        match table.columns.get(c.id as usize) {
            Some(tc) if tc.ty == c.ty => {}
            _ => bad(format!(
                "column id {} has type {} but the table disagrees",
                c.id, c.ty
            )),
        }
    }
    let widen = |b: Option<TimeBounds>, t: Timestamp| match b {
        None => Some(TimeBounds { min: t, max: t }),
        Some(b) => Some(TimeBounds {
            min: b.min.min(t),
            max: b.max.max(t),
        }),
    };
    let (mut vt, mut sy) = (None, None);
    let range = seg.partition.range();
    for v in &versions {
        vt = widen(vt, v.value_time);
        sy = widen(sy, v.system_time);
        match st.catalog.series_by_id(v.series) {
            Some(x) if x.table == seg.table => {}
            _ => bad(format!(
                "seq {}: series id {} is not in table {:?}",
                v.seq, v.series.0, seg.table
            )),
        }
        if !range.contains(v.value_time) {
            bad(format!("seq {}: value_time outside the partition", v.seq));
        }
        if v.seq == 0 || v.seq >= st.next_seq {
            bad(format!(
                "seq {} outside [1, next_seq {})",
                v.seq, st.next_seq
            ));
        }
        if st.last_system_time.is_none_or(|l| v.system_time > l) {
            bad(format!(
                "seq {}: system_time {} after the log's last_system_time",
                v.seq, v.system_time
            ));
        }
        if !seqs.insert(v.seq) {
            bad(format!("seq {} appears more than once", v.seq));
        }
        if v.deleted && v.values.iter().any(Option::is_some) {
            bad(format!("seq {}: a tombstone carries values", v.seq));
        }
        // SPEC §4.3: no two stored versions are content-identical (64-bit content hash;
        // trailing nulls are trimmed by the decoder, f64 hash bitwise).
        if let Some(other) = contents.insert(content_hash(v), v.seq) {
            bad(format!(
                "seq {} is content-identical to seq {other} (duplicate version of a point)",
                v.seq
            ));
        }
    }
    if vt != Some(stats.value_time) || sy != Some(stats.system_time) {
        bad("footer time statistics do not match the rows".into());
    }
    versions.len() as u64
}

/// `verify`.
pub fn verify(ctx: &Context) -> Res<()> {
    let store = ctx.store()?;
    let mut problems: Vec<String> = Vec::new();
    let mut notes: Vec<String> = Vec::new();
    let (mut entries, checkpoints) = log_listing(&store)?;
    // A truncated log (decision 0007): entry 1 is the truncation marker and the entries
    // below the oldest kept checkpoint are gone.
    if entries.first() == Some(&1) && read_entry(&store, 1).is_ok_and(|e| e.is_truncation_marker())
    {
        entries.remove(0);
        notes.push(format!(
            "the log is truncated (entries before {} were removed by gc)",
            entries.first().copied().unwrap_or(1)
        ));
    }
    let max = entries.last().copied().unwrap_or(0);
    let first = entries.first().copied().unwrap_or(1);
    for w in entries.windows(2) {
        if w[1] != w[0] + 1 {
            problems.push(format!(
                "log entries {}..{} are missing (later entries would be ignored)",
                w[0] + 1,
                w[1] - 1
            ));
        }
    }
    // Fold the log from the start (or from the earliest usable checkpoint whose
    // successors are all present, so that later checkpoints are checked against it).
    let mut state = State::empty();
    if first > 1 {
        let usable: Vec<u64> = checkpoints
            .iter()
            .copied()
            .filter(|&c| c + 1 >= first && c <= max)
            .collect();
        let mut loaded = false;
        for &c in &usable {
            match read_checkpoint(&store, c)
                .and_then(|cp| State::from_checkpoint(&cp).map_err(CliError::from))
            {
                Ok(s) => {
                    state = s;
                    loaded = true;
                    break;
                }
                Err(e) => problems.push(format!("checkpoint {c}: {}", e.message)),
            }
        }
        if !loaded && usable.is_empty() {
            problems.push(format!(
                "log entries 1..{} are missing and no checkpoint covers them",
                first - 1
            ));
        }
    }
    let mut folded: HashMap<u64, Checkpoint> = HashMap::new();
    let wanted: HashSet<u64> = checkpoints.iter().copied().collect();
    let mut v = state.version + 1;
    while v <= max {
        if !entries.contains(&v) {
            break;
        }
        match read_entry(&store, v).and_then(|e| state.apply(&e).map_err(CliError::from)) {
            Ok(next) => state = next,
            Err(e) => {
                problems.push(format!("log entry {v}: {}", e.message));
                break;
            }
        }
        if wanted.contains(&v) {
            folded.insert(v, normalise(state.to_checkpoint()));
        }
        v += 1;
    }
    for &c in &checkpoints {
        // The seal: checksum, and the CRC of the entry it summarises (if still present).
        match store.get(&log::checkpoint_key(c)) {
            Ok(bytes) => match Checkpoint::open(&bytes, c) {
                Err(e) => problems.push(format!("checkpoint {c}: {e}")),
                Ok(cp) if c > 0 && entries.contains(&c) => {
                    if let Ok(entry) = store.get(&log::entry_key(c))
                        && crc32c::crc32c(&entry) != cp.entry_crc32c
                    {
                        problems.push(format!(
                            "checkpoint {c} is not bound to log entry {c} (entry CRC differs)"
                        ));
                    }
                }
                Ok(_) => {}
            },
            Err(e) => problems.push(format!("reading checkpoint {c}: {e}")),
        }
        match read_checkpoint(&store, c) {
            Err(e) => problems.push(format!("checkpoint {c}: {}", e.message)),
            Ok(cp) => {
                let unique: HashSet<&String> = cp.batch_ids.iter().collect();
                if unique.len() != cp.batch_ids.len() {
                    problems.push(format!("checkpoint {c} lists a batch id twice"));
                }
                if let Err(e) = State::from_checkpoint(&cp) {
                    problems.push(format!("checkpoint {c}: {e}"));
                } else if cp.version != c {
                    problems.push(format!("checkpoint {c} says it is version {}", cp.version));
                } else if let Some(f) = folded.get(&c) {
                    if normalise(cp) != *f {
                        problems.push(format!(
                            "checkpoint {c} differs from replaying the log up to entry {c}"
                        ));
                    }
                } else if c + 1 < first {
                    notes.push(format!(
                        "checkpoint {c} predates the truncated log and is unused (gc removes it)"
                    ));
                }
            }
        }
    }
    match store.get(&log::hint_key()) {
        Ok(b) => match String::from_utf8(b)
            .ok()
            .and_then(|t| Wire::parse(&t).ok())
            .and_then(|w| {
                w.get("version")
                    .and_then(Wire::as_number)
                    .map(str::to_owned)
            })
            .and_then(|n| n.parse::<u64>().ok())
        {
            Some(h) if checkpoints.contains(&h) => {}
            _ => notes.push("the checkpoint hint is stale or unreadable (harmless)".into()),
        },
        Err(e) if e.kind() == StoreErrorKind::NotFound => {}
        Err(e) => problems.push(format!("reading the checkpoint hint: {e}")),
    }
    // The engine must open and agree with the fold.
    match ctx.open() {
        Ok(db) if db.version() != state.version => problems.push(format!(
            "the engine opens at version {} but the log replays to {}",
            db.version(),
            state.version
        )),
        Ok(db) => {
            // The batch ids the engine serves are exactly those the log records.
            let mut folded = state.to_checkpoint().batch_ids;
            folded.sort();
            match muninn_core::Database::snapshot(&db) {
                Ok(snap) if snap.batch_ids() != folded => problems
                    .push("the engine's batch ids differ from the ones recorded in the log".into()),
                Ok(_) => {}
                Err(e) => problems.push(format!("snapshot: {e}")),
            }
        }
        Err(e) => problems.push(format!(
            "the engine cannot open the database: {}",
            e.message
        )),
    }
    // Every live segment.
    let mut seqs = HashSet::new();
    let mut contents = HashMap::new();
    let mut rows = 0u64;
    let segs: Vec<_> = state.segments().cloned().collect();
    for seg in &segs {
        rows += check_segment(&store, &state, seg, &mut seqs, &mut contents, &mut problems);
    }
    // Seqs are contiguous: every seq below next_seq is stored exactly once (SPEC §4.2:
    // only stored rows consume seqs). Out-of-range and repeated seqs are reported above.
    if seqs.len() as u64 != state.next_seq.saturating_sub(1) {
        let missing: Vec<u64> = (1..state.next_seq)
            .filter(|q| !seqs.contains(q))
            .take(5)
            .collect();
        problems.push(format!(
            "{} of the seqs 1..{} are stored (missing e.g. {missing:?})",
            seqs.len(),
            state.next_seq
        ));
    }
    // Orphans (harmless; gc removes them).
    let live: HashSet<&Key> = segs.iter().map(|s| &s.key).collect();
    let retired: HashSet<&Key> = state.retired.iter().map(|(k, _)| k).collect();
    let orphans = store
        .list(log::SEGMENTS_PREFIX)
        .map_err(|e| store_err("listing segments", e))?
        .iter()
        .filter(|k| !live.contains(k) && !retired.contains(k))
        .count();
    if orphans > 0 {
        notes.push(format!(
            "{orphans} unreferenced segment file(s) (left by failed writes; `muninn gc` removes them)"
        ));
    }
    for n in &notes {
        output::line(&format!("note: {n}"))?;
    }
    for p in &problems {
        output::line(&format!("problem: {p}"))?;
    }
    if problems.is_empty() {
        output::line(&format!(
            "ok: log version {}, {} entries, {} checkpoint(s), {} segment(s), {} version(s)",
            state.version,
            entries.len(),
            checkpoints.len(),
            segs.len(),
            rows
        ))
    } else {
        Err(CliError::new(format!(
            "verify found {} problem(s)",
            problems.len()
        )))
    }
}
