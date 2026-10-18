//! Opening databases, the writer clock, and reading the engine state straight from the
//! log (for `info` and `verify`).

use std::path::{Path, PathBuf};

use muninn_core::Timestamp;
use muninn_engine::log::{self, Checkpoint, LogEntry};
use muninn_engine::state::State;
use muninn_engine::{Engine, EngineOptions};
use muninn_store::{Key, LocalFsStore, Store};

use crate::{CliError, Res};

/// A database engine on a local directory.
pub type Db = Engine<LocalFsStore>;

/// Marker object written by `init`.
pub const MARKER: &str = "muninn.json";

/// Global options shared by every command.
pub struct Context {
    db: Option<PathBuf>,
    clock: Option<Timestamp>,
}

impl Context {
    /// Builds the context from the global flags.
    pub fn new(db: Option<PathBuf>, writer_clock: Option<String>) -> Res<Context> {
        let clock = writer_clock
            .map(|s| parse_ts(&s, "--writer-clock"))
            .transpose()?;
        Ok(Context { db, clock })
    }

    /// The writer clock reading: `--writer-clock`, else the system clock.
    pub fn now(&self) -> Timestamp {
        self.clock.unwrap_or_else(|| {
            Timestamp::from_micros(muninn_engine::system_wall_clock()).unwrap_or(Timestamp::MAX)
        })
    }

    /// The database directory (`--db` / `MUNINN_DB`).
    pub fn dir(&self) -> Res<&Path> {
        self.db.as_deref().ok_or_else(|| {
            CliError::new("no database given: pass --db <dir> or set MUNINN_DB".to_owned())
        })
    }

    /// The store of an existing database.
    pub fn store(&self) -> Res<LocalFsStore> {
        open_store(self.dir()?)
    }

    /// Opens the existing database.
    pub fn open(&self) -> Res<Db> {
        self.open_with(EngineOptions::default())
    }

    /// Opens the existing database with engine options.
    pub fn open_with(&self, opts: EngineOptions) -> Res<Db> {
        Ok(Engine::open(self.store()?, opts)?)
    }
}

/// Parses a timestamp flag (SPEC §1 text form).
pub fn parse_ts(s: &str, what: &str) -> Res<Timestamp> {
    Timestamp::parse(s).map_err(|e| CliError::new(format!("invalid_timestamp: {what}: {e}")))
}

/// Parses an optional timestamp flag.
pub fn opt_ts(s: &Option<String>, what: &str) -> Res<Option<Timestamp>> {
    s.as_deref().map(|s| parse_ts(s, what)).transpose()
}

fn is_empty_dir(dir: &Path) -> Res<bool> {
    Ok(std::fs::read_dir(dir)?.next().is_none())
}

/// Creates a new database directory (it must not exist, or be empty).
pub fn init(dir: &Path) -> Res<LocalFsStore> {
    if dir.exists() && !(dir.is_dir() && is_empty_dir(dir)?) {
        return Err(CliError::new(format!(
            "{} already exists and is not an empty directory",
            dir.display()
        )));
    }
    let store = LocalFsStore::open(dir).map_err(|e| CliError::new(format!("io: {e}")))?;
    store
        .put(
            &Key::new(MARKER).expect("valid key"),
            b"{\"muninn\": \"database\", \"format\": 1}\n",
        )
        .map_err(|e| CliError::new(format!("io: {e}")))?;
    Ok(store)
}

/// Opens the store of an existing database directory.
pub fn open_store(dir: &Path) -> Res<LocalFsStore> {
    if !dir.join(MARKER).is_file() && !dir.join("log").is_dir() {
        return Err(CliError::new(format!(
            "{} is not a muninn database (create one with `muninn init {}`)",
            dir.display(),
            dir.display()
        )));
    }
    LocalFsStore::open(dir).map_err(|e| CliError::new(format!("io: {e}")))
}

/// Maps a store error.
pub fn store_err(what: &str, e: muninn_store::StoreError) -> CliError {
    CliError::new(format!("io: {what}: {e}"))
}

/// Log entry versions present on the store, and checkpoint versions.
pub fn log_listing<S: Store>(store: &S) -> Res<(Vec<u64>, Vec<u64>)> {
    let keys = store
        .list("log/")
        .map_err(|e| store_err("listing log", e))?;
    let mut entries = Vec::new();
    let mut checkpoints = Vec::new();
    for k in keys {
        let k = k.as_str();
        if let Some(v) = log::parse_checkpoint_key(k) {
            checkpoints.push(v);
        } else if let Some(d) = k.strip_prefix("log/").and_then(|r| r.strip_suffix(".json")) {
            if d.len() == 20 && d.bytes().all(|b| b.is_ascii_digit()) {
                entries.push(d.parse().expect("digits"));
            }
        }
    }
    entries.sort_unstable();
    checkpoints.sort_unstable();
    Ok((entries, checkpoints))
}

/// Reads and parses log entry `v`.
pub fn read_entry<S: Store>(store: &S, v: u64) -> Res<LogEntry> {
    let bytes = store
        .get(&log::entry_key(v))
        .map_err(|e| store_err(&format!("reading log entry {v}"), e))?;
    Ok(log::from_json(&bytes, &format!("log entry {v}"))?)
}

/// Reads and parses checkpoint `v`.
pub fn read_checkpoint<S: Store>(store: &S, v: u64) -> Res<Checkpoint> {
    let bytes = store
        .get(&log::checkpoint_key(v))
        .map_err(|e| store_err(&format!("reading checkpoint {v}"), e))?;
    Ok(log::from_json(&bytes, &format!("checkpoint {v}"))?)
}

/// The current state, rebuilt like the engine does: newest checkpoint + later entries.
pub fn load_state<S: Store>(store: &S) -> Res<State> {
    let (_, checkpoints) = log_listing(store)?;
    let mut state = match checkpoints.last() {
        Some(&v) => State::from_checkpoint(&read_checkpoint(store, v)?)?,
        None => State::empty(),
    };
    loop {
        let v = state.version + 1;
        match store.get(&log::entry_key(v)) {
            Ok(bytes) => {
                let e: LogEntry = log::from_json(&bytes, &format!("log entry {v}"))?;
                state = state.apply(&e)?;
            }
            Err(e) if e.kind() == muninn_store::StoreErrorKind::NotFound => return Ok(state),
            Err(e) => return Err(store_err(&format!("reading log entry {v}"), e)),
        }
    }
}
