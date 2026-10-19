//! The JSON scenario corpus (`spec/scenarios`) against the engine: on `MemoryStore`, on
//! `LocalFsStore`, reopening the engine before every read step, and compacting everything
//! before every read step.

use std::sync::{Arc, Mutex};

use muninn_conformance::{run_corpus, spec_scenarios_dir};
use muninn_core::api::*;
use muninn_core::types::{SeriesDef, TableDef};
use muninn_core::{Database, Reader, Result};
use muninn_engine::{Engine, EngineOptions, EngineSnapshot, WriteOptions};
use muninn_store::{LocalFsStore, MemoryStore};

fn small() -> EngineOptions {
    EngineOptions {
        checkpoint_interval: 3,
        segment: WriteOptions {
            block_rows: 2,
            series_block_rows: 1,
            compress: true,
            zstd_level: 1,
            format_version: 1,
        },
        gc_grace: std::time::Duration::ZERO,
        ..EngineOptions::default()
    }
}

#[test]
fn corpus_on_memory_store() {
    let report = run_corpus(&spec_scenarios_dir(), || {
        Engine::open(MemoryStore::new(), EngineOptions::default()).unwrap()
    });
    eprintln!("{report}");
    report.assert_ok();
}

#[test]
fn corpus_on_local_fs() {
    let dirs = Mutex::new(Vec::new());
    let report = run_corpus(&spec_scenarios_dir(), || {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::open(dir.path()).unwrap();
        dirs.lock().unwrap().push(dir);
        Engine::open(store, small()).unwrap()
    });
    eprintln!("{report}");
    report.assert_ok();
}

#[derive(Copy, Clone)]
enum Hook {
    Reopen,
    CompactAll,
}

/// A database that reopens the engine (or compacts everything, then collects garbage)
/// before every read.
struct Hooked {
    store: Arc<MemoryStore>,
    engine: Mutex<Arc<Engine<Arc<MemoryStore>>>>,
    hook: Hook,
}

impl Hooked {
    fn new(hook: Hook) -> Hooked {
        let store = Arc::new(MemoryStore::new());
        let engine = Engine::open(Arc::clone(&store), small()).unwrap();
        Hooked {
            store,
            engine: Mutex::new(Arc::new(engine)),
            hook,
        }
    }

    fn engine(&self) -> Arc<Engine<Arc<MemoryStore>>> {
        Arc::clone(&self.engine.lock().unwrap())
    }

    fn before_read(&self) -> Arc<Engine<Arc<MemoryStore>>> {
        match self.hook {
            Hook::Reopen => {
                let e = Arc::new(Engine::open(Arc::clone(&self.store), small()).unwrap());
                *self.engine.lock().unwrap() = Arc::clone(&e);
                e
            }
            Hook::CompactAll => {
                let e = self.engine();
                e.compact_all().unwrap();
                e.gc().unwrap();
                e
            }
        }
    }
}

impl Reader for Hooked {
    fn table(&self, name: &str) -> Result<TableDef> {
        self.engine().table(name)
    }
    fn tables(&self) -> Result<Vec<TableDef>> {
        self.engine().tables()
    }
    fn series(&self, name: &str) -> Result<SeriesDef> {
        self.engine().series(name)
    }
    fn table_series(&self, table: &str) -> Result<Vec<SeriesDef>> {
        self.engine().table_series(table)
    }
    fn latest(&self, req: &LatestRequest) -> Result<ReadResult<VersionRow>> {
        self.before_read().latest(req)
    }
    fn history(&self, req: &HistoryRequest) -> Result<ReadResult<VersionRow>> {
        self.before_read().history(req)
    }
    fn replay(&self, req: &ReplayRequest) -> Result<ReadResult<VersionRow>> {
        self.before_read().replay(req)
    }
    fn grid(&self, req: &GridRequest) -> Result<ReadResult<GridRow>> {
        self.before_read().grid(req)
    }
    fn asof_join(&self, req: &AsofJoinRequest) -> Result<ReadResult<AsofRow>> {
        self.before_read().asof_join(req)
    }
}

impl Database for Hooked {
    type Snapshot = EngineSnapshot<Arc<MemoryStore>>;
    fn snapshot(&self) -> Result<Self::Snapshot> {
        self.engine().snapshot()
    }
    fn create_table(&self, req: &CreateTable) -> Result<TableDef> {
        self.engine().create_table(req)
    }
    fn add_column(&self, req: &AddColumn) -> Result<TableDef> {
        self.engine().add_column(req)
    }
    fn create_series(&self, req: &CreateSeries) -> Result<SeriesDef> {
        self.engine().create_series(req)
    }
    fn create_series_batch(&self, reqs: &[CreateSeries]) -> Result<Vec<SeriesDef>> {
        self.engine().create_series_batch(reqs)
    }
    fn write(&self, batch: &WriteBatch) -> Result<WriteResult> {
        self.engine().write(batch)
    }
}

#[test]
fn corpus_reopening_before_every_read() {
    let report = run_corpus(&spec_scenarios_dir(), || Hooked::new(Hook::Reopen));
    eprintln!("{report}");
    report.assert_ok();
}

#[test]
fn corpus_compacting_before_every_read() {
    let report = run_corpus(&spec_scenarios_dir(), || Hooked::new(Hook::CompactAll));
    eprintln!("{report}");
    report.assert_ok();
}
