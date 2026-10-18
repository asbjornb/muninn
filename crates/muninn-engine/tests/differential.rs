//! Differential fuzzing: random workloads against the engine and `ModelDb` in lockstep
//! (every result and error must be identical), with compaction, reopen, GC and pinned
//! snapshots interleaved, on tiny blocks and day partitions so that many segments and
//! blocks exist. Case counts scale with `MUNINN_FUZZ_CASES`.

mod common;

use std::sync::Arc;

use common::*;
use muninn_store::{LocalFsStore, MemoryStore};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(160), max_shrink_iters: 2000, ..ProptestConfig::default() })]

    #[test]
    fn engine_matches_model(
        ops in arb_ops(40),
        block_rows in 1usize..5,
        checkpoint in 1u64..6,
        compress in any::<bool>(),
    ) {
        run_differential(Arc::new(MemoryStore::new()), test_options(block_rows, checkpoint, compress), &ops)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(160) / 8 + 1, ..ProptestConfig::default() })]

    /// The same on a real filesystem.
    #[test]
    fn engine_matches_model_on_local_fs(ops in arb_ops(30), block_rows in 1usize..4) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsStore::open(dir.path()).unwrap());
        run_differential(store, test_options(block_rows, 3, true), &ops)?;
    }
}
