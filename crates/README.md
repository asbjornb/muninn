# Rust crates

| Crate | What it is |
| --- | --- |
| [`muninn-core`](muninn-core) | Shared vocabulary and the in-memory model. `time` (µs-UTC `Timestamp` with RFC 3339 text form and own civil-date math, `Duration`), `types` (column types, values with bitwise identity, catalog definitions, `Version`, clocks, visibility and knowledge-order functions, ranges), `error` (one kind per SPEC §8 code plus engine-only `Io`/`Conflict`/`Internal`), `api` (request/response types, the `Reader` and `Database` traits), `model::ModelDb` — a naive, obviously-correct oracle of `spec/SPEC.md` — and `wire`, which runs JSON requests (SPEC §9 shapes) against any `Database` with the §10 structure checks and check order (dependency-free JSON parser included). No dependencies. |
| [`muninn-store`](muninn-store) | The sync object-store interface of decision 0005 (`Store`: `get`, `get_range`, `head`, `put`, `put_if_absent`, `list`, `delete`) with `MemoryStore`, `LocalFsStore` (atomic temp+fsync+rename/link) and `FaultyStore` (seeded fail-before / ambiguous fail-after / crash-at-op-N). `conformance` is a backend test suite every store must pass. No dependencies. |
| [`muninn-conformance`](muninn-conformance) | Runs `spec/scenarios/**/*.json` (SPEC §9) against any `Database` via `run_scenario` / `run_corpus`, passing each request loose through `muninn_core::wire`. No runtime dependencies. |
| [`muninn-format`](muninn-format) | Immutable segment files, specified byte for byte in [`spec/FORMAT.md`](../spec/FORMAT.md) (formats 1 and 2, format 2 written): one table partition per file, rows sorted by `(series, value_time, received_time, system_time, seq)` in blocks of column chunks (blocks end at series boundaries once they hold 1 024 rows; nibble-length delta integers, run-length / delta / delta-of-delta varints, bitmaps, plain f64, length-prefixed strings, optional zstd), CRC-32C per chunk and footer, footer with column directory, time/seq/series/column statistics and per-series row ranges. Opens from one ranged read of the tail; decoding is total (corruption is an error, never a panic or a wrong row) and reuses per-thread buffers. Depends on `crc32c`, `zstd`. |
| [`muninn-engine`](muninn-engine) | `Engine`: `Database` over any `Store`. Commit log of JSON entries (`put_if_absent`) with size-triggered checkpoints and a `_last_checkpoint` hint, truncated by `gc` below the older of the two newest checkpoints ([ADR 0007](../decisions/0007-log-truncation-and-batch-catalog.md)); batch series creation; the fast write path of [ADR 0009](../decisions/0009-fast-write-path.md) — group commit (`write` from any number of threads, `write_many`), each commit one log entry holding its rows as inline segments (the memtable, read like segment files), flushed into segment files by a background thread or `flush`; large writes go to segment files directly; dedup that rules out fresh rows by the received-time bounds of their partition, then by an in-memory index of each point's stored received-time range (stored versions are compared only for re-sends and out-of-order rows); snapshot reads with partition / segment / block / series-slice pruning and streaming `latest`/`grid` reductions; `history_lead` (lead-time filter pushdown); `compact` (by knowledge-time period, [ADR 0008](../decisions/0008-read-path-layout.md)), `compact_all`, `gc`; `restore` (import versions with their original `system_time`/`seq`, for dump/restore) and per-snapshot catalog log / batch ids / high-water marks. Tested with the scenario corpus (memory, local fs, reopen-before-read, flush-and-compact-before-read, files only, flush per write), differential fuzzing against `ModelDb` (writes, group commits, flushes, reads, compaction, GC, reopen), crash-at-every-op and ambiguous-failure injection, concurrent writers against the model, and acknowledged-write survival at every crash point. `examples/ingest.rs` benchmarks small writes from concurrent producers and bulk loads. Depends on `serde`, `serde_json`. |
| [`muninn-py`](muninn-py) | Python bindings (PyO3 + maturin, abi3 for Python ≥ 3.10): `muninn.open` / `memory`, catalog, writes from pyarrow / pandas / polars / dicts, the five reads returning `pyarrow.Table` with SPEC §11 receipts in the schema metadata, snapshots, compaction, GC, a `MuninnError` hierarchy keyed by spec code. Arrow via the PyCapsule interface (`arrow-pyarrow`, `arrow-array`, `arrow-schema` only). Tested with the scenario corpus and a hypothesis differential test against `spec/reference`. See its [README](muninn-py/README.md). |
| [`muninn-cli`](muninn-cli) | The `muninn` binary over a local directory: catalog commands, CSV/NDJSON import with line-numbered errors and resumable chunking, every SPEC §6 read with csv/ndjson/table output, `query` (SPEC §9 step JSON via `wire`), `compact`/`gc`/`info`/`verify`, and `dump`/`restore` ([`spec/DUMP.md`](../spec/DUMP.md)). Guide: [`docs/cli.md`](../docs/cli.md). Depends on `clap`, `csv`, `crc32c`. |

Spec ambiguities found while implementing are listed in [SPEC-QUESTIONS.md](SPEC-QUESTIONS.md).

## Working on it

```sh
task test   # cargo test --workspace
task lint   # rustfmt --check + clippy -D warnings
task fmt    # cargo fmt
task fuzz   # heavy differential + crash fuzzing (MUNINN_FUZZ_CASES, release build)
task py-test # build the Python bindings (uv + maturin) and run their pytest suite
```

The engine's property tests use small default case counts so `task test` stays fast; set
`MUNINN_FUZZ_CASES` (e.g. `5000`) for longer campaigns. `cargo test --release -p
muninn-engine --test engine -- --ignored perf_smoke --nocapture` prints rough timings;
`--test perf -- --ignored --nocapture` (engine: write/dedup cost against history, series
creation, reads) and `cargo test --release -p muninn-format --test perf -- --ignored
--nocapture` with `MUNINN_PERF_SEGMENTS=<segments dir>` (decode cost per row) are probes.

Rules: no `unsafe` (`unsafe_code = "forbid"` workspace-wide), doc comments on public
items (`missing_docs`), lean dependency trees (no async runtime, no Arrow in these
crates; only the `muninn-py` bindings use Arrow, and only its array/schema crates).
