# Rust crates

| Crate | What it is |
| --- | --- |
| [`muninn-core`](muninn-core) | Shared vocabulary and the in-memory model. `time` (µs-UTC `Timestamp` with RFC 3339 text form and own civil-date math, `Duration`), `types` (column types, values with bitwise identity, catalog definitions, `Version`, clocks, visibility and knowledge-order functions, ranges), `error` (one kind per SPEC §8 code plus engine-only `Io`/`Conflict`/`Internal`), `api` (request/response types, the `Reader` and `Database` traits), `model::ModelDb` — a naive, obviously-correct oracle of `spec/SPEC.md` — and `wire`, which runs JSON requests (SPEC §9 shapes) against any `Database` with the §10 structure checks and check order (dependency-free JSON parser included). No dependencies. |
| [`muninn-store`](muninn-store) | The sync object-store interface of decision 0005 (`Store`: `get`, `get_range`, `head`, `put`, `put_if_absent`, `list`, `delete`) with `MemoryStore`, `LocalFsStore` (atomic temp+fsync+rename/link) and `FaultyStore` (seeded fail-before / ambiguous fail-after / crash-at-op-N). `conformance` is a backend test suite every store must pass. No dependencies. |
| [`muninn-conformance`](muninn-conformance) | Runs `spec/scenarios/**/*.json` (SPEC §9) against any `Database` via `run_scenario` / `run_corpus`, passing each request loose through `muninn_core::wire`. No runtime dependencies. |

| [`muninn-format`](muninn-format) | Immutable segment files, specified byte for byte in [`spec/FORMAT.md`](../spec/FORMAT.md): one table partition per file, rows sorted by `(series, value_time, received_time, system_time, seq)` in blocks of column chunks (run-length / delta / delta-of-delta varints, bitmaps, plain f64, length-prefixed strings, optional zstd), CRC-32C per chunk and footer, footer with column directory, time/seq/series/column statistics and per-series row ranges. Opens from one ranged read of the tail; decoding is total (corruption is an error, never a panic or a wrong row). Depends on `crc32c`, `zstd`. |
| [`muninn-engine`](muninn-engine) | `Engine`: `Database` over any `Store`. Commit log of JSON entries (`put_if_absent`) with checkpoints and a `_last_checkpoint` hint; one segment per touched table partition per write; dedup against stored versions; snapshot reads with partition / segment / block / series-slice pruning and a single-pass `grid`; `compact`, `compact_all`, `gc`. Tested with the scenario corpus (memory, local fs, reopen-before-read, compact-before-read), differential fuzzing against `ModelDb`, crash-at-every-op and ambiguous-failure injection. Depends on `serde`, `serde_json`. |
| [`muninn-py`](muninn-py) | Python bindings (PyO3 + maturin, abi3 for Python ≥ 3.10): `muninn.open` / `memory`, catalog, writes from pyarrow / pandas / polars / dicts, the five reads returning `pyarrow.Table` with SPEC §11 receipts in the schema metadata, snapshots, compaction, GC, a `MuninnError` hierarchy keyed by spec code. Arrow via the PyCapsule interface (`arrow-pyarrow`, `arrow-array`, `arrow-schema` only). Tested with the scenario corpus and a hypothesis differential test against `spec/reference`. See its [README](muninn-py/README.md). |

Planned (decision 0001): `muninn-cli`.

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
muninn-engine --test engine -- --ignored perf_smoke --nocapture` prints rough timings.

Rules: no `unsafe` (`unsafe_code = "forbid"` workspace-wide), doc comments on public
items (`missing_docs`), lean dependency trees (no async runtime, no Arrow in these
crates; only the `muninn-py` bindings use Arrow, and only its array/schema crates).
