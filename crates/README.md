# Rust crates

| Crate | What it is |
| --- | --- |
| [`muninn-core`](muninn-core) | Shared vocabulary and the in-memory model. `time` (µs-UTC `Timestamp` with RFC 3339 text form and own civil-date math, `Duration`), `types` (column types, values with bitwise identity, catalog definitions, `Version`, clocks, visibility and knowledge-order functions, ranges), `error` (one kind per SPEC §8 code plus engine-only `Io`/`Conflict`/`Internal`), `api` (request/response types, the `Reader` and `Database` traits), and `model::ModelDb` — a naive, obviously-correct oracle of `spec/SPEC.md`. No dependencies. |
| [`muninn-store`](muninn-store) | The sync object-store interface of decision 0005 (`Store`: `get`, `get_range`, `head`, `put`, `put_if_absent`, `list`, `delete`) with `MemoryStore`, `LocalFsStore` (atomic temp+fsync+rename/link) and `FaultyStore` (seeded fail-before / ambiguous fail-after / crash-at-op-N). `conformance` is a backend test suite every store must pass. No dependencies. |
| [`muninn-conformance`](muninn-conformance) | Runs `spec/scenarios/**/*.json` (SPEC §9) against any `Database` via `run_scenario` / `run_corpus`. Depends on `serde_json`. |

Planned (decision 0001): `muninn-format` (segment files), `muninn-engine` (writer, commit
log, reads, compaction — must pass `muninn-conformance` and be differentially tested
against `ModelDb`), `muninn-cli`.

Spec ambiguities found while implementing are listed in [SPEC-QUESTIONS.md](SPEC-QUESTIONS.md).

## Working on it

```sh
task test   # cargo test --workspace
task lint   # rustfmt --check + clippy -D warnings
task fmt    # cargo fmt
```

Rules: no `unsafe` (`unsafe_code = "forbid"` workspace-wide), doc comments on public
items (`missing_docs`), lean dependency trees (no async runtime, no Arrow in these
crates).
