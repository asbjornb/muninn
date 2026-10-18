# 0001: Rust engine, Python reference spec, one repo

- Status: accepted
- Date: 2026-10-18

## Context
Roadmap step 0 asks for an executable spec before the engine. Language was open
(Rust vs C#). Python bindings matter most for backtesting; Arrow/Parquet ecosystem is
strongest in Rust.

## Decision
- `spec/` — normative `SPEC.md`, language-neutral JSON conformance scenarios, and a small
  pure-Python reference implementation (`spec/reference/`) that is deliberately naive and
  readable. It is the oracle.
- `crates/` — Rust workspace: `muninn-core` (types + an in-memory model of the spec),
  `muninn-store` (storage interface + backends), `muninn-format` (segment files),
  `muninn-engine` (writer, commit log, reads, compaction), `muninn-conformance`
  (runs every scenario against every implementation), `muninn-cli`.
- Python bindings later via PyO3/maturin; .NET via a server (Flight/HTTP) later.
- Two independent implementations of the semantics (Python reference, Rust model) are
  cross-checked with generated scenarios: the Python side generates random scenarios with
  expected results, the Rust side must reproduce them.

## Consequences
Every engine change is checked against an oracle written in a different language by a
different author. The spec, not the engine, is the source of truth.
