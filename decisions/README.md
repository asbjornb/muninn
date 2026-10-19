# Decisions

Lightweight decision records. One file per decision: `NNNN-short-title.md`.

```markdown
# NNNN: Title

- Status: proposed | accepted | superseded by NNNN
- Date: YYYY-MM-DD

## Context
## Decision
## Consequences
```

| # | Decision |
| --- | --- |
| [0001](0001-rust-core-python-reference.md) | Rust engine, Python reference spec, one repo |
| [0002](0002-microsecond-timestamps.md) | Timestamps are i64 microseconds UTC |
| [0003](0003-visibility-order-and-dedup.md) | Frontier clock orders versions; dedup on full content |
| [0004](0004-commit-log-no-separate-wal.md) | Commit log on the store; no separate WAL in v1 |
| [0005](0005-sync-engine-own-store-trait.md) | Synchronous engine over a small own Store trait |
| [0006](0006-durability-and-gc-safety.md) | Durability contract and GC safety |
| [0007](0007-log-truncation-and-batch-catalog.md) | Log truncation, size-triggered checkpoints, batch series creation |
| [0008](0008-read-path-layout.md) | Read path: knowledge-time compaction, series-aligned blocks, nibble encoding (segment format 2) |
