# 0005: Synchronous engine over a small own Store trait

- Status: accepted
- Date: 2026-10-18

## Context
The `object_store` crate is async and heavy. The engine is single-writer and scan-bound;
deterministic simulation testing wants full control over I/O and failures.

## Decision
`muninn-store` defines a tiny sync trait: `get`, `get_range`, `put` (overwrite),
`put_if_absent`, `list(prefix)`, `delete`, `head`. Backends: in-memory, local filesystem
(fsync + atomic rename/link for put-if-absent), and a deterministic fault-injecting
wrapper. S3/Azure/GCS come later through an adapter (likely over `object_store`).
Parallelism, where needed, is threads (rayon-style), not async.

## Consequences
Simple to test and reason about; cloud backends are a contained adapter.
