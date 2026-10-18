# 0002: Timestamps are i64 microseconds UTC

- Status: accepted
- Date: 2026-10-18

## Context
ns (kdb, Arrow default) vs µs. Energy/market data has no use for sub-µs precision.
i64 ns only spans 1677–2262; forward curves and long-dated contracts make range matter
more than precision.

## Decision
All engine timestamps are i64 microseconds since the Unix epoch, UTC. Valid range is
years 0001–9999. Arrow output uses `timestamp[us, UTC]`.

## Consequences
Lossless conversion to/from Arrow `us`, pandas (ns, for the 1677–2262 window), Parquet.
Ingesting ns data truncates; that's acceptable for this domain.
