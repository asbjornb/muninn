# Ambition

Strong AI makes code cheap; it doesn't make correctness, trust or a sharp design cheap.
So aim the ambition at **depth, not breadth**: a small core that is rigorous, a few workloads
we win by an order of magnitude, and a semantic model others can adopt. Goal 5 in the vision
("small enough to understand") still holds — feature sprawl is the main risk.

Many of these can be spiked in parallel.

## Performance: 10× on some metrics, against some competitors

Pick workloads where a purpose-built design should win big, and aim for ~10×, not 2×.
Measured by `09-benchmarks.md`.

- **Vintage-delta compression.** Consecutive versions of the same point sit next to each
  other in the sort order and differ slightly. Encode each version as a residual against
  the previous one (predictive coding, like video P-frames), then use modern float codecs
  (ALP, Chimp, FastLanes-style bit-packing) rather than Gorilla. Forecast datasets look
  like the best candidate for a headline number.
- **Single-pass frontier grid.** "For each of 365 decision times, latest known for the next
  36 h" in one sorted scan instead of 365 queries — resolve many frontiers per version
  chain at once. The quant's training-set query.
- **Checkpoints + deltas** (keyframes): derived "state as of F" snapshots, keyed by
  system time so backfills invalidate them correctly. Never change results.
- **Massive parallel backtesting.** Data is immutable, so a parameter sweep can fan out over
  hundreds of stateless readers against the same bucket.

## Spike: the format + spec as the product

Iceberg won through a spec, not an engine. Could Muninn be *the* open standard for
point-in-time time series?

- A documented on-disk format + semantics spec, with the step-0 scenarios as a
  **conformance suite**.
- Multiple readers: our engine, a DuckDB extension, a plain Python reader.
- Long shot: data vendors delivering vintaged data in this format, instead of every firm
  reconstructing vintages from APIs.

Spike questions: does a spec-first format constrain the engine too much (e.g. custom
encodings)? Would a DuckDB extension reading it be enough for most users? Who would adopt it?

## Later: provably leak-free

Not first, but worth doing once the semantics settle.

- **Leakage as information flow.** Noninterference from security research: a result as of
  frontier F must not change when anything known after F changes. Testable as a fuzzer:
  perturb/add/delete "future" data, assert results are bit-identical.
- **Formal methods** where they pay: visibility semantics in a small formal spec
  (Lean / Quint / TLA+); model-check the commit protocol (manifest CAS, writer lease,
  crash recovery).
- **Reproducibility receipts**: every result carries snapshot id + query + frontier hash, so
  it can be re-run bit for bit later. Useful for audit/compliance, not only quants.
- **Explain-the-diff**: "why did this backtest change between runs?" → the versions that
  differ, with provenance.

## Pitch: safe for AI agents

Agents doing quant research are notorious for lookahead bias (subtly wrong window
queries, fantastic Sharpe ratios). If the default semantics can't leak, every query an
agent writes is leak-free by construction. Probably little to build beyond a good
catalog with descriptions and maybe an MCP server — mostly a consequence of getting the
model and semantics right, and good marketing.

## Parked

- **Visibility as bounds** instead of a single time: store earliest/latest plausible
  visibility per version, run backtests optimistic / pessimistic / as a latency sweep, and
  learn bounds per source. Distinctive, but not convinced yet — revisit after real data in
  step 0.
