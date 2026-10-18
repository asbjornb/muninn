# Generated scenarios

`NNNN-random.json` are randomised conformance scenarios in exactly the §9 format of
`spec/SPEC.md`, with expectations computed by the Python reference implementation
(`spec/reference/muninn_ref`). They are the cross-implementation differential test: every
implementation must reproduce them exactly.

- Seeds 1–150, one file per seed; `name` is `random-<seed>`.
- Deterministic: `generate.py` uses only `random.Random(seed)`; regenerating gives
  byte-identical files.
- Expectations always use full `rows` (no `values` sugar), plus `error` and full write
  results.
- f64 values are emitted with Python's shortest round-trip repr (parsing to the nearest
  f64 gives back the exact value); non-finite values are the strings `"NaN"`, `"inf"`,
  `"-inf"`; i64 values beyond ±2^53 are decimal strings; timestamps are canonical §1 text.
- Inputs include equivalent spellings on purpose (`42` vs `42.0` in f64 columns, a
  `+01:00` timestamp equal to a `Z` one, `"7"` vs `7` in i64 columns).
- Every scenario ends with an unfiltered `history` of each table, i.e. the full stored
  state (all versions with their system_time and seq).
- Read and catalog steps contain at most one fault, so these files never depend on the
  order of argument checks that `spec/AMBIGUITIES.md` A7/A8 leave open. Write batches may
  contain several faults: their order is normative (§4.1).

## Regenerate

From the repository root:

```sh
python3 spec/reference/generate.py                     # rewrite seeds 1..150 here
python3 spec/reference/run_scenarios.py spec/scenarios # check everything
```

Other options: `--seed N --print` (one scenario to stdout), `--count`, `--start`,
`--out DIR` (e.g. a large throwaway batch for a longer fuzz run).

Regenerate whenever the reference or the generator changes, and commit the result with
the change that caused it. If a regenerated file differs, the diff is a semantic change:
review it like one.
