"""Fast but wrong doesn't count: at tiny scale every system's answer to every workload
query must equal the reference implementation's, row for row (SPEC §9 comparison rules).

    uv run --project bench pytest
    MUNINN_BENCH_SYSTEMS=duckdb-native,clickhouse uv run --project bench pytest

Systems whose runtime is unavailable (e.g. no ClickHouse binary) are skipped, not failed.
"""

from __future__ import annotations

import json
import os

import pytest

from competitors import registry
from competitors.compare import diff
from datagen.datasets import DATASETS
from queries.params import load_workload

# fixtures tiny / oracle / expected: conftest.py

SYSTEMS = os.environ.get("MUNINN_BENCH_SYSTEMS",
                         "muninn,duckdb-native,duckdb-parquet,duckdb-native-window,clickhouse").split(",")


def test_workload_covers_every_dataset_and_op():
    ws = load_workload()
    assert {w["dataset"] for w in ws} == set(DATASETS)
    assert {w["op"] for w in ws} == {"latest", "history", "replay", "grid", "asof_join"}


def test_oracle_results_are_not_trivial(expected):
    """Guard against a workload that only compares empty results."""
    by_id = {}
    for (_, qid, _), (_, res) in expected.items():
        if not qid.startswith("edge_"):
            by_id[qid] = by_id.get(qid, 0) + res.num_rows
    empty = [k for k, n in by_id.items() if n == 0]
    assert not empty, f"queries with only empty results at tiny scale: {empty}"


@pytest.mark.parametrize("system", SYSTEMS)
@pytest.mark.parametrize("dataset", list(DATASETS))
def test_system_matches_reference(system, dataset, tiny, expected, tmp_path):
    out, cats = tiny
    try:
        a = registry.make(system, tmp_path / system, threads=2, memory_limit_mb=1024)
        a.load(out / dataset, cats[dataset])
    except registry.Unavailable as e:
        pytest.skip(str(e))
    failures = []
    try:
        for (ds, qid, inst), (q, exp) in expected.items():
            if ds != dataset:
                continue
            got = a.run(q)
            d = diff(exp, got)
            if d:
                failures.append(f"{qid}#{inst}: {d}\n  query: {json.dumps(_short(q))}")
    finally:
        a.close()
    assert not failures, f"{len(failures)} mismatches:\n" + "\n".join(failures[:5])


def _short(q):
    q = dict(q)
    if "left" in q:
        q["left"] = f"<{len(q['left']['series'])} rows>"
    if "frontiers" in q:
        q["frontiers"] = f"<{len(q['frontiers'])} frontiers {q['frontiers'][:2]}...>"
    return q
