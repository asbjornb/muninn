"""The ingest benchmark's load paths (``Adapter.ingest``, bench/ingest.py) must load exactly
the dataset: after each mode, every workload and edge query matches the reference.

Muninn's bulk mode cuts writes regardless of the original batches, so its system_time
values differ by design: there, queries that use system_time (a system frontier or the
system clock) are skipped and the column is compared without it.
"""

from __future__ import annotations

import os

import pytest

from competitors import registry
from competitors.base import INGEST_MODES
from competitors.compare import diff
from datagen.datasets import DATASETS

SYSTEMS = [s for s in os.environ.get("MUNINN_BENCH_SYSTEMS",
                                     "muninn,duckdb-native,clickhouse").split(",")
           if s in ("muninn", "duckdb-native", "clickhouse", "clickhouse-fsync")]


@pytest.mark.parametrize("system", SYSTEMS)
@pytest.mark.parametrize("mode", INGEST_MODES)
@pytest.mark.parametrize("dataset", list(DATASETS))
def test_ingest_mode_matches_reference(system, mode, dataset, tiny, expected, tmp_path):
    out, cats = tiny
    cat = cats[dataset]
    try:
        a = registry.make(system, tmp_path / system, threads=2, memory_limit_mb=1024)
        r = a.ingest(out / dataset, cat, mode)
    except registry.Unavailable as e:
        pytest.skip(str(e))
    inexact = system == "muninn" and mode == "bulk"
    failures = []
    try:
        assert r["complete"] and r["rows"] == r["rows_total"] == cat["stats"]["versions"]
        if mode == "per-batch":
            assert r["writes"] == r["batches_total"]
        for (ds, qid, inst), (q, exp) in expected.items():
            if ds != dataset:
                continue
            if inexact and (q.get("system") is not None or q.get("clock") == "system"):
                continue
            got = a.run(q)
            if inexact and "system_time" in exp.column_names:
                exp, got = exp.drop_columns(["system_time"]), got.drop_columns(["system_time"])
            d = diff(exp, got)
            if d:
                failures.append(f"{qid}#{inst}: {d}")
    finally:
        a.close()
    assert not failures, f"{len(failures)} mismatches:\n" + "\n".join(failures[:5])


def _fast_write_path() -> bool:
    try:
        import muninn
    except ImportError:
        return False
    return hasattr(muninn.Database, "write_many")


@pytest.mark.parametrize("dataset", list(DATASETS))
def test_write_many_groups_match_reference(dataset, tiny, expected, tmp_path):
    """Per-batch through write_many (groups of 64 batches): still one transaction and one
    system_time per original batch, so results are exactly the reference's."""
    if not _fast_write_path():
        pytest.skip("Muninn binding missing or without write_many (before ADR 0009)")
    out, cats = tiny
    a = registry.make("muninn-wm64", tmp_path / "m", threads=2, memory_limit_mb=1024)
    try:
        r = a.ingest(out / dataset, cats[dataset], "per-batch")
        assert r["complete"] and r["exact_system_time"]
        assert r["calls"] == -(-r["writes"] // 64)
        bad = [(qid, inst) for (ds, qid, inst), (q, exp) in expected.items()
               if ds == dataset and diff(exp, a.run(q))]
        assert not bad, bad[:5]
    finally:
        a.close()


def test_concurrent_producers_store_every_row(tiny, tmp_path):
    """8 producer threads: batches commit in nondeterministic order (seq and system_time
    differ from the dataset's), but every row is stored exactly once."""
    if not _fast_write_path():
        pytest.skip("Muninn binding missing or without the fast write path (before ADR 0009)")
    out, cats = tiny
    a = registry.make("muninn-8p", tmp_path / "m", threads=2, memory_limit_mb=1024)
    try:
        r = a.ingest(out / "settlement", cats["settlement"], "per-batch")
        h = a.run({"op": "history", "table": cats["settlement"]["tables"][0]["name"]})
        assert h.num_rows == r["rows"] == cats["settlement"]["stats"]["versions"]
        assert not r["exact_system_time"]
    finally:
        a.close()


@pytest.mark.parametrize("system", SYSTEMS + ["clickhouse-fsync"])
def test_per_batch_prefix(system, tiny, tmp_path):
    out, cats = tiny
    try:
        a = registry.make(system, tmp_path / system, threads=2, memory_limit_mb=1024)
        r = a.ingest(out / "settlement", cats["settlement"], "per-batch", max_batches=5)
    except registry.Unavailable as e:
        pytest.skip(str(e))
    try:
        assert r["writes"] == 5 and not r["complete"] and 0 < r["rows"] < r["rows_total"]
        h = a.run({"op": "history", "table": cats["settlement"]["tables"][0]["name"]})
        assert h.num_rows == r["rows"]
    finally:
        a.close()
