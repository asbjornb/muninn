"""The generator plays the engine's part (SPEC §4.2 system_time/seq, §4.3 dedup). Check
that against the reference's own write path: export a prefix of each tiny dataset as a
§9 scenario — the write stream including exact duplicate re-sends, then read steps with
expectations from the directly loaded oracle — and run it with the reference's scenario
runner. Any difference in system_time, seq, stored/duplicate counts or read rows fails.
"""

from __future__ import annotations

import json
import sys

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from competitors.reference.adapter import REPO
from datagen.__main__ import generate_dataset
from datagen.datasets import DATASETS
from datagen.scenario_export import export_scenario, write_steps

sys.path.insert(0, str(REPO / "spec" / "reference"))
from muninn_ref.scenario import run_scenario  # noqa: E402

PREFIX = 1500  # the reference write path is O(n^2) by design


@pytest.mark.parametrize("dataset", list(DATASETS))
def test_write_stream_reproduces_engine_fields(dataset, tmp_path):
    cat = generate_dataset(dataset, "tiny", 11, tmp_path)
    d = tmp_path / dataset
    # truncate the dataset to its first PREFIX versions (and the re-sends up to then)
    v = pq.read_table(d / "versions.parquet").slice(0, PREFIX)
    pq.write_table(v, d / "versions.parquet")
    last = pc.max(v.column("system_time")).cast("int64").as_py()
    r = pq.read_table(d / "resends.parquet")
    r = r.filter(pc.less_equal(r.column("commit_clock").cast("int64"), last))
    pq.write_table(r, d / "resends.parquet")
    vt = v.column("value_time").cast("int64")
    cat["stats"]["value_time_min"] = pc.min(vt).as_py()
    cat["stats"]["value_time_max"] = pc.max(vt).as_py()
    cat["now"] = min(cat["now"], last)
    (d / "catalog.json").write_text(json.dumps(cat))
    path = export_scenario(d, max_rows=None, reads=True, path=tmp_path / "sc.json")
    sc = json.loads(path.read_text())
    writes = [s for s in sc["steps"] if s["op"] == "write"]
    assert sum(s["expect"]["stored"] for s in writes) == v.num_rows
    failures = run_scenario(path)
    assert not failures, "\n".join(f.render() for f in failures[:3])


def test_full_tiny_export_has_resends(tmp_path):
    cat = generate_dataset("forecasts", "tiny", 11, tmp_path)
    steps = write_steps(tmp_path / "forecasts", cat)
    assert any(s["op"] == "write" and s["expect"]["duplicates"] > 0 for s in steps)
