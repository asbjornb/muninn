"""Shared fixtures: the tiny datasets, the reference implementation loaded with them, and
the oracle's answer to every workload and edge query."""

from __future__ import annotations

import pytest

from competitors.reference.adapter import ReferenceAdapter
from datagen.__main__ import generate_dataset
from datagen.datasets import DATASETS
from queries.edges import edge_queries
from queries.params import workload_for


@pytest.fixture(scope="session")
def tiny(tmp_path_factory):
    out = tmp_path_factory.mktemp("tiny")
    cats = {name: generate_dataset(name, "tiny", 7, out) for name in DATASETS}
    return out, cats


@pytest.fixture(scope="session")
def oracle(tiny, tmp_path_factory):
    out, cats = tiny
    refs = {}
    for name in DATASETS:
        r = ReferenceAdapter(tmp_path_factory.mktemp("ref"))
        r.load(out / name, cats[name])
        refs[name] = r
    return refs


@pytest.fixture(scope="session")
def expected(tiny, oracle):
    out, cats = tiny
    exp = {}
    for name in DATASETS:
        qs = workload_for(name, cats[name], "tiny") + edge_queries(out / name, cats[name])
        for q in qs:
            exp[(q["dataset"], q["id"], q["instance"])] = (q, oracle[name].run(q))
    return exp
