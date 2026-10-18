"""The competitor interface.

A *system under test* is one :class:`Adapter` subclass. The runner hands it a dataset
directory (``versions.parquet`` + ``catalog.json``, see ``datagen``), then concrete
queries from ``queries.params`` (plain dicts, timestamps/durations as int µs), and
expects each result as a ``pyarrow.Table`` in the *canonical result shape* below. The
correctness test compares that shape, row for row, with the reference implementation.

Canonical result columns, by op (SPEC §6), in this order:

    latest      series, value_time, published_time, received_time, system_time, seq, <cols>
    history     series, value_time, published_time, received_time, system_time, seq, deleted, <cols>
    replay      same as history
    grid        frontier, <latest columns>
    asof_join   series, value_time, frontier, found, published_time, received_time,
                system_time, seq, <cols>

``series`` is the series *name* (string); timestamps are int64 µs (``timestamp[us]`` is
accepted too and normalised); ``<cols>`` are the table's value columns by name, in catalog
order, null where absent. Row order is the op's SPEC order.

Query dict fields (absent/None = unbounded / not given):

    op, table, series (list of names or None = all), value_from, value_to, clock,
    frontier, system,
    replay:     from, to      (half-open interval (from, to] on t_clock)
    grid:       frontiers (sorted, distinct), window_start/window_end (relative) or
                value_from/value_to (absolute)
    asof_join:  left = {"series": [...], "value_time": [...], "frontier": [...]}
    history:    lead_filter = {"column": c, "min": µs, "max": µs} -- optional derived
                filter: keep rows with c not null and min <= value_time - c < max
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

import pyarrow as pa

BASE_FIELDS = ["series", "value_time", "published_time", "received_time", "system_time", "seq"]


def result_fields(op: str, columns: list[str]) -> list[str]:
    if op == "latest":
        return BASE_FIELDS + columns
    if op in ("history", "replay"):
        return BASE_FIELDS + ["deleted"] + columns
    if op == "grid":
        return ["frontier"] + BASE_FIELDS + columns
    if op == "asof_join":
        return ["series", "value_time", "frontier", "found", "published_time", "received_time",
                "system_time", "seq"] + columns
    raise ValueError(op)


class Unavailable(Exception):
    """Raised by an adapter whose runtime is not installed; tests skip, the runner reports."""


class Adapter(ABC):
    """One system under test. Lifecycle: ``load`` once per dataset, then ``run`` many
    times; ``cold`` between cold and warm measurements; ``close`` at the end."""

    name: str = "?"
    #: free-text notes for the report (what "cold" means here, versions, settings)
    notes: str = ""

    def __init__(self, workdir: Path, threads: int = 6, memory_limit_mb: int = 4096):
        self.workdir = Path(workdir)
        self.threads = threads
        self.memory_limit_mb = memory_limit_mb

    @abstractmethod
    def load(self, dataset_dir: Path, catalog: dict) -> dict:
        """Ingest ``dataset_dir/versions.parquet`` into the system's best-practice layout.
        Returns at least ``{"ingest_seconds": float, "bytes_on_disk": int}``."""

    @abstractmethod
    def run(self, q: dict) -> pa.Table:
        """Execute one concrete query and return the canonical result table."""

    def cold(self) -> None:
        """Drop the system's own caches, as far as possible without root (the OS page
        cache usually stays warm; say so in ``notes``)."""

    def close(self) -> None:
        pass

    def version(self) -> str:
        return "?"


class SeriesMap:
    """Catalog helper: series name <-> id for one table."""

    def __init__(self, catalog: dict, table: str):
        self.by_name = {s["name"]: s["id"] for s in catalog["series"] if s["table"] == table}
        n = max(self.by_name.values(), default=0)
        names = [None] * (n + 1)
        for k, v in self.by_name.items():
            names[v] = k
        self.names = pa.array(names, type=pa.string())

    def ids(self, names) -> list[int] | None:
        if not names:
            return None
        return [self.by_name[n] for n in names]  # KeyError = unknown_series

    def name_column(self, ids: pa.Array) -> pa.Array:
        import pyarrow.compute as pc

        return pc.take(self.names, ids)
