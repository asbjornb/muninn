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
    #: set by bench/ingest.py when the fsync counter shim is loaded: returns a numpy array
    #: of cumulative (count, ns) per fsync category; adapters may sample it around writes
    probe = None

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

    def ingest(self, dataset_dir: Path, catalog: dict, mode: str, max_batches: int | None = None) -> dict:
        """Ingest benchmark (``bench/ingest.py``), one of :data:`INGEST_MODES`:

        * ``per-batch`` -- one transaction/commit per original write batch (= per distinct
          ``system_time``), in seq order; only the first ``max_batches`` batches if given;
        * ``bulk`` -- everything in as few large writes as the system allows.

        Afterwards the adapter must be queryable (``run``) on what was loaded. Returns at
        least ``mode, writes, rows, batches_total, rows_total, complete, write_seconds,
        finalize_seconds, wall_seconds, bytes_on_disk``; ``write_latency_ms`` (p50/p99/max
        per write, ``None`` when there is one write) where the adapter times writes."""
        raise Unavailable(f"{self.name}: ingest modes not implemented")


INGEST_MODES = ("per-batch", "bulk")


def batch_bounds(system_time) -> tuple:
    """(starts, ends) of the original write batches in a seq-ordered ``system_time`` array:
    §4.2 makes system_time unique per batch, so a batch is a run of equal values."""
    import numpy as np

    st = np.asarray(system_time)
    bounds = np.flatnonzero(np.diff(st)) + 1
    return np.concatenate([[0], bounds]), np.concatenate([bounds, [len(st)]])


def engine_rows(dataset_dir: Path, catalog: dict):
    """versions.parquet as the SQL baselines store it: ``series_id, value_time,
    published_time, received_time, system_time, seq, deleted, <cols>`` with timestamps as
    int64 µs, in seq order (the per-batch ingest source; prepared outside any timing)."""
    import pyarrow.parquet as pq

    t = catalog["tables"][0]
    types = {c["name"]: c["type"] for c in t["columns"]}
    names = ["series_id", "value_time", "published_time", "received_time", "system_time",
             "seq", "deleted"] + list(types)
    v = pq.read_table(Path(dataset_dir) / "versions.parquet", columns=names)
    cols = []
    for n in names:
        c = v.column(n)
        if pa.types.is_timestamp(c.type):
            c = c.cast(pa.int64())
        cols.append(c)
    return pa.table(cols, names=names).combine_chunks()


def latency_summary(seconds: list[float]) -> dict | None:
    """p50/p99/max per write in ms (``None`` for fewer than two writes)."""
    if len(seconds) < 2:
        return None
    import numpy as np

    a = np.asarray(seconds) * 1e3
    return {"p50": round(float(np.percentile(a, 50)), 3), "p99": round(float(np.percentile(a, 99)), 3),
            "max": round(float(a.max()), 3), "mean": round(float(a.mean()), 3)}


def dir_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in Path(path).rglob("*") if p.is_file())


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
