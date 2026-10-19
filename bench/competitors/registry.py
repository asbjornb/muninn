"""All systems under test, by name. Adding a system = one Adapter subclass + one line here."""

from __future__ import annotations

from .base import Adapter, Unavailable  # noqa: F401  (re-exported)


def _duckdb(layout, **kw):
    def make(workdir, threads, memory_limit_mb):
        from .duckdb.adapter import DuckDBAdapter, DuckDBParquetAdapter

        cls = DuckDBAdapter if layout == "native" else DuckDBParquetAdapter
        return cls(workdir, threads, memory_limit_mb, **kw)
    return make


def _clickhouse(**kw):
    def make(workdir, threads, memory_limit_mb):
        from .clickhouse.adapter import ClickHouseAdapter

        return ClickHouseAdapter(workdir, threads, memory_limit_mb, **kw)
    return make


def _muninn(**kw):
    def make(workdir, threads, memory_limit_mb):
        from .muninn.adapter import MuninnAdapter

        return MuninnAdapter(workdir, threads, memory_limit_mb, **kw)
    return make


SYSTEMS = {
    "duckdb-native": _duckdb("native"),
    "duckdb-parquet": _duckdb("parquet"),
    # alternative query strategies, kept so the tuning choice can be re-checked
    "duckdb-native-window": _duckdb("native", latest_strategy="window", grid_strategy="window"),
    "clickhouse": _clickhouse(),
    "clickhouse-arrayjoin": _clickhouse(grid_strategy="arrayjoin"),
    # parts fsynced on insert (ClickHouse's default does not); used by bench/ingest.py
    "clickhouse-fsync": _clickhouse(fsync=True),
    "muninn": _muninn(),
    # bulk ingest in writes of 100k rows instead of 1M (faster per row, bench/ingest.py)
    "muninn-100k": _muninn(bulk_rows=100_000),
    # per-batch ingest through the fast write path's group commit (ADR 0009; needs a build
    # with write_many): 64 original batches per write_many call, or 8 producer threads
    "muninn-wm64": _muninn(group=64, ingest_compact_every=0),
    "muninn-8p": _muninn(producers=8, ingest_compact_every=0),
}

DEFAULT_SYSTEMS = ["muninn", "duckdb-native", "duckdb-parquet", "clickhouse"]


def make(name, workdir, threads=6, memory_limit_mb=4096):
    a = SYSTEMS[name](workdir, threads, memory_limit_mb)
    a.name = name
    return a
