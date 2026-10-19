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


def _muninn():
    def make(workdir, threads, memory_limit_mb):
        from .muninn.adapter import MuninnAdapter

        return MuninnAdapter(workdir, threads, memory_limit_mb)
    return make


SYSTEMS = {
    "duckdb-native": _duckdb("native"),
    "duckdb-parquet": _duckdb("parquet"),
    # alternative query strategies, kept so the tuning choice can be re-checked
    "duckdb-native-window": _duckdb("native", latest_strategy="window", grid_strategy="window"),
    "clickhouse": _clickhouse(),
    "clickhouse-arrayjoin": _clickhouse(grid_strategy="arrayjoin"),
    "muninn": _muninn(),
}

DEFAULT_SYSTEMS = ["muninn", "duckdb-native", "duckdb-parquet", "clickhouse"]


def make(name, workdir, threads=6, memory_limit_mb=4096):
    a = SYSTEMS[name](workdir, threads, memory_limit_mb)
    a.name = name
    return a
