"""DuckDB baseline, two layouts sharing one set of queries:

* ``duckdb-native`` — a DuckDB database file, one table per dataset, rows physically
  sorted by ``(series_id, value_time, received_time, seq)`` so row-group zone maps prune
  series and value-time ranges.
* ``duckdb-parquet`` — the same sorted rows rewritten as zstd Parquet (row groups of
  122 880 rows, min/max statistics), queried through ``read_parquet`` with no DuckDB
  storage at all.

Semantics (SPEC §5/§6) and the tuning choices are explained in README.md next to this
file. The short version of the one non-obvious trick: the knowledge order
``(t_clock, system_time, seq)`` equals ``(t_clock, seq)`` because seq is assigned in commit
order and system_time never decreases in commit order (§4.2). So a version's order key
fits one HUGEINT, ``t_clock * 2^40 + seq``, and "latest visible at F" becomes an
``ASOF JOIN`` on ``key <= F * 2^40 + (2^40 - 1)``.
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

import duckdb
import pyarrow as pa

from ..base import (Adapter, SeriesMap, Unavailable, batch_bounds, engine_rows, latency_summary,
                    result_fields)

K = 1 << 40  # seq < 2^40 (10^12 versions) -- asserted at load


def tc(clock: str) -> str:
    if clock == "received":
        return "received_time"
    if clock == "published":
        return "coalesce(published_time, received_time)"  # §5: unknown -> received
    if clock == "system":
        return "system_time"
    raise ValueError(clock)


def key(clock: str) -> str:
    return f"({tc(clock)}::HUGEINT * {K} + seq)"


def _ints(xs) -> str:
    return ", ".join(str(int(x)) for x in xs)


class DuckDBAdapter(Adapter):
    layout = "native"
    #: "argmax" or "window" for latest; "asof" or "window" for grid. Defaults chosen by
    #: measurement (see README.md); the alternatives are kept so anyone can re-check.
    latest_strategy = "argmax"
    grid_strategy = "asof"
    frontier_table = True

    def __init__(self, workdir, threads=6, memory_limit_mb=4096, **kw):
        super().__init__(workdir, threads, memory_limit_mb)
        for k, v in kw.items():
            setattr(self, k, v)
        self.name = f"duckdb-{self.layout}"
        self.notes = (f"DuckDB {duckdb.__version__}, threads={threads}, "
                      f"memory_limit={memory_limit_mb}MB, layout={self.layout}, "
                      f"latest={self.latest_strategy}, grid={self.grid_strategy}. "
                      "Cold = fresh connection (DuckDB buffer pool and Parquet metadata empty); "
                      "the OS page cache is not dropped (no root).")
        self.con = None

    def version(self):
        return duckdb.__version__

    # ---------------------------------------------------------------------------------
    def _connect(self, read_only: bool, view: bool = True):
        cfg = {"threads": self.threads, "memory_limit": f"{self.memory_limit_mb}MB",
               "temp_directory": str(self.workdir / "tmp"),
               # The planner underestimates the (frontier x point) side of the grid and
               # would swap ASOF for a nested loop below this many rows; always use ASOF.
               "asof_loop_join_threshold": 0}
        if self.layout == "native":
            self.con = duckdb.connect(str(self.db_path), read_only=read_only, config=cfg)
        else:
            self.con = duckdb.connect(config=cfg)
            if view:
                self.con.execute(f"CREATE VIEW v AS SELECT * FROM read_parquet('{self.pq_path}')")

    def load(self, dataset_dir: Path, catalog: dict) -> dict:
        self.catalog = catalog
        t = catalog["tables"][0]
        self.table = t["name"]
        self.columns = [c["name"] for c in t["columns"]]
        self.col_types = {c["name"]: c["type"] for c in t["columns"]}
        self.smap = SeriesMap(catalog, self.table)
        if self.workdir.exists():
            shutil.rmtree(self.workdir)
        self.workdir.mkdir(parents=True)
        self.db_path = self.workdir / "muninn_bench.duckdb"
        self.pq_path = self.workdir / f"{self.table}.parquet"
        raw = dataset_dir / "versions.parquet"

        def conv(c):
            return f"epoch_us({c}) AS {c}" if self.col_types[c] == "timestamp" else c

        select = (
            "SELECT series_id, epoch_us(value_time) AS value_time, "
            "epoch_us(published_time) AS published_time, epoch_us(received_time) AS received_time, "
            "epoch_us(system_time) AS system_time, seq, deleted"
            + "".join(", " + conv(c) for c in self.columns)
            + f" FROM read_parquet('{raw}') ORDER BY series_id, value_time, received_time, seq"
        )
        t0 = time.perf_counter()
        if self.layout == "native":
            self._connect(read_only=False)
            self.con.execute(f"CREATE TABLE v AS {select}")
            self.con.execute("CHECKPOINT")
            max_seq = self.con.execute("SELECT max(seq) FROM v").fetchone()[0]
            self.con.close()
            ingest = time.perf_counter() - t0
            size = self.db_path.stat().st_size
        else:
            self._connect(read_only=False, view=False)
            self.con.execute(f"COPY ({select}) TO '{self.pq_path}' "
                             # no dictionary: DuckDB's Parquet dictionary merges -0.0
                             # and 0.0 (they compare equal), which changes results
                             "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880, "
                             "DICTIONARY_SIZE_LIMIT 0)")
            max_seq = self.con.execute(
                f"SELECT max(seq) FROM read_parquet('{self.pq_path}')").fetchone()[0]
            self.con.close()
            ingest = time.perf_counter() - t0
            size = self.pq_path.stat().st_size
        assert max_seq < K
        self._connect(read_only=True)
        return {"ingest_seconds": ingest, "bytes_on_disk": size}

    def ingest(self, dataset_dir: Path, catalog: dict, mode: str, max_batches: int | None = None) -> dict:
        """``bulk`` = ``load`` (one sorted CREATE TABLE AS + CHECKPOINT). ``per-batch``: a
        database file, then ``BEGIN; INSERT INTO v SELECT * FROM <batch>; COMMIT`` per
        original batch (each commit appends to and fsyncs the WAL; DuckDB checkpoints the
        WAL into the file by itself every 16 MB), then one CHECKPOINT. The table stays in
        arrival order, not the sorted layout the query runs use."""
        if self.layout != "native":
            raise Unavailable(f"{self.name}: ingest modes only for the native layout")
        if mode == "bulk":
            r = self.load(dataset_dir, catalog)
            n = catalog["stats"]["versions"]
            return {"mode": mode, "writes": 1, "rows": n, "batches_total": None, "rows_total": n,
                    "complete": True, "write_seconds": r["ingest_seconds"], "finalize_seconds": 0.0,
                    "wall_seconds": r["ingest_seconds"], "write_latency_ms": None,
                    "bytes_on_disk": r["bytes_on_disk"]}
        if mode != "per-batch":
            raise ValueError(mode)
        self.catalog = catalog
        t = catalog["tables"][0]
        self.table = t["name"]
        self.columns = [c["name"] for c in t["columns"]]
        self.col_types = {c["name"]: c["type"] for c in t["columns"]}
        self.smap = SeriesMap(catalog, self.table)
        if self.workdir.exists():
            shutil.rmtree(self.workdir)
        self.workdir.mkdir(parents=True)
        self.db_path = self.workdir / "muninn_bench.duckdb"
        rows = engine_rows(dataset_dir, catalog)
        starts, ends = batch_bounds(rows.column("system_time").to_numpy())
        n_batches = len(starts)
        if max_batches is not None:
            starts, ends = starts[:max_batches], ends[:max_batches]
        empty = rows.slice(0, 0)  # noqa: F841  (DuckDB replacement scan)
        t0 = time.perf_counter()
        self._connect(read_only=False)
        self.con.execute("CREATE TABLE v AS SELECT * FROM empty")
        lats = []
        for a, b in zip(starts.tolist(), ends.tolist()):
            t1 = time.perf_counter()
            self.con.register("b", rows.slice(a, b - a))
            self.con.execute("BEGIN")
            self.con.execute("INSERT INTO v SELECT * FROM b")
            self.con.execute("COMMIT")
            self.con.unregister("b")
            lats.append(time.perf_counter() - t1)
        t1 = time.perf_counter()
        self.con.execute("CHECKPOINT")
        t2 = time.perf_counter()
        loaded = self.con.execute("SELECT count(*), max(seq) FROM v").fetchone()
        self.con.close()
        nrows = int(ends[-1])
        assert loaded[0] == nrows and loaded[1] < K
        wal = self.db_path.with_name(self.db_path.name + ".wal")
        size = self.db_path.stat().st_size + (wal.stat().st_size if wal.exists() else 0)
        self._connect(read_only=True)
        return {"mode": mode, "writes": int(len(starts)), "rows": nrows,
                "batches_total": int(n_batches), "rows_total": rows.num_rows,
                "complete": nrows == rows.num_rows,
                "write_seconds": t1 - t0, "finalize_seconds": t2 - t1, "wall_seconds": t2 - t0,
                "write_latency_ms": latency_summary(lats), "bytes_on_disk": size}

    def cold(self):
        self.con.close()
        self._connect(read_only=True)

    def close(self):
        if self.con is not None:
            self.con.close()
            self.con = None

    # ---------------------------------------------------------------------------------
    def _where(self, q, *, frontier=True, value_range=None) -> list[str]:
        w = []
        ids = self.smap.ids(q.get("series"))
        if ids is not None:
            w.append(f"series_id IN ({_ints(ids)})")
        lo, hi = value_range if value_range else (q.get("value_from"), q.get("value_to"))
        if lo is not None:
            w.append(f"value_time >= {int(lo)}")
        if hi is not None:
            w.append(f"value_time < {int(hi)}")
        if q.get("system") is not None:
            w.append(f"system_time <= {int(q['system'])}")
        if frontier and q.get("frontier") is not None:
            w.append(f"{tc(self._clock(q))} <= {int(q['frontier'])}")
        return w

    def _clock(self, q):
        return q.get("clock") or self.catalog["tables"][0]["default_clock"]

    @staticmethod
    def _and(w):
        return " AND ".join(w) if w else "TRUE"

    def _cols(self, prefix=""):
        return "".join(f", {prefix}{c}" for c in self.columns)

    def sql(self, q: dict) -> tuple[str, dict]:
        op = q["op"]
        clk = self._clock(q)
        params = {}
        if op == "latest":
            w = self._and(self._where(q))
            if self.latest_strategy == "window":
                s = (f"SELECT series_id, value_time, published_time, received_time, system_time, seq"
                     f"{self._cols()} FROM (SELECT * FROM v WHERE {w} QUALIFY row_number() OVER "
                     f"(PARTITION BY series_id, value_time ORDER BY {tc(clk)} DESC, seq DESC) = 1) "
                     f"WHERE NOT deleted ORDER BY series_id, value_time")
            else:
                # One arg_max_null per column (plain arg_max skips NULL arguments). A single
                # arg_max(struct_pack(...)) would be simpler but turns -0.0 into 0.0 in
                # DuckDB 1.5.6, which breaks SPEC's bitwise f64 results.
                fields = ["published_time", "received_time", "system_time", "seq", "deleted"] + self.columns
                aggs = ", ".join(f"arg_max_null({f}, {key(clk)}) AS {f}" for f in fields)
                s = (f"SELECT series_id, value_time, published_time, received_time, system_time, "
                     f"seq{self._cols()} FROM (SELECT series_id, value_time, {aggs} FROM v WHERE {w} "
                     f"GROUP BY series_id, value_time) WHERE NOT deleted ORDER BY series_id, value_time")
        elif op == "history":
            w = self._where(q)
            lf = q.get("lead_filter")
            if lf:
                c = lf["column"]
                w += [f"{c} IS NOT NULL", f"value_time - {c} >= {int(lf['min'])}",
                      f"value_time - {c} < {int(lf['max'])}"]
            s = (f"SELECT series_id, value_time, published_time, received_time, system_time, seq, "
                 f"deleted{self._cols()} FROM v WHERE {self._and(w)} "
                 f"ORDER BY series_id, value_time, {tc(clk)}, seq")
        elif op == "replay":
            w = self._where(q, frontier=False)
            if q.get("from") is not None:
                w.append(f"{tc(clk)} > {int(q['from'])}")
            if q.get("to") is not None:
                w.append(f"{tc(clk)} <= {int(q['to'])}")
            s = (f"SELECT series_id, value_time, published_time, received_time, system_time, seq, "
                 f"deleted{self._cols()} FROM v WHERE {self._and(w)} ORDER BY {tc(clk)}, seq")
        elif op == "grid":
            self._params = {}
            s = self._grid_sql(q, clk)
            params = self._params
        elif op == "asof_join":
            s, params = self._asof_join_sql(q, clk)
        else:
            raise ValueError(op)
        return s, params

    def _grid_sql(self, q, clk):
        fs = q["frontiers"]
        if not fs:
            return ("SELECT NULL::BIGINT frontier, NULL::UINTEGER series_id, NULL::BIGINT value_time, "
                    "NULL::BIGINT published_time, NULL::BIGINT received_time, NULL::BIGINT system_time, "
                    "NULL::BIGINT seq" + "".join(f", NULL AS {c}" for c in self.columns) + " WHERE FALSE")
        relative = q.get("window_start") is not None
        if relative:
            ws, we = int(q["window_start"]), int(q["window_end"])
            span = (min(fs) + ws, max(fs) + we)
            fwin = f"p.value_time >= f.frontier + {ws} AND p.value_time < f.frontier + {we}"
        else:
            lo, hi = q.get("value_from"), q.get("value_to")
            span = (lo, hi)
            fwin = " AND ".join(x for x in [
                f"p.value_time >= {int(lo)}" if lo is not None else "",
                f"p.value_time < {int(hi)}" if hi is not None else ""] if x) or "TRUE"
        base = self._and(self._where(q, frontier=False, value_range=span))
        if self.frontier_table:
            # a registered Arrow table: the planner sees the real cardinality and picks a
            # range join (IEJoin) for the window condition instead of a nested loop
            self._params["frontiers"] = pa.table({"frontier": pa.array(fs, pa.int64())})
            fl = "frontiers"
        else:
            fl = f"(SELECT unnest([{_ints(fs)}]::BIGINT[]) AS frontier)"
        if self.grid_strategy == "window":
            return (f"SELECT frontier, series_id, value_time, published_time, received_time, "
                    f"system_time, seq{self._cols()} FROM (SELECT f.frontier, p.* FROM "
                    f"(SELECT *, {tc(clk)} AS t, {key(clk)} AS k FROM v WHERE {base}) p "
                    f"JOIN {fl} f ON {fwin} AND p.t <= f.frontier "
                    f"QUALIFY row_number() OVER (PARTITION BY f.frontier, p.series_id, p.value_time "
                    f"ORDER BY p.k DESC) = 1) WHERE NOT deleted "
                    f"ORDER BY frontier, series_id, value_time")
        if relative:
            # Frontiers are sorted, so the frontiers whose window holds a point form one
            # index range: (#f <= vt - we) .. (#f <= vt - ws) - 1. Two ASOF joins against
            # the (small) frontier list find it; unnest(range()) expands it. Linear in the
            # output, instead of a points x frontiers range join.
            pairs = (
                f"fi AS (SELECT frontier, (row_number() OVER (ORDER BY frontier))::BIGINT AS n FROM {fl}), "
                f"pr AS (SELECT p.series_id, p.value_time, coalesce(a.n, 0) AS lo, coalesce(b.n, 0) AS hi "
                f"FROM p ASOF LEFT JOIN fi a ON p.value_time - {we} >= a.frontier "
                f"ASOF LEFT JOIN fi b ON p.value_time - {ws} >= b.frontier), "
                f"l AS (SELECT fi.frontier, x.series_id, x.value_time, "
                f"fi.frontier::HUGEINT * {K} + {K - 1} AS fk FROM "
                f"(SELECT series_id, value_time, unnest(range(lo + 1, hi + 1)) AS n FROM pr WHERE hi > lo) x "
                f"JOIN fi USING (n)) ")
        else:
            pairs = (f"l AS (SELECT f.frontier, p.series_id, p.value_time, "
                     f"f.frontier::HUGEINT * {K} + {K - 1} AS fk FROM p JOIN {fl} f ON {fwin}) ")
        return (
            f"WITH r AS (SELECT *, {key(clk)} AS k FROM v WHERE {base}), "
            f"p AS (SELECT DISTINCT series_id, value_time FROM r), " + pairs +
            f"SELECT l.frontier, l.series_id, l.value_time, r.published_time, r.received_time, "
            f"r.system_time, r.seq{self._cols('r.')} FROM l ASOF JOIN r "
            f"ON l.series_id = r.series_id AND l.value_time = r.value_time AND l.fk >= r.k "
            f"WHERE NOT r.deleted ORDER BY l.frontier, l.series_id, l.value_time")

    def _asof_join_sql(self, q, clk):
        left = q["left"]
        ids = [self.smap.by_name[s] for s in left["series"]]
        lt = pa.table({
            "ord": pa.array(range(len(ids)), pa.int64()),
            "series_id": pa.array(ids, pa.uint32()),
            "value_time": pa.array(left["value_time"], pa.int64()),
            "frontier": pa.array(left["frontier"], pa.int64()),
        })
        rq = dict(q, series=sorted(set(left["series"])), frontier=None)
        if left["value_time"]:
            rq["value_from"], rq["value_to"] = min(left["value_time"]), max(left["value_time"]) + 1
        base = self._and(self._where(rq, frontier=False))
        right = ["published_time", "received_time", "system_time", "seq"] + self.columns
        sel = ", ".join(f"CASE WHEN found THEN {c} END AS {c}" for c in right)
        s = (f"SELECT series_id, value_time, frontier, found, {sel} FROM ("
             f"SELECT l.ord, l.series_id, l.value_time, l.frontier, "
             f"coalesce(NOT r.deleted, FALSE) AS found"
             + "".join(f", r.{c}" for c in right) +
             f" FROM (SELECT *, frontier::HUGEINT * {K} + {K - 1} AS fk FROM left_rows) l "
             f"ASOF LEFT JOIN (SELECT *, {key(clk)} AS k FROM v WHERE {base}) r "
             f"ON l.series_id = r.series_id AND l.value_time = r.value_time AND l.fk >= r.k) "
             f"ORDER BY ord")
        return s, {"left_rows": lt}

    def run(self, q: dict) -> pa.Table:
        s, params = self.sql(q)
        for name, tbl in params.items():
            self.con.register(name, tbl)
        try:
            res = self.con.execute(s).to_arrow_table()
        finally:
            for name in params:
                self.con.unregister(name)
        return self._canonical(q["op"], res)

    def _canonical(self, op, res: pa.Table) -> pa.Table:
        fields = result_fields(op, self.columns)
        cols = []
        for f in fields:
            if f == "series":
                cols.append(self.smap.name_column(res.column("series_id")))
            else:
                cols.append(res.column(f))
        return pa.table(cols, names=fields)


class DuckDBParquetAdapter(DuckDBAdapter):
    layout = "parquet"
