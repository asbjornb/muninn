"""ClickHouse baseline: one MergeTree table per dataset, queried over HTTP.

Runtime: the official ClickHouse static binary (``fetch.sh`` downloads and verifies it;
the same build ships in the ``clickhouse/clickhouse-server`` image) started as a local
server process with a config generated into the work directory. ``$CLICKHOUSE_BIN``
overrides the binary. Docker would work the same way; it was not usable on the machine
the first reports were made on (no docker group membership).

Schema and query choices are explained in README.md next to this file. In short:

* ``ORDER BY (series_id, value_time, received_time, seq)``, Delta+ZSTD codecs on the
  time columns, ZSTD on values, one fully merged part after ``OPTIMIZE FINAL``;
* latest/history/replay: ``argMax(tuple, (t_clock, seq))`` / plain filtered scans;
* grid and asof_join: ``ASOF JOIN`` on ``t_clock``. ClickHouse ASOF takes a single
  inequality column, so ties on t_clock are resolved first: the right side is
  pre-aggregated to one row per ``(series, value_time, t_clock)`` with
  ``argMax(..., seq)``, which makes the ASOF match unique and exact.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pyarrow as pa

from ..base import Adapter, SeriesMap, Unavailable, result_fields

TOOLS = Path(__file__).resolve().parents[2] / "tools" / "clickhouse" / "clickhouse-bin"

CH_TYPES = {"f64": "Float64", "i64": "Int64", "bool": "Bool", "string": "String",
            "timestamp": "Int64"}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def tc(clock: str, p: str = "") -> str:
    if clock == "received":
        return f"{p}received_time"
    if clock == "published":
        return f"coalesce({p}published_time, {p}received_time)"
    if clock == "system":
        return f"{p}system_time"
    raise ValueError(clock)


def _ints(xs) -> str:
    return ", ".join(str(int(x)) for x in xs)


CONFIG = """<clickhouse>
  <logger><level>warning</level><log>{d}/log/server.log</log><errorlog>{d}/log/err.log</errorlog><console>0</console></logger>
  <path>{d}/data/</path>
  <tmp_path>{d}/tmp/</tmp_path>
  <user_files_path>{d}/user_files/</user_files_path>
  <format_schema_path>{d}/format_schemas/</format_schema_path>
  <listen_host>127.0.0.1</listen_host>
  <http_port>{http}</http_port>
  <tcp_port>{tcp}</tcp_port>
  <max_server_memory_usage>{server_mem}</max_server_memory_usage>
  <mark_cache_size>268435456</mark_cache_size>
  <uncompressed_cache_size>0</uncompressed_cache_size>
  <mlock_executable>false</mlock_executable>
  <users>
    <default>
      <password></password>
      <networks><ip>127.0.0.1</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
      <access_management>1</access_management>
    </default>
  </users>
  <profiles>
    <default>
      <max_threads>{threads}</max_threads>
      <max_insert_threads>{threads}</max_insert_threads>
      <max_memory_usage>{mem}</max_memory_usage>
      <use_query_cache>0</use_query_cache>
      <output_format_arrow_string_as_string>1</output_format_arrow_string_as_string>
      <output_format_arrow_low_cardinality_as_dictionary>0</output_format_arrow_low_cardinality_as_dictionary>
    </default>
  </profiles>
  <quotas><default/></quotas>
</clickhouse>
"""


class ClickHouseAdapter(Adapter):
    name = "clickhouse"

    def __init__(self, workdir, threads=6, memory_limit_mb=4096, **kw):
        super().__init__(workdir, threads, memory_limit_mb)
        self.bin = os.environ.get("CLICKHOUSE_BIN") or (str(TOOLS) if TOOLS.exists() else shutil.which("clickhouse"))
        if not self.bin:
            raise Unavailable("no ClickHouse binary: run bench/competitors/clickhouse/fetch.sh "
                              "or set CLICKHOUSE_BIN")
        self.proc = None
        self.client = None
        self.notes = (f"ClickHouse server (official static binary), max_threads={threads}, "
                      f"max_memory_usage={memory_limit_mb}MB, MergeTree, query cache off. "
                      "Cold = SYSTEM DROP MARK/UNCOMPRESSED/MMAP/QUERY CACHE (OS page cache not dropped).")

    def version(self):
        out = subprocess.run([self.bin, "--version"], capture_output=True, text=True).stdout
        return out.strip().split("version")[-1].strip().split()[0] if out else "?"

    # ---------------------------------------------------------------------------------
    def _start(self):
        d = self.workdir / "server"
        for sub in ("log", "data", "tmp", "user_files", "format_schemas"):
            (d / sub).mkdir(parents=True, exist_ok=True)
        self.http = _free_port()
        cfg = d / "config.xml"
        cfg.write_text(CONFIG.format(d=d, http=self.http, tcp=_free_port(), threads=self.threads,
                                     mem=self.memory_limit_mb * 1024 * 1024,
                                     # server-wide cap: per-query limit + 1 GB for caches/background
                                     server_mem=(self.memory_limit_mb + 1024) * 1024 * 1024))
        self.proc = subprocess.Popen([self.bin, "server", f"--config-file={cfg}"],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                     cwd=d)
        import urllib.request

        for _ in range(300):
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{self.http}/ping", timeout=1) as r:
                    if r.read().strip() == b"Ok.":
                        break
            except OSError:
                if self.proc.poll() is not None:
                    raise RuntimeError(f"clickhouse server exited, see {d}/log")
                time.sleep(0.1)
        else:
            raise RuntimeError("clickhouse server did not start")
        import clickhouse_connect

        self.client = clickhouse_connect.get_client(host="127.0.0.1", port=self.http,
                                                    username="default", compress=False,
                                                    query_limit=0, connect_timeout=30,
                                                    send_receive_timeout=3600)

    def load(self, dataset_dir: Path, catalog: dict) -> dict:
        self.catalog = catalog
        t = catalog["tables"][0]
        self.table = t["name"]
        self.columns = [c["name"] for c in t["columns"]]
        self.col_types = {c["name"]: c["type"] for c in t["columns"]}
        self.default_clock = t["default_clock"]
        self.smap = SeriesMap(catalog, self.table)
        if self.workdir.exists():
            shutil.rmtree(self.workdir)
        self.workdir.mkdir(parents=True)
        self._start()
        uf = self.workdir / "server" / "user_files" / "versions.parquet"
        os.symlink((dataset_dir / "versions.parquet").resolve(), uf)

        def coldef(c):
            ty = self.col_types[c]
            if ty == "string":
                return f"{c} LowCardinality(Nullable(String))"
            if ty == "timestamp":
                return f"{c} Nullable(Int64) CODEC(Delta, ZSTD(3))"
            if ty == "f64":
                return f"{c} Nullable(Float64) CODEC(ZSTD(3))"
            return f"{c} Nullable({CH_TYPES[ty]}) CODEC(ZSTD(3))"

        ddl = (
            "CREATE TABLE v (series_id UInt32 CODEC(Delta, ZSTD(3)), "
            "value_time Int64 CODEC(Delta, ZSTD(3)), "
            "published_time Nullable(Int64) CODEC(Delta, ZSTD(3)), "
            "received_time Int64 CODEC(Delta, ZSTD(3)), "
            "system_time Int64 CODEC(Delta, ZSTD(3)), "
            "seq Int64 CODEC(Delta, ZSTD(3)), deleted Bool, "
            + ", ".join(coldef(c) for c in self.columns)
            + ") ENGINE = MergeTree ORDER BY (series_id, value_time, received_time, seq) "
            "SETTINGS index_granularity = 8192"
        )

        def conv(c):
            return f"toUnixTimestamp64Micro({c})" if self.col_types[c] == "timestamp" else c

        t0 = time.perf_counter()
        self.client.command(ddl)
        self.client.command(
            "INSERT INTO v SELECT series_id, toUnixTimestamp64Micro(value_time), "
            "toUnixTimestamp64Micro(published_time), toUnixTimestamp64Micro(received_time), "
            "toUnixTimestamp64Micro(system_time), seq, deleted"
            + "".join(", " + conv(c) for c in self.columns)
            + " FROM file('versions.parquet', Parquet)",
            settings={"max_insert_threads": self.threads, "max_threads": self.threads})
        self.client.command("OPTIMIZE TABLE v FINAL")
        ingest = time.perf_counter() - t0
        size = self.client.query(
            "SELECT sum(bytes_on_disk) FROM system.parts WHERE table = 'v' AND active").result_rows[0][0]
        assert self.client.query("SELECT max(seq) FROM v").result_rows[0][0] < 1 << 62
        return {"ingest_seconds": ingest, "bytes_on_disk": int(size)}

    def cold(self):
        for c in ("SYSTEM DROP MARK CACHE", "SYSTEM DROP UNCOMPRESSED CACHE", "SYSTEM DROP MMAP CACHE",
                  "SYSTEM DROP QUERY CACHE"):
            try:
                self.client.command(c)
            except Exception:  # noqa: BLE001 (older servers lack some of these)
                pass

    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None
        if self.proc is not None:
            self.proc.terminate()
            try:
                self.proc.wait(30)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            self.proc = None

    # ---------------------------------------------------------------------------------
    def _clock(self, q):
        return q.get("clock") or self.default_clock

    def _where(self, q, *, frontier=True, value_range=None, p="") -> list[str]:
        w = []
        ids = self.smap.ids(q.get("series"))
        if ids is not None:
            w.append(f"{p}series_id IN ({_ints(ids)})")
        lo, hi = value_range if value_range else (q.get("value_from"), q.get("value_to"))
        if lo is not None:
            w.append(f"{p}value_time >= {int(lo)}")
        if hi is not None:
            w.append(f"{p}value_time < {int(hi)}")
        if q.get("system") is not None:
            w.append(f"{p}system_time <= {int(q['system'])}")
        if frontier and q.get("frontier") is not None:
            w.append(f"{tc(self._clock(q), p)} <= {int(q['frontier'])}")
        return w

    @staticmethod
    def _and(w):
        return " AND ".join(w) if w else "1"

    def _cols(self, prefix=""):
        return "".join(f", {prefix}{c}" for c in self.columns)

    def sql(self, q: dict) -> tuple[str, dict | None]:
        op, clk = q["op"], self._clock(q)
        if op == "latest":
            fields = ["published_time", "received_time", "system_time", "seq", "deleted"] + self.columns
            tup = ", ".join(fields)
            sel = ", ".join(f"w.{i + 1} AS {f}" for i, f in enumerate(fields) if f != "deleted")
            return (f"SELECT series_id, value_time, {sel} FROM (SELECT series_id, value_time, "
                    f"argMax(tuple({tup}), ({tc(clk)}, seq)) AS w FROM v WHERE {self._and(self._where(q))} "
                    f"GROUP BY series_id, value_time) WHERE NOT w.5 ORDER BY series_id, value_time"), None
        if op == "history":
            w = self._where(q)
            lf = q.get("lead_filter")
            if lf:
                c = lf["column"]
                w += [f"{c} IS NOT NULL", f"value_time - {c} >= {int(lf['min'])}",
                      f"value_time - {c} < {int(lf['max'])}"]
            return (f"SELECT series_id, value_time, published_time, received_time, system_time, seq, "
                    f"deleted{self._cols()} FROM v WHERE {self._and(w)} "
                    f"ORDER BY series_id, value_time, {tc(clk)}, seq"), None
        if op == "replay":
            w = self._where(q, frontier=False)
            if q.get("from") is not None:
                w.append(f"{tc(clk)} > {int(q['from'])}")
            if q.get("to") is not None:
                w.append(f"{tc(clk)} <= {int(q['to'])}")
            return (f"SELECT series_id, value_time, published_time, received_time, system_time, seq, "
                    f"deleted{self._cols()} FROM v WHERE {self._and(w)} ORDER BY {tc(clk)}, seq"), None
        if op == "grid":
            return self._grid_sql(q, clk), None
        if op == "asof_join":
            return self._asof_join_sql(q, clk)
        raise ValueError(op)

    def _right(self, base, clk):
        """One row per (series, value_time, t_clock): the max-seq version (ties resolved)."""
        fields = ["published_time", "received_time", "system_time", "seq", "deleted"] + self.columns
        tup = ", ".join(fields)
        sel = ", ".join(f"w.{i + 1} AS {f}" for i, f in enumerate(fields))
        return (f"(SELECT series_id, value_time, t, {sel} FROM (SELECT series_id, value_time, "
                f"{tc(clk)} AS t, argMax(tuple({tup}), seq) AS w FROM v WHERE {base} "
                f"GROUP BY series_id, value_time, t))")

    def _grid_sql(self, q, clk):
        fs = q["frontiers"]
        if q.get("window_start") is not None:
            ws, we = int(q["window_start"]), int(q["window_end"])
            span = (min(fs) + ws, max(fs) + we) if fs else (0, 0)
            fwin = f"p.value_time >= f + {ws} AND p.value_time < f + {we}"
        else:
            lo, hi = q.get("value_from"), q.get("value_to")
            span = (lo, hi)
            fwin = " AND ".join(x for x in [f"p.value_time >= {int(lo)}" if lo is not None else "",
                                            f"p.value_time < {int(hi)}" if hi is not None else ""] if x) or "1"
        base = self._and(self._where(q, frontier=False, value_range=span))
        farr = f"[{_ints(fs)}]::Array(Int64)" if fs else "[]::Array(Int64)"
        r = self._right(base, clk)
        # left: (frontier, point) pairs; points from the filtered table, frontiers via arrayJoin
        left = (f"(SELECT f AS frontier, p.series_id AS series_id, p.value_time AS value_time FROM "
                f"(SELECT DISTINCT series_id, value_time FROM v WHERE {base}) AS p "
                f"ARRAY JOIN arrayFilter(f -> {fwin.replace('p.value_time', 'value_time')}, {farr}) AS f)")
        cols = ", ".join(f"r.{c}" for c in ["published_time", "received_time", "system_time", "seq"] + self.columns)
        return (f"SELECT l.frontier AS frontier, l.series_id AS series_id, l.value_time AS value_time, {cols} "
                f"FROM {left} AS l ASOF INNER JOIN {r} AS r "
                f"ON l.series_id = r.series_id AND l.value_time = r.value_time AND l.frontier >= r.t "
                f"WHERE NOT r.deleted ORDER BY frontier, series_id, value_time "
                f"SETTINGS join_algorithm = 'hash'")

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
        r = self._right(base, clk)
        right = ["published_time", "received_time", "system_time", "seq"] + self.columns
        sel = ", ".join(f"if(found, r.{c}, NULL) AS {c}" for c in right)
        s = (f"SELECT l.series_id AS series_id, l.value_time AS value_time, l.frontier AS frontier, "
             f"ifNull(NOT r.deleted, false) AS found, {sel} "
             f"FROM left_rows AS l ASOF LEFT JOIN {r} AS r "
             f"ON l.series_id = r.series_id AND l.value_time = r.value_time AND l.frontier >= r.t "
             f"ORDER BY l.ord SETTINGS join_use_nulls = 1")
        return s, lt

    def run(self, q: dict) -> pa.Table:
        s, left = self.sql(q)
        ext = None
        if left is not None:
            # left rows travel with the query as external data (one round trip)
            from clickhouse_connect.driver.external import ExternalData

            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, left.schema) as w:
                w.write_table(left)
            ext = ExternalData(file_name="left_rows.arrow", data=sink.getvalue().to_pybytes(),
                               fmt="ArrowStream",
                               structure="ord Int64, series_id UInt32, value_time Int64, frontier Int64")
        res = self.client.query_arrow(s, use_strings=True, external_data=ext)
        return self._canonical(q["op"], res)

    def _canonical(self, op, res: pa.Table) -> pa.Table:
        fields = result_fields(op, self.columns)
        cols = []
        for f in fields:
            if f == "series":
                cols.append(self.smap.name_column(res.column("series_id").cast(pa.int64())))
            else:
                c = res.column(f)
                if f == "found" or f == "deleted":
                    c = c.cast(pa.bool_())
                cols.append(c)
        return pa.table(cols, names=fields)
