"""Muninn engine via its Python binding (crates/muninn-py).

Install into the bench environment (builds the Rust extension in release mode):

    uv sync --project bench --group muninn

**Load.** The binding cannot import explicit system_time/seq, so the adapter reproduces
them through the write path: versions are written in seq order, one ``write`` per
original batch (= per distinct system_time, which §4.2 makes unique per batch) with the
writer clock set to that system_time. ``max(c, last + 1us)`` then returns exactly the
original system_time, and seq comes out identical because nothing is a duplicate. The
adapter verifies both after loading (``exact = True``).

A dataset with more than ``max_exact_batches`` original batches (the sparse dataset at
small/medium: one message per batch, 3.2M at medium; a commit costs ~0.3 ms and two
files) is loaded *coalesced*: consecutive original batches are grouped into writes of up
to ``coalesce_rows`` rows, the writer clock set to the group's last system_time. seq is
still identical and the knowledge order unchanged (the new system_time is monotone in
seq); only the ``system_time`` values differ, so for such datasets the runner compares
results without that one field (``exact = False``) and queries with a system frontier
are not meaningful. Afterwards ``compact()`` merges segments (counted as ingest, like
ClickHouse's OPTIMIZE FINAL).
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ..base import Adapter, Unavailable, result_fields

try:
    import muninn
except ImportError:  # pragma: no cover
    muninn = None


def _dur(us):
    return f"{int(us)}us"


class MuninnAdapter(Adapter):
    name = "muninn"
    max_exact_batches = 200_000
    coalesce_rows = 200_000
    #: compact + gc every this many writes during the load (bounds segment count and
    #: checkpoint size, like a background merge); 0 = only at the end
    compact_every = 500
    #: create_series is one commit each and checkpoints carry the whole catalog, so
    #: catalog creation is quadratic (6k series: 10 s, 49 MB of log). Above this many
    #: series a dataset is skipped (150k series extrapolates to ~1.7 h and ~30 GB of log).
    max_series = 50_000

    def __init__(self, workdir, threads=6, memory_limit_mb=4096, **kw):
        super().__init__(workdir, threads, memory_limit_mb)
        if muninn is None:
            raise Unavailable("Muninn binding not installed: uv sync --project bench --group muninn")
        for k, v in kw.items():
            setattr(self, k, v)
        self.notes = ("Muninn engine (Python binding, release build), local directory store; "
                      "one write per original batch (coalesced for datasets with > "
                      f"{self.max_exact_batches:,} batches), compact()+gc() every "
                      f"{self.compact_every} writes and at the end. Cold = database "
                      "reopened (OS page cache not dropped). Engine thread count not configurable.")
        self.db = None

    def version(self):
        return getattr(muninn, "__version__", "0.1.0")

    def _open(self):
        self.db = muninn.open(str(self.workdir / "db"))

    def load(self, dataset_dir: Path, catalog: dict) -> dict:
        t = catalog["tables"][0]
        self.table = t["name"]
        self.columns = [c["name"] for c in t["columns"]]
        if self.workdir.exists():
            shutil.rmtree(self.workdir)
        self.workdir.mkdir(parents=True)
        v = pq.read_table(dataset_dir / "versions.parquet")
        cols = ["series", "value_time", "published_time", "received_time", "deleted"] + self.columns
        data = v.select(cols)
        data = data.set_column(0, "series", data.column("series").cast(pa.string()))
        st = v.column("system_time").cast(pa.int64()).to_numpy()
        seq = v.column("seq").to_numpy()
        assert (np.diff(seq) == 1).all() and seq[0] == 1, "versions.parquet must be in seq order"
        bounds = np.flatnonzero(np.diff(st)) + 1
        starts = np.concatenate([[0], bounds])
        ends = np.concatenate([bounds, [len(st)]])
        self.exact = len(starts) <= self.max_exact_batches
        if not self.exact:  # group whole batches into writes of ~coalesce_rows rows
            grp_s, grp_e, a = [], [], 0
            while a < len(starts):
                b = a
                while b + 1 < len(starts) and ends[b + 1] - starts[a] <= self.coalesce_rows:
                    b += 1
                grp_s.append(starts[a])
                grp_e.append(ends[b])
                a = b + 1
            starts, ends = np.array(grp_s), np.array(grp_e)

        if len(catalog["series"]) > self.max_series:
            raise Unavailable(f"{len(catalog['series']):,} series > {self.max_series:,}: "
                              "create_series is quadratic in the catalog size (see adapter)")
        t0 = time.perf_counter()
        db = muninn.open(str(self.workdir / "db"), create=True)
        db.create_table(t["name"], columns={c["name"]: c["type"] for c in t["columns"]},
                        default_clock=t["default_clock"], partition=t["partition"])
        for s in catalog["series"]:
            db.create_series(s["name"], s["table"], tags=s["tags"] or None,
                             resolution=s["resolution"])
        for i, (a, b) in enumerate(zip(starts.tolist(), ends.tolist())):
            r = db.write(data.slice(a, b - a), clock=int(st[b - 1]))
            assert r.stored == b - a, (r, a, b)
            if self.compact_every and (i + 1) % self.compact_every == 0:
                db.compact()
                db.gc(grace="0s")
        write_s = time.perf_counter() - t0
        db.compact()
        db.gc(grace="0s")
        ingest = time.perf_counter() - t0
        size = sum(p.stat().st_size for p in (self.workdir / "db").rglob("*") if p.is_file())
        nfiles = sum(1 for p in (self.workdir / "db").rglob("*") if p.is_file())
        seg_bytes = sum(p.stat().st_size for p in (self.workdir / "db" / "segments").rglob("*")
                        if p.is_file())
        del db
        self._open()
        if self.exact:  # verify the engine assigned exactly the dataset's system_time/seq
            h = self.db.history(self.table)
            got = pa.table({"seq": h.column("seq").cast(pa.int64()),
                            "st": h.column("system_time").cast(pa.int64())}).sort_by("seq")
            assert got.num_rows == len(seq)
            assert (got.column("st").to_numpy() == st).all(), "system_time differs"
        self.notes_load = {"writes": int(len(starts)), "write_seconds": round(write_s, 3),
                           "files": nfiles, "segment_bytes": seg_bytes,
                           "log_bytes": size - seg_bytes, "exact_system_time": self.exact}
        return {"ingest_seconds": ingest, "bytes_on_disk": size, **self.notes_load}

    def cold(self):
        self.db = None
        self._open()

    def close(self):
        self.db = None

    # ---------------------------------------------------------------------------------
    def run(self, q: dict) -> pa.Table:
        db, op, tbl = self.db, q["op"], q["table"]
        series = q.get("series") or None
        if op in ("latest", "history"):
            res = getattr(db, op)(tbl, series=series, value_from=q.get("value_from"),
                                  value_to=q.get("value_to"), clock=q.get("clock"),
                                  frontier=q.get("frontier"), system=q.get("system"))
            lf = q.get("lead_filter")
            if lf:
                c = lf["column"]
                lead = pc.subtract(res.column("value_time").cast(pa.int64()), res.column(c).cast(pa.int64()))
                keep = pc.and_(pc.greater_equal(lead, lf["min"]), pc.less(lead, lf["max"]))
                res = res.filter(pc.fill_null(keep, False))
        elif op == "replay":
            res = db.replay(tbl, q.get("from"), q.get("to"), series=series,
                            value_from=q.get("value_from"), value_to=q.get("value_to"),
                            clock=q.get("clock"), system=q.get("system"))
        elif op == "grid":
            kw = {}
            if q.get("window_start") is not None:
                kw["window"] = (_dur(q["window_start"]), _dur(q["window_end"]))
            else:
                kw["value_from"], kw["value_to"] = q.get("value_from"), q.get("value_to")
            res = db.grid(tbl, q["frontiers"], series=series, clock=q.get("clock"),
                          system=q.get("system"), **kw)
        elif op == "asof_join":
            left = q["left"]
            lt = pa.table({"series": pa.array(left["series"], pa.string()),
                           "value_time": pa.array(left["value_time"], pa.int64()),
                           "frontier": pa.array(left["frontier"], pa.int64())})
            res = db.asof_join(tbl, lt, clock=q.get("clock"), system=q.get("system"))
        else:
            raise ValueError(op)
        return self._canonical(op, res)

    def _canonical(self, op, res: pa.Table) -> pa.Table:
        fields = result_fields(op, self.columns)
        names = set(res.column_names)
        out = []
        for f in fields:
            c = res.column(f + "_right") if op == "asof_join" and f + "_right" in names else res.column(f)
            out.append(c)
        return pa.table(out, names=fields)
