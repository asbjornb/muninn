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

from ..base import Adapter, Unavailable, batch_bounds, dir_bytes, latency_summary, result_fields

try:
    import muninn
except ImportError:  # pragma: no cover
    muninn = None


def _has_flush() -> bool:
    """Whether the binding has the fast write path (ADR 0009: memtable, ``flush``)."""
    return muninn is not None and hasattr(getattr(muninn, "Database", object), "flush")


def _dur(us):
    return f"{int(us)}us"


class MuninnAdapter(Adapter):
    name = "muninn"
    max_exact_batches = 200_000
    coalesce_rows = 200_000
    #: compact + gc every this many writes during the load (bounds segment count and
    #: checkpoint size, like a background merge); 0 = only at the end
    compact_every = 500
    #: series are created with create_series_batch, this many per commit
    series_batch = 50_000
    #: lead-time filters are passed to the engine (history(lead=...)) instead of being
    #: applied to the full history on the client
    lead_pushdown = True
    #: rows per write in the ``bulk`` ingest mode (bench/ingest.py)
    bulk_rows = 1_000_000
    #: per-batch ingest mode only: compact+gc every this many writes (None = compact_every;
    #: 0 = only at the end, e.g. with the fast write path's background flush, ADR 0009)
    ingest_compact_every = None
    #: per-batch ingest mode only: submit this many original batches per ``write_many``
    #: call (one group commit; each batch still its own transaction and system_time)
    group = 1
    #: per-batch ingest mode only: this many producer threads calling ``write`` (the
    #: engine groups concurrent writes into one commit, ADR 0009)
    producers = 1

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
                      "reopened (OS page cache not dropped). Engine thread count not configurable. "
                      f"Bulk ingest mode (bench/ingest.py): writes of {self.bulk_rows:,} rows, "
                      "compact()+gc() at the end.")
        self.db = None

    def version(self):
        return getattr(muninn, "__version__", "0.1.0")

    def _open(self):
        self.db = muninn.open(str(self.workdir / "db"))

    def _prepare(self, dataset_dir: Path, catalog: dict):
        """Reads versions.parquet into the write shape (outside any timing). Returns
        (write table, system_time, seq, batch starts, batch ends)."""
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
        starts, ends = batch_bounds(st)
        return data, st, seq, starts, ends

    def _create(self, catalog: dict):
        t = catalog["tables"][0]
        db = muninn.open(str(self.workdir / "db"), create=True)
        db.create_table(t["name"], columns={c["name"]: c["type"] for c in t["columns"]},
                        default_clock=t["default_clock"], partition=t["partition"])
        series = [{"name": x["name"], "table": x["table"], "tags": x["tags"] or None,
                   "resolution": x["resolution"]} for x in catalog["series"]]
        for k in range(0, len(series), self.series_batch):
            db.create_series_batch(series[k:k + self.series_batch])
        return db

    def _write_loop(self, db, data, starts, ends, clocks, compact_every):
        """One ``write`` per (start, end) with the given writer clocks. Returns per-write
        latencies (s) and the time spent in the periodic compact+gc."""
        lats, maint = [], 0.0
        probe = self.probe
        self.write_fsync = None if probe is None else probe() * 0
        for i, (a, b) in enumerate(zip(starts.tolist(), ends.tolist())):
            p0 = probe() if probe is not None else None
            t0 = time.perf_counter()
            r = db.write(data.slice(a, b - a), clock=int(clocks[i]))
            lats.append(time.perf_counter() - t0)
            if probe is not None:
                self.write_fsync += probe() - p0
            assert r.stored == b - a, (r, a, b)
            if compact_every and (i + 1) % compact_every == 0:
                t1 = time.perf_counter()
                db.compact()
                db.gc(grace="0s")
                maint += time.perf_counter() - t1
        return lats, maint

    def _sizes(self):
        root = self.workdir / "db"
        size = dir_bytes(root)
        seg_bytes = dir_bytes(root / "segments") if (root / "segments").exists() else 0
        nfiles = sum(1 for p in root.rglob("*") if p.is_file())
        return size, seg_bytes, nfiles

    def _verify_system_time(self, seq, st):
        """Checks that the engine assigned exactly the dataset's system_time/seq."""
        h = self.db.history(self.table)
        got = pa.table({"seq": h.column("seq").cast(pa.int64()),
                        "st": h.column("system_time").cast(pa.int64())}).sort_by("seq")
        assert got.num_rows == len(seq)
        assert (got.column("st").to_numpy() == st).all(), "system_time differs"

    def load(self, dataset_dir: Path, catalog: dict) -> dict:
        data, st, seq, starts, ends = self._prepare(dataset_dir, catalog)
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

        t0 = time.perf_counter()
        db = self._create(catalog)
        self._write_loop(db, data, starts, ends, st[ends - 1], self.compact_every)
        write_s = time.perf_counter() - t0
        db.compact()
        db.gc(grace="0s")
        ingest = time.perf_counter() - t0
        size, seg_bytes, nfiles = self._sizes()
        del db
        self._open()
        if self.exact:
            self._verify_system_time(seq, st)
        self.notes_load = {"writes": int(len(starts)), "write_seconds": round(write_s, 3),
                           "files": nfiles, "segment_bytes": seg_bytes,
                           "log_bytes": size - seg_bytes, "exact_system_time": self.exact}
        return {"ingest_seconds": ingest, "bytes_on_disk": size, **self.notes_load}

    def ingest(self, dataset_dir: Path, catalog: dict, mode: str, max_batches: int | None = None) -> dict:
        """``per-batch``: the same writes as ``load`` (one per original batch, writer clock =
        its system_time, compact+gc every ``compact_every`` writes) but never coalesced;
        ``max_batches`` cuts it to a prefix. ``bulk``: writes of ``bulk_rows`` rows in seq
        order (cut regardless of batch boundaries, so system_time values differ from the
        dataset's), one compact+gc at the end."""
        data, st, seq, starts, ends = self._prepare(dataset_dir, catalog)
        if self.reset_peak is not None:
            self.reset_peak()
        n_batches = len(starts)
        if mode == "per-batch":
            if max_batches is not None:
                starts, ends = starts[:max_batches], ends[:max_batches]
            compact_every = (self.compact_every if self.ingest_compact_every is None
                             else self.ingest_compact_every)
        elif mode == "bulk":
            n = len(st)
            starts = np.arange(0, n, self.bulk_rows)
            ends = np.minimum(starts + self.bulk_rows, n)
            compact_every = 0
        else:
            raise ValueError(mode)
        rows = int(ends[-1])
        t0 = time.perf_counter()
        db = self._create(catalog)
        setup_s = time.perf_counter() - t0
        calls = None
        if mode == "per-batch" and self.group > 1:
            lats, maint, calls = self._write_many_loop(db, data, starts, ends, st[ends - 1],
                                                       compact_every)
        elif mode == "per-batch" and self.producers > 1:
            lats, maint = self._threaded_loop(db, data, starts, ends, st[ends - 1])
        else:
            lats, maint = self._write_loop(db, data, starts, ends, st[ends - 1], compact_every)
        t1 = time.perf_counter()
        if _has_flush():  # fast write path (ADR 0009): memtable -> segment files
            db.flush()
        db.compact()
        db.gc(grace="0s")
        t2 = time.perf_counter()
        size, seg_bytes, nfiles = self._sizes()
        del db
        self._open()
        complete = rows == len(st)
        # concurrent producers commit the batches in nondeterministic order
        self.exact = mode == "per-batch" and complete and self.producers == 1
        if self.exact:
            self._verify_system_time(seq, st)
        return {"mode": mode, "writes": int(len(starts)), "rows": rows,
                "batches_total": int(n_batches), "rows_total": int(len(st)), "complete": complete,
                "setup_seconds": round(setup_s, 3),
                "write_seconds": t1 - t0, "finalize_seconds": t2 - t1, "wall_seconds": t2 - t0,
                "maintenance_seconds": round(maint, 3),
                "write_latency_ms": latency_summary(lats),
                "bytes_on_disk": size, "segment_bytes": seg_bytes, "log_bytes": size - seg_bytes,
                "files": nfiles, "exact_system_time": self.exact,
                # fsync (count, ns) per category inside db.write only (bench/fsyncstat.c)
                "write_fsync": None if self.write_fsync is None else self.write_fsync.tolist(),
                "group": self.group, "producers": self.producers, "calls": calls,
                "fast_write_path": _has_flush()}

    def _write_many_loop(self, db, data, starts, ends, clocks, compact_every):
        """``write_many`` over groups of ``group`` original batches; latency per call."""
        lats, maint, calls, done = [], 0.0, 0, 0
        self.write_fsync = None
        n = len(starts)
        for g in range(0, n, self.group):
            idx = range(g, min(g + self.group, n))
            items = [{"data": data.slice(int(starts[i]), int(ends[i] - starts[i])),
                      "clock": int(clocks[i])} for i in idx]
            t0 = time.perf_counter()
            res = db.write_many(items)
            lats.append(time.perf_counter() - t0)
            calls += 1
            for i, r in zip(idx, res):
                assert not isinstance(r, BaseException), r
                assert r.stored == ends[i] - starts[i], (r, i)
            before, done = done, done + len(items)
            if compact_every and done // compact_every > before // compact_every:
                t1 = time.perf_counter()
                db.compact()
                db.gc(grace="0s")
                maint += time.perf_counter() - t1
        return lats, maint, calls

    def _threaded_loop(self, db, data, starts, ends, clocks):
        """``producers`` threads take the original batches in order from a shared counter
        and ``write`` them (each blocks until durable); latency per write."""
        import threading

        lock = threading.Lock()
        nxt = [0]
        lats: list[float] = []
        errors: list[BaseException] = []
        self.write_fsync = None

        def work():
            mine = []
            try:
                while True:
                    with lock:
                        i = nxt[0]
                        nxt[0] += 1
                    if i >= len(starts):
                        break
                    a, b = int(starts[i]), int(ends[i])
                    t0 = time.perf_counter()
                    r = db.write(data.slice(a, b - a), clock=int(clocks[i]))
                    mine.append(time.perf_counter() - t0)
                    assert r.stored == b - a, (r, a, b)
            except BaseException as e:  # noqa: BLE001
                errors.append(e)
            with lock:
                lats.extend(mine)

        ts = [threading.Thread(target=work) for _ in range(self.producers)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        if errors:
            raise errors[0]
        return lats, 0.0

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
            lf = q.get("lead_filter")
            kw = {}
            if lf and op == "history" and self.lead_pushdown:
                # min <= value_time - column < max, nulls excluded: evaluated by the engine
                kw["lead"] = (lf["column"], int(lf["min"]), int(lf["max"]))
            res = getattr(db, op)(tbl, series=series, value_from=q.get("value_from"),
                                  value_to=q.get("value_to"), clock=q.get("clock"),
                                  frontier=q.get("frontier"), system=q.get("system"), **kw)
            if lf and not kw:
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
