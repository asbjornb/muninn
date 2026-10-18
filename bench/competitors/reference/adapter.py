"""The oracle: SPEC's Python reference implementation (spec/reference/muninn_ref).

Loading bypasses ``Database.write`` (its dedup check is O(n²) by design) and appends
``Version`` objects directly, with the system_time/seq the generator assigned under the
SPEC §4.2 rules. That the generator's assignment equals what ``write`` would do is checked
separately (tests/test_scenario_export.py replays the exported write stream through the
reference's own scenario runner).
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ..base import Adapter, result_fields

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "spec" / "reference"))

from muninn_ref import Database, Version, format_timestamp  # noqa: E402


def _ts(x):
    return None if x is None else format_timestamp(int(x))


def _dur(us):
    return None if us is None else f"{int(us)}us"


def _replay_is_left_open() -> bool:
    """SPEC §6.3 replay is moving from [from, to) to (from, to]. Probe which one the
    reference in this checkout implements so the oracle always answers (from, to]."""
    db = Database()
    db.create_table("t", [{"name": "v", "type": "f64"}])
    db.create_series("s", "t")
    db.write([{"series": "s", "value_time": "2000-01-01T00:00:00Z",
               "received_time": "2000-01-01T00:00:00Z", "values": {"v": 1.0}}],
             "2000-01-01T00:00:00Z")
    rows = db.replay("t", from_="2000-01-01T00:00:00Z", to="2000-01-01T00:00:00.000001Z")
    return len(rows) == 0  # (from, to] excludes the version at `from`


class ReferenceAdapter(Adapter):
    name = "reference"
    notes = "Pure-Python oracle (full scan per read); only for tiny-scale correctness checks."

    def load(self, dataset_dir: Path, catalog: dict) -> dict:
        t0 = time.perf_counter()
        db = Database()
        self.catalog = catalog
        table = catalog["tables"][0]
        self.table = table["name"]
        self.columns = [c["name"] for c in table["columns"]]
        self.col_types = {c["name"]: c["type"] for c in table["columns"]}
        db.create_table(table["name"], table["columns"], table["default_clock"],
                        table["partition"])
        for s in catalog["series"]:
            sid = db.create_series(s["name"], s["table"], s["tags"], resolution=s["resolution"])
            assert sid == s["id"]
        t = pq.read_table(dataset_dir / "versions.parquet")
        cols = {}
        for name in t.column_names:
            c = t.column(name)
            if pa.types.is_timestamp(c.type):
                c = c.cast(pa.int64())
            cols[name] = c.to_pylist()
        n = t.num_rows
        for i in range(n):
            db.versions.append(Version(
                series=cols["series_id"][i],
                value_time=cols["value_time"][i],
                published_time=cols["published_time"][i],
                received_time=cols["received_time"][i],
                deleted=cols["deleted"][i],
                values={c: cols[c][i] for c in self.columns},
                system_time=cols["system_time"][i],
                seq=cols["seq"][i],
            ))
            assert db.versions[-1].seq == len(db.versions)
        self.db = db
        self.left_open_replay = _replay_is_left_open()
        return {"ingest_seconds": time.perf_counter() - t0, "bytes_on_disk": 0}

    def run(self, q: dict) -> pa.Table:
        db, op = self.db, q["op"]
        common = dict(series=q.get("series"), clock=q.get("clock"), system=_ts(q.get("system")))
        if op in ("latest", "history"):
            rows = getattr(db, op)(q["table"], value_from=_ts(q.get("value_from")),
                                   value_to=_ts(q.get("value_to")),
                                   frontier=_ts(q.get("frontier")), **common)
            lf = q.get("lead_filter")
            if lf:
                rows = [r for r in rows if r["values"][lf["column"]] is not None
                        and lf["min"] <= r["value_time"] - r["values"][lf["column"]] < lf["max"]]
        elif op == "replay":
            lo, hi = q.get("from"), q.get("to")
            if not self.left_open_replay:
                # reference still implements [from, to): (a, b] == [a+1us, b+1us) on ints
                lo = None if lo is None else lo + 1
                hi = None if hi is None else hi + 1
            rows = db.replay(q["table"], value_from=_ts(q.get("value_from")),
                             value_to=_ts(q.get("value_to")), from_=_ts(lo), to=_ts(hi), **common)
        elif op == "grid":
            rows = db.grid(q["table"], [_ts(f) for f in q["frontiers"]],
                           value_from=_ts(q.get("value_from")), value_to=_ts(q.get("value_to")),
                           window_start=_dur(q.get("window_start")),
                           window_end=_dur(q.get("window_end")), **common)
        elif op == "asof_join":
            left = q["left"]
            rows = db.asof_join(q["table"], [
                {"series": s, "value_time": _ts(v), "frontier": _ts(f)}
                for s, v, f in zip(left["series"], left["value_time"], left["frontier"])],
                clock=q.get("clock"), system=_ts(q.get("system")))
        else:
            raise ValueError(op)
        return self._to_arrow(op, rows)

    def _to_arrow(self, op, rows) -> pa.Table:
        fields = result_fields(op, self.columns)
        data = {f: [] for f in fields}
        for r in rows:
            vals = r.get("values")
            for f in fields:
                if f in self.columns:
                    data[f].append(None if vals is None else vals[f])
                else:
                    data[f].append(r[f])
        types = {"series": pa.string(), "found": pa.bool_(), "deleted": pa.bool_()}
        col_arrow = {"f64": pa.float64(), "i64": pa.int64(), "string": pa.string(),
                     "bool": pa.bool_(), "timestamp": pa.int64()}
        arrays = []
        for f in fields:
            t = col_arrow[self.col_types[f]] if f in self.columns else types.get(f, pa.int64())
            arrays.append(pa.array(data[f], type=t))
        return pa.table(arrays, names=fields)
