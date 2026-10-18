"""Export a (tiny) dataset as a SPEC §9 scenario: catalog steps, the full write stream
(stored batches *and* the exact duplicate re-sends, in commit order, each with its
expected §4.5 result) and, optionally, read steps with expected rows.

Stored versions are grouped into write steps by system_time. Two generator batches that
the max(c, last) rule gave the same system_time merge into one step; that is equivalent
(same system_time, same seq order). Re-send batches are placed after the last stored batch
with system_time <= their writer clock; they store nothing, so they consume no seq.

Replay steps are left out on purpose while SPEC §6.3 moves from [from, to) to (from, to].
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ENGINE = ("system_time", "seq", "commit_clock")


def _rows_json(t: pa.Table, columns: list[dict]) -> list[dict]:
    from competitors.reference.adapter import format_timestamp

    col_types = {c["name"]: c["type"] for c in columns}
    d = {}
    for name in t.column_names:
        c = t.column(name)
        if pa.types.is_timestamp(c.type):
            c = c.cast(pa.int64())
        if pa.types.is_dictionary(c.type):
            c = c.cast(pa.string())
        d[name] = c.to_pylist()
    out = []
    for i in range(t.num_rows):
        vals = {}
        for c in columns:
            v = d[c["name"]][i]
            if v is not None and col_types[c["name"]] == "timestamp":
                v = format_timestamp(v)
            elif isinstance(v, float) and not np.isfinite(v):
                v = "NaN" if np.isnan(v) else ("inf" if v > 0 else "-inf")
            vals[c["name"]] = v
        row = {"series": d["series"][i], "value_time": format_timestamp(d["value_time"][i]),
               "published_time": None if d["published_time"][i] is None
               else format_timestamp(d["published_time"][i]),
               "received_time": format_timestamp(d["received_time"][i])}
        if d["deleted"][i]:
            row["deleted"] = True
            row["values"] = {}
        else:
            row["values"] = vals
        out.append(row)
    return out


def write_steps(dataset_dir: Path, catalog: dict, max_rows: int | None = None) -> list[dict]:
    from competitors.reference.adapter import format_timestamp

    table = catalog["tables"][0]
    v = pq.read_table(dataset_dir / "versions.parquet")
    if max_rows is not None:
        v = v.slice(0, max_rows)
    r = pq.read_table(dataset_dir / "resends.parquet")
    st = v.column("system_time").cast(pa.int64()).to_numpy()
    if max_rows is not None and len(st):
        cc = r.column("commit_clock").cast(pa.int64()).to_numpy()
        r = r.filter(pa.array(cc <= st[-1]))
    steps = [{"op": "create_table", "name": table["name"], "columns": table["columns"],
              "default_clock": table["default_clock"], "partition": table["partition"]}]
    for s in catalog["series"]:
        step = {"op": "create_series", "name": s["name"], "table": s["table"], "tags": s["tags"]}
        if s.get("resolution"):
            step["resolution"] = s["resolution"]
        steps.append(step)
    # stored batches: runs of equal system_time
    bounds = np.flatnonzero(np.diff(st)) + 1
    starts = np.concatenate([[0], bounds]) if len(st) else np.array([], int)
    ends = np.concatenate([bounds, [len(st)]]) if len(st) else np.array([], int)
    events = [(int(st[a]), 0, int(a), int(b)) for a, b in zip(starts, ends)]
    # re-sends: group rows by commit clock
    if r.num_rows:
        cc = r.column("commit_clock").cast(pa.int64()).to_numpy()
        order = np.argsort(cc, kind="stable")
        r = r.take(pa.array(order))
        cc = cc[order]
        rb = np.flatnonzero(np.diff(cc)) + 1
        for a, b in zip(np.concatenate([[0], rb]), np.concatenate([rb, [len(cc)]])):
            events.append((int(cc[a]), 1, int(a), int(b)))
    events.sort()
    for t_, kind, a, b in events:
        src = v if kind == 0 else r
        rows = _rows_json(src.slice(a, b - a), table["columns"])
        steps.append({"op": "write", "system_time": format_timestamp(t_), "rows": rows,
                      "expect": {"stored": 0 if kind else b - a, "duplicates": b - a if kind else 0,
                                 "batch_already_committed": False}})
    return steps


def read_steps(queries: list[dict], oracle, table: str) -> list[dict]:
    """Workload queries as §9 read steps, with the oracle's rows as expectation."""
    from competitors.reference.adapter import _dur, _ts

    steps = []
    for q in queries:
        if q["op"] == "replay" or q.get("lead_filter"):
            continue
        step = {"op": q["op"], "table": table}
        if q.get("series"):
            step["series"] = q["series"]
        for k in ("value_from", "value_to", "frontier", "system"):
            if q.get(k) is not None:
                step[k] = _ts(q[k])
        if q.get("clock"):
            step["clock"] = q["clock"]
        if q["op"] == "grid":
            step["frontiers"] = [_ts(f) for f in q["frontiers"]]
            if q.get("window_start") is not None:
                step["window_start"], step["window_end"] = _dur(q["window_start"]), _dur(q["window_end"])
        if q["op"] == "asof_join":
            step.pop("series", None)
            step["left"] = [{"series": s, "value_time": _ts(vt), "frontier": _ts(f)} for s, vt, f in
                            zip(q["left"]["series"], q["left"]["value_time"], q["left"]["frontier"])]
            native = oracle.db.asof_join(table, step["left"], clock=step.get("clock"),
                                         system=step.get("system"))
        else:
            from competitors.reference.adapter import ReferenceAdapter  # noqa: F401
            args = {k: step.get(k) for k in ("series", "value_from", "value_to", "clock", "system")}
            if q["op"] == "grid":
                native = oracle.db.grid(table, step["frontiers"], window_start=step.get("window_start"),
                                        window_end=step.get("window_end"), **args)
            else:
                native = getattr(oracle.db, q["op"])(table, frontier=step.get("frontier"), **args)
        step["expect"] = {"rows": [oracle.db.encode_row(table, row) for row in native]}
        steps.append(step)
    return steps


def export_scenario(dataset_dir: Path, max_rows: int | None = None, reads: bool = True,
                    path: Path | None = None) -> Path:
    from competitors.reference.adapter import ReferenceAdapter
    from queries.params import workload_for

    catalog = json.loads((dataset_dir / "catalog.json").read_text())
    steps = write_steps(dataset_dir, catalog, max_rows)
    if reads:
        oracle = ReferenceAdapter(dataset_dir)
        oracle.load(dataset_dir, catalog)
        steps += read_steps(workload_for(catalog["database"], catalog, catalog["scale"]), oracle,
                            catalog["tables"][0]["name"])
    sc = {"name": f"bench-{catalog['database']}-{catalog['scale']}-s{catalog['seed']}",
          "description": (f"Benchmark dataset '{catalog['database']}' ({catalog['scale']} scale, "
                          f"seed {catalog['seed']}) as a write stream, generated by bench/datagen; "
                          "read expectations from the reference implementation."),
          "steps": steps}
    path = path or dataset_dir / "scenario.json"
    path.write_text(json.dumps(sc, indent=None, separators=(",", ":")))
    return path
