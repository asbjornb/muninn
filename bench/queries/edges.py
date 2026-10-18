"""Boundary queries derived from the data, for the correctness check only.

Workload parameters are "natural" (10:00 decision times, random seconds) and almost never
land exactly on a version's timestamp, so they cannot tell ``<=`` from ``<``. These queries
put frontiers, system frontiers and replay bounds *exactly* on (and 1 µs before) stored
t_clock / system_time values, under every clock, and include duplicate grid frontiers,
absolute grid windows and as-of rows that miss.
"""

from __future__ import annotations

import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq

CLOCKS = ("received", "published", "system")
DAY = 86_400_000_000
H = 3_600_000_000


def edge_queries(dataset_dir, catalog, n=6, seed=0) -> list[dict]:
    t = pq.read_table(dataset_dir / "versions.parquet")
    names = t.column("series").cast("string").to_pylist()
    vt = t.column("value_time").cast("int64").to_numpy()
    rec = t.column("received_time").cast("int64").to_numpy()
    pub = t.column("published_time").cast("int64").fill_null(0).to_numpy()
    pub_null = pc.is_null(t.column("published_time")).to_numpy(zero_copy_only=False)
    syst = t.column("system_time").cast("int64").to_numpy()
    tcs = {"received": rec, "published": np.where(pub_null, rec, pub), "system": syst}
    table = catalog["tables"][0]["name"]
    g = np.random.default_rng([catalog["seed"], seed, 99])
    out = []

    def q(op, clock, **kw):
        out.append({"id": f"edge_{op}_{clock}", "instance": len(out), "dataset": catalog["database"],
                    "op": op, "table": table, "clock": clock, "series": None, "system": None, **kw})

    N = t.num_rows
    for clock in CLOCKS:
        tc = tcs[clock]
        for _ in range(n):
            i, j = (int(x) for x in g.integers(0, N, 2))
            lo = int(vt[i]) // DAY * DAY - DAY
            for f in (int(tc[i]), int(tc[i]) - 1):
                sysf = int(syst[j]) if g.random() < 0.5 else None
                q("latest", clock, series=[names[i]], value_from=lo, value_to=lo + 3 * DAY,
                  frontier=f, system=sysf)
                q("history", clock, series=[names[i]], value_from=lo, value_to=lo + 3 * DAY,
                  frontier=f, system=sysf)
            a, b = sorted((int(tc[i]), int(tc[j])))
            if b - a > 7 * DAY:
                b = a + 7 * DAY
            q("replay", clock, **{"from": a, "to": b}, system=int(syst[j]) if g.random() < 0.3 else None)
            q("replay", clock, **{"from": a - 1, "to": a})  # exactly the versions at a
            fs = sorted({int(tc[k]) for k in g.integers(0, N, 4)} | {int(tc[i])})
            q("grid", clock, series=[names[i]], frontiers=fs, window_start=-12 * H,
              window_end=24 * H, system=int(syst[j]) if g.random() < 0.3 else None)
            q("grid", clock, series=None, frontiers=fs, value_from=lo, value_to=lo + 2 * DAY)
        rows = g.integers(0, N, 40)
        left = {"series": [], "value_time": [], "frontier": []}
        for k in rows:
            k = int(k)
            for f in (int(tc[k]), int(tc[k]) - 1):
                left["series"].append(names[k])
                left["value_time"].append(int(vt[k]))
                left["frontier"].append(f)
        left["series"].append(names[0])
        left["value_time"].append(int(vt[0]) + 1)  # no such point
        left["frontier"].append(int(tc[0]))
        q("asof_join", clock, left=left, system=int(syst[int(rows[0])]))
        q("asof_join", clock, left=left)
    return out
