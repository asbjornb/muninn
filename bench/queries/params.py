"""Materialise the workload templates (workload.json) into concrete queries for a dataset.

Deterministic: every random choice comes from ``rng(seed, crc32(query id), instance)``.
Concrete queries are plain dicts in the shape documented in competitors/base.py
(timestamps and durations as int µs).
"""

from __future__ import annotations

import copy
import json
import re
import zlib
from pathlib import Path

import numpy as np

DAY = 86_400_000_000
UNITS = {"us": 1, "ms": 1_000, "s": 1_000_000, "m": 60_000_000, "h": 3_600_000_000, "d": DAY}
WORKLOAD = Path(__file__).with_name("workload.json")
TINY_REPEAT_CAP = 6


def dur(text: str) -> int:
    m = re.fullmatch(r"(-?\d+)(us|ms|s|m|h|d)", text.strip())
    if not m:
        raise ValueError(f"bad duration {text!r}")
    return int(m.group(1)) * UNITS[m.group(2)]


def expr(text: str, env: dict) -> int:
    """'anchor', 'anchor+1d', 'anchor-14h'."""
    m = re.fullmatch(r"([a-z_]+)(?:([+-])(\d+(?:us|ms|s|m|h|d)))?", text.strip())
    if not m:
        raise ValueError(f"bad time expression {text!r}")
    base = env[m.group(1)]
    if m.group(2):
        d = dur(m.group(3))
        base += d if m.group(2) == "+" else -d
    return base


def load_workload(path: Path = WORKLOAD) -> list[dict]:
    return json.loads(path.read_text())["queries"]


def anchors(catalog: dict) -> dict:
    st = catalog["stats"]
    return {
        "vt_min": st["value_time_min"] // DAY * DAY,
        "vt_max": st["value_time_max"],
        "now": catalog["now"],
    }


def _series_with_data(catalog) -> list[str]:
    # the generator emits data for every series except possibly some sparse ones; the
    # catalog lists all, so pick among those ids that have data recorded in stats
    return [s["name"] for s in catalog["series"]]


def materialise(template: dict, catalog: dict, scale: str) -> list[dict]:
    t = copy.deepcopy(template)
    if scale == "tiny":
        t.update(t.pop("tiny", {}))
        t["repeat"] = min(t.get("repeat", 1), TINY_REPEAT_CAP)
    t.pop("tiny", None)
    env0 = anchors(catalog)
    table = catalog["tables"][0]["name"]
    names = _series_with_data(catalog)
    out = []
    for i in range(t.get("repeat", 1)):
        g = np.random.default_rng([catalog["seed"], zlib.crc32(t["id"].encode()), i])
        env = dict(env0)
        q = {"id": t["id"], "instance": i, "dataset": t["dataset"], "op": t["op"],
             "table": table, "clock": t.get("clock"), "series": None, "system": None}
        if "series" in t:
            k = t["series"]["pick"]
            q["series"] = sorted(g.choice(names, size=min(k, len(names)), replace=False).tolist())
        if "range" in t:
            r = t["range"]
            ln = dur(r["len"])
            lo, hi = (expr(x, env) for x in r["within"])
            lo_d, hi_d = lo // DAY, max(lo // DAY, hi // DAY)
            start = int(g.integers(lo_d, hi_d + 1)) * DAY
            q["value_from"], q["value_to"] = start, start + ln
            env["range_start"] = start
        if "frontier" in t:
            q["frontier"] = _time(t["frontier"], env, g)
        if "system" in t:
            q["system"] = _time(t["system"], env, g)
        if "frontiers" in t:
            f = t["frontiers"]
            at = dur(f["daily_at"])
            first = expr(f["first"], env) // DAY
            last = expr(f["last"], env) // DAY
            days = list(range(first, last + 1))
            n = min(f["count"], len(days))
            off = int(g.integers(0, len(days) - n + 1))
            q["frontiers"] = [d * DAY + at for d in days[off:off + n]]
            q["window_start"], q["window_end"] = (dur(x) for x in t["window"])
        if "replay" in t:
            r = t["replay"]
            f = _time(r["from"], env, g)
            q["from"], q["to"] = f, f + dur(r["len"])
        if "lead_filter" in t:
            lf = t["lead_filter"]
            q["lead_filter"] = {"column": lf["column"], "min": dur(lf["min"]), "max": dur(lf["max"])}
        if "left" in t:
            q["left"] = _left(t["left"], env, g, names)
        out.append(q)
    return out


def _time(spec, env, g) -> int:
    if isinstance(spec, str):
        return expr(spec, env)
    lo, hi = (expr(x, env) for x in spec["random"])
    hi = max(lo, hi)
    # whole seconds, like a decision clock
    return int(g.integers(lo // 1_000_000, hi // 1_000_000 + 1)) * 1_000_000


def _left(spec, env, g, names) -> dict:
    lo, hi = (expr(x, env) for x in spec["within"])
    if "rows" in spec:
        n = spec["rows"]
        align = dur(spec.get("align", "1h"))
        series = g.choice(names, size=n).tolist()
        vt = (g.integers(lo // align, hi // align + 1, n) * align).tolist()
        a, b = (dur(x) for x in spec["lead"])
        lead = (g.integers(a // 1_000_000, b // 1_000_000 + 1, n) * 1_000_000).tolist()
        return {"series": series, "value_time": vt, "frontier": [v - d for v, d in zip(vt, lead)]}
    s = g.choice(names, size=spec["series_pick"], replace=False).tolist()
    step, ln = dur(spec["every"]), dur(spec["len"])
    start = int(g.integers(lo // DAY, max(lo // DAY, hi // DAY) + 1)) * DAY
    vts = list(range(start, start + ln, step))
    out = {"series": [], "value_time": [], "frontier": []}
    for name in s:
        for v in vts:
            out["series"].append(name)
            out["value_time"].append(v)
            out["frontier"].append(expr(spec["frontier"], {"vt": v}))
    return out


def workload_for(dataset: str, catalog: dict, scale: str, only: set | None = None) -> list[dict]:
    qs = []
    for t in load_workload():
        if t["dataset"] != dataset or (only and t["id"] not in only):
            continue
        qs.extend(materialise(t, catalog, scale))
    return qs
