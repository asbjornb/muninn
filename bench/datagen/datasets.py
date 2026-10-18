"""The synthetic datasets. Each is one table in its own database (see common.py).

Every generator emits batches in commit-clock order, one commit-day per chunk, and derives
all randomness from ``rng(seed, dataset, ...keys)`` so results do not depend on chunking.
Values are rounded the way real feeds are (0.01 EUR/MWh, 0.1 MW, 0.001 MWh) so that
compression numbers mean something.
"""

from __future__ import annotations

import itertools

import numpy as np

from .common import (DAY, H, MIN, MS, NULL_TS, S, Batch, SeriesSpec, TableSpec, VersionWriter,
                     rng, ts)

# "now" of every live dataset = START + days; the backfill covers five years before START.
START = ts(2024, 1, 1)

SCALES = {
    #            da_prices          forecasts                         settlement        sparse            backfill
    "tiny":   dict(da=(2, 10),   fc=dict(series=3, runs=4, horizon_days=1, days=6),
                   st=(1, 110),   sp=(60, 120),   bf=dict(series=1, step=DAY)),
    "small":  dict(da=(20, 365), fc=dict(series=2, runs=24, horizon_days=10, days=60),
                   st=(10, 365), sp=(20_000, 365), bf=dict(series=10, step=H)),
    "medium": dict(da=(40, 730), fc=dict(series=3, runs=24, horizon_days=10, days=365),
                   st=(40, 365), sp=(150_000, 365), bf=dict(series=60, step=H)),
    "large":  dict(da=(60, 1825), fc=dict(series=24, runs=24, horizon_days=10, days=365),
                   st=(200, 730), sp=(1_000_000, 365), bf=dict(series=1000, step=H)),
}

ZONES = ["DK1", "DK2", "SE1", "SE2", "SE3", "SE4", "NO1", "NO2", "NO3", "NO4", "NO5", "FI",
         "DE_LU", "NL", "BE", "FR", "AT", "PL", "CZ", "HU", "RO", "BG", "SI", "HR", "EE", "LV",
         "LT", "ES", "PT", "IT_NORD", "IT_CNOR", "IT_CSUD", "IT_SUD", "IT_SICI", "IT_SARD",
         "CH", "SK", "GR", "RS", "ME"]


# Rare quirks (corrections, tombstones, duplicates, skew, ...) fire this many times more
# often at tiny scale, so the correctness check exercises every one of them.
QUIRK = 1.0


def Q(p: float) -> float:
    return min(1.0, p * QUIRK)


def zone(i: int) -> str:
    return ZONES[i] if i < len(ZONES) else f"Z{i:03d}"


def _r2(x, decimals):
    return np.round(x, decimals)


def _choice_mask(g: np.random.Generator, n: int, p: float) -> np.ndarray:
    return g.random(n) < p


# =========================================================================================
# 1. Day-ahead prices: hourly -> 15 min half way, published ~11:45 UTC D-1, few revisions.
# =========================================================================================

DA_TABLE = TableSpec("da_prices", [("value", "f64")], default_clock="published", partition="month")


def da_series(cfg):
    n, _ = cfg["da"]
    return [SeriesSpec(i + 1, f"da_{zone(i).lower()}", {"zone": zone(i), "kind": "day_ahead"}, "1h")
            for i in range(n)]


def _da_prices(seed, sid, day, switch_day):
    """Deterministic delivery-day prices for one zone (independent of revisions)."""
    g = rng(seed, 1, sid, day)
    step = 15 * MIN if day >= switch_day else H
    n = DAY // step
    hours = np.arange(n) * step / H
    base = 40 + 15 * np.sin(2 * np.pi * (day % 365) / 365) + (sid % 7) * 3
    shape = 18 * np.sin(np.pi * (hours - 6) / 12) * (hours > 6) * (hours < 22) - 8 * (hours < 5)
    level = base + 12 * g.standard_normal()
    noise = np.cumsum(g.standard_normal(n)) * (2.0 if step == H else 1.0)
    spikes = (g.random(n) < 0.01) * g.exponential(150, n)
    p = level + shape + noise + spikes
    return START + day * DAY + np.arange(n) * step, _r2(p, 2)


def _da_events(seed, sid, x, n):
    """Revision events for delivery day x of one zone, decided once and deterministically."""
    g = rng(seed, 3, sid, x)
    ev = {"t": START + x * DAY + 14 * H + int(g.integers(0, 3600)) * S,  # on the delivery day
          "t2": START + (x + 1) * DAY + 9 * H + int(g.integers(0, 3600)) * S}  # the day after
    u = g.random(4)
    if u[0] < Q(0.015):  # price correction: a few points change
        k = int(g.integers(1, 5))
        ev["corr_idx"] = np.sort(g.choice(n, k, replace=False))
        ev["corr_delta"] = g.normal(0, 5, k)
    ev["repush"] = u[1] < Q(0.02)  # identical re-push with a new received time
    ev["tomb"] = int(g.integers(0, n)) if u[2] < Q(0.002) else None  # retraction, re-issued next day
    ev["pubcorr"] = int(g.integers(1, 20)) * MIN if u[3] < Q(0.01) else None  # true pub was earlier
    # silent correction: values change, the vendor keeps the original published time (FAQ);
    # under the published clock this ties with the original and system_time/seq decide
    ev["silent"] = int(g.integers(0, n)) if g.random() < Q(0.01) else None
    return ev


def _da_initial_times(seed, sid, d):
    """(published or NULL_TS, received, c, dup) of the initial auction batch on day d."""
    g = rng(seed, 2, sid, d)
    pub = START + d * DAY + 11 * H + 45 * MIN + int(g.integers(0, 180)) * S
    rec = pub + int(g.integers(30, 600)) * S
    if g.random() < Q(0.01):
        pub = rec + 2 * MIN  # quirk: published > received (vendor clock skew)
    pubv = NULL_TS if g.random() < 0.05 else pub  # quirk: unknown published time
    c = rec + int(g.integers(1, 5)) * S
    return pubv, rec, c, g.random() < Q(0.005)


def gen_da(cfg, seed, w: VersionWriter):
    n_zones, days = cfg["da"]
    switch = days // 2

    def pub_of(sid, d):
        return _da_initial_times(seed, sid, d)[0]
    for d in range(days):
        chunk = Batch(DA_TABLE)
        bn = itertools.count()
        for sid in range(1, n_zones + 1):
            # --- initial auction result for delivery day d+1, published on day d ------------
            vt, price = _da_prices(seed, sid, d + 1, switch)
            pubv, rec, c, dup = _da_initial_times(seed, sid, d)
            chunk.add(series_id=sid, value_time=vt, published_time=pubv, received_time=rec,
                      c=c, batch=next(bn), values={"value": price})
            if dup:  # exact duplicate re-send (dropped by dedup, SPEC 4.3)
                chunk.add(series_id=sid, value_time=vt, published_time=pubv, received_time=rec,
                          c=c + 20 * MIN, batch=next(bn), dup=True, values={"value": price})
            # --- revisions of delivery day d (published yesterday) --------------------------
            if d >= 1:
                vt0, price0 = _da_prices(seed, sid, d, switch)
                ev = _da_events(seed, sid, d, len(vt0))
                t = ev["t"]
                if "corr_idx" in ev:
                    idx = ev["corr_idx"]
                    chunk.add(series_id=sid, value_time=vt0[idx], published_time=t - 5 * MIN,
                              received_time=t, c=t + S, batch=next(bn),
                              values={"value": _r2(price0[idx] + ev["corr_delta"], 2)})
                    price0 = price0.copy()
                    price0[idx] = _r2(price0[idx] + ev["corr_delta"], 2)
                if ev["silent"] is not None:
                    i = ev["silent"]
                    chunk.add(series_id=sid, value_time=vt0[i:i + 1], published_time=pub_of(sid, d - 1),
                              received_time=t + 10 * MIN, c=t + 10 * MIN + S, batch=next(bn),
                              values={"value": _r2(price0[i:i + 1] + 1.5, 2)})
                if ev["repush"]:
                    chunk.add(series_id=sid, value_time=vt0, published_time=NULL_TS,
                              received_time=t + 2 * H, c=t + 2 * H + S, batch=next(bn),
                              values={"value": price0})
                if ev["tomb"] is not None:
                    i = ev["tomb"]
                    chunk.add(series_id=sid, value_time=vt0[i:i + 1], published_time=t + 30 * MIN,
                              received_time=t + 31 * MIN, c=t + 31 * MIN + S, batch=next(bn),
                              deleted=True, values={"value": np.array([np.nan])})
            # --- the day after delivery: published-time correction, re-issue after retraction
            if d >= 2:
                vt1, price1 = _da_prices(seed, sid, d - 1, switch)
                ev = _da_events(seed, sid, d - 1, len(vt1))
                if "corr_idx" in ev:
                    price1 = price1.copy()
                    price1[ev["corr_idx"]] = _r2(price1[ev["corr_idx"]] + ev["corr_delta"], 2)
                t2 = ev["t2"]
                if ev["pubcorr"] is not None:
                    true_pub = START + (d - 2) * DAY + 11 * H + 45 * MIN - ev["pubcorr"]
                    chunk.add(series_id=sid, value_time=vt1, published_time=true_pub,
                              received_time=t2, c=t2 + S, batch=next(bn), values={"value": price1})
                if ev["tomb"] is not None:
                    i = ev["tomb"]
                    chunk.add(series_id=sid, value_time=vt1[i:i + 1], published_time=t2 + MIN,
                              received_time=t2 + 2 * MIN, c=t2 + 2 * MIN + S, batch=next(bn),
                              values={"value": price1[i:i + 1]})
        w.commit(chunk, START + (d + 1) * DAY)


# =========================================================================================
# 2. Wind/solar forecasts: 15-min, hourly runs, 10-day horizon, several providers.
# =========================================================================================

FC_TABLE = TableSpec("forecasts", [("value", "f64"), ("issue_time", "timestamp")],
                     default_clock="received", partition="day")
PROVIDERS = ["meteo_a", "meteo_b", "meteo_c", "meteo_d"]
FC_AREAS = ["dk1", "dk2", "de", "nl", "fr", "se3", "no2", "pl"]


def fc_series(cfg):
    out = []
    combos = itertools.product(FC_AREAS, ["wind", "solar"], PROVIDERS)
    for i, (area, kind, prov) in enumerate(itertools.islice(combos, cfg["fc"]["series"])):
        out.append(SeriesSpec(i + 1, f"fc_{prov}_{kind}_{area}",
                              {"provider": prov, "kind": kind, "area": area}, "15m"))
    return out


def _fc_truth(seed, sid, kind, n):
    g = rng(seed, 10, sid)
    cap = 1000 + 400 * (sid % 5)
    if kind == "wind":
        x = np.empty(n)
        e = g.standard_normal(n) * 0.06
        x[0] = 0.0
        # AR(1) at 15-min resolution; long memory gives realistic weather regimes
        for i in range(1, n):  # n ~ 40k, once per series
            x[i] = 0.997 * x[i - 1] + e[i]
        return cap / (1 + np.exp(-(x * 2.5 - 0.3))), cap
    t = np.arange(n) * 15 / 60 % 24
    day = np.arange(n) // 96
    sun = np.clip(np.sin(np.pi * (t - 6) / 12), 0, None) * (0.6 + 0.4 * np.sin(2 * np.pi * (day - 80) / 365))
    clouds = np.repeat(g.beta(2, 2, n // 96 + 1), 96)[:n]
    return cap * sun * (0.3 + 0.7 * clouds), cap


def gen_fc(cfg, seed, w: VersionWriter, series):
    fc = cfg["fc"]
    runs, hd, days = fc["runs"], fc["horizon_days"], fc["days"]
    run_step = DAY // runs
    n_tot = (days + hd + 1) * 96
    pts = hd * 96
    state = []
    for s in series:
        kind = s.tags["kind"]
        truth, cap = _fc_truth(seed, s.id, kind, n_tot)
        z = rng(seed, 11, s.id).standard_normal(n_tot)
        state.append((s, kind, truth, cap, z))
    prov_idx = {p: i for i, p in enumerate(PROVIDERS)}
    rho = 0.985 if runs >= 24 else 0.9
    for d in range(days):
        chunk = Batch(FC_TABLE)
        bn = itertools.count()
        for r in range(runs):
            issue = START + d * DAY + r * run_step
            i0 = (issue - START) // (15 * MIN)
            sl = slice(i0, i0 + pts)
            lead = np.arange(pts) / pts
            for s, kind, truth, cap, z in state:
                g = rng(seed, 12, s.id, d, r)
                # error evolves smoothly between runs: AR(1) per target point
                z[sl] = rho * z[sl] + np.sqrt(1 - rho * rho) * g.standard_normal(pts)
                if g.random() < Q(0.01):
                    continue  # provider skipped a run
                sigma = cap * (0.015 + 0.12 * lead)
                val = truth[sl] + sigma * z[sl]
                if kind == "solar":
                    val = np.where(truth[sl] <= 0, 0.0, val)
                val = _r2(np.clip(val, 0, cap), 1)
                p = prov_idx[s.tags["provider"]] % 3
                if p == 0:  # stamps the run-cycle hour as "published" (tell-tale quirk)
                    pub = issue
                    rec = issue + int(g.integers(45, 75)) * MIN + int(g.integers(0, 60)) * S
                elif p == 1:  # true publication time
                    pub = issue + 50 * MIN + int(g.integers(0, 300)) * S
                    rec = pub + int(g.integers(60, 180)) * S
                    if g.random() < Q(0.005):
                        pub = rec + int(g.integers(1, 120)) * S  # published > received
                else:  # never tells us
                    pub = NULL_TS
                    rec = issue + int(g.integers(60, 90)) * MIN
                if g.random() < Q(0.005):
                    rec += int(g.integers(3, 6)) * H  # late delivery (out of order)
                c = rec + int(g.integers(1, 3)) * S
                vt = START + np.arange(i0, i0 + pts, dtype=np.int64) * 15 * MIN
                b = next(bn)
                chunk.add(series_id=s.id, value_time=vt, published_time=pub, received_time=rec,
                          c=c, batch=b, values={"value": val, "issue_time": issue})
                u = g.random()
                if u < Q(0.003):  # exact duplicate re-send
                    chunk.add(series_id=s.id, value_time=vt, published_time=pub, received_time=rec,
                              c=c + 10 * MIN, batch=next(bn), dup=True,
                              values={"value": val, "issue_time": issue})
                elif u < Q(0.003) + Q(0.0005):  # provider retracts an 8-point stretch of the run
                    o = int(g.integers(0, pts - 8))
                    chunk.add(series_id=s.id, value_time=vt[o:o + 8], published_time=NULL_TS,
                              received_time=rec + 30 * MIN, c=c + 30 * MIN, batch=next(bn),
                              deleted=True, values={"value": np.full(8, np.nan),
                                                    "issue_time": np.full(8, NULL_TS)})
        w.commit(chunk, START + (d + 1) * DAY)


# =========================================================================================
# 3. Settlement / metered actuals: 15 min, revised at D+1, D+5 and M+3.
# =========================================================================================

ST_TABLE = TableSpec("settlement", [("value", "f64"), ("status", "string")],
                     default_clock="received", partition="month")


def st_series(cfg):
    n, _ = cfg["st"]
    return [SeriesSpec(i + 1, f"mga_{100 + i}_consumption", {"mga": str(100 + i), "kind": "consumption"},
                       "15m") for i in range(n)]


def _st_truth(seed, sid, day):
    g = rng(seed, 20, sid, day)
    t = np.arange(96) / 4
    base = 50 + 10 * (sid % 9)
    prof = base * (0.7 + 0.3 * np.sin(np.pi * (t - 5) / 14).clip(0)) * (1 + 0.1 * np.cos(2 * np.pi * day / 365))
    return prof * (1 + 0.03 * g.standard_normal(96))


def _month_index(day):
    d = np.datetime64(START, "us") + np.timedelta64(int(day * DAY), "us")
    y, m = int(str(d)[:4]), int(str(d)[5:7])
    return y, m


def gen_st(cfg, seed, w: VersionWriter, series):
    _, days = cfg["st"]
    # month of each value day, and the commit day of its M+3 run (15th, 3 months after)
    month_days: dict[tuple, list[int]] = {}
    for day in range(days):
        month_days.setdefault(_month_index(day), []).append(day)
    m3_commit = {}
    for (y, m), ds in month_days.items():
        y2, m2 = (y, m + 3) if m + 3 <= 12 else (y + 1, m + 3 - 12)
        cd = (ts(y2, m2, 15) - START) // DAY  # the 15th of the third month after
        m3_commit.setdefault(cd, []).append(ds)
    for d in range(1, days + 1):  # commit day; "now" = START + days
        chunk = Batch(ST_TABLE)
        bn = itertools.count()
        for s in series:
            sid = s.id
            # D+1: first estimate for day d-1
            x = d - 1
            if 0 <= x < days:
                g = rng(seed, 21, sid, x)
                truth = _st_truth(seed, sid, x)
                v1 = _r2(truth * (1 + 0.03 * g.standard_normal(96)), 3)
                pub = START + d * DAY + 9 * H
                rec = pub + int(g.integers(60, 1800)) * S
                vt = START + x * DAY + np.arange(96) * 15 * MIN
                pubv = NULL_TS if g.random() < 0.1 else pub
                chunk.add(series_id=sid, value_time=vt, published_time=pubv,
                          received_time=rec, c=rec + S, batch=next(bn),
                          values={"value": v1, "status": "D+1"})
                if g.random() < Q(0.005):  # exact duplicate re-send
                    chunk.add(series_id=sid, value_time=vt, published_time=pubv,
                              received_time=rec, c=rec + H, batch=next(bn), dup=True,
                              values={"value": v1, "status": "D+1"})
            # D+5: full re-send, ~40% of points changed, rare tombstones (meter fault)
            x = d - 5
            if 0 <= x < days:
                g = rng(seed, 22, sid, x)
                truth = _st_truth(seed, sid, x)
                v1 = _r2(truth * (1 + 0.03 * rng(seed, 21, sid, x).standard_normal(96)), 3)
                changed = g.random(96) < 0.4
                v5 = np.where(changed, _r2(truth * (1 + 0.01 * g.standard_normal(96)), 3), v1)
                tomb = g.random(96) < Q(0.0005)
                v5 = np.where(tomb, np.nan, v5)
                status = np.where(tomb, None, "D+5").astype(object)
                pub = START + d * DAY + 9 * H
                rec = pub + int(g.integers(60, 3600)) * S
                vt = START + x * DAY + np.arange(96) * 15 * MIN
                chunk.add(series_id=sid, value_time=vt, published_time=pub, received_time=rec,
                          c=rec + S, batch=next(bn), deleted=tomb,
                          values={"value": v5, "status": status})
            # M+3: final values for a whole month
            for ds in m3_commit.get(d, []):
                g = rng(seed, 23, sid, ds[0])
                pub = START + d * DAY + 12 * H
                for x in ds:
                    vt = START + x * DAY + np.arange(96) * 15 * MIN
                    rec = pub + int(g.integers(60, 7200)) * S
                    chunk.add(series_id=sid, value_time=vt,
                              published_time=NULL_TS if g.random() < 0.5 else pub,
                              received_time=rec, c=rec + S, batch=next(bn),
                              values={"value": _r2(_st_truth(seed, sid, x), 3), "status": "M+3"})
        w.commit(chunk, START + (d + 1) * DAY)


# =========================================================================================
# 4. Many small sparse series: REMIT-style outage messages, irregular, revised.
# =========================================================================================

SP_TABLE = TableSpec("sparse", [("value", "f64"), ("value_end", "timestamp")],
                     default_clock="published", partition="month")


def sp_series(cfg):
    n, _ = cfg["sp"]
    return [SeriesSpec(i + 1, f"umm_unit_{i:07d}", {"kind": "unavailability",
                                                   "area": FC_AREAS[i % len(FC_AREAS)]}, None)
            for i in range(n)]


def gen_sp(cfg, seed, w: VersionWriter, series):
    n, days = cfg["sp"]
    gs = rng(seed, 30)
    rate = gs.gamma(0.7, 12 / 0.7, n) / days  # messages per series per day; mean ~12/period
    cap = _r2(gs.uniform(20, 1200, n), 0)
    # pool of open messages that may still be revised
    pool = {k: np.empty(0, np.int64) for k in ("sid", "vt", "next", "left", "end", "pub")}
    pool["val"] = np.empty(0)
    for d in range(days):
        chunk = Batch(SP_TABLE)
        g = rng(seed, 31, d)
        day0 = START + d * DAY
        # --- new messages -------------------------------------------------------------------
        counts = g.poisson(rate)
        sid = np.repeat(np.arange(1, n + 1), counts).astype(np.int64)
        k = len(sid)
        msg = day0 + g.integers(0, DAY // S, k) * S
        vt = msg - msg % MIN + (g.exponential(3, k) * DAY).astype(np.int64) // MIN * MIN
        end = vt + np.maximum(H, (g.exponential(1.5, k) * DAY).astype(np.int64) // MIN * MIN)
        val = _r2(cap[sid - 1] * g.uniform(0.1, 1.0, k), 1)
        left = g.geometric(0.55, k) - 1  # 0..n revisions
        nxt = msg + (g.exponential(2, k) * DAY).astype(np.int64) // S * S
        _emit_sp(chunk, g, sid, vt, msg, val, end, np.zeros(k, bool), batch0=0)
        # --- revisions due today --------------------------------------------------------------
        due = (pool["next"] < day0 + DAY) & (pool["left"] > 0)
        if due.any():
            idx = np.nonzero(due)[0]
            m = len(idx)
            rmsg = np.maximum(pool["next"][idx], day0)
            final = pool["left"][idx] == 1
            cancel = final & (g.random(m) < 0.1)  # outage cancelled -> tombstone
            rval = _r2(np.clip(pool["val"][idx] * g.uniform(0.7, 1.3, m), 0, None), 1)
            rend = pool["end"][idx] + (g.normal(0, 6, m) * H).astype(np.int64) // MIN * MIN
            rend = np.maximum(rend, pool["vt"][idx] + H)
            _emit_sp(chunk, g, pool["sid"][idx], pool["vt"][idx], rmsg, rval, rend, cancel,
                     batch0=k, orig_pub=pool["pub"][idx])
            pool["val"][idx], pool["end"][idx] = rval, rend
            pool["left"][idx] -= 1
            pool["left"][idx[cancel]] = 0
            pool["next"][idx] = rmsg + (g.exponential(2, m) * DAY).astype(np.int64) // S * S
        keep = pool["left"] > 0
        pool = {kk: np.concatenate([v[keep], new]) for (kk, v), new in
                zip(pool.items(), [sid, vt, nxt, left, end, msg, val])}
        w.commit(chunk, START + (d + 1) * DAY)


def _emit_sp(chunk, g, sid, vt, msg, val, end, deleted, batch0, orig_pub=None):
    m = len(sid)
    if m == 0:
        return
    pub = msg.copy() if orig_pub is None else np.where(g.random(m) < Q(0.05), orig_pub, msg)
    rec = msg + g.integers(5, 120, m) * S
    pub = np.where(g.random(m) < Q(0.02), NULL_TS, pub)
    chunk.add(series_id=sid, value_time=vt, published_time=pub, received_time=rec,
              c=rec + S, batch=batch0 + np.arange(m), deleted=deleted,
              values={"value": np.where(deleted, np.nan, val),
                      "value_end": np.where(deleted, NULL_TS, end)})


# =========================================================================================
# 5. Backfill: five years of history bulk-loaded at once, synthetic received times.
# =========================================================================================

BF_TABLE = TableSpec("backfill", [("value", "f64")], default_clock="received", partition="year")
BF_FROM, BF_TO = ts(2019, 1, 1), ts(2024, 1, 1)
BF_LOAD = ts(2025, 6, 1, 8)


def bf_series(cfg):
    n = cfg["bf"]["series"]
    step = cfg["bf"]["step"]
    kinds = ["load", "wind_actual", "solar_actual", "hydro_reservoir", "temperature"]
    return [SeriesSpec(i + 1, f"hist_{FC_AREAS[i % len(FC_AREAS)]}_{kinds[(i // 8) % 5]}_{i:04d}",
                       {"area": FC_AREAS[i % len(FC_AREAS)], "kind": kinds[(i // 8) % 5],
                        "source": "backfill"}, "1h" if step == H else "1d")
            for i in range(n)]


def gen_bf(cfg, seed, w: VersionWriter, series):
    step = cfg["bf"]["step"]
    n_pts = (BF_TO - BF_FROM) // step
    vt_all = BF_FROM + np.arange(n_pts, dtype=np.int64) * step
    pub_unknown_until = ts(2021, 1, 1)
    batch_no = 0
    chunk = Batch(BF_TABLE)
    rows = 0
    for s in series:
        g = rng(seed, 40, s.id)
        x = np.cumsum(g.standard_normal(n_pts)) * 0.5
        season = np.sin(2 * np.pi * np.arange(n_pts) * step / (365 * DAY))
        daily = np.sin(2 * np.pi * (np.arange(n_pts) * step % DAY) / DAY)
        val = _r2(100 + 30 * season + 10 * daily + x - x.mean(), 2)
        rec = vt_all + step + (g.integers(0, 600, n_pts) * S)
        pub = np.where((vt_all < pub_unknown_until) | (g.random(n_pts) < 0.2), NULL_TS, vt_all + step)
        corr = g.random(n_pts) < 0.05
        cval = _r2(val[corr] + g.normal(0, 3, corr.sum()), 2)
        crec = rec[corr] + 3 * DAY + g.integers(0, 3600, corr.sum()) * S
        # one batch per (series, month): originals then that month's corrections
        month = (vt_all - BF_FROM) // (30 * DAY)
        cmonth = month[corr]
        for mth in np.unique(month):
            sel = month == mth
            csel = cmonth == mth
            c = BF_LOAD + batch_no * 10 * MS
            vt_b = np.concatenate([vt_all[sel], vt_all[corr][csel]])
            dup_batch = g.random() < Q(0.005)
            for rep in range(2 if dup_batch else 1):
                chunk.add(series_id=s.id, value_time=vt_b,
                          published_time=np.concatenate([pub[sel], np.full(csel.sum(), NULL_TS)]),
                          received_time=np.concatenate([rec[sel], crec[csel]]),
                          c=c + rep * 5 * MS, batch=batch_no, dup=bool(rep),
                          values={"value": np.concatenate([val[sel], cval[csel]])})
                batch_no += 1
            rows += len(vt_b)
        if rows >= 2_000_000:
            w.commit(chunk, None)
            chunk, rows = Batch(BF_TABLE), 0
    w.commit(chunk, None)


DATASETS = {
    "da_prices": (DA_TABLE, da_series, gen_da),
    "forecasts": (FC_TABLE, fc_series, gen_fc),
    "settlement": (ST_TABLE, st_series, gen_st),
    "sparse": (SP_TABLE, sp_series, gen_sp),
    "backfill": (BF_TABLE, bf_series, gen_bf),
}


def now_of(name, cfg) -> int:
    """The dataset's 'now': the end of the live period (or the load time for the backfill)."""
    if name == "da_prices":
        return START + cfg["da"][1] * DAY
    if name == "forecasts":
        return START + cfg["fc"]["days"] * DAY
    if name == "settlement":
        return START + (cfg["st"][1] + 2) * DAY
    if name == "sparse":
        return START + cfg["sp"][1] * DAY
    return BF_LOAD + DAY
