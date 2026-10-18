"""§7 invariants, property-tested against the reference implementation.

Worlds are small (two tables, three series, four value_times, a 15-minute knowledge grid)
so that ties, duplicates and tombstones collide often.
"""

import copy
import json
import random

from hypothesis import assume, given, settings
from hypothesis import strategies as st

from muninn_ref.db import CLOCKS, Database, Version
from muninn_ref.errors import MuninnError
from muninn_ref.timefmt import format_timestamp, parse_duration, parse_timestamp
from muninn_ref.values import check_type

BASE = parse_timestamp("2025-01-01T00:00:00Z")
MIN = 60 * 10**6
HOUR = 60 * MIN

F64 = [0.0, -0.0, 1.0, 2.5, -7.0, 1e300, "NaN", "inf", "-inf", 42]
I64 = [0, 1, -1, "9007199254740993", 2**53]
STR = ["", "a", "b"]


def ts(us: int) -> str:
    return format_timestamp(us)


# ---------------------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------------------

SERIES = {"s1": "t", "s2": "t", "s3": "u"}
COLUMNS = {"t": {"a": F64, "b": I64}, "u": {"x": STR}}


def new_db() -> Database:
    db = Database()
    db.create_table("t", [{"name": "a", "type": "f64"}, {"name": "b", "type": "i64"}])
    db.create_table("u", [{"name": "x", "type": "string"}], default_clock="published")
    for name, table in SERIES.items():
        db.create_series(name, table)
    return db


grid_time = st.integers(0, 10).map(lambda k: BASE + k * 15 * MIN)
nudged_time = st.tuples(grid_time, st.sampled_from([0, 0, 0, 1, -1])).map(sum)


@st.composite
def rows(draw):
    series = draw(st.sampled_from(sorted(SERIES)))
    deleted = draw(st.integers(0, 5)) == 0
    row = {
        "series": series,
        "value_time": ts(BASE + draw(st.integers(0, 3)) * HOUR),
        "received_time": ts(draw(nudged_time)),
    }
    pt = draw(st.one_of(st.none(), nudged_time))
    if pt is not None:
        row["published_time"] = ts(pt)
    if deleted:
        row["deleted"] = True
    else:
        vals = {}
        for col, pool in COLUMNS[SERIES[series]].items():
            if draw(st.booleans()):
                vals[col] = draw(st.sampled_from(pool + [None]))
        row["values"] = vals
    return row


batch = st.tuples(nudged_time, st.lists(rows(), max_size=5))  # (writer clock, rows)
batches = st.lists(batch, max_size=8)


@st.composite
def contexts(draw):
    return {
        "clock": draw(st.sampled_from(CLOCKS)),
        "frontier": draw(st.one_of(st.none(), nudged_time)),
        "system": draw(st.one_of(st.none(), nudged_time)),
    }


# ---------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------


def apply(db: Database, bs) -> None:
    for c, rs in bs:
        db.write(rs, ts(c))


def dump(db: Database, table: str, result) -> str:
    """Bit-exact comparable form of a read result (-0.0 vs 0.0 and NaN distinguished)."""
    return json.dumps([db.encode_row(table, r) for r in result], allow_nan=False)


def opt(x):
    return None if x is None else ts(x)


def reads(db: Database, ctx: dict, seed: int) -> dict:
    """latest / history / grid / asof_join on both tables in context `ctx`. Grid frontiers
    and asof frontiers are all <= F, so §7.1 applies to them too."""
    rng = random.Random(seed)
    clock, frontier, system = ctx["clock"], ctx["frontier"], ctx["system"]
    out = {}
    for table in ("t", "u"):
        out[table, "latest"] = dump(db, table, db.latest(
            table, clock=clock, frontier=opt(frontier), system=opt(system)))
        out[table, "history"] = dump(db, table, db.history(
            table, clock=clock, frontier=opt(frontier), system=opt(system)))
        top = frontier if frontier is not None else BASE + 4 * HOUR
        fs = [top - rng.randrange(0, 8) * 15 * MIN for _ in range(3)]
        out[table, "grid"] = dump(db, table, db.grid(
            table, [ts(f) for f in fs], clock=clock, system=opt(system),
            window_start=rng.choice(["-1d", "0h", "-90m"]), window_end=rng.choice(["0h", "3h", "1d"])))
        left = [{"series": s, "value_time": ts(BASE + rng.randrange(4) * HOUR),
                 "frontier": ts(top - rng.randrange(0, 8) * 15 * MIN)}
                for s in [s for s, t in SERIES.items() if t == table] for _ in range(3)]
        out[table, "asof"] = dump(db, table, db.asof_join(table, left, clock=clock, system=opt(system)))
    return out


def invisible_batch(draw_rows, ctx: dict, rng: random.Random):
    """A batch (clock, rows) all of whose stored versions are invisible in ctx."""
    clock, frontier, system = ctx["clock"], ctx["frontier"], ctx["system"]
    options = []
    if system is not None:
        options.append("system")
    if frontier is not None:
        options.append("frontier")
    if not options:
        return None
    how = rng.choice(options)
    c = BASE + rng.randrange(0, 12) * 15 * MIN
    rs = copy.deepcopy(draw_rows)
    if how == "system":
        c = max(c, system + 1)  # system_time = max(c, last) >= c > S
    elif clock == "system":
        c = max(c, frontier + 1)  # t_system = system_time >= c > F
    else:
        for r in rs:
            late = ts(frontier + 1 + rng.randrange(0, 4) * 15 * MIN)
            if clock == "received":
                r["received_time"] = late
            else:  # published: t_published = published_time, or received_time if null
                if rng.random() < 0.5:
                    r["published_time"] = late
                else:
                    r.pop("published_time", None)
                    r["received_time"] = late
    return (c, rs)


# ---------------------------------------------------------------------------------------
# §7.1 Noninterference
# ---------------------------------------------------------------------------------------


@settings(max_examples=300, deadline=None)
@given(batches, contexts(), st.lists(st.lists(rows(), max_size=4), min_size=1, max_size=3),
       st.integers(0, 2**32))
def test_noninterference_appended_writes(base, ctx, extra, seed):
    """Appending writes whose versions are invisible in (clock, F, S) — new points, new
    versions of old points, tombstones — changes no read in that context."""
    assume(ctx["frontier"] is not None or ctx["system"] is not None)
    db = new_db()
    apply(db, base)
    before = reads(db, ctx, seed)
    rng = random.Random(seed)
    for rs in extra:
        c, invisible_rows = invisible_batch(rs, ctx, rng)
        db.write(invisible_rows, ts(c))
    assert reads(db, ctx, seed) == before


def _is_visible(v: Version, ctx: dict) -> bool:
    return Database.visible(v, ctx["clock"], ctx["frontier"], ctx["system"])


@settings(max_examples=300, deadline=None)
@given(batches, contexts(), st.integers(0, 2**32))
def test_noninterference_any_invisible_version(base, ctx, seed):
    """Stronger, at the version-set level: change every invisible version arbitrarily
    (values, deleted flag, timestamps — keeping it invisible), add invisible versions, and
    reorder the stored list (§7.2 physical independence). No read in ctx changes."""
    db = new_db()
    apply(db, base)
    before = reads(db, ctx, seed)
    rng = random.Random(seed)
    next_seq = len(db.versions) + 1
    out = []
    for v in db.versions:
        if _is_visible(v, ctx):
            out.append(v)
            continue
        out.append(_scramble(v, ctx, rng))
    for _ in range(rng.randrange(0, 4)):
        template = rng.choice(out) if out else None
        if template is None:
            break
        nv = _scramble(template, ctx, rng, seq=next_seq)
        if not _is_visible(nv, ctx):
            out.append(nv)
            next_seq += 1
    rng.shuffle(out)
    db.versions = out
    assert reads(db, ctx, seed) == before


def _scramble(v: Version, ctx: dict, rng: random.Random, seq: int | None = None) -> Version:
    """A random variant of v that stays invisible in ctx (if v was)."""
    deleted = rng.random() < 0.3
    table = SERIES[f"s{v.series}"]
    values = {} if deleted else {c: rng.choice(pool + [None]) for c, pool in COLUMNS[table].items()}
    types = {"a": "f64", "b": "i64", "x": "string"}
    values = {c: (None if x is None else check_type(types[c], x)) for c, x in values.items()}
    series = v.series if rng.random() < 0.5 else rng.choice([s for s in (1, 2, 3)
                                                             if SERIES[f"s{s}"] == table])
    new = Version(series=series,
                  value_time=v.value_time if rng.random() < 0.7 else BASE + rng.randrange(4) * HOUR,
                  published_time=v.published_time, received_time=v.received_time,
                  deleted=deleted, values=values, system_time=v.system_time,
                  seq=v.seq if seq is None else seq)
    if _is_visible(new, ctx):
        return v if seq is None else new  # keep the original if scrambling made it visible
    return new


# ---------------------------------------------------------------------------------------
# §7.4 Grid == repeated latest
# ---------------------------------------------------------------------------------------


@settings(max_examples=300, deadline=None)
@given(batches, contexts(), st.lists(nudged_time, max_size=5),
       st.one_of(st.none(), st.tuples(st.sampled_from(["-1d", "-2h", "-15m", "0h"]),
                                      st.sampled_from(["0h", "30m", "2h", "1d"]))),
       st.one_of(st.none(), st.integers(0, 4)), st.integers(0, 4))
def test_grid_equals_repeated_latest(base, ctx, fs, window, lo_h, span_h):
    db = new_db()
    apply(db, base)
    for table in ("t", "u"):
        if window is not None:
            got = db.grid(table, [ts(f) for f in fs], clock=ctx["clock"], system=opt(ctx["system"]),
                          window_start=window[0], window_end=window[1])
        else:
            vf = None if lo_h is None else ts(BASE + lo_h * HOUR)
            vt = None if lo_h is None else ts(BASE + (lo_h + span_h) * HOUR)
            got = db.grid(table, [ts(f) for f in fs], clock=ctx["clock"], system=opt(ctx["system"]),
                          value_from=vf, value_to=vt)
        expected = []
        for f in sorted(set(fs)):
            if window is not None:
                vf = ts(f + parse_duration(window[0]))
                vt = ts(f + parse_duration(window[1]))
            for r in db.latest(table, clock=ctx["clock"], frontier=ts(f), system=opt(ctx["system"]),
                               value_from=vf, value_to=vt):
                expected.append({"frontier": f, **r})
        assert dump(db, table, got) == dump(db, table, expected)


# ---------------------------------------------------------------------------------------
# §7.5 Dedup idempotency
# ---------------------------------------------------------------------------------------


@settings(max_examples=300, deadline=None)
@given(batches, batch, nudged_time, contexts(), st.integers(0, 2**32))
def test_dedup_idempotency(base, b, c2, ctx, seed):
    db = new_db()
    apply(db, base)
    db.write(b[1], ts(b[0]))
    versions = list(db.versions)
    before = reads(db, ctx, seed), reads(db, {"clock": "system", "frontier": None, "system": None}, seed)
    res = db.write(b[1], ts(c2))
    assert (res.stored, res.duplicates, res.batch_already_committed) == (0, len(b[1]), False)
    assert db.versions == versions
    after = reads(db, ctx, seed), reads(db, {"clock": "system", "frontier": None, "system": None}, seed)
    assert after == before


# ---------------------------------------------------------------------------------------
# Write atomicity: a failed batch has no effect (§4, §8)
# ---------------------------------------------------------------------------------------

CORRUPTIONS = [
    ("unknown_series", lambda r: r.update(series="nope")),
    ("unknown_column", lambda r: r.setdefault("values", {}).update(zz=1.0)),
    ("invalid_timestamp", lambda r: r.update(received_time="2025-02-30T00:00:00Z")),
    ("invalid_timestamp", lambda r: r.update(value_time="soon")),
]


@settings(max_examples=300, deadline=None)
@given(batches, st.lists(rows(), max_size=4), st.lists(rows(), max_size=4),
       st.sampled_from(CORRUPTIONS), nudged_time, batch, st.booleans())
def test_failed_batch_has_no_effect(base, pre, post, corruption, c, nxt, with_id):
    code, corrupt = corruption
    db = new_db()
    apply(db, base)
    bad = copy.deepcopy(pre[:1]) or [{"series": "s1", "value_time": ts(BASE),
                                       "received_time": ts(BASE)}]
    corrupt(bad[0])
    rows_ = pre + bad + post
    state = (list(db.versions), set(db.batch_ids))
    try:
        db.write(rows_, ts(c), batch_id="retry" if with_id else None)
    except MuninnError as e:
        assert e.code == code  # the first failing row is the corrupted one
    else:
        raise AssertionError("corrupted batch was accepted")
    assert (list(db.versions), set(db.batch_ids)) == state
    # Nothing was consumed: the next batch continues seq and uses max(c, last) as if the
    # failed batch never happened.
    last = max((v.system_time for v in db.versions), default=None)
    n = len(db.versions)
    res = db.write(nxt[1], ts(nxt[0]), batch_id="retry" if with_id else None)
    assert not res.batch_already_committed
    new = db.versions[n:]
    assert [v.seq for v in new] == list(range(n + 1, n + 1 + res.stored))
    expected_st = nxt[0] if last is None else max(nxt[0], last)
    assert all(v.system_time == expected_st for v in new)


# ---------------------------------------------------------------------------------------
# §4.2 bookkeeping invariants
# ---------------------------------------------------------------------------------------


@settings(max_examples=200, deadline=None)
@given(batches)
def test_seq_and_system_time(bs):
    db = new_db()
    last = None
    for c, rs in bs:
        n = len(db.versions)
        res = db.write(rs, ts(c))
        new = db.versions[n:]
        assert len(new) == res.stored and res.stored + res.duplicates == len(rs)
        expected = c if last is None else max(c, last)
        assert all(v.system_time == expected for v in new)
        if new:
            last = expected
    assert [v.seq for v in db.versions] == list(range(1, len(db.versions) + 1))
    sts = [v.system_time for v in db.versions]
    assert sts == sorted(sts)


# ---------------------------------------------------------------------------------------
# §7.7 Replay is the frontier delta
# ---------------------------------------------------------------------------------------


@settings(max_examples=400, deadline=None)
@given(batches, st.sampled_from(CLOCKS), st.one_of(st.none(), nudged_time),
       st.one_of(st.none(), nudged_time), st.one_of(st.none(), nudged_time),
       st.one_of(st.none(), st.lists(st.sampled_from(["s1", "s2"]), max_size=2)),
       st.one_of(st.none(), st.tuples(st.integers(0, 3), st.integers(0, 3))))
def test_replay_is_frontier_delta(base, clock, f1, f2, system, series, vrange):
    """latest at F2 == latest at F1 updated, in order, with replay(from=F1, to=F2)."""
    if f1 is not None and f2 is not None and f1 > f2:
        f1, f2 = f2, f1
    db = new_db()
    apply(db, base)
    vf = vt = None
    if vrange is not None:
        lo, hi = sorted(vrange)
        vf, vt = ts(BASE + lo * HOUR), ts(BASE + hi * HOUR)
    common = dict(series=series, value_from=vf, value_to=vt, clock=clock, system=opt(system))
    state = {}
    if f1 is not None:
        state = {(r["series"], r["value_time"]): r for r in db.latest("t", frontier=ts(f1), **common)}
    for r in db.replay("t", from_=opt(f1), to=opt(f2), **common):
        key = (r["series"], r["value_time"])
        if r["deleted"]:
            state.pop(key, None)
        else:
            state[key] = {k: v for k, v in r.items() if k != "deleted"}
    folded = sorted(state.values(),
                    key=lambda r: (db.series_by_name(r["series"]).id, r["value_time"]))
    expected = db.latest("t", frontier=opt(f2), **common)
    assert dump(db, "t", folded) == dump(db, "t", expected)
