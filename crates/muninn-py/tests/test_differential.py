"""Differential test: random catalogs, writes and reads through the Python bindings and
through the pure-Python reference (`spec/reference/muninn_ref`) must give identical
results — the same rows bit for bit, the same write results, the same error codes.

Worlds are small (two tables, three series, four value times, a 15-minute knowledge grid)
so that ties, duplicates, tombstones and resends collide often. Some inputs are invalid on
purpose (unknown series / columns, mismatched types, bad timestamps, reversed ranges).
"""

import json
import os

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from adapter import PyMuninn
from muninn_ref.db import CLOCKS, Database
from muninn_ref.errors import MuninnError
from muninn_ref.timefmt import format_timestamp, parse_timestamp

BASE = parse_timestamp("2025-01-01T00:00:00Z")
MIN = 60 * 10**6
HOUR = 60 * MIN

F64 = [0.0, -0.0, 1.0, 2.5, -7.0, 1e300, "NaN", "inf", "-inf", 42]
I64 = [0, 1, -1, "9007199254740993", 2**53, 2**62, -(2**63)]
STR = ["", "a", "b"]
TS = ["2025-01-01T00:00:00Z", "2024-12-31T23:59:59.999999Z", "2025-01-01T01:00:00+01:00"]
BOOL = [True, False]
# One of these is injected into some batches (each rejects the whole batch).
INVALID = [
    {"series": "nope"},
    {"values": {"zzz": 1}},
    {"values": {"a": "1.5"}},
    {"values": {"b": 1.5}},
    {"values": {"b": "x"}},
    {"values": {"c": "bad"}},
    {"value_time": "bad"},
    {"received_time": "2025-02-30T00:00:00Z"},
    {"deleted": True, "values": {"a": 1.0}},
]

SERIES = {"s1": "t", "s2": "t", "s3": "u"}
COLUMNS = {
    "t": {"a": F64, "b": I64, "c": TS},
    "u": {"x": STR, "y": BOOL},
}


def ts(us: int) -> str:
    return format_timestamp(us)


grid_time = st.integers(0, 10).map(lambda k: BASE + k * 15 * MIN)
nudged = st.tuples(grid_time, st.sampled_from([0, 0, 0, 1, -1])).map(sum)
time_text = nudged.map(ts)


@st.composite
def rows(draw):
    series = draw(st.sampled_from(sorted(SERIES)))
    table = SERIES[series]
    row = {
        "series": series,
        "value_time": ts(BASE + draw(st.integers(0, 3)) * HOUR),
        "received_time": draw(time_text),
    }
    if draw(st.booleans()):
        row["published_time"] = draw(st.one_of(st.none(), time_text))
    if draw(st.integers(0, 5)) == 0:
        row["deleted"] = True
    else:
        vals = {}
        cols = COLUMNS[table]
        if table == "t" and draw(st.integers(0, 3)) == 0:
            cols = {**cols, "added": [1.0, None]}  # unknown_column until add_column ran
        for col, pool in cols.items():
            if draw(st.booleans()):
                vals[col] = draw(st.sampled_from(pool + [None]))
        row["values"] = vals
    return row


@st.composite
def batch_rows(draw):
    rs = draw(st.lists(rows(), max_size=5))
    if rs and draw(st.integers(0, 5)) == 0:
        i = draw(st.integers(0, len(rs) - 1))
        rs[i] = {**rs[i], **draw(st.sampled_from(INVALID))}
    return rs


@st.composite
def contexts(draw):
    return {
        "clock": draw(st.sampled_from([None, *CLOCKS])),
        "frontier": draw(st.one_of(st.none(), st.none(), time_text)),
        "system": draw(st.one_of(st.none(), st.none(), time_text)),
    }


@st.composite
def value_range(draw):
    lo = draw(st.one_of(st.none(), st.none(), st.integers(0, 4).map(lambda k: ts(BASE + k * HOUR))))
    hi = draw(st.one_of(st.none(), st.none(), st.integers(0, 4).map(lambda k: ts(BASE + k * HOUR))))
    return {"value_from": lo, "value_to": hi}


def series_filter():
    return st.one_of(
        st.none(),
        st.lists(st.sampled_from(["s1", "s2", "s3", "s1", "s2", "s3", "nope"]), max_size=2),
    )


write_op = st.tuples(
    st.just("write"),
    st.fixed_dictionaries(
        {
            "clock": time_text,
            "rows": batch_rows(),
            "batch_id": st.one_of(st.none(), st.sampled_from(["b1", "b2", "b3"])),
        }
    ),
)
read_op = st.one_of(
    st.tuples(
        st.sampled_from(["latest", "history"]),
        st.fixed_dictionaries(
            {"table": st.sampled_from(["t", "u"] * 6 + ["zz"]), "series": series_filter(),
             "ctx": contexts(), "range": value_range()}
        ),
    ),
    st.tuples(
        st.just("replay"),
        st.fixed_dictionaries(
            {"table": st.sampled_from(["t", "u"]), "series": series_filter(), "ctx": contexts(),
             "range": value_range(), "from": st.one_of(st.none(), time_text),
             "to": st.one_of(st.none(), time_text)}
        ),
    ),
    st.tuples(
        st.just("grid"),
        st.fixed_dictionaries(
            {"table": st.sampled_from(["t", "u"]), "series": series_filter(), "ctx": contexts(),
             "range": value_range(), "frontiers": st.lists(time_text, max_size=4),
             "window": st.one_of(
                 st.none(),
                 st.tuples(st.sampled_from(["-1d", "0h", "-90m", "1h"]),
                           st.sampled_from(["0h", "3h", "1d", "30m"])),
             )}
        ),
    ),
    st.tuples(
        st.just("asof_join"),
        st.fixed_dictionaries(
            {"table": st.sampled_from(["t", "u"]), "ctx": contexts(),
             "left": st.lists(
                 st.fixed_dictionaries({
                     "series": st.sampled_from(["s1", "s2", "s1", "s2", "s3"]),
                     "value_time": st.integers(0, 3).map(lambda k: ts(BASE + k * HOUR)),
                     "frontier": time_text,
                 }),
                 max_size=4,
             )}
        ),
    ),
)
add_column_op = st.tuples(st.just("add_column"), st.just({}))
ops = st.tuples(
    st.lists(write_op, min_size=1, max_size=4),
    st.lists(st.one_of(write_op, read_op, read_op, read_op, add_column_op), max_size=12),
).map(lambda p: p[0] + p[1])


def new_world(db):
    db.create_table("t", [{"name": "a", "type": "f64"}, {"name": "b", "type": "i64"},
                          {"name": "c", "type": "timestamp"}])
    db.create_table("u", [{"name": "x", "type": "string"}, {"name": "y", "type": "bool"}],
                    default_clock="published", partition="month")
    for name, table in SERIES.items():
        db.create_series(name, table)


def outcome(f):
    try:
        return ("ok", f())
    except MuninnError as e:
        return ("error", e.code)


def run_op(db, op, a, encode):
    if op == "write":
        return outcome(lambda: db.write(a["rows"], a["clock"], a["batch_id"]).to_json())
    if op == "add_column":
        return outcome(lambda: db.add_column("t", "added", "f64") and None)
    ctx = a["ctx"]
    if op in ("latest", "history"):
        return outcome(lambda: encode(a["table"], getattr(db, op)(
            a["table"], series=a["series"], **a["range"], clock=ctx["clock"],
            frontier=ctx["frontier"], system=ctx["system"])))
    if op == "replay":
        return outcome(lambda: encode(a["table"], db.replay(
            a["table"], series=a["series"], **a["range"], clock=ctx["clock"],
            system=ctx["system"], from_=a["from"], to=a["to"])))
    if op == "grid":
        w = a["window"] or (None, None)
        rng = a["range"]  # with a window too: invalid_range (§10.5)
        return outcome(lambda: encode(a["table"], db.grid(
            a["table"], a["frontiers"], series=a["series"], **rng, clock=ctx["clock"],
            system=ctx["system"], window_start=w[0], window_end=w[1])))
    if op == "asof_join":
        return outcome(lambda: encode(a["table"], db.asof_join(
            a["table"], a["left"], clock=ctx["clock"], system=ctx["system"])))
    raise AssertionError(op)


@settings(max_examples=int(os.environ.get("MUNINN_PY_EXAMPLES", "300")), deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(ops)
def test_bindings_match_reference(program):
    ref = Database()
    new_world(ref)
    mine = PyMuninn()
    new_world(mine)

    def ref_encode(table, rows):
        return json.dumps([ref.encode_row(table, r) for r in rows], allow_nan=False)

    def my_encode(table, rows):
        return json.dumps(rows, allow_nan=False)

    for i, (op, a) in enumerate(program):
        want = run_op(ref, op, a, ref_encode)
        got = run_op(mine, op, a, my_encode)
        assert got == want, f"op {i} {op} {a}:\n  reference: {want}\n  bindings:  {got}"
