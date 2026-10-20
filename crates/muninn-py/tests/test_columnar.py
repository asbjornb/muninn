"""Arrow writes are validated and converted column by column (no map per row). They must
behave exactly like the same rows written as dicts: the same results, the same stored
versions, and for invalid input the same error class and message (SPEC §4.1 / §10.4: rows
in input order, steps 1-5 within a row)."""

import pyarrow as pa
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import muninn

BASE = 1_736_942_400_000_000  # 2025-01-15T12:00:00Z
H = 3_600_000_000
MAX_US = 253_402_300_799_999_999
TS = pa.timestamp("us", tz="UTC")


def fresh():
    d = muninn.memory()
    d.create_table("t", columns={"a": "f64", "b": "i64", "c": "timestamp", "d": "string"})
    d.create_table("u", columns={"x": "f64"})
    d.create_series_batch([{"name": "s1", "table": "t"}, {"name": "s2", "table": "t"},
                           {"name": "s3", "table": "u"}])
    return d


times = st.one_of(st.integers(0, 5).map(lambda k: BASE + k * H),
                  st.sampled_from([MAX_US + 1, -(2**62)]))


@st.composite
def batches(draw):
    n = draw(st.integers(0, 6))
    time_kind = draw(st.sampled_from(["ts", "int", "str"]))
    has = {c: draw(st.booleans()) for c in ["published_time", "deleted", "a", "b", "c", "d",
                                             "x", "zzz"]}
    rows = []
    for _ in range(n):
        r = {"series": draw(st.sampled_from(["s1", "s2", "s3", "s1", "nope"])),
             "value_time": draw(times), "received_time": draw(times)}
        if has["published_time"]:
            r["published_time"] = draw(st.one_of(st.none(), times))
        if has["deleted"]:
            r["deleted"] = draw(st.booleans())
        for c, s in [("a", st.floats(allow_nan=True)), ("b", st.integers(-3, 3)),
                     ("c", st.integers(0, 3).map(lambda k: BASE + k * H)),
                     ("d", st.sampled_from(["", "v", "NaN"])), ("x", st.floats(-1, 1)),
                     ("zzz", st.floats(-1, 1))]:
            if has[c]:
                r[c] = draw(st.one_of(st.none(), s, s))
        rows.append(r)
    chunks = draw(st.integers(1, 2))
    dictionary = draw(st.booleans())
    return rows, has, time_kind, chunks, dictionary


def to_arrow(rows, has, time_kind, chunks, dictionary):
    def tcol(vals):
        if time_kind == "int":
            return pa.array(vals, pa.int64())
        if time_kind == "str":
            return pa.array([None if v is None else muninn_text(v) for v in vals], pa.string())
        return pa.array(vals, pa.int64()).cast(TS, safe=False)

    cols = {"series": pa.array([r["series"] for r in rows], pa.string())}
    if dictionary:
        cols["series"] = cols["series"].dictionary_encode()
    for k in ["value_time", "received_time"] + (["published_time"] if has["published_time"] else []):
        cols[k] = tcol([r.get(k) for r in rows])
    if has["deleted"]:
        cols["deleted"] = pa.array([r["deleted"] for r in rows], pa.bool_())
    types = {"a": pa.float64(), "b": pa.int64(), "c": TS, "d": pa.string(), "x": pa.float64(),
             "zzz": pa.float64()}
    for c, ty in types.items():
        if has[c]:
            vals = [r.get(c) for r in rows]
            cols[c] = (pa.array(vals, pa.int64()).cast(TS) if c == "c" else pa.array(vals, ty))
    t = pa.table(cols)
    if chunks == 2 and t.num_rows > 1:
        k = t.num_rows // 2
        t = pa.concat_tables([t.slice(0, k), t.slice(k)])
    return t


def muninn_text(us):
    # Out-of-range values have no text form; any unparsable string is equivalent for them.
    if not (-62135596800000000 <= us <= MAX_US):
        return "0000-00-00T00:00:00Z"
    import datetime as dt
    d = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(microseconds=us)
    return d.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def to_dicts(rows, has, time_kind):
    out = []
    for r in rows:
        d = {}
        for k, v in r.items():
            if v is None:
                continue  # a null Arrow cell is an absent value
            if k in ("value_time", "received_time", "published_time") and time_kind == "str":
                v = muninn_text(v)
            if k == "c":
                v = muninn_text(v)
            d[k] = v
        out.append(d)
    return out


def outcome(db, data):
    try:
        r = db.write(data, clock=BASE + 100 * H)
        return ("ok", r.stored, r.duplicates, r.system_time, r.first_seq)
    except muninn.MuninnError as e:
        return (type(e).__name__, str(e))


def state(db):
    out = []
    for t in ["t", "u"]:
        h = db.history(t)
        out.append([tuple(map(repr, row.values())) for row in h.to_pylist()])
    return out


@settings(max_examples=400, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(st.lists(batches(), min_size=1, max_size=4))
def test_arrow_writes_behave_like_dict_rows(bs):
    a, b = fresh(), fresh()
    for rows, has, time_kind, chunks, dictionary in bs:
        got = outcome(a, to_arrow(rows, has, time_kind, chunks, dictionary))
        want = outcome(b, to_dicts(rows, has, time_kind))
        assert got == want
    assert state(a) == state(b)
