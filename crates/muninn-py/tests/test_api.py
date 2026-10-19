"""The Python API itself: input conversions (pandas / polars / pyarrow / dicts, timestamp
units and time zones, naive rejection, NaN / None, big integers), output dtypes,
reproducibility receipts, snapshot isolation, error codes and persistence."""

import datetime as dt
import math
import threading

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import muninn

UTC = dt.timezone.utc
T0 = dt.datetime(2025, 1, 15, 12, tzinfo=UTC)
T0_US = 1736942400000000
H = dt.timedelta(hours=1)


def us(d: dt.datetime) -> int:
    return (d - dt.datetime(1970, 1, 1, tzinfo=UTC)) // dt.timedelta(microseconds=1)


@pytest.fixture
def db():
    d = muninn.memory()
    d.create_table("prices", columns={"value": "f64", "volume": "i64"})
    d.create_series("DK1", "prices", tags={"area": "DK1", "kind": "spot"})
    d.create_series("DK2", "prices", tags={"area": "DK2", "kind": "spot"})
    return d


def values(t: pa.Table, col="value"):
    return t.column(col).to_pylist()


# ---------------------------------------------------------------------------------------
# Writes: every input form gives the same stored versions
# ---------------------------------------------------------------------------------------


def rows_dicts():
    return [
        {"series": "DK1", "value_time": T0 + H, "received_time": T0, "value": 1.5, "volume": 7},
        {"series": "DK2", "value_time": T0 + H, "received_time": T0, "published_time": T0 - H,
         "value": None, "volume": None},
    ]


def as_pandas():
    return pd.DataFrame({
        "series": ["DK1", "DK2"],
        "value_time": pd.to_datetime([T0 + H, T0 + H]),
        "received_time": pd.to_datetime([T0, T0]),
        "published_time": pd.to_datetime([None, T0 - H], utc=True),
        "value": [1.5, np.nan],
        "volume": pd.array([7, None], dtype="Int64"),
    })


def as_arrow():
    ts = pa.timestamp("us", tz="UTC")
    return pa.table({
        "series": ["DK1", "DK2"],
        "value_time": pa.array([T0 + H, T0 + H], ts),
        "received_time": pa.array([T0, T0], ts),
        "published_time": pa.array([None, T0 - H], ts),
        "value": pa.array([1.5, None], pa.float64()),
        "volume": pa.array([7, None], pa.int64()),
    })


def as_polars():
    return pl.DataFrame({
        "series": ["DK1", "DK2"],
        "value_time": [T0 + H, T0 + H],
        "received_time": [T0, T0],
        "published_time": [None, T0 - H],
        "value": [1.5, None],
        "volume": [7, None],
    })


@pytest.mark.parametrize("make", [rows_dicts, as_pandas, as_arrow, as_polars,
                                  lambda: as_arrow().to_batches()[0]],
                         ids=["dicts", "pandas", "pyarrow", "polars", "recordbatch"])
def test_every_input_form_stores_the_same(db, make):
    r = db.write(make(), clock=T0)
    assert (r.stored, r.duplicates, r.batch_already_committed) == (2, 0, False)
    assert r.system_time == T0 and r.first_seq == 1
    t = db.history("prices")
    assert t.column("series").to_pylist() == ["DK1", "DK2"]
    assert values(t) == [1.5, None]
    assert values(t, "volume") == [7, None]
    assert t.column("published_time").to_pylist() == [None, T0 - H]
    # Writing the same data in any other form is a pure duplicate (SPEC §4.3).
    for other in (rows_dicts, as_pandas, as_arrow, as_polars):
        r = db.write(other(), clock=T0 + H)
        assert (r.stored, r.duplicates) == (0, 2)


def test_nested_values_and_tombstones(db):
    db.write([{"series": "DK1", "value_time": "2025-01-16T00:00:00Z",
               "received_time": "2025-01-15T11:00:00Z", "values": {"value": 2.0}}], clock=T0)
    db.write([{"series": "DK1", "value_time": "2025-01-16T00:00:00Z",
               "received_time": "2025-01-15T13:00:00Z", "deleted": True}], clock=T0 + H)
    assert db.latest("prices").num_rows == 0
    assert db.latest("prices", frontier="2025-01-15T12:00:00Z").num_rows == 1
    h = db.history("prices")
    assert h.column("deleted").to_pylist() == [False, True]
    with pytest.raises(muninn.InvalidArgumentError):  # nested form takes no flat values
        db.write([{"series": "DK1", "value_time": 0, "received_time": 0, "values": {},
                   "value": 1.0}])


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
@pytest.mark.parametrize("tz", ["UTC", "Europe/Copenhagen", "+05:30", "America/New_York"])
def test_arrow_timestamps_any_unit_and_zone(db, unit, tz):
    ts = pa.timestamp(unit, tz=tz)
    # The same instants, whatever the unit and zone (Arrow stores UTC instants).
    vt = pa.array([us(T0 + H) // {"s": 10**6, "ms": 10**3, "us": 1, "ns": 1}[unit]
                   * {"s": 1, "ms": 1, "us": 1, "ns": 1000}[unit]]).cast(ts)
    rt = pa.array([us(T0) // {"s": 10**6, "ms": 10**3, "us": 1, "ns": 1}[unit]
                   * {"s": 1, "ms": 1, "us": 1, "ns": 1000}[unit]]).cast(ts)
    db.write(pa.table({"series": ["DK1"], "value_time": vt, "received_time": rt,
                       "value": [1.0]}), clock=T0)
    t = db.latest("prices")
    assert t.column("value_time").to_pylist() == [T0 + H]
    assert t.column("received_time").to_pylist() == [T0]


def test_ints_and_strings_as_timestamps(db):
    db.write(pa.table({"series": ["DK1", "DK2"],
                       "value_time": [us(T0 + H), us(T0 + H)],
                       "received_time": ["2025-01-15T12:00:00Z", "2025-01-15T13:00:00+01:00"],
                       "value": [1.0, 2.0]}), clock=T0)
    t = db.latest("prices")
    assert t.column("value_time").to_pylist() == [T0 + H, T0 + H]
    assert t.column("received_time").to_pylist() == [T0, T0]


def test_naive_timestamps_rejected_unless_assume_utc(db):
    naive = pd.DataFrame({"series": ["DK1"], "value_time": [pd.Timestamp("2025-01-16")],
                          "received_time": [pd.Timestamp("2025-01-15")], "value": [1.0]})
    with pytest.raises(muninn.InvalidTimestampError, match="naive"):
        db.write(naive)
    with pytest.raises(muninn.InvalidTimestampError, match="naive"):
        db.write([{"series": "DK1", "value_time": dt.datetime(2025, 1, 16),
                   "received_time": T0, "value": 1.0}])
    with pytest.raises(muninn.InvalidTimestampError):
        db.write(pa.table({"series": ["DK1"], "value_time": pa.array([0], pa.timestamp("us")),
                           "received_time": [0]}))
    assert db.history("prices").num_rows == 0  # nothing written (SPEC §8)
    db.write(naive, assume_utc=True, clock=T0)
    assert db.latest("prices").column("value_time").to_pylist() == [
        dt.datetime(2025, 1, 16, tzinfo=UTC)]
    with pytest.raises(muninn.InvalidTimestampError, match="naive"):
        db.latest("prices", frontier=dt.datetime(2025, 1, 16))


def test_sub_microsecond_is_rejected(db):
    vt = pa.array([1_000_000_000_001], pa.timestamp("ns", tz="UTC"))
    with pytest.raises(muninn.InvalidTimestampError, match="sub-microsecond"):
        db.write(pa.table({"series": ["DK1"], "value_time": vt, "received_time": [0]}))
    with pytest.raises(muninn.InvalidTimestampError, match="sub-microsecond"):
        db.latest("prices", frontier=pd.Timestamp("2025-01-01T00:00:00.000000001Z"))


def test_time_arguments_all_forms_agree(db):
    db.write(as_arrow(), clock=T0)
    forms = [T0, pd.Timestamp(T0), np.datetime64("2025-01-15T12:00:00"), "2025-01-15T12:00:00Z",
             "2025-01-15T13:00:00+01:00", T0_US, np.int64(T0_US),
             T0.astimezone(dt.timezone(dt.timedelta(hours=-5))), pa.scalar(T0)]
    want = db.latest("prices", frontier=T0)
    for f in forms:
        got = db.latest("prices", frontier=f)
        assert got.equals(want), f
        assert muninn.receipt(got)["frontier"] == "2025-01-15T12:00:00Z"
    for bad in [pd.NaT, np.datetime64("NaT", "us"), 1.5, True, dt.date(2025, 1, 15), object()]:
        with pytest.raises(muninn.InvalidTimestampError):
            db.latest("prices", frontier=bad)


def test_nan_and_null(db):
    # pandas: NaN is pandas' missing value → null. Arrow, polars and dicts: NaN is NaN.
    db.write(pd.DataFrame({"series": ["DK1"], "value_time": [T0], "received_time": [T0],
                           "value": [np.nan]}), clock=T0)
    db.write(pa.table({"series": ["DK1"], "value_time": pa.array([T0 + H]),
                       "received_time": pa.array([T0]), "value": [float("nan")]}), clock=T0)
    db.write(pl.DataFrame({"series": ["DK1"], "value_time": [T0 + 2 * H], "received_time": [T0],
                           "value": [float("nan")]}), clock=T0)
    db.write([{"series": "DK1", "value_time": T0 + 3 * H, "received_time": T0,
               "value": float("nan")},
              {"series": "DK2", "value_time": T0, "received_time": T0, "value": None},
              {"series": "DK2", "value_time": T0 + H, "received_time": T0,
               "value": np.float32(0.5), "volume": np.int16(-3)}], clock=T0)
    v = values(db.latest("prices"))
    assert v[0] is None and all(math.isnan(x) for x in v[1:4]) and v[4] is None
    assert v[5] == 0.5 and values(db.latest("prices", series="DK2"), "volume") == [None, -3]
    # -0.0 is not 0.0 (bitwise identity, SPEC §4.3).
    r = db.write([{"series": "DK2", "value_time": T0, "received_time": T0, "value": -0.0}],
                 clock=T0)
    assert r.stored == 1
    assert math.copysign(1, values(db.latest("prices", series="DK2"))[0]) == -1


def test_big_integers(db):
    big = [2**63 - 1, -(2**63), 2**53 + 1]
    db.write(pa.table({"series": ["DK1"] * 3, "value_time": [0, 1, 2], "received_time": [0] * 3,
                       "volume": pa.array(big, pa.int64())}), clock=T0)
    assert values(db.latest("prices"), "volume") == big
    db.write([{"series": "DK2", "value_time": 0, "received_time": 0, "volume": 2**63 - 1},
              {"series": "DK2", "value_time": 1, "received_time": 0, "volume": "-42"}], clock=T0)
    assert values(db.latest("prices", series=["DK2"]), "volume") == [2**63 - 1, -42]
    for bad in [2**63, -(2**63) - 1, 1.5, "1.5", True]:
        with pytest.raises(muninn.TypeMismatchError):
            db.write([{"series": "DK1", "value_time": 9, "received_time": 0, "volume": bad}])
    with pytest.raises(muninn.TypeMismatchError):
        db.write(pa.table({"series": ["DK1"], "value_time": [0], "received_time": [0],
                           "volume": pa.array([2**64 - 1], pa.uint64())}))
    # Integers into an f64 column are converted (SPEC §4.1).
    db.write([{"series": "DK1", "value_time": 5, "received_time": 0, "value": 2**53 + 1}],
             clock=T0)
    assert values(db.latest("prices", value_from=5, value_to=6)) == [float(2**53)]


def test_all_column_types_and_output_dtypes():
    db = muninn.memory()
    db.create_table("t", columns=[("f", "f64"), ("i", "i64"), ("b", "bool"), ("s", "string"),
                                  ("ts", "timestamp")])
    db.create_series("x", "t")
    db.write([{"series": "x", "value_time": T0, "received_time": T0, "f": 1.0, "i": 2,
               "b": True, "s": "hé", "ts": T0 - H}], clock=T0)
    t = db.latest("t")
    ts = pa.timestamp("us", tz="UTC")
    assert t.schema == pa.schema([
        pa.field("series", pa.string(), False), pa.field("series_id", pa.uint32(), False),
        pa.field("value_time", ts, False), pa.field("published_time", ts),
        pa.field("received_time", ts, False), pa.field("system_time", ts, False),
        pa.field("seq", pa.uint64(), False), ("f", pa.float64()), ("i", pa.int64()),
        ("b", pa.bool_()), ("s", pa.string()), ("ts", ts),
    ]).with_metadata(t.schema.metadata)
    assert t.to_pylist()[0]["ts"] == T0 - H and t.to_pylist()[0]["s"] == "hé"
    df = t.to_pandas()
    assert str(df["value_time"].dtype) == "datetime64[us, UTC]"
    assert df["f"].dtype == np.float64 and df["i"].dtype == np.int64
    p = pl.from_arrow(t)
    assert p.schema["value_time"] == pl.Datetime("us", "UTC")
    # Timestamp values from strings and pandas, type checks per column.
    db.write(pd.DataFrame({"series": ["x"], "value_time": [pd.Timestamp(T0 + H)],
                           "received_time": [pd.Timestamp(T0)],
                           "ts": [pd.Timestamp("2025-01-01", tz="Europe/Copenhagen")]}),
             clock=T0)
    assert db.latest("t").column("ts").to_pylist()[1] == dt.datetime(2024, 12, 31, 23,
                                                                      tzinfo=UTC)
    with pytest.raises(muninn.TypeMismatchError):
        db.write([{"series": "x", "value_time": T0, "received_time": T0, "ts": "yesterday"}])
    with pytest.raises(muninn.TypeMismatchError):
        db.write([{"series": "x", "value_time": T0, "received_time": T0, "b": 1}])


def test_categorical_and_dictionary_series(db):
    df = pl.DataFrame({"series": ["DK1", "DK2"], "value_time": [T0, T0],
                       "received_time": [T0, T0], "value": [1.0, 2.0]}).with_columns(
        pl.col("series").cast(pl.Categorical))
    assert db.write(df, clock=T0).stored == 2
    tbl = as_arrow()
    tbl = tbl.set_column(0, "series", tbl.column("series").dictionary_encode())
    assert db.write(tbl, clock=T0).stored == 2
    # Dictionary keys are resolved once per column, not per cell (this was quadratic).
    import time
    n = 200_000
    big = pa.table({"series": pa.array(["DK1", "DK2"] * (n // 2)).dictionary_encode(),
                    "value_time": pa.array(range(n), pa.int64()),
                    "received_time": pa.array([0] * n, pa.int64()),
                    "value": pa.array([1.0] * n)})
    start = time.perf_counter()
    assert db.write(big, clock=T0).stored == n
    assert time.perf_counter() - start < 10


def test_multi_table_columnar_batch():
    db = muninn.memory()
    db.create_table("a", columns={"x": "f64"})
    db.create_table("b", columns={"y": "string"})
    db.create_series("s1", "a")
    db.create_series("s2", "b")
    # Each row only has its own table's column; nulls in the other table's are ignored.
    r = db.write(pa.table({"series": ["s1", "s2"], "value_time": [0, 0], "received_time": [0, 0],
                           "x": [1.0, None], "y": [None, "hi"]}), clock=T0)
    assert r.stored == 2
    assert values(db.latest("a"), "x") == [1.0] and values(db.latest("b"), "y") == ["hi"]
    with pytest.raises(muninn.UnknownColumnError):
        db.write(pa.table({"series": ["s1"], "value_time": [1], "received_time": [0],
                           "y": ["no"]}))


def test_batch_ids_and_default_clock(db):
    r1 = db.write(rows_dicts(), batch_id="b1")
    assert r1.stored == 2 and r1.system_time is not None
    assert abs(r1.system_time - dt.datetime.now(UTC)) < dt.timedelta(minutes=1)
    r2 = db.write([], batch_id="b1")
    assert r2.batch_already_committed and r2.stored == 0 and r2.system_time is None
    with pytest.raises(muninn.InvalidArgumentError):
        db.write([], batch_id="")


# ---------------------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------------------


def forecast_world():
    db = muninn.memory()
    db.create_table("fc", columns={"value": "f64"}, default_clock="published")
    db.create_series("wind", "fc")
    # Two forecast runs for the same hours; run 2 supersedes run 1 once published.
    for run, pub in enumerate([T0, T0 + 6 * H]):
        db.write([{"series": "wind", "value_time": T0 + k * H, "published_time": pub,
                   "received_time": pub + dt.timedelta(minutes=5), "value": run * 10.0 + k}
                  for k in range(12, 36)], clock=pub + dt.timedelta(minutes=6))
    return db


def test_backtest_semantics():
    db = forecast_world()
    assert values(db.latest("fc", frontier=T0 + H))[:2] == [12.0, 13.0]
    assert values(db.latest("fc", frontier=T0 + 7 * H))[:2] == [22.0, 23.0]
    assert db.latest("fc", frontier=T0 - H).num_rows == 0
    g = db.grid("fc", [T0 + 7 * H, T0 + H, T0 + H], window=("11h", dt.timedelta(hours=13)))
    assert g.column("frontier").to_pylist() == [T0 + H] * 2 + [T0 + 7 * H] * 2
    assert values(g) == [12.0, 13.0, 28.0, 29.0]
    rp = db.replay("fc", T0, T0 + 6 * H)
    assert rp.num_rows == 24 and values(rp)[0] == 22.0
    with pytest.raises(muninn.InvalidRangeError):
        db.grid("fc", [T0], window=("6h", "1h"))
    with pytest.raises(muninn.InvalidRangeError):
        db.grid("fc", [T0], window=("6h", "1h"), value_from=T0)
    with pytest.raises(muninn.InvalidRangeError):
        db.latest("fc", value_from=T0 + H, value_to=T0)


def test_asof_join_appends_to_left():
    db = forecast_world()
    left = pd.DataFrame({
        "series": ["wind", "wind", "wind"],
        "value_time": [T0 + 14 * H, T0 + 14 * H, T0 + 40 * H],
        "frontier": [T0 + H, T0 + 7 * H, T0 + 7 * H],
        "signal": [1, 2, 3],
        "value": ["l1", "l2", "l3"],
    })
    out = db.asof_join("fc", left)
    assert out.column_names[:5] == ["series", "value_time", "frontier", "signal", "value"]
    assert out.column("found").to_pylist() == [True, True, False]
    assert out.column("value_right").to_pylist() == [14.0, 24.0, None]
    assert out.column("value").to_pylist() == ["l1", "l2", "l3"]
    # polars and pyarrow lefts work the same; lists of dicts give the spec columns.
    out_pl = db.asof_join("fc", pl.from_pandas(left))
    assert out_pl.column("value_right").to_pylist() == [14.0, 24.0, None]
    spec = db.asof_join("fc", [{"series": "wind", "value_time": T0 + 14 * H,
                                "frontier": T0 + H}])
    assert spec.column_names[:5] == ["series", "series_id", "value_time", "frontier", "found"]
    with pytest.raises(muninn.UnknownSeriesError):
        db.asof_join("fc", [{"series": "nope", "value_time": T0, "frontier": T0}])
    naive = left.assign(frontier=left["frontier"].dt.tz_localize(None))
    with pytest.raises(muninn.InvalidTimestampError):
        db.asof_join("fc", naive)
    assert db.asof_join("fc", naive, assume_utc=True).column("found").to_pylist() == [
        True, True, False]


def test_catalog_listing():
    db = muninn.memory()
    db.create_table("prices", columns={"value": float})
    db.create_table("fc", columns=[{"name": "p50", "type": "f64"}], default_clock="published",
                    partition="month")
    db.create_series("DK1", "prices", tags={"area": "DK1"}, timezone="Europe/Copenhagen",
                     resolution="1h")
    db.create_series("w", "fc", tags={"area": "DK1", "kind": "wind"},
                     resolution=dt.timedelta(minutes=15))
    db.add_column("prices", "volume", "i64")
    assert [t.name for t in db.tables()] == ["prices", "fc"]
    assert db.table("prices").columns == {"value": "f64", "volume": "i64"}
    assert db.table("fc").default_clock == "published" and db.table("fc").partition == "month"
    assert [s.name for s in db.series()] == ["DK1", "w"]
    assert [s.name for s in db.series(area="DK1", kind="wind")] == ["w"]
    assert [s.name for s in db.series("prices")] == ["DK1"]
    s = db.series("prices")[0]
    assert (s.id, s.timezone, s.resolution) == (1, "Europe/Copenhagen", dt.timedelta(hours=1))
    assert db.series("fc")[0].resolution == dt.timedelta(minutes=15)


# ---------------------------------------------------------------------------------------
# Receipts (SPEC §11)
# ---------------------------------------------------------------------------------------


def test_receipts_replay_bit_for_bit():
    db = forecast_world()
    left = pa.table({"series": ["wind"] * 2, "value_time": pa.array([T0 + 14 * H] * 2),
                     "frontier": pa.array([T0 + H, T0 + 7 * H]), "k": [1, 2]})
    snap = db.snapshot()
    reads = [
        db.latest("fc", frontier=T0 + H, series="wind", value_from=T0 + 13 * H),
        db.history("fc"),
        db.history("fc", clock="received", system=T0 + H),
        db.replay("fc", None, T0 + 6 * H, clock="system"),
        db.grid("fc", [T0 + H, T0 + 7 * H], window=("0h", "36h")),
        db.grid("fc", [T0 + 7 * H], value_from=T0 + 20 * H),
        db.asof_join("fc", left),
        db.asof_join("fc", [{"series": "wind", "value_time": T0 + 14 * H, "frontier": T0}]),
    ]
    receipts = [muninn.receipt(t) for t in reads]
    assert receipts[0]["op"] == "latest" and receipts[0]["clock"] == "published"
    assert receipts[0]["frontier"] == "2025-01-15T13:00:00Z"
    assert receipts[0]["request"]["series"] == ["wind"]
    assert receipts[0]["system"] == "2025-01-15T18:06:00Z"  # S from the snapshot
    assert receipts[2]["system"] == "2025-01-15T13:00:00Z"  # S as given (<= L)
    assert muninn.receipt(db.latest("fc", system="2030-01-01T00:00:00Z"))["system"] == (
        "2025-01-15T18:06:00Z")  # clamped to L (SPEC §11)
    # Later writes (new forecast run, corrections, a new column) change nothing at S.
    db.write([{"series": "wind", "value_time": T0 + k * H, "published_time": T0 + 7 * H,
               "received_time": T0 + 7 * H, "value": -1.0} for k in range(10, 40)],
             clock=T0 + 8 * H)
    db.add_column("fc", "p90", "f64")
    assert db.latest("fc", frontier=T0 + H, series="wind",
                     value_from=T0 + 13 * H).num_columns == reads[0].num_columns + 1
    for t, r in zip(reads, receipts):
        for target in (db, snap, db.snapshot()):
            kw = {"left": left} if r["op"] == "asof_join" and "k" in t.column_names else {}
            again = muninn.replay_receipt(target, t, **kw)
            assert again.equals(t), r["op"]
            assert muninn.receipt(again) == r
    # Receipts survive JSON round trips and pandas metadata loss.
    r0 = muninn.receipt(reads[0])
    assert muninn.replay_receipt(db, r0).equals(reads[0])
    import json
    assert muninn.replay_receipt(db, json.dumps(r0)).equals(reads[0])
    with pytest.raises(muninn.InvalidArgumentError):
        muninn.receipt(pa.table({"a": [1]}))


def test_receipt_of_empty_snapshot():
    db = muninn.memory()
    db.create_table("t", columns={"v": "f64"})
    db.create_series("s", "t")
    t = db.latest("t")
    r = muninn.receipt(t)
    assert r["system"] == "EMPTY"
    db.write([{"series": "s", "value_time": 0, "received_time": 0, "v": 1.0}], clock=T0)
    again = muninn.replay_receipt(db, t)
    assert again.num_rows == 0 and again.equals(t) and muninn.receipt(again) == r


# ---------------------------------------------------------------------------------------
# Snapshots, errors, persistence
# ---------------------------------------------------------------------------------------


def test_snapshot_isolation(db):
    db.write(rows_dicts(), clock=T0)
    snap = db.snapshot()
    before = snap.history("prices")
    assert snap.last_system_time == T0
    db.write([{"series": "DK1", "value_time": T0 + H, "received_time": T0 + H, "value": 9.0}],
             clock=T0 + H)
    db.create_series("DK3", "prices")
    db.compact()
    db.gc(grace=0)
    assert snap.history("prices").equals(before)
    assert [s.name for s in snap.series()] == ["DK1", "DK2"]
    assert db.history("prices").num_rows == 3
    assert snap.version < db.version
    assert "DK3" in [s.name for s in db.series()]


def test_concurrent_reads_release_the_gil(db):
    db.write([{"series": "DK1", "value_time": k, "received_time": 0, "value": float(k)}
              for k in range(2000)], clock=T0)
    want = db.latest("prices")
    errors = []

    def reader():
        try:
            for _ in range(20):
                assert db.latest("prices").equals(want)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for th in threads:
        th.start()
    db.write([{"series": "DK2", "value_time": 0, "received_time": 0, "value": 1.0}], clock=T0)
    for th in threads:
        th.join()
    assert not errors or all(isinstance(e, AssertionError) for e in errors)


@pytest.mark.parametrize("code,cls,call", [
    ("unknown_table", muninn.UnknownTableError, lambda db: db.latest("nope")),
    ("unknown_series", muninn.UnknownSeriesError, lambda db: db.latest("prices", series="x")),
    ("unknown_column", muninn.UnknownColumnError,
     lambda db: db.write([{"series": "DK1", "value_time": 0, "received_time": 0, "zz": 1}])),
    ("type_mismatch", muninn.TypeMismatchError,
     lambda db: db.write([{"series": "DK1", "value_time": 0, "received_time": 0,
                           "value": "x"}])),
    ("tombstone_with_values", muninn.TombstoneWithValuesError,
     lambda db: db.write([{"series": "DK1", "value_time": 0, "received_time": 0,
                           "deleted": True, "value": 1.0}])),
    ("invalid_timestamp", muninn.InvalidTimestampError,
     lambda db: db.latest("prices", frontier="2025-02-30T00:00:00Z")),
    ("invalid_range", muninn.InvalidRangeError,
     lambda db: db.replay("prices", T0 + H, T0)),
    ("invalid_name", muninn.InvalidNameError, lambda db: db.create_table("Bad Name")),
    ("already_exists", muninn.AlreadyExistsError, lambda db: db.create_series("DK1", "prices")),
    ("invalid_argument", muninn.InvalidArgumentError,
     lambda db: db.latest("prices", clock="wallclock")),
    ("invalid_argument", muninn.InvalidArgumentError, lambda db: db.write(42)),
    ("invalid_argument", muninn.InvalidArgumentError,
     lambda db: db.create_table("t", columns={"v": "float"})),
    ("io", muninn.IoError, lambda db: muninn.open("/nonexistent/muninn/db")),
])
def test_error_codes(db, code, cls, call):
    with pytest.raises(cls) as e:
        call(db)
    assert e.value.code == code
    assert isinstance(e.value, muninn.MuninnError)
    assert str(e.value).startswith(code)


def test_errors_follow_the_spec_check_order(db):
    # unknown_table beats a bad timestamp; unknown_series beats it too (SPEC §10.4).
    with pytest.raises(muninn.UnknownTableError):
        db.latest("nope", frontier="bad")
    with pytest.raises(muninn.UnknownSeriesError):
        db.latest("prices", series=["DK1", "x"], frontier="bad")
    # Structure first: a bad clock beats an unknown table.
    with pytest.raises(muninn.InvalidArgumentError):
        db.latest("nope", clock="wallclock")
    # A known batch id is a no-op even with bad rows (structure is still checked).
    db.write([], batch_id="b")
    assert db.write([{"series": "x", "value_time": "bad", "received_time": 0}],
                    batch_id="b").batch_already_committed


def test_persistence_across_reopen(tmp_path):
    path = tmp_path / "db"
    with pytest.raises(muninn.IoError):
        muninn.open(path)
    db = muninn.open(path, create=True)
    db.create_table("prices", columns={"value": "f64"}, partition="month")
    db.create_series("DK1", "prices", tags={"a": "b"})
    for k in range(5):
        db.write([{"series": "DK1", "value_time": T0 + k * H, "received_time": T0,
                   "value": float(k)}], clock=T0 + k * H, batch_id=f"b{k}")
    first = db.history("prices")
    r = muninn.receipt(db.latest("prices"))
    del db
    db = muninn.open(path)
    assert db.history("prices").equals(first)
    assert db.series()[0].tags == {"a": "b"}
    assert db.write([], batch_id="b3").batch_already_committed  # batch ids persist
    assert db.compact() == 1
    assert db.gc(grace="0s").deleted
    db2 = muninn.open(str(path))
    assert db2.history("prices").equals(first)
    assert muninn.replay_receipt(db2, r).equals(db.latest("prices"))
    (tmp_path / "other").mkdir()
    (tmp_path / "other" / "file.txt").write_text("x")
    with pytest.raises(muninn.IoError, match="not a Muninn database"):
        muninn.open(tmp_path / "other", create=True)


def test_polars_and_arrow_frontiers_reject_sub_microseconds(db):
    db.write(as_arrow(), clock=T0)
    ns = pl.Series([dt.datetime(2025, 1, 15, 12, tzinfo=UTC)]).cast(pl.Datetime("ns", "UTC"))
    assert db.grid("prices", ns).num_rows == db.grid("prices", [T0]).num_rows
    bad = pl.Series([1736942400000000001]).cast(pl.Datetime("ns", "UTC"))
    with pytest.raises(muninn.InvalidTimestampError, match="sub-microsecond"):
        db.grid("prices", bad)
    with pytest.raises(muninn.InvalidTimestampError, match="sub-microsecond"):
        db.grid("prices", pa.array([1736942400000000001], pa.timestamp("ns", tz="UTC")))
    with pytest.raises(muninn.InvalidTimestampError, match="naive"):
        db.grid("prices", pa.array([0], pa.timestamp("us")))


def test_series_id_value_column_is_never_clobbered():
    db = muninn.memory()
    db.create_table("t", {"series_id": "string", "x": "f64"})
    db.create_series("a", "t")
    db.write([{"series": "a", "value_time": 0, "received_time": 0, "series_id": "v", "x": 1.0}],
             clock=T0)
    for t in (db.latest("t"), db.history("t"), db.replay("t"), db.grid("t", [T0])):
        assert t.column("series_id").to_pylist() == ["v"]
        assert t.column("_series_id").to_pylist() == [1]
    j = db.asof_join("t", [{"series": "a", "value_time": 0, "frontier": T0}])
    assert j.column("series_id").to_pylist() == ["v"] and j.column("_series_id").to_pylist() == [1]
    # Schema per op is fixed whatever matched (SPEC §11 replay equality, concat_tables).
    miss = db.asof_join("t", [{"series": "a", "value_time": 5, "frontier": T0}])
    assert miss.schema.remove_metadata() == j.schema.remove_metadata()


def test_create_series_batch_is_atomic(db):
    made = db.create_series_batch(
        ["s0", {"name": "s1", "tags": {"k": "v"}, "resolution": "15m"},
         {"name": "s2", "table": "prices", "timezone": "Europe/Oslo"}],
        table="prices",
    )
    assert [s.name for s in made] == ["s0", "s1", "s2"]
    assert [s.id for s in made] == [3, 4, 5]
    assert made[1].tags == {"k": "v"} and made[2].timezone == "Europe/Oslo"
    assert {s.name for s in db.series("prices")} == {"DK1", "DK2", "s0", "s1", "s2"}
    v = db.version
    with pytest.raises(muninn.AlreadyExistsError):
        db.create_series_batch(["x", "y", "x"], table="prices")
    with pytest.raises(muninn.UnknownTableError):
        db.create_series_batch(["z", {"name": "w", "table": "nope"}], table="prices")
    with pytest.raises(muninn.InvalidArgumentError):
        db.create_series_batch([{"name": "q", "table": "prices", "colour": "red"}])
    assert db.version == v and len(db.series("prices")) == 5
    assert db.create_series_batch([]) == []


def test_gc_truncates_the_log(tmp_path):
    d = muninn.open(tmp_path / "db", create=True)
    d.create_table("t", columns={"x": "f64"})
    d.create_series_batch([f"s{i}" for i in range(100)], table="t")
    for i in range(6):
        d.write([{"series": f"s{i}", "value_time": i, "received_time": i, "x": 1.0}],
                clock=T0 + i * H)
        r = d.gc(grace="0s")
    assert r.log_deleted > 0
    assert not (tmp_path / "db" / "log" / "00000000000000000002.json").exists()
    d2 = muninn.open(tmp_path / "db")
    assert d2.latest("t").num_rows == 6 and len(d2.series("t")) == 100


def test_pandas_fallback_reports_spec_codes(db):
    with pytest.raises(muninn.TypeMismatchError):
        db.write(pd.DataFrame({"series": ["DK1"], "value_time": [T0], "received_time": [T0],
                               "volume": [2**64]}))
    with pytest.raises(muninn.TypeMismatchError):
        db.write(pd.DataFrame({"series": ["DK1", "DK1"], "value_time": [T0, T0 + H],
                               "received_time": [T0, T0], "value": [1.0, "z"]}))
    # Mixed but valid objects are written through the row path.
    r = db.write(pd.DataFrame({"series": ["DK1", "DK1"], "value_time": [T0, T0 + H],
                               "received_time": [T0, T0], "value": [1.0, "NaN"]}), clock=T0)
    assert r.stored == 2
    with pytest.raises(muninn.InvalidArgumentError):
        db.asof_join("prices", pd.DataFrame({"series": ["DK1"], "value_time": [T0],
                                             "frontier": [T0], "o": [object()]}))
