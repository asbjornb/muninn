"""Adversarial review (round 2) of the Python bindings: each test reproduces one finding and
is expected to fail until it is fixed (``xfail(strict=True)``)."""

import pyarrow as pa
import pandas as pd
import pytest

import muninn

VT = "2025-01-01T00:00:00Z"


def _db(columns=None):
    db = muninn.memory()
    db.create_table("t", columns or {"x": "f64"})
    db.create_series("a", "t")
    return db


def _row(vt=VT, **values):
    return {"series": "a", "value_time": vt, "received_time": VT, **values}


@pytest.mark.xfail(strict=True, reason="review finding 1")
def test_receipt_with_explicit_system_beyond_snapshot_replays_identically():
    # SPEC §11 / §7.3: replaying a receipt returns the same rows. A read with an explicit
    # S later than the snapshot's last system_time records that S verbatim, so a later
    # (backfilled) write committed at a system_time <= S shows up in the replay.
    db = _db()
    db.write([_row(x=1.0)], clock="2025-01-01T00:00:00Z")
    first = db.latest("t", system="2025-06-01T00:00:00Z")  # "as of now-ish"
    assert first.num_rows == 1
    # A later batch whose writer clock is before S: committed at 2025-02-01 <= S.
    db.write([_row(vt="2025-01-02T00:00:00Z", x=2.0)], clock="2025-02-01T00:00:00Z")
    again = muninn.replay_receipt(db, first)
    assert again.num_rows == first.num_rows
    assert again.equals(first)


@pytest.mark.xfail(strict=True, reason="review finding 3")
def test_asof_join_with_table_left_keeps_value_column_named_series_id():
    # `series_id` is not a reserved column name (SPEC §2.1). With a list-of-dicts left
    # the value is returned; with an Arrow/pandas left `_db.asof_join` skips every result
    # column named in `_FIXED_ASOF`, which silently drops the *value* column.
    db = _db({"x": "f64", "series_id": "string"})
    db.write([_row(x=1.0, series_id="hello")])
    left = pa.table(
        {
            "series": ["a"],
            "value_time": pa.array([pd.Timestamp(VT)]),
            "frontier": pa.array([pd.Timestamp("2030-01-01", tz="UTC")]),
        }
    )
    res = db.asof_join("t", left)
    assert "series_id" in res.column_names
    assert res.column("series_id").to_pylist() == ["hello"]


@pytest.mark.xfail(strict=True, reason="review finding 4")
def test_replay_receipt_after_adding_series_id_column_keeps_original_schema():
    # The output drops the `series_id` id column when the table has a value column of that
    # name. After `add_column("series_id")` the replay projects the new column away, but
    # the id column is gone too: the replay is not the original result.
    db = _db()
    db.write([_row(x=1.0)])
    first = db.latest("t")
    db.add_column("t", "series_id", "string")
    again = muninn.replay_receipt(db, first)
    assert again.column_names == first.column_names
    assert again.equals(first)


@pytest.mark.xfail(strict=True, reason="review finding 5")
def test_empty_snapshot_receipt_replays_identically():
    # A receipt of an empty snapshot has system = null and is replayed at S = MIN. The
    # first batch of a database may be committed at exactly MIN (writer clock MIN), and
    # then the replay of the "empty" read returns a row.
    db = _db()
    first = db.latest("t")
    assert first.num_rows == 0
    db.write(
        [{"series": "a", "value_time": VT, "received_time": "0001-01-01T00:00:00Z", "x": 1.0}],
        clock="0001-01-01T00:00:00Z",
    )
    assert muninn.replay_receipt(db, first).num_rows == 0


@pytest.mark.xfail(strict=True, reason="review finding 8")
def test_asof_join_result_schema_does_not_depend_on_matches():
    # Nullability of received_time/system_time/seq is `not null` when every left row
    # matched and nullable otherwise, so two results of the same query shape cannot be
    # concatenated (pa.concat_tables compares field nullability).
    db = _db()
    db.write([_row(x=1.0)])
    hit = db.asof_join("t", [{"series": "a", "value_time": VT, "frontier": "2030-01-01T00:00:00Z"}])
    miss = db.asof_join(
        "t",
        [{"series": "a", "value_time": "2031-01-01T00:00:00Z", "frontier": "2030-01-01T00:00:00Z"}],
    )
    assert hit.schema.remove_metadata() == miss.schema.remove_metadata()
    assert pa.concat_tables([hit, miss]).num_rows == 2


@pytest.mark.xfail(strict=True, reason="review finding 9")
def test_pandas_conversion_failures_are_muninn_errors():
    # Every other bad input raises a MuninnError (type_mismatch / invalid_argument); a
    # pandas object column that pyarrow cannot convert leaks pyarrow / builtin exceptions.
    db = _db({"x": "f64", "n": "i64"})
    with pytest.raises(muninn.MuninnError):
        db.write(pd.DataFrame({"series": ["a"], "value_time": [VT], "received_time": [VT],
                               "n": [2**64]}))
    with pytest.raises(muninn.MuninnError):
        db.write(pd.DataFrame({"series": ["a", "a"], "value_time": [VT, VT],
                               "received_time": [VT, VT], "x": [1.0, "z"]}))
