"""Writes of more than the engine's chunk size (100k rows) are validated, deduplicated and
encoded in chunks, but are still one atomic commit: one system_time, consecutive seqs in
input order, duplicates found across chunk boundaries, and an invalid row in a late chunk
rejects the whole write with its own row index (SPEC §4, §10.4). The Rust tests compare
chunked with unchunked processing at tiny chunk sizes; this checks the same through the
binding at the real size."""

import numpy as np
import pyarrow as pa
import pytest

import muninn

BASE = 1_736_942_400_000_000  # 2025-01-15T12:00:00Z
H = 3_600_000_000
CHUNK = 100_000
TS = pa.timestamp("us", tz="UTC")


def fresh():
    d = muninn.memory()
    d.create_table("t", columns={"a": "f64"})
    d.create_series_batch([{"name": f"s{i}", "table": "t"} for i in range(4)])
    return d


def rows(n):
    """`n` distinct rows: 4 series, runs of 96 points per received time."""
    i = np.arange(n)
    return {"series": [f"s{k}" for k in (i // 96) % 4],
            "value_time": BASE + (i % 96) * H // 4,
            "received_time": BASE + (i // 384) * H,
            "a": (i % 7).astype(float)}


def table(cols, order):
    return pa.table({"series": pa.array([cols["series"][j] for j in order]),
                     "value_time": pa.array(cols["value_time"][order], TS),
                     "received_time": pa.array(cols["received_time"][order], TS),
                     "a": pa.array(cols["a"][order])})


def test_large_write_is_one_commit_with_duplicates_across_chunks():
    n = 2 * CHUNK + 5_000
    cols = rows(n)
    order = list(range(n))
    # Duplicates: straddling both chunk boundaries, the first row again at the end.
    order.insert(CHUNK, CHUNK - 1)
    order.insert(2 * CHUNK, 2 * CHUNK - 2)
    order.append(0)
    d = fresh()
    v0 = d.version
    r = d.write(table(cols, np.array(order)), clock=BASE)
    assert (r.stored, r.duplicates) == (n, 3)
    assert r.first_seq == 1
    assert d.version == v0 + 1
    h = d.history("t")
    assert h.num_rows == n
    assert sorted(h.column("seq").to_pylist()) == list(range(1, n + 1))
    assert set(h.column("system_time").cast(pa.int64()).to_pylist()) == {BASE}
    # Seqs follow input order: row k of the input (without its duplicates) has seq k + 1.
    first = {}
    for pos, j in enumerate(order):
        first.setdefault(j, pos)
    kept_order = sorted(first, key=first.get)
    want = {(cols["series"][j], int(cols["value_time"][j]), int(cols["received_time"][j])): s + 1
            for s, j in enumerate(kept_order)}
    got = {(s, int(vt), int(rt)): q for s, vt, rt, q in zip(
        h.column("series").to_pylist(), h.column("value_time").cast(pa.int64()).to_pylist(),
        h.column("received_time").cast(pa.int64()).to_pylist(), h.column("seq").to_pylist())}
    assert got == want
    # A resend is all duplicates, against stored versions.
    r = d.write(table(cols, np.arange(n)), clock=BASE + 1)
    assert (r.stored, r.duplicates) == (0, n)


@pytest.mark.parametrize("bad, err", [
    ("series", muninn.UnknownSeriesError),
    ("tombstone", muninn.TombstoneWithValuesError),
])
def test_invalid_row_in_a_late_chunk_rejects_the_whole_write(bad, err):
    n = 2 * CHUNK + 10
    at = n - 3
    good = table(rows(n), np.arange(n))
    if bad == "series":
        names = good.column("series").to_pylist()
        names[at] = "nope"
        data = good.set_column(0, "series", pa.array(names))
    else:
        data = good.append_column("deleted", pa.array(np.arange(n) == at))
    d = fresh()
    v0 = d.version
    with pytest.raises(err, match=f"^[a-z_]+: row {at}:"):
        d.write(data, clock=BASE, batch_id="b")
    assert d.version == v0
    assert d.history("t").num_rows == 0
    # Nothing was recorded: the corrected batch with the same id is written.
    r = d.write(good, clock=BASE, batch_id="b")
    assert (r.stored, r.batch_already_committed) == (n, False)
    assert d.write(good, clock=BASE, batch_id="b").batch_already_committed
