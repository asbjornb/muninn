"""Shared pieces of the dataset generator: time constants, deterministic RNG, and the
writer that turns a stream of write batches into stored versions.

Each dataset is an independent Muninn database with one table. The generator plays the
part of the writer *and* the engine: dataset modules emit batches (rows + the writer's
clock reading ``c`` at commit), and :class:`VersionWriter` applies the engine rules of
SPEC §4:

* §4.2 ``batch.system_time = max(c, last)``, one database-wide ``seq`` starting at 1,
  each stored row taking the next value in batch input order;
* §4.3 exact duplicate re-sends (content-identical to a stored version) are *not*
  stored: they consume no seq and land in ``resends.parquet`` instead, so a competitor
  can measure dedup-at-ingest if it wants to.

Batches are committed in commit-clock order. Dataset modules emit roughly one commit-day
at a time; batches whose ``c`` falls after the chunk horizon are held back and merged into
the next chunk, so a late batch never pushes ``system_time`` of earlier arrivals forward.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

US = 1
MS = 1_000
S = 1_000_000
MIN = 60 * S
H = 60 * MIN
DAY = 24 * H


def ts(y: int, m: int, d: int, hh: int = 0, mm: int = 0) -> int:
    """UTC civil time -> µs since epoch."""
    return int(np.datetime64(f"{y:04d}-{m:02d}-{d:02d}T{hh:02d}:{mm:02d}", "us").astype(np.int64))


def rng(seed: int, *key: int) -> np.random.Generator:
    """Deterministic generator for a key path, independent of chunking/iteration order."""
    return np.random.default_rng([seed, *[int(k) & 0xFFFFFFFF for k in key]])


@dataclass
class TableSpec:
    name: str
    columns: list[tuple[str, str]]  # (name, type) with SPEC §2.1 types
    default_clock: str = "received"
    partition: str = "day"


@dataclass
class SeriesSpec:
    id: int
    name: str
    tags: dict = field(default_factory=dict)
    resolution: str | None = None


ARROW_TYPES = {
    "f64": pa.float64(),
    "i64": pa.int64(),
    "bool": pa.bool_(),
    "string": pa.string(),
    "timestamp": pa.timestamp("us", tz="UTC"),
}

TS_TYPE = pa.timestamp("us", tz="UTC")

NULL_TS = np.iinfo(np.int64).min  # in-generator sentinel for a null timestamp


class Batch:
    """Builder for a chunk of write batches as column arrays.

    Every row carries the batch's commit clock ``c`` and a batch number; rows of one batch
    are contiguous and in input order. ``dup`` marks rows that are exact re-sends of an
    already stored row (the generator knows this by construction).
    """

    def __init__(self, table: TableSpec):
        self.table = table
        self.parts: list[dict] = []

    def add(self, *, series_id, value_time, published_time, received_time, c, batch,
            deleted=None, dup=None, values: dict):
        n = len(value_time)
        if n == 0:
            return
        part = {
            "series_id": np.broadcast_to(np.asarray(series_id, dtype=np.uint32), (n,)),
            "value_time": np.asarray(value_time, dtype=np.int64),
            "published_time": np.broadcast_to(np.asarray(published_time, dtype=np.int64), (n,)),
            "received_time": np.broadcast_to(np.asarray(received_time, dtype=np.int64), (n,)),
            "c": np.broadcast_to(np.asarray(c, dtype=np.int64), (n,)),
            "batch": np.broadcast_to(np.asarray(batch, dtype=np.int64), (n,)),
            "deleted": np.zeros(n, bool) if deleted is None
            else np.broadcast_to(np.asarray(deleted, bool), (n,)),
            "dup": np.zeros(n, bool) if dup is None else np.broadcast_to(np.asarray(dup, bool), (n,)),
        }
        for name, t in self.table.columns:
            v = values[name]
            dtype = object if t == "string" else (np.int64 if t in ("timestamp", "i64") else None)
            arr = np.asarray(v, dtype=dtype)
            part[name] = arr if arr.shape == (n,) else np.broadcast_to(arr, (n,))
        self.parts.append(part)

    def concat(self) -> dict | None:
        if not self.parts:
            return None
        return {k: np.concatenate([p[k] for p in self.parts]) for k in self.parts[0]}


def _ts_array(a: np.ndarray) -> pa.Array:
    mask = a == NULL_TS
    return pa.array(np.where(mask, 0, a), type=pa.int64(), mask=mask if mask.any() else None).cast(TS_TYPE)


def _value_array(col_type: str, a: np.ndarray, null_mask: np.ndarray) -> pa.Array:
    if col_type == "timestamp":
        a = np.where(null_mask, NULL_TS, a.astype(np.int64))
        return _ts_array(a)
    if col_type == "f64":
        return pa.array(a.astype(np.float64), type=pa.float64(), mask=null_mask)
    if col_type == "i64":
        return pa.array(a.astype(np.int64), type=pa.int64(), mask=null_mask)
    if col_type == "bool":
        return pa.array(a.astype(bool), type=pa.bool_(), mask=null_mask)
    if col_type == "string":
        return pa.array(a.astype(object), type=pa.string(), mask=null_mask)
    raise ValueError(col_type)


class VersionWriter:
    """Applies SPEC §4.2/§4.3 to emitted batches and writes Parquet parts."""

    ROW_GROUP = 1 << 20

    def __init__(self, out_dir: Path, table: TableSpec, series: list[SeriesSpec]):
        self.out_dir = out_dir
        self.table = table
        self.series_names = np.array([""] + [s.name for s in series], dtype=object)
        self.last_system_time: int | None = None
        self.next_seq = 1
        self.pending: dict | None = None
        self.buf: list[pa.Table] = []
        self.buf_rows = 0
        self.dup_buf: list[pa.Table] = []
        self.stored = 0
        self.duplicates = 0
        self.batches = 0
        out_dir.mkdir(parents=True, exist_ok=True)
        self.schema = self._schema(with_engine=True)
        self.writer = pq.ParquetWriter(out_dir / "versions.parquet", self.schema,
                                       compression="zstd", compression_level=3)

    def _schema(self, with_engine: bool) -> pa.Schema:
        fields = [
            pa.field("series", pa.dictionary(pa.int32(), pa.string())),
            pa.field("series_id", pa.uint32(), nullable=False),
            pa.field("value_time", TS_TYPE, nullable=False),
            pa.field("published_time", TS_TYPE),
            pa.field("received_time", TS_TYPE, nullable=False),
        ]
        if with_engine:
            fields += [pa.field("system_time", TS_TYPE, nullable=False),
                       pa.field("seq", pa.int64(), nullable=False)]
        else:  # dropped re-sends: keep the writer clock reading so the stream can be replayed
            fields.append(pa.field("commit_clock", TS_TYPE, nullable=False))
        fields.append(pa.field("deleted", pa.bool_(), nullable=False))
        fields += [pa.field(n, ARROW_TYPES[t]) for n, t in self.table.columns]
        return pa.schema(fields)

    # -------------------------------------------------------------------------------
    def commit(self, chunk: Batch, horizon: int | None):
        """Commit every batch with c < horizon (all if horizon is None), in c order."""
        data = chunk.concat()
        if self.pending is not None:
            data = self.pending if data is None else {
                k: np.concatenate([self.pending[k], data[k]]) for k in data}
            self.pending = None
        if data is None:
            return
        # Stable sort by (c, batch) keeps input order inside each batch.
        order = np.lexsort((np.arange(len(data["c"])), data["batch"], data["c"]))
        data = {k: v[order] for k, v in data.items()}
        if horizon is not None:
            late = data["c"] >= horizon
            if late.any():
                self.pending = {k: v[late] for k, v in data.items()}
                data = {k: v[~late] for k, v in data.items()}
        if len(data["c"]) == 0:
            return
        dup = data["dup"]
        kept = {k: v[~dup] for k, v in data.items()}
        dropped = {k: v[dup] for k, v in data.items()}
        n = len(kept["c"])
        if n:
            # §4.2: system_time = max(c, last), non-decreasing over commit order.
            st = np.maximum.accumulate(kept["c"])
            if self.last_system_time is not None:
                st = np.maximum(st, self.last_system_time)
            self.last_system_time = int(st[-1])
            seq = np.arange(self.next_seq, self.next_seq + n, dtype=np.int64)
            self.next_seq += n
            self.stored += n
            # batch count: number of distinct (c, batch) runs
            self.batches += int(np.count_nonzero(np.diff(kept["batch"]) != 0) + 1)
            self._buffer(self._arrow(kept, st, seq))
        if len(dropped["c"]):
            self.duplicates += len(dropped["c"])
            self.dup_buf.append(self._arrow(dropped, None, None))

    def _arrow(self, d: dict, st, seq) -> pa.Table:
        sid = d["series_id"].astype(np.uint32)
        names = pa.array(self.series_names[sid], type=pa.string()).dictionary_encode()
        cols = {
            "series": names,
            "series_id": pa.array(sid, type=pa.uint32()),
            "value_time": _ts_array(d["value_time"]),
            "published_time": _ts_array(d["published_time"]),
            "received_time": _ts_array(d["received_time"]),
        }
        if st is not None:
            cols["system_time"] = _ts_array(st)
            cols["seq"] = pa.array(seq, type=pa.int64())
        else:
            cols["commit_clock"] = _ts_array(d["c"])
        deleted = d["deleted"].astype(bool)
        cols["deleted"] = pa.array(deleted, type=pa.bool_())
        for name, t in self.table.columns:
            a = d[name]
            if t == "f64":
                null = deleted | np.isnan(a.astype(np.float64))
            elif t == "timestamp":
                null = deleted | (a.astype(np.int64) == NULL_TS)
            else:
                null = deleted | (a == None)  # noqa: E711 (object arrays)
            cols[name] = _value_array(t, a, null)
        schema = self.schema if st is not None else self._schema(with_engine=False)
        return pa.table(cols, schema=schema)

    def _buffer(self, t: pa.Table):
        self.buf.append(t)
        self.buf_rows += t.num_rows
        if self.buf_rows >= self.ROW_GROUP:
            self._flush()

    def _flush(self):
        if self.buf:
            t = pa.concat_tables(self.buf).combine_chunks()
            # unify dictionaries so each row group has one series dictionary
            t = t.set_column(0, "series", t.column("series").cast(pa.string()).dictionary_encode())
            self.writer.write_table(t, row_group_size=self.ROW_GROUP)
            self.buf, self.buf_rows = [], 0

    def close(self) -> dict:
        if self.pending is not None:
            p, self.pending = self.pending, None
            self.commit(_FromDict(self.table, p), None)
        self._flush()
        self.writer.close()
        if self.dup_buf:
            t = pa.concat_tables(self.dup_buf)
        else:
            t = self._schema(with_engine=False).empty_table()
        pq.write_table(t, self.out_dir / "resends.parquet", compression="zstd")
        return {"stored": self.stored, "duplicates_dropped": self.duplicates,
                "batches": self.batches,
                "last_system_time": self.last_system_time}


class _FromDict(Batch):
    def __init__(self, table, d):
        super().__init__(table)
        self._d = d

    def concat(self):
        return self._d


def write_catalog(path: Path, catalog: dict):
    path.write_text(json.dumps(catalog, indent=1))
