"""Exact comparison of canonical result tables (SPEC §9 rules: complete row list, in
order; timestamps as integers; f64 bitwise)."""

from __future__ import annotations

import struct

import pyarrow as pa


def _norm_col(c: pa.ChunkedArray) -> list:
    t = c.type
    if pa.types.is_timestamp(t):
        c = c.cast(pa.int64())
    if pa.types.is_dictionary(t):
        c = c.cast(t.value_type)
    vals = c.to_pylist()
    if pa.types.is_floating(t):
        return [None if v is None else struct.pack("<d", v) for v in vals]
    if pa.types.is_integer(t) or pa.types.is_timestamp(t) or pa.types.is_decimal(t):
        return [None if v is None else int(v) for v in vals]
    return vals


def rows(t: pa.Table) -> list[tuple]:
    cols = [_norm_col(t.column(i)) for i in range(t.num_columns)]
    return list(zip(*cols)) if cols else []


def diff(expected: pa.Table, actual: pa.Table) -> str | None:
    """None if identical, else a short human-readable description of the first difference."""
    if expected.column_names != actual.column_names:
        return f"columns differ: expected {expected.column_names}, got {actual.column_names}"
    e, a = rows(expected), rows(actual)
    for i, (x, y) in enumerate(zip(e, a)):
        if x != y:
            return f"row {i} differs:\n  expected {_show(expected, x)}\n  actual   {_show(actual, y)}"
    if len(e) != len(a):
        return f"row count differs: expected {len(e)}, got {len(a)}"
    return None


def _show(t, row):
    out = {}
    for name, v in zip(t.column_names, row):
        out[name] = struct.unpack("<d", v)[0] if isinstance(v, bytes) else v
    return out
