"""Input normalisation: Python / numpy / pandas / polars values → what the native module
takes (ints = µs, strings = SPEC text, Arrow streams, lists of dicts).

Nothing here raises for a *bad value*: an unusable time becomes an :class:`_Invalid`
marker, which the native module reports as ``invalid_timestamp`` at the point the
SPEC §10.4 check order says it should (after ``unknown_table`` / ``unknown_series``).
"""

from __future__ import annotations

import datetime as _dt
import sys
from typing import Any

from ._errors import InvalidArgumentError

UTC = _dt.timezone.utc
EPOCH = _dt.datetime(1970, 1, 1, tzinfo=UTC)
_US = _dt.timedelta(microseconds=1)

# numpy datetime64 / timedelta64 units → µs multiplier (None: finer than µs).
_NP_UNITS = {
    "W": 7 * 86_400_000_000,
    "D": 86_400_000_000,
    "h": 3_600_000_000,
    "m": 60_000_000,
    "s": 1_000_000,
    "ms": 1_000,
    "us": 1,
    "μs": 1,
}
_NP_FINE = {"ns": 1_000, "ps": 1_000_000, "fs": 1_000_000_000, "as": 1_000_000_000_000}


class _Invalid:
    """A value that is not a usable timestamp / duration (reported later, in order)."""

    __slots__ = ("why",)

    def __init__(self, why: str) -> None:
        self.why = why

    def __str__(self) -> str:
        return self.why

    __repr__ = __str__


def _is_nat(x: Any) -> bool:
    return type(x).__name__ == "NaTType" or (
        type(x).__name__ in ("datetime64", "timedelta64") and x != x  # noqa: PLR0124
    )


def _np_scaled(value: int, unit: str, count: int, what: str) -> int | _Invalid:
    if unit in _NP_UNITS:
        return value * count * _NP_UNITS[unit]
    if unit in _NP_FINE:
        div = _NP_FINE[unit]
        total = value * count
        if total % div:
            return _Invalid(f"{what} has a sub-microsecond part (Muninn stores microseconds)")
        return total // div
    return _Invalid(f"numpy {what} unit {unit!r} is not supported (calendar units are ambiguous)")


def time_arg(x: Any, assume_utc: bool | None = False, nat_is_null: bool = False) -> Any:
    """A time argument → ``int`` µs since the epoch (UTC), ``str`` (SPEC §1 text, validated
    natively), ``None`` (only for NaT when ``nat_is_null``) or an :class:`_Invalid`.

    Accepted: ``int`` (µs), ``str``, timezone-aware ``datetime`` / ``pandas.Timestamp``
    (naive ones only with ``assume_utc=True``), ``numpy.datetime64`` (UTC by definition:
    numpy has no time zones), and numpy integers (µs)."""
    if x is None:
        return None
    if isinstance(x, bool):
        return _Invalid(f"{x!r} is not a timestamp")
    if isinstance(x, (int, str)):
        return x
    if _is_nat(x):
        return None if nat_is_null else _Invalid("NaT is not a timestamp")
    if isinstance(x, _dt.datetime):
        ns = getattr(x, "nanosecond", 0)  # pandas.Timestamp
        if ns:
            return _Invalid(f"{x!r} has a sub-microsecond part (Muninn stores microseconds)")
        if x.tzinfo is None or x.utcoffset() is None:
            if not assume_utc:
                return _Invalid(
                    f"naive datetime {x!r}: give it a time zone (e.g. tzinfo=timezone.utc)"
                    + ("" if assume_utc is None else ", or pass assume_utc=True")
                )
            x = x.replace(tzinfo=UTC)
        if hasattr(x, "to_pydatetime"):
            x = x.to_pydatetime()
        return (x - EPOCH) // _US
    if isinstance(x, _dt.date):
        return _Invalid(f"a date ({x!r}) is not a timestamp; use a datetime with a time zone")
    tname = type(x).__name__
    if tname == "datetime64":
        np = sys.modules["numpy"]
        unit, count = np.datetime_data(x.dtype)
        return _np_scaled(int(x.astype("int64")), unit, count, "datetime64")
    if hasattr(x, "__index__") and not isinstance(x, float):
        try:
            return int(x.__index__())  # numpy integers: µs
        except Exception:  # noqa: BLE001
            pass
    if hasattr(x, "as_py") and not callable(getattr(x, "__arrow_c_stream__", None)):
        return time_arg(x.as_py(), assume_utc, nat_is_null)  # pyarrow scalar
    return _Invalid(f"{x!r} ({tname}) is not a timestamp")


def dur_arg(x: Any) -> Any:
    """A duration → ``int`` µs, ``str`` (SPEC §1 duration text) or :class:`_Invalid`."""
    if x is None:
        return None
    if isinstance(x, bool):
        return _Invalid(f"{x!r} is not a duration")
    if isinstance(x, (int, str)):
        return x
    if isinstance(x, _dt.timedelta):
        ns = getattr(x, "nanoseconds", 0)  # pandas.Timedelta
        if ns:
            return _Invalid(f"{x!r} has a sub-microsecond part")
        return x // _US
    if type(x).__name__ == "timedelta64":
        if x != x:  # noqa: PLR0124
            return _Invalid("NaT is not a duration")
        np = sys.modules["numpy"]
        unit, count = np.datetime_data(x.dtype)
        return _np_scaled(int(x.astype("int64")), unit, count, "timedelta64")
    return _Invalid(f"{x!r} is not a duration")


def value_arg(x: Any, assume_utc: bool) -> tuple[str, Any]:
    """A non-primitive write value (numpy / pandas scalar, datetime, ...) → ``(kind,
    payload)`` for the native module."""
    if _is_nat(x) or type(x).__name__ == "NAType":
        return ("null", None)
    tname = type(x).__name__
    if tname in ("bool_", "bool") and hasattr(x, "item"):
        return ("bool", bool(x))
    if isinstance(x, (_dt.datetime, _dt.date)) or tname == "datetime64":
        t = time_arg(x, assume_utc)
        if isinstance(t, int):
            return ("ts", t)
        return ("invalid", str(t))
    if hasattr(x, "dtype") and hasattr(x, "item"):  # other numpy scalars
        kind = x.dtype.kind
        if kind in "iu":
            return ("int", int(x))
        if kind == "f":
            return ("float", float(x))
        if kind == "U":
            return ("str", str(x))
    return ("invalid", f"{x!r} ({tname})")


def is_arrow(x: Any) -> bool:
    return callable(getattr(x, "__arrow_c_stream__", None))


def _is_pandas_df(x: Any) -> bool:
    pd = sys.modules.get("pandas")
    return pd is not None and isinstance(x, pd.DataFrame)


def to_arrow(x: Any) -> Any:
    """pyarrow Table / RecordBatch / RecordBatchReader, polars DataFrame, pandas DataFrame,
    or anything with ``__arrow_c_stream__`` → an object with ``__arrow_c_stream__``;
    ``None`` if ``x`` is none of these."""
    import pyarrow as pa

    if isinstance(x, pa.RecordBatch):
        return pa.Table.from_batches([x])
    if _is_pandas_df(x):
        # pandas missing values (NaN in float columns, None, NaT) become nulls.
        return pa.Table.from_pandas(x, preserve_index=False)
    if is_arrow(x):
        return x
    to = getattr(x, "to_arrow", None)  # older polars
    if callable(to):
        return to()
    return None


def as_pa_table(x: Any) -> Any:
    import pyarrow as pa

    a = to_arrow(x)
    if a is None:
        return None
    return a if isinstance(a, pa.Table) else pa.table(a)


def series_arg(series: Any) -> Any:
    """``None``, one name, or any iterable of names → ``None`` / ``list``."""
    if series is None or isinstance(series, str):
        return None if series is None else [series]
    if isinstance(series, (list, tuple, set, frozenset)):
        return list(series)
    if hasattr(series, "to_pylist"):
        return series.to_pylist()
    if hasattr(series, "tolist"):
        return series.tolist()
    if hasattr(series, "__iter__") and not isinstance(series, (dict, bytes)):
        return list(series)
    return series  # rejected natively (invalid_argument)


def frontiers_arg(frontiers: Any) -> Any:
    """A list-like of times → list of normalised time arguments (``None`` stays ``None``:
    the native module rejects it)."""
    if frontiers is None or isinstance(frontiers, (str, bytes, dict)):
        return None
    if hasattr(frontiers, "to_pylist"):  # pyarrow
        frontiers = frontiers.to_pylist()
    try:
        items = list(frontiers)
    except TypeError:
        return None
    return [_read_time(f) for f in items]


def _read_time(x: Any) -> Any:
    # Reads never treat a missing marker as "unbounded": NaT is an error, not None.
    t = time_arg(x, None)
    return _Invalid("None is not a frontier") if t is None else t


def read_time(x: Any) -> Any:
    """An optional read bound: ``None`` = unbounded; NaT is rejected (never unbounded)."""
    return None if x is None else _read_time(x)


def window_arg(window: Any) -> tuple[Any, Any]:
    """``None`` or a ``(start, end)`` pair of durations."""
    if window is None:
        return (None, None)
    if isinstance(window, (str, bytes, dict)) or not hasattr(window, "__len__"):
        bad = _Invalid(f"window must be a (start, end) pair, not {window!r}")
        return (bad, bad)
    if len(window) != 2:
        bad = _Invalid(f"window must be a (start, end) pair, not {window!r}")
        return (bad, bad)
    return (dur_arg(window[0]), dur_arg(window[1]))


def columns_arg(columns: Any) -> list[tuple[Any, Any]]:
    """``{"name": "type"}``, ``[("name", "type")]`` or ``[{"name": ..., "type": ...}]``."""
    if columns is None:
        return []
    out = []
    if isinstance(columns, dict):
        items = list(columns.items())
    elif isinstance(columns, (list, tuple)):
        items = []
        for c in columns:
            if isinstance(c, dict):
                if set(c) != {"name", "type"}:
                    raise InvalidArgumentError(
                        f"a column spec has exactly `name` and `type`, not {sorted(c)}"
                    )
                items.append((c["name"], c["type"]))
            elif isinstance(c, (list, tuple)) and len(c) == 2:
                items.append((c[0], c[1]))
            else:
                raise InvalidArgumentError(f"bad column spec {c!r}")
    else:
        raise InvalidArgumentError(
            f"columns must be a dict or a list of (name, type), not {type(columns).__name__}"
        )
    for name, ty in items:
        out.append((name, _TYPE_ALIASES.get(ty, ty) if isinstance(ty, type) else ty))
    return out


_TYPE_ALIASES: dict[Any, str] = {
    float: "f64",
    int: "i64",
    bool: "bool",
    str: "string",
    _dt.datetime: "timestamp",
}


def micros_to_datetime(us: int | None) -> _dt.datetime | None:
    return None if us is None else EPOCH + _dt.timedelta(microseconds=us)
