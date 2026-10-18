"""Column values: §2.1 types, §4.1 step 3 type checks, §4.3 equality, JSON encoding.

Internal (native) representation of a non-null value by column type:

    f64        Python float (any bit pattern; NaN from JSON is the canonical quiet NaN)
    i64        Python int in [-2^63, 2^63-1]
    bool       Python bool
    string     Python str
    timestamp  Python int (µs since epoch, §1)

null is None.
"""

import math
import re
import struct

from .errors import MalformedInput, MuninnError
from .timefmt import I64_MAX, I64_MIN, format_timestamp, parse_timestamp

# §2.1 column types.
COLUMN_TYPES = ("f64", "i64", "bool", "string", "timestamp")

# §4.1: non-finite f64 values are written as these strings in JSON.
_NONFINITE_IN = {"NaN": math.nan, "inf": math.inf, "-inf": -math.inf}
_I64_STR_RE = re.compile(r"-?[0-9]+")

# Integers up to this magnitude are emitted as JSON numbers; beyond it as strings (§4.1:
# a decimal string is accepted "for values beyond 2^53").
JSON_SAFE_INT = 2**53


def check_type(col_type: str, raw):
    """§4.1 step 3: validate a non-null JSON value against a column type.

    Returns the native value or raises type_mismatch.
    """
    assert raw is not None
    if col_type == "f64":
        # "f64 accepts any JSON number (integers are converted)". bool is not a number.
        if isinstance(raw, bool):
            raise MuninnError("type_mismatch", f"f64 got {raw!r}")
        if isinstance(raw, float):
            return raw
        if isinstance(raw, int):
            try:
                return float(raw)  # nearest f64 (round-half-even)
            except OverflowError:
                raise MuninnError("type_mismatch", f"integer too large for f64: {raw}")
        if isinstance(raw, str) and raw in _NONFINITE_IN:
            return _NONFINITE_IN[raw]
        raise MuninnError("type_mismatch", f"f64 got {raw!r}")
    if col_type == "i64":
        # "integral numbers in i64 range, or a decimal string". A JSON number with a
        # fraction or exponent (parsed as float) is rejected (AMBIGUITIES.md A3).
        if isinstance(raw, bool):
            raise MuninnError("type_mismatch", f"i64 got {raw!r}")
        if isinstance(raw, int):
            value = raw
        elif isinstance(raw, str) and _I64_STR_RE.fullmatch(raw):
            value = int(raw)
        else:
            raise MuninnError("type_mismatch", f"i64 got {raw!r}")
        if not (I64_MIN <= value <= I64_MAX):
            raise MuninnError("type_mismatch", f"i64 out of range: {raw!r}")
        return value
    if col_type == "bool":
        if isinstance(raw, bool):
            return raw
        raise MuninnError("type_mismatch", f"bool got {raw!r}")
    if col_type == "string":
        if isinstance(raw, str):
            return raw
        raise MuninnError("type_mismatch", f"string got {raw!r}")
    if col_type == "timestamp":
        # "timestamp accepts the text form (§1)". Anything that is not a valid timestamp
        # (unparsable or out of range) is a type mismatch here (AMBIGUITIES.md A4).
        if not isinstance(raw, str):
            raise MuninnError("type_mismatch", f"timestamp got {raw!r}")
        try:
            return parse_timestamp(raw)
        except MuninnError:
            raise MuninnError("type_mismatch", f"not a valid timestamp: {raw!r}")
    raise MalformedInput(f"unknown column type {col_type!r}")


def f64_bits(x: float) -> int:
    return struct.unpack("<Q", struct.pack("<d", x))[0]


def same_value(col_type: str, a, b) -> bool:
    """§4.3: values compared by type — f64 bitwise, others by equality; null == null."""
    if a is None or b is None:
        return a is None and b is None
    if col_type == "f64":
        return f64_bits(a) == f64_bits(b)
    return a == b


def encode_value(col_type: str, v):
    """Native value -> JSON value (the inverse of check_type, canonical form)."""
    if v is None:
        return None
    if col_type == "f64":
        if math.isnan(v):
            return "NaN"
        if math.isinf(v):
            return "inf" if v > 0 else "-inf"
        return v  # json.dumps uses repr(): shortest string that round-trips exactly
    if col_type == "i64":
        return v if -JSON_SAFE_INT <= v <= JSON_SAFE_INT else str(v)
    if col_type == "timestamp":
        return format_timestamp(v)
    return v
