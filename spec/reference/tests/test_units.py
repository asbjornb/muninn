"""Unit tests: §1 timestamp/duration parsing and canonical formatting, §4.1 type checks."""

import datetime as dt
import math
import re

import pytest
from hypothesis import given
from hypothesis import strategies as st

from muninn_ref.errors import MuninnError
from muninn_ref.timefmt import (
    MAX_TS,
    MIN_TS,
    civil_from_days,
    days_from_civil,
    format_timestamp,
    parse_duration,
    parse_timestamp,
)
from muninn_ref.values import check_type, encode_value, f64_bits, same_value

EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)


def via_datetime(us: int) -> dt.datetime:
    return EPOCH + dt.timedelta(microseconds=us)


# ---------------------------------------------------------------------------------------
# §1 timestamps
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        ("1970-01-01T00:00:00Z", 0),
        ("1970-01-01T00:00:00.000001Z", 1),
        ("1969-12-31T23:59:59.999999Z", -1),
        ("2025-01-15T12:00:00Z", 1736942400 * 10**6),
        ("2025-01-15T12:00:00.25Z", 1736942400 * 10**6 + 250_000),
        ("2025-01-15T13:00:00+01:00", 1736942400 * 10**6),
        ("2025-01-15T06:30:00-05:30", 1736942400 * 10**6),
        ("2025-01-15T12:00:00-00:00", 1736942400 * 10**6),
        ("2025-01-15T12:00:00.000000Z", 1736942400 * 10**6),
        ("0001-01-01T00:00:00Z", MIN_TS),
        ("9999-12-31T23:59:59.999999Z", MAX_TS),
        ("0000-12-31T23:30:00-01:00", MIN_TS + 30 * 60 * 10**6),
        ("2024-02-29T00:00:00Z", 1709164800 * 10**6),
    ],
)
def test_parse_known(text, expected):
    assert parse_timestamp(text) == expected


def test_range_matches_datetime():
    assert via_datetime(MIN_TS) == dt.datetime(1, 1, 1, tzinfo=dt.UTC)
    assert via_datetime(MAX_TS) == dt.datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=dt.UTC)


@pytest.mark.parametrize(
    "text",
    [
        "2025-01-15T12:00:00",  # no zone
        "2025-01-15 12:00:00Z",  # space
        "2025-01-15t12:00:00Z",  # lower-case t
        "2025-01-15T12:00:00z",  # lower-case z
        "2025-02-29T00:00:00Z",  # not a leap year
        "1900-02-29T00:00:00Z",  # century, not leap
        "2025-04-31T00:00:00Z",
        "2025-00-10T00:00:00Z",
        "2025-13-10T00:00:00Z",
        "2025-01-00T00:00:00Z",
        "2025-01-15T24:00:00Z",
        "2025-01-15T12:60:00Z",
        "2016-12-31T23:59:60Z",  # leap second
        "2025-01-15T12:00:00.1234567Z",
        "2025-01-15T12:00:00.Z",
        "2025-01-15T12:00:00+0100",
        "2025-01-15T12:00:00+01",
        "2025-01-15T12:00:00+24:00",
        "2025-01-15T12:00:00+01:60",
        "2025-1-15T12:00:00Z",
        "+2025-01-15T12:00:00Z",
        "10000-01-01T00:00:00Z",
        "0000-12-31T23:59:59.999999Z",  # before 0001
        "0001-01-01T00:30:00+01:00",  # before 0001 after conversion
        "9999-12-31T23:30:00-01:00",  # after 9999 after conversion
        "２025-01-15T12:00:00Z",  # non-ASCII digit
        "2025-01-15T12:00:00Z ",
        "",
        None,
        0,
        1736942400000000,
        True,
    ],
)
def test_parse_rejects(text):
    with pytest.raises(MuninnError) as e:
        parse_timestamp(text)
    assert e.value.code == "invalid_timestamp"


@pytest.mark.parametrize(
    "us, text",
    [
        (0, "1970-01-01T00:00:00Z"),
        (1, "1970-01-01T00:00:00.000001Z"),
        (-1, "1969-12-31T23:59:59.999999Z"),
        (500_000, "1970-01-01T00:00:00.500000Z"),
        (MIN_TS, "0001-01-01T00:00:00Z"),
        (MAX_TS, "9999-12-31T23:59:59.999999Z"),
        (1736942400 * 10**6 + 100_000, "2025-01-15T12:00:00.100000Z"),
    ],
)
def test_format_canonical(us, text):
    assert format_timestamp(us) == text


CANONICAL = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{6})?Z")
timestamps = st.integers(min_value=MIN_TS, max_value=MAX_TS)


@given(timestamps)
def test_format_parse_roundtrip(us):
    text = format_timestamp(us)
    assert CANONICAL.fullmatch(text)
    assert (".000000" not in text) and (("." in text) == (us % 10**6 != 0))
    assert parse_timestamp(text) == us


@given(timestamps)
def test_format_agrees_with_datetime(us):
    d = via_datetime(us)
    expected = d.strftime("%Y-%m-%dT%H:%M:%S")
    expected = f"{d.year:04d}{expected[expected.index('-'):]}"
    expected += f".{d.microsecond:06d}Z" if d.microsecond else "Z"
    assert format_timestamp(us) == expected


DAY = 86_400 * 10**6


@given(st.integers(min_value=MIN_TS + DAY, max_value=MAX_TS - DAY),
       st.integers(min_value=-(23 * 60 + 59), max_value=23 * 60 + 59))
def test_offsets_convert_to_utc(us, offset_min):
    # (a day of margin: Python's datetime cannot represent the local time beyond 9999)
    local = via_datetime(us) + dt.timedelta(minutes=offset_min)
    sign = "-" if offset_min < 0 else "+"
    hh, mm = divmod(abs(offset_min), 60)
    text = local.strftime("%m-%dT%H:%M:%S")
    text = f"{local.year:04d}-{text}.{local.microsecond:06d}{sign}{hh:02d}:{mm:02d}"
    assert parse_timestamp(text) == us


@given(st.integers(min_value=-800_000, max_value=3_000_000))
def test_civil_roundtrip(days):
    assert days_from_civil(*civil_from_days(days)) == days


# ---------------------------------------------------------------------------------------
# §1 durations
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text, us",
    [
        ("0h", 0),
        ("-0m", 0),
        ("36h", 36 * 3600 * 10**6),
        ("-1d", -86400 * 10**6),
        ("90m", 90 * 60 * 10**6),
        ("1500ms", 1_500_000),
        ("7us", 7),
        ("007s", 7 * 10**6),
        ("9223372036854775807us", 2**63 - 1),
        ("-9223372036854775808us", -(2**63)),
        ("106751991d", 106751991 * 86400 * 10**6),
    ],
)
def test_duration_valid(text, us):
    assert parse_duration(text) == us


@pytest.mark.parametrize(
    "text",
    ["", "h", "1", "1 h", "1H", "1w", "+1h", "1.5h", "1h30m", "--1h", "9223372036854775808us",
     "106751992d", None, 5],
)
def test_duration_invalid(text):
    with pytest.raises(MuninnError) as e:
        parse_duration(text)
    assert e.value.code == "invalid_range"


# ---------------------------------------------------------------------------------------
# §4.1 types, §4.3 equality
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "typ, raw, native",
    [
        ("f64", 1, 1.0),
        ("f64", 1.5, 1.5),
        ("f64", -0.0, -0.0),
        ("f64", "inf", math.inf),
        ("f64", "-inf", -math.inf),
        ("f64", 2**53 + 1, float(2**53)),  # nearest f64 (ties to even)
        ("i64", 5, 5),
        ("i64", "-9223372036854775808", -(2**63)),
        ("i64", "007", 7),
        ("bool", False, False),
        ("string", "", ""),
        ("timestamp", "2025-01-15T13:00:00+01:00", 1736942400 * 10**6),
    ],
)
def test_check_type_accepts(typ, raw, native):
    got = check_type(typ, raw)
    assert type(got) is type(native)
    if typ == "f64":
        assert f64_bits(got) == f64_bits(native)
    else:
        assert got == native


def test_nan_is_canonical():
    assert f64_bits(check_type("f64", "NaN")) == 0x7FF8000000000000


@pytest.mark.parametrize(
    "typ, raw",
    [
        ("f64", True), ("f64", "1.5"), ("f64", "nan"), ("f64", "Infinity"), ("f64", [1.0]),
        ("i64", 1.5), ("i64", 2.0), ("i64", "1.5"), ("i64", "+1"), ("i64", " 1"), ("i64", True),
        ("i64", 2**63), ("i64", -(2**63) - 1), ("i64", "9223372036854775808"),
        ("bool", 1), ("bool", "true"), ("string", 5), ("string", False),
        ("timestamp", "2025-01-15"), ("timestamp", 0), ("timestamp", "9999-12-31T23:30:00-01:00"),
    ],
)
def test_check_type_rejects(typ, raw):
    with pytest.raises(MuninnError) as e:
        check_type(typ, raw)
    assert e.value.code == "type_mismatch"


def test_same_value_bitwise():
    assert same_value("f64", math.nan, math.nan)
    assert not same_value("f64", 0.0, -0.0)
    assert same_value("f64", None, None)
    assert not same_value("f64", None, 0.0)
    assert same_value("i64", 2**60, 2**60)


def test_encode_value():
    assert encode_value("f64", math.nan) == "NaN"
    assert encode_value("f64", -math.inf) == "-inf"
    assert encode_value("i64", 2**53) == 2**53
    assert encode_value("i64", 2**53 + 1) == "9007199254740993"
    assert encode_value("i64", -(2**53) - 1) == "-9007199254740993"
    assert encode_value("timestamp", 0) == "1970-01-01T00:00:00Z"
