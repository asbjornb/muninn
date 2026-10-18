"""§1 Timestamps and durations.

A timestamp is an int: microseconds since 1970-01-01T00:00:00Z (proleptic Gregorian,
no leap seconds). Dates are converted with Howard Hinnant's civil-date algorithms rather
than `datetime`, so that the arithmetic is explicit (and handles year 0000, which can
occur transiently on input like `0000-12-31T23:30:00-01:00` = `0001-01-01T00:30:00Z`).

Text grammar accepted (see spec/AMBIGUITIES.md A1 for the choices made):

    YYYY-MM-DDTHH:MM:SS[.f{1,6}](Z|+HH:MM|-HH:MM)

ASCII digits only, upper-case `T` and `Z`, seconds 00..59 (no leap seconds), offset hours
00..23 and minutes 00..59. The range check of §1 is applied to the UTC result.
"""

import re

from .errors import MuninnError

US_PER_S = 1_000_000
US_PER_DAY = 86_400 * US_PER_S


def days_from_civil(y: int, m: int, d: int) -> int:
    """Days since 1970-01-01 of the proleptic Gregorian date y-m-d."""
    y -= 1 if m <= 2 else 0
    era = y // 400
    yoe = y - era * 400  # [0, 399]
    mp = (m + 9) % 12  # March = 0
    doy = (153 * mp + 2) // 5 + d - 1  # [0, 365]
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy  # [0, 146096]
    return era * 146097 + doe - 719468


def civil_from_days(z: int) -> tuple[int, int, int]:
    """Inverse of days_from_civil."""
    z += 719468
    era = z // 146097
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    return (y + (1 if m <= 2 else 0), m, d)


def is_leap(y: int) -> bool:
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


def days_in_month(y: int, m: int) -> int:
    if m == 2:
        return 29 if is_leap(y) else 28
    return 30 if m in (4, 6, 9, 11) else 31


# §1: valid timestamps lie in [0001-01-01T00:00:00Z, 9999-12-31T23:59:59.999999Z].
MIN_TS = days_from_civil(1, 1, 1) * US_PER_DAY
MAX_TS = days_from_civil(9999, 12, 31) * US_PER_DAY + US_PER_DAY - 1

_TS_RE = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(?:\.([0-9]{1,6}))?"
    r"(Z|[+-][0-9]{2}:[0-9]{2})"
)


def check_range(ts: int) -> int:
    """§1 range check."""
    if not (MIN_TS <= ts <= MAX_TS):
        raise MuninnError("invalid_timestamp", f"out of range: {ts}")
    return ts


def parse_timestamp(text) -> int:
    """§1 text form -> microseconds. Raises invalid_timestamp."""
    if not isinstance(text, str):
        raise MuninnError("invalid_timestamp", f"not a timestamp string: {text!r}")
    m = _TS_RE.fullmatch(text)
    if m is None:
        raise MuninnError("invalid_timestamp", f"unparsable: {text!r}")
    year, month, day, hour, minute, second = (int(g) for g in m.groups()[:6])
    frac, offset = m.group(7), m.group(8)
    if not (1 <= month <= 12) or not (1 <= day <= days_in_month(year, month)):
        raise MuninnError("invalid_timestamp", f"no such date: {text!r}")
    if hour > 23 or minute > 59 or second > 59:
        raise MuninnError("invalid_timestamp", f"no such time: {text!r}")
    micros = int(frac.ljust(6, "0")) if frac else 0
    offset_us = 0
    if offset != "Z":
        oh, om = int(offset[1:3]), int(offset[4:6])
        if oh > 23 or om > 59:
            raise MuninnError("invalid_timestamp", f"bad offset: {text!r}")
        offset_us = (oh * 3600 + om * 60) * US_PER_S
        if offset[0] == "-":
            offset_us = -offset_us
    local = (
        days_from_civil(year, month, day) * US_PER_DAY
        + (hour * 3600 + minute * 60 + second) * US_PER_S
        + micros
    )
    # §1: numeric offsets are converted to UTC: local = utc + offset.
    return check_range(local - offset_us)


def to_timestamp(x) -> int:
    """Inputs (§9 JSON) carry timestamps only in the §1 text form; a JSON number (or a
    missing value) is not a valid timestamp."""
    return parse_timestamp(x)


def format_timestamp(ts: int) -> str:
    """§1 canonical output: `Z`, no fraction if sub-second part is zero, else 6 digits."""
    days, rem = divmod(ts, US_PER_DAY)
    y, mo, d = civil_from_days(days)
    secs, micros = divmod(rem, US_PER_S)
    h, rest = divmod(secs, 3600)
    mi, s = divmod(rest, 60)
    base = f"{y:04d}-{mo:02d}-{d:02d}T{h:02d}:{mi:02d}:{s:02d}"
    return f"{base}.{micros:06d}Z" if micros else f"{base}Z"


# §1 durations: [-]<integer><unit>, units us ms s m h d (d = 86 400 s exactly).
_DUR_RE = re.compile(r"(-?)([0-9]+)(us|ms|s|m|h|d)")
_UNIT_US = {
    "us": 1,
    "ms": 1_000,
    "s": US_PER_S,
    "m": 60 * US_PER_S,
    "h": 3600 * US_PER_S,
    "d": US_PER_DAY,
}
I64_MIN, I64_MAX = -(2**63), 2**63 - 1


def parse_duration(text) -> int:
    """§1 duration text -> microseconds.

    The spec gives no error code for a bad duration; the only place durations are read
    is the grid window, where §8 says a "bad window" is `invalid_range`. The value must
    fit in i64 microseconds (AMBIGUITIES.md A6).
    """
    if not isinstance(text, str):
        raise MuninnError("invalid_range", f"not a duration: {text!r}")
    m = _DUR_RE.fullmatch(text)
    if m is None:
        raise MuninnError("invalid_range", f"unparsable duration: {text!r}")
    value = int(m.group(2)) * _UNIT_US[m.group(3)]
    if m.group(1) == "-":
        value = -value
    if not (I64_MIN <= value <= I64_MAX):
        raise MuninnError("invalid_range", f"duration out of i64 range: {text!r}")
    return value
