//! Timestamps and durations (SPEC §1, decision 0002).
//!
//! A [`Timestamp`] is an `i64` count of microseconds since the Unix epoch, UTC, restricted
//! to the years 0001–9999. Text form is RFC 3339; civil-date conversion uses Howard
//! Hinnant's `days_from_civil` / `civil_from_days` algorithms over the proleptic Gregorian
//! calendar, implemented here so that the core crate has no date-library dependency.

use std::fmt;
use std::str::FromStr;

/// Microseconds per second.
pub const MICROS_PER_SECOND: i64 = 1_000_000;
/// Microseconds per minute.
pub const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
/// Microseconds per hour.
pub const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
/// Microseconds per day (`d` = 86 400 s exactly; SPEC §1 has no calendars in durations).
pub const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

/// A point in time: microseconds since 1970-01-01T00:00:00Z (SPEC §1).
///
/// The invariant `Timestamp::MIN <= t <= Timestamp::MAX` is enforced by every constructor,
/// so a `Timestamp` value is always valid. Ordering and equality are integer ordering and
/// equality — timestamps are never compared as strings.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

/// Error returned when text is not a valid SPEC §1 timestamp, or a microsecond count is out
/// of range. Converts into [`crate::Error`] with kind `InvalidTimestamp`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseTimestampError {
    message: String,
}

impl ParseTimestampError {
    fn new(message: impl Into<String>) -> Self {
        ParseTimestampError {
            message: message.into(),
        }
    }
}

impl fmt::Display for ParseTimestampError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ParseTimestampError {}

/// A broken-down UTC date and time (proleptic Gregorian calendar).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CivilDateTime {
    /// Year, 1..=9999 for any valid [`Timestamp`].
    pub year: i64,
    /// Month, 1..=12.
    pub month: u32,
    /// Day of month, 1..=31.
    pub day: u32,
    /// Hour, 0..=23.
    pub hour: u32,
    /// Minute, 0..=59.
    pub minute: u32,
    /// Second, 0..=59 (no leap seconds).
    pub second: u32,
    /// Sub-second part in microseconds, 0..=999_999.
    pub micros: u32,
}

/// Is `year` a leap year in the proleptic Gregorian calendar?
pub const fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Number of days in `month` (1..=12) of `year`. Returns 0 for an invalid month.
pub const fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Days since 1970-01-01 of the civil date `year-month-day` (Hinnant's `days_from_civil`).
///
/// Valid for any year representable without overflow; `month` must be 1..=12 and `day`
/// 1..=31 (the day is not checked against the month — callers validate first).
pub const fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400; // [0, 399]
    let m = month as i64;
    let mp = if m > 2 { m - 3 } else { m + 9 }; // March = 0
    let doy = (153 * mp + 2) / 5 + day as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Civil date `(year, month, day)` of `days` since 1970-01-01 (Hinnant's
/// `civil_from_days`). Inverse of [`days_from_civil`].
pub const fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

impl Timestamp {
    /// The earliest valid timestamp, `0001-01-01T00:00:00Z`.
    pub const MIN: Timestamp = Timestamp(-62_135_596_800 * MICROS_PER_SECOND);
    /// The latest valid timestamp, `9999-12-31T23:59:59.999999Z`.
    pub const MAX: Timestamp = Timestamp(253_402_300_800 * MICROS_PER_SECOND - 1);
    /// The Unix epoch, `1970-01-01T00:00:00Z`.
    pub const UNIX_EPOCH: Timestamp = Timestamp(0);

    /// Builds a timestamp from microseconds since the epoch; `None` if out of range.
    pub const fn from_micros(micros: i64) -> Option<Timestamp> {
        if micros >= Self::MIN.0 && micros <= Self::MAX.0 {
            Some(Timestamp(micros))
        } else {
            None
        }
    }

    /// [`Timestamp::from_micros`] for a whole vector (reusing its allocation): `Err(i)`
    /// with the index of the first out-of-range value.
    pub fn from_micros_vec(v: Vec<i64>) -> Result<Vec<Timestamp>, usize> {
        if let Some(i) = v
            .iter()
            .position(|x| !(Self::MIN.0..=Self::MAX.0).contains(x))
        {
            return Err(i);
        }
        Ok(v.into_iter().map(Timestamp).collect())
    }

    /// Appends `src` as timestamps to `dst` (no allocation if `dst` has the capacity):
    /// `Err(i)` with the index of the first out-of-range value, `dst` unchanged then.
    pub fn extend_from_micros(dst: &mut Vec<Timestamp>, src: &[i64]) -> Result<(), usize> {
        if let Some(i) = src
            .iter()
            .position(|x| !(Self::MIN.0..=Self::MAX.0).contains(x))
        {
            return Err(i);
        }
        dst.extend(src.iter().map(|&x| Timestamp(x)));
        Ok(())
    }

    /// Like [`Timestamp::from_micros`] but with an error suitable for `?`.
    pub fn try_from_micros(micros: i64) -> Result<Timestamp, ParseTimestampError> {
        Self::from_micros(micros).ok_or_else(|| {
            ParseTimestampError::new(format!(
                "timestamp {micros}us is outside 0001-01-01T00:00:00Z..=9999-12-31T23:59:59.999999Z"
            ))
        })
    }

    /// Microseconds since the Unix epoch.
    pub const fn as_micros(self) -> i64 {
        self.0
    }

    /// Builds a timestamp from civil UTC fields, validating every field and the range.
    pub fn from_civil(c: CivilDateTime) -> Result<Timestamp, ParseTimestampError> {
        if !(1..=12).contains(&c.month) {
            return Err(ParseTimestampError::new(format!(
                "month {} out of range",
                c.month
            )));
        }
        if c.day < 1 || c.day > days_in_month(c.year, c.month) {
            return Err(ParseTimestampError::new(format!(
                "day {} out of range for {:04}-{:02}",
                c.day, c.year, c.month
            )));
        }
        if c.hour > 23 || c.minute > 59 || c.second > 59 || c.micros > 999_999 {
            return Err(ParseTimestampError::new("time of day out of range"));
        }
        if !(1..=9999).contains(&c.year) {
            return Err(ParseTimestampError::new(format!(
                "year {} out of range",
                c.year
            )));
        }
        let secs = days_from_civil(c.year, c.month, c.day) * 86_400
            + i64::from(c.hour) * 3600
            + i64::from(c.minute) * 60
            + i64::from(c.second);
        Self::try_from_micros(secs * MICROS_PER_SECOND + i64::from(c.micros))
    }

    /// Breaks the timestamp into civil UTC fields.
    pub const fn to_civil(self) -> CivilDateTime {
        let days = self.0.div_euclid(MICROS_PER_DAY);
        let rem = self.0.rem_euclid(MICROS_PER_DAY);
        let (year, month, day) = civil_from_days(days);
        let secs = rem / MICROS_PER_SECOND;
        CivilDateTime {
            year,
            month,
            day,
            hour: (secs / 3600) as u32,
            minute: (secs / 60 % 60) as u32,
            second: (secs % 60) as u32,
            micros: (rem % MICROS_PER_SECOND) as u32,
        }
    }

    /// Adds a duration; `None` if the result is outside the valid range.
    pub fn checked_add(self, d: Duration) -> Option<Timestamp> {
        self.0.checked_add(d.0).and_then(Self::from_micros)
    }

    /// Parses the SPEC §1 text form.
    ///
    /// Accepted: `YYYY-MM-DDTHH:MM:SS[.f{1,6}](Z|+HH:MM|-HH:MM)`, uppercase `T` and `Z` only,
    /// no leap seconds. Numeric offsets are converted to UTC. The resulting instant must lie
    /// in `[Timestamp::MIN, Timestamp::MAX]` (so e.g. `0000-12-31T23:30:00-01:00` is valid:
    /// it is the instant `0001-01-01T00:30:00Z`).
    pub fn parse(s: &str) -> Result<Timestamp, ParseTimestampError> {
        parse_rfc3339(s)
            .map_err(|why| ParseTimestampError::new(format!("invalid timestamp {s:?}: {why}")))
    }
}

fn digits(b: &[u8], start: usize, len: usize) -> Result<u32, &'static str> {
    let part = b.get(start..start + len).ok_or("too short")?;
    let mut n = 0u32;
    for &c in part {
        if !c.is_ascii_digit() {
            return Err("expected digit");
        }
        n = n * 10 + u32::from(c - b'0');
    }
    Ok(n)
}

fn expect_byte(b: &[u8], at: usize, want: u8) -> Result<(), &'static str> {
    match b.get(at) {
        Some(&c) if c == want => Ok(()),
        Some(_) => Err("unexpected character"),
        None => Err("too short"),
    }
}

fn parse_rfc3339(s: &str) -> Result<Timestamp, &'static str> {
    let b = s.as_bytes();
    let year = i64::from(digits(b, 0, 4)?);
    expect_byte(b, 4, b'-')?;
    let month = digits(b, 5, 2)?;
    expect_byte(b, 7, b'-')?;
    let day = digits(b, 8, 2)?;
    expect_byte(b, 10, b'T')?;
    let hour = digits(b, 11, 2)?;
    expect_byte(b, 13, b':')?;
    let minute = digits(b, 14, 2)?;
    expect_byte(b, 16, b':')?;
    let second = digits(b, 17, 2)?;
    let mut pos = 19;

    let mut micros = 0u32;
    if b.get(pos) == Some(&b'.') {
        pos += 1;
        let start = pos;
        while pos < b.len() && b[pos].is_ascii_digit() {
            pos += 1;
        }
        let n = pos - start;
        if n == 0 {
            return Err("empty fraction");
        }
        if n > 6 {
            return Err("more than 6 fractional digits");
        }
        micros = digits(b, start, n)? * 10u32.pow(6 - n as u32);
    }

    let offset_secs: i64 = match b.get(pos) {
        Some(b'Z') => {
            pos += 1;
            0
        }
        Some(&sign @ (b'+' | b'-')) => {
            let oh = digits(b, pos + 1, 2)?;
            expect_byte(b, pos + 3, b':')?;
            let om = digits(b, pos + 4, 2)?;
            if oh > 23 || om > 59 {
                return Err("offset out of range");
            }
            pos += 6;
            let off = i64::from(oh) * 3600 + i64::from(om) * 60;
            if sign == b'+' { off } else { -off }
        }
        Some(_) => return Err("expected 'Z' or a numeric offset"),
        None => return Err("missing 'Z' or numeric offset"),
    };
    if pos != b.len() {
        return Err("trailing characters");
    }

    if !(1..=12).contains(&month) {
        return Err("month out of range");
    }
    if day < 1 || day > days_in_month(year, month) {
        return Err("day out of range for month");
    }
    if hour > 23 || minute > 59 || second > 59 {
        return Err("time of day out of range");
    }
    // Local wall-clock seconds, then shift by the offset to UTC. Year 0000 is allowed in
    // the *local* text as long as the UTC instant is in range (checked below).
    let local_secs = days_from_civil(year, month, day) * 86_400
        + i64::from(hour) * 3600
        + i64::from(minute) * 60
        + i64::from(second);
    let utc_micros = (local_secs - offset_secs) * MICROS_PER_SECOND + i64::from(micros);
    Timestamp::from_micros(utc_micros)
        .ok_or("outside 0001-01-01T00:00:00Z..=9999-12-31T23:59:59.999999Z")
}

impl fmt::Display for Timestamp {
    /// Canonical SPEC §1 form: no fraction if the sub-second part is zero, otherwise
    /// exactly 6 digits; always `Z`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let c = self.to_civil();
        write!(
            f,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            c.year, c.month, c.day, c.hour, c.minute, c.second
        )?;
        if c.micros != 0 {
            write!(f, ".{:06}", c.micros)?;
        }
        f.write_str("Z")
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl FromStr for Timestamp {
    type Err = ParseTimestampError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Timestamp::parse(s)
    }
}

impl TryFrom<i64> for Timestamp {
    type Error = ParseTimestampError;
    fn try_from(micros: i64) -> Result<Self, Self::Error> {
        Timestamp::try_from_micros(micros)
    }
}

impl From<Timestamp> for i64 {
    fn from(t: Timestamp) -> i64 {
        t.0
    }
}

/// A signed span of time in microseconds (SPEC §1). Text form `[-]<integer><unit>` with
/// unit one of `us ms s m h d`.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Duration(i64);

/// Error returned when text is not a valid SPEC §1 duration (or overflows `i64` µs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseDurationError {
    message: String,
}

impl fmt::Display for ParseDurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ParseDurationError {}

impl Duration {
    /// The zero duration.
    pub const ZERO: Duration = Duration(0);

    /// A duration of `micros` microseconds.
    pub const fn from_micros(micros: i64) -> Duration {
        Duration(micros)
    }

    /// Length in microseconds.
    pub const fn as_micros(self) -> i64 {
        self.0
    }

    /// Parses `[-]<integer><unit>`, unit one of `us ms s m h d`. No `+` sign, no
    /// whitespace, no fractional amounts, no compound forms (`1h30m`).
    pub fn parse(s: &str) -> Result<Duration, ParseDurationError> {
        let err = |why: &str| ParseDurationError {
            message: format!("invalid duration {s:?}: {why}"),
        };
        let (neg, rest) = match s.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, s),
        };
        let split = rest
            .find(|c: char| !c.is_ascii_digit())
            .ok_or_else(|| err("missing unit"))?;
        let (num, unit) = rest.split_at(split);
        if num.is_empty() {
            return Err(err("missing integer"));
        }
        let mult = match unit {
            "us" => 1,
            "ms" => 1_000,
            "s" => MICROS_PER_SECOND,
            "m" => MICROS_PER_MINUTE,
            "h" => MICROS_PER_HOUR,
            "d" => MICROS_PER_DAY,
            _ => return Err(err("unit must be one of us, ms, s, m, h, d")),
        };
        // Parse as i128 so that "-9223372036854775808us" works and overflow is detected.
        let n: i128 = num.parse().map_err(|_| err("integer too large"))?;
        let micros = n
            .checked_mul(i128::from(mult))
            .map(|v| if neg { -v } else { v })
            .and_then(|v| i64::try_from(v).ok())
            .ok_or_else(|| err("out of range"))?;
        Ok(Duration(micros))
    }
}

impl fmt::Display for Duration {
    /// Canonical text: the largest unit that divides the duration exactly (`0s` for zero).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 == 0 {
            return f.write_str("0s");
        }
        const UNITS: [(i64, &str); 6] = [
            (MICROS_PER_DAY, "d"),
            (MICROS_PER_HOUR, "h"),
            (MICROS_PER_MINUTE, "m"),
            (MICROS_PER_SECOND, "s"),
            (1_000, "ms"),
            (1, "us"),
        ];
        for (mult, unit) in UNITS {
            if self.0 % mult == 0 {
                return write!(f, "{}{unit}", self.0 / mult);
            }
        }
        unreachable!("every integer is divisible by 1")
    }
}

impl fmt::Debug for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl FromStr for Duration {
    type Err = ParseDurationError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Duration::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn ts(s: &str) -> Timestamp {
        Timestamp::parse(s).unwrap_or_else(|e| panic!("{e}"))
    }

    fn secs(s: &str) -> i64 {
        ts(s).as_micros() / MICROS_PER_SECOND
    }

    #[test]
    fn bounds_constants() {
        assert_eq!(Timestamp::MIN.as_micros(), -62_135_596_800_000_000);
        assert_eq!(Timestamp::MAX.as_micros(), 253_402_300_799_999_999);
        assert_eq!(Timestamp::MIN.to_string(), "0001-01-01T00:00:00Z");
        assert_eq!(Timestamp::MAX.to_string(), "9999-12-31T23:59:59.999999Z");
        assert_eq!(Timestamp::UNIX_EPOCH.to_string(), "1970-01-01T00:00:00Z");
        assert_eq!(Timestamp::from_micros(Timestamp::MIN.as_micros() - 1), None);
        assert_eq!(Timestamp::from_micros(Timestamp::MAX.as_micros() + 1), None);
        assert_eq!(Timestamp::from_micros(i64::MIN), None);
        assert_eq!(Timestamp::from_micros(i64::MAX), None);
    }

    #[test]
    fn known_values() {
        // Reference values from GNU `date -u -d ... +%s`.
        assert_eq!(secs("2025-01-15T12:00:00Z"), 1_736_942_400);
        assert_eq!(secs("9999-12-31T23:59:59Z"), 253_402_300_799);
        assert_eq!(secs("0001-01-01T00:00:00Z"), -62_135_596_800);
        assert_eq!(secs("2000-02-29T00:00:00Z"), 951_782_400);
        assert_eq!(secs("1900-03-01T00:00:00Z"), -2_203_891_200);
        assert_eq!(secs("2024-12-31T23:59:59Z"), 1_735_689_599);
        assert_eq!(secs("1600-02-29T00:00:00Z"), -11_670_998_400);
        assert_eq!(secs("1970-01-01T00:00:00Z"), 0);
        assert_eq!(secs("1969-12-31T23:59:59Z"), -1);
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        assert_eq!(days_from_civil(1, 1, 1), -719_162);
        assert_eq!(days_from_civil(9999, 12, 31), 2_932_896);
        assert_eq!(civil_from_days(-719_162), (1, 1, 1));
        assert_eq!(civil_from_days(2_932_896), (9999, 12, 31));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
    }

    #[test]
    fn leap_years() {
        assert!(is_leap_year(2000));
        assert!(is_leap_year(2024));
        assert!(is_leap_year(1600));
        assert!(is_leap_year(4));
        assert!(!is_leap_year(1900));
        assert!(!is_leap_year(2100));
        assert!(!is_leap_year(2023));
        assert!(!is_leap_year(1));
        assert!(Timestamp::parse("2024-02-29T00:00:00Z").is_ok());
        assert!(Timestamp::parse("2000-02-29T00:00:00Z").is_ok());
        assert!(Timestamp::parse("2023-02-29T00:00:00Z").is_err());
        assert!(Timestamp::parse("1900-02-29T00:00:00Z").is_err());
        assert!(Timestamp::parse("2100-02-29T00:00:00Z").is_err());
        assert_eq!(
            ts("2024-03-01T00:00:00Z").as_micros() - ts("2024-02-28T00:00:00Z").as_micros(),
            2 * MICROS_PER_DAY
        );
    }

    #[test]
    fn fractions_and_canonical_output() {
        assert_eq!(
            ts("2025-01-15T12:00:00.25Z").as_micros() % MICROS_PER_SECOND,
            250_000
        );
        assert_eq!(
            ts("2025-01-15T12:00:00.25Z").to_string(),
            "2025-01-15T12:00:00.250000Z"
        );
        assert_eq!(
            ts("2025-01-15T12:00:00.000001Z").to_string(),
            "2025-01-15T12:00:00.000001Z"
        );
        assert_eq!(
            ts("2025-01-15T12:00:00.000000Z").to_string(),
            "2025-01-15T12:00:00Z"
        );
        assert_eq!(
            ts("2025-01-15T12:00:00.0Z").to_string(),
            "2025-01-15T12:00:00Z"
        );
        assert_eq!(
            ts("2025-01-15T12:00:00.123456Z").to_string(),
            "2025-01-15T12:00:00.123456Z"
        );
        assert_eq!(ts("1969-12-31T23:59:59.999999Z").as_micros(), -1);
        assert_eq!(
            ts("1969-12-31T23:59:59.5Z").to_string(),
            "1969-12-31T23:59:59.500000Z"
        );
        assert!(Timestamp::parse("2025-01-15T12:00:00.1234567Z").is_err());
        assert!(Timestamp::parse("2025-01-15T12:00:00.Z").is_err());
    }

    #[test]
    fn offsets() {
        assert_eq!(ts("2025-01-15T13:00:00+01:00"), ts("2025-01-15T12:00:00Z"));
        assert_eq!(ts("2025-01-15T06:30:00-05:30"), ts("2025-01-15T12:00:00Z"));
        assert_eq!(ts("2025-01-15T12:00:00+00:00"), ts("2025-01-15T12:00:00Z"));
        assert_eq!(ts("2025-01-15T12:00:00-00:00"), ts("2025-01-15T12:00:00Z"));
        assert_eq!(
            ts("2025-01-01T00:30:00.5+01:00").to_string(),
            "2024-12-31T23:30:00.500000Z"
        );
        assert_eq!(
            ts("2025-01-15T12:00:00+23:59").as_micros(),
            ts("2025-01-14T12:01:00Z").as_micros()
        );
        // Offsets can move an in-range local time out of range, and vice versa.
        assert!(Timestamp::parse("0001-01-01T00:30:00+01:00").is_err());
        assert!(Timestamp::parse("9999-12-31T23:30:00-01:00").is_err());
        assert_eq!(
            ts("0000-12-31T23:30:00-01:00").to_string(),
            "0001-01-01T00:30:00Z"
        );
        assert_eq!(ts("9999-12-31T23:59:59.999999Z"), Timestamp::MAX);
        assert_eq!(ts("0001-01-01T01:00:00+01:00"), Timestamp::MIN);
        assert!(Timestamp::parse("2025-01-15T12:00:00+24:00").is_err());
        assert!(Timestamp::parse("2025-01-15T12:00:00+01:60").is_err());
        assert!(Timestamp::parse("2025-01-15T12:00:00+0100").is_err());
        assert!(Timestamp::parse("2025-01-15T12:00:00+01").is_err());
    }

    #[test]
    fn rejects_malformed() {
        for bad in [
            "",
            "2025-01-15",
            "2025-01-15T12:00:00",
            "2025-01-15 12:00:00Z",
            "2025-01-15t12:00:00Z",
            "2025-01-15T12:00:00z",
            "2025-1-15T12:00:00Z",
            "25-01-15T12:00:00Z",
            "+2025-01-15T12:00:00Z",
            "2025-01-15T12:00Z",
            "2025-13-01T00:00:00Z",
            "2025-00-01T00:00:00Z",
            "2025-01-00T00:00:00Z",
            "2025-01-32T00:00:00Z",
            "2025-04-31T00:00:00Z",
            "2025-01-15T24:00:00Z",
            "2025-01-15T12:60:00Z",
            "2025-01-15T12:00:60Z",
            "2025-01-15T12:00:00ZZ",
            "2025-01-15T12:00:00Z ",
            " 2025-01-15T12:00:00Z",
            "0000-01-01T00:00:00Z",
            "10000-01-01T00:00:00Z",
            "2025-01-15T12:00:00.-1Z",
            "２０２５-01-15T12:00:00Z",
        ] {
            assert!(Timestamp::parse(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn civil_roundtrip_every_day_in_range() {
        // Exhaustive over all ~3.65M days of 0001..=9999, checked against a naive counter.
        let mut days = days_from_civil(1, 1, 1);
        for year in 1..=9999i64 {
            for month in 1..=12 {
                for day in 1..=days_in_month(year, month) {
                    assert_eq!(days_from_civil(year, month, day), days);
                    assert_eq!(civil_from_days(days), (year, month, day));
                    days += 1;
                }
            }
        }
        assert_eq!(days - 1, days_from_civil(9999, 12, 31));
    }

    #[test]
    fn civil_fields() {
        let c = ts("2024-02-29T23:59:58.000123Z").to_civil();
        assert_eq!(
            c,
            CivilDateTime {
                year: 2024,
                month: 2,
                day: 29,
                hour: 23,
                minute: 59,
                second: 58,
                micros: 123
            }
        );
        assert_eq!(
            Timestamp::from_civil(c).unwrap(),
            ts("2024-02-29T23:59:58.000123Z")
        );
        assert!(Timestamp::from_civil(CivilDateTime { year: 0, ..c }).is_err());
        assert!(Timestamp::from_civil(CivilDateTime { month: 13, ..c }).is_err());
        assert!(Timestamp::from_civil(CivilDateTime { year: 2023, ..c }).is_err());
    }

    #[test]
    fn checked_add() {
        let d = Duration::parse("36h").unwrap();
        assert_eq!(
            ts("2025-01-15T00:00:00Z").checked_add(d),
            Some(ts("2025-01-16T12:00:00Z"))
        );
        assert_eq!(Timestamp::MAX.checked_add(Duration::from_micros(1)), None);
        assert_eq!(Timestamp::MIN.checked_add(Duration::from_micros(-1)), None);
        assert_eq!(
            Timestamp::MIN.checked_add(Duration::from_micros(i64::MIN)),
            None
        );
    }

    #[test]
    fn durations() {
        let us = |s: &str| Duration::parse(s).unwrap().as_micros();
        assert_eq!(us("36h"), 36 * MICROS_PER_HOUR);
        assert_eq!(us("-1d"), -MICROS_PER_DAY);
        assert_eq!(us("90m"), 90 * MICROS_PER_MINUTE);
        assert_eq!(us("0h"), 0);
        assert_eq!(us("-0h"), 0);
        assert_eq!(us("15s"), 15 * MICROS_PER_SECOND);
        assert_eq!(us("250ms"), 250_000);
        assert_eq!(us("7us"), 7);
        assert_eq!(us("007s"), 7 * MICROS_PER_SECOND);
        assert_eq!(us("9223372036854775807us"), i64::MAX);
        assert_eq!(us("-9223372036854775808us"), i64::MIN);
        for bad in [
            "",
            "h",
            "-h",
            "1",
            "-",
            "+1h",
            "1.5h",
            "1H",
            "1 h",
            " 1h",
            "1h ",
            "1hh",
            "1h30m",
            "1w",
            "1y",
            "1ns",
            "9223372036854775808us",
            "106751992d",
            "--1h",
            "1e3s",
        ] {
            assert!(Duration::parse(bad).is_err(), "{bad:?} should be rejected");
        }
        assert_eq!(Duration::parse("36h").unwrap().to_string(), "36h");
        assert_eq!(Duration::parse("48h").unwrap().to_string(), "2d");
        assert_eq!(Duration::parse("90m").unwrap().to_string(), "90m");
        assert_eq!(Duration::parse("-1500ms").unwrap().to_string(), "-1500ms");
        assert_eq!(Duration::parse("0d").unwrap().to_string(), "0s");
    }

    proptest! {
        #[test]
        fn timestamp_roundtrip(us in Timestamp::MIN.as_micros()..=Timestamp::MAX.as_micros()) {
            let t = Timestamp::from_micros(us).unwrap();
            let text = t.to_string();
            prop_assert_eq!(Timestamp::parse(&text).unwrap(), t);
            prop_assert_eq!(Timestamp::from_civil(t.to_civil()).unwrap(), t);
        }

        #[test]
        fn timestamp_roundtrip_with_offset(
            us in (Timestamp::MIN.as_micros() + MICROS_PER_DAY)..=(Timestamp::MAX.as_micros() - MICROS_PER_DAY),
            off_h in 0u32..24, off_m in 0u32..60, neg in any::<bool>(),
        ) {
            let t = Timestamp::from_micros(us).unwrap();
            let off = i64::from(off_h * 3600 + off_m * 60) * if neg { -1 } else { 1 };
            // Render the local wall-clock time for that offset and parse it back.
            let local = Timestamp::from_micros(us + off * MICROS_PER_SECOND).unwrap().to_string();
            let local = local.trim_end_matches('Z');
            let sign = if neg { '-' } else { '+' };
            let text = format!("{local}{sign}{off_h:02}:{off_m:02}");
            prop_assert_eq!(Timestamp::parse(&text).unwrap(), t);
        }

        #[test]
        fn civil_days_roundtrip(days in -800_000i64..3_000_000) {
            let (y, m, d) = civil_from_days(days);
            prop_assert!((1..=12).contains(&m));
            prop_assert!(d >= 1 && d <= days_in_month(y, m));
            prop_assert_eq!(days_from_civil(y, m, d), days);
        }

        #[test]
        fn ordering_is_integer_ordering(a in any::<i64>(), b in any::<i64>()) {
            if let (Some(x), Some(y)) = (Timestamp::from_micros(a), Timestamp::from_micros(b)) {
                prop_assert_eq!(x.cmp(&y), a.cmp(&b));
            }
        }

        #[test]
        fn duration_roundtrip(us in any::<i64>()) {
            let d = Duration::from_micros(us);
            prop_assert_eq!(Duration::parse(&d.to_string()).unwrap(), d);
        }

        #[test]
        fn parse_never_panics(chars in proptest::collection::vec(any::<char>(), 0..40)) {
            let s: String = chars.into_iter().collect();
            let _ = Timestamp::parse(&s);
            let _ = Duration::parse(&s);
        }

        #[test]
        fn parse_near_miss_never_panics(
            chars in proptest::collection::vec(proptest::sample::select("0123456789-:TZ.+umshd ".chars().collect::<Vec<_>>()), 0..32)
        ) {
            let s: String = chars.into_iter().collect();
            if let Ok(t) = Timestamp::parse(&s) {
                // Anything accepted must round-trip through the canonical form.
                prop_assert_eq!(Timestamp::parse(&t.to_string()).unwrap(), t);
            }
            if let Ok(d) = Duration::parse(&s) {
                prop_assert_eq!(Duration::parse(&d.to_string()).unwrap(), d);
            }
        }
    }
}
