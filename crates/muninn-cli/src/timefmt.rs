//! Timestamp formats of input files (`--time-format`).

use clap::ValueEnum;
use muninn_core::Timestamp;
use muninn_core::api::TimeInput;

/// How timestamps are written in an input file.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeFormat {
    /// SPEC §1 text form: RFC 3339 with `Z` or a numeric offset, 0–6 fraction digits.
    #[default]
    Rfc3339,
    /// Integer microseconds since the Unix epoch.
    UnixUs,
    /// Milliseconds since the epoch (up to 3 fraction digits).
    UnixMs,
    /// Seconds since the epoch (up to 6 fraction digits).
    UnixS,
}

impl TimeFormat {
    /// Converts one field to a [`TimeInput`] (validated later, in SPEC §4.1 order).
    pub fn input(self, s: &str) -> TimeInput {
        match self {
            TimeFormat::Rfc3339 => TimeInput::Text(s.to_owned()),
            _ => match self.micros(s) {
                Some(us) => TimeInput::Micros(us),
                None => TimeInput::Invalid(format!("{s:?} is not a {} timestamp", self.name())),
            },
        }
    }

    /// Parses one field to a timestamp, with a message on failure.
    pub fn parse(self, s: &str) -> Result<Timestamp, String> {
        self.input(s).resolve().map_err(|e| e.message().to_owned())
    }

    fn name(self) -> &'static str {
        match self {
            TimeFormat::Rfc3339 => "rfc3339",
            TimeFormat::UnixUs => "unix-us",
            TimeFormat::UnixMs => "unix-ms",
            TimeFormat::UnixS => "unix-s",
        }
    }

    /// `[-]digits[.digits]` scaled to microseconds, exactly (no rounding).
    fn micros(self, s: &str) -> Option<i64> {
        let frac_digits = match self {
            TimeFormat::Rfc3339 => return None,
            TimeFormat::UnixUs => 0,
            TimeFormat::UnixMs => 3,
            TimeFormat::UnixS => 6,
        };
        let (neg, rest) = match s.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, s),
        };
        let (int, frac) = match rest.split_once('.') {
            Some((i, f)) => (i, Some(f)),
            None => (rest, None),
        };
        let digits = |t: &str| !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit());
        if !digits(int) {
            return None;
        }
        let mut v: i128 = int.parse().ok()?;
        v = v.checked_mul(10i128.pow(frac_digits))?;
        if let Some(f) = frac {
            if !digits(f) || f.len() > frac_digits as usize {
                return None;
            }
            let scaled: i128 = f.parse::<i128>().ok()? * 10i128.pow(frac_digits - f.len() as u32);
            v += scaled;
        }
        let v = if neg { -v } else { v };
        i64::try_from(v).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_forms() {
        assert_eq!(TimeFormat::UnixS.micros("1.5"), Some(1_500_000));
        assert_eq!(TimeFormat::UnixMs.micros("-2.001"), Some(-2001));
        assert_eq!(TimeFormat::UnixUs.micros("17"), Some(17));
        assert_eq!(TimeFormat::UnixUs.micros("1.0"), None);
        assert_eq!(TimeFormat::UnixMs.micros("1.0001"), None);
        assert_eq!(TimeFormat::UnixS.micros("x"), None);
        assert_eq!(TimeFormat::UnixS.micros(""), None);
        assert_eq!(TimeFormat::UnixS.micros("99999999999999999999"), None);
    }
}
