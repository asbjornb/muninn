//! Partitions: UTC day / month / year of `value_time` (SPEC §2.1 `partition`,
//! FORMAT.md §2).

use muninn_core::time::{MICROS_PER_DAY, civil_from_days, days_from_civil};
use muninn_core::{Partitioning, TimeRange, Timestamp};

use crate::{Error, Result};

/// One partition of a table: all versions whose `value_time` falls in one UTC
/// day, month or year.
///
/// `index` is the partition's ordinal: days since 1970-01-01 (day), `year * 12 + month -
/// 1` (month), or the year (year). Ordinals increase with time, so partitions sort by
/// `index`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Partition {
    /// Granularity.
    pub granularity: Partitioning,
    /// Ordinal (see the type docs).
    pub index: i64,
}

impl Partition {
    /// The partition containing `t`.
    pub fn of(granularity: Partitioning, t: Timestamp) -> Partition {
        let c = t.to_civil();
        let index = match granularity {
            Partitioning::Day => t.as_micros().div_euclid(MICROS_PER_DAY),
            Partitioning::Month => c.year * 12 + i64::from(c.month) - 1,
            Partitioning::Year => c.year,
        };
        Partition { granularity, index }
    }

    /// The first instant of the partition, as µs since the epoch (may lie before
    /// [`Timestamp::MIN`] only for invalid partitions).
    fn start_micros(&self) -> i128 {
        let days = match self.granularity {
            Partitioning::Day => self.index,
            Partitioning::Month => days_from_civil(
                self.index.div_euclid(12),
                self.index.rem_euclid(12) as u32 + 1,
                1,
            ),
            Partitioning::Year => days_from_civil(self.index, 1, 1),
        };
        i128::from(days) * i128::from(MICROS_PER_DAY)
    }

    /// The next partition.
    pub fn next(&self) -> Partition {
        Partition {
            granularity: self.granularity,
            index: self.index + 1,
        }
    }

    /// Does any valid timestamp fall in this partition?
    pub fn is_valid(&self) -> bool {
        let (lo, hi) = (self.start_micros(), self.next().start_micros());
        lo <= i128::from(Timestamp::MAX.as_micros()) && hi > i128::from(Timestamp::MIN.as_micros())
    }

    /// The partition's value range `[start, end)`, clamped to valid timestamps (`to = None`
    /// for the last partition of year 9999).
    pub fn range(&self) -> TimeRange {
        let clamp = |x: i128| {
            let x = x.clamp(
                i128::from(Timestamp::MIN.as_micros()),
                i128::from(Timestamp::MAX.as_micros()),
            );
            Timestamp::from_micros(x as i64)
        };
        let hi = self.next().start_micros();
        TimeRange::new(
            clamp(self.start_micros()),
            if hi > i128::from(Timestamp::MAX.as_micros()) {
                None
            } else {
                clamp(hi)
            },
        )
    }

    /// The path label: `YYYY-MM-DD`, `YYYY-MM` or `YYYY`.
    pub fn label(&self) -> String {
        match self.granularity {
            Partitioning::Day => {
                let (y, m, d) = civil_from_days(self.index);
                format!("{y:04}-{m:02}-{d:02}")
            }
            Partitioning::Month => format!(
                "{:04}-{:02}",
                self.index.div_euclid(12),
                self.index.rem_euclid(12) + 1
            ),
            Partitioning::Year => format!("{:04}", self.index),
        }
    }

    /// Parses a [`Partition::label`].
    pub fn parse_label(granularity: Partitioning, s: &str) -> Result<Partition> {
        let bad = || Error::corrupt(format!("bad {granularity} partition label {s:?}"));
        let num = |p: &str, len: usize| -> Result<i64> {
            if p.len() != len || !p.bytes().all(|b| b.is_ascii_digit()) {
                return Err(bad());
            }
            p.parse().map_err(|_| bad())
        };
        let parts: Vec<&str> = s.split('-').collect();
        let p = match (granularity, parts.as_slice()) {
            (Partitioning::Day, [y, m, d]) => {
                let (y, m, d) = (num(y, 4)?, num(m, 2)?, num(d, 2)?);
                let ts = Timestamp::from_civil(muninn_core::time::CivilDateTime {
                    year: y,
                    month: u32::try_from(m).map_err(|_| bad())?,
                    day: u32::try_from(d).map_err(|_| bad())?,
                    hour: 0,
                    minute: 0,
                    second: 0,
                    micros: 0,
                })
                .map_err(|_| bad())?;
                Partition::of(Partitioning::Day, ts)
            }
            (Partitioning::Month, [y, m]) => {
                let (y, m) = (num(y, 4)?, num(m, 2)?);
                if !(1..=12).contains(&m) {
                    return Err(bad());
                }
                Partition {
                    granularity,
                    index: y * 12 + m - 1,
                }
            }
            (Partitioning::Year, [y]) => Partition {
                granularity,
                index: num(y, 4)?,
            },
            _ => return Err(bad()),
        };
        if !p.is_valid() || p.label() != s {
            return Err(bad());
        }
        Ok(p)
    }

    /// The inclusive ordinal range of partitions that can hold value times in `range`
    /// (`None` if the range is empty).
    pub fn covering(granularity: Partitioning, range: TimeRange) -> Option<(i64, i64)> {
        let lo = range.from.unwrap_or(Timestamp::MIN);
        let hi = match range.to {
            None => Timestamp::MAX,
            Some(t) if t <= lo => return None,
            Some(t) => Timestamp::from_micros(t.as_micros() - 1)?,
        };
        Some((
            Partition::of(granularity, lo).index,
            Partition::of(granularity, hi).index,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn ts(s: &str) -> Timestamp {
        Timestamp::parse(s).unwrap()
    }

    #[test]
    fn labels_and_ranges() {
        let t = ts("2025-03-15T12:34:56Z");
        let d = Partition::of(Partitioning::Day, t);
        assert_eq!(d.label(), "2025-03-15");
        assert_eq!(
            d.range(),
            TimeRange::new(
                Some(ts("2025-03-15T00:00:00Z")),
                Some(ts("2025-03-16T00:00:00Z"))
            )
        );
        let m = Partition::of(Partitioning::Month, t);
        assert_eq!(m.label(), "2025-03");
        assert_eq!(m.range().to, Some(ts("2025-04-01T00:00:00Z")));
        let y = Partition::of(Partitioning::Year, t);
        assert_eq!(y.label(), "2025");
        let last = Partition::of(Partitioning::Year, Timestamp::MAX);
        assert_eq!(last.label(), "9999");
        assert_eq!(last.range().to, None);
        assert_eq!(
            Partition::of(Partitioning::Day, Timestamp::MIN).label(),
            "0001-01-01"
        );
        for p in [d, m, y, last] {
            assert_eq!(
                Partition::parse_label(p.granularity, &p.label()).unwrap(),
                p
            );
        }
        for bad in ["2025-02-30", "2025-1-01", "0000-01-01", "x", "2025-13"] {
            assert!(Partition::parse_label(Partitioning::Day, bad).is_err());
        }
        assert!(Partition::parse_label(Partitioning::Month, "2025-13").is_err());
        assert!(Partition::parse_label(Partitioning::Year, "0000").is_err());
        assert_eq!(
            Partition::covering(Partitioning::Day, TimeRange::new(Some(t), Some(t))),
            None
        );
    }

    proptest! {
        #[test]
        fn partition_contains_its_timestamps(
            t in Timestamp::MIN.as_micros()..=Timestamp::MAX.as_micros(), g in 0u8..3,
            u in Timestamp::MIN.as_micros()..=Timestamp::MAX.as_micros(),
        ) {
            let g = [Partitioning::Day, Partitioning::Month, Partitioning::Year][g as usize];
            let t = Timestamp::from_micros(t).unwrap();
            let u = Timestamp::from_micros(u).unwrap();
            let p = Partition::of(g, t);
            prop_assert!(p.range().contains(t));
            prop_assert_eq!(p.range().contains(u), Partition::of(g, u) == p);
            prop_assert_eq!(Partition::parse_label(g, &p.label()).unwrap(), p);
            let (lo, hi) = (t.min(u), t.max(u));
            let (a, b) = Partition::covering(g, TimeRange::new(Some(lo), Some(hi))).unwrap_or((1, 0));
            if lo < hi {
                prop_assert_eq!(a, Partition::of(g, lo).index);
                prop_assert!(b <= Partition::of(g, hi).index);
            }
        }
    }
}
