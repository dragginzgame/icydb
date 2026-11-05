//! Canonical day-precision date atom.
//!
//! This module owns the strict calendar/date representation and its public
//! wire conversion. Database ordering and storage policy remain runtime-owned.

use crate::{Decimal, NumericValue, TypeParseError};

use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::{self, Debug, Display};

// Invariant:
// Date is internally represented as days since Unix epoch (`i32`) and is
// bounded to the proleptic Gregorian calendar range 0000-01-01..=9999-12-31.
// Human-readable Serde uses ISO-8601 text; binary Serde uses epoch days.
// Ordering and arithmetic remain numeric and deterministic over day counts.

//
// Date
//
// Represented as days since Unix epoch.
// Wire uses int32 epoch days; JSON uses ISO-8601 text (`YYYY-MM-DD`).
//

#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Date(i32);

impl Date {
    /// The Unix epoch date, 1970-01-01.
    pub const EPOCH: Self = Self(0);
    /// The earliest supported calendar date, 0000-01-01.
    pub const MIN: Self = Self(-719_528);
    /// The latest supported calendar date, 9999-12-31.
    pub const MAX: Self = Self(2_932_896);

    // March-based Gregorian cycles put the leap day at the end of a year.
    // The cycle contains 400 * 365 + 97 days; 1970-01-01 is day 719468
    // after 0000-03-01 in this calendar.
    const DAYS_PER_CYCLE: i32 = 146_097;
    const MARCH_EPOCH_OFFSET: i32 = 719_468;

    /// Build a date from exact calendar parts.
    ///
    /// Returns `None` when any component is out of range.
    #[must_use]
    pub fn try_new(y: i32, m: u8, d: u8) -> Option<Self> {
        if !(0..=9_999).contains(&y) {
            return None;
        }
        let month_days = match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) => 29,
            2 => 28,
            _ => return None,
        };
        if d == 0 || d > month_days {
            return None;
        }

        let year = y - i32::from(m <= 2);
        let cycle = year.div_euclid(400);
        let cycle_year = year.rem_euclid(400);
        let march_month = i32::from(m) + if m > 2 { -3 } else { 9 };
        let cycle_day = 365 * cycle_year + cycle_year / 4 - cycle_year / 100
            + (153 * march_month + 2) / 5
            + i32::from(d)
            - 1;
        Self::try_from_days_since_epoch(
            cycle * Self::DAYS_PER_CYCLE + cycle_day - Self::MARCH_EPOCH_OFFSET,
        )
    }

    /// Construct from the bounded internal day-count representation.
    #[must_use]
    pub const fn try_from_days_since_epoch(days: i32) -> Option<Self> {
        if days < Self::MIN.0 || days > Self::MAX.0 {
            None
        } else {
            Some(Self(days))
        }
    }

    /// Return the internal day-count representation.
    #[must_use]
    pub const fn as_days_since_epoch(self) -> i32 {
        self.0
    }

    /// Fallible conversion from `i64` day-count representation.
    #[must_use]
    pub fn try_from_i64(days: i64) -> Option<Self> {
        i32::try_from(days)
            .ok()
            .and_then(Self::try_from_days_since_epoch)
    }

    /// Fallible conversion from `u64` day-count representation.
    #[must_use]
    pub fn try_from_u64(days: u64) -> Option<Self> {
        i32::try_from(days)
            .ok()
            .and_then(Self::try_from_days_since_epoch)
    }

    /// Add a signed number of days without leaving the supported calendar.
    #[must_use]
    pub fn checked_add_days(self, days: i64) -> Option<Self> {
        i64::from(self.0)
            .checked_add(days)
            .and_then(Self::try_from_i64)
    }

    /// Subtract a signed number of days without leaving the supported calendar.
    #[must_use]
    pub fn checked_sub_days(self, days: i64) -> Option<Self> {
        i64::from(self.0)
            .checked_sub(days)
            .and_then(Self::try_from_i64)
    }

    /// Return the signed number of days from `earlier` to this date.
    #[must_use]
    pub fn days_since(self, earlier: Self) -> i64 {
        i64::from(self.0) - i64::from(earlier.0)
    }

    /// Returns the year component (e.g. 2025).
    #[must_use]
    pub fn year(self) -> i32 {
        self.calendar_parts().0
    }

    /// Returns the month component (1-12).
    #[must_use]
    pub fn month(self) -> u8 {
        self.calendar_parts().1
    }

    /// Returns the day-of-month component (1-31).
    #[must_use]
    pub fn day(self) -> u8 {
        self.calendar_parts().2
    }

    /// Parse a strict ISO `YYYY-MM-DD` string into a `Date`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let bytes = s.as_bytes();
        if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
            return None;
        }

        // Decode one strict fixed-width payload before calendar admission.
        let year = parse_ascii_i32(&bytes[0..4])?;
        let month = parse_ascii_u8(&bytes[5..7])?;
        let day = parse_ascii_u8(&bytes[8..10])?;

        Self::try_new(year, month, day)
    }

    // Invert the March-based cycle used by try_new. Accepted epoch days keep
    // all arithmetic in i32 and yield months 1..=12 and days 1..=31.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "bounded Gregorian conversion yields positive u8 month/day components"
    )]
    fn calendar_parts(self) -> (i32, u8, u8) {
        let march_day = self.0 + Self::MARCH_EPOCH_OFFSET;
        let cycle = march_day.div_euclid(Self::DAYS_PER_CYCLE);
        let cycle_day = march_day.rem_euclid(Self::DAYS_PER_CYCLE);
        let cycle_year =
            (cycle_day - cycle_day / 1_460 + cycle_day / 36_524 - cycle_day / 146_096) / 365;
        let year = cycle * 400 + cycle_year;
        let march_ordinal = cycle_day - (365 * cycle_year + cycle_year / 4 - cycle_year / 100);
        let march_month = (5 * march_ordinal + 2) / 153;
        let day = march_ordinal - (153 * march_month + 2) / 5 + 1;
        let month = march_month + if march_month < 10 { 3 } else { -9 };

        (year + i32::from(month <= 2), month as u8, day as u8)
    }
}

impl Debug for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Date({self})")
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (year, month, day) = self.calendar_parts();
        write!(f, "{year:04}-{month:02}-{day:02}")
    }
}

impl NumericValue for Date {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_i64(i64::from(self.0))
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_i32().and_then(Self::try_from_days_since_epoch)
    }
}

impl<'de> Deserialize<'de> for Date {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            return Self::parse(&String::deserialize(deserializer)?)
                .ok_or_else(|| serde::de::Error::custom(TypeParseError::InvalidDate));
        }
        // Integer ingress enforces both the wire width and calendar domain.
        Self::try_from_days_since_epoch(i32::deserialize(deserializer)?)
            .ok_or_else(|| serde::de::Error::custom(TypeParseError::InvalidDate))
    }
}

impl Serialize for Date {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            serializer.serialize_i32(self.0)
        }
    }
}

fn parse_ascii_i32(bytes: &[u8]) -> Option<i32> {
    bytes.iter().try_fold(0_i32, |value, byte| {
        byte.checked_sub(b'0')
            .filter(|digit| *digit <= 9)
            .and_then(|digit| value.checked_mul(10)?.checked_add(i32::from(digit)))
    })
}

fn parse_ascii_u8(bytes: &[u8]) -> Option<u8> {
    bytes.iter().try_fold(0_u8, |value, byte| {
        byte.checked_sub(b'0')
            .filter(|digit| *digit <= 9)
            .and_then(|digit| value.checked_mul(10)?.checked_add(digit))
    })
}

//
// TESTS
//

#[cfg(test)]
mod tests;
