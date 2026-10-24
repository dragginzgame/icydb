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
mod tests {
    use super::*;

    #[test]
    fn every_supported_calendar_day_matches_the_reference_calendar() {
        use time::{Date as ReferenceDate, Month};

        let epoch = ReferenceDate::from_calendar_date(1970, Month::January, 1).unwrap();
        for days in Date::MIN.as_days_since_epoch()..=Date::MAX.as_days_since_epoch() {
            let reference = ReferenceDate::from_julian_day(epoch.to_julian_day() + days).unwrap();
            let (year, month, day) = reference.to_calendar_date();
            let month = u8::from(month);
            let date = Date::try_from_days_since_epoch(days).unwrap();
            assert_eq!((date.year(), date.month(), date.day()), (year, month, day));
            assert_eq!(Date::try_new(year, month, day), Some(date));
        }
    }

    #[test]
    fn calendar_admission_matches_reference_at_month_and_leap_boundaries() {
        use time::{Date as ReferenceDate, Month};

        let epoch = ReferenceDate::from_calendar_date(1970, Month::January, 1).unwrap();
        for year in [
            0, 1, 4, 100, 400, 1582, 1900, 1969, 1970, 2000, 2024, 2100, 2400, 9999,
        ] {
            for month in 0..=13 {
                for day in 0..=32 {
                    let expected = Month::try_from(month)
                        .ok()
                        .and_then(|month| ReferenceDate::from_calendar_date(year, month, day).ok())
                        .map(|date| (date - epoch).whole_days());
                    assert_eq!(
                        Date::try_new(year, month, day)
                            .map(|date| i64::from(date.as_days_since_epoch())),
                        expected,
                        "{year:04}-{month:02}-{day:02}"
                    );
                }
            }
        }
    }

    #[test]
    fn serde_calendar_boundaries_and_batches_use_compact_days() {
        let today = Date::try_new(2026, 9, 12).unwrap();
        for (date, width) in [(Date::EPOCH, 1), (today, 3), (Date::MIN, 5), (Date::MAX, 5)] {
            let mut encoded = Vec::new();
            serde_cbor::to_writer(&mut encoded, &date).unwrap();
            assert_eq!(encoded.len(), width);
            let wire: serde_cbor::Value = serde_cbor::from_slice(encoded.as_slice()).unwrap();
            assert_eq!(
                wire,
                serde_cbor::Value::Integer(date.as_days_since_epoch().into())
            );
            assert_eq!(
                serde_cbor::from_slice::<Date>(encoded.as_slice()).unwrap(),
                date
            );

            let json = serde_json::to_string(&date).unwrap();
            assert_eq!(json, format!("\"{date}\""));
            assert_eq!(serde_json::from_str::<Date>(&json).unwrap(), date);

            let wire = crate::wire::encode_one(date).unwrap();
            assert_eq!(
                wire,
                crate::wire::encode_one(date.as_days_since_epoch()).unwrap()
            );
            assert_eq!(crate::wire::decode_one::<Date>(&wire).unwrap(), date);

            let batch = vec![date; 1_000];
            encoded.clear();
            serde_cbor::to_writer(&mut encoded, &batch).unwrap();
            assert_eq!(encoded.len(), 3 + 1_000 * width);
            assert_eq!(
                serde_cbor::from_slice::<Vec<Date>>(encoded.as_slice()).unwrap(),
                batch
            );
        }
    }

    #[test]
    fn binary_serde_rejects_days_outside_calendar_domain() {
        for day in [
            i64::MIN,
            i64::from(Date::MIN.as_days_since_epoch()) - 1,
            i64::from(Date::MAX.as_days_since_epoch()) + 1,
            i64::MAX,
        ] {
            let mut encoded = Vec::new();
            serde_cbor::to_writer(&mut encoded, &day).unwrap();
            assert!(serde_cbor::from_slice::<Date>(encoded.as_slice()).is_err());
        }
    }

    // Internal semantic/storage representation behavior.

    #[test]
    fn from_ymd_and_to_naive_date_round_trip() {
        let date = Date::try_new(2024, 10, 19).expect("valid calendar date should construct");
        assert_eq!(date.year(), 2024);
        assert_eq!(date.month(), 10);
        assert_eq!(date.day(), 19);
    }

    #[test]
    fn try_new_rejects_out_of_range_month_and_day() {
        assert!(Date::try_new(2025, 13, 99).is_none());
    }

    #[test]
    fn invalid_date_parse_returns_none() {
        assert!(Date::parse("2025-13-40").is_none());
        assert!(Date::try_new(2025, 2, 30).is_none());
    }

    #[test]
    fn try_new_rejects_out_of_range_year() {
        assert!(Date::try_new(-1, 1, 1).is_none());
        assert!(Date::try_new(10_000, 1, 1).is_none());
        assert!(Date::try_new(i32::MAX, 1, 1).is_none());
    }

    #[test]
    fn overflow_protection_in_try_from_u64() {
        // i32::MAX + 1 should safely fail
        let too_large = (i32::MAX as u64) + 1;
        assert!(Date::try_from_u64(too_large).is_none());
    }

    #[test]
    fn ordering_and_equality_follow_internal_day_count() {
        let d1 = Date::try_new(2020, 1, 1).unwrap();
        let d2 = Date::try_new(2021, 1, 1).unwrap();

        assert!(d1 < d2);
        assert!(d1.as_days_since_epoch() < d2.as_days_since_epoch());
        assert_eq!(d1, d1);
    }

    #[test]
    fn internal_day_count_helpers_round_trip() {
        let days = -365;
        let date = Date::try_from_days_since_epoch(days).expect("bounded day should construct");
        assert_eq!(date.as_days_since_epoch(), days);
    }

    #[test]
    fn raw_day_construction_rejects_values_outside_calendar_bounds() {
        assert_eq!(
            Date::try_from_days_since_epoch(Date::MIN.as_days_since_epoch()),
            Some(Date::MIN),
        );
        assert_eq!(
            Date::try_from_days_since_epoch(Date::MAX.as_days_since_epoch()),
            Some(Date::MAX),
        );
        assert!(Date::try_from_days_since_epoch(Date::MIN.as_days_since_epoch() - 1).is_none(),);
        assert!(Date::try_from_days_since_epoch(Date::MAX.as_days_since_epoch() + 1).is_none(),);
    }

    #[test]
    fn checked_day_arithmetic_obeys_calendar_bounds() {
        let leap_day = Date::try_new(2024, 2, 29).expect("leap day should construct");
        let march_first = Date::try_new(2024, 3, 1).expect("next day should construct");

        assert_eq!(leap_day.checked_add_days(1), Some(march_first));
        assert_eq!(march_first.checked_sub_days(1), Some(leap_day));
        assert_eq!(march_first.days_since(leap_day), 1);
        assert!(Date::MAX.checked_add_days(1).is_none());
        assert!(Date::MIN.checked_sub_days(1).is_none());
        assert!(Date::EPOCH.checked_add_days(i64::MAX).is_none());
        assert!(Date::EPOCH.checked_sub_days(i64::MIN).is_none());
    }

    #[test]
    fn display_formats_as_iso_date() {
        let date = Date::try_new(2025, 10, 19).unwrap();
        assert_eq!(format!("{date}"), "2025-10-19");
    }

    #[test]
    fn parse_stays_iso_strict() {
        assert_eq!(Date::parse("2025-10-19"), Date::try_new(2025, 10, 19));
        assert!(Date::parse("10/19/2025").is_none());
        assert!(Date::parse("2025-10-19T00:00:00Z").is_none());
    }

    #[test]
    fn parse_supports_pre_epoch_and_leap_year_cases() {
        assert_eq!(
            Date::parse("1900-01-01"),
            Date::try_new(1900, 1, 1),
            "expected non-leap-century date to parse",
        );
        assert_eq!(
            Date::parse("1969-12-31"),
            Date::try_new(1969, 12, 31),
            "expected pre-epoch date to parse",
        );
        assert_eq!(
            Date::parse("2000-02-29"),
            Date::try_new(2000, 2, 29),
            "expected leap-day date to parse",
        );
    }

    #[test]
    fn parse_rejects_invalid_non_leap_day() {
        assert!(Date::parse("1900-02-29").is_none());
    }

    #[test]
    fn calendar_boundaries_format_and_parse_exactly() {
        assert_eq!(Date::MIN.to_string(), "0000-01-01");
        assert_eq!(Date::MAX.to_string(), "9999-12-31");
        assert_eq!(Date::parse(Date::MIN.to_string().as_str()), Some(Date::MIN));
        assert_eq!(Date::parse(Date::MAX.to_string().as_str()), Some(Date::MAX));
    }

    #[test]
    fn wire_decode_rejects_out_of_range_epoch_days() {
        let min = crate::wire::encode_one(Date::MIN.as_days_since_epoch())
            .expect("minimum day should encode");
        let max = crate::wire::encode_one(Date::MAX.as_days_since_epoch())
            .expect("maximum day should encode");
        let below = crate::wire::encode_one(Date::MIN.as_days_since_epoch() - 1)
            .expect("out-of-range day should encode as raw i32");
        let above = crate::wire::encode_one(Date::MAX.as_days_since_epoch() + 1)
            .expect("out-of-range day should encode as raw i32");

        assert_eq!(
            crate::wire::decode_one::<Date>(&min).expect("minimum day should decode"),
            Date::MIN,
        );
        assert_eq!(
            crate::wire::decode_one::<Date>(&max).expect("maximum day should decode"),
            Date::MAX,
        );
        assert!(crate::wire::decode_one::<Date>(&below).is_err());
        assert!(crate::wire::decode_one::<Date>(&above).is_err());
    }
}
