//! Canonical day-precision date atom.
//!
//! This module owns the strict calendar/date representation and its public
//! wire conversion. Database ordering and storage policy remain runtime-owned.

use crate::{Decimal, NumericValue, TypeParseError};
use candid::CandidType;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::{self, Debug, Display};
use time::{Date as TimeDate, Duration as TimeDuration, Month};

// Invariant:
// Date is internally represented as days since Unix epoch (`i32`) and is
// bounded to the proleptic Gregorian calendar range 0000-01-01..=9999-12-31.
// API/JSON deserialization accepts ISO-8601 text (`YYYY-MM-DD`).
// Ordering and arithmetic remain numeric and deterministic over day counts.

//
// Date
//
// Represented as days since Unix epoch.
// API/JSON decode expects ISO-8601 text (`YYYY-MM-DD`).
//

#[derive(CandidType, Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Date(i32);

impl Date {
    /// The Unix epoch date, 1970-01-01.
    pub const EPOCH: Self = Self(0);
    /// The earliest supported calendar date, 0000-01-01.
    pub const MIN: Self = Self(-719_528);
    /// The latest supported calendar date, 9999-12-31.
    pub const MAX: Self = Self(2_932_896);

    const fn epoch_date() -> TimeDate {
        // Safe: constant valid date
        match TimeDate::from_calendar_date(1970, Month::January, 1) {
            Ok(d) => d,
            Err(_) => unreachable!(),
        }
    }

    /// Build a date from exact calendar parts.
    ///
    /// Returns `None` when any component is out of range.
    #[must_use]
    pub fn try_new(y: i32, m: u8, d: u8) -> Option<Self> {
        if !(0..=9_999).contains(&y) {
            return None;
        }
        let month = Month::try_from(m).ok()?;
        let date = TimeDate::from_calendar_date(y, month, d).ok()?;
        Self::from_time_date(date)
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
        self.to_time_date().year()
    }

    /// Returns the month component (1-12).
    #[must_use]
    pub fn month(self) -> u8 {
        self.to_time_date().month().into()
    }

    /// Returns the day-of-month component (1-31).
    #[must_use]
    pub fn day(self) -> u8 {
        self.to_time_date().day()
    }

    /// Parse a strict ISO `YYYY-MM-DD` string into a `Date`.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let bytes = s.as_bytes();
        if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
            return None;
        }

        // Phase 1: decode one strict fixed-width `YYYY-MM-DD` payload without
        // routing through the heavier `time` text parser.
        let year = parse_ascii_i32(&bytes[0..4])?;
        let month = parse_ascii_u8(&bytes[5..7])?;
        let day = parse_ascii_u8(&bytes[8..10])?;

        Self::try_new(year, month, day)
    }

    fn from_time_date(date: TimeDate) -> Option<Self> {
        let epoch = Self::epoch_date();
        let days = (date - epoch).whole_days();
        Self::try_from_i64(days)
    }

    // Safe public construction and persisted decoding enforce the bounded Date
    // invariant before this private calendar conversion is reachable.
    fn to_time_date(self) -> TimeDate {
        let epoch = Self::epoch_date();
        let delta = TimeDuration::days(self.0.into());
        match epoch.checked_add(delta) {
            Some(date) => date,
            None => unreachable!("bounded Date invariant must produce a supported calendar date"),
        }
    }
}

impl Debug for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Date({self})")
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.to_time_date();
        let month: u8 = d.month().into();
        write!(f, "{:04}-{:02}-{:02}", d.year(), month, d.day())
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
        struct DateVisitor;

        impl serde::de::Visitor<'_> for DateVisitor {
            type Value = Date;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("ISO date text or canonical epoch-day integer")
            }

            fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Date::try_from_days_since_epoch(value)
                    .ok_or_else(|| E::custom(TypeParseError::InvalidDate))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Date::try_from_i64(value).ok_or_else(|| E::custom(TypeParseError::InvalidDate))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Date::try_from_u64(value).ok_or_else(|| E::custom(TypeParseError::InvalidDate))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Date::parse(value).ok_or_else(|| E::custom(TypeParseError::InvalidDate))
            }
        }

        deserializer.deserialize_any(DateVisitor)
    }
}

impl Serialize for Date {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
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
    fn candid_decode_rejects_out_of_range_epoch_days() {
        let min =
            candid::encode_one(Date::MIN.as_days_since_epoch()).expect("minimum day should encode");
        let max =
            candid::encode_one(Date::MAX.as_days_since_epoch()).expect("maximum day should encode");
        let below = candid::encode_one(Date::MIN.as_days_since_epoch() - 1)
            .expect("out-of-range day should encode as raw i32");
        let above = candid::encode_one(Date::MAX.as_days_since_epoch() + 1)
            .expect("out-of-range day should encode as raw i32");

        assert_eq!(
            candid::decode_one::<Date>(&min).expect("minimum day should decode"),
            Date::MIN,
        );
        assert_eq!(
            candid::decode_one::<Date>(&max).expect("maximum day should decode"),
            Date::MAX,
        );
        assert!(candid::decode_one::<Date>(&below).is_err());
        assert!(candid::decode_one::<Date>(&above).is_err());
    }
}
