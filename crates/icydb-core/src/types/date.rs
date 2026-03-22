//! Module: types::date
//! Responsibility: module-local ownership and contracts for types::date.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    traits::{
        Atomic, FieldValue, FieldValueKind, NumCast, NumFromPrimitive, NumToPrimitive,
        SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use derive_more::{Add, AddAssign, FromStr, Sub, SubAssign};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display};
use time::{Date as TimeDate, Duration as TimeDuration, Month};

// Invariant:
// Date is internally stored as days since Unix epoch (`i32`).
// API/JSON serialization uses ISO-8601 text (`YYYY-MM-DD`).
// Ordering and arithmetic remain numeric and deterministic over day counts.

///
/// Date
///
/// Stored as days since Unix epoch.
/// API/JSON wire format is ISO-8601 text (`YYYY-MM-DD`).
///

#[derive(
    Add,
    AddAssign,
    CandidType,
    Clone,
    Copy,
    Default,
    Eq,
    FromStr,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Sub,
    SubAssign,
)]
#[repr(transparent)]
pub struct Date(i32);

impl Date {
    pub const EPOCH: Self = Self(0);
    pub const MIN: Self = Self(i32::MIN);
    pub const MAX: Self = Self(i32::MAX);

    const fn epoch_date() -> TimeDate {
        // Safe: constant valid date
        match TimeDate::from_calendar_date(1970, Month::January, 1) {
            Ok(d) => d,
            Err(_) => unreachable!(),
        }
    }

    /// Build a date from calendar parts with sanitizing behavior.
    ///
    /// Month and day are clamped into a valid calendar date when possible.
    /// Impossible year-month combinations fall back to [`Date::EPOCH`].
    #[must_use]
    pub fn new(y: i32, m: u8, d: u8) -> Self {
        let m = m.clamp(1, 12);

        let Ok(month) = Month::try_from(m) else {
            return Self::EPOCH;
        };

        let last_valid_day = (28..=31)
            .rev()
            .find(|&day| TimeDate::from_calendar_date(y, month, day).is_ok());

        let Some(last_valid_day) = last_valid_day else {
            return Self::EPOCH;
        };

        let d = d.clamp(1, last_valid_day);

        match TimeDate::from_calendar_date(y, month, d) {
            Ok(date) => Self::from_time_date(date),
            Err(_) => Self::EPOCH,
        }
    }

    /// Build a date from calendar parts using strict validation.
    ///
    /// Returns `None` when any component is out of range.
    #[must_use]
    pub fn new_checked(y: i32, m: u8, d: u8) -> Option<Self> {
        let month = Month::try_from(m).ok()?;
        let date = TimeDate::from_calendar_date(y, month, d).ok()?;
        Some(Self::from_time_date(date))
    }

    /// Construct directly from internal day-count representation.
    #[must_use]
    pub const fn from_days_since_epoch(days: i32) -> Self {
        Self(days)
    }

    /// Return the internal day-count representation.
    #[must_use]
    pub const fn as_days_since_epoch(self) -> i32 {
        self.0
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
        let year = parse_fixed_ascii_i32(&bytes[0..4])?;
        let month = parse_fixed_ascii_u8(&bytes[5..7])?;
        let day = parse_fixed_ascii_u8(&bytes[8..10])?;

        Self::new_checked(year, month, day)
    }

    /// Parse supported text date inputs.
    ///
    /// This currently mirrors [`Date::parse`] and intentionally keeps
    /// accepted formats strict and unambiguous.
    #[must_use]
    pub fn parse_flexible(s: &str) -> Option<Self> {
        Self::parse(s)
    }

    // `time::Date` arithmetic returns `i64` day deltas; this type is fixed to `i32`.
    #[expect(clippy::cast_possible_truncation)]
    fn from_time_date(date: TimeDate) -> Self {
        let epoch = Self::epoch_date();
        let days = (date - epoch).whole_days();
        Self(days as i32)
    }

    // Rebuild calendar components from internal epoch-day storage for display/helpers.
    fn to_time_date(self) -> TimeDate {
        let epoch = Self::epoch_date();
        let delta = TimeDuration::days(self.0.into());
        epoch.checked_add(delta).unwrap_or({
            if self.0 >= 0 {
                TimeDate::MAX
            } else {
                TimeDate::MIN
            }
        })
    }
}

// Parse one fixed-width ASCII digit slice into an `i32`.
fn parse_fixed_ascii_i32(bytes: &[u8]) -> Option<i32> {
    let mut value = 0_i32;
    for &byte in bytes {
        let digit = byte.checked_sub(b'0')?;
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(i32::from_u8(digit)?)?;
    }

    Some(value)
}

// Parse one fixed-width ASCII digit slice into a `u8`.
fn parse_fixed_ascii_u8(bytes: &[u8]) -> Option<u8> {
    let mut value = 0_u8;
    for &byte in bytes {
        let digit = byte.checked_sub(b'0')?;
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(digit)?;
    }

    Some(value)
}

impl Atomic for Date {}

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

impl FieldValue for Date {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Date(*self)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Date(v) => Some(*v),
            _ => None,
        }
    }
}

impl NumCast for Date {
    fn from<T: NumToPrimitive>(n: T) -> Option<Self> {
        n.to_i32().map(Self)
    }
}

impl NumFromPrimitive for Date {
    #[expect(clippy::cast_possible_truncation)]
    fn from_i64(n: i64) -> Option<Self> {
        Some(Self(n as i32))
    }

    #[expect(clippy::cast_possible_truncation)]
    fn from_u64(n: u64) -> Option<Self> {
        if i32::try_from(n).is_ok() {
            Some(Self(n as i32))
        } else {
            None
        }
    }
}

impl NumToPrimitive for Date {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }

    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }
}

impl SanitizeAuto for Date {}

impl SanitizeCustom for Date {}

impl Serialize for Date {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Date {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).ok_or_else(|| serde::de::Error::custom(format!("invalid date: {s}")))
    }
}

impl ValidateAuto for Date {}

impl ValidateCustom for Date {}

impl Visitable for Date {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    // Internal semantic/storage representation behavior.

    #[test]
    fn from_ymd_and_to_naive_date_round_trip() {
        let date = Date::new(2024, 10, 19);
        assert_eq!(date.year(), 2024);
        assert_eq!(date.month(), 10);
        assert_eq!(date.day(), 19);
    }

    #[test]
    fn new_sanitizes_out_of_range_month_and_day() {
        let sanitized = Date::new(2025, 13, 99);
        let strict = Date::new_checked(2025, 12, 31).expect("strict date should build");
        assert_eq!(sanitized, strict);
    }

    #[test]
    fn invalid_date_parse_returns_none() {
        assert!(Date::parse("2025-13-40").is_none());
        assert!(Date::new_checked(2025, 2, 30).is_none());
    }

    #[test]
    fn new_out_of_range_year_defaults_to_epoch() {
        let date = Date::new(i32::MAX, 1, 1);
        assert_eq!(date, Date::EPOCH);
    }

    #[test]
    fn overflow_protection_in_from_u64() {
        // i32::MAX + 1 should safely fail
        let too_large = (i32::MAX as u64) + 1;
        assert!(Date::from_u64(too_large).is_none());
    }

    #[test]
    fn ordering_and_equality_follow_internal_day_count() {
        let d1 = Date::new_checked(2020, 1, 1).unwrap();
        let d2 = Date::new_checked(2021, 1, 1).unwrap();

        assert!(d1 < d2);
        assert!(d1.as_days_since_epoch() < d2.as_days_since_epoch());
        assert_eq!(d1, d1);
    }

    #[test]
    fn internal_day_count_helpers_round_trip() {
        let days = -365;
        let date = Date::from_days_since_epoch(days);
        assert_eq!(date.as_days_since_epoch(), days);
        assert_eq!(date.as_days_since_epoch(), days);
    }

    #[test]
    fn display_formats_as_iso_date() {
        let date = Date::new_checked(2025, 10, 19).unwrap();
        assert_eq!(format!("{date}"), "2025-10-19");
    }

    #[test]
    fn parse_flexible_stays_iso_strict() {
        assert_eq!(
            Date::parse_flexible("2025-10-19"),
            Date::new_checked(2025, 10, 19)
        );
        assert!(Date::parse_flexible("10/19/2025").is_none());
        assert!(Date::parse_flexible("2025-10-19T00:00:00Z").is_none());
    }

    #[test]
    fn parse_supports_pre_epoch_and_leap_year_cases() {
        assert_eq!(
            Date::parse("1900-01-01"),
            Date::new_checked(1900, 1, 1),
            "expected non-leap-century date to parse",
        );
        assert_eq!(
            Date::parse("1969-12-31"),
            Date::new_checked(1969, 12, 31),
            "expected pre-epoch date to parse",
        );
        assert_eq!(
            Date::parse("2000-02-29"),
            Date::new_checked(2000, 2, 29),
            "expected leap-day date to parse",
        );
    }

    #[test]
    fn parse_rejects_invalid_non_leap_day() {
        assert!(Date::parse("1900-02-29").is_none());
    }

    #[test]
    fn extreme_internal_day_values_format_without_panicking() {
        let min_rendered = Date::MIN.to_string();
        let max_rendered = Date::MAX.to_string();

        assert!(!min_rendered.is_empty());
        assert!(!max_rendered.is_empty());
    }

    // API boundary serialization behavior.

    #[test]
    fn test_json_serializes_as_iso_string() {
        let date = Date::new_checked(2025, 10, 19).unwrap();
        let json = serde_json::to_string(&date).unwrap();
        assert_eq!(json, "\"2025-10-19\"");
    }

    #[test]
    fn test_json_deserializes_from_iso_string() {
        let date: Date = serde_json::from_str("\"2025-10-19\"").unwrap();
        assert_eq!(date, Date::new_checked(2025, 10, 19).unwrap());
    }

    #[test]
    fn test_json_rejects_invalid_iso_string() {
        let err = serde_json::from_str::<Date>("\"2025-13-40\"").unwrap_err();
        assert!(err.to_string().contains("invalid date"));

        let err = serde_json::from_str::<Date>("1710013530000").unwrap_err();
        assert!(err.to_string().contains("string"));
    }

    #[test]
    fn test_pre_epoch_date_roundtrip() {
        let date = Date::new_checked(1960, 1, 1).unwrap();
        let json = serde_json::to_string(&date).unwrap();
        let parsed: Date = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, date);
    }

    #[test]
    fn test_serde_boundary_uses_iso_string_not_raw_day_count() {
        let value = Date::new_checked(2025, 10, 19).expect("date should build");

        let bytes = serde_cbor::to_vec(&value).expect("date serialization should succeed");
        let wire: serde_cbor::Value =
            serde_cbor::from_slice(&bytes).expect("date cbor decode should succeed");
        assert_eq!(
            wire,
            serde_cbor::Value::Text("2025-10-19".to_string()),
            "date wire shape must remain ISO text at API boundaries",
        );

        let decoded: Date = serde_cbor::from_slice(&bytes).expect("date decode should succeed");
        assert_eq!(decoded, value);
    }
}
