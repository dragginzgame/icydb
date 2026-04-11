//! Module: types::duration
//! Defines the millisecond-native duration type used alongside timestamps and
//! numeric value conversion.

use crate::{
    traits::{
        Atomic, FieldValue, FieldValueKind, NumericValue, Repr, SanitizeAuto, SanitizeCustom,
        ValidateAuto, ValidateCustom, Visitable,
    },
    types::Decimal,
    value::Value,
};
use candid::CandidType;
use derive_more::{Display, FromStr};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    fmt,
    ops::{Add, AddAssign, Sub, SubAssign},
};

// Invariant:
// Timestamp and Duration are both millisecond-native.
// All arithmetic is millisecond-consistent.
// Wire format remains transparent u64.

//
// Duration
//
// Stored as milliseconds.
// Wire format remains a bare `u64`.
//

#[derive(
    CandidType, Clone, Copy, Debug, Default, Display, Eq, FromStr, PartialEq, Hash, Ord, PartialOrd,
)]
#[repr(transparent)]
pub struct Duration(u64);

impl Duration {
    pub const ZERO: Self = Self(0);
    pub const MIN: Self = Self(u64::MIN);
    pub const MAX: Self = Self(u64::MAX);

    // ratio constants
    const MS_PER_SEC: u64 = 1_000;
    const SECS_PER_MIN: u64 = 60;
    const MINS_PER_HOUR: u64 = 60;
    const HOURS_PER_DAY: u64 = 24;
    const DAYS_PER_WEEK: u64 = 7;

    // ---- Constructors ----

    #[must_use]
    pub const fn from_millis(ms: u64) -> Self {
        Self(ms)
    }

    /// Fallible conversion from `i64` milliseconds that rejects negatives.
    #[must_use]
    pub const fn try_from_i64(millis: i64) -> Option<Self> {
        if millis < 0 {
            return None;
        }

        Some(Self(millis.cast_unsigned()))
    }

    /// Fallible conversion from `u64` milliseconds.
    #[must_use]
    pub const fn try_from_u64(millis: u64) -> Option<Self> {
        Some(Self(millis))
    }

    #[must_use]
    pub(crate) const fn from_micros_truncating(us: u64) -> Self {
        Self(us / Self::MS_PER_SEC)
    }

    #[must_use]
    pub(crate) const fn from_nanos_truncating(ns: u64) -> Self {
        Self(ns / 1_000_000)
    }

    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs.saturating_mul(Self::MS_PER_SEC))
    }

    #[must_use]
    pub const fn from_minutes(mins: u64) -> Self {
        Self(
            mins.saturating_mul(Self::SECS_PER_MIN)
                .saturating_mul(Self::MS_PER_SEC),
        )
    }

    #[must_use]
    pub const fn from_hours(hours: u64) -> Self {
        Self(
            hours
                .saturating_mul(Self::MINS_PER_HOUR)
                .saturating_mul(Self::SECS_PER_MIN)
                .saturating_mul(Self::MS_PER_SEC),
        )
    }

    #[must_use]
    pub const fn from_days(days: u64) -> Self {
        Self(
            days.saturating_mul(Self::HOURS_PER_DAY)
                .saturating_mul(Self::MINS_PER_HOUR)
                .saturating_mul(Self::SECS_PER_MIN)
                .saturating_mul(Self::MS_PER_SEC),
        )
    }

    #[must_use]
    pub const fn from_weeks(weeks: u64) -> Self {
        Self(
            weeks
                .saturating_mul(Self::DAYS_PER_WEEK)
                .saturating_mul(Self::HOURS_PER_DAY)
                .saturating_mul(Self::MINS_PER_HOUR)
                .saturating_mul(Self::SECS_PER_MIN)
                .saturating_mul(Self::MS_PER_SEC),
        )
    }

    // ---- Conversion back to larger units ----

    #[must_use]
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn as_secs(self) -> u64 {
        self.0 / Self::MS_PER_SEC
    }

    #[must_use]
    pub const fn as_minutes(self) -> u64 {
        self.0 / (Self::SECS_PER_MIN * Self::MS_PER_SEC)
    }

    #[must_use]
    pub const fn as_hours(self) -> u64 {
        self.0 / (Self::MINS_PER_HOUR * Self::SECS_PER_MIN * Self::MS_PER_SEC)
    }

    #[must_use]
    pub const fn as_days(self) -> u64 {
        self.0 / (Self::HOURS_PER_DAY * Self::MINS_PER_HOUR * Self::SECS_PER_MIN * Self::MS_PER_SEC)
    }

    #[must_use]
    pub const fn as_weeks(self) -> u64 {
        self.0
            / (Self::DAYS_PER_WEEK
                * Self::HOURS_PER_DAY
                * Self::MINS_PER_HOUR
                * Self::SECS_PER_MIN
                * Self::MS_PER_SEC)
    }

    /// Parse integer milliseconds or unit-suffixed strings (`ms`, `s`, `m`, `h`, `d`).
    pub fn parse_flexible(s: &str) -> Result<Self, String> {
        // Phase 1: split one strict ASCII duration into digit and suffix parts
        // without routing through the heavier generic integer parser.
        let (digits, unit) = split_duration_digits_and_unit(s)
            .ok_or_else(|| "invalid duration format".to_string())?;
        let value = parse_duration_ascii_u64(digits)
            .ok_or_else(|| "invalid duration format".to_string())?;

        // Phase 2: apply the accepted unit family with the existing saturating
        // constructors so overflow semantics stay unchanged.
        let duration = match unit {
            DurationUnit::Millis => Self::from_millis(value),
            DurationUnit::Seconds => Self::from_secs(value),
            DurationUnit::Minutes => Self::from_minutes(value),
            DurationUnit::Hours => Self::from_hours(value),
            DurationUnit::Days => Self::from_days(value),
        };

        Ok(duration)
    }
}

#[derive(Clone, Copy)]
enum DurationUnit {
    Millis,
    Seconds,
    Minutes,
    Hours,
    Days,
}

// Split one duration literal into its digit prefix and accepted unit suffix.
fn split_duration_digits_and_unit(s: &str) -> Option<(&str, DurationUnit)> {
    if let Some(value) = s.strip_suffix("ms") {
        return Some((value, DurationUnit::Millis));
    }
    if let Some(value) = s.strip_suffix('s') {
        return Some((value, DurationUnit::Seconds));
    }
    if let Some(value) = s.strip_suffix('m') {
        return Some((value, DurationUnit::Minutes));
    }
    if let Some(value) = s.strip_suffix('h') {
        return Some((value, DurationUnit::Hours));
    }
    if let Some(value) = s.strip_suffix('d') {
        return Some((value, DurationUnit::Days));
    }
    if !s.is_empty() {
        return Some((s, DurationUnit::Millis));
    }

    None
}

// Parse one ASCII digit string into `u64`.
fn parse_duration_ascii_u64(s: &str) -> Option<u64> {
    let mut value = 0_u64;
    for byte in s.bytes() {
        let digit = byte.checked_sub(b'0')?;
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u64::from(digit))?;
    }

    Some(value)
}

impl Add for Duration {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0.saturating_add(rhs.0))
    }
}

impl AddAssign for Duration {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0.saturating_add(rhs.0);
    }
}

impl Sub for Duration {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0.saturating_sub(rhs.0))
    }
}

impl SubAssign for Duration {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 = self.0.saturating_sub(rhs.0);
    }
}

impl Serialize for Duration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DurationVisitor;

        impl serde::de::Visitor<'_> for DurationVisitor {
            type Value = Duration;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "milliseconds or duration string")
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(Duration::from_millis(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let millis =
                    u64::try_from(v).map_err(|_| E::custom("duration must be non-negative"))?;
                Ok(Duration::from_millis(millis))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Duration::parse_flexible(v).map_err(E::custom)
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&v)
            }
        }

        deserializer.deserialize_any(DurationVisitor)
    }
}

impl Repr for Duration {
    type Inner = u64;

    fn repr(&self) -> Self::Inner {
        self.0
    }

    fn from_repr(inner: Self::Inner) -> Self {
        Self(inner)
    }
}

impl FieldValue for Duration {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Duration(*self)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Duration(v) => Some(*v),
            _ => None,
        }
    }
}

impl Atomic for Duration {}

impl From<u64> for Duration {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl NumericValue for Duration {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_u64(self.0)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_u64().map(Self)
    }
}

impl SanitizeAuto for Duration {}

impl SanitizeCustom for Duration {}

impl ValidateAuto for Duration {}

impl ValidateCustom for Duration {}

impl Visitable for Duration {}

//
// TESTS
//

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_explicit_unit_suffix_constructors() {
        assert_eq!(Duration::from_millis(10).as_millis(), 10);
        assert_eq!(Duration::from_secs(2).as_millis(), 2_000);
        assert_eq!(Duration::from_minutes(1).as_millis(), 60_000);
        assert_eq!(Duration::from_hours(1).as_millis(), 3_600_000);
        assert_eq!(Duration::from_days(1).as_millis(), 86_400_000);
        assert_eq!(Duration::from_weeks(1).as_millis(), 604_800_000);
    }

    #[test]
    fn test_try_from_i64_rejects_negative() {
        let t = Duration::try_from_i64(-1);
        assert!(t.is_none());
    }

    #[test]
    fn test_duration_arithmetic_is_millisecond_saturating() {
        let a = Duration::from_millis(2_000);
        let b = Duration::from_millis(750);
        assert_eq!(a + b, Duration::from_millis(2_750));
        assert_eq!(a - b, Duration::from_millis(1_250));
        assert_eq!(b - a, Duration::ZERO);
    }

    #[test]
    fn test_duration_parse_integer() {
        let parsed = Duration::parse_flexible("5000").unwrap();
        assert_eq!(parsed, Duration::from_millis(5_000));
    }

    #[test]
    fn test_duration_parse_units() {
        assert_eq!(
            Duration::parse_flexible("150ms").unwrap(),
            Duration::from_millis(150)
        );
        assert_eq!(
            Duration::parse_flexible("5s").unwrap(),
            Duration::from_secs(5)
        );
        assert_eq!(
            Duration::parse_flexible("10m").unwrap(),
            Duration::from_minutes(10)
        );
        assert_eq!(
            Duration::parse_flexible("2h").unwrap(),
            Duration::from_hours(2)
        );
        assert_eq!(
            Duration::parse_flexible("3d").unwrap(),
            Duration::from_days(3)
        );
    }

    #[test]
    fn test_duration_parse_rejects_invalid_units_and_whitespace() {
        assert!(Duration::parse_flexible("3w").is_err());
        assert!(Duration::parse_flexible(" 5000 ").is_err());
    }

    #[test]
    fn test_duration_parse_rejects_overflow_inputs() {
        assert!(Duration::parse_flexible("18446744073709551616").is_err());
        assert!(Duration::parse_flexible("18446744073709551616s").is_err());
    }

    #[test]
    fn test_duration_constructors_and_addition_saturate_on_overflow() {
        assert_eq!(Duration::from_secs(u64::MAX), Duration::MAX);
        assert_eq!(Duration::from_minutes(u64::MAX), Duration::MAX);
        assert_eq!(Duration::from_hours(u64::MAX), Duration::MAX);
        assert_eq!(Duration::from_days(u64::MAX), Duration::MAX);
        assert_eq!(Duration::from_weeks(u64::MAX), Duration::MAX);

        let almost_max = Duration::from_millis(u64::MAX - 1);
        assert_eq!(almost_max + Duration::from_millis(10), Duration::MAX);
    }

    #[test]
    fn test_json_duration_roundtrip() {
        let d = Duration::from_secs(5);
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(json, "5000");
        let parsed: Duration = serde_json::from_str(&json).unwrap();
        assert_eq!(d, parsed);
    }

    #[test]
    fn test_json_duration_string_deserialization() {
        let from_millis: Duration = serde_json::from_str("\"5000\"").unwrap();
        assert_eq!(from_millis, Duration::from_millis(5_000));

        let from_seconds: Duration = serde_json::from_str("\"5s\"").unwrap();
        assert_eq!(from_seconds, Duration::from_secs(5));
    }

    #[test]
    fn test_serde_cbor_boundary_uses_integer_millis_not_text_duration() {
        let d = Duration::from_secs(5);

        let bytes = serde_cbor::to_vec(&d).expect("duration serialization should succeed");
        let wire: serde_cbor::Value =
            serde_cbor::from_slice(&bytes).expect("duration cbor decode should succeed");

        match wire {
            serde_cbor::Value::Integer(millis) => {
                assert_eq!(millis, 5_000);
            }
            other => panic!("duration wire shape must remain integer millis, got {other:?}"),
        }

        let decoded: Duration =
            serde_cbor::from_slice(&bytes).expect("duration decode should succeed");
        assert_eq!(decoded, d);
    }
}
