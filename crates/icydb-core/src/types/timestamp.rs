use crate::{
    traits::{
        AsView, Atomic, EntityKeyBytes, FieldValue, FieldValueKind, NumCast, NumFromPrimitive,
        NumToPrimitive, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
    types::{Duration, Repr},
    value::Value,
};
use candid::CandidType;
use canic_cdk::utils::time::now_millis;
use derive_more::{Display, FromStr};
use serde::{Deserialize, Serialize};
use std::ops::{Add, AddAssign, Sub, SubAssign};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

// Invariant:
// Timestamp and Duration are both millisecond-native.
// All arithmetic is millisecond-consistent.
// Wire format remains transparent u64.

///
/// Timestamp
///
/// Stored as Unix milliseconds.
/// Wire format remains a bare `u64` for backward compatibility.
///

#[derive(
    CandidType,
    Clone,
    Copy,
    Debug,
    Default,
    Display,
    Eq,
    FromStr,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
#[repr(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const EPOCH: Self = Self(u64::MIN);
    pub const MIN: Self = Self(u64::MIN);
    pub const MAX: Self = Self(u64::MAX);

    /// Construct from seconds (`u64`).
    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        Self(Duration::from_secs(secs).as_millis())
    }

    /// Construct from milliseconds (`u64`).
    #[must_use]
    pub const fn from_millis(ms: u64) -> Self {
        Self(ms)
    }

    /// Construct from microseconds (`u64`), truncating to whole milliseconds.
    #[must_use]
    pub const fn from_micros(us: u64) -> Self {
        Self(Duration::from_micros_truncating(us).as_millis())
    }

    /// Construct from nanoseconds (`u64`), truncating to whole milliseconds.
    #[must_use]
    pub const fn from_nanos(ns: u64) -> Self {
        Self(Duration::from_nanos_truncating(ns).as_millis())
    }

    #[expect(clippy::cast_sign_loss)]
    #[expect(clippy::cast_possible_truncation)]
    pub fn parse_rfc3339(s: &str) -> Result<Self, String> {
        let dt = OffsetDateTime::parse(s, &Rfc3339)
            .map_err(|e| format!("timestamp parse error: {e}"))?;

        let ts_millis = dt.unix_timestamp_nanos() / 1_000_000;

        if ts_millis < 0 {
            return Err("timestamp before epoch".to_string());
        }

        Ok(Self::from_millis(ts_millis as u64))
    }

    pub fn parse_flexible(s: &str) -> Result<Self, String> {
        // Try integer milliseconds.
        if let Ok(n) = s.parse::<u64>() {
            return Ok(Self::from_millis(n));
        }

        // Try RFC3339
        Self::parse_rfc3339(s)
    }

    /// Current wall-clock timestamp in milliseconds.
    #[must_use]
    pub fn now() -> Self {
        Self(now_millis())
    }

    /// Return Unix milliseconds as `u64`.
    #[must_use]
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// Return Unix seconds as `u64`.
    #[must_use]
    pub const fn as_secs(self) -> u64 {
        Duration::from_millis(self.0).as_secs()
    }
}

impl Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0.saturating_add(rhs.repr()))
    }
}

impl AddAssign<Duration> for Timestamp {
    fn add_assign(&mut self, rhs: Duration) {
        self.0 = self.0.saturating_add(rhs.repr());
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        Self(self.0.saturating_sub(rhs.repr()))
    }
}

impl SubAssign<Duration> for Timestamp {
    fn sub_assign(&mut self, rhs: Duration) {
        self.0 = self.0.saturating_sub(rhs.repr());
    }
}

impl Sub for Timestamp {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Self::Output {
        Duration::from_millis(self.0.saturating_sub(rhs.0))
    }
}

impl AsView for Timestamp {
    type ViewType = u64;

    fn as_view(&self) -> Self::ViewType {
        self.0
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self(view)
    }
}

impl Repr for Timestamp {
    type Inner = u64;

    fn repr(&self) -> Self::Inner {
        self.0
    }

    fn from_repr(inner: Self::Inner) -> Self {
        Self(inner)
    }
}

impl Atomic for Timestamp {}

impl EntityKeyBytes for Timestamp {
    const BYTE_LEN: usize = ::core::mem::size_of::<u64>();

    fn write_bytes(&self, out: &mut [u8]) {
        assert_eq!(out.len(), Self::BYTE_LEN);
        out.copy_from_slice(&self.as_millis().to_be_bytes());
    }
}

impl FieldValue for Timestamp {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Timestamp(*self)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Timestamp(v) => Some(*v),
            _ => None,
        }
    }
}

impl NumCast for Timestamp {
    fn from<T: NumToPrimitive>(n: T) -> Option<Self> {
        n.to_u64().map(Self)
    }
}

impl NumFromPrimitive for Timestamp {
    #[expect(clippy::cast_sign_loss)]
    fn from_i64(n: i64) -> Option<Self> {
        if n < 0 { None } else { Some(Self(n as u64)) }
    }

    fn from_u64(n: u64) -> Option<Self> {
        Some(Self(n))
    }
}

impl From<u64> for Timestamp {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl NumToPrimitive for Timestamp {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }

    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }
}

impl SanitizeAuto for Timestamp {}

impl SanitizeCustom for Timestamp {}

impl ValidateAuto for Timestamp {}

impl ValidateCustom for Timestamp {}

impl Visitable for Timestamp {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_secs() {
        let t = Timestamp::from_secs(42);
        assert_eq!(t.as_secs(), 42);
        assert_eq!(t.as_millis(), 42_000);
    }

    #[test]
    fn test_explicit_unit_suffix_constructors() {
        assert_eq!(Timestamp::from_secs(42).as_secs(), 42);
        assert_eq!(Timestamp::from_millis(1_234).as_millis(), 1_234);
        assert_eq!(Timestamp::from_micros(5_000_000).as_millis(), 5_000);
        assert_eq!(Timestamp::from_nanos(3_000_000_000).as_millis(), 3_000);
    }

    #[test]
    fn test_parse_rfc3339_manual() {
        // Real RFC-3339 timestamp, exactly how JustTCG returns them.
        let input = "2024-03-09T19:45:30Z";

        let parsed = Timestamp::parse_rfc3339(input).unwrap();

        // Verified UNIX time for that timestamp in milliseconds.
        let expected = 1_710_013_530_000u64;

        assert_eq!(parsed.as_millis(), expected);
    }

    #[test]
    fn test_parse_rfc3339_rejects_pre_epoch() {
        let result = Timestamp::parse_rfc3339("1969-12-31T23:59:59Z");
        assert!(result.is_err());
    }

    #[test]
    fn test_from_i64_rejects_negative() {
        let t = <Timestamp as NumFromPrimitive>::from_i64(-1);
        assert!(t.is_none());
    }

    #[test]
    fn test_from_millis() {
        let t = Timestamp::from_millis(1234);
        assert_eq!(t.as_millis(), 1_234);
        assert_eq!(t.as_secs(), 1);
    }

    #[test]
    fn test_from_micros() {
        let t = Timestamp::from_micros(5_000_000);
        assert_eq!(t.as_millis(), 5_000);
        assert_eq!(t.as_secs(), 5);
    }

    #[test]
    fn test_from_nanos() {
        let t = Timestamp::from_nanos(3_000_000_000);
        assert_eq!(t.as_millis(), 3_000);
        assert_eq!(t.as_secs(), 3);
    }

    #[test]
    fn test_parse_flexible_integer() {
        let t = Timestamp::parse_flexible("12345").unwrap();
        assert_eq!(t.as_millis(), 12_345);
    }

    #[test]
    fn test_parse_rfc3339_invalid() {
        let result = Timestamp::parse_rfc3339("not-a-timestamp");
        assert!(result.is_err());
    }

    #[test]
    fn test_now_is_nonzero() {
        let t = Timestamp::now();
        assert!(t.as_millis() > 0);
    }

    #[test]
    fn test_add_and_sub_with_duration_and_timestamp_difference() {
        let a = Timestamp::from_millis(5_000);
        let b = Timestamp::from_millis(2_000);
        let d = Duration::from_millis(999);

        assert_eq!(a + d, Timestamp::from_millis(5_999));
        assert_eq!(a - d, Timestamp::from_millis(4_001));
        assert_eq!(a - b, Duration::from_millis(3_000));
    }

    #[test]
    fn test_millisecond_precision_in_arithmetic() {
        let t = Timestamp::from_millis(1_500);
        let d = Duration::from_millis(1);
        assert_eq!(t + d, Timestamp::from_millis(1_501));
    }

    #[test]
    fn test_no_truncation_in_timestamp_duration_addition() {
        let t = Timestamp::from_millis(1_000);
        let d = Duration::from_millis(999);
        assert_eq!(t + d, Timestamp::from_millis(1_999));
    }

    #[test]
    fn test_cross_type_timestamp_difference_returns_millisecond_duration() {
        let t1 = Timestamp::from_millis(1_500);
        let t2 = Timestamp::from_millis(1_000);

        assert_eq!(t1 - t2, Duration::from_millis(500));
    }

    #[test]
    fn test_num_cast_roundtrip() {
        let t = Timestamp::from_secs(999);
        let i = t.to_u64().unwrap();
        assert_eq!(i, 999_000);

        let t2: Timestamp = i.into();
        assert_eq!(t2, t);
    }

    #[test]
    fn test_field_value() {
        let t = Timestamp::from_secs(77);
        let v = t.to_value();
        assert_eq!(v, Value::Timestamp(t));
    }
}
