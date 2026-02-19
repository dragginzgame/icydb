use crate::{
    traits::{
        AsView, Atomic, EntityKeyBytes, FieldValue, FieldValueKind, NumCast, NumFromPrimitive,
        NumToPrimitive, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
    types::{Duration, Repr},
    value::Value,
};
use candid::CandidType;
use canic_cdk::utils::time::now_secs;
use chrono::DateTime;
use derive_more::{Add, AddAssign, Display, FromStr, Sub, SubAssign};
use serde::{Deserialize, Serialize};

///
/// Timestamp
///
/// Stored as Unix seconds.
/// Wire format remains a bare `u64` for backward compatibility.
///

#[derive(
    Add,
    AddAssign,
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
    Sub,
    SubAssign,
)]
#[serde(transparent)]
#[repr(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const EPOCH: Self = Self(u64::MIN);
    pub const MIN: Self = Self(u64::MIN);
    pub const MAX: Self = Self(u64::MAX);
    const MILLIS_PER_SECOND: u64 = 1_000;
    const MICROS_PER_SECOND: u64 = 1_000_000;
    const NANOS_PER_SECOND: u64 = 1_000_000_000;

    /// Construct from seconds (`u64`).
    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs)
    }

    /// Construct from milliseconds (`u64`), truncating to whole seconds.
    #[must_use]
    pub const fn from_millis(ms: u64) -> Self {
        Self(ms / Self::MILLIS_PER_SECOND)
    }

    /// Construct from microseconds (`u64`), truncating to whole seconds.
    #[must_use]
    pub const fn from_micros(us: u64) -> Self {
        Self(us / Self::MICROS_PER_SECOND)
    }

    /// Construct from nanoseconds (`u64`), truncating to whole seconds.
    #[must_use]
    pub const fn from_nanos(ns: u64) -> Self {
        Self(ns / Self::NANOS_PER_SECOND)
    }

    #[expect(clippy::cast_sign_loss)]
    pub fn parse_rfc3339(s: &str) -> Result<Self, String> {
        let dt =
            DateTime::parse_from_rfc3339(s).map_err(|e| format!("timestamp parse error: {e}"))?;
        let ts = dt.timestamp();
        if ts < 0 {
            return Err("timestamp before epoch".to_string());
        }

        Ok(Self(ts as u64))
    }

    pub fn parse_flexible(s: &str) -> Result<Self, String> {
        // Try integer seconds
        if let Ok(n) = s.parse::<u64>() {
            return Ok(Self(n));
        }

        // Try RFC3339
        Self::parse_rfc3339(s)
    }

    #[must_use]
    /// Current wall-clock timestamp in seconds.
    pub fn now() -> Self {
        Self(now_secs())
    }

    /// Return Unix seconds as `u64`.
    #[must_use]
    pub const fn as_secs(self) -> u64 {
        self.0
    }

    /// Add a millisecond-backed duration, truncating sub-second precision.
    #[must_use]
    pub const fn saturating_add_duration_truncating(self, rhs: Duration) -> Self {
        Self(self.0.saturating_add(rhs.as_secs()))
    }

    /// Subtract a millisecond-backed duration, truncating sub-second precision.
    #[must_use]
    pub const fn saturating_sub_duration_truncating(self, rhs: Duration) -> Self {
        Self(self.0.saturating_sub(rhs.as_secs()))
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
        out.copy_from_slice(&self.as_secs().to_be_bytes());
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
    }

    #[test]
    fn test_explicit_unit_suffix_constructors() {
        assert_eq!(Timestamp::from_secs(42).as_secs(), 42);
        assert_eq!(Timestamp::from_millis(1_234).as_secs(), 1);
        assert_eq!(Timestamp::from_micros(5_000_000).as_secs(), 5);
        assert_eq!(Timestamp::from_nanos(3_000_000_000).as_secs(), 3);
    }

    #[test]
    fn test_parse_rfc3339_manual() {
        // Real RFC-3339 timestamp, exactly how JustTCG returns them.
        let input = "2024-03-09T19:45:30Z";

        let parsed = Timestamp::parse_rfc3339(input).unwrap();

        // Verified UNIX time for that timestamp.
        let expected = 1_710_013_530u64;

        assert_eq!(parsed.as_secs(), expected);
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
        assert_eq!(t.as_secs(), 1); // truncates
    }

    #[test]
    fn test_from_micros() {
        let t = Timestamp::from_micros(5_000_000);
        assert_eq!(t.as_secs(), 5);
    }

    #[test]
    fn test_from_nanos() {
        let t = Timestamp::from_nanos(3_000_000_000);
        assert_eq!(t.as_secs(), 3);
    }

    #[test]
    fn test_parse_flexible_integer() {
        let t = Timestamp::parse_flexible("12345").unwrap();
        assert_eq!(t.as_secs(), 12345);
    }

    #[test]
    fn test_parse_rfc3339_invalid() {
        let result = Timestamp::parse_rfc3339("not-a-timestamp");
        assert!(result.is_err());
    }

    #[test]
    fn test_now_is_nonzero() {
        let t = Timestamp::now();
        assert!(t.as_secs() > 0);
    }

    #[test]
    fn test_add_and_sub() {
        let a = Timestamp::from_secs(10);
        let b = Timestamp::from_secs(3);

        assert_eq!((a + b).as_secs(), 13);
        assert_eq!((a - b).as_secs(), 7);
    }

    #[test]
    fn test_num_cast_roundtrip() {
        let t = Timestamp::from_secs(999);
        let i = t.to_u64().unwrap();
        assert_eq!(i, 999);

        let t2 = Timestamp::from_secs(i);
        assert_eq!(t2, t);
    }

    #[test]
    fn test_field_value() {
        let t = Timestamp::from_secs(77);
        let v = t.to_value();
        assert_eq!(v, Value::Timestamp(t));
    }

    #[test]
    fn test_wire_format_is_bare_number() {
        let original = Timestamp::from_secs(42);

        let json = serde_json::to_string(&original).expect("timestamp JSON serialize");
        assert_eq!(json, "42");

        let decoded: Timestamp = serde_json::from_str("42").expect("timestamp JSON deserialize");
        assert_eq!(decoded, original);
    }
}
