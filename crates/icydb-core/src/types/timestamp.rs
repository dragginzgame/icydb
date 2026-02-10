use crate::{
    traits::{
        AsView, Atomic, EntityKeyBytes, FieldValue, FieldValueKind, NumCast, NumFromPrimitive,
        NumToPrimitive, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
    types::Duration,
    value::Value,
};
use candid::CandidType;
use canic_cdk::utils::time::now_secs;
use chrono::DateTime;
use derive_more::{Add, AddAssign, Display, FromStr, Sub, SubAssign};
use serde::{Deserialize, Serialize};

///
/// Timestamp
/// (in seconds)
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
#[repr(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const EPOCH: Self = Self(u64::MIN);
    pub const MIN: Self = Self(u64::MIN);
    pub const MAX: Self = Self(u64::MAX);

    /// Construct from seconds.
    #[must_use]
    pub const fn from_seconds(secs: u64) -> Self {
        Self(secs)
    }

    /// Construct from milliseconds (truncate to seconds).
    #[must_use]
    pub const fn from_millis(ms: u64) -> Self {
        Self(ms / 1_000)
    }

    /// Construct from microseconds (truncate to seconds).
    #[must_use]
    pub const fn from_micros(us: u64) -> Self {
        Self(us / 1_000_000)
    }

    /// Construct from nanoseconds (truncate to seconds).
    #[must_use]
    pub const fn from_nanos(ns: u64) -> Self {
        Self(ns / 1_000_000_000)
    }

    #[allow(clippy::cast_sign_loss)]
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

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
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

impl Atomic for Timestamp {}

impl EntityKeyBytes for Timestamp {
    const BYTE_LEN: usize = ::core::mem::size_of::<u64>();

    fn write_bytes(&self, out: &mut [u8]) {
        assert_eq!(out.len(), Self::BYTE_LEN);
        out.copy_from_slice(&self.get().to_be_bytes());
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

impl From<u64> for Timestamp {
    fn from(u: u64) -> Self {
        Self(u)
    }
}

impl PartialEq<u64> for Timestamp {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u64> for Timestamp {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl PartialEq<i64> for Timestamp {
    fn eq(&self, other: &i64) -> bool {
        if *other < 0 {
            false
        } else {
            self.0 == other.unsigned_abs()
        }
    }
}

impl PartialOrd<i64> for Timestamp {
    fn partial_cmp(&self, other: &i64) -> Option<std::cmp::Ordering> {
        if *other < 0 {
            Some(std::cmp::Ordering::Greater)
        } else {
            self.0.partial_cmp(&other.unsigned_abs())
        }
    }
}

impl PartialEq<Timestamp> for u64 {
    fn eq(&self, other: &Timestamp) -> bool {
        *self == other.0
    }
}

impl PartialOrd<Timestamp> for u64 {
    fn partial_cmp(&self, other: &Timestamp) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.0)
    }
}

impl PartialEq<Timestamp> for i64 {
    fn eq(&self, other: &Timestamp) -> bool {
        if *self < 0 {
            false
        } else {
            self.unsigned_abs() == other.0
        }
    }
}

impl PartialOrd<Timestamp> for i64 {
    fn partial_cmp(&self, other: &Timestamp) -> Option<std::cmp::Ordering> {
        if *self < 0 {
            Some(std::cmp::Ordering::Less)
        } else {
            self.unsigned_abs().partial_cmp(&other.0)
        }
    }
}

impl std::ops::Sub<Timestamp> for u64 {
    type Output = Self;

    fn sub(self, rhs: Timestamp) -> Self::Output {
        self.saturating_sub(rhs.0)
    }
}

impl std::ops::Sub<Timestamp> for i64 {
    type Output = Self;

    fn sub(self, rhs: Timestamp) -> Self::Output {
        self.saturating_sub(Self::try_from(rhs.0).unwrap_or(Self::MAX))
    }
}

impl std::ops::Add<u64> for Timestamp {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0.saturating_add(rhs))
    }
}

impl std::ops::AddAssign<u64> for Timestamp {
    fn add_assign(&mut self, rhs: u64) {
        self.0 = self.0.saturating_add(rhs);
    }
}

impl std::ops::Add<i64> for Timestamp {
    type Output = Self;

    fn add(self, rhs: i64) -> Self::Output {
        if rhs >= 0 {
            Self(self.0.saturating_add(rhs.unsigned_abs()))
        } else {
            Self(self.0.saturating_sub(rhs.unsigned_abs()))
        }
    }
}

impl std::ops::AddAssign<i64> for Timestamp {
    fn add_assign(&mut self, rhs: i64) {
        if rhs >= 0 {
            self.0 = self.0.saturating_add(rhs.unsigned_abs());
        } else {
            self.0 = self.0.saturating_sub(rhs.unsigned_abs());
        }
    }
}

impl std::ops::Sub<u64> for Timestamp {
    type Output = Self;

    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0.saturating_sub(rhs))
    }
}

impl std::ops::SubAssign<u64> for Timestamp {
    fn sub_assign(&mut self, rhs: u64) {
        self.0 = self.0.saturating_sub(rhs);
    }
}

impl std::ops::Sub<i64> for Timestamp {
    type Output = Self;

    fn sub(self, rhs: i64) -> Self::Output {
        if rhs >= 0 {
            Self(self.0.saturating_sub(rhs.unsigned_abs()))
        } else {
            Self(self.0.saturating_add(rhs.unsigned_abs()))
        }
    }
}

impl std::ops::SubAssign<i64> for Timestamp {
    fn sub_assign(&mut self, rhs: i64) {
        if rhs >= 0 {
            self.0 = self.0.saturating_sub(rhs.unsigned_abs());
        } else {
            self.0 = self.0.saturating_add(rhs.unsigned_abs());
        }
    }
}

impl std::ops::Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        // Timestamp stores seconds, so duration is truncated from millis to whole seconds.
        Self(self.0.saturating_add(rhs.as_secs()))
    }
}

impl std::ops::AddAssign<Duration> for Timestamp {
    fn add_assign(&mut self, rhs: Duration) {
        // Timestamp stores seconds, so duration is truncated from millis to whole seconds.
        self.0 = self.0.saturating_add(rhs.as_secs());
    }
}

impl std::ops::Sub<Duration> for Timestamp {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        // Timestamp stores seconds, so duration is truncated from millis to whole seconds.
        Self(self.0.saturating_sub(rhs.as_secs()))
    }
}

impl std::ops::SubAssign<Duration> for Timestamp {
    fn sub_assign(&mut self, rhs: Duration) {
        // Timestamp stores seconds, so duration is truncated from millis to whole seconds.
        self.0 = self.0.saturating_sub(rhs.as_secs());
    }
}

impl NumCast for Timestamp {
    fn from<T: NumToPrimitive>(n: T) -> Option<Self> {
        n.to_u64().map(Self)
    }
}

impl NumFromPrimitive for Timestamp {
    #[allow(clippy::cast_sign_loss)]
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
    fn test_from_seconds() {
        let t = Timestamp::from_seconds(42);
        assert_eq!(t.get(), 42);
    }

    #[test]
    fn test_parse_rfc3339_manual() {
        // Real RFC-3339 timestamp, exactly how JustTCG returns them.
        let input = "2024-03-09T19:45:30Z";

        let parsed = Timestamp::parse_rfc3339(input).unwrap();

        // Verified UNIX time for that timestamp.
        let expected = 1_710_013_530u64;

        assert_eq!(parsed.get(), expected);
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
        assert_eq!(t.get(), 1); // truncates
    }

    #[test]
    fn test_from_micros() {
        let t = Timestamp::from_micros(5_000_000);
        assert_eq!(t.get(), 5);
    }

    #[test]
    fn test_from_nanos() {
        let t = Timestamp::from_nanos(3_000_000_000);
        assert_eq!(t.get(), 3);
    }

    #[test]
    fn test_parse_flexible_integer() {
        let t = Timestamp::parse_flexible("12345").unwrap();
        assert_eq!(t.get(), 12345);
    }

    #[test]
    fn test_parse_rfc3339_invalid() {
        let result = Timestamp::parse_rfc3339("not-a-timestamp");
        assert!(result.is_err());
    }

    #[test]
    fn test_now_is_nonzero() {
        let t = Timestamp::now();
        assert!(t.get() > 0);
    }

    #[test]
    fn test_add_and_sub() {
        let a = Timestamp::from_seconds(10);
        let b = Timestamp::from_seconds(3);

        assert_eq!((a + b).get(), 13);
        assert_eq!((a - b).get(), 7);
    }

    #[test]
    fn test_num_cast_roundtrip() {
        let t = Timestamp::from_seconds(999);
        let i = t.to_u64().unwrap();
        assert_eq!(i, 999);

        let t2 = Timestamp::from_seconds(i);
        assert_eq!(t2, t);
    }

    #[test]
    fn test_field_value() {
        let t = Timestamp::from_seconds(77);
        let v = t.to_value();
        assert_eq!(v, Value::Timestamp(t));
    }

    #[test]
    fn test_add_and_sub_with_u64() {
        let mut t = Timestamp::from_seconds(10);

        assert_eq!((t + 5_u64).get(), 15);
        assert_eq!((t - 3_u64).get(), 7);

        t += 8_u64;
        assert_eq!(t.get(), 18);

        t -= 20_u64;
        assert_eq!(t.get(), 0);
    }

    #[test]
    fn test_add_and_sub_with_i64() {
        let mut t = Timestamp::from_seconds(10);

        assert_eq!((t + 5_i64).get(), 15);
        assert_eq!((t + (-3_i64)).get(), 7);
        assert_eq!((t - 3_i64).get(), 7);
        assert_eq!((t - (-5_i64)).get(), 15);

        t += 8_i64;
        assert_eq!(t.get(), 18);

        t += -20_i64;
        assert_eq!(t.get(), 0);

        t -= -3_i64;
        assert_eq!(t.get(), 3);

        t -= 10_i64;
        assert_eq!(t.get(), 0);

        // Ensure i64::MIN does not overflow and saturates safely.
        assert_eq!((Timestamp::from_seconds(5) + i64::MIN).get(), 0);
        assert_eq!(
            (Timestamp::from_seconds(5) - i64::MIN).get(),
            5_u64.saturating_add(i64::MIN.unsigned_abs())
        );
    }

    #[test]
    fn test_add_and_sub_with_duration() {
        let mut t = Timestamp::from_seconds(10);
        let delta = Duration::from_millis(2_500);

        // Duration is milliseconds; Timestamp arithmetic truncates to whole seconds.
        assert_eq!((t + delta).get(), 12);
        assert_eq!((t - delta).get(), 8);

        t += delta;
        assert_eq!(t.get(), 12);

        t -= Duration::from_secs(20);
        assert_eq!(t.get(), 0);
    }

    #[test]
    fn test_compare_with_scalars() {
        let t = Timestamp::from_seconds(10);

        assert!(t > 9_u64);
        assert!(t >= 10_u64);
        assert!(t < 11_u64);
        assert_eq!(t, 10_u64);

        assert!(t > -1_i64);
        assert!(t > 0_i64);
        assert!(t < 11_i64);
        assert_eq!(t, 10_i64);

        assert!(9_u64 < t);
        assert!(10_u64 <= t);
        assert!(11_i64 > t);
        assert!(-1_i64 < t);
    }

    #[test]
    fn test_sub_from_scalars() {
        let t = Timestamp::from_seconds(10);

        assert_eq!(15_u64 - t, 5);
        assert_eq!(5_u64 - t, 0);

        assert_eq!(15_i64 - t, 5);
        assert_eq!(0_i64 - t, -10);
    }
}
