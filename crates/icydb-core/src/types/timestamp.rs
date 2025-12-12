use crate::{
    db::primitives::{Nat64ListFilterKind, Nat64RangeFilterKind},
    traits::{
        FieldValue, Filterable, Inner, NumCast, NumFromPrimitive, NumToPrimitive, SanitizeAuto,
        SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom, View, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use canic_cdk::utils::time::now_secs;
use chrono::DateTime;
use derive_more::{Add, AddAssign, Deref, DerefMut, Display, FromStr, Sub, SubAssign};
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
    Deref,
    DerefMut,
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

        Ok(Self(dt.timestamp() as u64))
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

impl FieldValue for Timestamp {
    fn to_value(&self) -> Value {
        Value::Timestamp(*self)
    }
}

impl Filterable for Timestamp {
    type Filter = Nat64RangeFilterKind;
    type ListFilter = Nat64ListFilterKind;
}

impl From<u64> for Timestamp {
    fn from(u: u64) -> Self {
        Self(u)
    }
}

impl Inner<Self> for Timestamp {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
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
        Some(Self(n as u64))
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

impl UpdateView for Timestamp {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Timestamp {}

impl ValidateCustom for Timestamp {}

impl View for Timestamp {
    type ViewType = u64;

    fn to_view(&self) -> Self::ViewType {
        self.0
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self(view)
    }
}

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
}
