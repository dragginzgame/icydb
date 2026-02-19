use crate::{
    traits::{
        AsView, Atomic, FieldValue, FieldValueKind, NumCast, NumFromPrimitive, NumToPrimitive,
        SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
    types::Repr,
    value::Value,
};
use candid::CandidType;
use derive_more::{Display, FromStr};
use serde::{Deserialize, Serialize};

///
/// Duration
///
/// Stored as milliseconds.
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
}

impl AsView for Duration {
    type ViewType = u64;

    fn as_view(&self) -> Self::ViewType {
        self.0
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self(view)
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

impl NumCast for Duration {
    fn from<T: NumToPrimitive>(n: T) -> Option<Self> {
        n.to_u64().map(Self)
    }
}

impl NumFromPrimitive for Duration {
    #[expect(clippy::cast_sign_loss)]
    fn from_i64(n: i64) -> Option<Self> {
        if n < 0 { None } else { Some(Self(n as u64)) }
    }

    fn from_u64(n: u64) -> Option<Self> {
        Some(Self(n))
    }
}

impl From<u64> for Duration {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl NumToPrimitive for Duration {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }

    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }
}

impl SanitizeAuto for Duration {}

impl SanitizeCustom for Duration {}

impl ValidateAuto for Duration {}

impl ValidateCustom for Duration {}

impl Visitable for Duration {}

///
/// TESTS
///

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
    fn test_wire_format_is_bare_number() {
        let original = Duration::from_millis(1_500);

        let json = serde_json::to_string(&original).expect("duration JSON serialize");
        assert_eq!(json, "1500");

        let decoded: Duration = serde_json::from_str("1500").expect("duration JSON deserialize");
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_from_i64_rejects_negative() {
        let t = <Duration as NumFromPrimitive>::from_i64(-1);
        assert!(t.is_none());
    }
}
