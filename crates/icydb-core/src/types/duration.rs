use crate::{
    traits::{
        AsView, Atomic, FieldValue, FieldValueKind, NumCast, NumFromPrimitive, NumToPrimitive,
        SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use canic_cdk::utils::time::{now_millis, now_secs};
use derive_more::{Display, FromStr};
use serde::{Deserialize, Serialize};
use std::ops::{Add, AddAssign, Sub, SubAssign};

///
/// Duration
/// (in milliseconds)
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

    /// Duration since the Unix epoch in seconds
    #[must_use]
    pub fn now_secs() -> Self {
        Self(now_secs())
    }

    /// Duration since the Unix epoch in milliseconds
    #[must_use]
    pub fn now_millis() -> Self {
        Self(now_millis())
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

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

impl Add for Duration {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }
}

impl AddAssign for Duration {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0.saturating_add(rhs.0);
    }
}

impl Add<u64> for Duration {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0.saturating_add(rhs))
    }
}

impl AddAssign<u64> for Duration {
    fn add_assign(&mut self, rhs: u64) {
        self.0 = self.0.saturating_add(rhs);
    }
}

impl Add<i64> for Duration {
    type Output = Self;

    fn add(self, rhs: i64) -> Self::Output {
        if rhs >= 0 {
            Self(self.0.saturating_add(rhs.unsigned_abs()))
        } else {
            Self(self.0.saturating_sub(rhs.unsigned_abs()))
        }
    }
}

impl AddAssign<i64> for Duration {
    fn add_assign(&mut self, rhs: i64) {
        if rhs >= 0 {
            self.0 = self.0.saturating_add(rhs.unsigned_abs());
        } else {
            self.0 = self.0.saturating_sub(rhs.unsigned_abs());
        }
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

impl TryFrom<i32> for Duration {
    type Error = std::num::TryFromIntError;

    fn try_from(n: i32) -> Result<Self, Self::Error> {
        let v = Self(u64::try_from(n)?);
        Ok(v)
    }
}

impl From<u64> for Duration {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl PartialEq<u64> for Duration {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u64> for Duration {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl PartialEq<i64> for Duration {
    fn eq(&self, other: &i64) -> bool {
        if *other < 0 {
            false
        } else {
            self.0 == other.unsigned_abs()
        }
    }
}

impl PartialOrd<i64> for Duration {
    fn partial_cmp(&self, other: &i64) -> Option<std::cmp::Ordering> {
        if *other < 0 {
            Some(std::cmp::Ordering::Greater)
        } else {
            self.0.partial_cmp(&other.unsigned_abs())
        }
    }
}

impl PartialEq<Duration> for u64 {
    fn eq(&self, other: &Duration) -> bool {
        *self == other.0
    }
}

impl PartialOrd<Duration> for u64 {
    fn partial_cmp(&self, other: &Duration) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.0)
    }
}

impl PartialEq<Duration> for i64 {
    fn eq(&self, other: &Duration) -> bool {
        if *self < 0 {
            false
        } else {
            self.unsigned_abs() == other.0
        }
    }
}

impl PartialOrd<Duration> for i64 {
    fn partial_cmp(&self, other: &Duration) -> Option<std::cmp::Ordering> {
        if *self < 0 {
            Some(std::cmp::Ordering::Less)
        } else {
            self.unsigned_abs().partial_cmp(&other.0)
        }
    }
}

impl Sub<Duration> for u64 {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        self.saturating_sub(rhs.0)
    }
}

impl Sub<Duration> for i64 {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        self.saturating_sub(Self::try_from(rhs.0).unwrap_or(Self::MAX))
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

impl Sub for Duration {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }
}

impl SubAssign for Duration {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 = self.0.saturating_sub(rhs.0);
    }
}

impl Sub<u64> for Duration {
    type Output = Self;

    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0.saturating_sub(rhs))
    }
}

impl SubAssign<u64> for Duration {
    fn sub_assign(&mut self, rhs: u64) {
        self.0 = self.0.saturating_sub(rhs);
    }
}

impl Sub<i64> for Duration {
    type Output = Self;

    fn sub(self, rhs: i64) -> Self::Output {
        if rhs >= 0 {
            Self(self.0.saturating_sub(rhs.unsigned_abs()))
        } else {
            Self(self.0.saturating_add(rhs.unsigned_abs()))
        }
    }
}

impl SubAssign<i64> for Duration {
    fn sub_assign(&mut self, rhs: i64) {
        if rhs >= 0 {
            self.0 = self.0.saturating_sub(rhs.unsigned_abs());
        } else {
            self.0 = self.0.saturating_add(rhs.unsigned_abs());
        }
    }
}

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
    fn test_from_i64_rejects_negative() {
        let t = <Duration as NumFromPrimitive>::from_i64(-1);
        assert!(t.is_none());
    }

    #[test]
    fn test_add_and_sub_with_u64() {
        let mut d = Duration::from_millis(10);

        assert_eq!((d + 5_u64).get(), 15);
        assert_eq!((d - 3_u64).get(), 7);

        d += 8_u64;
        assert_eq!(d.get(), 18);

        d -= 20_u64;
        assert_eq!(d.get(), 0);
    }

    #[test]
    fn test_add_and_sub_with_i64() {
        let mut d = Duration::from_millis(10);

        assert_eq!((d + 5_i64).get(), 15);
        assert_eq!((d + (-3_i64)).get(), 7);
        assert_eq!((d - 3_i64).get(), 7);
        assert_eq!((d - (-5_i64)).get(), 15);

        d += 8_i64;
        assert_eq!(d.get(), 18);

        d += -20_i64;
        assert_eq!(d.get(), 0);

        d -= -3_i64;
        assert_eq!(d.get(), 3);

        d -= 10_i64;
        assert_eq!(d.get(), 0);

        // Ensure i64::MIN does not overflow and saturates safely.
        assert_eq!((Duration::from_millis(5) + i64::MIN).get(), 0);
        assert_eq!(
            (Duration::from_millis(5) - i64::MIN).get(),
            5_u64.saturating_add(i64::MIN.unsigned_abs())
        );
    }

    #[test]
    fn test_compare_with_scalars() {
        let d = Duration::from_millis(10);

        assert!(d > 9_u64);
        assert!(d >= 10_u64);
        assert!(d < 11_u64);
        assert_eq!(d, 10_u64);

        assert!(d > -1_i64);
        assert!(d > 0_i64);
        assert!(d < 11_i64);
        assert_eq!(d, 10_i64);

        assert!(9_u64 < d);
        assert!(10_u64 <= d);
        assert!(11_i64 > d);
        assert!(-1_i64 < d);
    }

    #[test]
    fn test_sub_from_scalars() {
        let d = Duration::from_millis(10);

        assert_eq!(15_u64 - d, 5);
        assert_eq!(5_u64 - d, 0);

        assert_eq!(15_i64 - d, 5);
        assert_eq!(0_i64 - d, -10);
    }
}
