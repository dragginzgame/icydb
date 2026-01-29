use crate::{
    traits::{
        FieldValue, Inner, NumCast, NumFromPrimitive, NumToPrimitive, SanitizeAuto, SanitizeCustom,
        UpdateView, ValidateAuto, ValidateCustom, View, Visitable,
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

impl FieldValue for Duration {
    fn to_value(&self) -> Value {
        Value::Duration(*self)
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

impl Inner<Self> for Duration {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
    }
}

impl NumCast for Duration {
    fn from<T: NumToPrimitive>(n: T) -> Option<Self> {
        n.to_u64().map(Self)
    }
}

impl NumFromPrimitive for Duration {
    #[allow(clippy::cast_sign_loss)]
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

impl UpdateView for Duration {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Duration {}

impl ValidateCustom for Duration {}

impl View for Duration {
    type ViewType = u64;

    fn to_view(&self) -> Self::ViewType {
        self.0
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self(view)
    }
}

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
}
