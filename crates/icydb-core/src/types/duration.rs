use crate::{
    db::primitives::{Nat64ListFilterKind, Nat64RangeFilterKind},
    traits::{
        FieldValue, Filterable, Inner, NumCast, NumFromPrimitive, NumToPrimitive, SanitizeAuto,
        SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom, View, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use canic::utils::time::now_secs;
use derive_more::{Add, AddAssign, Display, FromStr, Sub, SubAssign};
use serde::{Deserialize, Serialize};

///
/// Duration
/// (in milliseconds)
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
pub struct Duration(u64);

impl Duration {
    pub const ZERO: Self = Self(0);
    pub const MIN: Self = Self(u64::MIN);
    pub const MAX: Self = Self(u64::MAX);

    #[must_use]
    pub fn now() -> Self {
        Self(now_secs())
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
        Self(secs * 1_000)
    }

    #[must_use]
    pub const fn from_minutes(mins: u64) -> Self {
        Self(mins * 60 * 1_000)
    }

    #[must_use]
    pub const fn from_hours(hours: u64) -> Self {
        Self(hours * 60 * 60 * 1_000)
    }

    #[must_use]
    pub const fn from_days(days: u64) -> Self {
        Self(days * 24 * 60 * 60 * 1_000)
    }

    #[must_use]
    pub const fn from_weeks(weeks: u64) -> Self {
        Self(weeks * 24 * 60 * 60 * 1_000 * 7)
    }

    // ---- Conversion back to larger units ----

    #[must_use]
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn as_secs(self) -> u64 {
        self.0 / 1_000
    }

    #[must_use]
    pub const fn as_minutes(self) -> u64 {
        self.0 / (60 * 1_000)
    }

    #[must_use]
    pub const fn as_hours(self) -> u64 {
        self.0 / (60 * 60 * 1_000)
    }

    #[must_use]
    pub const fn as_days(self) -> u64 {
        self.0 / (24 * 60 * 60 * 1_000)
    }

    #[must_use]
    pub const fn as_weeks(self) -> u64 {
        self.0 / (24 * 60 * 60 * 1_000 * 7)
    }
}

impl FieldValue for Duration {
    fn to_value(&self) -> Value {
        Value::Duration(*self)
    }
}

impl Filterable for Duration {
    type Filter = Nat64RangeFilterKind;
    type ListFilter = Nat64ListFilterKind;
}

impl From<i32> for Duration {
    fn from(n: i32) -> Self {
        Self(u64::try_from(n).unwrap_or(0))
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
        Some(Self(n as u64))
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
