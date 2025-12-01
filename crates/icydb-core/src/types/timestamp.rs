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

    #[must_use]
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
