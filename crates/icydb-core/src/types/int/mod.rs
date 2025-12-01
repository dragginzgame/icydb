mod int128;

pub use int128::*;

use crate::{
    Value,
    db::primitives::{IntListFilterKind, IntRangeFilterKind},
    traits::{
        FieldValue, Filterable, Inner, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
};
use candid::{CandidType, Int as WrappedInt};
use canic::impl_storable_unbounded;
use derive_more::{Add, AddAssign, Deref, DerefMut, Display, FromStr, Sub, SubAssign};
use serde::{Deserialize, Serialize};
use std::iter::Sum;

///
/// Int
///

#[derive(
    Add,
    AddAssign,
    CandidType,
    Clone,
    Debug,
    Default,
    Deref,
    DerefMut,
    Display,
    Eq,
    PartialEq,
    FromStr,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    Sub,
    SubAssign,
)]
pub struct Int(WrappedInt);

impl Int {
    #[must_use]
    pub fn to_leb128(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out).expect("Int LEB128 encode");

        out
    }
}

impl FieldValue for Int {
    fn to_value(&self) -> Value {
        Value::IntBig(self.clone())
    }
}

impl Filterable for Int {
    type Filter = IntRangeFilterKind;
    type ListFilter = IntListFilterKind;
}

impl From<i32> for Int {
    fn from(n: i32) -> Self {
        Self(WrappedInt::from(n))
    }
}

impl From<WrappedInt> for Int {
    fn from(i: WrappedInt) -> Self {
        Self(i)
    }
}

impl Inner<Self> for Int {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
    }
}

impl SanitizeAuto for Int {}

impl SanitizeCustom for Int {}

impl_storable_unbounded!(Int);

impl Sum for Int {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |acc, x| acc + x)
    }
}

impl UpdateView for Int {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Int {}

impl ValidateCustom for Int {}

impl View for Int {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Visitable for Int {}
