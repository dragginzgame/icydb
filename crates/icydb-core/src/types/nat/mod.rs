mod nat128;

pub use nat128::*;

use crate::{
    db::primitives::{NatListFilterKind, NatRangeFilterKind},
    traits::{
        FieldValue, Filterable, Inner, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
    value::Value,
};
use candid::{CandidType, Nat as WrappedNat};
use derive_more::{Add, AddAssign, Deref, DerefMut, Display, FromStr, Sub, SubAssign};
use serde::{Deserialize, Serialize};
use std::iter::Sum;

///
/// Nat
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
pub struct Nat(WrappedNat);

impl Nat {
    #[must_use]
    pub fn to_leb128(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out).expect("Nat LEB128 encode");

        out
    }
}

impl FieldValue for Nat {
    fn to_value(&self) -> Value {
        Value::UintBig(self.clone())
    }
}

impl Filterable for Nat {
    type Filter = NatRangeFilterKind;
    type ListFilter = NatListFilterKind;
}

impl From<i32> for Nat {
    fn from(n: i32) -> Self {
        Self(WrappedNat::from(u32::try_from(n).unwrap_or(0)))
    }
}

impl From<WrappedNat> for Nat {
    fn from(n: WrappedNat) -> Self {
        Self(n)
    }
}

impl Inner<Self> for Nat {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
    }
}

impl SanitizeAuto for Nat {}

impl SanitizeCustom for Nat {}

impl Sum for Nat {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |acc, x| acc + x)
    }
}

impl UpdateView for Nat {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Nat {}

impl ValidateCustom for Nat {}

impl View for Nat {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Visitable for Nat {}
