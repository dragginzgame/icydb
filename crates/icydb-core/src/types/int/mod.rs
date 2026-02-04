mod int128;

pub use int128::*;

use crate::{
    prelude::*,
    traits::{
        FieldValue, Inner, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom,
        View, Visitable,
    },
};
use candid::{CandidType, Int as WrappedInt};
use derive_more::{Add, AddAssign, Display, FromStr, Sub, SubAssign};
use serde::{Deserialize, Serialize};
use std::{
    iter::Sum,
    ops::{Div, DivAssign, Mul, MulAssign},
};

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
    pub fn to_i128(&self) -> Option<i128> {
        let big = &self.0.0;

        i128::try_from(big).ok()
    }

    #[must_use]
    pub fn to_i64(&self) -> Option<i64> {
        let big = &self.0.0;

        i64::try_from(big).ok()
    }

    #[must_use]
    /// Serialize the arbitrary-precision integer to LEB128 bytes.
    pub fn to_leb128(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.0.encode(&mut out).expect("Int LEB128 encode");

        out
    }
}

impl Mul for Int {
    type Output = Self;

    fn mul(self, other: Self) -> Self::Output {
        Self(self.0 * other.0)
    }
}

impl MulAssign for Int {
    fn mul_assign(&mut self, other: Self) {
        self.0 *= other.0;
    }
}

impl Div for Int {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        Self(self.0 / other.0)
    }
}

impl DivAssign for Int {
    fn div_assign(&mut self, other: Self) {
        self.0 /= other.0;
    }
}

impl FieldValue for Int {
    fn to_value(&self) -> Value {
        Value::IntBig(self.clone())
    }
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
