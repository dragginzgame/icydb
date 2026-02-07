mod nat128;

pub use nat128::*;

use crate::{
    traits::{
        AsView, FieldValue, FieldValueKind, Inner, SanitizeAuto, SanitizeCustom, UpdateView,
        ValidateAuto, ValidateCustom, Visitable,
    },
    value::Value,
};
use candid::{CandidType, Nat as WrappedNat};
use derive_more::{Add, AddAssign, Display, FromStr, Sub, SubAssign};
use serde::{Deserialize, Serialize};
use std::{
    iter::Sum,
    ops::{Div, DivAssign, Mul, MulAssign},
};

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
    pub fn to_u128(&self) -> Option<u128> {
        let big = &self.0.0;

        u128::try_from(big).ok()
    }

    #[must_use]
    pub fn to_u64(&self) -> Option<u64> {
        let big = &self.0.0;

        u64::try_from(big).ok()
    }

    #[must_use]
    /// Serialize the arbitrary-precision natural to LEB128 bytes.
    pub fn to_leb128(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.0.encode(&mut out).expect("Nat LEB128 encode");

        out
    }

    /// Saturating addition (unbounded; equivalent to normal addition).
    #[must_use]
    pub fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }

    /// Saturating subtraction; clamps at zero on underflow.
    #[must_use]
    pub fn saturating_sub(self, rhs: Self) -> Self {
        if rhs > self {
            return Self::default();
        }

        Self(self.0 - rhs.0)
    }
}

impl AsView for Nat {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Div for Nat {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        Self(self.0 / other.0)
    }
}

impl DivAssign for Nat {
    fn div_assign(&mut self, other: Self) {
        self.0 /= other.0;
    }
}

impl FieldValue for Nat {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::UintBig(self.clone())
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::UintBig(v) => Some(v.clone()),
            _ => None,
        }
    }
}

impl From<u64> for Nat {
    fn from(n: u64) -> Self {
        Self(WrappedNat::from(n))
    }
}

impl From<WrappedNat> for Nat {
    fn from(n: WrappedNat) -> Self {
        Self(n)
    }
}

impl Mul for Nat {
    type Output = Self;

    fn mul(self, other: Self) -> Self::Output {
        Self(self.0 * other.0)
    }
}

impl MulAssign for Nat {
    fn mul_assign(&mut self, other: Self) {
        self.0 *= other.0;
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

impl TryFrom<i32> for Nat {
    type Error = std::num::TryFromIntError;

    fn try_from(n: i32) -> Result<Self, Self::Error> {
        let v = Self(WrappedNat::from(u32::try_from(n)?));
        Ok(v)
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

impl Visitable for Nat {}
