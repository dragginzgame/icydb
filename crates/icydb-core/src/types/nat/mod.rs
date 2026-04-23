//! Module: types::nat
//! Defines the unsigned big-integer runtime types used by typed values and
//! numeric arithmetic helpers.

mod nat128;

use crate::{
    traits::{
        Atomic, NumericValue, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom,
        ValueCodec, ValueSurfaceKind, ValueSurfaceMeta, Visitable,
    },
    types::Decimal,
    value::Value,
};
use candid::{CandidType, Nat as WrappedNat};
use derive_more::{Add, AddAssign, Sub, SubAssign};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    iter::Sum,
    ops::{Div, DivAssign, Mul, MulAssign},
    str::FromStr,
};

pub use nat128::*;

//
// Nat
//

#[derive(
    Add,
    AddAssign,
    CandidType,
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
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
    /// Return base-2^32 limbs for decimal key encoding.
    ///
    /// This allocates for the returned limb vector.
    #[must_use]
    pub(crate) fn u32_digits(&self) -> Vec<u32> {
        self.0.0.to_u32_digits()
    }

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

    /// Serialize the arbitrary-precision natural to LEB128 bytes.
    #[must_use]
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

impl fmt::Display for Nat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for Nat {
    type Err = <WrappedNat as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        WrappedNat::from_str(s).map(Self)
    }
}

impl Atomic for Nat {}

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

impl ValueSurfaceMeta for Nat {
    fn kind() -> ValueSurfaceKind {
        ValueSurfaceKind::Atomic
    }
}

impl ValueCodec for Nat {
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

impl NumericValue for Nat {
    fn try_to_decimal(&self) -> Option<Decimal> {
        self.to_u128().and_then(Decimal::from_u128)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_u128().map(WrappedNat::from).map(Self)
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

impl ValidateAuto for Nat {}

impl ValidateCustom for Nat {}

impl Visitable for Nat {}
