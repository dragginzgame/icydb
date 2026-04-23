//! Module: types::int
//! Defines the signed integer runtime types used by typed values and numeric
//! arithmetic helpers.

mod int128;

use crate::{
    traits::{
        Atomic, NumericValue, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom,
        ValueSurfaceDecode, ValueSurfaceEncode, ValueSurfaceKind, ValueSurfaceMeta, Visitable,
    },
    types::Decimal,
    value::Value,
};
use candid::{CandidType, Int as WrappedInt};
use derive_more::{Add, AddAssign, Sub, SubAssign};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    iter::Sum,
    ops::{Div, DivAssign, Mul, MulAssign},
    str::FromStr,
};

pub use int128::*;

//
// Int
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
pub struct Int(WrappedInt);

impl Int {
    /// Return sign and base-2^32 magnitude limbs for decimal key encoding.
    ///
    /// This allocates for the returned limb vector.
    #[must_use]
    pub(crate) fn sign_and_u32_digits(&self) -> (bool, Vec<u32>) {
        (
            self.0.0.cmp(&0.into()).is_lt(),
            self.0.0.magnitude().to_u32_digits(),
        )
    }

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

    /// Serialize the arbitrary-precision integer to LEB128 bytes.
    #[must_use]
    pub fn to_leb128(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.0.encode(&mut out).expect("Int LEB128 encode");

        out
    }

    /// Saturating addition (unbounded; equivalent to normal addition).
    #[must_use]
    pub fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }

    /// Saturating subtraction (unbounded; equivalent to normal subtraction).
    #[must_use]
    pub fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0 - rhs.0)
    }
}

impl fmt::Display for Int {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for Int {
    type Err = <WrappedInt as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        WrappedInt::from_str(s).map(Self)
    }
}

impl Atomic for Int {}

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

impl ValueSurfaceMeta for Int {
    fn kind() -> ValueSurfaceKind {
        ValueSurfaceKind::Atomic
    }
}

impl ValueSurfaceEncode for Int {
    fn to_value(&self) -> Value {
        Value::IntBig(self.clone())
    }
}

impl ValueSurfaceDecode for Int {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::IntBig(v) => Some(v.clone()),
            _ => None,
        }
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

impl NumericValue for Int {
    fn try_to_decimal(&self) -> Option<Decimal> {
        self.to_i128().and_then(Decimal::from_i128)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_i128().map(WrappedInt::from).map(Self)
    }
}

impl SanitizeAuto for Int {}

impl SanitizeCustom for Int {}

impl Sum for Int {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |acc, x| acc + x)
    }
}

impl ValidateAuto for Int {}

impl ValidateCustom for Int {}

impl Visitable for Int {}
