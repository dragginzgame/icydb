//! Module: types::nat_big
//! Defines the unsigned big-integer runtime types used by typed values and
//! numeric arithmetic helpers.

use crate::{
    types::{Decimal, NumericValue},
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable},
};
use candid::{CandidType, Nat as WrappedNat};
use derive_more::{Add, AddAssign, Sub, SubAssign};
use num_bigint::BigUint;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    iter::Sum,
    ops::{Div, DivAssign, Mul, MulAssign},
    str::FromStr,
};

//
// NatBig
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
pub struct NatBig(WrappedNat);

impl NatBig {
    #[must_use]
    pub(crate) const fn from_candid(value: WrappedNat) -> Self {
        Self(value)
    }

    #[must_use]
    pub(crate) fn from_biguint(value: BigUint) -> Self {
        Self::from_candid(WrappedNat::from(value))
    }

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

    /// Serialize this arbitrary-precision natural for internal hash and sort-key framing.
    #[must_use]
    pub(crate) fn to_leb128(&self) -> Vec<u8> {
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

impl fmt::Display for NatBig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for NatBig {
    type Err = <WrappedNat as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        WrappedNat::from_str(s).map(Self::from_candid)
    }
}

impl Div for NatBig {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        Self(self.0 / other.0)
    }
}

impl DivAssign for NatBig {
    fn div_assign(&mut self, other: Self) {
        self.0 /= other.0;
    }
}

impl RuntimeValueMeta for NatBig {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for NatBig {
    fn to_value(&self) -> Value {
        Value::NatBig(self.clone())
    }
}

impl RuntimeValueDecode for NatBig {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::NatBig(v) => Some(v.clone()),
            _ => None,
        }
    }
}

impl From<u64> for NatBig {
    fn from(n: u64) -> Self {
        Self::from_candid(WrappedNat::from(n))
    }
}

impl From<u32> for NatBig {
    fn from(n: u32) -> Self {
        Self::from_candid(WrappedNat::from(n))
    }
}

impl Mul for NatBig {
    type Output = Self;

    fn mul(self, other: Self) -> Self::Output {
        Self(self.0 * other.0)
    }
}

impl MulAssign for NatBig {
    fn mul_assign(&mut self, other: Self) {
        self.0 *= other.0;
    }
}

impl NumericValue for NatBig {
    fn try_to_decimal(&self) -> Option<Decimal> {
        self.to_u128().and_then(Decimal::from_u128)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_u128().map(WrappedNat::from).map(Self::from_candid)
    }
}

impl SanitizeAuto for NatBig {}

impl SanitizeCustom for NatBig {}

impl Sum for NatBig {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |acc, x| acc + x)
    }
}

impl TryFrom<i32> for NatBig {
    type Error = std::num::TryFromIntError;

    fn try_from(n: i32) -> Result<Self, Self::Error> {
        let v = Self::from_candid(WrappedNat::from(u32::try_from(n)?));
        Ok(v)
    }
}

impl ValidateAuto for NatBig {}

impl ValidateCustom for NatBig {}

impl Visitable for NatBig {}
