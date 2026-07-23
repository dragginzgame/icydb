//! Module: types::int_big
//! Defines the signed integer runtime types used by typed values and numeric
//! arithmetic helpers.

use crate::{
    types::{Decimal, NumericValue},
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable},
};
use candid::{CandidType, Int as WrappedInt};
use derive_more::{Add, AddAssign, Sub, SubAssign};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    iter::Sum,
    ops::{Div, DivAssign, Mul, MulAssign},
    str::FromStr,
};

//
// IntBig
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
pub struct IntBig(WrappedInt);

impl IntBig {
    #[must_use]
    pub(crate) const fn from_candid(value: WrappedInt) -> Self {
        Self(value)
    }

    #[must_use]
    pub(crate) fn from_bigint(value: BigInt) -> Self {
        Self::from_candid(WrappedInt::from(value))
    }

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

    /// Serialize this arbitrary-precision integer for internal hash and sort-key framing.
    #[must_use]
    pub(crate) fn to_leb128(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.0.encode(&mut out).expect("Int LEB128 encode");

        out
    }

    /// Return whether this value is zero without narrowing it.
    #[must_use]
    pub(crate) fn is_zero(&self) -> bool {
        self.0.0 == 0.into()
    }

    /// Return exact arbitrary-precision divisibility, rejecting a zero factor.
    #[must_use]
    pub(crate) fn is_multiple_of(&self, factor: &Self) -> Option<bool> {
        if factor.is_zero() {
            return None;
        }
        Some((&self.0.0 % &factor.0.0) == 0.into())
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

impl fmt::Display for IntBig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for IntBig {
    type Err = <WrappedInt as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        WrappedInt::from_str(s).map(Self::from_candid)
    }
}

impl Div for IntBig {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        Self(self.0 / other.0)
    }
}

impl DivAssign for IntBig {
    fn div_assign(&mut self, other: Self) {
        self.0 /= other.0;
    }
}

impl RuntimeValueMeta for IntBig {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for IntBig {
    fn to_value(&self) -> Value {
        Value::IntBig(self.clone())
    }
}

impl RuntimeValueDecode for IntBig {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::IntBig(v) => Some(v.clone()),
            _ => None,
        }
    }
}

impl From<i32> for IntBig {
    fn from(n: i32) -> Self {
        Self::from_candid(WrappedInt::from(n))
    }
}

impl From<i64> for IntBig {
    fn from(n: i64) -> Self {
        Self::from_candid(WrappedInt::from(n))
    }
}

impl Mul for IntBig {
    type Output = Self;

    fn mul(self, other: Self) -> Self::Output {
        Self(self.0 * other.0)
    }
}

impl MulAssign for IntBig {
    fn mul_assign(&mut self, other: Self) {
        self.0 *= other.0;
    }
}

impl NumericValue for IntBig {
    fn try_to_decimal(&self) -> Option<Decimal> {
        self.to_i128().and_then(Decimal::from_i128)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_i128().map(WrappedInt::from).map(Self::from_candid)
    }
}

impl SanitizeAuto for IntBig {}

impl SanitizeCustom for IntBig {}

impl Sum for IntBig {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |acc, x| acc + x)
    }
}

impl ValidateAuto for IntBig {}

impl ValidateCustom for IntBig {}

impl Visitable for IntBig {
    fn requires_application_write_callbacks() -> bool {
        false
    }
}
