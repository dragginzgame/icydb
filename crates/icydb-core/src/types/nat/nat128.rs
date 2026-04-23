//! Module: types::nat::nat128
//! Defines the fixed-width unsigned `u128` runtime wrapper used by typed values
//! and numeric arithmetic helpers.

use crate::{
    traits::{
        Atomic, NumericValue, RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind,
        RuntimeValueMeta, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
    types::Decimal,
    value::Value,
};
use candid::CandidType;
use derive_more::{Add, AddAssign, Sub, SubAssign, Sum};
use serde::Deserialize;
use std::{
    cmp::Ordering,
    fmt,
    ops::{Div, DivAssign, Mul, MulAssign, Rem},
};

//
// Nat128
//

#[derive(
    Add,
    AddAssign,
    CandidType,
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Sub,
    SubAssign,
    Sum,
)]
pub struct Nat128(u128);

impl Nat128 {
    #[must_use]
    pub const fn get(self) -> u128 {
        self.0
    }

    /// Saturating addition.
    #[must_use]
    pub const fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }

    /// Saturating subtraction.
    #[must_use]
    pub const fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }
}

impl fmt::Display for Nat128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Atomic for Nat128 {}

impl Div for Nat128 {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        Self(self.0 / other.0)
    }
}

impl DivAssign for Nat128 {
    fn div_assign(&mut self, other: Self) {
        self.0 /= other.0;
    }
}

impl RuntimeValueMeta for Nat128 {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Nat128 {
    fn to_value(&self) -> Value {
        Value::Uint128(*self)
    }
}

impl RuntimeValueDecode for Nat128 {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Uint128(v) => Some(*v),
            _ => None,
        }
    }
}

impl From<u128> for Nat128 {
    fn from(u: u128) -> Self {
        Self(u)
    }
}

impl Mul for Nat128 {
    type Output = Self;

    fn mul(self, other: Self) -> Self::Output {
        Self(self.0 * other.0)
    }
}

impl MulAssign for Nat128 {
    fn mul_assign(&mut self, other: Self) {
        self.0 *= other.0;
    }
}

impl NumericValue for Nat128 {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_u128(self.0)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_u128().map(Self)
    }
}

impl PartialEq<u128> for Nat128 {
    fn eq(&self, other: &u128) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Nat128> for u128 {
    fn eq(&self, other: &Nat128) -> bool {
        *self == other.0
    }
}

impl PartialOrd<u128> for Nat128 {
    fn partial_cmp(&self, other: &u128) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}

impl PartialOrd<Nat128> for u128 {
    fn partial_cmp(&self, other: &Nat128) -> Option<Ordering> {
        self.partial_cmp(&other.0)
    }
}

impl Rem for Nat128 {
    type Output = Self;

    fn rem(self, other: Self) -> Self::Output {
        Self(self.0 % other.0)
    }
}

impl SanitizeAuto for Nat128 {}

impl SanitizeCustom for Nat128 {}

impl<'de> Deserialize<'de> for Nat128 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: &[u8] = serde::Deserialize::deserialize(deserializer)?;
        if bytes.len() == 16 {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);

            Ok(Self(u128::from_be_bytes(arr)))
        } else {
            Err(serde::de::Error::custom("expected 16 bytes"))
        }
    }
}

impl ValidateAuto for Nat128 {}

impl ValidateCustom for Nat128 {}

impl Visitable for Nat128 {}
