//! Module: types::int::int128
//! Defines the fixed-width signed `i128` runtime wrapper used by typed values
//! and numeric arithmetic helpers.

use crate::{
    traits::{
        Atomic, FieldValue, FieldValueKind, NumericValue, SanitizeAuto, SanitizeCustom,
        ValidateAuto, ValidateCustom, Visitable,
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
// Int128
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
pub struct Int128(i128);

impl Int128 {
    #[must_use]
    pub const fn get(self) -> i128 {
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

impl fmt::Display for Int128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Atomic for Int128 {}

impl Div for Int128 {
    type Output = Self;

    fn div(self, other: Self) -> Self::Output {
        Self(self.0 / other.0)
    }
}

impl DivAssign for Int128 {
    fn div_assign(&mut self, other: Self) {
        self.0 /= other.0;
    }
}

impl FieldValue for Int128 {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Int128(*self)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Int128(v) => Some(*v),
            _ => None,
        }
    }
}

#[expect(clippy::cast_lossless)]
impl From<i32> for Int128 {
    fn from(n: i32) -> Self {
        Self(n as i128)
    }
}

impl From<i128> for Int128 {
    fn from(i: i128) -> Self {
        Self(i)
    }
}

impl Mul for Int128 {
    type Output = Self;

    fn mul(self, other: Self) -> Self::Output {
        Self(self.0 * other.0)
    }
}

impl MulAssign for Int128 {
    fn mul_assign(&mut self, other: Self) {
        self.0 *= other.0;
    }
}

impl NumericValue for Int128 {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_i128(self.0)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_i128().map(Self)
    }
}

impl PartialEq<i128> for Int128 {
    fn eq(&self, other: &i128) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Int128> for i128 {
    fn eq(&self, other: &Int128) -> bool {
        *self == other.0
    }
}

impl PartialOrd<i128> for Int128 {
    fn partial_cmp(&self, other: &i128) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}

impl PartialOrd<Int128> for i128 {
    fn partial_cmp(&self, other: &Int128) -> Option<Ordering> {
        self.partial_cmp(&other.0)
    }
}

impl Rem for Int128 {
    type Output = Self;

    fn rem(self, other: Self) -> Self::Output {
        Self(self.0 % other.0)
    }
}

impl SanitizeAuto for Int128 {}

impl SanitizeCustom for Int128 {}

impl<'de> Deserialize<'de> for Int128 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: &[u8] = serde::Deserialize::deserialize(deserializer)?;
        if bytes.len() == 16 {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(bytes);

            Ok(Self(i128::from_be_bytes(arr)))
        } else {
            Err(serde::de::Error::custom("expected 16 bytes"))
        }
    }
}

impl ValidateAuto for Int128 {}

impl ValidateCustom for Int128 {}

impl Visitable for Int128 {}
