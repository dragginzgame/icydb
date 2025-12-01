use crate::{
    db::primitives::{DecimalListFilterKind, DecimalRangeFilterKind},
    traits::{
        FieldValue, Filterable, Inner, NumCast, NumFromPrimitive, NumToPrimitive, SanitizeAuto,
        SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom, View, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use derive_more::{Add, AddAssign, Deref, DerefMut, Display, FromStr, Rem, Sub, SubAssign, Sum};
use rust_decimal::{Decimal as WrappedDecimal, MathematicalOps};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    ops::{Div, Mul},
};

///
/// Decimal
///

#[derive(
    Add,
    AddAssign,
    Clone,
    Copy,
    Debug,
    Default,
    Deref,
    DerefMut,
    Display,
    Eq,
    FromStr,
    PartialEq,
    Sum,
    Hash,
    Ord,
    PartialOrd,
    Rem,
    Sub,
    SubAssign,
)]
pub struct Decimal(WrappedDecimal);

impl Decimal {
    pub const ZERO: Self = Self(WrappedDecimal::ZERO);

    #[must_use]
    pub fn new(num: i64, scale: u32) -> Self {
        Self(WrappedDecimal::new(num, scale))
    }

    // NumCast::from gives a disambiguation
    pub fn from_num<N: NumCast>(n: N) -> Option<Self> {
        <Self as NumCast>::from(n)
    }

    ///
    /// WRAPPED FUNCTIONS
    ///

    #[must_use]
    pub fn round_dp(&self, dp: u32) -> Self {
        Self(self.0.round_dp(dp))
    }

    pub fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.0.checked_rem(*rhs).map(Self)
    }

    // via the MathematicalOps trait
    #[must_use]
    pub fn powu(&self, exp: u64) -> Self {
        Self(self.0.powu(exp))
    }

    #[must_use]
    pub fn from_i128_with_scale(num: i128, scale: u32) -> Self {
        WrappedDecimal::from_i128_with_scale(num, scale).into()
    }

    #[must_use]
    pub fn normalize(&self) -> Self {
        Self(self.0.normalize())
    }
}

impl CandidType for Decimal {
    fn _ty() -> candid::types::Type {
        candid::types::TypeInner::Text.into()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        serializer.serialize_text(&self.0.to_string())
    }
}

// Serde: always serialize as a string, and deserialize from a string.
// This ensures a stable textual representation across formats and avoids
// lossy float conversions.
impl Serialize for Decimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse::<WrappedDecimal>()
            .map(Decimal)
            .map_err(serde::de::Error::custom)
    }
}

impl<D: Into<Self>> Div<D> for Decimal {
    type Output = Self;

    fn div(self, d: D) -> Self::Output {
        let rhs: Self = d.into();
        Self(self.0 / rhs.0)
    }
}

impl FieldValue for Decimal {
    fn to_value(&self) -> Value {
        Value::Decimal(*self)
    }
}

impl Filterable for Decimal {
    type Filter = DecimalRangeFilterKind;
    type ListFilter = DecimalListFilterKind;
}

impl Inner<Self> for Decimal {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
    }
}

impl NumFromPrimitive for Decimal {
    fn from_i64(n: i64) -> Option<Self> {
        Some(WrappedDecimal::from(n).into())
    }

    fn from_u64(n: u64) -> Option<Self> {
        WrappedDecimal::from_u64(n).map(Self)
    }

    fn from_f32(n: f32) -> Option<Self> {
        WrappedDecimal::from_f32(n).map(Into::into)
    }

    fn from_f64(n: f64) -> Option<Self> {
        WrappedDecimal::from_f64(n).map(Into::into)
    }
}

impl From<WrappedDecimal> for Decimal {
    fn from(d: WrappedDecimal) -> Self {
        Self(d)
    }
}

// lossy f32 done on purpose as these ORM floats aren't designed for NaN etc.
impl From<f32> for Decimal {
    fn from(n: f32) -> Self {
        if n.is_finite() {
            WrappedDecimal::from_f32(n).unwrap_or(Self::ZERO.0).into()
        } else {
            Self::ZERO
        }
    }
}

impl From<f64> for Decimal {
    fn from(n: f64) -> Self {
        if n.is_finite() {
            WrappedDecimal::from_f64(n).unwrap_or(Self::ZERO.0).into()
        } else {
            Self::ZERO
        }
    }
}

macro_rules! impl_decimal_from_int {
    ( $( $type:ty ),* ) => {
        $(
            impl From<$type> for Decimal {
                fn from(n: $type) -> Self {
                    Self(rust_decimal::Decimal::from(n))
                }
            }
        )*
    };
}

impl_decimal_from_int!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);

impl<D: Into<Self>> Mul<D> for Decimal {
    type Output = Self;

    fn mul(self, d: D) -> Self::Output {
        let rhs: Self = d.into();
        Self(self.0 * rhs.0)
    }
}

impl NumCast for Decimal {
    fn from<T: NumToPrimitive>(n: T) -> Option<Self> {
        WrappedDecimal::from_f64(n.to_f64()?).map(Decimal)
    }
}

// all of these are needed if you want things to work
impl NumToPrimitive for Decimal {
    fn to_i32(&self) -> Option<i32> {
        self.0.to_i32()
    }

    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }

    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }

    fn to_u128(&self) -> Option<u128> {
        self.0.to_u128()
    }

    fn to_f32(&self) -> Option<f32> {
        self.0.to_f32()
    }

    fn to_f64(&self) -> Option<f64> {
        self.0.to_f64()
    }
}

// ----- Cross-type comparisons between Decimal and WrappedDecimal -----

impl PartialEq<WrappedDecimal> for Decimal {
    fn eq(&self, other: &WrappedDecimal) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Decimal> for WrappedDecimal {
    fn eq(&self, other: &Decimal) -> bool {
        *self == other.0
    }
}

impl PartialOrd<WrappedDecimal> for Decimal {
    fn partial_cmp(&self, other: &WrappedDecimal) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}

impl PartialOrd<Decimal> for WrappedDecimal {
    fn partial_cmp(&self, other: &Decimal) -> Option<Ordering> {
        self.partial_cmp(&other.0)
    }
}

impl SanitizeAuto for Decimal {}

impl SanitizeCustom for Decimal {}

impl UpdateView for Decimal {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Decimal {}

impl ValidateCustom for Decimal {}

// WrappedDecimal does not use CandidType
impl View for Decimal {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self(*view)
    }
}

impl Visitable for Decimal {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use candid::{decode_one, encode_one};
    use std::str::FromStr;

    #[test]
    fn decimal_candid_roundtrip() {
        let cases = [
            "0",
            "1",
            "-1",
            "42.5",
            "1234567890.123456789",
            "0.00000001",
            "1000000000000000000000000.000000000000000000000001",
        ];

        for s in cases {
            let d1 = Decimal::from_str(s).expect("parse decimal");

            // encode via Candid (should encode as text)
            let bytes = encode_one(d1).expect("candid encode");

            // decode back to Decimal
            let d2: Decimal = decode_one(&bytes).expect("candid decode to Decimal");
            assert_eq!(d2, d1, "roundtrip mismatch for {s}");

            // also ensure the on-wire representation is text by decoding as String
            let wire_str: String = decode_one(&bytes).expect("candid decode to String");
            assert_eq!(wire_str, d1.0.to_string(), "wire text mismatch for {s}");
        }
    }

    #[test]
    fn decimal_serde_json_string_roundtrip() {
        let cases = [
            "0",
            "1",
            "-1",
            "42.5",
            "1234567890.123456789",
            "0.00000001",
            "1000000000000000000000000.000000000000000000000001",
        ];

        for s in cases {
            let d = Decimal::from_str(s).expect("parse decimal");

            // Serialize to JSON: must be a JSON string containing the decimal text
            let json = serde_json::to_string(&d).expect("serde_json serialize");
            let expected = serde_json::to_string(&d.0.to_string()).unwrap();
            assert_eq!(json, expected, "JSON encoding should be a string for {s}");

            // Deserialize back and compare
            let back: Decimal = serde_json::from_str(&json).expect("serde_json deserialize");
            assert_eq!(back, d, "serde_json roundtrip mismatch for {s}");
        }
    }
}
