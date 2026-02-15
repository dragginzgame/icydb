use crate::{
    traits::{
        AsView, Atomic, FieldValue, FieldValueKind, NumCast, NumFromPrimitive, NumToPrimitive,
        SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use derive_more::{Add, AddAssign, Display, FromStr, Sub, SubAssign, Sum};
use rust_decimal::{Decimal as WrappedDecimal, MathematicalOps};
use serde::{Deserialize, Serialize};
use serde_bytes::{ByteBuf, Bytes};
use std::{
    cmp::Ordering,
    ops::{Div, DivAssign, Mul, MulAssign, Rem},
};

///
/// DecimalParts
///
/// Canonical decomposition of a Decimal.
///
/// Invariant:
/// - value == mantissa * 10^-scale
/// - mantissa carries the sign
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecimalParts {
    pub mantissa: i128,
    pub scale: u32,
}

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
    Display,
    Eq,
    FromStr,
    PartialEq,
    Sum,
    Hash,
    Ord,
    PartialOrd,
    Sub,
    SubAssign,
)]
pub struct Decimal(WrappedDecimal);

impl Decimal {
    pub const ZERO: Self = Self(WrappedDecimal::ZERO);

    #[must_use]
    /// Construct a decimal from mantissa and scale.
    pub fn new(num: i64, scale: u32) -> Self {
        Self(WrappedDecimal::new(num, scale))
    }

    /// Fallible conversion from common numeric types.
    pub fn from_num<N: NumCast>(n: N) -> Option<Self> {
        <Self as NumCast>::from(n)
    }

    ///
    /// PARTS
    ///

    /// Decompose into mantissa and scale.
    #[must_use]
    pub const fn parts(&self) -> DecimalParts {
        DecimalParts {
            mantissa: self.0.mantissa(),
            scale: self.0.scale(),
        }
    }

    /// Returns true if the decimal has no fractional component.
    #[must_use]
    pub const fn is_integer(&self) -> bool {
        self.0.scale() == 0
    }

    /// Scale by 10^target_scale and require an integer result.
    ///
    /// Returns `None` if:
    /// - fractional precision would be lost
    /// - integer overflow occurs
    #[must_use]
    pub fn scale_to_integer(&self, target_scale: u32) -> Option<i128> {
        let parts = self.parts();

        if parts.scale > target_scale {
            return None; // fractional remainder
        }

        let factor = 10i128.checked_pow(target_scale - parts.scale)?;
        parts.mantissa.checked_mul(factor)
    }

    ///
    /// WRAPPED FUNCTIONS
    ///

    #[must_use]
    /// Round to a given number of decimal places.
    pub fn round_dp(&self, dp: u32) -> Self {
        Self(self.0.round_dp(dp))
    }

    #[must_use]
    /// Return the absolute value of the decimal.
    pub fn abs(&self) -> Self {
        Self(self.0.abs())
    }

    /// Saturating addition.
    #[must_use]
    pub fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }

    /// Saturating subtraction.
    #[must_use]
    pub fn saturating_sub(self, rhs: Self) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }

    /// Checked remainder; returns `None` on division by zero.
    pub fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.0.checked_rem(rhs.0).map(Self)
    }

    #[must_use]
    /// Integer exponentiation.
    pub fn powu(&self, exp: u64) -> Self {
        Self(self.0.powu(exp))
    }

    #[must_use]
    /// Build from a raw mantissa and scale.
    pub fn from_i128_with_scale(num: i128, scale: u32) -> Self {
        WrappedDecimal::from_i128_with_scale(num, scale).into()
    }

    #[must_use]
    /// Normalize trailing zeros.
    pub fn normalize(&self) -> Self {
        Self(self.0.normalize())
    }

    /// Returns `true` if the value is negative.
    #[must_use]
    pub const fn is_sign_negative(&self) -> bool {
        self.0.is_sign_negative()
    }

    /// Returns the number of fractional decimal places.
    #[must_use]
    pub const fn scale(&self) -> u32 {
        self.0.scale()
    }

    /// Returns the mantissa component.
    #[must_use]
    pub const fn mantissa(&self) -> i128 {
        self.0.mantissa()
    }

    /// Returns `true` if the value is zero.
    #[must_use]
    pub const fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl AsView for Decimal {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Atomic for Decimal {}

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

// Serde:
// - Human-readable formats (e.g. JSON) use a decimal string for API ergonomics.
// - Non-human-readable formats (e.g. CBOR persistence) use compact binary parts.
impl Serialize for Decimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            return serializer.serialize_str(&self.0.to_string());
        }

        let mantissa_bytes = self.mantissa().to_be_bytes();
        (Bytes::new(&mantissa_bytes), self.scale()).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum DecimalPayload {
            Binary((ByteBuf, u32)),
            Text(String),
        }

        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            return s
                .parse::<WrappedDecimal>()
                .map(Decimal)
                .map_err(serde::de::Error::custom);
        }

        // Candid currently reports non-human-readable, but Decimal's Candid wire type is `text`.
        // Accept both payloads here so Candid decode remains correct while binary formats
        // continue to use the canonical `(mantissa_bytes, scale)` shape.
        let payload: DecimalPayload = Deserialize::deserialize(deserializer)?;
        let (mantissa_bytes, scale) = match payload {
            DecimalPayload::Binary(parts) => parts,
            DecimalPayload::Text(s) => {
                return s
                    .parse::<WrappedDecimal>()
                    .map(Decimal)
                    .map_err(serde::de::Error::custom);
            }
        };

        if mantissa_bytes.len() != 16 {
            return Err(serde::de::Error::custom(format!(
                "invalid decimal mantissa length: {} bytes (expected 16)",
                mantissa_bytes.len()
            )));
        }

        let mut mantissa_buf = [0u8; 16];
        mantissa_buf.copy_from_slice(mantissa_bytes.as_ref());
        let mantissa = i128::from_be_bytes(mantissa_buf);

        WrappedDecimal::try_from_i128_with_scale(mantissa, scale)
            .map(Decimal)
            .map_err(|err| {
                serde::de::Error::custom(format!("invalid decimal binary payload: {err}"))
            })
    }
}

impl FieldValue for Decimal {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Decimal(*self)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Decimal(v) => Some(*v),
            _ => None,
        }
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

impl<D: Into<Self>> MulAssign<D> for Decimal {
    fn mul_assign(&mut self, d: D) {
        let rhs: Self = d.into();
        self.0 *= rhs.0;
    }
}

impl<D: Into<Self>> Div<D> for Decimal {
    type Output = Self;

    fn div(self, d: D) -> Self::Output {
        let rhs: Self = d.into();
        Self(self.0 / rhs.0)
    }
}

impl<D: Into<Self>> DivAssign<D> for Decimal {
    fn div_assign(&mut self, d: D) {
        let rhs: Self = d.into();
        self.0 /= rhs.0;
    }
}

impl<D: Into<Self>> Rem<D> for Decimal {
    type Output = Self;

    fn rem(self, d: D) -> Self::Output {
        let rhs: Self = d.into();
        Self(self.0 % rhs.0)
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

impl ValidateAuto for Decimal {}

impl ValidateCustom for Decimal {}

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

    #[test]
    fn decimal_serde_cbor_binary_roundtrip() {
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

            let bytes = serde_cbor::to_vec(&d1).expect("cbor serialize");
            let d2: Decimal = serde_cbor::from_slice(&bytes).expect("cbor deserialize");
            assert_eq!(d2, d1, "cbor roundtrip mismatch for {s}");

            let wire: serde_cbor::Value =
                serde_cbor::from_slice(&bytes).expect("decode cbor value");
            match wire {
                serde_cbor::Value::Array(values) => {
                    assert_eq!(values.len(), 2, "expected [mantissa_bytes, scale] for {s}");
                    assert!(
                        matches!(values.first(), Some(serde_cbor::Value::Bytes(_))),
                        "expected mantissa bytes in first position for {s}"
                    );
                }
                other => panic!("expected binary decimal array payload for {s}; got {other:?}"),
            }
        }
    }

    #[test]
    fn decimal_serde_cbor_rejects_invalid_binary_mantissa_length() {
        let invalid = serde_cbor::to_vec(&(vec![1u8, 2, 3], 2u32))
            .expect("serialize invalid binary mantissa payload");
        let parsed: Result<Decimal, _> = serde_cbor::from_slice(&invalid);
        assert!(
            parsed.is_err(),
            "invalid binary mantissa length must be rejected"
        );
    }
}
