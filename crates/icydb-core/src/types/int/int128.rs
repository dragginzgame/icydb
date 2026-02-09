use crate::{
    prelude::*,
    traits::{
        AsView, Atomic, FieldValue, FieldValueKind, NumCast, NumFromPrimitive, NumToPrimitive,
        SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
};
use candid::CandidType;
use derive_more::{Add, AddAssign, Display, FromStr, Sub, SubAssign, Sum};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    ops::{Div, DivAssign, Mul, MulAssign, Rem},
};

///
/// Int128
///

#[derive(
    Add,
    AddAssign,
    CandidType,
    Clone,
    Copy,
    Debug,
    Default,
    Display,
    Eq,
    PartialEq,
    FromStr,
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

impl AsView for Int128 {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
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

#[allow(clippy::cast_lossless)]
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

impl NumCast for Int128 {
    fn from<T: NumToPrimitive>(i: T) -> Option<Self> {
        i.to_i128().map(Self)
    }
}

#[allow(clippy::cast_lossless)]
impl NumFromPrimitive for Int128 {
    fn from_i64(n: i64) -> Option<Self> {
        Some(Self(n as i128))
    }

    fn from_u64(n: u64) -> Option<Self> {
        Some(Self(n as i128))
    }
}

impl NumToPrimitive for Int128 {
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

impl Serialize for Int128 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0.to_be_bytes())
    }
}

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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: i128) {
        let int128: Int128 = v.into();

        // serialize
        let bytes = crate::serialize::serialize(&int128).expect("serialize failed");

        // must be length-prefixed
        // so length = 16 + 1/2 bytes overhead, but we just check round-trip.
        let decoded: Int128 = crate::serialize::deserialize(&bytes).expect("deserialize failed");

        assert_eq!(decoded, int128, "roundtrip failed for {v}");

        // sanity check on raw serialization: inner payload must be 16 bytes
        let raw = int128.0.to_be_bytes();
        let encoded_inner = &bytes[bytes.len() - 16..];
        assert_eq!(encoded_inner, &raw, "encoded inner bytes mismatch");
    }

    #[test]
    fn test_roundtrip_basic() {
        roundtrip(0);
        roundtrip(1);
        roundtrip(-1);
        roundtrip(1_234_567_890_123_456_789);
        roundtrip(-1_234_567_890_123_456_789);
    }

    #[test]
    fn test_roundtrip_edges() {
        roundtrip(i128::MIN);
        roundtrip(i128::MAX);
    }

    #[test]
    fn test_manual_encoding() {
        let v: Int128 = 42.into();
        let bytes = crate::serialize::serialize(&v).unwrap();
        let encoded_inner = &bytes[bytes.len() - 16..];
        assert_eq!(encoded_inner, &42i128.to_be_bytes());
    }
}
