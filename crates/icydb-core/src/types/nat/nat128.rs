use crate::{
    traits::{
        AsView, FieldValue, FieldValueKind, Inner, NumCast, NumToPrimitive, SanitizeAuto,
        SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use derive_more::{Add, AddAssign, Display, FromStr, Sub, SubAssign, Sum};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    ops::{Div, DivAssign, Mul, MulAssign, Rem},
};

///
/// Nat128
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

impl AsView for Nat128 {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

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

impl FieldValue for Nat128 {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Uint128(*self)
    }

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

impl Inner<Self> for Nat128 {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
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

impl NumCast for Nat128 {
    fn from<T: NumToPrimitive>(n: T) -> Option<Self> {
        n.to_u128().map(Self)
    }
}

impl NumToPrimitive for Nat128 {
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

impl Serialize for Nat128 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0.to_be_bytes())
    }
}

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

impl UpdateView for Nat128 {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) -> Result<(), crate::traits::Error> {
        *self = v;

        Ok(())
    }
}

impl ValidateAuto for Nat128 {}

impl ValidateCustom for Nat128 {}

impl Visitable for Nat128 {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: u128) {
        let nat128: Nat128 = v.into();

        // serialize
        let bytes = crate::serialize::serialize(&nat128).expect("serialize failed");

        // must be length-prefixed
        // so length = 16 + 1/2 bytes overhead, but we just check round-trip.
        let decoded: Nat128 = crate::serialize::deserialize(&bytes).expect("deserialize failed");

        assert_eq!(decoded, nat128, "roundtrip failed for {v}");

        // sanity check on raw serialization: inner payload must be 16 bytes
        let raw = nat128.0.to_be_bytes();
        let encoded_inner = &bytes[bytes.len() - 16..];
        assert_eq!(encoded_inner, &raw, "encoded inner bytes mismatch");
    }

    #[test]
    fn test_roundtrip_basic() {
        roundtrip(1);
        roundtrip(1_234_567_890_123_456_789);
    }

    #[test]
    fn test_roundtrip_edges() {
        roundtrip(u128::MIN);
        roundtrip(u128::MAX);
    }

    #[test]
    fn test_manual_encoding() {
        let v: Nat128 = 42.into();
        let bytes = crate::serialize::serialize(&v).unwrap();
        let encoded_inner = &bytes[bytes.len() - 16..];
        assert_eq!(encoded_inner, &42i128.to_be_bytes());
    }
}
