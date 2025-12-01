use crate::{
    db::primitives::{NatListFilterKind, NatRangeFilterKind},
    traits::{
        FieldValue, Filterable, Inner, NumCast, NumToPrimitive, SanitizeAuto, SanitizeCustom,
        UpdateView, ValidateAuto, ValidateCustom, View, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use derive_more::{Add, AddAssign, Deref, DerefMut, Display, FromStr, Sub, SubAssign, Sum};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

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
    Deref,
    DerefMut,
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
}

impl FieldValue for Nat128 {
    fn to_value(&self) -> Value {
        Value::Uint128(*self)
    }
}

impl Filterable for Nat128 {
    type Filter = NatRangeFilterKind;
    type ListFilter = NatListFilterKind;
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

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Nat128 {}

impl ValidateCustom for Nat128 {}

impl View for Nat128 {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Visitable for Nat128 {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{deserialize, serialize};

    fn roundtrip(v: u128) {
        let nat128: Nat128 = v.into();

        // serialize
        let bytes = serialize(&nat128).expect("serialize failed");

        // must be length-prefixed
        // so length = 16 + 1/2 bytes overhead, but we just check round-trip.
        let decoded: Nat128 = deserialize(&bytes).expect("deserialize failed");

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
        let bytes = serialize(&v).unwrap();
        let encoded_inner = &bytes[bytes.len() - 16..];
        assert_eq!(encoded_inner, &42i128.to_be_bytes());
    }
}
