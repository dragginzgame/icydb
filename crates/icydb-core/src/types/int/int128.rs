use crate::{
    Value,
    db::primitives::{IntListFilterKind, IntRangeFilterKind},
    traits::{
        FieldValue, Filterable, Inner, NumCast, NumFromPrimitive, NumToPrimitive, SanitizeAuto,
        SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom, View, Visitable,
    },
};
use candid::CandidType;
use derive_more::{Add, AddAssign, Deref, DerefMut, Display, FromStr, Sub, SubAssign, Sum};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

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
pub struct Int128(i128);

impl Int128 {
    #[must_use]
    pub const fn get(self) -> i128 {
        self.0
    }
}

impl FieldValue for Int128 {
    fn to_value(&self) -> Value {
        Value::Int128(*self)
    }
}

impl Filterable for Int128 {
    type Filter = IntRangeFilterKind;
    type ListFilter = IntListFilterKind;
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

impl Inner<Self> for Int128 {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
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

impl UpdateView for Int128 {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Int128 {}

impl ValidateCustom for Int128 {}

impl View for Int128 {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Visitable for Int128 {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{deserialize, serialize};

    fn roundtrip(v: i128) {
        let int128: Int128 = v.into();

        // serialize
        let bytes = serialize(&int128).expect("serialize failed");

        // must be length-prefixed
        // so length = 16 + 1/2 bytes overhead, but we just check round-trip.
        let decoded: Int128 = deserialize(&bytes).expect("deserialize failed");

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
        let bytes = serialize(&v).unwrap();
        let encoded_inner = &bytes[bytes.len() - 16..];
        assert_eq!(encoded_inner, &42i128.to_be_bytes());
    }
}
