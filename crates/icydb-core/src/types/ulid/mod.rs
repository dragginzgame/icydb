pub(crate) mod fixture;
pub(crate) mod generator;

use crate::{
    traits::{
        AsView, Atomic, EntityKeyBytes, FieldValue, FieldValueKind, SanitizeAuto, SanitizeCustom,
        ValidateAuto, ValidateCustom, Visitable,
    },
    types::GenerateKey,
    value::Value,
    visitor::VisitorContext,
};
use candid::CandidType;
use derive_more::{Deref, DerefMut, Display, FromStr};
use serde::{Deserialize, Serialize, Serializer, de::Deserializer};
use thiserror::Error as ThisError;
use ulid::Ulid as WrappedUlid;

///
/// Error
///

#[derive(Debug, ThisError)]
pub enum UlidError {
    #[error("invalid ulid string")]
    InvalidString,

    #[error("monotonic error - overflow")]
    GeneratorOverflow,
}

///
/// UlidDecodeError
///

#[derive(Debug, ThisError)]
pub enum UlidDecodeError {
    #[error("invalid ulid length: {len} bytes")]
    InvalidSize { len: usize },
}

///
/// Ulid
///

#[derive(
    Clone, Copy, Debug, Deref, DerefMut, Display, Eq, FromStr, Hash, Ord, PartialEq, PartialOrd,
)]
#[repr(transparent)]
pub struct Ulid(WrappedUlid);

impl Ulid {
    pub const STORED_SIZE: u32 = 16;

    pub const MIN: Self = Self::from_bytes([0x00; 16]);
    pub const MAX: Self = Self::from_bytes([0xFF; 16]);

    #[must_use]
    pub const fn nil() -> Self {
        Self(WrappedUlid::nil())
    }

    #[must_use]
    pub const fn from_parts(timestamp_ms: u64, random: u128) -> Self {
        Self(WrappedUlid::from_parts(timestamp_ms, random))
    }

    /// generate
    /// Generate a ULID with the current timestamp and a random value.
    /// Falls back to zeroed randomness if the RNG is unavailable and to nil on overflow.
    #[must_use]
    pub fn generate() -> Self {
        Self::try_generate().unwrap_or_else(|_| Self::nil())
    }

    #[must_use]
    /// Monotonic increment; returns `None` on overflow.
    pub fn increment(&self) -> Option<Self> {
        self.0.increment().map(Self::from)
    }

    /// try_generate
    /// Fallible ULID generation preserving error type (e.g., overflow).
    pub fn try_generate() -> Result<Self, UlidError> {
        generator::generate()
    }

    /// from_bytes
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(WrappedUlid::from_bytes(bytes))
    }

    pub const fn try_from_bytes(bytes: &[u8]) -> Result<Self, UlidDecodeError> {
        if bytes.len() != Self::STORED_SIZE as usize {
            return Err(UlidDecodeError::InvalidSize { len: bytes.len() });
        }

        let mut array = [0u8; 16];
        array.copy_from_slice(bytes);

        Ok(Self::from_bytes(array))
    }

    /// from_str
    #[expect(clippy::should_implement_trait)]
    pub fn from_str(encoded: &str) -> Result<Self, UlidError> {
        let this = WrappedUlid::from_string(encoded).map_err(|_| UlidError::InvalidString)?;

        Ok(Self(this))
    }

    /// from_u128
    #[must_use]
    pub const fn from_u128(n: u128) -> Self {
        Self(WrappedUlid::from_bytes(n.to_be_bytes()))
    }

    #[must_use]
    pub const fn max_storable() -> Self {
        Self::from_bytes([0xFF; 16])
    }
}

impl AsView for Ulid {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Atomic for Ulid {}

impl CandidType for Ulid {
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

impl Default for Ulid {
    fn default() -> Self {
        Self(WrappedUlid::nil())
    }
}

impl EntityKeyBytes for Ulid {
    const BYTE_LEN: usize = Self::STORED_SIZE as usize;

    fn write_bytes(&self, out: &mut [u8]) {
        assert_eq!(out.len(), Self::BYTE_LEN);
        out.copy_from_slice(&self.to_bytes());
    }
}

impl FieldValue for Ulid {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Ulid(*self)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Ulid(v) => Some(*v),
            _ => None,
        }
    }
}

impl From<WrappedUlid> for Ulid {
    fn from(ulid: WrappedUlid) -> Self {
        Self(ulid)
    }
}

impl GenerateKey for Ulid {
    fn generate() -> Self {
        Self::generate()
    }
}

impl PartialEq<WrappedUlid> for Ulid {
    fn eq(&self, other: &WrappedUlid) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Ulid> for WrappedUlid {
    fn eq(&self, other: &Ulid) -> bool {
        *self == other.0
    }
}

impl SanitizeAuto for Ulid {}

impl SanitizeCustom for Ulid {}

// The ulid crate's serde impls are gated behind its `serde` feature.
// With default-features disabled (to avoid pulling in `rand`), we implement
// Serialize/Deserialize here explicitly.
impl Serialize for Ulid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut buffer = [0; ::ulid::ULID_LEN];
        let text = self.array_to_str(&mut buffer);
        text.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Ulid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let deserialized_str = String::deserialize(deserializer)?;
        match WrappedUlid::from_string(&deserialized_str) {
            Ok(u) => Ok(Self(u)),
            Err(_) => Err(serde::de::Error::custom("invalid ulid string")),
        }
    }
}

impl TryFrom<&[u8]> for Ulid {
    type Error = UlidDecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

impl ValidateAuto for Ulid {
    fn validate_self(&self, ctx: &mut dyn VisitorContext) {
        if self.0 == WrappedUlid::nil() {
            ctx.issue("ulid is nil");
        }
    }
}

impl ValidateCustom for Ulid {}

impl Visitable for Ulid {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulid_max_size_is_bounded() {
        let ulid = Ulid::max_storable();
        let size = ulid.to_bytes().len();

        assert!(
            size <= Ulid::STORED_SIZE as usize,
            "serialized Ulid too large: got {size} bytes (limit {})",
            Ulid::STORED_SIZE
        );
    }

    #[test]
    fn test_ulid_string_roundtrip() {
        let u1 = Ulid::generate();
        let u2 = Ulid::from_str(&u1.to_string()).unwrap();

        assert_eq!(u1, u2);
    }

    #[test]
    fn ulid_bytes_roundtrip() {
        let ulid = Ulid::generate();
        let bytes = ulid.to_bytes();
        let decoded = Ulid::from_bytes(bytes);
        assert_eq!(ulid, decoded);
    }
}
