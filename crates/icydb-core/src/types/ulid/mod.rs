//! Module: types::ulid
//! Defines the ULID runtime type used by typed keys and persistence encoding.

mod generator;
#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::types::random;

use crate::{
    db::{EntityKeyBytes, EntityKeyBytesError, validate_entity_key_bytes_buffer},
    types::{GenerateKey, TypeParseError},
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{
        SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable, VisitorContext,
    },
};
use candid::CandidType;
use serde::{Deserialize, de::Deserializer};
use std::{fmt, str::FromStr};
use ulid::Ulid as WrappedUlid;

/// Error returned when parsing a ULID from text fails.
#[derive(Debug)]
pub enum UlidParseError {
    /// The input string is not a canonical ULID.
    InvalidString,
}

//
// UlidDecodeError
//

#[derive(Debug)]
pub enum UlidDecodeError {
    InvalidSize { len: usize },
}

//
// Ulid
//

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
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
    const fn from_timestamp_and_randomness(timestamp_ms: u64, randomness: u128) -> Self {
        Self(WrappedUlid::from_parts(timestamp_ms, randomness))
    }

    /// Generate a ULID with the current timestamp and a random value.
    ///
    /// # Panics
    ///
    /// Panics if randomness is unavailable or monotonic generation overflows.
    #[must_use]
    pub fn generate() -> Self {
        Self::try_generate().expect(
            "ULID generation requires initialized randomness and non-overflowing monotonic state",
        )
    }

    /// Monotonic increment; returns `None` on overflow.
    #[must_use]
    fn increment(&self) -> Option<Self> {
        self.0.increment().ok().map(Self)
    }

    /// try_generate
    /// Fallible ULID generation preserving error type (e.g., overflow).
    fn try_generate() -> Result<Self, generator::UlidGenerationError> {
        #[cfg(test)]
        random::seed_if_uninitialized_for_tests([0x55; 32]);

        generator::generate()
    }

    /// from_bytes
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(WrappedUlid::from_bytes(bytes))
    }

    /// Return the canonical 16-byte ULID payload.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0.to_bytes()
    }

    #[must_use]
    const fn timestamp_ms(self) -> u64 {
        self.0.timestamp_ms()
    }

    pub const fn try_from_bytes(bytes: &[u8]) -> Result<Self, UlidDecodeError> {
        if bytes.len() != Self::STORED_SIZE as usize {
            return Err(UlidDecodeError::InvalidSize { len: bytes.len() });
        }

        let mut array = [0u8; 16];
        array.copy_from_slice(bytes);

        Ok(Self::from_bytes(array))
    }

    /// from_u128
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn from_u128(n: u128) -> Self {
        Self(WrappedUlid::from_bytes(n.to_be_bytes()))
    }
}

impl fmt::Display for Ulid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Debug for Ulid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.to_string(), f)
    }
}

impl FromStr for Ulid {
    type Err = UlidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        WrappedUlid::from_string(s)
            .map(Self)
            .map_err(|_| UlidParseError::InvalidString)
    }
}

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

impl EntityKeyBytes for Ulid {
    const BYTE_LEN: usize = Self::STORED_SIZE as usize;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
        validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
        out.copy_from_slice(&self.to_bytes());

        Ok(())
    }
}

impl RuntimeValueMeta for Ulid {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Ulid {
    fn to_value(&self) -> Value {
        Value::Ulid(*self)
    }
}

impl RuntimeValueDecode for Ulid {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Ulid(v) => Some(*v),
            _ => None,
        }
    }
}

impl GenerateKey for Ulid {
    fn generate() -> Self {
        Self::generate()
    }
}

impl SanitizeAuto for Ulid {}

impl SanitizeCustom for Ulid {}

impl<'de> Deserialize<'de> for Ulid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let deserialized_str = String::deserialize(deserializer)?;
        match WrappedUlid::from_string(&deserialized_str) {
            Ok(u) => Ok(Self(u)),
            Err(_) => Err(serde::de::Error::custom(TypeParseError::InvalidUlid)),
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
            ctx.issue("ulid must not be nil");
        }
    }
}

impl ValidateCustom for Ulid {}

impl Visitable for Ulid {}
