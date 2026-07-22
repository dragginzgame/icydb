//! Module: types::principal
//! Defines the principal runtime wrapper used by typed values, persistence
//! encoding, and identity-bearing API surfaces.

use crate::{
    db::{EntityKeyBytes, EntityKeyBytesError, validate_entity_key_bytes_buffer},
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable},
};
use candid::{CandidType, Principal as WrappedPrincipal};
use serde::Deserialize;
use std::{fmt, str::FromStr};

//
// PrincipalError
//

#[derive(Debug)]
pub enum PrincipalError {
    InvalidText,
}

//
// PrincipalDecodeError
//
// Errors returned when decoding a principal from bytes.
//

#[derive(Debug)]
pub enum PrincipalDecodeError {
    TooLarge { len: usize },
}

//
// PrincipalEncodeError
//
// Error returned when encoding a principal for persistence.
//

#[derive(Debug)]
pub enum PrincipalEncodeError {
    TooLarge { len: usize, max: usize },
}

//
// Principal
//

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Principal(WrappedPrincipal);

impl Principal {
    pub const MAX_LENGTH_IN_BYTES: u32 = 29;

    pub const MIN: Self = Self::from_slice(&[0x00; 29]);
    pub const MAX: Self = Self::from_slice(&[0xFF; 29]);

    #[must_use]
    pub const fn anonymous() -> Self {
        Self(WrappedPrincipal::anonymous())
    }

    pub fn from_text(text: &str) -> Result<Self, PrincipalError> {
        let inner = WrappedPrincipal::from_text(text).map_err(|_| PrincipalError::InvalidText)?;

        Ok(Self(inner))
    }

    #[must_use]
    pub const fn from_slice(slice: &[u8]) -> Self {
        Self(WrappedPrincipal::from_slice(slice))
    }

    #[must_use]
    // This cannot be const across the supported `candid` dependency range:
    // older compatible `ic_principal` releases expose non-const byte access.
    #[allow(clippy::missing_const_for_fn)]
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Borrow this principal's validated stored bytes without allocating.
    pub fn stored_bytes(&self) -> Result<&[u8], PrincipalEncodeError> {
        let bytes = self.as_slice();
        let len = bytes.len();
        if len > Self::MAX_LENGTH_IN_BYTES as usize {
            return Err(PrincipalEncodeError::TooLarge {
                len,
                max: Self::MAX_LENGTH_IN_BYTES as usize,
            });
        }

        Ok(bytes)
    }

    /// Encode this principal into bytes, enforcing the max-length invariant.
    pub fn to_bytes(self) -> Result<Vec<u8>, PrincipalEncodeError> {
        Ok(self.stored_bytes()?.to_vec())
    }

    pub const fn try_from_bytes(bytes: &[u8]) -> Result<Self, PrincipalDecodeError> {
        if bytes.len() > Self::MAX_LENGTH_IN_BYTES as usize {
            return Err(PrincipalDecodeError::TooLarge { len: bytes.len() });
        }

        Ok(Self::from_slice(bytes))
    }
}

impl fmt::Display for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl EntityKeyBytes for Principal {
    const BYTE_LEN: usize = 1 + Self::MAX_LENGTH_IN_BYTES as usize;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
        validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
        out.fill(0);

        let principal =
            self.stored_bytes()
                .map_err(|PrincipalEncodeError::TooLarge { len, max }| {
                    EntityKeyBytesError::ValueTooLong { len, max }
                })?;
        let len = principal.len();
        let (tag, payload) = out
            .split_first_mut()
            .ok_or(EntityKeyBytesError::BufferLength {
                expected: Self::BYTE_LEN,
                actual: 0,
            })?;
        *tag = u8::try_from(len).map_err(|_| EntityKeyBytesError::ValueTooLong {
            len,
            max: usize::from(u8::MAX),
        })?;
        let max = payload.len();
        let payload = payload
            .get_mut(..len)
            .ok_or(EntityKeyBytesError::ValueTooLong { len, max })?;
        payload.copy_from_slice(principal);

        Ok(())
    }
}

impl RuntimeValueMeta for Principal {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Principal {
    fn to_value(&self) -> Value {
        Value::Principal(*self)
    }
}

impl RuntimeValueDecode for Principal {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Principal(v) => Some(*v),
            _ => None,
        }
    }
}

impl RuntimeValueMeta for WrappedPrincipal {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for WrappedPrincipal {
    fn to_value(&self) -> Value {
        Value::Principal((*self).into())
    }
}

impl RuntimeValueDecode for WrappedPrincipal {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Principal(v) => Some((*v).into()),
            _ => None,
        }
    }
}

impl From<WrappedPrincipal> for Principal {
    fn from(p: WrappedPrincipal) -> Self {
        Self(p)
    }
}

impl From<Principal> for WrappedPrincipal {
    fn from(p: Principal) -> Self {
        p.0
    }
}

impl FromStr for Principal {
    type Err = PrincipalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let this = WrappedPrincipal::from_str(s)
            .map(Self)
            .map_err(|_| PrincipalError::InvalidText)?;

        Ok(this)
    }
}

impl SanitizeAuto for Principal {}

impl SanitizeCustom for Principal {}

impl TryFrom<&[u8]> for Principal {
    type Error = PrincipalDecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

impl ValidateAuto for Principal {}

impl ValidateCustom for Principal {}

impl Visitable for Principal {
    fn requires_application_write_callbacks() -> bool {
        false
    }
}

//
// TESTS
//

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_max_size_is_bounded() {
        let principal = Principal::MAX;
        let size = principal.0.as_slice().len();

        assert!(
            size <= Principal::MAX_LENGTH_IN_BYTES as usize,
            "serialized Principal too large: got {size} bytes (limit {})",
            Principal::MAX_LENGTH_IN_BYTES
        );
    }

    #[test]
    fn principal_storable_roundtrip() {
        let inputs = vec![
            Principal::anonymous(),
            Principal::from_slice(&[1, 2, 3, 4]),
            Principal::from_slice(&[0xFF; 29]),
        ];

        for original in inputs {
            let bytes = original.to_bytes().expect("principal encode");
            let decoded = Principal::try_from_bytes(&bytes).expect("decode should succeed");
            assert_eq!(decoded, original, "Roundtrip failed for {original:?}");
        }
    }

    #[test]
    fn zero_length_principal_has_canonical_fixed_width_key_bytes() {
        let principal = Principal::from_slice(&[]);
        let mut bytes = [0xff; Principal::BYTE_LEN];

        principal
            .write_bytes(&mut bytes)
            .expect("zero-length principal should encode");

        assert_eq!(bytes, [0; Principal::BYTE_LEN]);
    }

    #[test]
    fn principal_serialized_size_is_within_bounds() {
        for len in 0..=Principal::MAX_LENGTH_IN_BYTES {
            let bytes: Vec<u8> = (0..len).map(u8::try_from).map(Result::unwrap).collect();
            let principal = Principal::from_slice(&bytes);
            let encoded = principal.to_bytes().expect("principal encode");
            assert!(
                encoded.len() <= Principal::MAX_LENGTH_IN_BYTES as usize,
                "Encoded size {} exceeded max {}",
                encoded.len(),
                Principal::MAX_LENGTH_IN_BYTES
            );
        }
    }

    #[test]
    fn principal_from_bytes_rejects_oversized() {
        let size = (Principal::MAX_LENGTH_IN_BYTES as usize) + 1;
        let buf = vec![0u8; size];
        assert!(Principal::try_from_bytes(&buf).is_err());
    }
}
