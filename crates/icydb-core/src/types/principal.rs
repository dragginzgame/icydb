use crate::{
    traits::{
        FieldValue, Inner, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom,
        View, Visitable,
    },
    value::Value,
};
use canic_cdk::candid::{CandidType, Principal as WrappedPrincipal};
use derive_more::{Deref, DerefMut, Display};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use thiserror::Error as ThisError;

///
/// PrincipalError
///

#[derive(Debug, ThisError)]
pub enum PrincipalError {
    #[error("{0}")]
    Wrapped(String),
}

///
/// Principal
///

#[derive(
    CandidType,
    Clone,
    Copy,
    Debug,
    Deref,
    DerefMut,
    Display,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
)]
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
        let inner = WrappedPrincipal::from_text(text)
            .map_err(|e| PrincipalError::Wrapped(e.to_string()))?;

        Ok(Self(inner))
    }

    #[must_use]
    pub const fn from_slice(slice: &[u8]) -> Self {
        Self(WrappedPrincipal::from_slice(slice))
    }

    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }

    pub const fn try_from_bytes(bytes: &[u8]) -> Result<Self, PrincipalDecodeError> {
        if bytes.len() > Self::MAX_LENGTH_IN_BYTES as usize {
            return Err(PrincipalDecodeError::TooLarge { len: bytes.len() });
        }

        Ok(Self::from_slice(bytes))
    }

    #[must_use]
    pub const fn dummy(n: u8) -> Self {
        Self::from_slice(&[n; 29])
    }

    #[must_use]
    pub const fn max_storable() -> Self {
        Self::from_slice(&[0xFF; 29])
    }
}

#[derive(Debug, ThisError)]
pub enum PrincipalDecodeError {
    #[error("principal exceeds max length: {len} bytes")]
    TooLarge { len: usize },
}

impl TryFrom<&[u8]> for Principal {
    type Error = PrincipalDecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

impl Default for Principal {
    fn default() -> Self {
        Self(WrappedPrincipal::from_slice(&[]))
    }
}

impl FieldValue for Principal {
    fn to_value(&self) -> Value {
        Value::Principal(*self)
    }
}

impl FieldValue for WrappedPrincipal {
    fn to_value(&self) -> Value {
        Value::Principal(self.into())
    }
}

impl From<WrappedPrincipal> for Principal {
    fn from(p: WrappedPrincipal) -> Self {
        Self(p)
    }
}

impl From<&WrappedPrincipal> for Principal {
    fn from(p: &WrappedPrincipal) -> Self {
        Self(*p)
    }
}

impl From<Principal> for WrappedPrincipal {
    fn from(p: Principal) -> Self {
        *p
    }
}

impl FromStr for Principal {
    type Err = PrincipalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parses textual principals (e.g., "aaaaa-aa"). Returns a detailed error on failure.
        let this = WrappedPrincipal::from_str(s)
            .map(Self)
            .map_err(|e| PrincipalError::Wrapped(e.to_string()))?;

        Ok(this)
    }
}

impl Inner<Self> for Principal {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
    }
}

impl PartialEq<WrappedPrincipal> for Principal {
    fn eq(&self, other: &WrappedPrincipal) -> bool {
        self.0 == *other
    }
}

impl PartialEq<Principal> for WrappedPrincipal {
    fn eq(&self, other: &Principal) -> bool {
        *self == other.0
    }
}

impl SanitizeAuto for Principal {}

impl SanitizeCustom for Principal {}

impl UpdateView for Principal {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Principal {}

impl ValidateCustom for Principal {}

// The WrappedPrincipal type doesn't have Default so we can't
// use it as a View
impl View for Principal {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Visitable for Principal {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principal_max_size_is_bounded() {
        let principal = Principal::max_storable();
        let size = principal.as_slice().len();

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
            let bytes = original.to_bytes();
            let decoded = Principal::try_from_bytes(&bytes).expect("decode should succeed");
            assert_eq!(decoded, original, "Roundtrip failed for {original:?}");
        }
    }

    #[test]
    fn principal_serialized_size_is_within_bounds() {
        for len in 0..=Principal::MAX_LENGTH_IN_BYTES {
            let bytes: Vec<u8> = (0..len).map(u8::try_from).map(Result::unwrap).collect();
            let principal = Principal::from_slice(&bytes);
            let encoded = principal.to_bytes();
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
