use crate::{
    ThisError,
    db::primitives::filter::{TextEqualityFilterKind, TextListFilterKind},
    traits::{
        FieldValue, Filterable, Inner, SanitizeAuto, SanitizeCustom, Storable, UpdateView,
        ValidateAuto, ValidateCustom, View, Visitable,
    },
    value::Value,
};
use canic::cdk::{
    api::msg_caller,
    candid::{CandidType, Principal as WrappedPrincipal},
    structures::storable::Bound,
};
use derive_more::{Deref, DerefMut, Display};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, str::FromStr};

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
    pub const STORABLE_MAX_SIZE: u32 = 29;

    pub const MIN: Self = Self::from_slice(&[0x00; 29]);
    pub const MAX: Self = Self::from_slice(&[0xFF; 29]);

    #[must_use]
    pub const fn anonymous() -> Self {
        Self(WrappedPrincipal::anonymous())
    }

    #[must_use]
    pub fn msg_caller() -> Self {
        Self(msg_caller())
    }

    #[must_use]
    pub const fn from_slice(slice: &[u8]) -> Self {
        Self(WrappedPrincipal::from_slice(slice))
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

impl Filterable for Principal {
    type Filter = TextEqualityFilterKind;
    type ListFilter = TextListFilterKind;
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

impl Storable for Principal {
    const BOUND: Bound = Bound::Bounded {
        max_size: Self::STORABLE_MAX_SIZE,
        is_fixed_size: true,
    };

    fn into_bytes(self) -> Vec<u8> {
        self.0.to_bytes().to_vec()
    }

    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.0.as_slice())
    }

    fn from_bytes(bytes: Cow<[u8]>) -> Self {
        Self::from_slice(bytes.as_ref())
    }
}

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
    use crate::traits::Storable;

    #[test]
    fn principal_max_size_is_bounded() {
        let principal = Principal::max_storable();
        let size = Storable::to_bytes(&principal).len();

        assert!(
            size <= Principal::STORABLE_MAX_SIZE as usize,
            "serialized Principal too large: got {size} bytes (limit {})",
            Principal::STORABLE_MAX_SIZE
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
            let decoded = Principal::from_bytes(bytes);
            assert_eq!(decoded, original, "Roundtrip failed for {original:?}");
        }
    }

    #[test]
    fn principal_serialized_size_is_within_bounds() {
        for len in 0..=Principal::STORABLE_MAX_SIZE {
            let bytes: Vec<u8> = (0..len).map(u8::try_from).map(Result::unwrap).collect();
            let principal = Principal::from_slice(&bytes);
            let encoded = principal.to_bytes();
            assert!(
                encoded.len() <= Principal::STORABLE_MAX_SIZE as usize,
                "Encoded size {} exceeded max {}",
                encoded.len(),
                Principal::STORABLE_MAX_SIZE
            );
        }
    }
}
