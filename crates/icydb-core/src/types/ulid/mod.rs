pub mod fixture;
pub mod generator;

use crate::{
    ThisError,
    db::primitives::{TextFilterKind, TextListFilterKind},
    traits::{
        FieldValue, Filterable, Inner, SanitizeAuto, SanitizeCustom, Storable, UpdateView,
        ValidateAuto, ValidateCustom, View, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use canic::{cdk::structures::storable::Bound, types::Ulid as WrappedUlid};
use derive_more::{Deref, DerefMut, Display, FromStr};
use icydb_error::ErrorTree;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

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
/// Ulid
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
    FromStr,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
)]
#[repr(transparent)]
pub struct Ulid(WrappedUlid);

impl Ulid {
    pub const STORABLE_MAX_SIZE: u32 = 16;

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
    /// Panics on generator overflow. Use `try_generate` to handle errors.
    #[must_use]
    pub fn generate() -> Self {
        Self::try_generate().unwrap()
    }

    #[must_use]
    pub fn increment(&self) -> Option<Self> {
        self.0.increment().map(Self::from)
    }

    /// try_generate
    /// Fallible ULID generation preserving error type (e.g., monotonic overflow).
    pub fn try_generate() -> Result<Self, UlidError> {
        generator::generate()
    }

    /// from_bytes
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(WrappedUlid::from_bytes(bytes))
    }

    /// from_str
    #[allow(clippy::should_implement_trait)]
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

impl Default for Ulid {
    fn default() -> Self {
        Self(WrappedUlid::nil())
    }
}

impl FieldValue for Ulid {
    fn to_value(&self) -> Value {
        Value::Ulid(*self)
    }
}

impl Filterable for Ulid {
    type Filter = TextFilterKind;
    type ListFilter = TextListFilterKind;
}

impl From<WrappedUlid> for Ulid {
    fn from(ulid: WrappedUlid) -> Self {
        Self(ulid)
    }
}

impl Inner<Self> for Ulid {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
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

impl Storable for Ulid {
    const BOUND: Bound = Bound::Bounded {
        max_size: Self::STORABLE_MAX_SIZE,
        is_fixed_size: true,
    };

    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Owned(self.0.to_bytes().to_vec())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0.to_bytes().to_vec()
    }

    fn from_bytes(bytes: Cow<[u8]>) -> Self {
        assert!(
            bytes.len() == 16,
            "Invalid Ulid byte length: expected 16, got {}",
            bytes.len()
        );

        let mut array = [0u8; 16];
        array.copy_from_slice(&bytes);

        Self::from_bytes(array)
    }
}

impl UpdateView for Ulid {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Ulid {
    fn validate_self(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::default();

        if self.0 == WrappedUlid::nil() {
            errs.add("ulid is nil");
        }

        errs.result()
    }
}

impl ValidateCustom for Ulid {}

impl View for Ulid {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self(*view)
    }
}

impl Visitable for Ulid {}

///
/// TESTS
///

#[cfg(test)]
mod test {
    use super::*;
    use crate::traits::Storable;

    #[test]
    fn ulid_max_size_is_bounded() {
        let ulid = Ulid::max_storable();
        let size = Storable::to_bytes(&ulid).len();

        assert!(
            size <= Ulid::STORABLE_MAX_SIZE as usize,
            "serialized Ulid too large: got {size} bytes (limit {})",
            Ulid::STORABLE_MAX_SIZE
        );
    }

    #[test]
    fn test_ulid_string_roundtrip() {
        let u1 = Ulid::generate();
        let u2 = Ulid::from_str(&u1.to_string()).unwrap();

        assert_eq!(u1, u2);
    }
}
