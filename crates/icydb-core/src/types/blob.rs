use crate::{
    patch::AtomicPatch,
    traits::{
        AsView, FieldValue, FieldValueKind, Inner, SanitizeAuto, SanitizeCustom, ValidateAuto,
        ValidateCustom, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::fmt::{self, Display};

///
/// Blob
///
/// Blob is a semantic binary value; raw byte access is explicit via accessors (no `Deref`).
/// Display prints a size summary; it does not print content.
///

#[derive(
    CandidType, Clone, Debug, Default, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Blob(ByteBuf);

impl Blob {
    #[must_use]
    pub fn as_mut_bytes(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// View the blob as a byte slice.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Clone the blob into a new byte vector.
    #[must_use]
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    /// Length of the blob in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the blob is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsView for Blob {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl AtomicPatch for Blob {}

impl Display for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[blob ({} bytes)]", self.0.len())
    }
}

impl FieldValue for Blob {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Blob(self.0.to_vec())
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Blob(v) => Some(Self::from(v.clone())),
            _ => None,
        }
    }
}

impl From<Vec<u8>> for Blob {
    fn from(bytes: Vec<u8>) -> Self {
        Self(ByteBuf::from(bytes))
    }
}

impl From<&[u8]> for Blob {
    fn from(bytes: &[u8]) -> Self {
        Self(ByteBuf::from(bytes))
    }
}

impl<const N: usize> From<&[u8; N]> for Blob {
    fn from(bytes: &[u8; N]) -> Self {
        Self(ByteBuf::from(&bytes[..]))
    }
}

impl Inner<Self> for Blob {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
    }
}

impl SanitizeAuto for Blob {}

impl SanitizeCustom for Blob {}

impl ValidateAuto for Blob {}

impl ValidateCustom for Blob {}

impl Visitable for Blob {}
