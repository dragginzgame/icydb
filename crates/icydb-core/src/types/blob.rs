use crate::{
    db::primitives::NoFilterKind,
    traits::{
        FieldValue, Filterable, Inner, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use derive_more::{Deref, DerefMut};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::fmt::{self, Display};

///
/// Blob
/// Display prints a size summary; it does not print content.
///

#[derive(
    CandidType,
    Clone,
    Debug,
    Default,
    Deref,
    DerefMut,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
)]
pub struct Blob(ByteBuf);

impl Blob {
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Display for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[blob ({} bytes)]", self.0.len())
    }
}

impl FieldValue for Blob {
    fn to_value(&self) -> Value {
        Value::Blob(self.to_vec())
    }
}

impl Filterable for Blob {
    type Filter = NoFilterKind;
    type ListFilter = NoFilterKind;
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

impl UpdateView for Blob {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Blob {}

impl ValidateCustom for Blob {}

impl View for Blob {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Visitable for Blob {}
