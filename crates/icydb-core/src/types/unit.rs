use crate::{
    traits::{
        FieldValue, Inner, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom,
        View, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use derive_more::Display;
use serde::{Deserialize, Serialize};

///
/// Unit
///

#[derive(
    CandidType,
    Clone,
    Copy,
    Debug,
    Default,
    Display,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
)]
pub struct Unit;

impl FieldValue for () {
    fn kind() -> crate::traits::FieldValueKind {
        crate::traits::FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Unit
    }

    fn from_value(value: &Value) -> Option<Self> {
        matches!(value, Value::Unit).then_some(())
    }
}

impl FieldValue for Unit {
    fn kind() -> crate::traits::FieldValueKind {
        crate::traits::FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Unit
    }

    fn from_value(value: &Value) -> Option<Self> {
        matches!(value, Value::Unit).then_some(Self)
    }
}

impl Inner<Self> for Unit {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
    }
}

impl SanitizeAuto for Unit {}

impl SanitizeCustom for Unit {}

impl UpdateView for Unit {
    type UpdateViewType = Self;
}

impl ValidateAuto for Unit {}

impl ValidateCustom for Unit {}

impl View for Unit {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Visitable for Unit {}
