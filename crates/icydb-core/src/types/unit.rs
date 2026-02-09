use crate::{
    traits::{
        AsView, Atomic, EntityKeyBytes, FieldValue, FieldValueKind, SanitizeAuto, SanitizeCustom,
        ValidateAuto, ValidateCustom, Visitable,
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

impl AsView for Unit {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl EntityKeyBytes for Unit {
    const BYTE_LEN: usize = 0;

    fn write_bytes(&self, out: &mut [u8]) {
        assert_eq!(out.len(), Self::BYTE_LEN);
    }
}

impl FieldValue for () {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Unit
    }

    fn from_value(value: &Value) -> Option<Self> {
        matches!(value, Value::Unit).then_some(())
    }
}

impl FieldValue for Unit {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Unit
    }

    fn from_value(value: &Value) -> Option<Self> {
        matches!(value, Value::Unit).then_some(Self)
    }
}

impl SanitizeAuto for Unit {}

impl SanitizeCustom for Unit {}

impl Atomic for Unit {}

impl ValidateAuto for Unit {}

impl ValidateCustom for Unit {}

impl Visitable for Unit {}
