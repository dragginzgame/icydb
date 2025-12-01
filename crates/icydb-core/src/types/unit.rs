use crate::{
    db::primitives::NoFilterKind,
    traits::{
        FieldValue, Filterable, Inner, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto,
        ValidateCustom, View, Visitable,
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
    fn to_value(&self) -> Value {
        Value::Unit
    }
}

impl FieldValue for Unit {
    fn to_value(&self) -> Value {
        Value::Unit
    }
}

impl Filterable for Unit {
    type Filter = NoFilterKind;
    type ListFilter = NoFilterKind;
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

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Visitable for Unit {}
