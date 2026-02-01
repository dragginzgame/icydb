use crate::{
    key::Key,
    model::field::EntityFieldKind,
    traits::{
        EntityKind, FieldValue, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use serde::{Deserialize, Serialize};

///
/// Ref
///
/// Typed reference to another entity's primary key.
/// Keeps the target entity type in the type system.
/// Intended for relation fields and reference extraction.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Ref<T>
where
    T: EntityKind,
{
    key: T::PrimaryKey,
}

impl<T> Ref<T>
where
    T: EntityKind,
{
    /// Construct a new typed reference from a primary key value.
    #[must_use]
    pub const fn new(key: T::PrimaryKey) -> Self {
        Self { key }
    }

    /// Return the referenced primary key value.
    #[must_use]
    pub const fn key(&self) -> T::PrimaryKey {
        self.key
    }

    /// Runtime kind metadata for reference fields.
    pub const KIND: EntityFieldKind = EntityFieldKind::Ref {
        target_path: T::PATH,
        key_kind: &T::MODEL.primary_key.kind,
    };
}

impl<T> Default for Ref<T>
where
    T: EntityKind,
    T::PrimaryKey: Default,
{
    fn default() -> Self {
        Self {
            key: T::PrimaryKey::default(),
        }
    }
}

impl<T> From<Ref<T>> for Key
where
    T: EntityKind,
{
    fn from(reference: Ref<T>) -> Self {
        reference.key.into()
    }
}

impl<T> CandidType for Ref<T>
where
    T: EntityKind,
    T::PrimaryKey: CandidType,
{
    fn _ty() -> candid::types::Type {
        <T::PrimaryKey as CandidType>::_ty()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        self.key.idl_serialize(serializer)
    }
}

impl<T> FieldValue for Ref<T>
where
    T: EntityKind,
{
    fn to_value(&self) -> Value {
        let key: Key = self.key.into();
        key.to_value()
    }
}

impl<T> View for Ref<T>
where
    T: EntityKind,
    T::PrimaryKey: Default,
{
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl<T> UpdateView for Ref<T>
where
    T: EntityKind,
    T::PrimaryKey: CandidType + Default,
{
    type UpdateViewType = Self;

    fn merge(&mut self, update: Self::UpdateViewType) {
        *self = update;
    }
}

impl<T> SanitizeAuto for Ref<T> where T: EntityKind {}
impl<T> SanitizeCustom for Ref<T> where T: EntityKind {}
impl<T> ValidateAuto for Ref<T> where T: EntityKind {}
impl<T> ValidateCustom for Ref<T> where T: EntityKind {}
impl<T> Visitable for Ref<T> where T: EntityKind {}
