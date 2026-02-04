use crate::{
    traits::{
        EntityIdentity, FieldValue, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

///
/// Ref
///
/// Typed reference to another entity's primary key.
/// This is an *identity type*, not a semantic value.
///
/// If a generic identity wrapper must be Copy, never derive Copy or Clone;
/// always implement both manually.
///

#[repr(transparent)]
pub struct Ref<E>
where
    E: EntityIdentity,
{
    id: E::Id,
    _marker: PhantomData<fn() -> E>,
}

impl<E> Ref<E>
where
    E: EntityIdentity,
{
    /// Construct a Ref from a semantic identity value.
    #[must_use]
    pub const fn new(id: E::Id) -> Self {
        Self {
            id,
            _marker: PhantomData,
        }
    }

    /// Convert this identity key into a semantic Value.
    ///
    /// This is intended ONLY for planner invariants, diagnostics,
    /// explain output, and fingerprinting.
    pub fn as_value(&self) -> Value {
        self.id.to_value()
    }

    #[must_use]
    pub const fn id(self) -> E::Id {
        self.id
    }

    /// Return the primary key.
    #[must_use]
    pub const fn key(self) -> E::Id {
        self.id
    }
}

impl<E> Copy for Ref<E> where E: EntityIdentity {}

#[allow(clippy::expl_impl_clone_on_copy)]
impl<E> Clone for Ref<E>
where
    E: EntityIdentity,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<E> std::fmt::Debug for Ref<E>
where
    E: EntityIdentity,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Ref").field(&self.id).finish()
    }
}

impl<E> CandidType for Ref<E>
where
    E: EntityIdentity,
    E::Id: CandidType,
{
    fn _ty() -> candid::types::Type {
        <E::Id as CandidType>::_ty()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        self.id.idl_serialize(serializer)
    }
}

impl<E> Eq for Ref<E> where E: EntityIdentity {}

impl<E> PartialEq for Ref<E>
where
    E: EntityIdentity,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<E> FieldValue for Ref<E>
where
    E: EntityIdentity,
{
    fn to_value(&self) -> Value {
        self.as_value()
    }
}

impl<E> Hash for Ref<E>
where
    E: EntityIdentity,
    E::Id: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<E> Ord for Ref<E>
where
    E: EntityIdentity,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl<E> PartialOrd for Ref<E>
where
    E: EntityIdentity,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> Serialize for Ref<E>
where
    E: EntityIdentity,
    E::Id: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.id.serialize(serializer)
    }
}

impl<'de, E> Deserialize<'de> for Ref<E>
where
    E: EntityIdentity,
    E::Id: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let id = E::Id::deserialize(deserializer)?;

        Ok(Self::new(id))
    }
}

impl<T> Default for Ref<T>
where
    T: EntityIdentity,
    T::Id: Default,
{
    fn default() -> Self {
        Self::new(T::Id::default())
    }
}

impl<T> fmt::Display for Ref<T>
where
    T: EntityIdentity,
    T::Id: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}

impl<T> SanitizeAuto for Ref<T> where T: EntityIdentity {}

impl<T> SanitizeCustom for Ref<T> where T: EntityIdentity {}

impl<T> UpdateView for Ref<T>
where
    T: EntityIdentity,
    T::Id: CandidType + Default,
{
    type UpdateViewType = Self;

    fn merge(&mut self, update: Self::UpdateViewType) {
        *self = update;
    }
}

impl<T> ValidateAuto for Ref<T> where T: EntityIdentity {}

impl<T> ValidateCustom for Ref<T> where T: EntityIdentity {}

impl<E> View for Ref<E>
where
    E: EntityIdentity,
    E::Id: Default,
{
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl<T> Visitable for Ref<T> where T: EntityIdentity {}
