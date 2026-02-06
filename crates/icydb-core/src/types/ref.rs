use crate::{
    traits::{
        EntityStorageKey, FieldValue, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto,
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
/// Stores raw key material for relations.
///
/// If a generic identity wrapper must be Copy, never derive Copy or Clone;
/// always implement both manually.
///

#[repr(transparent)]
pub struct Ref<E>
where
    E: EntityStorageKey,
{
    key: E::Key,
    _marker: PhantomData<fn() -> E>,
}

impl<E> Ref<E>
where
    E: EntityStorageKey,
{
    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    /// Construct a typed entity reference from raw storage key material.
    ///
    /// ## Semantics
    /// This function is intended **only for schema-level construction**:
    /// - handwritten constructors in schema crates
    /// - derive-generated relation initialization
    ///
    /// Application code must not invent references arbitrarily.
    #[must_use]
    pub const fn from_storage_key(key: E::Key) -> Self {
        Self {
            key,
            _marker: PhantomData,
        }
    }

    // ------------------------------------------------------------------
    // Storage access (core-only)
    // ------------------------------------------------------------------

    /// Consume this reference and return raw storage key material.
    ///
    /// Core-only escape hatch for intent building, planning,
    /// execution, and persistence. Application code must not
    /// depend on storage identity.
    #[must_use]
    pub(crate) const fn into_storage_key(self) -> E::Key {
        self.key
    }

    // ------------------------------------------------------------------
    // Diagnostics
    // ------------------------------------------------------------------

    /// Convert this reference key into a semantic `Value`.
    ///
    /// Intended only for planner invariants, diagnostics,
    /// explain output, and fingerprinting.
    pub fn as_value(&self) -> Value {
        self.key.to_value()
    }
}

impl<E> CandidType for Ref<E>
where
    E: EntityStorageKey,
    E::Key: CandidType,
{
    fn _ty() -> candid::types::Type {
        <E::Key as CandidType>::_ty()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        self.key.idl_serialize(serializer)
    }
}

#[allow(clippy::expl_impl_clone_on_copy)]
impl<E> Clone for Ref<E>
where
    E: EntityStorageKey,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<E> Copy for Ref<E> where E: EntityStorageKey {}

impl<E> std::fmt::Debug for Ref<E>
where
    E: EntityStorageKey,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Ref").field(&self.key).finish()
    }
}

impl<E> Default for Ref<E>
where
    E: EntityStorageKey,
    E::Key: Default,
{
    fn default() -> Self {
        Self::from_storage_key(E::Key::default())
    }
}

impl<E> fmt::Display for Ref<E>
where
    E: EntityStorageKey,
    E::Key: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.fmt(f)
    }
}

impl<E> Eq for Ref<E> where E: EntityStorageKey {}

impl<E> PartialEq for Ref<E>
where
    E: EntityStorageKey,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<E> FieldValue for Ref<E>
where
    E: EntityStorageKey,
{
    fn to_value(&self) -> Value {
        self.as_value()
    }
}

impl<E> Hash for Ref<E>
where
    E: EntityStorageKey,
    E::Key: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

impl<E> Ord for Ref<E>
where
    E: EntityStorageKey,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl<E> PartialOrd for Ref<E>
where
    E: EntityStorageKey,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> SanitizeAuto for Ref<E> where E: EntityStorageKey {}

impl<E> SanitizeCustom for Ref<E> where E: EntityStorageKey {}

impl<E> Serialize for Ref<E>
where
    E: EntityStorageKey,
    E::Key: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.key.serialize(serializer)
    }
}

impl<'de, E> Deserialize<'de> for Ref<E>
where
    E: EntityStorageKey,
    E::Key: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let key = E::Key::deserialize(deserializer)?;

        Ok(Self::from_storage_key(key))
    }
}

impl<E> UpdateView for Ref<E>
where
    E: EntityStorageKey,
    E::Key: CandidType + Default,
{
    type UpdateViewType = Self;

    fn merge(&mut self, update: Self::UpdateViewType) {
        *self = update;
    }
}

impl<E> ValidateAuto for Ref<E> where E: EntityStorageKey {}

impl<E> ValidateCustom for Ref<E> where E: EntityStorageKey {}

impl<E> View for Ref<E>
where
    E: EntityStorageKey,
    E::Key: Default,
{
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl<E> Visitable for Ref<E> where E: EntityStorageKey {}
