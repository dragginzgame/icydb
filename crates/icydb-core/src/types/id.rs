use crate::{
    traits::{
        EntityStorageKey, FieldValue, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom,
        View, Visitable,
    },
    types::Ref,
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
/// Id
///
/// Typed primary-key wrapper for entity identities.
/// Carries entity context without changing the underlying key type.
/// Serializes identically to `E::Key`.
///

#[repr(transparent)]
pub struct Id<E: EntityStorageKey> {
    key: E::Key,
    _marker: PhantomData<fn() -> E>,
}

impl<E> Id<E>
where
    E: EntityStorageKey,
{
    /// Construct a typed identity from the raw key value.
    #[must_use]
    pub(crate) const fn new(key: E::Key) -> Self {
        Self {
            key,
            _marker: PhantomData,
        }
    }

    /// Returns the underlying key.
    #[must_use]
    pub(crate) const fn key(&self) -> E::Key {
        self.key
    }

    /// Consume this identity and return the raw key.
    #[must_use]
    pub(crate) const fn into_key(self) -> E::Key {
        self.key
    }

    /// Convert this identity key into a semantic Value.
    ///
    /// This is intended ONLY for planner invariants, diagnostics,
    /// explain output, and fingerprinting.
    pub fn as_value(&self) -> Value {
        self.key.to_value()
    }
}

impl<E> CandidType for Id<E>
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
impl<E> Clone for Id<E>
where
    E: EntityStorageKey,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<E> Copy for Id<E> where E: EntityStorageKey {}

impl<E> fmt::Debug for Id<E>
where
    E: EntityStorageKey,
    E::Key: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Id").field(&self.key).finish()
    }
}

impl<E> Default for Id<E>
where
    E: EntityStorageKey,
    E::Key: Default,
{
    fn default() -> Self {
        Self::new(E::Key::default())
    }
}

impl<E> fmt::Display for Id<E>
where
    E: EntityStorageKey,
    E::Key: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.fmt(f)
    }
}

impl<E> Eq for Id<E>
where
    E: EntityStorageKey,
    E::Key: Eq,
{
}

impl<E> PartialEq for Id<E>
where
    E: EntityStorageKey,
    E::Key: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<E> From<Id<E>> for Ref<E>
where
    E: EntityStorageKey,
{
    fn from(identity: Id<E>) -> Self {
        Self::from_storage_key(identity.into_key())
    }
}

impl<E> Hash for Id<E>
where
    E: EntityStorageKey,
    E::Key: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

impl<E> Ord for Id<E>
where
    E: EntityStorageKey,
    E::Key: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl<E> PartialOrd for Id<E>
where
    E: EntityStorageKey,
    E::Key: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> FieldValue for Id<E>
where
    E: EntityStorageKey,
    E::Key: FieldValue,
{
    fn to_value(&self) -> Value {
        self.key.to_value()
    }

    fn from_value(value: &Value) -> Option<Self> {
        let key = E::Key::from_value(value)?;

        Some(Self::new(key))
    }
}

impl<E> SanitizeAuto for Id<E> where E: EntityStorageKey {}

impl<E> SanitizeCustom for Id<E> where E: EntityStorageKey {}

impl<E> Serialize for Id<E>
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

impl<'de, E> Deserialize<'de> for Id<E>
where
    E: EntityStorageKey,
    E::Key: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let key = E::Key::deserialize(deserializer)?;

        Ok(Self::new(key))
    }
}

impl<E> ValidateAuto for Id<E> where E: EntityStorageKey {}

impl<E> ValidateCustom for Id<E> where E: EntityStorageKey {}

impl<E> View for Id<E>
where
    E: EntityStorageKey,
    E::Key: View,
{
    type ViewType = <E::Key as View>::ViewType;

    fn to_view(&self) -> Self::ViewType {
        View::to_view(&self.key())
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self::new(View::from_view(view))
    }
}

impl<E> Visitable for Id<E> where E: EntityStorageKey {}
