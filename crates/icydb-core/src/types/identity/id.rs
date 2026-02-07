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
    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    /// Construct an entity identity from raw storage key material.
    ///
    /// ## Semantics
    /// This function is intended **only for entity construction**:
    /// - handwritten constructors in schema crates
    /// - derive-generated entity initialization
    ///
    /// Application code must not invent identities arbitrarily.
    #[must_use]
    pub(crate) const fn from_storage_key(key: E::Key) -> Self {
        Self {
            key,
            _marker: PhantomData,
        }
    }

    // ------------------------------------------------------------------
    // Storage access (core-only)
    // ------------------------------------------------------------------

    /// Borrow the underlying storage key.
    ///
    /// Core-only. Application code must not depend on storage identity.
    #[must_use]
    pub(crate) const fn storage_key(&self) -> E::Key {
        self.key
    }

    /// Consume this identity and return raw storage key material.
    ///
    /// Core-only. Use only at intent / execution boundaries.
    #[must_use]
    pub(crate) const fn into_storage_key(self) -> E::Key {
        self.key
    }

    // ------------------------------------------------------------------
    // Diagnostics
    // ------------------------------------------------------------------

    /// Convert this identity key into a semantic `Value`.
    ///
    /// Intended only for planner invariants, diagnostics,
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
        Self::from_storage_key(E::Key::default())
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

        Some(Self::from_storage_key(key))
    }
}

impl<E> From<Id<E>> for Value
where
    E: EntityStorageKey,
{
    fn from(id: Id<E>) -> Self {
        id.as_value()
    }
}

impl<E> From<&Id<E>> for Value
where
    E: EntityStorageKey,
{
    fn from(id: &Id<E>) -> Self {
        id.as_value()
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

        Ok(Self::from_storage_key(key))
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
        View::to_view(&self.storage_key())
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self::from_storage_key(View::from_view(view))
    }
}

impl<E> UpdateView for Id<E>
where
    E: EntityStorageKey,
    E::Key: UpdateView,
{
    type UpdateViewType = <E::Key as UpdateView>::UpdateViewType;

    fn merge(&mut self, update: Self::UpdateViewType) {
        let mut next_key = self.storage_key();
        next_key.merge(update);
        *self = Self::from_storage_key(next_key);
    }
}

impl<E> Visitable for Id<E> where E: EntityStorageKey {}

#[cfg(test)]
mod tests {
    use super::Id;
    use crate::{
        traits::{EntityStorageKey, FieldValue},
        value::Value,
    };

    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    struct TestEntity;

    impl EntityStorageKey for TestEntity {
        type Key = u64;
    }

    #[test]
    fn field_value_round_trip_uses_underlying_key() {
        let id = Id::<TestEntity>::from_storage_key(7);
        let value = id.to_value();
        assert_eq!(value, Value::Uint(7));

        let decoded = Id::<TestEntity>::from_value(&value).expect("u64 value should decode to Id");
        assert_eq!(decoded, id);
    }

    #[test]
    fn field_value_rejects_incompatible_value_kind() {
        let decoded = Id::<TestEntity>::from_value(&Value::Text("not-a-key".to_string()));
        assert!(decoded.is_none());
    }

    #[test]
    fn into_value_for_owned_and_borrowed_id_match_as_value() {
        let id = Id::<TestEntity>::from_storage_key(42);
        let expected = id.as_value();
        let borrowed = Value::from(&id);
        let owned = Value::from(id);

        assert_eq!(borrowed, expected);
        assert_eq!(owned, expected);
    }
}
