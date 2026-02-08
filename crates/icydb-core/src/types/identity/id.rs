use crate::{
    traits::{
        EntityKey, EntityKeyBytes, FieldValue, FieldValueKind, SanitizeAuto, SanitizeCustom,
        ValidateAuto, ValidateCustom, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use serde::{Deserialize, Serialize, Serializer, de::Deserializer};
use std::{
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

///
/// Id
///
/// Typed primary-key value for an entity.
///
/// ## Purpose
/// `Id<E>` is a *boundary type*:
/// - used at API, DTO, and query boundaries
/// - enforces entity-kind correctness at compile time
/// - prevents accidental mixing of primary keys across entities
/// - carries identity shape only (not authority)
///
/// ## Security model
/// `Id<E>` is a public identifier and is **not** a secret or capability.
/// It may be deserialized from untrusted input.
/// `Id<E>` is a public identifier. It is not a secret and must never be treated as proof of
/// authorization, existence, or ownership.
///
/// Possession of an `Id<E>` does NOT imply:
/// - existence of the referenced entity
/// - authorization or ownership
/// - correctness of the value
///
/// All uses of `Id<E>` must perform explicit lookup and validation.
///
/// ## Storage model
/// - Entities themselves store **primitive key values only**
/// - Conversion between `Id<E>` and the primitive key is explicit
/// - `Id<E>` serializes identically to `E::Key`
///
/// ## Safety
/// Construction from raw key material is intentionally explicit so call sites show where typed
/// identity values are introduced. This is not an authorization or trust boundary.
///

#[repr(transparent)]
pub struct Id<E: EntityKey> {
    key: E::Key,
    _marker: PhantomData<fn() -> E>,
}

impl<E> Id<E>
where
    E: EntityKey,
{
    pub const KEY_BYTES: usize = E::Key::BYTE_LEN;

    // ------------------------------------------------------------------
    // Construction (explicit)
    // ------------------------------------------------------------------

    /// Construct a typed primary-key value from a raw key.
    ///
    /// ## Invariant
    /// Callers must already know that `key` is the primary key for `E`.
    /// This function does **not** validate the association.
    /// It does not prove row existence, ownership, or authorization.
    ///
    /// This is an explicit boundary conversion from storage-level
    /// representation to a typed entity key.
    pub fn from_key(key: E::Key) -> Self {
        Self {
            key,
            _marker: ::core::marker::PhantomData,
        }
    }

    // ------------------------------------------------------------------
    // Boundary conversion
    // ------------------------------------------------------------------

    /// Return the underlying primitive primary-key value.
    ///
    /// ## Semantics
    /// This is the *explicit boundary crossing* from typed identity
    /// to storage-level representation.
    ///
    /// Typical use:
    /// - assigning entity fields
    /// - persistence
    /// - foreign-key storage
    ///
    /// Exposing the primitive key does not change trust semantics: the value remains public and
    /// non-authoritative until verified in context.
    #[must_use]
    pub const fn key(&self) -> E::Key {
        self.key
    }

    #[must_use]
    pub const fn into_key(self) -> E::Key {
        self.key
    }

    // ------------------------------------------------------------------
    // Diagnostics / value integration
    // ------------------------------------------------------------------

    /// Convert this typed primary-key value into a semantic `Value`.
    ///
    /// Intended for:
    /// - query planning
    /// - diagnostics / explain output
    /// - fingerprinting
    pub fn as_value(&self) -> Value {
        self.key.to_value()
    }
}

// ----------------------------------------------------------------------
// Wire / view integration
// ----------------------------------------------------------------------

impl<E> CandidType for Id<E>
where
    E: EntityKey,
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

// ----------------------------------------------------------------------
// Standard trait impls
// ----------------------------------------------------------------------

#[allow(clippy::expl_impl_clone_on_copy)]
impl<E> Clone for Id<E>
where
    E: EntityKey,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<E> Copy for Id<E> where E: EntityKey {}

impl<E> fmt::Debug for Id<E>
where
    E: EntityKey,
    E::Key: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Id").field(&self.key).finish()
    }
}

impl<E> fmt::Display for Id<E>
where
    E: EntityKey,
    E::Key: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.fmt(f)
    }
}

impl<E> Eq for Id<E>
where
    E: EntityKey,
    E::Key: Eq,
{
}

impl<E> PartialEq for Id<E>
where
    E: EntityKey,
    E::Key: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<E> FieldValue for Id<E>
where
    E: EntityKey,
    E::Key: FieldValue,
{
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        self.key().to_value()
    }

    fn from_value(value: &Value) -> Option<Self> {
        let key = <E::Key as FieldValue>::from_value(value)?;
        Some(Self::from_key(key))
    }
}

impl<E> Hash for Id<E>
where
    E: EntityKey,
    E::Key: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

impl<E> Ord for Id<E>
where
    E: EntityKey,
    E::Key: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl<E> PartialOrd for Id<E>
where
    E: EntityKey,
    E::Key: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> From<Id<E>> for Value
where
    E: EntityKey,
{
    fn from(id: Id<E>) -> Self {
        id.as_value()
    }
}

impl<E> From<&Id<E>> for Value
where
    E: EntityKey,
{
    fn from(id: &Id<E>) -> Self {
        id.as_value()
    }
}

impl<E> Serialize for Id<E>
where
    E: EntityKey,
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
    E: EntityKey,
    E::Key: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let key = E::Key::deserialize(deserializer)?;

        Ok(Self::from_key(key))
    }
}

impl<E> SanitizeAuto for Id<E> where E: EntityKey {}
impl<E> SanitizeCustom for Id<E> where E: EntityKey {}
impl<E> ValidateAuto for Id<E> where E: EntityKey {}
impl<E> ValidateCustom for Id<E> where E: EntityKey {}
impl<E> Visitable for Id<E> where E: EntityKey {}

// ----------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::Id;
    use crate::{traits::EntityKey, value::Value};

    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    struct TestEntity;

    impl EntityKey for TestEntity {
        type Key = u64;
    }

    #[test]
    fn into_value_for_owned_and_borrowed_id_match_as_value() {
        let id = Id::<TestEntity>::from_key(42);
        let expected = id.as_value();
        let borrowed = Value::from(&id);
        let owned = Value::from(id);

        assert_eq!(borrowed, expected);
        assert_eq!(owned, expected);
    }
}
