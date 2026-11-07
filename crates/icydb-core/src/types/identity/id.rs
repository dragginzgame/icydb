//! Module: types::identity::id
//! Defines the typed entity identifier wrapper used at API, query, and value
//! boundaries.

use crate::{
    db::{
        EntityKey, EntityKeyBytes, EntityKeyBytesError, KeyValueCodec, PrimaryKeyEncode,
        PrimaryKeyEncodeError, PrimaryKeyValue,
    },
    types::{GenerateKey, Subaccount},
    value::Value,
};

use serde::{Deserialize, de::Deserializer};
use std::{
    fmt,
    hash::{Hash, Hasher},
};

#[cfg(all(feature = "wire-schema", not(target_family = "wasm")))]
use ic_wire::typescript::{GenerationError, SchemaContext, Wire};
#[cfg(all(feature = "wire-schema", not(target_family = "wasm")))]
use serde_json::Value as SchemaValue;

//
// Id
//
// Typed primary-key value for an entity.
//
// ## Purpose
// `Id<E>` is a *boundary type*:
// - used at API, DTO, and query boundaries
// - enforces entity-kind correctness at compile time
// - prevents accidental mixing of primary keys across entities
// - carries identity shape only (not authority)
//
// ## Security model
// `Id<E>` is a public identifier and is **not** a secret or capability.
// It may be deserialized from untrusted input.
// `Id<E>` is a public identifier. It is not a secret and must never be treated as proof of
// authorization, existence, or ownership.
//
// Possession of an `Id<E>` does NOT imply:
// - existence of the referenced entity
// - authorization or ownership
// - correctness of the value
//
// All uses of `Id<E>` must perform explicit lookup and validation.
//
// ## Storage model
// - Entities themselves store **primitive key values only**
// - Conversion between `Id<E>` and the primitive key is explicit
// - `Id<E>` deserializes identically to `E::Key`
//
// ## Safety
// Construction from raw key material is intentionally explicit so call sites show where typed
// identity values are introduced. This is not an authorization or trust boundary.
//

#[repr(transparent)]
pub struct Id<E: EntityKey> {
    key: E::Key,
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
    pub const fn from_key(key: E::Key) -> Self {
        Self { key }
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
        self.key.to_key_value()
    }
}

impl<E> Id<E>
where
    E: EntityKey,
    E::Key: EntityKeyBytes,
{
    /// Derive the ledger subaccount for this identity.
    ///
    /// This is a pure, deterministic identity projection.
    /// It does NOT imply ownership, authorization, or existence.
    ///
    /// # Errors
    ///
    /// Returns [`EntityKeyBytesError`] when the key cannot produce its
    /// declared fixed-width representation.
    pub fn subaccount(&self) -> Result<Subaccount, EntityKeyBytesError> {
        Ok(Subaccount::from_array(self.project()?.into_bytes()))
    }
}

impl<E> Id<E>
where
    E: EntityKey,
    E::Key: GenerateKey,
{
    /// Generate a new typed primary-key value for entities whose
    /// key type supports local generation.
    ///
    /// This method does not imply persistence, existence, or authorization.
    /// Generated keys need not be unpredictable; see [`GenerateKey::generate`].
    ///
    /// # Errors
    ///
    /// Returns an internal generation error when the key generator cannot
    /// obtain entropy or exhausts its monotonic sequence.
    pub fn generate() -> Result<Self, crate::error::InternalError> {
        E::Key::generate().map(Self::from_key)
    }
}

impl<E> PrimaryKeyEncode for Id<E>
where
    E: EntityKey,
{
    fn to_primary_key_value(&self) -> Result<PrimaryKeyValue, PrimaryKeyEncodeError> {
        self.key.to_primary_key_value()
    }
}

// ----------------------------------------------------------------------
// Wire / view integration
// ----------------------------------------------------------------------

// ----------------------------------------------------------------------
// Standard trait impls
// ----------------------------------------------------------------------

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

impl<E: EntityKey> serde::Serialize for Id<E>
where
    E::Key: serde::Serialize,
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&self.key, serializer)
    }
}

// Like Serialize, host metadata exposes only the key, never the entity model.
#[cfg(all(feature = "wire-schema", not(target_family = "wasm")))]
impl<E: EntityKey> Wire for Id<E>
where
    E::Key: Wire,
{
    fn native_schema(context: &mut SchemaContext) -> Result<SchemaValue, GenerationError> {
        E::Key::native_schema(context)
    }
}

// ----------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------

#[cfg(test)]
mod tests;
