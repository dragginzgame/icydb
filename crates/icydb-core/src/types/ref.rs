use crate::{
    db::store::StorageKey,
    model::field::EntityFieldKind,
    traits::{
        EntityId, EntityKind, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
    types::{Account, Principal, Subaccount, Timestamp, Ulid},
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
pub struct Ref<E> {
    key: StorageKey,
    _marker: PhantomData<*const E>,
}

impl<E> Ref<E> {
    /// Construct a new typed reference from a storage key.
    #[must_use]
    pub fn new(key: StorageKey) -> Self {
        Self {
            key,
            _marker: PhantomData,
        }
    }

    /// Convert this identity key into a semantic Value.
    ///
    /// This is intended ONLY for planner invariants, diagnostics,
    /// explain output, and fingerprinting.
    pub fn as_value(&self) -> Value {
        self.key.as_value()
    }

    #[must_use]
    pub(crate) const fn from_raw(key: StorageKey) -> Self {
        Self {
            key,
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub const fn raw(self) -> StorageKey {
        self.key
    }
}

// No bounds. Ever.
impl<E> Copy for Ref<E> {}

impl<E> Clone for Ref<E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<E> std::fmt::Debug for Ref<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Ref").field(&self.key).finish()
    }
}

impl<E: EntityKind> EntityId for Ref<E> {}

//
// Equality / ordering / hashing
//

impl<T: EntityKind> Eq for Ref<T> {}

impl<T: EntityKind> PartialEq for Ref<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<T: EntityKind> Ord for Ref<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl<T: EntityKind> PartialOrd for Ref<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl<T: EntityKind> Hash for Ref<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

impl<E> Serialize for Ref<E> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.key.serialize(serializer)
    }
}

impl<'de, E> Deserialize<'de> for Ref<E> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let key = StorageKey::deserialize(deserializer)?;

        Ok(Self::from_raw(key))
    }
}

//
// Defaults (schema-driven)
//

impl<T: EntityKind> Default for Ref<T> {
    fn default() -> Self {
        Self {
            key: default_storage_key(&T::MODEL.primary_key.kind),
            _marker: PhantomData,
        }
    }
}

//
// Candid
//

impl<T> CandidType for Ref<T>
where
    T: EntityKind,
{
    fn _ty() -> candid::types::Type {
        <StorageKey as CandidType>::_ty()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        self.key.idl_serialize(serializer)
    }
}

//
// View / update
//

impl<T: EntityKind> View for Ref<T> {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl<T: EntityKind> UpdateView for Ref<T> {
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

//
// Helpers
//

fn default_storage_key(kind: &EntityFieldKind) -> StorageKey {
    match kind {
        EntityFieldKind::Account => StorageKey::Account(Account::default()),
        EntityFieldKind::Int => StorageKey::Int(0),
        EntityFieldKind::Principal => StorageKey::Principal(Principal::default()),
        EntityFieldKind::Subaccount => StorageKey::Subaccount(Subaccount::default()),
        EntityFieldKind::Timestamp => StorageKey::Timestamp(Timestamp::default()),
        EntityFieldKind::Uint => StorageKey::Uint(0),
        EntityFieldKind::Ulid => StorageKey::Ulid(Ulid::default()),
        EntityFieldKind::Ref { key_kind, .. } => default_storage_key(key_kind),
        _ => StorageKey::Unit,
    }
}

//
// Display
//

impl<T: EntityKind> fmt::Display for Ref<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.fmt(f)
    }
}
