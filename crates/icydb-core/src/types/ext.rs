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
    fmt::{self, Debug, Display},
    hash::{Hash, Hasher},
    marker::PhantomData,
};

///
/// Ext
///
/// Typed external reference to an entity identity.
///
/// Unlike `Ref<E>`, this type does NOT imply:
/// - local existence
/// - referential integrity
/// - planner visibility
///
/// It is a typed identity only.
///

#[repr(transparent)]
pub struct Ext<E>
where
    E: EntityIdentity,
{
    id: E::Id,
    _marker: PhantomData<fn() -> E>,
}

impl<E> Ext<E>
where
    E: EntityIdentity,
{
    /// Construct an external reference from an identity value.
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

    /// Return the underlying identity.
    #[must_use]
    pub const fn id(self) -> E::Id {
        self.id
    }

    /// Alias for symmetry with `Ref`.
    #[must_use]
    pub const fn key(self) -> E::Id {
        self.id
    }
}

impl<E> CandidType for Ext<E>
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

impl<E> Copy for Ext<E>
where
    E: EntityIdentity,
    E::Id: Copy,
{
}

#[allow(clippy::expl_impl_clone_on_copy)]
impl<E> Clone for Ext<E>
where
    E: EntityIdentity,
    E::Id: Copy,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<E> Debug for Ext<E>
where
    E: EntityIdentity,
    E::Id: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Ext").field(&self.id).finish()
    }
}

impl<E> Default for Ext<E>
where
    E: EntityIdentity,
    E::Id: Default,
{
    fn default() -> Self {
        Self::new(E::Id::default())
    }
}

impl<E> Display for Ext<E>
where
    E: EntityIdentity,
    E::Id: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.id, f)
    }
}

impl<E> Eq for Ext<E>
where
    E: EntityIdentity,
    E::Id: Eq,
{
}

impl<E> PartialEq for Ext<E>
where
    E: EntityIdentity,
    E::Id: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<E> FieldValue for Ext<E>
where
    E: EntityIdentity,
{
    fn to_value(&self) -> Value {
        self.as_value()
    }
}

impl<E> Hash for Ext<E>
where
    E: EntityIdentity,
    E::Id: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<E> Ord for Ext<E>
where
    E: EntityIdentity,
    E::Id: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl<E> PartialOrd for Ext<E>
where
    E: EntityIdentity,
    E::Id: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> Serialize for Ext<E>
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

impl<'de, E> Deserialize<'de> for Ext<E>
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

impl<T> SanitizeAuto for Ext<T> where T: EntityIdentity {}

impl<T> SanitizeCustom for Ext<T> where T: EntityIdentity {}

impl<T> UpdateView for Ext<T>
where
    T: EntityIdentity,
    T::Id: CandidType + Default,
{
    type UpdateViewType = Self;

    fn merge(&mut self, update: Self::UpdateViewType) {
        *self = update;
    }
}

impl<T> ValidateAuto for Ext<T> where T: EntityIdentity {}

impl<T> ValidateCustom for Ext<T> where T: EntityIdentity {}

impl<E> View for Ext<E>
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

impl<T> Visitable for Ext<T> where T: EntityIdentity {}
