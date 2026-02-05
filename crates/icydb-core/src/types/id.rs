use crate::{
    traits::{
        EntityIdentity, FieldValue, Identifies, Inner, SanitizeAuto, SanitizeCustom, UpdateView,
        ValidateAuto, ValidateCustom, View, Visitable,
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
/// Serializes identically to `E::Id`.
///

#[repr(transparent)]
pub struct Id<E: EntityIdentity> {
    id: E::Id,
    _marker: PhantomData<fn() -> E>,
}

impl<E> Id<E>
where
    E: EntityIdentity,
{
    /// Construct a typed identity from the raw key value.
    #[must_use]
    pub const fn new(id: E::Id) -> Self {
        Self {
            id,
            _marker: PhantomData,
        }
    }

    /// Borrow the underlying key.
    #[must_use]
    pub const fn key(&self) -> &E::Id {
        &self.id
    }

    /// Consume into the raw key.
    #[must_use]
    pub const fn into_inner(self) -> E::Id {
        self.id
    }
}

impl<E> CandidType for Id<E>
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

#[allow(clippy::expl_impl_clone_on_copy)]
impl<E> Clone for Id<E>
where
    E: EntityIdentity,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<E> Copy for Id<E> where E: EntityIdentity {}

impl<E> fmt::Debug for Id<E>
where
    E: EntityIdentity,
    E::Id: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Id").field(&self.id).finish()
    }
}

impl<E> Default for Id<E>
where
    E: EntityIdentity,
    E::Id: Default,
{
    fn default() -> Self {
        Self::new(E::Id::default())
    }
}

impl<E> fmt::Display for Id<E>
where
    E: EntityIdentity,
    E::Id: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}

impl<E> Eq for Id<E>
where
    E: EntityIdentity,
    E::Id: Eq,
{
}

impl<E> PartialEq for Id<E>
where
    E: EntityIdentity,
    E::Id: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<E> From<Id<E>> for Ref<E>
where
    E: EntityIdentity,
{
    fn from(id: Id<E>) -> Self {
        Self::new(id.id)
    }
}

impl<E> Hash for Id<E>
where
    E: EntityIdentity,
    E::Id: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<E> Ord for Id<E>
where
    E: EntityIdentity,
    E::Id: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl<E> PartialOrd for Id<E>
where
    E: EntityIdentity,
    E::Id: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> FieldValue for Id<E>
where
    E: EntityIdentity,
    E::Id: FieldValue,
{
    fn to_value(&self) -> Value {
        self.id.to_value()
    }

    fn from_value(value: &Value) -> Option<Self> {
        let id = E::Id::from_value(value)?;

        Some(Self::new(id))
    }
}

impl<E> Inner<E::Id> for Id<E>
where
    E: EntityIdentity,
{
    fn inner(&self) -> &E::Id {
        &self.id
    }

    fn into_inner(self) -> E::Id {
        self.id
    }
}

impl<E> SanitizeAuto for Id<E> where E: EntityIdentity {}

impl<E> SanitizeCustom for Id<E> where E: EntityIdentity {}

impl<E> Serialize for Id<E>
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

impl<'de, E> Deserialize<'de> for Id<E>
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

impl<E> UpdateView for Id<E>
where
    E: EntityIdentity,
    E::Id: CandidType + Default,
{
    type UpdateViewType = E::Id;

    fn merge(&mut self, update: Self::UpdateViewType) {
        self.id = update;
    }
}

impl<E> ValidateAuto for Id<E> where E: EntityIdentity {}

impl<E> ValidateCustom for Id<E> where E: EntityIdentity {}

impl<E> View for Id<E>
where
    E: EntityIdentity,
    E::Id: Copy + Default,
{
    type ViewType = E::Id;

    fn to_view(&self) -> Self::ViewType {
        self.id
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self::new(view)
    }
}

impl<E> Visitable for Id<E> where E: EntityIdentity {}

impl<E> Identifies<E> for Id<E>
where
    E: EntityIdentity,
{
    fn key(&self) -> E::Id {
        self.id
    }
}
