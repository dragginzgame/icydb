use candid::CandidType;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    fmt,
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
pub struct Ext<E, Id> {
    id: Id,
    _marker: PhantomData<fn() -> E>,
}

impl<E, Id> Ext<E, Id> {
    /// Construct an external reference from an identity value.
    #[must_use]
    pub const fn new(id: Id) -> Self {
        Self {
            id,
            _marker: PhantomData,
        }
    }

    /// Return the underlying identity.
    #[must_use]
    pub fn id(self) -> Id {
        self.id
    }

    /// Alias for symmetry with `Ref`.
    #[must_use]
    pub fn key(self) -> Id {
        self.id
    }
}

impl<E, Id> Copy for Ext<E, Id> where Id: Copy {}

#[allow(clippy::expl_impl_clone_on_copy)]
impl<E, Id> Clone for Ext<E, Id>
where
    Id: Copy,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<E, Id> fmt::Debug for Ext<E, Id>
where
    Id: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Ext").field(&self.id).finish()
    }
}

impl<E, Id> PartialEq for Ext<E, Id>
where
    Id: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<E, Id> Eq for Ext<E, Id> where Id: Eq {}

impl<E, Id> Hash for Ext<E, Id>
where
    Id: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<E, Id> Ord for Ext<E, Id>
where
    Id: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl<E, Id> PartialOrd for Ext<E, Id>
where
    Id: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<E, Id> Serialize for Ext<E, Id>
where
    Id: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.id.serialize(serializer)
    }
}

impl<'de, E, Id> Deserialize<'de> for Ext<E, Id>
where
    Id: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let id = Id::deserialize(deserializer)?;
        Ok(Self::new(id))
    }
}

impl<E, Id> CandidType for Ext<E, Id>
where
    Id: CandidType,
{
    fn _ty() -> candid::types::Type {
        <Id as CandidType>::_ty()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        self.id.idl_serialize(serializer)
    }
}

impl<E, Id> fmt::Display for Ext<E, Id>
where
    Id: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}
