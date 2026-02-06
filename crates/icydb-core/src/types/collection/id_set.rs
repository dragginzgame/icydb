use crate::{
    traits::{
        EntityStorageKey, FieldValue, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
    types::Id,
    value::Value,
    view::SetPatch,
    visitor::{VisitorContext, VisitorCore, VisitorMutCore, perform_visit},
};
use candid::CandidType;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

///
/// IdSet
///
/// Canonical set of typed entity identities.
///
/// - Uniqueness is enforced by entity identity ordering (`E::Key`).
/// - Ordering is canonical (ascending by identity) and does NOT reflect insertion history.
/// - No ordering-based or predicate-based mutation APIs are provided.
/// - In-place mutation of elements is forbidden to preserve ordering invariants.
/// - This type represents *identity only*; it does not imply resolvability or existence checks.
///

#[repr(transparent)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IdSet<E: EntityStorageKey>(Vec<Id<E>>);

impl<E> IdSet<E>
where
    E: EntityStorageKey,
{
    /// Create an empty identity set.
    #[must_use]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Build an identity set, discarding duplicate identities.
    #[must_use]
    pub fn from_ids(ids: Vec<Id<E>>) -> Self {
        let mut set = Self::new();
        for id in ids {
            set.insert(id);
        }
        set
    }

    /// Return the number of identities in the set.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the set is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return an iterator over the identities.
    pub fn iter(&self) -> std::slice::Iter<'_, Id<E>> {
        self.0.iter()
    }

    /// Insert an identity, returning `true` if it was newly inserted.
    pub fn insert(&mut self, id: Id<E>) -> bool {
        let key = id.into_storage_key();

        match self.find_index(&key) {
            Ok(_) => false,
            Err(index) => {
                self.0.insert(index, id);
                true
            }
        }
    }

    /// Remove an identity, returning `true` if it was present.
    pub fn remove(&mut self, id: &Id<E>) -> bool {
        let key = id.into_storage_key();

        match self.find_index(&key) {
            Ok(index) => {
                self.0.remove(index);
                true
            }
            Err(_) => false,
        }
    }

    /// Returns `true` if the set contains the identity.
    #[must_use]
    pub fn contains(&self, id: &Id<E>) -> bool {
        self.contains_key(&id.into_storage_key())
    }

    /// Returns `true` if the set contains the given storage key.
    #[must_use]
    fn contains_key(&self, key: &E::Key) -> bool {
        self.find_index(key).is_ok()
    }

    /// Clear all identities from the set.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Locate a key in the sorted identity list.
    fn find_index(&self, key: &E::Key) -> Result<usize, usize> {
        self.0
            .binary_search_by(|candidate| candidate.into_storage_key().cmp(key))
    }

    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    fn assert_sorted(&self) {
        debug_assert!(
            self.0
                .windows(2)
                .all(|w| w[0].into_storage_key() < w[1].into_storage_key())
        );
    }
}

impl<E> IdSet<E>
where
    E: EntityStorageKey,
    Id<E>: UpdateView + Default,
{
    /// Apply set patches, enforcing identity uniqueness and deterministic ordering.
    pub fn apply_patches(&mut self, patches: Vec<SetPatch<<Id<E> as UpdateView>::UpdateViewType>>) {
        self.merge(patches);
    }
}

impl<E> CandidType for IdSet<E>
where
    E: EntityStorageKey,
    E::Key: CandidType,
{
    fn _ty() -> candid::types::Type {
        <Vec<Id<E>> as CandidType>::_ty()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        <Vec<Id<E>> as CandidType>::idl_serialize(&self.0, serializer)
    }
}

impl<E> IntoIterator for IdSet<E>
where
    E: EntityStorageKey,
{
    type Item = Id<E>;
    type IntoIter = std::vec::IntoIter<Id<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, E> IntoIterator for &'a IdSet<E>
where
    E: EntityStorageKey,
{
    type Item = &'a Id<E>;
    type IntoIter = std::slice::Iter<'a, Id<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<E> FieldValue for IdSet<E>
where
    E: EntityStorageKey,
{
    fn to_value(&self) -> Value {
        Value::List(self.0.iter().map(FieldValue::to_value).collect())
    }
}

impl<E> SanitizeAuto for IdSet<E> where E: EntityStorageKey {}
impl<E> SanitizeCustom for IdSet<E> where E: EntityStorageKey {}

impl<E> Serialize for IdSet<E>
where
    E: EntityStorageKey,
    E::Key: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de, E> Deserialize<'de> for IdSet<E>
where
    E: EntityStorageKey,
    Id<E>: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ids = Vec::<Id<E>>::deserialize(deserializer)?;
        Ok(Self::from_ids(ids))
    }
}

impl<E> ValidateAuto for IdSet<E>
where
    E: EntityStorageKey,
{
    fn validate_self(&self, ctx: &mut dyn VisitorContext) {
        for id in self {
            id.validate_self(ctx);
        }
    }
}

impl<E> ValidateCustom for IdSet<E>
where
    E: EntityStorageKey,
{
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        for id in self {
            id.validate_custom(ctx);
        }
    }
}

impl<E> Visitable for IdSet<E>
where
    E: EntityStorageKey,
{
    fn drive(&self, visitor: &mut dyn VisitorCore) {
        for (i, id) in self.iter().enumerate() {
            perform_visit(visitor, id, i);
        }
    }

    fn drive_mut(&mut self, _visitor: &mut dyn VisitorMutCore) {
        // Intentionally empty: mutating identities can invalidate canonical ordering.
    }
}

impl<E> View for IdSet<E>
where
    E: EntityStorageKey,
    Id<E>: View,
{
    type ViewType = Vec<<Id<E> as View>::ViewType>;

    fn to_view(&self) -> Self::ViewType {
        self.iter().map(View::to_view).collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self::from_ids(view.into_iter().map(Id::<E>::from_view).collect())
    }
}

impl<E> UpdateView for IdSet<E>
where
    E: EntityStorageKey,
    Id<E>: UpdateView + Default,
{
    type UpdateViewType = Vec<SetPatch<<Id<E> as UpdateView>::UpdateViewType>>;

    fn merge(&mut self, patches: Self::UpdateViewType) {
        for patch in patches {
            match patch {
                SetPatch::Insert(value) => {
                    let mut id = Id::<E>::default();
                    id.merge(value);
                    self.insert(id);
                }
                SetPatch::Remove(value) => {
                    let mut id = Id::<E>::default();
                    id.merge(value);
                    self.remove(&id);
                }
                SetPatch::Overwrite { values } => {
                    self.clear();
                    for value in values {
                        let mut id = Id::<E>::default();
                        id.merge(value);
                        self.insert(id);
                    }
                }
                SetPatch::Clear => self.clear(),
            }
        }
    }
}
