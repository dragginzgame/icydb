use crate::{
    traits::{
        AsView, EntityKey, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom,
        ViewPatchError, Visitable,
    },
    types::Id,
    view::SetPatch,
    visitor::{VisitorContext, VisitorCore, VisitorMutCore, perform_visit},
};
use candid::CandidType;
use serde::{Deserialize, Deserializer};

///
/// IdSet
///
/// Canonical set of typed primary-key values.
///
/// - Uniqueness is enforced by primary-key ordering (`E::Key`).
/// - Ordering is canonical (ascending by key) and does NOT reflect insertion history.
/// - No ordering-based or predicate-based mutation APIs are provided.
/// - In-place mutation of elements is forbidden to preserve ordering invariants.
/// - This type stores primary-key values only; it does not imply resolvability or existence checks.
/// - IDs in this set are public identifiers and do not grant authorization or ownership.
///

#[repr(transparent)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IdSet<E: EntityKey>(Vec<Id<E>>);

impl<E> IdSet<E>
where
    E: EntityKey,
{
    /// Create an empty primary-key set.
    #[must_use]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Build a primary-key set, discarding duplicates.
    #[must_use]
    pub fn from_ids(ids: Vec<Id<E>>) -> Self {
        let mut set = Self::new();
        for id in ids {
            set.insert(id);
        }
        set
    }

    /// Return the number of primary-key values in the set.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the set is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return an iterator over primary-key values.
    pub fn iter(&self) -> std::slice::Iter<'_, Id<E>> {
        self.0.iter()
    }

    /// Insert a primary-key value, returning `true` if it was newly inserted.
    pub fn insert(&mut self, id: Id<E>) -> bool {
        let key = id.key();

        match self.find_index(&key) {
            Ok(_) => false,
            Err(index) => {
                self.0.insert(index, id);
                true
            }
        }
    }

    /// Remove a primary-key value, returning `true` if it was present.
    pub fn remove(&mut self, id: &Id<E>) -> bool {
        let key = id.key();

        match self.find_index(&key) {
            Ok(index) => {
                self.0.remove(index);
                true
            }
            Err(_) => false,
        }
    }

    /// Returns `true` if the set contains the primary-key value.
    #[must_use]
    pub fn contains(&self, id: &Id<E>) -> bool {
        self.contains_key(&id.key())
    }

    /// Returns `true` if the set contains the given storage key.
    #[must_use]
    fn contains_key(&self, key: &E::Key) -> bool {
        self.find_index(key).is_ok()
    }

    /// Clear all primary-key values from the set.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Locate a key in the sorted identity list.
    fn find_index(&self, key: &E::Key) -> Result<usize, usize> {
        self.0
            .binary_search_by(|candidate| candidate.key().cmp(key))
    }

    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    fn assert_sorted(&self) {
        debug_assert!(self.0.windows(2).all(|w| w[0].key() < w[1].key()));
    }
}

impl<E> IdSet<E>
where
    E: EntityKey,
    Id<E>: UpdateView + Default,
{
    /// Apply set patches, enforcing primary-key uniqueness and deterministic ordering.
    pub fn apply_patches(
        &mut self,
        patches: Vec<SetPatch<<Id<E> as UpdateView>::UpdateViewType>>,
    ) -> Result<(), ViewPatchError> {
        self.merge(patches)
    }
}

impl<E> AsView for IdSet<E>
where
    E: EntityKey,
    Id<E>: AsView,
{
    type ViewType = Vec<<Id<E> as AsView>::ViewType>;

    fn as_view(&self) -> Self::ViewType {
        self.iter().map(AsView::as_view).collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self::from_ids(view.into_iter().map(Id::<E>::from_view).collect())
    }
}

impl<E> CandidType for IdSet<E>
where
    E: EntityKey,
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
    E: EntityKey,
{
    type Item = Id<E>;
    type IntoIter = std::vec::IntoIter<Id<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, E> IntoIterator for &'a IdSet<E>
where
    E: EntityKey,
{
    type Item = &'a Id<E>;
    type IntoIter = std::slice::Iter<'a, Id<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<E> SanitizeAuto for IdSet<E> where E: EntityKey {}
impl<E> SanitizeCustom for IdSet<E> where E: EntityKey {}

impl<'de, E> Deserialize<'de> for IdSet<E>
where
    E: EntityKey,
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
    E: EntityKey,
{
    fn validate_self(&self, ctx: &mut dyn VisitorContext) {
        for id in self {
            id.validate_self(ctx);
        }
    }
}

impl<E> ValidateCustom for IdSet<E>
where
    E: EntityKey,
{
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        for id in self {
            id.validate_custom(ctx);
        }
    }
}

impl<E> Visitable for IdSet<E>
where
    E: EntityKey,
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

impl<E> UpdateView for IdSet<E>
where
    E: EntityKey,
    Id<E>: UpdateView + Default,
{
    type UpdateViewType = Vec<SetPatch<<Id<E> as UpdateView>::UpdateViewType>>;

    fn merge(
        &mut self,
        patches: Self::UpdateViewType,
    ) -> Result<(), crate::traits::ViewPatchError> {
        for patch in patches {
            match patch {
                SetPatch::Insert(value) => {
                    let mut id = Id::<E>::default();
                    id.merge(value).map_err(|err| err.with_field("insert"))?;
                    self.insert(id);
                }
                SetPatch::Remove(value) => {
                    let mut id = Id::<E>::default();
                    id.merge(value).map_err(|err| err.with_field("remove"))?;
                    self.remove(&id);
                }
                SetPatch::Overwrite { values } => {
                    self.clear();
                    for (index, value) in values.into_iter().enumerate() {
                        let mut id = Id::<E>::default();
                        id.merge(value)
                            .map_err(|err| err.with_field("overwrite").with_index(index))?;
                        self.insert(id);
                    }
                }
                SetPatch::Clear => self.clear(),
            }
        }

        Ok(())
    }
}
