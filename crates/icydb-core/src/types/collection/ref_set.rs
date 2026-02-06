use crate::{
    traits::{
        EntityStorageKey, FieldValue, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto,
        ValidateCustom, View, Visitable,
    },
    types::Ref,
    value::Value,
    view::SetPatch,
    visitor::{VisitorContext, VisitorCore, VisitorMutCore, perform_visit},
};
use candid::CandidType;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

///
/// RefSet
///
/// Canonical set of typed entity references.
///
/// - Uniqueness is enforced by `E::Key`.
/// - Ordering is canonical (ascending by key) and does NOT reflect insertion history.
/// - No ordering-based or predicate-based mutation APIs are provided.
/// - In-place mutation of elements is forbidden to preserve ordering invariants.
///

#[repr(transparent)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RefSet<E: EntityStorageKey>(Vec<Ref<E>>);

impl<E> RefSet<E>
where
    E: EntityStorageKey,
{
    /// Create an empty ref set.
    #[must_use]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Build a ref set, discarding duplicate keys.
    #[must_use]
    pub fn from_refs(refs: Vec<Ref<E>>) -> Self {
        let mut set = Self::new();
        for reference in refs {
            set.insert(reference);
        }

        set
    }

    /// Return the number of references in the set.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the set is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return an iterator over the references.
    pub fn iter(&self) -> std::slice::Iter<'_, Ref<E>> {
        self.0.iter()
    }

    /// Insert a reference, returning `true` if it was newly inserted.
    pub fn insert(&mut self, reference: Ref<E>) -> bool {
        let key = reference.key();

        match self.find_index(&key) {
            Ok(_) => false,
            Err(index) => {
                self.0.insert(index, reference);
                true
            }
        }
    }

    /// Remove a reference by key, returning `true` if it was present.
    pub fn remove(&mut self, reference: &Ref<E>) -> bool {
        let key = reference.key();

        match self.find_index(&key) {
            Ok(index) => {
                self.0.remove(index);
                true
            }
            Err(_) => false,
        }
    }

    /// Returns `true` if the set contains the reference.
    #[must_use]
    pub fn contains(&self, reference: &Ref<E>) -> bool {
        self.contains_key(&reference.key())
    }

    /// Returns `true` if the set contains the key.
    #[must_use]
    fn contains_key(&self, key: &E::Key) -> bool {
        self.find_index(key).is_ok()
    }

    /// Clear all references from the set.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    // Locate a key in the sorted list.
    fn find_index(&self, key: &E::Key) -> Result<usize, usize> {
        self.0
            .binary_search_by(|candidate| candidate.key().cmp(key))
    }

    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    fn assert_sorted(&self) {
        debug_assert!(self.0.windows(2).all(|w| { w[0].key() < w[1].key() }));
    }
}

impl<E> RefSet<E>
where
    E: EntityStorageKey,
    Ref<E>: UpdateView + Default,
{
    /// Apply set patches, enforcing key uniqueness and deterministic ordering.
    pub fn apply_patches(
        &mut self,
        patches: Vec<SetPatch<<Ref<E> as UpdateView>::UpdateViewType>>,
    ) {
        self.merge(patches);
    }
}

impl<E> CandidType for RefSet<E>
where
    E: EntityStorageKey,
    E::Key: CandidType,
{
    fn _ty() -> candid::types::Type {
        <Vec<Ref<E>> as CandidType>::_ty()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        <Vec<Ref<E>> as CandidType>::idl_serialize(&self.0, serializer)
    }
}

impl<E> IntoIterator for RefSet<E>
where
    E: EntityStorageKey,
{
    type Item = Ref<E>;
    type IntoIter = std::vec::IntoIter<Ref<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, E> IntoIterator for &'a RefSet<E>
where
    E: EntityStorageKey,
{
    type Item = &'a Ref<E>;
    type IntoIter = std::slice::Iter<'a, Ref<E>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<E> FieldValue for RefSet<E>
where
    E: EntityStorageKey,
{
    fn to_value(&self) -> Value {
        Value::List(self.0.iter().map(FieldValue::to_value).collect())
    }
}

impl<E> SanitizeAuto for RefSet<E> where E: EntityStorageKey {}

impl<E> SanitizeCustom for RefSet<E> where E: EntityStorageKey {}

impl<E> Serialize for RefSet<E>
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

impl<'de, E> Deserialize<'de> for RefSet<E>
where
    E: EntityStorageKey,
    Ref<E>: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let refs = Vec::<Ref<E>>::deserialize(deserializer)?;

        Ok(Self::from_refs(refs))
    }
}

impl<E> ValidateAuto for RefSet<E>
where
    E: EntityStorageKey,
{
    fn validate_self(&self, ctx: &mut dyn VisitorContext) {
        for value in self {
            value.validate_self(ctx);
        }
    }
}

impl<E> ValidateCustom for RefSet<E>
where
    E: EntityStorageKey,
{
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        for value in self {
            value.validate_custom(ctx);
        }
    }
}

impl<E> Visitable for RefSet<E>
where
    E: EntityStorageKey,
{
    fn drive(&self, visitor: &mut dyn VisitorCore) {
        for (i, value) in self.iter().enumerate() {
            perform_visit(visitor, value, i);
        }
    }

    fn drive_mut(&mut self, _visitor: &mut dyn VisitorMutCore) {
        // Intentionally empty: mutating references can invalidate key ordering.
    }
}

impl<E> View for RefSet<E>
where
    E: EntityStorageKey,
    Ref<E>: View,
{
    type ViewType = Vec<<Ref<E> as View>::ViewType>;

    fn to_view(&self) -> Self::ViewType {
        self.iter().map(View::to_view).collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self::from_refs(view.into_iter().map(Ref::<E>::from_view).collect())
    }
}

impl<E> UpdateView for RefSet<E>
where
    E: EntityStorageKey,
    Ref<E>: UpdateView + Default,
{
    type UpdateViewType = Vec<SetPatch<<Ref<E> as UpdateView>::UpdateViewType>>;

    fn merge(&mut self, patches: Self::UpdateViewType) {
        for patch in patches {
            match patch {
                SetPatch::Insert(value) => {
                    let mut reference = Ref::<E>::default();
                    reference.merge(value);
                    self.insert(reference);
                }
                SetPatch::Remove(value) => {
                    let mut reference = Ref::<E>::default();
                    reference.merge(value);
                    self.remove(&reference);
                }
                SetPatch::Overwrite { values } => {
                    self.clear();

                    for value in values {
                        let mut reference = Ref::<E>::default();
                        reference.merge(value);
                        self.insert(reference);
                    }
                }
                SetPatch::Clear => self.clear(),
            }
        }
    }
}
