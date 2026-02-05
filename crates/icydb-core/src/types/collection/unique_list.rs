use crate::{
    traits::{
        FieldValue, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom, View,
        Visitable,
    },
    value::Value,
    view::SetPatch,
    visitor::{VisitorContext, VisitorCore, VisitorMutCore, perform_visit, perform_visit_mut},
};
use candid::CandidType;
use derive_more::Deref;
use serde::{Deserialize, Deserializer, Serialize};
use std::hash::Hash;

///
/// UniqueList
///
/// Ordered list that enforces uniqueness on insertion.
/// Deterministic order is first-seen insertion order.
///

#[repr(transparent)]
#[derive(CandidType, Clone, Debug, Default, Deref, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct UniqueList<T>(Vec<T>);

impl<T> UniqueList<T> {
    /// Create an empty unique list.
    #[must_use]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Return the number of items in the list.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the list is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return an iterator over the list.
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }

    /// Return a mutable iterator over the list.
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, T> {
        self.0.iter_mut()
    }

    /// Clear all items from the list.
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl<T> UniqueList<T>
where
    T: Eq + Hash,
{
    /// Build a unique list, discarding later duplicates.
    #[must_use]
    pub fn from_vec(values: Vec<T>) -> Self {
        let mut list = Self::new();
        for value in values {
            list.insert(value);
        }

        list
    }

    /// Returns `true` if the list already contains the value.
    #[must_use]
    pub fn contains(&self, value: &T) -> bool {
        self.0.iter().any(|existing| existing == value)
    }

    /// Insert a value, returning `true` if it was newly inserted.
    pub fn insert(&mut self, value: T) -> bool {
        if self.contains(&value) {
            return false;
        }

        self.0.push(value);

        true
    }

    /// Remove a value, returning `true` if it was present.
    pub fn remove(&mut self, value: &T) -> bool {
        if let Some(index) = self.0.iter().position(|existing| existing == value) {
            self.0.remove(index);
            return true;
        }

        false
    }
}

impl<T> IntoIterator for UniqueList<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a UniqueList<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut UniqueList<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

impl<T: FieldValue> FieldValue for UniqueList<T> {
    fn to_value(&self) -> Value {
        Value::List(self.0.iter().map(FieldValue::to_value).collect())
    }
}

impl<'de, T> Deserialize<'de> for UniqueList<T>
where
    T: Deserialize<'de> + Eq + Hash,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = Vec::<T>::deserialize(deserializer)?;

        Ok(Self::from_vec(values))
    }
}

impl<T: SanitizeAuto> SanitizeAuto for UniqueList<T> {
    fn sanitize_self(&mut self, ctx: &mut dyn VisitorContext) {
        for v in self.iter_mut() {
            v.sanitize_self(ctx);
        }
    }
}

impl<T: SanitizeCustom> SanitizeCustom for UniqueList<T> {
    fn sanitize_custom(&mut self, ctx: &mut dyn VisitorContext) {
        for v in self.iter_mut() {
            v.sanitize_custom(ctx);
        }
    }
}

impl<T: ValidateAuto> ValidateAuto for UniqueList<T> {
    fn validate_self(&self, ctx: &mut dyn VisitorContext) {
        for v in self {
            v.validate_self(ctx);
        }
    }
}

impl<T: ValidateCustom> ValidateCustom for UniqueList<T> {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        for v in self {
            v.validate_custom(ctx);
        }
    }
}

impl<T: Visitable> Visitable for UniqueList<T> {
    fn drive(&self, visitor: &mut dyn VisitorCore) {
        for (i, value) in self.iter().enumerate() {
            perform_visit(visitor, value, i);
        }
    }

    fn drive_mut(&mut self, visitor: &mut dyn VisitorMutCore) {
        for (i, value) in self.iter_mut().enumerate() {
            perform_visit_mut(visitor, value, i);
        }
    }
}

impl<T> View for UniqueList<T>
where
    T: View + Eq + Hash,
{
    type ViewType = Vec<T::ViewType>;

    fn to_view(&self) -> Self::ViewType {
        self.iter().map(View::to_view).collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self::from_vec(view.into_iter().map(T::from_view).collect())
    }
}

impl<T> UpdateView for UniqueList<T>
where
    T: UpdateView + Default + Eq + Hash,
{
    type UpdateViewType = Vec<SetPatch<T::UpdateViewType>>;

    fn merge(&mut self, patches: Self::UpdateViewType) {
        for patch in patches {
            match patch {
                SetPatch::Insert(value) => {
                    let mut elem = T::default();
                    elem.merge(value);
                    self.insert(elem);
                }
                SetPatch::Remove(value) => {
                    let mut elem = T::default();
                    elem.merge(value);
                    self.remove(&elem);
                }
                SetPatch::Overwrite { values } => {
                    self.clear();

                    for value in values {
                        let mut elem = T::default();
                        elem.merge(value);
                        self.insert(elem);
                    }
                }
                SetPatch::Clear => self.clear(),
            }
        }
    }
}
