use crate::{
    traits::{
        AsView, FieldValue, FieldValueKind, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto,
        ValidateCustom, Visitable,
    },
    value::Value,
    view::ListPatch,
    visitor::{VisitorContext, VisitorCore, VisitorMutCore, perform_visit, perform_visit_mut},
};
use candid::CandidType;
use derive_more::Deref;
use serde::{Deserialize, Serialize};

///
/// OrderedList
///
/// Ordered, duplicate-friendly list used for many-cardinality fields.
/// Preserves insertion order and serializes identically to `Vec<T>`.
///
/// Mutation is explicit and positional; `OrderedList` does not expose
/// `DerefMut` to avoid accidental bypass of list semantics.
///

#[repr(transparent)]
#[derive(CandidType, Clone, Debug, Default, Deref, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct OrderedList<T>(Vec<T>);

impl<T> OrderedList<T> {
    /// Create an empty ordered list.
    #[must_use]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Build an ordered list from an existing vector.
    #[must_use]
    pub const fn from_vec(values: Vec<T>) -> Self {
        Self(values)
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

    /// Return the item at `index`, if it exists.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<&T> {
        self.0.get(index)
    }

    /// Return a mutable reference to the item at `index`, if it exists.
    #[must_use]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.0.get_mut(index)
    }

    /// Append an item to the list.
    pub fn push(&mut self, value: T) {
        self.0.push(value);
    }

    /// Remove and return the last item, if any.
    pub fn pop(&mut self) -> Option<T> {
        self.0.pop()
    }

    /// Insert an item at `index`, clamping out-of-bounds indices to the tail.
    pub fn insert(&mut self, index: usize, value: T) {
        let idx = index.min(self.0.len());
        self.0.insert(idx, value);
    }

    /// Remove and return the item at `index`, if it exists.
    pub fn remove(&mut self, index: usize) -> Option<T> {
        if index < self.0.len() {
            Some(self.0.remove(index))
        } else {
            None
        }
    }

    /// Clear all items from the list.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Retain only the items specified by the predicate.
    pub fn retain<F>(&mut self, predicate: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.0.retain(predicate);
    }
}

impl<T> OrderedList<T>
where
    T: UpdateView + Default,
{
    /// Apply positional patches using `ListPatch` semantics.
    pub fn apply_patches(
        &mut self,
        patches: Vec<ListPatch<T::UpdateViewType>>,
    ) -> Result<(), crate::traits::ViewPatchError> {
        self.merge(patches)
    }
}

impl<T: AsView> AsView for OrderedList<T> {
    type ViewType = Vec<T::ViewType>;

    fn as_view(&self) -> Self::ViewType {
        self.iter().map(AsView::as_view).collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self::from_vec(view.into_iter().map(T::from_view).collect())
    }
}

impl<T: FieldValue> FieldValue for OrderedList<T> {
    fn kind() -> FieldValueKind {
        FieldValueKind::Structured { queryable: true }
    }

    fn to_value(&self) -> Value {
        Value::List(self.0.iter().map(FieldValue::to_value).collect())
    }

    fn from_value(value: &Value) -> Option<Self> {
        let Value::List(values) = value else {
            return None;
        };

        let mut out = Vec::with_capacity(values.len());
        for value in values {
            out.push(T::from_value(value)?);
        }

        Some(Self(out))
    }
}

impl<T> From<Vec<T>> for OrderedList<T> {
    fn from(values: Vec<T>) -> Self {
        Self(values)
    }
}

impl<T> From<OrderedList<T>> for Vec<T> {
    fn from(values: OrderedList<T>) -> Self {
        values.0
    }
}

impl<T> IntoIterator for OrderedList<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a OrderedList<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut OrderedList<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

impl<T: SanitizeAuto> SanitizeAuto for OrderedList<T> {
    fn sanitize_self(&mut self, ctx: &mut dyn VisitorContext) {
        for v in self.iter_mut() {
            v.sanitize_self(ctx);
        }
    }
}

impl<T: SanitizeCustom> SanitizeCustom for OrderedList<T> {
    fn sanitize_custom(&mut self, ctx: &mut dyn VisitorContext) {
        for v in self.iter_mut() {
            v.sanitize_custom(ctx);
        }
    }
}

impl<T: ValidateAuto> ValidateAuto for OrderedList<T> {
    fn validate_self(&self, ctx: &mut dyn VisitorContext) {
        for v in self {
            v.validate_self(ctx);
        }
    }
}

impl<T: ValidateCustom> ValidateCustom for OrderedList<T> {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        for v in self {
            v.validate_custom(ctx);
        }
    }
}

impl<T: Visitable> Visitable for OrderedList<T> {
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

impl<T> UpdateView for OrderedList<T>
where
    T: UpdateView + Default,
{
    type UpdateViewType = Vec<ListPatch<T::UpdateViewType>>;

    fn merge(
        &mut self,
        patches: Self::UpdateViewType,
    ) -> Result<(), crate::traits::ViewPatchError> {
        for patch in patches {
            match patch {
                ListPatch::Update { index, patch } => {
                    if let Some(elem) = self.get_mut(index) {
                        elem.merge(patch)?;
                    }
                }
                ListPatch::Insert { index, value } => {
                    let mut elem = T::default();
                    elem.merge(value)?;
                    self.insert(index, elem);
                }
                ListPatch::Push { value } => {
                    let mut elem = T::default();
                    elem.merge(value)?;
                    self.push(elem);
                }
                ListPatch::Overwrite { values } => {
                    self.clear();

                    for value in values {
                        let mut elem = T::default();
                        elem.merge(value)?;
                        self.push(elem);
                    }
                }
                ListPatch::Remove { index } => {
                    self.remove(index);
                }
                ListPatch::Clear => self.clear(),
            }
        }

        Ok(())
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_list_patches_are_positional() {
        let mut list: OrderedList<u8> = vec![10, 20, 30].into();
        let patches = vec![
            ListPatch::Update {
                index: 1,
                patch: 99,
            },
            ListPatch::Insert {
                index: 1,
                value: 11,
            },
            ListPatch::Remove { index: 0 },
        ];

        list.apply_patches(patches)
            .expect("ordered list patch merge should succeed");

        let expected: OrderedList<u8> = vec![11, 99, 30].into();
        assert_eq!(list, expected);
    }

    #[test]
    fn ordered_list_retain_filters_items() {
        let mut list: OrderedList<u8> = vec![1, 2, 3, 4, 5].into();

        list.retain(|value| value % 2 == 0);

        let expected: OrderedList<u8> = vec![2, 4].into();
        assert_eq!(list, expected);
    }
}
