use crate::{
    traits::{
        FieldValue, SanitizeAuto, SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom, View,
        Visitable,
    },
    value::Value,
    view::MapPatch,
    visitor::{VisitorContext, VisitorCore, VisitorMutCore, perform_visit, perform_visit_mut},
};
use candid::CandidType;
use derive_more::Deref;
use serde::{Deserialize, Deserializer, Serialize};

///
/// KeyedList
///
/// Deterministic key-ordered list of `(K, V)` entries.
/// Enforces unique keys and sorts by ascending key order.
///

#[derive(CandidType, Clone, Debug, Default, Deref, Eq, PartialEq, Serialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct KeyedList<K, V>(Vec<(K, V)>);

impl<K, V> KeyedList<K, V> {
    /// Create an empty keyed list.
    #[must_use]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Return the number of entries in the list.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the list is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return an iterator over `(key, value)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.0.iter().map(|(k, v)| (k, v))
    }

    /// Return a mutable iterator over `(key, value)` pairs.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        self.0.iter_mut().map(|(k, v)| (&*k, v))
    }

    /// Clear all entries from the list.
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl<K, V> KeyedList<K, V>
where
    K: Ord,
{
    /// Build a keyed list, keeping the last value for each key.
    #[must_use]
    pub fn from_vec(entries: Vec<(K, V)>) -> Self {
        let mut list = Self::new();
        for (key, value) in entries {
            list.insert(key, value);
        }

        list
    }

    /// Return a reference to the value for `key` if present.
    #[must_use]
    pub fn get(&self, key: &K) -> Option<&V> {
        self.find_index(key).ok().map(|idx| &self.0[idx].1)
    }

    /// Return a mutable reference to the value for `key` if present.
    #[must_use]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.find_index(key).ok().map(|idx| &mut self.0[idx].1)
    }

    /// Insert or replace a value for `key`, returning the old value if present.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        match self.find_index(&key) {
            Ok(index) => Some(std::mem::replace(&mut self.0[index].1, value)),
            Err(index) => {
                self.0.insert(index, (key, value));
                None
            }
        }
    }

    /// Remove the entry for `key`, returning the value if present.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        match self.find_index(key) {
            Ok(index) => Some(self.0.remove(index).1),
            Err(_) => None,
        }
    }

    /// Returns `true` if the list contains `key`.
    #[must_use]
    pub fn contains_key(&self, key: &K) -> bool {
        self.find_index(key).is_ok()
    }

    // Locate a key in the sorted list.
    fn find_index(&self, key: &K) -> Result<usize, usize> {
        self.0.binary_search_by(|(candidate, _)| candidate.cmp(key))
    }
}

impl<K, V> IntoIterator for KeyedList<K, V> {
    type Item = (K, V);
    type IntoIter = std::vec::IntoIter<(K, V)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, K, V> IntoIterator for &'a KeyedList<K, V> {
    type Item = &'a (K, V);
    type IntoIter = std::slice::Iter<'a, (K, V)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<K, V> FieldValue for KeyedList<K, V>
where
    K: FieldValue,
    V: FieldValue,
{
    fn to_value(&self) -> Value {
        let entries = self
            .0
            .iter()
            .map(|(k, v)| Value::List(vec![k.to_value(), v.to_value()]))
            .collect();

        Value::List(entries)
    }
}

impl<'de, K, V> Deserialize<'de> for KeyedList<K, V>
where
    K: Deserialize<'de> + Ord,
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<(K, V)>::deserialize(deserializer)?;

        Ok(Self::from_vec(entries))
    }
}

impl<K, V> SanitizeAuto for KeyedList<K, V>
where
    V: SanitizeAuto,
{
    fn sanitize_self(&mut self, ctx: &mut dyn VisitorContext) {
        // Keys are intentionally skipped to preserve key ordering.
        for (_key, value) in self.iter_mut() {
            value.sanitize_self(ctx);
        }
    }
}

impl<K, V> SanitizeCustom for KeyedList<K, V>
where
    V: SanitizeCustom,
{
    fn sanitize_custom(&mut self, ctx: &mut dyn VisitorContext) {
        // Keys are intentionally skipped to preserve key ordering.
        for (_key, value) in self.iter_mut() {
            value.sanitize_custom(ctx);
        }
    }
}

impl<K, V> ValidateAuto for KeyedList<K, V>
where
    K: ValidateAuto,
    V: ValidateAuto,
{
    fn validate_self(&self, ctx: &mut dyn VisitorContext) {
        for (key, value) in self.iter() {
            key.validate_self(ctx);
            value.validate_self(ctx);
        }
    }
}

impl<K, V> ValidateCustom for KeyedList<K, V>
where
    K: ValidateCustom,
    V: ValidateCustom,
{
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        for (key, value) in self.iter() {
            key.validate_custom(ctx);
            value.validate_custom(ctx);
        }
    }
}

impl<K, V> Visitable for KeyedList<K, V>
where
    K: Visitable,
    V: Visitable,
{
    fn drive(&self, visitor: &mut dyn VisitorCore) {
        for (i, (key, value)) in self.iter().enumerate() {
            perform_visit(visitor, key, i);
            perform_visit(visitor, value, i);
        }
    }

    fn drive_mut(&mut self, visitor: &mut dyn VisitorMutCore) {
        // Keys are intentionally skipped to preserve key ordering.
        for (i, (_key, value)) in self.iter_mut().enumerate() {
            perform_visit_mut(visitor, value, i);
        }
    }
}

impl<K, V> View for KeyedList<K, V>
where
    K: View + Ord,
    V: View,
{
    type ViewType = Vec<(K::ViewType, V::ViewType)>;

    fn to_view(&self) -> Self::ViewType {
        self.iter()
            .map(|(key, value)| (key.to_view(), value.to_view()))
            .collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        Self::from_vec(
            view.into_iter()
                .map(|(key, value)| (K::from_view(key), V::from_view(value)))
                .collect(),
        )
    }
}

impl<K, V> UpdateView for KeyedList<K, V>
where
    K: UpdateView + Default + Ord,
    V: UpdateView + Default,
{
    type UpdateViewType = Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>;

    fn merge(&mut self, patches: Self::UpdateViewType) {
        for patch in patches {
            match patch {
                MapPatch::Upsert { key, value } => {
                    let mut key_value = K::default();
                    key_value.merge(key);

                    if let Some(existing) = self.get_mut(&key_value) {
                        existing.merge(value);
                    } else {
                        let mut value_value = V::default();
                        value_value.merge(value);
                        self.insert(key_value, value_value);
                    }
                }
                MapPatch::Remove { key } => {
                    let mut key_value = K::default();
                    key_value.merge(key);
                    self.remove(&key_value);
                }
                MapPatch::Overwrite { entries } => {
                    self.clear();

                    for (key, value) in entries {
                        let mut key_value = K::default();
                        key_value.merge(key);

                        let mut value_value = V::default();
                        value_value.merge(value);
                        self.insert(key_value, value_value);
                    }
                }
                MapPatch::Clear => self.clear(),
            }
        }
    }
}
