use crate::view::{ListPatch, MapPatch, SetPatch};
use candid::CandidType;
use std::{
    collections::{
        BTreeMap, BTreeSet, HashMap, HashSet, btree_map::Entry as BTreeMapEntry,
        hash_map::Entry as HashMapEntry,
    },
    hash::{BuildHasher, Hash},
    iter::IntoIterator,
};
use thiserror::Error as ThisError;

///
/// AsView
///
/// Recursive for all field/value nodes
/// `from_view` is infallible; view values are treated as canonical.
///

pub trait AsView: Sized {
    type ViewType: Default;

    fn as_view(&self) -> Self::ViewType;
    fn from_view(view: Self::ViewType) -> Self;
}

impl AsView for () {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {}

    fn from_view((): Self::ViewType) -> Self {}
}

impl AsView for String {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

// Make Box<T> *not* appear in the view type
impl<T: AsView> AsView for Box<T> {
    type ViewType = T::ViewType;

    fn as_view(&self) -> Self::ViewType {
        // Delegate to inner value
        T::as_view(self.as_ref())
    }

    fn from_view(view: Self::ViewType) -> Self {
        // Re-box after reconstructing inner
        Self::new(T::from_view(view))
    }
}

impl<T: AsView> AsView for Option<T> {
    type ViewType = Option<T::ViewType>;

    fn as_view(&self) -> Self::ViewType {
        self.as_ref().map(AsView::as_view)
    }

    fn from_view(view: Self::ViewType) -> Self {
        view.map(T::from_view)
    }
}

impl<T: AsView> AsView for Vec<T> {
    type ViewType = Vec<T::ViewType>;

    fn as_view(&self) -> Self::ViewType {
        self.iter().map(AsView::as_view).collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view.into_iter().map(T::from_view).collect()
    }
}

impl<T, S> AsView for HashSet<T, S>
where
    T: AsView + Eq + Hash + Clone,
    S: BuildHasher + Default,
{
    type ViewType = Vec<T::ViewType>;

    fn as_view(&self) -> Self::ViewType {
        self.iter().map(AsView::as_view).collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view.into_iter().map(T::from_view).collect()
    }
}

impl<K, V, S> AsView for HashMap<K, V, S>
where
    K: AsView + Eq + Hash + Clone,
    V: AsView,
    S: BuildHasher + Default,
{
    type ViewType = Vec<(K::ViewType, V::ViewType)>;

    fn as_view(&self) -> Self::ViewType {
        self.iter()
            .map(|(k, v)| (k.as_view(), v.as_view()))
            .collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view.into_iter()
            .map(|(k, v)| (K::from_view(k), V::from_view(v)))
            .collect()
    }
}

impl<T> AsView for BTreeSet<T>
where
    T: AsView + Ord + Clone,
{
    type ViewType = Vec<T::ViewType>;

    fn as_view(&self) -> Self::ViewType {
        self.iter().map(AsView::as_view).collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view.into_iter().map(T::from_view).collect()
    }
}

impl<K, V> AsView for BTreeMap<K, V>
where
    K: AsView + Ord + Clone,
    V: AsView,
{
    type ViewType = Vec<(K::ViewType, V::ViewType)>;

    fn as_view(&self) -> Self::ViewType {
        self.iter()
            .map(|(k, v)| (k.as_view(), v.as_view()))
            .collect()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view.into_iter()
            .map(|(k, v)| (K::from_view(k), V::from_view(v)))
            .collect()
    }
}

#[macro_export]
macro_rules! impl_view {
    ($($type:ty),*) => {
        $(
            impl AsView for $type {
                type ViewType = Self;

                fn as_view(&self) -> Self::ViewType {
                    *self
                }

                fn from_view(view: Self::ViewType) -> Self {
                    view
                }
            }
        )*
    };
}

impl_view!(bool, i8, i16, i32, i64, u8, u16, u32, u64);

impl AsView for f32 {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        if view.is_finite() {
            if view == 0.0 { 0.0 } else { view }
        } else {
            0.0
        }
    }
}

impl AsView for f64 {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        if view.is_finite() {
            if view == 0.0 { 0.0 } else { view }
        } else {
            0.0
        }
    }
}

///
/// CreateView
///

pub trait CreateView: AsView {
    /// Payload accepted when creating this value.
    ///
    /// This is often equal to ViewType, but may differ
    /// (e.g. Option<T>, defaults, omissions).
    type CreateViewType: CandidType + Default;

    fn from_create_view(view: Self::CreateViewType) -> Self;
}

///
/// ViewPatchError
///
/// Structured failures for user-driven patch application.
///
#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum ViewPatchError {
    #[error("invalid patch shape: expected {expected}, found {actual}")]
    InvalidPatchShape {
        expected: &'static str,
        actual: &'static str,
    },

    #[error("missing key for map operation: {operation}")]
    MissingKey { operation: &'static str },

    #[error("invalid patch cardinality: expected {expected}, found {actual}")]
    CardinalityViolation { expected: usize, actual: usize },
}

///
/// UpdateView
///

pub trait UpdateView: AsView {
    /// Payload accepted when updating this value.
    type UpdateViewType: CandidType + Default;

    /// Merge the update payload into self.
    fn merge(&mut self, _update: Self::UpdateViewType) -> Result<(), ViewPatchError> {
        Ok(())
    }
}

impl<T> UpdateView for Option<T>
where
    T: UpdateView + Default,
{
    type UpdateViewType = Option<T::UpdateViewType>;

    fn merge(&mut self, update: Self::UpdateViewType) -> Result<(), ViewPatchError> {
        match update {
            None => {
                // Field was provided (outer Some), inner None means explicit delete
                *self = None;
            }
            Some(inner_update) => {
                if let Some(inner_value) = self.as_mut() {
                    inner_value.merge(inner_update)?;
                } else {
                    let mut new_value = T::default();
                    new_value.merge(inner_update)?;
                    *self = Some(new_value);
                }
            }
        }

        Ok(())
    }
}

impl<T> UpdateView for Vec<T>
where
    T: UpdateView + Default,
{
    // Payload is T::UpdateViewType, which *is* CandidType
    type UpdateViewType = Vec<ListPatch<T::UpdateViewType>>;

    fn merge(&mut self, patches: Self::UpdateViewType) -> Result<(), ViewPatchError> {
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
                    let idx = index.min(self.len());
                    self.insert(idx, elem);
                }
                ListPatch::Push { value } => {
                    let mut elem = T::default();
                    elem.merge(value)?;
                    self.push(elem);
                }
                ListPatch::Overwrite { values } => {
                    self.clear();
                    self.reserve(values.len());

                    for value in values {
                        let mut elem = T::default();
                        elem.merge(value)?;
                        self.push(elem);
                    }
                }
                ListPatch::Remove { index } => {
                    if index < self.len() {
                        self.remove(index);
                    }
                }
                ListPatch::Clear => self.clear(),
            }
        }

        Ok(())
    }
}

impl<T, S> UpdateView for HashSet<T, S>
where
    T: UpdateView + Clone + Default + Eq + Hash,
    S: BuildHasher + Default,
{
    type UpdateViewType = Vec<SetPatch<T::UpdateViewType>>;

    fn merge(&mut self, patches: Self::UpdateViewType) -> Result<(), ViewPatchError> {
        for patch in patches {
            match patch {
                SetPatch::Insert(value) => {
                    let mut elem = T::default();
                    elem.merge(value)?;
                    self.insert(elem);
                }
                SetPatch::Remove(value) => {
                    let mut elem = T::default();
                    elem.merge(value)?;
                    self.remove(&elem);
                }
                SetPatch::Overwrite { values } => {
                    self.clear();

                    for value in values {
                        let mut elem = T::default();
                        elem.merge(value)?;
                        self.insert(elem);
                    }
                }
                SetPatch::Clear => self.clear(),
            }
        }

        Ok(())
    }
}

/// Internal representation used to normalize map patches before application.
enum MapPatchOp<K, V> {
    Insert { key: K, value: V },
    Remove { key: K },
    Replace { key: K, value: V },
    Clear,
}

impl<K, V, S> UpdateView for HashMap<K, V, S>
where
    K: UpdateView + Clone + Default + Eq + Hash,
    V: UpdateView + Default,
    S: BuildHasher + Default,
{
    type UpdateViewType = Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>;

    #[expect(clippy::too_many_lines)]
    fn merge(&mut self, patches: Self::UpdateViewType) -> Result<(), ViewPatchError> {
        // Phase 1: decode patch payload into concrete keys.
        let mut ops = Vec::with_capacity(patches.len());
        for patch in patches {
            match patch {
                MapPatch::Insert { key, value } => {
                    let mut key_value = K::default();
                    key_value.merge(key)?;
                    ops.push(MapPatchOp::Insert {
                        key: key_value,
                        value,
                    });
                }
                MapPatch::Remove { key } => {
                    let mut key_value = K::default();
                    key_value.merge(key)?;
                    ops.push(MapPatchOp::Remove { key: key_value });
                }
                MapPatch::Replace { key, value } => {
                    let mut key_value = K::default();
                    key_value.merge(key)?;
                    ops.push(MapPatchOp::Replace {
                        key: key_value,
                        value,
                    });
                }
                MapPatch::Clear => ops.push(MapPatchOp::Clear),
            }
        }

        // Phase 2: reject ambiguous patch batches to keep semantics deterministic.
        let mut saw_clear = false;
        let mut touched = HashSet::with_capacity(ops.len());
        for op in &ops {
            match op {
                MapPatchOp::Clear => {
                    if saw_clear {
                        return Err(ViewPatchError::InvalidPatchShape {
                            expected: "at most one Clear operation per map patch batch",
                            actual: "duplicate Clear operations",
                        });
                    }
                    saw_clear = true;
                    if ops.len() != 1 {
                        return Err(ViewPatchError::CardinalityViolation {
                            expected: 1,
                            actual: ops.len(),
                        });
                    }
                }
                MapPatchOp::Insert { key, .. }
                | MapPatchOp::Remove { key }
                | MapPatchOp::Replace { key, .. } => {
                    if saw_clear {
                        return Err(ViewPatchError::InvalidPatchShape {
                            expected: "Clear must be the only operation in a map patch batch",
                            actual: "Clear combined with key operation",
                        });
                    }
                    if !touched.insert(key.clone()) {
                        return Err(ViewPatchError::InvalidPatchShape {
                            expected: "unique key operations per map patch batch",
                            actual: "duplicate key operation",
                        });
                    }
                }
            }
        }
        if saw_clear {
            self.clear();
            return Ok(());
        }

        // Phase 3: apply deterministic map operations.
        for op in ops {
            match op {
                MapPatchOp::Insert { key, value } => match self.entry(key) {
                    HashMapEntry::Occupied(mut slot) => {
                        slot.get_mut().merge(value)?;
                    }
                    HashMapEntry::Vacant(slot) => {
                        let mut value_value = V::default();
                        value_value.merge(value)?;
                        slot.insert(value_value);
                    }
                },
                MapPatchOp::Remove { key } => {
                    if self.remove(&key).is_none() {
                        return Err(ViewPatchError::MissingKey {
                            operation: "remove",
                        });
                    }
                }
                MapPatchOp::Replace { key, value } => match self.entry(key) {
                    HashMapEntry::Occupied(mut slot) => {
                        slot.get_mut().merge(value)?;
                    }
                    HashMapEntry::Vacant(_) => {
                        return Err(ViewPatchError::MissingKey {
                            operation: "replace",
                        });
                    }
                },
                MapPatchOp::Clear => {
                    return Err(ViewPatchError::InvalidPatchShape {
                        expected: "Clear to be handled before apply phase",
                        actual: "Clear reached apply phase",
                    });
                }
            }
        }

        Ok(())
    }
}

impl<T> UpdateView for BTreeSet<T>
where
    T: UpdateView + Clone + Default + Ord,
{
    type UpdateViewType = Vec<SetPatch<T::UpdateViewType>>;

    fn merge(&mut self, patches: Self::UpdateViewType) -> Result<(), ViewPatchError> {
        for patch in patches {
            match patch {
                SetPatch::Insert(value) => {
                    let mut elem = T::default();
                    elem.merge(value)?;
                    self.insert(elem);
                }
                SetPatch::Remove(value) => {
                    let mut elem = T::default();
                    elem.merge(value)?;
                    self.remove(&elem);
                }
                SetPatch::Overwrite { values } => {
                    self.clear();

                    for value in values {
                        let mut elem = T::default();
                        elem.merge(value)?;
                        self.insert(elem);
                    }
                }
                SetPatch::Clear => self.clear(),
            }
        }

        Ok(())
    }
}

impl<K, V> UpdateView for BTreeMap<K, V>
where
    K: UpdateView + Clone + Default + Ord,
    V: UpdateView + Default,
{
    type UpdateViewType = Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>;

    #[expect(clippy::too_many_lines)]
    fn merge(&mut self, patches: Self::UpdateViewType) -> Result<(), ViewPatchError> {
        // Phase 1: decode patch payload into concrete keys.
        let mut ops = Vec::with_capacity(patches.len());
        for patch in patches {
            match patch {
                MapPatch::Insert { key, value } => {
                    let mut key_value = K::default();
                    key_value.merge(key)?;
                    ops.push(MapPatchOp::Insert {
                        key: key_value,
                        value,
                    });
                }
                MapPatch::Remove { key } => {
                    let mut key_value = K::default();
                    key_value.merge(key)?;
                    ops.push(MapPatchOp::Remove { key: key_value });
                }
                MapPatch::Replace { key, value } => {
                    let mut key_value = K::default();
                    key_value.merge(key)?;
                    ops.push(MapPatchOp::Replace {
                        key: key_value,
                        value,
                    });
                }
                MapPatch::Clear => ops.push(MapPatchOp::Clear),
            }
        }

        // Phase 2: reject ambiguous patch batches to keep semantics deterministic.
        let mut saw_clear = false;
        let mut touched = BTreeSet::new();
        for op in &ops {
            match op {
                MapPatchOp::Clear => {
                    if saw_clear {
                        return Err(ViewPatchError::InvalidPatchShape {
                            expected: "at most one Clear operation per map patch batch",
                            actual: "duplicate Clear operations",
                        });
                    }
                    saw_clear = true;
                    if ops.len() != 1 {
                        return Err(ViewPatchError::CardinalityViolation {
                            expected: 1,
                            actual: ops.len(),
                        });
                    }
                }
                MapPatchOp::Insert { key, .. }
                | MapPatchOp::Remove { key }
                | MapPatchOp::Replace { key, .. } => {
                    if saw_clear {
                        return Err(ViewPatchError::InvalidPatchShape {
                            expected: "Clear must be the only operation in a map patch batch",
                            actual: "Clear combined with key operation",
                        });
                    }
                    if !touched.insert(key.clone()) {
                        return Err(ViewPatchError::InvalidPatchShape {
                            expected: "unique key operations per map patch batch",
                            actual: "duplicate key operation",
                        });
                    }
                }
            }
        }
        if saw_clear {
            self.clear();
            return Ok(());
        }

        // Phase 3: apply deterministic map operations.
        for op in ops {
            match op {
                MapPatchOp::Insert { key, value } => match self.entry(key) {
                    BTreeMapEntry::Occupied(mut slot) => {
                        slot.get_mut().merge(value)?;
                    }
                    BTreeMapEntry::Vacant(slot) => {
                        let mut value_value = V::default();
                        value_value.merge(value)?;
                        slot.insert(value_value);
                    }
                },
                MapPatchOp::Remove { key } => {
                    if self.remove(&key).is_none() {
                        return Err(ViewPatchError::MissingKey {
                            operation: "remove",
                        });
                    }
                }
                MapPatchOp::Replace { key, value } => match self.entry(key) {
                    BTreeMapEntry::Occupied(mut slot) => {
                        slot.get_mut().merge(value)?;
                    }
                    BTreeMapEntry::Vacant(_) => {
                        return Err(ViewPatchError::MissingKey {
                            operation: "replace",
                        });
                    }
                },
                MapPatchOp::Clear => {
                    return Err(ViewPatchError::InvalidPatchShape {
                        expected: "Clear to be handled before apply phase",
                        actual: "Clear reached apply phase",
                    });
                }
            }
        }

        Ok(())
    }
}

macro_rules! impl_update_view {
    ($($type:ty),*) => {
        $(
            impl UpdateView for $type {
                type UpdateViewType = Self;

                fn merge(
                    &mut self,
                    update: Self::UpdateViewType,
                ) -> Result<(), ViewPatchError> {
                    *self = update;

                    Ok(())
                }
            }
        )*
    };
}

impl_update_view!(bool, i8, i16, i32, i64, u8, u16, u32, u64, String);
