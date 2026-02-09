use crate::{
    traits::AsView,
    view::{ListPatch, MapPatch, SetPatch},
};
use candid::CandidType;
use std::{
    collections::{
        BTreeMap, BTreeSet, HashMap, HashSet, btree_map::Entry as BTreeMapEntry,
        hash_map::Entry as HashMapEntry,
    },
    hash::{BuildHasher, Hash},
};
use thiserror::Error as ThisError;

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

    #[error("patch merge failed at {path}: {source}")]
    Context {
        path: String,
        #[source]
        source: Box<Self>,
    },
}

impl ViewPatchError {
    /// Prepend a field segment to the merge error path.
    #[must_use]
    pub fn with_field(self, field: impl AsRef<str>) -> Self {
        self.with_path_segment(field.as_ref())
    }

    /// Prepend an index segment to the merge error path.
    #[must_use]
    pub fn with_index(self, index: usize) -> Self {
        self.with_path_segment(format!("[{index}]"))
    }

    /// Return the full contextual path, if available.
    #[must_use]
    pub const fn path(&self) -> Option<&str> {
        match self {
            Self::Context { path, .. } => Some(path.as_str()),
            _ => None,
        }
    }

    /// Return the innermost, non-context merge error variant.
    #[must_use]
    pub fn leaf(&self) -> &Self {
        match self {
            Self::Context { source, .. } => source.leaf(),
            _ => self,
        }
    }

    #[must_use]
    fn with_path_segment(self, segment: impl Into<String>) -> Self {
        let segment = segment.into();
        match self {
            Self::Context { path, source } => Self::Context {
                path: Self::join_segments(segment.as_str(), path.as_str()),
                source,
            },
            source => Self::Context {
                path: segment,
                source: Box::new(source),
            },
        }
    }

    #[must_use]
    fn join_segments(prefix: &str, suffix: &str) -> String {
        if suffix.starts_with('[') {
            format!("{prefix}{suffix}")
        } else {
            format!("{prefix}.{suffix}")
        }
    }
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
                    inner_value
                        .merge(inner_update)
                        .map_err(|err| err.with_field("value"))?;
                } else {
                    let mut new_value = T::default();
                    new_value
                        .merge(inner_update)
                        .map_err(|err| err.with_field("value"))?;
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
                        elem.merge(patch).map_err(|err| err.with_index(index))?;
                    }
                }
                ListPatch::Insert { index, value } => {
                    let idx = index.min(self.len());
                    let mut elem = T::default();
                    elem.merge(value).map_err(|err| err.with_index(idx))?;
                    self.insert(idx, elem);
                }
                ListPatch::Push { value } => {
                    let idx = self.len();
                    let mut elem = T::default();
                    elem.merge(value).map_err(|err| err.with_index(idx))?;
                    self.push(elem);
                }
                ListPatch::Overwrite { values } => {
                    self.clear();
                    self.reserve(values.len());

                    for (index, value) in values.into_iter().enumerate() {
                        let mut elem = T::default();
                        elem.merge(value).map_err(|err| err.with_index(index))?;
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
                    elem.merge(value).map_err(|err| err.with_field("insert"))?;
                    self.insert(elem);
                }
                SetPatch::Remove(value) => {
                    let mut elem = T::default();
                    elem.merge(value).map_err(|err| err.with_field("remove"))?;
                    self.remove(&elem);
                }
                SetPatch::Overwrite { values } => {
                    self.clear();

                    for (index, value) in values.into_iter().enumerate() {
                        let mut elem = T::default();
                        elem.merge(value)
                            .map_err(|err| err.with_field("overwrite").with_index(index))?;
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
                    key_value
                        .merge(key)
                        .map_err(|err| err.with_field("insert").with_field("key"))?;
                    ops.push(MapPatchOp::Insert {
                        key: key_value,
                        value,
                    });
                }
                MapPatch::Remove { key } => {
                    let mut key_value = K::default();
                    key_value
                        .merge(key)
                        .map_err(|err| err.with_field("remove").with_field("key"))?;
                    ops.push(MapPatchOp::Remove { key: key_value });
                }
                MapPatch::Replace { key, value } => {
                    let mut key_value = K::default();
                    key_value
                        .merge(key)
                        .map_err(|err| err.with_field("replace").with_field("key"))?;
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
                        slot.get_mut()
                            .merge(value)
                            .map_err(|err| err.with_field("insert").with_field("value"))?;
                    }
                    HashMapEntry::Vacant(slot) => {
                        let mut value_value = V::default();
                        value_value
                            .merge(value)
                            .map_err(|err| err.with_field("insert").with_field("value"))?;
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
                        slot.get_mut()
                            .merge(value)
                            .map_err(|err| err.with_field("replace").with_field("value"))?;
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
                    elem.merge(value).map_err(|err| err.with_field("insert"))?;
                    self.insert(elem);
                }
                SetPatch::Remove(value) => {
                    let mut elem = T::default();
                    elem.merge(value).map_err(|err| err.with_field("remove"))?;
                    self.remove(&elem);
                }
                SetPatch::Overwrite { values } => {
                    self.clear();

                    for (index, value) in values.into_iter().enumerate() {
                        let mut elem = T::default();
                        elem.merge(value)
                            .map_err(|err| err.with_field("overwrite").with_index(index))?;
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
                    key_value
                        .merge(key)
                        .map_err(|err| err.with_field("insert").with_field("key"))?;
                    ops.push(MapPatchOp::Insert {
                        key: key_value,
                        value,
                    });
                }
                MapPatch::Remove { key } => {
                    let mut key_value = K::default();
                    key_value
                        .merge(key)
                        .map_err(|err| err.with_field("remove").with_field("key"))?;
                    ops.push(MapPatchOp::Remove { key: key_value });
                }
                MapPatch::Replace { key, value } => {
                    let mut key_value = K::default();
                    key_value
                        .merge(key)
                        .map_err(|err| err.with_field("replace").with_field("key"))?;
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
                        slot.get_mut()
                            .merge(value)
                            .map_err(|err| err.with_field("insert").with_field("value"))?;
                    }
                    BTreeMapEntry::Vacant(slot) => {
                        let mut value_value = V::default();
                        value_value
                            .merge(value)
                            .map_err(|err| err.with_field("insert").with_field("value"))?;
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
                        slot.get_mut()
                            .merge(value)
                            .map_err(|err| err.with_field("replace").with_field("value"))?;
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
