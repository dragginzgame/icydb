use crate::{
    patch::{ListPatch, MapPatch, SetPatch},
    traits::{Atomic, UpdateView},
};
use std::{
    collections::{
        BTreeMap, BTreeSet, HashMap, HashSet, btree_map::Entry as BTreeMapEntry,
        hash_map::Entry as HashMapEntry,
    },
    hash::{BuildHasher, Hash},
};
use thiserror::Error as ThisError;

///
/// MergePatchError
///
/// Structured failures for user-driven patch application.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub enum MergePatchError {
    #[error("invalid patch shape: expected {expected}, found {actual}")]
    InvalidShape {
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

impl MergePatchError {
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

/// Apply full-replacement semantics for atomic update payloads.
pub fn merge_atomic<T>(value: &mut T, patch: T) -> Result<(), MergePatchError>
where
    T: Atomic + UpdateView<UpdateViewType = T>,
{
    *value = patch;

    Ok(())
}

/// Apply optional update payloads with create-on-update semantics.
pub fn merge_option<T>(
    value: &mut Option<T>,
    patch: Option<T::UpdateViewType>,
) -> Result<(), MergePatchError>
where
    T: UpdateView + Default,
{
    match patch {
        None => {
            // Explicit delete
            *value = None;
        }
        Some(inner_patch) => {
            if let Some(inner_value) = value.as_mut() {
                inner_value
                    .merge(inner_patch)
                    .map_err(|err| err.with_field("value"))?;
            } else {
                let mut new_value = T::default();
                new_value
                    .merge(inner_patch)
                    .map_err(|err| err.with_field("value"))?;
                *value = Some(new_value);
            }
        }
    }

    Ok(())
}

/// Apply ordered list patch operations in sequence.
pub fn merge_vec<T>(
    values: &mut Vec<T>,
    patches: Vec<ListPatch<T::UpdateViewType>>,
) -> Result<(), MergePatchError>
where
    T: UpdateView + Default,
{
    for patch in patches {
        match patch {
            ListPatch::Update { index, patch } => {
                if let Some(elem) = values.get_mut(index) {
                    elem.merge(patch).map_err(|err| err.with_index(index))?;
                }
            }

            ListPatch::Insert { index, value } => {
                let idx = index.min(values.len());
                let mut elem = T::default();
                elem.merge(value).map_err(|err| err.with_index(idx))?;
                values.insert(idx, elem);
            }

            ListPatch::Push { value } => {
                let idx = values.len();
                let mut elem = T::default();
                elem.merge(value).map_err(|err| err.with_index(idx))?;
                values.push(elem);
            }

            ListPatch::Overwrite {
                values: next_values,
            } => {
                values.clear();
                values.reserve(next_values.len());

                for (index, value) in next_values.into_iter().enumerate() {
                    let mut elem = T::default();
                    elem.merge(value).map_err(|err| err.with_index(index))?;
                    values.push(elem);
                }
            }

            ListPatch::Remove { index } => {
                if index < values.len() {
                    values.remove(index);
                }
            }

            ListPatch::Clear => values.clear(),
        }
    }

    Ok(())
}

/// Apply set patch operations for hash sets.
pub fn merge_hash_set<T, S>(
    values: &mut HashSet<T, S>,
    patches: Vec<SetPatch<T::UpdateViewType>>,
) -> Result<(), MergePatchError>
where
    T: UpdateView + Clone + Default + Eq + Hash,
    S: BuildHasher + Default,
{
    for patch in patches {
        match patch {
            SetPatch::Insert(value) => {
                let mut elem = T::default();
                elem.merge(value).map_err(|err| err.with_field("insert"))?;
                values.insert(elem);
            }

            SetPatch::Remove(value) => {
                let mut elem = T::default();
                elem.merge(value).map_err(|err| err.with_field("remove"))?;
                values.remove(&elem);
            }

            SetPatch::Overwrite {
                values: next_values,
            } => {
                values.clear();

                for (index, value) in next_values.into_iter().enumerate() {
                    let mut elem = T::default();
                    elem.merge(value)
                        .map_err(|err| err.with_field("overwrite").with_index(index))?;
                    values.insert(elem);
                }
            }

            SetPatch::Clear => values.clear(),
        }
    }

    Ok(())
}

/// Internal representation used to normalize map patches before application.
enum MapPatchOp<K, V> {
    Insert { key: K, value: V },
    Remove { key: K },
    Replace { key: K, value: V },
    Clear,
}

/// Apply map patch operations for hash maps.
#[expect(clippy::too_many_lines)]
pub fn merge_hash_map<K, V, S>(
    values: &mut HashMap<K, V, S>,
    patches: Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>,
) -> Result<(), MergePatchError>
where
    K: UpdateView + Clone + Default + Eq + Hash,
    V: UpdateView + Default,
    S: BuildHasher + Default,
{
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
                    return Err(MergePatchError::InvalidShape {
                        expected: "at most one Clear operation per map patch batch",
                        actual: "duplicate Clear operations",
                    });
                }
                saw_clear = true;
                if ops.len() != 1 {
                    return Err(MergePatchError::CardinalityViolation {
                        expected: 1,
                        actual: ops.len(),
                    });
                }
            }
            MapPatchOp::Insert { key, .. }
            | MapPatchOp::Remove { key }
            | MapPatchOp::Replace { key, .. } => {
                if saw_clear {
                    return Err(MergePatchError::InvalidShape {
                        expected: "Clear must be the only operation in a map patch batch",
                        actual: "Clear combined with key operation",
                    });
                }
                if !touched.insert(key.clone()) {
                    return Err(MergePatchError::InvalidShape {
                        expected: "unique key operations per map patch batch",
                        actual: "duplicate key operation",
                    });
                }
            }
        }
    }

    if saw_clear {
        values.clear();
        return Ok(());
    }

    // Phase 3: apply deterministic map operations.
    for op in ops {
        match op {
            MapPatchOp::Insert { key, value } => match values.entry(key) {
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
                if values.remove(&key).is_none() {
                    return Err(MergePatchError::MissingKey {
                        operation: "remove",
                    });
                }
            }

            MapPatchOp::Replace { key, value } => match values.entry(key) {
                HashMapEntry::Occupied(mut slot) => {
                    slot.get_mut()
                        .merge(value)
                        .map_err(|err| err.with_field("replace").with_field("value"))?;
                }
                HashMapEntry::Vacant(_) => {
                    return Err(MergePatchError::MissingKey {
                        operation: "replace",
                    });
                }
            },

            MapPatchOp::Clear => {
                return Err(MergePatchError::InvalidShape {
                    expected: "Clear to be handled before apply phase",
                    actual: "Clear reached apply phase",
                });
            }
        }
    }

    Ok(())
}

/// Apply set patch operations for ordered sets.
pub fn merge_btree_set<T>(
    values: &mut BTreeSet<T>,
    patches: Vec<SetPatch<T::UpdateViewType>>,
) -> Result<(), MergePatchError>
where
    T: UpdateView + Clone + Default + Ord,
{
    for patch in patches {
        match patch {
            SetPatch::Insert(value) => {
                let mut elem = T::default();
                elem.merge(value).map_err(|err| err.with_field("insert"))?;
                values.insert(elem);
            }
            SetPatch::Remove(value) => {
                let mut elem = T::default();
                elem.merge(value).map_err(|err| err.with_field("remove"))?;
                values.remove(&elem);
            }
            SetPatch::Overwrite {
                values: next_values,
            } => {
                values.clear();

                for (index, value) in next_values.into_iter().enumerate() {
                    let mut elem = T::default();
                    elem.merge(value)
                        .map_err(|err| err.with_field("overwrite").with_index(index))?;
                    values.insert(elem);
                }
            }
            SetPatch::Clear => values.clear(),
        }
    }

    Ok(())
}

/// Apply map patch operations for ordered maps.
#[expect(clippy::too_many_lines)]
pub fn merge_btree_map<K, V>(
    values: &mut BTreeMap<K, V>,
    patches: Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>,
) -> Result<(), MergePatchError>
where
    K: UpdateView + Clone + Default + Ord,
    V: UpdateView + Default,
{
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
                    return Err(MergePatchError::InvalidShape {
                        expected: "at most one Clear operation per map patch batch",
                        actual: "duplicate Clear operations",
                    });
                }
                saw_clear = true;
                if ops.len() != 1 {
                    return Err(MergePatchError::CardinalityViolation {
                        expected: 1,
                        actual: ops.len(),
                    });
                }
            }
            MapPatchOp::Insert { key, .. }
            | MapPatchOp::Remove { key }
            | MapPatchOp::Replace { key, .. } => {
                if saw_clear {
                    return Err(MergePatchError::InvalidShape {
                        expected: "Clear must be the only operation in a map patch batch",
                        actual: "Clear combined with key operation",
                    });
                }
                if !touched.insert(key.clone()) {
                    return Err(MergePatchError::InvalidShape {
                        expected: "unique key operations per map patch batch",
                        actual: "duplicate key operation",
                    });
                }
            }
        }
    }
    if saw_clear {
        values.clear();
        return Ok(());
    }

    // Phase 3: apply deterministic map operations.
    for op in ops {
        match op {
            MapPatchOp::Insert { key, value } => match values.entry(key) {
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
                if values.remove(&key).is_none() {
                    return Err(MergePatchError::MissingKey {
                        operation: "remove",
                    });
                }
            }
            MapPatchOp::Replace { key, value } => match values.entry(key) {
                BTreeMapEntry::Occupied(mut slot) => {
                    slot.get_mut()
                        .merge(value)
                        .map_err(|err| err.with_field("replace").with_field("value"))?;
                }
                BTreeMapEntry::Vacant(_) => {
                    return Err(MergePatchError::MissingKey {
                        operation: "replace",
                    });
                }
            },
            MapPatchOp::Clear => {
                return Err(MergePatchError::InvalidShape {
                    expected: "Clear to be handled before apply phase",
                    actual: "Clear reached apply phase",
                });
            }
        }
    }

    Ok(())
}
