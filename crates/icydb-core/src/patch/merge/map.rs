use crate::{
    patch::{MapPatch, merge::error::MergePatchError},
    traits::UpdateView,
};
use std::{
    collections::{BTreeMap, HashMap},
    hash::{BuildHasher, Hash},
    marker::PhantomData,
};

/// Internal representation used to normalize map patches before application.
enum MapPatchOp<K, V> {
    Insert { key: K, value: V },
    Remove { key: K },
    Replace { key: K, value: V },
    Clear,
}

/// Storage adapter for map merge operations.
trait MapAdapter<K, V> {
    type Map;

    fn get_mut<'a>(map: &'a mut Self::Map, key: &K) -> Option<&'a mut V>;
    fn insert(map: &mut Self::Map, key: K, value: V);
    fn remove(map: &mut Self::Map, key: &K);
    fn clear(map: &mut Self::Map);
    fn contains_key(map: &Self::Map, key: &K) -> bool;
}

/// HashMap-backed map adapter.
struct HashMapAdapter<S>(PhantomData<S>);

impl<K, V, S> MapAdapter<K, V> for HashMapAdapter<S>
where
    K: Eq + Hash,
    S: BuildHasher + Default,
{
    type Map = HashMap<K, V, S>;

    fn get_mut<'a>(map: &'a mut Self::Map, key: &K) -> Option<&'a mut V> {
        map.get_mut(key)
    }

    fn insert(map: &mut Self::Map, key: K, value: V) {
        map.insert(key, value);
    }

    fn remove(map: &mut Self::Map, key: &K) {
        map.remove(key);
    }

    fn clear(map: &mut Self::Map) {
        map.clear();
    }

    fn contains_key(map: &Self::Map, key: &K) -> bool {
        map.contains_key(key)
    }
}

/// BTreeMap-backed map adapter.
struct BTreeMapAdapter;

impl<K, V> MapAdapter<K, V> for BTreeMapAdapter
where
    K: Ord,
{
    type Map = BTreeMap<K, V>;

    fn get_mut<'a>(map: &'a mut Self::Map, key: &K) -> Option<&'a mut V> {
        map.get_mut(key)
    }

    fn insert(map: &mut Self::Map, key: K, value: V) {
        map.insert(key, value);
    }

    fn remove(map: &mut Self::Map, key: &K) {
        map.remove(key);
    }

    fn clear(map: &mut Self::Map) {
        map.clear();
    }

    fn contains_key(map: &Self::Map, key: &K) -> bool {
        map.contains_key(key)
    }
}

// Phase 1: decode map patch payloads into concrete keys.
fn decode_map_ops<K, V>(
    patches: Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>,
) -> Result<Vec<MapPatchOp<K, V::UpdateViewType>>, MergePatchError>
where
    K: UpdateView + Clone + Default,
    V: UpdateView + Default,
{
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

    Ok(ops)
}

// Phase 2: reject ambiguous map patch batches to keep semantics deterministic.
fn validate_map_ops<K, V>(ops: &[MapPatchOp<K, V>]) -> Result<(), MergePatchError>
where
    K: Clone + Eq,
{
    let mut saw_clear = false;
    let mut touched = Vec::with_capacity(ops.len());

    for op in ops {
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

                if touched.iter().any(|existing: &K| existing == key) {
                    return Err(MergePatchError::InvalidShape {
                        expected: "unique key operations per map patch batch",
                        actual: "duplicate key operation",
                    });
                }
                touched.push(key.clone());
            }
        }
    }

    Ok(())
}

// Phase 3: apply deterministic map operations.
fn apply_map_ops<K, V, A>(
    values: &mut A::Map,
    ops: Vec<MapPatchOp<K, V::UpdateViewType>>,
) -> Result<(), MergePatchError>
where
    K: Eq,
    V: UpdateView + Default,
    A: MapAdapter<K, V>,
{
    for op in ops {
        match op {
            MapPatchOp::Insert { key, value } => {
                if let Some(slot) = A::get_mut(values, &key) {
                    slot.merge(value)
                        .map_err(|err| err.with_field("insert").with_field("value"))?;
                } else {
                    let mut value_value = V::default();
                    value_value
                        .merge(value)
                        .map_err(|err| err.with_field("insert").with_field("value"))?;
                    A::insert(values, key, value_value);
                }
            }
            MapPatchOp::Remove { key } => {
                // Align with list/set semantics: removing a missing entry is a no-op.
                A::remove(values, &key);
            }
            MapPatchOp::Replace { key, value } => {
                // Align with list/set semantics: replacing a missing entry is a no-op.
                if A::contains_key(values, &key)
                    && let Some(slot) = A::get_mut(values, &key)
                {
                    slot.merge(value)
                        .map_err(|err| err.with_field("replace").with_field("value"))?;
                }
            }
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

// Shared map merge pipeline for all map backends.
fn merge_map<K, V, A>(
    values: &mut A::Map,
    patches: Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>,
) -> Result<(), MergePatchError>
where
    K: UpdateView + Clone + Default + Eq,
    V: UpdateView + Default,
    A: MapAdapter<K, V>,
{
    let ops = decode_map_ops::<K, V>(patches)?;
    validate_map_ops(&ops)?;

    if matches!(ops.as_slice(), [MapPatchOp::Clear]) {
        A::clear(values);
        return Ok(());
    }

    apply_map_ops::<K, V, A>(values, ops)
}

/// Apply map patch operations for hash maps.
pub fn merge_hash_map<K, V, S>(
    values: &mut HashMap<K, V, S>,
    patches: Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>,
) -> Result<(), MergePatchError>
where
    K: UpdateView + Clone + Default + Eq + Hash,
    V: UpdateView + Default,
    S: BuildHasher + Default,
{
    merge_map::<K, V, HashMapAdapter<S>>(values, patches)
}

/// Apply map patch operations for ordered maps.
pub fn merge_btree_map<K, V>(
    values: &mut BTreeMap<K, V>,
    patches: Vec<MapPatch<K::UpdateViewType, V::UpdateViewType>>,
) -> Result<(), MergePatchError>
where
    K: UpdateView + Clone + Default + Ord,
    V: UpdateView + Default,
{
    merge_map::<K, V, BTreeMapAdapter>(values, patches)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{MergePatchError, merge_btree_map, merge_hash_map};
    use crate::patch::MapPatch;
    use std::collections::{BTreeMap, HashMap};

    fn hash_map(entries: &[(u8, u8)]) -> HashMap<u8, u8> {
        entries.iter().copied().collect()
    }

    fn btree_map(entries: &[(u8, u8)]) -> BTreeMap<u8, u8> {
        entries.iter().copied().collect()
    }

    #[test]
    fn hash_map_merge_sequence_insert_remove_overwrite_clear() -> Result<(), MergePatchError> {
        let mut values: HashMap<u8, u8> = HashMap::new();

        merge_hash_map(&mut values, vec![MapPatch::Insert { key: 1, value: 10 }])?;
        assert_eq!(values, hash_map(&[(1, 10)]));

        merge_hash_map(&mut values, vec![MapPatch::Remove { key: 1 }])?;
        assert!(values.is_empty());

        merge_hash_map(&mut values, vec![MapPatch::Replace { key: 1, value: 99 }])?;
        assert!(values.is_empty());

        merge_hash_map(&mut values, vec![MapPatch::Insert { key: 1, value: 10 }])?;
        merge_hash_map(&mut values, vec![MapPatch::Replace { key: 1, value: 42 }])?;
        assert_eq!(values, hash_map(&[(1, 42)]));

        merge_hash_map(&mut values, vec![MapPatch::Clear])?;
        assert!(values.is_empty());

        Ok(())
    }

    #[test]
    fn btree_map_merge_sequence_insert_remove_overwrite_clear() -> Result<(), MergePatchError> {
        let mut values: BTreeMap<u8, u8> = BTreeMap::new();

        merge_btree_map(&mut values, vec![MapPatch::Insert { key: 1, value: 10 }])?;
        assert_eq!(values, btree_map(&[(1, 10)]));

        merge_btree_map(&mut values, vec![MapPatch::Remove { key: 1 }])?;
        assert!(values.is_empty());

        merge_btree_map(&mut values, vec![MapPatch::Replace { key: 1, value: 99 }])?;
        assert!(values.is_empty());

        merge_btree_map(&mut values, vec![MapPatch::Insert { key: 1, value: 10 }])?;
        merge_btree_map(&mut values, vec![MapPatch::Replace { key: 1, value: 42 }])?;
        assert_eq!(values, btree_map(&[(1, 42)]));

        merge_btree_map(&mut values, vec![MapPatch::Clear])?;
        assert!(values.is_empty());

        Ok(())
    }

    #[test]
    fn hash_map_merge_is_deterministic_under_patch_reordering() -> Result<(), MergePatchError> {
        let initial = hash_map(&[(1, 10), (2, 20), (4, 40)]);
        let patches_a = vec![
            MapPatch::Replace { key: 1, value: 11 },
            MapPatch::Remove { key: 2 },
            MapPatch::Insert { key: 3, value: 30 },
        ];
        let patches_b = vec![
            MapPatch::Insert { key: 3, value: 30 },
            MapPatch::Replace { key: 1, value: 11 },
            MapPatch::Remove { key: 2 },
        ];

        let mut left = initial.clone();
        let mut right = initial;

        merge_hash_map(&mut left, patches_a)?;
        merge_hash_map(&mut right, patches_b)?;

        assert_eq!(left, right);
        assert_eq!(left, hash_map(&[(1, 11), (3, 30), (4, 40)]));

        Ok(())
    }

    #[test]
    fn btree_map_merge_is_deterministic_under_patch_reordering() -> Result<(), MergePatchError> {
        let initial = btree_map(&[(1, 10), (2, 20), (4, 40)]);
        let patches_a = vec![
            MapPatch::Replace { key: 1, value: 11 },
            MapPatch::Remove { key: 2 },
            MapPatch::Insert { key: 3, value: 30 },
        ];
        let patches_b = vec![
            MapPatch::Insert { key: 3, value: 30 },
            MapPatch::Replace { key: 1, value: 11 },
            MapPatch::Remove { key: 2 },
        ];

        let mut left = initial.clone();
        let mut right = initial;

        merge_btree_map(&mut left, patches_a)?;
        merge_btree_map(&mut right, patches_b)?;

        assert_eq!(left, right);
        assert_eq!(left, btree_map(&[(1, 11), (3, 30), (4, 40)]));

        Ok(())
    }
}
