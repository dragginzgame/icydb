use candid::CandidType;
use serde::{Deserialize, Serialize};

///
/// ListPatch
///
/// Positional list patches applied in order.
/// Indices refer to the list state at the time each patch executes.
/// `Insert` clamps out-of-bounds indices to the tail; `Remove` ignores invalid indices.
/// `Update` only applies to existing elements and never creates new entries.
/// `Overwrite` replaces the entire list with the provided values.
///

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub enum ListPatch<U> {
    Update { index: usize, patch: U },
    Insert { index: usize, value: U },
    Push { value: U },
    Overwrite { values: Vec<U> },
    Remove { index: usize },
    Clear,
}

///
/// SetPatch
///
/// Set operations applied in-order; `Overwrite` replaces the entire set.
///

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub enum SetPatch<U> {
    Insert(U),
    Remove(U),
    Overwrite { values: Vec<U> },
    Clear,
}

///
/// MapPatch
///
/// Deterministic map mutations.
///
/// - Maps are unordered values; insertion order is discarded.
/// - `Insert` is an upsert.
/// - `Replace` requires an existing key.
/// - `Remove` requires an existing key.
/// - `Clear` must be the only patch in the batch.
///
/// Invalid patch shapes and missing-key operations are returned by
/// `UpdateView::merge` as `ViewPatchError`.
///

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub enum MapPatch<K, V> {
    Insert { key: K, value: V },
    Remove { key: K },
    Replace { key: K, value: V },
    Clear,
}

impl<K, V> From<(K, Option<V>)> for MapPatch<K, V> {
    fn from((key, value): (K, Option<V>)) -> Self {
        match value {
            Some(value) => Self::Insert { key, value },
            None => Self::Remove { key },
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{UpdateView, ViewPatchError};
    use std::collections::{BTreeMap, HashMap, HashSet};

    #[test]
    fn vec_partial_patches() {
        let mut values = vec![10u8, 20, 30];
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

        values
            .merge(patches)
            .expect("list patch merge should succeed");
        assert_eq!(values, vec![11, 99, 30]);
    }

    #[test]
    fn vec_overwrite_replaces_contents() {
        let mut values = vec![1u8, 2, 3];
        let patches = vec![ListPatch::Overwrite {
            values: vec![9u8, 8],
        }];

        values
            .merge(patches)
            .expect("list patch merge should succeed");
        assert_eq!(values, vec![9, 8]);
    }

    #[test]
    fn set_insert_remove_without_clear() {
        let mut set: HashSet<u8> = [1, 2, 3].into_iter().collect();
        let patches = vec![SetPatch::Remove(2), SetPatch::Insert(4)];

        set.merge(patches).expect("set patch merge should succeed");
        let expected: HashSet<u8> = [1, 3, 4].into_iter().collect();
        assert_eq!(set, expected);
    }

    #[test]
    fn set_overwrite_replaces_contents() {
        let mut set: HashSet<u8> = [1, 2, 3].into_iter().collect();
        let patches = vec![SetPatch::Overwrite {
            values: vec![3u8, 4, 5],
        }];

        set.merge(patches).expect("set patch merge should succeed");
        let expected: HashSet<u8> = [3, 4, 5].into_iter().collect();
        assert_eq!(set, expected);
    }

    #[test]
    fn map_insert_in_place_and_remove() {
        let mut map: HashMap<String, u8> = [("a".into(), 1u8), ("keep".into(), 9u8)]
            .into_iter()
            .collect();

        let patches = vec![
            MapPatch::Insert {
                key: "a".to_string(),
                value: 5u8,
            },
            MapPatch::Remove {
                key: "keep".to_string(),
            },
            MapPatch::Insert {
                key: "insert".to_string(),
                value: 7u8,
            },
        ];

        map.merge(patches).expect("map patch merge should succeed");

        assert_eq!(map.get("a"), Some(&5));
        assert_eq!(map.get("insert"), Some(&7));
        assert!(!map.contains_key("keep"));
    }

    #[test]
    fn map_replace_updates_existing_entry() {
        let mut map: HashMap<String, u8> = [("keep".into(), 1u8), ("replace".into(), 2u8)]
            .into_iter()
            .collect();

        let patches = vec![MapPatch::Replace {
            key: "replace".to_string(),
            value: 8u8,
        }];

        map.merge(patches).expect("map patch merge should succeed");

        assert_eq!(map.get("keep"), Some(&1));
        assert_eq!(map.get("replace"), Some(&8));
    }

    #[test]
    fn btree_map_clear_replaces_with_empty_map() {
        let mut map: BTreeMap<String, u8> =
            [("a".into(), 1u8), ("b".into(), 2u8)].into_iter().collect();

        map.merge(vec![MapPatch::Clear])
            .expect("map clear patch should succeed");

        assert!(map.is_empty());
    }

    #[test]
    fn map_remove_missing_key_returns_error() {
        let mut map: HashMap<String, u8> = std::iter::once(("a".into(), 1u8)).collect();
        let err = map
            .merge(vec![MapPatch::Remove {
                key: "missing".to_string(),
            }])
            .expect_err("missing remove key should fail");
        assert!(matches!(
            err.leaf(),
            ViewPatchError::MissingKey {
                operation: "remove"
            }
        ));
    }

    #[test]
    fn map_replace_missing_key_returns_error() {
        let mut map: HashMap<String, u8> = std::iter::once(("a".into(), 1u8)).collect();
        let err = map
            .merge(vec![MapPatch::Replace {
                key: "missing".to_string(),
                value: 3u8,
            }])
            .expect_err("missing replace key should fail");
        assert!(matches!(
            err.leaf(),
            ViewPatchError::MissingKey {
                operation: "replace"
            }
        ));
    }

    #[test]
    fn map_clear_with_other_operations_returns_error() {
        let mut map: HashMap<String, u8> = std::iter::once(("a".into(), 1u8)).collect();
        let err = map
            .merge(vec![
                MapPatch::Clear,
                MapPatch::Insert {
                    key: "b".to_string(),
                    value: 2u8,
                },
            ])
            .expect_err("clear combined with key ops should fail");
        assert!(matches!(
            err.leaf(),
            ViewPatchError::CardinalityViolation {
                expected: 1,
                actual: 2,
            }
        ));
    }

    #[test]
    fn map_duplicate_key_operations_returns_error() {
        let mut map: HashMap<String, u8> = std::iter::once(("a".into(), 1u8)).collect();
        let err = map
            .merge(vec![
                MapPatch::Insert {
                    key: "a".to_string(),
                    value: 3u8,
                },
                MapPatch::Replace {
                    key: "a".to_string(),
                    value: 4u8,
                },
            ])
            .expect_err("duplicate key operations should fail");
        assert!(matches!(
            err.leaf(),
            ViewPatchError::InvalidPatchShape {
                expected: "unique key operations per map patch batch",
                actual: "duplicate key operation",
            }
        ));
    }
}
