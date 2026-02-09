use candid::CandidType;
use serde::{Deserialize, Serialize};

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
    use crate::patch::merge::{MergePatch, MergePatchError};
    use std::collections::{BTreeMap, HashMap};

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
            MergePatchError::MissingKey {
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
            MergePatchError::MissingKey {
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
            MergePatchError::CardinalityViolation {
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
            MergePatchError::InvalidShape {
                expected: "unique key operations per map patch batch",
                actual: "duplicate key operation",
            }
        ));
    }
}
