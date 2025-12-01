use crate::traits::{CreateView, FilterView, UpdateView, View as OtherView};
use candid::CandidType;
use serde::{Deserialize, Serialize};

///
/// Type Aliases
///

pub type View<T> = <T as OtherView>::ViewType;
pub type Create<T> = <T as CreateView>::CreateViewType;
pub type Update<T> = <T as UpdateView>::UpdateViewType;
pub type Filter<T> = <T as FilterView>::FilterViewType;

///
/// ListPatch
///

/// Patches apply sequentially; indices outside the current length are clamped to the tail and
/// invalid removals are ignored.
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

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub enum MapPatch<K, V> {
    Upsert { key: K, value: V },
    Remove { key: K },
    Overwrite { entries: Vec<(K, V)> },
    Clear,
}

impl<K, V> From<(K, Option<V>)> for MapPatch<K, V> {
    fn from((key, value): (K, Option<V>)) -> Self {
        match value {
            Some(value) => Self::Upsert { key, value },
            None => Self::Remove { key },
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod test {
    use super::{ListPatch, MapPatch, SetPatch};
    use crate::traits::UpdateView;
    use std::collections::{HashMap, HashSet};

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

        values.merge(patches);
        assert_eq!(values, vec![11, 99, 30]);
    }

    #[test]
    fn vec_overwrite_replaces_contents() {
        let mut values = vec![1u8, 2, 3];
        let patches = vec![ListPatch::Overwrite {
            values: vec![9u8, 8],
        }];

        values.merge(patches);
        assert_eq!(values, vec![9, 8]);
    }

    #[test]
    fn set_insert_remove_without_clear() {
        let mut set: HashSet<u8> = [1, 2, 3].into_iter().collect();
        let patches = vec![SetPatch::Remove(2), SetPatch::Insert(4)];

        set.merge(patches);
        let expected: HashSet<u8> = [1, 3, 4].into_iter().collect();
        assert_eq!(set, expected);
    }

    #[test]
    fn set_overwrite_replaces_contents() {
        let mut set: HashSet<u8> = [1, 2, 3].into_iter().collect();
        let patches = vec![SetPatch::Overwrite {
            values: vec![3u8, 4, 5],
        }];

        set.merge(patches);
        let expected: HashSet<u8> = [3, 4, 5].into_iter().collect();
        assert_eq!(set, expected);
    }

    #[test]
    fn map_upsert_in_place_and_remove() {
        let mut map: HashMap<String, u8> = [("a".into(), 1u8), ("keep".into(), 9u8)]
            .into_iter()
            .collect();

        let patches = vec![
            MapPatch::Upsert {
                key: "a".to_string(),
                value: 5u8,
            },
            MapPatch::Remove {
                key: "keep".to_string(),
            },
            MapPatch::Upsert {
                key: "insert".to_string(),
                value: 7u8,
            },
        ];

        map.merge(patches);

        assert_eq!(map.get("a"), Some(&5));
        assert_eq!(map.get("insert"), Some(&7));
        assert!(!map.contains_key("keep"));
    }

    #[test]
    fn map_overwrite_replaces_contents() {
        let mut map: HashMap<String, u8> = [("keep".into(), 1u8), ("drop".into(), 2u8)]
            .into_iter()
            .collect();

        let patches = vec![MapPatch::Overwrite {
            entries: vec![
                ("first".to_string(), 9u8),
                ("second".to_string(), 5u8),
                ("first".to_string(), 1u8),
            ],
        }];

        map.merge(patches);

        assert_eq!(map.get("first"), Some(&1));
        assert_eq!(map.get("second"), Some(&5));
        assert!(!map.contains_key("keep"));
        assert!(!map.contains_key("drop"));
    }
}
