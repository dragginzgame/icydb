use candid::CandidType;
use serde::{Deserialize, Serialize};

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
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::patch::merge::MergePatch;
    use std::collections::HashSet;

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
}
