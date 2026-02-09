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
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::patch::merge::MergePatch;

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
}
