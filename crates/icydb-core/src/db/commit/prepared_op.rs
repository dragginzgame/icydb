//! Module: commit::prepared_op
//! Responsibility: define mechanical prepared commit mutation payloads.
//! Does not own: mutation planning, store apply sequencing, or recovery orchestration.
//! Boundary: commit::{prepare,relation,executor} -> commit::prepared_op -> commit::apply.

use crate::db::{
    data::{DataStore, RawDataKey, RawRow},
    index::{IndexStore, RawIndexEntry, RawIndexKey},
};
use std::{cell::RefCell, thread::LocalKey};

///
/// PreparedIndexMutation
///
/// Mechanical index mutation derived from one prepared row operation.
/// Invariant: all payload bytes are already validated and ready for infallible apply.
///

#[derive(Clone)]
pub(crate) struct PreparedIndexMutation {
    pub(crate) store: &'static LocalKey<RefCell<IndexStore>>,
    pub(crate) key: RawIndexKey,
    pub(crate) value: Option<RawIndexEntry>,
    pub(crate) delta_kind: PreparedIndexDeltaKind,
}

///
/// PreparedIndexDeltaKind
///
/// Logical mutation-class annotation used for commit-window delta aggregation.
/// This is observability metadata and must not affect mutation semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparedIndexDeltaKind {
    None,
    IndexInsert,
    IndexRemove,
    ReverseIndexInsert,
    ReverseIndexRemove,
}

impl PreparedIndexDeltaKind {
    /// Resolve one reverse-index delta kind from old/new membership state.
    #[must_use]
    pub(crate) const fn from_reverse_index_membership(
        old_contains: bool,
        new_contains: bool,
    ) -> Self {
        match (old_contains, new_contains) {
            (true, false) => Self::ReverseIndexRemove,
            (false, true) => Self::ReverseIndexInsert,
            _ => Self::None,
        }
    }

    /// Project one delta kind into index/reverse-index counter increments.
    #[must_use]
    pub(crate) const fn counter_increments(self) -> (usize, usize, usize, usize) {
        match self {
            Self::None => (0, 0, 0, 0),
            Self::IndexInsert => (1, 0, 0, 0),
            Self::IndexRemove => (0, 1, 0, 0),
            Self::ReverseIndexInsert => (0, 0, 1, 0),
            Self::ReverseIndexRemove => (0, 0, 0, 1),
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::commit::PreparedIndexDeltaKind;

    #[test]
    fn reverse_index_membership_maps_to_expected_delta_kind() {
        assert_eq!(
            PreparedIndexDeltaKind::from_reverse_index_membership(true, false),
            PreparedIndexDeltaKind::ReverseIndexRemove,
        );
        assert_eq!(
            PreparedIndexDeltaKind::from_reverse_index_membership(false, true),
            PreparedIndexDeltaKind::ReverseIndexInsert,
        );
        assert_eq!(
            PreparedIndexDeltaKind::from_reverse_index_membership(false, false),
            PreparedIndexDeltaKind::None,
        );
        assert_eq!(
            PreparedIndexDeltaKind::from_reverse_index_membership(true, true),
            PreparedIndexDeltaKind::None,
        );
    }

    #[test]
    fn delta_kind_counter_increments_match_index_variants() {
        assert_eq!(
            PreparedIndexDeltaKind::IndexInsert.counter_increments(),
            (1, 0, 0, 0),
        );
        assert_eq!(
            PreparedIndexDeltaKind::IndexRemove.counter_increments(),
            (0, 1, 0, 0),
        );
    }

    #[test]
    fn delta_kind_counter_increments_match_reverse_index_variants() {
        assert_eq!(
            PreparedIndexDeltaKind::ReverseIndexInsert.counter_increments(),
            (0, 0, 1, 0),
        );
        assert_eq!(
            PreparedIndexDeltaKind::ReverseIndexRemove.counter_increments(),
            (0, 0, 0, 1),
        );
        assert_eq!(
            PreparedIndexDeltaKind::None.counter_increments(),
            (0, 0, 0, 0)
        );
    }
}

///
/// PreparedRowCommitOp
///
/// Mechanical store mutation derived from one commit-marker row op.
/// Invariant: `index_ops` and `data_*` fields represent one atomic row transition.
///

#[derive(Clone)]
pub(in crate::db) struct PreparedRowCommitOp {
    pub(crate) index_ops: Vec<PreparedIndexMutation>,
    pub(crate) data_store: &'static LocalKey<RefCell<DataStore>>,
    pub(crate) data_key: RawDataKey,
    pub(crate) data_value: Option<RawRow>,
}
