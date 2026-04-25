//! Module: index::plan::delta
//! Responsibility: pure index membership deltas derived from row transitions.
//! Does not own: commit-op materialization, executor context wiring, or apply sequencing.
//! Boundary: index planning emits these deltas; commit preparation lowers them to store writes.

use crate::db::{data::StorageKey, index::IndexKey};

///
/// IndexMutationPlan
///
/// Deterministic collection of per-index membership deltas derived from one
/// row transition. The plan is intentionally independent of commit markers
/// and prepared apply operations.
///

#[derive(Debug)]
pub(in crate::db) struct IndexMutationPlan {
    pub(in crate::db) groups: Vec<IndexDeltaGroup>,
}

impl IndexMutationPlan {
    /// Build one plan from already-derived per-index delta groups.
    pub(in crate::db) const fn new(groups: Vec<IndexDeltaGroup>) -> Self {
        Self { groups }
    }
}

///
/// IndexDeltaGroup
///
/// Delta group for a single index definition. Grouping preserves the existing
/// per-index planning boundary while keeping each membership change as an
/// index-owned delta rather than a commit-owned operation.
///

#[derive(Debug)]
pub(in crate::db) struct IndexDeltaGroup {
    pub(in crate::db) index_store: &'static str,
    pub(in crate::db) index_fields: String,
    pub(in crate::db) deltas: Vec<IndexDelta>,
}

impl IndexDeltaGroup {
    /// Build one per-index delta group.
    #[must_use]
    pub(in crate::db) const fn new(
        index_store: &'static str,
        index_fields: String,
        deltas: Vec<IndexDelta>,
    ) -> Self {
        Self {
            index_store,
            index_fields,
            deltas,
        }
    }
}

///
/// IndexDelta
///
/// One primary-key membership change for one secondary index key. The delta
/// records only index-domain facts: which key owns the membership, which row
/// primary key is affected, and whether that membership is inserted or removed.
///

#[derive(Debug)]
pub(in crate::db) enum IndexDelta {
    /// Remove one primary-key membership from one index key.
    Remove(IndexMembershipDelta),
    /// Insert one primary-key membership into one index key.
    Insert(IndexMembershipDelta),
}

impl IndexDelta {
    /// Build one removal membership delta.
    #[must_use]
    pub(in crate::db) const fn remove(key: IndexKey, primary_key: StorageKey) -> Self {
        Self::Remove(IndexMembershipDelta { key, primary_key })
    }

    /// Build one insertion membership delta.
    #[must_use]
    pub(in crate::db) const fn insert(key: IndexKey, primary_key: StorageKey) -> Self {
        Self::Insert(IndexMembershipDelta { key, primary_key })
    }
}

///
/// IndexMembershipDelta
///
/// Index-domain payload shared by insert and remove deltas. Commit preparation
/// reads this payload when it materializes raw index entries, but the payload
/// itself carries no commit-layer concepts.
///

#[derive(Debug)]
pub(in crate::db) struct IndexMembershipDelta {
    pub(in crate::db) key: IndexKey,
    pub(in crate::db) primary_key: StorageKey,
}
