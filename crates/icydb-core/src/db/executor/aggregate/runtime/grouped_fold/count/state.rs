//! Module: executor::aggregate::runtime::grouped_fold::count::state
//! Responsibility: grouped `COUNT(*)` state and bucket storage.
//! Boundary: owns count buckets while ingest and finalization live in siblings.

use crate::{
    db::executor::{
        aggregate::{ExecutionContext, GroupError, runtime::grouped_fold::utils::GroupIndexBucket},
        group::{
            GroupKey, StableHash, StableHashBuildHasher, StableHashMap,
            retained_hash_entry_backing_bytes, try_reserve_hash_entry, try_reserve_vec_elements,
        },
    },
    error::InternalError,
};

///
/// GroupedCountState
///
/// GroupedCountState keeps the dedicated grouped `COUNT(*)` fold on a
/// borrowed-probe fast path and defers owned `GroupKey` construction until a
/// genuinely new group must be inserted.
///

pub(super) struct GroupedCountState {
    pub(super) groups: Vec<(GroupKey, u32)>,
    pub(super) bucket_index: StableHashMap<GroupIndexBucket>,
}

impl GroupedCountState {
    // Build one empty grouped-count state container.
    pub(super) const fn new() -> Self {
        Self {
            groups: Vec::new(),
            bucket_index: StableHashMap::with_hasher(StableHashBuildHasher),
        }
    }

    // Increment one existing grouped-count bucket after lookup has already
    // proven the candidate group index is valid.
    pub(super) fn increment_existing_group(
        &mut self,
        existing_index: usize,
    ) -> Result<(), InternalError> {
        let (_, count) = self
            .groups
            .get_mut(existing_index)
            .ok_or_else(InternalError::query_executor_invariant)?;
        *count = count.saturating_add(1);
        Ok(())
    }

    // Insert one newly observed grouped key after the borrowed fast path has
    // already ruled out an existing canonical group match.
    pub(super) fn insert_new_group(
        &mut self,
        group_hash: StableHash,
        group_key: GroupKey,
        grouped_execution_context: &mut ExecutionContext,
    ) -> Result<(), InternalError> {
        let group_count_before_insert = self.groups.len();
        let group_capacity_before_insert = self.groups.capacity();
        grouped_execution_context
            .record_new_group(
                group_count_before_insert,
                group_capacity_before_insert,
                &group_key,
            )
            .map_err(GroupError::into_internal_error)?;
        let new_index = self.groups.len();
        let new_hash_bucket = !self.bucket_index.contains_key(&group_hash);
        let index_backing_bytes = self.bucket_index.get(&group_hash).map_or_else(
            retained_hash_entry_backing_bytes::<StableHash, GroupIndexBucket>,
            GroupIndexBucket::retained_insert_backing_bytes,
        );
        grouped_execution_context
            .record_structural_backing(index_backing_bytes)
            .map_err(GroupError::into_internal_error)?;
        try_reserve_vec_elements(&mut self.groups, 1)?;
        if new_hash_bucket {
            try_reserve_hash_entry(&mut self.bucket_index)?;
        }
        self.groups.push((group_key, 1));
        if let Some(bucket) = self.bucket_index.get_mut(&group_hash) {
            bucket.push_index(new_index)?;
        } else {
            self.bucket_index
                .insert(group_hash, GroupIndexBucket::single(new_index));
        }
        Ok(())
    }

    // Consume this grouped-count state into finalized `(group_key, count)` rows.
    pub(super) fn into_groups(self) -> Vec<(GroupKey, u32)> {
        self.groups
    }
}
