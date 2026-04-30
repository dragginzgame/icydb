//! Module: executor::aggregate::runtime::grouped_fold::count::state
//! Responsibility: grouped `COUNT(*)` state and bucket storage.
//! Boundary: owns count buckets while ingest and finalization live in siblings.

use crate::{
    db::executor::{
        aggregate::{
            ExecutionContext, GroupError,
            runtime::grouped_fold::{metrics, utils::GroupIndexBucket},
        },
        group::{GroupKey, StableHash, StableHashBuildHasher, StableHashMap},
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
    #[cfg(test)]
    pub(super) fn new() -> Self {
        Self::with_capacity(0)
    }

    // Build one grouped-count state container with caller-provided capacity
    // hints from the resolved key stream. Group cardinality is bounded by row
    // cardinality, so this avoids repeated table growth on exact streams.
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            groups: Vec::with_capacity(capacity),
            bucket_index: StableHashMap::with_capacity_and_hasher(capacity, StableHashBuildHasher),
        }
    }

    // Increment one existing grouped-count bucket under the measured
    // existing-group update contract shared by every grouped-count ingest lane.
    pub(super) fn measure_existing_group_increment(
        &mut self,
        existing_index: usize,
        source: &'static str,
    ) -> Result<(), InternalError> {
        let (update_local_instructions, update_result) =
            metrics::measure(|| self.increment_existing_group(existing_index, source));
        metrics::record_existing_group_hit(update_local_instructions);

        update_result
    }

    // Insert one newly observed grouped key under the measured new-group
    // insert contract shared by every grouped-count ingest lane.
    pub(super) fn measure_new_group_insert(
        &mut self,
        group_hash: StableHash,
        group_key: GroupKey,
        grouped_execution_context: &mut ExecutionContext,
    ) -> Result<(), InternalError> {
        let (insert_local_instructions, insert_result) = metrics::measure(|| {
            self.finish_new_group_insert(group_hash, group_key, grouped_execution_context)
        });
        metrics::record_new_group_insert(insert_local_instructions);

        insert_result
    }

    // Increment one existing grouped-count bucket after lookup has already
    // proven the candidate group index is valid for the caller's ingest lane.
    fn increment_existing_group(
        &mut self,
        existing_index: usize,
        source: &'static str,
    ) -> Result<(), InternalError> {
        let (_, count) = self.groups.get_mut(existing_index).ok_or_else(|| {
            InternalError::query_executor_invariant(format!(
                "grouped count state missing {source} group: index={existing_index}",
            ))
        })?;
        *count = count.saturating_add(1);
        Ok(())
    }

    // Insert one newly observed grouped key after the borrowed fast path has
    // already ruled out an existing canonical group match.
    fn finish_new_group_insert(
        &mut self,
        group_hash: StableHash,
        group_key: GroupKey,
        grouped_execution_context: &mut ExecutionContext,
    ) -> Result<(), InternalError> {
        let group_count_before_insert = self.groups.len();
        let group_capacity_before_insert = self.groups.capacity();
        grouped_execution_context
            .record_new_group(group_count_before_insert, group_capacity_before_insert)
            .map_err(GroupError::into_internal_error)?;
        let new_index = self.groups.len();
        self.groups.push((group_key, 1));
        self.bucket_index
            .entry(group_hash)
            .and_modify(|bucket| bucket.push_index(new_index))
            .or_insert_with(|| GroupIndexBucket::single(new_index));
        Ok(())
    }

    // Consume this grouped-count state into finalized `(group_key, count)` rows.
    pub(super) fn into_groups(self) -> Vec<(GroupKey, u32)> {
        self.groups
    }
}
