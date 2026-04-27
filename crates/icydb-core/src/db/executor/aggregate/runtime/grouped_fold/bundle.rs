//! Module: executor::aggregate::runtime::grouped_fold::bundle
//! Responsibility: shared grouped state ownership for the generic grouped fold path.
//! Does not own: grouped planner policy or grouped page/projection finalization.
//! Boundary: keeps generic grouped execution group-centric instead of engine-centric.

use std::collections::HashMap;

use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::{
            aggregate::{
                AggregateKind, ExecutionContext, FoldControl, GroupError,
                contracts::{AggregateStateFactory, GroupedTerminalAggregateState},
                runtime::grouped_fold::{
                    count::materialize_group_key_from_row_view,
                    utils::{
                        find_matching_group_index_in_bucket, group_key_matches_row_view,
                        stable_hash_group_values_from_row_view,
                    },
                },
            },
            group::{GroupKey, StableHash},
            pipeline::runtime::RowView,
        },
        numeric::canonical_value_compare,
        query::plan::{FieldSlot, expr::ScalarProjectionExpr},
    },
    error::InternalError,
    value::Value,
};

///
/// GroupedAggregateBundleSpec
///
/// GroupedAggregateBundleSpec captures the one aggregate slot blueprint used
/// to instantiate per-group grouped terminal states inside the shared bundle.
///

pub(super) struct GroupedAggregateBundleSpec {
    kind: AggregateKind,
    direction: Direction,
    distinct: bool,
    target_field: Option<FieldSlot>,
    compiled_input_expr: Option<ScalarProjectionExpr>,
    compiled_filter_expr: Option<ScalarProjectionExpr>,
    max_distinct_values_per_group: u64,
}

impl GroupedAggregateBundleSpec {
    // Build the canonical grouped bundle invariant for unsupported field-target
    // aggregate kinds that should already have been removed before grouped
    // bundle construction.
    fn unsupported_field_target_aggregate(kind: AggregateKind) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped field-target aggregate reached executor after planning: {kind:?}",
        ))
    }

    /// Build one bundle aggregate-slot blueprint.
    pub(super) fn new(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
        target_field: Option<FieldSlot>,
        compiled_input_expr: Option<ScalarProjectionExpr>,
        compiled_filter_expr: Option<ScalarProjectionExpr>,
        max_distinct_values_per_group: u64,
    ) -> Result<Self, InternalError> {
        if (target_field.is_some() || compiled_input_expr.is_some())
            && !kind.supports_field_target_v1()
        {
            return Err(Self::unsupported_field_target_aggregate(kind));
        }

        Ok(Self {
            kind,
            direction,
            distinct,
            target_field,
            compiled_input_expr,
            compiled_filter_expr,
            max_distinct_values_per_group,
        })
    }

    // Materialize one grouped terminal reducer state for this aggregate slot.
    fn build_state(&self) -> GroupedTerminalAggregateState {
        AggregateStateFactory::create_grouped_terminal(
            self.kind,
            self.direction,
            self.distinct,
            self.target_field.clone(),
            self.compiled_input_expr.clone(),
            self.compiled_filter_expr.clone(),
            self.max_distinct_values_per_group,
        )
    }
}

///
/// GroupedAggregateGroupState
///
/// GroupedAggregateGroupState keeps every aggregate slot state for one group
/// together so grouped finalize can walk one group table once.
///

struct GroupedAggregateGroupState {
    aggregate_states: Vec<GroupedTerminalAggregateState>,
    done_flags: Vec<bool>,
}

impl GroupedAggregateGroupState {
    // Build one per-group aggregate state row from the bundle aggregate specs.
    fn from_specs(specs: &[GroupedAggregateBundleSpec]) -> Self {
        Self {
            aggregate_states: specs
                .iter()
                .map(GroupedAggregateBundleSpec::build_state)
                .collect(),
            done_flags: vec![false; specs.len()],
        }
    }
}

///
/// GroupedAggregateGroupEntry
///
/// GroupedAggregateGroupEntry keeps one canonical group key beside its
/// aggregate state row so the generic grouped bundle can use bucketed indices
/// without duplicating group keys across a second lookup map.
///

struct GroupedAggregateGroupEntry {
    group_key: GroupKey,
    group_state: GroupedAggregateGroupState,
}

impl GroupedAggregateGroupEntry {
    // Build one new grouped bundle entry from the canonical group key and the
    // bundle-owned aggregate slot specs.
    fn from_specs(group_key: GroupKey, specs: &[GroupedAggregateBundleSpec]) -> Self {
        Self {
            group_key,
            group_state: GroupedAggregateGroupState::from_specs(specs),
        }
    }
}

///
/// GroupedFinalizeGroup
///
/// GroupedFinalizeGroup carries one canonical group key plus its per-group
/// aggregate states after ingest, but before aggregate outputs are finalized.
/// This lets page finalization stop early without first finalizing every group.
///

pub(super) struct GroupedFinalizeGroup {
    group_key: GroupKey,
    aggregate_states: Vec<GroupedTerminalAggregateState>,
}

impl GroupedFinalizeGroup {
    /// Finalize one single-aggregate grouped row directly.
    #[must_use]
    pub(super) fn finalize_single(self) -> (GroupKey, Value) {
        let mut aggregate_states = self.aggregate_states.into_iter();
        let aggregate_value = aggregate_states
            .next()
            .expect("single-aggregate grouped bundle must keep one aggregate state per group")
            .finalize();
        let has_trailing_aggregate_state = aggregate_states.next().is_some();
        debug_assert!(
            !has_trailing_aggregate_state,
            "single-aggregate grouped bundle must not retain trailing aggregate states",
        );

        (self.group_key, aggregate_value)
    }

    /// Finalize one multi-aggregate grouped row directly.
    #[must_use]
    pub(super) fn finalize(self, aggregate_count: usize) -> (GroupKey, Vec<Value>) {
        let aggregate_values = self
            .aggregate_states
            .into_iter()
            .map(GroupedTerminalAggregateState::finalize)
            .collect::<Vec<_>>();
        debug_assert_eq!(
            aggregate_values.len(),
            aggregate_count,
            "grouped bundle finalize must preserve declared aggregate slot count",
        );

        (self.group_key, aggregate_values)
    }
}

///
/// GroupedAggregateBundle
///
/// GroupedAggregateBundle owns the generic grouped fold path's shared group
/// table so ingest and finalize stay group-centric instead of engine-centric.
///

pub(super) struct GroupedAggregateBundle {
    aggregate_specs: Vec<GroupedAggregateBundleSpec>,
    bucket_index: HashMap<StableHash, Vec<usize>>,
    groups: Vec<GroupedAggregateGroupEntry>,
}

impl GroupedAggregateBundle {
    /// Build one empty grouped aggregate bundle.
    #[must_use]
    pub(super) fn new(aggregate_specs: Vec<GroupedAggregateBundleSpec>) -> Self {
        Self {
            aggregate_specs,
            bucket_index: HashMap::new(),
            groups: Vec::new(),
        }
    }

    // Search one borrowed stable-hash bucket for an existing canonical group
    // entry without first materializing a fresh owned key for this row.
    fn find_matching_borrowed_group_index(
        &self,
        borrowed_group_hash: StableHash,
        row_view: &RowView,
        group_fields: &[FieldSlot],
    ) -> Result<Option<usize>, GroupError> {
        let Some(bucket) = self.bucket_index.get(&borrowed_group_hash) else {
            return Ok(None);
        };

        find_matching_group_index_in_bucket(
            bucket.as_slice(),
            self.groups.len(),
            |group_index| self.groups.get(group_index).map(|entry| &entry.group_key),
            |group_key| group_key_matches_row_view(group_key, row_view, group_fields),
            || {},
            |group_index, group_count| {
                InternalError::query_executor_invariant(format!(
                    "grouped aggregate bucket index out of bounds: index={group_index} len={group_count}",
                ))
            },
        )
        .map_err(GroupError::from)
    }

    // Create one new group entry and preserve grouped budget accounting under
    // the old per-aggregate-state budget model.
    fn insert_new_group(
        &mut self,
        group_key: GroupKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<usize, GroupError> {
        let group_count_before_insert = self.groups.len();
        let group_capacity_before_insert = self.groups.capacity();
        execution_context.record_new_group_states(
            group_count_before_insert,
            group_capacity_before_insert,
            self.aggregate_specs.len(),
        )?;
        let new_index = self.groups.len();
        let group_hash = group_key.hash();
        self.groups.push(GroupedAggregateGroupEntry::from_specs(
            group_key,
            self.aggregate_specs.as_slice(),
        ));
        self.bucket_index
            .entry(group_hash)
            .or_default()
            .push(new_index);

        Ok(new_index)
    }

    // Resolve one grouped bundle row on the borrowed-probe fast path. Existing
    // group hits compare directly against the row view, and only misses
    // materialize an owned group key for insertion.
    fn resolve_borrowed_group_index(
        &mut self,
        execution_context: &mut ExecutionContext,
        row_view: &RowView,
        group_fields: &[FieldSlot],
    ) -> Result<usize, GroupError> {
        let borrowed_group_hash = stable_hash_group_values_from_row_view(row_view, group_fields)
            .map_err(GroupError::from)?;

        if let Some(group_index) =
            self.find_matching_borrowed_group_index(borrowed_group_hash, row_view, group_fields)?
        {
            return Ok(group_index);
        }

        let group_key =
            materialize_group_key_from_row_view(row_view, group_fields, Some(borrowed_group_hash))
                .map_err(GroupError::from)?;

        self.insert_new_group(group_key, execution_context)
    }

    // Resolve one grouped bundle row on the owned-key path used when group
    // fields cannot be compared through borrowed row-slot probes.
    fn resolve_owned_group_index(
        &mut self,
        execution_context: &mut ExecutionContext,
        row_view: &RowView,
        group_fields: &[FieldSlot],
    ) -> Result<usize, GroupError> {
        let group_key = materialize_group_key_from_row_view(row_view, group_fields, None)
            .map_err(GroupError::from)?;

        self.insert_new_group(group_key, execution_context)
    }

    // Apply one grouped input row to the resolved per-group aggregate states.
    fn apply_row_to_group(
        group_state: &mut GroupedAggregateGroupState,
        data_key: &DataKey,
        row_view: &RowView,
        execution_context: &mut ExecutionContext,
    ) -> Result<(), GroupError> {
        // Phase 1: walk the aggregate slots stored on this group directly so
        // short-circuiting stays local to the group entry instead of living in
        // a separate per-engine side structure.
        for (done, aggregate_state) in group_state
            .done_flags
            .iter_mut()
            .zip(group_state.aggregate_states.iter_mut())
        {
            if *done {
                continue;
            }

            let fold_control: FoldControl =
                aggregate_state.apply_with_row_view(data_key, Some(row_view), execution_context)?;
            if matches!(fold_control, FoldControl::Break) {
                *done = true;
            }
        }

        Ok(())
    }

    // Finish one grouped row after a concrete ingest path has resolved the
    // target group index.
    fn apply_row_to_resolved_group(
        &mut self,
        data_key: &DataKey,
        row_view: &RowView,
        execution_context: &mut ExecutionContext,
        group_index: usize,
    ) -> Result<(), GroupError> {
        let group_state = self
            .groups
            .get_mut(group_index)
            .map(|entry| &mut entry.group_state)
            .ok_or_else(|| {
                GroupError::from(InternalError::query_executor_invariant(format!(
                    "grouped bundle missing resolved group state for index: {group_index}",
                )))
            })?;

        Self::apply_row_to_group(group_state, data_key, row_view, execution_context)
    }

    /// Ingest one grouped row through the borrowed existing-group probe path.
    pub(super) fn ingest_row_with_borrowed_group_probe(
        &mut self,
        execution_context: &mut ExecutionContext,
        data_key: &DataKey,
        row_view: &RowView,
        group_fields: &[FieldSlot],
    ) -> Result<(), GroupError> {
        // Phase 1: resolve existing groups through borrowed row-slot hashing
        // so hits avoid materializing a fresh owned group key.
        let group_index =
            self.resolve_borrowed_group_index(execution_context, row_view, group_fields)?;

        self.apply_row_to_resolved_group(data_key, row_view, execution_context, group_index)
    }

    /// Ingest one grouped row through the owned group-key path.
    pub(super) fn ingest_row_with_owned_group_key(
        &mut self,
        execution_context: &mut ExecutionContext,
        data_key: &DataKey,
        row_view: &RowView,
        group_fields: &[FieldSlot],
    ) -> Result<(), GroupError> {
        // Phase 1: materialize the canonical owned key directly for grouped
        // routes whose field slots cannot use borrowed row-slot probes.
        let group_index =
            self.resolve_owned_group_index(execution_context, row_view, group_fields)?;

        self.apply_row_to_resolved_group(data_key, row_view, execution_context, group_index)
    }

    /// Return the number of aggregate slots carried by this grouped bundle.
    #[must_use]
    pub(super) const fn aggregate_count(&self) -> usize {
        self.aggregate_specs.len()
    }

    /// Return the grouped bundle as unsorted pre-finalize group entries.
    pub(super) fn into_groups(self) -> Vec<GroupedFinalizeGroup> {
        self.groups
            .into_iter()
            .map(|group_entry| GroupedFinalizeGroup {
                group_key: group_entry.group_key,
                aggregate_states: group_entry.group_state.aggregate_states,
            })
            .collect()
    }

    /// Return the grouped bundle as canonical-order groups whose aggregate
    /// states have not been finalized yet.
    #[must_use]
    pub(super) fn into_sorted_groups(self) -> Vec<GroupedFinalizeGroup> {
        let expected_group_count = self.groups.len();
        let mut out = self.into_groups();

        // Phase 2: preserve deterministic canonical grouped-key order across
        // grouped-bundle insertion order.
        out.sort_by(|left, right| {
            canonical_value_compare(
                left.group_key.canonical_value(),
                right.group_key.canonical_value(),
            )
        });
        debug_assert_eq!(
            out.len(),
            expected_group_count,
            "grouped sorted finalize groups cardinality must match tracked group count",
        );

        out
    }
}
