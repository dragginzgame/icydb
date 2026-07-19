//! Module: executor::aggregate::runtime::grouped_fold::bundle
//! Responsibility: shared grouped state ownership for the generic grouped fold path.
//! Does not own: grouped planner policy or grouped page/projection finalization.
//! Boundary: keeps generic grouped execution group-centric instead of engine-centric.

use crate::{
    db::{
        data::DecodedDataStoreKey,
        direction::Direction,
        executor::{
            aggregate::{
                AggregateKind, CompiledExpr, ExecutionContext, FieldSlot, FoldControl, GroupError,
                contracts::{
                    AggregateStateFactory, GroupedDistinctExecutionMode,
                    GroupedTerminalAggregateState,
                },
                field::{
                    AggregateFieldValueError, FieldSlot as AggregateFieldSlot,
                    resolve_aggregate_target_slot_from_planner_slot,
                },
                runtime::grouped_fold::{
                    count::materialize_group_key_from_row_view,
                    utils::{
                        GroupIndexBucket, compare_grouped_boundary_values,
                        find_matching_group_index_in_bucket, group_key_matches_row_view,
                        stable_hash_group_values_from_row_view,
                    },
                },
            },
            group::{GroupKey, StableHash, StableHashBuildHasher, StableHashMap},
            pipeline::runtime::RowView,
        },
        numeric::canonical_value_compare,
    },
    error::InternalError,
    value::Value,
};
use std::cmp::Ordering;

///
/// GroupedAggregateBundleSpec
///
/// GroupedAggregateBundleSpec captures the one aggregate slot blueprint used
/// to instantiate per-group grouped terminal states inside the shared bundle.
///

pub(super) struct GroupedAggregateBundleSpec {
    kind: AggregateKind,
    direction: Direction,
    distinct_mode: GroupedDistinctExecutionMode,
    target_field: Option<AggregateFieldSlot>,
    grouped_input_expr: Option<CompiledExpr>,
    grouped_filter_expr: Option<CompiledExpr>,
    max_distinct_values_per_group: u64,
}

impl GroupedAggregateBundleSpec {
    // Build the canonical grouped bundle invariant for unsupported field-target
    // aggregate kinds that should already have been removed before grouped
    // bundle construction.
    fn unsupported_field_target_aggregate(_kind: AggregateKind) -> InternalError {
        InternalError::query_executor_invariant()
    }

    /// Build one bundle aggregate-slot blueprint.
    pub(super) fn new(
        kind: AggregateKind,
        direction: Direction,
        distinct_mode: GroupedDistinctExecutionMode,
        target_field: Option<FieldSlot>,
        compiled_input_expr: Option<CompiledExpr>,
        compiled_filter_expr: Option<CompiledExpr>,
        max_distinct_values_per_group: u64,
    ) -> Result<Self, InternalError> {
        if (target_field.is_some() || compiled_input_expr.is_some())
            && !kind.supports_field_target()
        {
            return Err(Self::unsupported_field_target_aggregate(kind));
        }
        let target_field = target_field
            .as_ref()
            .map(|planned| resolve_aggregate_target_slot_from_planner_slot(kind, planned))
            .transpose()
            .map_err(AggregateFieldValueError::into_internal_error)?;
        Ok(Self {
            kind,
            direction,
            distinct_mode,
            target_field,
            grouped_input_expr: compiled_input_expr,
            grouped_filter_expr: compiled_filter_expr,
            max_distinct_values_per_group,
        })
    }

    // Materialize one grouped terminal reducer state for this aggregate slot.
    fn build_state(&self) -> GroupedTerminalAggregateState {
        AggregateStateFactory::create_grouped_terminal(
            self.kind,
            self.direction,
            self.distinct_mode,
            self.target_field,
            self.grouped_input_expr.clone(),
            self.grouped_filter_expr.clone(),
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
/// OrderedGroupFoldState
///
/// OrderedGroupFoldState is the single canonical ordered group-transition
/// owner shared by generic reducers and specialized grouped `COUNT(*)`.
/// It validates monotonicity, owns one active key/state pair, and releases the
/// live reservation before handing a closed group to page selection.
///

pub(super) struct OrderedGroupFoldState<State> {
    active: Option<(GroupKey, State)>,
    aggregate_state_count: usize,
}

impl<State> OrderedGroupFoldState<State> {
    /// Build one empty ordered transition state for a fixed reducer-slot count.
    #[must_use]
    pub(super) const fn new(aggregate_state_count: usize) -> Self {
        Self {
            active: None,
            aggregate_state_count,
        }
    }

    /// Apply one row to its ordered group and visit a group exactly when its key closes.
    ///
    /// The visitor returns `true` once page selection has enough information
    /// to stop the source scan. In that case the incoming next group is not
    /// opened, so no unused aggregate state survives the lookahead boundary.
    pub(super) fn apply_row(
        &mut self,
        execution_context: &mut ExecutionContext,
        incoming_key: GroupKey,
        direction: Direction,
        create_state: impl FnOnce() -> State,
        apply_to_state: impl FnOnce(&mut State, &mut ExecutionContext) -> Result<(), GroupError>,
        visit_closed_group: impl FnOnce(GroupKey, State) -> Result<bool, InternalError>,
    ) -> Result<bool, GroupError> {
        let transition = self.active.as_ref().map(|(active_key, _)| {
            compare_grouped_boundary_values(
                direction,
                active_key.canonical_value(),
                incoming_key.canonical_value(),
            )
        });

        match transition {
            None => {}
            Some(Ordering::Equal) => {
                let active = self
                    .active
                    .as_mut()
                    .map(|(_, state)| state)
                    .ok_or_else(|| GroupError::from(InternalError::query_executor_invariant()))?;
                apply_to_state(active, execution_context)?;
                return Ok(false);
            }
            Some(Ordering::Greater) => {
                return Err(GroupError::from(InternalError::query_executor_invariant()));
            }
            Some(Ordering::Less) => {
                let closed = self
                    .take_active(execution_context)
                    .ok_or_else(|| GroupError::from(InternalError::query_executor_invariant()))?;
                if visit_closed_group(closed.0, closed.1).map_err(GroupError::from)? {
                    return Ok(true);
                }
            }
        }

        self.open_group(incoming_key, create_state, execution_context)?;
        let active = self
            .active
            .as_mut()
            .map(|(_, state)| state)
            .ok_or_else(|| GroupError::from(InternalError::query_executor_invariant()))?;
        apply_to_state(active, execution_context)?;

        Ok(false)
    }

    /// Close the final active group after ordered input exhaustion.
    pub(super) fn finish(
        &mut self,
        execution_context: &mut ExecutionContext,
        visit_closed_group: impl FnOnce(GroupKey, State) -> Result<bool, InternalError>,
    ) -> Result<(), GroupError> {
        let Some(closed) = self.take_active(execution_context) else {
            return Ok(());
        };
        let _selection_complete =
            visit_closed_group(closed.0, closed.1).map_err(GroupError::from)?;

        Ok(())
    }

    // Open one active group after reserving its one-group live-state budget.
    fn open_group(
        &mut self,
        group_key: GroupKey,
        create_state: impl FnOnce() -> State,
        execution_context: &mut ExecutionContext,
    ) -> Result<(), GroupError> {
        execution_context.reserve_ordered_group_states(self.aggregate_state_count)?;
        self.active = Some((group_key, create_state()));

        Ok(())
    }

    // Release and consume the active group into the shared finalize contract.
    fn take_active(
        &mut self,
        execution_context: &mut ExecutionContext,
    ) -> Option<(GroupKey, State)> {
        let active = self.active.take()?;
        execution_context.release_ordered_group_states(self.aggregate_state_count);

        Some(active)
    }
}

///
/// OrderedGroupedAggregateFold
///
/// OrderedGroupedAggregateFold adapts generic aggregate reducer states onto
/// the shared ordered group-transition owner.
///

pub(super) struct OrderedGroupedAggregateFold {
    aggregate_specs: Vec<GroupedAggregateBundleSpec>,
    transitions: OrderedGroupFoldState<GroupedAggregateGroupState>,
}

impl OrderedGroupedAggregateFold {
    /// Build one empty ordered generic aggregate fold.
    #[must_use]
    pub(super) const fn new(aggregate_specs: Vec<GroupedAggregateBundleSpec>) -> Self {
        let aggregate_count = aggregate_specs.len();
        Self {
            aggregate_specs,
            transitions: OrderedGroupFoldState::new(aggregate_count),
        }
    }

    /// Return the number of aggregate slots carried by each active group.
    #[must_use]
    pub(super) const fn aggregate_count(&self) -> usize {
        self.aggregate_specs.len()
    }

    /// Ingest one ordered generic aggregate row through the shared transition owner.
    pub(super) fn ingest_row(
        &mut self,
        execution_context: &mut ExecutionContext,
        data_key: &DecodedDataStoreKey,
        row_view: &RowView,
        group_fields: &[FieldSlot],
        direction: Direction,
        visit_closed_group: impl FnOnce(GroupedFinalizeGroup) -> Result<bool, InternalError>,
    ) -> Result<bool, GroupError> {
        let incoming_key = materialize_group_key_from_row_view(row_view, group_fields, None)
            .map_err(GroupError::from)?;
        let specs = self.aggregate_specs.as_slice();

        self.transitions.apply_row(
            execution_context,
            incoming_key,
            direction,
            || GroupedAggregateGroupState::from_specs(specs),
            |state, context| {
                GroupedAggregateBundle::apply_row_to_group(state, data_key, row_view, context)
            },
            |group_key, state| {
                visit_closed_group(GroupedFinalizeGroup {
                    group_key,
                    aggregate_states: state.aggregate_states,
                })
            },
        )
    }

    /// Close the final active generic aggregate group after input exhaustion.
    pub(super) fn finish(
        &mut self,
        execution_context: &mut ExecutionContext,
        visit_closed_group: impl FnOnce(GroupedFinalizeGroup) -> Result<bool, InternalError>,
    ) -> Result<(), GroupError> {
        self.transitions
            .finish(execution_context, |group_key, state| {
                visit_closed_group(GroupedFinalizeGroup {
                    group_key,
                    aggregate_states: state.aggregate_states,
                })
            })
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
    pub(super) fn finalize_single(self) -> Result<(GroupKey, Value), InternalError> {
        let mut aggregate_states = self.aggregate_states.into_iter();
        let Some(aggregate_state) = aggregate_states.next() else {
            return Err(InternalError::query_executor_invariant());
        };
        let aggregate_value = aggregate_state.finalize()?;
        if aggregate_states.next().is_some() {
            return Err(InternalError::query_executor_invariant());
        }

        Ok((self.group_key, aggregate_value))
    }

    /// Finalize one multi-aggregate grouped row directly.
    pub(super) fn finalize(
        self,
        aggregate_count: usize,
    ) -> Result<(GroupKey, Vec<Value>), InternalError> {
        let aggregate_values = self
            .aggregate_states
            .into_iter()
            .map(GroupedTerminalAggregateState::finalize)
            .collect::<Result<Vec<_>, _>>()?;
        debug_assert_eq!(
            aggregate_values.len(),
            aggregate_count,
            "grouped bundle finalize must preserve declared aggregate slot count",
        );

        Ok((self.group_key, aggregate_values))
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
    bucket_index: StableHashMap<GroupIndexBucket>,
    groups: Vec<GroupedAggregateGroupEntry>,
}

impl GroupedAggregateBundle {
    /// Build one empty grouped aggregate bundle.
    #[must_use]
    pub(super) fn new(
        aggregate_specs: Vec<GroupedAggregateBundleSpec>,
        group_capacity_hint: usize,
    ) -> Self {
        Self {
            aggregate_specs,
            bucket_index: StableHashMap::with_capacity_and_hasher(
                group_capacity_hint,
                StableHashBuildHasher,
            ),
            groups: Vec::with_capacity(group_capacity_hint),
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
            |_group_index, _group_count| InternalError::query_executor_invariant(),
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
            .and_modify(|bucket| bucket.push_index(new_index))
            .or_insert_with(|| GroupIndexBucket::single(new_index));

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
        data_key: &DecodedDataStoreKey,
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
        data_key: &DecodedDataStoreKey,
        row_view: &RowView,
        execution_context: &mut ExecutionContext,
        group_index: usize,
    ) -> Result<(), GroupError> {
        let group_state = self
            .groups
            .get_mut(group_index)
            .map(|entry| &mut entry.group_state)
            .ok_or_else(|| GroupError::from(InternalError::query_executor_invariant()))?;

        Self::apply_row_to_group(group_state, data_key, row_view, execution_context)
    }

    /// Ingest one grouped row through the borrowed existing-group probe path.
    pub(super) fn ingest_row_with_borrowed_group_probe(
        &mut self,
        execution_context: &mut ExecutionContext,
        data_key: &DecodedDataStoreKey,
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
        data_key: &DecodedDataStoreKey,
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{GroupedAggregateBundleSpec, OrderedGroupedAggregateFold};
    use crate::{
        db::{
            data::{DecodedDataStoreKey, PrimaryKeyComponent},
            direction::Direction,
            executor::{
                aggregate::{
                    AggregateKind, ExecutionConfig, ExecutionContext, GroupError,
                    contracts::GroupedDistinctExecutionMode,
                },
                pipeline::runtime::RowView,
            },
            query::plan::FieldSlot,
        },
        types::EntityTag,
        value::Value,
    };

    fn data_key(value: u64) -> DecodedDataStoreKey {
        let raw =
            DecodedDataStoreKey::new(EntityTag::new(1), &PrimaryKeyComponent::Nat64(value).into())
                .to_raw()
                .expect("ordered grouped test key should encode");

        DecodedDataStoreKey::try_from_raw(&raw).expect("ordered grouped test key should decode")
    }

    fn count_rows_fold() -> OrderedGroupedAggregateFold {
        let spec = GroupedAggregateBundleSpec::new(
            AggregateKind::Count,
            Direction::Asc,
            GroupedDistinctExecutionMode::new(false, false),
            None,
            None,
            None,
            u64::MAX,
        )
        .expect("ordered grouped COUNT state should build");

        OrderedGroupedAggregateFold::new(vec![spec])
    }

    fn row(group: u64) -> RowView {
        RowView::new(vec![Some(Value::Nat64(group))])
    }

    #[test]
    fn ordered_grouped_fold_finalizes_transitions_and_releases_live_state() {
        let mut fold = count_rows_fold();
        let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
        let group_fields = [FieldSlot::from_test_slot(0, "group")];
        let mut finalized = Vec::new();

        for (key, group) in [(1, 10), (2, 10), (3, 20), (4, 20)] {
            let stopped = fold
                .ingest_row(
                    &mut context,
                    &data_key(key),
                    &row(group),
                    &group_fields,
                    Direction::Asc,
                    |closed| {
                        finalized.push(closed.finalize_single()?);
                        Ok(false)
                    },
                )
                .expect("monotonic ordered grouped row should fold");
            assert!(!stopped);
        }
        fold.finish(&mut context, |closed| {
            finalized.push(closed.finalize_single()?);
            Ok(false)
        })
        .expect("final active ordered group should close");

        assert_eq!(
            finalized,
            vec![
                (
                    crate::db::executor::group::GroupKey::from_group_values(vec![Value::Nat64(10)])
                        .expect("group key"),
                    Value::Nat64(2)
                ),
                (
                    crate::db::executor::group::GroupKey::from_group_values(vec![Value::Nat64(20)])
                        .expect("group key"),
                    Value::Nat64(2)
                ),
            ],
        );
        assert_eq!(context.budget().groups(), 2);
        assert_eq!(context.budget().aggregate_states(), 2);
        assert_eq!(context.budget().estimated_bytes(), 0);
        assert!(context.budget().peak_estimated_bytes() > 0);
        let runtime_stats = context.successful_runtime_stats(false);
        assert_eq!(runtime_stats.groups_observed(), 2);
        assert_eq!(runtime_stats.groups_finalized(), 2);
        assert_eq!(runtime_stats.peak_live_groups(), 1);
        assert_eq!(runtime_stats.peak_live_aggregate_states(), 1);
        assert_eq!(runtime_stats.peak_live_distinct_values(), 0);
        assert!(!runtime_stats.early_scan_stop());
    }

    #[test]
    fn ordered_grouped_fold_fails_closed_on_non_monotonic_group_keys() {
        let mut fold = count_rows_fold();
        let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
        let group_fields = [FieldSlot::from_test_slot(0, "group")];

        fold.ingest_row(
            &mut context,
            &data_key(1),
            &row(20),
            &group_fields,
            Direction::Asc,
            |_| Ok(false),
        )
        .expect("first ordered group should open");
        let err = fold
            .ingest_row(
                &mut context,
                &data_key(2),
                &row(10),
                &group_fields,
                Direction::Asc,
                |_| Ok(false),
            )
            .expect_err("descending key on ascending route must fail closed");

        assert!(matches!(err, GroupError::Internal(_)));
    }

    #[test]
    fn ordered_grouped_fold_does_not_open_next_group_after_selection_stop() {
        let mut fold = count_rows_fold();
        let mut context = ExecutionContext::new(ExecutionConfig::unbounded());
        let group_fields = [FieldSlot::from_test_slot(0, "group")];

        fold.ingest_row(
            &mut context,
            &data_key(1),
            &row(10),
            &group_fields,
            Direction::Asc,
            |_| Ok(false),
        )
        .expect("first ordered group should open");
        let stopped = fold
            .ingest_row(
                &mut context,
                &data_key(2),
                &row(20),
                &group_fields,
                Direction::Asc,
                |_| Ok(true),
            )
            .expect("selection stop should close the active group");

        assert!(stopped);
        assert_eq!(context.budget().groups(), 1);
        assert_eq!(context.budget().estimated_bytes(), 0);
        let runtime_stats = context.successful_runtime_stats(true);
        assert_eq!(runtime_stats.groups_observed(), 1);
        assert_eq!(runtime_stats.groups_finalized(), 1);
        assert_eq!(runtime_stats.peak_live_groups(), 1);
        assert!(runtime_stats.early_scan_stop());
    }
}
