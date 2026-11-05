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
                contracts::{GroupedDistinctExecutionMode, GroupedTerminalAggregateState},
                field::{
                    AggregateFieldValueError, FieldSlot as AggregateFieldSlot,
                    resolve_aggregate_target_slot_from_planner_slot,
                },
                runtime::grouped_fold::{
                    count::materialize_group_key_from_row_view,
                    utils::{
                        GroupIndexBucket, compare_grouped_boundary_values,
                        find_matching_group_index_in_bucket, group_key_matches_path_group_values,
                        group_key_matches_row_view, group_key_matches_single_group_value,
                        materialize_path_group_key, resolve_group_field_value,
                        resolve_path_group_values, stable_hash_group_values_from_row_view,
                        stable_hash_path_group_values, stable_hash_single_group_value,
                    },
                },
            },
            group::{
                GroupKey, StableHash, StableHashBuildHasher, StableHashMap,
                compare_borrowed_group_values, retained_hash_entry_backing_bytes,
                try_reserve_hash_entry, try_reserve_vec_elements,
                validate_borrowed_canonical_value,
            },
            pipeline::runtime::RowView,
        },
        query::plan::GroupField,
        resource::budget::charge_sort_work,
    },
    error::InternalError,
    value::Value,
};
use std::{cmp::Ordering, rc::Rc};

///
/// GroupedAggregateBundleSpec
///
/// GroupedAggregateBundleSpec captures the one aggregate slot blueprint used
/// to instantiate per-group grouped terminal states inside the shared bundle.
///

pub(super) struct GroupedAggregateBundleSpec {
    kind: AggregateKind,
    distinct_mode: GroupedDistinctExecutionMode,
    target_field: Option<AggregateFieldSlot>,
    grouped_input_expr: Option<Rc<CompiledExpr>>,
    grouped_filter_expr: Option<Rc<CompiledExpr>>,
    max_distinct_values_per_group: u64,
}

impl GroupedAggregateBundleSpec {
    /// Build one bundle aggregate-slot blueprint.
    pub(super) fn new(
        kind: AggregateKind,
        distinct_mode: GroupedDistinctExecutionMode,
        target_field: Option<&FieldSlot>,
        compiled_input_expr: Option<CompiledExpr>,
        compiled_filter_expr: Option<CompiledExpr>,
        max_distinct_values_per_group: u64,
    ) -> Result<Self, InternalError> {
        if (target_field.is_some() || compiled_input_expr.is_some())
            && !kind.supports_field_target()
        {
            return Err(InternalError::query_executor_invariant());
        }
        let target_field = target_field
            .map(|planned| resolve_aggregate_target_slot_from_planner_slot(kind, planned))
            .transpose()
            .map_err(AggregateFieldValueError::into_internal_error)?;
        Ok(Self {
            kind,
            distinct_mode,
            target_field,
            grouped_input_expr: compiled_input_expr.map(Rc::new),
            grouped_filter_expr: compiled_filter_expr.map(Rc::new),
            max_distinct_values_per_group,
        })
    }

    // Share the immutable expressions; reducers and distinct sets are created
    // independently for each group and may outlive this blueprint.
    fn build_state(&self) -> GroupedTerminalAggregateState {
        GroupedTerminalAggregateState::new(
            self.kind,
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
    /// The repeatable borrowed input is materialized only when a group opens.
    pub(super) fn apply_row<'row, Values>(
        &mut self,
        execution_context: &mut ExecutionContext,
        incoming_values: impl Fn() -> Values,
        direction: Direction,
        create_state: impl FnOnce() -> State,
        apply_to_state: impl FnOnce(&mut State, &mut ExecutionContext) -> Result<(), GroupError>,
        visit_closed_group: impl FnOnce(GroupKey, State) -> Result<bool, InternalError>,
    ) -> Result<bool, GroupError>
    where
        Values: Iterator<Item = Result<&'row Value, InternalError>>,
    {
        let transition = self.compare_row(direction, incoming_values())?;

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

        // Repeated and unused lookahead rows only borrow their key values.
        // Own and hash once the transition proves this key will be retained.
        let values = incoming_values()
            .map(Result::<&Value, InternalError>::cloned)
            .collect::<Result<Vec<_>, _>>()?;
        let group_key = GroupKey::from_group_values(values).map_err(GroupError::from)?;
        self.open_group(group_key, create_state, execution_context)?;
        let active = self
            .active
            .as_mut()
            .map(|(_, state)| state)
            .ok_or_else(|| GroupError::from(InternalError::query_executor_invariant()))?;
        apply_to_state(active, execution_context)?;

        Ok(false)
    }

    // Validate every borrowed component, even after an earlier component fixes
    // the ordering. Canonicalization used to reject malformed later maps before
    // any transition or page-selection callback could run.
    fn compare_row<'row>(
        &self,
        direction: Direction,
        incoming_values: impl Iterator<Item = Result<&'row Value, InternalError>>,
    ) -> Result<Option<Ordering>, InternalError> {
        let canonical_values = self
            .active
            .as_ref()
            .map(|(key, _)| match key.canonical_value() {
                Value::List(values) => Ok(values.as_slice()),
                _ => Err(InternalError::query_executor_invariant()),
            })
            .transpose()?;
        let mut ordering = Ordering::Equal;
        let mut value_count = 0;
        for incoming in incoming_values {
            let incoming = incoming?;
            validate_borrowed_canonical_value(incoming)?;
            if let Some(values) = canonical_values {
                let canonical = values
                    .get(value_count)
                    .ok_or_else(InternalError::query_executor_invariant)?;
                if ordering.is_eq() {
                    ordering = compare_borrowed_group_values(canonical, incoming);
                }
            }
            value_count += 1;
        }
        if canonical_values.is_some_and(|values| values.len() != value_count) {
            return Err(InternalError::query_executor_invariant());
        }

        Ok(canonical_values.map(|_| match direction {
            Direction::Asc => ordering,
            Direction::Desc => ordering.reverse(),
        }))
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

    // Open one active group, including an implicit group before any source row,
    // after reserving its one-group live-state budget.
    pub(super) fn open_group(
        &mut self,
        group_key: GroupKey,
        create_state: impl FnOnce() -> State,
        execution_context: &mut ExecutionContext,
    ) -> Result<(), GroupError> {
        if self.active.is_some() {
            return Err(GroupError::from(InternalError::query_executor_invariant()));
        }
        execution_context.reserve_ordered_group_states(self.aggregate_state_count, &group_key)?;
        self.active = Some((group_key, create_state()));

        Ok(())
    }

    // Release and consume the active group into the shared finalize contract.
    fn take_active(
        &mut self,
        execution_context: &mut ExecutionContext,
    ) -> Option<(GroupKey, State)> {
        let active = self.active.take()?;
        execution_context.release_ordered_group_states(self.aggregate_state_count, &active.0);

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

    // Admit the empty-tuple group with the ordinary initial reducer states.
    pub(super) fn open_implicit_group(
        &mut self,
        execution_context: &mut ExecutionContext,
    ) -> Result<(), GroupError> {
        let specs = self.aggregate_specs.as_slice();
        self.transitions.open_group(
            GroupKey::from_group_values(Vec::new()).map_err(GroupError::from)?,
            || GroupedAggregateGroupState::from_specs(specs),
            execution_context,
        )
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
        self.ingest_group_key(
            execution_context,
            data_key,
            row_view,
            || {
                group_fields
                    .iter()
                    .map(|field| row_view.require_slot_value(field.index()))
            },
            direction,
            visit_closed_group,
        )
    }

    /// Ingest one ordered path-aware row through the shared transition owner.
    pub(super) fn ingest_path_row(
        &mut self,
        execution_context: &mut ExecutionContext,
        data_key: &DecodedDataStoreKey,
        row_view: &RowView,
        group_fields: &[GroupField],
        direction: Direction,
        visit_closed_group: impl FnOnce(GroupedFinalizeGroup) -> Result<bool, InternalError>,
    ) -> Result<bool, GroupError> {
        self.ingest_group_key(
            execution_context,
            data_key,
            row_view,
            || {
                group_fields
                    .iter()
                    .map(|field| resolve_group_field_value(row_view, field))
            },
            direction,
            visit_closed_group,
        )
    }

    fn ingest_group_key<'row, Values>(
        &mut self,
        execution_context: &mut ExecutionContext,
        data_key: &DecodedDataStoreKey,
        row_view: &RowView,
        incoming_values: impl Fn() -> Values,
        direction: Direction,
        visit_closed_group: impl FnOnce(GroupedFinalizeGroup) -> Result<bool, InternalError>,
    ) -> Result<bool, GroupError>
    where
        Values: Iterator<Item = Result<&'row Value, InternalError>>,
    {
        let specs = self.aggregate_specs.as_slice();

        self.transitions.apply_row(
            execution_context,
            incoming_values,
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
    pub(super) const fn new(aggregate_specs: Vec<GroupedAggregateBundleSpec>) -> Self {
        Self {
            aggregate_specs,
            bucket_index: StableHashMap::with_hasher(StableHashBuildHasher),
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
            |group_index| self.groups.get(group_index).map(|entry| &entry.group_key),
            |group_key| group_key_matches_row_view(group_key, row_view, group_fields),
        )
        .map_err(GroupError::from)
    }

    fn find_matching_path_group_index(
        &self,
        group_hash: StableHash,
        values: &[&Value],
    ) -> Result<Option<usize>, GroupError> {
        let Some(bucket) = self.bucket_index.get(&group_hash) else {
            return Ok(None);
        };

        find_matching_group_index_in_bucket(
            bucket.as_slice(),
            |group_index| self.groups.get(group_index).map(|entry| &entry.group_key),
            |group_key| group_key_matches_path_group_values(group_key, values),
        )
        .map_err(GroupError::from)
    }

    // Admit one canonical group and its reducer states before retaining backing.
    pub(super) fn insert_new_group(
        &mut self,
        group_key: GroupKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<usize, GroupError> {
        execution_context.record_new_group_states(self.aggregate_specs.len(), &group_key)?;
        let new_index = self.groups.len();
        let group_hash = group_key.hash();
        let new_hash_bucket = !self.bucket_index.contains_key(&group_hash);
        let index_backing_bytes = self.bucket_index.get(&group_hash).map_or_else(
            retained_hash_entry_backing_bytes::<StableHash, GroupIndexBucket>,
            GroupIndexBucket::retained_insert_backing_bytes,
        );
        execution_context.record_structural_backing(index_backing_bytes)?;
        try_reserve_vec_elements(&mut self.groups, 1).map_err(GroupError::from)?;
        if new_hash_bucket {
            try_reserve_hash_entry(&mut self.bucket_index).map_err(GroupError::from)?;
        }
        self.groups.push(GroupedAggregateGroupEntry::from_specs(
            group_key,
            self.aggregate_specs.as_slice(),
        ));
        if let Some(bucket) = self.bucket_index.get_mut(&group_hash) {
            bucket.push_index(new_index).map_err(GroupError::from)?;
        } else {
            self.bucket_index
                .insert(group_hash, GroupIndexBucket::single(new_index));
        }

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

        // Owned keys already carry canonical equality and a stable hash. Probe
        // the existing bucket before charging or allocating another group.
        if let Some(bucket) = self.bucket_index.get(&group_key.hash()) {
            let existing = find_matching_group_index_in_bucket(
                bucket.as_slice(),
                |group_index| self.groups.get(group_index).map(|entry| &entry.group_key),
                |existing| Ok(existing == &group_key),
            )
            .map_err(GroupError::from)?;
            if let Some(group_index) = existing {
                return Ok(group_index);
            }
        }

        self.insert_new_group(group_key, execution_context)
    }

    fn resolve_path_group_index(
        &mut self,
        execution_context: &mut ExecutionContext,
        row_view: &RowView,
        group_fields: &[GroupField],
    ) -> Result<usize, GroupError> {
        if let [field] = group_fields {
            let group_value =
                resolve_group_field_value(row_view, field).map_err(GroupError::from)?;
            let group_hash =
                stable_hash_single_group_value(group_value).map_err(GroupError::from)?;
            if let Some(bucket) = self.bucket_index.get(&group_hash) {
                let existing = find_matching_group_index_in_bucket(
                    bucket.as_slice(),
                    |group_index| self.groups.get(group_index).map(|entry| &entry.group_key),
                    |group_key| group_key_matches_single_group_value(group_key, group_value),
                )
                .map_err(GroupError::from)?;
                if let Some(group_index) = existing {
                    return Ok(group_index);
                }
            }

            let identity_canonical_form = field.has_identity_group_canonical_form();
            let group_key = if identity_canonical_form {
                GroupKey::from_single_canonical_group_value_with_hash(
                    group_value.clone(),
                    group_hash,
                )
            } else {
                GroupKey::from_single_group_value_with_hash(group_value.clone(), group_hash)
                    .map_err(GroupError::from)?
            };
            return self.insert_new_group(group_key, execution_context);
        }

        let values = resolve_path_group_values(row_view, group_fields).map_err(GroupError::from)?;
        let group_hash = stable_hash_path_group_values(&values).map_err(GroupError::from)?;
        if let Some(group_index) = self.find_matching_path_group_index(group_hash, &values)? {
            return Ok(group_index);
        }

        let group_key =
            materialize_path_group_key(&values, group_hash).map_err(GroupError::from)?;
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

    /// Ingest one row through allocation-free path-aware hash/equality probes.
    pub(super) fn ingest_row_with_path_group_probe(
        &mut self,
        execution_context: &mut ExecutionContext,
        data_key: &DecodedDataStoreKey,
        row_view: &RowView,
        group_fields: &[GroupField],
    ) -> Result<(), GroupError> {
        let group_index =
            self.resolve_path_group_index(execution_context, row_view, group_fields)?;
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

    /// Return groups in the same directional key order used by page selection
    /// and continuation boundaries, before finalizing their aggregate states.
    pub(super) fn into_sorted_groups(
        self,
        direction: Direction,
    ) -> Result<Vec<GroupedFinalizeGroup>, InternalError> {
        let expected_group_count = self.groups.len();
        charge_sort_work::<GroupedFinalizeGroup>(expected_group_count)?;
        let mut out = self.into_groups();

        // Sorting and cursor filtering must agree before HAVING and offset
        // consume this stream; candidate ranking cannot repair its order later.
        out.sort_by(|left, right| {
            compare_grouped_boundary_values(
                direction,
                left.group_key.canonical_value(),
                right.group_key.canonical_value(),
            )
        });
        debug_assert_eq!(
            out.len(),
            expected_group_count,
            "grouped sorted finalize groups cardinality must match tracked group count",
        );

        Ok(out)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
