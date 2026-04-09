//! Module: executor::aggregate::runtime::grouped_fold::bundle
//! Responsibility: shared grouped state ownership for the generic grouped fold path.
//! Does not own: grouped planner policy or grouped page/projection finalization.
//! Boundary: keeps generic grouped execution group-centric instead of engine-centric.

use std::collections::HashMap;

use crate::{
    db::{
        contracts::canonical_value_compare,
        data::DataKey,
        direction::Direction,
        executor::{
            aggregate::{
                AggregateKind, ExecutionContext, FoldControl, GroupError,
                contracts::{AggregateStateFactory, GroupedTerminalAggregateState},
            },
            group::{GroupKey, StableHash},
            pipeline::contracts::RowView,
        },
        query::plan::FieldSlot,
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
    max_distinct_values_per_group: u64,
}

impl GroupedAggregateBundleSpec {
    /// Build one bundle aggregate-slot blueprint.
    pub(super) const fn new(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
        target_field: Option<FieldSlot>,
        max_distinct_values_per_group: u64,
    ) -> Self {
        Self {
            kind,
            direction,
            distinct,
            target_field,
            max_distinct_values_per_group,
        }
    }

    // Materialize one grouped terminal reducer state for this aggregate slot.
    fn build_state(&self) -> GroupedTerminalAggregateState {
        AggregateStateFactory::create_grouped_terminal(
            self.kind,
            self.direction,
            self.distinct,
            self.target_field.clone(),
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
/// GroupedAggregateBundle
///
/// GroupedAggregateBundle owns the generic grouped fold path's shared group
/// table so ingest and finalize stay group-centric instead of engine-centric.
///

pub(super) struct GroupedAggregateBundle {
    aggregate_specs: Vec<GroupedAggregateBundleSpec>,
    borrowed_lookup_keys: HashMap<StableHash, Vec<GroupKey>>,
    groups: HashMap<GroupKey, GroupedAggregateGroupState>,
}

impl GroupedAggregateBundle {
    /// Build one empty grouped aggregate bundle.
    #[must_use]
    pub(super) fn new(aggregate_specs: Vec<GroupedAggregateBundleSpec>) -> Self {
        Self {
            aggregate_specs,
            borrowed_lookup_keys: HashMap::new(),
            groups: HashMap::new(),
        }
    }

    // Return true when one canonical grouped key matches the borrowed grouped
    // slot values from this row under the grouped executor equality contract.
    fn group_key_matches_row_view(
        group_key: &GroupKey,
        row_view: &RowView,
        group_fields: &[FieldSlot],
    ) -> Result<bool, InternalError> {
        let Value::List(canonical_group_values) = group_key.canonical_value() else {
            return Err(InternalError::query_executor_invariant(
                "grouped aggregate key must remain a canonical Value::List".to_string(),
            ));
        };
        if canonical_group_values.len() != group_fields.len() {
            return Err(InternalError::query_executor_invariant(format!(
                "grouped aggregate key field count drifted from route group fields: key_len={} group_fields_len={}",
                canonical_group_values.len(),
                group_fields.len(),
            )));
        }

        for (field, canonical_group_value) in group_fields.iter().zip(canonical_group_values) {
            if canonical_value_compare(
                row_view.require_slot_ref(field.index())?,
                canonical_group_value,
            ) != std::cmp::Ordering::Equal
            {
                return Ok(false);
            }
        }

        Ok(true)
    }

    // Search one borrowed stable-hash bucket for an existing canonical group
    // key without first materializing a fresh owned key for this row.
    fn find_matching_borrowed_group_key(
        &self,
        borrowed_group_hash: StableHash,
        row_view: &RowView,
        group_fields: &[FieldSlot],
    ) -> Result<Option<GroupKey>, GroupError> {
        let Some(bucket) = self.borrowed_lookup_keys.get(&borrowed_group_hash) else {
            return Ok(None);
        };

        for group_key in bucket {
            if Self::group_key_matches_row_view(group_key, row_view, group_fields)? {
                return Ok(Some(group_key.clone()));
            }
        }

        Ok(None)
    }

    // Materialize one owned canonical group key only when the borrowed lookup
    // path does not already resolve the group.
    fn materialize_owned_group_key<'a>(
        row_view: &RowView,
        group_fields: &[FieldSlot],
        owned_group_key: &'a mut Option<GroupKey>,
    ) -> Result<&'a GroupKey, GroupError> {
        if owned_group_key.is_none() {
            *owned_group_key = Some(
                super::materialize_group_key_from_row_view(row_view, group_fields)
                    .map_err(GroupError::from)?,
            );
        }

        owned_group_key.as_ref().ok_or_else(|| {
            GroupError::from(InternalError::query_executor_invariant(
                "grouped owned group key must materialize before use".to_string(),
            ))
        })
    }

    // Create one new group entry and preserve grouped budget accounting under
    // the old per-aggregate-state budget model.
    fn insert_new_group(
        &mut self,
        group_key: GroupKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<(), GroupError> {
        let group_count_before_insert = self.groups.len();
        let group_capacity_before_insert = self.groups.capacity();
        execution_context.record_new_group_states(
            &group_key,
            group_count_before_insert,
            group_capacity_before_insert,
            self.aggregate_specs.len(),
        )?;
        self.borrowed_lookup_keys
            .entry(group_key.hash())
            .or_default()
            .push(group_key.clone());
        let inserted = self.groups.insert(
            group_key,
            GroupedAggregateGroupState::from_specs(self.aggregate_specs.as_slice()),
        );
        debug_assert!(
            inserted.is_none(),
            "new grouped bundle group insertion must not replace an existing group",
        );

        Ok(())
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

    /// Ingest one grouped row into the shared grouped bundle.
    pub(super) fn ingest_row(
        &mut self,
        execution_context: &mut ExecutionContext,
        data_key: &DataKey,
        row_view: &RowView,
        group_fields: &[FieldSlot],
        borrowed_group_hash: Option<StableHash>,
        owned_group_key: &mut Option<GroupKey>,
    ) -> Result<(), GroupError> {
        // Phase 1: resolve the group through borrowed row-slot hashing when
        // possible so existing-group hits stay allocation-free.
        let resolved_group_key = if let Some(borrowed_group_hash) = borrowed_group_hash {
            if let Some(group_key) =
                self.find_matching_borrowed_group_key(borrowed_group_hash, row_view, group_fields)?
            {
                group_key
            } else {
                Self::materialize_owned_group_key(row_view, group_fields, owned_group_key)?.clone()
            }
        } else {
            Self::materialize_owned_group_key(row_view, group_fields, owned_group_key)?.clone()
        };

        // Phase 2: create one new group entry only when the canonical grouped
        // key is unseen in the shared bundle.
        if !self.groups.contains_key(&resolved_group_key) {
            self.insert_new_group(resolved_group_key.clone(), execution_context)?;
        }

        let group_state = self.groups.get_mut(&resolved_group_key).ok_or_else(|| {
            GroupError::from(InternalError::query_executor_invariant(format!(
                "grouped bundle missing resolved group state for key: {:?}",
                resolved_group_key.canonical_value(),
            )))
        })?;

        Self::apply_row_to_group(group_state, data_key, row_view, execution_context)
    }

    /// Finalize the shared grouped bundle into canonical grouped rows.
    #[must_use]
    pub(super) fn finalize(self) -> Vec<(Value, Vec<Value>)> {
        let expected_group_count = self.groups.len();
        let aggregate_count = self.aggregate_specs.len();
        let mut out = Vec::with_capacity(expected_group_count);

        // Phase 1: finalize each group's aggregate states exactly once into
        // one row-shaped payload.
        for (group_key, group_state) in self.groups {
            let aggregate_values = group_state
                .aggregate_states
                .into_iter()
                .map(GroupedTerminalAggregateState::finalize)
                .collect::<Vec<_>>();
            debug_assert_eq!(
                aggregate_values.len(),
                aggregate_count,
                "grouped bundle finalize must preserve declared aggregate slot count",
            );
            out.push((group_key.canonical_value().clone(), aggregate_values));
        }

        // Phase 2: preserve deterministic canonical grouped-key order across
        // hash-table iteration.
        out.sort_by(|left, right| canonical_value_compare(&left.0, &right.0));
        debug_assert_eq!(
            out.len(),
            expected_group_count,
            "grouped bundle finalize output cardinality must match tracked group count",
        );

        out
    }
}
