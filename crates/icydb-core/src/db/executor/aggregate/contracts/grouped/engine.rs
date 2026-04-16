//! Module: db::executor::aggregate::contracts::grouped::engine
//! Responsibility: grouped aggregate state ownership and scalar aggregate engine contracts.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes grouped reducer contracts while keeping grouped implementation details internal.

#[cfg(test)]
use crate::{
    db::{
        contracts::canonical_value_compare,
        executor::{
            aggregate::contracts::{
                error::GroupError, grouped::ExecutionContext, state::GroupedTerminalAggregateState,
            },
            group::{GroupKey, StableHash},
            pipeline::contracts::RowView,
        },
        query::plan::FieldSlot,
    },
    value::Value,
};
use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::aggregate::contracts::{
            spec::{AggregateKind, ScalarAggregateOutput},
            state::{
                AggregateStateFactory, FoldControl, ScalarAggregateState,
                ScalarTerminalAggregateState,
            },
        },
    },
    error::InternalError,
};
#[cfg(test)]
use std::collections::HashMap;

///
/// GroupedAggregateOutput
///
/// GroupedAggregateOutput carries one finalized grouped terminal row: one
/// canonical group key paired with one structural aggregate output value.
/// Finalized rows are emitted in deterministic canonical order.
///

#[cfg(test)]
pub(in crate::db::executor) struct GroupedAggregateOutput {
    group_key: GroupKey,
    output: Value,
}

#[cfg(test)]
impl GroupedAggregateOutput {
    pub(in crate::db::executor::aggregate) fn into_value_pair(self) -> (Value, Value) {
        (self.group_key.canonical_value().clone(), self.output)
    }
}

///
/// GroupedAggregateState
///
/// GroupedAggregateState stores per-group grouped aggregate state machines
/// keyed directly by canonical group keys.
/// Group-local states are built by `AggregateStateFactory` and finalized in
/// canonical key order independent of hash-table insertion order.
///

#[cfg(test)]
pub(in crate::db::executor) struct GroupedAggregateState {
    kind: AggregateKind,
    direction: Direction,
    distinct: bool,
    target_field: Option<FieldSlot>,
    max_distinct_values_per_group: u64,
    groups: HashMap<GroupKey, GroupedTerminalAggregateState>,
}

#[cfg(test)]
impl GroupedAggregateState {
    // Build the canonical grouped-state invariant for unsupported field-target
    // aggregate kinds that should already have been removed before grouped
    // state construction.
    fn unsupported_field_target_aggregate(kind: AggregateKind) -> InternalError {
        InternalError::query_executor_invariant(format!(
            "grouped field-target aggregate reached executor after planning: {kind:?}",
        ))
    }

    /// Build one empty grouped aggregate state container.
    #[cfg(test)]
    #[expect(
        dead_code,
        reason = "grouped contract tests still exercise the convenience constructor"
    )]
    pub(in crate::db::executor::aggregate) fn new(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
        max_distinct_values_per_group: u64,
    ) -> Result<Self, InternalError> {
        Self::new_with_target(
            kind,
            direction,
            distinct,
            None,
            max_distinct_values_per_group,
        )
    }

    /// Build one empty grouped aggregate state container with one optional
    /// grouped field-target slot.
    pub(in crate::db::executor::aggregate) fn new_with_target(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
        target_field: Option<FieldSlot>,
        max_distinct_values_per_group: u64,
    ) -> Result<Self, InternalError> {
        if target_field.is_some()
            && !matches!(
                kind,
                AggregateKind::Count
                    | AggregateKind::Sum
                    | AggregateKind::Avg
                    | AggregateKind::Min
                    | AggregateKind::Max
            )
        {
            return Err(Self::unsupported_field_target_aggregate(kind));
        }

        Ok(Self {
            kind,
            direction,
            distinct,
            target_field,
            max_distinct_values_per_group,
            groups: HashMap::new(),
        })
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

    // Search one borrowed stable-hash bucket for a canonical grouped key that
    // matches the current row without first materializing an owned `GroupKey`.
    fn find_matching_borrowed_group_key(
        &self,
        borrowed_group_hash: StableHash,
        row_view: &RowView,
        group_fields: &[FieldSlot],
    ) -> Result<Option<GroupKey>, GroupError> {
        // Keep the borrowed probe contract for tests without carrying a second
        // grouped-key index beside the canonical group-state map itself.
        for group_key in self.groups.keys() {
            if group_key.hash() != borrowed_group_hash {
                continue;
            }
            if Self::group_key_matches_row_view(group_key, row_view, group_fields)? {
                return Ok(Some(group_key.clone()));
            }
        }

        Ok(None)
    }

    // Apply one `(group_key, data_key)` row into grouped aggregate state using
    // a borrowed grouped key to avoid hot-path clone churn at ingest callsites.
    #[cfg(test)]
    pub(in crate::db::executor::aggregate) fn apply_borrowed(
        &mut self,
        group_key: &GroupKey,
        data_key: &DataKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        self.apply_borrowed_with_row_view(group_key, data_key, None, execution_context)
    }

    // Apply one `(group_key, data_key)` row plus one already-decoded grouped
    // row view when grouped field-target semantics need slot access.
    pub(in crate::db::executor::aggregate) fn apply_borrowed_with_row_view(
        &mut self,
        group_key: &GroupKey,
        data_key: &DataKey,
        row_view: Option<&RowView>,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        // Phase 1: resolve updates for existing groups directly by canonical
        // grouped key instead of routing through a stable-hash bucket scan.
        if let Some(state) = self.groups.get_mut(group_key) {
            return state.apply_with_row_view(data_key, row_view, execution_context);
        }

        // Phase 2: create a new group when the canonical key is unseen.
        let group_count_before_insert = self.groups.len();
        let group_capacity_before_insert = self.groups.capacity();
        let mut state = AggregateStateFactory::create_grouped_terminal(
            self.kind,
            self.direction,
            self.distinct,
            self.target_field.clone(),
            self.max_distinct_values_per_group,
        );
        let fold_control = state.apply_with_row_view(data_key, row_view, execution_context)?;
        execution_context.record_new_canonical_group(
            group_key,
            group_count_before_insert,
            group_capacity_before_insert,
        )?;
        self.groups.insert(group_key.clone(), state);

        Ok(fold_control)
    }

    // Apply one grouped row while probing existing groups from borrowed row
    // slots first and materializing an owned canonical key only on misses.
    pub(in crate::db::executor::aggregate) fn apply_with_borrowed_group_probe(
        &mut self,
        data_key: &DataKey,
        row_view: &RowView,
        group_fields: &[FieldSlot],
        borrowed_group_hash: Option<StableHash>,
        owned_group_key: &mut Option<GroupKey>,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        // Phase 1: keep existing-group hits on borrowed row-slot hashing and
        // equality so the generic grouped ingest path matches the specialized
        // grouped-count fast path.
        if let Some(borrowed_group_hash) = borrowed_group_hash
            && let Some(matched_group_key) =
                self.find_matching_borrowed_group_key(borrowed_group_hash, row_view, group_fields)?
        {
            let state = self.groups.get_mut(&matched_group_key).ok_or_else(|| {
                GroupError::from(InternalError::query_executor_invariant(format!(
                    "grouped aggregate state missing borrowed-probed group key: hash={borrowed_group_hash}",
                )))
            })?;

            return state.apply_with_row_view(data_key, Some(row_view), execution_context);
        }

        // Phase 2: fall back to one lazily materialized canonical grouped key
        // when the row opens a new group or uses structured grouped values.
        let group_key = if let Some(group_key) = owned_group_key {
            group_key
        } else {
            let group_values = row_view.group_values(group_fields)?;
            let group_key = GroupKey::from_group_values(group_values)
                .map_err(|err| GroupError::from(err.into_internal_error()))?;
            owned_group_key.insert(group_key)
        };

        self.apply_borrowed_with_row_view(group_key, data_key, Some(row_view), execution_context)
    }

    /// Finalize all groups into deterministic grouped aggregate outputs.
    #[must_use]
    pub(in crate::db::executor::aggregate) fn finalize(self) -> Vec<GroupedAggregateOutput> {
        let expected_output_count = self.groups.len();
        let mut out = Vec::with_capacity(expected_output_count);

        // Phase 1: finalize every grouped state into one flat output buffer.
        for (group_key, state) in self.groups {
            out.push(GroupedAggregateOutput {
                group_key,
                output: state.finalize(),
            });
        }

        // Phase 2: sort finalized rows globally by canonical grouped-key
        // value so ordered grouped execution never inherits hash-table
        // iteration order as an accidental output contract.
        out.sort_by(|left, right| {
            canonical_value_compare(
                left.group_key.canonical_value(),
                right.group_key.canonical_value(),
            )
        });

        debug_assert_eq!(
            out.len(),
            expected_output_count,
            "grouped finalize output cardinality must match tracked grouped state slots",
        );

        out
    }
}

///
/// ScalarAggregateEngine
///
/// ScalarAggregateEngine is the structural scalar aggregate reducer engine shared by scalar
/// aggregate execution spines.
///

pub(in crate::db::executor) struct ScalarAggregateEngine {
    state: ScalarTerminalAggregateState,
}

impl ScalarAggregateEngine {
    /// Build one scalar aggregate engine.
    #[must_use]
    pub(in crate::db::executor) fn new_scalar(kind: AggregateKind, direction: Direction) -> Self {
        Self {
            state: AggregateStateFactory::create_scalar_terminal(kind, direction, false),
        }
    }

    /// Ingest one scalar candidate key into this aggregate engine.
    pub(in crate::db::executor) fn ingest(
        &mut self,
        data_key: &DataKey,
    ) -> Result<FoldControl, InternalError> {
        self.state.apply(data_key)
    }

    /// Finalize this scalar aggregate engine into one terminal output payload.
    #[must_use]
    pub(in crate::db::executor) fn finalize(self) -> ScalarAggregateOutput {
        self.state.finalize()
    }
}

// Execute one scalar aggregate engine through one canonical ingest/finalize authority.
// The caller supplies loop/key ingestion behavior while this boundary owns the
// terminal finalize projection.
pub(in crate::db::executor) fn execute_scalar_aggregate<F>(
    mut engine: ScalarAggregateEngine,
    mut ingest_all: F,
) -> Result<ScalarAggregateOutput, InternalError>
where
    F: FnMut(&mut ScalarAggregateEngine) -> Result<(), InternalError>,
{
    ingest_all(&mut engine)?;

    Ok(engine.finalize())
}
