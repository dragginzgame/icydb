//! Module: db::executor::aggregate::contracts::grouped::engine
//! Responsibility: grouped aggregate state ownership and scalar aggregate engine contracts.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes grouped reducer contracts while keeping grouped implementation details internal.

use crate::{
    db::{
        contracts::canonical_value_compare,
        data::DataKey,
        direction::Direction,
        executor::{
            aggregate::contracts::{
                error::GroupError,
                grouped::ExecutionContext,
                spec::{AggregateKind, ScalarAggregateOutput},
                state::{
                    AggregateStateFactory, FoldControl, GroupedTerminalAggregateState,
                    ScalarAggregateState, ScalarTerminalAggregateState,
                },
            },
            group::{GroupKey, StableHash, canonical_group_key_equals},
            pipeline::contracts::RowView,
        },
        query::plan::FieldSlot,
    },
    error::InternalError,
    value::Value,
};
use std::collections::BTreeMap;

type ScalarAggregateIngestAllFn<'f> =
    dyn FnMut(&mut ScalarAggregateEngine) -> Result<(), InternalError> + 'f;

///
/// GroupedAggregateOutput
///
/// GroupedAggregateOutput carries one finalized grouped terminal row: one
/// canonical group key paired with one structural aggregate output value.
/// Finalized rows are emitted in deterministic canonical order.
///

pub(in crate::db::executor) struct GroupedAggregateOutput {
    group_key: GroupKey,
    output: Value,
}

impl GroupedAggregateOutput {
    pub(in crate::db::executor::aggregate) fn into_value_pair(self) -> (Value, Value) {
        (self.group_key.canonical_value().clone(), self.output)
    }
}

///
/// GroupedAggregateStateSlot
///
/// GroupedAggregateStateSlot stores one canonical group key with one
/// group-local structural terminal aggregate state machine.
/// Slots remain bucket-local and are finalized deterministically.
///

pub(in crate::db::executor::aggregate::contracts::grouped) struct GroupedAggregateStateSlot {
    group_key: GroupKey,
    state: GroupedTerminalAggregateState,
}

impl GroupedAggregateStateSlot {
    #[must_use]
    const fn group_key(&self) -> &GroupKey {
        &self.group_key
    }
}

///
/// GroupedAggregateState
///
/// GroupedAggregateState stores per-group grouped aggregate state machines
/// keyed by canonical group keys and stable-hash buckets.
/// Group-local states are built by `AggregateStateFactory` and finalized in a
/// deterministic order independent of insertion order.
///

pub(in crate::db::executor) struct GroupedAggregateState {
    kind: AggregateKind,
    direction: Direction,
    distinct: bool,
    target_field: Option<FieldSlot>,
    max_distinct_values_per_group: u64,
    groups: BTreeMap<StableHash, Vec<GroupedAggregateStateSlot>>,
}

impl GroupedAggregateState {
    /// Build one empty grouped aggregate state container.
    #[cfg(test)]
    #[expect(
        dead_code,
        reason = "grouped contract tests still exercise the compatibility constructor"
    )]
    #[must_use]
    pub(in crate::db::executor::aggregate) const fn new(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
        max_distinct_values_per_group: u64,
    ) -> Self {
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
    #[must_use]
    pub(in crate::db::executor::aggregate) const fn new_with_target(
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
            groups: BTreeMap::new(),
        }
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
        // Phase 1: resolve updates for existing buckets/groups.
        let hash = group_key.hash();
        if let Some(bucket) = self.groups.get_mut(&hash) {
            if let Some(slot) = bucket
                .iter_mut()
                .find(|slot| canonical_group_key_equals(slot.group_key(), group_key))
            {
                return slot
                    .state
                    .apply_with_row_view(data_key, row_view, execution_context);
            }

            // New group in an existing bucket.
            let mut state = AggregateStateFactory::create_grouped_terminal(
                self.kind,
                self.direction,
                self.distinct,
                self.target_field.clone(),
                self.max_distinct_values_per_group,
            );
            let fold_control = state.apply_with_row_view(data_key, row_view, execution_context)?;
            execution_context.record_new_group(
                group_key,
                false,
                bucket.len(),
                bucket.capacity(),
            )?;
            bucket.push(GroupedAggregateStateSlot {
                group_key: group_key.clone(),
                state,
            });

            return Ok(fold_control);
        }

        // Phase 2: create a new bucket + group when hash was unseen.
        let mut state = AggregateStateFactory::create_grouped_terminal(
            self.kind,
            self.direction,
            self.distinct,
            self.target_field.clone(),
            self.max_distinct_values_per_group,
        );
        let fold_control = state.apply_with_row_view(data_key, row_view, execution_context)?;
        execution_context.record_new_group(group_key, true, 0, 0)?;
        self.groups.insert(
            hash,
            vec![GroupedAggregateStateSlot {
                group_key: group_key.clone(),
                state,
            }],
        );

        Ok(fold_control)
    }

    /// Finalize all groups into deterministic grouped aggregate outputs.
    #[must_use]
    pub(in crate::db::executor::aggregate) fn finalize(self) -> Vec<GroupedAggregateOutput> {
        let expected_output_count = self
            .groups
            .values()
            .fold(0usize, |count, bucket| count.saturating_add(bucket.len()));
        let mut out = Vec::with_capacity(expected_output_count);

        // Phase 1: finalize every grouped state slot into one flat output
        // buffer without treating stable-hash bucket order as output order.
        for (_, bucket) in self.groups {
            for slot in bucket {
                out.push(GroupedAggregateOutput {
                    group_key: slot.group_key,
                    output: slot.state.finalize(),
                });
            }
        }

        // Phase 2: sort finalized rows globally by canonical grouped-key
        // value so ordered grouped execution never inherits stable-hash
        // bucket order as an accidental output contract.
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
/// GroupedAggregateEngine
///
/// GroupedAggregateEngine is the structural grouped reducer boundary used by
/// grouped runtime execution. Grouped fold logic consumes only this trait so
/// grouped runtime no longer needs entity-typed aggregate engine containers.
///

pub(in crate::db::executor) trait GroupedAggregateEngine {
    /// Ingest one grouped row into one grouped aggregate engine.
    fn ingest(
        &mut self,
        execution_context: &mut ExecutionContext,
        data_key: &DataKey,
        group_key: &GroupKey,
        row_view: &RowView,
    ) -> Result<FoldControl, GroupError>;

    /// Finalize one grouped aggregate engine into structural `(group_key, value)` pairs.
    fn finalize(self: Box<Self>) -> Result<Vec<(Value, Value)>, InternalError>;
}

impl GroupedAggregateEngine for GroupedAggregateState {
    fn ingest(
        &mut self,
        execution_context: &mut ExecutionContext,
        data_key: &DataKey,
        group_key: &GroupKey,
        row_view: &RowView,
    ) -> Result<FoldControl, GroupError> {
        self.apply_borrowed_with_row_view(group_key, data_key, Some(row_view), execution_context)
    }

    fn finalize(self: Box<Self>) -> Result<Vec<(Value, Value)>, InternalError> {
        Ok((*self)
            .finalize()
            .into_iter()
            .map(GroupedAggregateOutput::into_value_pair)
            .collect())
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
    pub(in crate::db::executor) const fn new_scalar(
        kind: AggregateKind,
        direction: Direction,
    ) -> Self {
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
pub(in crate::db::executor) fn execute_scalar_aggregate(
    mut engine: ScalarAggregateEngine,
    ingest_all: &mut ScalarAggregateIngestAllFn<'_>,
) -> Result<ScalarAggregateOutput, InternalError> {
    ingest_all(&mut engine)?;

    Ok(engine.finalize())
}
