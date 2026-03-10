//! Module: db::executor::aggregate::contracts::grouped::engine
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::contracts::grouped::engine.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        contracts::canonical_value_compare,
        data::DataKey,
        direction::Direction,
        executor::{
            aggregate::contracts::{
                error::GroupError,
                spec::{AggregateKind, AggregateOutput},
                state::{
                    AggregateState, AggregateStateFactory, FoldControl, TerminalAggregateState,
                },
            },
            group::{GroupKey, StableHash, canonical_group_key_equals},
        },
    },
    error::InternalError,
    traits::EntityKind,
};
use std::collections::BTreeMap;

use crate::db::executor::aggregate::contracts::grouped::context::ExecutionContext;

///
/// GroupedAggregateOutput
///
/// GroupedAggregateOutput carries one finalized grouped terminal row:
/// one canonical group key paired with one aggregate terminal output.
/// Finalized rows are emitted in deterministic canonical order.
///

pub(in crate::db::executor) struct GroupedAggregateOutput<E: EntityKind> {
    group_key: GroupKey,
    output: AggregateOutput<E>,
}

impl<E: EntityKind> GroupedAggregateOutput<E> {
    #[must_use]
    pub(in crate::db::executor) const fn group_key(&self) -> &GroupKey {
        &self.group_key
    }

    #[must_use]
    pub(in crate::db::executor) const fn output(&self) -> &AggregateOutput<E> {
        &self.output
    }
}

///
/// GroupedAggregateStateSlot
///
/// GroupedAggregateStateSlot stores one canonical group key with one
/// group-local terminal aggregate state machine.
/// Slots remain bucket-local and are finalized deterministically.
///

pub(in crate::db::executor::aggregate::contracts::grouped) struct GroupedAggregateStateSlot<
    E: EntityKind,
> {
    group_key: GroupKey,
    state: TerminalAggregateState<E>,
}

impl<E: EntityKind> GroupedAggregateStateSlot<E> {
    #[must_use]
    const fn group_key(&self) -> &GroupKey {
        &self.group_key
    }
}

///
/// GroupedAggregateState
///
/// GroupedAggregateState stores per-group aggregate state machines keyed by
/// canonical group keys and stable-hash buckets.
/// Group-local states are built by `AggregateStateFactory` and finalized in a
/// deterministic order independent of insertion order.
///

pub(in crate::db::executor) struct GroupedAggregateState<E: EntityKind> {
    kind: AggregateKind,
    direction: Direction,
    distinct: bool,
    max_distinct_values_per_group: u64,
    groups: BTreeMap<StableHash, Vec<GroupedAggregateStateSlot<E>>>,
}

impl<E: EntityKind> GroupedAggregateState<E> {
    /// Build one empty grouped aggregate state container.
    #[must_use]
    pub(in crate::db::executor::aggregate) const fn new(
        kind: AggregateKind,
        direction: Direction,
        distinct: bool,
        max_distinct_values_per_group: u64,
    ) -> Self {
        Self {
            kind,
            direction,
            distinct,
            max_distinct_values_per_group,
            groups: BTreeMap::new(),
        }
    }

    /// Apply one `(group_key, data_key)` row into grouped aggregate state.
    pub(in crate::db::executor::aggregate) fn apply(
        &mut self,
        group_key: GroupKey,
        data_key: &DataKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        // Phase 1: resolve updates for existing buckets/groups.
        let hash = group_key.hash();
        if let Some(bucket) = self.groups.get_mut(&hash) {
            if let Some(slot) = bucket
                .iter_mut()
                .find(|slot| canonical_group_key_equals(slot.group_key(), &group_key))
            {
                return slot.state.apply_grouped(data_key, execution_context);
            }

            // New group in an existing bucket.
            let mut state = AggregateStateFactory::create_terminal(
                self.kind,
                self.direction,
                self.distinct,
                self.max_distinct_values_per_group,
            );
            let fold_control = state.apply_grouped(data_key, execution_context)?;
            execution_context.record_new_group::<E>(
                &group_key,
                false,
                bucket.len(),
                bucket.capacity(),
            )?;
            bucket.push(GroupedAggregateStateSlot { group_key, state });

            return Ok(fold_control);
        }

        // Phase 2: create a new bucket + group when hash was unseen.
        let mut state = AggregateStateFactory::create_terminal(
            self.kind,
            self.direction,
            self.distinct,
            self.max_distinct_values_per_group,
        );
        let fold_control = state.apply_grouped(data_key, execution_context)?;
        execution_context.record_new_group::<E>(&group_key, true, 0, 0)?;
        self.groups
            .insert(hash, vec![GroupedAggregateStateSlot { group_key, state }]);

        Ok(fold_control)
    }

    /// Return the current number of grouped keys tracked by this state.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) fn group_count(&self) -> usize {
        self.groups
            .values()
            .fold(0usize, |count, bucket| count.saturating_add(bucket.len()))
    }

    /// Finalize all groups into deterministic grouped aggregate outputs.
    #[must_use]
    pub(in crate::db::executor::aggregate) fn finalize(self) -> Vec<GroupedAggregateOutput<E>> {
        let expected_output_count = self
            .groups
            .values()
            .fold(0usize, |count, bucket| count.saturating_add(bucket.len()));
        let mut out = Vec::with_capacity(expected_output_count);

        // Phase 1: walk stable-hash buckets in deterministic key order.
        for (_, mut bucket) in self.groups {
            // Phase 2: break hash-collision ties by canonical group-key value.
            bucket.sort_by(|left, right| {
                canonical_value_compare(
                    left.group_key().canonical_value(),
                    right.group_key().canonical_value(),
                )
            });

            // Phase 3: finalize states in deterministic bucket order.
            for slot in bucket {
                out.push(GroupedAggregateOutput {
                    group_key: slot.group_key,
                    output: slot.state.finalize(),
                });
            }
        }
        debug_assert_eq!(
            out.len(),
            expected_output_count,
            "grouped finalize output cardinality must match tracked grouped state slots",
        );

        out
    }
}

///
/// AggregateEngine
///
/// Canonical aggregate reducer engine shared by scalar and grouped execution
/// spines. This keeps ingest/finalize semantics centralized across both modes.
///

pub(in crate::db::executor) enum AggregateEngine<E: EntityKind> {
    Scalar(TerminalAggregateState<E>),
    Grouped(GroupedAggregateState<E>),
}

impl<E: EntityKind> AggregateEngine<E> {
    /// Build one scalar aggregate engine.
    #[must_use]
    pub(in crate::db::executor) const fn new_scalar(
        kind: AggregateKind,
        direction: Direction,
    ) -> Self {
        Self::Scalar(AggregateStateFactory::create_terminal(
            kind,
            direction,
            false,
            u64::MAX,
        ))
    }

    /// Wrap one grouped aggregate state into the shared aggregate engine.
    #[must_use]
    pub(in crate::db::executor::aggregate::contracts::grouped) const fn from_grouped_state(
        state: GroupedAggregateState<E>,
    ) -> Self {
        Self::Grouped(state)
    }

    /// Ingest one scalar row into the scalar aggregate engine.
    pub(in crate::db::executor) fn ingest_scalar(
        &mut self,
        key: &DataKey,
    ) -> Result<FoldControl, InternalError> {
        match self {
            Self::Scalar(state) => state.apply(key),
            Self::Grouped(_) => Err(InternalError::query_executor_invariant(
                "scalar aggregate ingest reached grouped aggregate engine",
            )),
        }
    }

    /// Ingest one grouped row into the grouped aggregate engine.
    pub(in crate::db::executor) fn ingest_grouped(
        &mut self,
        group_key: GroupKey,
        data_key: &DataKey,
        execution_context: &mut ExecutionContext,
    ) -> Result<FoldControl, GroupError> {
        match self {
            Self::Grouped(state) => state.apply(group_key, data_key, execution_context),
            Self::Scalar(_) => Err(GroupError::Internal(
                InternalError::query_executor_invariant(
                    "grouped aggregate ingest reached scalar aggregate engine",
                ),
            )),
        }
    }

    /// Finalize one scalar aggregate engine into one scalar aggregate output.
    pub(in crate::db::executor) fn finalize_scalar(
        self,
    ) -> Result<AggregateOutput<E>, InternalError> {
        match self {
            Self::Scalar(state) => Ok(state.finalize()),
            Self::Grouped(_) => Err(InternalError::query_executor_invariant(
                "scalar aggregate finalize reached grouped aggregate engine",
            )),
        }
    }

    /// Finalize one grouped aggregate engine into grouped aggregate outputs.
    pub(in crate::db::executor) fn finalize_grouped(
        self,
    ) -> Result<Vec<GroupedAggregateOutput<E>>, InternalError> {
        match self {
            Self::Grouped(state) => Ok(state.finalize()),
            Self::Scalar(_) => Err(InternalError::query_executor_invariant(
                "grouped aggregate finalize reached scalar aggregate engine",
            )),
        }
    }
}
