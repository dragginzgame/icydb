//! Module: executor::aggregate::contracts::state::factory
//! Responsibility: aggregate state-machine construction.
//! Does not own: route planning, stream execution, or reducer semantics.
//! Boundary: converts prepared aggregate contracts into initial reducer state.

use crate::db::{
    direction::Direction,
    executor::{
        aggregate::contracts::{
            plan::CompiledExpr,
            spec::AggregateKind,
            state::{
                GroupedAggregateReducerState, GroupedDistinctExecutionMode,
                GroupedTerminalAggregateState,
            },
        },
        aggregate::field::FieldSlot as AggregateFieldSlot,
        group::GroupKeySet,
    },
};

///
/// AggregateStateFactory
///
/// AggregateStateFactory builds canonical scalar and grouped terminal state
/// machines from route-owned kind/direction decisions.
/// This keeps state initialization centralized at one boundary.
///

pub(in crate::db::executor) struct AggregateStateFactory;

impl AggregateStateFactory {
    /// Build one grouped terminal aggregate state machine for grouped reducers.
    #[must_use]
    pub(in crate::db::executor) fn create_grouped_terminal(
        kind: AggregateKind,
        direction: Direction,
        distinct_mode: GroupedDistinctExecutionMode,
        target_field: Option<AggregateFieldSlot>,
        grouped_input_expr: Option<CompiledExpr>,
        grouped_filter_expr: Option<CompiledExpr>,
        max_distinct_values_per_group: u64,
    ) -> GroupedTerminalAggregateState {
        GroupedTerminalAggregateState {
            kind,
            direction,
            distinct_mode,
            max_distinct_values_per_group,
            distinct_keys: if distinct_mode.enabled() {
                Some(GroupKeySet::new())
            } else {
                None
            },
            target_field,
            grouped_input_expr,
            grouped_filter_expr,
            requires_primary_key_value: kind.requires_decoded_id(),
            reducer: GroupedAggregateReducerState::for_kind(kind),
        }
    }
}
