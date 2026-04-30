use crate::db::{
    direction::Direction,
    executor::{
        aggregate::contracts::{
            spec::{AggregateKind, ScalarTerminalKind},
            state::{
                CompiledExpr, GroupedAggregateReducerState, GroupedDistinctExecutionMode,
                GroupedTerminalAggregateState, ScalarAggregateReducerState,
                ScalarTerminalAggregateState,
            },
        },
        group::GroupKeySet,
    },
    query::plan::FieldSlot,
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
    /// Build one scalar terminal aggregate state machine for kernel reducers.
    #[must_use]
    pub(in crate::db::executor) fn create_scalar_terminal(
        kind: ScalarTerminalKind,
        direction: Direction,
        distinct: bool,
    ) -> ScalarTerminalAggregateState {
        ScalarTerminalAggregateState {
            kind,
            direction,
            distinct,
            distinct_keys: if distinct {
                Some(GroupKeySet::new())
            } else {
                None
            },
            requires_storage_key: kind.aggregate_kind().requires_decoded_id(),
            reducer: ScalarAggregateReducerState::for_terminal_kind(kind),
        }
    }

    /// Build one grouped terminal aggregate state machine for grouped reducers.
    #[must_use]
    pub(in crate::db::executor) fn create_grouped_terminal(
        kind: AggregateKind,
        direction: Direction,
        distinct_mode: GroupedDistinctExecutionMode,
        target_field: Option<FieldSlot>,
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
            requires_storage_key: kind.requires_decoded_id(),
            reducer: GroupedAggregateReducerState::for_kind(kind),
        }
    }
}
