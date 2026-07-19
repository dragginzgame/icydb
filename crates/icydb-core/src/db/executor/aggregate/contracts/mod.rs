//! Module: executor::aggregate::contracts
//! Responsibility: aggregate runtime contracts, specs, grouped state, and errors.
//! Does not own: aggregate execution branching/orchestration behavior.
//! Boundary: shared aggregate contract surface consumed by aggregate executors.
#![deny(unreachable_patterns)]

mod error;
mod grouped;
mod plan;
mod spec;
mod state;

pub(in crate::db::executor) use error::{GroupBudgetResourceCode, GroupError};
#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) use grouped::GroupedRuntimeStats;
pub(in crate::db::executor) use grouped::{
    ExecutionConfig, ExecutionContext, ScalarAggregateEngine, execute_scalar_aggregate,
};
#[cfg(all(test, feature = "sql"))]
pub(in crate::db::executor) use plan::BinaryOp;
pub(in crate::db::executor) use plan::{
    AccessPlannedQuery, AggregateKind, CompiledExpr, CoveringProjectionFacts,
    CoveringProjectionOrder, EffectiveRuntimeFilterProgram, Expr, FieldSlot,
    GlobalDistinctAggregateKind, GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy,
    GroupedExecutionConfig, OrderDirection, OrderSpec, PageSpec, PlannedProjectionLayout,
    ProjectionSpec, constant_covering_projection_value_from_access,
    eval_builder_expr_for_value_preview, global_distinct_group_spec_for_aggregate_identity,
    plan_covering_index_adjacent_distinct_eligible, plan_covering_index_projection_facts,
};
#[cfg(feature = "sql")]
pub(in crate::db::executor) use plan::{
    FieldId, ProjectionField, admit_true_only_boolean_value,
    compile_scalar_projection_expr_from_schema,
};
pub(in crate::db::executor) use spec::{ScalarAggregateOutput, ScalarTerminalKind};
pub(in crate::db::executor) use state::{
    AggregateFoldMode, AggregateStateFactory, FoldControl, GroupedDistinctExecutionMode,
    GroupedTerminalAggregateState,
};
