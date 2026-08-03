//! Module: executor::aggregate::contracts
//! Responsibility: aggregate runtime contracts, specs, grouped state, and errors.
//! Does not own: aggregate execution branching/orchestration behavior.
//! Boundary: shared aggregate contract surface consumed by aggregate executors.
#![deny(unreachable_patterns)]

mod error;
mod grouped;
mod plan;
mod state;

pub(in crate::db::executor) use error::{GroupBudgetResourceCode, GroupError};
#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) use grouped::GroupedRuntimeStats;
pub(in crate::db::executor) use grouped::{ExecutionConfig, ExecutionContext};
#[cfg(all(test, feature = "sql"))]
pub(in crate::db::executor) use plan::BinaryOp;
pub(in crate::db::executor) use plan::{
    AccessPlannedQuery, AggregateKind, CompiledExpr, EffectiveRuntimeFilterProgram, FieldSlot,
    GlobalDistinctAggregateKind, GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy,
    OrderDirection, PlannedProjectionLayout, ProjectionSpec,
};
#[cfg(feature = "sql")]
pub(in crate::db::executor) use plan::{
    Expr, FieldId, PageSpec, ProjectionField, admit_true_only_boolean_value,
    compile_scalar_projection_expr_with_schema,
};
#[cfg(feature = "sql")]
pub(in crate::db::executor) use state::AggregateFoldMode;
pub(in crate::db::executor) use state::{
    AggregateStateFactory, FoldControl, GroupedDistinctExecutionMode, GroupedTerminalAggregateState,
};
