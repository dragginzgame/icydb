//! Module: executor::aggregate
//! Responsibility: structural aggregate contracts and runtime execution.
//! Does not own: typed/dynamic result adapters or logical query planning.
//! Boundary: consumes accepted prepared plans and emits structural aggregate rows.

pub(in crate::db::executor) mod capability;
mod contracts;
mod exact_terminal;
pub(in crate::db::executor) mod field;
pub(in crate::db::executor) mod runtime;
#[cfg(feature = "sql")]
mod scalar_terminals;
pub(in crate::db::executor::aggregate) mod value_reducer;

pub(in crate::db::executor) use capability::{
    AggregateExecutionPolicyInputs, derive_aggregate_execution_policy,
};
#[cfg(feature = "sql")]
pub(in crate::db::executor) use contracts::AggregateFoldMode;
#[cfg(all(test, feature = "sql"))]
pub(in crate::db::executor) use contracts::BinaryOp;
#[cfg(feature = "sql")]
pub(in crate::db::executor) use contracts::FieldId;
pub(in crate::db::executor) use contracts::{
    AccessPlannedQuery, AggregateKind, CompiledExpr, EffectiveRuntimeFilterProgram,
    ExecutionConfig, ExecutionContext, FieldSlot, FoldControl, GlobalDistinctAggregateKind,
    GroupError, GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy, OrderDirection,
    PlannedProjectionLayout, ProjectionSpec,
};
#[cfg(feature = "sql")]
pub(in crate::db::executor) use contracts::{
    Expr, ProjectionField, admit_true_only_boolean_value,
    compile_scalar_projection_expr_with_schema,
};
#[cfg(feature = "sql")]
pub(in crate::db) use exact_terminal::execute_exact_indexed_numeric_aggregate_for_canister;
pub(in crate::db) use exact_terminal::{
    ExactCardinalityTarget, execute_exact_cardinality_for_canister,
};
#[cfg(feature = "sql")]
pub(in crate::db) use scalar_terminals::{
    StructuralAggregateRequest, StructuralAggregateTerminal, StructuralAggregateTerminalKind,
    execute_structural_aggregate_rows_for_canister,
};
