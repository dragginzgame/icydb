//! Module: executor::aggregate::contracts::plan
//! Responsibility: aggregate-facing query-plan contracts.
//! Does not own: logical aggregate planning or query-plan construction.
//! Boundary: centralizes query-plan DTOs consumed by aggregate execution.

#[cfg(feature = "sql")]
pub(in crate::db::executor) use crate::db::query::plan::expr::{
    Expr, FieldId, ProjectionField, admit_true_only_boolean_value,
    compile_scalar_projection_expr_with_schema,
};
pub(in crate::db::executor) use crate::db::query::plan::{
    AccessPlannedQuery, AggregateKind, EffectiveRuntimeFilterProgram, FieldSlot,
    GlobalDistinctAggregateKind, GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy,
    OrderDirection, PlannedProjectionLayout,
    expr::{CompiledExpr, ProjectionSpec, collapse_true_only_boolean_admission},
};

#[cfg(all(test, feature = "sql"))]
pub(in crate::db::executor) use crate::db::query::plan::expr::BinaryOp;
