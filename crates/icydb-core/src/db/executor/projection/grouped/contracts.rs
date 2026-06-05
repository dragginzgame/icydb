//! Module: db::executor::projection::grouped::contracts
//! Responsibility: executor-facing grouped projection contracts.
//! Does not own: planner expression construction or grouped aggregate planning.
//! Boundary: centralizes query-plan DTOs consumed by grouped projection execution.

pub(in crate::db::executor) use crate::db::query::plan::{
    FieldSlot, GroupedAggregateExecutionSpec, PlannedProjectionLayout,
    expr::{
        CompiledExpr, CompiledExprValueReader, ProjectionSpec, compile_grouped_projection_expr,
        compile_grouped_projection_plan, evaluate_grouped_having_expr,
    },
};
