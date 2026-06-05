//! Module: db::executor::projection::materialize::contracts
//! Responsibility: materialization test and execution projection contracts.
//! Does not own: planner expression construction or projection planning.
//! Boundary: centralizes query-plan DTOs consumed by projection materialization.

#[cfg(test)]
pub(in crate::db::executor::projection) use crate::db::query::plan::{
    PageSpec,
    expr::{CompiledExpr, ProjectionSpec, compile_scalar_projection_expr_for_model_only},
};

#[cfg(not(test))]
pub(in crate::db::executor::projection) use crate::db::query::plan::{
    PageSpec,
    expr::{CompiledExpr, ProjectionSpec},
};

pub(in crate::db::executor::projection) use crate::db::query::plan::AccessPlannedQuery;
