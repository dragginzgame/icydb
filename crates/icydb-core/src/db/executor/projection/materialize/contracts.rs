//! Module: db::executor::projection::materialize::contracts
//! Responsibility: materialization test and execution projection contracts.
//! Does not own: planner expression construction or projection planning.
//! Boundary: centralizes query-plan DTOs consumed by projection materialization.

pub(in crate::db::executor::projection) use crate::db::query::plan::AccessPlannedQuery;
pub(in crate::db::executor::projection) use crate::db::query::plan::expr::CompiledExpr;
pub(in crate::db::executor::projection) use crate::db::query::plan::expr::ProjectionSpec;
