//! Module: db::executor::projection::materialize::contracts
//! Responsibility: materialization test and execution projection contracts.
//! Does not own: planner expression construction or projection planning.
//! Boundary: centralizes query-plan DTOs consumed by projection materialization.

pub(in crate::db::executor::projection) use crate::db::query::plan::AccessPlannedQuery;
#[cfg(feature = "query")]
pub(in crate::db::executor::projection) use crate::db::query::plan::PageSpec;
pub(in crate::db::executor::projection) use crate::db::query::plan::expr::CompiledExpr;
#[cfg(feature = "query")]
pub(in crate::db::executor::projection) use crate::db::query::plan::expr::ProjectionSpec;
