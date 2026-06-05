//! Module: db::executor::projection::covering::contracts
//! Responsibility: executor-facing covering projection plan contracts.
//! Does not own: planner covering-read derivation or access planning.
//! Boundary: centralizes query-plan DTOs consumed by covering projection execution.

pub(in crate::db::executor) use crate::db::query::plan::{
    AccessPlannedQuery, CoveringExistingRowMode, CoveringProjectionOrder,
    CoveringReadExecutionPlan, CoveringReadField, CoveringReadFieldSource, CoveringReadPlan,
    PageSpec,
};
