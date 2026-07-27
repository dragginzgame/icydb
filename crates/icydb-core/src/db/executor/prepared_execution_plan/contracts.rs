//! Module: db::executor::prepared_execution_plan::contracts
//! Responsibility: executor-facing prepared-plan query contracts.
//! Does not own: query planning, access selection, or covering-read derivation.
//! Boundary: centralizes query-plan DTOs consumed by prepared execution plans.

pub(in crate::db::executor) use crate::db::query::plan::{
    AcceptedContinuationIdentity, AccessPlannedQuery, CoveringHybridReadExecutionPlan,
    CoveringReadExecutionPlan, ExecutionOrdering, PlannedContinuationContract,
};
