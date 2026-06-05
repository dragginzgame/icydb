//! Module: db::executor::prepared_execution_plan::contracts
//! Responsibility: executor-facing prepared-plan query contracts.
//! Does not own: query planning, access selection, or covering-read derivation.
//! Boundary: centralizes query-plan DTOs consumed by prepared execution plans.

pub(in crate::db::executor) use crate::db::query::plan::{
    AccessPlannedQuery, CoveringReadExecutionPlan, CoveringReadPlan, ExecutionOrdering, GroupSpec,
    OrderSpec, PlannedContinuationContract, QueryMode,
    constant_covering_projection_value_from_access,
    covering_index_projection_facts_with_primary_key_names as covering_index_projection_facts,
};
