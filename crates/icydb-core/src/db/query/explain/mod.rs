//! Module: query::explain
//! Responsibility: deterministic, read-only projection of logical query plans.
//! Does not own: plan execution or semantic validation.
//! Boundary: diagnostics/explain surface over intent/planner outputs.

mod access_projection;
mod execution;
mod json;
mod nodes;
mod plan;
mod projection;
mod render;
mod writer;

pub(in crate::db) use access_projection::explain_access_plan;
pub use execution::{
    ExplainAggregateTerminalPlan, ExplainExecutionDescriptor, ExplainExecutionMode,
    ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainExecutionOrderingSource,
};
pub(in crate::db) use execution::{
    ExplainPropertyMap, FinalizedQueryDiagnostics,
    annotate_aggregate_execution_identity_properties, property_keys, property_values,
};
pub use plan::{
    ExplainAccessCandidateV1, ExplainAccessDecisionKind, ExplainAccessDecisionV1,
    ExplainEligibleAlternativeV1, ExplainPlan, ExplainRejectedIndexV1, ExplainResidualSummaryV1,
    ExplainSelectedAccessV1,
};
pub(in crate::db) use plan::{
    ExplainAccessPath, ExplainDeleteLimit, ExplainGroupAggregate, ExplainGroupField,
    ExplainGrouping, ExplainOrderBy, ExplainOrderPushdown, ExplainPagination, ExplainPredicate,
    SecondaryOrderPushdownRejection,
};
#[cfg(test)]
pub(in crate::db) use plan::{ExplainGroupHaving, ExplainOrder};
pub(in crate::db) use projection::explain_projection_field_name;

///
/// TESTS
///

#[cfg(test)]
mod tests;
