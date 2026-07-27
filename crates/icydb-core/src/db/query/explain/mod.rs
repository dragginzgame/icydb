//! Module: query::explain
//! Responsibility: deterministic, read-only projection of logical query plans.
//! Does not own: plan execution or semantic validation.
//! Boundary: diagnostics/explain surface over intent/planner outputs.

mod access_projection;
#[cfg(any(test, feature = "sql-explain"))]
mod execution;
#[cfg(any(test, feature = "sql-explain"))]
mod json;
#[cfg(any(test, feature = "sql-explain"))]
mod nodes;
mod plan;
#[cfg(any(test, feature = "sql-explain"))]
mod projection;
#[cfg(any(test, feature = "sql-explain"))]
mod render;
mod writer;

pub(in crate::db) use access_projection::explain_access_plan;
#[cfg(any(test, feature = "sql-explain"))]
pub use execution::{
    ExplainAggregateTerminalPlan, ExplainExecutionDescriptor, ExplainExecutionMode,
    ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainExecutionOrderingSource,
};
#[cfg(any(test, feature = "sql-explain"))]
pub(in crate::db) use execution::{
    ExplainPropertyMap, FinalizedQueryDiagnostics,
    annotate_aggregate_execution_identity_properties, property_keys, property_values,
};
#[cfg(any(test, feature = "sql-explain"))]
pub(in crate::db) use plan::ExplainPredicate;
pub use plan::{
    ExplainAccessCandidate, ExplainAccessDecision, ExplainAccessDecisionKind,
    ExplainEligibleAlternative, ExplainPlan, ExplainRejectedIndex, ExplainResidualSummary,
    ExplainSelectedAccess,
};
pub(in crate::db) use plan::{
    ExplainAccessPath, ExplainDeleteLimit, ExplainGroupAggregate, ExplainGroupField,
    ExplainGrouping, ExplainOrderBy, ExplainOrderPushdown, ExplainPagination,
    SecondaryOrderPushdownRejection,
};
#[cfg(any(test, feature = "sql-explain"))]
pub(in crate::db) use projection::explain_projection_field_name;
