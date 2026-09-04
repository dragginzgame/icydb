//! Module: query::explain
//! Responsibility: deterministic, read-only projection of logical query plans.
//! Does not own: plan execution or semantic validation.
//! Boundary: diagnostics/explain surface over intent/planner outputs.

mod access_projection;
#[cfg(feature = "sql")]
mod execution;
#[cfg(feature = "sql")]
mod json;
#[cfg(feature = "sql")]
mod nodes;
mod plan;
#[cfg(feature = "sql")]
mod projection;
#[cfg(feature = "sql")]
mod render;
mod writer;

pub(in crate::db) use access_projection::explain_access_plan;
#[cfg(feature = "sql")]
pub(in crate::db) use execution::{
    ExplainExecutionDescriptor, ExplainExecutionMode, ExplainExecutionNodeDescriptor,
    ExplainExecutionNodeType, ExplainExecutionOrderingSource, ExplainPropertyMap,
    FinalizedQueryDiagnostics, annotate_aggregate_execution_identity_properties, property_keys,
    property_values,
};
#[cfg(feature = "sql")]
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
#[cfg(feature = "sql")]
pub(in crate::db) use projection::explain_projection_field_name;
