//! Module: query::explain
//! Responsibility: deterministic, read-only projection of logical query plans.
//! Does not own: plan execution or semantic validation.
//! Boundary: diagnostics/explain surface over intent/planner outputs.

mod access_projection;
mod execution;
mod json;
mod nodes;
mod plan;
mod predicate;
mod projection;
mod render;
mod writer;

pub(in crate::db) use access_projection::explain_access_plan;
pub(crate) use execution::ExplainPropertyMap;
pub(in crate::db) use execution::FinalizedQueryDiagnostics;
pub use execution::{
    ExplainAggregateTerminalPlan, ExplainExecutionDescriptor, ExplainExecutionMode,
    ExplainExecutionNodeDescriptor, ExplainExecutionNodeType, ExplainExecutionOrderingSource,
};
pub use plan::ExplainPlan;
pub(crate) use plan::{
    ExplainAccessPath, ExplainDeleteLimit, ExplainGroupAggregate, ExplainGroupField,
    ExplainGrouping, ExplainOrderBy, ExplainOrderPushdown, ExplainPagination, ExplainPredicate,
    SecondaryOrderPushdownRejection,
};
#[cfg(test)]
pub(crate) use plan::{ExplainGroupHaving, ExplainOrder};
pub(in crate::db) use predicate::explain_predicate_from_expr;
pub(in crate::db) use projection::explain_projection_field_name;

///
/// TESTS
///

#[cfg(test)]
mod tests;
