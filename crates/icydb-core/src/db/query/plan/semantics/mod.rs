//! Module: query::plan::semantics
//! Responsibility: semantic interpretation for query-plan model contracts.
//! Does not own: constructors or planner algorithm selection.
//! Boundary: meaning-level helpers over data-only plan model types.

mod access_projection;
mod group_distinct;
mod group_having;
mod group_model;
mod grouped_strategy;
mod identity;
mod logical;
mod pushdown;

#[cfg(feature = "sql")]
pub(in crate::db) use access_projection::access_plan_label;
#[cfg(any(feature = "sql", test))]
pub(in crate::db) use access_projection::project_explain_access_path;
pub(in crate::db) use access_projection::{
    AccessPlanProjection, project_access_plan, write_explain_access_strategy_label,
};
pub(in crate::db) use group_distinct::{
    GroupDistinctAdmissibility, GroupDistinctPolicyReason, grouped_distinct_admissibility,
    resolve_global_distinct_field_aggregate,
};
pub(in crate::db) use group_having::grouped_cursor_policy_violation;
pub(in crate::db) use group_having::{
    GroupedCursorPolicyViolation, grouped_having_binary_compare_op,
    grouped_having_compare_op_supported,
};
#[cfg(feature = "sql")]
pub(in crate::db) use group_model::canonicalize_grouped_having_numeric_literal_for_expr;
pub(in crate::db) use group_model::group_aggregate_spec_expr;
pub(in crate::db) use grouped_strategy::{GroupedPlanFallbackReason, GroupedPlanStrategy};
pub(in crate::db) use grouped_strategy::{
    grouped_plan_strategy, grouped_plan_strategy_for_explain,
};
#[cfg(feature = "sql")]
pub(in crate::db) use identity::AggregateSemanticKey;
pub(in crate::db) use identity::{AggregateIdentity, AggregateSemanticKeyRef};
pub(in crate::db::query) use logical::residual_filter_facts_for_access;
pub(in crate::db) use pushdown::{LogicalPushdownEligibility, derive_logical_pushdown_eligibility};
