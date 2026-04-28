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

pub(crate) use access_projection::{
    AccessPlanProjection, access_plan_label, explain_access_kind_label,
    explain_access_strategy_label, project_access_plan, project_explain_access_path,
};
pub(in crate::db) use group_distinct::global_distinct_group_spec_for_aggregate_identity;
pub(crate) use group_distinct::{
    GroupDistinctAdmissibility, GroupDistinctPolicyReason, grouped_distinct_admissibility,
    resolve_global_distinct_field_aggregate,
};
#[cfg(test)]
pub(crate) use group_distinct::{
    global_distinct_field_aggregate_admissibility, is_global_distinct_field_aggregate_candidate,
};
#[cfg(test)]
pub(crate) use group_having::evaluate_grouped_having_compare;
pub(in crate::db) use group_having::grouped_cursor_policy_violation;
pub(in crate::db) use group_having::grouped_having_compare_expr;
pub(crate) use group_having::{
    GroupedCursorPolicyViolation, grouped_having_binary_compare_op,
    grouped_having_compare_op_supported,
};
pub(in crate::db) use group_model::group_aggregate_spec_expr;
pub(in crate::db) use grouped_strategy::grouped_plan_strategy;
pub(crate) use grouped_strategy::{GroupedPlanFallbackReason, GroupedPlanStrategy};
pub(crate) use identity::{AggregateIdentity, AggregateSemanticKey};
pub(in crate::db) use pushdown::{LogicalPushdownEligibility, derive_logical_pushdown_eligibility};
