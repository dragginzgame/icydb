//! Module: query::plan
//! Responsibility: logical query-plan module wiring and boundary re-exports.
//! Does not own: plan-model construction or semantic helper implementation details.
//! Boundary: intent/explain/planner/validator consumers import from this root only.

mod access_plan;
mod access_planner;
mod constant_predicate;
mod continuation;
mod covering;
#[expect(dead_code)]
pub(crate) mod expr;
mod group;
mod grouped_layout;
mod limit_zero;
mod logical_builder;
mod model;
mod model_builder;
mod order_contract;
mod planner;
mod projection;
mod semantics;
#[cfg(test)]
mod tests;
pub(crate) mod validate;

pub(crate) use access_plan::AccessPlannedQuery;
pub(in crate::db::query) use access_planner::{
    AccessPlanningInputs, normalize_query_predicate, plan_query_access,
};
pub(in crate::db::query) use constant_predicate::{
    fold_constant_predicate, predicate_is_constant_false,
};
pub(in crate::db) use continuation::{
    AccessWindowLookaheadPolicy, ContinuationContract, GroupedContinuationWindow,
    ScalarAccessWindowPlan, effective_offset_for_cursor_window,
};
pub(in crate::db) use covering::{
    CoveringProjectionContext, CoveringProjectionOrder,
    constant_covering_projection_value_from_access, covering_index_adjacent_distinct_eligible,
    covering_index_projection_context, index_covering_existing_rows_terminal_eligible,
};
pub(in crate::db) use group::{
    GroupedDistinctExecutionStrategy, GroupedExecutorHandoff, PlannedProjectionLayout,
    grouped_executor_handoff,
};
pub(in crate::db) use grouped_layout::validate_grouped_projection_layout;
pub(in crate::db::query) use limit_zero::is_limit_zero_load_window;
pub(in crate::db::query) use logical_builder::{
    LogicalPlanningInputs, build_logical_plan, logical_query_from_logical_inputs,
};
pub use model::OrderDirection;
pub(crate) use model::{AggregateKind, DistinctExecutionStrategy};
pub(in crate::db) use model::{ContinuationPolicy, ExecutionShapeSignature, PlannerRouteProfile};
pub(crate) use model::{
    DeleteLimitSpec, FieldSlot, GroupAggregateSpec, GroupHavingClause, GroupHavingSpec,
    GroupHavingSymbol, GroupPlan, GroupSpec, GroupedExecutionConfig, LogicalPlan, OrderSpec,
    PageSpec, ScalarPlan,
};
pub use model::{DeleteSpec, LoadSpec, QueryMode};
pub(in crate::db) use order_contract::{ExecutionOrderContract, ExecutionOrdering};
pub(crate) use planner::{PlannerError, plan_access};
pub(crate) use projection::{lower_projection_identity, lower_projection_intent};
pub(in crate::db) use semantics::global_distinct_group_spec_for_semantic_aggregate;
pub(crate) use semantics::{
    AccessPlanProjection, GroupDistinctAdmissibility, GroupDistinctPolicyReason,
    GroupedCursorPolicyViolation, GroupedPlanStrategyHint, grouped_distinct_admissibility,
    grouped_having_compare_op_supported, project_access_plan, project_explain_access_path,
    resolve_global_distinct_field_aggregate,
};
pub(in crate::db) use semantics::{
    LogicalPushdownEligibility, derive_logical_pushdown_eligibility,
};
#[cfg(test)]
pub(crate) use semantics::{
    global_distinct_field_aggregate_admissibility, is_global_distinct_field_aggregate_candidate,
};
#[cfg(test)]
pub(crate) use validate::GroupPlanError;
pub use validate::PlanError;
pub(crate) use validate::{
    CursorOrderPlanShapeError, CursorPagingPolicyError, FluentLoadPolicyViolation,
    IntentKeyAccessKind, IntentKeyAccessPolicyViolation, PolicyPlanError, has_explicit_order,
    resolve_group_field_slot, validate_cursor_order_plan_shape,
    validate_cursor_paging_requirements, validate_fluent_non_paged_mode,
    validate_fluent_paged_mode, validate_group_query_semantics, validate_intent_key_access_policy,
    validate_intent_plan_shape, validate_order_shape, validate_query_semantics,
};
#[cfg(test)]
pub(crate) use validate::{PlanPolicyError, PlanUserError};

pub(in crate::db) fn grouped_cursor_policy_violation_for_continuation(
    grouped: &GroupPlan,
    cursor_present: bool,
) -> Option<GroupedCursorPolicyViolation> {
    semantics::grouped_cursor_policy_violation(grouped, cursor_present)
}

// Project grouped strategy hint for consumers that need grouped execution guidance.
pub(in crate::db) fn grouped_plan_strategy_hint_for_plan<K>(
    plan: &AccessPlannedQuery<K>,
) -> Option<GroupedPlanStrategyHint> {
    semantics::grouped_plan_strategy_hint(plan)
}

#[cfg(test)]
pub(crate) fn grouped_cursor_policy_violation_for_test(
    grouped: &GroupPlan,
    cursor_present: bool,
) -> Option<GroupedCursorPolicyViolation> {
    semantics::grouped_cursor_policy_violation(grouped, cursor_present)
}
