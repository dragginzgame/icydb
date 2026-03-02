//! Module: query::plan
//! Responsibility: logical query-plan module wiring and boundary re-exports.
//! Does not own: plan-model construction or semantic helper implementation details.
//! Boundary: intent/explain/planner/validator consumers import from this root only.

mod access_plan;
mod group;
mod model;
mod model_builder;
mod planner;
mod semantics;
#[cfg(test)]
mod tests;
pub(crate) mod validate;

pub(crate) use access_plan::AccessPlannedQuery;
pub(in crate::db) use group::{GroupedExecutorHandoff, grouped_executor_handoff};
pub use model::OrderDirection;
pub(crate) use model::{AggregateKind, DeleteSpec, LoadSpec, QueryMode};
pub(crate) use model::{
    DeleteLimitSpec, FieldSlot, GroupAggregateKind, GroupAggregateSpec, GroupHavingClause,
    GroupHavingSpec, GroupHavingSymbol, GroupPlan, GroupSpec, GroupedExecutionConfig, LogicalPlan,
    OrderSpec, PageSpec, ScalarPlan,
};
pub(crate) use planner::{PlannerError, plan_access};
pub(crate) use semantics::{
    AccessPlanProjection, GroupDistinctAdmissibility, GroupDistinctPolicyReason,
    GroupedPlanStrategyHint, grouped_distinct_admissibility, grouped_having_compare_op_supported,
    grouped_plan_strategy_hint, project_access_plan, project_explain_access_path,
    resolve_global_distinct_field_aggregate,
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
