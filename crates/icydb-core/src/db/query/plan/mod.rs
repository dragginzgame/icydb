//! Module: query::plan
//! Responsibility: logical query-plan module wiring and boundary re-exports.
//! Does not own: plan-model construction or semantic helper implementation details.
//! Boundary: intent/explain/planner/validator consumers import from this root only.

mod access_choice;
mod access_plan;
mod access_planner;
mod constant_predicate;
mod continuation;
mod covering;
pub(crate) mod expr;
mod group;
mod grouped_layout;
mod key_item_match;
mod limit_zero;
mod logical_builder;
mod model;
mod model_builder;
mod order_contract;
mod order_term;
mod planner;
mod projection;
mod semantics;
#[cfg(test)]
mod tests;
pub(crate) mod validate;

use crate::model::index::IndexModel;

pub(in crate::db) use access_choice::{
    AccessChoiceExplainSnapshot, project_access_choice_explain_snapshot_with_indexes,
};
pub(crate) use access_plan::AccessPlannedQuery;
pub(in crate::db) use access_plan::{
    ResolvedOrder, ResolvedOrderField, ResolvedOrderValueSource, StaticPlanningShape,
};
pub(in crate::db::query) use access_planner::{
    AccessPlanningInputs, normalize_query_predicate, plan_query_access,
};
pub(in crate::db::query) use constant_predicate::{
    fold_constant_predicate, predicate_is_constant_false,
};
pub(in crate::db) use continuation::{
    ContinuationContract, ScalarAccessWindowPlan, effective_offset_for_cursor_window,
};
pub(in crate::db) use covering::{
    CoveringExistingRowMode, CoveringProjectionContext, CoveringProjectionOrder,
    CoveringReadExecutionPlan, CoveringReadFieldSource,
    constant_covering_projection_value_from_access, covering_index_adjacent_distinct_eligible,
    covering_index_projection_context, covering_read_execution_plan_from_fields,
    covering_read_reason_code_for_load_plan, covering_strict_predicate_compatible,
    index_covering_existing_rows_terminal_eligible,
};
#[cfg(test)]
pub(in crate::db) use group::GroupedAggregateProjectionSpec;
#[cfg(test)]
pub(in crate::db) use group::GroupedExecutorHandoff;
pub(in crate::db) use group::{
    GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy, GroupedFoldPath,
    PlannedProjectionLayout, grouped_aggregate_execution_specs_with_model,
    grouped_aggregate_projection_specs_from_projection_spec, grouped_executor_handoff,
    resolved_grouped_distinct_execution_strategy_for_model,
};
pub(in crate::db) use grouped_layout::validate_grouped_projection_layout;
pub(in crate::db::query) use limit_zero::is_limit_zero_load_window;
pub(in crate::db::query) use logical_builder::{
    LogicalPlanningInputs, build_logical_plan, canonicalize_order_spec,
    logical_query_from_logical_inputs,
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
pub(in crate::db) use order_contract::{
    DeterministicSecondaryOrderContract, ExecutionOrderContract, ExecutionOrdering,
};
pub(in crate::db) use order_term::{ExpressionOrderTerm, index_order_terms};
#[cfg(test)]
pub(crate) use planner::plan_access;
pub(crate) use planner::{PlannerError, plan_access_with_order};
pub(in crate::db) use planner::{
    residual_query_predicate_after_access_path_bounds,
    residual_query_predicate_after_filtered_access,
};
pub(crate) use projection::{
    lower_direct_projection_slots, lower_projection_identity, lower_projection_intent,
};
#[cfg(test)]
pub(crate) use semantics::GroupedPlanAggregateFamily;
pub(in crate::db) use semantics::global_distinct_group_spec_for_semantic_aggregate;
pub(crate) use semantics::{
    AccessPlanProjection, GroupDistinctAdmissibility, GroupDistinctPolicyReason,
    GroupedCursorPolicyViolation, GroupedPlanFallbackReason, GroupedPlanStrategy,
    grouped_distinct_admissibility, grouped_having_compare_op_supported, project_access_plan,
    project_explain_access_path, resolve_global_distinct_field_aggregate,
};
pub(in crate::db) use semantics::{
    LogicalPushdownEligibility, derive_logical_pushdown_eligibility,
    grouped_cursor_policy_violation, grouped_plan_aggregate_family, grouped_plan_strategy,
};
#[cfg(test)]
pub(crate) use semantics::{
    global_distinct_field_aggregate_admissibility, is_global_distinct_field_aggregate_candidate,
};
#[cfg(test)]
pub(crate) use validate::GroupPlanError;
pub use validate::PlanError;
pub(crate) use validate::{
    CursorPagingPolicyError, FluentLoadPolicyViolation, IntentKeyAccessKind,
    IntentKeyAccessPolicyViolation, PolicyPlanError, has_explicit_order,
    resolve_aggregate_target_field_slot, resolve_group_field_slot,
    validate_cursor_order_plan_shape, validate_fluent_non_paged_mode, validate_fluent_paged_mode,
    validate_group_query_semantics, validate_intent_key_access_policy, validate_intent_plan_shape,
    validate_order_shape, validate_query_semantics,
};
#[cfg(test)]
pub(crate) use validate::{PlanPolicyError, PlanUserError};

///
/// VisibleIndexes
///
/// Planner-bound index slice that has already passed runtime visibility
/// gating at the session boundary, or one schema-owned detached slice for
/// tooling/tests that intentionally do not carry runtime store context.
///

#[derive(Clone, Copy, Debug)]
pub(in crate::db) struct VisibleIndexes<'a> {
    indexes: &'a [&'static IndexModel],
}

impl<'a> VisibleIndexes<'a> {
    #[must_use]
    pub(in crate::db) const fn none() -> Self {
        Self { indexes: &[] }
    }

    #[must_use]
    pub(in crate::db) const fn planner_visible(indexes: &'a [&'static IndexModel]) -> Self {
        Self { indexes }
    }

    #[must_use]
    pub(in crate::db) const fn schema_owned(indexes: &'a [&'static IndexModel]) -> Self {
        Self { indexes }
    }

    #[must_use]
    pub(in crate::db) const fn as_slice(&self) -> &'a [&'static IndexModel] {
        self.indexes
    }
}
