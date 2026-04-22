//! Module: query::plan
//! Responsibility: logical query-plan module wiring and boundary re-exports.
//! Does not own: plan-model construction or semantic helper implementation details.
//! Boundary: intent/explain/planner/validator consumers import from this root only.

mod access_choice;
mod access_plan;
mod access_planner;
mod continuation;
mod covering;
pub(crate) mod expr;
mod group;
mod grouped_layout;
mod key_item_match;
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

use crate::{db::Predicate, model::index::IndexModel};

pub(in crate::db::query) use access_choice::rerank_access_plan_by_residual_burden_with_indexes;
pub(in crate::db) use access_choice::{
    AccessChoiceCandidateExplainSummary, AccessChoiceExplainSnapshot, AccessChoiceResidualBurden,
};
pub(crate) use access_plan::AccessPlannedQuery;
pub(in crate::db) use access_plan::{
    EffectiveRuntimeFilterProgram, PlannedNonIndexAccessReason, ResolvedOrder, ResolvedOrderField,
    ResolvedOrderValueSource, StaticPlanningShape,
};
pub(in crate::db::query) use access_planner::{
    AccessPlanningInputs, normalize_query_predicate, plan_query_access,
};
pub(in crate::db) use continuation::{
    PlannedContinuationContract, ScalarAccessWindowPlan, effective_offset_for_cursor_window,
};
pub(in crate::db) use covering::{
    CoveringExistingRowMode, CoveringProjectionContext, CoveringProjectionOrder,
    CoveringReadExecutionPlan, CoveringReadField, CoveringReadFieldSource,
    constant_covering_projection_value_from_access, covering_hybrid_projection_plan_from_fields,
    covering_index_adjacent_distinct_eligible, covering_index_projection_context,
    covering_read_execution_plan_from_fields, covering_read_reason_code_for_load_plan,
    covering_strict_predicate_compatible, index_covering_existing_rows_terminal_eligible,
};
pub(in crate::db) use group::{
    GroupedAggregateExecutionSpec, GroupedDistinctExecutionStrategy, GroupedFoldPath,
    PlannedProjectionLayout, grouped_aggregate_execution_specs,
    grouped_aggregate_specs_from_projection_spec, grouped_executor_handoff,
    resolved_grouped_distinct_execution_strategy_for_model,
};
pub(in crate::db) use grouped_layout::validate_grouped_projection_layout;
pub(in crate::db::query) use logical_builder::{
    LogicalPlanningInputs, build_logical_plan, canonicalize_order_spec_for_grouping,
    logical_query_from_logical_inputs,
};
pub use model::OrderDirection;
pub(crate) use model::OrderTerm;
pub(in crate::db) use model::render_scalar_filter_expr_sql_label;
pub(crate) use model::{AggregateKind, DistinctExecutionStrategy};
pub(in crate::db) use model::{ContinuationPolicy, ExecutionShapeSignature, PlannerRouteProfile};
pub(crate) use model::{
    DeleteLimitSpec, FieldSlot, GroupAggregateSpec, GroupPlan, GroupSpec, GroupedExecutionConfig,
    LogicalPlan, OrderSpec, PageSpec, ScalarPlan,
};
pub use model::{DeleteSpec, LoadSpec, QueryMode};
pub(in crate::db) use order_contract::{
    DeterministicSecondaryOrderContract, ExecutionOrderContract, ExecutionOrdering,
};
pub(in crate::db) use order_term::index_order_terms;
#[cfg(test)]
pub(crate) use planner::plan_access;
pub(in crate::db::query) use planner::{PlannedAccessSelection, plan_access_selection_with_order};
pub(crate) use planner::{PlannerError, plan_access_with_order};
pub(in crate::db) use planner::{
    residual_query_predicate_after_access_path_bounds,
    residual_query_predicate_after_filtered_access,
};
pub(crate) use projection::{
    lower_direct_projection_slots, lower_global_aggregate_projection, lower_projection_identity,
    lower_projection_intent,
};
#[cfg(test)]
pub(crate) use semantics::GroupedPlanAggregateFamily;
pub(in crate::db) use semantics::global_distinct_group_spec_for_semantic_aggregate;
pub(crate) use semantics::{
    AccessPlanProjection, GroupDistinctAdmissibility, GroupDistinctPolicyReason,
    GroupedCursorPolicyViolation, GroupedPlanFallbackReason, GroupedPlanStrategy,
    access_plan_label, explain_access_kind_label, grouped_distinct_admissibility,
    grouped_having_binary_compare_op, grouped_having_compare_op_supported, project_access_plan,
    project_explain_access_path, resolve_global_distinct_field_aggregate,
};
pub(in crate::db) use semantics::{
    LogicalPushdownEligibility, derive_logical_pushdown_eligibility,
    grouped_cursor_policy_violation, grouped_having_compare_expr, grouped_plan_aggregate_family,
    grouped_plan_strategy,
};
pub(in crate::db) use semantics::{
    canonicalize_grouped_having_numeric_literal_for_field_kind, group_aggregate_spec_expr,
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
    validate_query_semantics,
};

/// Return true when a query mode declares an explicit load `LIMIT 0` window.
#[must_use]
pub(in crate::db::query) fn is_limit_zero_load_window(mode: QueryMode) -> bool {
    matches!(mode, QueryMode::Load(spec) if spec.limit() == Some(0))
}

/// Fold canonical constant predicates before access routing.
///
/// Contract:
/// - `Some(Predicate::True)` is elided to `None`
/// - `Some(Predicate::False)` is preserved so explain semantics remain explicit
/// - all other predicates are passed through unchanged
#[must_use]
pub(in crate::db::query) fn fold_constant_predicate(
    predicate: Option<Predicate>,
) -> Option<Predicate> {
    match predicate {
        Some(Predicate::True) => None,
        other => other,
    }
}

/// Return true when the normalized predicate is a canonical constant false.
#[must_use]
pub(in crate::db::query) const fn predicate_is_constant_false(
    predicate: Option<&Predicate>,
) -> bool {
    matches!(predicate, Some(Predicate::False))
}
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
