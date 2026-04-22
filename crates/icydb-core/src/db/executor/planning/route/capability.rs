//! Module: db::executor::planning::route::capability
//! Responsibility: derive route capability snapshots from executable plans.
//! Does not own: fast-path execution dispatch or post-access kernel behavior.
//! Boundary: capability and eligibility helpers for route planning.

use crate::db::{
    direction::Direction,
    executor::{
        aggregate::{AggregateExecutionPolicyInputs, derive_aggregate_execution_policy},
        route::{
            AggregateRouteShape, LoadOrderRouteContract, LoadOrderRouteReason,
            access_order_satisfied_by_route_contract, bounded_probe_hint_is_safe,
            pk_order_stream_fast_path_shape_supported, secondary_order_contract_active,
        },
    },
    query::plan::{AccessPlannedQuery, OrderDirection, PlannerRouteProfile},
};

use crate::db::executor::planning::route::{ExecutionRoutePlan, RouteCapabilities};

/// Derive budget-safety flags for one plan at the route capability boundary.
pub(in crate::db::executor) fn derive_budget_safety_flags_for_model(
    plan: &AccessPlannedQuery,
) -> (bool, bool, bool) {
    let logical = plan.scalar_plan();
    // Route-budget safety consumes the planner-frozen residual artifacts
    // directly so ordered-route eligibility no longer depends on re-deriving
    // residual state from semantic filter ownership and access satisfaction.
    let residual_filter_present =
        plan.has_residual_filter_expr() || plan.has_residual_filter_predicate();
    let access_order_satisfied_by_path = access_order_satisfied_by_route_contract(plan);
    let has_order = logical
        .order
        .as_ref()
        .is_some_and(|order| !order.fields.is_empty());
    let requires_post_access_sort = has_order && !access_order_satisfied_by_path;

    (
        residual_filter_present,
        access_order_satisfied_by_path,
        requires_post_access_sort,
    )
}

// Derive the canonical load-order route decision once so route capability,
// verbose explain, and route tests can all consume the same contract+reason
// pair without re-classifying fallback shapes downstream.
fn derive_load_order_route_decision_for_model(
    plan: &AccessPlannedQuery,
) -> (LoadOrderRouteContract, LoadOrderRouteReason) {
    if !plan.scalar_plan().mode.is_load() {
        return (
            LoadOrderRouteContract::MaterializedFallback,
            LoadOrderRouteReason::None,
        );
    }

    let (residual_filter_present, _, requires_post_access_sort) =
        derive_budget_safety_flags_for_model(plan);
    if residual_filter_present {
        return (
            LoadOrderRouteContract::MaterializedFallback,
            LoadOrderRouteReason::ResidualFilterBlocksDirectStreaming,
        );
    }
    if requires_post_access_sort {
        return (
            LoadOrderRouteContract::MaterializedFallback,
            LoadOrderRouteReason::RequiresMaterializedSort,
        );
    }

    if let Some(reason) = secondary_prefix_streaming_requires_materialized_boundary(plan) {
        return (LoadOrderRouteContract::MaterializedBoundary, reason);
    }

    (
        LoadOrderRouteContract::DirectStreaming,
        LoadOrderRouteReason::None,
    )
}

// Some secondary-prefix ORDER BY shapes are semantically pushdown-compatible
// but still rely on the canonical materialized page boundary for correctness.
// Keep that runtime limitation local to route capability derivation so ordered
// access contracts stay visible while unsafe streaming windows fail closed.
fn secondary_prefix_streaming_requires_materialized_boundary(
    plan: &AccessPlannedQuery,
) -> Option<LoadOrderRouteReason> {
    let logical = plan.scalar_plan();
    let access_class = plan.access_strategy().class();
    let (index, _prefix_len) = access_class.single_path_index_prefix_details()?;

    // DISTINCT over secondary-prefix routes still depends on materialized
    // deduplication rather than direct ordered streaming.
    if logical.distinct {
        return Some(LoadOrderRouteReason::DistinctRequiresMaterialization);
    }

    // Reverse streaming over non-unique secondary-prefix routes is still not
    // page-stable when duplicate secondary values are present, so keep those
    // shapes on the canonical materialized lane for now.
    (!index.is_unique()
        && logical.order.as_ref().is_some_and(|order| {
            order
                .fields
                .iter()
                .any(|term| term.direction() == OrderDirection::Desc)
        }))
    .then_some(LoadOrderRouteReason::DescendingNonUniqueSecondaryPrefixNotAdmitted)
}

/// Return true when bounded physical fetch hints are valid for this direction.
pub(in crate::db::executor::planning::route) const fn direction_allows_physical_fetch_hint(
    direction: Direction,
    desc_physical_reverse_supported: bool,
) -> bool {
    !matches!(direction, Direction::Desc) || desc_physical_reverse_supported
}

impl ExecutionRoutePlan {
    // Return the effective physical fetch hint for fallback stream resolution.
    // DESC fallback must disable bounded hints when reverse traversal is unavailable.
    pub(in crate::db::executor) const fn fallback_physical_fetch_hint(
        &self,
        direction: Direction,
    ) -> Option<usize> {
        if direction_allows_physical_fetch_hint(direction, self.desc_physical_reverse_supported()) {
            self.scan_hints.physical_fetch_hint
        } else {
            None
        }
    }
}

pub(in crate::db::executor::planning::route) fn derive_execution_capabilities_for_model(
    plan: &AccessPlannedQuery,
    direction: Direction,
    aggregate_shape: Option<AggregateRouteShape<'_>>,
) -> RouteCapabilities {
    let (residual_filter_present, _, requires_post_access_sort) =
        derive_budget_safety_flags_for_model(plan);
    let (load_order_route_contract, load_order_route_reason) =
        derive_load_order_route_decision_for_model(plan);
    let aggregate_execution_policy = derive_aggregate_execution_policy(
        plan,
        direction,
        aggregate_shape,
        AggregateExecutionPolicyInputs::new(residual_filter_present, requires_post_access_sort),
    );
    let field_min_eligibility = aggregate_execution_policy.field_min_fast_path();
    let field_max_eligibility = aggregate_execution_policy.field_max_fast_path();

    RouteCapabilities {
        load_order_route_contract,
        load_order_route_reason,
        pk_order_fast_path_eligible: pk_order_stream_fast_path_shape_supported(plan),
        count_pushdown_shape_supported: aggregate_execution_policy.count_pushdown_shape_supported(),
        composite_aggregate_fast_path_eligible: aggregate_execution_policy
            .composite_aggregate_fast_path_eligible(),
        bounded_probe_hint_safe: bounded_probe_hint_is_safe(plan),
        field_min_fast_path_eligible: field_min_eligibility.eligible,
        field_max_fast_path_eligible: field_max_eligibility.eligible,
        field_min_fast_path_ineligibility_reason: field_min_eligibility.ineligibility_reason,
        field_max_fast_path_ineligibility_reason: field_max_eligibility.ineligibility_reason,
    }
}

pub(in crate::db::executor::planning::route) fn desc_physical_reverse_traversal_supported(
    plan: &AccessPlannedQuery,
    direction: Direction,
) -> bool {
    matches!(direction, Direction::Desc) && plan.supports_reverse_traversal()
}

pub(in crate::db::executor::planning::route) const fn count_pushdown_existing_rows_shape_supported(
    access_class: &crate::db::access::AccessRouteClass,
) -> bool {
    access_class.single_path() && (access_class.prefix_scan() || access_class.range_scan())
}

pub(in crate::db::executor::planning::route) fn index_range_limit_pushdown_shape_supported_for_model(
    plan: &AccessPlannedQuery,
    planner_route_profile: &PlannerRouteProfile,
) -> bool {
    let access_class = plan.access_strategy().class();
    let planner_bypass_empty_order = plan
        .scalar_plan()
        .order
        .as_ref()
        .is_some_and(|order| order.fields.is_empty());
    let order_present = plan
        .scalar_plan()
        .order
        .as_ref()
        .is_some_and(|order| !order.fields.is_empty());
    let order_contract =
        secondary_order_contract_active(planner_route_profile.logical_pushdown_eligibility())
            .then(|| planner_route_profile.secondary_order_contract())
            .flatten();

    // Planner-owned order contracts never emit `Some(OrderSpec { fields: [] })`.
    // Treat that planner-bypass shape as invalid rather than silently
    // downgrading it to the same semantics as "no ORDER BY".
    if planner_bypass_empty_order {
        return false;
    }

    access_class.index_range_limit_pushdown_shape_supported_for_order_contract(
        order_contract,
        order_present,
    )
}
