//! Module: db::executor::planning::route::capability
//! Responsibility: derive route capability snapshots from executable plans.
//! Does not own: fast-path execution dispatch or post-access kernel behavior.
//! Boundary: capability and eligibility helpers for route planning.

use crate::db::{
    direction::Direction,
    executor::{
        aggregate::{AggregateExecutionPolicyInputs, derive_aggregate_execution_policy},
        route::{
            AggregateRouteShape, LoadOrderRouteDecision, LoadOrderRouteReason,
            LoadTerminalFastPathContract, access_order_satisfied_by_route_contract,
            bounded_probe_hint_is_safe, pk_order_stream_fast_path_shape_supported,
            secondary_order_contract_active,
        },
    },
    query::plan::{AccessPlannedQuery, OrderDirection, PlannerRouteProfile},
};

use crate::db::executor::planning::route::{
    ExecutionRoutePlan, RouteCapabilities,
    index_range_limit_pushdown_shape_supported_for_order_contract,
};

///
/// LoadRouteCapabilityFacts
///
/// Route-owned shared load-capability fact snapshot for one validated plan.
/// This exists so route capability derivation and load hint helpers can reuse
/// the same residual-filter, post-access-sort, and load-order decision pass
/// instead of walking the same plan facts through parallel local helpers.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor::planning::route) struct LoadRouteCapabilityFacts {
    residual_filter_present: bool,
    requires_post_access_sort: bool,
    load_order_route_decision: LoadOrderRouteDecision,
}

impl LoadRouteCapabilityFacts {
    // Derive the shared load-capability fact snapshot from one validated plan.
    fn from_plan(plan: &AccessPlannedQuery) -> Self {
        let logical = plan.scalar_plan();

        // Phase 1: collect the shared budget and order facts that downstream
        // route helpers currently need from the same logical plan.
        let residual_filter_present =
            plan.has_residual_filter_expr() || plan.has_residual_filter_predicate();
        let access_order_satisfied_by_path = access_order_satisfied_by_route_contract(plan);
        let has_order = logical
            .order
            .as_ref()
            .is_some_and(|order| !order.fields.is_empty());
        let requires_post_access_sort = has_order && !access_order_satisfied_by_path;

        // Phase 2: project those facts onto the canonical load-order route
        // decision so route capability and hint callers share one owner.
        let load_order_route_decision = if !logical.mode.is_load() {
            LoadOrderRouteDecision::materialized_fallback(LoadOrderRouteReason::None)
        } else if residual_filter_present {
            LoadOrderRouteDecision::materialized_fallback(
                LoadOrderRouteReason::ResidualFilterBlocksDirectStreaming,
            )
        } else if requires_post_access_sort {
            LoadOrderRouteDecision::materialized_fallback(
                LoadOrderRouteReason::RequiresMaterializedSort,
            )
        } else if let Some(decision) =
            secondary_prefix_streaming_requires_materialized_boundary(plan)
        {
            decision
        } else {
            LoadOrderRouteDecision::direct_streaming()
        };

        Self {
            residual_filter_present,
            requires_post_access_sort,
            load_order_route_decision,
        }
    }

    #[must_use]
    const fn residual_filter_present(self) -> bool {
        self.residual_filter_present
    }

    #[must_use]
    const fn requires_post_access_sort(self) -> bool {
        self.requires_post_access_sort
    }

    #[must_use]
    const fn load_order_route_decision(self) -> LoadOrderRouteDecision {
        self.load_order_route_decision
    }
}

// Derive the shared load-capability fact snapshot once so route capability and
// load-hint helpers do not re-derive the same plan facts independently.
fn derive_load_route_capability_facts_for_model(
    plan: &AccessPlannedQuery,
) -> LoadRouteCapabilityFacts {
    LoadRouteCapabilityFacts::from_plan(plan)
}

// Some secondary-prefix ORDER BY shapes are semantically pushdown-compatible
// but still rely on the canonical materialized page boundary for correctness.
// Keep that runtime limitation local to route capability derivation so ordered
// access contracts stay visible while unsafe streaming windows fail closed.
fn secondary_prefix_streaming_requires_materialized_boundary(
    plan: &AccessPlannedQuery,
) -> Option<LoadOrderRouteDecision> {
    let logical = plan.scalar_plan();
    let access_capabilities = plan.access_strategy().capabilities();
    let index = access_capabilities
        .single_path_index_prefix_details()?
        .index();

    // DISTINCT over secondary-prefix routes still depends on materialized
    // deduplication rather than direct ordered streaming.
    if logical.distinct {
        return Some(LoadOrderRouteDecision::materialized_boundary(
            LoadOrderRouteReason::DistinctRequiresMaterialization,
        ));
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
    .then_some(LoadOrderRouteDecision::materialized_boundary(
        LoadOrderRouteReason::DescendingNonUniqueSecondaryPrefixNotAdmitted,
    ))
}

// Resolve the narrower EXPLAIN-visible access-order satisfaction signal from
// the route-owned order contract plus any selected load fast path. EXPLAIN
// still needs to distinguish access-preserved ordering from shapes that rely
// on the shared materialized boundary even when the generic route contract
// proves the broader ordered-load capability.
pub(in crate::db::executor) fn explain_access_order_satisfied_for_model(
    plan: &AccessPlannedQuery,
    load_terminal_fast_path: Option<&LoadTerminalFastPathContract>,
) -> bool {
    if !access_order_satisfied_by_route_contract(plan) {
        return false;
    }

    let access_capabilities = plan.access_strategy().capabilities();
    let Some(order_contract) =
        plan.scalar_plan().order.as_ref().and_then(|order| {
            order.deterministic_secondary_order_contract(plan.primary_key_name())
        })
    else {
        return true;
    };

    if let Some(details) = access_capabilities.single_path_index_prefix_details()
        && !details.index().is_unique()
        && details.slot_arity() > 0
        && matches!(order_contract.direction(), OrderDirection::Desc)
    {
        return false;
    }

    if load_terminal_fast_path.is_some() {
        return true;
    }

    let Some(details) = access_capabilities.single_path_index_range_details() else {
        return true;
    };
    let index = details.index();
    let prefix_len = details.slot_arity();
    if index.is_unique() {
        return true;
    }
    if prefix_len == 0 {
        return true;
    }

    order_contract.non_primary_key_terms().len() <= 1
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
    let load_route_capability_facts = derive_load_route_capability_facts_for_model(plan);
    let aggregate_execution_policy = derive_aggregate_execution_policy(
        plan,
        direction,
        aggregate_shape,
        AggregateExecutionPolicyInputs::new(
            load_route_capability_facts.residual_filter_present(),
            load_route_capability_facts.requires_post_access_sort(),
        ),
    );
    let field_min_eligibility = aggregate_execution_policy.field_min_fast_path();
    let field_max_eligibility = aggregate_execution_policy.field_max_fast_path();

    RouteCapabilities {
        load_order_route_decision: load_route_capability_facts.load_order_route_decision(),
        pk_order_fast_path_eligible: pk_order_stream_fast_path_shape_supported(plan),
        count_pushdown_shape_supported: aggregate_execution_policy.count_pushdown_shape_supported(),
        composite_aggregate_fast_path_eligible: aggregate_execution_policy
            .composite_aggregate_fast_path_eligible(),
        residual_filter_present: load_route_capability_facts.residual_filter_present(),
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
    access_capabilities: &crate::db::access::AccessCapabilities,
) -> bool {
    access_capabilities.is_single_path()
        && (access_capabilities.prefix_scan() || access_capabilities.range_scan())
}

pub(in crate::db::executor::planning::route) fn index_range_limit_pushdown_shape_supported_for_model(
    plan: &AccessPlannedQuery,
    planner_route_profile: &PlannerRouteProfile,
) -> bool {
    let access_capabilities = plan.access_strategy().capabilities();
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

    index_range_limit_pushdown_shape_supported_for_order_contract(
        &access_capabilities,
        order_contract,
        order_present,
    )
}
