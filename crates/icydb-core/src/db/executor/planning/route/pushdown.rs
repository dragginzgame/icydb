//! Module: db::executor::planning::route::pushdown
//! Responsibility: secondary-index ORDER BY pushdown feasibility routing.
//! Does not own: logical ORDER BY validation semantics.
//! Boundary: route-owned capability assessment over validated logical+access plans.

use crate::db::{
    access::PushdownApplicability,
    query::plan::{
        AccessPlannedQuery, DeterministicSecondaryOrderContract, LogicalPushdownEligibility,
        PlannerRouteProfile,
    },
};

fn validated_secondary_order_contract(
    planner_route_profile: &PlannerRouteProfile,
) -> Option<&DeterministicSecondaryOrderContract> {
    secondary_order_contract_active(planner_route_profile.logical_pushdown_eligibility())
        .then_some(())?;

    planner_route_profile.secondary_order_contract()
}

/// Derive route pushdown applicability from planner-owned logical eligibility and
/// route-owned access capabilities. Route must not re-derive logical shape policy.
pub(in crate::db) fn derive_secondary_pushdown_applicability_from_contract(
    plan: &AccessPlannedQuery,
    planner_route_profile: &PlannerRouteProfile,
) -> PushdownApplicability {
    let Some(order_contract) = validated_secondary_order_contract(planner_route_profile) else {
        return PushdownApplicability::NotApplicable;
    };

    let access_class = plan.access_strategy().class();

    access_class.secondary_order_pushdown_applicability(order_contract)
}

/// Return whether planner logical pushdown eligibility allows route-level
/// secondary-order contracts to remain active.
pub(in crate::db::executor) const fn secondary_order_contract_active(
    logical_pushdown_eligibility: LogicalPushdownEligibility,
) -> bool {
    logical_pushdown_eligibility.secondary_order_allowed()
        && !logical_pushdown_eligibility.requires_full_materialization()
}

/// Return whether access traversal already satisfies the logical `ORDER BY`
/// contract under planner-owned pushdown eligibility decisions.
pub(in crate::db::executor) fn access_order_satisfied_by_route_contract(
    plan: &AccessPlannedQuery,
) -> bool {
    let logical = plan.scalar_plan();
    let Some(order) = logical.order.as_ref() else {
        return false;
    };
    let access_class = plan.access_strategy().class();
    let planner_route_profile = plan.planner_route_profile();
    let has_order_fields = !order.fields.is_empty();
    // `ORDER BY primary_key` is satisfied by access shapes whose final stream
    // order is already primary-key ordered. Secondary index paths stay ordered,
    // but that order is owned by the index key, so they must not claim PK-order
    // satisfaction merely because they are monotonic.
    let primary_key_order_satisfied = order.is_primary_key_only(plan.primary_key_name())
        && access_class.ordered()
        && !access_class.has_index_path();
    let secondary_pushdown_eligible = validated_secondary_order_contract(planner_route_profile)
        .is_some_and(|order_contract| {
            access_class.index_path_satisfies_secondary_order_contract(order_contract)
        });

    has_order_fields && (primary_key_order_satisfied || secondary_pushdown_eligible)
}
