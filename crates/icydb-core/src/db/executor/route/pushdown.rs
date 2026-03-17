//! Module: db::executor::route::pushdown
//! Responsibility: secondary-index ORDER BY pushdown feasibility routing.
//! Does not own: logical ORDER BY validation semantics.
//! Boundary: route-owned capability assessment over validated logical+access plans.

use crate::{
    db::{
        access::PushdownApplicability,
        query::plan::{
            AccessPlannedQuery, LogicalPushdownEligibility, OrderSpec, ScalarPlan,
            secondary_order_contract_is_deterministic,
        },
    },
    model::entity::EntityModel,
};

fn validated_secondary_order_for_contract<'a>(
    model: &EntityModel,
    logical: &'a ScalarPlan,
    logical_pushdown_eligibility: LogicalPushdownEligibility,
) -> Option<&'a OrderSpec> {
    (secondary_order_contract_active(logical_pushdown_eligibility)
        && secondary_order_contract_is_deterministic(model, logical))
    .then_some(())?;

    logical.order.as_ref()
}

/// Derive route pushdown applicability from planner-owned logical eligibility and
/// route-owned access capabilities. Route must not re-derive logical shape policy.
pub(in crate::db) fn derive_secondary_pushdown_applicability_from_contract<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
    logical_pushdown_eligibility: LogicalPushdownEligibility,
) -> PushdownApplicability {
    let Some(order) = validated_secondary_order_for_contract(
        model,
        plan.scalar_plan(),
        logical_pushdown_eligibility,
    ) else {
        return PushdownApplicability::NotApplicable;
    };

    let access_class = plan.access_strategy().class();

    access_class.secondary_order_pushdown_applicability(model, order)
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
pub(in crate::db::executor) fn access_order_satisfied_by_route_contract_for_model<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> bool {
    let logical = plan.scalar_plan();
    let Some(order) = logical.order.as_ref() else {
        return false;
    };
    let access_class = plan.access_strategy().class();
    let logical_pushdown_eligibility = plan
        .planner_route_profile(model)
        .logical_pushdown_eligibility();
    let index_prefix_details = access_class.single_path_index_prefix_details();
    let index_range_details = access_class.single_path_index_range_details();
    let has_order_fields = !order.fields.is_empty();
    let primary_key_order_satisfied =
        order.is_primary_key_only(model.primary_key.name) && access_class.ordered();
    let secondary_contract_active = secondary_order_contract_active(logical_pushdown_eligibility);
    let has_index_path = index_prefix_details.is_some() || index_range_details.is_some();
    let unique_prefix_ok = index_prefix_details.is_none_or(|(index, _)| index.is_unique());
    let secondary_pushdown_eligible = derive_secondary_pushdown_applicability_from_contract(
        model,
        plan,
        logical_pushdown_eligibility,
    )
    .is_eligible();

    has_order_fields
        && (primary_key_order_satisfied
            || (secondary_contract_active
                && has_index_path
                && unique_prefix_ok
                && secondary_pushdown_eligible))
}
