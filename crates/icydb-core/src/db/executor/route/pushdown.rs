//! Module: db::executor::route::pushdown
//! Responsibility: secondary-index ORDER BY pushdown feasibility routing.
//! Does not own: logical ORDER BY validation semantics.
//! Boundary: route-owned capability assessment over validated logical+access plans.

use crate::{
    db::{
        access::PushdownApplicability,
        direction::Direction,
        executor::route::direction_from_order,
        query::plan::{AccessPlannedQuery, LogicalPushdownEligibility, OrderDirection, ScalarPlan},
    },
    model::entity::EntityModel,
    traits::EntitySchema,
};

fn order_fields_as_direction_refs(
    order_fields: &[(String, OrderDirection)],
) -> Vec<(&str, Direction)> {
    order_fields
        .iter()
        .map(|(field, direction)| (field.as_str(), direction_from_order(*direction)))
        .collect()
}

fn validated_secondary_order_fields_for_contract<'a>(
    model: &EntityModel,
    logical: &'a ScalarPlan,
    logical_pushdown_eligibility: LogicalPushdownEligibility,
) -> Option<Vec<(&'a str, Direction)>> {
    if !secondary_order_contract_active(logical_pushdown_eligibility) {
        return None;
    }

    let order_fields = logical
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields))?;

    debug_assert!(
        !order_fields.is_empty(),
        "planner-pushed secondary-order eligibility requires at least one ORDER BY field",
    );
    let (last_field, expected_direction) = order_fields.last()?;
    debug_assert_eq!(
        *last_field, model.primary_key.name,
        "planner-pushed secondary-order eligibility requires primary-key tie-break field",
    );
    debug_assert!(
        order_fields
            .iter()
            .all(|(_, direction)| *direction == *expected_direction),
        "planner-pushed secondary-order eligibility requires one uniform ORDER BY direction",
    );

    Some(order_fields)
}

/// Derive route pushdown applicability from planner-owned logical eligibility and
/// route-owned access capabilities. Route must not re-derive logical shape policy.
pub(in crate::db) fn derive_secondary_pushdown_applicability_from_contract<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
    logical_pushdown_eligibility: LogicalPushdownEligibility,
) -> PushdownApplicability {
    let Some(order_fields) = validated_secondary_order_fields_for_contract(
        model,
        plan.scalar_plan(),
        logical_pushdown_eligibility,
    ) else {
        return PushdownApplicability::NotApplicable;
    };

    let access_class = plan.access_strategy().class();

    access_class.secondary_order_pushdown_applicability(model, &order_fields)
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
pub(in crate::db::executor) fn access_order_satisfied_by_route_contract<E, K>(
    plan: &AccessPlannedQuery<K>,
) -> bool
where
    E: EntitySchema<Key = K>,
{
    let logical = plan.scalar_plan();
    let Some(order) = logical.order.as_ref() else {
        return false;
    };
    if order.fields.is_empty() {
        return false;
    }

    let access_class = plan.access_strategy().class();
    if order.fields.len() == 1
        && order.fields[0].0 == E::MODEL.primary_key.name
        && access_class.ordered()
    {
        return true;
    }

    let logical_pushdown_eligibility = plan
        .planner_route_profile(E::MODEL)
        .logical_pushdown_eligibility();
    if !secondary_order_contract_active(logical_pushdown_eligibility) {
        return false;
    }

    let index_prefix_details = access_class.single_path_index_prefix_details();
    let index_range_details = access_class.single_path_index_range_details();
    if index_prefix_details.is_none() && index_range_details.is_none() {
        return false;
    }
    if let Some((index, _)) = index_prefix_details
        && !index.unique
    {
        return false;
    }

    derive_secondary_pushdown_applicability_from_contract(
        E::MODEL,
        plan,
        logical_pushdown_eligibility,
    )
    .is_eligible()
}
