//! Module: db::executor::route::pushdown
//! Responsibility: secondary-index ORDER BY pushdown feasibility routing.
//! Does not own: logical ORDER BY validation semantics.
//! Boundary: route-owned capability assessment over validated logical+access plans.

use crate::{
    db::{
        access::{PushdownApplicability, lower_executable_access_plan},
        direction::Direction,
        executor::route::direction_from_order,
        query::plan::{AccessPlannedQuery, LogicalPushdownEligibility, OrderDirection, ScalarPlan},
    },
    model::entity::EntityModel,
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
    if !logical_pushdown_eligibility.secondary_order_allowed()
        || logical_pushdown_eligibility.requires_full_materialization()
    {
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

    let executable_plan = lower_executable_access_plan(&plan.access);
    let access_class: crate::db::access::AccessRouteClass = executable_plan.class();

    access_class.secondary_order_pushdown_applicability(model, &order_fields)
}
