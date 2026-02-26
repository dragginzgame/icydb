#[cfg(test)]
use crate::db::access::{
    assess_secondary_order_pushdown_if_applicable_from_parts,
    assess_secondary_order_pushdown_if_applicable_validated_from_parts,
};
use crate::{
    db::{
        access::assess_secondary_order_pushdown_from_parts,
        direction::Direction,
        query::plan::{AccessPlannedQuery, OrderDirection},
    },
    model::entity::EntityModel,
};

#[cfg(test)]
pub(crate) use crate::db::access::PushdownApplicability;
pub(crate) use crate::db::access::{
    PushdownSurfaceEligibility, SecondaryOrderPushdownEligibility, SecondaryOrderPushdownRejection,
};

fn direction_from_order(direction: OrderDirection) -> Direction {
    if direction == OrderDirection::Desc {
        Direction::Desc
    } else {
        Direction::Asc
    }
}

fn order_fields_as_direction_refs(
    order_fields: &[(String, OrderDirection)],
) -> Vec<(&str, Direction)> {
    order_fields
        .iter()
        .map(|(field, direction)| (field.as_str(), direction_from_order(*direction)))
        .collect()
}

/// Evaluate the secondary-index ORDER BY pushdown matrix for one plan.
pub(crate) fn assess_secondary_order_pushdown<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> SecondaryOrderPushdownEligibility {
    let order_fields = plan
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields));

    assess_secondary_order_pushdown_from_parts(model, order_fields.as_deref(), &plan.access)
}

#[cfg(test)]
/// Evaluate pushdown eligibility only when secondary-index ORDER BY is applicable.
///
/// Returns `PushdownApplicability::NotApplicable` for non-applicable shapes:
/// - no ORDER BY fields
/// - access path is not a secondary index path
pub(crate) fn assess_secondary_order_pushdown_if_applicable<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    let order_fields = plan
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields));

    assess_secondary_order_pushdown_if_applicable_from_parts(
        model,
        order_fields.as_deref(),
        &plan.access,
    )
}

/// Evaluate pushdown applicability for plans that have already passed full
/// logical/executor validation.
///
/// This variant keeps applicability explicit and assumes validated invariants
/// with debug assertions, while preserving safe fallbacks in release builds.
#[cfg(test)]
pub(crate) fn assess_secondary_order_pushdown_if_applicable_validated<K>(
    model: &EntityModel,
    plan: &AccessPlannedQuery<K>,
) -> PushdownApplicability {
    let order_fields = plan
        .order
        .as_ref()
        .map(|order| order_fields_as_direction_refs(&order.fields));

    assess_secondary_order_pushdown_if_applicable_validated_from_parts(
        model,
        order_fields.as_deref(),
        &plan.access,
    )
}
