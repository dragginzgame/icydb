//! Module: query::plan::validate::grouped::cursor
//! Responsibility: grouped cursor-order feasibility checks for planner validation.
//! Does not own: runtime grouped cursor continuation behavior or token decoding.
//! Boundary: validates grouped order/paging alignment before plan admission.

use crate::db::query::plan::{
    FieldSlot, GroupSpec, OrderSpec, ScalarPlan,
    expr::{
        GroupedOrderTermAdmissibility, GroupedTopKOrderTermAdmissibility,
        classify_grouped_order_term_for_field, classify_grouped_top_k_order_term,
        grouped_top_k_order_term_requires_heap,
    },
    validate::{GroupPlanError, PlanError},
};

///
/// GroupedOrderCursorLane
///
/// Planner-local grouped cursor lane chosen from the declared grouped ORDER BY
/// terms. Canonical keeps the grouped-key ordered contract. TopK reserves the
/// bounded aggregate-order lane that still requires LIMIT.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupedOrderCursorLane {
    Canonical,
    TopK,
}

// Validate grouped cursor-order constraints in one dedicated gate.
pub(crate) fn validate_group_cursor_constraints(
    logical: &ScalarPlan,
    group: &GroupSpec,
) -> Result<(), PlanError> {
    // Grouped pagination/order constraints are cursor-domain policy:
    // aggregate ORDER BY requires LIMIT for bounded execution and must align
    // with the grouped-key prefix.
    let Some(order) = logical.order.as_ref() else {
        return Ok(());
    };
    let page = logical
        .page
        .as_ref()
        .ok_or_else(|| PlanError::from(GroupPlanError::order_requires_limit()))?;

    page.limit
        .map(|_| ())
        .ok_or_else(|| PlanError::from(GroupPlanError::order_requires_limit()))?;

    let _ = validate_order_lane(order, group.group_fields.as_slice())?;

    Ok(())
}

// Validate that grouped ORDER BY terms stay on one supported planner lane.
//
// Canonical grouped ordering still requires grouped-key prefix alignment.
// Aggregate-driven grouped ordering may reserve the bounded Top-K lane instead,
// but only when every term is admissible under the grouped post-aggregate
// expression model.
fn validate_order_lane(
    order: &OrderSpec,
    group_fields: &[FieldSlot],
) -> Result<GroupedOrderCursorLane, PlanError> {
    let grouped_field_names = group_fields
        .iter()
        .map(FieldSlot::field)
        .collect::<Vec<_>>();
    let top_k_required = order
        .fields
        .iter()
        .any(|term| grouped_top_k_order_term_requires_heap(term.expr()));

    if top_k_required {
        return validate_top_k_order_lane(order, grouped_field_names.as_slice());
    }

    validate_canonical_order_lane(order, group_fields)
}

// Validate one aggregate-free grouped ORDER BY list against the canonical
// grouped-key cursor contract that still powers resumable grouped ordering.
fn validate_canonical_order_lane(
    order: &OrderSpec,
    group_fields: &[FieldSlot],
) -> Result<GroupedOrderCursorLane, PlanError> {
    if order.fields.len() < group_fields.len() {
        return Err(PlanError::from(
            GroupPlanError::order_prefix_not_aligned_with_group_keys(),
        ));
    }

    for (index, term) in order.fields.iter().enumerate() {
        let order_field = term.rendered_label();

        if index < group_fields.len() {
            match classify_grouped_order_term_for_field(term.expr(), group_fields[index].field()) {
                GroupedOrderTermAdmissibility::Preserves(_) => {}
                GroupedOrderTermAdmissibility::PrefixMismatch => {
                    return Err(PlanError::from(
                        GroupPlanError::order_prefix_not_aligned_with_group_keys(),
                    ));
                }
                GroupedOrderTermAdmissibility::UnsupportedExpression => {
                    return Err(PlanError::from(
                        GroupPlanError::order_expression_not_admissible(order_field),
                    ));
                }
            }
        }
    }

    Ok(GroupedOrderCursorLane::Canonical)
}

// Validate one aggregate-driven grouped ORDER BY list against the bounded Top-K
// lane. Once any aggregate order term is present, grouped-key tie-breakers no
// longer need to preserve canonical prefix order because the lane is already
// materialized and non-resumable in this release.
fn validate_top_k_order_lane(
    order: &OrderSpec,
    grouped_field_names: &[&str],
) -> Result<GroupedOrderCursorLane, PlanError> {
    for term in &order.fields {
        let order_field = term.rendered_label();

        match classify_grouped_top_k_order_term(term.expr(), grouped_field_names) {
            GroupedTopKOrderTermAdmissibility::Admissible => {}
            GroupedTopKOrderTermAdmissibility::NonGroupFieldReference => {
                return Err(PlanError::from(
                    GroupPlanError::order_prefix_not_aligned_with_group_keys(),
                ));
            }
            GroupedTopKOrderTermAdmissibility::UnsupportedExpression => {
                return Err(PlanError::from(
                    GroupPlanError::order_expression_not_admissible(order_field),
                ));
            }
        }
    }

    Ok(GroupedOrderCursorLane::TopK)
}
