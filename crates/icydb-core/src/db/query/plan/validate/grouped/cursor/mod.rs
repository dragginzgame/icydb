//! Module: query::plan::validate::grouped::cursor
//! Responsibility: grouped cursor-order feasibility checks for planner validation.
//! Does not own: runtime grouped cursor continuation behavior or token decoding.
//! Boundary: validates grouped order/paging alignment before plan admission.

use crate::db::query::plan::{
    GroupFieldSet, GroupSpec, OrderSpec, ScalarPlan,
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
pub(in crate::db::query) fn validate_group_cursor_constraints(
    logical: &ScalarPlan,
    group: &GroupSpec,
) -> Result<(), PlanError> {
    // Grouped pagination/order constraints are cursor-domain policy. A finite
    // canonical group-key order may return its complete bounded group set
    // without a row LIMIT. Aggregate-driven ordering still needs a finite
    // Top-K window because its retained candidate bound comes from LIMIT.
    let Some(order) = logical.order.as_ref() else {
        return Ok(());
    };

    let lane = validate_order_lane(order, &group.group_fields)?;
    let has_limit = logical.page.as_ref().and_then(|page| page.limit).is_some();
    if matches!(lane, GroupedOrderCursorLane::TopK) && !has_limit {
        return Err(PlanError::from(GroupPlanError::order_requires_limit()));
    }
    if matches!(lane, GroupedOrderCursorLane::Canonical)
        && !has_limit
        && !group.execution.is_finite_bounded()
    {
        return Err(PlanError::from(GroupPlanError::order_requires_limit()));
    }

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
    group_fields: &GroupFieldSet,
) -> Result<GroupedOrderCursorLane, PlanError> {
    let top_k_required = order
        .fields
        .iter()
        .any(|term| grouped_top_k_order_term_requires_heap(term.expr()));

    if top_k_required {
        return validate_top_k_order_lane(order, group_fields);
    }

    validate_canonical_order_lane(order, group_fields)
}

// Validate one aggregate-free grouped ORDER BY list against the canonical
// grouped-key cursor contract that still powers resumable grouped ordering.
fn validate_canonical_order_lane(
    order: &OrderSpec,
    group_fields: &GroupFieldSet,
) -> Result<GroupedOrderCursorLane, PlanError> {
    if order.fields.len() < group_fields.len() {
        return Err(PlanError::from(
            GroupPlanError::order_prefix_not_aligned_with_group_keys(),
        ));
    }

    for (index, term) in order.fields.iter().enumerate() {
        let order_field = term.rendered_label();

        if index < group_fields.len() {
            let Some(group_field) = group_fields.get(index) else {
                return Err(PlanError::from(
                    GroupPlanError::order_prefix_not_aligned_with_group_keys(),
                ));
            };
            match classify_grouped_order_term_for_field(term.expr(), group_field) {
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
    group_fields: &GroupFieldSet,
) -> Result<GroupedOrderCursorLane, PlanError> {
    for term in &order.fields {
        let order_field = term.rendered_label();

        match classify_grouped_top_k_order_term(term.expr(), group_fields) {
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
