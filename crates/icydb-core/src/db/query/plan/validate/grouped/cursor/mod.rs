//! Module: query::plan::validate::grouped::cursor
//! Responsibility: grouped cursor-order feasibility checks for planner validation.
//! Does not own: runtime grouped cursor continuation behavior or token decoding.
//! Boundary: validates grouped order/paging alignment before plan admission.

use crate::db::query::plan::{
    FieldSlot, GroupSpec, OrderSpec, ScalarPlan,
    expr::{GroupedOrderTermAdmissibility, classify_grouped_order_term_for_field},
    validate::{GroupPlanError, PlanError},
};

// Validate grouped cursor-order constraints in one dedicated gate.
pub(in crate::db::query::plan::validate) fn validate_group_cursor_constraints(
    logical: &ScalarPlan,
    group: &GroupSpec,
) -> Result<(), PlanError> {
    // Grouped pagination/order constraints are cursor-domain policy:
    // grouped ORDER BY requires LIMIT and must align with grouped-key prefix.
    let Some(order) = logical.order.as_ref() else {
        return Ok(());
    };
    logical
        .page
        .as_ref()
        .and_then(|page| page.limit)
        .map(|_| ())
        .ok_or_else(|| PlanError::from(GroupPlanError::order_requires_limit()))?;
    validate_order_prefix_alignment(order, group.group_fields.as_slice())
}

// Validate that ORDER BY starts with GROUP BY key fields in declaration order,
// distinguishing true prefix mismatch from unsupported-but-evaluable grouped
// order expressions.
fn validate_order_prefix_alignment(
    order: &OrderSpec,
    group_fields: &[FieldSlot],
) -> Result<(), PlanError> {
    if order.fields.len() < group_fields.len() {
        return Err(PlanError::from(
            GroupPlanError::order_prefix_not_aligned_with_group_keys(),
        ));
    }

    for (group_field, (order_field, _)) in group_fields.iter().zip(order.fields.iter()) {
        match classify_grouped_order_term_for_field(order_field, group_field.field()) {
            GroupedOrderTermAdmissibility::Preserves(_) => {}
            GroupedOrderTermAdmissibility::PrefixMismatch => {
                return Err(PlanError::from(
                    GroupPlanError::order_prefix_not_aligned_with_group_keys(),
                ));
            }
            GroupedOrderTermAdmissibility::UnsupportedExpression => {
                return Err(PlanError::from(
                    GroupPlanError::order_expression_not_admissible(order_field.clone()),
                ));
            }
        }
    }

    Ok(())
}
