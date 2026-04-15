//! Module: query::plan::validate::grouped::cursor
//! Responsibility: grouped cursor-order feasibility checks for planner validation.
//! Does not own: runtime grouped cursor continuation behavior or token decoding.
//! Boundary: validates grouped order/paging alignment before plan admission.

use crate::db::query::plan::{
    FieldSlot, GroupSpec, OrderSpec, ScalarPlan,
    expr::order_term_preserves_group_field_order,
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
    order_prefix_aligned_with_group_fields(order, group.group_fields.as_slice())
        .then_some(())
        .ok_or_else(|| PlanError::from(GroupPlanError::order_prefix_not_aligned_with_group_keys()))
}

// Return true when ORDER BY starts with GROUP BY key fields in declaration order.
fn order_prefix_aligned_with_group_fields(order: &OrderSpec, group_fields: &[FieldSlot]) -> bool {
    (order.fields.len() >= group_fields.len())
        && group_fields
            .iter()
            .zip(order.fields.iter())
            .all(|(group_field, (order_field, _))| {
                order_term_preserves_group_field_order(order_field, group_field.field())
            })
}
