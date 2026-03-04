use crate::db::query::plan::{
    FieldSlot, GroupSpec, OrderSpec, ScalarPlan,
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
    if logical.page.as_ref().and_then(|page| page.limit).is_none() {
        return Err(PlanError::from(GroupPlanError::OrderRequiresLimit));
    }
    if order_prefix_aligned_with_group_fields(order, group.group_fields.as_slice()) {
        return Ok(());
    }

    Err(PlanError::from(
        GroupPlanError::OrderPrefixNotAlignedWithGroupKeys,
    ))
}

// Return true when ORDER BY starts with GROUP BY key fields in declaration order.
fn order_prefix_aligned_with_group_fields(order: &OrderSpec, group_fields: &[FieldSlot]) -> bool {
    if order.fields.len() < group_fields.len() {
        return false;
    }

    group_fields
        .iter()
        .zip(order.fields.iter())
        .all(|(group_field, (order_field, _))| order_field == group_field.field())
}
