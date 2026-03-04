use crate::db::query::plan::{
    LoadSpec, OrderSpec,
    validate::{CursorOrderPlanShapeError, CursorPagingPolicyError},
};

/// Validate cursor-pagination readiness for a load-spec + ordering pair.
pub(crate) const fn validate_cursor_paging_requirements(
    has_order: bool,
    spec: LoadSpec,
) -> Result<(), CursorPagingPolicyError> {
    if !has_order {
        return Err(CursorPagingPolicyError::CursorRequiresOrder);
    }
    if spec.limit.is_none() {
        return Err(CursorPagingPolicyError::CursorRequiresLimit);
    }

    Ok(())
}

/// Validate cursor-order shape and return the logical order contract when present.
pub(crate) const fn validate_cursor_order_plan_shape(
    order: Option<&OrderSpec>,
    require_explicit_order: bool,
) -> Result<Option<&OrderSpec>, CursorOrderPlanShapeError> {
    let Some(order) = order else {
        if require_explicit_order {
            return Err(CursorOrderPlanShapeError::MissingExplicitOrder);
        }

        return Ok(None);
    };

    if order.fields.is_empty() {
        return Err(CursorOrderPlanShapeError::EmptyOrderSpec);
    }

    Ok(Some(order))
}
