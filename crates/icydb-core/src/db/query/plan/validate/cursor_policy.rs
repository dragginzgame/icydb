//! Module: query::plan::validate::cursor_policy
//! Responsibility: planner cursor policy feasibility checks for load/order plan shapes.
//! Does not own: cursor token decode/encode semantics or runtime cursor advancement behavior.
//! Boundary: validates cursor paging/order prerequisites before plan admission.

use crate::db::query::plan::{OrderSpec, validate::CursorOrderPlanShapeError};

/// Validate cursor-order shape and return the logical order contract when present.
pub(in crate::db) fn validate_cursor_order_plan_shape(
    order: Option<&OrderSpec>,
    require_explicit_order: bool,
) -> Result<Option<&OrderSpec>, CursorOrderPlanShapeError> {
    match (order, require_explicit_order) {
        (None, true) => Err(CursorOrderPlanShapeError::missing_explicit_order()),
        (None, false) => Ok(None),
        (Some(order), _) => (!order.fields.is_empty())
            .then_some(order)
            .ok_or(CursorOrderPlanShapeError::empty_order_spec())
            .map(Some),
    }
}
