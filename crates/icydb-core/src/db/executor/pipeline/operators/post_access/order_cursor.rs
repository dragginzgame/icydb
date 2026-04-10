//! Module: executor::pipeline::operators::post_access::order_cursor
//! Responsibility: post-access bridge to shared structural ordering helpers.
//! Does not own: order semantics or cursor boundary validation logic.
//! Boundary: applies planner-frozen order programs for post-access ordering operators.

use crate::db::{
    executor::{OrderReadableRow, apply_structural_order_window},
    query::plan::ResolvedOrder,
};

/// Apply canonical structural ordering to post-access rows.
pub(super) fn apply_resolved_order<R>(rows: &mut Vec<R>, resolved_order: &ResolvedOrder)
where
    R: OrderReadableRow,
{
    apply_structural_order_window(rows, resolved_order, None);
}

/// Apply bounded canonical structural ordering for first-page optimization paths.
pub(super) fn apply_resolved_order_bounded<R>(
    rows: &mut Vec<R>,
    resolved_order: &ResolvedOrder,
    keep_count: usize,
) where
    R: OrderReadableRow,
{
    apply_structural_order_window(rows, resolved_order, Some(keep_count));
}
