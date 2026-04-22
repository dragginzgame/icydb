//! Module: db::executor::pipeline::operators::post_access::terminal::order
//! Defines post-access ordering helpers for load and delete terminal flows.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    cursor::CursorBoundary,
    executor::{
        ExecutionKernel, OrderReadableRow,
        pipeline::operators::post_access::order_cursor::{
            apply_resolved_order as apply_post_access_resolved_order,
            apply_resolved_order_bounded as apply_post_access_resolved_order_bounded,
        },
        route::access_order_satisfied_by_route_contract,
    },
    query::plan::AccessPlannedQuery,
};
use crate::error::InternalError;

// Apply ordering with bounded first-page optimization when available.
pub(in crate::db::executor::pipeline::operators::post_access) fn apply_order_phase<R>(
    plan: &AccessPlannedQuery,
    has_predicate: bool,
    rows: &mut Vec<R>,
    cursor: Option<&CursorBoundary>,
    filtered: bool,
) -> Result<(bool, usize), InternalError>
where
    R: OrderReadableRow,
{
    let bounded_order_keep = ExecutionKernel::bounded_order_keep_count(plan, cursor);
    if let Some(resolved_order) = plan.resolved_order()
        && !resolved_order.fields().is_empty()
    {
        if has_predicate && !filtered {
            return Err(InternalError::scalar_page_ordering_after_filtering_required());
        }

        // If access traversal already satisfies requested ORDER BY
        // semantics, preserve stream order and skip in-memory sorting.
        if access_order_satisfied_by_route_contract(plan) {
            return Ok((true, rows.len()));
        }

        let resolved_order = plan.require_resolved_order()?;
        let ordered_total = rows.len();
        if rows.len() > 1 {
            if let Some(keep_count) = bounded_order_keep {
                apply_post_access_resolved_order_bounded(rows, resolved_order, keep_count);
            } else {
                apply_post_access_resolved_order(rows, resolved_order);
            }
        }

        // Keep logical post-order cardinality even when bounded ordering
        // trims the working set for load-page execution.
        return Ok((true, ordered_total));
    }

    Ok((false, rows.len()))
}
