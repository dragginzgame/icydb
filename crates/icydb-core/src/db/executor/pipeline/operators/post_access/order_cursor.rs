//! Module: executor::pipeline::operators::post_access::order_cursor
//! Responsibility: post-access bridge to shared structural ordering helpers.
//! Does not own: order semantics or cursor boundary validation logic.
//! Boundary: resolves model-owned order slots for post-access ordering operators.

use crate::{
    db::{
        executor::{
            OrderReadableRow, apply_structural_order, apply_structural_order_bounded,
            resolve_structural_order,
        },
        query::plan::OrderSpec,
    },
    model::entity::EntityModel,
};

/// Apply canonical structural ordering to post-access rows.
pub(super) fn apply_order_spec<R>(rows: &mut [R], model: &EntityModel, order: &OrderSpec)
where
    R: OrderReadableRow,
{
    let resolved_order = resolve_structural_order(model, order);
    apply_structural_order(rows, &resolved_order);
}

/// Apply bounded canonical structural ordering for first-page optimization paths.
pub(super) fn apply_order_spec_bounded<R>(
    rows: &mut Vec<R>,
    model: &EntityModel,
    order: &OrderSpec,
    keep_count: usize,
) where
    R: OrderReadableRow,
{
    let resolved_order = resolve_structural_order(model, order);
    apply_structural_order_bounded(rows, &resolved_order, keep_count);
}
