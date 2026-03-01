//! Module: executor::kernel::post_access::order_cursor
//! Responsibility: kernel bridge to cursor-owned ordering helpers.
//! Does not own: sort semantics or cursor boundary validation logic.
//! Boundary: thin adapter layer used by kernel post-access pipeline.

use crate::{
    db::{
        cursor::{
            apply_order_spec as apply_cursor_order_spec,
            apply_order_spec_bounded as apply_cursor_order_spec_bounded,
        },
        executor::kernel::post_access::PlanRow,
        query::plan::OrderSpec,
    },
    traits::{EntityKind, EntityValue},
};

/// Apply canonical cursor-owned ordering to post-access rows.
pub(super) fn apply_order_spec<E, R>(rows: &mut [R], order: &OrderSpec)
where
    E: EntityKind + EntityValue,
    R: PlanRow<E>,
{
    apply_cursor_order_spec::<E, R, _>(rows, order, |row| row.entity());
}

/// Apply bounded canonical ordering for first-page optimization paths.
pub(super) fn apply_order_spec_bounded<E, R>(
    rows: &mut Vec<R>,
    order: &OrderSpec,
    keep_count: usize,
) where
    E: EntityKind + EntityValue,
    R: PlanRow<E>,
{
    apply_cursor_order_spec_bounded::<E, R, _>(rows, order, keep_count, |row| row.entity());
}
