use crate::{
    db::{
        cursor::{
            apply_order_spec as apply_cursor_order_spec,
            apply_order_spec_bounded as apply_cursor_order_spec_bounded,
        },
        executor::kernel::post_access::PlanRow,
        plan::OrderSpec,
    },
    traits::{EntityKind, EntityValue},
};

// Post-access order/cursor bridge.
// Keeps kernel call sites stable while canonical ordering logic lives in db/cursor.
pub(super) fn apply_order_spec<E, R>(rows: &mut [R], order: &OrderSpec)
where
    E: EntityKind + EntityValue,
    R: PlanRow<E>,
{
    apply_cursor_order_spec::<E, R, _>(rows, order, |row| row.entity());
}

// Post-access bounded ordering bridge for first-page load optimization.
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
