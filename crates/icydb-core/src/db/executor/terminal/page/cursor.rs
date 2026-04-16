use crate::{
    db::{
        cursor::{MaterializedCursorRow, next_cursor_for_materialized_rows},
        direction::Direction,
        executor::{
            EntityAuthority,
            order::cursor_boundary_from_orderable_row,
            pipeline::contracts::{CursorEmissionMode, PageCursor},
            terminal::page::KernelRow,
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

// Resolve the optional scalar page cursor once from the post-access rows.
pub(super) fn build_scalar_page_cursor(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    rows: &[KernelRow],
    cursor_emission: CursorEmissionMode,
    rows_after_cursor: usize,
    continuation: &crate::db::executor::ScalarContinuationContext,
    direction: Direction,
) -> Result<Option<PageCursor>, InternalError> {
    if !cursor_emission.enabled() {
        return Ok(None);
    }

    let post_access_rows = rows.len();
    let last_cursor_row = resolve_last_cursor_row(authority, plan, rows)?;

    Ok(next_cursor_for_materialized_rows(
        &plan.access,
        plan.scalar_plan().order.as_ref(),
        plan.scalar_plan().page.as_ref(),
        post_access_rows,
        last_cursor_row,
        rows_after_cursor,
        continuation.post_access_cursor_boundary(),
        continuation.previous_index_range_anchor(),
        direction,
        continuation.continuation_signature(),
    )?
    .map(PageCursor::Scalar))
}

// Resolve the last structural cursor row before typed response decode.
fn resolve_last_cursor_row(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    rows: &[KernelRow],
) -> Result<Option<MaterializedCursorRow>, InternalError> {
    let Some(resolved_order) = plan.resolved_order() else {
        return Ok(None);
    };
    let Some(row) = rows.last() else {
        return Ok(None);
    };

    // Phase 1: derive the structural boundary from already-materialized row slots.
    let boundary = cursor_boundary_from_orderable_row(row, resolved_order);

    // Phase 2: derive the optional raw index-range anchor once for index-range paths.
    let index_anchor = if let Some((index, _, _, _)) = plan.access.as_index_range_path() {
        let data_key = &row
            .data_row
            .as_ref()
            .ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "slot-only kernel row reached cursor anchor derivation path",
                )
            })?
            .0;
        let mut read_slot = |slot| row.slot_ref(slot);
        authority
            .index_key_from_slot_ref_reader(data_key.storage_key(), index, &mut read_slot)?
            .map(|key| key.to_raw())
    } else {
        None
    };

    Ok(Some(MaterializedCursorRow::new(boundary, index_anchor)))
}
