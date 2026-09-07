use crate::{
    db::{
        data::DataRow,
        executor::{
            pipeline::contracts::StructuralCursorPage,
            terminal::page::{KernelRow, RetainedSlotRow},
        },
    },
    error::InternalError,
};

// Structural cursor payload finalization still has two families:
// outward data-row pages and outward retained-slot-row pages.
// The executor resolves that family once before the final row-shaping pass.
#[derive(Clone, Copy)]
pub(in crate::db::executor) enum StructuralCursorPayloadStrategy {
    DataRows,
    SlotRows,
}

// Select one final structural payload family before converting kernel rows
// into their outward cursor page boundary.
pub(in crate::db::executor) const fn select_structural_cursor_payload_strategy(
    retain_slot_rows: bool,
) -> StructuralCursorPayloadStrategy {
    if retain_slot_rows {
        return StructuralCursorPayloadStrategy::SlotRows;
    }

    StructuralCursorPayloadStrategy::DataRows
}

// Finalize one already-materialized kernel row set onto the outward
// structural cursor page boundary without re-branching inside the row loop.
pub(in crate::db::executor) fn finalize_structural_cursor_payload(
    rows: Vec<KernelRow>,
    finalize_mode: StructuralCursorPayloadStrategy,
) -> Result<StructuralCursorPage, InternalError> {
    match finalize_mode {
        StructuralCursorPayloadStrategy::DataRows => Ok(StructuralCursorPage::new(
            collect_structural_data_rows(rows)?,
        )),
        StructuralCursorPayloadStrategy::SlotRows => Ok(StructuralCursorPage::new_with_slot_rows(
            collect_structural_slot_rows(rows)?,
        )),
    }
}

// Convert kernel rows into retained slot rows in one straight-line pass.
pub(in crate::db::executor) fn collect_structural_slot_rows(
    rows: Vec<KernelRow>,
) -> Result<Vec<RetainedSlotRow>, InternalError> {
    rows.into_iter()
        .map(KernelRow::into_retained_slot_row)
        .collect()
}

// Convert kernel rows into data rows in one straight-line pass.
fn collect_structural_data_rows(rows: Vec<KernelRow>) -> Result<Vec<DataRow>, InternalError> {
    rows.into_iter().map(KernelRow::into_data_row).collect()
}
