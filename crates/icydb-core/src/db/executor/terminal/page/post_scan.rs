use crate::{
    db::{
        data::DataRow,
        executor::{
            pipeline::contracts::{PageCursor, StructuralCursorPage},
            projection::{PreparedProjectionContract, validate_prepared_projection_row},
            terminal::page::{KernelRow, RetainedSlotRow},
        },
    },
    error::InternalError,
};

///
/// StructuralPostScanTailStrategy
///
/// StructuralPostScanTailStrategy owns the remaining shared structural
/// post-scan tail for scalar materialization.
/// It applies shared projection validation and final payload shaping so the
/// main page path and cursorless short path consume the same tail boundary.
///

#[derive(Clone, Copy)]
pub(super) struct StructuralPostScanTailStrategy<'a> {
    projection_validation: Option<&'a PreparedProjectionContract>,
    retain_slot_rows: bool,
}

impl<'a> StructuralPostScanTailStrategy<'a> {
    // Build one shared structural post-scan tail from already-resolved
    // projection validation and final payload policy.
    pub(super) const fn new(
        projection_validation: Option<&'a PreparedProjectionContract>,
        retain_slot_rows: bool,
    ) -> Self {
        Self {
            projection_validation,
            retain_slot_rows,
        }
    }

    // Apply the resolved structural post-scan tail before cursor derivation
    // and outward payload shaping.
    pub(super) fn apply(&self, rows: &[KernelRow]) -> Result<(), InternalError> {
        validate_prepared_projection_rows(self.projection_validation, rows)
    }

    // Finalize one already-materialized structural row set onto the outward
    // payload family selected for this tail.
    pub(super) fn finalize_payload(
        &self,
        rows: Vec<KernelRow>,
        next_cursor: Option<PageCursor>,
    ) -> Result<StructuralCursorPage, InternalError> {
        finalize_structural_cursor_payload(
            rows,
            select_structural_cursor_payload_strategy(self.retain_slot_rows, next_cursor),
        )
    }
}

// Structural cursor payload finalization still has two families:
// outward data-row pages and outward retained-slot-row pages.
// The executor resolves that family once before the final row-shaping pass.
#[derive(Clone)]
pub(in crate::db::executor) enum StructuralCursorPayloadStrategy {
    DataRows { next_cursor: Option<PageCursor> },
    SlotRows { next_cursor: Option<PageCursor> },
}

// Select one final structural payload family before converting kernel rows
// into their outward cursor page boundary.
pub(in crate::db::executor) const fn select_structural_cursor_payload_strategy(
    retain_slot_rows: bool,
    next_cursor: Option<PageCursor>,
) -> StructuralCursorPayloadStrategy {
    if retain_slot_rows {
        return StructuralCursorPayloadStrategy::SlotRows { next_cursor };
    }

    #[cfg(not(feature = "sql"))]
    let _ = retain_slot_rows;

    StructuralCursorPayloadStrategy::DataRows { next_cursor }
}

// Require the prepared projection-validation bundle whenever a retained-slot
// path still asks the shared executor validator to run.
pub(super) fn required_prepared_projection_validation(
    prepared_projection_validation: Option<&PreparedProjectionContract>,
) -> Result<&PreparedProjectionContract, InternalError> {
    prepared_projection_validation.ok_or_else(InternalError::query_executor_invariant)
}

// Finalize one already-materialized kernel row set onto the outward
// structural cursor page boundary without re-branching inside the row loop.
pub(in crate::db::executor) fn finalize_structural_cursor_payload(
    rows: Vec<KernelRow>,
    finalize_mode: StructuralCursorPayloadStrategy,
) -> Result<StructuralCursorPage, InternalError> {
    match finalize_mode {
        StructuralCursorPayloadStrategy::DataRows { next_cursor } => Ok(StructuralCursorPage::new(
            collect_structural_data_rows(rows)?,
            next_cursor,
        )),
        StructuralCursorPayloadStrategy::SlotRows { next_cursor } => {
            Ok(StructuralCursorPage::new_with_slot_rows(
                collect_structural_slot_rows(rows)?,
                next_cursor,
            ))
        }
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

// Run the shared slot-row projection validator from already-prepared
// projection state when this tail still owns that validation pass.
fn validate_prepared_projection_rows(
    prepared_projection_validation: Option<&PreparedProjectionContract>,
    rows: &[KernelRow],
) -> Result<(), InternalError> {
    let Some(prepared_projection_validation) = prepared_projection_validation else {
        return Ok(());
    };
    for row in rows {
        validate_prepared_projection_row(prepared_projection_validation, row)?;
    }

    Ok(())
}
