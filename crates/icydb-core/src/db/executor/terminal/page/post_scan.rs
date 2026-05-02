use crate::{
    db::{
        data::DataRow,
        executor::{
            pipeline::contracts::{MaterializedExecutionPayload, PageCursor, StructuralCursorPage},
            projection::{PreparedSlotProjectionValidation, validate_prepared_projection_row},
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

use crate::db::executor::terminal::page::{KernelRow, RetainedSlotRow};

///
/// StructuralPostScanPageWindowStrategy
///
/// StructuralPostScanPageWindowStrategy freezes whether the structural
/// post-scan tail still owns one page-window pass before final payload
/// shaping.
///

#[derive(Clone, Copy)]
pub(super) enum StructuralPostScanPageWindowStrategy {
    NotPresent,
    CursorlessRetainedWindow,
}

///
/// StructuralPostScanTailStrategy
///
/// StructuralPostScanTailStrategy owns the remaining shared structural
/// post-scan tail for scalar materialization.
/// It applies the resolved page-window policy, shared projection validation,
/// and final payload shaping so the main page path and cursorless short path
/// consume the same tail boundary.
///

#[derive(Clone, Copy)]
pub(super) struct StructuralPostScanTailStrategy<'a> {
    page_window_strategy: StructuralPostScanPageWindowStrategy,
    projection_validation: Option<&'a PreparedSlotProjectionValidation>,
    final_payload_strategy: FinalPayloadStrategy,
}

impl<'a> StructuralPostScanTailStrategy<'a> {
    // Build one shared structural post-scan tail from already-resolved page
    // window, projection validation, and final payload policy.
    pub(super) const fn new(
        page_window_strategy: StructuralPostScanPageWindowStrategy,
        projection_validation: Option<&'a PreparedSlotProjectionValidation>,
        final_payload_strategy: FinalPayloadStrategy,
    ) -> Self {
        Self {
            page_window_strategy,
            projection_validation,
            final_payload_strategy,
        }
    }

    // Apply the resolved structural post-scan tail before cursor derivation
    // and outward payload shaping.
    pub(super) fn apply(
        &self,
        plan: &AccessPlannedQuery,
        rows: &mut Vec<KernelRow>,
    ) -> Result<(), InternalError> {
        self.apply_with_pre_applied_page_window(plan, rows, false)
    }

    // Apply the resolved structural post-scan tail when an upstream scan may
    // have already applied the cursorless page offset during collection.
    pub(super) fn apply_with_pre_applied_page_window(
        &self,
        plan: &AccessPlannedQuery,
        rows: &mut Vec<KernelRow>,
        page_window_already_applied: bool,
    ) -> Result<(), InternalError> {
        if matches!(
            self.page_window_strategy,
            StructuralPostScanPageWindowStrategy::CursorlessRetainedWindow
        ) && !page_window_already_applied
            && self.final_payload_strategy.retains_slot_rows()
            && !cursorless_short_path_page_window_is_redundant(plan, rows.len())
        {
            apply_cursorless_short_path_page_window(plan, rows);
        }

        validate_prepared_projection_rows(self.projection_validation, rows.as_slice())
    }

    // Finalize one already-materialized structural row set onto the outward
    // payload family selected for this tail.
    pub(super) fn finalize_payload(
        &self,
        rows: Vec<KernelRow>,
        next_cursor: Option<PageCursor>,
    ) -> Result<MaterializedExecutionPayload, InternalError> {
        finalize_structural_cursor_payload(
            rows,
            self.final_payload_strategy.finalize_mode(next_cursor),
        )
    }
}

///
/// FinalPayloadStrategy
///
/// FinalPayloadStrategy freezes the outward scalar payload family selected for
/// one materialization plan.
/// The terminal tail then only adds the already-derived next cursor instead of
/// reinterpreting retain-slot policy at the last step.
///

#[derive(Clone, Copy)]
pub(super) struct FinalPayloadStrategy {
    retain_slot_rows: bool,
}

impl FinalPayloadStrategy {
    // Resolve the scalar final payload family from the outer slot-retention
    // policy once at plan construction time.
    pub(super) const fn from_retain_slot_rows(retain_slot_rows: bool) -> Self {
        Self { retain_slot_rows }
    }

    // Return whether this final payload strategy keeps retained slot rows
    // instead of final data rows.
    const fn retains_slot_rows(self) -> bool {
        self.retain_slot_rows
    }

    // Attach the already-built cursor boundary to the frozen final payload
    // family for this scalar materialization plan.
    const fn finalize_mode(
        self,
        next_cursor: Option<PageCursor>,
    ) -> StructuralCursorPayloadStrategy {
        select_structural_cursor_payload_strategy(self.retain_slot_rows, next_cursor)
    }
}

// Structural cursor payload finalization still has two families:
// outward data-row pages and outward retained-slot-row pages.
// The executor resolves that family once before the final row-shaping pass.
#[derive(Clone)]
pub(in crate::db::executor) enum StructuralCursorPayloadStrategy {
    DataRows {
        next_cursor: Option<PageCursor>,
    },
    #[cfg(feature = "sql")]
    SlotRows {
        next_cursor: Option<PageCursor>,
    },
}

impl StructuralCursorPayloadStrategy {}

// Select one final structural payload family before converting kernel rows
// into their outward cursor page boundary.
pub(in crate::db::executor) const fn select_structural_cursor_payload_strategy(
    retain_slot_rows: bool,
    next_cursor: Option<PageCursor>,
) -> StructuralCursorPayloadStrategy {
    #[cfg(feature = "sql")]
    if retain_slot_rows {
        return StructuralCursorPayloadStrategy::SlotRows { next_cursor };
    }

    #[cfg(not(feature = "sql"))]
    let _ = retain_slot_rows;

    StructuralCursorPayloadStrategy::DataRows { next_cursor }
}

// Return whether the cursorless retained-slot path already staged its final
// LIMIT/OFFSET window.
fn cursorless_short_path_page_window_is_redundant(
    plan: &AccessPlannedQuery,
    row_count: usize,
) -> bool {
    let Some(page) = plan.scalar_plan().page.as_ref() else {
        return true;
    };

    if page.offset != 0 {
        return false;
    }

    page.limit
        .is_none_or(|limit| row_count <= usize::try_from(limit).unwrap_or(usize::MAX))
}

// Apply the cursorless LIMIT/OFFSET window directly on the collected row set
// when the route already guarantees final order and the outer surface does not
// retain scalar continuation state.
fn apply_cursorless_short_path_page_window<T>(plan: &AccessPlannedQuery, rows: &mut Vec<T>) {
    let Some(page) = plan.scalar_plan().page.as_ref() else {
        return;
    };

    let total = rows.len();
    let start = usize::try_from(page.offset)
        .unwrap_or(usize::MAX)
        .min(total);
    let end = match page.limit {
        Some(limit) => start
            .saturating_add(usize::try_from(limit).unwrap_or(usize::MAX))
            .min(total),
        None => total,
    };
    if start == 0 {
        rows.truncate(end);
        return;
    }

    let mut kept = 0usize;
    for read_index in start..end {
        if kept != read_index {
            rows.swap(kept, read_index);
        }
        kept = kept.saturating_add(1);
    }

    rows.truncate(kept);
}

// Require the prepared projection-validation bundle whenever a retained-slot
// path still asks the shared executor validator to run.
pub(super) fn required_prepared_projection_validation(
    prepared_projection_validation: Option<&PreparedSlotProjectionValidation>,
) -> Result<&PreparedSlotProjectionValidation, InternalError> {
    prepared_projection_validation.ok_or_else(|| {
        InternalError::query_executor_invariant(
            "retained-slot projection validation requires prepared projection state",
        )
    })
}

// Finalize one already-materialized kernel row set onto the outward
// structural cursor page boundary without re-branching inside the row loop.
pub(in crate::db::executor) fn finalize_structural_cursor_payload(
    rows: Vec<KernelRow>,
    finalize_mode: StructuralCursorPayloadStrategy,
) -> Result<MaterializedExecutionPayload, InternalError> {
    match finalize_mode {
        StructuralCursorPayloadStrategy::DataRows { next_cursor } => Ok(StructuralCursorPage::new(
            collect_structural_data_rows(rows)?,
            next_cursor,
        )),
        #[cfg(feature = "sql")]
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
    prepared_projection_validation: Option<&PreparedSlotProjectionValidation>,
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
