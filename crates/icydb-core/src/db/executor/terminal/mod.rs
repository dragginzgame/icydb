//! Module: executor::terminal
//! Responsibility: terminal adapters (`take`, top-k/bottom-k row/value projections) for read execution responses.
//! Does not own: core pipeline execution routing or predicate/index planning semantics.
//! Boundary: terminal-level post-processing over canonical materialized read responses.

pub(in crate::db::executor) mod page;
mod row_decode;
#[cfg(test)]
mod tests;

pub(in crate::db) use page::KernelRow;
pub(in crate::db) use page::PageWorkEnvelope;
pub(in crate::db) use page::RetainedSlotRow;
pub(in crate::db::executor) use page::{
    ProductionScalarOutputWork, RetainedSlotLayout, RetainedSlotValueMode,
    begin_production_scalar_page_unit, finish_production_scalar_page_unit,
    production_scalar_page_access_entry_limit, production_scalar_page_work_is_active,
    with_production_scalar_page_work,
};
pub(in crate::db::executor) use row_decode::RowDecoder;
pub(in crate::db) use row_decode::RowLayout;

// Centralize payload-byte saturation so terminal behavior stays explicit and
// testable without requiring oversized persisted rows.

#[cfg(test)]
pub(in crate::db::executor::terminal) const fn bytes_window_limit_exhausted(
    limit_remaining: Option<usize>,
) -> bool {
    matches!(limit_remaining, Some(0))
}

#[cfg(test)]
pub(in crate::db::executor::terminal) const fn bytes_window_accept_row(
    offset_remaining: &mut usize,
    limit_remaining: &mut Option<usize>,
) -> bool {
    if *offset_remaining > 0 {
        *offset_remaining = offset_remaining.saturating_sub(1);
        return false;
    }

    if let Some(remaining) = limit_remaining.as_mut() {
        if *remaining == 0 {
            return false;
        }
        *remaining = remaining.saturating_sub(1);
    }

    true
}
