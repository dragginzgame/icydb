//! Module: executor::terminal
//! Responsibility: terminal adapters (`take`, top-k/bottom-k row/value projections) for read execution responses.
//! Does not own: core pipeline execution routing or predicate/index planning semantics.
//! Boundary: terminal-level post-processing over canonical materialized read responses.

mod bytes;
pub(in crate::db::executor) mod page;
mod ranking;
mod row_decode;
#[cfg(test)]
mod tests;
mod typed_response;

use crate::{
    db::{data::encode_structural_value_storage_bytes, executor::saturating_row_len},
    error::InternalError,
    value::Value,
};

pub(in crate::db) use page::KernelRow;
pub(in crate::db) use page::RetainedSlotRow;
#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) use page::with_direct_data_row_phase_attribution;
pub(in crate::db::executor) use page::{RetainedSlotLayout, RetainedSlotValueMode};
#[cfg(feature = "diagnostics")]
pub use page::{ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use page::{ScalarMaterializationLaneMetrics, with_scalar_materialization_lane_metrics};
pub(in crate::db::executor) use row_decode::RowDecoder;
pub(in crate::db) use row_decode::RowLayout;
pub(in crate::db::executor) use typed_response::{
    decode_data_rows_into_cursor_page, decode_data_rows_into_entity_response,
};

// Centralize payload-byte saturation so terminal behavior stays explicit and
// testable without requiring oversized persisted rows.
pub(in crate::db::executor::terminal) const fn saturating_add_payload_len(
    total: u64,
    row_len: usize,
) -> u64 {
    total.saturating_add(saturating_row_len(row_len))
}

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

// Encode one value using the owner-local structural storage codec and return
// its payload length.
pub(in crate::db::executor::terminal) fn serialized_value_len(
    value: &Value,
) -> Result<usize, InternalError> {
    let encoded = encode_structural_value_storage_bytes(value)
        .map_err(InternalError::bytes_field_value_encode_failed)?;

    Ok(encoded.len())
}
