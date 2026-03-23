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

use crate::{
    db::{executor::saturating_row_len, query::plan::PageSpec},
    error::InternalError,
    serialize::serialized_len,
    value::Value,
};

pub(in crate::db) use page::KernelRow;
pub(in crate::db::executor) use row_decode::{RowDecoder, RowLayout};

// Centralize payload-byte saturation so terminal behavior stays explicit and
// testable without requiring oversized persisted rows.
pub(in crate::db::executor::terminal) const fn saturating_add_payload_len(
    total: u64,
    row_len: usize,
) -> u64 {
    total.saturating_add(saturating_row_len(row_len))
}

pub(in crate::db::executor::terminal) fn bytes_page_window_state(
    page: Option<&PageSpec>,
) -> (usize, Option<usize>) {
    let Some(page) = page else {
        return (0, None);
    };
    let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
    let limit = page
        .limit
        .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

    (offset, limit)
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

// Serialize one value using the canonical runtime codec and return payload len.
pub(in crate::db::executor::terminal) fn serialized_value_len(
    value: &Value,
) -> Result<usize, InternalError> {
    serialized_len(value).map_err(|err| {
        InternalError::serialize_internal(format!("bytes(field) value encode failed: {err}"))
    })
}
