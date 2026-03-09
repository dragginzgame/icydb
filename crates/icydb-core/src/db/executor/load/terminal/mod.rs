//! Module: executor::load::terminal
//! Responsibility: load terminal adapters (`take`, top-k/bottom-k row/value projections).
//! Does not own: core load execution routing or predicate/index planning semantics.
//! Boundary: terminal-level post-processing over canonical materialized load responses.

mod bytes;
mod ranking;
#[cfg(test)]
mod tests;

use crate::{
    db::{executor::saturating_row_len, query::plan::PageSpec},
    error::InternalError,
    serialize::serialized_len,
    value::Value,
};

// Centralize payload-byte saturation so terminal behavior stays explicit and
// testable without requiring oversized persisted rows.
pub(in crate::db::executor::load::terminal) const fn saturating_add_payload_len(
    total: u64,
    row_len: usize,
) -> u64 {
    total.saturating_add(saturating_row_len(row_len))
}

pub(in crate::db::executor::load::terminal) fn bytes_page_window_state(
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
pub(in crate::db::executor::load::terminal) const fn bytes_window_limit_exhausted(
    limit_remaining: Option<usize>,
) -> bool {
    matches!(limit_remaining, Some(0))
}

#[cfg(test)]
pub(in crate::db::executor::load::terminal) const fn bytes_window_accept_row(
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

pub(in crate::db::executor::load::terminal) use crate::db::error::executor_invariant as invariant;

// Serialize one value using the canonical runtime codec and return payload len.
pub(in crate::db::executor::load::terminal) fn serialized_value_len(
    value: &Value,
) -> Result<usize, InternalError> {
    serialized_len(value).map_err(|err| {
        InternalError::serialize_internal(format!("bytes(field) value encode failed: {err}"))
    })
}
