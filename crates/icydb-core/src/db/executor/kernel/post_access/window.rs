//! Module: executor::kernel::post_access::window
//! Responsibility: in-memory pagination/delete window application helpers.
//! Does not own: query planning or access-path execution behavior.
//! Boundary: post-access vector windowing utilities for kernel pipelines.

use crate::db::executor::compute_page_window;

/// Apply offset/limit pagination to an in-memory vector, in-place.
///
/// - `offset` and `limit` are logical (u32) pagination parameters
/// - Conversion to `usize` happens only at the indexing boundary
#[expect(clippy::cast_possible_truncation)]
pub(super) fn apply_pagination<T>(rows: &mut Vec<T>, offset: u32, limit: Option<u32>) {
    let total: u32 = rows.len() as u32;

    // If offset is past the end, clear everything.
    if offset >= total {
        rows.clear();
        return;
    }

    let start_usize = usize::try_from(offset).unwrap_or(usize::MAX);
    let total_usize = usize::try_from(total).unwrap_or(usize::MAX);
    let end_usize = match limit {
        Some(limit) => compute_page_window(offset, limit, false)
            .keep_count
            .min(total_usize),
        None => total_usize,
    };

    // Drop leading rows, then truncate to window size.
    rows.drain(..start_usize);
    rows.truncate(end_usize.saturating_sub(start_usize));
}

/// Apply an in-memory delete row cap in-place.
pub(super) fn apply_delete_limit<T>(rows: &mut Vec<T>, max_rows: u32) {
    let limit = usize::min(rows.len(), max_rows as usize);
    rows.truncate(limit);
}
