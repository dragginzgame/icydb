//! Module: executor::pipeline::operators::post_access::window
//! Responsibility: in-memory pagination/delete window application helpers.
//! Does not own: query planning or access-path execution behavior.
//! Boundary: post-access vector windowing utilities for execution pipelines.

/// Apply an in-memory delete row cap in-place.
pub(super) fn apply_delete_limit<T>(rows: &mut Vec<T>, max_rows: u32) {
    let limit = usize::min(rows.len(), max_rows as usize);
    rows.truncate(limit);
}
