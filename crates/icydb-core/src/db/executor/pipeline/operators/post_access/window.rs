//! Module: executor::pipeline::operators::post_access::window
//! Responsibility: in-memory pagination/delete window application helpers.
//! Does not own: query planning or access-path execution behavior.
//! Boundary: post-access vector windowing utilities for execution pipelines.

/// Apply one ordered delete window in-place.
pub(super) fn apply_delete_window<T>(rows: &mut Vec<T>, offset: u32, limit: Option<u32>) {
    let offset = usize::min(rows.len(), offset as usize);
    if offset > 0 {
        rows.drain(..offset);
    }

    if let Some(limit) = limit {
        let limit = usize::min(rows.len(), limit as usize);
        rows.truncate(limit);
    }
}
