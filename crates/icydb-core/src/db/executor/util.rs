//! Module: executor::util
//! Responsibility: tiny helpers shared by executor runtime and executor-local tests.
//! Does not own: execution semantics, routing, or plan validation.

/// Apply one offset/limit window to an already ordered in-memory row set.
///
/// This helper owns only the vector slicing mechanics. Callers remain
/// responsible for deciding whether paging, projection, or delete semantics
/// allow this window to run at their phase boundary.
pub(in crate::db::executor) fn apply_offset_limit_window<T>(
    rows: &mut Vec<T>,
    offset: u32,
    limit: Option<u32>,
) {
    let offset = usize::min(rows.len(), usize::try_from(offset).unwrap_or(usize::MAX));
    if offset > 0 {
        rows.drain(..offset);
    }

    if let Some(limit) = limit {
        let limit = usize::min(rows.len(), usize::try_from(limit).unwrap_or(usize::MAX));
        rows.truncate(limit);
    }
}

/// Convert one row-count length into `u32` using saturating semantics.
#[must_use]
pub(in crate::db::executor) fn saturating_u32_len(row_len: usize) -> u32 {
    u32::try_from(row_len).unwrap_or(u32::MAX)
}
