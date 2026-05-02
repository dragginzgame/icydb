//! Module: db::executor::projection::path
//! Responsibility: projection-local nested value-storage path resolution.
//! Does not own: planner path lowering, predicate evaluation, or index access.
//! Boundary: hides `ValueStorageView` behind an executor projection helper.

use crate::db::data::{FieldDecodeError, ValueStorageView};

/// Resolve one nested map path using already-encoded segment bytes.
pub(in crate::db::executor::projection) fn resolve_path_segments<'a>(
    raw_bytes: &'a [u8],
    segment_bytes: &[Box<[u8]>],
) -> Result<Option<&'a [u8]>, FieldDecodeError> {
    let mut current = ValueStorageView::from_raw_validated(raw_bytes)?;

    // The caller has already resolved the root field to a persisted slot
    // payload. Traversal therefore starts at the first nested segment rather
    // than attempting to treat the raw row as a value-storage map.
    for segment in segment_bytes {
        current = match current.map_text_key_bytes(segment)? {
            Some(next) => next,
            None => return Ok(None),
        };
    }

    Ok(Some(current.as_bytes()))
}
