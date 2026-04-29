//! Module: db::executor::projection::path
//! Responsibility: projection-local nested value-storage path resolution.
//! Does not own: planner path lowering, predicate evaluation, or index access.
//! Boundary: hides `ValueStorageView` behind an executor projection helper.

use crate::{
    db::data::{FieldDecodeError, ValueStorageView, decode_structural_value_storage_bytes},
    value::Value,
};

///
/// CompiledPath
///
/// Executor-owned nested path program used by projection and predicate
/// evaluation.
/// The string form is retained for labels, while the byte form is used by the
/// map walker so per-row key comparison avoids repeated UTF-8 decoding.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CompiledPath {
    /// Owned nested map-key sequence traversed below the resolved root slot.
    pub(in crate::db) segments: Vec<String>,
    /// Precomputed UTF-8 bytes for each nested map-key segment.
    pub(in crate::db) segment_bytes: Vec<Vec<u8>>,
}

impl CompiledPath {
    /// Build a compiled path from already-normalized nested map segments.
    #[must_use]
    pub(in crate::db) fn new(segments: Vec<String>) -> Self {
        let segment_bytes = segments
            .iter()
            .map(|segment| segment.as_bytes().to_vec())
            .collect();

        Self {
            segments,
            segment_bytes,
        }
    }

    /// Borrow the nested map-key sequence used by the value-storage walker.
    #[must_use]
    pub(in crate::db) const fn segments(&self) -> &[String] {
        self.segments.as_slice()
    }

    /// Borrow precomputed nested map-key bytes for scan-time comparison.
    #[must_use]
    pub(in crate::db) const fn segment_bytes(&self) -> &[Vec<u8>] {
        self.segment_bytes.as_slice()
    }
}

/// Resolve one nested map path inside an already-selected root field payload.
pub(in crate::db::executor::projection) fn resolve_path_compiled<'a>(
    raw_bytes: &'a [u8],
    path: &CompiledPath,
) -> Result<Option<&'a [u8]>, FieldDecodeError> {
    let mut current = ValueStorageView::from_raw(raw_bytes)?;

    // The caller has already resolved the root field to a persisted slot
    // payload. Traversal therefore starts at the first nested segment rather
    // than attempting to treat the raw row as a value-storage map.
    for segment in path.segment_bytes() {
        current = match current.map_text_key_bytes(segment)? {
            Some(next) => next,
            None => return Ok(None),
        };
    }

    Ok(Some(current.as_bytes()))
}

/// Resolve and materialize one nested map path through the shared path walker.
pub(in crate::db::executor::projection) fn resolve_and_decode(
    raw_bytes: &[u8],
    path: &CompiledPath,
) -> Result<Option<Value>, FieldDecodeError> {
    let Some(value_bytes) = resolve_path_compiled(raw_bytes, path)? else {
        return Ok(None);
    };

    decode_structural_value_storage_bytes(value_bytes).map(Some)
}
