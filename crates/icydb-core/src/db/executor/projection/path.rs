//! Module: db::executor::projection::path
//! Responsibility: projection-local nested value-storage path resolution.
//! Does not own: planner path lowering, predicate evaluation, or index access.
//! Boundary: hides `ValueStorageView` behind an executor projection helper.

use crate::{
    db::{
        data::{FieldDecodeError, ValueStorageView},
        query::plan::expr::ProjectionEvalError,
    },
    error::InternalError,
    value::Value,
};

/// Walk one already-materialized record path without cloning nested maps.
/// Missing members and null ancestors both project as an absent scalar leaf;
/// a non-map ancestor is persisted-row corruption.
pub(in crate::db::executor) fn resolve_value_field_path<'value>(
    root: &'value Value,
    field: &str,
    segments: &[String],
) -> Result<Option<&'value Value>, ProjectionEvalError> {
    let mut current = root;
    for segment in segments {
        if matches!(current, Value::Null) {
            return Ok(None);
        }
        let entries = current.as_map().ok_or_else(|| {
            let err = InternalError::persisted_row_field_decode_failed(
                field,
                "field-path traversal requires a map value",
            );
            ProjectionEvalError::FieldPathEvaluationFailed {
                class: err.class(),
                origin: err.origin(),
            }
        })?;
        let Some((_, value)) = entries
            .iter()
            .find(|(key, _)| matches!(key, Value::Text(text) if text == segment))
        else {
            return Ok(None);
        };
        current = value;
    }

    Ok(Some(current))
}

/// Resolve one nested map path using already-encoded segment bytes.
pub(in crate::db::executor) fn resolve_path_segments<'a>(
    raw_bytes: &'a [u8],
    segment_bytes: &[Box<[u8]>],
) -> Result<Option<&'a [u8]>, FieldDecodeError> {
    let mut current = ValueStorageView::from_raw_validated(raw_bytes)?;

    // The caller has already resolved the root field to a persisted slot
    // payload. Traversal therefore starts at the first nested segment rather
    // than attempting to treat the raw row as a value-storage map.
    for segment in segment_bytes {
        if current.is_null() {
            return Ok(None);
        }
        current = match current.map_text_key_bytes(segment)? {
            Some(next) => next,
            None => return Ok(None),
        };
    }

    Ok(Some(current.as_bytes()))
}
