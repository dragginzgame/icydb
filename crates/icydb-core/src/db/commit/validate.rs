use crate::{
    db::{
        commit::{CommitMarker, commit_corruption_message},
        data::{DataKey, MAX_ROW_BYTES, RawDataKey},
    },
    error::InternalError,
    traits::Storable,
};
use std::borrow::Cow;

/// Validate commit-marker row-op shape invariants.
///
/// Every row op must represent a concrete mutation:
/// - insert (`before=None`, `after=Some`)
/// - update (`before=Some`, `after=Some`)
/// - delete (`before=Some`, `after=None`)
///
/// The empty shape (`before=None`, `after=None`) is corruption.
pub(crate) fn validate_commit_marker_shape(marker: &CommitMarker) -> Result<(), InternalError> {
    // Phase 1: reject row ops that cannot encode any mutation semantics.
    for row_op in &marker.row_ops {
        if row_op.entity_path.is_empty() {
            return Err(InternalError::store_corruption(commit_corruption_message(
                "row op has empty entity_path",
            )));
        }
        if row_op.before.is_none() && row_op.after.is_none() {
            return Err(InternalError::store_corruption(commit_corruption_message(
                "row op has neither before nor after payload",
            )));
        }

        // Guard row payload size at marker-decode boundary so recovery does not
        // need to classify oversized persisted bytes during apply preparation.
        for (label, payload) in [
            ("before", row_op.before.as_ref()),
            ("after", row_op.after.as_ref()),
        ] {
            if let Some(bytes) = payload
                && bytes.len() > MAX_ROW_BYTES as usize
            {
                return Err(InternalError::store_corruption(commit_corruption_message(
                    format!(
                        "row op {label} payload exceeds max size: {} bytes (limit {MAX_ROW_BYTES})",
                        bytes.len()
                    ),
                )));
            }
        }

        if row_op.key.len() != DataKey::STORED_SIZE_USIZE {
            return Err(InternalError::store_corruption(commit_corruption_message(
                format!(
                    "row op key has invalid length: {} bytes (expected {})",
                    row_op.key.len(),
                    DataKey::STORED_SIZE_USIZE
                ),
            )));
        }
        let raw_key = <RawDataKey as Storable>::from_bytes(Cow::Borrowed(row_op.key.as_slice()));
        DataKey::try_from_raw(&raw_key).map_err(|err| {
            InternalError::store_corruption(commit_corruption_message(format!(
                "row op key decode failed: {err}"
            )))
        })?;
    }

    Ok(())
}
