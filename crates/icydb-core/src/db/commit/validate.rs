use crate::{
    db::commit::{CommitMarker, commit_corruption_message},
    error::InternalError,
};

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
        if row_op.before.is_none() && row_op.after.is_none() {
            return Err(InternalError::store_corruption(commit_corruption_message(
                "row op has neither before nor after payload",
            )));
        }
    }

    Ok(())
}
