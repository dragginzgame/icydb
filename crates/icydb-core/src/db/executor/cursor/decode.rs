use crate::{
    db::cursor::{
        CursorBoundary, CursorPlanError,
        decode_pk_cursor_boundary as decode_pk_cursor_boundary_shared,
    },
    error::InternalError,
    traits::EntityKind,
};

/// Decode a typed primary-key cursor boundary for PK-ordered executor paths.
pub(in crate::db) fn decode_pk_cursor_boundary<E>(
    boundary: Option<&CursorBoundary>,
) -> Result<Option<E::Key>, InternalError>
where
    E: EntityKind,
{
    decode_pk_cursor_boundary_shared::<E>(boundary).map_err(|err| match err {
        CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch { value: None, .. } => {
            InternalError::query_executor_invariant("pk cursor slot must be present")
        }
        CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch { value: Some(_), .. } => {
            InternalError::query_executor_invariant("pk cursor slot type mismatch")
        }
        _ => InternalError::query_executor_invariant(err.to_string()),
    })
}
