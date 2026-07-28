//! Module: cursor::spine
//! Responsibility: centralized cursor validation spine for continuation resume contracts.
//! Does not own: cursor wire serialization format or planner query-shape semantics.
//! Boundary: validates direction/window/boundary/anchor invariants before resume execution.

use crate::db::{
    cursor::{
        ContinuationSignature, CursorPlanError, GroupedContinuationToken, ValidatedGroupedCursor,
        validate_cursor_window_offset,
    },
    direction::Direction,
};

/// Validate continuation token signature against the executable signature.
fn validate_cursor_signature(
    entity_path: &str,
    expected_signature: &ContinuationSignature,
    actual_signature: &ContinuationSignature,
) -> Result<(), CursorPlanError> {
    if actual_signature != expected_signature {
        return Err(CursorPlanError::continuation_cursor_signature_mismatch(
            entity_path,
            expected_signature,
            actual_signature,
        ));
    }

    Ok(())
}

/// Validate and materialize grouped cursor state through the canonical cursor spine.
#[cfg(test)]
pub(in crate::db::cursor) fn validate_grouped_cursor(
    cursor: Option<&[u8]>,
    entity_path: &str,
    continuation_signature: ContinuationSignature,
    expected_direction: Direction,
    expected_initial_offset: u32,
) -> Result<ValidatedGroupedCursor, CursorPlanError> {
    let Some(cursor) = cursor else {
        return Ok(ValidatedGroupedCursor::none());
    };
    let token =
        GroupedContinuationToken::decode(cursor).map_err(CursorPlanError::from_token_wire_error)?;

    validate_cursor_signature(entity_path, &continuation_signature, &token.signature())?;
    validate_grouped_cursor_direction(expected_direction, token.direction())?;
    validate_cursor_window_offset(expected_initial_offset, token.initial_offset())?;

    Ok(ValidatedGroupedCursor::new_validated(
        token.last_group_key().to_vec(),
        token.initial_offset(),
    ))
}

/// Validate and materialize already-decoded grouped cursor state through the
/// canonical grouped cursor spine.
pub(in crate::db::cursor) fn validate_grouped_cursor_token(
    cursor: Option<GroupedContinuationToken>,
    entity_path: &str,
    continuation_signature: ContinuationSignature,
    expected_direction: Direction,
    expected_initial_offset: u32,
) -> Result<ValidatedGroupedCursor, CursorPlanError> {
    let Some(token) = cursor else {
        return Ok(ValidatedGroupedCursor::none());
    };
    let (signature, last_group_key, direction, initial_offset) = token.into_components();

    validate_cursor_signature(entity_path, &continuation_signature, &signature)?;
    validate_grouped_cursor_direction(expected_direction, direction)?;
    validate_cursor_window_offset(expected_initial_offset, initial_offset)?;

    Ok(ValidatedGroupedCursor::new_validated(
        last_group_key,
        initial_offset,
    ))
}

// Grouped continuation cursors must match the grouped execution direction so
// resume-boundary filtering stays consistent with grouped page ordering.
fn validate_grouped_cursor_direction(
    expected_direction: Direction,
    actual_direction: Direction,
) -> Result<(), CursorPlanError> {
    if actual_direction != expected_direction {
        return Err(CursorPlanError::grouped_continuation_cursor_direction_mismatch());
    }

    Ok(())
}
