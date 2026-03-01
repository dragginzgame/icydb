use crate::db::{
    cursor::{
        ContinuationSignature, CursorPlanError, GroupedContinuationToken, GroupedPlannedCursor,
        validate_cursor_window_offset,
    },
    direction::Direction,
};

// Validate and materialize grouped cursor state through one grouped-only spine.
pub(in crate::db) fn validate_grouped_cursor(
    cursor: Option<&[u8]>,
    entity_path: &'static str,
    continuation_signature: ContinuationSignature,
    expected_initial_offset: u32,
) -> Result<GroupedPlannedCursor, CursorPlanError> {
    let Some(cursor) = cursor else {
        return Ok(GroupedPlannedCursor::none());
    };
    let token = decode_grouped_cursor_token(cursor)?;

    validate_grouped_cursor_signature(entity_path, &continuation_signature, &token.signature())?;
    validate_grouped_cursor_direction(token.direction())?;
    validate_cursor_window_offset(expected_initial_offset, token.initial_offset())?;

    Ok(GroupedPlannedCursor::new(
        token.last_group_key().to_vec(),
        token.initial_offset(),
    ))
}

// Revalidate grouped cursor offset compatibility for executor-provided state.
pub(in crate::db) fn revalidate_grouped_cursor_state(
    expected_initial_offset: u32,
    cursor: GroupedPlannedCursor,
) -> Result<GroupedPlannedCursor, CursorPlanError> {
    if cursor.is_empty() {
        return Ok(GroupedPlannedCursor::none());
    }
    validate_cursor_window_offset(expected_initial_offset, cursor.initial_offset())?;

    Ok(cursor)
}

// Decode one grouped continuation token through the grouped token codec boundary.
fn decode_grouped_cursor_token(cursor: &[u8]) -> Result<GroupedContinuationToken, CursorPlanError> {
    GroupedContinuationToken::decode(cursor)
        .map_err(|err| CursorPlanError::invalid_continuation_cursor_payload(err.to_string()))
}

// Validate grouped continuation signature against the executable grouped shape.
fn validate_grouped_cursor_signature(
    entity_path: &'static str,
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

// Grouped continuation cursors are constrained to ascending logical order.
fn validate_grouped_cursor_direction(direction: Direction) -> Result<(), CursorPlanError> {
    if direction != Direction::Asc {
        return Err(CursorPlanError::invalid_continuation_cursor_payload(
            "grouped continuation cursor direction must be ascending",
        ));
    }

    Ok(())
}
