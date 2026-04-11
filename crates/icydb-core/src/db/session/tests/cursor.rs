use super::*;

#[test]
fn session_cursor_error_mapping_parity_boundary_arity() {
    assert_cursor_mapping_parity(
        || CursorPlanError::continuation_cursor_boundary_arity_mismatch(2, 1),
        |inner| {
            matches!(
                inner,
                CursorPlanError::ContinuationCursorBoundaryArityMismatch {
                    expected: 2,
                    found: 1
                }
            )
        },
    );
}

#[test]
fn session_cursor_error_mapping_parity_window_mismatch() {
    assert_cursor_mapping_parity(
        || CursorPlanError::continuation_cursor_window_mismatch(8, 3),
        |inner| {
            matches!(
                inner,
                CursorPlanError::ContinuationCursorWindowMismatch {
                    expected_offset: 8,
                    actual_offset: 3
                }
            )
        },
    );
}

#[test]
fn session_cursor_error_mapping_parity_decode_reason() {
    assert_cursor_mapping_parity(
        || {
            CursorPlanError::invalid_continuation_cursor(
                crate::db::codec::cursor::CursorDecodeError::OddLength,
            )
        },
        |inner| {
            matches!(
                inner,
                CursorPlanError::InvalidContinuationCursor {
                    reason: crate::db::codec::cursor::CursorDecodeError::OddLength
                }
            )
        },
    );
}

#[test]
fn session_cursor_error_mapping_parity_primary_key_type_mismatch() {
    assert_cursor_mapping_parity(
        || {
            CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                "id",
                "ulid",
                Some(crate::value::Value::Text("not-a-ulid".to_string())),
            )
        },
        |inner| {
            matches!(
                inner,
                CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                    field,
                    expected,
                    value: Some(crate::value::Value::Text(value))
                } if field == "id" && expected == "ulid" && value == "not-a-ulid"
            )
        },
    );
}

#[test]
fn session_cursor_error_mapping_parity_matrix_preserves_cursor_variants() {
    // Keep one matrix-level canary test name so cross-module audit references remain stable.
    assert_cursor_mapping_parity(
        || CursorPlanError::continuation_cursor_boundary_arity_mismatch(2, 1),
        |inner| {
            matches!(
                inner,
                CursorPlanError::ContinuationCursorBoundaryArityMismatch {
                    expected: 2,
                    found: 1
                }
            )
        },
    );
}
