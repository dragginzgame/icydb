use super::*;

#[test]
fn session_cursor_error_mapping_parity_matrix_preserves_cursor_variants() {
    for (build_error, assert_inner) in [
        (
            Box::new(|| CursorPlanError::continuation_cursor_boundary_arity_mismatch(2, 1))
                as Box<dyn Fn() -> CursorPlanError>,
            Box::new(|inner: &CursorPlanError| {
                matches!(
                    inner,
                    CursorPlanError::ContinuationCursorBoundaryArityMismatch { .. }
                )
            }) as Box<dyn Fn(&CursorPlanError) -> bool>,
        ),
        (
            Box::new(|| CursorPlanError::continuation_cursor_window_mismatch(8, 3)),
            Box::new(|inner: &CursorPlanError| {
                matches!(
                    inner,
                    CursorPlanError::ContinuationCursorWindowMismatch { .. }
                )
            }),
        ),
        (
            Box::new(|| {
                CursorPlanError::invalid_continuation_cursor(
                    crate::db::cursor::CursorDecodeError::OddLength,
                )
            }),
            Box::new(|inner: &CursorPlanError| {
                matches!(
                    inner,
                    CursorPlanError::InvalidContinuationCursor {
                        reason: crate::db::cursor::CursorDecodeError::OddLength
                    }
                )
            }),
        ),
        (
            Box::new(CursorPlanError::continuation_cursor_primary_key_type_mismatch),
            Box::new(|inner: &CursorPlanError| {
                matches!(
                    inner,
                    CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch { .. }
                )
            }),
        ),
    ] {
        assert_cursor_mapping_parity(build_error, assert_inner);
    }
}
