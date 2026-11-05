use crate::db::{
    QueryError, QueryExecutionError,
    cursor::{CursorDecodeError, CursorPlanError, CursorSignaturePrefix},
};
use icydb_diagnostic_code::{DiagnosticCode, ErrorClass, ErrorOrigin};

#[test]
fn cursor_invariant_preserves_runtime_class_and_origin_at_query_boundaries() {
    let error =
        QueryError::from_cursor_plan_error(CursorPlanError::ContinuationCursorInvariantViolation);
    assert!(matches!(
        &error,
        QueryError::Execute(QueryExecutionError::InvariantViolation(_))
    ));
    let diagnostic = error.diagnostic();
    assert_eq!(diagnostic.code(), DiagnosticCode::RuntimeInvariantViolation);
    assert_eq!(diagnostic.class(), ErrorClass::InvariantViolation);
    assert_eq!(diagnostic.origin(), ErrorOrigin::Cursor);
    assert!(error.diagnostic_facts().is_empty());
}

#[test]
fn cursor_rejections_preserve_reason_and_facts_at_query_boundaries() {
    for cursor in [
        CursorPlanError::InvalidContinuationCursor {
            reason: CursorDecodeError::TooLong { len: 20, max: 10 },
        },
        CursorPlanError::grouped_continuation_cursor_direction_mismatch(),
        CursorPlanError::ContinuationCursorSignatureMismatch {
            expected: CursorSignaturePrefix::UNKNOWN,
            actual: CursorSignaturePrefix::UNKNOWN,
        },
        CursorPlanError::ContinuationCursorWindowMismatch {
            expected_offset: 4,
            actual_offset: 2,
        },
    ] {
        let facts = cursor.diagnostic_facts();
        assert!(!facts.is_empty());
        let error = QueryError::from_cursor_plan_error(cursor);
        let diagnostic = error.diagnostic();
        assert_eq!(
            diagnostic.code(),
            DiagnosticCode::QueryInvalidContinuationCursor
        );
        assert_eq!(diagnostic.class(), ErrorClass::Query);
        assert_eq!(diagnostic.origin(), ErrorOrigin::Cursor);
        assert_eq!(error.diagnostic_facts(), facts);
    }
}
