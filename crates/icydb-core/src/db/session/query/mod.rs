//! Module: db::session::query
//! Responsibility: session-bound query planning, explain, and cursor execution
//! helpers that recover store visibility before delegating to query-owned logic.
//! Does not own: query intent construction or executor runtime semantics.
//! Boundary: resolves session visibility and cursor policy before handing work to the planner/executor.

mod cache;
mod cardinality_tiebreak;
mod dynamic;
mod exact_count;
mod exact_key;
mod grouped;
mod projection;

use crate::db::{QueryError, executor::ExecutorPlanError};

#[cfg(feature = "sql")]
pub(in crate::db) use cache::QueryPlanCacheReuse;
#[cfg(feature = "sql")]
pub(in crate::db::session) use cache::query_plan_requires_cardinality_lifecycle_recheck;
#[doc(hidden)]
pub use exact_key::{
    MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES, MAX_TYPED_EXACT_KEY_BATCH_ITEMS,
    MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES, MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES,
};
#[cfg(feature = "sql")]
pub(in crate::db) use projection::StructuralProjectionContract;
#[cfg(feature = "sql")]
pub(in crate::db::session) use projection::StructuralProjectionPayload;
#[cfg(feature = "sql")]
pub(in crate::db::session) use projection::projection_labels_from_projection_spec;

// Convert executor plan-surface failures at the session boundary so query
// errors do not import executor-owned error enums.
pub(in crate::db::session) fn query_error_from_executor_plan_error(
    err: ExecutorPlanError,
) -> QueryError {
    match err {
        ExecutorPlanError::Cursor(err) => QueryError::from_cursor_plan_error(*err),
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        QueryExecutionError,
        cursor::{CursorDecodeError, CursorPlanError, CursorSignaturePrefix},
    };
    use icydb_diagnostic_code::{DiagnosticCode, DiagnosticDecodeReason, ErrorClass, ErrorOrigin};

    #[test]
    fn cursor_invariant_preserves_runtime_class_and_origin_at_query_boundaries() {
        for convert in [QueryError::from_cursor_plan_error, |error| {
            query_error_from_executor_plan_error(ExecutorPlanError::from(error))
        }] {
            let error = convert(CursorPlanError::ContinuationCursorInvariantViolation);
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
    }

    #[test]
    fn cursor_rejections_preserve_reason_and_facts_at_query_boundaries() {
        for convert in [QueryError::from_cursor_plan_error, |error| {
            query_error_from_executor_plan_error(ExecutorPlanError::from(error))
        }] {
            for cursor in [
                CursorPlanError::InvalidContinuationCursor {
                    reason: CursorDecodeError::TooLong { len: 20, max: 10 },
                },
                CursorPlanError::InvalidContinuationCursorPayload {
                    reason: DiagnosticDecodeReason::CursorGroupedDirectionMismatch,
                    index: Some(2),
                },
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
                let error = convert(cursor);
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
    }
}
