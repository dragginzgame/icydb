//! Module: db::error::cursor
//!
//! Responsibility: cursor-boundary invariant and plan-error conversion helpers.
//! Does not own: cursor token decoding itself.
//! Boundary: cursor plan/domain failures are mapped here.

use crate::{db::cursor::CursorPlanError, error::InternalError};

/// Map cursor-plan failures into runtime taxonomy classes.
///
/// Cursor token/version/signature/window/payload mismatches are external
/// input failures (`Unsupported` at cursor origin). Only explicit
/// continuation invariant violations remain invariant-class failures.
pub(crate) fn from_cursor_plan_error(err: CursorPlanError) -> InternalError {
    match err {
        CursorPlanError::ContinuationCursorInvariantViolation { reason } => {
            InternalError::cursor_invariant(InternalError::executor_invariant_message(reason))
        }
        CursorPlanError::InvalidContinuationCursor { .. }
        | CursorPlanError::InvalidContinuationCursorPayload { .. }
        | CursorPlanError::ContinuationCursorVersionMismatch { .. }
        | CursorPlanError::ContinuationCursorSignatureMismatch { .. }
        | CursorPlanError::ContinuationCursorBoundaryArityMismatch { .. }
        | CursorPlanError::ContinuationCursorWindowMismatch { .. }
        | CursorPlanError::ContinuationCursorBoundaryTypeMismatch { .. }
        | CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch { .. } => {
            InternalError::cursor_unsupported(err.to_string())
        }
    }
}
