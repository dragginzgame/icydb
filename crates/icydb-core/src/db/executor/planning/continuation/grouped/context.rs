//! Module: db::executor::planning::continuation::grouped::context
//! Responsibility: grouped continuation runtime context assembly and cursor emission.
//! Does not own: grouped route feasibility selection or grouped fold/output operators.
//! Boundary: continuation authority for grouped runtime cursor context.

use crate::{
    db::{
        cursor::{ContinuationSignature, GroupedContinuationToken},
        direction::Direction,
        executor::{GroupedPaginationWindow, pipeline::contracts::PageCursor},
    },
    error::InternalError,
    value::Value,
};

///
/// GroupedContinuationContext
///
/// Runtime grouped continuation context derived from immutable continuation
/// contracts. Carries grouped continuation signature, boundary arity, and one
/// grouped pagination projection bundle consumed by grouped runtime stages.
///

pub(in crate::db::executor) struct GroupedContinuationContext {
    continuation_signature: ContinuationSignature,
    continuation_boundary_arity: usize,
    grouped_pagination_window: GroupedPaginationWindow,
    direction: Direction,
}

impl GroupedContinuationContext {
    /// Construct grouped continuation runtime context from grouped contract values.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        continuation_signature: ContinuationSignature,
        continuation_boundary_arity: usize,
        grouped_pagination_window: GroupedPaginationWindow,
        direction: Direction,
    ) -> Self {
        Self {
            continuation_signature,
            continuation_boundary_arity,
            grouped_pagination_window,
            direction,
        }
    }

    /// Return whether grouped resume-boundary filtering is active.
    #[must_use]
    pub(in crate::db::executor) const fn resume_boundary_applied(&self) -> bool {
        self.grouped_pagination_window.resume_boundary().is_some()
    }

    /// Return whether grouped candidate selection bound is active.
    #[must_use]
    pub(in crate::db::executor) const fn selection_bound_applied(&self) -> bool {
        self.grouped_pagination_window.selection_bound().is_some()
    }

    /// Borrow grouped runtime pagination projection.
    #[must_use]
    pub(in crate::db::executor) const fn grouped_pagination_window(
        &self,
    ) -> &GroupedPaginationWindow {
        &self.grouped_pagination_window
    }

    /// Build one grouped next cursor after validating grouped boundary arity.
    pub(in crate::db::executor) fn grouped_next_cursor(
        &self,
        last_group_key: Vec<Value>,
    ) -> Result<PageCursor, InternalError> {
        if last_group_key.len() != self.continuation_boundary_arity {
            return Err(InternalError::query_executor_invariant(format!(
                "grouped continuation boundary arity mismatch: expected {}, found {}",
                self.continuation_boundary_arity,
                last_group_key.len()
            )));
        }

        Ok(PageCursor::Grouped(
            GroupedContinuationToken::new_with_direction(
                self.continuation_signature,
                last_group_key,
                self.direction,
                self.grouped_pagination_window.resume_initial_offset(),
            ),
        ))
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            cursor::ContinuationSignature,
            executor::{GroupedContinuationContext, GroupedPaginationWindow},
        },
        value::Value,
    };

    #[test]
    fn grouped_continuation_context_marks_initial_window_as_unapplied() {
        let window = GroupedPaginationWindow::new(Some(3), 2, Some(6), 2, None);
        let continuation = GroupedContinuationContext::new(
            ContinuationSignature::from_bytes([1; 32]),
            1,
            window,
            crate::db::direction::Direction::Asc,
        );

        assert!(!continuation.resume_boundary_applied());
        assert!(continuation.selection_bound_applied());
    }

    #[test]
    fn grouped_continuation_context_marks_resume_window_as_applied() {
        let window =
            GroupedPaginationWindow::new(Some(3), 0, Some(4), 2, Some(Value::List(Vec::new())));
        let continuation = GroupedContinuationContext::new(
            ContinuationSignature::from_bytes([2; 32]),
            1,
            window,
            crate::db::direction::Direction::Asc,
        );

        assert!(continuation.resume_boundary_applied());
        assert!(continuation.selection_bound_applied());
    }
}
