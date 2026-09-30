//! Module: executor::planning::continuation::grouped::context
//! Responsibility: grouped continuation runtime context assembly and cursor emission.
//! Does not own: grouped route feasibility selection or grouped fold/output operators.
//! Boundary: continuation authority for grouped runtime cursor context.

use crate::{
    db::{
        cursor::{ContinuationSignature, GroupedContinuationToken},
        direction::Direction,
        executor::GroupedPaginationWindow,
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
    ) -> Result<GroupedContinuationToken, InternalError> {
        if last_group_key.len() != self.continuation_boundary_arity {
            return Err(InternalError::query_executor_invariant());
        }

        Ok(GroupedContinuationToken::new_with_direction(
            self.continuation_signature,
            last_group_key,
            self.direction,
            self.grouped_pagination_window.resume_initial_offset(),
        ))
    }
}
