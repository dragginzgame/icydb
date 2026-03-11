//! Module: db::executor::continuation::grouped::context
//! Responsibility: grouped continuation runtime context assembly and cursor emission.
//! Does not own: grouped route feasibility selection or grouped fold/output operators.
//! Boundary: continuation authority for grouped runtime cursor context.

use crate::{
    db::{
        cursor::ContinuationSignature,
        executor::{
            ContinuationEngine, GroupedContinuationCapabilities, GroupedPaginationWindow,
            pipeline::contracts::PageCursor,
        },
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
    capabilities: GroupedContinuationCapabilities,
    continuation_signature: ContinuationSignature,
    continuation_boundary_arity: usize,
    grouped_pagination_window: GroupedPaginationWindow,
}

impl GroupedContinuationContext {
    /// Construct grouped continuation runtime context from grouped contract values.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        capabilities: GroupedContinuationCapabilities,
        continuation_signature: ContinuationSignature,
        continuation_boundary_arity: usize,
        grouped_pagination_window: GroupedPaginationWindow,
    ) -> Self {
        Self {
            capabilities,
            continuation_signature,
            continuation_boundary_arity,
            grouped_pagination_window,
        }
    }

    /// Return immutable grouped continuation capabilities for this execution.
    #[must_use]
    pub(in crate::db::executor) const fn capabilities(&self) -> GroupedContinuationCapabilities {
        self.capabilities
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
            return Err(crate::db::error::query_executor_invariant(format!(
                "grouped continuation boundary arity mismatch: expected {}, found {}",
                self.continuation_boundary_arity,
                last_group_key.len()
            )));
        }

        Ok(PageCursor::Grouped(
            ContinuationEngine::grouped_next_cursor_token(
                self.continuation_signature,
                last_group_key,
                self.grouped_pagination_window.resume_initial_offset(),
            ),
        ))
    }
}
