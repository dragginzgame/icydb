use crate::{
    db::{
        cursor::{
            ContinuationSignature, CursorBoundary, GroupedContinuationToken, PlannedCursor,
            range_token_from_validated_cursor_anchor,
        },
        direction::Direction,
        executor::RangeToken,
    },
    value::Value,
};

///
/// ContinuationEngine
///
/// Executor-owned continuation protocol facade.
/// Centralizes scalar cursor runtime bindings and grouped cursor token emission
/// so executor load paths consume one boundary for runtime continuation payloads.
///

pub(in crate::db::executor) struct ContinuationEngine;

impl ContinuationEngine {
    /// Derive scalar runtime cursor/access bindings from one validated cursor.
    #[must_use]
    pub(in crate::db::executor) fn scalar_runtime(
        cursor: PlannedCursor,
    ) -> ScalarContinuationRuntime {
        ScalarContinuationRuntime::new(cursor)
    }

    /// Build one grouped continuation token for grouped page finalization.
    #[must_use]
    pub(in crate::db::executor) const fn grouped_next_cursor_token(
        continuation_signature: ContinuationSignature,
        last_group_key: Vec<Value>,
        resume_initial_offset: u32,
    ) -> GroupedContinuationToken {
        GroupedContinuationToken::new_with_direction(
            continuation_signature,
            last_group_key,
            Direction::Asc,
            resume_initial_offset,
        )
    }
}

///
/// ScalarContinuationRuntime
///
/// Normalized scalar continuation runtime state.
/// Carries the validated cursor plus pre-derived boundary and index-range anchor
/// bindings so load/route code does not decode cursor internals directly.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ScalarContinuationRuntime {
    cursor_boundary: Option<CursorBoundary>,
    index_range_token: Option<RangeToken>,
}

impl ScalarContinuationRuntime {
    /// Construct one empty scalar continuation runtime for initial executions.
    #[must_use]
    pub(in crate::db::executor) const fn initial() -> Self {
        Self {
            cursor_boundary: None,
            index_range_token: None,
        }
    }

    /// Construct one scalar continuation runtime from explicit boundary/token parts.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn from_parts(
        cursor_boundary: Option<CursorBoundary>,
        index_range_token: Option<RangeToken>,
    ) -> Self {
        Self {
            cursor_boundary,
            index_range_token,
        }
    }

    /// Build one scalar runtime cursor binding bundle from one planned cursor.
    #[must_use]
    pub(in crate::db::executor) fn new(cursor: PlannedCursor) -> Self {
        let cursor_boundary = cursor.boundary().cloned();
        let index_range_token = cursor
            .index_range_anchor()
            .map(range_token_from_validated_cursor_anchor);

        Self {
            cursor_boundary,
            index_range_token,
        }
    }

    /// Borrow optional scalar cursor boundary.
    #[must_use]
    pub(in crate::db::executor) const fn cursor_boundary(&self) -> Option<&CursorBoundary> {
        self.cursor_boundary.as_ref()
    }

    /// Borrow optional index-range continuation anchor token.
    #[must_use]
    pub(in crate::db::executor) const fn index_range_token(&self) -> Option<&RangeToken> {
        self.index_range_token.as_ref()
    }
}
