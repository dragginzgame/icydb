use crate::{
    db::{
        cursor::{
            ContinuationSignature, CursorBoundary, GroupedContinuationToken, GroupedPlannedCursor,
            PlannedCursor, range_token_from_validated_cursor_anchor,
        },
        direction::Direction,
        executor::RangeToken,
        query::plan::AccessPlannedQuery,
    },
    value::Value,
};

///
/// ContinuationEngine
///
/// Executor-owned continuation protocol facade.
/// Centralizes cursor decode/revalidation entrypoints and grouped pagination
/// token/window contracts so executor load paths consume one boundary.
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

    /// Derive grouped pagination contracts from one grouped plan + grouped cursor state.
    #[must_use]
    pub(in crate::db::executor) fn grouped_paging_contract<K>(
        plan: &AccessPlannedQuery<K>,
        cursor: &GroupedPlannedCursor,
    ) -> GroupedContinuationPaging {
        let initial_offset = plan
            .scalar_plan()
            .page
            .as_ref()
            .map_or(0, |page| page.offset);
        let resume_initial_offset = if cursor.is_empty() {
            initial_offset
        } else {
            cursor.initial_offset()
        };
        let resume_boundary = cursor
            .last_group_key()
            .map(|last_group_key| Value::List(last_group_key.to_vec()));
        let apply_initial_offset = cursor.is_empty();
        let limit = plan
            .scalar_plan()
            .page
            .as_ref()
            .and_then(|page| page.limit)
            .and_then(|limit| usize::try_from(limit).ok());
        let initial_offset_for_page = if apply_initial_offset {
            usize::try_from(initial_offset).unwrap_or(usize::MAX)
        } else {
            0
        };
        let selection_bound = limit.and_then(|limit| {
            limit
                .checked_add(initial_offset_for_page)
                .and_then(|count| count.checked_add(1))
        });

        GroupedContinuationPaging::new(
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        )
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

///
/// GroupedContinuationPaging
///
/// Canonical grouped paging contract derived from grouped plan + cursor state.
/// Keeps grouped resume boundary, offset semantics, and bounded selection shape
/// centralized outside grouped fold mechanics.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedContinuationPaging {
    limit: Option<usize>,
    initial_offset_for_page: usize,
    selection_bound: Option<usize>,
    resume_initial_offset: u32,
    resume_boundary: Option<Value>,
}

impl GroupedContinuationPaging {
    /// Construct one grouped continuation paging contract.
    #[must_use]
    const fn new(
        limit: Option<usize>,
        initial_offset_for_page: usize,
        selection_bound: Option<usize>,
        resume_initial_offset: u32,
        resume_boundary: Option<Value>,
    ) -> Self {
        Self {
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        }
    }

    /// Decompose grouped paging fields into load-fold friendly tuple order.
    #[must_use]
    pub(in crate::db::executor) fn into_parts(
        self,
    ) -> (Option<usize>, usize, Option<usize>, u32, Option<Value>) {
        (
            self.limit,
            self.initial_offset_for_page,
            self.selection_bound,
            self.resume_initial_offset,
            self.resume_boundary,
        )
    }
}
