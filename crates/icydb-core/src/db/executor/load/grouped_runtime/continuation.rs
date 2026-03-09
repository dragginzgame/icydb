use crate::{
    db::{
        cursor::ContinuationSignature,
        executor::{
            ContinuationEngine,
            load::{PageCursor, invariant},
        },
        query::plan::GroupedContinuationWindow,
    },
    error::InternalError,
    value::Value,
};

///
/// GroupedPaginationWindow
///
/// Runtime grouped pagination projection consumed by grouped fold/page stages.
/// Separates grouped paging primitives from route/fold call signatures so grouped
/// continuation window semantics flow through one runtime boundary object.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor::load) struct GroupedPaginationWindow {
    limit: Option<usize>,
    initial_offset_for_page: usize,
    selection_bound: Option<usize>,
    resume_initial_offset: u32,
    resume_boundary: Option<Value>,
}

impl GroupedPaginationWindow {
    /// Build runtime grouped pagination projection from planner continuation window contract.
    #[must_use]
    pub(in crate::db::executor::load) fn from_contract(window: GroupedContinuationWindow) -> Self {
        let (
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        ) = window.into_parts();

        Self {
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        }
    }

    /// Return grouped page limit for this execution window.
    #[must_use]
    pub(in crate::db::executor::load) const fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Return grouped page-initial offset for this execution window.
    #[must_use]
    pub(in crate::db::executor::load) const fn initial_offset_for_page(&self) -> usize {
        self.initial_offset_for_page
    }

    /// Return bounded grouped candidate selection cap (`offset + limit + 1`) when active.
    #[must_use]
    pub(in crate::db::executor::load) const fn selection_bound(&self) -> Option<usize> {
        self.selection_bound
    }

    /// Return resume offset encoded into grouped continuation tokens.
    #[must_use]
    pub(in crate::db::executor::load) const fn resume_initial_offset(&self) -> u32 {
        self.resume_initial_offset
    }

    /// Borrow optional grouped resume boundary value for continuation filtering.
    #[must_use]
    pub(in crate::db::executor::load) const fn resume_boundary(&self) -> Option<&Value> {
        self.resume_boundary.as_ref()
    }
}

///
/// GroupedContinuationCapabilities
///
/// Immutable grouped continuation capability projection derived from grouped
/// cursor-application state and grouped pagination window shape.
/// Grouped route/fold layers consume this capability contract instead of
/// re-deriving continuation gates from raw window primitives.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor::load) struct GroupedContinuationCapabilities {
    applied: bool,
    resume_boundary_applied: bool,
    selection_bound_applied: bool,
}

impl GroupedContinuationCapabilities {
    /// Construct one grouped continuation capability projection from grouped paging window shape.
    #[must_use]
    pub(in crate::db::executor::load) const fn from_window(
        applied: bool,
        window: &GroupedPaginationWindow,
    ) -> Self {
        Self {
            applied,
            resume_boundary_applied: window.resume_boundary().is_some(),
            selection_bound_applied: window.selection_bound().is_some(),
        }
    }

    /// Return whether grouped continuation is applied for this execution.
    #[must_use]
    pub(in crate::db::executor::load) const fn applied(self) -> bool {
        self.applied
    }

    /// Return whether grouped resume-boundary filtering is active.
    #[must_use]
    pub(in crate::db::executor::load) const fn resume_boundary_applied(self) -> bool {
        self.resume_boundary_applied
    }

    /// Return whether grouped candidate selection bound is active.
    #[must_use]
    pub(in crate::db::executor::load) const fn selection_bound_applied(self) -> bool {
        self.selection_bound_applied
    }
}

///
/// GroupedContinuationContext
///
/// Runtime grouped continuation context derived from immutable continuation
/// contracts. Carries grouped continuation signature, boundary arity, and one
/// grouped pagination projection bundle consumed by grouped runtime stages.
///

pub(in crate::db::executor::load) struct GroupedContinuationContext {
    capabilities: GroupedContinuationCapabilities,
    continuation_signature: ContinuationSignature,
    continuation_boundary_arity: usize,
    grouped_pagination_window: GroupedPaginationWindow,
}

impl GroupedContinuationContext {
    /// Construct grouped continuation runtime context from grouped contract values.
    #[must_use]
    pub(in crate::db::executor::load) const fn new(
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
    pub(in crate::db::executor::load) const fn capabilities(
        &self,
    ) -> GroupedContinuationCapabilities {
        self.capabilities
    }

    /// Borrow grouped runtime pagination projection.
    #[must_use]
    pub(in crate::db::executor::load) const fn grouped_pagination_window(
        &self,
    ) -> &GroupedPaginationWindow {
        &self.grouped_pagination_window
    }

    /// Build one grouped next cursor after validating grouped boundary arity.
    pub(in crate::db::executor::load) fn grouped_next_cursor(
        &self,
        last_group_key: Vec<Value>,
    ) -> Result<PageCursor, InternalError> {
        if last_group_key.len() != self.continuation_boundary_arity {
            return Err(invariant(format!(
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::executor::load::{GroupedContinuationCapabilities, GroupedPaginationWindow};

    #[test]
    fn grouped_continuation_capabilities_mark_initial_window_as_unapplied() {
        let window = GroupedPaginationWindow {
            limit: Some(3),
            initial_offset_for_page: 2,
            selection_bound: Some(6),
            resume_initial_offset: 2,
            resume_boundary: None,
        };
        let capabilities = GroupedContinuationCapabilities::from_window(false, &window);

        assert!(!capabilities.applied());
        assert!(!capabilities.resume_boundary_applied());
        assert!(capabilities.selection_bound_applied());
    }

    #[test]
    fn grouped_continuation_capabilities_mark_resume_window_as_applied() {
        let window = GroupedPaginationWindow {
            limit: Some(3),
            initial_offset_for_page: 0,
            selection_bound: Some(4),
            resume_initial_offset: 2,
            resume_boundary: Some(crate::value::Value::List(Vec::new())),
        };
        let capabilities = GroupedContinuationCapabilities::from_window(true, &window);

        assert!(capabilities.applied());
        assert!(capabilities.resume_boundary_applied());
        assert!(capabilities.selection_bound_applied());
    }
}
