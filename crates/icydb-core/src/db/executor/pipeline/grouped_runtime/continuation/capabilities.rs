//! Module: db::executor::pipeline::grouped_runtime::continuation::capabilities
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::grouped_runtime::continuation::capabilities.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::executor::pipeline::grouped_runtime::GroupedPaginationWindow;

///
/// GroupedContinuationCapabilities
///
/// Immutable grouped continuation capability projection derived from grouped
/// cursor-application state and grouped pagination window shape.
/// Grouped route/fold layers consume this capability contract instead of
/// re-deriving continuation gates from raw window primitives.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedContinuationCapabilities {
    applied: bool,
    resume_boundary_applied: bool,
    selection_bound_applied: bool,
}

impl GroupedContinuationCapabilities {
    /// Construct one grouped continuation capability projection from grouped paging window shape.
    #[must_use]
    pub(in crate::db::executor) const fn from_window(
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
    pub(in crate::db::executor) const fn applied(self) -> bool {
        self.applied
    }

    /// Return whether grouped resume-boundary filtering is active.
    #[must_use]
    pub(in crate::db::executor) const fn resume_boundary_applied(self) -> bool {
        self.resume_boundary_applied
    }

    /// Return whether grouped candidate selection bound is active.
    #[must_use]
    pub(in crate::db::executor) const fn selection_bound_applied(self) -> bool {
        self.selection_bound_applied
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::executor::pipeline::grouped_runtime::{
        GroupedContinuationCapabilities, GroupedPaginationWindow,
    };

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
