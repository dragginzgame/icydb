//! Module: db::executor::pipeline::grouped_runtime::continuation::window
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::grouped_runtime::continuation::window.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::value::Value;

///
/// GroupedPaginationWindow
///
/// Runtime grouped pagination projection consumed by grouped fold/page stages.
/// Separates grouped paging primitives from route/fold call signatures so grouped
/// continuation window semantics flow through one runtime boundary object.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedPaginationWindow {
    pub(super) limit: Option<usize>,
    pub(super) initial_offset_for_page: usize,
    pub(super) selection_bound: Option<usize>,
    pub(super) resume_initial_offset: u32,
    pub(super) resume_boundary: Option<Value>,
}

impl GroupedPaginationWindow {
    /// Build runtime grouped pagination projection from continuation contract primitives.
    #[must_use]
    pub(in crate::db::executor) const fn new(
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

    /// Return grouped page limit for this execution window.
    #[must_use]
    pub(in crate::db::executor) const fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Return grouped page-initial offset for this execution window.
    #[must_use]
    pub(in crate::db::executor) const fn initial_offset_for_page(&self) -> usize {
        self.initial_offset_for_page
    }

    /// Return bounded grouped candidate selection cap (`offset + limit + 1`) when active.
    #[must_use]
    pub(in crate::db::executor) const fn selection_bound(&self) -> Option<usize> {
        self.selection_bound
    }

    /// Return resume offset encoded into grouped continuation tokens.
    #[must_use]
    pub(in crate::db::executor) const fn resume_initial_offset(&self) -> u32 {
        self.resume_initial_offset
    }

    /// Borrow optional grouped resume boundary value for continuation filtering.
    #[must_use]
    pub(in crate::db::executor) const fn resume_boundary(&self) -> Option<&Value> {
        self.resume_boundary.as_ref()
    }
}
