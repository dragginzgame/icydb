use crate::{db::query::plan::GroupedContinuationWindow, value::Value};

///
/// GroupedPaginationWindow
///
/// Runtime grouped pagination projection consumed by grouped fold/page stages.
/// Separates grouped paging primitives from route/fold call signatures so grouped
/// continuation window semantics flow through one runtime boundary object.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor::load) struct GroupedPaginationWindow {
    pub(super) limit: Option<usize>,
    pub(super) initial_offset_for_page: usize,
    pub(super) selection_bound: Option<usize>,
    pub(super) resume_initial_offset: u32,
    pub(super) resume_boundary: Option<Value>,
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
