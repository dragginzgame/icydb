//! Module: db::executor::route::contracts::continuation
//! Responsibility: module-local ownership and contracts for db::executor::route::contracts::continuation.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{executor::ContinuationCapabilities, query::plan::ScalarAccessWindowPlan};

///
/// ContinuationMode
///
/// Route-owned continuation classification used to keep resume-policy decisions
/// explicit and isolated from access-shape modeling.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ContinuationMode {
    Initial,
    CursorBoundary,
    IndexRangeAnchor,
}

///
/// RouteContinuationPlan
///
/// Route-owned continuation projection bundle.
/// Keeps continuation capabilities and route-window
/// semantics under one immutable routing contract.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct RouteContinuationPlan {
    capabilities: ContinuationCapabilities,
    pub(in crate::db::executor) effective_offset: u32,
    pub(in crate::db::executor::route) access_window_keep: AccessWindow,
    pub(in crate::db::executor::route) access_window_fetch: AccessWindow,
}

impl RouteContinuationPlan {
    #[must_use]
    pub(in crate::db::executor::route) const fn new(
        capabilities: ContinuationCapabilities,
        effective_offset: u32,
        access_window_keep: AccessWindow,
        access_window_fetch: AccessWindow,
    ) -> Self {
        Self {
            capabilities,
            effective_offset,
            access_window_keep,
            access_window_fetch,
        }
    }

    #[must_use]
    pub(in crate::db::executor::route) fn from_scalar_access_window_plan(
        capabilities: ContinuationCapabilities,
        window_plan: ScalarAccessWindowPlan,
    ) -> Self {
        let effective_offset = window_plan.effective_offset();
        let lower_bound = window_plan.lower_bound();
        let keep_count = window_plan.keep_count();
        let page_limit = window_plan.limit();
        let fetch_count = window_plan.fetch_count();
        let access_window_keep = AccessWindow::new(lower_bound, keep_count, page_limit, keep_count);
        let access_window_fetch =
            AccessWindow::new(lower_bound, keep_count, page_limit, fetch_count);

        Self::new(
            capabilities,
            effective_offset,
            access_window_keep,
            access_window_fetch,
        )
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn mode(self) -> ContinuationMode {
        self.capabilities.mode()
    }

    #[must_use]
    pub(in crate::db::executor) const fn capabilities(self) -> ContinuationCapabilities {
        self.capabilities
    }

    #[must_use]
    pub(in crate::db::executor) const fn effective_offset(self) -> u32 {
        self.effective_offset
    }

    #[must_use]
    pub(in crate::db::executor) const fn limit(&self) -> Option<u32> {
        self.access_window_keep.page_limit()
    }

    #[must_use]
    pub(in crate::db::executor) const fn keep_access_window(&self) -> &AccessWindow {
        &self.access_window_keep
    }

    #[must_use]
    pub(in crate::db::executor) const fn fetch_access_window(&self) -> &AccessWindow {
        &self.access_window_fetch
    }
}

///
/// AccessWindow
///
/// Route-projected bounded access-window contract.
/// `lower_bound` is the effective offset, `upper_bound` is the optional bounded
/// keep-count horizon, and `fetch_limit` is the optional bounded access budget.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct AccessWindow {
    lower_bound: usize,
    upper_bound: Option<usize>,
    page_limit: Option<u32>,
    fetch_limit: Option<usize>,
}

impl AccessWindow {
    /// Construct one immutable access-window contract.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        lower_bound: usize,
        upper_bound: Option<usize>,
        page_limit: Option<u32>,
        fetch_limit: Option<usize>,
    ) -> Self {
        Self {
            lower_bound,
            upper_bound,
            page_limit,
            fetch_limit,
        }
    }

    /// Return the effective lower-bound offset.
    #[must_use]
    pub(in crate::db::executor) const fn lower_bound(self) -> usize {
        self.lower_bound
    }

    /// Return the optional page-limit window width.
    #[must_use]
    pub(in crate::db::executor) const fn page_limit(self) -> Option<u32> {
        self.page_limit
    }

    /// Return the optional bounded fetch limit.
    #[must_use]
    pub(in crate::db::executor) const fn fetch_limit(self) -> Option<usize> {
        self.fetch_limit
    }

    /// Return true when the window is explicitly `LIMIT 0`.
    #[must_use]
    pub(in crate::db::executor) const fn is_zero_window(self) -> bool {
        matches!(self.page_limit, Some(0))
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        executor::{ContinuationCapabilities, route::contracts::continuation::ContinuationMode},
        query::plan::ContinuationPolicy,
    };

    use crate::db::executor::route::contracts::continuation::{
        AccessWindow, RouteContinuationPlan,
    };

    fn route_continuation(
        effective_offset: u32,
        access_window_keep: AccessWindow,
        access_window_fetch: AccessWindow,
    ) -> RouteContinuationPlan {
        RouteContinuationPlan::new(
            ContinuationCapabilities::new(
                ContinuationMode::Initial,
                ContinuationPolicy::new(true, true, true),
            ),
            effective_offset,
            access_window_keep,
            access_window_fetch,
        )
    }

    #[test]
    fn route_continuation_access_window_limit_zero_projects_zero_fetch_limit() {
        let continuation = route_continuation(
            4,
            AccessWindow::new(4, Some(4), Some(0), Some(4)),
            AccessWindow::new(4, Some(4), Some(0), Some(0)),
        );
        let access_window = continuation.fetch_access_window();

        assert_eq!(access_window.lower_bound(), 4);
        assert_eq!(access_window.page_limit(), Some(0));
        assert_eq!(access_window.fetch_limit(), Some(0));
        assert!(
            access_window.is_zero_window(),
            "LIMIT 0 route windows must project zero-fetch access windows",
        );
    }

    #[test]
    fn route_continuation_access_window_bounded_limit_projects_offset_and_fetch_counts() {
        let continuation = route_continuation(
            3,
            AccessWindow::new(3, Some(5), Some(2), Some(5)),
            AccessWindow::new(3, Some(5), Some(2), Some(6)),
        );
        let keep_window = continuation.keep_access_window();
        let fetch_window = continuation.fetch_access_window();

        assert_eq!(keep_window.lower_bound(), 3);
        assert_eq!(keep_window.page_limit(), Some(2));
        assert_eq!(keep_window.fetch_limit(), Some(5));
        assert!(!keep_window.is_zero_window());
        assert_eq!(fetch_window.fetch_limit(), Some(6));
    }

    #[test]
    fn route_continuation_access_window_unbounded_limit_projects_unbounded_fetch() {
        let continuation = route_continuation(
            0,
            AccessWindow::new(0, None, None, None),
            AccessWindow::new(0, None, None, None),
        );
        let access_window = continuation.fetch_access_window();

        assert_eq!(access_window.lower_bound(), 0);
        assert_eq!(access_window.page_limit(), None);
        assert_eq!(access_window.fetch_limit(), None);
        assert!(!access_window.is_zero_window());
    }
}
