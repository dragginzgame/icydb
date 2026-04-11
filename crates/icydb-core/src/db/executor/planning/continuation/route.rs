//! Module: db::executor::planning::continuation::route
//! Responsibility: continuation-owned route window and continuation-mode contracts.
//! Does not own: route feasibility derivation or planner continuation-policy semantics.
//! Boundary: continuation authority exports immutable route continuation primitives.

use crate::db::query::plan::{ContinuationPolicy, ScalarAccessWindowPlan};

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
/// Keeps continuation flags and route-window semantics under one immutable
/// routing contract.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(clippy::struct_excessive_bools)]
pub(in crate::db::executor) struct RouteContinuationPlan {
    mode: ContinuationMode,
    applied: bool,
    strict_advance_required_when_applied: bool,
    grouped_safe_when_applied: bool,
    index_range_limit_pushdown_allowed: bool,
    pub(in crate::db::executor) access_window_keep: AccessWindow,
    pub(in crate::db::executor) access_window_fetch: AccessWindow,
}

impl RouteContinuationPlan {
    #[must_use]
    const fn new(
        mode: ContinuationMode,
        continuation_policy: ContinuationPolicy,
        access_window_keep: AccessWindow,
        access_window_fetch: AccessWindow,
    ) -> Self {
        let applied = !matches!(mode, ContinuationMode::Initial);

        Self {
            mode,
            applied,
            strict_advance_required_when_applied: !applied
                || continuation_policy.requires_strict_advance(),
            grouped_safe_when_applied: !applied || continuation_policy.is_grouped_safe(),
            index_range_limit_pushdown_allowed: !continuation_policy.requires_anchor()
                || !matches!(mode, ContinuationMode::CursorBoundary),
            access_window_keep,
            access_window_fetch,
        }
    }

    #[must_use]
    pub(in crate::db::executor::planning::continuation) fn from_scalar_access_window_plan(
        mode: ContinuationMode,
        continuation_policy: ContinuationPolicy,
        window_plan: ScalarAccessWindowPlan,
    ) -> Self {
        let lower_bound = window_plan.lower_bound();
        let page_limit = window_plan.limit();
        let keep_count = window_plan.keep_count();
        let fetch_count = window_plan.fetch_count();
        let access_window_keep = AccessWindow::new(lower_bound, page_limit, keep_count);
        let access_window_fetch = AccessWindow::new(lower_bound, page_limit, fetch_count);

        Self::new(
            mode,
            continuation_policy,
            access_window_keep,
            access_window_fetch,
        )
    }

    /// Construct one canonical initial continuation plan for mutation-style routes.
    #[must_use]
    const fn initial_with_policy(continuation_policy: ContinuationPolicy) -> Self {
        Self::new(
            ContinuationMode::Initial,
            continuation_policy,
            AccessWindow::new(0, None, None),
            AccessWindow::new(0, None, None),
        )
    }

    /// Construct one canonical initial continuation plan for mutation routes.
    #[must_use]
    pub(in crate::db::executor) const fn initial_for_mutation() -> Self {
        Self::initial_with_policy(ContinuationPolicy::new(true, true, true))
    }

    #[must_use]
    pub(in crate::db::executor) const fn mode(self) -> ContinuationMode {
        self.mode
    }

    #[must_use]
    pub(in crate::db::executor) const fn applied(self) -> bool {
        self.applied
    }

    #[must_use]
    pub(in crate::db::executor) const fn strict_advance_required_when_applied(self) -> bool {
        self.strict_advance_required_when_applied
    }

    #[must_use]
    pub(in crate::db::executor) const fn grouped_safe_when_applied(self) -> bool {
        self.grouped_safe_when_applied
    }

    #[must_use]
    pub(in crate::db::executor) const fn index_range_limit_pushdown_allowed(self) -> bool {
        self.index_range_limit_pushdown_allowed
    }

    #[must_use]
    pub(in crate::db::executor) fn effective_offset(self) -> u32 {
        u32::try_from(self.access_window_keep.lower_bound()).unwrap_or(u32::MAX)
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
/// `lower_bound` is the effective offset, and `fetch_limit` is the optional
/// bounded access budget derived from the planner-owned scalar window plan.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct AccessWindow {
    lower_bound: usize,
    page_limit: Option<u32>,
    fetch_limit: Option<usize>,
}

impl AccessWindow {
    /// Construct one immutable access-window contract.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        lower_bound: usize,
        page_limit: Option<u32>,
        fetch_limit: Option<usize>,
    ) -> Self {
        Self {
            lower_bound,
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
        executor::{AccessWindow, ContinuationMode, RouteContinuationPlan},
        query::plan::ContinuationPolicy,
    };

    fn route_continuation(
        access_window_keep: AccessWindow,
        access_window_fetch: AccessWindow,
    ) -> RouteContinuationPlan {
        RouteContinuationPlan::new(
            ContinuationMode::Initial,
            ContinuationPolicy::new(true, true, true),
            access_window_keep,
            access_window_fetch,
        )
    }

    #[test]
    fn route_continuation_access_window_limit_zero_projects_zero_fetch_limit() {
        let continuation = route_continuation(
            AccessWindow::new(4, Some(0), Some(4)),
            AccessWindow::new(4, Some(0), Some(0)),
        );
        let access_window = continuation.fetch_access_window();

        assert_eq!(continuation.effective_offset(), 4);
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
            AccessWindow::new(3, Some(2), Some(5)),
            AccessWindow::new(3, Some(2), Some(6)),
        );
        let keep_window = continuation.keep_access_window();
        let fetch_window = continuation.fetch_access_window();

        assert_eq!(continuation.effective_offset(), 3);
        assert_eq!(keep_window.lower_bound(), 3);
        assert_eq!(keep_window.page_limit(), Some(2));
        assert_eq!(keep_window.fetch_limit(), Some(5));
        assert!(!keep_window.is_zero_window());
        assert_eq!(fetch_window.fetch_limit(), Some(6));
    }

    #[test]
    fn route_continuation_access_window_unbounded_limit_projects_unbounded_fetch() {
        let continuation = route_continuation(
            AccessWindow::new(0, None, None),
            AccessWindow::new(0, None, None),
        );
        let access_window = continuation.fetch_access_window();

        assert_eq!(continuation.effective_offset(), 0);
        assert_eq!(access_window.lower_bound(), 0);
        assert_eq!(access_window.page_limit(), None);
        assert_eq!(access_window.fetch_limit(), None);
        assert!(!access_window.is_zero_window());
    }

    #[test]
    fn route_continuation_initial_mode_applies_policy_flags() {
        let continuation =
            RouteContinuationPlan::initial_with_policy(ContinuationPolicy::new(true, false, false));

        assert_eq!(continuation.mode(), ContinuationMode::Initial);
        assert!(!continuation.applied());
        assert!(continuation.strict_advance_required_when_applied());
        assert!(continuation.grouped_safe_when_applied());
        assert!(continuation.index_range_limit_pushdown_allowed());
    }

    #[test]
    fn route_continuation_cursor_boundary_disables_index_range_pushdown_when_anchor_required() {
        let continuation = RouteContinuationPlan::new(
            ContinuationMode::CursorBoundary,
            ContinuationPolicy::new(true, true, true),
            AccessWindow::new(0, None, None),
            AccessWindow::new(0, None, None),
        );

        assert!(continuation.applied());
        assert!(continuation.strict_advance_required_when_applied());
        assert!(continuation.grouped_safe_when_applied());
        assert!(!continuation.index_range_limit_pushdown_allowed());
    }

    #[test]
    fn route_continuation_anchor_mode_keeps_index_range_pushdown_enabled() {
        let continuation = RouteContinuationPlan::new(
            ContinuationMode::IndexRangeAnchor,
            ContinuationPolicy::new(true, true, true),
            AccessWindow::new(0, None, None),
            AccessWindow::new(0, None, None),
        );

        assert_eq!(continuation.mode(), ContinuationMode::IndexRangeAnchor);
        assert!(continuation.applied());
        assert!(continuation.index_range_limit_pushdown_allowed());
    }
}
