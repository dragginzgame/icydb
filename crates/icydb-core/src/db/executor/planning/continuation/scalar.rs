//! Module: executor::planning::continuation::scalar
//! Responsibility: scalar continuation planning/runtime bindings for access resume behavior.
//! Does not own: cursor token encoding policy or planner semantic ownership.
//! Boundary: consumes validated continuation contracts and computes scalar resume inputs.

use crate::{
    db::{
        cursor::{
            CursorBoundary, effective_keep_count_for_limit as continuation_keep_count_for_limit,
            effective_page_offset_for_window as continuation_page_offset_for_window,
        },
        direction::Direction,
        executor::{
            AccessScanContinuationInput, ContinuationMode, RouteContinuationPlan,
            planning::route::LoadOrderRouteMode,
        },
        query::plan::{AccessPlannedQuery, ContinuationPolicy},
    },
    error::InternalError,
};

///
/// ScalarContinuationContext
///
/// Normalized scalar continuation runtime state.
/// Carries the validated cursor plus pre-derived boundary and index-range anchor
/// bindings so load/route code does not decode cursor internals directly.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ScalarContinuationContext {
    Initial,
}

impl ScalarContinuationContext {
    /// Construct one empty scalar continuation runtime for initial executions.
    #[must_use]
    pub(in crate::db::executor) const fn initial() -> Self {
        Self::Initial
    }

    /// Borrow optional scalar cursor boundary.
    #[must_use]
    pub(in crate::db::executor) const fn cursor_boundary(self) -> Option<&'static CursorBoundary> {
        match self {
            Self::Initial => None,
        }
    }

    /// Return whether this scalar continuation context has one cursor boundary.
    #[must_use]
    pub(in crate::db::executor) const fn has_cursor_boundary(self) -> bool {
        match self {
            Self::Initial => false,
        }
    }

    /// Derive route continuation mode from scalar continuation context shape.
    #[must_use]
    pub(in crate::db::executor) const fn route_continuation_mode(self) -> ContinuationMode {
        match self {
            Self::Initial => ContinuationMode::Initial,
        }
    }

    /// Derive one route continuation plan from scalar runtime state and planner policy.
    ///
    /// This keeps continuation/window derivation in continuation authority so
    /// route planning consumes one pre-derived continuation contract.
    #[must_use]
    pub(in crate::db::executor) fn route_continuation_plan(
        self,
        plan: &AccessPlannedQuery,
        continuation_policy: ContinuationPolicy,
    ) -> RouteContinuationPlan {
        RouteContinuationPlan::from_scalar_access_window_plan(
            self.route_continuation_mode(),
            continuation_policy,
            plan.scalar_access_window_plan(self.has_cursor_boundary()),
        )
    }

    /// Build access-stream continuation input for routed stream resolution.
    #[must_use]
    pub(in crate::db::executor) const fn access_scan_input(
        self,
        direction: Direction,
    ) -> AccessScanContinuationInput<'static> {
        match self {
            Self::Initial => {
                AccessScanContinuationInput::with_primary_key_boundary(None, direction, None)
            }
        }
    }

    /// Assert scalar route-continuation invariants against this runtime context.
    ///
    /// Keeps scalar continuation protocol sanity checks centralized in
    /// continuation runtime so load entrypoints consume one invariant boundary.
    pub(in crate::db::executor) fn debug_assert_route_continuation_invariants(
        self,
        plan: &AccessPlannedQuery,
        route_continuation: RouteContinuationPlan,
    ) {
        debug_assert!(
            route_continuation.strict_advance_required_when_applied(),
            "route invariant: continuation executions must enforce strict advancement policy",
        );
        debug_assert_eq!(
            route_continuation.effective_offset(),
            continuation_page_offset_for_window(plan, self.has_cursor_boundary()),
            "route window effective offset must match logical plan offset semantics",
        );
    }

    /// Derive effective keep count (`offset + limit`) under this continuation context.
    #[must_use]
    pub(in crate::db::executor) fn keep_count_for_limit_window(
        self,
        plan: &AccessPlannedQuery,
        limit: u32,
    ) -> usize {
        continuation_keep_count_for_limit(plan, self.has_cursor_boundary(), limit)
    }

    /// Validate load scan-budget hint preconditions under this continuation context.
    ///
    /// Bounded load scan hints are only valid for non-continuation executions on
    /// streaming-safe access shapes where access order is already final.
    pub(in crate::db::executor) fn validate_load_scan_budget_hint(
        self,
        scan_budget_hint: Option<usize>,
        load_order_route_mode: LoadOrderRouteMode,
    ) -> Result<(), InternalError> {
        match self {
            Self::Initial => {}
        }
        if scan_budget_hint.is_some() && !load_order_route_mode.allows_streaming_load() {
            return Err(InternalError::query_executor_invariant());
        }

        Ok(())
    }
}
