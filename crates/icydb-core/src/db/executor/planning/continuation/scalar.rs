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
use std::rc::Rc;

///
/// ScalarContinuationContext
///
/// Normalized scalar continuation runtime state.
/// Carries the validated cursor plus pre-derived boundary and index-range anchor
/// bindings so load/route code does not decode cursor internals directly.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ScalarContinuationContext {
    cursor_boundary: Option<Rc<CursorBoundary>>,
    physical_primary_key_boundary: Option<Rc<CursorBoundary>>,
}

impl ScalarContinuationContext {
    /// Construct one empty scalar continuation runtime for initial executions.
    #[must_use]
    pub(in crate::db) const fn initial() -> Self {
        Self {
            cursor_boundary: None,
            physical_primary_key_boundary: None,
        }
    }

    /// Construct one runtime continuation after its authenticated token and
    /// immutable page contract have been validated by the session boundary.
    #[must_use]
    pub(in crate::db) fn resumed(cursor_boundary: CursorBoundary) -> Self {
        Self {
            cursor_boundary: Some(Rc::new(cursor_boundary)),
            physical_primary_key_boundary: None,
        }
    }

    /// Construct one resumed runtime with authenticated physical primary-key progress.
    ///
    /// `cursor_boundary` remains the last row actually emitted. The physical
    /// boundary may be later when residual predicates rejected additional
    /// candidates before the page work envelope stopped.
    #[must_use]
    pub(in crate::db) fn resumed_with_primary_progress(
        cursor_boundary: Option<CursorBoundary>,
        physical_primary_key_boundary: CursorBoundary,
    ) -> Self {
        Self {
            cursor_boundary: cursor_boundary.map(Rc::new),
            physical_primary_key_boundary: Some(Rc::new(physical_primary_key_boundary)),
        }
    }

    /// Borrow optional scalar cursor boundary.
    #[must_use]
    pub(in crate::db::executor) fn cursor_boundary(&self) -> Option<&CursorBoundary> {
        self.cursor_boundary.as_deref()
    }

    /// Return whether this scalar continuation has logical or physical progress.
    #[must_use]
    pub(in crate::db) const fn has_progress(&self) -> bool {
        self.cursor_boundary.is_some() || self.physical_primary_key_boundary.is_some()
    }

    /// Derive route continuation mode from scalar continuation context shape.
    #[must_use]
    pub(in crate::db::executor) const fn route_continuation_mode(&self) -> ContinuationMode {
        if self.has_progress() {
            ContinuationMode::CursorBoundary
        } else {
            ContinuationMode::Initial
        }
    }

    /// Derive one route continuation plan from scalar runtime state and planner policy.
    ///
    /// This keeps continuation/window derivation in continuation authority so
    /// route planning consumes one pre-derived continuation contract.
    #[must_use]
    pub(in crate::db::executor) fn route_continuation_plan(
        &self,
        plan: &AccessPlannedQuery,
        continuation_policy: ContinuationPolicy,
    ) -> RouteContinuationPlan {
        RouteContinuationPlan::from_scalar_access_window_plan(
            self.route_continuation_mode(),
            continuation_policy,
            plan.scalar_access_window_plan(self.has_progress()),
        )
    }

    /// Build access-stream continuation input for routed stream resolution.
    #[must_use]
    pub(in crate::db::executor) fn access_scan_input(
        &self,
        direction: Direction,
        plan: &AccessPlannedQuery,
    ) -> AccessScanContinuationInput<'_> {
        let primary_key_ordered = plan
            .primary_key_names()
            .ok()
            .is_some_and(|primary_key_names| {
                plan.scalar_plan().order.as_ref().is_some_and(|order| {
                    order
                        .primary_key_only_direction_fields(primary_key_names.as_slice())
                        .is_some()
                })
            });
        AccessScanContinuationInput::with_primary_key_boundary(
            None,
            direction,
            primary_key_ordered
                .then_some(
                    self.physical_primary_key_boundary
                        .as_deref()
                        .or_else(|| self.cursor_boundary()),
                )
                .flatten(),
        )
    }

    /// Assert scalar route-continuation invariants against this runtime context.
    ///
    /// Keeps scalar continuation protocol sanity checks centralized in
    /// continuation runtime so load entrypoints consume one invariant boundary.
    pub(in crate::db::executor) fn debug_assert_route_continuation_invariants(
        &self,
        plan: &AccessPlannedQuery,
        route_continuation: RouteContinuationPlan,
    ) {
        debug_assert!(
            route_continuation.strict_advance_required_when_applied(),
            "route invariant: continuation executions must enforce strict advancement policy",
        );
        debug_assert_eq!(
            route_continuation.effective_offset(),
            continuation_page_offset_for_window(plan, self.has_progress()),
            "route window effective offset must match logical plan offset semantics",
        );
    }

    /// Derive effective keep count (`offset + limit`) under this continuation context.
    #[must_use]
    pub(in crate::db::executor) fn keep_count_for_limit_window(
        &self,
        plan: &AccessPlannedQuery,
        limit: u32,
    ) -> usize {
        continuation_keep_count_for_limit(plan, self.has_progress(), limit)
    }

    /// Validate load scan-budget hint preconditions under this continuation context.
    ///
    /// Bounded load scan hints are only valid for non-continuation executions on
    /// streaming-safe access shapes where access order is already final.
    pub(in crate::db::executor) fn validate_load_scan_budget_hint(
        &self,
        scan_budget_hint: Option<usize>,
        load_order_route_mode: LoadOrderRouteMode,
    ) -> Result<(), InternalError> {
        if scan_budget_hint.is_some() && self.has_progress() {
            return Err(InternalError::query_executor_invariant());
        }
        if scan_budget_hint.is_some() && !load_order_route_mode.allows_streaming_load() {
            return Err(InternalError::query_executor_invariant());
        }

        Ok(())
    }
}
