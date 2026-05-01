//! Module: db::executor::planning::route::hints::load
//! Defines lightweight load-routing hints used to explain and classify chosen
//! executor routes.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    executor::{
        ExecutionKernel,
        planning::route::{
            AccessWindow, IndexRangeLimitSpec, RouteCapabilities, RouteContinuationPlan,
            TopNSeekSpec, secondary_order_contract_active,
        },
    },
    query::plan::{AccessPlannedQuery, PlannerRouteProfile},
};

/// Assess index-range limit pushdown once for this execution and produce the bounded fetch spec.
pub(in crate::db::executor::planning::route) fn assess_index_range_limit_pushdown_for_model(
    continuation: RouteContinuationPlan,
    probe_fetch_hint: Option<usize>,
    index_range_limit_pushdown_shape_supported: bool,
    capabilities: RouteCapabilities,
) -> Option<IndexRangeLimitSpec> {
    let access_window = *continuation.fetch_access_window();
    index_range_limit_pushdown_shape_supported.then_some(())?;
    continuation
        .index_range_limit_pushdown_allowed()
        .then_some(())?;
    let fetch = probe_fetch_hint.or_else(|| bounded_window_fetch_hint(access_window))?;

    (!capabilities.residual_filter_present()
        || residual_filter_predicate_pushdown_fetch_is_safe(fetch))
    .then_some(IndexRangeLimitSpec { fetch })
}

/// Shared load-page scan-budget hint gate.
pub(in crate::db::executor::planning::route) fn load_scan_budget_hint(
    continuation: RouteContinuationPlan,
    capabilities: RouteCapabilities,
) -> Option<usize> {
    bounded_streaming_load_window_fetch_hint(continuation, capabilities)
}

/// Build an explicit top-N seek contract for ordered load windows when route eligibility permits bounded access traversal.
pub(in crate::db::executor::planning::route) fn top_n_seek_spec_for_model(
    plan: &AccessPlannedQuery,
    planner_route_profile: &PlannerRouteProfile,
    continuation: RouteContinuationPlan,
    capabilities: RouteCapabilities,
) -> Option<TopNSeekSpec> {
    let logical = plan.scalar_plan();
    let has_order = logical
        .order
        .as_ref()
        .is_some_and(|order| !order.fields.is_empty());
    (logical.mode.is_load() && has_order).then_some(())?;
    secondary_order_contract_active(planner_route_profile.logical_pushdown_eligibility())
        .then_some(())?;
    planner_route_profile.secondary_order_contract()?;
    bounded_streaming_load_window_fetch_hint(continuation, capabilities)
        .filter(|_| capabilities.load_order_route_contract().allows_top_n_seek())
        .map(TopNSeekSpec::new)
}

// Resolve one bounded fetch hint for streaming-safe load windows. This keeps
// the continuation/window gate shared between scan-budget hinting and Top-N
// seek derivation so those load-hint surfaces do not re-derive the same
// bounded streaming window facts independently.
fn bounded_streaming_load_window_fetch_hint(
    continuation: RouteContinuationPlan,
    capabilities: RouteCapabilities,
) -> Option<usize> {
    (!continuation.applied()).then_some(())?;
    capabilities
        .load_order_route_contract()
        .allows_streaming_load()
        .then_some(())?;

    bounded_window_fetch_hint(*continuation.fetch_access_window())
}

/// Return whether bounded aggregate probe hints are safe for this plan.
pub(in crate::db::executor::planning::route) fn bounded_probe_hint_is_safe(
    plan: &AccessPlannedQuery,
) -> bool {
    let offset =
        usize::try_from(ExecutionKernel::effective_page_offset(plan, None)).unwrap_or(usize::MAX);
    let distinct_enabled = plan.scalar_plan().distinct;

    !(distinct_enabled && offset > 0)
}

/// Return whether one bounded fetch remains safe under residual-predicate filtering.
const fn residual_filter_predicate_pushdown_fetch_is_safe(fetch: usize) -> bool {
    fetch <= residual_filter_predicate_pushdown_fetch_cap()
}

/// Return one widened bounded fetch for residual-filter retries when the
/// current bounded probe under-fills the requested post-access keep window.
pub(in crate::db::executor) fn widened_residual_filter_predicate_pushdown_fetch(
    current_fetch: usize,
    keep_count: usize,
    post_access_rows: usize,
) -> Option<usize> {
    let cap = residual_filter_predicate_pushdown_fetch_cap();
    if keep_count == 0 || current_fetch >= cap {
        return None;
    }

    let growth = if post_access_rows == 0 {
        std::cmp::max(keep_count, 1)
    } else {
        std::cmp::max(
            std::cmp::max(
                current_fetch.saturating_sub(post_access_rows),
                keep_count.saturating_sub(post_access_rows),
            ),
            1,
        )
    };
    let widened_fetch = current_fetch.saturating_add(growth);
    let capped_fetch = if widened_fetch > cap {
        cap
    } else {
        widened_fetch
    };

    if capped_fetch > current_fetch {
        Some(capped_fetch)
    } else {
        None
    }
}

pub(in crate::db::executor) const fn residual_filter_predicate_pushdown_fetch_cap() -> usize {
    256
}

/// Resolve one bounded fetch hint from one access window contract.
pub(super) const fn bounded_window_fetch_hint(access_window: AccessWindow) -> Option<usize> {
    if access_window.is_zero_window() {
        return Some(0);
    }

    access_window.fetch_limit()
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::executor::planning::route::widened_residual_filter_predicate_pushdown_fetch;

    #[test]
    fn widened_residual_fetch_grows_underfilled_bounded_probe() {
        assert_eq!(
            widened_residual_filter_predicate_pushdown_fetch(3, 2, 0),
            Some(5),
            "zero-match underfill should widen the bounded fetch enough to look past the missing keep window",
        );
        assert_eq!(
            widened_residual_filter_predicate_pushdown_fetch(3, 2, 1),
            Some(5),
            "partial underfill should widen by the observed discard gap instead of falling back immediately",
        );
    }
}
