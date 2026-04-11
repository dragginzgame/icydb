//! Module: db::executor::route::hints::load
//! Defines lightweight load-routing hints used to explain and classify chosen
//! executor routes.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    executor::{ContinuationCapabilities, ExecutionKernel},
    query::plan::{AccessPlannedQuery, PlannerRouteProfile},
};

use crate::db::executor::planning::route::{
    AccessWindow, IndexRangeLimitSpec, RouteCapabilities, RouteContinuationPlan, TopNSeekSpec,
    capability::derive_budget_safety_flags_for_model, secondary_order_contract_active,
};

/// Assess index-range limit pushdown once for this execution and produce the bounded fetch spec.
pub(in crate::db::executor::planning::route) fn assess_index_range_limit_pushdown_for_model(
    plan: &AccessPlannedQuery,
    continuation: RouteContinuationPlan,
    probe_fetch_hint: Option<usize>,
    index_range_limit_pushdown_shape_supported: bool,
) -> Option<IndexRangeLimitSpec> {
    let (access_window, continuation_capabilities) = continuation_hint_inputs(continuation);
    let (has_residual_filter, _, _) = derive_budget_safety_flags_for_model(plan);
    index_range_limit_pushdown_shape_supported.then_some(())?;
    continuation_capabilities
        .index_range_limit_pushdown_allowed()
        .then_some(())?;
    let fetch = probe_fetch_hint.or_else(|| bounded_window_fetch_hint(access_window))?;

    (!has_residual_filter || residual_predicate_pushdown_fetch_is_safe(fetch))
        .then_some(IndexRangeLimitSpec { fetch })
}

/// Shared load-page scan-budget hint gate.
pub(in crate::db::executor::planning::route) fn load_scan_budget_hint(
    plan: &AccessPlannedQuery,
    continuation: RouteContinuationPlan,
    capabilities: RouteCapabilities,
) -> Option<usize> {
    let (access_window, continuation_capabilities) = continuation_hint_inputs(continuation);
    let fetch_hint = bounded_window_fetch_hint(access_window);

    plan.access_strategy().load_window_early_stop_hint(
        continuation_capabilities.applied(),
        capabilities.load_order_route_contract,
        fetch_hint,
    )
}

/// Build an explicit top-N seek contract for ordered load windows when route eligibility permits bounded access traversal.
pub(in crate::db::executor::planning::route) fn top_n_seek_spec_for_model(
    plan: &AccessPlannedQuery,
    planner_route_profile: &PlannerRouteProfile,
    continuation: RouteContinuationPlan,
    capabilities: RouteCapabilities,
) -> Option<TopNSeekSpec> {
    let (access_window, continuation_capabilities) = continuation_hint_inputs(continuation);
    let logical = plan.scalar_plan();
    let has_order = logical
        .order
        .as_ref()
        .is_some_and(|order| !order.fields.is_empty());
    (logical.mode.is_load() && has_order).then_some(())?;
    secondary_order_contract_active(planner_route_profile.logical_pushdown_eligibility())
        .then_some(())?;
    planner_route_profile.secondary_order_contract()?;
    capabilities
        .load_order_route_contract
        .allows_top_n_seek()
        .then_some(())?;
    (!continuation_capabilities.applied()).then_some(())?;

    bounded_window_fetch_hint(access_window).map(TopNSeekSpec::new)
}

// Load-route hint helpers all read the same continuation snapshot fields, so
// unpack them once at the module boundary.
const fn continuation_hint_inputs(
    continuation: RouteContinuationPlan,
) -> (AccessWindow, ContinuationCapabilities) {
    (
        *continuation.fetch_access_window(),
        continuation.capabilities(),
    )
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
pub(in crate::db::executor::planning::route) const fn residual_predicate_pushdown_fetch_is_safe(
    fetch: usize,
) -> bool {
    fetch <= residual_predicate_pushdown_fetch_cap()
}

/// Return one widened bounded fetch for residual-filter retries when the
/// current bounded probe under-fills the requested post-access keep window.
pub(in crate::db::executor) fn widened_residual_predicate_pushdown_fetch(
    current_fetch: usize,
    keep_count: usize,
    post_access_rows: usize,
) -> Option<usize> {
    let cap = residual_predicate_pushdown_fetch_cap();
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

pub(in crate::db::executor) const fn residual_predicate_pushdown_fetch_cap() -> usize {
    256
}

/// Resolve one bounded fetch hint from one access window contract.
pub(in crate::db::executor::planning::route) const fn bounded_window_fetch_hint(
    access_window: AccessWindow,
) -> Option<usize> {
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
    use crate::db::executor::planning::route::widened_residual_predicate_pushdown_fetch;

    #[test]
    fn widened_residual_fetch_grows_underfilled_bounded_probe() {
        assert_eq!(
            widened_residual_predicate_pushdown_fetch(3, 2, 0),
            Some(5),
            "zero-match underfill should widen the bounded fetch enough to look past the missing keep window",
        );
        assert_eq!(
            widened_residual_predicate_pushdown_fetch(3, 2, 1),
            Some(5),
            "partial underfill should widen by the observed discard gap instead of falling back immediately",
        );
    }
}
