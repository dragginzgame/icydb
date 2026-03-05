//! Module: db::executor::route::hints
//! Responsibility: route-owned bounded-fetch and scan-budget hint derivation.
//! Does not own: route capability derivation or dispatch execution.
//! Boundary: emits optional hints consumed by stream/runtime surfaces.

use crate::{
    db::{
        direction::Direction,
        executor::{ExecutionKernel, load::LoadExecutor},
        query::builder::AggregateExpr,
        query::plan::{AccessPlannedQuery, DistinctExecutionStrategy},
    },
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{
    IndexRangeLimitSpec, RouteCapabilities, RouteContinuationPlan, RouteWindowPlan,
    aggregate_bounded_probe_fetch_hint, aggregate_supports_bounded_probe_hint,
    direction_allows_physical_fetch_hint,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Assess index-range limit pushdown once for this execution and produce
    // the bounded fetch spec when all eligibility gates pass.
    pub(super) fn assess_index_range_limit_pushdown(
        plan: &AccessPlannedQuery<E::Key>,
        continuation: RouteContinuationPlan,
        probe_fetch_hint: Option<usize>,
        capabilities: RouteCapabilities,
    ) -> Option<IndexRangeLimitSpec> {
        let route_window = continuation.window();
        if !capabilities.index_range_limit_pushdown_shape_eligible {
            return None;
        }
        if !continuation.index_range_limit_pushdown_allowed() {
            return None;
        }
        if let Some(fetch) = probe_fetch_hint {
            if plan.scalar_plan().predicate.is_some()
                && !Self::residual_predicate_pushdown_fetch_is_safe(fetch)
            {
                return None;
            }

            return Some(IndexRangeLimitSpec { fetch });
        }

        let page = plan.scalar_plan().page.as_ref()?;
        let limit = page.limit?;
        if limit == 0 {
            return Some(IndexRangeLimitSpec { fetch: 0 });
        }

        let fetch = route_window.fetch_count_for(true)?;
        if plan.scalar_plan().predicate.is_some()
            && !Self::residual_predicate_pushdown_fetch_is_safe(fetch)
        {
            return None;
        }

        Some(IndexRangeLimitSpec { fetch })
    }

    // Shared load-page scan-budget hint gate.
    pub(super) const fn load_scan_budget_hint(
        continuation: RouteContinuationPlan,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if !continuation.load_scan_budget_hint_allowed(capabilities) {
            return None;
        }

        continuation.window().fetch_count_for(true)
    }

    // Shared bounded-probe safety gate for aggregate key-stream hints.
    // Contract:
    // - DISTINCT + offset must remain unbounded so deduplication is applied
    //   before offset consumption without risking under-fetch.
    // - If dedup/projection/composite semantics evolve, this gate is the first
    //   place to re-evaluate bounded-probe correctness.
    pub(super) fn bounded_probe_hint_is_safe(plan: &AccessPlannedQuery<E::Key>) -> bool {
        let offset = usize::try_from(ExecutionKernel::effective_page_offset(plan, None))
            .unwrap_or(usize::MAX);
        let distinct_enabled = !matches!(
            plan.distinct_execution_strategy(),
            DistinctExecutionStrategy::None
        );

        !(distinct_enabled && offset > 0)
    }

    // Residual predicates are allowed for index-range limit pushdown only when
    // the bounded fetch remains small. This caps amplification risk when the
    // post-access residual filter rejects many bounded candidates.
    pub(super) const fn residual_predicate_pushdown_fetch_is_safe(fetch: usize) -> bool {
        fetch <= Self::residual_predicate_pushdown_fetch_cap()
    }

    pub(in crate::db::executor) const fn residual_predicate_pushdown_fetch_cap() -> usize {
        256
    }

    pub(super) const fn count_pushdown_fetch_hint(
        route_window: RouteWindowPlan,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if !capabilities.bounded_probe_hint_safe {
            return None;
        }

        route_window.fetch_count_for(false)
    }

    pub(super) fn aggregate_probe_fetch_hint(
        aggregate: &AggregateExpr,
        direction: Direction,
        capabilities: RouteCapabilities,
        route_window: RouteWindowPlan,
    ) -> Option<usize> {
        if aggregate.target_field().is_some() {
            return None;
        }
        let kind = aggregate.kind();
        if !aggregate_supports_bounded_probe_hint(kind) {
            return None;
        }
        if route_window.limit() == Some(0) {
            return Some(0);
        }
        if !direction_allows_physical_fetch_hint(
            direction,
            capabilities.desc_physical_reverse_supported,
        ) {
            return None;
        }
        if !capabilities.bounded_probe_hint_safe {
            return None;
        }

        let offset = usize::try_from(route_window.effective_offset).unwrap_or(usize::MAX);
        let page_limit = route_window
            .limit()
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

        aggregate_bounded_probe_fetch_hint(kind, direction, offset, page_limit)
    }
}
