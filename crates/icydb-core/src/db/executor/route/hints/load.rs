//! Module: db::executor::route::hints::load
//! Responsibility: module-local ownership and contracts for db::executor::route::hints::load.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::{ExecutionKernel, load::LoadExecutor},
        query::plan::{AccessPlannedQuery, secondary_order_contract_is_deterministic},
    },
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{
    AccessWindow, IndexRangeLimitSpec, RouteCapabilities, RouteContinuationPlan, TopNSeekSpec,
    derive_budget_safety_flags,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Assess index-range limit pushdown once for this execution and produce
    // the bounded fetch spec when all eligibility gates pass.
    pub(in crate::db::executor::route) fn assess_index_range_limit_pushdown(
        plan: &AccessPlannedQuery<E::Key>,
        continuation: RouteContinuationPlan,
        probe_fetch_hint: Option<usize>,
        capabilities: RouteCapabilities,
    ) -> Option<IndexRangeLimitSpec> {
        let access_window = *continuation.fetch_access_window();
        let continuation_capabilities = continuation.capabilities();
        let (has_residual_filter, _, _) = derive_budget_safety_flags::<E, _>(plan);
        capabilities
            .index_range_limit_pushdown_shape_supported
            .then_some(())?;
        continuation_capabilities
            .index_range_limit_pushdown_allowed()
            .then_some(())?;
        let fetch = probe_fetch_hint.or_else(|| Self::bounded_window_fetch_hint(access_window))?;
        (!has_residual_filter || Self::residual_predicate_pushdown_fetch_is_safe(fetch))
            .then_some(IndexRangeLimitSpec { fetch })
    }

    // Shared load-page scan-budget hint gate.
    pub(in crate::db::executor::route) fn load_scan_budget_hint(
        plan: &AccessPlannedQuery<E::Key>,
        continuation: RouteContinuationPlan,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        let continuation_capabilities = continuation.capabilities();
        let access_window = *continuation.fetch_access_window();
        let fetch_hint = Self::bounded_window_fetch_hint(access_window);

        plan.access_strategy().load_window_early_stop_hint(
            continuation_capabilities.applied(),
            capabilities.stream_order_contract_safe,
            fetch_hint,
        )
    }

    // Build an explicit top-N seek contract for ordered load windows when
    // route eligibility permits bounded access traversal.
    pub(in crate::db::executor::route) fn top_n_seek_spec(
        plan: &AccessPlannedQuery<E::Key>,
        continuation: RouteContinuationPlan,
        capabilities: RouteCapabilities,
    ) -> Option<TopNSeekSpec> {
        let continuation_capabilities = continuation.capabilities();
        let logical = plan.scalar_plan();
        let has_order = logical
            .order
            .as_ref()
            .is_some_and(|order| !order.fields.is_empty());
        (logical.mode.is_load() && has_order).then_some(())?;
        secondary_order_contract_is_deterministic(E::MODEL, logical).then_some(())?;
        capabilities.stream_order_contract_safe.then_some(())?;
        (!continuation_capabilities.applied()).then_some(())?;

        let access_window = *continuation.fetch_access_window();

        Self::bounded_window_fetch_hint(access_window).map(TopNSeekSpec::new)
    }

    // Shared bounded-probe safety gate for aggregate key-stream hints.
    // Contract:
    // - DISTINCT + offset must remain unbounded so deduplication is applied
    //   before offset consumption without risking under-fetch.
    // - If dedup/projection/composite semantics evolve, this gate is the first
    //   place to re-evaluate bounded-probe correctness.
    pub(in crate::db::executor::route) fn bounded_probe_hint_is_safe(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> bool {
        let offset = usize::try_from(ExecutionKernel::effective_page_offset(plan, None))
            .unwrap_or(usize::MAX);
        let distinct_enabled = plan.scalar_plan().distinct;

        !(distinct_enabled && offset > 0)
    }

    // Residual predicates are allowed for index-range limit pushdown only when
    // the bounded fetch remains small. This caps amplification risk when the
    // post-access residual filter rejects many bounded candidates.
    pub(in crate::db::executor::route) const fn residual_predicate_pushdown_fetch_is_safe(
        fetch: usize,
    ) -> bool {
        fetch <= Self::residual_predicate_pushdown_fetch_cap()
    }

    pub(in crate::db::executor) const fn residual_predicate_pushdown_fetch_cap() -> usize {
        256
    }

    // Resolve one bounded fetch hint from one access window contract.
    // Zero-window contracts always project `Some(0)` so callers can preserve
    // deterministic empty-window scan budgeting.
    pub(in crate::db::executor::route) const fn bounded_window_fetch_hint(
        access_window: AccessWindow,
    ) -> Option<usize> {
        if access_window.is_zero_window() {
            return Some(0);
        }

        access_window.fetch_limit()
    }
}
