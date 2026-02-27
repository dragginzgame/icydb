use crate::{
    db::{
        cursor::CursorBoundary,
        direction::Direction,
        executor::{
            ExecutionKernel, RangeToken,
            aggregate::{AggregateKind, AggregateSpec},
            load::LoadExecutor,
        },
        query::plan::AccessPlannedQuery,
    },
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{
    ContinuationMode, IndexRangeLimitSpec, RouteCapabilities, RouteWindowPlan,
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
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RangeToken>,
        route_window: RouteWindowPlan,
        probe_fetch_hint: Option<usize>,
        capabilities: RouteCapabilities,
    ) -> Option<IndexRangeLimitSpec> {
        if !capabilities.index_range_limit_pushdown_shape_eligible {
            return None;
        }
        if cursor_boundary.is_some() && index_range_anchor.is_none() {
            return None;
        }
        if let Some(fetch) = probe_fetch_hint {
            if plan.predicate.is_some() && !Self::residual_predicate_pushdown_fetch_is_safe(fetch) {
                return None;
            }

            return Some(IndexRangeLimitSpec { fetch });
        }

        let page = plan.page.as_ref()?;
        let limit = page.limit?;
        if limit == 0 {
            return Some(IndexRangeLimitSpec { fetch: 0 });
        }

        let fetch = Self::page_window_fetch_count(route_window, true)?;
        if plan.predicate.is_some() && !Self::residual_predicate_pushdown_fetch_is_safe(fetch) {
            return None;
        }

        Some(IndexRangeLimitSpec { fetch })
    }

    // Shared load-page scan-budget hint gate.
    pub(super) const fn load_scan_budget_hint(
        continuation_mode: ContinuationMode,
        route_window: RouteWindowPlan,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if !matches!(continuation_mode, ContinuationMode::Initial) {
            return None;
        }
        if !capabilities.streaming_access_shape_safe {
            return None;
        }

        Self::page_window_fetch_count(route_window, true)
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
        !(plan.distinct && offset > 0)
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

    pub(super) fn count_pushdown_fetch_hint(
        plan: &AccessPlannedQuery<E::Key>,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if !capabilities.bounded_probe_hint_safe {
            return None;
        }

        let route_window = Self::derive_route_window(plan, None);
        Self::page_window_fetch_count(route_window, false)
    }

    pub(super) fn aggregate_probe_fetch_hint(
        plan: &AccessPlannedQuery<E::Key>,
        spec: &AggregateSpec,
        direction: Direction,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if spec.target_field().is_some() {
            return None;
        }
        let kind = spec.kind();
        if !matches!(
            kind,
            AggregateKind::Exists
                | AggregateKind::Min
                | AggregateKind::Max
                | AggregateKind::First
                | AggregateKind::Last
        ) {
            return None;
        }
        if plan.page.as_ref().is_some_and(|page| page.limit == Some(0)) {
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

        let offset = usize::try_from(ExecutionKernel::effective_page_offset(plan, None))
            .unwrap_or(usize::MAX);
        let page_limit = plan
            .page
            .as_ref()
            .and_then(|page| page.limit)
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

        match kind {
            AggregateKind::Exists | AggregateKind::First => Some(offset.saturating_add(1)),
            AggregateKind::Min if direction == Direction::Asc => Some(offset.saturating_add(1)),
            AggregateKind::Max if direction == Direction::Desc => Some(offset.saturating_add(1)),
            AggregateKind::Last => page_limit.map(|limit| offset.saturating_add(limit)),
            _ => None,
        }
    }

    // Shared page-window fetch computation for bounded routing hints.
    pub(super) const fn page_window_fetch_count(
        route_window: RouteWindowPlan,
        needs_extra: bool,
    ) -> Option<usize> {
        route_window.fetch_count_for(needs_extra)
    }
}
