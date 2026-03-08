//! Module: db::executor::route::hints
//! Responsibility: route-owned bounded-fetch and scan-budget hint derivation.
//! Does not own: route capability derivation or dispatch execution.
//! Boundary: emits optional hints consumed by stream/runtime surfaces.

use crate::{
    db::{
        direction::Direction,
        executor::{
            ExecutionKernel,
            load::LoadExecutor,
            route::{AggregateSeekSpec, TopNSeekSpec},
        },
        query::builder::AggregateExpr,
        query::plan::{AccessPlannedQuery, AggregateKind},
    },
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{
    AccessWindow, IndexRangeLimitSpec, RouteCapabilities, RouteContinuationPlan,
    aggregate_bounded_probe_fetch_hint, aggregate_supports_bounded_probe_hint,
    derive_budget_safety_flags, direction_allows_physical_fetch_hint,
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
        let access_window = *continuation.fetch_access_window();
        let continuation_capabilities = continuation.capabilities();
        let (has_residual_filter, _, _) = derive_budget_safety_flags::<E, _>(plan);
        if !capabilities.index_range_limit_pushdown_shape_eligible {
            return None;
        }
        if !continuation_capabilities.index_range_limit_pushdown_allowed() {
            return None;
        }
        let fetch = probe_fetch_hint.or_else(|| Self::bounded_window_fetch_hint(access_window))?;
        if has_residual_filter && !Self::residual_predicate_pushdown_fetch_is_safe(fetch) {
            return None;
        }

        Some(IndexRangeLimitSpec { fetch })
    }

    // Shared load-page scan-budget hint gate.
    pub(super) fn load_scan_budget_hint(
        plan: &AccessPlannedQuery<E::Key>,
        continuation: RouteContinuationPlan,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        let continuation_capabilities = continuation.capabilities();
        let access_window = *continuation.fetch_access_window();
        let fetch_hint = Self::bounded_window_fetch_hint(access_window);

        plan.access_strategy().load_window_early_stop_hint(
            continuation_capabilities.applied(),
            capabilities.streaming_access_shape_safe,
            fetch_hint,
        )
    }

    // Build an explicit top-N seek contract for ordered load windows when
    // route eligibility permits bounded access traversal.
    pub(super) fn top_n_seek_spec(
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
        if !logical.mode.is_load() || !has_order {
            return None;
        }
        if !capabilities.streaming_access_shape_safe {
            return None;
        }
        if continuation_capabilities.applied() {
            return None;
        }

        let access_window = *continuation.fetch_access_window();

        Self::bounded_window_fetch_hint(access_window).map(TopNSeekSpec::new)
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
        let distinct_enabled = plan.scalar_plan().distinct;

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

    // Resolve one bounded fetch hint from one access window contract.
    // Zero-window contracts always project `Some(0)` so callers can preserve
    // deterministic empty-window scan budgeting.
    const fn bounded_window_fetch_hint(access_window: AccessWindow) -> Option<usize> {
        if access_window.is_zero_window() {
            return Some(0);
        }

        access_window.fetch_limit()
    }

    pub(super) const fn count_pushdown_fetch_hint(
        access_window: AccessWindow,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if !capabilities.bounded_probe_hint_safe {
            return None;
        }

        Self::bounded_window_fetch_hint(access_window)
    }

    pub(super) fn aggregate_probe_fetch_hint(
        plan: &AccessPlannedQuery<E::Key>,
        aggregate: &AggregateExpr,
        direction: Direction,
        capabilities: RouteCapabilities,
        access_window: AccessWindow,
    ) -> Option<usize> {
        let kind = aggregate.kind();
        // Field-target extrema probe hints require deterministic tie coverage.
        // MIN(field) ASC can always short-circuit on the first existing row.
        // MAX(field) DESC can short-circuit only when ties are impossible.
        if aggregate.target_field().is_some() {
            if matches!((kind, direction), (AggregateKind::Min, Direction::Asc)) {
                if !capabilities.field_min_fast_path_eligible {
                    return None;
                }
            } else if matches!((kind, direction), (AggregateKind::Max, Direction::Desc)) {
                if !capabilities.field_max_fast_path_eligible {
                    return None;
                }
                if !Self::field_target_max_probe_shape_is_tie_free(plan, aggregate) {
                    return None;
                }
            } else {
                return None;
            }
        }
        if !aggregate_supports_bounded_probe_hint(kind) {
            return None;
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

        if access_window.is_zero_window() {
            return Some(0);
        }
        let offset = access_window.lower_bound();
        let page_limit = access_window
            .page_limit()
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

        aggregate_bounded_probe_fetch_hint(kind, direction, offset, page_limit)
    }

    // Build an explicit aggregate seek contract when bounded aggregate probe
    // hints are eligible for one extrema terminal shape.
    pub(super) fn aggregate_seek_spec(
        plan: &AccessPlannedQuery<E::Key>,
        aggregate: &AggregateExpr,
        direction: Direction,
        capabilities: RouteCapabilities,
        access_window: AccessWindow,
    ) -> Option<AggregateSeekSpec> {
        if !aggregate.kind().is_extrema() {
            return None;
        }
        let fetch = Self::aggregate_probe_fetch_hint(
            plan,
            aggregate,
            direction,
            capabilities,
            access_window,
        )?;

        Some(match direction {
            Direction::Asc => AggregateSeekSpec::First { fetch },
            Direction::Desc => AggregateSeekSpec::Last { fetch },
        })
    }

    // Field-target MAX probe hints are safe only when the chosen path guarantees
    // no duplicate target values can appear in traversal order.
    fn field_target_max_probe_shape_is_tie_free(
        plan: &AccessPlannedQuery<E::Key>,
        aggregate: &AggregateExpr,
    ) -> bool {
        let Some(target_field) = aggregate.target_field() else {
            return false;
        };

        let access_class = plan.access_strategy().class();
        let index_model = access_class
            .single_path_index_prefix_details()
            .or_else(|| access_class.single_path_index_range_details())
            .map(|(index, _)| index);

        Self::is_tie_free_probe_target(target_field, index_model)
    }

    // One canonical tie-free target guard for bounded MAX(field) probe hints.
    // Tie-free means:
    // - target is primary key, or
    // - target is backed by a unique single-field leading index.
    fn is_tie_free_probe_target(
        target_field: &str,
        index_model: Option<crate::model::index::IndexModel>,
    ) -> bool {
        if target_field == E::MODEL.primary_key.name {
            return true;
        }

        let Some(index_model) = index_model else {
            return false;
        };

        index_model.is_unique()
            && index_model.fields().len() == 1
            && index_model
                .fields()
                .first()
                .is_some_and(|field| *field == target_field)
    }
}
