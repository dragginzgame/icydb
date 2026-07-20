//! Module: db::executor::pipeline::entrypoints::scalar::hints
//! Responsibility: final scalar route-plan hint normalization before execution.
//! Does not own: route selection or scalar kernel execution itself.
//! Boundary: mutates scan hints after route planning and before scalar execution.

use crate::db::{
    executor::{
        ExecutionPreparation, ExecutionRoutePlan, ScalarContinuationContext,
        planning::route::top_n_seek_lookahead_required_for_shape,
        route::{
            access_order_satisfied_by_route_mode, branch_set_page_keep_cap_shape_supported,
            index_prefix_set_page_fetch_hint_shape_supported,
        },
    },
    query::plan::AccessPlannedQuery,
};

///
/// ScalarRouteTerminal
///
/// ScalarRouteTerminal identifies the terminal-specific adjustment included in
/// the single final scalar route normalization pass. Materialized pages may
/// derive the safe index-set page hint; kernel-row sinks do not.
///

pub(super) enum ScalarRouteTerminal {
    #[cfg(feature = "sql")]
    KernelRows,
    MaterializedPage,
}

///
/// UnpagedLoadHintStrategy
///
/// Strategy selected once for unpaged scalar execution hinting so the route-plan
/// mutation phase applies one mechanical outcome.
///

enum UnpagedLoadHintStrategy {
    None,
    TopNSeekWindow { fetch: usize },
    PreserveOrderedIndexLeafStream,
}

impl UnpagedLoadHintStrategy {
    const fn resolve(
        resolved_continuation: &ScalarContinuationContext,
        unpaged_rows_mode: bool,
        top_n_seek_requires_lookahead: bool,
        route_plan: &ExecutionRoutePlan,
    ) -> Self {
        if !unpaged_rows_mode || resolved_continuation.cursor_boundary().is_some() {
            return Self::None;
        }

        if let Some(top_n_seek_spec) = route_plan.top_n_seek_spec() {
            if !route_plan.is_streaming()
                || !route_plan.load_order_route_mode().allows_streaming_load()
            {
                return Self::None;
            }

            let fetch = if top_n_seek_spec.fetch() == 0 {
                0
            } else if !top_n_seek_requires_lookahead {
                let Some(fetch) = route_plan.continuation().keep_access_window().fetch_limit()
                else {
                    return Self::None;
                };

                fetch
            } else {
                // Deduplicating lookup shapes need one extra lookahead row to
                // preserve parity after key normalization before windowing.
                top_n_seek_spec.fetch()
            };

            return Self::TopNSeekWindow { fetch };
        }

        if route_plan
            .index_leaf_order_policy()
            .preserves_leaf_index_order()
            && route_plan.scan_hints.physical_fetch_hint.is_none()
        {
            return Self::PreserveOrderedIndexLeafStream;
        }

        Self::None
    }

    const fn apply(self, route_plan: &mut ExecutionRoutePlan) {
        match self {
            Self::None => {}
            Self::TopNSeekWindow { fetch } => {
                route_plan.scan_hints.physical_fetch_hint = Some(fetch);
                route_plan.scan_hints.load_scan_budget_hint = Some(fetch);
            }
            Self::PreserveOrderedIndexLeafStream => {
                route_plan.scan_hints.physical_fetch_hint = Some(usize::MAX);
            }
        }
    }
}

// Unpaged `execute()` does not need continuation lookahead rows. For
// route-eligible top-N seek windows, constrain both access probe and load
// scan-budget hints to the keep-count window (without continuation +1).
const fn apply_unpaged_top_n_seek_hints(
    resolved_continuation: &ScalarContinuationContext,
    unpaged_rows_mode: bool,
    top_n_seek_requires_lookahead: bool,
    route_plan: &mut ExecutionRoutePlan,
) {
    let strategy = UnpagedLoadHintStrategy::resolve(
        resolved_continuation,
        unpaged_rows_mode,
        top_n_seek_requires_lookahead,
        route_plan,
    );

    strategy.apply(route_plan);
}

// Apply every terminal-local route mutation once after route selection and
// immediately before scalar execution consumes the plan.
pub(super) fn normalize_scalar_route_for_execution(
    route_plan: &mut ExecutionRoutePlan,
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    unpaged_rows_mode: bool,
    suppress_route_scan_hints: bool,
    terminal: ScalarRouteTerminal,
    execution_preparation: &ExecutionPreparation,
) {
    let top_n_seek_requires_lookahead = plan
        .access_shape_facts()
        .single_path_facts()
        .is_some_and(|shape_facts| top_n_seek_lookahead_required_for_shape(&shape_facts));
    apply_unpaged_top_n_seek_hints(
        continuation,
        unpaged_rows_mode,
        top_n_seek_requires_lookahead,
        route_plan,
    );

    if suppress_route_scan_hints {
        route_plan.scan_hints.physical_fetch_hint = None;
        route_plan.scan_hints.load_scan_budget_hint = None;
    }

    if matches!(terminal, ScalarRouteTerminal::MaterializedPage) {
        apply_index_set_page_fetch_hint(
            route_plan,
            plan,
            continuation,
            execution_preparation
                .effective_runtime_filter_program()
                .is_some(),
        );
    }
}

fn apply_index_set_page_fetch_hint(
    route_plan: &mut ExecutionRoutePlan,
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    residual_filter_present: bool,
) {
    let access_shape_facts = plan.access_shape_facts();
    let single_path_facts = access_shape_facts.single_path_facts();
    let branch_set_page = single_path_facts
        .as_ref()
        .is_some_and(branch_set_page_keep_cap_shape_supported);
    let index_prefix_set_page = single_path_facts
        .as_ref()
        .is_some_and(index_prefix_set_page_fetch_hint_shape_supported);
    if route_plan.scan_hints.physical_fetch_hint.is_some()
        || residual_filter_present
        || !cursor_fetch_hint_safe(route_plan, plan, continuation)
        || !index_prefix_set_page
        || !plan.scalar_plan().mode.is_load()
        || plan.scalar_plan().distinct
        || (branch_set_page
            && plan
                .scalar_plan()
                .order
                .as_ref()
                .is_none_or(|order| order.fields.is_empty()))
        || !access_order_satisfied_by_route_mode(plan)
        || !route_plan.load_order_route_mode().allows_streaming_load()
    {
        return;
    }

    let Some(limit) = plan.scalar_plan().page.as_ref().and_then(|page| page.limit) else {
        return;
    };

    let fetch = if limit == 0 {
        0
    } else {
        continuation
            .keep_count_for_limit_window(plan, limit)
            .saturating_add(1)
    };
    route_plan.scan_hints.physical_fetch_hint = Some(fetch);
}

fn cursor_fetch_hint_safe(
    route_plan: &ExecutionRoutePlan,
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
) -> bool {
    if !continuation.has_cursor_boundary() {
        return true;
    }

    let access_continuation = continuation.access_scan_input(route_plan.direction(), plan);
    access_continuation.primary_key_boundary().is_some()
        || access_continuation
            .index_scan_continuation()
            .anchor()
            .is_some()
}
