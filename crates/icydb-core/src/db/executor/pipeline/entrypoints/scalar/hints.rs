//! Module: db::executor::pipeline::entrypoints::scalar::hints
//! Responsibility: finalize scalar scan contracts before execution.
//! Does not own: route selection or scalar kernel execution itself.
//! Boundary: consumes prepared runtime and freezes terminal-specific scan decisions.

use crate::db::{
    executor::{
        ExecutionRoutePlan, ScalarContinuationContext,
        pipeline::entrypoints::scalar::runtime::PreparedScalarRouteRuntime,
        planning::route::top_n_seek_lookahead_required_for_shape,
        route::{
            ScanHintPlan, access_order_satisfied_by_route_mode,
            branch_set_page_keep_cap_shape_supported,
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
    KernelRows,
    MaterializedPage,
}

// Unpaged execution can omit page lookahead except where key normalization
// requires it. None leaves ordinary route hints unchanged.
fn unpaged_top_n_seek_fetch(
    route: &ExecutionRoutePlan,
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    unpaged_rows_mode: bool,
) -> Option<usize> {
    if !unpaged_rows_mode || continuation.cursor_boundary().is_some() {
        return None;
    }
    let spec = route.top_n_seek_spec()?;
    if !route.is_streaming() || !route.load_order_route_mode().allows_streaming_load() {
        return None;
    }
    if spec.fetch() == 0 {
        return Some(0);
    }
    let requires_lookahead = plan
        .access_shape_facts()
        .single_path_facts()
        .is_some_and(|shape| top_n_seek_lookahead_required_for_shape(&shape));
    if requires_lookahead {
        Some(spec.fetch())
    } else {
        route.continuation().keep_access_window().fetch_limit()
    }
}

// Finalize all scalar scan decisions together. The existing runtime bundle
// carries every knob, including exact-probe suppression; the execution spine
// consumes the returned route without patching its hint/spec copies again.
pub(super) fn finalize_scalar_route_for_execution(
    mut prepared: PreparedScalarRouteRuntime,
    terminal: &ScalarRouteTerminal,
) -> PreparedScalarRouteRuntime {
    let route = &prepared.route_plan;
    let plan = prepared.plan_core.plan();
    let top_n_fetch = unpaged_top_n_seek_fetch(
        route,
        plan,
        &prepared.continuation,
        prepared.unpaged_rows_mode,
    );
    let preserve_leaf_order = prepared.unpaged_rows_mode
        && prepared.continuation.cursor_boundary().is_none()
        && route.top_n_seek_spec().is_none()
        && route.index_leaf_order_policy().preserves_leaf_index_order()
        && route.scan_hints.physical_fetch_hint.is_none();
    let physical_fetch_hint = if prepared.suppress_route_scan_hints {
        None
    } else {
        top_n_fetch
            .or_else(|| preserve_leaf_order.then_some(usize::MAX))
            .or(route.scan_hints.physical_fetch_hint)
    };
    let physical_fetch_hint = if matches!(terminal, ScalarRouteTerminal::MaterializedPage) {
        index_set_page_fetch_hint(
            route,
            plan,
            &prepared.continuation,
            prepared.prep.effective_runtime_filter_program().is_some(),
            physical_fetch_hint,
        )
        .or(physical_fetch_hint)
    } else {
        physical_fetch_hint
    };
    let load_scan_budget_hint = if prepared.suppress_route_scan_hints {
        None
    } else {
        top_n_fetch.or(route.scan_hints.load_scan_budget_hint)
    };
    let exact_probe = prepared.enforced_scan_probe_limit.is_some();
    let index_range_limit_spec = if exact_probe {
        None
    } else {
        route.index_range_limit_spec.map(|mut spec| {
            spec.fetch = top_n_fetch.unwrap_or(spec.fetch);
            spec
        })
    };
    let scan_hints = ScanHintPlan {
        physical_fetch_hint: if exact_probe {
            None
        } else {
            physical_fetch_hint
        },
        load_scan_budget_hint: if exact_probe {
            None
        } else {
            load_scan_budget_hint
        },
        ..route.scan_hints
    };
    let top_n_seek_spec = if exact_probe {
        None
    } else {
        route.top_n_seek_spec
    };
    prepared.route_plan.scan_hints = scan_hints;
    prepared.route_plan.index_range_limit_spec = index_range_limit_spec;
    prepared.route_plan.top_n_seek_spec = top_n_seek_spec;
    prepared
}

fn index_set_page_fetch_hint(
    route_plan: &ExecutionRoutePlan,
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    residual_filter_present: bool,
    physical_fetch_hint: Option<usize>,
) -> Option<usize> {
    let access_shape_facts = plan.access_shape_facts();
    let single_path_facts = access_shape_facts.single_path_facts();
    let branch_set_page = single_path_facts
        .as_ref()
        .is_some_and(branch_set_page_keep_cap_shape_supported);
    let index_prefix_set_page = single_path_facts
        .as_ref()
        .is_some_and(index_prefix_set_page_fetch_hint_shape_supported);
    if physical_fetch_hint.is_some()
        || residual_filter_present
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
        || !continuation.can_bound_ordered_scan(plan)
    {
        return None;
    }

    let limit = plan.scalar_plan().page.as_ref()?.limit?;

    let fetch = if limit == 0 {
        0
    } else {
        continuation
            .keep_count_for_limit_window(plan, limit)
            .saturating_add(1)
    };
    Some(fetch)
}
