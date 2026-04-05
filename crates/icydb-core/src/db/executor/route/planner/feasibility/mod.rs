//! Module: executor::route::planner::feasibility
//! Responsibility: derive continuation/window/capability feasibility state.
//! Does not own: route-intent normalization or execution-mode selection.
//! Boundary: staged feasibility derivation for route planning.

mod gates;
mod grouped_strategy;

use crate::{
    db::{
        executor::{
            aggregate::AggregateKind,
            continuation::ScalarContinuationContext,
            route::{
                RouteContinuationPlan, ScanHintPlan, aggregate_probe_fetch_hint_for_model,
                aggregate_seek_spec_for_model, assess_index_range_limit_pushdown_for_model,
                count_pushdown_fetch_hint, derive_aggregate_route_direction,
                derive_execution_capabilities_for_model, derive_load_route_direction,
                load_scan_budget_hint,
                planner::{RouteDerivationContext, RouteFeasibilityStage, RouteIntentStage},
                top_n_seek_spec_for_model,
            },
        },
        query::builder::AggregateExpr,
        query::plan::{
            AccessPlannedQuery, GroupedPlanStrategyHint, LogicalPushdownEligibility,
            PlannerRouteProfile,
        },
    },
    model::entity::EntityModel,
};

use crate::db::executor::route::planner::feasibility::gates::{
    index_range_limit_pushdown_allowed_for_grouped, load_scan_hints_allowed_for_intent,
};
use crate::db::executor::route::planner::feasibility::grouped_strategy::grouped_execution_strategy_for_runtime;

pub(in crate::db::executor::route::planner) fn derive_execution_feasibility_stage_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    probe_fetch_hint: Option<usize>,
    planner_route_profile: &PlannerRouteProfile,
    intent_stage: &RouteIntentStage,
) -> RouteFeasibilityStage {
    let continuation_policy = *planner_route_profile.continuation_policy();
    let route_continuation = continuation.route_continuation_plan(plan, continuation_policy);
    let continuation_capabilities = route_continuation.capabilities();
    let derivation = derive_route_derivation_context_for_model(
        model,
        plan,
        intent_stage,
        planner_route_profile.logical_pushdown_eligibility(),
        route_continuation,
        probe_fetch_hint,
    );
    let kind = intent_stage.kind();
    let index_range_limit_pushdown_enabled =
        index_range_limit_pushdown_allowed_for_grouped(intent_stage.grouped);

    let index_range_limit_spec = index_range_limit_pushdown_enabled
        .then(|| {
            assess_index_range_limit_pushdown_for_model(
                model,
                plan,
                route_continuation,
                derivation.scan_hints.physical_fetch_hint,
                derivation.capabilities,
            )
        })
        .flatten();
    let _ = (kind.is_none() && !intent_stage.grouped)
        .then_some(())
        .and_then(|()| index_range_limit_spec.zip(derivation.scan_hints.load_scan_budget_hint))
        .inspect(|(index_range_limit_spec, load_scan_budget_hint)| {
            debug_assert_eq!(
                index_range_limit_spec.fetch, *load_scan_budget_hint,
                "route invariant: load index-range fetch hint and load scan budget must remain aligned"
            );
        });
    debug_assert!(
        index_range_limit_spec.is_none()
            || derivation
                .capabilities
                .index_range_limit_pushdown_shape_supported,
        "route invariant: index-range limit spec requires pushdown-eligible shape",
    );
    debug_assert!(
        !derivation.count_pushdown_eligible
            || kind.is_some_and(AggregateKind::is_count)
                && (derivation.capabilities.count_pushdown_shape_supported
                    || derivation
                        .capabilities
                        .count_pushdown_existing_rows_shape_supported),
        "route invariant: COUNT pushdown eligibility must match COUNT-safe capability set",
    );
    let _ = (kind.is_none() && !intent_stage.grouped)
        .then_some(())
        .inspect(|()| {
            debug_assert_eq!(
                derivation.scan_hints.load_scan_budget_hint,
                load_scan_budget_hint(plan, route_continuation, derivation.capabilities),
                "route invariant: load scan-budget hints must match access-strategy early-stop contract",
            );
        });
    debug_assert!(
        !intent_stage.grouped
            || derivation.scan_hints.load_scan_budget_hint.is_none()
                && derivation.scan_hints.physical_fetch_hint.is_none()
                && derivation.top_n_seek_spec.is_none()
                && index_range_limit_spec.is_none(),
        "route invariant: grouped intent must not derive load/aggregate scan hints or index-range pushdown specs",
    );
    debug_assert!(
        continuation_capabilities.strict_advance_required_when_applied(),
        "route invariant: continuation executions must require strict advancement",
    );
    debug_assert!(
        !intent_stage.grouped || continuation_capabilities.grouped_safe_when_applied(),
        "route invariant: grouped continuation executions must satisfy planner-projected continuation policy safety",
    );

    RouteFeasibilityStage {
        continuation: route_continuation,
        derivation,
        index_range_limit_spec,
    }
}

pub(in crate::db::executor::route::planner) fn derive_route_derivation_context_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    intent_stage: &RouteIntentStage,
    logical_pushdown_eligibility: LogicalPushdownEligibility,
    continuation: RouteContinuationPlan,
    probe_fetch_hint: Option<usize>,
) -> RouteDerivationContext {
    let aggregate_expr = intent_stage.aggregate_expr.as_ref();
    let grouped = intent_stage.grouped;
    let grouped_plan_strategy_hint = intent_stage.grouped_plan_strategy_hint;
    let secondary_pushdown_applicability =
        crate::db::executor::route::derive_secondary_pushdown_applicability_from_contract(
            model,
            plan,
            logical_pushdown_eligibility,
        );
    let direction = aggregate_expr.map_or_else(
        || derive_load_route_direction(plan),
        |aggregate| derive_aggregate_route_direction(plan, aggregate),
    );
    let capabilities =
        derive_execution_capabilities_for_model(model, plan, direction, aggregate_expr);
    let kind = aggregate_expr.map(AggregateExpr::kind);
    let count_pushdown_eligible = kind.is_some_and(|aggregate_kind| {
        aggregate_kind.is_count()
            && (capabilities.count_pushdown_shape_supported
                || capabilities.count_pushdown_existing_rows_shape_supported)
    });
    let load_scan_hints_enabled = load_scan_hints_allowed_for_intent(kind, grouped);
    let keep_access_window = *continuation.keep_access_window();

    let count_pushdown_probe_fetch_hint = count_pushdown_eligible
        .then(|| count_pushdown_fetch_hint(keep_access_window, capabilities))
        .flatten();
    let aggregate_terminal_probe_fetch_hint = aggregate_expr.and_then(|aggregate| {
        aggregate_probe_fetch_hint_for_model(
            model,
            plan,
            aggregate,
            direction,
            capabilities,
            keep_access_window,
        )
    });
    let aggregate_seek_spec = aggregate_expr.and_then(|aggregate| {
        aggregate_seek_spec_for_model(
            model,
            plan,
            aggregate,
            direction,
            capabilities,
            keep_access_window,
        )
    });
    let aggregate_physical_fetch_hint =
        count_pushdown_probe_fetch_hint.or(aggregate_terminal_probe_fetch_hint);
    let top_n_seek_spec = load_scan_hints_enabled
        .then(|| top_n_seek_spec_for_model(model, plan, continuation, capabilities))
        .flatten();
    let load_physical_fetch_hint = load_scan_hints_enabled
        .then_some(probe_fetch_hint)
        .flatten();
    let physical_fetch_hint =
        kind.map_or(load_physical_fetch_hint, |_| aggregate_physical_fetch_hint);
    let load_scan_budget_hint = load_scan_hints_enabled
        .then(|| load_scan_budget_hint(plan, continuation, capabilities))
        .flatten();
    let grouped_execution_strategy = grouped.then(|| {
        debug_assert!(
            grouped_plan_strategy_hint.is_some(),
            "route invariant: grouped feasibility derivation requires planner-projected grouped strategy hint",
        );
        debug_assert!(
            logical_pushdown_eligibility.grouped_aggregate_allowed(),
            "route invariant: grouped feasibility derivation requires planner-projected grouped aggregate eligibility",
        );
        let planner_grouped_strategy_hint =
            grouped_plan_strategy_hint.unwrap_or(GroupedPlanStrategyHint::HashGroup);

        grouped_execution_strategy_for_runtime(
            plan,
            planner_grouped_strategy_hint,
            direction,
            capabilities.desc_physical_reverse_supported,
            capabilities.load_order_route_contract.allows_streaming_load(),
        )
    });

    RouteDerivationContext {
        direction,
        capabilities,
        secondary_pushdown_applicability,
        scan_hints: ScanHintPlan {
            physical_fetch_hint,
            load_scan_budget_hint,
        },
        top_n_seek_spec,
        count_pushdown_eligible,
        aggregate_physical_fetch_hint,
        aggregate_seek_spec,
        grouped_execution_strategy,
    }
}
