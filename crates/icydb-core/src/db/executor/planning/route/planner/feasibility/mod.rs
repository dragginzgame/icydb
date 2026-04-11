//! Module: executor::route::planner::feasibility
//! Responsibility: derive continuation/window/capability feasibility state.
//! Does not own: route-intent normalization or execution-mode selection.
//! Boundary: staged feasibility derivation for route planning.

mod gates;

use crate::db::{
    direction::Direction,
    executor::{
        AccessWindow,
        aggregate::AggregateKind,
        planning::continuation::ScalarContinuationContext,
        route::{
            AggregateRouteShape, AggregateSeekSpec, GroupedExecutionMode,
            GroupedExecutionModeProjection, RouteCapabilities, RouteContinuationPlan, ScanHintPlan,
            TopNSeekSpec, aggregate_probe_fetch_hint, aggregate_seek_spec,
            assess_index_range_limit_pushdown_for_model,
            capability::{
                count_pushdown_existing_rows_shape_supported,
                desc_physical_reverse_traversal_supported,
                index_range_limit_pushdown_shape_supported_for_model,
            },
            count_pushdown_fetch_hint, derive_aggregate_route_direction,
            derive_execution_capabilities_for_model, derive_load_route_direction,
            load_scan_budget_hint,
            planner::{
                RouteDerivationContext, RouteFeasibilityStage, RouteIntentStage,
                stages::{RouteCountPushdownState, RouteDerivationSupport},
            },
            top_n_seek_spec_for_model,
        },
    },
    query::plan::{AccessPlannedQuery, GroupedPlanStrategy, PlannerRouteProfile},
};

use crate::db::executor::planning::route::planner::feasibility::gates::{
    index_range_limit_pushdown_allowed_for_grouped, load_scan_hints_allowed_for_intent,
};

pub(in crate::db::executor::planning::route::planner) fn derive_execution_feasibility_stage_for_model(
    plan: &AccessPlannedQuery,
    continuation: &ScalarContinuationContext,
    probe_fetch_hint: Option<usize>,
    planner_route_profile: &PlannerRouteProfile,
    intent_stage: &RouteIntentStage<'_>,
) -> RouteFeasibilityStage {
    let continuation_policy = *planner_route_profile.continuation_policy();
    let route_continuation = continuation.route_continuation_plan(plan, continuation_policy);
    let derivation = derive_route_derivation_context_for_model(
        plan,
        intent_stage,
        planner_route_profile,
        route_continuation,
        probe_fetch_hint,
    );
    let kind = intent_stage.kind();
    let index_range_limit_pushdown_enabled =
        index_range_limit_pushdown_allowed_for_grouped(intent_stage.grouped);

    let index_range_limit_spec = index_range_limit_pushdown_enabled
        .then(|| {
            assess_index_range_limit_pushdown_for_model(
                plan,
                route_continuation,
                derivation.scan_hints.physical_fetch_hint,
                derivation
                    .support
                    .index_range_limit_pushdown_shape_supported,
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
                .support
                .index_range_limit_pushdown_shape_supported,
        "route invariant: index-range limit spec requires pushdown-eligible shape",
    );
    debug_assert!(
        !derivation.count_pushdown.eligible
            || kind.is_some_and(AggregateKind::is_count)
                && (derivation.capabilities.count_pushdown_shape_supported
                    || derivation.count_pushdown.existing_rows_shape_supported),
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
        route_continuation.strict_advance_required_when_applied(),
        "route invariant: continuation executions must require strict advancement",
    );
    debug_assert!(
        !intent_stage.grouped || route_continuation.grouped_safe_when_applied(),
        "route invariant: grouped continuation executions must satisfy planner-projected continuation policy safety",
    );

    RouteFeasibilityStage {
        continuation: route_continuation,
        derivation,
        index_range_limit_spec,
    }
}

pub(in crate::db::executor::planning::route::planner) fn derive_route_derivation_context_for_model(
    plan: &AccessPlannedQuery,
    intent_stage: &RouteIntentStage<'_>,
    planner_route_profile: &PlannerRouteProfile,
    continuation: RouteContinuationPlan,
    probe_fetch_hint: Option<usize>,
) -> RouteDerivationContext {
    // Derive the invariant route shape and capability snapshot first so the
    // later scan-hint and grouped-mode phases can stay focused on one concern.
    let aggregate_shape = intent_stage.aggregate_shape;
    let grouped = intent_stage.grouped;
    let grouped_plan_strategy = intent_stage.grouped_plan_strategy;
    let logical_pushdown_eligibility = planner_route_profile.logical_pushdown_eligibility();
    let secondary_pushdown_applicability =
        crate::db::executor::planning::route::derive_secondary_pushdown_applicability_from_contract(
            plan,
            planner_route_profile,
        );
    let direction = aggregate_shape.map_or_else(
        || derive_load_route_direction(plan),
        |aggregate| derive_aggregate_route_direction(plan, aggregate),
    );
    let (capabilities, support, count_pushdown) = derive_route_capability_state_for_model(
        plan,
        planner_route_profile,
        direction,
        aggregate_shape,
    );
    let kind: Option<AggregateKind> = aggregate_shape.map(AggregateRouteShape::kind);
    let load_scan_hints_enabled = load_scan_hints_allowed_for_intent(kind, grouped);
    let access_window = *continuation.keep_access_window();
    let (scan_hints, aggregate_physical_fetch_hint, aggregate_seek_spec, top_n_seek_spec) =
        derive_route_scan_hints_for_model(RouteScanHintInputs {
            plan,
            planner_route_profile,
            continuation,
            probe_fetch_hint,
            aggregate_shape,
            kind,
            access_window,
            load_scan_hints_enabled,
            direction,
            capabilities,
            support: &support,
            count_pushdown: &count_pushdown,
        });
    let grouped_execution_mode = derive_grouped_execution_mode_for_intent(
        grouped,
        grouped_plan_strategy,
        logical_pushdown_eligibility.grouped_aggregate_allowed(),
        direction,
        capabilities,
        support.desc_physical_reverse_supported,
    );

    RouteDerivationContext {
        direction,
        capabilities,
        support,
        count_pushdown,
        secondary_pushdown_applicability,
        scan_hints,
        top_n_seek_spec,
        aggregate_physical_fetch_hint,
        aggregate_seek_spec,
        grouped_execution_mode,
    }
}

// Derive the static route capability and COUNT shape state once before any
// scan-hint or grouped-mode decisions consume them.
fn derive_route_capability_state_for_model(
    plan: &AccessPlannedQuery,
    planner_route_profile: &PlannerRouteProfile,
    direction: Direction,
    aggregate_shape: Option<AggregateRouteShape<'_>>,
) -> (
    RouteCapabilities,
    RouteDerivationSupport,
    RouteCountPushdownState,
) {
    let access_class = plan.access_strategy().class();
    let existing_rows_shape_supported = count_pushdown_existing_rows_shape_supported(&access_class);
    let support = RouteDerivationSupport {
        desc_physical_reverse_supported: desc_physical_reverse_traversal_supported(plan, direction),
        index_range_limit_pushdown_shape_supported:
            index_range_limit_pushdown_shape_supported_for_model(plan, planner_route_profile),
    };
    let capabilities = derive_execution_capabilities_for_model(plan, direction, aggregate_shape);
    let count_pushdown = RouteCountPushdownState {
        existing_rows_shape_supported,
        eligible: aggregate_shape
            .map(AggregateRouteShape::kind)
            .is_some_and(|aggregate_kind| {
                aggregate_kind.is_count()
                    && (capabilities.count_pushdown_shape_supported
                        || existing_rows_shape_supported)
            }),
    };

    (capabilities, support, count_pushdown)
}

// Route scan hints and seek specs are derived together because they share the
// same access-window and capability inputs.
fn derive_route_scan_hints_for_model(
    inputs: RouteScanHintInputs<'_>,
) -> (
    ScanHintPlan,
    Option<usize>,
    Option<AggregateSeekSpec>,
    Option<TopNSeekSpec>,
) {
    let count_pushdown_probe_fetch_hint = inputs
        .count_pushdown
        .eligible
        .then(|| count_pushdown_fetch_hint(inputs.access_window, inputs.capabilities))
        .flatten();
    let aggregate_terminal_probe_fetch_hint = inputs.aggregate_shape.and_then(|aggregate| {
        aggregate_probe_fetch_hint(
            inputs.plan,
            aggregate,
            inputs.direction,
            inputs.support.desc_physical_reverse_supported,
            inputs.capabilities,
            inputs.access_window,
        )
    });
    let aggregate_seek_spec = inputs.aggregate_shape.and_then(|aggregate| {
        aggregate_seek_spec(
            inputs.plan,
            aggregate,
            inputs.direction,
            inputs.support.desc_physical_reverse_supported,
            inputs.capabilities,
            inputs.access_window,
        )
    });
    let aggregate_physical_fetch_hint =
        count_pushdown_probe_fetch_hint.or(aggregate_terminal_probe_fetch_hint);
    let top_n_seek_spec = inputs
        .load_scan_hints_enabled
        .then(|| {
            top_n_seek_spec_for_model(
                inputs.plan,
                inputs.planner_route_profile,
                inputs.continuation,
                inputs.capabilities,
            )
        })
        .flatten();
    let load_physical_fetch_hint = inputs
        .load_scan_hints_enabled
        .then_some(inputs.probe_fetch_hint)
        .flatten();
    let physical_fetch_hint = inputs
        .kind
        .map_or(load_physical_fetch_hint, |_| aggregate_physical_fetch_hint);
    let load_scan_budget_hint = inputs
        .load_scan_hints_enabled
        .then(|| load_scan_budget_hint(inputs.plan, inputs.continuation, inputs.capabilities))
        .flatten();

    (
        ScanHintPlan {
            physical_fetch_hint,
            load_scan_budget_hint,
        },
        aggregate_physical_fetch_hint,
        aggregate_seek_spec,
        top_n_seek_spec,
    )
}

///
/// RouteScanHintInputs
///
/// Bundles the immutable inputs shared by route scan-hint and seek-spec
/// derivation so the helper can stay phase-focused without a long parameter
/// list.
///

struct RouteScanHintInputs<'a> {
    plan: &'a AccessPlannedQuery,
    planner_route_profile: &'a PlannerRouteProfile,
    continuation: RouteContinuationPlan,
    probe_fetch_hint: Option<usize>,
    aggregate_shape: Option<AggregateRouteShape<'a>>,
    kind: Option<AggregateKind>,
    access_window: AccessWindow,
    load_scan_hints_enabled: bool,
    direction: Direction,
    capabilities: RouteCapabilities,
    support: &'a RouteDerivationSupport,
    count_pushdown: &'a RouteCountPushdownState,
}

// Grouped execution mode stays optional, but derive it behind one helper so
// the main feasibility function only coordinates phases.
fn derive_grouped_execution_mode_for_intent(
    grouped: bool,
    grouped_plan_strategy: Option<GroupedPlanStrategy>,
    grouped_aggregate_allowed: bool,
    direction: Direction,
    capabilities: RouteCapabilities,
    desc_physical_reverse_supported: bool,
) -> Option<GroupedExecutionMode> {
    grouped.then(|| {
        debug_assert!(
            grouped_plan_strategy.is_some(),
            "route invariant: grouped feasibility derivation requires planner-projected grouped strategy",
        );
        debug_assert!(
            grouped_aggregate_allowed,
            "route invariant: grouped feasibility derivation requires planner-projected grouped aggregate eligibility",
        );
        let planner_grouped_strategy = grouped_plan_strategy
            .expect("grouped feasibility derivation requires planner-projected grouped strategy");

        GroupedExecutionMode::from_planner_strategy(
            planner_grouped_strategy,
            GroupedExecutionModeProjection::from_route_inputs(
                direction,
                desc_physical_reverse_supported,
                capabilities
                    .load_order_route_contract
                    .allows_ordered_group_projection(),
            ),
        )
    })
}
