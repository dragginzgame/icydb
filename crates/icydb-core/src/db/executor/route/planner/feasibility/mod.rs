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
            pipeline::contracts::LoadExecutor,
            route::{
                RouteContinuationPlan, ScanHintPlan,
                planner::{RouteDerivationContext, RouteFeasibilityStage, RouteIntentStage},
            },
        },
        query::builder::AggregateExpr,
        query::plan::{
            AccessPlannedQuery, GroupedPlanStrategyHint, LogicalPushdownEligibility,
            PlannerRouteProfile,
        },
    },
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::planner::feasibility::gates::{
    index_range_limit_pushdown_allowed_for_grouped, load_scan_hints_allowed_for_intent,
};
use crate::db::executor::route::planner::feasibility::grouped_strategy::grouped_execution_strategy_for_runtime;

#[cfg(test)]
pub(in crate::db::executor) use grouped_strategy::grouped_ordered_runtime_revalidation_flag_count_guard;

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(in crate::db::executor::route::planner) fn derive_execution_feasibility_stage(
        plan: &AccessPlannedQuery<E::Key>,
        continuation: &ScalarContinuationContext,
        probe_fetch_hint: Option<usize>,
        planner_route_profile: &PlannerRouteProfile,
        intent_stage: &RouteIntentStage,
    ) -> RouteFeasibilityStage {
        let continuation_policy = *planner_route_profile.continuation_policy();
        let route_continuation =
            Self::derive_route_continuation(plan, continuation, continuation_policy);
        let continuation_capabilities = route_continuation.capabilities();
        let derivation = Self::derive_route_derivation_context(
            plan,
            intent_stage,
            planner_route_profile.logical_pushdown_eligibility(),
            route_continuation,
            probe_fetch_hint,
        );
        let kind = intent_stage.kind();
        let index_range_limit_pushdown_enabled =
            index_range_limit_pushdown_allowed_for_grouped(intent_stage.grouped);

        // COUNT fold-mode discipline: non-count pushdowns must not route COUNT
        // through non-COUNT streaming fast paths.
        let index_range_limit_spec = index_range_limit_pushdown_enabled
            .then(|| {
                Self::assess_index_range_limit_pushdown(
                    plan,
                    route_continuation,
                    derivation.scan_hints.physical_fetch_hint,
                    derivation.capabilities,
                )
            })
            .flatten();
        let _ = (kind.is_none() && !intent_stage.grouped)
            .then_some(())
            .and_then(|()| {
                index_range_limit_spec.zip(derivation.scan_hints.load_scan_budget_hint)
            })
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
                    Self::load_scan_budget_hint(
                        plan,
                        route_continuation,
                        derivation.capabilities
                    ),
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

    pub(in crate::db::executor::route::planner) fn derive_route_derivation_context(
        plan: &AccessPlannedQuery<E::Key>,
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
                E::MODEL,
                plan,
                logical_pushdown_eligibility,
            );
        let direction = aggregate_expr.map_or_else(
            || Self::derive_load_route_direction(plan),
            |aggregate| Self::derive_aggregate_route_direction(plan, aggregate),
        );
        let capabilities = Self::derive_execution_capabilities(plan, direction, aggregate_expr);
        let kind = aggregate_expr.map(AggregateExpr::kind);
        let count_pushdown_eligible = kind.is_some_and(|aggregate_kind| {
            Self::is_count_pushdown_eligible(aggregate_kind, capabilities)
        });
        let load_scan_hints_enabled = load_scan_hints_allowed_for_intent(kind, grouped);
        let keep_access_window = *continuation.keep_access_window();

        // Aggregate probes must not assume DESC physical reverse traversal
        // when the access shape cannot emit descending order natively.
        let count_pushdown_probe_fetch_hint = count_pushdown_eligible
            .then(|| Self::count_pushdown_fetch_hint(keep_access_window, capabilities))
            .flatten();
        let aggregate_terminal_probe_fetch_hint = aggregate_expr.and_then(|aggregate| {
            Self::aggregate_probe_fetch_hint(
                plan,
                aggregate,
                direction,
                capabilities,
                keep_access_window,
            )
        });
        let aggregate_seek_spec = aggregate_expr.and_then(|aggregate| {
            Self::aggregate_seek_spec(plan, aggregate, direction, capabilities, keep_access_window)
        });
        let aggregate_physical_fetch_hint =
            count_pushdown_probe_fetch_hint.or(aggregate_terminal_probe_fetch_hint);
        let top_n_seek_spec = load_scan_hints_enabled
            .then(|| Self::top_n_seek_spec(plan, continuation, capabilities))
            .flatten();
        let load_physical_fetch_hint = load_scan_hints_enabled
            .then_some(probe_fetch_hint)
            .flatten();
        let physical_fetch_hint =
            kind.map_or(load_physical_fetch_hint, |_| aggregate_physical_fetch_hint);
        let load_scan_budget_hint = load_scan_hints_enabled
            .then(|| Self::load_scan_budget_hint(plan, continuation, capabilities))
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

            // Planner strategy hint already captures grouped semantic policy
            // (including HAVING operator admissibility). Route feasibility only
            // revalidates physical/runtime capability constraints.
            grouped_execution_strategy_for_runtime(
                plan,
                planner_grouped_strategy_hint,
                direction,
                capabilities.desc_physical_reverse_supported,
                capabilities.stream_order_contract_safe,
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
}
