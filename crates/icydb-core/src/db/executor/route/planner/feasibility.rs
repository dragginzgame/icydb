//! Module: executor::route::planner::feasibility
//! Responsibility: derive continuation/window/capability feasibility state.
//! Does not own: route-intent normalization or execution-mode selection.
//! Boundary: staged feasibility derivation for route planning.

use crate::{
    db::{
        direction::Direction,
        executor::{
            aggregate::AggregateKind,
            continuation::ScalarContinuationContext,
            load::LoadExecutor,
            route::{
                AggregateSeekSpec, GroupedExecutionStrategy, RouteContinuationPlan, ScanHintPlan,
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

///
/// IndexRangeLimitGateReason
///
/// Route-owned reasons why index-range limit pushdown derivation is skipped.
/// Keeps feasibility policy explicit and additive as route rules evolve.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IndexRangeLimitGateReason {
    CountTerminalIntent,
    GroupedIntent,
}

///
/// IndexRangeLimitGateContext
///
/// Minimal policy context for index-range limit pushdown pre-gates.
/// This context is intentionally pure and independent from execution mechanics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IndexRangeLimitGateContext {
    count_terminal: bool,
    grouped: bool,
}

impl IndexRangeLimitGateContext {
    #[must_use]
    const fn new(count_terminal: bool, grouped: bool) -> Self {
        Self {
            count_terminal,
            grouped,
        }
    }
}

///
/// IndexRangeLimitFeasibilityRule
///
/// Declarative feasibility rule for index-range pushdown pre-gates.
/// Rules are evaluated in order, and first violation short-circuits derivation.
///

#[derive(Clone, Copy)]
struct IndexRangeLimitFeasibilityRule {
    reason: IndexRangeLimitGateReason,
    violated: fn(IndexRangeLimitGateContext) -> bool,
}

impl IndexRangeLimitFeasibilityRule {
    #[must_use]
    const fn new(
        reason: IndexRangeLimitGateReason,
        violated: fn(IndexRangeLimitGateContext) -> bool,
    ) -> Self {
        Self { reason, violated }
    }
}

const INDEX_RANGE_LIMIT_FEASIBILITY_RULES: &[IndexRangeLimitFeasibilityRule] = &[
    IndexRangeLimitFeasibilityRule::new(
        IndexRangeLimitGateReason::CountTerminalIntent,
        index_range_limit_gate_count_terminal_violated,
    ),
    IndexRangeLimitFeasibilityRule::new(
        IndexRangeLimitGateReason::GroupedIntent,
        index_range_limit_gate_grouped_violated,
    ),
];

const fn index_range_limit_gate_count_terminal_violated(ctx: IndexRangeLimitGateContext) -> bool {
    ctx.count_terminal
}

const fn index_range_limit_gate_grouped_violated(ctx: IndexRangeLimitGateContext) -> bool {
    ctx.grouped
}

fn index_range_limit_gate_rejection(
    ctx: IndexRangeLimitGateContext,
) -> Option<IndexRangeLimitGateReason> {
    for rule in INDEX_RANGE_LIMIT_FEASIBILITY_RULES {
        if (rule.violated)(ctx) {
            return Some(rule.reason);
        }
    }

    None
}

///
/// LoadScanHintGateReason
///
/// Route-owned reasons why load-bound scan hints are suppressed.
/// Applies to load probe fetch hints and load scan-budget hints uniformly.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LoadScanHintGateReason {
    AggregateIntent,
    GroupedIntent,
}

///
/// LoadScanHintGateContext
///
/// Pure policy context for load-bound scan hint eligibility.
/// This remains independent from hint derivation mechanics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LoadScanHintGateContext {
    has_aggregate: bool,
    grouped: bool,
}

impl LoadScanHintGateContext {
    #[must_use]
    const fn new(has_aggregate: bool, grouped: bool) -> Self {
        Self {
            has_aggregate,
            grouped,
        }
    }
}

///
/// LoadScanHintFeasibilityRule
///
/// Declarative feasibility rule for load-bound scan hints.
/// Rules are evaluated in order, and first violation short-circuits hint derivation.
///

#[derive(Clone, Copy)]
struct LoadScanHintFeasibilityRule {
    reason: LoadScanHintGateReason,
    violated: fn(LoadScanHintGateContext) -> bool,
}

impl LoadScanHintFeasibilityRule {
    #[must_use]
    const fn new(
        reason: LoadScanHintGateReason,
        violated: fn(LoadScanHintGateContext) -> bool,
    ) -> Self {
        Self { reason, violated }
    }
}

const LOAD_SCAN_HINT_FEASIBILITY_RULES: &[LoadScanHintFeasibilityRule] = &[
    LoadScanHintFeasibilityRule::new(
        LoadScanHintGateReason::AggregateIntent,
        load_scan_hint_gate_aggregate_intent_violated,
    ),
    LoadScanHintFeasibilityRule::new(
        LoadScanHintGateReason::GroupedIntent,
        load_scan_hint_gate_grouped_intent_violated,
    ),
];

const fn load_scan_hint_gate_aggregate_intent_violated(ctx: LoadScanHintGateContext) -> bool {
    ctx.has_aggregate
}

const fn load_scan_hint_gate_grouped_intent_violated(ctx: LoadScanHintGateContext) -> bool {
    ctx.grouped
}

fn load_scan_hint_gate_rejection(ctx: LoadScanHintGateContext) -> Option<LoadScanHintGateReason> {
    for rule in LOAD_SCAN_HINT_FEASIBILITY_RULES {
        if (rule.violated)(ctx) {
            return Some(rule.reason);
        }
    }

    None
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(in crate::db::executor::route::planner) fn derive_route_feasibility_stage(
        plan: &AccessPlannedQuery<E::Key>,
        continuation: &ScalarContinuationContext,
        probe_fetch_hint: Option<usize>,
        planner_route_profile: &PlannerRouteProfile,
        intent_stage: &RouteIntentStage,
    ) -> RouteFeasibilityStage {
        let continuation_policy = *planner_route_profile.continuation_policy();
        let route_continuation =
            Self::derive_route_continuation(plan, continuation, continuation_policy);
        let derivation = Self::derive_route_derivation_context(
            plan,
            intent_stage,
            planner_route_profile.logical_pushdown_eligibility(),
            route_continuation,
            probe_fetch_hint,
        );
        let kind = intent_stage.kind();
        let count_terminal = kind.is_some_and(AggregateKind::is_count);
        let index_range_limit_gate =
            IndexRangeLimitGateContext::new(count_terminal, intent_stage.grouped);
        let index_range_limit_gate_rejection =
            index_range_limit_gate_rejection(index_range_limit_gate);

        // COUNT fold-mode discipline: non-count pushdowns must not route COUNT
        // through non-COUNT streaming fast paths.
        let index_range_limit_spec = if index_range_limit_gate_rejection.is_some() {
            None
        } else {
            Self::assess_index_range_limit_pushdown(
                plan,
                route_continuation,
                derivation.scan_hints.physical_fetch_hint,
                derivation.capabilities,
            )
        };
        if kind.is_none()
            && !intent_stage.grouped
            && let (Some(index_range_limit_spec), Some(load_scan_budget_hint)) = (
                index_range_limit_spec,
                derivation.scan_hints.load_scan_budget_hint,
            )
        {
            debug_assert_eq!(
                index_range_limit_spec.fetch, load_scan_budget_hint,
                "route invariant: load index-range fetch hint and load scan budget must remain aligned"
            );
        }
        debug_assert!(
            index_range_limit_spec.is_none()
                || derivation
                    .capabilities
                    .index_range_limit_pushdown_shape_eligible,
            "route invariant: index-range limit spec requires pushdown-eligible shape",
        );
        debug_assert!(
            !derivation.count_pushdown_eligible
                || kind.is_some_and(AggregateKind::is_count)
                    && derivation.capabilities.streaming_access_shape_safe
                    && derivation
                        .capabilities
                        .count_pushdown_access_shape_supported,
            "route invariant: COUNT pushdown eligibility must match COUNT-safe capability set",
        );
        if kind.is_none() && !intent_stage.grouped {
            debug_assert_eq!(
                derivation.scan_hints.load_scan_budget_hint,
                plan.access_strategy().load_window_early_stop_hint(
                    route_continuation.applied(),
                    derivation.capabilities.streaming_access_shape_safe,
                    route_continuation.window().fetch_count_for(true),
                ),
                "route invariant: load scan-budget hints must match access-strategy early-stop contract",
            );
        }
        debug_assert!(
            !intent_stage.grouped
                || derivation.scan_hints.load_scan_budget_hint.is_none()
                    && derivation.scan_hints.physical_fetch_hint.is_none()
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
            page_limit_is_zero: plan
                .scalar_plan()
                .page
                .as_ref()
                .is_some_and(|page| page.limit == Some(0)),
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
        let capabilities = Self::derive_route_capabilities(plan, direction, aggregate_expr);
        let kind = aggregate_expr.map(AggregateExpr::kind);
        let count_pushdown_eligible = kind.is_some_and(|aggregate_kind| {
            Self::is_count_pushdown_eligible(aggregate_kind, capabilities)
        });
        let load_scan_hint_gate = LoadScanHintGateContext::new(kind.is_some(), grouped);
        let load_scan_hint_gate_rejection = load_scan_hint_gate_rejection(load_scan_hint_gate);

        // Aggregate probes must not assume DESC physical reverse traversal
        // when the access shape cannot emit descending order natively.
        let count_pushdown_probe_fetch_hint = if count_pushdown_eligible {
            Self::count_pushdown_fetch_hint(continuation.window(), capabilities)
        } else {
            None
        };
        let aggregate_terminal_probe_fetch_hint = aggregate_expr.and_then(|aggregate| {
            Self::aggregate_probe_fetch_hint(
                aggregate,
                direction,
                capabilities,
                continuation.window(),
            )
        });
        let aggregate_seek_spec = aggregate_expr.and_then(|aggregate| {
            Self::aggregate_seek_spec(aggregate, direction, capabilities, continuation.window())
        });
        let aggregate_physical_fetch_hint =
            count_pushdown_probe_fetch_hint.or(aggregate_terminal_probe_fetch_hint);
        let aggregate_secondary_extrema_probe_fetch_hint =
            aggregate_seek_spec.map(AggregateSeekSpec::fetch);

        let load_physical_fetch_hint = if load_scan_hint_gate_rejection.is_some() {
            None
        } else {
            probe_fetch_hint
        };
        let physical_fetch_hint = if kind.is_some() {
            aggregate_physical_fetch_hint
        } else {
            load_physical_fetch_hint
        };
        let load_scan_budget_hint = if load_scan_hint_gate_rejection.is_none() {
            Self::load_scan_budget_hint(plan, continuation, capabilities)
        } else {
            None
        };
        let grouped_execution_strategy = if grouped {
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
            let grouped_ordered_eligibility = derive_grouped_ordered_eligibility(
                plan,
                planner_grouped_strategy_hint,
                direction,
                capabilities.desc_physical_reverse_supported,
                capabilities.streaming_access_shape_safe,
            );
            Some(grouped_execution_strategy_for_plan_hint(
                grouped_ordered_eligibility,
            ))
        } else {
            None
        };

        RouteDerivationContext {
            direction,
            capabilities,
            secondary_pushdown_applicability,
            scan_hints: ScanHintPlan {
                physical_fetch_hint,
                load_scan_budget_hint,
            },
            count_pushdown_eligible,
            aggregate_physical_fetch_hint,
            aggregate_seek_spec,
            aggregate_secondary_extrema_probe_fetch_hint,
            grouped_execution_strategy,
        }
    }
}

///
/// GroupedOrderedEligibility
///
/// Executor-owned grouped ordered-strategy eligibility matrix.
/// This matrix revalidates planner ordered-group hints against runtime capability
/// constraints before strategy projection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GroupedOrderedEligibility {
    ordered_hint: bool,
    direction_compatible: bool,
    streaming_access_shape_safe: bool,
}

impl GroupedOrderedEligibility {
    const fn is_eligible(self) -> bool {
        self.ordered_hint && self.direction_compatible && self.streaming_access_shape_safe
    }
}

// Derive one grouped ordered-strategy eligibility matrix snapshot.
const fn derive_grouped_ordered_eligibility<K>(
    _plan: &AccessPlannedQuery<K>,
    plan_hint: GroupedPlanStrategyHint,
    direction: Direction,
    desc_physical_reverse_supported: bool,
    streaming_access_shape_safe: bool,
) -> GroupedOrderedEligibility {
    GroupedOrderedEligibility {
        ordered_hint: matches!(plan_hint, GroupedPlanStrategyHint::OrderedGroup),
        direction_compatible: !matches!(direction, Direction::Desc)
            || desc_physical_reverse_supported,
        streaming_access_shape_safe,
    }
}

// Resolve one route-level grouped strategy from one revalidated eligibility matrix.
const fn grouped_execution_strategy_for_plan_hint(
    grouped_ordered_eligibility: GroupedOrderedEligibility,
) -> GroupedExecutionStrategy {
    if grouped_ordered_eligibility.is_eligible() {
        GroupedExecutionStrategy::OrderedMaterialized
    } else {
        GroupedExecutionStrategy::HashMaterialized
    }
}

#[cfg(test)]
pub(in crate::db::executor) const fn grouped_ordered_runtime_revalidation_flag_count_guard() -> usize
{
    let _ = GroupedOrderedEligibility {
        ordered_hint: false,
        direction_compatible: false,
        streaming_access_shape_safe: false,
    };

    3
}
