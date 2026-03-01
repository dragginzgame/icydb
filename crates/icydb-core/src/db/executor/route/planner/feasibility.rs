//! Module: executor::route::planner::feasibility
//! Responsibility: derive continuation/window/capability feasibility state.
//! Does not own: route-intent normalization or execution-mode selection.
//! Boundary: staged feasibility derivation for route planning.

use crate::{
    db::{
        access::PushdownApplicability,
        cursor::CursorBoundary,
        direction::Direction,
        executor::{
            RangeToken,
            aggregate::{AggregateKind, AggregateSpec},
            load::LoadExecutor,
            route::{
                ContinuationMode, GroupedExecutionStrategy, RouteWindowPlan, ScanHintPlan,
                planner::{RouteDerivationContext, RouteFeasibilityStage, RouteIntentStage},
            },
        },
        predicate::CompareOp,
        query::plan::{AccessPlannedQuery, GroupedPlanStrategyHint, grouped_plan_strategy_hint},
    },
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(in crate::db::executor::route::planner) fn derive_route_feasibility_stage(
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RangeToken>,
        probe_fetch_hint: Option<usize>,
        intent_stage: &RouteIntentStage,
    ) -> RouteFeasibilityStage {
        let continuation_mode = Self::derive_continuation_mode(cursor_boundary, index_range_anchor);
        let route_window = Self::derive_route_window(plan, cursor_boundary);
        let secondary_pushdown_applicability = Self::derive_secondary_pushdown_applicability(plan);
        let derivation = Self::derive_route_derivation_context(
            plan,
            intent_stage.aggregate_spec.as_ref(),
            intent_stage.grouped,
            continuation_mode,
            route_window,
            probe_fetch_hint,
            secondary_pushdown_applicability,
        );
        let kind = intent_stage.kind();
        let count_terminal = kind.is_some_and(AggregateKind::is_count);

        // COUNT fold-mode discipline: non-count pushdowns must not route COUNT
        // through non-COUNT streaming fast paths.
        let index_range_limit_spec = if count_terminal || intent_stage.grouped {
            None
        } else {
            Self::assess_index_range_limit_pushdown(
                plan,
                cursor_boundary,
                index_range_anchor,
                route_window,
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
        debug_assert!(
            derivation.scan_hints.load_scan_budget_hint.is_none()
                || cursor_boundary.is_none() && derivation.capabilities.streaming_access_shape_safe,
            "route invariant: load scan-budget hints require non-continuation streaming-safe shape",
        );
        debug_assert!(
            !intent_stage.grouped
                || derivation.scan_hints.load_scan_budget_hint.is_none()
                    && derivation.scan_hints.physical_fetch_hint.is_none()
                    && index_range_limit_spec.is_none(),
            "route invariant: grouped intent must not derive load/aggregate scan hints or index-range pushdown specs",
        );

        RouteFeasibilityStage {
            continuation_mode,
            route_window,
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
        aggregate_spec: Option<&AggregateSpec>,
        grouped: bool,
        continuation_mode: ContinuationMode,
        route_window: RouteWindowPlan,
        probe_fetch_hint: Option<usize>,
        secondary_pushdown_applicability: PushdownApplicability,
    ) -> RouteDerivationContext {
        let direction = aggregate_spec.map_or_else(
            || Self::derive_load_route_direction(plan),
            |spec| Self::derive_aggregate_route_direction(plan, spec),
        );
        let capabilities = Self::derive_route_capabilities(plan, direction, aggregate_spec);
        let kind = aggregate_spec.map(AggregateSpec::kind);
        let count_pushdown_eligible = kind.is_some_and(|aggregate_kind| {
            Self::is_count_pushdown_eligible(aggregate_kind, capabilities)
        });

        // Aggregate probes must not assume DESC physical reverse traversal
        // when the access shape cannot emit descending order natively.
        let count_pushdown_probe_fetch_hint = if count_pushdown_eligible {
            Self::count_pushdown_fetch_hint(route_window, capabilities)
        } else {
            None
        };
        let aggregate_terminal_probe_fetch_hint = aggregate_spec.and_then(|spec| {
            Self::aggregate_probe_fetch_hint(spec, direction, capabilities, route_window)
        });
        let aggregate_physical_fetch_hint =
            count_pushdown_probe_fetch_hint.or(aggregate_terminal_probe_fetch_hint);
        let aggregate_secondary_extrema_probe_fetch_hint = kind
            .filter(|aggregate_kind| aggregate_kind.is_extrema())
            .and(aggregate_physical_fetch_hint);

        let physical_fetch_hint = if kind.is_some() {
            aggregate_physical_fetch_hint
        } else if grouped {
            None
        } else {
            probe_fetch_hint
        };
        let load_scan_budget_hint = if kind.is_none() && !grouped {
            Self::load_scan_budget_hint(continuation_mode, route_window, capabilities)
        } else {
            None
        };
        let grouped_execution_strategy = if grouped {
            let grouped_ordered_eligibility = derive_grouped_ordered_eligibility(
                plan,
                grouped_plan_strategy_hint(plan).unwrap_or(GroupedPlanStrategyHint::HashGroup),
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
/// and grouped semantic guardrails before strategy projection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(clippy::struct_excessive_bools)]
struct GroupedOrderedEligibility {
    ordered_hint: bool,
    direction_compatible: bool,
    streaming_access_shape_safe: bool,
    aggregates_streaming_compatible: bool,
    having_streaming_compatible: bool,
    distinct_streaming_compatible: bool,
}

impl GroupedOrderedEligibility {
    const fn is_eligible(self) -> bool {
        self.ordered_hint
            && self.direction_compatible
            && self.streaming_access_shape_safe
            && self.aggregates_streaming_compatible
            && self.having_streaming_compatible
            && self.distinct_streaming_compatible
    }
}

// Derive one grouped ordered-strategy eligibility matrix snapshot.
fn derive_grouped_ordered_eligibility<K>(
    plan: &AccessPlannedQuery<K>,
    plan_hint: GroupedPlanStrategyHint,
    direction: Direction,
    desc_physical_reverse_supported: bool,
    streaming_access_shape_safe: bool,
) -> GroupedOrderedEligibility {
    let grouped = plan.grouped_plan();
    let aggregates_streaming_compatible = grouped.is_some_and(|grouped| {
        grouped
            .group
            .aggregates
            .iter()
            .all(crate::db::query::plan::GroupAggregateSpec::streaming_compatible_v1)
    });
    let having_streaming_compatible = grouped.is_none_or(|grouped| {
        grouped.having.as_ref().is_none_or(|having| {
            having.clauses().iter().all(|clause| {
                matches!(
                    clause.op(),
                    CompareOp::Eq
                        | CompareOp::Ne
                        | CompareOp::Lt
                        | CompareOp::Lte
                        | CompareOp::Gt
                        | CompareOp::Gte
                )
            })
        })
    });

    GroupedOrderedEligibility {
        ordered_hint: matches!(plan_hint, GroupedPlanStrategyHint::OrderedGroup),
        direction_compatible: !matches!(direction, Direction::Desc)
            || desc_physical_reverse_supported,
        streaming_access_shape_safe,
        aggregates_streaming_compatible,
        having_streaming_compatible,
        // Query-level DISTINCT remains incompatible with grouped ordered strategy in this line.
        distinct_streaming_compatible: !plan.scalar_plan().distinct,
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
