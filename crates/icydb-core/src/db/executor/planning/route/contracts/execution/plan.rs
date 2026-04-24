//! Module: db::executor::planning::route::contracts::execution::plan
//! Defines execution-plan route contracts consumed by executor preparation and
//! explain surfaces.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    access::PushdownApplicability,
    direction::Direction,
    executor::{
        aggregate::AggregateFoldMode,
        route::{
            LoadTerminalFastPathContract,
            contracts::{
                RouteCapabilities, RouteContinuationPlan,
                execution::{
                    AggregateSeekSpec, GroupedExecutionMode, GroupedExecutionModeProjection,
                    GroupedRouteDecisionOutcome, GroupedRouteObservability,
                    GroupedRouteRejectionReason, IndexRangeLimitSpec, LoadOrderRouteContract,
                    LoadOrderRouteReason, RouteExecutionMode, ScanHintPlan, TopNSeekSpec,
                },
                shape::{FastPathOrder, RouteShapeKind},
            },
        },
    },
    query::plan::GroupedPlanStrategy,
};

///
/// ExecutionRoutePlan
///
/// Canonical route decision payload for staged execution routing.
/// This is the single boundary that owns route-derived direction, pagination
/// window semantics, continuation mode, execution mode, pushdown eligibility,
/// DESC physical reverse-traversal capability, and scan-hint decisions.
///

#[derive(Clone)]
pub(in crate::db::executor) struct ExecutionRoutePlan {
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) route_shape_kind: RouteShapeKind,
    pub(in crate::db::executor) continuation: RouteContinuationPlan,
    pub(in crate::db::executor) execution_mode: RouteExecutionMode,
    pub(in crate::db::executor) desc_physical_reverse_supported: bool,
    pub(in crate::db::executor) secondary_pushdown_applicability: PushdownApplicability,
    pub(in crate::db::executor) index_range_limit_spec: Option<IndexRangeLimitSpec>,
    pub(in crate::db::executor::planning::route) capabilities: RouteCapabilities,
    pub(in crate::db::executor) fast_path_order: &'static [FastPathOrder],
    pub(in crate::db::executor) top_n_seek_spec: Option<TopNSeekSpec>,
    pub(in crate::db::executor) aggregate_seek_spec: Option<AggregateSeekSpec>,
    pub(in crate::db::executor) scan_hints: ScanHintPlan,
    pub(in crate::db::executor) aggregate_fold_mode: AggregateFoldMode,
    pub(in crate::db::executor) grouped_plan_strategy: Option<GroupedPlanStrategy>,
    pub(in crate::db::executor) grouped_execution_mode: Option<GroupedExecutionMode>,
    pub(in crate::db::executor) load_terminal_fast_path: Option<LoadTerminalFastPathContract>,
}

impl ExecutionRoutePlan {
    /// Construct one grouped route plan for executor tests that only need
    /// grouped runtime window semantics without full route derivation.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) const fn grouped_for_test(direction: Direction) -> Self {
        Self {
            direction,
            route_shape_kind: RouteShapeKind::AggregateGrouped,
            continuation: RouteContinuationPlan::initial_for_mutation(),
            execution_mode: RouteExecutionMode::Materialized,
            desc_physical_reverse_supported: false,
            secondary_pushdown_applicability: PushdownApplicability::NotApplicable,
            index_range_limit_spec: None,
            capabilities: RouteCapabilities {
                load_order_route_decision:
                    crate::db::executor::route::LoadOrderRouteDecision::materialized_fallback(
                        LoadOrderRouteReason::None,
                    ),
                pk_order_fast_path_eligible: false,
                count_pushdown_shape_supported: false,
                composite_aggregate_fast_path_eligible: false,
                residual_filter_present: false,
                bounded_probe_hint_safe: false,
                field_min_fast_path_eligible: false,
                field_max_fast_path_eligible: false,
                field_min_fast_path_ineligibility_reason: None,
                field_max_fast_path_ineligibility_reason: None,
            },
            fast_path_order: &[],
            top_n_seek_spec: None,
            aggregate_seek_spec: None,
            scan_hints: ScanHintPlan {
                physical_fetch_hint: None,
                load_scan_budget_hint: None,
            },
            aggregate_fold_mode: AggregateFoldMode::ExistingRows,
            grouped_plan_strategy: None,
            grouped_execution_mode: Some(GroupedExecutionMode::HashMaterialized),
            load_terminal_fast_path: None,
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn direction(&self) -> Direction {
        self.direction
    }

    #[must_use]
    pub(in crate::db::executor) const fn continuation(&self) -> RouteContinuationPlan {
        self.continuation
    }

    #[must_use]
    pub(in crate::db::executor) const fn load_terminal_fast_path(
        &self,
    ) -> Option<&LoadTerminalFastPathContract> {
        self.load_terminal_fast_path.as_ref()
    }

    #[must_use]
    pub(in crate::db::executor) const fn route_shape_kind(&self) -> RouteShapeKind {
        self.route_shape_kind
    }

    pub(in crate::db::executor) const fn execution_mode(&self) -> RouteExecutionMode {
        self.execution_mode
    }

    #[must_use]
    pub(in crate::db::executor) const fn is_streaming(&self) -> bool {
        matches!(self.execution_mode, RouteExecutionMode::Streaming)
    }

    #[must_use]
    pub(in crate::db::executor) const fn is_materialized(&self) -> bool {
        matches!(self.execution_mode, RouteExecutionMode::Materialized)
    }

    // Grouped route observability projection.
    // Non-grouped routes intentionally report no grouped diagnostics payload.
    /// Project grouped route observability payload when grouped routing is active.
    #[must_use]
    pub(in crate::db::executor) const fn grouped_observability(
        &self,
    ) -> Option<GroupedRouteObservability> {
        match self.route_shape_kind() {
            RouteShapeKind::AggregateGrouped => {
                let grouped_plan_strategy = self
                    .grouped_plan_strategy
                    .expect("grouped route observability requires planner-owned grouped strategy");
                let grouped_execution_mode = self.grouped_execution_mode.expect(
                    "grouped route observability requires route-projected grouped execution mode",
                );
                let projected_grouped_execution_mode = GroupedExecutionMode::from_planner_strategy(
                    grouped_plan_strategy,
                    GroupedExecutionModeProjection::from_route_inputs(
                        self.direction,
                        self.desc_physical_reverse_supported,
                        self.capabilities
                            .load_order_route_contract()
                            .allows_ordered_group_projection(),
                    ),
                );
                debug_assert!(
                    matches!(
                        (grouped_execution_mode, projected_grouped_execution_mode),
                        (
                            GroupedExecutionMode::HashMaterialized,
                            GroupedExecutionMode::HashMaterialized,
                        ) | (
                            GroupedExecutionMode::OrderedMaterialized,
                            GroupedExecutionMode::OrderedMaterialized,
                        )
                    ),
                    "grouped route observability must project grouped execution mode only from planner strategy plus route capabilities",
                );
                let eligible = self.fast_path_order.is_empty();
                let (outcome, rejection_reason) = if eligible {
                    debug_assert!(
                        matches!(self.execution_mode, RouteExecutionMode::Materialized),
                        "grouped route observability currently models only materialized grouped execution",
                    );
                    (GroupedRouteDecisionOutcome::MaterializedFallback, None)
                } else {
                    (
                        GroupedRouteDecisionOutcome::Rejected,
                        Some(GroupedRouteRejectionReason::CapabilityMismatch),
                    )
                };

                Some(GroupedRouteObservability {
                    outcome,
                    rejection_reason,
                    planner_fallback_reason: grouped_plan_strategy.fallback_reason(),
                    eligible,
                    execution_mode: self.execution_mode(),
                    grouped_execution_mode,
                })
            }
            RouteShapeKind::LoadScalar
            | RouteShapeKind::AggregateCount
            | RouteShapeKind::AggregateNonCount
            | RouteShapeKind::MutationDelete => None,
        }
    }

    // True when DESC execution can traverse the physical access path in reverse.
    pub(in crate::db::executor) const fn desc_physical_reverse_supported(&self) -> bool {
        self.desc_physical_reverse_supported
    }

    // True when secondary-prefix pushdown is enabled for this route.
    pub(in crate::db::executor) const fn secondary_fast_path_eligible(&self) -> bool {
        self.secondary_pushdown_applicability.is_eligible()
    }

    // True when the plan shape supports direct PK ordered streaming fast path.
    pub(in crate::db::executor) const fn pk_order_fast_path_eligible(&self) -> bool {
        self.capabilities.pk_order_fast_path_eligible
    }

    // True when access shape is streaming-safe for final order semantics.
    pub(in crate::db::executor) const fn load_order_route_contract(
        &self,
    ) -> LoadOrderRouteContract {
        self.capabilities.load_order_route_contract()
    }

    // Route-owned explanation for why one ordered load shape stayed direct or
    // materialized at the current boundary.
    pub(in crate::db::executor) const fn load_order_route_reason(&self) -> LoadOrderRouteReason {
        self.capabilities.load_order_route_reason()
    }

    // True when index-range limit pushdown is enabled for this route.
    pub(in crate::db::executor) const fn index_range_limit_fast_path_enabled(&self) -> bool {
        self.index_range_limit_spec.is_some()
    }

    // True when composite aggregate fast-path execution is shape-safe.
    pub(in crate::db::executor) const fn composite_aggregate_fast_path_eligible(&self) -> bool {
        self.capabilities.composite_aggregate_fast_path_eligible
    }

    // True when route permits a future `min(field)` fast path.
    pub(in crate::db::executor) const fn field_min_fast_path_eligible(&self) -> bool {
        self.capabilities.field_min_fast_path_eligible
    }

    // True when route permits a future `max(field)` fast path.
    pub(in crate::db::executor) const fn field_max_fast_path_eligible(&self) -> bool {
        self.capabilities.field_max_fast_path_eligible
    }

    // Route-owned fast-path dispatch order. Executors must dispatch using this
    // order instead of introducing ad-hoc aggregate/runtime micro fast paths.
    pub(in crate::db::executor) const fn fast_path_order(&self) -> &'static [FastPathOrder] {
        self.fast_path_order
    }

    // Route-owned load fast-path gate for one candidate route.
    pub(in crate::db::executor) const fn load_fast_path_route_eligible(
        &self,
        route: FastPathOrder,
    ) -> bool {
        match route {
            FastPathOrder::PrimaryKey => self.pk_order_fast_path_eligible(),
            FastPathOrder::SecondaryPrefix => {
                self.secondary_fast_path_eligible()
                    // Field-target extrema streaming also consumes this loader-owned
                    // secondary stream path even when ORDER BY pushdown is not active.
                    || self.field_min_fast_path_eligible()
                    || self.field_max_fast_path_eligible()
            }
            FastPathOrder::IndexRange => self.index_range_limit_fast_path_enabled(),
            FastPathOrder::PrimaryScan | FastPathOrder::Composite => false,
        }
    }

    // Route-owned bounded probe hint for secondary Min/Max single-step probing.
    // This prevents executor-local hint math from drifting outside routing.
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_seek_spec(&self) -> Option<AggregateSeekSpec> {
        self.aggregate_seek_spec
    }

    // Route-owned bounded fetch hint derived from aggregate seek contract.
    #[must_use]
    pub(in crate::db::executor) fn aggregate_seek_fetch_hint(&self) -> Option<usize> {
        self.aggregate_seek_spec().map(AggregateSeekSpec::fetch)
    }

    // Route-owned bounded fetch contract for ordered load top-N seek windows.
    #[must_use]
    pub(in crate::db::executor) const fn top_n_seek_spec(&self) -> Option<TopNSeekSpec> {
        self.top_n_seek_spec
    }
}
