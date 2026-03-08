//! Module: db::executor::route::contracts
//! Responsibility: route-owned contracts, capability snapshots, and precedence constants.
//! Does not own: capability derivation algorithms or route planning flow.
//! Boundary: shared immutable route types consumed by route submodules and executor runtime.

use crate::db::{
    access::PushdownApplicability,
    direction::Direction,
    executor::{
        AccessExecutionDescriptor, ContinuationCapabilities,
        aggregate::{AggregateFoldMode, capability::AggregateFieldExtremaIneligibilityReason},
    },
    query::builder::AggregateExpr,
    query::plan::{GroupedPlanStrategyHint, ScalarAccessWindowPlan},
};

///
/// ExecutionMode
///
/// Canonical route-level execution shape selected by the routing gate.
/// Keeps streaming-vs-materialized decisions explicit and testable.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ExecutionMode {
    Streaming,
    Materialized,
}

///
/// GroupedExecutionStrategy
///
/// Canonical grouped execution strategy label selected by route planning.
/// Variants are runtime-truthful and explicitly mark materialized execution.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum GroupedExecutionStrategy {
    HashMaterialized,
    OrderedMaterialized,
}

///
/// ScanHintPlan
///
/// Canonical scan-hint payload produced by route planning.
/// Keeps bounded fetch/budget hints under one boundary.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db::executor) struct ScanHintPlan {
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) load_scan_budget_hint: Option<usize>,
}

///
/// IndexRangeLimitSpec
///
/// Canonical route decision payload for index-range limit pushdown.
/// Encodes the bounded fetch size after all eligibility gates pass.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct IndexRangeLimitSpec {
    pub(in crate::db::executor) fetch: usize,
}

///
/// AggregateSeekSpec
///
/// Canonical route contract for aggregate index-edge seek execution.
/// Encodes seek edge (`first`/`last`) and bounded fetch budget in one payload.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AggregateSeekSpec {
    First { fetch: usize },
    Last { fetch: usize },
}

impl AggregateSeekSpec {
    #[must_use]
    pub(in crate::db::executor) const fn fetch(self) -> usize {
        match self {
            Self::First { fetch } | Self::Last { fetch } => fetch,
        }
    }
}

///
/// TopNSeekSpec
///
/// Canonical route contract for ordered load `LIMIT` seek windows.
/// Encodes the bounded fetch size for one top-N access pass.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct TopNSeekSpec {
    fetch: usize,
}

impl TopNSeekSpec {
    #[must_use]
    pub(in crate::db::executor::route) const fn new(fetch: usize) -> Self {
        Self { fetch }
    }

    #[must_use]
    pub(in crate::db::executor) const fn fetch(self) -> usize {
        self.fetch
    }
}

///
/// ContinuationMode
///
/// Route-owned continuation classification used to keep resume-policy decisions
/// explicit and isolated from access-shape modeling.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ContinuationMode {
    Initial,
    CursorBoundary,
    IndexRangeAnchor,
}

///
/// RouteContinuationPlan
///
/// Route-owned continuation projection bundle.
/// Keeps continuation capabilities and route-window
/// semantics under one immutable routing contract.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct RouteContinuationPlan {
    capabilities: ContinuationCapabilities,
    pub(in crate::db::executor) effective_offset: u32,
    pub(in crate::db::executor::route) access_window_keep: AccessWindow,
    pub(in crate::db::executor::route) access_window_fetch: AccessWindow,
}

impl RouteContinuationPlan {
    #[must_use]
    pub(in crate::db::executor::route) const fn new(
        capabilities: ContinuationCapabilities,
        effective_offset: u32,
        access_window_keep: AccessWindow,
        access_window_fetch: AccessWindow,
    ) -> Self {
        Self {
            capabilities,
            effective_offset,
            access_window_keep,
            access_window_fetch,
        }
    }

    #[must_use]
    pub(in crate::db::executor::route) fn from_scalar_access_window_plan(
        capabilities: ContinuationCapabilities,
        window_plan: ScalarAccessWindowPlan,
    ) -> Self {
        let effective_offset = window_plan.effective_offset();
        let lower_bound = window_plan.lower_bound();
        let keep_count = window_plan.keep_count();
        let page_limit = window_plan.limit();
        let fetch_count = window_plan.fetch_count();
        let access_window_keep = AccessWindow::new(lower_bound, keep_count, page_limit, keep_count);
        let access_window_fetch =
            AccessWindow::new(lower_bound, keep_count, page_limit, fetch_count);

        Self::new(
            capabilities,
            effective_offset,
            access_window_keep,
            access_window_fetch,
        )
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn mode(self) -> ContinuationMode {
        self.capabilities.mode()
    }

    #[must_use]
    pub(in crate::db::executor) const fn capabilities(self) -> ContinuationCapabilities {
        self.capabilities
    }

    #[must_use]
    pub(in crate::db::executor) const fn effective_offset(self) -> u32 {
        self.effective_offset
    }

    #[must_use]
    pub(in crate::db::executor) const fn limit(&self) -> Option<u32> {
        self.access_window_keep.page_limit()
    }

    #[must_use]
    pub(in crate::db::executor) const fn keep_access_window(&self) -> &AccessWindow {
        &self.access_window_keep
    }

    #[must_use]
    pub(in crate::db::executor) const fn fetch_access_window(&self) -> &AccessWindow {
        &self.access_window_fetch
    }
}

///
/// AccessWindow
///
/// Route-projected bounded access-window contract.
/// `lower_bound` is the effective offset, `upper_bound` is the optional bounded
/// keep-count horizon, and `fetch_limit` is the optional bounded access budget.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct AccessWindow {
    lower_bound: usize,
    upper_bound: Option<usize>,
    page_limit: Option<u32>,
    fetch_limit: Option<usize>,
}

impl AccessWindow {
    /// Construct one immutable access-window contract.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        lower_bound: usize,
        upper_bound: Option<usize>,
        page_limit: Option<u32>,
        fetch_limit: Option<usize>,
    ) -> Self {
        Self {
            lower_bound,
            upper_bound,
            page_limit,
            fetch_limit,
        }
    }

    /// Return the effective lower-bound offset.
    #[must_use]
    pub(in crate::db::executor) const fn lower_bound(self) -> usize {
        self.lower_bound
    }

    /// Return the optional page-limit window width.
    #[must_use]
    pub(in crate::db::executor) const fn page_limit(self) -> Option<u32> {
        self.page_limit
    }

    /// Return the optional bounded fetch limit.
    #[must_use]
    pub(in crate::db::executor) const fn fetch_limit(self) -> Option<usize> {
        self.fetch_limit
    }

    /// Return true when the window is explicitly `LIMIT 0`.
    #[must_use]
    pub(in crate::db::executor) const fn is_zero_window(self) -> bool {
        matches!(self.page_limit, Some(0))
    }
}

///
/// ExecutionRoutePlan
///
/// Canonical route decision payload for load/aggregate execution.
/// This is the single boundary that owns route-derived direction, pagination
/// window semantics, continuation mode, execution mode, pushdown eligibility,
/// DESC physical reverse-traversal capability, and scan-hint decisions.
///

#[derive(Clone)]
pub(in crate::db::executor) struct ExecutionRoutePlan {
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) route_shape_kind: RouteShapeKind,
    pub(in crate::db::executor) continuation: RouteContinuationPlan,
    pub(in crate::db::executor) execution_mode: ExecutionMode,
    pub(in crate::db::executor) execution_mode_case: ExecutionModeRouteCase,
    pub(in crate::db::executor) secondary_pushdown_applicability: PushdownApplicability,
    pub(in crate::db::executor) index_range_limit_spec: Option<IndexRangeLimitSpec>,
    pub(in crate::db::executor::route) capabilities: RouteCapabilities,
    pub(in crate::db::executor) fast_path_order: &'static [FastPathOrder],
    pub(in crate::db::executor) top_n_seek_spec: Option<TopNSeekSpec>,
    pub(in crate::db::executor) aggregate_seek_spec: Option<AggregateSeekSpec>,
    pub(in crate::db::executor) scan_hints: ScanHintPlan,
    pub(in crate::db::executor) aggregate_fold_mode: AggregateFoldMode,
    pub(in crate::db::executor) grouped_execution_strategy: Option<GroupedExecutionStrategy>,
}

impl ExecutionRoutePlan {
    #[must_use]
    pub(in crate::db::executor) const fn direction(&self) -> Direction {
        self.direction
    }

    #[must_use]
    pub(in crate::db::executor) const fn continuation(&self) -> RouteContinuationPlan {
        self.continuation
    }

    #[must_use]
    pub(in crate::db::executor) const fn shape(&self) -> ExecutionRouteShape {
        ExecutionRouteShape {
            route_shape_kind: self.route_shape_kind,
            execution_mode: self.execution_mode,
            execution_mode_case: self.execution_mode_case,
        }
    }

    // True when DESC execution can traverse the physical access path in reverse.
    pub(in crate::db::executor) const fn desc_physical_reverse_supported(&self) -> bool {
        self.capabilities.desc_physical_reverse_supported
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
    pub(in crate::db::executor) const fn streaming_access_shape_safe(&self) -> bool {
        self.capabilities.streaming_access_shape_safe
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

    #[cfg(test)]
    pub(in crate::db::executor) const fn count_pushdown_access_shape_supported(&self) -> bool {
        self.capabilities.count_pushdown_access_shape_supported
    }

    #[cfg(test)]
    pub(in crate::db::executor) const fn count_pushdown_existing_rows_shape_supported(
        &self,
    ) -> bool {
        self.capabilities
            .count_pushdown_existing_rows_shape_supported
    }

    #[cfg(test)]
    pub(in crate::db::executor) const fn index_range_limit_pushdown_shape_eligible(&self) -> bool {
        self.capabilities.index_range_limit_pushdown_shape_eligible
    }

    #[cfg(test)]
    pub(in crate::db::executor) const fn bounded_probe_hint_safe(&self) -> bool {
        self.capabilities.bounded_probe_hint_safe
    }

    // Route-owned diagnostic reason for why `min(field)` fast path is ineligible.
    #[cfg(test)]
    pub(in crate::db::executor) const fn field_min_fast_path_ineligibility_reason(
        &self,
    ) -> Option<FieldExtremaIneligibilityReason> {
        self.capabilities.field_min_fast_path_ineligibility_reason
    }

    // Route-owned diagnostic reason for why `max(field)` fast path is ineligible.
    #[cfg(test)]
    pub(in crate::db::executor) const fn field_max_fast_path_ineligibility_reason(
        &self,
    ) -> Option<FieldExtremaIneligibilityReason> {
        self.capabilities.field_max_fast_path_ineligibility_reason
    }

    // Route-owned fast-path dispatch order. Executors must dispatch using this
    // order instead of introducing ad-hoc aggregate/load micro fast paths.
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

///
/// FastPathOrder
///
/// Shared fast-path precedence model used by load and aggregate routing.
/// Routing implementations remain separate, but they iterate one canonical order.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum FastPathOrder {
    PrimaryKey,
    SecondaryPrefix,
    PrimaryScan,
    IndexRange,
    Composite,
}

// Contract: fast-path precedence is a stability boundary. Any change here must
// be intentional, accompanied by route-order tests, and called out in changelog.
pub(in crate::db::executor) const LOAD_FAST_PATH_ORDER: [FastPathOrder; 3] = [
    FastPathOrder::PrimaryKey,
    FastPathOrder::SecondaryPrefix,
    FastPathOrder::IndexRange,
];

// Contract: aggregate dispatch precedence is ordered for semantic and
// performance stability. Do not reorder casually.
pub(in crate::db::executor) const AGGREGATE_FAST_PATH_ORDER: [FastPathOrder; 5] = [
    FastPathOrder::PrimaryKey,
    FastPathOrder::SecondaryPrefix,
    FastPathOrder::PrimaryScan,
    FastPathOrder::IndexRange,
    FastPathOrder::Composite,
];

// Contract: grouped aggregate routes are materialized-only in this audit pass
// and must not participate in scalar aggregate fast-path dispatch.
pub(in crate::db::executor) const GROUPED_AGGREGATE_FAST_PATH_ORDER: [FastPathOrder; 0] = [];

// Contract: mutation routes are materialized-only and do not participate in
// load/aggregate fast-path precedence.
pub(in crate::db::executor) const MUTATION_FAST_PATH_ORDER: [FastPathOrder; 0] = [];

///
/// RoutedKeyStreamRequest
///
/// Canonical stream-construction request variants for route-owned key-stream
/// resolution across load and aggregate execution paths.
///

pub(in crate::db::executor) enum RoutedKeyStreamRequest<'a, K> {
    AccessDescriptor(AccessExecutionDescriptor<'a, K>),
}

///
/// RouteIntent
///

pub(in crate::db::executor::route) enum RouteIntent {
    Load,
    Aggregate {
        aggregate: AggregateExpr,
        aggregate_force_materialized_due_to_predicate_uncertainty: bool,
    },
    AggregateGrouped {
        grouped_plan_strategy_hint: GroupedPlanStrategyHint,
        aggregate_force_materialized_due_to_predicate_uncertainty: bool,
    },
}

///
/// RouteShapeKind
///
/// Planner-to-router semantic execution shape contract.
/// This shape is independent from streaming/materialized execution policy and
/// allows route dispatch migration away from feature-combination branching.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum RouteShapeKind {
    LoadScalar,
    AggregateCount,
    AggregateNonCount,
    AggregateGrouped,
    MutationDelete,
}

///
/// ExecutionModeRouteCase
///
/// Canonical route-case partition for execution-mode decisions.
/// This keeps streaming/materialized branching explicit under one gate.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ExecutionModeRouteCase {
    Load,
    AggregateCount,
    AggregateNonCount,
    AggregateGrouped,
}

///
/// ExecutionRouteShape
///
/// Canonical executor-facing route shape descriptor.
/// This carries only shape axes (kind + mode + mode case) so runtime consumers
/// can make shape decisions without reaching through the full route payload.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct ExecutionRouteShape {
    route_shape_kind: RouteShapeKind,
    execution_mode: ExecutionMode,
    execution_mode_case: ExecutionModeRouteCase,
}

impl ExecutionRouteShape {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn route_shape_kind(self) -> RouteShapeKind {
        self.route_shape_kind
    }

    #[must_use]
    pub(in crate::db::executor) const fn execution_mode(self) -> ExecutionMode {
        self.execution_mode
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn execution_mode_case(self) -> ExecutionModeRouteCase {
        self.execution_mode_case
    }

    #[must_use]
    pub(in crate::db::executor) const fn is_streaming(self) -> bool {
        matches!(self.execution_mode, ExecutionMode::Streaming)
    }

    #[must_use]
    pub(in crate::db::executor) const fn is_materialized(self) -> bool {
        matches!(self.execution_mode, ExecutionMode::Materialized)
    }
}

///
/// GroupedRouteDecisionOutcome
///
/// Grouped route decision outcome surface.
/// Keeps grouped route diagnostics aligned with route selection semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum GroupedRouteDecisionOutcome {
    Selected,
    Rejected,
    MaterializedFallback,
}

///
/// GroupedRouteRejectionReason
///
/// Grouped route rejection taxonomy.
/// These reasons are route-owned and represent route-gate failures only.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum GroupedRouteRejectionReason {
    CapabilityMismatch,
}

///
/// GroupedRouteObservability
///
/// Grouped route observability payload.
/// Carries route outcome, optional rejection reason, eligibility, and
/// selected execution mode for grouped intents.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedRouteObservability {
    pub(in crate::db::executor::route) outcome: GroupedRouteDecisionOutcome,
    pub(in crate::db::executor::route) rejection_reason: Option<GroupedRouteRejectionReason>,
    pub(in crate::db::executor::route) eligible: bool,
    pub(in crate::db::executor::route) execution_mode: ExecutionMode,
    pub(in crate::db::executor::route) grouped_execution_strategy: GroupedExecutionStrategy,
}

impl GroupedRouteObservability {
    #[must_use]
    pub(in crate::db::executor) const fn outcome(self) -> GroupedRouteDecisionOutcome {
        self.outcome
    }

    #[must_use]
    pub(in crate::db::executor) const fn rejection_reason(
        self,
    ) -> Option<GroupedRouteRejectionReason> {
        self.rejection_reason
    }

    #[must_use]
    pub(in crate::db::executor) const fn eligible(self) -> bool {
        self.eligible
    }

    #[must_use]
    pub(in crate::db::executor) const fn execution_mode(self) -> ExecutionMode {
        self.execution_mode
    }

    #[must_use]
    pub(in crate::db::executor) const fn grouped_execution_strategy(
        self,
    ) -> GroupedExecutionStrategy {
        self.grouped_execution_strategy
    }
}

///
/// FieldExtremaIneligibilityReason
///
/// Route-surfaced alias of aggregate-policy field-extrema ineligibility reasons.
/// This preserves route diagnostics while aggregate capability policy owns derivation.
///

pub(in crate::db::executor) type FieldExtremaIneligibilityReason =
    AggregateFieldExtremaIneligibilityReason;

///
/// RouteCapabilities
///
/// Canonical derived capability snapshot for one logical plan and direction.
/// Route planning derives this once, then consumes it for eligibility and hint
/// decisions to reduce drift across helpers.
///

#[expect(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct RouteCapabilities {
    pub(in crate::db::executor) streaming_access_shape_safe: bool,
    pub(in crate::db::executor) pk_order_fast_path_eligible: bool,
    pub(in crate::db::executor) desc_physical_reverse_supported: bool,
    pub(in crate::db::executor) count_pushdown_access_shape_supported: bool,
    pub(in crate::db::executor) count_pushdown_existing_rows_shape_supported: bool,
    pub(in crate::db::executor) index_range_limit_pushdown_shape_eligible: bool,
    pub(in crate::db::executor) composite_aggregate_fast_path_eligible: bool,
    pub(in crate::db::executor) bounded_probe_hint_safe: bool,
    pub(in crate::db::executor) field_min_fast_path_eligible: bool,
    pub(in crate::db::executor) field_max_fast_path_eligible: bool,
    pub(in crate::db::executor) field_min_fast_path_ineligibility_reason:
        Option<FieldExtremaIneligibilityReason>,
    pub(in crate::db::executor) field_max_fast_path_ineligibility_reason:
        Option<FieldExtremaIneligibilityReason>,
}

#[cfg(test)]
pub(in crate::db::executor) const fn route_capability_flag_count_guard() -> usize {
    let _ = RouteCapabilities {
        streaming_access_shape_safe: false,
        pk_order_fast_path_eligible: false,
        desc_physical_reverse_supported: false,
        count_pushdown_access_shape_supported: false,
        count_pushdown_existing_rows_shape_supported: false,
        index_range_limit_pushdown_shape_eligible: false,
        composite_aggregate_fast_path_eligible: false,
        bounded_probe_hint_safe: false,
        field_min_fast_path_eligible: false,
        field_max_fast_path_eligible: false,
        field_min_fast_path_ineligibility_reason: None,
        field_max_fast_path_ineligibility_reason: None,
    };

    10
}

#[cfg(test)]
pub(in crate::db::executor) const fn route_execution_mode_case_count_guard() -> usize {
    let _ = [
        ExecutionModeRouteCase::Load,
        ExecutionModeRouteCase::AggregateCount,
        ExecutionModeRouteCase::AggregateNonCount,
        ExecutionModeRouteCase::AggregateGrouped,
    ];

    4
}

#[cfg(test)]
pub(in crate::db::executor) const fn route_shape_kind_count_guard() -> usize {
    let _ = [
        RouteShapeKind::LoadScalar,
        RouteShapeKind::AggregateCount,
        RouteShapeKind::AggregateNonCount,
        RouteShapeKind::AggregateGrouped,
        RouteShapeKind::MutationDelete,
    ];

    5
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{AccessWindow, ContinuationMode, RouteContinuationPlan};
    use crate::db::{executor::ContinuationCapabilities, query::plan::ContinuationPolicy};

    fn route_continuation(
        effective_offset: u32,
        access_window_keep: AccessWindow,
        access_window_fetch: AccessWindow,
    ) -> RouteContinuationPlan {
        RouteContinuationPlan::new(
            ContinuationCapabilities::new(
                ContinuationMode::Initial,
                ContinuationPolicy::new(true, true, true),
            ),
            effective_offset,
            access_window_keep,
            access_window_fetch,
        )
    }

    #[test]
    fn route_continuation_access_window_limit_zero_projects_zero_fetch_limit() {
        let continuation = route_continuation(
            4,
            AccessWindow::new(4, Some(4), Some(0), Some(4)),
            AccessWindow::new(4, Some(4), Some(0), Some(0)),
        );
        let access_window = continuation.fetch_access_window();

        assert_eq!(access_window.lower_bound(), 4);
        assert_eq!(access_window.page_limit(), Some(0));
        assert_eq!(access_window.fetch_limit(), Some(0));
        assert!(
            access_window.is_zero_window(),
            "LIMIT 0 route windows must project zero-fetch access windows",
        );
    }

    #[test]
    fn route_continuation_access_window_bounded_limit_projects_offset_and_fetch_counts() {
        let continuation = route_continuation(
            3,
            AccessWindow::new(3, Some(5), Some(2), Some(5)),
            AccessWindow::new(3, Some(5), Some(2), Some(6)),
        );
        let keep_window = continuation.keep_access_window();
        let fetch_window = continuation.fetch_access_window();

        assert_eq!(keep_window.lower_bound(), 3);
        assert_eq!(keep_window.page_limit(), Some(2));
        assert_eq!(keep_window.fetch_limit(), Some(5));
        assert!(!keep_window.is_zero_window());
        assert_eq!(fetch_window.fetch_limit(), Some(6));
    }

    #[test]
    fn route_continuation_access_window_unbounded_limit_projects_unbounded_fetch() {
        let continuation = route_continuation(
            0,
            AccessWindow::new(0, None, None, None),
            AccessWindow::new(0, None, None, None),
        );
        let access_window = continuation.fetch_access_window();

        assert_eq!(access_window.lower_bound(), 0);
        assert_eq!(access_window.page_limit(), None);
        assert_eq!(access_window.fetch_limit(), None);
        assert!(!access_window.is_zero_window());
    }
}
