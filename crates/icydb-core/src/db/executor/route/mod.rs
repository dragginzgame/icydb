mod capability;
mod fast_path;
mod guard;
mod hints;
mod mode;
mod planner;
pub(super) use guard::*;

use crate::db::{
    access::{AccessPath, PushdownApplicability},
    direction::Direction,
    executor::{
        AccessPlanStreamRequest, IndexStreamConstraints, StreamExecutionHints,
        aggregate::{AggregateFoldMode, AggregateSpec},
        compute_page_window,
    },
};
use crate::error::InternalError;

// -----------------------------------------------------------------------------
// Route Subdomains (Pre-Split Planning)
// -----------------------------------------------------------------------------
// 1) Route contracts and immutable capability snapshots.
// 2) Capability derivation for one validated plan + direction.
// 3) Execution mode, hint, and pushdown gating.
// 4) Access-shape eligibility and traversal-support helpers.
// 5) Route decision matrix and precedence contract tests.
//
// Soft feature budget:
// - Each new aggregate/routing feature should add at most +1 capability flag.
// - Each new aggregate/routing feature should add at most +1 execution-mode case.
// - Eligibility helper definitions stay route-owned.

///
/// ExecutionMode
///
/// Canonical route-level execution shape selected by the routing gate.
/// Keeps streaming-vs-materialized decisions explicit and testable.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExecutionMode {
    Streaming,
    Materialized,
}

/// Return true when this access path is eligible for PK stream fast-path execution.
#[must_use]
pub(in crate::db::executor) const fn supports_pk_stream_access_path<K>(
    path: &AccessPath<K>,
) -> bool {
    matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. })
}

///
/// ScanHintPlan
///
/// Canonical scan-hint payload produced by route planning.
/// Keeps bounded fetch/budget hints under one boundary.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ScanHintPlan {
    pub(super) physical_fetch_hint: Option<usize>,
    pub(super) load_scan_budget_hint: Option<usize>,
}

///
/// IndexRangeLimitSpec
///
/// Canonical route decision payload for index-range limit pushdown.
/// Encodes the bounded fetch size after all eligibility gates pass.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct IndexRangeLimitSpec {
    pub(super) fetch: usize,
}

///
/// ContinuationMode
///
/// Route-owned continuation classification used to keep resume-policy decisions
/// explicit and isolated from access-shape modeling.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ContinuationMode {
    Initial,
    CursorBoundary,
    IndexRangeAnchor,
}

///
/// RouteWindowPlan
///
/// Route-owned pagination window contract derived from logical page settings and
/// continuation context.
///
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct RouteWindowPlan {
    pub(super) effective_offset: u32,
    limit: Option<u32>,
    keep_count: Option<usize>,
    fetch_count: Option<usize>,
}

impl RouteWindowPlan {
    #[must_use]
    pub(super) fn new(effective_offset: u32, limit: Option<u32>) -> Self {
        let (keep_count, fetch_count) = match limit {
            Some(limit) => {
                let keep = compute_page_window(effective_offset, limit, false).keep_count;
                let fetch = compute_page_window(effective_offset, limit, true).fetch_count;
                (Some(keep), Some(fetch))
            }
            None => (None, None),
        };

        Self {
            effective_offset,
            limit,
            keep_count,
            fetch_count,
        }
    }

    #[must_use]
    pub(super) const fn limit(&self) -> Option<u32> {
        self.limit
    }

    #[must_use]
    pub(super) const fn fetch_count_for(&self, needs_extra: bool) -> Option<usize> {
        if needs_extra {
            self.fetch_count
        } else {
            self.keep_count
        }
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
pub(super) struct ExecutionRoutePlan {
    direction: Direction,
    continuation_mode: ContinuationMode,
    window: RouteWindowPlan,
    pub(super) execution_mode: ExecutionMode,
    #[expect(dead_code)]
    execution_mode_case: ExecutionModeRouteCase,
    secondary_pushdown_applicability: PushdownApplicability,
    pub(super) index_range_limit_spec: Option<IndexRangeLimitSpec>,
    capabilities: RouteCapabilities,
    fast_path_order: &'static [FastPathOrder],
    aggregate_secondary_extrema_probe_fetch_hint: Option<usize>,
    pub(super) scan_hints: ScanHintPlan,
    pub(super) aggregate_fold_mode: AggregateFoldMode,
}

impl ExecutionRoutePlan {
    #[must_use]
    pub(super) const fn direction(&self) -> Direction {
        self.direction
    }

    #[must_use]
    pub(super) const fn continuation_mode(&self) -> ContinuationMode {
        self.continuation_mode
    }

    #[must_use]
    pub(super) const fn window(&self) -> RouteWindowPlan {
        self.window
    }

    #[must_use]
    #[cfg(test)]
    pub(super) const fn execution_mode_case(&self) -> ExecutionModeRouteCase {
        self.execution_mode_case
    }

    // Grouped route observability projection for grouped-readiness scaffolding.
    // Non-grouped routes intentionally report no grouped diagnostics payload.
    #[expect(dead_code)]
    pub(in crate::db::executor) const fn grouped_observability(
        &self,
    ) -> Option<GroupedRouteObservability> {
        match self.execution_mode_case {
            ExecutionModeRouteCase::AggregateGrouped => Some(GroupedRouteObservability {
                outcome: GroupedRouteDecisionOutcome::MaterializedBlocked,
                rejection_reason: GroupedRouteRejectionReason::RuntimeDisabled,
            }),
            _ => None,
        }
    }

    // Return the effective physical fetch hint for fallback stream resolution.
    // DESC fallback must disable bounded hints when reverse traversal is unavailable.
    pub(super) const fn fallback_physical_fetch_hint(&self, direction: Direction) -> Option<usize> {
        if direction_allows_physical_fetch_hint(direction, self.desc_physical_reverse_supported()) {
            self.scan_hints.physical_fetch_hint
        } else {
            None
        }
    }

    // True when DESC execution can traverse the physical access path in reverse.
    pub(super) const fn desc_physical_reverse_supported(&self) -> bool {
        self.capabilities.desc_physical_reverse_supported
    }

    // True when secondary-prefix pushdown is enabled for this route.
    pub(super) const fn secondary_fast_path_eligible(&self) -> bool {
        self.secondary_pushdown_applicability.is_eligible()
    }

    // True when the plan shape supports direct PK ordered streaming fast path.
    pub(super) const fn pk_order_fast_path_eligible(&self) -> bool {
        self.capabilities.pk_order_fast_path_eligible
    }

    // True when access shape is streaming-safe for final order semantics.
    pub(super) const fn streaming_access_shape_safe(&self) -> bool {
        self.capabilities.streaming_access_shape_safe
    }

    // True when index-range limit pushdown is enabled for this route.
    pub(super) const fn index_range_limit_fast_path_enabled(&self) -> bool {
        self.index_range_limit_spec.is_some()
    }

    // True when composite aggregate fast-path execution is shape-safe.
    pub(super) const fn composite_aggregate_fast_path_eligible(&self) -> bool {
        self.capabilities.composite_aggregate_fast_path_eligible
    }

    // True when route permits a future `min(field)` fast path.
    pub(super) const fn field_min_fast_path_eligible(&self) -> bool {
        self.capabilities.field_min_fast_path_eligible
    }

    // True when route permits a future `max(field)` fast path.
    pub(super) const fn field_max_fast_path_eligible(&self) -> bool {
        self.capabilities.field_max_fast_path_eligible
    }

    #[cfg(test)]
    pub(super) const fn count_pushdown_access_shape_supported(&self) -> bool {
        self.capabilities.count_pushdown_access_shape_supported
    }

    #[cfg(test)]
    pub(super) const fn index_range_limit_pushdown_shape_eligible(&self) -> bool {
        self.capabilities.index_range_limit_pushdown_shape_eligible
    }

    #[cfg(test)]
    pub(super) const fn bounded_probe_hint_safe(&self) -> bool {
        self.capabilities.bounded_probe_hint_safe
    }

    // Route-owned diagnostic reason for why `min(field)` fast path is ineligible.
    #[cfg(test)]
    pub(super) const fn field_min_fast_path_ineligibility_reason(
        &self,
    ) -> Option<FieldExtremaIneligibilityReason> {
        self.capabilities.field_min_fast_path_ineligibility_reason
    }

    // Route-owned diagnostic reason for why `max(field)` fast path is ineligible.
    #[cfg(test)]
    pub(super) const fn field_max_fast_path_ineligibility_reason(
        &self,
    ) -> Option<FieldExtremaIneligibilityReason> {
        self.capabilities.field_max_fast_path_ineligibility_reason
    }

    // Route-owned fast-path dispatch order. Executors must dispatch using this
    // order instead of introducing ad-hoc aggregate/load micro fast paths.
    pub(super) const fn fast_path_order(&self) -> &'static [FastPathOrder] {
        self.fast_path_order
    }

    // Route-owned bounded probe hint for secondary Min/Max single-step probing.
    // This prevents executor-local hint math from drifting outside routing.
    pub(super) const fn secondary_extrema_probe_fetch_hint(&self) -> Option<usize> {
        self.aggregate_secondary_extrema_probe_fetch_hint
    }

    const fn for_mutation(capabilities: RouteCapabilities) -> Self {
        Self {
            direction: Direction::Asc,
            continuation_mode: ContinuationMode::Initial,
            window: RouteWindowPlan {
                effective_offset: 0,
                limit: None,
                keep_count: None,
                fetch_count: None,
            },
            execution_mode: ExecutionMode::Materialized,
            execution_mode_case: ExecutionModeRouteCase::Load,
            secondary_pushdown_applicability: PushdownApplicability::NotApplicable,
            index_range_limit_spec: None,
            capabilities,
            fast_path_order: &MUTATION_FAST_PATH_ORDER,
            aggregate_secondary_extrema_probe_fetch_hint: None,
            scan_hints: ScanHintPlan {
                physical_fetch_hint: None,
                load_scan_budget_hint: None,
            },
            aggregate_fold_mode: AggregateFoldMode::ExistingRows,
        }
    }
}

///
/// FastPathOrder
///
/// Shared fast-path precedence model used by load and aggregate routing.
/// Routing implementations remain separate, but they iterate one canonical order.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FastPathOrder {
    PrimaryKey,
    SecondaryPrefix,
    PrimaryScan,
    IndexRange,
    Composite,
}

// Contract: fast-path precedence is a stability boundary. Any change here must
// be intentional, accompanied by route-order tests, and called out in changelog.
pub(super) const LOAD_FAST_PATH_ORDER: [FastPathOrder; 3] = [
    FastPathOrder::PrimaryKey,
    FastPathOrder::SecondaryPrefix,
    FastPathOrder::IndexRange,
];

// Contract: aggregate dispatch precedence is ordered for semantic and
// performance stability. Do not reorder casually.
pub(super) const AGGREGATE_FAST_PATH_ORDER: [FastPathOrder; 5] = [
    FastPathOrder::PrimaryKey,
    FastPathOrder::SecondaryPrefix,
    FastPathOrder::PrimaryScan,
    FastPathOrder::IndexRange,
    FastPathOrder::Composite,
];

// Contract: grouped aggregate routes are materialized-only in this audit pass
// and must not participate in scalar aggregate fast-path dispatch.
pub(super) const GROUPED_AGGREGATE_FAST_PATH_ORDER: [FastPathOrder; 0] = [];

// Contract: mutation routes are materialized-only and do not participate in
// load/aggregate fast-path precedence.
pub(super) const MUTATION_FAST_PATH_ORDER: [FastPathOrder; 0] = [];

/// Iterate route-owned fast-path precedence through a shared verify+execute gate.
///
/// Verification runs first for each route; execution is attempted only when
/// verification returns a marker. Returns the first successful execution hit.
pub(in crate::db::executor) fn try_first_verified_fast_path_hit<T, M, V, E>(
    fast_path_order: &[FastPathOrder],
    mut verify_route: V,
    mut execute_verified_route: E,
) -> Result<Option<T>, InternalError>
where
    V: FnMut(FastPathOrder) -> Result<Option<M>, InternalError>,
    E: FnMut(M) -> Result<Option<T>, InternalError>,
{
    for route in fast_path_order.iter().copied() {
        let Some(marker) = verify_route(route)? else {
            continue;
        };
        if let Some(hit) = execute_verified_route(marker)? {
            return Ok(Some(hit));
        }
    }

    Ok(None)
}

///
/// RoutedKeyStreamRequest
///
/// Canonical stream-construction request variants for route-owned key-stream
/// resolution across load and aggregate execution paths.
///

pub(in crate::db::executor) enum RoutedKeyStreamRequest<'a, K> {
    AccessPlan(AccessPlanStreamRequest<'a, K>),
    AccessPath {
        access: &'a AccessPath<K>,
        constraints: IndexStreamConstraints<'a>,
        direction: Direction,
        hints: StreamExecutionHints<'a>,
    },
}

///
/// RouteIntent
///

enum RouteIntent {
    Load,
    Aggregate {
        spec: AggregateSpec,
        aggregate_force_materialized_due_to_predicate_uncertainty: bool,
    },
    AggregateGrouped {
        aggregate_force_materialized_due_to_predicate_uncertainty: bool,
    },
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
/// GroupedRouteDecisionOutcome
///
/// Grouped route decision outcome surface for grouped observability scaffolding.
/// This keeps grouped route diagnostics explicit while grouped runtime remains
/// disabled in `0.32.x`.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum GroupedRouteDecisionOutcome {
    MaterializedBlocked,
}

///
/// GroupedRouteRejectionReason
///
/// Grouped route rejection taxonomy for grouped observability scaffolding.
/// Runtime-enabled grouped execution should replace this placeholder reason set.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum GroupedRouteRejectionReason {
    RuntimeDisabled,
}

///
/// GroupedRouteObservability
///
/// Grouped route observability payload locked for grouped-readiness.
/// Carries one explicit route outcome and one grouped rejection reason.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct GroupedRouteObservability {
    outcome: GroupedRouteDecisionOutcome,
    rejection_reason: GroupedRouteRejectionReason,
}

impl GroupedRouteObservability {
    #[must_use]
    pub(in crate::db::executor) const fn outcome(self) -> GroupedRouteDecisionOutcome {
        self.outcome
    }

    #[must_use]
    pub(in crate::db::executor) const fn rejection_reason(self) -> GroupedRouteRejectionReason {
        self.rejection_reason
    }
}

///
/// FieldExtremaIneligibilityReason
///
/// Canonical route-owned reason taxonomy for field-extrema ineligibility.
/// These reasons are stable test/explain diagnostics for future feature enablement.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FieldExtremaIneligibilityReason {
    SpecMissing,
    AggregateKindMismatch,
    TargetFieldMissing,
    UnknownTargetField,
    UnsupportedFieldType,
    DistinctNotSupported,
    PageLimitNotSupported,
    OffsetNotSupported,
    CompositePathNotSupported,
    NoMatchingIndex,
    DescReverseTraversalNotSupported,
}

///
/// FieldExtremaEligibility
///
/// Route-owned eligibility snapshot for one field-extrema aggregate shape.
/// Carries both the boolean decision and the first ineligibility reason.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FieldExtremaEligibility {
    eligible: bool,
    ineligibility_reason: Option<FieldExtremaIneligibilityReason>,
}

///
/// RouteCapabilities
///
/// Canonical derived capability snapshot for one logical plan and direction.
/// Route planning derives this once, then consumes it for eligibility and hint
/// decisions to reduce drift across helpers.
///

#[expect(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RouteCapabilities {
    streaming_access_shape_safe: bool,
    pk_order_fast_path_eligible: bool,
    desc_physical_reverse_supported: bool,
    count_pushdown_access_shape_supported: bool,
    index_range_limit_pushdown_shape_eligible: bool,
    composite_aggregate_fast_path_eligible: bool,
    bounded_probe_hint_safe: bool,
    field_min_fast_path_eligible: bool,
    field_max_fast_path_eligible: bool,
    field_min_fast_path_ineligibility_reason: Option<FieldExtremaIneligibilityReason>,
    field_max_fast_path_ineligibility_reason: Option<FieldExtremaIneligibilityReason>,
}

#[cfg(test)]
pub(in crate::db::executor) const fn route_capability_flag_count_guard() -> usize {
    let _ = RouteCapabilities {
        streaming_access_shape_safe: false,
        pk_order_fast_path_eligible: false,
        desc_physical_reverse_supported: false,
        count_pushdown_access_shape_supported: false,
        index_range_limit_pushdown_shape_eligible: false,
        composite_aggregate_fast_path_eligible: false,
        bounded_probe_hint_safe: false,
        field_min_fast_path_eligible: false,
        field_max_fast_path_eligible: false,
        field_min_fast_path_ineligibility_reason: None,
        field_max_fast_path_ineligibility_reason: None,
    };

    9
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

const fn direction_allows_physical_fetch_hint(
    direction: Direction,
    desc_physical_reverse_supported: bool,
) -> bool {
    !matches!(direction, Direction::Desc) || desc_physical_reverse_supported
}
