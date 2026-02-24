#[cfg(test)]
mod tests;

use crate::{
    db::{
        executor::{
            AccessPlanStreamRequest, Context, IndexStreamConstraints, OrderedKeyStreamBox,
            StreamExecutionHints,
            fold::{AggregateFoldMode, AggregateKind, AggregateSpec},
            load::{LoadExecutor, aggregate_field::AggregateFieldValueError},
        },
        index::RawIndexKey,
        query::{
            plan::{
                AccessPath, AccessPlan, CursorBoundary, Direction, LogicalPlan,
                compute_page_window, validate::PushdownApplicability,
            },
            predicate::PredicateFieldSlots,
        },
    },
    error::InternalError,
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
};

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
/// ExecutionRoutePlan
///
/// Canonical route decision payload for load/aggregate execution.
/// This is the single boundary that owns execution mode, pushdown eligibility,
/// DESC physical reverse-traversal capability, and scan-hint decisions.
///

#[derive(Clone)]
pub(super) struct ExecutionRoutePlan {
    pub(super) execution_mode: ExecutionMode,
    secondary_pushdown_applicability: PushdownApplicability,
    pub(super) index_range_limit_spec: Option<IndexRangeLimitSpec>,
    capabilities: RouteCapabilities,
    fast_path_order: &'static [FastPathOrder],
    aggregate_secondary_extrema_probe_fetch_hint: Option<usize>,
    pub(super) scan_hints: ScanHintPlan,
    pub(super) aggregate_fold_mode: AggregateFoldMode,
}

impl ExecutionRoutePlan {
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
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) const fn field_min_fast_path_eligible(&self) -> bool {
        self.capabilities.field_min_fast_path_eligible
    }

    // True when route permits a future `max(field)` fast path.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) const fn field_max_fast_path_eligible(&self) -> bool {
        self.capabilities.field_max_fast_path_eligible
    }

    // Route-owned diagnostic reason for why `min(field)` fast path is ineligible.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) const fn field_min_fast_path_ineligibility_reason(
        &self,
    ) -> Option<FieldExtremaIneligibilityReason> {
        self.capabilities.field_min_fast_path_ineligibility_reason
    }

    // Route-owned diagnostic reason for why `max(field)` fast path is ineligible.
    #[cfg_attr(not(test), allow(dead_code))]
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
            execution_mode: ExecutionMode::Materialized,
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

// Contract: mutation routes are materialized-only and do not participate in
// load/aggregate fast-path precedence.
pub(super) const MUTATION_FAST_PATH_ORDER: [FastPathOrder; 0] = [];

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
    Load {
        direction: Direction,
    },
    Aggregate {
        direction: Direction,
        spec: AggregateSpec,
    },
}

///
/// ExecutionModeRouteCase
///
/// Canonical route-case partition for execution-mode decisions.
/// This keeps streaming/materialized branching explicit under one gate.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExecutionModeRouteCase {
    Load,
    AggregateCount,
    AggregateNonCount,
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

const fn direction_allows_physical_fetch_hint(
    direction: Direction,
    desc_physical_reverse_supported: bool,
) -> bool {
    !matches!(direction, Direction::Desc) || desc_physical_reverse_supported
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Resolve one routed key stream through the canonical stream-construction
    /// facade so route consumers do not call context stream builders directly.
    pub(in crate::db::executor) fn resolve_routed_key_stream(
        ctx: &Context<'_, E>,
        request: RoutedKeyStreamRequest<'_, E::Key>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        match request {
            RoutedKeyStreamRequest::AccessPlan(stream_request) => {
                ctx.ordered_key_stream_from_access_plan_with_index_range_anchor(stream_request)
            }
            RoutedKeyStreamRequest::AccessPath {
                access,
                constraints,
                direction,
                hints,
            } => ctx.ordered_key_stream_from_access(access, constraints, direction, hints),
        }
    }

    // ------------------------------------------------------------------
    // Capability derivation
    // ------------------------------------------------------------------

    // Derive a canonical route capability snapshot for one plan + direction.
    fn derive_route_capabilities(
        plan: &LogicalPlan<E::Key>,
        direction: Direction,
        aggregate_spec: Option<&AggregateSpec>,
    ) -> RouteCapabilities {
        let field_min_eligibility =
            Self::assess_field_min_fast_path_eligibility(plan, direction, aggregate_spec);
        let field_max_eligibility =
            Self::assess_field_max_fast_path_eligibility(plan, direction, aggregate_spec);

        RouteCapabilities {
            streaming_access_shape_safe: plan.is_streaming_access_shape_safe::<E>(),
            pk_order_fast_path_eligible: Self::pk_order_stream_fast_path_shape_supported(plan),
            desc_physical_reverse_supported: Self::is_desc_physical_reverse_traversal_supported(
                &plan.access,
                direction,
            ),
            count_pushdown_access_shape_supported: Self::count_pushdown_access_shape_supported(
                &plan.access,
            ),
            index_range_limit_pushdown_shape_eligible:
                Self::is_index_range_limit_pushdown_shape_eligible(plan),
            composite_aggregate_fast_path_eligible: Self::is_composite_aggregate_fast_path_eligible(
                plan,
            ),
            bounded_probe_hint_safe: Self::bounded_probe_hint_is_safe(plan),
            field_min_fast_path_eligible: field_min_eligibility.eligible,
            field_max_fast_path_eligible: field_max_eligibility.eligible,
            field_min_fast_path_ineligibility_reason: field_min_eligibility.ineligibility_reason,
            field_max_fast_path_ineligibility_reason: field_max_eligibility.ineligibility_reason,
        }
    }

    // ------------------------------------------------------------------
    // Route plan derivation
    // ------------------------------------------------------------------

    // Build canonical execution routing for load execution.
    pub(super) fn build_execution_route_plan_for_load(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        probe_fetch_hint: Option<usize>,
        direction: Direction,
    ) -> Result<ExecutionRoutePlan, InternalError> {
        Self::validate_pk_fast_path_boundary_if_applicable(plan, cursor_boundary)?;

        Ok(Self::build_execution_route_plan(
            plan,
            cursor_boundary,
            index_range_anchor,
            probe_fetch_hint,
            RouteIntent::Load { direction },
        ))
    }

    // Build canonical execution routing for mutation execution.
    pub(super) fn build_execution_route_plan_for_mutation(
        plan: &LogicalPlan<E::Key>,
    ) -> Result<ExecutionRoutePlan, InternalError> {
        if !plan.mode.is_delete() {
            return Err(InternalError::query_executor_invariant(
                "mutation route planning requires delete plans",
            ));
        }

        let capabilities = Self::derive_route_capabilities(plan, Direction::Asc, None);

        Ok(ExecutionRoutePlan::for_mutation(capabilities))
    }

    pub(in crate::db::executor) fn validate_mutation_route_stage(
        plan: &LogicalPlan<E::Key>,
    ) -> Result<(), InternalError> {
        let _mutation_route_plan = Self::build_execution_route_plan_for_mutation(plan)?;

        Ok(())
    }

    // Build canonical execution routing for aggregate execution.
    #[cfg(test)]
    pub(super) fn build_execution_route_plan_for_aggregate(
        plan: &LogicalPlan<E::Key>,
        kind: AggregateKind,
        direction: Direction,
    ) -> ExecutionRoutePlan {
        Self::build_execution_route_plan_for_aggregate_spec(
            plan,
            AggregateSpec::for_terminal(kind),
            direction,
        )
    }

    // Build canonical execution routing for aggregate execution via spec.
    pub(super) fn build_execution_route_plan_for_aggregate_spec(
        plan: &LogicalPlan<E::Key>,
        spec: AggregateSpec,
        direction: Direction,
    ) -> ExecutionRoutePlan {
        Self::build_execution_route_plan(
            plan,
            None,
            None,
            None,
            RouteIntent::Aggregate { direction, spec },
        )
    }

    // Shared route gate for load + aggregate execution.
    #[expect(clippy::too_many_lines)]
    fn build_execution_route_plan(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        probe_fetch_hint: Option<usize>,
        intent: RouteIntent,
    ) -> ExecutionRoutePlan {
        let secondary_pushdown_applicability =
            crate::db::query::plan::validate::assess_secondary_order_pushdown_if_applicable_validated(
                E::MODEL,
                plan,
            );
        let (direction, aggregate_spec, fast_path_order, is_load_intent) = match intent {
            RouteIntent::Load { direction } => (direction, None, &LOAD_FAST_PATH_ORDER[..], true),
            RouteIntent::Aggregate { direction, spec } => {
                (direction, Some(spec), &AGGREGATE_FAST_PATH_ORDER[..], false)
            }
        };
        let kind = aggregate_spec.as_ref().map(AggregateSpec::kind);
        debug_assert!(
            (kind.is_none() && fast_path_order == LOAD_FAST_PATH_ORDER.as_slice())
                || (kind.is_some() && fast_path_order == AGGREGATE_FAST_PATH_ORDER.as_slice()),
            "route invariant: route intent must map to the canonical fast-path order contract",
        );
        let capabilities =
            Self::derive_route_capabilities(plan, direction, aggregate_spec.as_ref());
        let count_pushdown_eligible = kind.is_some_and(|aggregate_kind| {
            Self::is_count_pushdown_eligible(aggregate_kind, capabilities)
        });
        let count_terminal = matches!(kind, Some(AggregateKind::Count));

        // Aggregate probes must not assume DESC physical reverse traversal
        // when the access shape cannot emit descending order natively.
        let count_pushdown_probe_fetch_hint = if count_pushdown_eligible {
            Self::count_pushdown_fetch_hint(plan, capabilities)
        } else {
            None
        };
        let aggregate_terminal_probe_fetch_hint = aggregate_spec
            .as_ref()
            .and_then(|spec| Self::aggregate_probe_fetch_hint(plan, spec, direction, capabilities));
        let aggregate_physical_fetch_hint =
            count_pushdown_probe_fetch_hint.or(aggregate_terminal_probe_fetch_hint);
        let aggregate_secondary_extrema_probe_fetch_hint = match kind {
            Some(AggregateKind::Min | AggregateKind::Max) => aggregate_physical_fetch_hint,
            Some(
                AggregateKind::Count
                | AggregateKind::Exists
                | AggregateKind::First
                | AggregateKind::Last,
            )
            | None => None,
        };
        let physical_fetch_hint = match kind {
            Some(_) => aggregate_physical_fetch_hint,
            None => probe_fetch_hint,
        };
        let load_scan_budget_hint = if is_load_intent {
            Self::load_scan_budget_hint(plan, cursor_boundary, capabilities)
        } else {
            None
        };

        let index_range_limit_spec = if count_terminal {
            // COUNT fold-mode discipline: non-count pushdowns must not route COUNT
            // through non-COUNT streaming fast paths.
            None
        } else {
            Self::assess_index_range_limit_pushdown(
                plan,
                cursor_boundary,
                index_range_anchor,
                physical_fetch_hint,
                capabilities,
            )
        };
        if is_load_intent
            && let (Some(index_range_limit_spec), Some(load_scan_budget_hint)) =
                (index_range_limit_spec, load_scan_budget_hint)
        {
            debug_assert_eq!(
                index_range_limit_spec.fetch, load_scan_budget_hint,
                "route invariant: load index-range fetch hint and load scan budget must remain aligned"
            );
        }
        debug_assert!(
            index_range_limit_spec.is_none()
                || capabilities.index_range_limit_pushdown_shape_eligible,
            "route invariant: index-range limit spec requires pushdown-eligible shape",
        );
        debug_assert!(
            !count_pushdown_eligible
                || matches!(kind, Some(AggregateKind::Count))
                    && capabilities.streaming_access_shape_safe
                    && capabilities.count_pushdown_access_shape_supported,
            "route invariant: COUNT pushdown eligibility must match COUNT-safe capability set",
        );
        debug_assert!(
            load_scan_budget_hint.is_none()
                || cursor_boundary.is_none() && capabilities.streaming_access_shape_safe,
            "route invariant: load scan-budget hints require non-continuation streaming-safe shape",
        );
        let aggregate_fold_mode = if count_terminal {
            AggregateFoldMode::KeysOnly
        } else {
            AggregateFoldMode::ExistingRows
        };

        let execution_case = match kind {
            None => ExecutionModeRouteCase::Load,
            Some(AggregateKind::Count) => ExecutionModeRouteCase::AggregateCount,
            Some(
                AggregateKind::Exists
                | AggregateKind::Min
                | AggregateKind::Max
                | AggregateKind::First
                | AggregateKind::Last,
            ) => ExecutionModeRouteCase::AggregateNonCount,
        };
        let execution_mode = match execution_case {
            ExecutionModeRouteCase::Load => {
                if capabilities.streaming_access_shape_safe {
                    ExecutionMode::Streaming
                } else {
                    ExecutionMode::Materialized
                }
            }
            ExecutionModeRouteCase::AggregateCount => {
                if count_pushdown_eligible {
                    ExecutionMode::Streaming
                } else {
                    ExecutionMode::Materialized
                }
            }
            ExecutionModeRouteCase::AggregateNonCount => {
                if capabilities.streaming_access_shape_safe
                    || secondary_pushdown_applicability.is_eligible()
                    || index_range_limit_spec.is_some()
                {
                    ExecutionMode::Streaming
                } else {
                    ExecutionMode::Materialized
                }
            }
        };
        debug_assert!(
            kind.is_none()
                || index_range_limit_spec.is_none()
                || matches!(execution_mode, ExecutionMode::Streaming),
            "route invariant: aggregate index-range limit pushdown must execute in streaming mode",
        );
        debug_assert!(
            !count_terminal || index_range_limit_spec.is_none(),
            "route invariant: COUNT terminals must not route through index-range limit pushdown",
        );
        debug_assert!(
            capabilities.bounded_probe_hint_safe
                || aggregate_physical_fetch_hint.is_none()
                || plan.page.as_ref().is_some_and(|page| page.limit == Some(0)),
            "route invariant: DISTINCT+offset must disable bounded aggregate probe hints",
        );

        ExecutionRoutePlan {
            execution_mode,
            secondary_pushdown_applicability,
            index_range_limit_spec,
            capabilities,
            fast_path_order,
            aggregate_secondary_extrema_probe_fetch_hint,
            scan_hints: ScanHintPlan {
                physical_fetch_hint,
                load_scan_budget_hint,
            },
            aggregate_fold_mode,
        }
    }

    // ------------------------------------------------------------------
    // Hint and pushdown gates
    // ------------------------------------------------------------------

    // Assess index-range limit pushdown once for this execution and produce
    // the bounded fetch spec when all eligibility gates pass.
    fn assess_index_range_limit_pushdown(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        probe_fetch_hint: Option<usize>,
        capabilities: RouteCapabilities,
    ) -> Option<IndexRangeLimitSpec> {
        if !capabilities.index_range_limit_pushdown_shape_eligible {
            return None;
        }
        if cursor_boundary.is_some() && index_range_anchor.is_none() {
            return None;
        }
        if let Some(fetch) = probe_fetch_hint {
            if plan.predicate.is_some() && !Self::residual_predicate_pushdown_fetch_is_safe(fetch) {
                return None;
            }

            return Some(IndexRangeLimitSpec { fetch });
        }

        let page = plan.page.as_ref()?;
        let limit = page.limit?;
        if limit == 0 {
            return Some(IndexRangeLimitSpec { fetch: 0 });
        }

        let fetch = Self::page_window_fetch_count(plan, cursor_boundary, true)?;
        if plan.predicate.is_some() && !Self::residual_predicate_pushdown_fetch_is_safe(fetch) {
            return None;
        }

        Some(IndexRangeLimitSpec { fetch })
    }

    // Shared load-page scan-budget hint gate.
    fn load_scan_budget_hint(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if cursor_boundary.is_some() {
            return None;
        }
        if !capabilities.streaming_access_shape_safe {
            return None;
        }

        Self::page_window_fetch_count(plan, cursor_boundary, true)
    }

    // Shared bounded-probe safety gate for aggregate key-stream hints.
    // Contract:
    // - DISTINCT + offset must remain unbounded so deduplication is applied
    //   before offset consumption without risking under-fetch.
    // - If dedup/projection/composite semantics evolve, this gate is the first
    //   place to re-evaluate bounded-probe correctness.
    fn bounded_probe_hint_is_safe(plan: &LogicalPlan<E::Key>) -> bool {
        let offset = usize::try_from(plan.effective_page_offset(None)).unwrap_or(usize::MAX);
        !(plan.distinct && offset > 0)
    }

    // Residual predicates are allowed for index-range limit pushdown only when
    // the bounded fetch remains small. This caps amplification risk when the
    // post-access residual filter rejects many bounded candidates.
    const fn residual_predicate_pushdown_fetch_is_safe(fetch: usize) -> bool {
        fetch <= Self::residual_predicate_pushdown_fetch_cap()
    }

    const fn residual_predicate_pushdown_fetch_cap() -> usize {
        256
    }

    // Determine whether every compiled predicate field slot is available on
    // the active single-path index access shape.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn predicate_slots_fully_covered_by_index_path(
        access: &AccessPlan<E::Key>,
        predicate_slots: Option<&PredicateFieldSlots>,
    ) -> bool {
        let Some(predicate_slots) = predicate_slots else {
            return false;
        };
        let required = predicate_slots.required_slots();
        if required.is_empty() {
            return false;
        }
        let Some(mut index_slots) = Self::resolved_index_slots_for_access_path(access) else {
            return false;
        };
        index_slots.sort_unstable();
        index_slots.dedup();

        required
            .iter()
            .all(|slot| index_slots.binary_search(slot).is_ok())
    }

    // Resolve index fields for a single-path index access shape to entity slots.
    pub(super) fn resolved_index_slots_for_access_path(
        access: &AccessPlan<E::Key>,
    ) -> Option<Vec<usize>> {
        let path = access.as_path()?;
        let index_fields = match path {
            AccessPath::IndexPrefix { index, .. } | AccessPath::IndexRange { index, .. } => {
                index.fields
            }
            AccessPath::ByKey(_)
            | AccessPath::ByKeys(_)
            | AccessPath::KeyRange { .. }
            | AccessPath::FullScan => return None,
        };

        let mut slots = Vec::with_capacity(index_fields.len());
        for field_name in index_fields {
            let slot = resolve_field_slot(E::MODEL, field_name)?;
            slots.push(slot);
        }

        Some(slots)
    }

    // ------------------------------------------------------------------
    // Access-shape eligibility helpers
    // ------------------------------------------------------------------

    const fn count_pushdown_path_shape_supported(path: &AccessPath<E::Key>) -> bool {
        matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. })
    }

    fn count_pushdown_access_shape_supported(access: &AccessPlan<E::Key>) -> bool {
        match access {
            AccessPlan::Path(path) => Self::count_pushdown_path_shape_supported(path),
            AccessPlan::Union(_) | AccessPlan::Intersection(_) => false,
        }
    }

    // Route-owned gate for PK full-scan/key-range ordered fast-path eligibility.
    pub(super) fn pk_order_stream_fast_path_shape_supported(plan: &LogicalPlan<E::Key>) -> bool {
        if !plan.mode.is_load() {
            return false;
        }

        let supports_pk_stream_access = plan
            .access
            .as_path()
            .is_some_and(AccessPath::is_full_scan_or_key_range);
        if !supports_pk_stream_access {
            return false;
        }

        let Some(order) = plan.order.as_ref() else {
            return false;
        };

        order.fields.len() == 1 && order.fields[0].0 == E::MODEL.primary_key.name
    }

    const fn is_count_pushdown_eligible(
        kind: AggregateKind,
        capabilities: RouteCapabilities,
    ) -> bool {
        matches!(kind, AggregateKind::Count)
            && capabilities.streaming_access_shape_safe
            && capabilities.count_pushdown_access_shape_supported
    }

    fn count_pushdown_fetch_hint(
        plan: &LogicalPlan<E::Key>,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if !capabilities.bounded_probe_hint_safe {
            return None;
        }

        Self::page_window_fetch_count(plan, None, false)
    }

    fn aggregate_probe_fetch_hint(
        plan: &LogicalPlan<E::Key>,
        spec: &AggregateSpec,
        direction: Direction,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if spec.target_field().is_some() {
            return None;
        }
        let kind = spec.kind();
        if !matches!(
            kind,
            AggregateKind::Exists
                | AggregateKind::Min
                | AggregateKind::Max
                | AggregateKind::First
                | AggregateKind::Last
        ) {
            return None;
        }
        if plan.page.as_ref().is_some_and(|page| page.limit == Some(0)) {
            return Some(0);
        }
        if !direction_allows_physical_fetch_hint(
            direction,
            capabilities.desc_physical_reverse_supported,
        ) {
            return None;
        }
        if !capabilities.bounded_probe_hint_safe {
            return None;
        }

        let offset = usize::try_from(plan.effective_page_offset(None)).unwrap_or(usize::MAX);
        let page_limit = plan
            .page
            .as_ref()
            .and_then(|page| page.limit)
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

        match kind {
            AggregateKind::Exists | AggregateKind::First => Some(offset.saturating_add(1)),
            AggregateKind::Min if direction == Direction::Asc => Some(offset.saturating_add(1)),
            AggregateKind::Max if direction == Direction::Desc => Some(offset.saturating_add(1)),
            AggregateKind::Last => page_limit.map(|limit| offset.saturating_add(limit)),
            _ => None,
        }
    }

    // Placeholder assessment for future `min(field)` fast paths.
    // Intentionally ineligible in 0.24.x while field-extrema semantics are finalized.
    fn assess_field_min_fast_path_eligibility(
        plan: &LogicalPlan<E::Key>,
        direction: Direction,
        aggregate_spec: Option<&AggregateSpec>,
    ) -> FieldExtremaEligibility {
        Self::assess_field_extrema_fast_path_eligibility(
            plan,
            direction,
            aggregate_spec,
            AggregateKind::Min,
        )
    }

    // Placeholder assessment for future `max(field)` fast paths.
    // Intentionally ineligible in 0.24.x while field-extrema semantics are finalized.
    fn assess_field_max_fast_path_eligibility(
        plan: &LogicalPlan<E::Key>,
        direction: Direction,
        aggregate_spec: Option<&AggregateSpec>,
    ) -> FieldExtremaEligibility {
        Self::assess_field_extrema_fast_path_eligibility(
            plan,
            direction,
            aggregate_spec,
            AggregateKind::Max,
        )
    }

    // Shared scaffolding for future field-extrema eligibility routing.
    // Contract:
    // - field-extrema fast path is enabled only for index-leading
    //   access shapes with full-window semantics.
    // - unsupported shapes return explicit route-owned reasons.
    fn assess_field_extrema_fast_path_eligibility(
        plan: &LogicalPlan<E::Key>,
        direction: Direction,
        aggregate_spec: Option<&AggregateSpec>,
        extrema_kind: AggregateKind,
    ) -> FieldExtremaEligibility {
        let Some(spec) = aggregate_spec else {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::SpecMissing),
            };
        };
        if spec.kind() != extrema_kind {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::AggregateKindMismatch),
            };
        }
        let Some(target_field) = spec.target_field() else {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::TargetFieldMissing),
            };
        };
        let field_validation =
            crate::db::executor::load::aggregate_field::validate_orderable_aggregate_target_field::<
                E,
            >(target_field);
        if let Err(err) = field_validation {
            let reason = match err {
                AggregateFieldValueError::UnknownField { .. } => {
                    FieldExtremaIneligibilityReason::UnknownTargetField
                }
                AggregateFieldValueError::UnsupportedFieldKind { .. }
                | AggregateFieldValueError::MissingFieldValue { .. }
                | AggregateFieldValueError::FieldValueTypeMismatch { .. }
                | AggregateFieldValueError::IncomparableFieldValues { .. } => {
                    FieldExtremaIneligibilityReason::UnsupportedFieldType
                }
            };
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(reason),
            };
        }
        if plan.distinct {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::DistinctNotSupported),
            };
        }
        let offset = usize::try_from(plan.effective_page_offset(None)).unwrap_or(usize::MAX);
        if offset > 0 {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::OffsetNotSupported),
            };
        }
        if Self::is_composite_access_shape(&plan.access) {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(
                    FieldExtremaIneligibilityReason::CompositePathNotSupported,
                ),
            };
        }
        if !Self::field_extrema_target_has_matching_index(plan, target_field) {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::NoMatchingIndex),
            };
        }
        if !direction_allows_physical_fetch_hint(
            direction,
            Self::is_desc_physical_reverse_traversal_supported(&plan.access, direction),
        ) {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(
                    FieldExtremaIneligibilityReason::DescReverseTraversalNotSupported,
                ),
            };
        }
        if plan.page.as_ref().is_some_and(|page| page.limit.is_some()) {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::PageLimitNotSupported),
            };
        }

        FieldExtremaEligibility {
            eligible: true,
            ineligibility_reason: None,
        }
    }

    fn field_extrema_target_has_matching_index(
        plan: &LogicalPlan<E::Key>,
        target_field: &str,
    ) -> bool {
        let Some(path) = plan.access.as_path() else {
            return false;
        };
        if target_field == E::MODEL.primary_key.name {
            return matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. });
        }

        match path {
            AccessPath::IndexPrefix { index, .. } | AccessPath::IndexRange { index, .. } => index
                .fields
                .first()
                .is_some_and(|field| *field == target_field),
            AccessPath::ByKey(_)
            | AccessPath::ByKeys(_)
            | AccessPath::KeyRange { .. }
            | AccessPath::FullScan => false,
        }
    }

    fn is_desc_physical_reverse_traversal_supported(
        access: &AccessPlan<E::Key>,
        direction: Direction,
    ) -> bool {
        if !matches!(direction, Direction::Desc) {
            return false;
        }

        Self::access_supports_reverse_traversal(access)
    }

    fn access_supports_reverse_traversal(access: &AccessPlan<E::Key>) -> bool {
        match access {
            AccessPlan::Path(path) => Self::path_supports_reverse_traversal(path),
            AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
                children.iter().all(Self::access_supports_reverse_traversal)
            }
        }
    }

    // Composite aggregate fast-path eligibility must stay explicit:
    // - composite access shape only (`Union` / `Intersection`)
    // - no residual predicate filtering
    // - no post-access reordering
    fn is_composite_aggregate_fast_path_eligible(plan: &LogicalPlan<E::Key>) -> bool {
        if !Self::is_composite_access_shape(&plan.access) {
            return false;
        }

        let metadata = plan.budget_safety_metadata::<E>();
        if metadata.has_residual_filter {
            return false;
        }
        if metadata.requires_post_access_sort {
            return false;
        }

        true
    }

    // Shared page-window fetch computation for bounded routing hints.
    fn page_window_fetch_count(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        needs_extra: bool,
    ) -> Option<usize> {
        let page = plan.page.as_ref()?;
        let limit = page.limit?;

        Some(
            compute_page_window(
                plan.effective_page_offset(cursor_boundary),
                limit,
                needs_extra,
            )
            .fetch_count,
        )
    }

    const fn path_supports_reverse_traversal(path: &AccessPath<E::Key>) -> bool {
        matches!(
            path,
            AccessPath::ByKey(_)
                | AccessPath::KeyRange { .. }
                | AccessPath::IndexPrefix { .. }
                | AccessPath::IndexRange { .. }
                | AccessPath::FullScan
        )
    }

    const fn is_composite_access_shape(access: &AccessPlan<E::Key>) -> bool {
        matches!(access, AccessPlan::Union(_) | AccessPlan::Intersection(_))
    }

    // Route-owned shape gate for index-range limited pushdown eligibility.
    fn is_index_range_limit_pushdown_shape_eligible(plan: &LogicalPlan<E::Key>) -> bool {
        let Some((index, prefix, _, _)) = plan.access.as_index_range_path() else {
            return false;
        };
        let index_fields = index.fields;
        let prefix_len = prefix.len();

        if let Some(order) = plan.order.as_ref()
            && !order.fields.is_empty()
        {
            let Some(expected_direction) = order.fields.last().map(|(_, direction)| *direction)
            else {
                return false;
            };
            if order
                .fields
                .iter()
                .any(|(_, direction)| *direction != expected_direction)
            {
                return false;
            }

            let mut expected =
                Vec::with_capacity(index_fields.len().saturating_sub(prefix_len) + 1);
            expected.extend(index_fields.iter().skip(prefix_len).copied());
            expected.push(E::MODEL.primary_key.name);
            if order.fields.len() != expected.len() {
                return false;
            }
            if !order
                .fields
                .iter()
                .map(|(field, _)| field.as_str())
                .eq(expected)
            {
                return false;
            }
        }

        true
    }
}
