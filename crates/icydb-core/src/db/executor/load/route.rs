use crate::{
    db::{
        executor::{
            fold::{AggregateFoldMode, AggregateKind, AggregateSpec},
            load::{IndexRangeLimitSpec, LoadExecutor, aggregate_field::AggregateFieldValueError},
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
    StreamingAccessShapeNotSupported,
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

#[allow(clippy::struct_excessive_bools)]
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
            return Some(IndexRangeLimitSpec { fetch });
        }

        let page = plan.page.as_ref()?;
        let limit = page.limit?;
        if limit == 0 {
            return Some(IndexRangeLimitSpec { fetch: 0 });
        }

        let fetch = Self::page_window_fetch_count(plan, cursor_boundary, true)?;

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

    // Determine whether every compiled predicate field slot is available on
    // the active single-path index access shape.
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
    // - field-extrema fast path is enabled only for streaming-safe index-leading
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
        if !plan.is_streaming_access_shape_safe::<E>() {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(
                    FieldExtremaIneligibilityReason::StreamingAccessShapeNotSupported,
                ),
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
        if plan.predicate.is_some() {
            return false;
        }

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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        AGGREGATE_FAST_PATH_ORDER, ExecutionMode, ExecutionModeRouteCase, FastPathOrder,
        FieldExtremaIneligibilityReason, LOAD_FAST_PATH_ORDER, RouteCapabilities,
    };
    use crate::{
        db::{
            executor::{
                fold::{AggregateFoldMode, AggregateKind, AggregateSpec},
                load::LoadExecutor,
            },
            query::{
                ReadConsistency,
                plan::{
                    AccessPath, AccessPlan, CursorBoundary, Direction, LogicalPlan, OrderDirection,
                    OrderSpec, PageSpec,
                },
                predicate::{Predicate, PredicateFieldSlots},
            },
        },
        model::{field::FieldKind, index::IndexModel},
        traits::Path,
        types::Ulid,
        value::Value,
    };
    use icydb_derive::FieldProjection;
    use serde::{Deserialize, Serialize};
    use std::ops::Bound;

    const ROUTE_FEATURE_SOFT_BUDGET_DELTA: usize = 1;
    const ROUTE_CAPABILITY_FLAG_BASELINE_0247: usize = 9;
    const ROUTE_EXECUTION_MODE_CASE_BASELINE_0246: usize = 3;
    const ROUTE_EXECUTION_MODE_CASES_0246: [ExecutionModeRouteCase; 3] = [
        ExecutionModeRouteCase::Load,
        ExecutionModeRouteCase::AggregateCount,
        ExecutionModeRouteCase::AggregateNonCount,
    ];

    const fn route_capability_flag_count_guard() -> usize {
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

    fn route_execution_mode_case_count_guard() -> usize {
        ROUTE_EXECUTION_MODE_CASES_0246.len()
    }

    fn assert_no_eligibility_helper_defs(file_label: &str, source: &str) {
        for line in source.lines() {
            let trimmed = line.trim_start();
            let defines_eligibility_helper = (trimmed.starts_with("fn is_")
                || trimmed.starts_with("const fn is_"))
                && trimmed.contains("eligible");
            assert!(
                !defines_eligibility_helper,
                "{file_label} must keep eligibility helpers route-owned (found: {trimmed})"
            );
        }
    }

    crate::test_canister! {
        ident = RouteMatrixCanister,
    }

    crate::test_store! {
        ident = RouteMatrixStore,
        canister = RouteMatrixCanister,
    }

    static ROUTE_MATRIX_SCORE_KIND: FieldKind = FieldKind::Uint;
    static ROUTE_MATRIX_INDEX_FIELDS: [&str; 1] = ["rank"];
    static ROUTE_MATRIX_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
        "rank_idx",
        RouteMatrixStore::PATH,
        &ROUTE_MATRIX_INDEX_FIELDS,
        false,
    )];

    #[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
    struct RouteMatrixEntity {
        id: Ulid,
        rank: u32,
        label: String,
        scores: Vec<u32>,
    }

    crate::test_entity_schema! {
        ident = RouteMatrixEntity,
        id = Ulid,
        id_field = id,
        entity_name = "RouteMatrixEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("rank", FieldKind::Uint),
            ("label", FieldKind::Text),
            ("scores", FieldKind::List(&ROUTE_MATRIX_SCORE_KIND)),
        ],
        indexes = [&ROUTE_MATRIX_INDEX_MODELS[0]],
        store = RouteMatrixStore,
        canister = RouteMatrixCanister,
    }

    fn field_extrema_index_range_plan(
        direction: OrderDirection,
        offset: u32,
        distinct: bool,
    ) -> LogicalPlan<Ulid> {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::IndexRange {
                index: ROUTE_MATRIX_INDEX_MODELS[0],
                prefix: vec![],
                lower: Bound::Included(Value::Uint(10)),
                upper: Bound::Excluded(Value::Uint(30)),
            },
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), direction),
                ("id".to_string(), direction),
            ],
        });
        plan.page = Some(PageSpec {
            limit: Some(4),
            offset,
        });
        plan.distinct = distinct;

        plan
    }

    #[test]
    fn load_fast_path_order_matches_expected_precedence() {
        assert_eq!(
            LOAD_FAST_PATH_ORDER,
            [
                FastPathOrder::PrimaryKey,
                FastPathOrder::SecondaryPrefix,
                FastPathOrder::IndexRange,
            ],
            "load fast-path precedence must stay stable"
        );
    }

    #[test]
    fn aggregate_fast_path_order_matches_expected_precedence() {
        assert_eq!(
            AGGREGATE_FAST_PATH_ORDER,
            [
                FastPathOrder::PrimaryKey,
                FastPathOrder::SecondaryPrefix,
                FastPathOrder::PrimaryScan,
                FastPathOrder::IndexRange,
                FastPathOrder::Composite,
            ],
            "aggregate fast-path precedence must stay stable"
        );
    }

    #[test]
    fn aggregate_fast_path_order_starts_with_load_contract_prefix() {
        assert!(
            AGGREGATE_FAST_PATH_ORDER
                .starts_with(&[FastPathOrder::PrimaryKey, FastPathOrder::SecondaryPrefix]),
            "aggregate precedence must preserve load-first prefix to avoid subtle route drift"
        );
    }

    #[test]
    fn route_plan_load_uses_route_owned_fast_path_order() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
            &plan,
            None,
            None,
            None,
            Direction::Asc,
        )
        .expect("load route plan should build");

        assert_eq!(route_plan.fast_path_order(), &LOAD_FAST_PATH_ORDER);
    }

    #[test]
    fn route_plan_aggregate_uses_route_owned_fast_path_order() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::Exists,
                Direction::Asc,
            );

        assert_eq!(route_plan.fast_path_order(), &AGGREGATE_FAST_PATH_ORDER);
    }

    #[test]
    fn predicate_slot_coverage_matches_single_index_path_fields() {
        let access = AccessPlan::path(AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        });
        let predicate_slots = PredicateFieldSlots::resolve::<RouteMatrixEntity>(&Predicate::eq(
            "rank".to_string(),
            Value::Uint(7),
        ));

        let covered =
            LoadExecutor::<RouteMatrixEntity>::predicate_slots_fully_covered_by_index_path(
                &access,
                Some(&predicate_slots),
            );

        assert!(
            covered,
            "rank predicate should be covered by rank index path"
        );
    }

    #[test]
    fn predicate_slot_coverage_rejects_non_indexed_predicate_fields() {
        let access = AccessPlan::path(AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        });
        let predicate_slots = PredicateFieldSlots::resolve::<RouteMatrixEntity>(&Predicate::eq(
            "label".to_string(),
            Value::Text("x".to_string()),
        ));

        let covered =
            LoadExecutor::<RouteMatrixEntity>::predicate_slots_fully_covered_by_index_path(
                &access,
                Some(&predicate_slots),
            );

        assert!(
            !covered,
            "label predicate must not be covered by single-field rank index path"
        );
    }

    #[test]
    fn predicate_slot_coverage_requires_index_backed_access_path() {
        let access = AccessPlan::path(AccessPath::<Ulid>::FullScan);
        let predicate_slots = PredicateFieldSlots::resolve::<RouteMatrixEntity>(&Predicate::eq(
            "rank".to_string(),
            Value::Uint(7),
        ));

        let covered =
            LoadExecutor::<RouteMatrixEntity>::predicate_slots_fully_covered_by_index_path(
                &access,
                Some(&predicate_slots),
            );

        assert!(
            !covered,
            "full-scan access is intentionally out of index-slot coverage scope"
        );
    }

    #[test]
    fn route_matrix_field_extrema_capability_flags_enable_for_eligible_shapes() {
        let mut min_plan =
            LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        min_plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        let mut max_plan =
            LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        max_plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });

        let min_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &min_plan,
                AggregateSpec::for_target_field(AggregateKind::Min, "id"),
                Direction::Asc,
            );
        let max_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &max_plan,
                AggregateSpec::for_target_field(AggregateKind::Max, "id"),
                Direction::Desc,
            );

        assert!(min_route.field_min_fast_path_eligible());
        assert!(!min_route.field_max_fast_path_eligible());
        assert!(!max_route.field_min_fast_path_eligible());
        assert!(max_route.field_max_fast_path_eligible());
        assert_eq!(min_route.field_min_fast_path_ineligibility_reason(), None);
        assert_eq!(max_route.field_max_fast_path_ineligibility_reason(), None);
    }

    #[test]
    fn route_matrix_field_extrema_capability_rejects_unknown_target_field() {
        let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, false);

        let route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &plan,
                AggregateSpec::for_target_field(AggregateKind::Min, "missing_field"),
                Direction::Asc,
            );

        assert!(!route.field_min_fast_path_eligible());
        assert!(!route.field_max_fast_path_eligible());
        assert_eq!(
            route.field_min_fast_path_ineligibility_reason(),
            Some(FieldExtremaIneligibilityReason::UnknownTargetField)
        );
    }

    #[test]
    fn route_matrix_field_extrema_reason_rejects_unsupported_field_type() {
        let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, false);

        let route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &plan,
                AggregateSpec::for_target_field(AggregateKind::Min, "scores"),
                Direction::Asc,
            );

        assert_eq!(
            route.field_min_fast_path_ineligibility_reason(),
            Some(FieldExtremaIneligibilityReason::UnsupportedFieldType)
        );
    }

    #[test]
    fn route_matrix_field_extrema_reason_rejects_distinct_shape() {
        let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, true);

        let route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &plan,
                AggregateSpec::for_target_field(AggregateKind::Min, "rank"),
                Direction::Asc,
            );

        assert_eq!(
            route.field_min_fast_path_ineligibility_reason(),
            Some(FieldExtremaIneligibilityReason::DistinctNotSupported)
        );
    }

    #[test]
    fn route_matrix_field_extrema_reason_rejects_offset_shape() {
        let plan = field_extrema_index_range_plan(OrderDirection::Asc, 1, false);

        let route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &plan,
                AggregateSpec::for_target_field(AggregateKind::Min, "rank"),
                Direction::Asc,
            );

        assert_eq!(
            route.field_min_fast_path_ineligibility_reason(),
            Some(FieldExtremaIneligibilityReason::OffsetNotSupported)
        );
    }

    #[test]
    fn route_matrix_field_extrema_reason_rejects_composite_access_shape() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        let child_path = AccessPath::<Ulid>::IndexRange {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            prefix: vec![],
            lower: Bound::Included(Value::Uint(10)),
            upper: Bound::Excluded(Value::Uint(30)),
        };
        plan.access = AccessPlan::Union(vec![
            AccessPlan::path(child_path.clone()),
            AccessPlan::path(child_path),
        ]);
        plan.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        plan.page = Some(PageSpec {
            limit: Some(4),
            offset: 0,
        });

        let route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &plan,
                AggregateSpec::for_target_field(AggregateKind::Min, "rank"),
                Direction::Asc,
            );

        assert_eq!(
            route.field_min_fast_path_ineligibility_reason(),
            Some(FieldExtremaIneligibilityReason::CompositePathNotSupported)
        );
    }

    #[test]
    fn route_matrix_field_extrema_reason_rejects_no_matching_index() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(4),
            offset: 0,
        });

        let route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &plan,
                AggregateSpec::for_target_field(AggregateKind::Min, "rank"),
                Direction::Asc,
            );

        assert_eq!(
            route.field_min_fast_path_ineligibility_reason(),
            Some(FieldExtremaIneligibilityReason::NoMatchingIndex)
        );
    }

    #[test]
    fn route_matrix_field_extrema_reason_rejects_page_limit_shape() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(4),
            offset: 0,
        });

        let route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &plan,
                AggregateSpec::for_target_field(AggregateKind::Min, "id"),
                Direction::Asc,
            );

        assert_eq!(
            route.field_min_fast_path_ineligibility_reason(),
            Some(FieldExtremaIneligibilityReason::PageLimitNotSupported)
        );
    }

    #[test]
    fn route_matrix_field_target_min_fallback_route_matches_terminal_min() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(2),
            offset: 1,
        });

        let terminal_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::Min,
                Direction::Asc,
            );
        let field_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &plan,
                AggregateSpec::for_target_field(AggregateKind::Min, "rank"),
                Direction::Asc,
            );

        assert_eq!(field_route.execution_mode, terminal_route.execution_mode);
        assert_eq!(field_route.scan_hints.physical_fetch_hint, None);
        assert_eq!(
            field_route.scan_hints.load_scan_budget_hint,
            terminal_route.scan_hints.load_scan_budget_hint
        );
        assert_eq!(
            field_route.index_range_limit_spec,
            terminal_route.index_range_limit_spec
        );
        assert_eq!(
            field_route.aggregate_fold_mode,
            terminal_route.aggregate_fold_mode
        );
        assert!(!field_route.field_min_fast_path_eligible());
        assert!(!field_route.field_max_fast_path_eligible());
    }

    #[test]
    fn route_matrix_field_target_unknown_field_fallback_route_matches_terminal_min() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(2),
            offset: 1,
        });

        let terminal_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::Min,
                Direction::Asc,
            );
        let unknown_field_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &plan,
                AggregateSpec::for_target_field(AggregateKind::Min, "missing_field"),
                Direction::Asc,
            );

        assert_eq!(
            unknown_field_route.execution_mode,
            terminal_route.execution_mode
        );
        assert_eq!(unknown_field_route.scan_hints.physical_fetch_hint, None);
        assert_eq!(
            unknown_field_route.scan_hints.load_scan_budget_hint,
            terminal_route.scan_hints.load_scan_budget_hint
        );
        assert_eq!(
            unknown_field_route.index_range_limit_spec,
            terminal_route.index_range_limit_spec
        );
        assert_eq!(
            unknown_field_route.aggregate_fold_mode,
            terminal_route.aggregate_fold_mode
        );
        assert!(!unknown_field_route.field_min_fast_path_eligible());
        assert!(!unknown_field_route.field_max_fast_path_eligible());
    }

    #[test]
    fn route_matrix_field_target_max_fallback_route_matches_terminal_max_desc() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(2),
            offset: 1,
        });

        let terminal_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::Max,
                Direction::Desc,
            );
        let field_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &plan,
                AggregateSpec::for_target_field(AggregateKind::Max, "rank"),
                Direction::Desc,
            );

        assert_eq!(field_route.execution_mode, terminal_route.execution_mode);
        assert_eq!(field_route.scan_hints.physical_fetch_hint, None);
        assert_eq!(
            field_route.scan_hints.load_scan_budget_hint,
            terminal_route.scan_hints.load_scan_budget_hint
        );
        assert_eq!(
            field_route.index_range_limit_spec,
            terminal_route.index_range_limit_spec
        );
        assert_eq!(
            field_route.aggregate_fold_mode,
            terminal_route.aggregate_fold_mode
        );
        assert!(!field_route.field_min_fast_path_eligible());
        assert!(!field_route.field_max_fast_path_eligible());
    }

    #[test]
    fn route_matrix_field_target_non_extrema_fallback_route_matches_terminal_count() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(3),
            offset: 2,
        });

        let terminal_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::Count,
                Direction::Asc,
            );
        let field_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
                &plan,
                AggregateSpec::for_target_field(AggregateKind::Count, "rank"),
                Direction::Asc,
            );

        assert_eq!(field_route.execution_mode, terminal_route.execution_mode);
        assert_eq!(
            field_route.scan_hints.physical_fetch_hint,
            terminal_route.scan_hints.physical_fetch_hint
        );
        assert_eq!(
            field_route.scan_hints.load_scan_budget_hint,
            terminal_route.scan_hints.load_scan_budget_hint
        );
        assert_eq!(
            field_route.index_range_limit_spec,
            terminal_route.index_range_limit_spec
        );
        assert_eq!(
            field_route.aggregate_fold_mode,
            terminal_route.aggregate_fold_mode
        );
        assert!(!field_route.field_min_fast_path_eligible());
        assert!(!field_route.field_max_fast_path_eligible());
    }

    #[test]
    fn route_capabilities_full_scan_desc_pk_order_reflect_expected_flags() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(3),
            offset: 2,
        });
        let capabilities = LoadExecutor::<RouteMatrixEntity>::derive_route_capabilities(
            &plan,
            Direction::Desc,
            None,
        );

        assert!(capabilities.streaming_access_shape_safe);
        assert!(capabilities.desc_physical_reverse_supported);
        assert!(capabilities.count_pushdown_access_shape_supported);
        assert!(!capabilities.index_range_limit_pushdown_shape_eligible);
        assert!(!capabilities.composite_aggregate_fast_path_eligible);
        assert!(capabilities.bounded_probe_hint_safe);
        assert!(!capabilities.field_min_fast_path_eligible);
        assert!(!capabilities.field_max_fast_path_eligible);
    }

    #[test]
    fn route_capabilities_by_keys_desc_distinct_offset_disable_probe_hint() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::ByKeys(vec![
                Ulid::from_u128(7303),
                Ulid::from_u128(7301),
                Ulid::from_u128(7302),
            ]),
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        plan.distinct = true;
        plan.page = Some(PageSpec {
            limit: Some(2),
            offset: 1,
        });
        let capabilities = LoadExecutor::<RouteMatrixEntity>::derive_route_capabilities(
            &plan,
            Direction::Desc,
            None,
        );

        assert!(capabilities.streaming_access_shape_safe);
        assert!(!capabilities.desc_physical_reverse_supported);
        assert!(!capabilities.count_pushdown_access_shape_supported);
        assert!(!capabilities.index_range_limit_pushdown_shape_eligible);
        assert!(!capabilities.composite_aggregate_fast_path_eligible);
        assert!(!capabilities.bounded_probe_hint_safe);
        assert!(!capabilities.field_min_fast_path_eligible);
        assert!(!capabilities.field_max_fast_path_eligible);
    }

    #[test]
    fn route_matrix_load_pk_desc_with_page_uses_streaming_budget_and_reverse() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(3),
            offset: 2,
        });
        let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
            &plan,
            None,
            None,
            None,
            Direction::Desc,
        )
        .expect("load route plan should build");

        assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
        assert!(route_plan.desc_physical_reverse_supported());
        assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
        assert_eq!(route_plan.scan_hints.load_scan_budget_hint, Some(6));
        assert!(route_plan.index_range_limit_spec.is_none());
    }

    #[test]
    fn route_matrix_load_index_range_cursor_without_anchor_disables_pushdown() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::IndexRange {
                index: ROUTE_MATRIX_INDEX_MODELS[0],
                prefix: vec![],
                lower: Bound::Included(Value::Uint(10)),
                upper: Bound::Excluded(Value::Uint(20)),
            },
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Desc),
                ("id".to_string(), OrderDirection::Desc),
            ],
        });
        plan.page = Some(PageSpec {
            limit: Some(2),
            offset: 0,
        });
        let cursor = CursorBoundary { slots: Vec::new() };
        let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
            &plan,
            Some(&cursor),
            None,
            None,
            Direction::Desc,
        )
        .expect("load route plan should build");

        assert_eq!(route_plan.execution_mode, ExecutionMode::Materialized);
        assert!(route_plan.desc_physical_reverse_supported());
        assert!(route_plan.index_range_limit_spec.is_none());
        assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
    }

    #[test]
    fn route_matrix_load_non_pk_order_disables_scan_budget_hint() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("rank".to_string(), OrderDirection::Desc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(3),
            offset: 2,
        });
        let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
            &plan,
            None,
            None,
            None,
            Direction::Desc,
        )
        .expect("load route plan should build");

        assert_eq!(route_plan.execution_mode, ExecutionMode::Materialized);
        assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
    }

    #[test]
    fn route_matrix_load_by_keys_desc_disables_fallback_fetch_hint_without_reverse_support() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::ByKeys(vec![
                Ulid::from_u128(7203),
                Ulid::from_u128(7201),
                Ulid::from_u128(7202),
            ]),
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
            &plan,
            None,
            None,
            Some(4),
            Direction::Desc,
        )
        .expect("load route plan should build");

        assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(4));
        assert_eq!(
            route_plan.fallback_physical_fetch_hint(Direction::Desc),
            None
        );
        assert_eq!(
            route_plan.fallback_physical_fetch_hint(Direction::Asc),
            Some(4)
        );
    }

    #[test]
    fn route_matrix_aggregate_count_pk_order_is_streaming_keys_only() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(4),
            offset: 2,
        });
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::Count,
                Direction::Asc,
            );

        assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
        assert!(matches!(
            route_plan.aggregate_fold_mode,
            AggregateFoldMode::KeysOnly
        ));
        assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(6));
    }

    #[test]
    fn route_matrix_aggregate_fold_mode_contract_maps_non_count_to_existing_rows() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        for kind in [
            AggregateKind::Exists,
            AggregateKind::Min,
            AggregateKind::Max,
            AggregateKind::First,
            AggregateKind::Last,
        ] {
            let route_plan =
                LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                    &plan,
                    kind,
                    Direction::Asc,
                );

            assert!(matches!(
                route_plan.aggregate_fold_mode,
                AggregateFoldMode::ExistingRows
            ));
        }
    }

    #[test]
    fn route_matrix_aggregate_count_secondary_shape_materializes() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::IndexPrefix {
                index: ROUTE_MATRIX_INDEX_MODELS[0],
                values: vec![Value::Uint(7)],
            },
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::Count,
                Direction::Asc,
            );

        assert_eq!(route_plan.execution_mode, ExecutionMode::Materialized);
        assert!(matches!(
            route_plan.aggregate_fold_mode,
            AggregateFoldMode::KeysOnly
        ));
    }

    #[test]
    fn route_matrix_aggregate_distinct_offset_last_disables_probe_hint() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        plan.distinct = true;
        plan.page = Some(PageSpec {
            limit: Some(3),
            offset: 1,
        });
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::Last,
                Direction::Desc,
            );

        assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
        assert!(matches!(
            route_plan.aggregate_fold_mode,
            AggregateFoldMode::ExistingRows
        ));
        assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    }

    #[test]
    fn route_matrix_aggregate_distinct_offset_disables_bounded_probe_hints_for_terminals() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        plan.distinct = true;
        plan.page = Some(PageSpec {
            limit: Some(3),
            offset: 1,
        });

        for kind in [
            AggregateKind::Count,
            AggregateKind::Exists,
            AggregateKind::Min,
            AggregateKind::Max,
            AggregateKind::First,
            AggregateKind::Last,
        ] {
            let route_plan =
                LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                    &plan,
                    kind,
                    Direction::Asc,
                );

            assert_eq!(
                route_plan.scan_hints.physical_fetch_hint, None,
                "DISTINCT+offset must disable bounded aggregate hints for {kind:?}"
            );
            assert_eq!(
                route_plan.secondary_extrema_probe_fetch_hint(),
                None,
                "DISTINCT+offset must disable secondary extrema probe hints for {kind:?}"
            );
        }
    }

    #[test]
    fn route_matrix_aggregate_by_keys_desc_disables_probe_hint_without_reverse_support() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::ByKeys(vec![
                Ulid::from_u128(7103),
                Ulid::from_u128(7101),
                Ulid::from_u128(7102),
            ]),
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(2),
            offset: 1,
        });
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::First,
                Direction::Desc,
            );

        assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
        assert!(!route_plan.desc_physical_reverse_supported());
        assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    }

    #[test]
    fn route_matrix_aggregate_secondary_extrema_probe_hints_lock_offset_plus_one() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::IndexPrefix {
                index: ROUTE_MATRIX_INDEX_MODELS[0],
                values: vec![Value::Uint(7)],
            },
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        plan.page = Some(PageSpec {
            limit: None,
            offset: 2,
        });

        let min_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Min,
            Direction::Asc,
        );
        let max_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Max,
            Direction::Asc,
        );
        assert_eq!(min_asc.scan_hints.physical_fetch_hint, Some(3));
        assert_eq!(max_asc.scan_hints.physical_fetch_hint, None);
        assert_eq!(min_asc.secondary_extrema_probe_fetch_hint(), Some(3));
        assert_eq!(max_asc.secondary_extrema_probe_fetch_hint(), None);

        let first_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::First,
            Direction::Asc,
        );
        assert_eq!(
            first_asc.secondary_extrema_probe_fetch_hint(),
            None,
            "secondary extrema probe hints must stay route-owned and Min/Max-only"
        );

        plan.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Desc),
                ("id".to_string(), OrderDirection::Desc),
            ],
        });
        let max_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Max,
            Direction::Desc,
        );
        let min_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Min,
            Direction::Desc,
        );
        assert_eq!(max_desc.scan_hints.physical_fetch_hint, Some(3));
        assert_eq!(min_desc.scan_hints.physical_fetch_hint, None);
        assert_eq!(max_desc.secondary_extrema_probe_fetch_hint(), Some(3));
        assert_eq!(min_desc.secondary_extrema_probe_fetch_hint(), None);
    }

    #[test]
    fn route_matrix_aggregate_index_range_desc_with_window_enables_pushdown_hint() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::IndexRange {
                index: ROUTE_MATRIX_INDEX_MODELS[0],
                prefix: vec![],
                lower: Bound::Included(Value::Uint(10)),
                upper: Bound::Excluded(Value::Uint(30)),
            },
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Desc),
                ("id".to_string(), OrderDirection::Desc),
            ],
        });
        plan.page = Some(PageSpec {
            limit: Some(2),
            offset: 1,
        });
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::Last,
                Direction::Desc,
            );

        assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
        assert!(route_plan.desc_physical_reverse_supported());
        assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(3));
        assert_eq!(
            route_plan.index_range_limit_spec.map(|spec| spec.fetch),
            Some(3)
        );
    }

    #[test]
    fn route_matrix_aggregate_count_pushdown_boundary_matrix() {
        let mut full_scan =
            LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        full_scan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        let full_scan_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &full_scan,
                AggregateKind::Count,
                Direction::Asc,
            );
        assert_eq!(full_scan_route.execution_mode, ExecutionMode::Streaming);
        assert!(matches!(
            full_scan_route.aggregate_fold_mode,
            AggregateFoldMode::KeysOnly
        ));

        let mut key_range = LogicalPlan::new(
            AccessPath::<Ulid>::KeyRange {
                start: Ulid::from_u128(1),
                end: Ulid::from_u128(9),
            },
            ReadConsistency::MissingOk,
        );
        key_range.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        let key_range_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &key_range,
                AggregateKind::Count,
                Direction::Asc,
            );
        assert_eq!(key_range_route.execution_mode, ExecutionMode::Streaming);
        assert!(matches!(
            key_range_route.aggregate_fold_mode,
            AggregateFoldMode::KeysOnly
        ));

        let mut secondary = LogicalPlan::new(
            AccessPath::<Ulid>::IndexPrefix {
                index: ROUTE_MATRIX_INDEX_MODELS[0],
                values: vec![Value::Uint(7)],
            },
            ReadConsistency::MissingOk,
        );
        secondary.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        let secondary_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &secondary,
                AggregateKind::Count,
                Direction::Asc,
            );
        assert_eq!(secondary_route.execution_mode, ExecutionMode::Materialized);
        assert!(matches!(
            secondary_route.aggregate_fold_mode,
            AggregateFoldMode::KeysOnly
        ));

        let mut index_range = LogicalPlan::new(
            AccessPath::<Ulid>::IndexRange {
                index: ROUTE_MATRIX_INDEX_MODELS[0],
                prefix: vec![],
                lower: Bound::Included(Value::Uint(10)),
                upper: Bound::Excluded(Value::Uint(30)),
            },
            ReadConsistency::MissingOk,
        );
        index_range.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        index_range.page = Some(PageSpec {
            limit: Some(2),
            offset: 1,
        });
        let index_range_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &index_range,
                AggregateKind::Count,
                Direction::Asc,
            );
        assert_eq!(
            index_range_route.execution_mode,
            ExecutionMode::Materialized
        );
        assert!(index_range_route.index_range_limit_spec.is_none());
        assert!(matches!(
            index_range_route.aggregate_fold_mode,
            AggregateFoldMode::KeysOnly
        ));
    }

    #[test]
    fn route_matrix_secondary_extrema_probe_eligibility_is_min_max_only() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::IndexPrefix {
                index: ROUTE_MATRIX_INDEX_MODELS[0],
                values: vec![Value::Uint(7)],
            },
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        plan.page = Some(PageSpec {
            limit: None,
            offset: 2,
        });

        let min_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Min,
            Direction::Asc,
        );
        let max_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Max,
            Direction::Asc,
        );
        let first_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::First,
            Direction::Asc,
        );
        let exists_asc =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::Exists,
                Direction::Asc,
            );
        let last_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Last,
            Direction::Asc,
        );
        assert_eq!(min_asc.secondary_extrema_probe_fetch_hint(), Some(3));
        assert_eq!(max_asc.secondary_extrema_probe_fetch_hint(), None);
        assert_eq!(first_asc.secondary_extrema_probe_fetch_hint(), None);
        assert_eq!(exists_asc.secondary_extrema_probe_fetch_hint(), None);
        assert_eq!(last_asc.secondary_extrema_probe_fetch_hint(), None);

        plan.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Desc),
                ("id".to_string(), OrderDirection::Desc),
            ],
        });
        let min_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Min,
            Direction::Desc,
        );
        let max_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Max,
            Direction::Desc,
        );
        assert_eq!(min_desc.secondary_extrema_probe_fetch_hint(), None);
        assert_eq!(max_desc.secondary_extrema_probe_fetch_hint(), Some(3));
    }

    #[test]
    fn route_matrix_load_desc_reverse_support_gate_allows_and_blocks_fetch_hint() {
        let mut reverse_capable =
            LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        reverse_capable.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        let reverse_capable_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
                &reverse_capable,
                None,
                None,
                Some(5),
                Direction::Desc,
            )
            .expect("reverse-capable load route should build");
        assert!(reverse_capable_route.desc_physical_reverse_supported());
        assert_eq!(
            reverse_capable_route.scan_hints.physical_fetch_hint,
            Some(5)
        );
        assert_eq!(
            reverse_capable_route.fallback_physical_fetch_hint(Direction::Desc),
            Some(5)
        );

        let mut reverse_blocked = LogicalPlan::new(
            AccessPath::<Ulid>::ByKeys(vec![
                Ulid::from_u128(7_203),
                Ulid::from_u128(7_201),
                Ulid::from_u128(7_202),
            ]),
            ReadConsistency::MissingOk,
        );
        reverse_blocked.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        let reverse_blocked_route =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
                &reverse_blocked,
                None,
                None,
                Some(5),
                Direction::Desc,
            )
            .expect("reverse-blocked load route should build");
        assert!(!reverse_blocked_route.desc_physical_reverse_supported());
        assert_eq!(
            reverse_blocked_route.scan_hints.physical_fetch_hint,
            Some(5)
        );
        assert_eq!(
            reverse_blocked_route.fallback_physical_fetch_hint(Direction::Desc),
            None
        );
    }

    #[test]
    fn route_feature_budget_capability_flags_stay_within_soft_delta() {
        let capability_flags = route_capability_flag_count_guard();
        assert!(
            capability_flags
                <= ROUTE_CAPABILITY_FLAG_BASELINE_0247 + ROUTE_FEATURE_SOFT_BUDGET_DELTA,
            "route capability flags exceeded soft feature budget; consolidate before adding more flags"
        );
    }

    #[test]
    fn route_feature_budget_execution_mode_cases_stay_within_soft_delta() {
        let execution_mode_cases = route_execution_mode_case_count_guard();
        assert!(
            execution_mode_cases
                <= ROUTE_EXECUTION_MODE_CASE_BASELINE_0246 + ROUTE_FEATURE_SOFT_BUDGET_DELTA,
            "route execution-mode branching exceeded soft feature budget; consolidate before adding more cases"
        );
    }

    #[test]
    fn route_feature_budget_no_eligibility_helpers_outside_route_module() {
        let aggregate_source = include_str!("aggregate.rs");
        let execute_source = include_str!("execute.rs");
        let index_range_limit_source = include_str!("index_range_limit.rs");
        let page_source = include_str!("page.rs");
        let pk_stream_source = include_str!("pk_stream.rs");
        let secondary_index_source = include_str!("secondary_index.rs");
        let mod_source = include_str!("mod.rs");

        assert_no_eligibility_helper_defs("aggregate.rs", aggregate_source);
        assert_no_eligibility_helper_defs("execute.rs", execute_source);
        assert_no_eligibility_helper_defs("index_range_limit.rs", index_range_limit_source);
        assert_no_eligibility_helper_defs("page.rs", page_source);
        assert_no_eligibility_helper_defs("pk_stream.rs", pk_stream_source);
        assert_no_eligibility_helper_defs("secondary_index.rs", secondary_index_source);
        assert_no_eligibility_helper_defs("mod.rs", mod_source);
    }
}
