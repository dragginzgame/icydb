use crate::{
    db::{
        executor::{
            Context, OrderedKeyStreamBox,
            aggregate::capability::field_is_orderable,
            fold::{AggregateFoldMode, AggregateKind, AggregateSpec},
            load::LoadExecutor,
        },
        index::RawIndexKey,
        query::{
            contracts::cursor::CursorBoundary,
            plan::{AccessPath, AccessPlan, AccessPlannedQuery, Direction},
            predicate::PredicateFieldSlots,
        },
    },
    error::InternalError,
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{
    AGGREGATE_FAST_PATH_ORDER, ContinuationMode, ExecutionMode, ExecutionModeRouteCase,
    ExecutionRoutePlan, FieldExtremaEligibility, FieldExtremaIneligibilityReason,
    IndexRangeLimitSpec, LOAD_FAST_PATH_ORDER, RouteCapabilities, RouteIntent,
    RouteOrderSlotPolicy, RouteWindowPlan, RoutedKeyStreamRequest, ScanHintPlan,
    derive_scan_direction, direction_allows_physical_fetch_hint,
};

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
    pub(in crate::db::executor::route) fn derive_route_capabilities(
        plan: &AccessPlannedQuery<E::Key>,
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
    pub(in crate::db::executor) fn build_execution_route_plan_for_load(
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        probe_fetch_hint: Option<usize>,
    ) -> Result<ExecutionRoutePlan, InternalError> {
        Self::validate_pk_fast_path_boundary_if_applicable(plan, cursor_boundary)?;

        Ok(Self::build_execution_route_plan(
            plan,
            cursor_boundary,
            index_range_anchor,
            probe_fetch_hint,
            RouteIntent::Load,
        ))
    }

    // Build canonical execution routing for mutation execution.
    pub(in crate::db::executor) fn build_execution_route_plan_for_mutation(
        plan: &AccessPlannedQuery<E::Key>,
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
        plan: &AccessPlannedQuery<E::Key>,
    ) -> Result<(), InternalError> {
        let _mutation_route_plan = Self::build_execution_route_plan_for_mutation(plan)?;

        Ok(())
    }

    // Build canonical execution routing for aggregate execution.
    #[cfg(test)]
    pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate(
        plan: &AccessPlannedQuery<E::Key>,
        kind: AggregateKind,
    ) -> ExecutionRoutePlan {
        Self::build_execution_route_plan_for_aggregate_spec(plan, AggregateSpec::for_terminal(kind))
    }

    // Build canonical execution routing for aggregate execution via spec.
    pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate_spec(
        plan: &AccessPlannedQuery<E::Key>,
        spec: AggregateSpec,
    ) -> ExecutionRoutePlan {
        Self::build_execution_route_plan(plan, None, None, None, RouteIntent::Aggregate { spec })
    }

    // Shared route gate for load + aggregate execution.
    #[expect(clippy::too_many_lines)]
    fn build_execution_route_plan(
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        probe_fetch_hint: Option<usize>,
        intent: RouteIntent,
    ) -> ExecutionRoutePlan {
        let continuation_mode = Self::derive_continuation_mode(cursor_boundary, index_range_anchor);
        let route_window = Self::derive_route_window(plan, cursor_boundary);
        let secondary_pushdown_applicability =
            crate::db::query::plan::validate::assess_secondary_order_pushdown_if_applicable_validated(
                E::MODEL,
                plan,
            );
        let (direction, aggregate_spec, fast_path_order, is_load_intent) = match intent {
            RouteIntent::Load => (
                Self::derive_load_route_direction(plan),
                None,
                &LOAD_FAST_PATH_ORDER[..],
                true,
            ),
            RouteIntent::Aggregate { spec } => {
                let direction = Self::derive_aggregate_route_direction(plan, &spec);
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
            Self::load_scan_budget_hint(continuation_mode, route_window, capabilities)
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
                route_window,
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
            direction,
            continuation_mode,
            window: route_window,
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

    fn derive_load_route_direction(plan: &AccessPlannedQuery<E::Key>) -> Direction {
        plan.order.as_ref().map_or(Direction::Asc, |order| {
            derive_scan_direction(order, RouteOrderSlotPolicy::First)
        })
    }

    fn derive_aggregate_route_direction(
        plan: &AccessPlannedQuery<E::Key>,
        spec: &AggregateSpec,
    ) -> Direction {
        if spec.target_field().is_some() {
            return match spec.kind() {
                AggregateKind::Min => Direction::Asc,
                AggregateKind::Max => Direction::Desc,
                AggregateKind::Count
                | AggregateKind::Exists
                | AggregateKind::First
                | AggregateKind::Last => Self::derive_load_route_direction(plan),
            };
        }

        Self::derive_load_route_direction(plan)
    }

    const fn derive_continuation_mode(
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
    ) -> ContinuationMode {
        match (cursor_boundary, index_range_anchor) {
            (_, Some(_)) => ContinuationMode::IndexRangeAnchor,
            (Some(_), None) => ContinuationMode::CursorBoundary,
            (None, None) => ContinuationMode::Initial,
        }
    }

    fn derive_route_window(
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> RouteWindowPlan {
        let effective_offset = plan.effective_page_offset(cursor_boundary);
        let limit = plan.page.as_ref().and_then(|page| page.limit);

        RouteWindowPlan::new(effective_offset, limit)
    }

    // Assess index-range limit pushdown once for this execution and produce
    // the bounded fetch spec when all eligibility gates pass.
    fn assess_index_range_limit_pushdown(
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        route_window: RouteWindowPlan,
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

        let fetch = Self::page_window_fetch_count(route_window, true)?;
        if plan.predicate.is_some() && !Self::residual_predicate_pushdown_fetch_is_safe(fetch) {
            return None;
        }

        Some(IndexRangeLimitSpec { fetch })
    }

    // Shared load-page scan-budget hint gate.
    const fn load_scan_budget_hint(
        continuation_mode: ContinuationMode,
        route_window: RouteWindowPlan,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if !matches!(continuation_mode, ContinuationMode::Initial) {
            return None;
        }
        if !capabilities.streaming_access_shape_safe {
            return None;
        }

        Self::page_window_fetch_count(route_window, true)
    }

    // Shared bounded-probe safety gate for aggregate key-stream hints.
    // Contract:
    // - DISTINCT + offset must remain unbounded so deduplication is applied
    //   before offset consumption without risking under-fetch.
    // - If dedup/projection/composite semantics evolve, this gate is the first
    //   place to re-evaluate bounded-probe correctness.
    fn bounded_probe_hint_is_safe(plan: &AccessPlannedQuery<E::Key>) -> bool {
        let offset = usize::try_from(plan.effective_page_offset(None)).unwrap_or(usize::MAX);
        !(plan.distinct && offset > 0)
    }

    // Residual predicates are allowed for index-range limit pushdown only when
    // the bounded fetch remains small. This caps amplification risk when the
    // post-access residual filter rejects many bounded candidates.
    const fn residual_predicate_pushdown_fetch_is_safe(fetch: usize) -> bool {
        fetch <= Self::residual_predicate_pushdown_fetch_cap()
    }

    pub(in crate::db::executor::route) const fn residual_predicate_pushdown_fetch_cap() -> usize {
        256
    }

    // Determine whether every compiled predicate field slot is available on
    // the active single-path index access shape.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db::executor) fn predicate_slots_fully_covered_by_index_path(
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
    pub(in crate::db::executor) fn resolved_index_slots_for_access_path(
        access: &AccessPlan<E::Key>,
    ) -> Option<Vec<usize>> {
        let path = access.as_path()?;
        let index_fields = match path {
            AccessPath::IndexPrefix { index, .. } => index.fields,
            AccessPath::IndexRange { spec } => {
                let index = spec.index();
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
    pub(in crate::db::executor) fn pk_order_stream_fast_path_shape_supported(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> bool {
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
        plan: &AccessPlannedQuery<E::Key>,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if !capabilities.bounded_probe_hint_safe {
            return None;
        }

        let route_window = Self::derive_route_window(plan, None);
        Self::page_window_fetch_count(route_window, false)
    }

    fn aggregate_probe_fetch_hint(
        plan: &AccessPlannedQuery<E::Key>,
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
        plan: &AccessPlannedQuery<E::Key>,
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
        plan: &AccessPlannedQuery<E::Key>,
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
        plan: &AccessPlannedQuery<E::Key>,
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
        if resolve_field_slot(E::MODEL, target_field).is_none() {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::UnknownTargetField),
            };
        }
        if !field_is_orderable::<E>(target_field) {
            return FieldExtremaEligibility {
                eligible: false,
                ineligibility_reason: Some(FieldExtremaIneligibilityReason::UnsupportedFieldType),
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
        plan: &AccessPlannedQuery<E::Key>,
        target_field: &str,
    ) -> bool {
        let Some(path) = plan.access.as_path() else {
            return false;
        };
        if target_field == E::MODEL.primary_key.name {
            return matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. });
        }

        match path {
            AccessPath::IndexPrefix { index, .. } => index
                .fields
                .first()
                .is_some_and(|field| *field == target_field),
            AccessPath::IndexRange { spec } => spec
                .index()
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
    fn is_composite_aggregate_fast_path_eligible(plan: &AccessPlannedQuery<E::Key>) -> bool {
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
    const fn page_window_fetch_count(
        route_window: RouteWindowPlan,
        needs_extra: bool,
    ) -> Option<usize> {
        route_window.fetch_count_for(needs_extra)
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
    fn is_index_range_limit_pushdown_shape_eligible(plan: &AccessPlannedQuery<E::Key>) -> bool {
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
