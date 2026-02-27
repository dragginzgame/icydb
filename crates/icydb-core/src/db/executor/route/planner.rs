use crate::{
    db::{
        access::PushdownApplicability,
        cursor::CursorBoundary,
        direction::Direction,
        executor::{
            Context, ExecutionPlan, ExecutionPreparation, OrderedKeyStreamBox, RangeToken,
            aggregate::{AggregateFoldMode, AggregateKind, AggregateSpec},
            load::LoadExecutor,
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{
    AGGREGATE_FAST_PATH_ORDER, ContinuationMode, ExecutionMode, ExecutionModeRouteCase,
    ExecutionRoutePlan, LOAD_FAST_PATH_ORDER, RouteCapabilities, RouteIntent, RouteWindowPlan,
    ScanHintPlan,
};

///
/// RouteDerivationContext
///
/// Immutable route-owned derivation bundle for one validated plan + intent.
/// Keeps direction, capability snapshot, scan hints, and secondary-order
/// pushdown applicability aligned under one boundary.
///

struct RouteDerivationContext {
    direction: Direction,
    capabilities: RouteCapabilities,
    secondary_pushdown_applicability: PushdownApplicability,
    scan_hints: ScanHintPlan,
    count_pushdown_eligible: bool,
    aggregate_physical_fetch_hint: Option<usize>,
    aggregate_secondary_extrema_probe_fetch_hint: Option<usize>,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Resolve one routed key stream through the canonical stream-construction
    /// facade so route consumers do not call context stream builders directly.
    pub(in crate::db::executor) fn resolve_routed_key_stream(
        ctx: &Context<'_, E>,
        request: crate::db::executor::route::RoutedKeyStreamRequest<'_, E::Key>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        match request {
            crate::db::executor::route::RoutedKeyStreamRequest::AccessPlan(stream_request) => {
                ctx.ordered_key_stream_from_access_plan_with_index_range_anchor(stream_request)
            }
            crate::db::executor::route::RoutedKeyStreamRequest::AccessPath {
                access,
                constraints,
                direction,
                hints,
            } => ctx.ordered_key_stream_from_access(access, constraints, direction, hints),
        }
    }

    // Build canonical execution routing for load execution.
    pub(in crate::db::executor) fn build_execution_route_plan_for_load(
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RangeToken>,
        probe_fetch_hint: Option<usize>,
    ) -> Result<ExecutionPlan, InternalError> {
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
    ) -> Result<ExecutionPlan, InternalError> {
        if !plan.mode.is_delete() {
            return Err(InternalError::query_executor_invariant(
                "mutation route planning requires delete plans",
            ));
        }

        let capabilities = Self::derive_route_capabilities(plan, Direction::Asc, None);

        Ok(ExecutionRoutePlan::for_mutation(capabilities))
    }

    // Build canonical execution routing for aggregate execution.
    #[cfg(test)]
    pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate(
        plan: &AccessPlannedQuery<E::Key>,
        kind: AggregateKind,
    ) -> ExecutionPlan {
        Self::build_execution_route_plan_for_aggregate_spec(plan, AggregateSpec::for_terminal(kind))
    }

    // Build canonical execution routing for aggregate execution via spec.
    #[cfg(test)]
    pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate_spec(
        plan: &AccessPlannedQuery<E::Key>,
        spec: AggregateSpec,
    ) -> ExecutionPlan {
        let execution_preparation = ExecutionPreparation::for_plan::<E>(plan);

        Self::build_execution_route_plan_for_aggregate_spec_with_preparation(
            plan,
            spec,
            &execution_preparation,
        )
    }

    // Build canonical aggregate execution routing using one precomputed
    // execution-preparation bundle to avoid duplicate strict predicate compilation.
    pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate_spec_with_preparation(
        plan: &AccessPlannedQuery<E::Key>,
        spec: AggregateSpec,
        execution_preparation: &ExecutionPreparation,
    ) -> ExecutionPlan {
        Self::build_execution_route_plan(
            plan,
            None,
            None,
            None,
            RouteIntent::Aggregate {
                spec,
                aggregate_force_materialized_due_to_predicate_uncertainty:
                    Self::aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
                        execution_preparation,
                    ),
            },
        )
    }

    // Shared route gate for load + aggregate execution.
    #[expect(clippy::too_many_lines)]
    fn build_execution_route_plan(
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RangeToken>,
        probe_fetch_hint: Option<usize>,
        intent: RouteIntent,
    ) -> ExecutionRoutePlan {
        let continuation_mode = Self::derive_continuation_mode(cursor_boundary, index_range_anchor);
        let route_window = Self::derive_route_window(plan, cursor_boundary);
        let secondary_pushdown_applicability = Self::derive_secondary_pushdown_applicability(plan);
        let (
            aggregate_spec,
            fast_path_order,
            aggregate_force_materialized_due_to_predicate_uncertainty,
        ) = match intent {
            RouteIntent::Load => (None, &LOAD_FAST_PATH_ORDER[..], false),
            RouteIntent::Aggregate {
                spec,
                aggregate_force_materialized_due_to_predicate_uncertainty,
            } => (
                Some(spec),
                &AGGREGATE_FAST_PATH_ORDER[..],
                aggregate_force_materialized_due_to_predicate_uncertainty,
            ),
        };
        let kind = aggregate_spec.as_ref().map(AggregateSpec::kind);
        debug_assert!(
            (kind.is_none() && fast_path_order == LOAD_FAST_PATH_ORDER.as_slice())
                || (kind.is_some() && fast_path_order == AGGREGATE_FAST_PATH_ORDER.as_slice()),
            "route invariant: route intent must map to the canonical fast-path order contract",
        );
        let derivation = Self::derive_route_derivation_context(
            plan,
            aggregate_spec.as_ref(),
            continuation_mode,
            route_window,
            probe_fetch_hint,
            secondary_pushdown_applicability,
        );
        let aggregate_force_materialized_due_to_predicate_uncertainty =
            kind.is_some() && aggregate_force_materialized_due_to_predicate_uncertainty;
        let count_terminal = matches!(kind, Some(AggregateKind::Count));

        let mut index_range_limit_spec = if count_terminal {
            // COUNT fold-mode discipline: non-count pushdowns must not route COUNT
            // through non-COUNT streaming fast paths.
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
                || matches!(kind, Some(AggregateKind::Count))
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
                if Self::load_streaming_allowed(
                    derivation.capabilities,
                    index_range_limit_spec.is_some(),
                ) {
                    ExecutionMode::Streaming
                } else {
                    ExecutionMode::Materialized
                }
            }
            ExecutionModeRouteCase::AggregateCount => {
                if aggregate_force_materialized_due_to_predicate_uncertainty {
                    ExecutionMode::Materialized
                } else if derivation.count_pushdown_eligible {
                    ExecutionMode::Streaming
                } else {
                    ExecutionMode::Materialized
                }
            }
            ExecutionModeRouteCase::AggregateNonCount => {
                if aggregate_force_materialized_due_to_predicate_uncertainty {
                    ExecutionMode::Materialized
                } else if Self::aggregate_non_count_streaming_allowed(
                    aggregate_spec.as_ref(),
                    derivation.capabilities,
                    derivation.secondary_pushdown_applicability.is_eligible(),
                    index_range_limit_spec.is_some(),
                ) {
                    ExecutionMode::Streaming
                } else {
                    ExecutionMode::Materialized
                }
            }
        };
        if kind.is_some() && matches!(execution_mode, ExecutionMode::Materialized) {
            index_range_limit_spec = None;
        }
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
            derivation.capabilities.bounded_probe_hint_safe
                || derivation.aggregate_physical_fetch_hint.is_none()
                || plan.page.as_ref().is_some_and(|page| page.limit == Some(0)),
            "route invariant: DISTINCT+offset must disable bounded aggregate probe hints",
        );

        ExecutionRoutePlan {
            direction: derivation.direction,
            continuation_mode,
            window: route_window,
            execution_mode,
            secondary_pushdown_applicability: derivation.secondary_pushdown_applicability,
            index_range_limit_spec,
            capabilities: derivation.capabilities,
            fast_path_order,
            aggregate_secondary_extrema_probe_fetch_hint: derivation
                .aggregate_secondary_extrema_probe_fetch_hint,
            scan_hints: derivation.scan_hints,
            aggregate_fold_mode,
        }
    }

    fn derive_route_derivation_context(
        plan: &AccessPlannedQuery<E::Key>,
        aggregate_spec: Option<&AggregateSpec>,
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
            Self::count_pushdown_fetch_hint(plan, capabilities)
        } else {
            None
        };
        let aggregate_terminal_probe_fetch_hint = aggregate_spec
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

        let physical_fetch_hint = if kind.is_some() {
            aggregate_physical_fetch_hint
        } else {
            probe_fetch_hint
        };
        let load_scan_budget_hint = if kind.is_none() {
            Self::load_scan_budget_hint(continuation_mode, route_window, capabilities)
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
        }
    }
}
