#[cfg(test)]
use crate::db::executor::group::grouped_execution_context_from_planner_config;
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
    ExecutionRoutePlan, FastPathOrder, GROUPED_AGGREGATE_FAST_PATH_ORDER, IndexRangeLimitSpec,
    LOAD_FAST_PATH_ORDER, RouteCapabilities, RouteIntent, RouteWindowPlan, ScanHintPlan,
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

///
/// RouteIntentStage
///
/// Immutable route-intent normalization for staged route derivation.
/// Captures aggregate presence, canonical fast-path order, and materialization
/// forcing policy in one typed boundary.
///

struct RouteIntentStage {
    aggregate_spec: Option<AggregateSpec>,
    grouped: bool,
    fast_path_order: &'static [FastPathOrder],
    aggregate_force_materialized_due_to_predicate_uncertainty: bool,
}

impl RouteIntentStage {
    fn kind(&self) -> Option<AggregateKind> {
        self.aggregate_spec.as_ref().map(AggregateSpec::kind)
    }
}

///
/// RouteFeasibilityStage
///
/// Immutable route feasibility derivation stage.
/// Captures continuation/window policy, capability snapshot, scan hints, and
/// index-range limit feasibility before execution-mode resolution.
///

struct RouteFeasibilityStage {
    continuation_mode: ContinuationMode,
    route_window: RouteWindowPlan,
    derivation: RouteDerivationContext,
    index_range_limit_spec: Option<IndexRangeLimitSpec>,
}

///
/// RouteExecutionStage
///
/// Immutable execution-mode stage derived from feasibility and intent.
/// Captures final execution mode, aggregate fold mode, and post-mode
/// index-range limit routing.
///

struct RouteExecutionStage {
    execution_mode_case: ExecutionModeRouteCase,
    execution_mode: ExecutionMode,
    aggregate_fold_mode: AggregateFoldMode,
    index_range_limit_spec: Option<IndexRangeLimitSpec>,
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

    // Build canonical grouped aggregate routing from one grouped query wrapper.
    #[cfg(test)]
    pub(in crate::db::executor) fn build_execution_route_plan_for_grouped_plan(
        grouped: &crate::db::query::plan::GroupedPlan<E::Key>,
    ) -> ExecutionPlan {
        let _grouped_execution_context =
            grouped_execution_context_from_planner_config(Some(grouped.group.execution));
        let execution_preparation = ExecutionPreparation::for_plan::<E>(&grouped.base);

        Self::build_execution_route_plan(
            &grouped.base,
            None,
            None,
            None,
            RouteIntent::AggregateGrouped {
                aggregate_force_materialized_due_to_predicate_uncertainty:
                    Self::aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
                        &execution_preparation,
                    ),
            },
        )
    }

    // Shared route gate for load + aggregate execution.
    fn build_execution_route_plan(
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RangeToken>,
        probe_fetch_hint: Option<usize>,
        intent: RouteIntent,
    ) -> ExecutionRoutePlan {
        // Phase 1: normalize route intent into one immutable intent stage.
        let intent_stage = Self::derive_route_intent_stage(intent);

        // Phase 2: derive continuation/window/capability feasibility.
        let feasibility_stage = Self::derive_route_feasibility_stage(
            plan,
            cursor_boundary,
            index_range_anchor,
            probe_fetch_hint,
            &intent_stage,
        );

        // Phase 3: resolve execution mode and fold-mode from feasibility + intent.
        let execution_stage =
            Self::derive_route_execution_stage(plan, &intent_stage, &feasibility_stage);

        // Phase 4: assemble the final immutable route contract.
        Self::assemble_execution_route_plan(intent_stage, feasibility_stage, execution_stage)
    }

    fn derive_route_intent_stage(intent: RouteIntent) -> RouteIntentStage {
        let stage = match intent {
            RouteIntent::Load => RouteIntentStage {
                aggregate_spec: None,
                grouped: false,
                fast_path_order: &LOAD_FAST_PATH_ORDER,
                aggregate_force_materialized_due_to_predicate_uncertainty: false,
            },
            RouteIntent::Aggregate {
                spec,
                aggregate_force_materialized_due_to_predicate_uncertainty,
            } => RouteIntentStage {
                aggregate_spec: Some(spec),
                grouped: false,
                fast_path_order: &AGGREGATE_FAST_PATH_ORDER,
                aggregate_force_materialized_due_to_predicate_uncertainty,
            },
            RouteIntent::AggregateGrouped {
                aggregate_force_materialized_due_to_predicate_uncertainty,
            } => RouteIntentStage {
                aggregate_spec: None,
                grouped: true,
                fast_path_order: &GROUPED_AGGREGATE_FAST_PATH_ORDER,
                aggregate_force_materialized_due_to_predicate_uncertainty,
            },
        };
        let kind = stage.kind();
        debug_assert!(
            (kind.is_none()
                && !stage.grouped
                && stage.fast_path_order == LOAD_FAST_PATH_ORDER.as_slice())
                || (kind.is_some()
                    && !stage.grouped
                    && stage.fast_path_order == AGGREGATE_FAST_PATH_ORDER.as_slice())
                || (kind.is_none()
                    && stage.grouped
                    && stage.fast_path_order == GROUPED_AGGREGATE_FAST_PATH_ORDER.as_slice()),
            "route invariant: route intent must map to the canonical fast-path order contract",
        );
        debug_assert!(
            !stage.grouped || stage.aggregate_spec.is_none() && stage.fast_path_order.is_empty(),
            "route invariant: grouped intent must not carry scalar aggregate specs or fast-path routes",
        );

        stage
    }

    fn derive_route_feasibility_stage(
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
        let count_terminal = matches!(kind, Some(AggregateKind::Count));

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
        }
    }

    fn derive_route_execution_stage(
        plan: &AccessPlannedQuery<E::Key>,
        intent_stage: &RouteIntentStage,
        feasibility_stage: &RouteFeasibilityStage,
    ) -> RouteExecutionStage {
        let kind = intent_stage.kind();
        let aggregate_force_materialized_due_to_predicate_uncertainty = (kind.is_some()
            || intent_stage.grouped)
            && intent_stage.aggregate_force_materialized_due_to_predicate_uncertainty;
        let count_terminal = matches!(kind, Some(AggregateKind::Count));
        let execution_case = if intent_stage.grouped {
            ExecutionModeRouteCase::AggregateGrouped
        } else {
            match kind {
                None => ExecutionModeRouteCase::Load,
                Some(AggregateKind::Count) => ExecutionModeRouteCase::AggregateCount,
                Some(
                    AggregateKind::Exists
                    | AggregateKind::Min
                    | AggregateKind::Max
                    | AggregateKind::First
                    | AggregateKind::Last,
                ) => ExecutionModeRouteCase::AggregateNonCount,
            }
        };
        let execution_mode = match execution_case {
            ExecutionModeRouteCase::Load => {
                if Self::load_streaming_allowed(
                    feasibility_stage.derivation.capabilities,
                    feasibility_stage.index_range_limit_spec.is_some(),
                ) {
                    ExecutionMode::Streaming
                } else {
                    ExecutionMode::Materialized
                }
            }
            ExecutionModeRouteCase::AggregateCount => {
                if aggregate_force_materialized_due_to_predicate_uncertainty {
                    ExecutionMode::Materialized
                } else if feasibility_stage.derivation.count_pushdown_eligible {
                    ExecutionMode::Streaming
                } else {
                    ExecutionMode::Materialized
                }
            }
            ExecutionModeRouteCase::AggregateNonCount => {
                if aggregate_force_materialized_due_to_predicate_uncertainty {
                    ExecutionMode::Materialized
                } else if Self::aggregate_non_count_streaming_allowed(
                    intent_stage.aggregate_spec.as_ref(),
                    feasibility_stage.derivation.capabilities,
                    feasibility_stage
                        .derivation
                        .secondary_pushdown_applicability
                        .is_eligible(),
                    feasibility_stage.index_range_limit_spec.is_some(),
                ) {
                    ExecutionMode::Streaming
                } else {
                    ExecutionMode::Materialized
                }
            }
            ExecutionModeRouteCase::AggregateGrouped => ExecutionMode::Materialized,
        };
        let index_range_limit_spec = if (kind.is_some() || intent_stage.grouped)
            && matches!(execution_mode, ExecutionMode::Materialized)
        {
            None
        } else {
            feasibility_stage.index_range_limit_spec
        };

        debug_assert!(
            (kind.is_none() && !intent_stage.grouped)
                || index_range_limit_spec.is_none()
                || matches!(execution_mode, ExecutionMode::Streaming),
            "route invariant: aggregate index-range limit pushdown must execute in streaming mode",
        );
        debug_assert!(
            !count_terminal || index_range_limit_spec.is_none(),
            "route invariant: COUNT terminals must not route through index-range limit pushdown",
        );
        debug_assert!(
            feasibility_stage
                .derivation
                .capabilities
                .bounded_probe_hint_safe
                || feasibility_stage
                    .derivation
                    .aggregate_physical_fetch_hint
                    .is_none()
                || plan.page.as_ref().is_some_and(|page| page.limit == Some(0)),
            "route invariant: DISTINCT+offset must disable bounded aggregate probe hints",
        );

        let aggregate_fold_mode = if count_terminal {
            AggregateFoldMode::KeysOnly
        } else {
            AggregateFoldMode::ExistingRows
        };

        RouteExecutionStage {
            execution_mode_case: execution_case,
            execution_mode,
            aggregate_fold_mode,
            index_range_limit_spec,
        }
    }

    fn assemble_execution_route_plan(
        intent_stage: RouteIntentStage,
        feasibility_stage: RouteFeasibilityStage,
        execution_stage: RouteExecutionStage,
    ) -> ExecutionRoutePlan {
        let RouteFeasibilityStage {
            continuation_mode,
            route_window,
            derivation,
            index_range_limit_spec: _,
        } = feasibility_stage;

        ExecutionRoutePlan {
            direction: derivation.direction,
            continuation_mode,
            window: route_window,
            execution_mode: execution_stage.execution_mode,
            execution_mode_case: execution_stage.execution_mode_case,
            secondary_pushdown_applicability: derivation.secondary_pushdown_applicability,
            index_range_limit_spec: execution_stage.index_range_limit_spec,
            capabilities: derivation.capabilities,
            fast_path_order: intent_stage.fast_path_order,
            aggregate_secondary_extrema_probe_fetch_hint: derivation
                .aggregate_secondary_extrema_probe_fetch_hint,
            scan_hints: derivation.scan_hints,
            aggregate_fold_mode: execution_stage.aggregate_fold_mode,
        }
    }

    fn derive_route_derivation_context(
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
