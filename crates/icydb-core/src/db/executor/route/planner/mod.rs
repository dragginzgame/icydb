mod capability;
mod fast_path;
mod hints;
mod mode;

use crate::{
    db::{
        executor::{
            Context, ExecutionPlan, OrderedKeyStreamBox,
            aggregate::{AggregateFoldMode, AggregateKind, AggregateSpec},
            load::LoadExecutor,
        },
        lowering::LoweredKey,
        query::{
            contracts::cursor::CursorBoundary,
            plan::{AccessPlannedQuery, Direction},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{
    AGGREGATE_FAST_PATH_ORDER, ExecutionMode, ExecutionModeRouteCase, ExecutionRoutePlan,
    LOAD_FAST_PATH_ORDER, RouteIntent, ScanHintPlan,
};

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
        index_range_anchor: Option<&LoweredKey>,
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
    ) -> ExecutionPlan {
        Self::build_execution_route_plan_for_aggregate_spec(plan, AggregateSpec::for_terminal(kind))
    }

    // Build canonical execution routing for aggregate execution via spec.
    pub(in crate::db::executor) fn build_execution_route_plan_for_aggregate_spec(
        plan: &AccessPlannedQuery<E::Key>,
        spec: AggregateSpec,
    ) -> ExecutionPlan {
        Self::build_execution_route_plan(plan, None, None, None, RouteIntent::Aggregate { spec })
    }

    // Shared route gate for load + aggregate execution.
    #[expect(clippy::too_many_lines)]
    fn build_execution_route_plan(
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&LoweredKey>,
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
        let aggregate_force_materialized_due_to_predicate_uncertainty =
            kind.is_some() && Self::aggregate_force_materialized_due_to_predicate_uncertainty(plan);
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
                if Self::load_streaming_allowed(capabilities, index_range_limit_spec.is_some()) {
                    ExecutionMode::Streaming
                } else {
                    ExecutionMode::Materialized
                }
            }
            ExecutionModeRouteCase::AggregateCount => {
                if aggregate_force_materialized_due_to_predicate_uncertainty {
                    ExecutionMode::Materialized
                } else if count_pushdown_eligible {
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
                    capabilities,
                    secondary_pushdown_applicability.is_eligible(),
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
}
