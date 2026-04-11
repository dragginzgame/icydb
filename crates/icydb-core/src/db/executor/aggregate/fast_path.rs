//! Module: executor::aggregate::fast_path
//! Responsibility: aggregate fast-path verification and branch execution.
//! Does not own: fast-path precedence policy (route-owned) or logical planning.
//! Boundary: aggregate fast-path branch helpers invoked by aggregate orchestration.

use crate::{
    db::{
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutableAccess, ExecutionKernel,
            aggregate::{
                AggregateFastPathInputs, AggregateFoldMode, AggregateKind, ScalarAggregateOutput,
            },
            pipeline::{contracts::FastPathKeyResult, operators::decorate_key_stream_for_plan},
            route::{
                FastPathOrder, derive_budget_safety_flags_for_model,
                ensure_index_range_aggregate_fast_path_specs,
                ensure_secondary_aggregate_fast_path_arity, try_first_verified_fast_path_hit,
            },
            scan::{FastStreamRouteKind, FastStreamRouteRequest, execute_fast_stream_route},
            stream::access::TraversalRuntime,
        },
        index::predicate::IndexPredicateExecution,
        predicate::MissingRowPolicy,
        query::plan::AccessPlannedQuery,
        registry::StoreHandle,
    },
    error::InternalError,
};

///
/// VerifiedAggregateFastPathRoute
///
/// Capability marker returned only by aggregate fast-path eligibility verification.
/// Fast-path branch dispatch requires this marker so branch execution cannot skip
/// the shared gate by accident.
///

struct VerifiedAggregateFastPathRoute {
    route: FastPathOrder,
}

impl ExecutionKernel {
    /// Resolve one structural access request and fold one aggregate terminal from it.
    pub(in crate::db::executor) fn fold_aggregate_from_structural_access(
        traversal_runtime: TraversalRuntime,
        store: StoreHandle,
        plan: &AccessPlannedQuery,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
        access: ExecutableAccess<'_, crate::db::access::AccessKey>,
    ) -> Result<(ScalarAggregateOutput, usize), InternalError> {
        let mut key_stream = traversal_runtime.ordered_key_stream_from_runtime_access(access)?;

        Self::run_streaming_aggregate_reducer(
            store,
            plan,
            kind,
            direction,
            fold_mode,
            &mut key_stream,
        )
    }

    /// Try one secondary-index aggregate fold attempt and preserve fast-path scan accounting.
    pub(in crate::db::executor) fn try_fold_secondary_index_aggregate(
        inputs: &AggregateFastPathInputs<'_>,
        probe_fetch_hint: Option<usize>,
    ) -> Result<Option<(ScalarAggregateOutput, usize)>, InternalError> {
        let index_predicate_execution =
            Self::aggregate_index_predicate_execution(inputs.index_predicate_program);
        let runtime = TraversalRuntime::new(inputs.store, inputs.authority.entity_tag());
        let Some(fast) = execute_fast_stream_route(
            &runtime,
            FastStreamRouteKind::SecondaryIndex,
            FastStreamRouteRequest::SecondaryIndex {
                plan: inputs.logical_plan,
                index_prefix_spec: inputs.index_prefix_specs.first(),
                stream_direction: inputs.direction,
                probe_fetch_hint,
                index_predicate_execution,
            },
        )?
        else {
            return Ok(None);
        };
        let (aggregate_output, rows_scanned) = Self::fold_aggregate_from_fast_path_result(
            inputs.store,
            inputs.logical_plan,
            inputs.direction,
            inputs.kind,
            inputs.fold_mode,
            fast,
        )?;
        if let Some(fetch) = probe_fetch_hint {
            debug_assert!(
                rows_scanned <= fetch,
                "secondary extrema probe rows_scanned must not exceed bounded fetch",
            );
        }

        Ok(Some((aggregate_output, rows_scanned)))
    }

    // Shared aggregate fast-path eligibility verifier.
    //
    // All aggregate fast-path dispatch must pass through this gate before
    // invoking any `try_execute_*` branch so route eligibility checks, arity
    // guards, and branch preconditions cannot drift across call sites.
    fn verify_aggregate_fast_path_eligibility(
        inputs: &AggregateFastPathInputs<'_>,
        route: FastPathOrder,
    ) -> Result<Option<VerifiedAggregateFastPathRoute>, InternalError> {
        match route {
            // Primary-key point/batch aggregate fast path is branch-local and
            // intentionally independent of route capability flags.
            FastPathOrder::PrimaryKey => Ok(Some(VerifiedAggregateFastPathRoute { route })),
            FastPathOrder::SecondaryPrefix => {
                ensure_secondary_aggregate_fast_path_arity(
                    inputs.route_plan.secondary_fast_path_eligible(),
                    inputs.index_prefix_specs.len(),
                )?;
                if inputs.route_plan.secondary_fast_path_eligible() {
                    Ok(Some(VerifiedAggregateFastPathRoute { route }))
                } else {
                    Ok(None)
                }
            }
            // Primary-scan aggregate fast path is only attempted when route
            // planning provided a bounded probe hint for this terminal.
            FastPathOrder::PrimaryScan => {
                if inputs.physical_fetch_hint.is_some() {
                    Ok(Some(VerifiedAggregateFastPathRoute { route }))
                } else {
                    Ok(None)
                }
            }
            FastPathOrder::IndexRange => {
                ensure_index_range_aggregate_fast_path_specs(
                    inputs.route_plan.index_range_limit_fast_path_enabled(),
                    inputs.index_prefix_specs.len(),
                    inputs.index_range_specs.len(),
                )?;
                if inputs.route_plan.index_range_limit_fast_path_enabled() {
                    Ok(Some(VerifiedAggregateFastPathRoute { route }))
                } else {
                    Ok(None)
                }
            }
            FastPathOrder::Composite => {
                if inputs.route_plan.composite_aggregate_fast_path_eligible() {
                    Ok(Some(VerifiedAggregateFastPathRoute { route }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    // Execute one aggregate fast-path branch only after route verification has
    // produced a capability marker from the shared eligibility gate.
    fn try_execute_verified_aggregate_fast_path(
        inputs: &AggregateFastPathInputs<'_>,
        verified_route: VerifiedAggregateFastPathRoute,
    ) -> Result<Option<(ScalarAggregateOutput, usize)>, InternalError> {
        match verified_route.route {
            FastPathOrder::PrimaryKey => Self::try_execute_primary_key_access_aggregate(
                inputs.store,
                inputs.authority.entity_tag(),
                inputs.logical_plan,
                inputs.direction,
                inputs.kind,
                inputs.fold_mode,
            ),
            FastPathOrder::SecondaryPrefix => Self::try_execute_index_prefix_aggregate(inputs),
            FastPathOrder::PrimaryScan => Self::try_execute_primary_scan_aggregate(
                inputs.store,
                inputs.authority.entity_tag(),
                inputs.logical_plan,
                inputs.direction,
                inputs.physical_fetch_hint,
                inputs.kind,
                inputs.fold_mode,
            ),
            FastPathOrder::IndexRange => Self::try_execute_index_range_aggregate(inputs),
            FastPathOrder::Composite => Self::try_execute_composite_aggregate(inputs),
        }
    }

    /// Attempt aggregate fast-path execution through route-owned fast-path order.
    pub(in crate::db::executor) fn try_fast_path_aggregate(
        inputs: &AggregateFastPathInputs<'_>,
    ) -> Result<Option<(ScalarAggregateOutput, usize)>, InternalError> {
        let fast_path_hit = try_first_verified_fast_path_hit(
            inputs.route_plan.fast_path_order(),
            |route| Self::verify_aggregate_fast_path_eligibility(inputs, route),
            |verified_route| Self::try_execute_verified_aggregate_fast_path(inputs, verified_route),
        )?;
        if let Some((aggregate_output, rows_scanned)) = fast_path_hit {
            return Ok(Some((aggregate_output, rows_scanned)));
        }

        // Fast exit: effective limit == 0 has an empty aggregate window and can
        // return terminal defaults without constructing or scanning key streams.
        if inputs.physical_fetch_hint == Some(0) {
            return Ok(Some((Self::aggregate_zero_window_result(inputs.kind), 0)));
        }

        Ok(None)
    }

    // Apply kernel DISTINCT decoration to one fast-path stream result, then
    // fold one aggregate terminal while preserving fast-path scan accounting.
    fn fold_aggregate_from_fast_path_result(
        store: StoreHandle,
        plan: &AccessPlannedQuery,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
        mut fast: FastPathKeyResult,
    ) -> Result<(ScalarAggregateOutput, usize), InternalError> {
        fast.ordered_key_stream =
            decorate_key_stream_for_plan(fast.ordered_key_stream, plan, direction);
        let rows_scanned = fast.rows_scanned;
        let (aggregate_output, _keys_scanned) = Self::run_streaming_aggregate_reducer(
            store,
            plan,
            kind,
            direction,
            fold_mode,
            &mut fast.ordered_key_stream,
        )?;

        Ok((aggregate_output, rows_scanned))
    }

    // Resolve aggregate terminals for primary-key point/batch plans through the
    // canonical routed key-stream boundary so all access-shape execution uses
    // one shared stream-construction path.
    fn try_execute_primary_key_access_aggregate(
        store: StoreHandle,
        entity_tag: crate::types::EntityTag,
        plan: &AccessPlannedQuery,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(ScalarAggregateOutput, usize)>, InternalError> {
        let access_strategy = plan.access.resolve_strategy();
        let Some(executable_path) = access_strategy.as_path() else {
            return Ok(None);
        };
        let capabilities = executable_path.capabilities();
        if capabilities.is_by_keys_empty() {
            return Ok(Some((Self::aggregate_zero_window_result(kind), 0)));
        }
        if !capabilities.is_key_direct_access() {
            return Ok(None);
        }
        let (has_residual_filter, _, _) = derive_budget_safety_flags_for_model(plan);
        if has_residual_filter {
            return Ok(None);
        }

        let access = ExecutableAccess::new(
            &plan.access,
            AccessStreamBindings::no_index(direction),
            None,
            None,
        );
        let (aggregate_output, keys_scanned) = Self::fold_aggregate_from_structural_access(
            TraversalRuntime::new(store, entity_tag),
            store,
            plan,
            direction,
            kind,
            fold_mode,
            access,
        )?;

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // Resolve aggregate terminals directly for index-prefix access plans when
    // canonical secondary ordering is pushdown-eligible.
    fn try_execute_index_prefix_aggregate(
        inputs: &AggregateFastPathInputs<'_>,
    ) -> Result<Option<(ScalarAggregateOutput, usize)>, InternalError> {
        // Probe hint selection is route-owned; prefer explicit aggregate seek
        // contracts, then reuse generic route scan hints.
        let probe_fetch_hint = inputs
            .route_plan
            .aggregate_seek_fetch_hint()
            .or(inputs.route_plan.scan_hints.physical_fetch_hint);
        let Some((probe_output, probe_rows_scanned)) =
            Self::try_fold_secondary_index_aggregate(inputs, probe_fetch_hint)?
        else {
            return Ok(None);
        };

        if !Self::secondary_extrema_probe_may_be_inconclusive(
            inputs.consistency(),
            inputs.kind,
            probe_fetch_hint,
            &probe_output,
            probe_rows_scanned,
        ) {
            return Ok(Some((probe_output, probe_rows_scanned)));
        }

        // Ignore + bounded secondary probe can under-fetch when leading index
        // entries are stale. Retry unbounded to preserve terminal correctness.
        let Some((aggregate_output, fallback_rows_scanned)) =
            // Keep native index traversal order for fallback retries.
            Self::try_fold_secondary_index_aggregate(inputs, Some(usize::MAX))?
        else {
            return Ok(None);
        };

        Ok(Some((
            aggregate_output,
            probe_rows_scanned.saturating_add(fallback_rows_scanned),
        )))
    }

    // Resolve aggregate terminals directly for full-scan/key-range access plans.
    // This keeps canonical stream semantics while avoiding generic route assembly.
    fn try_execute_primary_scan_aggregate(
        store: StoreHandle,
        entity_tag: crate::types::EntityTag,
        plan: &AccessPlannedQuery,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(ScalarAggregateOutput, usize)>, InternalError> {
        let access_strategy = plan.access.resolve_strategy();
        let Some(executable_path) = access_strategy.as_path() else {
            return Ok(None);
        };
        if !executable_path
            .capabilities()
            .supports_count_pushdown_shape()
        {
            return Ok(None);
        }

        let (aggregate_output, keys_scanned) = Self::fold_aggregate_from_structural_access(
            TraversalRuntime::new(store, entity_tag),
            store,
            plan,
            direction,
            kind,
            fold_mode,
            ExecutableAccess::new(
                &plan.access,
                AccessStreamBindings::no_index(direction),
                physical_fetch_hint,
                None,
            ),
        )?;

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // Resolve aggregate terminals directly for index-range access plans.
    // This reuses canonical range traversal while preserving one fold engine.
    fn try_execute_index_range_aggregate(
        inputs: &AggregateFastPathInputs<'_>,
    ) -> Result<Option<(ScalarAggregateOutput, usize)>, InternalError> {
        let Some(index_range_limit_spec) = inputs.route_plan.index_range_limit_spec.as_ref() else {
            return Ok(None);
        };

        let runtime = TraversalRuntime::new(inputs.store, inputs.authority.entity_tag());
        let Some(fast) = execute_fast_stream_route(
            &runtime,
            FastStreamRouteKind::IndexRangeLimitPushdown,
            FastStreamRouteRequest::IndexRangeLimitPushdown {
                plan: inputs.logical_plan,
                index_range_spec: inputs.index_range_specs.first(),
                continuation: AccessScanContinuationInput::new(None, inputs.direction),
                effective_fetch: index_range_limit_spec.fetch,
                index_predicate_execution: Self::aggregate_index_predicate_execution(
                    inputs.index_predicate_program,
                ),
            },
        )?
        else {
            return Ok(None);
        };
        let (aggregate_output, rows_scanned) = Self::fold_aggregate_from_fast_path_result(
            inputs.store,
            inputs.logical_plan,
            inputs.direction,
            inputs.kind,
            inputs.fold_mode,
            fast,
        )?;
        Ok(Some((aggregate_output, rows_scanned)))
    }

    // Resolve aggregate terminals directly for composite access plans by
    // reusing canonical composite stream production.
    fn try_execute_composite_aggregate(
        inputs: &AggregateFastPathInputs<'_>,
    ) -> Result<Option<(ScalarAggregateOutput, usize)>, InternalError> {
        let access = ExecutableAccess::new(
            &inputs.logical_plan.access,
            AccessStreamBindings::new(
                inputs.index_prefix_specs,
                inputs.index_range_specs,
                AccessScanContinuationInput::new(None, inputs.direction),
            ),
            inputs.physical_fetch_hint,
            Self::aggregate_index_predicate_execution(inputs.index_predicate_program),
        );
        let (aggregate_output, keys_scanned) = Self::fold_aggregate_from_structural_access(
            TraversalRuntime::new(inputs.store, inputs.authority.entity_tag()),
            inputs.store,
            inputs.logical_plan,
            inputs.direction,
            inputs.kind,
            inputs.fold_mode,
            access,
        )?;

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // Build one optional index-only predicate execution request for aggregate
    // stream producers from a strict-compiled index predicate program.
    #[expect(clippy::single_option_map)]
    fn aggregate_index_predicate_execution(
        program: Option<&crate::db::index::IndexPredicateProgram>,
    ) -> Option<IndexPredicateExecution<'_>> {
        program.map(|program| IndexPredicateExecution {
            program,
            rejected_keys_counter: None,
        })
    }

    // Return the aggregate terminal value for an empty effective output window.
    const fn aggregate_zero_window_result(kind: AggregateKind) -> ScalarAggregateOutput {
        kind.zero_output()
    }

    // Ignore can skip stale leading index entries. If a bounded Min/Max
    // probe returns None exactly at the fetch boundary, the outcome is
    // inconclusive and must retry unbounded.
    const fn secondary_extrema_probe_may_be_inconclusive(
        consistency: MissingRowPolicy,
        kind: AggregateKind,
        probe_fetch_hint: Option<usize>,
        probe_output: &ScalarAggregateOutput,
        probe_rows_scanned: usize,
    ) -> bool {
        if !matches!(consistency, MissingRowPolicy::Ignore) {
            return false;
        }
        if !kind.is_extrema() {
            return false;
        }

        let Some(fetch) = probe_fetch_hint else {
            return false;
        };
        if fetch == 0 || probe_rows_scanned < fetch {
            return false;
        }

        kind.is_unresolved_extrema_output(probe_output)
    }
}
