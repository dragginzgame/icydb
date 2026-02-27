use crate::{
    db::{
        Context,
        access::AccessPath,
        contracts::ReadConsistency,
        direction::Direction,
        executor::{
            AccessPlanStreamRequest, AccessStreamBindings, ExecutionKernel, IndexStreamConstraints,
            LoweredIndexPrefixSpec, StreamExecutionHints,
            aggregate::{
                AggregateFastPathInputs, AggregateFoldMode, AggregateKind, AggregateOutput,
            },
            load::{FastPathKeyResult, LoadExecutor},
            route::{
                FastPathOrder, RoutedKeyStreamRequest,
                ensure_index_range_aggregate_fast_path_specs,
                ensure_secondary_aggregate_fast_path_arity, try_first_fast_path_hit,
            },
        },
        index::predicate::IndexPredicateExecution,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
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
    // Resolve one routed key stream request, then fold one aggregate terminal
    // over the resolved stream using canonical aggregate fold behavior.
    pub(in crate::db::executor) fn fold_aggregate_from_routed_stream_request<E>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
        stream_request: RoutedKeyStreamRequest<'_, E::Key>,
    ) -> Result<(AggregateOutput<E>, usize), InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let mut key_stream = LoadExecutor::<E>::resolve_routed_key_stream(ctx, stream_request)?;

        Self::run_streaming_aggregate_reducer(
            ctx,
            plan,
            kind,
            direction,
            fold_mode,
            key_stream.as_mut(),
        )
    }

    // Resolve one secondary index order stream attempt and fold one aggregate
    // terminal from it, preserving rows-scanned accounting from the fast path.
    #[expect(clippy::too_many_arguments)]
    pub(in crate::db::executor) fn try_fold_secondary_index_aggregate<E>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        index_prefix_spec: Option<&LoweredIndexPrefixSpec>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let Some(fast) = LoadExecutor::<E>::try_execute_secondary_index_order_stream(
            ctx,
            plan,
            index_prefix_spec,
            physical_fetch_hint,
            index_predicate_execution,
        )?
        else {
            return Ok(None);
        };
        let (aggregate_output, rows_scanned) = Self::fold_aggregate_from_fast_path_result(
            ctx, plan, direction, kind, fold_mode, fast,
        )?;
        if let Some(fetch) = physical_fetch_hint {
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
    fn verify_aggregate_fast_path_eligibility<E>(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
        route: FastPathOrder,
    ) -> Result<Option<VerifiedAggregateFastPathRoute>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
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
    fn try_execute_verified_aggregate_fast_path<E>(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
        verified_route: VerifiedAggregateFastPathRoute,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match verified_route.route {
            FastPathOrder::PrimaryKey => Self::try_execute_primary_key_access_aggregate(
                inputs.ctx,
                inputs.logical_plan,
                inputs.direction,
                inputs.kind,
                inputs.fold_mode,
            ),
            FastPathOrder::SecondaryPrefix => Self::try_execute_index_prefix_aggregate(
                inputs.ctx,
                inputs,
                inputs.direction,
                inputs.kind,
                inputs.fold_mode,
            ),
            FastPathOrder::PrimaryScan => Self::try_execute_primary_scan_aggregate(
                inputs.ctx,
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

    fn verified_aggregate_fast_path_route<E>(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
        route: FastPathOrder,
    ) -> Result<Option<VerifiedAggregateFastPathRoute>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let Some(verified_route) = Self::verify_aggregate_fast_path_eligibility(inputs, route)?
        else {
            return Ok(None);
        };

        Ok(Some(verified_route))
    }

    // Attempt aggregate fast-path execution strictly through route-owned
    // fast-path order. Returns `Some` when one branch fully resolves the terminal.
    pub(in crate::db::executor) fn try_fast_path_aggregate<E>(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let fast_path_hit =
            try_first_fast_path_hit(inputs.route_plan.fast_path_order(), |route| {
                let Some(verified_route) = Self::verified_aggregate_fast_path_route(inputs, route)?
                else {
                    return Ok(None);
                };

                Self::try_execute_verified_aggregate_fast_path(inputs, verified_route)
            })?;
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

    // Fold one aggregate terminal against an already resolved ordered key stream
    // using canonical aggregate streaming semantics.
    fn fold_aggregate_over_key_stream<E>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
        key_stream: &mut dyn crate::db::executor::OrderedKeyStream,
    ) -> Result<(AggregateOutput<E>, usize), InternalError>
    where
        E: EntityKind + EntityValue,
    {
        Self::run_streaming_aggregate_reducer(ctx, plan, kind, direction, fold_mode, key_stream)
    }

    // Apply kernel DISTINCT decoration to one fast-path stream result, then
    // fold one aggregate terminal while preserving fast-path scan accounting.
    fn fold_aggregate_from_fast_path_result<E>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
        mut fast: FastPathKeyResult,
    ) -> Result<(AggregateOutput<E>, usize), InternalError>
    where
        E: EntityKind + EntityValue,
    {
        fast.ordered_key_stream =
            Self::decorate_key_stream_for_plan(fast.ordered_key_stream, plan, direction);
        let rows_scanned = fast.rows_scanned;
        let (aggregate_output, _keys_scanned) = Self::fold_aggregate_over_key_stream(
            ctx,
            plan,
            direction,
            kind,
            fold_mode,
            fast.ordered_key_stream.as_mut(),
        )?;

        Ok((aggregate_output, rows_scanned))
    }

    // Resolve aggregate terminals for primary-key point/batch plans through the
    // canonical routed key-stream boundary so all access-shape execution uses
    // one shared stream-construction path.
    fn try_execute_primary_key_access_aggregate<E>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let Some(path) = plan.access.as_path() else {
            return Ok(None);
        };
        match path {
            AccessPath::ByKeys(keys) if keys.is_empty() => return Ok(None),
            AccessPath::ByKey(_) | AccessPath::ByKeys(_) => {}
            _ => return Ok(None),
        }
        if plan.predicate.is_some() {
            return Ok(None);
        }

        let stream_request = AccessPlanStreamRequest {
            access: &plan.access,
            bindings: AccessStreamBindings {
                index_prefix_specs: &[],
                index_range_specs: &[],
                index_range_anchor: None,
                direction,
            },
            key_comparator: crate::db::executor::load::key_stream_comparator_from_direction(
                direction,
            ),
            physical_fetch_hint: None,
            index_predicate_execution: None,
        };
        let (aggregate_output, keys_scanned) = Self::fold_aggregate_from_routed_stream_request(
            ctx,
            plan,
            direction,
            kind,
            fold_mode,
            RoutedKeyStreamRequest::AccessPlan(stream_request),
        )?;

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // Resolve aggregate terminals directly for index-prefix access plans when
    // canonical secondary ordering is pushdown-eligible.
    fn try_execute_index_prefix_aggregate<E>(
        ctx: &Context<'_, E>,
        inputs: &AggregateFastPathInputs<'_, '_, E>,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        // Probe hint selection is route-owned; use route physical hints first,
        // then fall back to secondary-extrema probe hints when present.
        let probe_fetch_hint = inputs
            .route_plan
            .scan_hints
            .physical_fetch_hint
            .or_else(|| inputs.route_plan.secondary_extrema_probe_fetch_hint());
        let index_predicate_execution =
            Self::aggregate_index_predicate_execution(inputs.index_predicate_program);
        let Some((probe_output, probe_rows_scanned)) = Self::try_fold_secondary_index_aggregate(
            ctx,
            inputs.logical_plan,
            inputs.index_prefix_specs.first(),
            probe_fetch_hint,
            index_predicate_execution,
            direction,
            kind,
            fold_mode,
        )?
        else {
            return Ok(None);
        };

        if !Self::secondary_extrema_probe_requires_fallback(
            inputs.logical_plan.consistency,
            kind,
            probe_fetch_hint,
            &probe_output,
            probe_rows_scanned,
        ) {
            return Ok(Some((probe_output, probe_rows_scanned)));
        }

        // MissingOk + bounded secondary probe can under-fetch when leading index
        // entries are stale. Retry unbounded to preserve terminal correctness.
        let Some((aggregate_output, fallback_rows_scanned)) =
            Self::try_fold_secondary_index_aggregate(
                ctx,
                inputs.logical_plan,
                inputs.index_prefix_specs.first(),
                // Keep native index traversal order for fallback retries.
                Some(usize::MAX),
                index_predicate_execution,
                direction,
                kind,
                fold_mode,
            )?
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
    fn try_execute_primary_scan_aggregate<E>(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery<E::Key>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let Some(path) = plan.access.as_path() else {
            return Ok(None);
        };
        if !matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. }) {
            return Ok(None);
        }

        let (aggregate_output, keys_scanned) = Self::fold_aggregate_from_routed_stream_request(
            ctx,
            plan,
            direction,
            kind,
            fold_mode,
            RoutedKeyStreamRequest::AccessPath {
                access: path,
                constraints: IndexStreamConstraints {
                    prefix: None,
                    range: None,
                    anchor: None,
                },
                direction,
                hints: StreamExecutionHints {
                    physical_fetch_hint,
                    predicate_execution: None,
                },
            },
        )?;

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // Resolve aggregate terminals directly for index-range access plans.
    // This reuses canonical range traversal while preserving one fold engine.
    fn try_execute_index_range_aggregate<E>(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let Some(index_range_limit_spec) = inputs.route_plan.index_range_limit_spec.as_ref() else {
            return Ok(None);
        };

        let Some(fast) = LoadExecutor::<E>::try_execute_index_range_limit_pushdown_stream(
            inputs.ctx,
            inputs.logical_plan,
            inputs.index_range_specs.first(),
            None,
            inputs.direction,
            index_range_limit_spec.fetch,
            Self::aggregate_index_predicate_execution(inputs.index_predicate_program),
        )?
        else {
            return Ok(None);
        };
        let (aggregate_output, rows_scanned) = Self::fold_aggregate_from_fast_path_result(
            inputs.ctx,
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
    fn try_execute_composite_aggregate<E>(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let stream_request = AccessPlanStreamRequest {
            access: &inputs.logical_plan.access,
            bindings: AccessStreamBindings {
                index_prefix_specs: inputs.index_prefix_specs,
                index_range_specs: inputs.index_range_specs,
                index_range_anchor: None,
                direction: inputs.direction,
            },
            key_comparator: crate::db::executor::load::key_stream_comparator_from_direction(
                inputs.direction,
            ),
            physical_fetch_hint: inputs.physical_fetch_hint,
            index_predicate_execution: Self::aggregate_index_predicate_execution(
                inputs.index_predicate_program,
            ),
        };
        let (aggregate_output, keys_scanned) = Self::fold_aggregate_from_routed_stream_request(
            inputs.ctx,
            inputs.logical_plan,
            inputs.direction,
            inputs.kind,
            inputs.fold_mode,
            RoutedKeyStreamRequest::AccessPlan(stream_request),
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
    const fn aggregate_zero_window_result<E>(kind: AggregateKind) -> AggregateOutput<E>
    where
        E: EntityKind + EntityValue,
    {
        kind.zero_output()
    }

    // MissingOk can skip stale leading index entries. If a bounded Min/Max
    // probe returns None exactly at the fetch boundary, the outcome is
    // inconclusive and must retry unbounded.
    const fn secondary_extrema_probe_requires_fallback<E>(
        consistency: ReadConsistency,
        kind: AggregateKind,
        probe_fetch_hint: Option<usize>,
        probe_output: &AggregateOutput<E>,
        probe_rows_scanned: usize,
    ) -> bool
    where
        E: EntityKind + EntityValue,
    {
        if !matches!(consistency, ReadConsistency::MissingOk) {
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
