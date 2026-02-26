use crate::{
    db::{
        Context,
        access::AccessPath,
        direction::Direction,
        executor::{
            AccessPlanStreamRequest, AccessStreamBindings, ExecutablePlan, ExecutionKernel,
            IndexPredicateCompileMode, IndexStreamConstraints, LoweredIndexPrefixSpec,
            StreamExecutionHints,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, apply_aggregate_direction,
                compare_entities_by_orderable_field, compare_entities_for_field_extrema,
                resolve_orderable_aggregate_target_slot,
            },
            aggregate::{
                AggregateExecutionDescriptor, AggregateFastPathInputs, AggregateFoldMode,
                AggregateKind, AggregateOutput, AggregateSpec, PreparedAggregateStreamingInputs,
            },
            compile_predicate_slots,
            load::{ExecutionInputs, FastPathKeyResult, LoadExecutor},
            plan_metrics::{record_plan_metrics, record_rows_scanned},
            route::{
                ExecutionMode, FastPathOrder, RoutedKeyStreamRequest,
                ensure_index_range_aggregate_fast_path_specs,
                ensure_secondary_aggregate_fast_path_arity,
            },
        },
        index::predicate::IndexPredicateExecution,
        plan::{AccessPlannedQuery, validate::validate_executor_plan},
        policy,
        query::ReadConsistency,
        response::Response,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::cmp::Ordering;

///
/// AggregateReducerDispatch
///
/// AggregateReducerDispatch maps one aggregate descriptor to one kernel-owned
/// reducer execution adapter. This keeps orchestration declarative and avoids
/// re-deriving execution-mode semantics at call sites.
///

enum AggregateReducerDispatch<'a> {
    Materialized {
        spec: &'a AggregateSpec,
    },
    FieldExtremaStreaming {
        kind: AggregateKind,
        target_field: &'a str,
        direction: Direction,
        route_plan: &'a crate::db::executor::ExecutionPlan,
    },
    StreamingFold,
}

///
/// AggregateReducerSelection
///
/// AggregateReducerSelection carries either a completed aggregate output or a
/// plan that must continue through canonical streaming fold execution.
///

// Streaming selection carries an owned executable plan; keep this explicit
// expectation until reducer migration shrinks the enum payload.
#[expect(clippy::large_enum_variant)]
enum AggregateReducerSelection<E: EntityKind + EntityValue> {
    Completed(AggregateOutput<E>),
    Streaming(ExecutablePlan<E>),
}

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

impl<'a> AggregateReducerDispatch<'a> {
    // Derive one reducer adapter from a validated aggregate descriptor.
    fn from_descriptor(descriptor: &'a AggregateExecutionDescriptor) -> Self {
        if matches!(
            descriptor.route_plan.execution_mode,
            ExecutionMode::Materialized
        ) {
            return Self::Materialized {
                spec: &descriptor.spec,
            };
        }
        if let Some(target_field) = descriptor.spec.target_field() {
            return Self::FieldExtremaStreaming {
                kind: descriptor.spec.kind(),
                target_field,
                direction: descriptor.direction,
                route_plan: &descriptor.route_plan,
            };
        }

        Self::StreamingFold
    }

    // Execute eager reducers immediately, or defer to canonical streaming fold.
    fn execute_or_stream<E>(
        self,
        executor: &LoadExecutor<E>,
        plan: ExecutablePlan<E>,
    ) -> Result<AggregateReducerSelection<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match self {
            Self::Materialized { spec } => Ok(AggregateReducerSelection::Completed(
                ExecutionKernel::execute_materialized_aggregate_spec(executor, plan, spec)?,
            )),
            Self::FieldExtremaStreaming {
                kind,
                target_field,
                direction,
                route_plan,
            } => Ok(AggregateReducerSelection::Completed(
                ExecutionKernel::execute_field_target_extrema_aggregate(
                    executor,
                    plan,
                    kind,
                    target_field,
                    direction,
                    route_plan,
                )?,
            )),
            Self::StreamingFold => Ok(AggregateReducerSelection::Streaming(plan)),
        }
    }
}

impl ExecutionKernel {
    // Build one canonical aggregate descriptor so kernel dispatch works from a
    // validated shape with route-owned mode/direction/fold decisions.
    fn build_aggregate_execution_descriptor<E>(
        plan: &ExecutablePlan<E>,
        spec: AggregateSpec,
    ) -> Result<AggregateExecutionDescriptor, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        debug_assert!(
            policy::validate_plan_shape(plan.as_inner()).is_ok(),
            "aggregate executor received a plan shape that bypassed planning validation",
        );

        spec.ensure_supported_for_execution()
            .map_err(|err| InternalError::executor_unsupported(err.to_string()))?;

        // Route derivation interprets plan shape only. Re-validate first so
        // capability snapshots are always built from a validated logical plan.
        validate_executor_plan::<E>(plan.as_inner())?;

        // Route planning owns aggregate streaming/materialized decisions,
        // direction derivation, and bounded probe-hint derivation.
        let predicate_slots = compile_predicate_slots::<E>(plan.as_inner());
        let strict_index_predicate_program =
            predicate_slots
                .as_ref()
                .and_then(|resolved_predicate_slots| {
                    let index_slots = LoadExecutor::<E>::resolved_index_slots_for_access_path(
                        &plan.as_inner().access,
                    )?;
                    Self::compile_index_predicate_program_from_slots(
                        resolved_predicate_slots,
                        index_slots.as_slice(),
                        IndexPredicateCompileMode::StrictAllOrNone,
                    )
                });
        let route_plan = LoadExecutor::<E>::build_execution_route_plan_for_aggregate_spec(
            plan.as_inner(),
            spec.clone(),
        );
        let direction = route_plan.direction();

        Ok(AggregateExecutionDescriptor {
            spec,
            direction,
            route_plan,
            strict_index_predicate_program,
        })
    }

    // Consume one executable aggregate plan into canonical streaming execution
    // inputs used by both aggregate streaming branches.
    fn prepare_aggregate_streaming_inputs<E>(
        executor: &'_ LoadExecutor<E>,
        plan: ExecutablePlan<E>,
    ) -> Result<PreparedAggregateStreamingInputs<'_, E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        // Direction and specs must be read before consuming `ExecutablePlan`.
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();

        // Move into logical + compiled predicate state.
        let logical_plan = plan.into_inner();
        let predicate_slots = compile_predicate_slots::<E>(&logical_plan);

        // Re-validate executor invariants at the logical boundary.
        validate_executor_plan::<E>(&logical_plan)?;

        // Recover read context and record plan metrics before stream resolution.
        let ctx = executor.recovered_context()?;
        record_plan_metrics(&logical_plan.access);

        Ok(PreparedAggregateStreamingInputs {
            ctx,
            logical_plan,
            index_prefix_specs,
            index_range_specs,
            predicate_slots,
        })
    }

    // Execute one aggregate terminal via canonical materialized load execution.
    // Kernel owns field-target vs non-field reducer selection for this branch.
    fn execute_materialized_aggregate_spec<E>(
        executor: &LoadExecutor<E>,
        plan: ExecutablePlan<E>,
        spec: &AggregateSpec,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let kind = spec.kind();
        if let Some(target_field) = spec.target_field() {
            // Validate field-target semantics before execution to preserve
            // fail-fast unsupported behavior without scan-budget consumption.
            let field_slot = resolve_orderable_aggregate_target_slot::<E>(target_field)
                .map_err(AggregateFieldValueError::into_internal_error)?;
            let response = executor.execute(plan)?;

            return Self::aggregate_field_extrema_from_materialized(
                response,
                kind,
                target_field,
                field_slot,
            );
        }
        let response = executor.execute(plan)?;

        Ok(Self::aggregate_from_materialized(response, kind))
    }

    // Reduce one materialized response into a standard aggregate terminal
    // result using canonical materialized semantics.
    fn aggregate_from_materialized<E>(
        response: Response<E>,
        kind: AggregateKind,
    ) -> AggregateOutput<E>
    where
        E: EntityKind + EntityValue,
    {
        match kind {
            AggregateKind::Count => AggregateOutput::Count(response.count()),
            AggregateKind::Exists => AggregateOutput::Exists(!response.is_empty()),
            AggregateKind::Min => {
                AggregateOutput::Min(response.into_iter().map(|(id, _)| id).min())
            }
            AggregateKind::Max => {
                AggregateOutput::Max(response.into_iter().map(|(id, _)| id).max())
            }
            AggregateKind::First => AggregateOutput::First(response.id()),
            AggregateKind::Last => {
                AggregateOutput::Last(response.into_iter().map(|(id, _)| id).last())
            }
        }
    }

    // Reduce one materialized response into a field-target extrema id with the
    // deterministic tie-break contract `(field_value, primary_key_asc)`.
    fn aggregate_field_extrema_from_materialized<E>(
        response: Response<E>,
        kind: AggregateKind,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        if !matches!(kind, AggregateKind::Min | AggregateKind::Max) {
            return Err(InternalError::query_executor_invariant(
                "materialized field-extrema reduction requires MIN/MAX terminal",
            ));
        }
        let compare_direction = match kind {
            AggregateKind::Min => Direction::Asc,
            AggregateKind::Max => Direction::Desc,
            AggregateKind::Count
            | AggregateKind::Exists
            | AggregateKind::First
            | AggregateKind::Last => {
                return Err(InternalError::query_executor_invariant(
                    "materialized field-extrema reduction reached non-extrema terminal",
                ));
            }
        };

        let mut selected: Option<(Id<E>, E)> = None;
        for (id, entity) in response {
            let should_replace = match selected.as_ref() {
                Some((_, current)) => {
                    compare_entities_for_field_extrema(
                        &entity,
                        current,
                        target_field,
                        field_slot,
                        compare_direction,
                    )
                    .map_err(AggregateFieldValueError::into_internal_error)?
                        == Ordering::Less
                }
                None => true,
            };
            if should_replace {
                selected = Some((id, entity));
            }
        }

        let selected_id = selected.map(|(id, _)| id);

        Ok(match kind {
            AggregateKind::Min => AggregateOutput::Min(selected_id),
            AggregateKind::Max => AggregateOutput::Max(selected_id),
            AggregateKind::Count
            | AggregateKind::Exists
            | AggregateKind::First
            | AggregateKind::Last => {
                return Err(InternalError::query_executor_invariant(
                    "materialized field-extrema reduction reached non-extrema terminal",
                ));
            }
        })
    }

    // Execute one route-eligible field-target extrema aggregate through kernel-
    // owned streaming setup, stream resolution, and fold orchestration.
    fn execute_field_target_extrema_aggregate<E>(
        executor: &LoadExecutor<E>,
        plan: ExecutablePlan<E>,
        kind: AggregateKind,
        target_field: &str,
        direction: Direction,
        route_plan: &crate::db::executor::ExecutionPlan,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let field_fast_path_eligible = match kind {
            AggregateKind::Min => route_plan.field_min_fast_path_eligible(),
            AggregateKind::Max => route_plan.field_max_fast_path_eligible(),
            AggregateKind::Count
            | AggregateKind::Exists
            | AggregateKind::First
            | AggregateKind::Last => {
                return Err(InternalError::query_executor_invariant(
                    "field-target aggregate execution requires MIN/MAX terminal",
                ));
            }
        };
        if !field_fast_path_eligible {
            return Err(InternalError::query_executor_invariant(
                "field-target aggregate streaming requires route-eligible field-extrema fast path",
            ));
        }

        // Validate the field target before any stream execution work so
        // unsupported targets fail without scan-budget consumption.
        let field_slot = resolve_orderable_aggregate_target_slot::<E>(target_field)
            .map_err(AggregateFieldValueError::into_internal_error)?;

        // Reuse shared aggregate streaming setup and route-owned stream resolution.
        let prepared = Self::prepare_aggregate_streaming_inputs(executor, plan)?;
        let execution_inputs = ExecutionInputs {
            ctx: &prepared.ctx,
            plan: &prepared.logical_plan,
            stream_bindings: AccessStreamBindings {
                index_prefix_specs: prepared.index_prefix_specs.as_slice(),
                index_range_specs: prepared.index_range_specs.as_slice(),
                index_range_anchor: None,
                direction,
            },
            predicate_slots: prepared.predicate_slots.as_ref(),
        };
        let mut resolved = Self::resolve_execution_key_stream(
            &execution_inputs,
            route_plan,
            IndexPredicateCompileMode::StrictAllOrNone,
        )?;
        let (aggregate_output, keys_scanned) = LoadExecutor::<E>::fold_streaming_field_extrema(
            &prepared.ctx,
            prepared.logical_plan.consistency,
            resolved.key_stream.as_mut(),
            target_field,
            field_slot,
            kind,
            direction,
        )?;

        let rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        record_rows_scanned::<E>(rows_scanned);

        Ok(aggregate_output)
    }

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

    // Attempt aggregate fast-path execution strictly through route-owned
    // fast-path order. Returns `Some` when one branch fully resolves the terminal.
    pub(in crate::db::executor) fn try_fast_path_aggregate<E>(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        for route in inputs.route_plan.fast_path_order().iter().copied() {
            let Some(verified_route) = Self::verify_aggregate_fast_path_eligibility(inputs, route)?
            else {
                continue;
            };

            if let Some((aggregate_output, rows_scanned)) =
                Self::try_execute_verified_aggregate_fast_path(inputs, verified_route)?
            {
                return Ok(Some((aggregate_output, rows_scanned)));
            }
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
        program: Option<&crate::db::access::IndexPredicateProgram>,
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
        match kind {
            AggregateKind::Count => AggregateOutput::Count(0),
            AggregateKind::Exists => AggregateOutput::Exists(false),
            AggregateKind::Min => AggregateOutput::Min(None),
            AggregateKind::Max => AggregateOutput::Max(None),
            AggregateKind::First => AggregateOutput::First(None),
            AggregateKind::Last => AggregateOutput::Last(None),
        }
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
        if !matches!(kind, AggregateKind::Min | AggregateKind::Max) {
            return false;
        }

        let Some(fetch) = probe_fetch_hint else {
            return false;
        };
        if fetch == 0 || probe_rows_scanned < fetch {
            return false;
        }

        matches!(
            (kind, probe_output),
            (AggregateKind::Min, AggregateOutput::Min(None))
                | (AggregateKind::Max, AggregateOutput::Max(None))
        )
    }

    // Execute one aggregate spec through kernel-owned orchestration while
    // preserving route-owned execution-mode and fast-path behavior.
    pub(in crate::db::executor) fn execute_aggregate_spec<E>(
        executor: &LoadExecutor<E>,
        plan: ExecutablePlan<E>,
        spec: AggregateSpec,
    ) -> Result<AggregateOutput<E>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let descriptor = Self::build_aggregate_execution_descriptor(&plan, spec)?;
        let kind = descriptor.spec.kind();

        // Kernel-owned reducer adapter selection. Eager reducers return
        // immediately; streaming reducers continue through canonical key-stream
        // preparation and fold.
        let plan = match AggregateReducerDispatch::from_descriptor(&descriptor)
            .execute_or_stream(executor, plan)?
        {
            AggregateReducerSelection::Completed(aggregate_output) => return Ok(aggregate_output),
            AggregateReducerSelection::Streaming(plan) => plan,
        };
        let fold_mode = descriptor.route_plan.aggregate_fold_mode;
        let physical_fetch_hint = descriptor.route_plan.scan_hints.physical_fetch_hint;
        let prepared = Self::prepare_aggregate_streaming_inputs(executor, plan)?;

        let fast_path_inputs = AggregateFastPathInputs {
            ctx: &prepared.ctx,
            logical_plan: &prepared.logical_plan,
            route_plan: &descriptor.route_plan,
            index_prefix_specs: prepared.index_prefix_specs.as_slice(),
            index_range_specs: prepared.index_range_specs.as_slice(),
            index_predicate_program: descriptor.strict_index_predicate_program.as_ref(),
            direction: descriptor.direction,
            physical_fetch_hint,
            kind,
            fold_mode,
        };
        // Policy boundary: all aggregate optimizations must dispatch through the
        // route-owned fast-path order below (no ad-hoc kind-specialized branches
        // in executor call sites).
        if let Some((aggregate_output, rows_scanned)) =
            Self::try_fast_path_aggregate(&fast_path_inputs)?
        {
            record_rows_scanned::<E>(rows_scanned);
            return Ok(aggregate_output);
        }

        // Build canonical execution inputs. This must match the load executor
        // path exactly to preserve ordering and DISTINCT behavior.
        let execution_inputs = ExecutionInputs {
            ctx: &prepared.ctx,
            plan: &prepared.logical_plan,
            stream_bindings: AccessStreamBindings {
                index_prefix_specs: prepared.index_prefix_specs.as_slice(),
                index_range_specs: prepared.index_range_specs.as_slice(),
                index_range_anchor: None,
                direction: descriptor.direction,
            },
            predicate_slots: prepared.predicate_slots.as_ref(),
        };

        // Resolve the ordered key stream using canonical routing logic.
        let mut resolved = Self::resolve_execution_key_stream(
            &execution_inputs,
            &descriptor.route_plan,
            IndexPredicateCompileMode::StrictAllOrNone,
        )?;

        // Fold through the canonical kernel reducer runner. Dispatch-level
        // field-target/materialized decisions were already handled above.
        let (aggregate_output, keys_scanned) = Self::run_streaming_aggregate_reducer(
            &prepared.ctx,
            &prepared.logical_plan,
            kind,
            descriptor.direction,
            fold_mode,
            resolved.key_stream.as_mut(),
        )?;

        // Preserve row-scan metrics semantics.
        // If a fast-path overrides scan accounting, honor it.
        let rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        record_rows_scanned::<E>(rows_scanned);

        Ok(aggregate_output)
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Streaming reducer for index-leading field extrema. This keeps execution in
    // key-stream mode and stops once the first non-tie worse field value appears.
    pub(in crate::db::executor) fn fold_streaming_field_extrema(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        key_stream: &mut dyn crate::db::executor::OrderedKeyStream,
        target_field: &str,
        field_slot: FieldSlot,
        kind: AggregateKind,
        direction: Direction,
    ) -> Result<(AggregateOutput<E>, usize), InternalError> {
        if direction != Self::field_extrema_aggregate_direction(kind)? {
            return Err(InternalError::query_executor_invariant(
                "field-extrema fold direction must match aggregate terminal semantics",
            ));
        }

        let mut keys_scanned = 0usize;
        let mut selected: Option<(Id<E>, E)> = None;

        while let Some(data_key) = key_stream.next_key()? {
            keys_scanned = keys_scanned.saturating_add(1);
            let Some(entity) = Self::read_entity_for_field_extrema(ctx, consistency, &data_key)?
            else {
                continue;
            };
            let id = Id::from_key(data_key.try_key::<E>()?);
            let selected_was_empty = selected.is_none();
            let candidate_replaces = match selected.as_ref() {
                Some((_, current)) => {
                    compare_entities_for_field_extrema(
                        &entity,
                        current,
                        target_field,
                        field_slot,
                        direction,
                    )
                    .map_err(AggregateFieldValueError::into_internal_error)?
                        == Ordering::Less
                }
                None => true,
            };
            if candidate_replaces {
                selected = Some((id, entity));
                if selected_was_empty && matches!(kind, AggregateKind::Min) {
                    // MIN(field) under ascending index-leading traversal is resolved
                    // by the first in-window existing row.
                    break;
                }
                continue;
            }

            let Some((_, current)) = selected.as_ref() else {
                continue;
            };
            let field_order =
                compare_entities_by_orderable_field(&entity, current, target_field, field_slot)
                    .map_err(AggregateFieldValueError::into_internal_error)?;
            let directional_field_order = apply_aggregate_direction(field_order, direction);

            // Once traversal leaves the winning field-value group, the ordered
            // stream cannot produce a better extrema candidate.
            if directional_field_order == Ordering::Greater {
                break;
            }
        }

        let selected_id = selected.map(|(id, _)| id);
        let output = match kind {
            AggregateKind::Min => AggregateOutput::Min(selected_id),
            AggregateKind::Max => AggregateOutput::Max(selected_id),
            AggregateKind::Count
            | AggregateKind::Exists
            | AggregateKind::First
            | AggregateKind::Last => {
                return Err(InternalError::query_executor_invariant(
                    "field-extrema fold reached non-extrema terminal",
                ));
            }
        };

        Ok((output, keys_scanned))
    }
}
