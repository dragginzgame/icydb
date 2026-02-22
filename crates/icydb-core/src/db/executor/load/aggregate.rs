use crate::{
    db::{
        Context,
        executor::{
            AccessPlanStreamRequest, AccessStreamBindings, DistinctOrderedKeyStream,
            OrderedKeyStreamBox,
            fold::{AggregateFoldMode, AggregateKind, AggregateOutput, AggregateWindowState},
            load::{
                LoadExecutor,
                aggregate_guard::{
                    ensure_index_range_aggregate_fast_path_specs,
                    ensure_secondary_aggregate_fast_path_arity,
                },
                execute::ExecutionInputs,
                route::{
                    AGGREGATE_FAST_PATH_ORDER, ExecutionMode, ExecutionRoutePlan, FastPathOrder,
                },
            },
            plan::{record_plan_metrics, record_rows_scanned},
        },
        query::{
            ReadConsistency,
            plan::{
                AccessPath, Direction, ExecutablePlan, IndexPrefixSpec, IndexRangeSpec,
                LogicalPlan, validate::validate_executor_plan,
            },
        },
        response::Response,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};

///
/// AggregateFastPathInputs
///
/// Aggregate fast-path execution inputs bundled for one dispatch entry.
/// Keeps branch routing parameters aligned between aggregate path helpers.
///

struct AggregateFastPathInputs<'exec, 'ctx, E: EntityKind + EntityValue> {
    ctx: &'exec Context<'ctx, E>,
    logical_plan: &'exec LogicalPlan<E::Key>,
    route_plan: &'exec ExecutionRoutePlan,
    index_prefix_specs: &'exec [IndexPrefixSpec],
    index_range_specs: &'exec [IndexRangeSpec],
    direction: Direction,
    physical_fetch_hint: Option<usize>,
    kind: AggregateKind,
    fold_mode: AggregateFoldMode,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(in crate::db) fn aggregate_count(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<u32, InternalError> {
        match self.execute_aggregate(plan, AggregateKind::Count)? {
            AggregateOutput::Count(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate COUNT result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_exists(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<bool, InternalError> {
        match self.execute_aggregate(plan, AggregateKind::Exists)? {
            AggregateOutput::Exists(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate EXISTS result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_min(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match self.execute_aggregate(plan, AggregateKind::Min)? {
            AggregateOutput::Min(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MIN result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_max(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match self.execute_aggregate(plan, AggregateKind::Max)? {
            AggregateOutput::Max(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MAX result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_first(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match self.execute_aggregate(plan, AggregateKind::First)? {
            AggregateOutput::First(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate FIRST result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_last(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match self.execute_aggregate(plan, AggregateKind::Last)? {
            AggregateOutput::Last(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate LAST result kind mismatch",
            )),
        }
    }

    // Execute one aggregate terminal. Use streaming fold for conservative-safe
    // plan shapes, otherwise fall back to canonical materialized execution.
    //
    // IMPORTANT:
    // - Streaming eligibility must remain aligned with load fast-path routing.
    // - COUNT pushdown (0.22.1+) must remain a strict subset of streaming safety.
    // - This function must reuse the same key-stream construction path as `execute()`
    //   to preserve ordering, DISTINCT, and pagination semantics.
    fn execute_aggregate(
        &self,
        plan: ExecutablePlan<E>,
        kind: AggregateKind,
    ) -> Result<AggregateOutput<E>, InternalError> {
        // Route derivation interprets plan shape only. Re-validate first so
        // capability snapshots are always built from a validated logical plan.
        validate_executor_plan::<E>(plan.as_inner())?;

        // Route planning owns aggregate streaming/materialized decisions and
        // bounded probe-hint derivation.
        let direction = plan.direction();
        let route_plan =
            Self::build_execution_route_plan_for_aggregate(plan.as_inner(), kind, direction);
        if matches!(route_plan.execution_mode, ExecutionMode::Materialized) {
            let response = self.execute(plan)?;
            return Ok(Self::aggregate_from_materialized(response, kind));
        }
        let fold_mode = route_plan.aggregate_fold_mode;
        let physical_fetch_hint = route_plan.scan_hints.physical_fetch_hint;

        // Direction must be captured before consuming the ExecutablePlan.
        // After `into_inner()`, we operate purely on LogicalPlan.
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();

        // Move into the underlying logical plan.
        // After this point, `plan` is consumed.
        let logical_plan = plan.into_inner();

        // Re-validate executor invariants at the logical boundary.
        validate_executor_plan::<E>(&logical_plan)?;

        // Obtain recovered execution context (read-consistency aware).
        let ctx = self.db.recovered_context::<E>()?;

        // Record plan-level metrics before execution begins.
        // This mirrors the load execution path.
        record_plan_metrics(&logical_plan.access);

        let fast_path_inputs = AggregateFastPathInputs {
            ctx: &ctx,
            logical_plan: &logical_plan,
            route_plan: &route_plan,
            index_prefix_specs: index_prefix_specs.as_slice(),
            index_range_specs: index_range_specs.as_slice(),
            direction,
            physical_fetch_hint,
            kind,
            fold_mode,
        };
        if let Some((aggregate_output, rows_scanned)) =
            Self::try_fast_path_aggregate(&fast_path_inputs)?
        {
            record_rows_scanned::<E>(rows_scanned);
            return Ok(aggregate_output);
        }

        // Build canonical execution inputs. This must match the load executor
        // path exactly to preserve ordering and DISTINCT behavior.
        let execution_inputs = ExecutionInputs {
            ctx: &ctx,
            plan: &logical_plan,
            stream_bindings: AccessStreamBindings {
                index_prefix_specs: index_prefix_specs.as_slice(),
                index_range_specs: index_range_specs.as_slice(),
                index_range_anchor: None,
                direction,
            },
        };

        // Resolve the ordered key stream using canonical routing logic.
        let mut resolved = Self::resolve_execution_key_stream(&execution_inputs, &route_plan)?;

        // Fold via one streaming engine. COUNT pushdown uses key-only mode;
        // other terminals use row-existence mode.
        let (aggregate_output, keys_scanned) = Self::fold_streaming_aggregate(
            &ctx,
            &logical_plan,
            logical_plan.consistency,
            direction,
            resolved.key_stream.as_mut(),
            kind,
            fold_mode,
        )?;

        // Preserve row-scan metrics semantics.
        // If a fast-path overrides scan accounting, honor it.
        let rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        record_rows_scanned::<E>(rows_scanned);

        Ok(aggregate_output)
    }

    // Attempt aggregate fast-path execution in canonical priority order.
    // Returns `Some` when one branch fully resolves the terminal.
    fn try_fast_path_aggregate(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        for route in AGGREGATE_FAST_PATH_ORDER {
            match route {
                FastPathOrder::PrimaryKey => {
                    // Aggregate-aware fast path for primary-key point/batch access shapes.
                    // This keeps semantics identical while avoiding generic stream setup.
                    if let Some((aggregate_output, rows_scanned)) =
                        Self::try_execute_primary_key_access_aggregate(
                            inputs.ctx,
                            inputs.logical_plan,
                            inputs.direction,
                            inputs.kind,
                        )?
                    {
                        return Ok(Some((aggregate_output, rows_scanned)));
                    }
                }
                FastPathOrder::SecondaryPrefix => {
                    // Aggregate-aware fast path for secondary index-prefix plans that are
                    // eligible for canonical order pushdown.
                    if let Some((aggregate_output, rows_scanned)) =
                        Self::try_execute_index_prefix_aggregate(
                            inputs.ctx,
                            inputs,
                            inputs.direction,
                            inputs.kind,
                            inputs.fold_mode,
                        )?
                    {
                        return Ok(Some((aggregate_output, rows_scanned)));
                    }
                }
                FastPathOrder::PrimaryScan => {
                    // Aggregate-aware fast path for primary-data range/full scans.
                    // This reuses canonical fold logic while skipping generic stream routing.
                    if inputs.physical_fetch_hint.is_some()
                        && let Some((aggregate_output, rows_scanned)) =
                            Self::try_execute_primary_scan_aggregate(
                                inputs.ctx,
                                inputs.logical_plan,
                                inputs.direction,
                                inputs.physical_fetch_hint,
                                inputs.kind,
                                inputs.fold_mode,
                            )?
                    {
                        return Ok(Some((aggregate_output, rows_scanned)));
                    }
                }
                FastPathOrder::IndexRange => {
                    // Aggregate-aware fast path for index-range plans using lowered
                    // byte-level range specs and shared fold semantics.
                    if let Some((aggregate_output, rows_scanned)) =
                        Self::try_execute_index_range_aggregate(inputs)?
                    {
                        return Ok(Some((aggregate_output, rows_scanned)));
                    }
                }
                FastPathOrder::Composite => {
                    // Aggregate-aware fast path for composite plans. This reuses canonical
                    // composite stream construction and keeps aggregate folding shared.
                    if let Some((aggregate_output, rows_scanned)) =
                        Self::try_execute_composite_aggregate(inputs)?
                    {
                        return Ok(Some((aggregate_output, rows_scanned)));
                    }
                }
            }
        }

        // Fast exit: effective limit == 0 has an empty aggregate window and can
        // return terminal defaults without constructing or scanning key streams.
        if inputs.physical_fetch_hint == Some(0) {
            return Ok(Some((Self::aggregate_zero_window_result(inputs.kind), 0)));
        }

        Ok(None)
    }

    // Return the aggregate terminal value for an empty effective output window.
    const fn aggregate_zero_window_result(kind: AggregateKind) -> AggregateOutput<E> {
        match kind {
            AggregateKind::Count => AggregateOutput::Count(0),
            AggregateKind::Exists => AggregateOutput::Exists(false),
            AggregateKind::Min => AggregateOutput::Min(None),
            AggregateKind::Max => AggregateOutput::Max(None),
            AggregateKind::First => AggregateOutput::First(None),
            AggregateKind::Last => AggregateOutput::Last(None),
        }
    }

    fn aggregate_from_materialized(
        response: Response<E>,
        kind: AggregateKind,
    ) -> AggregateOutput<E> {
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

    // Resolve aggregate terminals directly for primary-key point/batch plans.
    // This preserves consistency + window semantics without building streams.
    fn try_execute_primary_key_access_aggregate(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        direction: Direction,
        kind: AggregateKind,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        let Some(path) = plan.access.as_path() else {
            return Ok(None);
        };
        let ordered_keys = match path {
            AccessPath::ByKey(key) => vec![*key],
            AccessPath::ByKeys(keys) => {
                let mut deduped = Context::<E>::dedup_keys(keys.clone());
                if direction == Direction::Desc {
                    deduped.reverse();
                }

                deduped
            }
            _ => return Ok(None),
        };
        if ordered_keys.is_empty() {
            return Ok(None);
        }
        if plan.predicate.is_some() {
            return Ok(None);
        }

        // Phase 1: apply window exhaustion before touching storage.
        let mut window = AggregateWindowState::from_plan(plan);
        if window.exhausted() {
            return Ok(Some((Self::aggregate_zero_window_result(kind), 0)));
        }

        // Phase 2: iterate canonical candidate keys and enforce the same
        // consistency + window semantics used by streaming aggregation.
        let mut keys_scanned = 0usize;
        let mut count = 0u32;
        let mut exists = false;
        let mut min_id = None::<Id<E>>;
        let mut max_id = None::<Id<E>>;
        let mut first_id = None::<Id<E>>;
        let mut last_id = None::<Id<E>>;
        for key in ordered_keys {
            if window.exhausted() {
                break;
            }

            keys_scanned = keys_scanned.saturating_add(1);
            let data_key = Context::<E>::data_key_from_key(key)?;
            if !Self::key_qualifies_for_fold(
                ctx,
                plan.consistency,
                AggregateFoldMode::ExistingRows,
                &data_key,
            )? {
                continue;
            }
            if !window.accept_existing_row() {
                continue;
            }

            let id = Id::from_key(key);
            match kind {
                AggregateKind::Count => {
                    count = count.saturating_add(1);
                }
                AggregateKind::Exists => {
                    exists = true;
                    break;
                }
                AggregateKind::Min => {
                    min_id = Some(id);
                    if direction == Direction::Asc {
                        break;
                    }
                }
                AggregateKind::Max => {
                    max_id = Some(id);
                    if direction == Direction::Desc {
                        break;
                    }
                }
                AggregateKind::First => {
                    first_id = Some(id);
                    break;
                }
                AggregateKind::Last => {
                    last_id = Some(id);
                }
            }
        }

        // Phase 3: project one terminal output from the reducer state.
        let aggregate_output = match kind {
            AggregateKind::Count => AggregateOutput::Count(count),
            AggregateKind::Exists => AggregateOutput::Exists(exists),
            AggregateKind::Min => AggregateOutput::Min(min_id),
            AggregateKind::Max => AggregateOutput::Max(max_id),
            AggregateKind::First => AggregateOutput::First(first_id),
            AggregateKind::Last => AggregateOutput::Last(last_id),
        };

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // Resolve aggregate terminals directly for index-prefix access plans when
    // canonical secondary ordering is pushdown-eligible.
    fn try_execute_index_prefix_aggregate(
        ctx: &Context<'_, E>,
        inputs: &AggregateFastPathInputs<'_, '_, E>,
        direction: Direction,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        ensure_secondary_aggregate_fast_path_arity(
            inputs.route_plan.secondary_fast_path_eligible(),
            inputs.index_prefix_specs.len(),
        )?;
        let probe_fetch_hint =
            Self::secondary_extrema_probe_fetch_hint(kind, inputs.physical_fetch_hint);
        let Some(mut fast) = Self::try_execute_secondary_index_order_stream(
            ctx,
            inputs.logical_plan,
            inputs.index_prefix_specs.first(),
            &inputs.route_plan.secondary_pushdown_applicability,
            probe_fetch_hint,
        )?
        else {
            return Ok(None);
        };
        fast.ordered_key_stream =
            Self::maybe_wrap_distinct_stream(fast.ordered_key_stream, inputs.logical_plan.distinct);

        let probe_rows_scanned = fast.rows_scanned;
        if let Some(fetch) = probe_fetch_hint {
            debug_assert!(
                probe_rows_scanned <= fetch,
                "secondary extrema probe rows_scanned must not exceed bounded fetch",
            );
        }
        let (probe_output, _probe_keys_scanned) = Self::fold_streaming_aggregate(
            ctx,
            inputs.logical_plan,
            inputs.logical_plan.consistency,
            direction,
            fast.ordered_key_stream.as_mut(),
            kind,
            fold_mode,
        )?;

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
        let Some(mut fallback) = Self::try_execute_secondary_index_order_stream(
            ctx,
            inputs.logical_plan,
            inputs.index_prefix_specs.first(),
            &inputs.route_plan.secondary_pushdown_applicability,
            // Keep native index traversal order for fallback retries.
            Some(usize::MAX),
        )?
        else {
            return Ok(None);
        };
        fallback.ordered_key_stream = Self::maybe_wrap_distinct_stream(
            fallback.ordered_key_stream,
            inputs.logical_plan.distinct,
        );
        let fallback_rows_scanned = fallback.rows_scanned;
        let (aggregate_output, _fallback_keys_scanned) = Self::fold_streaming_aggregate(
            ctx,
            inputs.logical_plan,
            inputs.logical_plan.consistency,
            direction,
            fallback.ordered_key_stream.as_mut(),
            kind,
            fold_mode,
        )?;

        Ok(Some((
            aggregate_output,
            probe_rows_scanned.saturating_add(fallback_rows_scanned),
        )))
    }

    // Resolve aggregate terminals directly for full-scan/key-range access plans.
    // This keeps canonical stream semantics while avoiding generic route assembly.
    fn try_execute_primary_scan_aggregate(
        ctx: &Context<'_, E>,
        plan: &LogicalPlan<E::Key>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        kind: AggregateKind,
        fold_mode: AggregateFoldMode,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        let Some(path) = plan.access.as_path() else {
            return Ok(None);
        };
        if !matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. }) {
            return Ok(None);
        }

        let mut key_stream = ctx.ordered_key_stream_from_access_with_index_range_anchor(
            path,
            None,
            None,
            None,
            direction,
            physical_fetch_hint,
        )?;
        let (aggregate_output, keys_scanned) = Self::fold_streaming_aggregate(
            ctx,
            plan,
            plan.consistency,
            direction,
            key_stream.as_mut(),
            kind,
            fold_mode,
        )?;

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // Resolve aggregate terminals directly for index-range access plans.
    // This reuses canonical range traversal while preserving one fold engine.
    fn try_execute_index_range_aggregate(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        ensure_index_range_aggregate_fast_path_specs(
            inputs.route_plan.index_range_limit_fast_path_enabled(),
            inputs.index_prefix_specs.len(),
            inputs.index_range_specs.len(),
        )?;
        let Some(index_range_limit_spec) = inputs.route_plan.index_range_limit_spec.as_ref() else {
            return Ok(None);
        };

        let Some(mut fast) = Self::try_execute_index_range_limit_pushdown_stream(
            inputs.ctx,
            inputs.logical_plan,
            inputs.index_range_specs.first(),
            None,
            inputs.direction,
            index_range_limit_spec.fetch,
        )?
        else {
            return Ok(None);
        };
        fast.ordered_key_stream =
            Self::maybe_wrap_distinct_stream(fast.ordered_key_stream, inputs.logical_plan.distinct);

        let rows_scanned = fast.rows_scanned;
        let (aggregate_output, _keys_scanned) = Self::fold_streaming_aggregate(
            inputs.ctx,
            inputs.logical_plan,
            inputs.logical_plan.consistency,
            inputs.direction,
            fast.ordered_key_stream.as_mut(),
            inputs.kind,
            inputs.fold_mode,
        )?;

        Ok(Some((aggregate_output, rows_scanned)))
    }

    // Resolve aggregate terminals directly for composite access plans by
    // reusing canonical composite stream production.
    fn try_execute_composite_aggregate(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        if !inputs.route_plan.composite_aggregate_fast_path_eligible() {
            return Ok(None);
        }

        let stream_request = AccessPlanStreamRequest {
            access: &inputs.logical_plan.access,
            bindings: AccessStreamBindings {
                index_prefix_specs: inputs.index_prefix_specs,
                index_range_specs: inputs.index_range_specs,
                index_range_anchor: None,
                direction: inputs.direction,
            },
            key_comparator: super::key_stream_comparator_from_plan(
                inputs.logical_plan,
                inputs.direction,
            ),
            physical_fetch_hint: inputs.physical_fetch_hint,
        };
        let mut key_stream = inputs
            .ctx
            .ordered_key_stream_from_access_plan_with_index_range_anchor(stream_request)?;

        // Composite paths must remain row-aware for COUNT in 0.24 scope.
        let fold_mode = AggregateFoldMode::ExistingRows;
        let (aggregate_output, keys_scanned) = Self::fold_streaming_aggregate(
            inputs.ctx,
            inputs.logical_plan,
            inputs.logical_plan.consistency,
            inputs.direction,
            key_stream.as_mut(),
            inputs.kind,
            fold_mode,
        )?;

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // Secondary extrema single-step probes are only meaningful when route
    // planning computed a bounded hint for Min/Max endpoint-compatible shapes.
    const fn secondary_extrema_probe_fetch_hint(
        kind: AggregateKind,
        physical_fetch_hint: Option<usize>,
    ) -> Option<usize> {
        match kind {
            AggregateKind::Min | AggregateKind::Max => physical_fetch_hint,
            AggregateKind::Count
            | AggregateKind::Exists
            | AggregateKind::First
            | AggregateKind::Last => None,
        }
    }

    // MissingOk can skip stale leading index entries. If a bounded Min/Max
    // probe returns None exactly at the fetch boundary, the outcome is
    // inconclusive and must retry unbounded.
    const fn secondary_extrema_probe_requires_fallback(
        consistency: ReadConsistency,
        kind: AggregateKind,
        probe_fetch_hint: Option<usize>,
        probe_output: &AggregateOutput<E>,
        probe_rows_scanned: usize,
    ) -> bool {
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

    // Wrap fast-path streams with DISTINCT semantics only when requested.
    fn maybe_wrap_distinct_stream(
        ordered_key_stream: OrderedKeyStreamBox,
        distinct: bool,
    ) -> OrderedKeyStreamBox {
        if distinct {
            return Box::new(DistinctOrderedKeyStream::new(ordered_key_stream));
        }

        ordered_key_stream
    }
}
