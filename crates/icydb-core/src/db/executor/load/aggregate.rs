use crate::{
    db::{
        Context,
        executor::{
            AccessPlanStreamRequest, AccessStreamBindings, DistinctOrderedKeyStream,
            OrderedKeyStreamBox,
            fold::{
                AggregateFoldMode, AggregateKind, AggregateOutput, AggregateReducerState,
                AggregateSpec, AggregateWindowState, FoldControl,
            },
            load::{
                LoadExecutor,
                aggregate_field::{
                    AggregateFieldValueError, apply_aggregate_direction,
                    compare_entities_by_orderable_field, compare_entities_for_field_extrema,
                    extract_numeric_field_decimal, validate_numeric_aggregate_target_field,
                    validate_orderable_aggregate_target_field,
                },
                aggregate_guard::{
                    ensure_index_range_aggregate_fast_path_specs,
                    ensure_secondary_aggregate_fast_path_arity,
                },
                execute::ExecutionInputs,
                route::{ExecutionMode, ExecutionRoutePlan, FastPathOrder},
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
    model::field::FieldKind,
    traits::{EntityKind, EntityValue},
    types::{Decimal, Id},
};
use std::cmp::Ordering;

// -----------------------------------------------------------------------------
// Aggregate Subdomains (Pre-Split Planning)
// -----------------------------------------------------------------------------
// 1) Terminal wrappers (`count/exists/min/max/first/last`).
// 2) Aggregate orchestration (validation, route, stream setup).
// 3) Fast-path dispatch via route-owned precedence.
// 4) Fast-path implementations by access shape.
// 5) Fallback + terminal utility helpers.

///
/// NumericFieldAggregateKind
///
/// Internal selector for field-target numeric aggregate terminals.
///

#[derive(Clone, Copy)]
enum NumericFieldAggregateKind {
    Sum,
    Avg,
}

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
    // ------------------------------------------------------------------
    // Terminal wrappers
    // ------------------------------------------------------------------

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

    pub(in crate::db) fn aggregate_min_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Id<E>>, InternalError> {
        let target_field = target_field.into();
        match self.execute_aggregate_spec(
            plan,
            AggregateSpec::for_target_field(AggregateKind::Min, target_field),
        )? {
            AggregateOutput::Min(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MIN(field) result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_max_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Id<E>>, InternalError> {
        let target_field = target_field.into();
        match self.execute_aggregate_spec(
            plan,
            AggregateSpec::for_target_field(AggregateKind::Max, target_field),
        )? {
            AggregateOutput::Max(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MAX(field) result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_nth_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
        nth: usize,
    ) -> Result<Option<Id<E>>, InternalError> {
        let target_field = target_field.into();

        self.execute_nth_field_aggregate(plan, target_field.as_str(), nth)
    }

    pub(in crate::db) fn aggregate_sum_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Decimal>, InternalError> {
        self.execute_numeric_field_aggregate(
            plan,
            target_field.into().as_str(),
            NumericFieldAggregateKind::Sum,
        )
    }

    pub(in crate::db) fn aggregate_avg_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Decimal>, InternalError> {
        self.execute_numeric_field_aggregate(
            plan,
            target_field.into().as_str(),
            NumericFieldAggregateKind::Avg,
        )
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

    // ------------------------------------------------------------------
    // Aggregate orchestration
    // ------------------------------------------------------------------

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
        self.execute_aggregate_spec(plan, AggregateSpec::for_terminal(kind))
    }

    // Execute one aggregate using an explicit aggregate spec. This keeps
    // unsupported aggregate taxonomy and route capability selection under one
    // shared boundary as field-target aggregates are introduced.
    pub(in crate::db::executor) fn execute_aggregate_spec(
        &self,
        plan: ExecutablePlan<E>,
        spec: AggregateSpec,
    ) -> Result<AggregateOutput<E>, InternalError> {
        let kind = spec.kind();
        let target_field = spec.target_field().map(str::to_string);
        spec.ensure_supported_for_execution()
            .map_err(|err| InternalError::executor_unsupported(err.to_string()))?;

        // Route derivation interprets plan shape only. Re-validate first so
        // capability snapshots are always built from a validated logical plan.
        validate_executor_plan::<E>(plan.as_inner())?;

        // Route planning owns aggregate streaming/materialized decisions and
        // bounded probe-hint derivation.
        let direction = if target_field.is_some() {
            Self::field_extrema_aggregate_direction(kind)?
        } else {
            plan.direction()
        };
        let route_plan =
            Self::build_execution_route_plan_for_aggregate_spec(plan.as_inner(), spec, direction);
        if let Some(target_field) = target_field {
            return self.execute_field_target_extrema_aggregate(
                plan,
                kind,
                target_field.as_str(),
                direction,
                &route_plan,
            );
        }
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

    // Execute `min(field)` / `max(field)` via canonical materialized fallback.
    // Route still owns eligibility and hint derivation; this branch currently
    // keeps field-target semantics correctness-first until fast paths are enabled.
    fn execute_field_target_extrema_aggregate(
        &self,
        plan: ExecutablePlan<E>,
        kind: AggregateKind,
        target_field: &str,
        direction: Direction,
        route_plan: &ExecutionRoutePlan,
    ) -> Result<AggregateOutput<E>, InternalError> {
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

        // Validate user-provided field targets before any scan-budget consumption.
        let expected_kind = validate_orderable_aggregate_target_field::<E>(target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        if !field_fast_path_eligible {
            // Preserve canonical query semantics by selecting candidates from the
            // fully materialized response window and then applying field-extrema rules.
            let response = self.execute(plan)?;
            return Self::aggregate_field_extrema_from_materialized(
                response,
                kind,
                target_field,
                expected_kind,
            );
        }
        if !matches!(route_plan.execution_mode, ExecutionMode::Streaming) {
            return Err(InternalError::query_executor_invariant(
                "field-extrema fast path requires streaming execution mode",
            ));
        }

        // Route-planned streaming path for index-leading field extrema.
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let logical_plan = plan.into_inner();
        validate_executor_plan::<E>(&logical_plan)?;
        let ctx = self.db.recovered_context::<E>()?;
        record_plan_metrics(&logical_plan.access);
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
        let mut resolved = Self::resolve_execution_key_stream(&execution_inputs, route_plan)?;
        let (aggregate_output, keys_scanned) = Self::fold_streaming_field_extrema(
            &ctx,
            logical_plan.consistency,
            resolved.key_stream.as_mut(),
            target_field,
            expected_kind,
            kind,
            direction,
        )?;
        let rows_scanned = resolved.rows_scanned_override.unwrap_or(keys_scanned);
        record_rows_scanned::<E>(rows_scanned);

        Ok(aggregate_output)
    }

    // Execute one field-target numeric aggregate (`sum(field)` / `avg(field)`)
    // via canonical materialized fallback semantics.
    fn execute_numeric_field_aggregate(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
        kind: NumericFieldAggregateKind,
    ) -> Result<Option<Decimal>, InternalError> {
        let expected_kind = validate_numeric_aggregate_target_field::<E>(target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        let response = self.execute(plan)?;

        Self::aggregate_numeric_field_from_materialized(response, target_field, expected_kind, kind)
    }

    // Execute one field-target nth aggregate (`nth(field, n)`) via canonical
    // materialized fallback semantics.
    fn execute_nth_field_aggregate(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
        nth: usize,
    ) -> Result<Option<Id<E>>, InternalError> {
        let expected_kind = validate_orderable_aggregate_target_field::<E>(target_field)
            .map_err(Self::map_aggregate_field_value_error)?;
        let response = self.execute(plan)?;

        Self::aggregate_nth_field_from_materialized(response, target_field, expected_kind, nth)
    }

    // ------------------------------------------------------------------
    // Fast-path dispatch
    // ------------------------------------------------------------------

    // Attempt aggregate fast-path execution strictly through route-owned
    // fast-path order. Returns `Some` when one branch fully resolves the terminal.
    fn try_fast_path_aggregate(
        inputs: &AggregateFastPathInputs<'_, '_, E>,
    ) -> Result<Option<(AggregateOutput<E>, usize)>, InternalError> {
        for route in inputs.route_plan.fast_path_order().iter().copied() {
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
                            inputs.fold_mode,
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

    // ------------------------------------------------------------------
    // Fast-path implementations
    // ------------------------------------------------------------------

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

    // Reduce one materialized response into a field-target extrema id with the
    // deterministic tie-break contract `(field_value, primary_key_asc)`.
    fn aggregate_field_extrema_from_materialized(
        response: Response<E>,
        kind: AggregateKind,
        target_field: &str,
        expected_kind: FieldKind,
    ) -> Result<AggregateOutput<E>, InternalError> {
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
                        expected_kind,
                        compare_direction,
                    )
                    .map_err(Self::map_aggregate_field_value_error)?
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

    // Reduce one materialized response into `sum(field)` / `avg(field)` over
    // numeric field values coerced to Decimal.
    fn aggregate_numeric_field_from_materialized(
        response: Response<E>,
        target_field: &str,
        expected_kind: FieldKind,
        kind: NumericFieldAggregateKind,
    ) -> Result<Option<Decimal>, InternalError> {
        let mut sum = Decimal::ZERO;
        let mut row_count = 0u64;
        for (_, entity) in response {
            let value = extract_numeric_field_decimal(&entity, target_field, expected_kind)
                .map_err(Self::map_aggregate_field_value_error)?;
            sum += value;
            row_count = row_count.saturating_add(1);
        }
        if row_count == 0 {
            return Ok(None);
        }

        let output = match kind {
            NumericFieldAggregateKind::Sum => sum,
            NumericFieldAggregateKind::Avg => {
                let Some(divisor) = Decimal::from_num(row_count) else {
                    return Err(InternalError::query_executor_invariant(
                        "numeric field AVG divisor conversion overflowed decimal bounds",
                    ));
                };

                sum / divisor
            }
        };

        Ok(Some(output))
    }

    // Reduce one materialized response into `nth(field, n)` using deterministic
    // ordering `(field_value_asc, primary_key_asc)`.
    fn aggregate_nth_field_from_materialized(
        response: Response<E>,
        target_field: &str,
        expected_kind: FieldKind,
        nth: usize,
    ) -> Result<Option<Id<E>>, InternalError> {
        // Phase 1: materialize rows into deterministic field ordering so
        // position selection is stable under ties.
        let mut ordered_rows: Vec<(Id<E>, E)> = Vec::new();
        for candidate in response {
            let mut insert_index = ordered_rows.len();
            for (index, (_, current)) in ordered_rows.iter().enumerate() {
                let ordering = compare_entities_for_field_extrema(
                    &candidate.1,
                    current,
                    target_field,
                    expected_kind,
                    Direction::Asc,
                )
                .map_err(Self::map_aggregate_field_value_error)?;
                if ordering == Ordering::Less {
                    insert_index = index;
                    break;
                }
            }

            ordered_rows.insert(insert_index, candidate);
        }

        // Phase 2: project the requested ordinal position.
        if nth >= ordered_rows.len() {
            return Ok(None);
        }

        Ok(ordered_rows.into_iter().nth(nth).map(|(id, _)| id))
    }

    // Streaming reducer for index-leading field extrema. This keeps execution in
    // key-stream mode and stops once the first non-tie worse field value appears.
    fn fold_streaming_field_extrema(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        key_stream: &mut dyn crate::db::executor::OrderedKeyStream,
        target_field: &str,
        expected_kind: FieldKind,
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
                        expected_kind,
                        direction,
                    )
                    .map_err(Self::map_aggregate_field_value_error)?
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
                compare_entities_by_orderable_field(&entity, current, target_field, expected_kind)
                    .map_err(Self::map_aggregate_field_value_error)?;
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

    // Load one entity for field-extrema stream folding while preserving read
    // consistency classification behavior.
    fn read_entity_for_field_extrema(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        key: &crate::db::data::DataKey,
    ) -> Result<Option<E>, InternalError> {
        let decode_row = |row| {
            let mut decoded = Context::<E>::deserialize_rows(vec![(key.clone(), row)])?;
            let Some((_, entity)) = decoded.pop() else {
                return Err(InternalError::query_executor_invariant(
                    "field-extrema row decode expected one decoded entity",
                ));
            };

            Ok(entity)
        };
        match consistency {
            ReadConsistency::Strict => {
                let row = ctx.read_strict(key)?;
                Ok(Some(decode_row(row)?))
            }
            ReadConsistency::MissingOk => match ctx.read(key) {
                Ok(row) => Ok(Some(decode_row(row)?)),
                Err(err) if err.is_not_found() => Ok(None),
                Err(err) => Err(err),
            },
        }
    }

    fn field_extrema_aggregate_direction(kind: AggregateKind) -> Result<Direction, InternalError> {
        match kind {
            AggregateKind::Min => Ok(Direction::Asc),
            AggregateKind::Max => Ok(Direction::Desc),
            AggregateKind::Count
            | AggregateKind::Exists
            | AggregateKind::First
            | AggregateKind::Last => Err(InternalError::query_executor_invariant(
                "field-target aggregate direction requires MIN/MAX terminal",
            )),
        }
    }

    // Map field-target aggregate extraction/comparison failures into taxonomy-correct
    // execution errors.
    fn map_aggregate_field_value_error(err: AggregateFieldValueError) -> InternalError {
        let message = err.to_string();
        match err {
            AggregateFieldValueError::UnknownField { .. }
            | AggregateFieldValueError::UnsupportedFieldKind { .. } => {
                InternalError::executor_unsupported(message)
            }
            AggregateFieldValueError::MissingFieldValue { .. }
            | AggregateFieldValueError::FieldValueTypeMismatch { .. }
            | AggregateFieldValueError::IncomparableFieldValues { .. } => {
                InternalError::query_executor_invariant(message)
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
        fold_mode: AggregateFoldMode,
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
        let mut state = AggregateReducerState::for_kind(kind);
        for key in ordered_keys {
            if window.exhausted() {
                break;
            }

            keys_scanned = keys_scanned.saturating_add(1);
            let data_key = Context::<E>::data_key_from_key(key)?;
            if !Self::key_qualifies_for_fold(ctx, plan.consistency, fold_mode, &data_key)? {
                continue;
            }
            if !window.accept_existing_row() {
                continue;
            }
            if matches!(
                state.update_from_data_key(kind, direction, &data_key)?,
                FoldControl::Break
            ) {
                break;
            }
        }

        // Phase 3: project one terminal output from the reducer state.
        let aggregate_output = state.into_output();

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
        if !inputs.route_plan.secondary_fast_path_eligible() {
            return Ok(None);
        }
        // Probe hint selection is route-owned; executor only consumes it.
        let probe_fetch_hint = inputs.route_plan.secondary_extrema_probe_fetch_hint();
        let Some(mut fast) = Self::try_execute_secondary_index_order_stream(
            ctx,
            inputs.logical_plan,
            inputs.index_prefix_specs.first(),
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

        let (aggregate_output, keys_scanned) = Self::fold_streaming_aggregate(
            inputs.ctx,
            inputs.logical_plan,
            inputs.logical_plan.consistency,
            inputs.direction,
            key_stream.as_mut(),
            inputs.kind,
            inputs.fold_mode,
        )?;

        Ok(Some((aggregate_output, keys_scanned)))
    }

    // ------------------------------------------------------------------
    // Fallback and terminal utilities
    // ------------------------------------------------------------------

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
