//! Module: executor::aggregate::projection
//! Responsibility: field-value projection terminals over materialized responses.
//! Does not own: grouped key canonicalization internals or route planning logic.
//! Boundary: projection terminal helpers (`values`, `distinct_values`, `first/last value`).
//!
//! `distinct_values_by(field)` here is a non-grouped effective-window helper.
//! Grouped Class B DISTINCT accounting is enforced only through grouped
//! execution context boundaries.

mod covering;

use crate::{
    db::{
        data::{DataKey, DataRow},
        direction::Direction,
        executor::{
            CoveringProjectionComponentRows, ExecutablePlan, ExecutionKernel,
            PreparedAggregatePlan,
            aggregate::{
                AggregateKind, PreparedAggregateSpec, PreparedAggregateStreamingInputs,
                PreparedAggregateTargetField, PreparedCoveringDistinctStrategy,
                PreparedScalarProjectionExecutionState, PreparedScalarProjectionOp,
                PreparedScalarProjectionStrategy, ScalarProjectionWindow,
                field::{
                    AggregateFieldValueError, FieldSlot,
                    extract_orderable_field_value_from_decoded_slot,
                    extract_orderable_field_value_with_slot_ref_reader,
                    resolve_any_aggregate_target_slot_from_planner_slot,
                    resolve_orderable_aggregate_target_slot_from_planner_slot,
                },
                materialized_distinct::insert_materialized_distinct_value,
                projection::covering::{
                    CoveringProjectionValues, covering_index_adjacent_distinct_eligible,
                    covering_index_projection_context, dedup_adjacent_values,
                    dedup_values_preserving_first, scalar_window_for_covering_projection,
                },
            },
            covering_projection_scan_direction, covering_requires_row_presence_check,
            decode_single_covering_projection_pairs,
            group::GroupKeySet,
            pipeline::contracts::LoadExecutor,
            reorder_covering_projection_pairs,
            resolve_covering_projection_components_from_lowered_specs,
            terminal::{RowDecoder, RowLayout},
        },
        predicate::MissingRowPolicy,
        query::plan::{
            CoveringProjectionContext, FieldSlot as PlannedFieldSlot,
            constant_covering_projection_value_from_access,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
    value::Value,
};

type ValueProjection = Vec<(DataKey, Value)>;
type CoveringProjectionPairRows = Vec<(DataKey, Value)>;
type CoveringProjectionPairsResolution = Result<Option<CoveringProjectionPairRows>, InternalError>;

// Typed boundary request for one scalar field-projection terminal family call.
pub(in crate::db) enum ScalarProjectionBoundaryRequest {
    Values,
    DistinctValues,
    CountNonNull,
    CountDistinct,
    ValuesWithIds,
    TerminalValue { terminal_kind: AggregateKind },
}

// Typed boundary output for one scalar field-projection terminal family call.
pub(in crate::db) enum ScalarProjectionBoundaryOutput {
    Count(u32),
    Values(Vec<Value>),
    ValuesWithDataKeys(ValueProjection),
    TerminalValue(Option<Value>),
}

impl ScalarProjectionBoundaryOutput {
    // Build the canonical boundary mismatch for projection output decoding.
    fn output_kind_mismatch(message: &'static str) -> InternalError {
        InternalError::query_executor_invariant(message)
    }

    // Decode one plain-value projection boundary output.
    pub(in crate::db) fn into_values(self) -> Result<Vec<Value>, InternalError> {
        match self {
            Self::Values(values) => Ok(values),
            _ => Err(Self::output_kind_mismatch(
                "scalar projection boundary values output kind mismatch",
            )),
        }
    }

    // Decode one count-distinct projection boundary output.
    pub(in crate::db) fn into_count(self) -> Result<u32, InternalError> {
        match self {
            Self::Count(value) => Ok(value),
            _ => Err(Self::output_kind_mismatch(
                "scalar projection boundary count output kind mismatch",
            )),
        }
    }

    // Decode one `(id, value)` projection boundary output.
    pub(in crate::db) fn into_values_with_ids<E>(self) -> Result<Vec<(Id<E>, Value)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        match self {
            Self::ValuesWithDataKeys(values) => values
                .into_iter()
                .map(|(data_key, value)| Ok((Id::from_key(data_key.try_key::<E>()?), value)))
                .collect(),
            _ => Err(Self::output_kind_mismatch(
                "scalar projection boundary values-with-ids output kind mismatch",
            )),
        }
    }

    // Decode one terminal-value projection boundary output.
    pub(in crate::db) fn into_terminal_value(self) -> Result<Option<Value>, InternalError> {
        match self {
            Self::TerminalValue(value) => Ok(value),
            _ => Err(Self::output_kind_mismatch(
                "scalar projection boundary terminal-value output kind mismatch",
            )),
        }
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one `MIN(field)` / `MAX(field)` value request through the
    // aggregate field-extrema path and then project the winning row's field
    // value. This keeps SQL aggregate extrema on the dedicated aggregate
    // route instead of the two-step ranked-id plus follow-up field load path.
    pub(in crate::db) fn execute_scalar_extrema_value_boundary(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        terminal_kind: AggregateKind,
    ) -> Result<Option<Value>, InternalError> {
        if !terminal_kind.is_extrema() {
            return Err(InternalError::query_executor_invariant(
                "scalar extrema value boundary requires MIN/MAX aggregate kind",
            ));
        }

        let plan = plan.into_prepared_aggregate_plan();
        let field_slot = resolve_orderable_aggregate_target_slot_from_planner_slot(&target_field)
            .map_err(AggregateFieldValueError::into_internal_error)?;
        let prepared = self.prepare_scalar_aggregate_boundary(plan)?;

        self.execute_selected_value_field_projection_with_slot(
            prepared,
            target_field.field(),
            field_slot,
            terminal_kind,
        )
    }

    // Execute one scalar field-projection terminal family request from the
    // typed API boundary, lower plan-derived policy into one prepared
    // projection contract, and then execute that contract.
    pub(in crate::db) fn execute_scalar_projection_boundary(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        request: ScalarProjectionBoundaryRequest,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        let prepared = self.prepare_scalar_projection_boundary(
            plan.into_prepared_aggregate_plan(),
            target_field,
            request,
        )?;

        self.execute_prepared_scalar_projection_boundary(prepared)
    }

    // Lower one public scalar field-projection request into one prepared
    // projection contract that no longer retains `ExecutablePlan<E>`.
    fn prepare_scalar_projection_boundary(
        &self,
        plan: PreparedAggregatePlan,
        target_field: PlannedFieldSlot,
        request: ScalarProjectionBoundaryRequest,
    ) -> Result<PreparedScalarProjectionExecutionState<'_>, InternalError> {
        let target_field_name = target_field.field().to_string();
        let field_slot = resolve_any_aggregate_target_slot_from_planner_slot(&target_field)
            .map_err(AggregateFieldValueError::into_internal_error)?;
        let prepared = self.prepare_scalar_aggregate_boundary(plan)?;

        let op = PreparedScalarProjectionOp::from_request(request);
        op.validate_terminal_value_kind()?;

        let strategy = Self::prepare_scalar_projection_strategy(&prepared, &target_field_name, op);

        Ok(PreparedScalarProjectionExecutionState {
            boundary: crate::db::executor::aggregate::PreparedScalarProjectionBoundary {
                target_field_name,
                field_slot,
                op,
            },
            strategy,
            prepared,
        })
    }

    // Execute one prepared field-projection contract without re-reading
    // access-path, covering, or distinct policy from the original plan.
    fn execute_prepared_scalar_projection_boundary(
        &self,
        prepared_state: PreparedScalarProjectionExecutionState<'_>,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        let PreparedScalarProjectionExecutionState {
            boundary,
            strategy,
            prepared,
        } = prepared_state;

        match strategy {
            PreparedScalarProjectionStrategy::Materialized => {
                let row_layout = prepared.authority.row_layout();

                self.execute_materialized_scalar_projection_boundary(
                    boundary,
                    prepared,
                    &row_layout,
                )
            }
            PreparedScalarProjectionStrategy::StreamingCountNonNull { direction } => {
                Self::execute_streaming_count_non_null_scalar_projection_boundary(
                    boundary, prepared, direction,
                )
            }
            PreparedScalarProjectionStrategy::CoveringIndex {
                context,
                window,
                distinct,
            } => self.execute_covering_scalar_projection_boundary(
                boundary, prepared, context, window, distinct,
            ),
            PreparedScalarProjectionStrategy::CoveringConstant { value } => {
                self.execute_constant_scalar_projection_boundary(boundary, prepared, value)
            }
        }
    }

    // Resolve one non-generic execution strategy for the prepared projection
    // contract before runtime execution begins.
    fn prepare_scalar_projection_strategy(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        op: PreparedScalarProjectionOp,
    ) -> PreparedScalarProjectionStrategy {
        if !prepared.has_predicate()
            && let Some(context) = covering_index_projection_context(
                &prepared.logical_plan.access,
                prepared.order_spec(),
                target_field,
                prepared.authority.primary_key_name(),
            )
        {
            let window = ScalarProjectionWindow {
                offset: scalar_window_for_covering_projection(prepared.page_spec()).0,
                limit: scalar_window_for_covering_projection(prepared.page_spec()).1,
            };
            let distinct = match op {
                PreparedScalarProjectionOp::DistinctValues
                | PreparedScalarProjectionOp::CountDistinct => {
                    Some(if covering_index_adjacent_distinct_eligible(context) {
                        PreparedCoveringDistinctStrategy::Adjacent
                    } else {
                        PreparedCoveringDistinctStrategy::PreserveFirst
                    })
                }
                _ => None,
            };

            return PreparedScalarProjectionStrategy::CoveringIndex {
                context,
                window,
                distinct,
            };
        }

        match op {
            PreparedScalarProjectionOp::Values
            | PreparedScalarProjectionOp::DistinctValues
            | PreparedScalarProjectionOp::CountDistinct
            | PreparedScalarProjectionOp::TerminalValue { .. } => {
                if let Some(value) =
                    Self::constant_covering_projection_value_if_eligible(prepared, target_field)
                {
                    return PreparedScalarProjectionStrategy::CoveringConstant { value };
                }
            }
            PreparedScalarProjectionOp::CountNonNull => {
                if let Some(value) =
                    Self::constant_covering_projection_value_if_eligible(prepared, target_field)
                {
                    return PreparedScalarProjectionStrategy::CoveringConstant { value };
                }
                if prepared.supports_streaming_existing_row_field_fold() {
                    return PreparedScalarProjectionStrategy::StreamingCountNonNull {
                        direction: prepared.streaming_existing_row_field_direction(),
                    };
                }
            }
            PreparedScalarProjectionOp::ValuesWithIds => {}
        }

        PreparedScalarProjectionStrategy::Materialized
    }

    // Execute one prepared covering-index projection contract. Decode failures
    // that prove the covering payload is unusable fall back to the canonical
    // materialized boundary without re-deriving strategy from the plan.
    fn execute_covering_scalar_projection_boundary(
        &self,
        boundary: crate::db::executor::aggregate::PreparedScalarProjectionBoundary,
        prepared: PreparedAggregateStreamingInputs<'_>,
        context: CoveringProjectionContext,
        window: ScalarProjectionWindow,
        distinct: Option<PreparedCoveringDistinctStrategy>,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        match boundary.op {
            PreparedScalarProjectionOp::Values => {
                if let Some(covering_projection) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    return Ok(ScalarProjectionBoundaryOutput::Values(
                        covering_projection.values,
                    ));
                }
            }
            PreparedScalarProjectionOp::DistinctValues => {
                if let Some(covering_projection) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    let values = match distinct {
                        Some(PreparedCoveringDistinctStrategy::Adjacent) => {
                            dedup_adjacent_values(covering_projection.values)
                        }
                        Some(PreparedCoveringDistinctStrategy::PreserveFirst) => {
                            dedup_values_preserving_first(covering_projection.values)?
                        }
                        None => {
                            return Err(boundary.op.covering_distinct_strategy_required());
                        }
                    };

                    return Ok(ScalarProjectionBoundaryOutput::Values(values));
                }
            }
            PreparedScalarProjectionOp::CountNonNull => {
                if let Some(covering_projection) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    let mut count = 0usize;
                    for value in covering_projection.values {
                        if !matches!(value, Value::Null) {
                            count = count.saturating_add(1);
                        }
                    }

                    return Ok(ScalarProjectionBoundaryOutput::Count(
                        u32::try_from(count).unwrap_or(u32::MAX),
                    ));
                }
            }
            PreparedScalarProjectionOp::CountDistinct => {
                if let Some(covering_projection) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    let values = match distinct {
                        Some(PreparedCoveringDistinctStrategy::Adjacent) => {
                            dedup_adjacent_values(covering_projection.values)
                        }
                        Some(PreparedCoveringDistinctStrategy::PreserveFirst) => {
                            dedup_values_preserving_first(covering_projection.values)?
                        }
                        None => {
                            return Err(boundary.op.covering_distinct_strategy_required());
                        }
                    };

                    return Ok(ScalarProjectionBoundaryOutput::Count(
                        u32::try_from(values.len()).unwrap_or(u32::MAX),
                    ));
                }
            }
            PreparedScalarProjectionOp::ValuesWithIds => {
                if let Some(values) =
                    Self::covering_index_projection_values_from_context_structural(
                        &prepared, context, window,
                    )?
                {
                    return Ok(ScalarProjectionBoundaryOutput::ValuesWithDataKeys(values));
                }
            }
            PreparedScalarProjectionOp::TerminalValue { terminal_kind } => {
                if let Some(covering_projection) =
                    Self::covering_index_projection_values_with_context_from_prepared(
                        &prepared, context, window,
                    )?
                {
                    PreparedScalarProjectionOp::TerminalValue { terminal_kind }
                        .validate_terminal_value_kind()?;
                    let value = match terminal_kind {
                        AggregateKind::First => covering_projection.values.first().cloned(),
                        AggregateKind::Last => covering_projection.values.last().cloned(),
                        _ => unreachable!(),
                    };

                    return Ok(ScalarProjectionBoundaryOutput::TerminalValue(value));
                }
            }
        }

        let row_layout = prepared.authority.row_layout();

        self.execute_materialized_scalar_projection_boundary(boundary, prepared, &row_layout)
    }

    // Execute one prepared constant projection contract without revisiting
    // covering eligibility checks.
    fn execute_constant_scalar_projection_boundary(
        &self,
        boundary: crate::db::executor::aggregate::PreparedScalarProjectionBoundary,
        prepared: PreparedAggregateStreamingInputs<'_>,
        value: Value,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        match boundary.op {
            PreparedScalarProjectionOp::Values => {
                let row_count = ExecutionKernel::execute_prepared_aggregate_state(
                    self,
                    ExecutionKernel::prepare_aggregate_execution_state_from_prepared(
                        prepared,
                        PreparedAggregateSpec::terminal(AggregateKind::Count),
                    ),
                )?
                .into_count("projection COUNT helper result kind mismatch")?;
                let output_len = usize::try_from(row_count).unwrap_or(usize::MAX);

                Ok(ScalarProjectionBoundaryOutput::Values(vec![
                    value;
                    output_len
                ]))
            }
            PreparedScalarProjectionOp::DistinctValues => {
                let has_rows = ExecutionKernel::execute_prepared_aggregate_state(
                    self,
                    ExecutionKernel::prepare_aggregate_execution_state_from_prepared(
                        prepared,
                        PreparedAggregateSpec::terminal(AggregateKind::Exists),
                    ),
                )?
                .into_exists("projection EXISTS helper result kind mismatch")?;
                Ok(ScalarProjectionBoundaryOutput::Values(if has_rows {
                    vec![value]
                } else {
                    Vec::new()
                }))
            }
            PreparedScalarProjectionOp::CountNonNull => Ok(ScalarProjectionBoundaryOutput::Count(
                if matches!(value, Value::Null) {
                    0
                } else {
                    ExecutionKernel::execute_prepared_aggregate_state(
                        self,
                        ExecutionKernel::prepare_aggregate_execution_state_from_prepared(
                            prepared,
                            PreparedAggregateSpec::terminal(AggregateKind::Count),
                        ),
                    )?
                    .into_count("projection COUNT helper result kind mismatch")?
                },
            )),
            PreparedScalarProjectionOp::CountDistinct => {
                let has_rows = ExecutionKernel::execute_prepared_aggregate_state(
                    self,
                    ExecutionKernel::prepare_aggregate_execution_state_from_prepared(
                        prepared,
                        PreparedAggregateSpec::terminal(AggregateKind::Exists),
                    ),
                )?
                .into_exists("projection EXISTS helper result kind mismatch")?;
                Ok(ScalarProjectionBoundaryOutput::Count(u32::from(has_rows)))
            }
            PreparedScalarProjectionOp::TerminalValue { .. } => {
                let has_rows = ExecutionKernel::execute_prepared_aggregate_state(
                    self,
                    ExecutionKernel::prepare_aggregate_execution_state_from_prepared(
                        prepared,
                        PreparedAggregateSpec::terminal(AggregateKind::Exists),
                    ),
                )?
                .into_exists("projection EXISTS helper result kind mismatch")?;
                Ok(ScalarProjectionBoundaryOutput::TerminalValue(
                    has_rows.then_some(value),
                ))
            }
            PreparedScalarProjectionOp::ValuesWithIds => {
                Err(boundary.op.constant_covering_strategy_unsupported())
            }
        }
    }

    // Execute one prepared materialized projection contract.
    fn execute_materialized_scalar_projection_boundary(
        &self,
        boundary: crate::db::executor::aggregate::PreparedScalarProjectionBoundary,
        prepared: PreparedAggregateStreamingInputs<'_>,
        row_layout: &RowLayout,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        if let PreparedScalarProjectionOp::TerminalValue { terminal_kind } = boundary.op {
            return self
                .execute_selected_value_field_projection_with_slot(
                    prepared,
                    &boundary.target_field_name,
                    boundary.field_slot,
                    terminal_kind,
                )
                .map(ScalarProjectionBoundaryOutput::TerminalValue);
        }

        let page = self.execute_scalar_materialized_page_stage(prepared)?;
        let (rows, _) = page.into_parts();

        match boundary.op {
            PreparedScalarProjectionOp::Values => {
                let projected_values = Self::project_field_values_from_materialized_structural(
                    rows,
                    row_layout,
                    &boundary.target_field_name,
                    boundary.field_slot,
                )?;

                Ok(ScalarProjectionBoundaryOutput::Values(
                    projected_values
                        .into_iter()
                        .map(|(_, value)| value)
                        .collect(),
                ))
            }
            PreparedScalarProjectionOp::DistinctValues => {
                Self::project_field_values_from_materialized_structural(
                    rows,
                    row_layout,
                    &boundary.target_field_name,
                    boundary.field_slot,
                )
                .and_then(project_distinct_field_values_from_structural_projection)
                .map(ScalarProjectionBoundaryOutput::Values)
            }
            PreparedScalarProjectionOp::CountNonNull => {
                Self::count_non_null_field_values_from_materialized_structural(
                    rows,
                    row_layout,
                    &boundary.target_field_name,
                    boundary.field_slot,
                )
                .map(ScalarProjectionBoundaryOutput::Count)
            }
            PreparedScalarProjectionOp::CountDistinct => {
                Self::project_field_values_from_materialized_structural(
                    rows,
                    row_layout,
                    &boundary.target_field_name,
                    boundary.field_slot,
                )
                .and_then(project_distinct_field_values_from_structural_projection)
                .map(|values| {
                    ScalarProjectionBoundaryOutput::Count(
                        u32::try_from(values.len()).unwrap_or(u32::MAX),
                    )
                })
            }
            PreparedScalarProjectionOp::ValuesWithIds => {
                Self::project_field_values_from_materialized_structural(
                    rows,
                    row_layout,
                    &boundary.target_field_name,
                    boundary.field_slot,
                )
                .map(ScalarProjectionBoundaryOutput::ValuesWithDataKeys)
            }
            PreparedScalarProjectionOp::TerminalValue { .. } => {
                Err(boundary.op.materialized_branch_unreachable())
            }
        }
    }

    // Execute COUNT(field) directly from one ordered existing-row stream when
    // the prepared aggregate shape preserves the canonical streaming contract.
    fn execute_streaming_count_non_null_scalar_projection_boundary(
        boundary: crate::db::executor::aggregate::PreparedScalarProjectionBoundary,
        prepared: PreparedAggregateStreamingInputs<'_>,
        direction: Direction,
    ) -> Result<ScalarProjectionBoundaryOutput, InternalError> {
        let prepared = prepared.into_core();
        let mut count = 0u32;
        Self::for_each_existing_stream_row(prepared, direction, |row| {
            let mut read_slot = |index: usize| row.slot_ref(index);
            let value = extract_orderable_field_value_with_slot_ref_reader(
                &boundary.target_field_name,
                boundary.field_slot,
                &mut read_slot,
            )
            .map_err(AggregateFieldValueError::into_internal_error)?;
            if !matches!(value, Value::Null) {
                count = count.saturating_add(1);
            }
            Ok(())
        })?;

        Ok(ScalarProjectionBoundaryOutput::Count(count))
    }

    // Execute one field-target selected-value projection (`first_value_by` /
    // `last_value_by` / SQL `MIN/MAX(field)`) using a planner-validated slot
    // and route-owned selected-row semantics.
    fn execute_selected_value_field_projection_with_slot(
        &self,
        prepared: PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
        field_slot: FieldSlot,
        terminal_kind: AggregateKind,
    ) -> Result<Option<Value>, InternalError> {
        let consistency = prepared.consistency();
        let store = prepared.store;
        let entity_tag = prepared.authority.entity_tag();
        let row_layout = prepared.authority.row_layout();
        let aggregate = if terminal_kind.is_extrema() {
            PreparedAggregateSpec::field_target(
                terminal_kind,
                PreparedAggregateTargetField::new(
                    target_field.to_string(),
                    field_slot,
                    true,
                    true,
                    target_field == prepared.authority.primary_key_name(),
                ),
            )
        } else {
            PreparedAggregateSpec::terminal(terminal_kind)
        };
        let state =
            ExecutionKernel::prepare_aggregate_execution_state_from_prepared(prepared, aggregate);
        let selected_key = ExecutionKernel::execute_prepared_aggregate_state(self, state)?
            .into_optional_id_terminal(
                terminal_kind,
                "terminal value projection result kind mismatch",
            )?;
        let Some(selected_key) = selected_key else {
            return Ok(None);
        };

        let key = DataKey::new(entity_tag, selected_key);
        let Some(value) = Self::read_field_value_for_aggregate(
            store,
            &row_layout,
            consistency,
            &key,
            target_field,
            field_slot,
        )?
        else {
            return Ok(None);
        };

        Ok(Some(value))
    }

    // Count non-null field values directly from materialized rows without
    // retaining the intermediate projection vector.
    fn count_non_null_field_values_from_materialized_structural(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<u32, InternalError> {
        let mut count = 0_u32;

        for (data_key, raw_row) in rows {
            let value = RowDecoder::decode_required_slot_value(
                row_layout,
                data_key.storage_key(),
                &raw_row,
                field_slot.index,
            )?;
            let value =
                extract_orderable_field_value_from_decoded_slot(target_field, field_slot, value)
                    .map_err(AggregateFieldValueError::into_internal_error)?;

            if !matches!(value, Value::Null) {
                count = count.saturating_add(1);
            }
        }

        Ok(count)
    }

    // Project materialized structural rows into structural `(data_key, value)`
    // pairs while preserving the effective response row order.
    fn project_field_values_from_materialized_structural(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<ValueProjection, InternalError> {
        rows.into_iter()
            .map(|(data_key, raw_row)| {
                let value = RowDecoder::decode_required_slot_value(
                    row_layout,
                    data_key.storage_key(),
                    &raw_row,
                    field_slot.index,
                )?;

                extract_orderable_field_value_from_decoded_slot(target_field, field_slot, value)
                    .map(|value| (data_key, value))
                    .map_err(AggregateFieldValueError::into_internal_error)
            })
            .collect()
    }

    // Resolve one constant field projection value when access shape guarantees
    // that target-field value is fixed by index-prefix equality bindings.
    //
    // Guard rails:
    // - only enabled for `MissingRowPolicy::Ignore` to preserve strict
    //   missing-row corruption surfacing behavior.
    // - only applies when the target field is bound by index-prefix equality.
    fn constant_covering_projection_value_if_eligible(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        target_field: &str,
    ) -> Option<Value> {
        if !matches!(prepared.consistency(), MissingRowPolicy::Ignore) {
            return None;
        }

        constant_covering_projection_value_from_access(&prepared.logical_plan.access, target_field)
    }

    // Resolve one index-covered projection value vector from already-prepared
    // covering strategy metadata.
    fn covering_index_projection_values_with_context_from_prepared(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        context: CoveringProjectionContext,
        window: ScalarProjectionWindow,
    ) -> Result<Option<CoveringProjectionValues>, InternalError> {
        let Some(projected_pairs) =
            Self::covering_index_projection_pairs_from_context(prepared, context, window)?
        else {
            return Ok(None);
        };

        let values = projected_pairs
            .into_iter()
            .map(|(_, value)| value)
            .collect();

        Ok(Some(CoveringProjectionValues { values }))
    }

    // Resolve one index-covered structural `(data_key, value)` projection
    // vector from already prepared covering strategy metadata.
    fn covering_index_projection_values_from_context_structural(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        context: CoveringProjectionContext,
        window: ScalarProjectionWindow,
    ) -> Result<Option<ValueProjection>, InternalError> {
        Self::covering_index_projection_pairs_from_context(prepared, context, window)
    }

    // Resolve one covering projection pair vector from already prepared
    // covering-index strategy metadata.
    fn covering_index_projection_pairs_from_context(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        context: CoveringProjectionContext,
        window: ScalarProjectionWindow,
    ) -> CoveringProjectionPairsResolution {
        // Phase 1: read component pairs in the order implied by the covering contract.
        let scan_direction = covering_projection_scan_direction(context.order_contract);
        let raw_pairs = Self::read_covering_projection_component_pairs(
            prepared,
            context.component_index,
            scan_direction,
        )?;

        // Phase 2: enforce missing-row policy and decode projection components.
        let Some(mut projected_pairs) = decode_single_covering_projection_pairs(
            raw_pairs,
            prepared.store,
            prepared.consistency(),
            covering_requires_row_presence_check(),
            "aggregate covering projection expected one decoded component",
            Ok,
        )?
        else {
            return Ok(None);
        };

        // Phase 3: realign to post-access order and apply prepared effective window.
        reorder_covering_projection_pairs(context.order_contract, projected_pairs.as_mut_slice());
        apply_scalar_projection_window_in_place(&mut projected_pairs, window);

        Ok(Some(projected_pairs))
    }

    // Read one index-backed `(data_key, encoded_component)` stream for covering
    // projection decoding.
    fn read_covering_projection_component_pairs(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        component_index: usize,
        direction: crate::db::direction::Direction,
    ) -> Result<CoveringProjectionComponentRows, InternalError> {
        resolve_covering_projection_components_from_lowered_specs(
            prepared.authority.entity_tag(),
            prepared.index_prefix_specs.as_slice(),
            prepared.index_range_specs.as_slice(),
            direction,
            usize::MAX,
            &[component_index],
            |index| prepared.store_resolver.try_get_store(index.store()),
        )
    }
}

fn project_distinct_field_values_from_structural_projection(
    projected_values: ValueProjection,
) -> Result<Vec<Value>, InternalError> {
    let mut distinct_values = GroupKeySet::default();
    let mut distinct_projected_values = Vec::with_capacity(projected_values.len());

    // Phase 1: preserve first-observed order while deduplicating on canonical
    // group-key equality over structural projection values.
    for (_, value) in projected_values {
        if !insert_materialized_distinct_value(&mut distinct_values, &value)? {
            continue;
        }
        distinct_projected_values.push(value);
    }

    Ok(distinct_projected_values)
}

// Apply one prepared scalar projection page window in place so covering
// projection paths do not allocate a second pair vector after reordering.
fn apply_scalar_projection_window_in_place<T>(
    projected_pairs: &mut Vec<(DataKey, T)>,
    window: ScalarProjectionWindow,
) {
    let keep_start = window.offset.min(projected_pairs.len());
    if keep_start > 0 {
        projected_pairs.drain(..keep_start);
    }

    if let Some(limit) = window.limit
        && projected_pairs.len() > limit
    {
        projected_pairs.truncate(limit);
    }
}
