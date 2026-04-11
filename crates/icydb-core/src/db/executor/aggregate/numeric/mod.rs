//! Module: executor::aggregate::numeric
//! Responsibility: numeric field-target aggregate terminals (`sum`/`avg`).
//! Does not own: numeric coercion policy beyond field helper contracts.
//! Boundary: materialized numeric aggregate helpers for load executor terminals.

///
/// TESTS
///

#[cfg(test)]
mod tests;

use crate::{
    db::{
        access::AccessPathKind,
        data::DataRow,
        direction::Direction,
        executor::{
            ExecutablePlan, PreparedAggregatePlan,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot,
                extract_numeric_field_decimal_from_decoded_slot,
                extract_numeric_field_decimal_with_slot_ref_reader,
                resolve_numeric_aggregate_target_slot_from_planner_slot,
            },
            aggregate::{
                PreparedAggregateStreamingInputs, PreparedAggregateStreamingInputsCore,
                PreparedScalarNumericAggregateStrategy, PreparedScalarNumericBoundary,
                PreparedScalarNumericExecutionState, PreparedScalarNumericOp,
                PreparedScalarNumericPayload,
            },
            pipeline::contracts::LoadExecutor,
            terminal::{RowDecoder, RowLayout},
        },
        numeric::{add_decimal_terms, average_decimal_terms},
        query::plan::{ExecutionOrderContract, FieldSlot as PlannedFieldSlot},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Decimal,
    value::Value,
};

// Typed boundary request for one numeric field aggregate terminal family call.
#[derive(Clone, Copy)]
pub(in crate::db) enum ScalarNumericFieldBoundaryRequest {
    Sum,
    SumDistinct,
    Avg,
    AvgDistinct,
}

impl ScalarNumericFieldBoundaryRequest {
    // Resolve one public numeric request into the non-generic numeric operation.
    const fn prepared_op(self) -> PreparedScalarNumericOp {
        match self {
            Self::Sum | Self::SumDistinct => PreparedScalarNumericOp::Sum,
            Self::Avg | Self::AvgDistinct => PreparedScalarNumericOp::Avg,
        }
    }

    // Return whether this request must route through grouped global DISTINCT execution.
    const fn requires_global_distinct(self) -> bool {
        matches!(self, Self::SumDistinct | Self::AvgDistinct)
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one numeric field aggregate family request from the typed API
    // boundary, lower plan-derived policy into one prepared contract, and then
    // execute the prepared boundary.
    pub(in crate::db) fn execute_numeric_field_boundary(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        request: ScalarNumericFieldBoundaryRequest,
    ) -> Result<Option<Decimal>, InternalError> {
        let prepared = self.prepare_scalar_numeric_boundary(
            plan.into_prepared_aggregate_plan(),
            target_field,
            request,
        )?;

        self.execute_prepared_scalar_numeric_boundary(prepared)
    }

    // Lower one public numeric field aggregate request into a prepared
    // non-generic contract plus the runtime payload needed to execute it.
    fn prepare_scalar_numeric_boundary(
        &self,
        plan: PreparedAggregatePlan,
        target_field: PlannedFieldSlot,
        request: ScalarNumericFieldBoundaryRequest,
    ) -> Result<PreparedScalarNumericExecutionState<'_>, InternalError> {
        // Phase 1: resolve the plan-free numeric boundary exactly once.
        let boundary =
            Self::resolve_prepared_scalar_numeric_boundary(&plan, &target_field, request)?;

        // Phase 2: derive the execution payload family from the prepared boundary.
        let payload = self.prepare_scalar_numeric_payload(plan, &boundary, request)?;

        Ok(PreparedScalarNumericExecutionState { boundary, payload })
    }

    // Execute one prepared numeric aggregate contract without re-deriving
    // strategy from the original plan.
    fn execute_prepared_scalar_numeric_boundary(
        &self,
        prepared_state: PreparedScalarNumericExecutionState<'_>,
    ) -> Result<Option<Decimal>, InternalError> {
        let PreparedScalarNumericExecutionState { boundary, payload } = prepared_state;

        match payload {
            PreparedScalarNumericPayload::Aggregate { strategy, prepared } => {
                let prepared = *prepared;
                if prepared.window_is_provably_empty() {
                    return Ok(None);
                }

                match strategy {
                    PreparedScalarNumericAggregateStrategy::Streaming => {
                        Self::aggregate_numeric_field_from_streaming(
                            prepared.into_core(),
                            &boundary.target_field_name,
                            boundary.field_slot,
                            boundary.op,
                        )
                    }
                    PreparedScalarNumericAggregateStrategy::Materialized => {
                        let row_layout = prepared.authority.row_layout();
                        let page = self.execute_scalar_materialized_page_stage(prepared)?;
                        let (rows, _) = page.into_parts();

                        Self::aggregate_numeric_field_from_materialized(
                            rows,
                            &row_layout,
                            &boundary.target_field_name,
                            boundary.field_slot,
                            boundary.op,
                        )
                    }
                }
            }
            PreparedScalarNumericPayload::GlobalDistinct { route } => {
                let value = self.execute_prepared_global_distinct_grouped_aggregate(*route)?;

                decode_global_distinct_numeric_output(value, boundary.op)
            }
        }
    }

    // Reduce one materialized response into `sum(field)` / `avg(field)` over
    // numeric field values coerced to Decimal.
    fn aggregate_numeric_field_from_materialized(
        rows: Vec<DataRow>,
        row_layout: &RowLayout,
        target_field: &str,
        field_slot: FieldSlot,
        kind: PreparedScalarNumericOp,
    ) -> Result<Option<Decimal>, InternalError> {
        let mut accumulator = NumericAggregateAccumulator::new();
        for (data_key, raw_row) in rows {
            let value = RowDecoder::decode_required_slot_value(
                row_layout,
                data_key.storage_key(),
                &raw_row,
                field_slot.index,
            )?;
            let value =
                extract_numeric_field_decimal_from_decoded_slot(target_field, field_slot, value)
                    .map_err(AggregateFieldValueError::into_internal_error)?;
            accumulator.add(value);
        }

        finalize_numeric_field_output(accumulator, kind)
    }

    // Return whether numeric field aggregates can use one direct key-stream fold.
    fn streaming_numeric_field_aggregate_eligible(
        prepared: &PreparedAggregateStreamingInputs<'_>,
    ) -> bool {
        if !Self::aggregate_predicate_safe(prepared) {
            return false;
        }

        let access_strategy = prepared.logical_plan.access.resolve_strategy();
        let Some(path) = access_strategy.as_path() else {
            return false;
        };
        let path_kind = path.capabilities().kind();
        if !Self::aggregate_access_path_safe(path_kind) {
            return false;
        }

        Self::aggregate_page_window_safe(prepared, path_kind)
    }

    // Return whether predicate and distinct planner flags preserve one
    // canonical direct stream-fold contract.
    const fn aggregate_predicate_safe(prepared: &PreparedAggregateStreamingInputs<'_>) -> bool {
        prepared.has_no_predicate_or_distinct()
    }

    // Return whether the resolved access path kind can support one direct
    // numeric stream fold without fan-out duplication risks.
    const fn aggregate_access_path_safe(path_kind: AccessPathKind) -> bool {
        path_kind.supports_streaming_numeric_fold()
    }

    // Return whether one paged ORDER BY window preserves one direct numeric
    // stream-fold contract under primary-key order constraints.
    fn aggregate_page_window_safe(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        path_kind: AccessPathKind,
    ) -> bool {
        if prepared.page_spec().is_none() {
            return true;
        }
        let Some(_order) = prepared.order_spec() else {
            // Planner rejects unordered pagination, but fail closed if bypassed.
            return false;
        };
        if prepared
            .explicit_primary_key_order_direction(prepared.authority.primary_key_name())
            .is_none()
        {
            return false;
        }

        path_kind.supports_streaming_numeric_fold_for_paged_primary_key_window()
    }

    // Fold numeric field aggregates directly from one ordered key stream without
    // materializing the full response window.
    fn aggregate_numeric_field_from_streaming(
        prepared: PreparedAggregateStreamingInputsCore,
        target_field: &str,
        field_slot: FieldSlot,
        kind: PreparedScalarNumericOp,
    ) -> Result<Option<Decimal>, InternalError> {
        let direction = Self::aggregate_numeric_stream_direction(&prepared);
        let mut accumulator = NumericAggregateAccumulator::new();
        Self::for_each_existing_stream_row(prepared, direction, |row| {
            let mut read_slot = |index: usize| row.slot_ref(index);
            let value = extract_numeric_field_decimal_with_slot_ref_reader(
                target_field,
                field_slot,
                &mut read_slot,
            )
            .map_err(AggregateFieldValueError::into_internal_error)?;
            accumulator.add(value);
            Ok(())
        })?;

        // Phase 4: finish SUM/AVG output with shared numeric arithmetic semantics.
        finalize_numeric_field_output(accumulator, kind)
    }

    fn aggregate_numeric_stream_direction(
        prepared: &PreparedAggregateStreamingInputsCore,
    ) -> Direction {
        ExecutionOrderContract::from_plan(false, prepared.order_spec()).primary_scan_direction()
    }

    // Resolve the plan-free numeric boundary once from the typed request and
    // planner field slot so both aggregate and global-DISTINCT payloads share
    // the same field/op contract.
    fn resolve_prepared_scalar_numeric_boundary(
        _plan: &PreparedAggregatePlan,
        target_field: &PlannedFieldSlot,
        request: ScalarNumericFieldBoundaryRequest,
    ) -> Result<PreparedScalarNumericBoundary, InternalError> {
        let field_slot = resolve_numeric_aggregate_target_slot_from_planner_slot(target_field)
            .map_err(AggregateFieldValueError::into_internal_error)?;

        Ok(PreparedScalarNumericBoundary {
            target_field_name: target_field.field().to_string(),
            field_slot,
            op: request.prepared_op(),
        })
    }

    // Lower the already-resolved numeric boundary into the concrete execution
    // payload family without rebuilding field/op metadata in each branch.
    fn prepare_scalar_numeric_payload(
        &self,
        plan: PreparedAggregatePlan,
        boundary: &PreparedScalarNumericBoundary,
        request: ScalarNumericFieldBoundaryRequest,
    ) -> Result<PreparedScalarNumericPayload<'_>, InternalError> {
        if request.requires_global_distinct() {
            let route = self.prepare_global_distinct_grouped_route(
                plan,
                boundary.op.aggregate_kind(),
                &boundary.target_field_name,
            )?;

            return Ok(PreparedScalarNumericPayload::GlobalDistinct {
                route: Box::new(route),
            });
        }

        let prepared = self.prepare_scalar_aggregate_boundary(plan)?;
        let strategy = if Self::streaming_numeric_field_aggregate_eligible(&prepared) {
            PreparedScalarNumericAggregateStrategy::Streaming
        } else {
            PreparedScalarNumericAggregateStrategy::Materialized
        };

        Ok(PreparedScalarNumericPayload::Aggregate {
            strategy,
            prepared: Box::new(prepared),
        })
    }
}

///
/// NumericAggregateAccumulator
///
/// Shared `(sum, rows)` accumulator for numeric field aggregate terminals.
///

#[derive(Clone, Copy)]
struct NumericAggregateAccumulator {
    sum: Decimal,
    row_count: u64,
}

impl NumericAggregateAccumulator {
    const fn new() -> Self {
        Self {
            sum: Decimal::ZERO,
            row_count: 0,
        }
    }

    fn add(&mut self, value: Decimal) {
        self.sum = add_numeric_decimal(self.sum, value);
        self.row_count = self.row_count.saturating_add(1);
    }
}

// Finalize SUM/AVG numeric field output from one shared `(sum, row_count)`
// accumulator pair so streaming and materialized paths stay behavior-identical.
fn finalize_numeric_field_output(
    accumulator: NumericAggregateAccumulator,
    kind: PreparedScalarNumericOp,
) -> Result<Option<Decimal>, InternalError> {
    if accumulator.row_count == 0 {
        return Ok(None);
    }

    let output = match kind {
        PreparedScalarNumericOp::Sum => accumulator.sum,
        PreparedScalarNumericOp::Avg => {
            let Some(avg) = average_decimal_terms(accumulator.sum, accumulator.row_count) else {
                return Err(kind.avg_divisor_conversion_invariant());
            };

            avg
        }
    };

    Ok(Some(output))
}

// Decode one global grouped DISTINCT numeric terminal result from the structural
// grouped aggregate output surface without duplicating `Value` matching at each
// terminal callsite.
fn decode_global_distinct_numeric_output(
    value: Option<Value>,
    op: PreparedScalarNumericOp,
) -> Result<Option<Decimal>, InternalError> {
    match value {
        Some(Value::Decimal(value)) => Ok(Some(value)),
        Some(Value::Null) | None => Ok(None),
        Some(value) => Err(op.grouped_distinct_output_type_mismatch(&value)),
    }
}

// Add one decimal term to one aggregate numeric accumulator through the shared
// numeric arithmetic contract so projection/aggregate arithmetic semantics stay aligned.
fn add_numeric_decimal(sum: Decimal, value: Decimal) -> Decimal {
    add_decimal_terms(sum, value)
}
