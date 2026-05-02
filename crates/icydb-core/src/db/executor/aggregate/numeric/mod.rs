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
        access::LoweredAccess,
        data::{DataRow, RawRow},
        direction::Direction,
        executor::{
            PreparedAggregatePlan, PreparedExecutionPlan,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot,
                extract_numeric_field_decimal_from_decoded_slot,
                extract_numeric_field_decimal_with_slot_ref_reader,
                resolve_numeric_aggregate_target_slot_from_planner_slot,
            },
            aggregate::{
                AggregateKind, PreparedAggregateStreamingInputs,
                PreparedScalarNumericAggregateStrategy, PreparedScalarNumericBoundary,
                PreparedScalarNumericOp, PreparedScalarNumericPayload,
            },
            pipeline::contracts::LoadExecutor,
            route::{
                paged_primary_key_numeric_fold_shape_supported,
                streaming_numeric_fold_shape_supported,
            },
            terminal::{RowDecoder, RowLayout},
        },
        numeric::{NumericEvalError, add_decimal_terms_checked, average_decimal_terms_checked},
        query::{
            builder::aggregate::ScalarNumericFieldBoundaryRequest,
            plan::{ExecutionOrderContract, FieldSlot as PlannedFieldSlot},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Decimal,
    value::{StorageKey, Value},
};

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
        plan: PreparedExecutionPlan<E>,
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
    ) -> Result<PreparedScalarNumericBoundary<'_>, InternalError> {
        // Phase 1: resolve the plan-free numeric field boundary exactly once.
        let field_slot = resolve_numeric_aggregate_target_slot_from_planner_slot(&target_field)
            .map_err(AggregateFieldValueError::into_internal_error)?;
        let target_field_name = target_field.field().to_string();
        let op = request.prepared_op();

        // Phase 2: derive the execution payload family from the prepared boundary.
        let payload = self.prepare_scalar_numeric_payload(
            plan,
            op.aggregate_kind(),
            target_field_name.as_str(),
            request,
        )?;

        Ok(PreparedScalarNumericBoundary {
            target_field_name,
            field_slot,
            op,
            payload,
        })
    }

    // Execute one prepared numeric aggregate contract without re-deriving
    // strategy from the original plan.
    fn execute_prepared_scalar_numeric_boundary(
        &self,
        prepared_boundary: PreparedScalarNumericBoundary<'_>,
    ) -> Result<Option<Decimal>, InternalError> {
        match prepared_boundary.payload {
            PreparedScalarNumericPayload::Aggregate {
                strategy,
                window_provably_empty,
                prepared,
            } => {
                let prepared = *prepared;
                if window_provably_empty {
                    return Ok(None);
                }

                match strategy {
                    PreparedScalarNumericAggregateStrategy::Streaming => {
                        Self::aggregate_numeric_field_from_streaming(
                            prepared,
                            &prepared_boundary.target_field_name,
                            prepared_boundary.field_slot,
                            prepared_boundary.op,
                        )
                    }
                    PreparedScalarNumericAggregateStrategy::Materialized => {
                        let (rows, row_layout) = self.load_materialized_aggregate_rows(prepared)?;

                        Self::aggregate_numeric_field_from_materialized(
                            rows,
                            &row_layout,
                            &prepared_boundary.target_field_name,
                            prepared_boundary.field_slot,
                            prepared_boundary.op,
                        )
                    }
                }
            }
            PreparedScalarNumericPayload::GlobalDistinct { route } => {
                let value = self.execute_prepared_global_distinct_grouped_aggregate(*route)?;

                decode_global_distinct_numeric_output(value, prepared_boundary.op)
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
            accumulator.add(Self::decode_numeric_materialized_row_decimal(
                row_layout,
                data_key.storage_key(),
                &raw_row,
                target_field,
                field_slot,
            )?)?;
        }

        finalize_numeric_field_output(accumulator, kind)
    }

    // Return whether numeric field aggregates can use one direct key-stream fold.
    fn streaming_numeric_field_aggregate_eligible(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        lowered_access: &LoweredAccess<'_, Value>,
    ) -> bool {
        if prepared.has_predicate() || prepared.logical_plan.scalar_plan().distinct {
            return false;
        }

        let Some(path) = lowered_access.executable().as_path() else {
            return false;
        };
        let capabilities = path.capabilities();
        if !streaming_numeric_fold_shape_supported(capabilities) {
            return false;
        }

        Self::aggregate_page_window_safe(
            prepared,
            paged_primary_key_numeric_fold_shape_supported(capabilities),
        )
    }

    // Return whether one paged ORDER BY window preserves one direct numeric
    // stream-fold contract under primary-key order constraints.
    fn aggregate_page_window_safe(
        prepared: &PreparedAggregateStreamingInputs<'_>,
        paged_primary_key_window_safe: bool,
    ) -> bool {
        if prepared.page_spec().is_none() {
            return true;
        }
        let Some(_order) = prepared.order_spec() else {
            // Planner rejects unordered pagination, but fail closed if bypassed.
            return false;
        };
        if prepared
            .order_spec()
            .and_then(|order| {
                order.primary_key_only_direction(prepared.authority.primary_key_name())
            })
            .is_none()
        {
            return false;
        }

        paged_primary_key_window_safe
    }

    // Fold numeric field aggregates directly from one ordered key stream without
    // materializing the full response window.
    fn aggregate_numeric_field_from_streaming(
        prepared: PreparedAggregateStreamingInputs<'_>,
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
            accumulator.add(value)?;
            Ok(())
        })?;

        // Phase 4: finish SUM/AVG output with shared numeric arithmetic semantics.
        finalize_numeric_field_output(accumulator, kind)
    }

    fn aggregate_numeric_stream_direction(
        prepared: &PreparedAggregateStreamingInputs<'_>,
    ) -> Direction {
        ExecutionOrderContract::from_plan(false, prepared.logical_plan.scalar_plan().order.as_ref())
            .primary_scan_direction()
    }

    // Lower the already-resolved numeric boundary into the concrete execution
    // payload family without rebuilding request semantics in each branch.
    fn prepare_scalar_numeric_payload(
        &self,
        plan: PreparedAggregatePlan,
        aggregate_kind: AggregateKind,
        target_field_name: &str,
        request: ScalarNumericFieldBoundaryRequest,
    ) -> Result<PreparedScalarNumericPayload<'_>, InternalError> {
        if request.requires_global_distinct() {
            let route = self.prepare_global_distinct_grouped_route(
                plan,
                aggregate_kind,
                target_field_name,
            )?;

            return Ok(PreparedScalarNumericPayload::GlobalDistinct {
                route: Box::new(route),
            });
        }

        let prepared = self.prepare_scalar_aggregate_boundary(plan)?;
        let (strategy, window_provably_empty) = {
            let lowered_access = prepared.lowered_access()?;
            let strategy =
                if Self::streaming_numeric_field_aggregate_eligible(&prepared, &lowered_access) {
                    PreparedScalarNumericAggregateStrategy::Streaming
                } else {
                    PreparedScalarNumericAggregateStrategy::Materialized
                };
            (strategy, prepared.window_is_provably_empty(&lowered_access))
        };

        Ok(PreparedScalarNumericPayload::Aggregate {
            strategy,
            window_provably_empty,
            prepared: Box::new(prepared),
        })
    }

    // Decode one materialized row slot into a Decimal using the same numeric
    // field-target contract that the streaming fold path applies per row.
    fn decode_numeric_materialized_row_decimal(
        row_layout: &RowLayout,
        storage_key: StorageKey,
        raw_row: &RawRow,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Decimal, InternalError> {
        let value = RowDecoder::decode_required_slot_value(
            row_layout,
            storage_key,
            raw_row,
            field_slot.index,
        )?;
        extract_numeric_field_decimal_from_decoded_slot(target_field, field_slot, value)
            .map_err(AggregateFieldValueError::into_internal_error)
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

    fn add(&mut self, value: Decimal) -> Result<(), InternalError> {
        self.sum = add_numeric_decimal(self.sum, value)?;
        self.row_count = self.row_count.saturating_add(1);

        Ok(())
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
            average_decimal_terms_checked(accumulator.sum, accumulator.row_count)
                .map_err(NumericEvalError::into_internal_error)?
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
fn add_numeric_decimal(sum: Decimal, value: Decimal) -> Result<Decimal, InternalError> {
    add_decimal_terms_checked(sum, value).map_err(NumericEvalError::into_internal_error)
}
