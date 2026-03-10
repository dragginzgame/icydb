//! Module: executor::aggregate::numeric
//! Responsibility: numeric field-target aggregate terminals (`sum`/`avg`).
//! Does not own: numeric coercion policy beyond field helper contracts.
//! Boundary: materialized numeric aggregate helpers for load executor terminals.

use crate::{
    db::{
        access::AccessPathKind,
        direction::Direction,
        executor::{
            AccessExecutionDescriptor, AccessScanContinuationInput, AccessStreamBindings,
            ExecutablePlan, ExecutionKernel, ExecutionOptimizationCounter, ExecutionPreparation,
            aggregate::aggregate_window_is_provably_empty,
            aggregate::field::{
                FieldSlot, extract_numeric_field_decimal,
                resolve_numeric_aggregate_target_slot_from_planner_slot,
            },
            load::LoadExecutor,
            plan_metrics::record_rows_scanned,
            validate_executor_plan,
        },
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic},
        query::plan::{ExecutionOrderContract, FieldSlot as PlannedFieldSlot, OrderSpec},
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Decimal,
    value::Value,
};

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

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute `sum(field)` over the effective response window using one
    /// planner-resolved numeric field slot.
    pub(in crate::db) fn aggregate_sum_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Decimal>, InternalError> {
        let field_slot =
            resolve_numeric_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(Self::map_aggregate_field_value_error)?;
        if aggregate_window_is_provably_empty(&plan) {
            return Ok(None);
        }
        if Self::streaming_numeric_field_aggregate_eligible(&plan) {
            return self.aggregate_numeric_field_from_streaming(
                plan,
                target_field.field(),
                field_slot,
                NumericFieldAggregateKind::Sum,
            );
        }

        let response = self.execute(plan)?;

        Self::aggregate_numeric_field_from_materialized(
            response,
            target_field.field(),
            field_slot,
            NumericFieldAggregateKind::Sum,
        )
    }

    /// Execute global `sum(distinct field)` through grouped zero-key execution
    /// using one planner-resolved field slot.
    pub(in crate::db) fn aggregate_sum_distinct_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Decimal>, InternalError> {
        let value = self.execute_global_distinct_field_grouped_aggregate(
            plan,
            crate::db::query::plan::AggregateKind::Sum,
            target_field.field(),
        )?;

        match value {
            Some(crate::value::Value::Decimal(value)) => Ok(Some(value)),
            Some(crate::value::Value::Null) | None => Ok(None),
            Some(value) => Err(InternalError::query_executor_invariant(format!(
                "global SUM(DISTINCT field) grouped output type mismatch: {value:?}",
            ))),
        }
    }

    /// Execute `avg(field)` over the effective response window using one
    /// planner-resolved numeric field slot.
    pub(in crate::db) fn aggregate_avg_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Decimal>, InternalError> {
        let field_slot =
            resolve_numeric_aggregate_target_slot_from_planner_slot::<E>(&target_field)
                .map_err(Self::map_aggregate_field_value_error)?;
        if aggregate_window_is_provably_empty(&plan) {
            return Ok(None);
        }
        if Self::streaming_numeric_field_aggregate_eligible(&plan) {
            return self.aggregate_numeric_field_from_streaming(
                plan,
                target_field.field(),
                field_slot,
                NumericFieldAggregateKind::Avg,
            );
        }

        let response = self.execute(plan)?;

        Self::aggregate_numeric_field_from_materialized(
            response,
            target_field.field(),
            field_slot,
            NumericFieldAggregateKind::Avg,
        )
    }

    // Reduce one materialized response into `sum(field)` / `avg(field)` over
    // numeric field values coerced to Decimal.
    fn aggregate_numeric_field_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        kind: NumericFieldAggregateKind,
    ) -> Result<Option<Decimal>, InternalError> {
        let mut sum = Decimal::ZERO;
        let mut row_count = 0u64;
        for row in response {
            let value = extract_numeric_field_decimal(row.entity_ref(), target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            sum = add_numeric_decimal(sum, value)?;
            row_count = row_count.saturating_add(1);
        }

        finalize_numeric_field_output(sum, row_count, kind)
    }

    // Return whether numeric field aggregates can use one direct key-stream fold.
    //
    // This gate intentionally stays conservative:
    // - single-path access only (no union/intersection fan-out)
    // - no residual predicate
    // - paged windows only when ORDER BY is exactly primary key
    // - no multi-lookup index fan-out shapes with duplicate-key risk
    fn streaming_numeric_field_aggregate_eligible(plan: &ExecutablePlan<E>) -> bool {
        if plan.has_predicate() || plan.is_distinct() {
            return false;
        }

        let access_strategy = plan.access().resolve_strategy();
        let Some(path) = access_strategy.as_path() else {
            return false;
        };
        let path_kind = path.capabilities().kind();
        let path_kind_streaming_safe = matches!(
            path_kind,
            AccessPathKind::ByKey
                | AccessPathKind::ByKeys
                | AccessPathKind::FullScan
                | AccessPathKind::KeyRange
                | AccessPathKind::IndexPrefix
                | AccessPathKind::IndexRange
        );
        if !path_kind_streaming_safe {
            return false;
        }

        Self::ordered_page_window_streaming_eligible(plan, path_kind)
    }

    fn ordered_page_window_streaming_eligible(
        plan: &ExecutablePlan<E>,
        path_kind: AccessPathKind,
    ) -> bool {
        let Some(_page) = plan.page_spec() else {
            return true;
        };
        let Some(order) = plan.order_spec() else {
            // Planner rejects unordered pagination, but fail closed if bypassed.
            return false;
        };

        if !Self::order_spec_is_primary_key_only(order) {
            return false;
        }

        matches!(
            path_kind,
            AccessPathKind::ByKey
                | AccessPathKind::ByKeys
                | AccessPathKind::FullScan
                | AccessPathKind::KeyRange
        )
    }

    fn order_spec_is_primary_key_only(order: &OrderSpec) -> bool {
        matches!(order.fields.as_slice(), [(field, _direction)] if field == E::MODEL.primary_key.name)
    }

    // Fold numeric field aggregates directly from one ordered key stream without
    // materializing the full response window.
    fn aggregate_numeric_field_from_streaming(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
        field_slot: FieldSlot,
        kind: NumericFieldAggregateKind,
    ) -> Result<Option<Decimal>, InternalError> {
        Self::record_execution_optimization_hit_for_tests(
            ExecutionOptimizationCounter::NumericFieldStreamingFoldFastPath,
        );

        // Phase 1: capture lowered index specs and consume executable plan into logical form.
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let consistency = plan.consistency();
        let direction = Self::aggregate_numeric_stream_direction(&plan);
        let logical_plan = plan.into_inner();
        validate_executor_plan::<E>(&logical_plan)?;
        let execution_preparation = ExecutionPreparation::for_plan::<E>(&logical_plan);
        let mut window = ExecutionKernel::window_cursor_contract(&logical_plan, None);

        // Phase 2: resolve the canonical ordered key stream from access descriptors.
        let index_predicate_execution = execution_preparation.strict_mode().map(|program| {
            crate::db::index::predicate::IndexPredicateExecution {
                program,
                rejected_keys_counter: None,
            }
        });
        let ctx = self.recovered_context()?;
        let descriptor = AccessExecutionDescriptor::from_bindings(
            &logical_plan.access,
            AccessStreamBindings::new(
                index_prefix_specs.as_slice(),
                index_range_specs.as_slice(),
                AccessScanContinuationInput::new(None, direction),
            ),
            None,
            index_predicate_execution,
        );
        let mut key_stream = ctx.ordered_key_stream_from_access_descriptor(descriptor)?;

        // Phase 3: stream-fold numeric values directly from row reads.
        let mut rows_scanned = 0usize;
        let mut sum = Decimal::ZERO;
        let mut row_count = 0u64;
        while !window.exhausted() {
            let Some(data_key) = key_stream.next_key()? else {
                break;
            };
            let Some(entity) = Self::read_entity_for_field_extrema(&ctx, consistency, &data_key)?
            else {
                continue;
            };
            rows_scanned = rows_scanned.saturating_add(1);
            if !window.accept_existing_row() {
                continue;
            }
            let value = extract_numeric_field_decimal(&entity, target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            sum = add_numeric_decimal(sum, value)?;
            row_count = row_count.saturating_add(1);
        }
        record_rows_scanned::<E>(rows_scanned);

        // Phase 4: finish SUM/AVG output with shared numeric arithmetic semantics.
        finalize_numeric_field_output(sum, row_count, kind)
    }

    fn aggregate_numeric_stream_direction(plan: &ExecutablePlan<E>) -> Direction {
        ExecutionOrderContract::from_plan(false, plan.order_spec()).primary_scan_direction()
    }
}

// Finalize SUM/AVG numeric field output from one shared `(sum, row_count)`
// accumulator pair so streaming and materialized paths stay behavior-identical.
fn finalize_numeric_field_output(
    sum: Decimal,
    row_count: u64,
    kind: NumericFieldAggregateKind,
) -> Result<Option<Decimal>, InternalError> {
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

            divide_numeric_decimal(sum, divisor)?
        }
    };

    Ok(Some(output))
}

// Add one decimal term to one aggregate numeric accumulator through the shared
// numeric arithmetic contract so projection/aggregate arithmetic semantics stay aligned.
fn add_numeric_decimal(sum: Decimal, value: Decimal) -> Result<Decimal, InternalError> {
    let Some(next) = apply_numeric_arithmetic(
        NumericArithmeticOp::Add,
        &Value::Decimal(sum),
        &Value::Decimal(value),
    ) else {
        return Err(InternalError::query_executor_invariant(
            "numeric aggregate addition produced non-coercible decimal operands",
        ));
    };

    Ok(next)
}

// Divide one decimal accumulator by one decimal divisor through the shared
// numeric arithmetic contract so aggregate AVG inherits canonical rounding behavior.
fn divide_numeric_decimal(sum: Decimal, divisor: Decimal) -> Result<Decimal, InternalError> {
    let Some(result) = apply_numeric_arithmetic(
        NumericArithmeticOp::Div,
        &Value::Decimal(sum),
        &Value::Decimal(divisor),
    ) else {
        return Err(InternalError::query_executor_invariant(
            "numeric aggregate division produced non-coercible decimal operands",
        ));
    };

    Ok(result)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::executor::aggregate::numeric::{add_numeric_decimal, divide_numeric_decimal},
        types::Decimal,
    };

    #[test]
    fn aggregate_numeric_addition_uses_shared_saturating_decimal_semantics() {
        let left = Decimal::from_i128_with_scale(i128::MAX, 0);
        let right = Decimal::from_i128_with_scale(1, 0);

        let result = add_numeric_decimal(left, right).expect("decimal add should succeed");

        assert_eq!(result, Decimal::from_i128_with_scale(i128::MAX, 0));
    }

    #[test]
    fn aggregate_numeric_avg_division_uses_shared_rounding_semantics() {
        let sum = Decimal::from_num(-1_i64).expect("sum decimal");
        let divisor = Decimal::from_num(6_u64).expect("divisor decimal");

        let result = divide_numeric_decimal(sum, divisor).expect("decimal div should succeed");

        assert_eq!(
            result,
            Decimal::from_i128_with_scale(-166_666_666_666_666_667, 18)
        );
    }
}
