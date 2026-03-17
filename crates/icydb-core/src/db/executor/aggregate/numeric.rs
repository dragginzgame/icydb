//! Module: executor::aggregate::numeric
//! Responsibility: numeric field-target aggregate terminals (`sum`/`avg`).
//! Does not own: numeric coercion policy beyond field helper contracts.
//! Boundary: materialized numeric aggregate helpers for load executor terminals.

use crate::{
    db::{
        access::AccessPathKind,
        cursor::{ContinuationRuntime, LoopAction},
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutableAccess, ExecutablePlan,
            ExecutionKernel, ExecutionOptimizationCounter, ExecutionPreparation,
            KeyStreamLoopControl,
            aggregate::aggregate_window_is_provably_empty,
            aggregate::field::{
                FieldSlot, extract_numeric_field_decimal,
                resolve_numeric_aggregate_target_slot_from_planner_slot,
            },
            pipeline::contracts::LoadExecutor,
            plan_metrics::record_rows_scanned,
            validate_executor_plan,
        },
        numeric::{add_decimal_terms, average_decimal_terms},
        query::plan::{ExecutionOrderContract, FieldSlot as PlannedFieldSlot},
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Decimal,
};
use std::cell::RefCell;

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
    // Execute one field-target numeric aggregate terminal (`sum`/`avg`) through
    // one shared slot-resolution and execution-path contract.
    fn aggregate_numeric_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
        kind: NumericFieldAggregateKind,
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
                kind,
            );
        }

        let response = self.execute(plan)?;

        Self::aggregate_numeric_field_from_materialized(
            response,
            target_field.field(),
            field_slot,
            kind,
        )
    }

    /// Execute `sum(field)` over the effective response window using one
    /// planner-resolved numeric field slot.
    pub(in crate::db) fn aggregate_sum_by_slot(
        &self,
        plan: ExecutablePlan<E>,
        target_field: PlannedFieldSlot,
    ) -> Result<Option<Decimal>, InternalError> {
        self.aggregate_numeric_by_slot(plan, target_field, NumericFieldAggregateKind::Sum)
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
            Some(value) => Err(crate::db::error::query_executor_invariant(format!(
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
        self.aggregate_numeric_by_slot(plan, target_field, NumericFieldAggregateKind::Avg)
    }

    // Reduce one materialized response into `sum(field)` / `avg(field)` over
    // numeric field values coerced to Decimal.
    fn aggregate_numeric_field_from_materialized(
        response: EntityResponse<E>,
        target_field: &str,
        field_slot: FieldSlot,
        kind: NumericFieldAggregateKind,
    ) -> Result<Option<Decimal>, InternalError> {
        let mut accumulator = NumericAggregateAccumulator::new();
        for row in response {
            let value = extract_numeric_field_decimal(row.entity_ref(), target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            accumulator.add(value);
        }

        finalize_numeric_field_output(accumulator, kind)
    }

    // Return whether numeric field aggregates can use one direct key-stream fold.
    fn streaming_numeric_field_aggregate_eligible(plan: &ExecutablePlan<E>) -> bool {
        if !Self::aggregate_predicate_safe(plan) {
            return false;
        }

        let access_strategy = plan.access().resolve_strategy();
        let Some(path) = access_strategy.as_path() else {
            return false;
        };
        let path_kind = path.capabilities().kind();
        if !Self::aggregate_access_path_safe(path_kind) {
            return false;
        }

        Self::aggregate_page_window_safe(plan, path_kind)
    }

    // Return whether predicate and distinct planner flags preserve one
    // canonical direct stream-fold contract.
    const fn aggregate_predicate_safe(plan: &ExecutablePlan<E>) -> bool {
        plan.has_no_predicate_or_distinct()
    }

    // Return whether the resolved access path kind can support one direct
    // numeric stream fold without fan-out duplication risks.
    const fn aggregate_access_path_safe(path_kind: AccessPathKind) -> bool {
        path_kind.supports_streaming_numeric_fold()
    }

    // Return whether one paged ORDER BY window preserves one direct numeric
    // stream-fold contract under primary-key order constraints.
    fn aggregate_page_window_safe(plan: &ExecutablePlan<E>, path_kind: AccessPathKind) -> bool {
        if plan.page_spec().is_none() {
            return true;
        }
        let Some(_order) = plan.order_spec() else {
            // Planner rejects unordered pagination, but fail closed if bypassed.
            return false;
        };
        if plan.explicit_primary_key_order_direction().is_none() {
            return false;
        }

        path_kind.supports_streaming_numeric_fold_for_paged_primary_key_window()
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
        // Phase 1: capture lowered index specs and consume executable plan into logical form.
        let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
        let index_range_specs = plan.index_range_specs()?.to_vec();
        let consistency = plan.consistency();
        let direction = Self::aggregate_numeric_stream_direction(&plan);
        let logical_plan = plan.into_inner();
        validate_executor_plan::<E>(&logical_plan)?;
        let execution_preparation = ExecutionPreparation::for_plan::<E>(&logical_plan);
        let continuation = RefCell::new(ContinuationRuntime::from_window(
            ExecutionKernel::window_cursor_contract(&logical_plan, None),
        ));

        // Phase 2: resolve the canonical ordered key stream from access descriptors.
        let index_predicate_execution = execution_preparation.strict_mode().map(|program| {
            crate::db::index::predicate::IndexPredicateExecution {
                program,
                rejected_keys_counter: None,
            }
        });
        let ctx = self.recovered_context()?;
        let access = ExecutableAccess::new(
            &logical_plan.access,
            AccessStreamBindings::new(
                index_prefix_specs.as_slice(),
                index_range_specs.as_slice(),
                AccessScanContinuationInput::new(None, direction),
            ),
            None,
            index_predicate_execution,
        );
        let mut key_stream = ctx.ordered_key_stream_from_runtime_access(access)?;
        Self::record_execution_optimization_hit_for_tests(
            ExecutionOptimizationCounter::NumericFieldStreamingFoldFastPath,
        );

        // Phase 3: stream-fold numeric values directly from row reads.
        let mut rows_scanned = 0usize;
        let mut accumulator = NumericAggregateAccumulator::new();
        let mut pre_key =
            || Self::loop_control_from_continuation_action(continuation.borrow_mut().pre_fetch());
        let mut on_key =
            |_data_key, entity: Option<E>| -> Result<KeyStreamLoopControl, InternalError> {
                let Some(entity) = entity else {
                    return Ok(KeyStreamLoopControl::Emit);
                };
                rows_scanned = rows_scanned.saturating_add(1);
                match continuation.borrow_mut().accept_row() {
                    LoopAction::Skip => return Ok(KeyStreamLoopControl::Skip),
                    LoopAction::Emit => {}
                    LoopAction::Stop => return Ok(KeyStreamLoopControl::Stop),
                }
                let value = extract_numeric_field_decimal(&entity, target_field, field_slot)
                    .map_err(Self::map_aggregate_field_value_error)?;
                accumulator.add(value);

                Ok(KeyStreamLoopControl::Emit)
            };
        Self::drive_field_entity_stream(
            &ctx,
            consistency,
            key_stream.as_mut(),
            &mut pre_key,
            &mut on_key,
        )?;
        record_rows_scanned::<E>(rows_scanned);

        // Phase 4: finish SUM/AVG output with shared numeric arithmetic semantics.
        finalize_numeric_field_output(accumulator, kind)
    }

    fn aggregate_numeric_stream_direction(plan: &ExecutablePlan<E>) -> Direction {
        ExecutionOrderContract::from_plan(false, plan.order_spec()).primary_scan_direction()
    }

    const fn loop_control_from_continuation_action(action: LoopAction) -> KeyStreamLoopControl {
        match action {
            LoopAction::Skip => KeyStreamLoopControl::Skip,
            LoopAction::Emit => KeyStreamLoopControl::Emit,
            LoopAction::Stop => KeyStreamLoopControl::Stop,
        }
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
    kind: NumericFieldAggregateKind,
) -> Result<Option<Decimal>, InternalError> {
    if accumulator.row_count == 0 {
        return Ok(None);
    }

    let output = match kind {
        NumericFieldAggregateKind::Sum => accumulator.sum,
        NumericFieldAggregateKind::Avg => {
            let Some(avg) = average_decimal_terms(accumulator.sum, accumulator.row_count) else {
                return Err(crate::db::error::query_executor_invariant(
                    "numeric field AVG divisor conversion overflowed decimal bounds",
                ));
            };

            avg
        }
    };

    Ok(Some(output))
}

// Add one decimal term to one aggregate numeric accumulator through the shared
// numeric arithmetic contract so projection/aggregate arithmetic semantics stay aligned.
fn add_numeric_decimal(sum: Decimal, value: Decimal) -> Decimal {
    add_decimal_terms(sum, value)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{executor::aggregate::numeric::add_numeric_decimal, numeric::average_decimal_terms},
        types::Decimal,
    };

    #[test]
    fn aggregate_numeric_addition_uses_shared_saturating_decimal_semantics() {
        let left = Decimal::from_i128_with_scale(i128::MAX, 0);
        let right = Decimal::from_i128_with_scale(1, 0);

        let result = add_numeric_decimal(left, right);

        assert_eq!(result, Decimal::from_i128_with_scale(i128::MAX, 0));
    }

    #[test]
    fn aggregate_numeric_avg_division_uses_shared_rounding_semantics() {
        let sum = Decimal::from_num(-1_i64).expect("sum decimal");

        let result =
            average_decimal_terms(sum, 6_u64).expect("decimal avg should produce one value");

        assert_eq!(
            result,
            Decimal::from_i128_with_scale(-166_666_666_666_666_667, 18)
        );
    }
}
