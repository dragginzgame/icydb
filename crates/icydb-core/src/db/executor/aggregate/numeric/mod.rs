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
        cursor::{ContinuationRuntime, LoopAction},
        data::DataRow,
        direction::Direction,
        executor::{
            AccessScanContinuationInput, AccessStreamBindings, ExecutableAccess, ExecutablePlan,
            ExecutionKernel, ExecutionPreparation, KeyStreamLoopControl, PreparedAggregatePlan,
            StructuralTraversalRuntime,
            aggregate::field::{
                FieldSlot, extract_numeric_field_decimal_with_slot_reader,
                resolve_numeric_aggregate_target_slot_from_planner_slot_with_model,
            },
            aggregate::{
                PreparedAggregateStreamingInputs, PreparedAggregateStreamingInputsCore,
                PreparedScalarNumericBoundary, PreparedScalarNumericExecutionState,
                PreparedScalarNumericOp, PreparedScalarNumericStrategy,
            },
            pipeline::contracts::LoadExecutor,
            plan_metrics::record_rows_scanned_for_path,
            preparation::resolved_index_slots_for_access_path,
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
use std::cell::RefCell;

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
        let target_field_name = target_field.field().to_string();
        let authority = plan.authority();
        let field_slot = resolve_numeric_aggregate_target_slot_from_planner_slot_with_model(
            authority.model(),
            &target_field,
        )
        .map_err(Self::map_aggregate_field_value_error)?;
        let op = request.prepared_op();

        if request.requires_global_distinct() {
            let boundary = PreparedScalarNumericBoundary {
                target_field_name,
                field_slot,
                op,
                strategy: PreparedScalarNumericStrategy::GlobalDistinctGrouped,
            };
            let route = self.prepare_global_distinct_grouped_route(
                plan,
                op.aggregate_kind(),
                &boundary.target_field_name,
            )?;

            return Ok(PreparedScalarNumericExecutionState::GlobalDistinct {
                boundary,
                route: Box::new(route),
            });
        }

        let prepared = self.prepare_scalar_aggregate_boundary(plan)?;
        let strategy = if Self::streaming_numeric_field_aggregate_eligible(&prepared) {
            PreparedScalarNumericStrategy::Streaming
        } else {
            PreparedScalarNumericStrategy::Materialized
        };

        Ok(PreparedScalarNumericExecutionState::Aggregate {
            boundary: PreparedScalarNumericBoundary {
                target_field_name,
                field_slot,
                op,
                strategy,
            },
            prepared: Box::new(prepared),
        })
    }

    // Execute one prepared numeric aggregate contract without re-deriving
    // strategy from the original plan.
    fn execute_prepared_scalar_numeric_boundary(
        &self,
        prepared_state: PreparedScalarNumericExecutionState<'_>,
    ) -> Result<Option<Decimal>, InternalError> {
        match prepared_state {
            PreparedScalarNumericExecutionState::Aggregate { boundary, prepared } => {
                let prepared = *prepared;
                if prepared.window_is_provably_empty() {
                    return Ok(None);
                }

                match boundary.strategy {
                    PreparedScalarNumericStrategy::Streaming => {
                        Self::aggregate_numeric_field_from_streaming(
                            prepared.into_core(),
                            &boundary.target_field_name,
                            boundary.field_slot,
                            boundary.op,
                        )
                    }
                    PreparedScalarNumericStrategy::Materialized => {
                        let row_layout = RowLayout::from_model(prepared.authority.model());
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
                    PreparedScalarNumericStrategy::GlobalDistinctGrouped => {
                        Err(crate::db::error::query_executor_invariant(
                            "numeric aggregate direct execution reached global DISTINCT strategy",
                        ))
                    }
                }
            }
            PreparedScalarNumericExecutionState::GlobalDistinct { boundary, route } => {
                let value = self.execute_prepared_global_distinct_grouped_aggregate(*route)?;

                decode_global_distinct_numeric_output(value, boundary.op.aggregate_name())
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
        let row_decoder = RowDecoder::structural();

        for row in rows {
            let row = row_decoder.decode(row_layout, row)?;
            let value = extract_numeric_field_decimal_with_slot_reader(
                target_field,
                field_slot,
                &mut |index| row.slot(index),
            )
            .map_err(Self::map_aggregate_field_value_error)?;
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
            .explicit_primary_key_order_direction(prepared.authority.model().primary_key.name)
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
        // Phase 1: consume prepared aggregate stage state into one direct stream fold.
        let consistency = prepared.consistency();
        let direction = Self::aggregate_numeric_stream_direction(&prepared);
        let row_layout = RowLayout::from_model(prepared.authority.model());
        let PreparedAggregateStreamingInputsCore {
            authority,
            store,
            logical_plan,
            index_prefix_specs,
            index_range_specs,
            ..
        } = prepared;
        let execution_preparation = ExecutionPreparation::from_plan(
            authority.model(),
            &logical_plan,
            resolved_index_slots_for_access_path(
                authority.model(),
                logical_plan.access.resolve_strategy().executable(),
            ),
        );
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
        let runtime = StructuralTraversalRuntime::new(store, authority.entity_tag());
        let mut key_stream = runtime.ordered_key_stream_from_structural_runtime_access(access)?;

        // Phase 3: stream-fold numeric values directly from row reads.
        let mut rows_scanned = 0usize;
        let mut accumulator = NumericAggregateAccumulator::new();
        let mut pre_key =
            || Self::loop_control_from_continuation_action(continuation.borrow_mut().pre_fetch());
        let mut on_key = |_data_key,
                          row: Option<crate::db::executor::terminal::page::KernelRow>|
         -> Result<KeyStreamLoopControl, InternalError> {
            let Some(row) = row else {
                return Ok(KeyStreamLoopControl::Emit);
            };
            rows_scanned = rows_scanned.saturating_add(1);
            match continuation.borrow_mut().accept_row() {
                LoopAction::Skip => return Ok(KeyStreamLoopControl::Skip),
                LoopAction::Emit => {}
                LoopAction::Stop => return Ok(KeyStreamLoopControl::Stop),
            }
            let value = extract_numeric_field_decimal_with_slot_reader(
                target_field,
                field_slot,
                &mut |index| row.slot(index),
            )
            .map_err(Self::map_aggregate_field_value_error)?;
            accumulator.add(value);

            Ok(KeyStreamLoopControl::Emit)
        };
        Self::drive_field_row_stream(
            store,
            &row_layout,
            consistency,
            key_stream.as_mut(),
            &mut pre_key,
            &mut on_key,
        )?;
        record_rows_scanned_for_path(authority.entity_path(), rows_scanned);

        // Phase 4: finish SUM/AVG output with shared numeric arithmetic semantics.
        finalize_numeric_field_output(accumulator, kind)
    }

    fn aggregate_numeric_stream_direction(
        prepared: &PreparedAggregateStreamingInputsCore,
    ) -> Direction {
        ExecutionOrderContract::from_plan(false, prepared.order_spec()).primary_scan_direction()
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
    kind: PreparedScalarNumericOp,
) -> Result<Option<Decimal>, InternalError> {
    if accumulator.row_count == 0 {
        return Ok(None);
    }

    let output = match kind {
        PreparedScalarNumericOp::Sum => accumulator.sum,
        PreparedScalarNumericOp::Avg => {
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

// Decode one global grouped DISTINCT numeric terminal result from the structural
// grouped aggregate output surface without duplicating `Value` matching at each
// terminal callsite.
fn decode_global_distinct_numeric_output(
    value: Option<Value>,
    aggregate_name: &str,
) -> Result<Option<Decimal>, InternalError> {
    match value {
        Some(Value::Decimal(value)) => Ok(Some(value)),
        Some(Value::Null) | None => Ok(None),
        Some(value) => Err(crate::db::error::query_executor_invariant(format!(
            "global {aggregate_name}(DISTINCT field) grouped output type mismatch: {value:?}",
        ))),
    }
}

// Add one decimal term to one aggregate numeric accumulator through the shared
// numeric arithmetic contract so projection/aggregate arithmetic semantics stay aligned.
fn add_numeric_decimal(sum: Decimal, value: Decimal) -> Decimal {
    add_decimal_terms(sum, value)
}
