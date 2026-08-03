//! Module: db::executor::aggregate::runtime::grouped_distinct::aggregate
//! Responsibility: structural global DISTINCT grouped aggregate execution.
//! Does not own: grouped planning policy or shared grouped fold mechanics.
//! Boundary: consumes structural grouped row/runtime contracts and emits one grouped row.

use crate::db::executor::aggregate::runtime::grouped_distinct::global_distinct_field_target_and_kind;
use crate::{
    db::{
        executor::{
            RuntimeGroupedRow,
            aggregate::{
                EffectiveRuntimeFilterProgram, ExecutionContext, GlobalDistinctAggregateKind,
                GroupError, GroupedDistinctExecutionStrategy,
                field::{
                    AggregateFieldValueError, FieldSlot,
                    resolve_any_aggregate_target_slot_from_planner_slot,
                    resolve_numeric_aggregate_target_slot_from_planner_slot,
                },
                value_reducer::ValueReducerState,
            },
            group::{CanonicalKey, GroupKeySet, KeyCanonicalError},
            pipeline::contracts::ResolvedExecutionKeyStream,
            pipeline::runtime::{RowView, StructuralGroupedRowRuntime},
        },
        numeric::coerce_numeric_decimal,
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    types::Decimal,
    value::Value,
};

///
/// GlobalDistinctFieldAggregateDispatcher
///
/// GlobalDistinctFieldAggregateDispatcher resolves one target field once and
/// exposes structural row-view extraction helpers used by the grouped global
/// DISTINCT runtime loop.
///

struct GlobalDistinctFieldAggregateDispatcher {
    field_slot: FieldSlot,
    needs_numeric: bool,
}

// Resolve one grouped DISTINCT aggregate kind or report the caller-owned
// invariant when the planner strategy omitted that field-target aggregate.
fn global_distinct_aggregate_kind(
    execution_strategy: &GroupedDistinctExecutionStrategy,
) -> Result<GlobalDistinctAggregateKind, InternalError> {
    let aggregate_kind = execution_strategy
        .global_distinct_aggregate_kind()
        .ok_or_else(InternalError::query_executor_invariant)?;

    aggregate_kind
        .global_distinct_kind()
        .ok_or_else(InternalError::query_executor_invariant)
}

impl GlobalDistinctAggregateKind {
    // Return whether this grouped global-DISTINCT reducer needs numeric payload
    // admission instead of key-only counting.
    const fn needs_numeric_distinct_payload(self) -> bool {
        !matches!(self, Self::Count)
    }

    // Resolve one grouped global-DISTINCT reducer family into the local
    // ingest/finalize dispatch modes consumed by the runtime loop.
    const fn reducer_spec(self) -> DistinctReducerSpec {
        match self {
            Self::Count => DistinctReducerSpec {
                apply_mode: DistinctApplyMode::Count,
                reducer: ValueReducerState::count(),
            },
            Self::Sum => DistinctReducerSpec {
                apply_mode: DistinctApplyMode::Numeric,
                reducer: ValueReducerState::sum(),
            },
            Self::Avg => DistinctReducerSpec {
                apply_mode: DistinctApplyMode::Numeric,
                reducer: ValueReducerState::avg(),
            },
        }
    }
}

impl GlobalDistinctFieldAggregateDispatcher {
    // Resolve one grouped global DISTINCT field reducer from the planner-frozen
    // grouped DISTINCT strategy contract.
    fn resolve(
        execution_strategy: &GroupedDistinctExecutionStrategy,
    ) -> Result<Self, InternalError> {
        let (target_slot, _) = global_distinct_field_target_and_kind(execution_strategy)
            .ok_or_else(InternalError::query_executor_invariant)?;
        let reducer_kind = global_distinct_aggregate_kind(execution_strategy)?;
        let field_slot = if reducer_kind.needs_numeric_distinct_payload() {
            resolve_numeric_aggregate_target_slot_from_planner_slot(target_slot)
                .map_err(AggregateFieldValueError::into_internal_error)?
        } else {
            resolve_any_aggregate_target_slot_from_planner_slot(target_slot)
                .map_err(AggregateFieldValueError::into_internal_error)?
        };
        Ok(Self {
            field_slot,
            needs_numeric: reducer_kind.needs_numeric_distinct_payload(),
        })
    }

    // Extract the canonical distinct value and optional numeric payload from one
    // structural row view using one slot-reader pass.
    fn extract(&self, row_view: &RowView) -> Result<(Value, Option<Decimal>), InternalError> {
        let distinct_value = row_view.extract_orderable_field_value(self.field_slot)?;
        let numeric_value = if self.needs_numeric {
            let Some(decimal) = coerce_numeric_decimal(&distinct_value) else {
                return Err(AggregateFieldValueError::field_value_type_mismatch(
                    self.field_slot,
                    &distinct_value,
                )
                .into_internal_error());
            };

            Some(decimal)
        } else {
            None
        };

        Ok((distinct_value, numeric_value))
    }
}

///
/// DistinctReducerSpec
///
/// DistinctReducerSpec resolves grouped DISTINCT reducer behavior once so the
/// hot ingest/finalize path does not branch on aggregate kind repeatedly.
///

struct DistinctReducerSpec {
    apply_mode: DistinctApplyMode,
    reducer: ValueReducerState,
}

impl DistinctReducerSpec {
    // Resolve one reducer kind into structural ingest/finalize dispatch.
    fn from_strategy(
        execution_strategy: &GroupedDistinctExecutionStrategy,
    ) -> Result<Self, InternalError> {
        let reducer_kind = global_distinct_aggregate_kind(execution_strategy)?;

        Ok(reducer_kind.reducer_spec())
    }
}

///
/// DistinctApplyMode
///
/// DistinctApplyMode resolves grouped DISTINCT ingest behavior once so COUNT
/// can remain infallible while SUM and AVG keep the numeric validation path.
///

enum DistinctApplyMode {
    Count,
    Numeric,
}

///
/// GlobalDistinctFieldAccumulator
///
/// GlobalDistinctFieldAccumulator owns the reducer state for one global grouped
/// DISTINCT field terminal after value admission/deduplication.
///

struct GlobalDistinctFieldAccumulator {
    apply_mode: DistinctApplyMode,
    reducer: ValueReducerState,
}

impl GlobalDistinctFieldAccumulator {
    // Build one empty global DISTINCT reducer state.
    fn new(reducer_spec: DistinctReducerSpec) -> Self {
        Self {
            apply_mode: reducer_spec.apply_mode,
            reducer: reducer_spec.reducer,
        }
    }

    // Apply one admitted distinct field value to the reducer state.
    fn apply_distinct_value(
        &mut self,
        numeric_value: Option<Decimal>,
    ) -> Result<(), InternalError> {
        match self.apply_mode {
            DistinctApplyMode::Count => self.reducer.increment_count(),
            DistinctApplyMode::Numeric => {
                let Some(numeric_value) = numeric_value else {
                    return Err(GroupError::numeric_ingest_payload_required().into_internal_error());
                };

                self.reducer.ingest_decimal(numeric_value)
            }
        }
    }

    // Finalize the reducer state into one grouped aggregate output value.
    fn finalize(self) -> Result<Value, InternalError> {
        self.reducer.into_final_value()
    }
}

// Execute one global DISTINCT grouped field aggregate over one structural key
// stream and emit the singleton grouped row expected by grouped DISTINCT routing.
pub(in crate::db::executor) fn execute_global_distinct_field_aggregate(
    consistency: MissingRowPolicy,
    row_runtime: &StructuralGroupedRowRuntime,
    resolved: &mut ResolvedExecutionKeyStream,
    effective_runtime_filter_program: Option<&EffectiveRuntimeFilterProgram>,
    grouped_execution_context: &mut ExecutionContext,
    execution_strategy: &GroupedDistinctExecutionStrategy,
    row_counters: (&mut usize, &mut usize),
) -> Result<RuntimeGroupedRow, InternalError> {
    // Phase 1: resolve structural field access and initialize distinct reducer state.
    let reducer_spec = DistinctReducerSpec::from_strategy(execution_strategy)?;
    let dispatcher = GlobalDistinctFieldAggregateDispatcher::resolve(execution_strategy)?;
    let mut distinct_values = GroupKeySet::new();
    let mut accumulator = GlobalDistinctFieldAccumulator::new(reducer_spec);
    let (scanned_rows, filtered_rows) = row_counters;

    // Phase 2: walk the resolved key stream, admit distinct values, and update
    // reducer state in one straight-line loop.
    while let Some(data_key) = resolved.key_stream_mut().next_key()? {
        let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
            continue;
        };
        *scanned_rows = (*scanned_rows).saturating_add(1);
        if let Some(effective_runtime_filter_program) = effective_runtime_filter_program
            && !row_view.eval_filter_program(effective_runtime_filter_program)?
        {
            continue;
        }
        *filtered_rows = (*filtered_rows).saturating_add(1);

        let (distinct_value, numeric_value) = dispatcher.extract(&row_view)?;
        let distinct_key = distinct_value
            .canonical_key()
            .map_err(KeyCanonicalError::into_internal_error)?;
        let admitted = grouped_execution_context
            .admit_distinct_key(
                &mut distinct_values,
                grouped_execution_context
                    .config()
                    .max_distinct_values_per_group(),
                distinct_key,
            )
            .map_err(GroupError::into_internal_error)?;
        if !admitted {
            continue;
        }

        accumulator.apply_distinct_value(numeric_value)?;
    }

    // Phase 3: emit the singleton grouped row owned by grouped global DISTINCT execution.
    Ok(RuntimeGroupedRow::new(
        Vec::new(),
        vec![accumulator.finalize()?],
    ))
}

#[cfg(test)]
mod tests {
    use crate::{types::Decimal, value::Value};

    use super::{GlobalDistinctAggregateKind, GlobalDistinctFieldAccumulator};

    #[test]
    fn global_distinct_accumulator_delegates_to_shared_value_reducers() {
        let one = Decimal::from_i64(1).expect("decimal one");
        let three = Decimal::from_i64(3).expect("decimal three");

        let mut count =
            GlobalDistinctFieldAccumulator::new(GlobalDistinctAggregateKind::Count.reducer_spec());
        count.apply_distinct_value(None).expect("count ingest");
        count.apply_distinct_value(None).expect("count ingest");
        assert_eq!(count.finalize().expect("count finalize"), Value::Nat64(2));

        let mut sum =
            GlobalDistinctFieldAccumulator::new(GlobalDistinctAggregateKind::Sum.reducer_spec());
        sum.apply_distinct_value(Some(one)).expect("sum ingest");
        sum.apply_distinct_value(Some(three)).expect("sum ingest");
        assert_eq!(
            sum.finalize().expect("sum finalize"),
            Value::Decimal(Decimal::from_i64(4).expect("decimal four")),
        );

        let mut avg =
            GlobalDistinctFieldAccumulator::new(GlobalDistinctAggregateKind::Avg.reducer_spec());
        avg.apply_distinct_value(Some(one)).expect("avg ingest");
        avg.apply_distinct_value(Some(three)).expect("avg ingest");
        assert_eq!(
            avg.finalize().expect("avg finalize"),
            Value::Decimal(Decimal::from_i64(2).expect("decimal two")),
        );
    }
}
