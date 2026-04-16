//! Module: db::executor::aggregate::runtime::grouped_distinct::aggregate
//! Responsibility: structural global DISTINCT grouped aggregate execution.
//! Does not own: grouped planning policy or shared grouped fold mechanics.
//! Boundary: consumes structural grouped row/runtime contracts and emits one grouped row.

use crate::db::executor::aggregate::runtime::grouped_distinct::global_distinct_field_target_and_kind;
use crate::{
    db::{
        GroupedRow,
        executor::{
            aggregate::{
                ExecutionContext, GroupError,
                field::{
                    AggregateFieldValueError, FieldSlot,
                    resolve_any_aggregate_target_slot_from_planner_slot,
                    resolve_numeric_aggregate_target_slot_from_planner_slot,
                },
            },
            group::{CanonicalKey, GroupKeySet, KeyCanonicalError},
            pipeline::contracts::{
                ResolvedExecutionKeyStream, RowView, StructuralGroupedRowRuntime,
            },
        },
        numeric::coerce_numeric_decimal,
        predicate::{MissingRowPolicy, PredicateProgram},
        query::plan::{AggregateKind, GroupedDistinctExecutionStrategy},
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
    field_name: String,
    field_slot: FieldSlot,
    needs_numeric: bool,
}

// Resolve one grouped DISTINCT aggregate kind or report the caller-owned
// invariant when the planner strategy omitted that field-target aggregate.
fn global_distinct_aggregate_kind(
    execution_strategy: &GroupedDistinctExecutionStrategy,
    missing_message: &'static str,
) -> Result<AggregateKind, InternalError> {
    execution_strategy
        .global_distinct_aggregate_kind()
        .ok_or_else(|| InternalError::query_executor_invariant(missing_message))
}

///
/// SupportedGlobalDistinctAggregateKind
///
/// SupportedGlobalDistinctAggregateKind narrows planner aggregate kinds down to
/// the grouped DISTINCT field-target reducer family this runtime actually
/// admits.
/// Dispatcher and reducer-spec resolution share this enum so COUNT/SUM/AVG
/// support and rejection text stay aligned.
///

#[derive(Clone, Copy)]
enum SupportedGlobalDistinctAggregateKind {
    Count,
    Sum,
    Avg,
}

impl SupportedGlobalDistinctAggregateKind {
    fn from_strategy(
        execution_strategy: &GroupedDistinctExecutionStrategy,
        missing_message: &'static str,
        unsupported_message: &'static str,
    ) -> Result<Self, InternalError> {
        let aggregate_kind = global_distinct_aggregate_kind(execution_strategy, missing_message)?;

        Self::from_aggregate_kind(aggregate_kind, unsupported_message)
    }

    fn from_aggregate_kind(
        aggregate_kind: AggregateKind,
        unsupported_message: &'static str,
    ) -> Result<Self, InternalError> {
        match aggregate_kind {
            AggregateKind::Count => Ok(Self::Count),
            AggregateKind::Sum => Ok(Self::Sum),
            AggregateKind::Avg => Ok(Self::Avg),
            AggregateKind::Exists
            | AggregateKind::Min
            | AggregateKind::Max
            | AggregateKind::First
            | AggregateKind::Last => {
                Err(InternalError::query_executor_invariant(unsupported_message))
            }
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
            .ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "grouped DISTINCT dispatcher requires a global field-target strategy",
                )
            })?;
        let reducer_kind = SupportedGlobalDistinctAggregateKind::from_strategy(
            execution_strategy,
            "grouped DISTINCT dispatcher requires a global field-target aggregate kind",
            "grouped DISTINCT dispatcher admits only COUNT/SUM/AVG field-target aggregates",
        )?;
        let (field_slot, needs_numeric) = match reducer_kind {
            SupportedGlobalDistinctAggregateKind::Count => (
                resolve_any_aggregate_target_slot_from_planner_slot(target_slot)
                    .map_err(AggregateFieldValueError::into_internal_error)?,
                false,
            ),
            SupportedGlobalDistinctAggregateKind::Sum
            | SupportedGlobalDistinctAggregateKind::Avg => (
                resolve_numeric_aggregate_target_slot_from_planner_slot(target_slot)
                    .map_err(AggregateFieldValueError::into_internal_error)?,
                true,
            ),
        };
        let field_name = target_slot.field().to_string();

        Ok(Self {
            field_name,
            field_slot,
            needs_numeric,
        })
    }

    // Extract the canonical distinct value and optional numeric payload from one
    // structural row view using one slot-reader pass.
    fn extract(
        &self,
        row_view: &RowView,
    ) -> Result<(Value, Option<Decimal>), AggregateFieldValueError> {
        let distinct_value =
            row_view.extract_orderable_field_value(self.field_name.as_str(), self.field_slot)?;
        let numeric_value = if self.needs_numeric {
            let Some(decimal) = coerce_numeric_decimal(&distinct_value) else {
                return Err(AggregateFieldValueError::FieldValueTypeMismatch {
                    field: self.field_name.clone(),
                    kind: self.field_slot.kind,
                    value: Box::new(distinct_value),
                });
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
    finalize_mode: DistinctFinalizeMode,
}

impl DistinctReducerSpec {
    // Resolve one reducer kind into structural ingest/finalize dispatch.
    fn from_strategy(
        execution_strategy: &GroupedDistinctExecutionStrategy,
    ) -> Result<Self, InternalError> {
        let reducer_kind = SupportedGlobalDistinctAggregateKind::from_strategy(
            execution_strategy,
            "grouped DISTINCT reducer requires a global field-target aggregate kind",
            "grouped DISTINCT reducer admits only COUNT/SUM/AVG field-target aggregates",
        )?;

        let reducer_spec = match reducer_kind {
            SupportedGlobalDistinctAggregateKind::Count => Self {
                apply_mode: DistinctApplyMode::Count,
                finalize_mode: DistinctFinalizeMode::Count,
            },
            SupportedGlobalDistinctAggregateKind::Sum => Self {
                apply_mode: DistinctApplyMode::Numeric,
                finalize_mode: DistinctFinalizeMode::Sum,
            },
            SupportedGlobalDistinctAggregateKind::Avg => Self {
                apply_mode: DistinctApplyMode::Numeric,
                finalize_mode: DistinctFinalizeMode::Avg,
            },
        };

        Ok(reducer_spec)
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
/// DistinctFinalizeMode
///
/// DistinctFinalizeMode resolves grouped DISTINCT finalization once so the
/// runtime can keep infallible reducers infallible and isolate the error-
/// producing AVG path to a single branch.
///

enum DistinctFinalizeMode {
    Count,
    Sum,
    Avg,
}

///
/// GlobalDistinctFieldAccumulator
///
/// GlobalDistinctFieldAccumulator owns the reducer state for one global grouped
/// DISTINCT field terminal after value admission/deduplication.
///

struct GlobalDistinctFieldAccumulator {
    distinct_count: u64,
    numeric_sum: Decimal,
    saw_numeric_value: bool,
    apply_mode: DistinctApplyMode,
    finalize_mode: DistinctFinalizeMode,
}

impl GlobalDistinctFieldAccumulator {
    // Build one empty global DISTINCT reducer state.
    const fn new(reducer_spec: DistinctReducerSpec) -> Self {
        Self {
            distinct_count: 0,
            numeric_sum: Decimal::ZERO,
            saw_numeric_value: false,
            apply_mode: reducer_spec.apply_mode,
            finalize_mode: reducer_spec.finalize_mode,
        }
    }

    // Apply one admitted distinct field value to the reducer state.
    fn apply_distinct_value(
        &mut self,
        numeric_value: Option<Decimal>,
    ) -> Result<(), InternalError> {
        self.distinct_count = self.distinct_count.saturating_add(1);

        match self.apply_mode {
            DistinctApplyMode::Count => Ok(()),
            DistinctApplyMode::Numeric => Self::apply_numeric(self, numeric_value),
        }
    }

    // Finalize the reducer state into one grouped aggregate output value.
    fn finalize(self) -> Result<Value, InternalError> {
        match self.finalize_mode {
            DistinctFinalizeMode::Count => Ok(Self::finalize_count(self)),
            DistinctFinalizeMode::Sum => Ok(Self::finalize_sum(self)),
            DistinctFinalizeMode::Avg => Self::finalize_avg(self),
        }
    }

    fn apply_numeric(
        state: &mut Self,
        numeric_value: Option<Decimal>,
    ) -> Result<(), InternalError> {
        let Some(numeric_value) = numeric_value else {
            return Err(GroupError::numeric_ingest_payload_required().into_internal_error());
        };
        state.numeric_sum = crate::db::numeric::add_decimal_terms(state.numeric_sum, numeric_value);
        state.saw_numeric_value = true;

        Ok(())
    }

    const fn finalize_count(state: Self) -> Value {
        Value::Uint(state.distinct_count)
    }

    fn finalize_sum(state: Self) -> Value {
        state
            .saw_numeric_value
            .then_some(state.numeric_sum)
            .map_or(Value::Null, Value::Decimal)
    }

    // Build the canonical grouped DISTINCT AVG finalization invariant.
    fn avg_divisor_conversion_invariant() -> InternalError {
        InternalError::query_executor_invariant(
            "global grouped AVG(DISTINCT field) divisor conversion overflowed decimal bounds",
        )
    }

    fn finalize_avg(state: Self) -> Result<Value, InternalError> {
        if !state.saw_numeric_value || state.distinct_count == 0 {
            return Ok(Value::Null);
        }
        let Some(avg) =
            crate::db::numeric::average_decimal_terms(state.numeric_sum, state.distinct_count)
        else {
            return Err(Self::avg_divisor_conversion_invariant());
        };

        Ok(Value::Decimal(avg))
    }
}

// Execute one global DISTINCT grouped field aggregate over one structural key
// stream and emit the singleton grouped row expected by grouped DISTINCT routing.
pub(in crate::db::executor) fn execute_global_distinct_field_aggregate(
    consistency: MissingRowPolicy,
    row_runtime: &StructuralGroupedRowRuntime,
    resolved: &mut ResolvedExecutionKeyStream,
    compiled_predicate: Option<&PredicateProgram>,
    grouped_execution_context: &mut ExecutionContext,
    execution_strategy: &GroupedDistinctExecutionStrategy,
    row_counters: (&mut usize, &mut usize),
) -> Result<GroupedRow, InternalError> {
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
        if let Some(compiled_predicate) = compiled_predicate
            && !row_view.eval_predicate(compiled_predicate)
        {
            continue;
        }
        *filtered_rows = (*filtered_rows).saturating_add(1);

        let (distinct_value, numeric_value) = dispatcher
            .extract(&row_view)
            .map_err(AggregateFieldValueError::into_internal_error)?;
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
    Ok(GroupedRow::new(Vec::new(), vec![accumulator.finalize()?]))
}
