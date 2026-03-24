//! Module: db::executor::aggregate::runtime::grouped_distinct::aggregate
//! Responsibility: structural global DISTINCT grouped aggregate execution.
//! Does not own: grouped planning policy or shared grouped fold mechanics.
//! Boundary: consumes structural grouped row/runtime contracts and emits one grouped row.

use crate::{
    db::{
        GroupedRow,
        data::DataKey,
        executor::{
            KeyStreamLoopControl,
            aggregate::{
                ExecutionContext, GroupError,
                field::{
                    AggregateFieldValueError, FieldSlot,
                    resolve_any_aggregate_target_slot_with_model,
                    resolve_numeric_aggregate_target_slot_with_model,
                },
            },
            drive_key_stream_with_control_flow,
            group::{CanonicalKey, GroupKeySet, KeyCanonicalError},
            pipeline::contracts::{GroupedRowRuntime, ResolvedExecutionKeyStream, RowView},
        },
        numeric::coerce_numeric_decimal,
        predicate::{MissingRowPolicy, PredicateProgram},
    },
    error::InternalError,
    model::entity::EntityModel,
    types::Decimal,
    value::Value,
};

///
/// GlobalDistinctFieldAggregateKind
///
/// GlobalDistinctFieldAggregateKind selects the supported reducer semantics for
/// grouped global DISTINCT field execution.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum GlobalDistinctFieldAggregateKind {
    Count,
    Sum,
    Avg,
}

///
/// GlobalDistinctFieldAggregateDispatcher
///
/// GlobalDistinctFieldAggregateDispatcher resolves one target field once and
/// exposes structural row-view extraction helpers used by the grouped global
/// DISTINCT runtime loop.
///

struct GlobalDistinctFieldAggregateDispatcher {
    field_name: &'static str,
    field_slot: FieldSlot,
    needs_numeric: bool,
}

impl GlobalDistinctFieldAggregateDispatcher {
    // Resolve one grouped global DISTINCT field reducer against structural model metadata.
    fn resolve_with_model(
        model: &'static EntityModel,
        execution_spec: (&str, GlobalDistinctFieldAggregateKind),
    ) -> Result<Self, AggregateFieldValueError> {
        let (target_field, reducer_kind) = execution_spec;
        let (field_slot, needs_numeric) = match reducer_kind {
            GlobalDistinctFieldAggregateKind::Count => (
                resolve_any_aggregate_target_slot_with_model(model, target_field)?,
                false,
            ),
            GlobalDistinctFieldAggregateKind::Sum | GlobalDistinctFieldAggregateKind::Avg => (
                resolve_numeric_aggregate_target_slot_with_model(model, target_field)?,
                true,
            ),
        };
        let field_name = model
            .fields
            .get(field_slot.index)
            .map(crate::model::field::FieldModel::name)
            .ok_or_else(|| AggregateFieldValueError::UnknownField {
                field: target_field.to_string(),
            })?;

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
            row_view.extract_orderable_field_value(self.field_name, self.field_slot)?;
        let numeric_value = if self.needs_numeric {
            let Some(decimal) = coerce_numeric_decimal(&distinct_value) else {
                return Err(AggregateFieldValueError::FieldValueTypeMismatch {
                    field: self.field_name.to_string(),
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
    const fn from_kind(reducer_kind: GlobalDistinctFieldAggregateKind) -> Self {
        match reducer_kind {
            GlobalDistinctFieldAggregateKind::Count => Self {
                apply_mode: DistinctApplyMode::Count,
                finalize_mode: DistinctFinalizeMode::Count,
            },
            GlobalDistinctFieldAggregateKind::Sum => Self {
                apply_mode: DistinctApplyMode::Numeric,
                finalize_mode: DistinctFinalizeMode::Sum,
            },
            GlobalDistinctFieldAggregateKind::Avg => Self {
                apply_mode: DistinctApplyMode::Numeric,
                finalize_mode: DistinctFinalizeMode::Avg,
            },
        }
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
#[expect(clippy::too_many_arguments)]
pub(in crate::db::executor) fn execute_global_distinct_field_aggregate(
    consistency: MissingRowPolicy,
    row_runtime: &dyn GroupedRowRuntime,
    resolved: &mut ResolvedExecutionKeyStream,
    compiled_predicate: Option<&PredicateProgram>,
    grouped_execution_context: &mut ExecutionContext,
    entity_model: &'static EntityModel,
    execution_spec: (&str, GlobalDistinctFieldAggregateKind),
    row_counters: (&mut usize, &mut usize),
) -> Result<GroupedRow, InternalError> {
    // Phase 1: resolve structural field access and initialize distinct reducer state.
    let reducer_spec = DistinctReducerSpec::from_kind(execution_spec.1);
    let dispatcher =
        GlobalDistinctFieldAggregateDispatcher::resolve_with_model(entity_model, execution_spec)
            .map_err(AggregateFieldValueError::into_internal_error)?;
    let mut distinct_values = GroupKeySet::new();
    let mut accumulator = GlobalDistinctFieldAccumulator::new(reducer_spec);
    let (scanned_rows, filtered_rows) = row_counters;

    // Phase 2: walk the resolved key stream, admit distinct values, and update reducer state.
    let mut on_key = |data_key: DataKey| -> Result<KeyStreamLoopControl, InternalError> {
        let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
            return Ok(KeyStreamLoopControl::Emit);
        };
        *scanned_rows = (*scanned_rows).saturating_add(1);
        if let Some(compiled_predicate) = compiled_predicate
            && !row_view.eval_predicate(compiled_predicate)
        {
            return Ok(KeyStreamLoopControl::Emit);
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
            return Ok(KeyStreamLoopControl::Emit);
        }

        accumulator.apply_distinct_value(numeric_value)?;

        Ok(KeyStreamLoopControl::Emit)
    };
    drive_key_stream_with_control_flow(
        resolved.key_stream_mut(),
        &mut || KeyStreamLoopControl::Emit,
        &mut on_key,
    )?;

    // Phase 3: emit the singleton grouped row owned by grouped global DISTINCT execution.
    Ok(GroupedRow::new(Vec::new(), vec![accumulator.finalize()?]))
}
