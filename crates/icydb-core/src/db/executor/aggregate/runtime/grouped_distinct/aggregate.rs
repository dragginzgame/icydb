//! Module: db::executor::aggregate::runtime::grouped_distinct::aggregate
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_distinct::aggregate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        Context, GroupedRow,
        executor::{
            KeyStreamLoopControl,
            aggregate::{
                ExecutionContext,
                field::{
                    AggregateFieldValueError, FieldSlot, extract_numeric_field_decimal,
                    extract_orderable_field_value,
                },
            },
            drive_key_stream_with_control_flow,
            group::{CanonicalKey, GroupKeySet, KeyCanonicalError},
            pipeline::contracts::{LoadExecutor, ResolvedExecutionKeyStream},
        },
        numeric::{add_decimal_terms, average_decimal_terms},
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Decimal,
    value::Value,
};

///
/// GlobalDistinctFieldAggregateKind
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum GlobalDistinctFieldAggregateKind {
    Count,
    Sum,
    Avg,
}

type NumericFinalizeFn = fn(Decimal, u64, bool) -> Result<Value, InternalError>;

enum GlobalDistinctFieldReducerMode {
    Count,
    Numeric { finalize_numeric: NumericFinalizeFn },
}

///
/// GlobalDistinctFieldAggregateDispatcher
///
/// GlobalDistinctFieldAggregateDispatcher owns aggregate-kind dispatch for
/// grouped global DISTINCT field reducers.
/// This centralizes kind-to-slot and kind-to-finalization routing so the fold
/// loop remains kind-agnostic.
///

struct GlobalDistinctFieldAggregateDispatcher<'a> {
    target_field: &'a str,
    field_slot: FieldSlot,
    mode: GlobalDistinctFieldReducerMode,
}

///
/// GlobalDistinctFieldAccumulator
///
/// GlobalDistinctFieldAccumulator stores reducer-local mutable counters for one
/// grouped global DISTINCT field aggregate execution pass.
///

struct GlobalDistinctFieldAccumulator {
    distinct_count: u64,
    numeric_sum: Decimal,
    saw_numeric_value: bool,
}

impl GlobalDistinctFieldAccumulator {
    // Build one empty grouped global DISTINCT field accumulator.
    const fn new() -> Self {
        Self {
            distinct_count: 0,
            numeric_sum: Decimal::ZERO,
            saw_numeric_value: false,
        }
    }
}

impl<'a> GlobalDistinctFieldAggregateDispatcher<'a> {
    // Build one dispatcher from one execution spec and resolve slot contracts.
    fn resolve<E>(
        execution_spec: (&'a str, GlobalDistinctFieldAggregateKind),
    ) -> Result<Self, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let (target_field, aggregate_kind) = execution_spec;
        match aggregate_kind {
            GlobalDistinctFieldAggregateKind::Count => Ok(Self {
                target_field,
                field_slot: LoadExecutor::<E>::resolve_any_field_slot(target_field)?,
                mode: GlobalDistinctFieldReducerMode::Count,
            }),
            GlobalDistinctFieldAggregateKind::Sum => Ok(Self {
                target_field,
                field_slot: LoadExecutor::<E>::resolve_numeric_field_slot(target_field)?,
                mode: GlobalDistinctFieldReducerMode::Numeric {
                    finalize_numeric: Self::finalize_numeric_sum,
                },
            }),
            GlobalDistinctFieldAggregateKind::Avg => Ok(Self {
                target_field,
                field_slot: LoadExecutor::<E>::resolve_numeric_field_slot(target_field)?,
                mode: GlobalDistinctFieldReducerMode::Numeric {
                    finalize_numeric: Self::finalize_numeric_avg,
                },
            }),
        }
    }

    // Resolve one canonical distinct value for key admission and dedup.
    fn distinct_value<E>(&self, entity: &E) -> Result<Value, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        extract_orderable_field_value(entity, self.target_field, self.field_slot)
            .map_err(AggregateFieldValueError::into_internal_error)
    }

    // Apply one admitted distinct entity into the reducer accumulator.
    fn ingest_distinct_entity<E>(
        &self,
        entity: &E,
        accumulator: &mut GlobalDistinctFieldAccumulator,
    ) -> Result<(), InternalError>
    where
        E: EntityKind + EntityValue,
    {
        accumulator.distinct_count = accumulator.distinct_count.saturating_add(1);

        if let GlobalDistinctFieldReducerMode::Numeric { .. } = self.mode {
            let numeric_value =
                extract_numeric_field_decimal(entity, self.target_field, self.field_slot)
                    .map_err(AggregateFieldValueError::into_internal_error)?;
            accumulator.numeric_sum = add_decimal_terms(accumulator.numeric_sum, numeric_value);
            accumulator.saw_numeric_value = true;
        }

        Ok(())
    }

    // Finalize one reducer accumulator into one grouped output value.
    fn finalize_value(
        &self,
        accumulator: GlobalDistinctFieldAccumulator,
    ) -> Result<Value, InternalError> {
        match self.mode {
            GlobalDistinctFieldReducerMode::Count => Ok(Value::Uint(accumulator.distinct_count)),
            GlobalDistinctFieldReducerMode::Numeric { finalize_numeric } => finalize_numeric(
                accumulator.numeric_sum,
                accumulator.distinct_count,
                accumulator.saw_numeric_value,
            ),
        }
    }

    // Finalize SUM(DISTINCT field) reducer output.
    fn finalize_numeric_sum(
        numeric_sum: Decimal,
        _distinct_count: u64,
        saw_numeric_value: bool,
    ) -> Result<Value, InternalError> {
        if saw_numeric_value {
            Ok(Value::Decimal(numeric_sum))
        } else {
            Ok(Value::Null)
        }
    }

    // Finalize AVG(DISTINCT field) reducer output.
    fn finalize_numeric_avg(
        numeric_sum: Decimal,
        distinct_count: u64,
        saw_numeric_value: bool,
    ) -> Result<Value, InternalError> {
        if !saw_numeric_value || distinct_count == 0 {
            return Ok(Value::Null);
        }
        let Some(avg) = average_decimal_terms(numeric_sum, distinct_count) else {
            return Err(crate::db::error::query_executor_invariant(
                "global grouped AVG(DISTINCT field) divisor conversion overflowed decimal bounds",
            ));
        };

        Ok(Value::Decimal(avg))
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one global DISTINCT field-target grouped aggregate with grouped
    // distinct budget accounting and deterministic reducer behavior.
    pub(in crate::db::executor) fn execute_global_distinct_field_aggregate(
        consistency: MissingRowPolicy,
        ctx: &Context<'_, E>,
        resolved: &mut ResolvedExecutionKeyStream,
        compiled_predicate: Option<&crate::db::predicate::PredicateProgram>,
        grouped_execution_context: &mut ExecutionContext,
        execution_spec: (&str, GlobalDistinctFieldAggregateKind),
        row_counters: (&mut usize, &mut usize),
    ) -> Result<GroupedRow, InternalError> {
        let (scanned_rows, filtered_rows) = row_counters;
        let dispatcher = GlobalDistinctFieldAggregateDispatcher::resolve::<E>(execution_spec)?;
        let mut distinct_values = GroupKeySet::new();
        let mut accumulator = GlobalDistinctFieldAccumulator::new();

        grouped_execution_context
            .record_implicit_single_group::<E>()
            .map_err(Self::map_group_error)?;

        drive_key_stream_with_control_flow(
            resolved.key_stream_mut(),
            &mut || KeyStreamLoopControl::Emit,
            &mut |key| {
                let row = match consistency {
                    MissingRowPolicy::Error => ctx.read_strict(&key),
                    MissingRowPolicy::Ignore => ctx.read(&key),
                };
                let row = match row {
                    Ok(row) => row,
                    Err(err) if err.is_not_found() => return Ok(KeyStreamLoopControl::Emit),
                    Err(err) => return Err(err),
                };
                *scanned_rows = scanned_rows.saturating_add(1);
                let (_, entity) = Context::<E>::deserialize_row((key, row))?;
                if let Some(compiled_predicate) = compiled_predicate
                    && !compiled_predicate.eval(&entity)
                {
                    return Ok(KeyStreamLoopControl::Emit);
                }
                *filtered_rows = filtered_rows.saturating_add(1);

                let distinct_value = dispatcher.distinct_value(&entity)?;
                let distinct_key = distinct_value
                    .canonical_key()
                    .map_err(KeyCanonicalError::into_internal_error)?;
                let distinct_admitted = grouped_execution_context
                    .admit_distinct_key(
                        &mut distinct_values,
                        grouped_execution_context
                            .config()
                            .max_distinct_values_per_group(),
                        distinct_key,
                    )
                    .map_err(Self::map_group_error)?;
                if !distinct_admitted {
                    return Ok(KeyStreamLoopControl::Emit);
                }

                dispatcher.ingest_distinct_entity(&entity, &mut accumulator)?;

                Ok(KeyStreamLoopControl::Emit)
            },
        )?;

        let aggregate_value = dispatcher.finalize_value(accumulator)?;

        Ok(GroupedRow::new(Vec::new(), vec![aggregate_value]))
    }
}
