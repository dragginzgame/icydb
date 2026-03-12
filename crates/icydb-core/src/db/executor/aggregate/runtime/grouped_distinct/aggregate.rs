//! Module: db::executor::aggregate::runtime::grouped_distinct::aggregate
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_distinct::aggregate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        Context, GroupedRow,
        executor::{
            aggregate::{
                ExecutionContext,
                field::{
                    AggregateFieldValueError, extract_numeric_field_decimal,
                    extract_orderable_field_value,
                },
            },
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

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum GlobalDistinctFieldAggregateKind {
    Count,
    Sum,
    Avg,
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one global DISTINCT field-target grouped aggregate with grouped
    // distinct budget accounting and deterministic reducer behavior.
    #[expect(clippy::too_many_lines)]
    pub(in crate::db::executor) fn execute_global_distinct_field_aggregate(
        consistency: MissingRowPolicy,
        ctx: &Context<'_, E>,
        resolved: &mut ResolvedExecutionKeyStream,
        compiled_predicate: Option<&crate::db::predicate::PredicateProgram>,
        grouped_execution_context: &mut ExecutionContext,
        execution_spec: (&str, GlobalDistinctFieldAggregateKind),
        row_counters: (&mut usize, &mut usize),
    ) -> Result<GroupedRow, InternalError> {
        let (target_field, aggregate_kind) = execution_spec;
        let (scanned_rows, filtered_rows) = row_counters;
        let field_slot = if matches!(
            aggregate_kind,
            GlobalDistinctFieldAggregateKind::Sum | GlobalDistinctFieldAggregateKind::Avg
        ) {
            Self::resolve_numeric_field_slot(target_field)?
        } else {
            Self::resolve_any_field_slot(target_field)?
        };
        let mut distinct_values = GroupKeySet::new();
        let mut distinct_count = 0u64;
        let mut sum = Decimal::ZERO;
        let mut saw_sum_value = false;

        grouped_execution_context
            .record_implicit_single_group::<E>()
            .map_err(Self::map_group_error)?;

        while let Some(key) = resolved.key_stream_mut().next_key()? {
            let row = match consistency {
                MissingRowPolicy::Error => ctx.read_strict(&key),
                MissingRowPolicy::Ignore => ctx.read(&key),
            };
            let row = match row {
                Ok(row) => row,
                Err(err) if err.is_not_found() => continue,
                Err(err) => return Err(err),
            };
            *scanned_rows = scanned_rows.saturating_add(1);
            let (_, entity) = Context::<E>::deserialize_row((key, row))?;
            if let Some(compiled_predicate) = compiled_predicate
                && !compiled_predicate.eval(&entity)
            {
                continue;
            }
            *filtered_rows = filtered_rows.saturating_add(1);

            let distinct_value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(AggregateFieldValueError::into_internal_error)?;
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
                continue;
            }

            if matches!(
                aggregate_kind,
                GlobalDistinctFieldAggregateKind::Sum | GlobalDistinctFieldAggregateKind::Avg
            ) {
                distinct_count = distinct_count.saturating_add(1);
                let numeric_value =
                    extract_numeric_field_decimal(&entity, target_field, field_slot)
                        .map_err(AggregateFieldValueError::into_internal_error)?;
                sum = add_decimal_terms(sum, numeric_value);
                saw_sum_value = true;
            } else {
                distinct_count = distinct_count.saturating_add(1);
            }
        }

        let aggregate_value = match aggregate_kind {
            GlobalDistinctFieldAggregateKind::Count => Value::Uint(distinct_count),
            GlobalDistinctFieldAggregateKind::Sum => {
                if saw_sum_value {
                    Value::Decimal(sum)
                } else {
                    Value::Null
                }
            }
            GlobalDistinctFieldAggregateKind::Avg => {
                if !saw_sum_value || distinct_count == 0 {
                    Value::Null
                } else {
                    let Some(avg) = average_decimal_terms(sum, distinct_count) else {
                        return Err(crate::db::error::query_executor_invariant(
                            "global grouped AVG(DISTINCT field) divisor conversion overflowed decimal bounds",
                        ));
                    };

                    Value::Decimal(avg)
                }
            }
        };

        Ok(GroupedRow::new(Vec::new(), vec![aggregate_value]))
    }
}
