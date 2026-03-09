//! Module: executor::load::grouped_distinct
//! Responsibility: grouped global DISTINCT field-target runtime handling.
//! Does not own: grouped planning policy or generic grouped fold mechanics.
//! Boundary: grouped DISTINCT special-case helpers used by load grouped execution.

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
            load::{LoadExecutor, ResolvedExecutionKeyStream},
        },
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic},
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Decimal,
    value::Value,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Execute one global DISTINCT field-target grouped aggregate with grouped
    // distinct budget accounting and deterministic reducer behavior.
    pub(super) fn execute_global_distinct_field_aggregate(
        consistency: MissingRowPolicy,
        ctx: &Context<'_, E>,
        resolved: &mut ResolvedExecutionKeyStream,
        compiled_predicate: Option<&crate::db::predicate::PredicateProgram>,
        grouped_execution_context: &mut ExecutionContext,
        execution_spec: (&str, bool),
        row_counters: (&mut usize, &mut usize),
    ) -> Result<GroupedRow, InternalError> {
        let (target_field, aggregate_kind_is_sum) = execution_spec;
        let (scanned_rows, filtered_rows) = row_counters;
        let field_slot = if aggregate_kind_is_sum {
            Self::resolve_numeric_field_slot(target_field)?
        } else {
            Self::resolve_any_field_slot(target_field)?
        };
        let mut distinct_values = GroupKeySet::new();
        let mut count = 0u32;
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

            if aggregate_kind_is_sum {
                let numeric_value =
                    extract_numeric_field_decimal(&entity, target_field, field_slot)
                        .map_err(AggregateFieldValueError::into_internal_error)?;
                let Some(next_sum) = apply_numeric_arithmetic(
                    NumericArithmeticOp::Add,
                    &Value::Decimal(sum),
                    &Value::Decimal(numeric_value),
                ) else {
                    return Err(super::invariant(
                        "global grouped SUM(DISTINCT field) addition failed numeric coercion",
                    ));
                };
                sum = next_sum;
                saw_sum_value = true;
            } else {
                count = count.saturating_add(1);
            }
        }

        let aggregate_value = if aggregate_kind_is_sum {
            if saw_sum_value {
                Value::Decimal(sum)
            } else {
                Value::Null
            }
        } else {
            Value::Uint(u64::from(count))
        };

        Ok(GroupedRow::new(Vec::new(), vec![aggregate_value]))
    }

    // Apply grouped pagination semantics to the singleton global grouped row.
    pub(super) fn page_global_distinct_grouped_row(
        row: GroupedRow,
        initial_offset_for_page: usize,
        limit: Option<usize>,
    ) -> Vec<GroupedRow> {
        page_global_distinct_grouped_row_for_window(row, initial_offset_for_page, limit)
    }
}

// Apply grouped pagination semantics to one singleton global DISTINCT grouped
// row using routed grouped pagination window primitives.
fn page_global_distinct_grouped_row_for_window(
    row: GroupedRow,
    initial_offset_for_page: usize,
    limit: Option<usize>,
) -> Vec<GroupedRow> {
    if initial_offset_for_page > 0 || limit == Some(0) {
        return Vec::new();
    }

    vec![row]
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_distinct_grouped_row_paging_offset_consumes_singleton_row() {
        let row = GroupedRow::new(Vec::new(), vec![Value::Uint(1)]);

        let paged = page_global_distinct_grouped_row_for_window(row, 1, Some(1));

        assert!(
            paged.is_empty(),
            "grouped singleton rows must be skipped when grouped window offset is non-zero",
        );
    }

    #[test]
    fn global_distinct_grouped_row_paging_zero_limit_consumes_singleton_row() {
        let row = GroupedRow::new(Vec::new(), vec![Value::Uint(1)]);

        let paged = page_global_distinct_grouped_row_for_window(row, 0, Some(0));

        assert!(
            paged.is_empty(),
            "grouped singleton rows must be skipped when grouped window limit is zero",
        );
    }

    #[test]
    fn global_distinct_grouped_row_paging_emits_singleton_without_offset_or_zero_limit() {
        let row = GroupedRow::new(Vec::new(), vec![Value::Uint(1)]);
        let row_unbounded = row.clone();

        let bounded = page_global_distinct_grouped_row_for_window(row, 0, Some(5));
        let unbounded = page_global_distinct_grouped_row_for_window(row_unbounded, 0, None);

        assert_eq!(
            bounded.len(),
            1,
            "grouped singleton rows must be emitted when grouped window keeps at least one row",
        );
        assert_eq!(
            unbounded.len(),
            1,
            "grouped singleton rows must be emitted for unbounded grouped windows",
        );
    }
}
