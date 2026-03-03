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
        query::plan::{AccessPlannedQuery, AggregateKind, GroupDistinctPolicyReason},
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
        plan: &AccessPlannedQuery<E::Key>,
        ctx: &Context<'_, E>,
        resolved: &mut ResolvedExecutionKeyStream,
        compiled_predicate: Option<&crate::db::predicate::PredicateProgram>,
        grouped_execution_context: &mut ExecutionContext,
        aggregate_spec: (AggregateKind, &str),
        row_counters: (&mut usize, &mut usize),
    ) -> Result<GroupedRow, InternalError> {
        let (aggregate_kind, target_field) = aggregate_spec;
        let aggregate_kind_is_sum = global_distinct_kind_is_sum(aggregate_kind)?;
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

        while let Some(key) = resolved.key_stream.next_key()? {
            let row = match plan.scalar_plan().consistency {
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
        page: Option<&crate::db::query::plan::PageSpec>,
    ) -> Vec<GroupedRow> {
        let Some(page) = page else {
            return vec![row];
        };
        if page.offset > 0 || page.limit == Some(0) {
            return Vec::new();
        }

        vec![row]
    }
}

// Validate grouped global DISTINCT aggregate kind at runtime boundary.
// Planner should enforce this, but executor must fail-closed for bypassed shapes.
fn global_distinct_kind_is_sum(aggregate_kind: AggregateKind) -> Result<bool, InternalError> {
    if !aggregate_kind.supports_global_distinct_without_group_keys() {
        return Err(crate::db::executor::load::invariant(
            GroupDistinctPolicyReason::GlobalDistinctUnsupportedAggregateKind.invariant_message(),
        ));
    }

    Ok(aggregate_kind.is_sum())
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouped_global_distinct_kind_guard_accepts_count_and_sum() {
        assert!(
            !global_distinct_kind_is_sum(AggregateKind::Count)
                .expect("COUNT should be accepted by global DISTINCT kind guard")
        );
        assert!(
            global_distinct_kind_is_sum(AggregateKind::Sum)
                .expect("SUM should be accepted by global DISTINCT kind guard")
        );
    }

    #[test]
    fn grouped_global_distinct_kind_guard_rejects_non_count_sum() {
        let err = global_distinct_kind_is_sum(AggregateKind::Exists)
            .expect_err("non COUNT/SUM global DISTINCT aggregate kind must fail closed");

        assert_eq!(err.class, crate::error::ErrorClass::InvariantViolation);
        assert_eq!(err.origin, crate::error::ErrorOrigin::Query);
        assert!(
            err.message
                .contains("global DISTINCT grouped aggregate shape supports COUNT/SUM only"),
            "global DISTINCT kind guard must expose planner-policy invariant message: {err:?}",
        );
    }
}
