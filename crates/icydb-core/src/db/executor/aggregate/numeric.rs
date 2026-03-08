//! Module: executor::aggregate::numeric
//! Responsibility: numeric field-target aggregate terminals (`sum`/`avg`).
//! Does not own: numeric coercion policy beyond field helper contracts.
//! Boundary: materialized numeric aggregate helpers for load executor terminals.

use crate::{
    db::{
        executor::{
            ExecutablePlan,
            aggregate::aggregate_window_is_provably_empty,
            aggregate::field::{
                FieldSlot, extract_numeric_field_decimal,
                resolve_numeric_aggregate_target_slot_from_planner_slot,
            },
            load::LoadExecutor,
        },
        numeric::{NumericArithmeticOp, apply_numeric_arithmetic},
        query::plan::FieldSlot as PlannedFieldSlot,
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
            Some(value) => Err(crate::db::error::executor_invariant(format!(
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
        if row_count == 0 {
            return Ok(None);
        }

        let output = match kind {
            NumericFieldAggregateKind::Sum => sum,
            NumericFieldAggregateKind::Avg => {
                let Some(divisor) = Decimal::from_num(row_count) else {
                    return Err(crate::db::error::executor_invariant(
                        "numeric field AVG divisor conversion overflowed decimal bounds",
                    ));
                };

                divide_numeric_decimal(sum, divisor)?
            }
        };

        Ok(Some(output))
    }
}

// Add one decimal term to one aggregate numeric accumulator through the shared
// numeric arithmetic contract so projection/aggregate arithmetic semantics stay aligned.
fn add_numeric_decimal(sum: Decimal, value: Decimal) -> Result<Decimal, InternalError> {
    let Some(next) = apply_numeric_arithmetic(
        NumericArithmeticOp::Add,
        &Value::Decimal(sum),
        &Value::Decimal(value),
    ) else {
        return Err(crate::db::error::executor_invariant(
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
        return Err(crate::db::error::executor_invariant(
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
