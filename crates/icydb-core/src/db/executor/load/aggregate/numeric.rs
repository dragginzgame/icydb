use crate::{
    db::{
        executor::{
            aggregate::field::{FieldSlot, extract_numeric_field_decimal},
            load::LoadExecutor,
        },
        query::plan::lowering::ExecutablePlan,
        response::Response,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Decimal,
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
    pub(in crate::db) fn aggregate_sum_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Decimal>, InternalError> {
        self.execute_numeric_field_aggregate(
            plan,
            target_field.into().as_str(),
            NumericFieldAggregateKind::Sum,
        )
    }

    pub(in crate::db) fn aggregate_avg_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Decimal>, InternalError> {
        self.execute_numeric_field_aggregate(
            plan,
            target_field.into().as_str(),
            NumericFieldAggregateKind::Avg,
        )
    }

    // Execute one field-target numeric aggregate (`sum(field)` / `avg(field)`)
    // via canonical materialized fallback semantics.
    fn execute_numeric_field_aggregate(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
        kind: NumericFieldAggregateKind,
    ) -> Result<Option<Decimal>, InternalError> {
        let field_slot = Self::resolve_numeric_field_slot(target_field)?;
        let response = self.execute(plan)?;

        Self::aggregate_numeric_field_from_materialized(response, target_field, field_slot, kind)
    }

    // Reduce one materialized response into `sum(field)` / `avg(field)` over
    // numeric field values coerced to Decimal.
    fn aggregate_numeric_field_from_materialized(
        response: Response<E>,
        target_field: &str,
        field_slot: FieldSlot,
        kind: NumericFieldAggregateKind,
    ) -> Result<Option<Decimal>, InternalError> {
        let mut sum = Decimal::ZERO;
        let mut row_count = 0u64;
        for (_, entity) in response {
            let value = extract_numeric_field_decimal(&entity, target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            sum += value;
            row_count = row_count.saturating_add(1);
        }
        if row_count == 0 {
            return Ok(None);
        }

        let output = match kind {
            NumericFieldAggregateKind::Sum => sum,
            NumericFieldAggregateKind::Avg => {
                let Some(divisor) = Decimal::from_num(row_count) else {
                    return Err(InternalError::query_executor_invariant(
                        "numeric field AVG divisor conversion overflowed decimal bounds",
                    ));
                };

                sum / divisor
            }
        };

        Ok(Some(output))
    }
}
