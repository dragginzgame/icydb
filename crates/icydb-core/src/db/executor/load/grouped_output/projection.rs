use crate::{
    db::{
        GroupedRow,
        executor::{
            aggregate::AggregateOutput,
            load::{LoadExecutor, invariant},
        },
        query::{
            builder::AggregateExpr,
            plan::{FieldSlot, PlannedProjectionLayout, expr::ProjectionSpec},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Evaluate grouped projection semantics for each grouped row while preserving
    // grouped response contract at the public boundary.
    pub(in crate::db::executor::load) fn project_grouped_rows_from_projection(
        projection: &ProjectionSpec,
        projection_layout: &PlannedProjectionLayout,
        group_fields: &[FieldSlot],
        aggregate_exprs: &[AggregateExpr],
        rows: Vec<GroupedRow>,
    ) -> Result<Vec<GroupedRow>, InternalError> {
        let mut projected_rows = Vec::with_capacity(rows.len());
        for row in rows {
            projected_rows.push(Self::project_grouped_row_from_projection(
                projection,
                projection_layout,
                group_fields,
                aggregate_exprs,
                row.group_key(),
                row.aggregate_values(),
            )?);
        }

        Ok(projected_rows)
    }

    // Evaluate one grouped projection expression row and convert it into
    // grouped `(group_key, aggregate_values)` payload vectors.
    pub(in crate::db::executor::load) fn project_grouped_row_from_projection(
        projection: &ProjectionSpec,
        projection_layout: &PlannedProjectionLayout,
        group_fields: &[FieldSlot],
        aggregate_exprs: &[AggregateExpr],
        group_key_values: &[Value],
        aggregate_values: &[Value],
    ) -> Result<GroupedRow, InternalError> {
        let grouped_row = crate::db::executor::load::projection::GroupedRowView::new(
            group_key_values,
            aggregate_values,
            group_fields,
            aggregate_exprs,
        );
        let projected_values =
            crate::db::executor::load::projection::evaluate_grouped_projection_values(
                projection,
                &grouped_row,
            )
            .map_err(|err| {
                InternalError::query_invalid_logical_plan(format!(
                    "grouped projection evaluation failed: {err}",
                ))
            })?;

        let mut projected_group_key =
            Vec::with_capacity(projection_layout.group_field_positions().len());
        for position in projection_layout.group_field_positions() {
            let Some(value) = projected_values.get(*position) else {
                return Err(invariant(format!(
                    "grouped projection layout group-field position out of bounds: position={position}, projected_len={}",
                    projected_values.len()
                )));
            };
            projected_group_key.push(value.clone());
        }

        let mut projected_aggregate_values =
            Vec::with_capacity(projection_layout.aggregate_positions().len());
        for position in projection_layout.aggregate_positions() {
            let Some(value) = projected_values.get(*position) else {
                return Err(invariant(format!(
                    "grouped projection layout aggregate position out of bounds: position={position}, projected_len={}",
                    projected_values.len()
                )));
            };
            projected_aggregate_values.push(value.clone());
        }

        Ok(GroupedRow::new(
            projected_group_key,
            projected_aggregate_values,
        ))
    }

    // Convert one aggregate output payload into grouped response value payload.
    pub(in crate::db::executor::load) fn aggregate_output_to_value(
        output: &AggregateOutput<E>,
    ) -> Value {
        match output {
            AggregateOutput::Count(value) => Value::Uint(u64::from(*value)),
            AggregateOutput::Sum(value) => value.map_or(Value::Null, Value::Decimal),
            AggregateOutput::Exists(value) => Value::Bool(*value),
            AggregateOutput::Min(value)
            | AggregateOutput::Max(value)
            | AggregateOutput::First(value)
            | AggregateOutput::Last(value) => value.map_or(Value::Null, Value::from),
        }
    }
}
