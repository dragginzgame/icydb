//! Module: db::executor::aggregate::runtime::grouped_output::projection
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_output::projection.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        GroupedRow,
        executor::projection::*,
        query::{
            builder::AggregateExpr,
            plan::{FieldSlot, PlannedProjectionLayout, expr::ProjectionSpec},
        },
    },
    error::InternalError,
    value::Value,
};

// Evaluate grouped projection semantics for each grouped row while preserving
// grouped response contract at the public boundary.
pub(in crate::db::executor) fn project_grouped_rows_from_projection(
    projection: &ProjectionSpec,
    projection_layout: &PlannedProjectionLayout,
    group_fields: &[FieldSlot],
    aggregate_exprs: &[AggregateExpr],
    rows: Vec<GroupedRow>,
) -> Result<Vec<GroupedRow>, InternalError> {
    let mut projected_rows = Vec::with_capacity(rows.len());
    for row in rows {
        projected_rows.push(project_grouped_row_from_projection(
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

// Evaluate one grouped projection expression row and convert it into grouped
// `(group_key, aggregate_values)` payload vectors.
fn project_grouped_row_from_projection(
    projection: &ProjectionSpec,
    projection_layout: &PlannedProjectionLayout,
    group_fields: &[FieldSlot],
    aggregate_exprs: &[AggregateExpr],
    group_key_values: &[Value],
    aggregate_values: &[Value],
) -> Result<GroupedRow, InternalError> {
    let grouped_row = GroupedRowView::new(
        group_key_values,
        aggregate_values,
        group_fields,
        aggregate_exprs,
    );
    let projected_values =
        evaluate_grouped_projection_values(projection, &grouped_row).map_err(|err| {
            crate::db::error::query_invalid_logical_plan(format!(
                "grouped projection evaluation failed: {err}",
            ))
        })?;
    let projected_group_key = projected_values_for_positions(
        projected_values.as_slice(),
        projection_layout.group_field_positions(),
        "group-field",
    )?;
    let projected_aggregate_values = projected_values_for_positions(
        projected_values.as_slice(),
        projection_layout.aggregate_positions(),
        "aggregate",
    )?;

    Ok(GroupedRow::new(
        projected_group_key,
        projected_aggregate_values,
    ))
}

// Project one stable set of row positions into one cloned value vector. Grouped
// output layout splitting reuses this for both grouped-key and aggregate payloads.
fn projected_values_for_positions(
    projected_values: &[Value],
    positions: &[usize],
    position_kind: &str,
) -> Result<Vec<Value>, InternalError> {
    let mut values = Vec::with_capacity(positions.len());
    for position in positions {
        let Some(value) = projected_values.get(*position) else {
            return Err(crate::db::error::query_executor_invariant(format!(
                "grouped projection layout {position_kind} position out of bounds: position={position}, projected_len={}",
                projected_values.len()
            )));
        };
        values.push(value.clone());
    }

    Ok(values)
}
