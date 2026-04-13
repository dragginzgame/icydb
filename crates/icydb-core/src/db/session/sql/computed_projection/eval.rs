//! Module: db::session::sql::computed_projection::eval
//! Responsibility: apply one validated computed SQL projection plan to an
//! already materialized SQL projection payload.
//! Does not own: SQL parsing, computed-projection planning, or payload routing.
//! Boundary: transforms row values only after session SQL planning has fixed the
//! computed column contract.

use crate::db::{
    GroupedRow, QueryError,
    session::sql::{
        computed_projection::model::SqlComputedProjectionPlan, projection::SqlProjectionPayload,
    },
};

// Apply one computed SQL projection plan to one field-loaded SQL payload while
// preserving row order and row count.
pub(in crate::db::session::sql::computed_projection) fn apply_computed_sql_projection_payload(
    payload: SqlProjectionPayload,
    plan: &SqlComputedProjectionPlan,
) -> Result<SqlProjectionPayload, QueryError> {
    let (_, rows, row_count) = payload.into_parts();
    let mut projected_rows = Vec::with_capacity(rows.len());

    // Phase 1: transform each base row cell-by-cell in declaration order.
    for row in rows {
        if row.len() != plan.items.len() {
            return Err(QueryError::invariant(
                "computed SQL projection row arity did not match session transform plan",
            ));
        }

        let mut projected_row = Vec::with_capacity(row.len());
        for (value, item) in row.into_iter().zip(plan.items.iter()) {
            projected_row.push(item.expr().apply_value(value)?);
        }
        projected_rows.push(projected_row);
    }

    // Phase 2: replace the base field labels with the requested computed
    // projection labels at the final session SQL boundary.
    let columns = plan
        .items
        .iter()
        .map(|item| item.output_label.clone())
        .collect::<Vec<_>>();

    Ok(SqlProjectionPayload::new(
        columns,
        projected_rows,
        row_count,
    ))
}

// Apply one grouped computed SQL projection plan to already-grouped rows while
// preserving row order, aggregate values, and continuation behavior.
pub(in crate::db::session::sql::computed_projection) fn apply_computed_sql_projection_grouped_rows(
    rows: Vec<GroupedRow>,
    plan: &SqlComputedProjectionPlan,
) -> Result<Vec<GroupedRow>, QueryError> {
    let group_key_arity = plan.group_key_arity();
    let projected_columns = plan.items.len();
    let mut projected_rows = Vec::with_capacity(rows.len());

    // Phase 1: transform only grouped key cells through the computed lane and
    // preserve aggregate outputs exactly as the grouped runtime produced them.
    for row in rows {
        let group_key = row.group_key();
        let aggregate_values = row.aggregate_values();

        if group_key.len() != group_key_arity
            || projected_columns != group_key.len().saturating_add(aggregate_values.len())
        {
            return Err(QueryError::invariant(
                "grouped computed SQL projection row shape did not match session transform plan",
            ));
        }

        let mut projected_group_key = Vec::with_capacity(group_key.len());
        for (value, item) in group_key.iter().cloned().zip(plan.items.iter()) {
            projected_group_key.push(item.expr().apply_value(value)?);
        }

        projected_rows.push(GroupedRow::from_parts(
            projected_group_key,
            aggregate_values.iter().cloned(),
        ));
    }

    Ok(projected_rows)
}
