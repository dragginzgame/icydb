//! Module: db::session::sql::execute::write_returning
//! Responsibility: SQL write `RETURNING` and statement-result shaping.
//! Does not own: SQL write selection, mutation execution, or patch construction.
//! Boundary: converts already-mutated rows into the public SQL statement result shape.

use crate::{
    db::{
        PersistedRow, QueryError,
        session::sql::{
            SqlStatementResult,
            projection::{SqlProjectionPayload, projection_labels_from_fields},
        },
        sql::parser::SqlReturningProjection,
    },
    traits::{CanisterKind, EntityValue},
    value::Value,
};

/// Shape one SQL INSERT/UPDATE mutation result after the write has already run.
///
/// This helper owns only the statement-result envelope conversion for rows that
/// were returned by the mutation path. It intentionally does not select rows,
/// build patches, or perform mutation execution.
pub(in crate::db::session::sql::execute) fn sql_write_statement_result<C, E>(
    entities: Vec<E>,
    returning: Option<&SqlReturningProjection>,
) -> Result<SqlStatementResult, QueryError>
where
    C: CanisterKind,
    E: PersistedRow<Canister = C> + EntityValue,
{
    let row_count = u32::try_from(entities.len()).unwrap_or(u32::MAX);

    match returning {
        None => Ok(SqlStatementResult::Count { row_count }),
        Some(SqlReturningProjection::All) => {
            let columns = projection_labels_from_fields(E::MODEL.fields());
            let mut rows = Vec::with_capacity(entities.len());

            for entity in entities {
                rows.push(sql_returning_all_values(&entity, E::MODEL.fields().len())?);
            }

            Ok(SqlProjectionPayload::new(
                columns,
                vec![None; E::MODEL.fields().len()],
                rows,
                row_count,
            )
            .into_statement_result())
        }
        Some(SqlReturningProjection::Fields(fields)) => {
            let columns = projection_labels_from_fields(E::MODEL.fields());
            let indices = sql_returning_field_indices(&columns, fields)?;
            let mut rows = Vec::with_capacity(entities.len());

            // Project field-list RETURNING rows directly from typed mutation
            // outputs. This avoids constructing full rows for blob-heavy
            // entities when callers return only a small subset of fields.
            for entity in entities {
                let mut row = Vec::with_capacity(indices.len());

                for index in &indices {
                    let value = entity.get_value_by_index(*index).ok_or_else(|| {
                        QueryError::invariant(
                            "SQL write statement projection row must include every returned field",
                        )
                    })?;
                    row.push(value);
                }

                rows.push(row);
            }

            Ok(
                SqlProjectionPayload::new(
                    fields.clone(),
                    vec![None; fields.len()],
                    rows,
                    row_count,
                )
                .into_statement_result(),
            )
        }
    }
}

// Materialize every field from one typed write result for `RETURNING *`.
fn sql_returning_all_values<E>(entity: &E, field_count: usize) -> Result<Vec<Value>, QueryError>
where
    E: EntityValue,
{
    let mut row = Vec::with_capacity(field_count);

    for index in 0..field_count {
        let value = entity.get_value_by_index(index).ok_or_else(|| {
            QueryError::invariant(
                "SQL write statement projection row must include every declared field",
            )
        })?;
        row.push(value);
    }

    Ok(row)
}

// Resolve a SQL RETURNING field list against the target entity columns once so
// per-row projection can move or read values by slot without redoing label
// lookups.
fn sql_returning_field_indices(
    columns: &[String],
    fields: &[String],
) -> Result<Vec<usize>, QueryError> {
    let mut indices = Vec::with_capacity(fields.len());

    for field in fields {
        let index = columns
            .iter()
            .position(|column| column == field)
            .ok_or_else(|| {
                QueryError::unsupported_query(format!(
                    "SQL RETURNING field '{field}' does not exist on the target entity"
                ))
            })?;
        indices.push(index);
    }

    Ok(indices)
}

/// Apply one SQL `RETURNING` projection to materialized mutation rows.
///
/// The caller supplies rows that already match the target entity column order.
/// This boundary keeps SQL write execution separate from the public statement
/// result shape used by session SQL responses.
pub(in crate::db::session::sql::execute) fn sql_returning_statement_projection(
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    row_count: u32,
    returning: &SqlReturningProjection,
) -> Result<SqlStatementResult, QueryError> {
    match returning {
        SqlReturningProjection::All => Ok(SqlProjectionPayload::new(
            columns,
            vec![None; rows.first().map_or(0, Vec::len)],
            rows,
            row_count,
        )
        .into_statement_result()),
        SqlReturningProjection::Fields(fields) => {
            let indices = sql_returning_field_indices(&columns, fields)?;
            let selection = sql_returning_field_selection(&indices)?;

            let mut projected_rows = Vec::with_capacity(rows.len());
            for row in rows {
                projected_rows.push(sql_returning_project_owned_row(row, selection.as_slice())?);
            }

            Ok(SqlProjectionPayload::new(
                fields.clone(),
                vec![None; fields.len()],
                projected_rows,
                row_count,
            )
            .into_statement_result())
        }
    }
}

// Build an input-index ordered projection plan for owned SQL RETURNING rows.
// The output position keeps caller-requested field order, while the input index
// order lets each row be consumed once without allocating a full-row
// `Vec<Option<Value>>` just to move selected values out.
fn sql_returning_field_selection(indices: &[usize]) -> Result<Vec<(usize, usize)>, QueryError> {
    let mut selection = Vec::with_capacity(indices.len());

    for (output_index, input_index) in indices.iter().copied().enumerate() {
        if selection
            .iter()
            .any(|(existing_index, _)| *existing_index == input_index)
        {
            return Err(sql_returning_projection_alignment_error());
        }
        selection.push((input_index, output_index));
    }
    selection.sort_unstable_by_key(|(input_index, _)| *input_index);

    Ok(selection)
}

// Project one owned SQL RETURNING row by moving only selected values into a
// field-count-sized output buffer. This keeps duplicate-field rejection and
// missing-column failures aligned with the previous take-from-Option behavior.
fn sql_returning_project_owned_row(
    row: Vec<Value>,
    selection: &[(usize, usize)],
) -> Result<Vec<Value>, QueryError> {
    let mut projected = (0..selection.len()).map(|_| None).collect::<Vec<_>>();
    let mut selection_position = 0usize;

    // Phase 1: consume the row once, moving selected values into their
    // caller-requested output positions.
    for (input_index, value) in row.into_iter().enumerate() {
        if selection_position >= selection.len() {
            break;
        }
        let (selected_input_index, output_index) = selection[selection_position];
        if input_index == selected_input_index {
            projected[output_index] = Some(value);
            selection_position = selection_position.saturating_add(1);
        }
    }

    if selection_position != selection.len() {
        return Err(sql_returning_projection_alignment_error());
    }

    projected
        .into_iter()
        .map(|value| value.ok_or_else(sql_returning_projection_alignment_error))
        .collect()
}

// Build the shared invariant for owned SQL RETURNING row/column mismatches.
fn sql_returning_projection_alignment_error() -> QueryError {
    QueryError::invariant("SQL RETURNING projection row must align with declared columns")
}
