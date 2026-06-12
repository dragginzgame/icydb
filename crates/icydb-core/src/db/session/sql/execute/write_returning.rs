//! Module: db::session::sql::execute::write_returning
//! Responsibility: SQL write `RETURNING` and statement-result shaping.
//! Does not own: SQL write selection, mutation execution, or patch construction.
//! Boundary: converts already-mutated rows into the public SQL statement result shape.

use crate::{
    db::{
        PersistedRow, QueryError,
        schema::AcceptedRowLayoutRuntimeContract,
        session::sql::{
            SqlStatementResult,
            projection::{
                sql_projection_statement_result_from_fallible_value_rows,
                sql_projection_statement_result_from_value_rows,
            },
        },
        sql::parser::SqlReturningProjection,
    },
    error::InternalError,
    traits::EntityValue,
    value::{OutputValue, Value, render_output_value_text},
};
use candid::{CandidType, Encode};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

#[derive(CandidType)]
enum SqlReturningResponseSizeProbe {
    Projection(SqlReturningProjectionSizeProbe),
}

#[derive(CandidType)]
struct SqlReturningProjectionSizeProbe {
    entity: String,
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    row_count: u32,
}

struct SqlReturningRenderedProjectionRows {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    row_count: u32,
}

/// Shape one SQL INSERT/UPDATE mutation result after the write has already run.
///
/// This helper owns only the statement-result envelope conversion for rows that
/// were returned by the mutation path. It intentionally does not select rows,
/// build patches, or perform mutation execution.
pub(in crate::db::session::sql::execute) fn sql_write_statement_result<E>(
    entities: Vec<E>,
    returning: Option<&SqlReturningProjection>,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
) -> Result<SqlStatementResult, QueryError>
where
    E: PersistedRow + EntityValue,
{
    let row_count = u32::try_from(entities.len()).unwrap_or(u32::MAX);

    match returning {
        None => Ok(SqlStatementResult::Count { row_count }),
        Some(SqlReturningProjection::All) => {
            let all_columns = projection_labels_from_accepted_write_descriptor(descriptor);
            let all_field_count = descriptor.required_slot_count();

            sql_projection_statement_result_from_fallible_value_rows(
                all_columns,
                vec![None; all_field_count],
                entities
                    .into_iter()
                    .map(|entity| sql_returning_all_values(&entity, all_field_count)),
                row_count,
            )
        }
        Some(SqlReturningProjection::Fields(fields)) => {
            let all_columns = projection_labels_from_accepted_write_descriptor(descriptor);
            let indices = sql_returning_field_indices(&all_columns, fields)?;

            // Project field-list RETURNING rows directly from typed mutation
            // outputs. This avoids constructing full rows for blob-heavy
            // entities when callers return only a small subset of fields.
            sql_projection_statement_result_from_fallible_value_rows(
                fields.clone(),
                vec![None; fields.len()],
                entities
                    .into_iter()
                    .map(|entity| sql_returning_selected_values(&entity, indices.as_slice())),
                row_count,
            )
        }
    }
}

/// Derive canonical SQL `RETURNING *` labels from the accepted row descriptor.
///
/// The accepted descriptor is the statement-result shape authority for SQL
/// write paths. Generated model fields are still used by typed codecs after the
/// descriptor has been proven generated-compatible, but they do not choose the
/// outward all-column contract here.
pub(in crate::db::session::sql::execute) fn projection_labels_from_accepted_write_descriptor(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
) -> Vec<String> {
    descriptor
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

/// Validate one SQL write RETURNING projection before mutation execution.
pub(in crate::db::session::sql::execute) fn validate_sql_returning_projection_fields(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    returning: Option<&SqlReturningProjection>,
) -> Result<(), QueryError> {
    let Some(SqlReturningProjection::Fields(fields)) = returning else {
        return Ok(());
    };
    let columns = projection_labels_from_accepted_write_descriptor(descriptor);
    let indices = sql_returning_field_indices(columns.as_slice(), fields)?;

    sql_returning_field_selection(indices.as_slice()).map(|_| ())
}

/// Validate one public/generated SQL `UPDATE RETURNING` response-size budget
/// against the already-prepared mutation after-images.
///
/// This must run after structural mutation validation has produced sanitized
/// after-images but before the executor opens its commit window.
pub(in crate::db::session::sql::execute) fn validate_sql_returning_response_byte_cap<E>(
    entity_name: &str,
    entities: &[E],
    returning: Option<&SqlReturningProjection>,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    max_response_bytes: Option<u32>,
) -> Result<(), InternalError>
where
    E: EntityValue,
{
    let (Some(returning), Some(max_response_bytes)) = (returning, max_response_bytes) else {
        return Ok(());
    };

    let payload_len = encoded_sql_returning_projection_response_len(
        entity_name,
        entities,
        returning,
        descriptor,
    )?;
    let max_response_bytes = usize::try_from(max_response_bytes).unwrap_or(usize::MAX);
    if payload_len > max_response_bytes {
        return Err(InternalError::query_sql_write_boundary(
            SqlWriteBoundaryCode::ReturningResponseTooLarge,
        ));
    }

    Ok(())
}

// Materialize every field from one typed write result for `RETURNING *`.
fn sql_returning_all_values<E>(entity: &E, field_count: usize) -> Result<Vec<Value>, QueryError>
where
    E: EntityValue,
{
    let mut row = Vec::with_capacity(field_count);

    for index in 0..field_count {
        let value = entity
            .get_value_by_index(index)
            .ok_or_else(QueryError::invariant)?;
        row.push(value);
    }

    Ok(row)
}

// Project a typed write result into the caller-requested RETURNING field list
// without first materializing the full entity row.
fn sql_returning_selected_values<E>(entity: &E, indices: &[usize]) -> Result<Vec<Value>, QueryError>
where
    E: EntityValue,
{
    let mut row = Vec::with_capacity(indices.len());

    for index in indices {
        let value = entity
            .get_value_by_index(*index)
            .ok_or_else(QueryError::invariant)?;
        row.push(value);
    }

    Ok(row)
}

fn encoded_sql_returning_projection_response_len<E>(
    entity_name: &str,
    entities: &[E],
    returning: &SqlReturningProjection,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
) -> Result<usize, InternalError>
where
    E: EntityValue,
{
    let rendered = sql_returning_rendered_projection_rows(entities, returning, descriptor)?;
    let payload = SqlReturningResponseSizeProbe::Projection(SqlReturningProjectionSizeProbe {
        entity: entity_name.to_string(),
        columns: rendered.columns,
        rows: rendered.rows,
        row_count: rendered.row_count,
    });
    let encoded = Encode!(&payload).map_err(|_| InternalError::query_executor_invariant())?;

    Ok(encoded.len())
}

fn sql_returning_rendered_projection_rows<E>(
    entities: &[E],
    returning: &SqlReturningProjection,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
) -> Result<SqlReturningRenderedProjectionRows, InternalError>
where
    E: EntityValue,
{
    let row_count = u32::try_from(entities.len()).unwrap_or(u32::MAX);

    match returning {
        SqlReturningProjection::All => {
            let columns = projection_labels_from_accepted_write_descriptor(descriptor);
            let field_count = descriptor.required_slot_count();
            let rows = entities
                .iter()
                .map(|entity| {
                    sql_returning_all_values(entity, field_count)
                        .map(render_sql_returning_value_row)
                        .map_err(query_error_to_internal_invariant)
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(SqlReturningRenderedProjectionRows {
                columns,
                rows,
                row_count,
            })
        }
        SqlReturningProjection::Fields(fields) => {
            let all_columns = projection_labels_from_accepted_write_descriptor(descriptor);
            let indices = sql_returning_field_indices(&all_columns, fields)
                .map_err(query_error_to_internal_invariant)?;
            let rows = entities
                .iter()
                .map(|entity| {
                    sql_returning_selected_values(entity, indices.as_slice())
                        .map(render_sql_returning_value_row)
                        .map_err(query_error_to_internal_invariant)
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(SqlReturningRenderedProjectionRows {
                columns: fields.clone(),
                rows,
                row_count,
            })
        }
    }
}

fn render_sql_returning_value_row(row: Vec<Value>) -> Vec<String> {
    row.into_iter()
        .map(|value| render_output_value_text(&OutputValue::from(value)))
        .collect()
}

fn query_error_to_internal_invariant(_err: QueryError) -> InternalError {
    InternalError::query_executor_invariant()
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
                QueryError::sql_write_boundary(SqlWriteBoundaryCode::UnknownReturningField)
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
        SqlReturningProjection::All => Ok(sql_projection_statement_result_from_value_rows(
            columns,
            vec![None; rows.first().map_or(0, Vec::len)],
            rows,
            row_count,
        )),
        SqlReturningProjection::Fields(fields) => {
            let indices = sql_returning_field_indices(&columns, fields)?;
            let selection = sql_returning_field_selection(&indices)?;
            let output_ordered = sql_returning_selection_is_output_ordered(selection.as_slice());

            sql_projection_statement_result_from_fallible_value_rows(
                fields.clone(),
                vec![None; fields.len()],
                rows.into_iter().map(|row| {
                    sql_returning_project_owned_row_for_selection(
                        row,
                        selection.as_slice(),
                        output_ordered,
                    )
                }),
                row_count,
            )
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
            return Err(QueryError::sql_write_boundary(
                SqlWriteBoundaryCode::DuplicateReturningField,
            ));
        }
        selection.push((input_index, output_index));
    }
    selection.sort_unstable_by_key(|(input_index, _)| *input_index);

    Ok(selection)
}

// Detect the common `RETURNING a, b, c` case where caller-requested output
// order matches input-column order. That lane can move selected values directly
// into the result row without routing them through an optional output buffer.
fn sql_returning_selection_is_output_ordered(selection: &[(usize, usize)]) -> bool {
    selection
        .iter()
        .enumerate()
        .all(|(output_index, (_, selected_output_index))| *selected_output_index == output_index)
}

// Dispatch one owned projection row through the selected row-move lane. Keeping
// this as a row-level helper lets callers stream projection into final response
// encoding instead of staging a second page of projected `Value` rows.
fn sql_returning_project_owned_row_for_selection(
    row: Vec<Value>,
    selection: &[(usize, usize)],
    output_ordered: bool,
) -> Result<Vec<Value>, QueryError> {
    if output_ordered {
        sql_returning_project_owned_row_in_output_order(row, selection)
    } else {
        sql_returning_project_owned_row(row, selection)
    }
}

// Project one owned SQL RETURNING row by moving schema-ordered selected values
// directly into output order. This fast lane is only used after duplicate
// fields have been rejected and the selection is known to be output ordered.
fn sql_returning_project_owned_row_in_output_order(
    row: Vec<Value>,
    selection: &[(usize, usize)],
) -> Result<Vec<Value>, QueryError> {
    let mut projected = Vec::with_capacity(selection.len());
    let mut selection_position = 0usize;

    // Phase 1: consume the row once and move each selected input directly into
    // the caller-visible output row.
    for (input_index, value) in row.into_iter().enumerate() {
        if selection_position >= selection.len() {
            break;
        }
        let (selected_input_index, output_index) = selection[selection_position];
        debug_assert_eq!(selection_position, output_index);
        if input_index == selected_input_index {
            projected.push(value);
            selection_position = selection_position.saturating_add(1);
        }
    }

    if selection_position != selection.len() {
        return Err(sql_returning_projection_alignment_error());
    }

    Ok(projected)
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
    QueryError::invariant()
}
