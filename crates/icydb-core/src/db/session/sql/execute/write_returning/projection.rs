//! Module: db::session::sql::execute::write_returning::projection
//! Responsibility: SQL write `RETURNING` projection selection and statement-result shaping.
//! Does not own: mutation execution or response-size budget enforcement.
//! Boundary: maps accepted write-row layouts and returned rows into SQL statement result projections.

use crate::{
    db::{
        PersistedRow, QueryError,
        schema::{
            AcceptedEnumCatalog, AcceptedRowLayoutRuntimeContract, AcceptedValueCatalogHandle,
            authored_projection::AcceptedAuthoredFieldProjection,
            enum_catalog::ValueAdmissionBudget, output_value_from_runtime,
        },
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
    traits::AuthoredFieldProjection,
    value::{OutputValue, Value},
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

pub(super) struct SqlReturningProjectionRows {
    pub(super) columns: Vec<String>,
    pub(super) rows: Vec<Vec<OutputValue>>,
    pub(super) row_count: u32,
}

pub(super) struct SqlReturningFieldProjection {
    output_columns: Vec<String>,
    selection: Vec<(usize, usize)>,
    output_ordered: bool,
}

impl SqlReturningFieldProjection {
    pub(super) fn from_fields(
        input_columns: &[String],
        fields: &[String],
    ) -> Result<Self, QueryError> {
        let indices = sql_returning_field_indices(input_columns, fields)?;
        let selection = sql_returning_field_selection(&indices)?;
        let output_ordered = sql_returning_selection_is_output_ordered(selection.as_slice());

        Ok(Self {
            output_columns: fields.to_vec(),
            selection,
            output_ordered,
        })
    }

    pub(super) fn output_columns(&self) -> Vec<String> {
        self.output_columns.clone()
    }

    fn output_fixed_scales(&self) -> Vec<Option<u32>> {
        vec![None; self.output_columns.len()]
    }

    pub(super) fn project_entity<E>(
        &self,
        accepted: &AcceptedAuthoredFieldProjection<'_>,
        entity: &E,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: AuthoredFieldProjection,
    {
        let mut projected = (0..self.output_columns.len())
            .map(|_| None)
            .collect::<Vec<_>>();
        let mut budget = ValueAdmissionBudget::standard();

        for (input_index, output_index) in &self.selection {
            let admitted = accepted
                .admit_field(entity, *input_index, &mut budget)
                .map_err(|_| QueryError::invariant())?;
            projected[*output_index] = Some(admitted.value().clone());
        }

        projected
            .into_iter()
            .map(|value| value.ok_or_else(sql_returning_projection_alignment_error))
            .collect()
    }

    pub(super) fn project_owned_row(&self, row: Vec<Value>) -> Result<Vec<Value>, QueryError> {
        sql_returning_project_owned_row_for_selection(
            row,
            self.selection.as_slice(),
            self.output_ordered,
        )
    }
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
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<SqlStatementResult, QueryError>
where
    E: PersistedRow,
{
    let row_count = u32::try_from(entities.len()).unwrap_or(u32::MAX);
    let row_contract = descriptor.row_decode_contract(value_catalog.clone());
    let accepted = AcceptedAuthoredFieldProjection::new(&row_contract);

    match returning {
        None => Ok(SqlStatementResult::Count { row_count }),
        Some(SqlReturningProjection::All) => {
            let all_columns = projection_labels_from_accepted_write_descriptor(descriptor);
            let all_field_count = descriptor.required_slot_count();

            sql_projection_statement_result_from_fallible_value_rows(
                value_catalog.enum_catalog(),
                all_columns,
                vec![None; all_field_count],
                entities
                    .into_iter()
                    .map(|entity| sql_returning_all_values(&accepted, &entity, all_field_count)),
                row_count,
            )
        }
        Some(SqlReturningProjection::Fields(fields)) => {
            let all_columns = projection_labels_from_accepted_write_descriptor(descriptor);
            let projection = SqlReturningFieldProjection::from_fields(&all_columns, fields)?;
            let output_columns = projection.output_columns();
            let fixed_scales = projection.output_fixed_scales();

            // Project field-list RETURNING rows directly from typed mutation
            // outputs. This avoids constructing full rows for blob-heavy
            // entities when callers return only a small subset of fields.
            sql_projection_statement_result_from_fallible_value_rows(
                value_catalog.enum_catalog(),
                output_columns,
                fixed_scales,
                entities
                    .into_iter()
                    .map(|entity| projection.project_entity(&accepted, &entity)),
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
    SqlReturningFieldProjection::from_fields(columns.as_slice(), fields).map(|_| ())
}

// Materialize every field from one typed write result for `RETURNING *`.
pub(super) fn sql_returning_all_values<E>(
    accepted: &AcceptedAuthoredFieldProjection<'_>,
    entity: &E,
    field_count: usize,
) -> Result<Vec<Value>, QueryError>
where
    E: AuthoredFieldProjection,
{
    let mut row = Vec::with_capacity(field_count);
    let mut budget = ValueAdmissionBudget::standard();

    for index in 0..field_count {
        let admitted = accepted
            .admit_field(entity, index, &mut budget)
            .map_err(|_| QueryError::invariant())?;
        row.push(admitted.value().clone());
    }

    Ok(row)
}

pub(super) fn sql_returning_projection_rows<E>(
    entities: &[E],
    returning: &SqlReturningProjection,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<SqlReturningProjectionRows, InternalError>
where
    E: AuthoredFieldProjection,
{
    let row_count = u32::try_from(entities.len()).unwrap_or(u32::MAX);
    let row_contract = descriptor.row_decode_contract(value_catalog.clone());
    let accepted = AcceptedAuthoredFieldProjection::new(&row_contract);

    match returning {
        SqlReturningProjection::All => {
            let columns = projection_labels_from_accepted_write_descriptor(descriptor);
            let field_count = descriptor.required_slot_count();
            let rows = entities
                .iter()
                .map(|entity| {
                    sql_returning_all_values(&accepted, entity, field_count)
                        .and_then(|row| {
                            sql_returning_output_value_row(value_catalog.enum_catalog(), row)
                        })
                        .map_err(query_error_to_internal_invariant)
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(SqlReturningProjectionRows {
                columns,
                rows,
                row_count,
            })
        }
        SqlReturningProjection::Fields(fields) => {
            let all_columns = projection_labels_from_accepted_write_descriptor(descriptor);
            let projection = SqlReturningFieldProjection::from_fields(&all_columns, fields)
                .map_err(query_error_to_internal_invariant)?;
            let rows = entities
                .iter()
                .map(|entity| {
                    projection
                        .project_entity(&accepted, entity)
                        .and_then(|row| {
                            sql_returning_output_value_row(value_catalog.enum_catalog(), row)
                        })
                        .map_err(query_error_to_internal_invariant)
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(SqlReturningProjectionRows {
                columns: projection.output_columns(),
                rows,
                row_count,
            })
        }
    }
}

pub(super) fn sql_returning_output_value_row(
    enum_catalog: &AcceptedEnumCatalog,
    row: Vec<Value>,
) -> Result<Vec<OutputValue>, QueryError> {
    row.iter()
        .map(|value| {
            output_value_from_runtime(enum_catalog, value).map_err(|_error| QueryError::invariant())
        })
        .collect()
}

pub(super) fn sql_materialized_returning_projection_rows(
    enum_catalog: &AcceptedEnumCatalog,
    columns: &[String],
    rows: &[Vec<Value>],
    row_count: u32,
    returning: &SqlReturningProjection,
) -> Result<SqlReturningProjectionRows, InternalError> {
    match returning {
        SqlReturningProjection::All => Ok(SqlReturningProjectionRows {
            columns: columns.to_vec(),
            rows: rows
                .iter()
                .cloned()
                .map(|row| sql_returning_output_value_row(enum_catalog, row))
                .collect::<Result<Vec<_>, _>>()
                .map_err(query_error_to_internal_invariant)?,
            row_count,
        }),
        SqlReturningProjection::Fields(fields) => {
            let projection = SqlReturningFieldProjection::from_fields(columns, fields)
                .map_err(query_error_to_internal_invariant)?;
            let rows = rows
                .iter()
                .cloned()
                .map(|row| {
                    projection
                        .project_owned_row(row)
                        .and_then(|row| sql_returning_output_value_row(enum_catalog, row))
                        .map_err(query_error_to_internal_invariant)
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(SqlReturningProjectionRows {
                columns: projection.output_columns(),
                rows,
                row_count,
            })
        }
    }
}

pub(super) fn query_error_to_internal_invariant(_err: QueryError) -> InternalError {
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
    enum_catalog: &AcceptedEnumCatalog,
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    row_count: u32,
    returning: &SqlReturningProjection,
) -> Result<SqlStatementResult, QueryError> {
    match returning {
        SqlReturningProjection::All => sql_projection_statement_result_from_value_rows(
            enum_catalog,
            columns,
            vec![None; rows.first().map_or(0, Vec::len)],
            rows,
            row_count,
        ),
        SqlReturningProjection::Fields(fields) => {
            let projection = SqlReturningFieldProjection::from_fields(&columns, fields)?;
            let output_columns = projection.output_columns();
            let fixed_scales = projection.output_fixed_scales();

            sql_projection_statement_result_from_fallible_value_rows(
                enum_catalog,
                output_columns,
                fixed_scales,
                rows.into_iter()
                    .map(|row| projection.project_owned_row(row)),
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
