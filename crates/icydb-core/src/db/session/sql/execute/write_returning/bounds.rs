//! Module: db::session::sql::execute::write_returning::bounds
//! Responsibility: SQL write `RETURNING` row-count and response-byte budget enforcement.
//! Does not own: mutation execution or public SQL statement-result projection.
//! Boundary: validates prepared mutation after-images before commit or response shaping.

use crate::{
    db::{
        RowProjectionOutput,
        schema::AcceptedEnumCatalog,
        session::{response::output_row_from_runtime, sql::write_policy::SqlWriteReturningBounds},
        sql::parser::SqlReturningProjection,
    },
    error::InternalError,
    value::{OutputValue, Value},
};

use icydb_diagnostic_code::{DiagnosticFactTag, SqlWriteBoundaryCode};

use super::projection::{SqlReturningFieldProjection, query_error_to_internal_invariant};

// Account for each row payload once; the facade supplies the current endpoint
// envelope framing. Postcard owns both payload and count-prefix representation.
fn encoded_payload_value_len(value: &impl serde::Serialize) -> Result<usize, InternalError> {
    icydb_schema::wire::encoded_len(value).map_err(|_| InternalError::query_executor_invariant())
}

fn encoded_payload_vec_prefix_len(count: usize) -> Result<usize, InternalError> {
    let count = u64::try_from(count).map_err(|_| InternalError::query_executor_invariant())?;
    icydb_schema::wire::encoded_len(&count).map_err(|_| InternalError::query_executor_invariant())
}

/// Validate SQL write `RETURNING` bounds for rows that are already materialized
/// in accepted-schema column order.
pub(in crate::db::session::sql::execute) fn validate_sql_materialized_returning_bounds(
    response_len: fn(RowProjectionOutput) -> Result<usize, icydb_schema::wire::Error>,
    entity_name: &str,
    columns: &[String],
    rows: &[Vec<Value>],
    returning: &SqlReturningProjection,
    enum_catalog: &AcceptedEnumCatalog,
    bounds: Option<SqlWriteReturningBounds>,
) -> Result<(), InternalError> {
    let Some(bounds) = bounds else {
        return Ok(());
    };

    validate_sql_returning_row_count(rows.len(), bounds.max_rows)?;

    if let Some(max_response_bytes) = bounds.max_response_bytes {
        let max_response_bytes = usize::try_from(max_response_bytes).unwrap_or(usize::MAX);
        if let SqlReturningLengthCheck::Exceeded(actual_length) =
            encoded_sql_materialized_returning_projection_response_len_check(
                response_len,
                entity_name,
                columns,
                rows,
                returning,
                enum_catalog,
                max_response_bytes,
            )?
        {
            return Err(sql_returning_response_too_large_error(
                actual_length,
                max_response_bytes,
            ));
        }
    }

    Ok(())
}

fn validate_sql_returning_row_count(
    row_count: usize,
    max_rows: Option<u32>,
) -> Result<(), InternalError> {
    let Some(max_rows) = max_rows else {
        return Ok(());
    };
    let max_rows = usize::try_from(max_rows).unwrap_or(usize::MAX);
    if row_count <= max_rows {
        return Ok(());
    }

    Err(InternalError::query_sql_write_boundary_with_facts(
        SqlWriteBoundaryCode::ReturningRowsTooMany,
        vec![
            (DiagnosticFactTag::ActualCount, row_count as u64),
            (DiagnosticFactTag::Limit, max_rows as u64),
        ],
    ))
}

fn sql_returning_response_too_large_error(
    actual_length: Option<usize>,
    max_response_bytes: usize,
) -> InternalError {
    let mut facts = Vec::with_capacity(2);
    if let Some(actual_length) = actual_length {
        facts.push((DiagnosticFactTag::ActualLength, actual_length as u64));
    }
    facts.push((DiagnosticFactTag::Limit, max_response_bytes as u64));
    InternalError::query_sql_write_boundary_with_facts(
        SqlWriteBoundaryCode::ReturningResponseTooLarge,
        facts,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlReturningLengthCheck {
    WithinLimit,
    Exceeded(Option<usize>),
}

fn encoded_sql_materialized_returning_projection_response_len_check(
    response_len: fn(RowProjectionOutput) -> Result<usize, icydb_schema::wire::Error>,
    entity_name: &str,
    columns: &[String],
    rows: &[Vec<Value>],
    returning: &SqlReturningProjection,
    enum_catalog: &AcceptedEnumCatalog,
    max_response_bytes: usize,
) -> Result<SqlReturningLengthCheck, InternalError> {
    let projection = match returning {
        SqlReturningProjection::All => None,
        SqlReturningProjection::Fields(fields) => Some(
            SqlReturningFieldProjection::from_fields(columns, fields)
                .map_err(query_error_to_internal_invariant)?,
        ),
    };
    let columns = projection.as_ref().map_or_else(
        || columns.to_vec(),
        SqlReturningFieldProjection::output_columns,
    );
    let mut output = RowProjectionOutput {
        entity: entity_name.to_string(),
        columns,
        rows: Vec::new(),
        row_count: u32::try_from(rows.len()).unwrap_or(u32::MAX),
    };
    let empty_len =
        response_len(output.clone()).map_err(|_| InternalError::query_executor_invariant())?;
    let prefix_len = encoded_payload_vec_prefix_len(rows.len())?;
    let empty_prefix_len = encoded_payload_vec_prefix_len(0)?;
    let Some(base_len) = empty_len
        .checked_sub(empty_prefix_len)
        .and_then(|len| len.checked_add(prefix_len))
    else {
        return Ok(SqlReturningLengthCheck::Exceeded(None));
    };
    let project_row = |row: &[Value]| match &projection {
        None => output_row_from_runtime(enum_catalog, row.to_vec())
            .map_err(query_error_to_internal_invariant),
        Some(projection) => projection
            .project_borrowed_row(row)
            .and_then(|row| output_row_from_runtime(enum_catalog, row))
            .map_err(query_error_to_internal_invariant),
    };
    let length_check = encoded_sql_returning_rows_len_exceeds_max(
        base_len,
        max_response_bytes,
        rows.iter().map(|row| project_row(row)),
        &mut output.rows,
    )?;
    if !matches!(length_check, SqlReturningLengthCheck::WithinLimit) {
        return Ok(length_check);
    }

    // Reuse the rows retained by the byte check. One final size-only traversal
    // must still admit the aggregate node/heap/depth cost of the actual facade
    // envelope before mutation commit, not per row.
    let full_len = response_len(output).map_err(|_| InternalError::query_executor_invariant())?;
    Ok(if full_len > max_response_bytes {
        SqlReturningLengthCheck::Exceeded(Some(full_len))
    } else {
        SqlReturningLengthCheck::WithinLimit
    })
}

fn encoded_sql_returning_rows_len_exceeds_max(
    mut response_len: usize,
    max_response_bytes: usize,
    rows: impl ExactSizeIterator<Item = Result<Vec<OutputValue>, InternalError>>,
    admitted_rows: &mut Vec<Vec<OutputValue>>,
) -> Result<SqlReturningLengthCheck, InternalError> {
    let row_count = rows.len();
    if response_len > max_response_bytes {
        return Ok(SqlReturningLengthCheck::Exceeded(
            (row_count == 0).then_some(response_len),
        ));
    }
    for (index, row) in rows.enumerate() {
        let row = row?;
        let row_len = encoded_payload_value_len(&row)?;
        let Some(next_len) = response_len.checked_add(row_len) else {
            return Ok(SqlReturningLengthCheck::Exceeded(None));
        };
        response_len = next_len;
        if response_len > max_response_bytes {
            // A prefix proves refusal but is not the full reply's ActualLength.
            return Ok(SqlReturningLengthCheck::Exceeded(
                (index + 1 == row_count).then_some(response_len),
            ));
        }
        // Retain only rows already inside the byte budget, avoiding a second
        // projection while keeping early refusal bounded by the admitted prefix.
        admitted_rows.push(row);
    }
    Ok(SqlReturningLengthCheck::WithinLimit)
}

#[cfg(test)]
mod tests;
