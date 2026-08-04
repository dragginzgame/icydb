//! Module: db::session::sql::execute::write_returning::bounds
//! Responsibility: SQL write `RETURNING` row-count and response-byte budget enforcement.
//! Does not own: mutation execution or public SQL statement-result projection.
//! Boundary: validates prepared mutation after-images before commit or response shaping.

use crate::{
    db::{
        schema::AcceptedEnumCatalog, session::sql::write_policy::SqlWriteReturningBounds,
        sql::parser::SqlReturningProjection,
    },
    error::InternalError,
    value::{OutputValue, Value},
};
use candid::{CandidType, Encode};
use icydb_diagnostic_code::{DiagnosticFactTag, SqlWriteBoundaryCode};

use super::projection::{
    SqlReturningFieldProjection, SqlReturningProjectionRows, query_error_to_internal_invariant,
    sql_materialized_returning_projection_rows, sql_returning_output_value_row,
};

#[derive(CandidType)]
enum SqlReturningResponseSizeProbe {
    Projection(SqlReturningProjectionSizeProbe),
}

#[derive(CandidType)]
struct SqlReturningProjectionSizeProbe {
    entity: String,
    columns: Vec<String>,
    rows: Vec<Vec<OutputValue>>,
    row_count: u32,
}

/// Validate SQL write `RETURNING` bounds for rows that are already materialized
/// in accepted-schema column order.
pub(in crate::db::session::sql::execute) fn validate_sql_materialized_returning_bounds(
    entity_name: &str,
    columns: &[String],
    rows: &[Vec<Value>],
    row_count: u32,
    returning: &SqlReturningProjection,
    enum_catalog: &AcceptedEnumCatalog,
    bounds: Option<SqlWriteReturningBounds>,
) -> Result<(), InternalError> {
    let Some(bounds) = bounds else {
        return Ok(());
    };

    validate_sql_returning_row_count(
        usize::try_from(row_count).unwrap_or(usize::MAX),
        bounds.max_rows,
    )?;

    if let Some(max_response_bytes) = bounds.max_response_bytes {
        let max_response_bytes = usize::try_from(max_response_bytes).unwrap_or(usize::MAX);
        if let SqlReturningLengthCheck::Exceeded(actual_length) =
            encoded_sql_materialized_returning_projection_response_len_check(
                entity_name,
                columns,
                rows,
                row_count,
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

        let projected = sql_materialized_returning_projection_rows(
            enum_catalog,
            columns,
            rows,
            row_count,
            returning,
        )?;
        let payload_len = encoded_sql_returning_projection_payload_len(entity_name, projected)?;
        if payload_len > max_response_bytes {
            return Err(sql_returning_response_too_large_error(
                Some(payload_len),
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
    entity_name: &str,
    columns: &[String],
    rows: &[Vec<Value>],
    row_count: u32,
    returning: &SqlReturningProjection,
    enum_catalog: &AcceptedEnumCatalog,
    max_response_bytes: usize,
) -> Result<SqlReturningLengthCheck, InternalError> {
    match returning {
        SqlReturningProjection::All => {
            let base_len = encoded_empty_sql_returning_projection_payload_len(
                entity_name,
                columns.to_vec(),
                row_count,
            )?;

            encoded_sql_returning_rows_len_exceeds_max(
                base_len,
                max_response_bytes,
                rows.iter().map(|row| {
                    sql_returning_output_value_row(enum_catalog, row.clone())
                        .map_err(query_error_to_internal_invariant)
                }),
            )
        }
        SqlReturningProjection::Fields(fields) => {
            let projection = SqlReturningFieldProjection::from_fields(columns, fields)
                .map_err(query_error_to_internal_invariant)?;
            let base_len = encoded_empty_sql_returning_projection_payload_len(
                entity_name,
                projection.output_columns(),
                row_count,
            )?;

            encoded_sql_returning_rows_len_exceeds_max(
                base_len,
                max_response_bytes,
                rows.iter().cloned().map(|row| {
                    projection
                        .project_owned_row(row)
                        .and_then(|row| sql_returning_output_value_row(enum_catalog, row))
                        .map_err(query_error_to_internal_invariant)
                }),
            )
        }
    }
}

fn encoded_empty_sql_returning_projection_payload_len(
    entity_name: &str,
    columns: Vec<String>,
    row_count: u32,
) -> Result<usize, InternalError> {
    encoded_sql_returning_projection_payload_len(
        entity_name,
        SqlReturningProjectionRows {
            columns,
            rows: Vec::new(),
            row_count,
        },
    )
}

fn encoded_sql_returning_rows_len_exceeds_max(
    mut estimated_payload_len: usize,
    max_response_bytes: usize,
    rows: impl Iterator<Item = Result<Vec<OutputValue>, InternalError>>,
) -> Result<SqlReturningLengthCheck, InternalError> {
    if estimated_payload_len > max_response_bytes {
        return Ok(SqlReturningLengthCheck::Exceeded(Some(
            estimated_payload_len,
        )));
    }

    for row in rows {
        let row = row?;
        let row_len = Encode!(&row)
            .map_err(|_| InternalError::query_executor_invariant())?
            .len();
        let Some(next_payload_len) = estimated_payload_len.checked_add(row_len) else {
            return Ok(SqlReturningLengthCheck::Exceeded(None));
        };
        estimated_payload_len = next_payload_len;
        if estimated_payload_len > max_response_bytes {
            return Ok(SqlReturningLengthCheck::Exceeded(Some(
                estimated_payload_len,
            )));
        }
    }

    Ok(SqlReturningLengthCheck::WithinLimit)
}

fn encoded_sql_returning_projection_payload_len(
    entity_name: &str,
    projected: SqlReturningProjectionRows,
) -> Result<usize, InternalError> {
    let payload = SqlReturningResponseSizeProbe::Projection(SqlReturningProjectionSizeProbe {
        entity: entity_name.to_string(),
        columns: projected.columns,
        rows: projected.rows,
        row_count: projected.row_count,
    });
    let encoded = Encode!(&payload).map_err(|_| InternalError::query_executor_invariant())?;

    Ok(encoded.len())
}

#[cfg(test)]
mod tests {
    use super::{sql_returning_response_too_large_error, validate_sql_returning_row_count};
    use icydb_diagnostic_code::DiagnosticFactTag;

    #[test]
    fn returning_row_limit_error_retains_actual_count_and_limit() {
        let error = validate_sql_returning_row_count(3, Some(2))
            .expect_err("row count above returning limit should reject");

        assert_eq!(
            error.diagnostic_facts(),
            vec![
                (DiagnosticFactTag::ActualCount, 3),
                (DiagnosticFactTag::Limit, 2),
            ],
        );
    }

    #[test]
    fn returning_byte_limit_error_retains_exact_length_and_limit() {
        let error = sql_returning_response_too_large_error(Some(17), 16);

        assert_eq!(
            error.diagnostic_facts(),
            vec![
                (DiagnosticFactTag::ActualLength, 17),
                (DiagnosticFactTag::Limit, 16),
            ],
        );
    }
}
