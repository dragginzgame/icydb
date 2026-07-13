//! Module: db::session::sql::execute::write_returning::bounds
//! Responsibility: SQL write `RETURNING` row-count and response-byte budget enforcement.
//! Does not own: mutation execution or public SQL statement-result projection.
//! Boundary: validates prepared mutation after-images before commit or response shaping.

use crate::{
    db::{
        schema::{
            AcceptedEnumCatalog, AcceptedEnumCatalogHandle, AcceptedRowLayoutRuntimeContract,
            authored_projection::AcceptedAuthoredFieldProjection,
        },
        session::sql::write_policy::SqlWriteReturningBounds,
        sql::parser::SqlReturningProjection,
    },
    error::InternalError,
    traits::{AuthoredFieldProjection, EntityValue},
    value::{OutputValue, Value},
};
use candid::{CandidType, Encode};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

use super::projection::{
    SqlReturningFieldProjection, SqlReturningProjectionRows,
    projection_labels_from_accepted_write_descriptor, query_error_to_internal_invariant,
    sql_materialized_returning_projection_rows, sql_returning_all_values,
    sql_returning_output_value_row, sql_returning_projection_rows,
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

/// Validate one SQL write `RETURNING` row and response budget against the
/// already-prepared mutation after-images.
///
/// This must run after structural mutation validation has produced sanitized
/// after-images but before the executor opens its commit window.
pub(in crate::db::session::sql::execute) fn validate_sql_returning_bounds<E>(
    entity_name: &str,
    entities: &[E],
    returning: Option<&SqlReturningProjection>,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    enum_catalog: &AcceptedEnumCatalogHandle,
    bounds: Option<SqlWriteReturningBounds>,
) -> Result<(), InternalError>
where
    E: EntityValue + AuthoredFieldProjection,
{
    let Some(returning) = returning else {
        return Ok(());
    };
    let Some(bounds) = bounds else {
        return Ok(());
    };

    validate_sql_returning_row_count(entities.len(), bounds.max_rows)?;

    if let Some(max_response_bytes) = bounds.max_response_bytes {
        let max_response_bytes = usize::try_from(max_response_bytes).unwrap_or(usize::MAX);
        if encoded_sql_returning_projection_response_len_exceeds_max(
            entity_name,
            entities,
            returning,
            descriptor,
            enum_catalog,
            max_response_bytes,
        )? {
            return Err(sql_returning_response_too_large_error());
        }

        let payload_len = encoded_sql_returning_projection_response_len(
            entity_name,
            entities,
            returning,
            descriptor,
            enum_catalog,
        )?;
        if payload_len > max_response_bytes {
            return Err(sql_returning_response_too_large_error());
        }
    }

    Ok(())
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
        if encoded_sql_materialized_returning_projection_response_len_exceeds_max(
            entity_name,
            columns,
            rows,
            row_count,
            returning,
            enum_catalog,
            max_response_bytes,
        )? {
            return Err(sql_returning_response_too_large_error());
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
            return Err(sql_returning_response_too_large_error());
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

    Err(InternalError::query_sql_write_boundary(
        SqlWriteBoundaryCode::ReturningRowsTooMany,
    ))
}

fn sql_returning_response_too_large_error() -> InternalError {
    InternalError::query_sql_write_boundary(SqlWriteBoundaryCode::ReturningResponseTooLarge)
}

fn encoded_sql_returning_projection_response_len<E>(
    entity_name: &str,
    entities: &[E],
    returning: &SqlReturningProjection,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    enum_catalog: &AcceptedEnumCatalogHandle,
) -> Result<usize, InternalError>
where
    E: AuthoredFieldProjection,
{
    let projected = sql_returning_projection_rows(entities, returning, descriptor, enum_catalog)?;
    encoded_sql_returning_projection_payload_len(entity_name, projected)
}

fn encoded_sql_returning_projection_response_len_exceeds_max<E>(
    entity_name: &str,
    entities: &[E],
    returning: &SqlReturningProjection,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    enum_catalog: &AcceptedEnumCatalogHandle,
    max_response_bytes: usize,
) -> Result<bool, InternalError>
where
    E: AuthoredFieldProjection,
{
    let row_count = u32::try_from(entities.len()).unwrap_or(u32::MAX);
    let row_contract = descriptor.row_decode_contract(enum_catalog.clone());
    let accepted = AcceptedAuthoredFieldProjection::new(&row_contract);

    match returning {
        SqlReturningProjection::All => {
            let columns = projection_labels_from_accepted_write_descriptor(descriptor);
            let field_count = descriptor.required_slot_count();
            let base_len = encoded_empty_sql_returning_projection_payload_len(
                entity_name,
                columns,
                row_count,
            )?;

            encoded_sql_returning_rows_len_exceeds_max(
                base_len,
                max_response_bytes,
                entities.iter().map(|entity| {
                    sql_returning_all_values(&accepted, entity, field_count)
                        .and_then(|row| sql_returning_output_value_row(enum_catalog.catalog(), row))
                        .map_err(query_error_to_internal_invariant)
                }),
            )
        }
        SqlReturningProjection::Fields(fields) => {
            let all_columns = projection_labels_from_accepted_write_descriptor(descriptor);
            let projection = SqlReturningFieldProjection::from_fields(&all_columns, fields)
                .map_err(query_error_to_internal_invariant)?;
            let base_len = encoded_empty_sql_returning_projection_payload_len(
                entity_name,
                projection.output_columns(),
                row_count,
            )?;

            encoded_sql_returning_rows_len_exceeds_max(
                base_len,
                max_response_bytes,
                entities.iter().map(|entity| {
                    projection
                        .project_entity(&accepted, entity)
                        .and_then(|row| sql_returning_output_value_row(enum_catalog.catalog(), row))
                        .map_err(query_error_to_internal_invariant)
                }),
            )
        }
    }
}

fn encoded_sql_materialized_returning_projection_response_len_exceeds_max(
    entity_name: &str,
    columns: &[String],
    rows: &[Vec<Value>],
    row_count: u32,
    returning: &SqlReturningProjection,
    enum_catalog: &AcceptedEnumCatalog,
    max_response_bytes: usize,
) -> Result<bool, InternalError> {
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
) -> Result<bool, InternalError> {
    if estimated_payload_len > max_response_bytes {
        return Ok(true);
    }

    for row in rows {
        let row = row?;
        let row_len = Encode!(&row)
            .map_err(|_| InternalError::query_executor_invariant())?
            .len();
        estimated_payload_len = estimated_payload_len.saturating_add(row_len);
        if estimated_payload_len > max_response_bytes {
            return Ok(true);
        }
    }

    Ok(false)
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
