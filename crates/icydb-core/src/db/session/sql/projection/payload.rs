//! Module: db::session::sql::projection::payload
//! Responsibility: adapt structural projection payloads and runtime value
//! rows into outward SQL statement results.
//! Does not own: projection execution, labeling, or textual rendering policy.
//! Boundary: SQL response shaping begins only after the query-owned structural
//! payload boundary.

use crate::{
    db::{
        QueryError,
        schema::{AcceptedEnumCatalog, output_value_from_runtime},
        session::{query::StructuralProjectionPayload, sql::SqlStatementResult},
    },
    value::{OutputValue, Value},
};

/// Adapt one query-owned structural projection payload into the public SQL
/// statement envelope.
pub(in crate::db::session::sql) fn sql_statement_result_from_structural_projection_payload(
    payload: StructuralProjectionPayload,
) -> Result<SqlStatementResult, QueryError> {
    let (columns, fixed_scales, rows, row_count) = payload.into_output_components()?;

    Ok(SqlStatementResult::Projection {
        columns,
        fixed_scales,
        rows,
        row_count,
    })
}

/// Convert already-projected value rows into the public SQL statement shape.
///
/// This is the final SQL response boundary for lanes that can project and
/// encode one row at a time without first constructing another value-row page.
pub(in crate::db::session::sql) fn sql_projection_statement_result_from_value_rows(
    catalog: &AcceptedEnumCatalog,
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
    rows: impl IntoIterator<Item = Vec<Value>>,
    row_count: u32,
) -> Result<SqlStatementResult, QueryError> {
    Ok(SqlStatementResult::Projection {
        columns,
        fixed_scales,
        rows: rows
            .into_iter()
            .map(|row| sql_output_row_from_value_row(catalog, row))
            .collect::<Result<Vec<_>, _>>()?,
        row_count,
    })
}

/// Convert fallibly projected value rows into the public SQL statement shape.
///
/// Write `RETURNING` callers use this to fuse row projection with output-value
/// encoding while preserving the first projection error exactly.
pub(in crate::db::session::sql) fn sql_projection_statement_result_from_fallible_value_rows(
    catalog: &AcceptedEnumCatalog,
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
    rows: impl IntoIterator<Item = Result<Vec<Value>, QueryError>>,
    row_count: u32,
) -> Result<SqlStatementResult, QueryError> {
    let rows = rows
        .into_iter()
        .map(|row| row.and_then(|row| sql_output_row_from_value_row(catalog, row)))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(SqlStatementResult::Projection {
        columns,
        fixed_scales,
        rows,
        row_count,
    })
}

// Move one structural SQL value row into the outward value representation used
// by shared SQL statement results.
fn sql_output_row_from_value_row(
    catalog: &AcceptedEnumCatalog,
    row: Vec<Value>,
) -> Result<Vec<OutputValue>, QueryError> {
    row.iter()
        .map(|value| {
            output_value_from_runtime(catalog, value).map_err(|_error| QueryError::invariant())
        })
        .collect()
}
