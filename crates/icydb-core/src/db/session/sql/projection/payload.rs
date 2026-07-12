//! Module: db::session::sql::projection::payload
//! Responsibility: own the outward SQL projection payload types returned by
//! session SQL statement surfaces.
//! Does not own: projection execution, labeling, or textual rendering policy.
//! Boundary: keeps SQL projection DTOs stable and separate from executor internals.

use crate::{
    db::{
        QueryError,
        schema::{AcceptedEnumCatalog, AcceptedEnumCatalogHandle, output_value_from_runtime},
        session::sql::SqlStatementResult,
    },
    value::{OutputValue, Value},
};

type SqlProjectionPayloadComponents = (Vec<String>, Vec<Option<u32>>, Vec<Vec<Value>>, u32);

///
/// SqlProjectionPayload
///
/// Generic-free row-oriented SQL projection payload carried across the shared
/// SQL statement surface. This keeps SQL `SELECT` results structural so the
/// query lane does not rebuild typed response rows before rendering values.
///

#[derive(Debug)]
pub(in crate::db::session::sql) struct SqlProjectionPayload {
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
    rows: Vec<Vec<Value>>,
    row_count: u32,
    enum_catalog: AcceptedEnumCatalogHandle,
}

impl SqlProjectionPayload {
    #[must_use]
    pub(in crate::db::session::sql) const fn new(
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        rows: Vec<Vec<Value>>,
        row_count: u32,
        enum_catalog: AcceptedEnumCatalogHandle,
    ) -> Self {
        Self {
            columns,
            fixed_scales,
            rows,
            row_count,
            enum_catalog,
        }
    }

    #[must_use]
    pub(in crate::db::session::sql) fn into_components(self) -> SqlProjectionPayloadComponents {
        (self.columns, self.fixed_scales, self.rows, self.row_count)
    }

    pub(in crate::db::session::sql) fn into_statement_result(
        self,
    ) -> Result<SqlStatementResult, QueryError> {
        sql_projection_statement_result_from_value_rows(
            self.enum_catalog.catalog(),
            self.columns,
            self.fixed_scales,
            self.rows,
            self.row_count,
        )
    }
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
