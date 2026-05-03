//! Module: db::session::sql::projection::payload
//! Responsibility: own the outward SQL projection payload types returned by
//! session SQL statement surfaces.
//! Does not own: projection execution, labeling, or textual rendering policy.
//! Boundary: keeps SQL projection DTOs stable and separate from executor internals.

use crate::{
    db::session::sql::SqlStatementResult,
    value::{OutputValue, Value},
};

type SqlProjectionPayloadParts = (Vec<String>, Vec<Option<u32>>, Vec<Vec<Value>>, u32);

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
}

impl SqlProjectionPayload {
    #[must_use]
    pub(in crate::db::session::sql) const fn new(
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        rows: Vec<Vec<Value>>,
        row_count: u32,
    ) -> Self {
        Self {
            columns,
            fixed_scales,
            rows,
            row_count,
        }
    }

    #[must_use]
    pub(in crate::db::session::sql) fn into_parts(self) -> SqlProjectionPayloadParts {
        (self.columns, self.fixed_scales, self.rows, self.row_count)
    }

    #[must_use]
    pub(in crate::db::session::sql) fn into_statement_result(self) -> SqlStatementResult {
        sql_projection_statement_result_from_value_rows(
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
#[must_use]
pub(in crate::db::session::sql) fn sql_projection_statement_result_from_value_rows(
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
    rows: impl IntoIterator<Item = Vec<Value>>,
    row_count: u32,
) -> SqlStatementResult {
    SqlStatementResult::Projection {
        columns,
        fixed_scales,
        rows: rows
            .into_iter()
            .map(sql_output_row_from_value_row)
            .collect(),
        row_count,
    }
}

/// Convert fallibly projected value rows into the public SQL statement shape.
///
/// Write `RETURNING` callers use this to fuse row projection with output-value
/// encoding while preserving the first projection error exactly.
pub(in crate::db::session::sql) fn sql_projection_statement_result_from_fallible_value_rows<E>(
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
    rows: impl IntoIterator<Item = Result<Vec<Value>, E>>,
    row_count: u32,
) -> Result<SqlStatementResult, E> {
    let rows = rows
        .into_iter()
        .map(|row| row.map(sql_output_row_from_value_row))
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
fn sql_output_row_from_value_row(row: Vec<Value>) -> Vec<OutputValue> {
    row.into_iter().map(OutputValue::from).collect()
}
