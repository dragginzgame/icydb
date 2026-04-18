//! Module: db::session::sql::projection::payload
//! Responsibility: own the outward SQL projection payload types returned by
//! session SQL statement surfaces.
//! Does not own: projection execution, labeling, or textual rendering policy.
//! Boundary: keeps SQL projection DTOs stable and separate from executor internals.

use crate::{
    db::{QueryError, executor::GroupedCursorPage, session::sql::SqlStatementResult},
    value::Value,
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
        SqlStatementResult::Projection {
            columns: self.columns,
            fixed_scales: self.fixed_scales,
            rows: self.rows,
            row_count: self.row_count,
        }
    }
}

/// Build one grouped SQL statement result directly from one grouped page while
/// preserving the SQL surface's grouped-cursor hex encoding contract.
pub(in crate::db::session::sql) fn grouped_sql_statement_result_from_page(
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
    page: GroupedCursorPage,
) -> Result<SqlStatementResult, QueryError> {
    let next_cursor = page
        .next_cursor
        .map(|cursor| {
            let Some(token) = cursor.as_grouped() else {
                return Err(QueryError::grouped_paged_emitted_scalar_continuation());
            };

            token.encode_hex().map_err(|err| {
                QueryError::serialize_internal(format!(
                    "failed to serialize grouped continuation cursor: {err}"
                ))
            })
        })
        .transpose()?;
    let row_count = u32::try_from(page.rows.len()).unwrap_or(u32::MAX);

    Ok(SqlStatementResult::Grouped {
        columns,
        fixed_scales,
        rows: page.rows,
        row_count,
        next_cursor,
    })
}
