//! Module: db::session::sql::projection::payload
//! Responsibility: own the outward SQL projection payload types returned by
//! session SQL statement surfaces.
//! Does not own: projection execution, labeling, or textual rendering policy.
//! Boundary: keeps SQL projection DTOs stable and separate from executor internals.

use crate::{
    db::{GroupedRow, session::sql::SqlStatementResult},
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

/// Build one grouped SQL statement result at the session SQL projection
/// boundary so grouped row packaging stays out of executor routing code.
pub(in crate::db::session::sql) fn grouped_sql_statement_result(
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
    rows: Vec<GroupedRow>,
    next_cursor: Option<String>,
) -> SqlStatementResult {
    let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);

    SqlStatementResult::Grouped {
        columns,
        fixed_scales,
        rows,
        row_count,
        next_cursor,
    }
}
