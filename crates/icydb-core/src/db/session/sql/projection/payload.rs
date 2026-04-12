//! Module: db::session::sql::projection::payload
//! Responsibility: own the outward SQL projection payload types returned by
//! session SQL statement surfaces.
//! Does not own: projection execution, labeling, or textual rendering policy.
//! Boundary: keeps SQL projection DTOs stable and separate from executor internals.

use crate::{db::session::sql::SqlStatementResult, value::Value};

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
    rows: Vec<Vec<Value>>,
    row_count: u32,
}

impl SqlProjectionPayload {
    #[must_use]
    pub(in crate::db::session::sql) const fn new(
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        row_count: u32,
    ) -> Self {
        Self {
            columns,
            rows,
            row_count,
        }
    }

    #[must_use]
    pub(in crate::db::session::sql) fn into_parts(self) -> (Vec<String>, Vec<Vec<Value>>, u32) {
        (self.columns, self.rows, self.row_count)
    }

    #[must_use]
    pub(in crate::db::session::sql) fn into_statement_result(self) -> SqlStatementResult {
        SqlStatementResult::Projection {
            columns: self.columns,
            rows: self.rows,
            row_count: self.row_count,
        }
    }
}
