//! Module: db::session::sql::projection::payload
//! Responsibility: module-local ownership and contracts for db::session::sql::projection::payload.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{db::session::sql::SqlDispatchResult, value::Value};

///
/// SqlProjectionPayload
///
/// Generic-free row-oriented SQL projection payload carried across the shared
/// SQL dispatch surface. This keeps SQL `SELECT` results structural so the
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
    pub(in crate::db::session::sql) fn into_dispatch_result(self) -> SqlDispatchResult {
        SqlDispatchResult::Projection {
            columns: self.columns,
            rows: self.rows,
            row_count: self.row_count,
        }
    }
}
