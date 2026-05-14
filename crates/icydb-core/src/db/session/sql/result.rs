//! Module: db::session::sql::result
//! Responsibility: SQL statement result DTOs returned by session SQL surfaces.
//! Does not own: execution, projection materialization, or compile routing.
//! Boundary: stable SQL-shaped response payloads.

use crate::{db::GroupedRow, db::sql::ddl::SqlDdlPreparationReport, value::OutputValue};

///
/// SqlStatementResult
///
/// Unified SQL statement payload returned by shared SQL lane execution.
/// Query, mutation, explain, and metadata statements all shape their response
/// through this single session-owned enum.
///

#[derive(Debug)]
pub enum SqlStatementResult {
    Count {
        row_count: u32,
    },
    Projection {
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        rows: Vec<Vec<OutputValue>>,
        row_count: u32,
    },
    ProjectionText {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        row_count: u32,
    },
    Grouped {
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        rows: Vec<GroupedRow>,
        row_count: u32,
        next_cursor: Option<String>,
    },
    Explain(String),
    Describe(crate::db::EntitySchemaDescription),
    ShowIndexes(Vec<String>),
    ShowColumns(Vec<crate::db::EntityFieldDescription>),
    ShowEntities(Vec<String>),
    Ddl(SqlDdlPreparationReport),
}
