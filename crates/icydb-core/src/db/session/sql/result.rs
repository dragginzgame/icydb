//! Module: db::session::sql::result
//! Responsibility: SQL statement result DTOs returned by session SQL surfaces.
//! Does not own: execution, projection materialization, or compile routing.
//! Boundary: stable SQL-shaped response payloads.

use crate::{db::GroupedRow, db::sql::ddl::SqlDdlPreparationReport, value::OutputValue};

///
/// SqlStatementResult
///
/// Unified SQL statement payload returned by shared SQL lane execution.
/// Query, mutation, and metadata statements all shape their response through
/// this single session-owned enum.
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
    Grouped {
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        rows: Vec<GroupedRow>,
        row_count: u32,
        next_cursor: Option<String>,
    },
    #[cfg(feature = "sql-explain")]
    Explain(String),
    Describe(crate::db::EntitySchemaDescription),
    ShowConstraints(Vec<crate::db::EntityConstraintDescription>),
    ShowIndexes(Vec<String>),
    ShowColumns(Vec<crate::db::EntityFieldDescription>),
    ShowEntities {
        entities: Vec<crate::db::EntityCatalogDescription>,
        verbose: bool,
    },
    ShowStores {
        stores: Vec<crate::db::StoreCatalogDescription>,
        verbose: bool,
    },
    ShowMemory(Vec<crate::db::MemoryCatalogDescription>),
    Ddl(SqlDdlPreparationReport),
}
