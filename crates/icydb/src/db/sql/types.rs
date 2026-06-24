//! Module: db::sql::types
//!
//! Responsibility: public SQL result and rendering facade.
//! Does not own: SQL parsing, lowering, planning, or execution.
//! Boundary: converts executed core SQL outputs into endpoint-friendly payloads.

#[cfg(feature = "sql-explain")]
use crate::db::sql::table_render::render_explain_lines;
use crate::db::{
    EntityCatalogDescription, EntityFieldDescription, EntitySchemaDescription,
    MemoryCatalogDescription, StoreCatalogDescription,
    response::{ProjectionRows, RowProjectionOutput},
    sql::table_render::{
        SqlDdlRenderInput, render_count_lines, render_describe_lines, render_grouped_lines,
        render_query_rows_lines, render_show_columns_lines, render_show_entities_lines,
        render_show_entities_verbose_lines, render_show_indexes_lines, render_show_memory_lines,
        render_show_stores_lines, render_show_stores_verbose_lines, render_sql_ddl_lines,
    },
};

use candid::CandidType;
use serde::Deserialize;

/// Compatibility alias for SQL endpoint callers. The concrete row payload now
/// lives under `db::response` so fluent write-returning does not depend on the
/// SQL module.
pub type SqlProjectionRows = ProjectionRows;

/// Compatibility alias for SQL endpoint callers. The concrete row payload now
/// lives under `db::response` so fluent write-returning does not depend on the
/// SQL module.
pub type SqlQueryRowsOutput = RowProjectionOutput;

#[cfg_attr(doc, doc = "SqlGroupedRowsOutput\n\nStructured grouped SQL payload.")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlGroupedRowsOutput {
    pub entity: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: u32,
    pub next_cursor: Option<String>,
}

#[cfg_attr(doc, doc = "SqlQueryResult\n\nUnified SQL endpoint result.")]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlQueryResult {
    Count {
        entity: String,
        row_count: u32,
    },
    Projection(SqlQueryRowsOutput),
    Grouped(SqlGroupedRowsOutput),
    #[cfg(feature = "sql-explain")]
    Explain {
        entity: String,
        explain: String,
    },
    Describe(EntitySchemaDescription),
    ShowIndexes {
        entity: String,
        indexes: Vec<String>,
    },
    ShowColumns {
        entity: String,
        columns: Vec<EntityFieldDescription>,
    },
    ShowEntities {
        entities: Vec<EntityCatalogDescription>,
        verbose: bool,
    },
    ShowStores {
        stores: Vec<StoreCatalogDescription>,
        verbose: bool,
    },
    ShowMemory {
        memory: Vec<MemoryCatalogDescription>,
    },
    Ddl {
        entity: String,
        mutation_kind: String,
        target_index: String,
        target_store: String,
        field_path: Vec<String>,
        status: String,
        rows_scanned: u64,
        index_keys_written: u64,
    },
}

impl SqlQueryResult {
    /// Render this payload into deterministic shell-friendly lines.
    #[must_use]
    pub fn render_lines(&self) -> Vec<String> {
        match self {
            Self::Count { entity, row_count } => render_count_lines(entity.as_str(), *row_count),
            Self::Projection(rows) => render_query_rows_lines(rows),
            Self::Grouped(rows) => render_grouped_lines(rows),
            #[cfg(feature = "sql-explain")]
            Self::Explain { explain, .. } => render_explain_lines(explain.as_str()),
            Self::Describe(description) => render_describe_lines(description),
            Self::ShowIndexes { entity, indexes } => {
                render_show_indexes_lines(entity.as_str(), indexes.as_slice())
            }
            Self::ShowColumns { entity, columns } => {
                render_show_columns_lines(entity.as_str(), columns.as_slice())
            }
            Self::ShowEntities { entities, verbose } => {
                if *verbose {
                    render_show_entities_verbose_lines(entities.as_slice())
                } else {
                    render_show_entities_lines(entities.as_slice())
                }
            }
            Self::ShowStores { stores, verbose } => {
                if *verbose {
                    render_show_stores_verbose_lines(stores.as_slice())
                } else {
                    render_show_stores_lines(stores.as_slice())
                }
            }
            Self::ShowMemory { memory } => render_show_memory_lines(memory.as_slice()),
            Self::Ddl {
                entity,
                mutation_kind,
                target_index,
                target_store,
                field_path,
                status,
                rows_scanned,
                index_keys_written,
            } => render_sql_ddl_lines(SqlDdlRenderInput {
                entity: entity.as_str(),
                mutation_kind: mutation_kind.as_str(),
                target_index: target_index.as_str(),
                target_store: target_store.as_str(),
                field_path: field_path.as_slice(),
                status: status.as_str(),
                rows_scanned: *rows_scanned,
                index_keys_written: *index_keys_written,
            }),
        }
    }

    /// Render this payload into one newline-separated display string.
    #[must_use]
    pub fn render_text(&self) -> String {
        self.render_lines().join("\n")
    }
}
