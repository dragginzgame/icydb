//! Module: db::session::sql::result
//! Responsibility: SQL statement result DTOs returned by session SQL surfaces.
//! Does not own: execution, projection materialization, or compile routing.
//! Boundary: stable SQL-shaped response payloads.

use crate::{db::GroupedRow, db::sql::ddl::SqlDdlPreparationReport, value::OutputValue};

/// Native public SQL output, retaining scalar paging independently of SQL display.
#[doc(hidden)]
#[derive(Debug)]
pub enum SqlStatementPageResult {
    /// One bounded scalar page and its accepted projection display scales.
    Scalar {
        /// Canonical public page, including authenticated progress and work.
        page: crate::db::LiveQueryPageOutput,
        /// Projection-local decimal display scales.
        /// Optional fixed decimal scales aligned with output columns.
        fixed_scales: Vec<Option<u32>>,
    },
    /// Metadata or a grouped result carrying its own grouped continuation.
    Statement {
        /// Executed SQL statement output.
        result: SqlStatementResult,
        /// Original SQL entity label.
        entity: String,
    },
}

///
/// SqlStatementResult
///
/// Unified SQL statement payload returned by shared SQL lane execution.
/// Query, mutation, and metadata statements all shape their response through
/// this single session-owned enum.
///

#[derive(Debug)]
pub enum SqlStatementResult {
    /// A count-only query or mutation result.
    Count {
        /// Number of rows represented by the result.
        row_count: u32,
    },
    /// Rows projected into the requested output columns.
    Projection {
        /// Output column names in projection order.
        columns: Vec<String>,
        /// Optional fixed decimal scales aligned with output columns.
        fixed_scales: Vec<Option<u32>>,
        /// Result rows in the admitted output order.
        rows: Vec<Vec<OutputValue>>,
        /// Number of rows represented by the result.
        row_count: u32,
    },
    /// Grouped rows and an optional continuation cursor.
    Grouped {
        /// Output column names in projection order.
        columns: Vec<String>,
        /// Optional fixed decimal scales aligned with output columns.
        fixed_scales: Vec<Option<u32>>,
        /// Result rows in the admitted output order.
        rows: Vec<GroupedRow>,
        /// Number of rows represented by the result.
        row_count: u32,
        /// Opaque continuation token, when another result page is available.
        next_cursor: Option<String>,
    },
    #[cfg(feature = "sql")]
    /// A rendered explanation of the admitted query plan.
    Explain(String),
    /// An accepted entity schema description.
    Describe(crate::db::SqlDescribeOutput),
    /// Accepted constraints for the selected entity.
    ShowConstraints(Vec<crate::db::EntityConstraintDescription>),
    /// Names of the selected entity's accepted indexes.
    ShowIndexes(Vec<String>),
    /// Accepted field descriptions for the selected entity.
    ShowColumns(crate::db::SqlShowColumnsOutput),
    /// Accepted relation descriptions for the selected entity.
    ShowRelations(crate::db::SqlShowRelationsOutput),
    /// Entries in the accepted entity catalog.
    ShowEntities {
        /// Accepted entity catalog entries.
        entities: Vec<crate::db::EntityCatalogDescription>,
        /// Whether the caller requested the verbose catalog presentation.
        verbose: bool,
    },
    /// Entries in the accepted store catalog.
    ShowStores {
        /// Accepted store catalog entries.
        stores: Vec<crate::db::StoreCatalogDescription>,
        /// Whether the caller requested the verbose catalog presentation.
        verbose: bool,
    },
    /// Memory allocation descriptions exposed by the catalog.
    ShowMemory(Vec<crate::db::MemoryCatalogDescription>),
    /// Preparation or execution report for a schema mutation.
    Ddl(SqlDdlPreparationReport),
}
