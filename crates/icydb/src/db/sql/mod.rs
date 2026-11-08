//! Module: db::sql
//!
//! Responsibility: public SQL result and rendering facade.
//! Does not own: SQL parsing, lowering, planning, or execution.
//! Boundary: converts executed core SQL outputs into endpoint-friendly payloads.

//! Defines the public SQL text/result payload types exposed by the facade crate.
//!
//! This module consumes already-executed SQL outputs and renders stable
//! endpoint-friendly row payloads; parsing and execution stay in `icydb-core`.

mod convert;
#[cfg(test)]
mod tests;
mod types;
mod value_render;

#[cfg(feature = "sql")]
pub(crate) use convert::sql_query_page_result_from_statement;
pub(crate) use convert::sql_query_result_from_statement;

pub use types::{
    SqlConstraintValidationOutput, SqlGroupedRowsOutput, SqlQueryPageResult, SqlQueryResult,
};

pub use value_render::sql_projection_display_rows;
