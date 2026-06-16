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
mod table_render;
#[cfg(test)]
mod tests;
mod types;
mod value_render;

pub use crate::db::response::render_output_value_text as render_value_text;
pub(crate) use convert::sql_query_result_from_statement;
pub use table_render::{
    render_count_lines, render_describe_lines, render_explain_lines, render_grouped_lines,
    render_projection_display_rows_lines, render_projection_lines, render_show_columns_lines,
    render_show_entities_lines, render_show_entities_verbose_lines, render_show_indexes_lines,
    render_show_memory_lines, render_show_stores_lines, render_show_stores_verbose_lines,
};
pub use types::{SqlGroupedRowsOutput, SqlProjectionRows, SqlQueryResult, SqlQueryRowsOutput};
