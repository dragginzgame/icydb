//! Module: db::response
//!
//! Responsibility: public database response payloads.
//! Does not own: query execution, storage mutation, or core response construction.
//! Boundary: adapts core response shapes to facade-facing Candid-friendly types.

mod rows;

// re-exports
pub use icydb_core::db::{ExecutionTrace, GroupedRow};
pub use rows::{RowProjectionOutput, render_output_value_text};
