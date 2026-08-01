//! Module: db::response
//!
//! Responsibility: public database response payloads.
//! Does not own: query execution, storage mutation, or core response construction.
//! Boundary: adapts core response shapes to facade-facing Candid-friendly types.

// re-exports
pub use icydb_core::db::{ExecutionTrace, GroupedRow};
pub use icydb_core::value::render_output_value_text;
