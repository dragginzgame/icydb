//! Module: session::response
//! Responsibility: session-boundary response finalization helpers.
//! Does not own: public response DTO shape, cursor grammar, or executor runtime behavior.
//! Boundary: converts executor page carriers into public response envelopes.

mod grouped;
mod scalar;

#[cfg(feature = "sql")]
pub(in crate::db) use grouped::finalize_structural_grouped_projection_result;
pub(in crate::db) use grouped::{finalize_grouped_paged_execution, sql_grouped_cursor_from_bytes};
pub(in crate::db) use scalar::finalize_scalar_paged_execution;
