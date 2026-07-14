//! Module: db::response::rows
//!
//! Responsibility: public database response payloads.
//! Does not own: query execution, storage mutation, or core response construction.
//! Boundary: adapts core response shapes to facade-facing Candid-friendly types.

use candid::CandidType;
use icydb_core::value::OutputValue;
use serde::Deserialize;

pub use icydb_core::value::render_output_value_text;

///
/// RowProjectionOutput
///
/// Candid-friendly projected row payload shared by fluent write-returning
/// helpers and SQL projection endpoints.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct RowProjectionOutput {
    pub entity: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<OutputValue>>,
    pub row_count: u32,
}

impl RowProjectionOutput {
    /// Render row values into stable display strings.
    #[must_use]
    pub fn rendered_rows(&self) -> Vec<Vec<String>> {
        render_rows(self.rows.as_slice())
    }
}

fn render_rows(rows: &[Vec<OutputValue>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(render_output_value_text).collect())
        .collect()
}
