//! Module: response::rows
//! Responsibility: engine-neutral projected-row response payloads.
//! Does not own: query execution, SQL adaptation, or typed row decoding.
//! Boundary: shared dynamic/SQL projection output over accepted public values.

use crate::value::{OutputValue, render_output_value_text};

use serde::Deserialize;

/// Row-oriented output from one accepted-schema-driven projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub struct RowProjectionOutput {
    /// Accepted entity name used for the read.
    pub entity: String,
    /// Selected output-column names in row order.
    pub columns: Vec<String>,
    /// Row-oriented output values.
    pub rows: Vec<Vec<OutputValue>>,
    /// Number of returned rows.
    pub row_count: u32,
}

impl RowProjectionOutput {
    /// Return the number of rows carried by this output.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.rows.len()
    }

    /// Return whether this output carries no rows.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Render row values into stable display strings.
    #[must_use]
    pub fn rendered_rows(&self) -> Vec<Vec<String>> {
        self.rows
            .iter()
            .map(|row| row.iter().map(render_output_value_text).collect())
            .collect()
    }
}

#[cfg(test)]
mod tests;
