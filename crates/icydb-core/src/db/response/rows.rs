//! Module: response::rows
//! Responsibility: engine-neutral projected-row response payloads.
//! Does not own: query execution, SQL adaptation, or typed row decoding.
//! Boundary: shared dynamic/SQL projection output over accepted public values.

use crate::value::{OutputValue, render_output_value_text};
use candid::CandidType;
use serde::Deserialize;

/// Row-oriented output from one accepted-schema-driven projection.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
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
mod tests {
    use super::*;

    #[derive(CandidType)]
    struct FrozenRowProjectionOutputWire {
        entity: String,
        columns: Vec<String>,
        rows: Vec<Vec<OutputValue>>,
        row_count: u32,
    }

    #[test]
    fn row_projection_output_preserves_the_frozen_candid_record_shape() {
        let current = RowProjectionOutput {
            entity: "example".to_string(),
            columns: vec!["id".to_string()],
            rows: Vec::new(),
            row_count: 0,
        };
        let frozen = FrozenRowProjectionOutputWire {
            entity: current.entity.clone(),
            columns: current.columns.clone(),
            rows: current.rows.clone(),
            row_count: current.row_count,
        };

        assert_eq!(current.len(), 0);
        assert!(current.is_empty());

        assert_eq!(
            candid::encode_one(&current).expect("current row projection should encode"),
            candid::encode_one(&frozen).expect("frozen row projection should encode"),
        );
    }
}
