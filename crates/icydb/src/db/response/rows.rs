use candid::CandidType;
use serde::Deserialize;

pub use icydb_core::value::render_output_value_text;

///
/// ProjectionRows
///
/// Render-ready projected row values shared by fluent write-returning helpers
/// and SQL endpoint responses.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionRows {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    row_count: u32,
}

impl ProjectionRows {
    /// Construct one projection row payload.
    #[must_use]
    pub const fn new(columns: Vec<String>, rows: Vec<Vec<String>>, row_count: u32) -> Self {
        Self {
            columns,
            rows,
            row_count,
        }
    }

    /// Borrow projection column names.
    #[must_use]
    pub const fn columns(&self) -> &[String] {
        self.columns.as_slice()
    }

    /// Borrow rendered row values.
    #[must_use]
    pub const fn rows(&self) -> &[Vec<String>] {
        self.rows.as_slice()
    }

    /// Return projected row count.
    #[must_use]
    pub const fn row_count(&self) -> u32 {
        self.row_count
    }

    /// Consume and return projection columns, rendered rows, and row count.
    #[must_use]
    pub fn into_columns_rows_and_count(self) -> (Vec<String>, Vec<Vec<String>>, u32) {
        (self.columns, self.rows, self.row_count)
    }
}

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
    pub rows: Vec<Vec<String>>,
    pub row_count: u32,
}

impl RowProjectionOutput {
    /// Build one endpoint-friendly rows payload from one projection result.
    #[must_use]
    pub fn from_projection(entity: String, projection: ProjectionRows) -> Self {
        let (columns, rows, row_count) = projection.into_columns_rows_and_count();

        Self {
            entity,
            columns,
            rows,
            row_count,
        }
    }

    /// Borrow this output as one render-ready projection row payload.
    #[must_use]
    pub fn as_projection_rows(&self) -> ProjectionRows {
        ProjectionRows::new(self.columns.clone(), self.rows.clone(), self.row_count)
    }
}
