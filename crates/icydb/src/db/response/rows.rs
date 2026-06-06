use crate::value::{OutputValue, OutputValueEnum};
use candid::CandidType;
use icydb_core::db::encode_hex_lower;
use serde::Deserialize;

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

/// Render one output value into a stable text form for row projection payloads.
#[must_use]
pub fn render_output_value_text(value: &OutputValue) -> String {
    match value {
        OutputValue::Account(v) => v.to_string(),
        OutputValue::Blob(v) => render_blob_value(v),
        OutputValue::Bool(v) => v.to_string(),
        OutputValue::Date(v) => v.to_string(),
        OutputValue::Decimal(v) => v.to_string(),
        OutputValue::Duration(v) => render_duration_value(v.as_millis()),
        OutputValue::Enum(v) => render_enum(v),
        OutputValue::Float32(v) => v.to_string(),
        OutputValue::Float64(v) => v.to_string(),
        OutputValue::Int64(v) => v.to_string(),
        OutputValue::Int128(v) => v.to_string(),
        OutputValue::IntBig(v) => v.to_string(),
        OutputValue::List(items) => render_list_value(items.as_slice()),
        OutputValue::Map(entries) => render_map_value(entries.as_slice()),
        OutputValue::Null => "null".to_string(),
        OutputValue::Principal(v) => v.to_string(),
        OutputValue::Subaccount(v) => v.to_string(),
        OutputValue::Text(v) => v.clone(),
        OutputValue::Timestamp(v) => v.as_millis().to_string(),
        OutputValue::Nat64(v) => v.to_string(),
        OutputValue::Nat128(v) => v.to_string(),
        OutputValue::NatBig(v) => v.to_string(),
        OutputValue::Ulid(v) => v.to_string(),
        OutputValue::Unit => "()".to_string(),
    }
}

fn render_blob_value(bytes: &[u8]) -> String {
    let mut rendered = String::from("0x");
    rendered.push_str(encode_hex_lower(bytes).as_str());

    rendered
}

fn render_duration_value(millis: u64) -> String {
    let mut rendered = millis.to_string();
    rendered.push_str("ms");

    rendered
}

fn render_list_value(items: &[OutputValue]) -> String {
    let mut rendered = String::from("[");

    for (index, item) in items.iter().enumerate() {
        if index != 0 {
            rendered.push_str(", ");
        }

        rendered.push_str(render_output_value_text(item).as_str());
    }

    rendered.push(']');

    rendered
}

fn render_map_value(entries: &[(OutputValue, OutputValue)]) -> String {
    let mut rendered = String::from("{");

    for (index, (key, value)) in entries.iter().enumerate() {
        if index != 0 {
            rendered.push_str(", ");
        }

        rendered.push_str(render_output_value_text(key).as_str());
        rendered.push_str(": ");
        rendered.push_str(render_output_value_text(value).as_str());
    }

    rendered.push('}');

    rendered
}

fn render_enum(value: &OutputValueEnum) -> String {
    let mut rendered = String::new();
    if let Some(path) = value.path() {
        rendered.push_str(path);
        rendered.push_str("::");
    }
    rendered.push_str(value.variant());
    if let Some(payload) = value.payload() {
        rendered.push('(');
        rendered.push_str(render_output_value_text(payload).as_str());
        rendered.push(')');
    }

    rendered
}
