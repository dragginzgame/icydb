#[cfg(all(feature = "sql", feature = "diagnostics"))]
use crate::value::{Value, ValueEnum};

#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db::session::sql::projection::runtime) fn render_projected_sql_rows_text(
    rows: Vec<Vec<Value>>,
) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| render_sql_projection_value_text(&value))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
fn render_sql_projection_value_text(value: &Value) -> String {
    match value {
        Value::Account(v) => v.to_string(),
        Value::Blob(v) => render_sql_projection_blob(v.as_slice()),
        Value::Bool(v) => v.to_string(),
        Value::Date(v) => v.to_string(),
        Value::Decimal(v) => v.to_string(),
        Value::Duration(v) => render_sql_projection_duration(v.as_millis()),
        Value::Enum(v) => render_sql_projection_enum(v),
        Value::Float32(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Int(v) => v.to_string(),
        Value::Int128(v) => v.to_string(),
        Value::IntBig(v) => v.to_string(),
        Value::List(items) => render_sql_projection_list(items.as_slice()),
        Value::Map(entries) => render_sql_projection_map(entries.as_slice()),
        Value::Null => "null".to_string(),
        Value::Principal(v) => v.to_string(),
        Value::Subaccount(v) => v.to_string(),
        Value::Text(v) => v.clone(),
        Value::Timestamp(v) => v.as_millis().to_string(),
        Value::Uint(v) => v.to_string(),
        Value::Uint128(v) => v.to_string(),
        Value::UintBig(v) => v.to_string(),
        Value::Ulid(v) => v.to_string(),
        Value::Unit => "()".to_string(),
    }
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
fn render_sql_projection_blob(bytes: &[u8]) -> String {
    let mut rendered = String::from("0x");
    rendered.push_str(sql_projection_hex_encode(bytes).as_str());

    rendered
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
fn render_sql_projection_duration(millis: u64) -> String {
    let mut rendered = millis.to_string();
    rendered.push_str("ms");

    rendered
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
fn render_sql_projection_list(items: &[Value]) -> String {
    let mut rendered = String::from("[");

    for (index, item) in items.iter().enumerate() {
        if index != 0 {
            rendered.push_str(", ");
        }

        rendered.push_str(render_sql_projection_value_text(item).as_str());
    }

    rendered.push(']');

    rendered
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
fn render_sql_projection_map(entries: &[(Value, Value)]) -> String {
    let mut rendered = String::from("{");

    for (index, (key, value)) in entries.iter().enumerate() {
        if index != 0 {
            rendered.push_str(", ");
        }

        rendered.push_str(render_sql_projection_value_text(key).as_str());
        rendered.push_str(": ");
        rendered.push_str(render_sql_projection_value_text(value).as_str());
    }

    rendered.push('}');

    rendered
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
fn sql_projection_hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }

    out
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
fn render_sql_projection_enum(value: &ValueEnum) -> String {
    let mut rendered = String::new();
    if let Some(path) = value.path() {
        rendered.push_str(path);
        rendered.push_str("::");
    }
    rendered.push_str(value.variant());
    if let Some(payload) = value.payload() {
        rendered.push('(');
        rendered.push_str(render_sql_projection_value_text(payload).as_str());
        rendered.push(')');
    }

    rendered
}
