//! Module: db::sql_shared::literal_render
//! Responsibility: canonical rendering of reduced SQL scalar literals.
//! Does not own: field-kind coercion, accepted catalogs, or expression formatting.
//! Boundary: SQL metadata renderers consume one shared parseable literal spelling.

use crate::value::Value;

pub(in crate::db) fn render_scalar_sql_value(value: &Value) -> Option<String> {
    Some(match value {
        Value::Blob(bytes) => render_blob_sql_value(bytes),
        Value::Bool(value) => value.to_string().to_uppercase(),
        Value::Decimal(value) => value.to_string(),
        Value::Float32(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Int128(value) => value.to_string(),
        Value::IntBig(value) => value.to_string(),
        Value::Nat64(value) => value.to_string(),
        Value::Nat128(value) => value.to_string(),
        Value::NatBig(value) => value.to_string(),
        Value::Null => "NULL".to_string(),
        Value::Text(text) => format!("'{}'", text.replace('\'', "''")),
        Value::List(_)
        | Value::Account(_)
        | Value::Date(_)
        | Value::Duration(_)
        | Value::Enum(_)
        | Value::Map(_)
        | Value::Principal(_)
        | Value::Subaccount(_)
        | Value::Timestamp(_)
        | Value::Ulid(_)
        | Value::Unit => return None,
    })
}

fn render_blob_sql_value(bytes: &[u8]) -> String {
    let mut rendered = String::with_capacity(bytes.len().saturating_mul(2) + 3);
    rendered.push_str("X'");
    for byte in bytes {
        rendered.push(hex_digit(byte >> 4));
        rendered.push(hex_digit(byte & 0x0f));
    }
    rendered.push('\'');
    rendered
}

const fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'A' + nibble - 10) as char,
        _ => '?',
    }
}
