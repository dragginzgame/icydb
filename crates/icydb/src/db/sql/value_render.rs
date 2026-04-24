use crate::value::{OutputValue, OutputValueEnum};
use icydb_core::types::Decimal;

#[cfg_attr(doc, doc = "Render one value into a shell-friendly stable text form.")]
#[must_use]
pub fn render_value_text(value: &OutputValue) -> String {
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
        OutputValue::Int(v) => v.to_string(),
        OutputValue::Int128(v) => v.to_string(),
        OutputValue::IntBig(v) => v.to_string(),
        OutputValue::List(items) => render_list_value(items.as_slice()),
        OutputValue::Map(entries) => render_map_value(entries.as_slice()),
        OutputValue::Null => "null".to_string(),
        OutputValue::Principal(v) => v.to_string(),
        OutputValue::Subaccount(v) => v.to_string(),
        OutputValue::Text(v) => v.clone(),
        OutputValue::Timestamp(v) => v.as_millis().to_string(),
        OutputValue::Uint(v) => v.to_string(),
        OutputValue::Uint128(v) => v.to_string(),
        OutputValue::UintBig(v) => v.to_string(),
        OutputValue::Ulid(v) => v.to_string(),
        OutputValue::Unit => "()".to_string(),
    }
}

pub(in crate::db::sql) fn render_projection_rows(
    columns: &[String],
    fixed_scales: &[Option<u32>],
    rows: Vec<Vec<OutputValue>>,
) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .enumerate()
                .map(|(index, value)| {
                    render_projection_value_text(
                        columns.get(index),
                        fixed_scales.get(index).copied().flatten(),
                        &value,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

pub(in crate::db::sql) fn render_projection_value_text(
    column: Option<&String>,
    fixed_scale: Option<u32>,
    value: &OutputValue,
) -> String {
    let Some(scale) =
        fixed_scale.or_else(|| column.and_then(|label| round_projection_scale(label.as_str())))
    else {
        return render_value_text(value);
    };

    match value {
        OutputValue::Decimal(decimal) => render_decimal_with_fixed_scale(decimal, scale),
        _ => render_value_text(value),
    }
}

fn round_projection_scale(column: &str) -> Option<u32> {
    let body = column
        .trim()
        .strip_prefix("ROUND(")?
        .strip_suffix(')')?
        .trim();
    let (_, scale) = body.rsplit_once(',')?;

    scale.trim().parse::<u32>().ok()
}

fn render_decimal_with_fixed_scale(decimal: &Decimal, scale: u32) -> String {
    let rounded = decimal.round_dp(scale);

    if rounded.mantissa() == 0 {
        if scale == 0 {
            return "0".to_string();
        }

        return format!("0.{:0<width$}", "", width = scale as usize);
    }

    let negative = rounded.mantissa().is_negative();
    let digits = rounded.mantissa().unsigned_abs().to_string();
    let fixed = decimal_digits_with_scale(digits.as_str(), rounded.scale(), scale);

    if negative { format!("-{fixed}") } else { fixed }
}

fn decimal_digits_with_scale(digits: &str, current_scale: u32, target_scale: u32) -> String {
    if target_scale == 0 {
        return digits.to_string();
    }

    let current_scale = current_scale as usize;
    let target_scale = target_scale as usize;
    let (integer, fraction) = if digits.len() <= current_scale {
        let zeros = "0".repeat(current_scale - digits.len());
        ("0".to_string(), format!("{zeros}{digits}"))
    } else {
        let split = digits.len() - current_scale;
        (digits[..split].to_string(), digits[split..].to_string())
    };

    let mut rendered = integer;
    rendered.push('.');
    rendered.push_str(fraction.as_str());

    for _ in current_scale..target_scale {
        rendered.push('0');
    }

    rendered
}

fn render_blob_value(bytes: &[u8]) -> String {
    let mut rendered = String::from("0x");
    rendered.push_str(hex_encode(bytes).as_str());

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

        rendered.push_str(render_value_text(item).as_str());
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

        rendered.push_str(render_value_text(key).as_str());
        rendered.push_str(": ");
        rendered.push_str(render_value_text(value).as_str());
    }

    rendered.push('}');

    rendered
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }

    out
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
        rendered.push_str(render_value_text(payload).as_str());
        rendered.push(')');
    }

    rendered
}
