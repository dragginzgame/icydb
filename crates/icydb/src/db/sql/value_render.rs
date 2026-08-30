//! Module: db::sql::value_render
//!
//! Responsibility: public SQL result and rendering facade.
//! Does not own: SQL parsing, lowering, planning, or execution.
//! Boundary: converts executed core SQL outputs into endpoint-friendly payloads.

use crate::value::{OutputValue, PublicValue};
use icydb_core::value::render_output_value_text;

use icydb_core::types::Decimal;

pub(in crate::db::sql) fn sql_projection_output_rows(
    fixed_scales: &[Option<u32>],
    rows: Vec<Vec<OutputValue>>,
) -> Vec<Vec<OutputValue>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .enumerate()
                .map(|(index, value)| {
                    sql_projection_output_value(fixed_scales.get(index).copied().flatten(), value)
                })
                .collect()
        })
        .collect()
}

pub(in crate::db::sql) fn render_projection_rows(rows: &[Vec<OutputValue>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(render_output_value_text).collect::<Vec<_>>())
        .collect()
}

fn sql_projection_output_value(fixed_scale: Option<u32>, value: OutputValue) -> OutputValue {
    let Some(scale) = fixed_scale else {
        return value;
    };

    if let PublicValue::Decimal(decimal) = value.as_public() {
        return OutputValue::text(render_decimal_with_fixed_scale(decimal, scale));
    }

    value
}

pub(in crate::db::sql) fn render_projection_value_text(
    fixed_scale: Option<u32>,
    value: &OutputValue,
) -> String {
    let Some(scale) = fixed_scale else {
        return render_output_value_text(value);
    };

    match value.as_public() {
        PublicValue::Decimal(decimal) => render_decimal_with_fixed_scale(decimal, scale),
        _ => render_output_value_text(value),
    }
}

fn render_decimal_with_fixed_scale(decimal: &Decimal, scale: u32) -> String {
    let scale = scale.min(Decimal::max_supported_scale());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_fixed_scale_metadata_clamps_zero_scale_width() {
        let rendered =
            render_projection_value_text(Some(4_000_000_000), &OutputValue::decimal(Decimal::ZERO));

        assert_eq!(rendered, "0.0000000000000000000000000000");
    }
}
