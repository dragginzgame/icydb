use crate::{db::response::render_output_value_text, value::OutputValue};
use icydb_core::types::Decimal;

pub(in crate::db::sql) fn sql_projection_output_rows(
    columns: &[String],
    fixed_scales: &[Option<u32>],
    rows: Vec<Vec<OutputValue>>,
) -> Vec<Vec<OutputValue>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .enumerate()
                .map(|(index, value)| {
                    sql_projection_output_value(
                        columns.get(index),
                        fixed_scales.get(index).copied().flatten(),
                        value,
                    )
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

fn sql_projection_output_value(
    column: Option<&String>,
    fixed_scale: Option<u32>,
    value: OutputValue,
) -> OutputValue {
    let Some(scale) =
        fixed_scale.or_else(|| column.and_then(|label| round_projection_scale(label.as_str())))
    else {
        return value;
    };

    match value {
        OutputValue::Decimal(decimal) => {
            OutputValue::Text(render_decimal_with_fixed_scale(&decimal, scale))
        }
        other => other,
    }
}

pub(in crate::db::sql) fn render_projection_value_text(
    column: Option<&String>,
    fixed_scale: Option<u32>,
    value: &OutputValue,
) -> String {
    let Some(scale) =
        fixed_scale.or_else(|| column.and_then(|label| round_projection_scale(label.as_str())))
    else {
        return render_output_value_text(value);
    };

    match value {
        OutputValue::Decimal(decimal) => render_decimal_with_fixed_scale(decimal, scale),
        _ => render_output_value_text(value),
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
