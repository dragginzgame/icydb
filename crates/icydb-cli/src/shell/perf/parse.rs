//! Module: shell perf payload parsing.
//! Responsibility: decode JSON-shaped perf responses into SQL results and attribution.
//! Does not own: canister calls, shell rendering, or instruction collection.
//! Boundary: normalizes legacy candid JSON shapes before typed deserialization.

use icydb::db::sql::SqlQueryResult;
use serde_json::Value;

use crate::shell::perf::{ShellPerfAttribution, ShellPerfAttributionInput};

pub(in crate::shell) fn parse_perf_result(
    value: &Value,
) -> Result<(SqlQueryResult, ShellPerfAttribution), String> {
    Ok((
        parse_perf_sql_result(value)?,
        parse_perf_attribution(value)?,
    ))
}

fn parse_perf_sql_result(value: &Value) -> Result<SqlQueryResult, String> {
    let result_value = value
        .get("result")
        .ok_or_else(|| "perf result missing result payload".to_string())?;
    let mut result_value = result_value.clone();

    normalize_grouped_next_cursor_json(&mut result_value);

    serde_json::from_value::<SqlQueryResult>(result_value).map_err(|err| err.to_string())
}

pub(in crate::shell) fn normalize_grouped_next_cursor_json(value: &mut Value) {
    let Some(grouped) = value.get_mut("Grouped") else {
        return;
    };
    let Some(grouped_object) = grouped.as_object_mut() else {
        return;
    };
    let Some(next_cursor) = grouped_object.get_mut("next_cursor") else {
        return;
    };

    if let Some(normalized) = normalized_candid_option_json(next_cursor) {
        *next_cursor = normalized;
    }
}

fn normalized_candid_option_json(value: &mut Value) -> Option<Value> {
    match value {
        Value::Array(values) if values.is_empty() => Some(Value::Null),
        Value::Array(values) if values.len() == 1 => values.pop(),
        _ => None,
    }
}

fn parse_perf_attribution(value: &Value) -> Result<ShellPerfAttribution, String> {
    Ok(ShellPerfAttribution::new(ShellPerfAttributionInput {
        total: parse_perf_u64(value, "instructions")?,
        planner: parse_perf_u64(value, "planner_instructions")?,
        store: parse_perf_u64_or_default(value, "store_instructions")?,
        executor: parse_perf_u64(value, "executor_instructions")?,
        pure_covering_decode: parse_perf_u64_or_default(
            value,
            "pure_covering_decode_instructions",
        )?,
        pure_covering_row_assembly: parse_perf_u64_or_default(
            value,
            "pure_covering_row_assembly_instructions",
        )?,
        decode: parse_perf_u64_or_default(value, "decode_instructions")?,
        compiler: parse_perf_u64(value, "compiler_instructions")?,
    }))
}

fn parse_perf_u64(value: &Value, field: &str) -> Result<u64, String> {
    let field_value = value
        .get(field)
        .ok_or_else(|| format!("perf result missing {field}"))?;

    match field_value {
        Value::Number(number) => number
            .as_u64()
            .ok_or_else(|| format!("perf field {field} is not a u64")),
        Value::String(text) => text
            .parse()
            .map_err(|_| format!("perf field {field} is not a u64")),
        _ => Err(format!("perf field {field} has unsupported type")),
    }
}

fn parse_perf_u64_or_default(value: &Value, field: &str) -> Result<u64, String> {
    if value.get(field).is_none() {
        return Ok(0);
    }

    parse_perf_u64(value, field)
}
