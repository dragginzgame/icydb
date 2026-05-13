use icydb::db::sql::SqlQueryResult;
use serde_json::Value;

use crate::shell::perf::ShellPerfAttribution;

pub(crate) fn parse_perf_result(
    value: &Value,
) -> Result<(SqlQueryResult, ShellPerfAttribution), String> {
    let result_value = value
        .get("result")
        .ok_or_else(|| "perf result missing result payload".to_string())?;
    let mut result_value = result_value.clone();

    normalize_grouped_next_cursor_json(&mut result_value);

    let result =
        serde_json::from_value::<SqlQueryResult>(result_value).map_err(|err| err.to_string())?;

    Ok((
        result,
        ShellPerfAttribution {
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
        },
    ))
}

pub(crate) fn normalize_grouped_next_cursor_json(value: &mut Value) {
    let Some(grouped) = value.get_mut("Grouped") else {
        return;
    };
    let Some(grouped_object) = grouped.as_object_mut() else {
        return;
    };
    let Some(next_cursor) = grouped_object.get_mut("next_cursor") else {
        return;
    };

    match next_cursor {
        Value::Array(values) if values.is_empty() => *next_cursor = Value::Null,
        Value::Array(values) if values.len() == 1 => {
            *next_cursor = values.pop().unwrap_or(Value::Null);
        }
        _ => {}
    }
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
