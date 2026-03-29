use crate::{
    db::{
        QueryError,
        session::sql::{
            computed_projection::model::{
                SqlComputedProjectionItem, SqlComputedProjectionPlan,
                SqlComputedProjectionTransform,
            },
            projection::SqlProjectionPayload,
        },
    },
    value::Value,
};

// Build one deterministic text-input mismatch error for computed SQL projection.
fn computed_sql_projection_text_input_error(
    item: &SqlComputedProjectionItem,
    other: &Value,
) -> QueryError {
    QueryError::unsupported_query(format!(
        "{}({}) requires text input, found {other:?}",
        item.transform.label(),
        item.source_field,
    ))
}

// Resolve the optional text literal argument used by the binary text helpers.
fn computed_sql_projection_text_literal(
    item: &SqlComputedProjectionItem,
) -> Result<Option<&str>, QueryError> {
    match item.literal.as_ref() {
        Some(Value::Null) => Ok(None),
        Some(Value::Text(text)) => Ok(Some(text.as_str())),
        Some(other) => Err(QueryError::unsupported_query(format!(
            "{}({}, ...) requires text literal argument, found {other:?}",
            item.transform.label(),
            item.source_field,
        ))),
        None => Err(QueryError::invariant(format!(
            "{} projection item was missing its literal argument",
            item.transform.label(),
        ))),
    }
}

// Resolve the second optional text literal used by `REPLACE`.
fn computed_sql_projection_second_text_literal(
    item: &SqlComputedProjectionItem,
) -> Result<Option<&str>, QueryError> {
    match item.literal2.as_ref() {
        Some(Value::Null) => Ok(None),
        Some(Value::Text(text)) => Ok(Some(text.as_str())),
        Some(other) => Err(QueryError::unsupported_query(format!(
            "{}({}, ..., ...) requires text literal argument, found {other:?}",
            item.transform.label(),
            item.source_field,
        ))),
        None => Err(QueryError::invariant(format!(
            "{} projection item was missing its second literal argument",
            item.transform.label(),
        ))),
    }
}

// Resolve one integer-like literal used by the numeric text projection
// helpers.
fn computed_sql_projection_numeric_literal(
    label: &'static str,
    value: Option<&Value>,
) -> Result<Option<i64>, QueryError> {
    match value {
        Some(Value::Null) => Ok(None),
        Some(Value::Int(value)) => Ok(Some(*value)),
        Some(Value::Uint(value)) => Ok(Some(i64::try_from(*value).unwrap_or(i64::MAX))),
        Some(other) => Err(QueryError::unsupported_query(format!(
            "computed SQL projection numeric {label} requires integer or NULL literal, found {other:?}",
        ))),
        None if label == "length" => Ok(None),
        None => Err(QueryError::invariant(format!(
            "computed SQL projection item was missing its {label} literal",
        ))),
    }
}

// Apply one validated numeric text transform using the current narrow
// session-owned SQL projection contract.
fn apply_numeric_text_projection(
    text: &str,
    item: &SqlComputedProjectionItem,
) -> Result<Value, QueryError> {
    match item.transform {
        SqlComputedProjectionTransform::Left => {
            let len = computed_sql_projection_numeric_literal("length", item.literal.as_ref())?;

            Ok(match len {
                Some(len) => Value::Text(left_chars(text, len)),
                None => Value::Null,
            })
        }
        SqlComputedProjectionTransform::Right => {
            let len = computed_sql_projection_numeric_literal("length", item.literal.as_ref())?;

            Ok(match len {
                Some(len) => Value::Text(right_chars(text, len)),
                None => Value::Null,
            })
        }
        SqlComputedProjectionTransform::Substring => {
            let start = computed_sql_projection_numeric_literal("start", item.literal.as_ref())?;
            let len = computed_sql_projection_numeric_literal("length", item.literal2.as_ref())?;

            Ok(match start {
                Some(start) => Value::Text(substring_1_based(text, start, len)),
                None => Value::Null,
            })
        }
        _ => Err(QueryError::invariant(
            "numeric text projection helper received a non-numeric transform",
        )),
    }
}

// Return the SQL-style one-based character position of `needle` in `haystack`.
// Returns `0` when the substring is absent.
fn text_position_1_based(haystack: &str, needle: &str) -> u64 {
    let Some(byte_index) = haystack.find(needle) else {
        return 0;
    };
    let char_offset = haystack[..byte_index].chars().count();

    u64::try_from(char_offset)
        .unwrap_or(u64::MAX)
        .saturating_add(1)
}

// Return the first `count` characters from `text` using character semantics.
fn left_chars(text: &str, count: i64) -> String {
    if count <= 0 {
        return String::new();
    }

    text.chars()
        .take(usize::try_from(count).unwrap_or(usize::MAX))
        .collect()
}

// Return the last `count` characters from `text` using character semantics.
fn right_chars(text: &str, count: i64) -> String {
    if count <= 0 {
        return String::new();
    }

    let count = usize::try_from(count).unwrap_or(usize::MAX);
    let total = text.chars().count();
    let skip = total.saturating_sub(count);

    text.chars().skip(skip).collect()
}

// Apply the narrow SQL-style `SUBSTRING(text, start, len?)` contract using
// 1-based character indexing.
fn substring_1_based(text: &str, start: i64, len: Option<i64>) -> String {
    if start <= 0 {
        return String::new();
    }
    if matches!(len, Some(length) if length <= 0) {
        return String::new();
    }

    let start_index = usize::try_from(start.saturating_sub(1)).unwrap_or(usize::MAX);
    let chars = text.chars().skip(start_index);

    match len {
        Some(length) => chars
            .take(usize::try_from(length).unwrap_or(usize::MAX))
            .collect(),
        None => chars.collect(),
    }
}

// Apply one nullable boolean text predicate after resolving the shared literal
// contract for the binary SQL text helpers.
fn apply_binary_text_predicate_projection(
    text: &str,
    item: &SqlComputedProjectionItem,
    predicate: impl FnOnce(&str, &str) -> bool,
) -> Result<Value, QueryError> {
    let literal = computed_sql_projection_text_literal(item)?;

    Ok(match literal {
        Some(needle) => Value::Bool(predicate(text, needle)),
        None => Value::Null,
    })
}

// Apply one non-null text transform after the structural field load has
// already guaranteed declaration order and row shape.
fn apply_non_null_computed_text_projection(
    text: String,
    item: &SqlComputedProjectionItem,
) -> Result<Value, QueryError> {
    match item.transform {
        SqlComputedProjectionTransform::Trim => Ok(Value::Text(text.trim().to_string())),
        SqlComputedProjectionTransform::Ltrim => Ok(Value::Text(text.trim_start().to_string())),
        SqlComputedProjectionTransform::Rtrim => Ok(Value::Text(text.trim_end().to_string())),
        SqlComputedProjectionTransform::Lower => Ok(Value::Text(text.to_lowercase())),
        SqlComputedProjectionTransform::Upper => Ok(Value::Text(text.to_uppercase())),
        SqlComputedProjectionTransform::Length => {
            let len = u64::try_from(text.chars().count()).unwrap_or(u64::MAX);

            Ok(Value::Uint(len))
        }
        SqlComputedProjectionTransform::Left
        | SqlComputedProjectionTransform::Right
        | SqlComputedProjectionTransform::Substring => {
            apply_numeric_text_projection(text.as_str(), item)
        }
        SqlComputedProjectionTransform::StartsWith => {
            apply_binary_text_predicate_projection(text.as_str(), item, |text, needle| {
                text.starts_with(needle)
            })
        }
        SqlComputedProjectionTransform::EndsWith => {
            apply_binary_text_predicate_projection(text.as_str(), item, |text, needle| {
                text.ends_with(needle)
            })
        }
        SqlComputedProjectionTransform::Contains => {
            apply_binary_text_predicate_projection(text.as_str(), item, |text, needle| {
                text.contains(needle)
            })
        }
        SqlComputedProjectionTransform::Position => {
            let literal = computed_sql_projection_text_literal(item)?;

            Ok(match literal {
                Some(needle) => Value::Uint(text_position_1_based(text.as_str(), needle)),
                None => Value::Null,
            })
        }
        SqlComputedProjectionTransform::Replace => {
            let from = computed_sql_projection_text_literal(item)?;
            let to = computed_sql_projection_second_text_literal(item)?;

            Ok(match (from, to) {
                (Some(from), Some(to)) => Value::Text(text.replace(from, to)),
                _ => Value::Null,
            })
        }
        SqlComputedProjectionTransform::Field => Ok(Value::Text(text)),
    }
}

// Apply one computed SQL projection item to one already-loaded source cell.
fn apply_computed_sql_projection_value(
    value: Value,
    item: &SqlComputedProjectionItem,
) -> Result<Value, QueryError> {
    match item.transform {
        SqlComputedProjectionTransform::Field => Ok(value),
        SqlComputedProjectionTransform::Trim
        | SqlComputedProjectionTransform::Ltrim
        | SqlComputedProjectionTransform::Rtrim
        | SqlComputedProjectionTransform::Lower
        | SqlComputedProjectionTransform::Upper
        | SqlComputedProjectionTransform::Length
        | SqlComputedProjectionTransform::Left
        | SqlComputedProjectionTransform::Right
        | SqlComputedProjectionTransform::StartsWith
        | SqlComputedProjectionTransform::EndsWith
        | SqlComputedProjectionTransform::Contains
        | SqlComputedProjectionTransform::Position
        | SqlComputedProjectionTransform::Replace
        | SqlComputedProjectionTransform::Substring => match value {
            Value::Null => Ok(Value::Null),
            Value::Text(text) => apply_non_null_computed_text_projection(text, item),
            other => Err(computed_sql_projection_text_input_error(item, &other)),
        },
    }
}

// Apply one computed SQL projection plan to one field-loaded SQL payload while
// preserving row order and row count.
pub(in crate::db::session::sql::computed_projection) fn apply_computed_sql_projection_payload(
    payload: SqlProjectionPayload,
    plan: &SqlComputedProjectionPlan,
) -> Result<SqlProjectionPayload, QueryError> {
    let (_, rows, row_count) = payload.into_parts();
    let mut projected_rows = Vec::with_capacity(rows.len());

    // Phase 1: transform each base row cell-by-cell in declaration order.
    for row in rows {
        if row.len() != plan.items.len() {
            return Err(QueryError::invariant(
                "computed SQL projection row arity did not match session transform plan",
            ));
        }

        let mut projected_row = Vec::with_capacity(row.len());
        for (value, item) in row.into_iter().zip(plan.items.iter()) {
            projected_row.push(apply_computed_sql_projection_value(value, item)?);
        }
        projected_rows.push(projected_row);
    }

    // Phase 2: replace the base field labels with the requested computed
    // projection labels at the final session SQL boundary.
    let columns = plan
        .items
        .iter()
        .map(|item| item.output_label.clone())
        .collect::<Vec<_>>();

    Ok(SqlProjectionPayload::new(
        columns,
        projected_rows,
        row_count,
    ))
}
