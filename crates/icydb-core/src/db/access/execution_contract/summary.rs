use crate::{
    db::access::execution_contract::{
        ExecutableAccessNode, ExecutableAccessPath, ExecutableAccessPlan, ExecutionPathPayload,
    },
    model::index::IndexModel,
    value::Value,
};
use std::{fmt, ops::Bound};

pub(in crate::db::access::execution_contract) fn summarize_executable_access_plan<K>(
    plan: &ExecutableAccessPlan<'_, K>,
) -> String
where
    K: fmt::Debug,
{
    match plan.node() {
        ExecutableAccessNode::Path(path) => summarize_executable_access_path(path),
        ExecutableAccessNode::Union(children) => {
            format!("Union({})", summarize_composite_children(children))
        }
        ExecutableAccessNode::Intersection(children) => {
            format!("Intersection({})", summarize_composite_children(children))
        }
    }
}

fn summarize_composite_children<K>(children: &[ExecutableAccessPlan<'_, K>]) -> String
where
    K: fmt::Debug,
{
    let preview_len = children.len().min(3);
    let mut preview = Vec::with_capacity(preview_len);
    for child in children.iter().take(preview_len) {
        preview.push(summarize_executable_access_plan(child));
    }

    if children.len() > preview_len {
        preview.push(format!("... +{} more", children.len() - preview_len));
    }

    preview.join(", ")
}

fn summarize_executable_access_path<K>(path: &ExecutableAccessPath<'_, K>) -> String
where
    K: fmt::Debug,
{
    match path.payload() {
        ExecutionPathPayload::ByKey(key) => format!("IndexLookup(pk={key:?})"),
        ExecutionPathPayload::ByKeys(keys) => format!("IndexLookupMany(pk_count={})", keys.len()),
        ExecutionPathPayload::KeyRange { start, end } => {
            format!("PrimaryKeyRange([{start:?}, {end:?}))")
        }
        ExecutionPathPayload::IndexPrefix => {
            if let Some((index, prefix_len)) = path.index_prefix_details() {
                if prefix_len == 0 {
                    format!("IndexPrefix({})", index.name())
                } else {
                    format!("IndexPrefix({} prefix_len={prefix_len})", index.name())
                }
            } else {
                "IndexPrefix".to_string()
            }
        }
        ExecutionPathPayload::IndexMultiLookup { value_count } => {
            if let Some((index, _)) = path.index_prefix_details() {
                format!("IndexMultiLookup({} values={value_count})", index.name())
            } else {
                format!("IndexMultiLookup(values={value_count})")
            }
        }
        ExecutionPathPayload::IndexRange {
            prefix_values,
            lower,
            upper,
        } => {
            if let Some((index, prefix_len)) = path.index_range_details() {
                summarize_index_range_with_model(index, prefix_len, prefix_values, lower, upper)
            } else {
                format!(
                    "IndexRange(prefix={prefix_values:?} {})",
                    summarize_interval(lower, upper),
                )
            }
        }
        ExecutionPathPayload::FullScan => "FullScan".to_string(),
    }
}

fn summarize_index_range_with_model(
    index: IndexModel,
    prefix_len: usize,
    prefix_values: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> String {
    let prefix = summarize_index_prefix_terms(index.fields(), prefix_values);
    let interval = summarize_interval(lower, upper);

    if let Some(range_field) = index.fields().get(prefix_len) {
        if prefix.is_empty() {
            format!("IndexRange({range_field} {interval})")
        } else {
            format!("IndexRange({prefix}; {range_field} {interval})")
        }
    } else if prefix.is_empty() {
        format!("IndexRange({interval})")
    } else {
        format!("IndexRange({prefix}; {interval})")
    }
}

fn summarize_index_prefix_terms(index_fields: &[&'static str], values: &[Value]) -> String {
    index_fields
        .iter()
        .copied()
        .zip(values.iter())
        .map(|(field, value)| format!("{field}={}", summarize_value(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn summarize_interval(lower: &Bound<Value>, upper: &Bound<Value>) -> String {
    let (lower_bracket, lower_value) = match lower {
        Bound::Included(value) => ("[", summarize_value(value)),
        Bound::Excluded(value) => ("(", summarize_value(value)),
        Bound::Unbounded => ("(", "-inf".to_string()),
    };
    let (upper_value, upper_bracket) = match upper {
        Bound::Included(value) => (summarize_value(value), "]"),
        Bound::Excluded(value) => (summarize_value(value), ")"),
        Bound::Unbounded => ("+inf".to_string(), ")"),
    };

    format!("{lower_bracket}{lower_value}, {upper_value}{upper_bracket}")
}

fn summarize_value(value: &Value) -> String {
    match value {
        Value::Text(text) => format!("{text:?}"),
        _ => format!("{value:?}"),
    }
}
