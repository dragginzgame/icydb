//! Module: access::execution_contract::summary
//! Responsibility: debug/diagnostic summarization of executable access contracts.
//! Does not own: access contract construction or runtime traversal semantics.
//! Boundary: renders executable access plans/paths into stable human-readable summaries.

use crate::{
    db::access::execution_contract::{
        ExecutableAccessNode, ExecutableAccessPlan, ExecutionPathPayload,
    },
    value::Value,
};
use std::{fmt, fmt::Write as _, ops::Bound};

pub(in crate::db) fn summarize_executable_access_plan<K>(
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

fn summarize_executable_access_path<K>(path: &ExecutionPathPayload<'_, K>) -> String
where
    K: fmt::Debug,
{
    match path {
        ExecutionPathPayload::ByKey(key) => format!("IndexLookup(pk={key:?})"),
        ExecutionPathPayload::ByKeys(keys) => format!("IndexLookupMany(pk_count={})", keys.len()),
        ExecutionPathPayload::KeyRange { start, end } => {
            format!("PrimaryKeyRange([{start:?}, {end:?}))")
        }
        ExecutionPathPayload::IndexPrefix { .. } => {
            if let Some(details) = path.index_prefix_details() {
                let prefix_len = details.slot_arity();
                if prefix_len == 0 {
                    format!("IndexPrefix({})", details.name())
                } else {
                    format!("IndexPrefix({} prefix_len={prefix_len})", details.name())
                }
            } else {
                "IndexPrefix".to_string()
            }
        }
        ExecutionPathPayload::IndexMultiLookup { value_count, .. } => {
            if let Some(details) = path.index_prefix_details() {
                format!("IndexMultiLookup({} values={value_count})", details.name())
            } else {
                format!("IndexMultiLookup(values={value_count})")
            }
        }
        ExecutionPathPayload::IndexRange {
            prefix_values,
            lower,
            upper,
            ..
        } => {
            if let Some(details) = path.index_range_details() {
                let prefix_len = details.slot_arity();
                let prefix = summarize_index_prefix_terms(details, prefix_values);
                let interval = summarize_interval(lower, upper);

                if let Some(range_field) = details.key_field_at(prefix_len) {
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

fn summarize_index_prefix_terms(
    details: crate::db::access::IndexShapeDetails,
    values: &[Value],
) -> String {
    let mut summary = String::new();

    for (component_index, value) in values.iter().enumerate() {
        let Some(field) = details.key_field_at(component_index) else {
            break;
        };
        if !summary.is_empty() {
            summary.push_str(", ");
        }
        write!(&mut summary, "{field}={}", summarize_value(value))
            .expect("writing to String should succeed");
    }

    summary
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
