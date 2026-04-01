//! Module: query::explain::access_projection
//! Responsibility: access-path projection adapters for EXPLAIN.
//! Does not own: logical plan policy or execution descriptor rendering.
//! Boundary: planner access path -> explain access DTOs/json adapters.

use crate::{
    db::{
        access::{AccessPathDispatch, AccessPlan, AccessPlanDispatch, dispatch_access_plan},
        query::explain::{ExplainAccessPath, writer::JsonWriter},
    },
    traits::FieldValue,
};
use std::fmt::Write;

pub(in crate::db::query::explain) fn write_access_json(
    access: &ExplainAccessPath,
    out: &mut String,
) {
    write_access_json_inner(access, out);
}

// Render the stable explain-access JSON payload directly from the final DTO
// shape instead of routing back through the generic access visitor.
fn write_access_json_inner(access: &ExplainAccessPath, out: &mut String) {
    match access {
        ExplainAccessPath::ByKey { key } => {
            let mut object = JsonWriter::begin_object(out);
            object.field_str("type", "ByKey");
            object.field_value_debug("key", key);
            object.finish();
        }
        ExplainAccessPath::ByKeys { keys } => {
            let mut object = JsonWriter::begin_object(out);
            object.field_str("type", "ByKeys");
            object.field_debug_slice("keys", keys);
            object.finish();
        }
        ExplainAccessPath::KeyRange { start, end } => {
            let mut object = JsonWriter::begin_object(out);
            object.field_str("type", "KeyRange");
            object.field_value_debug("start", start);
            object.field_value_debug("end", end);
            object.finish();
        }
        ExplainAccessPath::IndexPrefix {
            name,
            fields,
            prefix_len,
            values,
        } => {
            let mut object = JsonWriter::begin_object(out);
            object.field_str("type", "IndexPrefix");
            object.field_str("name", name);
            object.field_str_slice("fields", fields);
            object.field_u64("prefix_len", *prefix_len as u64);
            object.field_debug_slice("values", values);
            object.finish();
        }
        ExplainAccessPath::IndexMultiLookup {
            name,
            fields,
            values,
        } => {
            let mut object = JsonWriter::begin_object(out);
            object.field_str("type", "IndexMultiLookup");
            object.field_str("name", name);
            object.field_str_slice("fields", fields);
            object.field_debug_slice("values", values);
            object.finish();
        }
        ExplainAccessPath::IndexRange {
            name,
            fields,
            prefix_len,
            prefix,
            lower,
            upper,
        } => {
            let mut object = JsonWriter::begin_object(out);
            object.field_str("type", "IndexRange");
            object.field_str("name", name);
            object.field_str_slice("fields", fields);
            object.field_u64("prefix_len", *prefix_len as u64);
            object.field_debug_slice("prefix", prefix);
            object.field_value_debug("lower", lower);
            object.field_value_debug("upper", upper);
            object.finish();
        }
        ExplainAccessPath::FullScan => {
            let mut object = JsonWriter::begin_object(out);
            object.field_str("type", "FullScan");
            object.finish();
        }
        ExplainAccessPath::Union(children) => {
            let mut object = JsonWriter::begin_object(out);
            object.field_str("type", "Union");
            object.field_with("children", |out| write_access_json_children(children, out));
            object.finish();
        }
        ExplainAccessPath::Intersection(children) => {
            let mut object = JsonWriter::begin_object(out);
            object.field_str("type", "Intersection");
            object.field_with("children", |out| write_access_json_children(children, out));
            object.finish();
        }
    }
}

// Render one explain-access child list with deterministic ordering and punctuation.
fn write_access_json_children(children: &[ExplainAccessPath], out: &mut String) {
    out.push('[');
    for (index, child) in children.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_access_json_inner(child, out);
    }
    out.push(']');
}

pub(in crate::db) fn write_access_strategy_label(out: &mut String, access: &ExplainAccessPath) {
    match access {
        ExplainAccessPath::ByKey { .. } => out.push_str("ByKey"),
        ExplainAccessPath::ByKeys { .. } => out.push_str("ByKeys"),
        ExplainAccessPath::KeyRange { .. } => out.push_str("KeyRange"),
        ExplainAccessPath::IndexPrefix { name, .. } => {
            let _ = write!(out, "IndexPrefix({name})");
        }
        ExplainAccessPath::IndexMultiLookup { name, .. } => {
            let _ = write!(out, "IndexMultiLookup({name})");
        }
        ExplainAccessPath::IndexRange { name, .. } => {
            let _ = write!(out, "IndexRange({name})");
        }
        ExplainAccessPath::FullScan => out.push_str("FullScan"),
        ExplainAccessPath::Union(children) => {
            let _ = write!(out, "Union({})", children.len());
        }
        ExplainAccessPath::Intersection(children) => {
            let _ = write!(out, "Intersection({})", children.len());
        }
    }
}

fn explain_access_plan<K>(access: &AccessPlan<K>) -> ExplainAccessPath
where
    K: FieldValue,
{
    match dispatch_access_plan(access) {
        AccessPlanDispatch::Path(path) => explain_access_path_dispatch(path),
        AccessPlanDispatch::Union(children) => {
            ExplainAccessPath::Union(children.iter().map(explain_access_plan).collect())
        }
        AccessPlanDispatch::Intersection(children) => {
            ExplainAccessPath::Intersection(children.iter().map(explain_access_plan).collect())
        }
    }
}

fn explain_access_path_dispatch<K>(path: AccessPathDispatch<'_, K>) -> ExplainAccessPath
where
    K: FieldValue,
{
    match path {
        AccessPathDispatch::ByKey(key) => ExplainAccessPath::ByKey {
            key: key.to_value(),
        },
        AccessPathDispatch::ByKeys(keys) => ExplainAccessPath::ByKeys {
            keys: keys.iter().map(FieldValue::to_value).collect(),
        },
        AccessPathDispatch::KeyRange { start, end } => ExplainAccessPath::KeyRange {
            start: start.to_value(),
            end: end.to_value(),
        },
        AccessPathDispatch::IndexPrefix { index, values } => ExplainAccessPath::IndexPrefix {
            name: index.name(),
            fields: index.fields().to_vec(),
            prefix_len: values.len(),
            values: values.to_vec(),
        },
        AccessPathDispatch::IndexMultiLookup { index, values } => {
            ExplainAccessPath::IndexMultiLookup {
                name: index.name(),
                fields: index.fields().to_vec(),
                values: values.to_vec(),
            }
        }
        AccessPathDispatch::IndexRange { spec } => ExplainAccessPath::IndexRange {
            name: spec.index().name(),
            fields: spec.index().fields().to_vec(),
            prefix_len: spec.prefix_values().len(),
            prefix: spec.prefix_values().to_vec(),
            lower: spec.lower().clone(),
            upper: spec.upper().clone(),
        },
        AccessPathDispatch::FullScan => ExplainAccessPath::FullScan,
    }
}

impl ExplainAccessPath {
    pub(in crate::db) fn from_access_plan<K>(access: &AccessPlan<K>) -> Self
    where
        K: FieldValue,
    {
        explain_access_plan(access)
    }
}
