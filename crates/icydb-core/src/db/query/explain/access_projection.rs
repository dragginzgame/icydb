//! Module: query::explain::access_projection
//! Responsibility: access-path projection adapters for EXPLAIN.
//! Does not own: logical plan policy or execution descriptor rendering.
//! Boundary: planner access path -> explain access DTOs/json adapters.

use crate::{
    db::{
        access::AccessPlan,
        query::{
            explain::{ExplainAccessPath, writer::JsonWriter},
            plan::{AccessPlanProjection, project_access_plan},
        },
    },
    value::Value,
};
use std::fmt::Write;

///
/// ExplainAccessProjection
///
/// Local EXPLAIN adapter that consumes the planner-owned access traversal
/// contract and projects it into the transport-facing `ExplainAccessPath` DTO.
///

struct ExplainAccessProjection;

impl<K> AccessPlanProjection<K> for ExplainAccessProjection
where
    K: crate::traits::FieldValue,
{
    type Output = ExplainAccessPath;

    fn by_key(&mut self, key: &K) -> Self::Output {
        ExplainAccessPath::ByKey {
            key: key.to_value(),
        }
    }

    fn by_keys(&mut self, keys: &[K]) -> Self::Output {
        ExplainAccessPath::ByKeys {
            keys: keys
                .iter()
                .map(crate::traits::FieldValue::to_value)
                .collect(),
        }
    }

    fn key_range(&mut self, start: &K, end: &K) -> Self::Output {
        ExplainAccessPath::KeyRange {
            start: start.to_value(),
            end: end.to_value(),
        }
    }

    fn index_prefix(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        ExplainAccessPath::IndexPrefix {
            name: index_name,
            fields: index_fields.to_vec(),
            prefix_len,
            values: values.to_vec(),
        }
    }

    fn index_multi_lookup(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        values: &[Value],
    ) -> Self::Output {
        ExplainAccessPath::IndexMultiLookup {
            name: index_name,
            fields: index_fields.to_vec(),
            values: values.to_vec(),
        }
    }

    fn index_range(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        prefix: &[Value],
        lower: &std::ops::Bound<Value>,
        upper: &std::ops::Bound<Value>,
    ) -> Self::Output {
        ExplainAccessPath::IndexRange {
            name: index_name,
            fields: index_fields.to_vec(),
            prefix_len,
            prefix: prefix.to_vec(),
            lower: lower.clone(),
            upper: upper.clone(),
        }
    }

    fn full_scan(&mut self) -> Self::Output {
        ExplainAccessPath::FullScan
    }

    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
        ExplainAccessPath::Union(children)
    }

    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
        ExplainAccessPath::Intersection(children)
    }
}

pub(in crate::db::query::explain) fn write_access_json(
    access: &ExplainAccessPath,
    out: &mut String,
) {
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
            object.field_with("children", |out| {
                out.push('[');
                for (index, child) in children.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    write_access_json(child, out);
                }
                out.push(']');
            });
            object.finish();
        }
        ExplainAccessPath::Intersection(children) => {
            let mut object = JsonWriter::begin_object(out);
            object.field_str("type", "Intersection");
            object.field_with("children", |out| {
                out.push('[');
                for (index, child) in children.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    write_access_json(child, out);
                }
                out.push(']');
            });
            object.finish();
        }
    }
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

pub(in crate::db) fn explain_access_plan<K>(access: &AccessPlan<K>) -> ExplainAccessPath
where
    K: crate::traits::FieldValue,
{
    project_access_plan(access, &mut ExplainAccessProjection)
}
