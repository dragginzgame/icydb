//! Module: query::explain::access_projection
//! Responsibility: access-path projection adapters for EXPLAIN.
//! Does not own: logical plan policy or execution descriptor rendering.
//! Boundary: planner access path -> explain access DTOs/json adapters.

use crate::{
    db::{
        access::AccessPlan,
        query::{
            explain::{ExplainAccessPath, writer::JsonWriter},
            plan::{AccessPlanProjection, project_access_plan, project_explain_access_path},
        },
    },
    traits::KeyValueCodec,
    value::Value,
};

///
/// ExplainAccessProjection
///
/// Local EXPLAIN adapter that consumes the planner-owned access traversal
/// contract and projects it into the transport-facing `ExplainAccessPath` DTO.
///

struct ExplainAccessProjection;

impl<K> AccessPlanProjection<K> for ExplainAccessProjection
where
    K: KeyValueCodec,
{
    type Output = ExplainAccessPath;

    fn by_key(&mut self, key: &K) -> Self::Output {
        ExplainAccessPath::ByKey {
            key: key.to_key_value(),
        }
    }

    fn by_keys(&mut self, keys: &[K]) -> Self::Output {
        ExplainAccessPath::ByKeys {
            keys: keys.iter().map(KeyValueCodec::to_key_value).collect(),
        }
    }

    fn key_range(&mut self, start: &K, end: &K) -> Self::Output {
        ExplainAccessPath::KeyRange {
            start: start.to_key_value(),
            end: end.to_key_value(),
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

///
/// ExplainAccessJsonProjection
///
/// EXPLAIN JSON projection over the canonical explain-access DTO.
/// This keeps access JSON rendering on the same shared projection contract as
/// the rest of the access classifiers instead of maintaining another local
/// recursive `ExplainAccessPath` match tree.
///
struct ExplainAccessJsonProjection;

impl AccessPlanProjection<Value> for ExplainAccessJsonProjection {
    type Output = String;

    fn by_key(&mut self, key: &Value) -> Self::Output {
        let mut out = String::new();
        let mut object = JsonWriter::begin_object(&mut out);
        object.field_str("type", "ByKey");
        object.field_value_debug("key", key);
        object.finish();

        out
    }

    fn by_keys(&mut self, keys: &[Value]) -> Self::Output {
        let mut out = String::new();
        let mut object = JsonWriter::begin_object(&mut out);
        object.field_str("type", "ByKeys");
        object.field_debug_slice("keys", keys);
        object.finish();

        out
    }

    fn key_range(&mut self, start: &Value, end: &Value) -> Self::Output {
        let mut out = String::new();
        let mut object = JsonWriter::begin_object(&mut out);
        object.field_str("type", "KeyRange");
        object.field_value_debug("start", start);
        object.field_value_debug("end", end);
        object.finish();

        out
    }

    fn index_prefix(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        let mut out = String::new();
        let mut object = JsonWriter::begin_object(&mut out);
        object.field_str("type", "IndexPrefix");
        object.field_str("name", index_name);
        object.field_str_slice("fields", index_fields);
        object.field_u64("prefix_len", prefix_len as u64);
        object.field_debug_slice("values", values);
        object.finish();

        out
    }

    fn index_multi_lookup(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        values: &[Value],
    ) -> Self::Output {
        let mut out = String::new();
        let mut object = JsonWriter::begin_object(&mut out);
        object.field_str("type", "IndexMultiLookup");
        object.field_str("name", index_name);
        object.field_str_slice("fields", index_fields);
        object.field_debug_slice("values", values);
        object.finish();

        out
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
        let mut out = String::new();
        let mut object = JsonWriter::begin_object(&mut out);
        object.field_str("type", "IndexRange");
        object.field_str("name", index_name);
        object.field_str_slice("fields", index_fields);
        object.field_u64("prefix_len", prefix_len as u64);
        object.field_debug_slice("prefix", prefix);
        object.field_value_debug("lower", lower);
        object.field_value_debug("upper", upper);
        object.finish();

        out
    }

    fn full_scan(&mut self) -> Self::Output {
        let mut out = String::new();
        let mut object = JsonWriter::begin_object(&mut out);
        object.field_str("type", "FullScan");
        object.finish();

        out
    }

    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
        let mut out = String::new();
        let mut object = JsonWriter::begin_object(&mut out);
        object.field_str("type", "Union");
        object.field_with("children", |out| {
            out.push('[');
            for (index, child) in children.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(child);
            }
            out.push(']');
        });
        object.finish();

        out
    }

    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
        let mut out = String::new();
        let mut object = JsonWriter::begin_object(&mut out);
        object.field_str("type", "Intersection");
        object.field_with("children", |out| {
            out.push('[');
            for (index, child) in children.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(child);
            }
            out.push(']');
        });
        object.finish();

        out
    }
}

pub(in crate::db::query::explain) fn write_access_json(
    access: &ExplainAccessPath,
    out: &mut String,
) {
    out.push_str(&project_explain_access_path(
        access,
        &mut ExplainAccessJsonProjection,
    ));
}

pub(in crate::db) fn explain_access_plan<K>(access: &AccessPlan<K>) -> ExplainAccessPath
where
    K: KeyValueCodec,
{
    project_access_plan(access, &mut ExplainAccessProjection)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::query::plan::explain_access_strategy_label, value::Value};

    #[test]
    fn explain_access_strategy_label_projects_stable_render_labels() {
        assert_eq!(
            explain_access_strategy_label(&ExplainAccessPath::ByKey {
                key: Value::Uint(1),
            }),
            "ByKey",
        );
        assert_eq!(
            explain_access_strategy_label(&ExplainAccessPath::Union(vec![
                ExplainAccessPath::FullScan,
                ExplainAccessPath::ByKeys {
                    keys: vec![Value::Uint(2)],
                },
            ])),
            "Union(2)",
        );
    }
}
