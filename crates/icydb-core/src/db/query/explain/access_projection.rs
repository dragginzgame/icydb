//! Module: query::explain::access_projection
//! Responsibility: access-path projection adapters for EXPLAIN.
//! Does not own: logical plan policy or execution descriptor rendering.
//! Boundary: planner access path -> explain access DTOs/json adapters.

use crate::{
    db::{
        access::AccessPlan,
        query::{
            access::{AccessPathVisitor, visit_explain_access_path},
            explain::{ExplainAccessPath, writer::JsonWriter},
            plan::{AccessPlanProjection, project_access_plan},
        },
    },
    traits::FieldValue,
    value::Value,
};
use std::ops::Bound;

///
/// ExplainJsonVisitor
///
/// Visitor that renders one `ExplainAccessPath` subtree into stable JSON.
///

struct ExplainJsonVisitor<'a> {
    out: &'a mut String,
}

impl AccessPathVisitor<()> for ExplainJsonVisitor<'_> {
    fn visit_by_key(&mut self, key: &Value) {
        let mut object = JsonWriter::begin_object(self.out);
        object.field_str("type", "ByKey");
        object.field_value_debug("key", key);
        object.finish();
    }

    fn visit_by_keys(&mut self, keys: &[Value]) {
        let mut object = JsonWriter::begin_object(self.out);
        object.field_str("type", "ByKeys");
        object.field_debug_slice("keys", keys);
        object.finish();
    }

    fn visit_key_range(&mut self, start: &Value, end: &Value) {
        let mut object = JsonWriter::begin_object(self.out);
        object.field_str("type", "KeyRange");
        object.field_value_debug("start", start);
        object.field_value_debug("end", end);
        object.finish();
    }

    fn visit_index_prefix(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) {
        let mut object = JsonWriter::begin_object(self.out);
        object.field_str("type", "IndexPrefix");
        object.field_str("name", name);
        object.field_str_slice("fields", fields);
        object.field_u64("prefix_len", prefix_len as u64);
        object.field_debug_slice("values", values);
        object.finish();
    }

    fn visit_index_multi_lookup(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        values: &[Value],
    ) {
        let mut object = JsonWriter::begin_object(self.out);
        object.field_str("type", "IndexMultiLookup");
        object.field_str("name", name);
        object.field_str_slice("fields", fields);
        object.field_debug_slice("values", values);
        object.finish();
    }

    fn visit_index_range(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) {
        let mut object = JsonWriter::begin_object(self.out);
        object.field_str("type", "IndexRange");
        object.field_str("name", name);
        object.field_str_slice("fields", fields);
        object.field_u64("prefix_len", prefix_len as u64);
        object.field_debug_slice("prefix", prefix);
        object.field_value_debug("lower", lower);
        object.field_value_debug("upper", upper);
        object.finish();
    }

    fn visit_full_scan(&mut self) {
        let mut object = JsonWriter::begin_object(self.out);
        object.field_str("type", "FullScan");
        object.finish();
    }

    fn visit_union(&mut self, children: &[ExplainAccessPath]) {
        let mut object = JsonWriter::begin_object(self.out);
        object.field_str("type", "Union");
        object.field_with("children", |out| {
            out.push('[');
            for (index, child) in children.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                let mut visitor = ExplainJsonVisitor { out };
                visit_explain_access_path(child, &mut visitor);
            }
            out.push(']');
        });
        object.finish();
    }

    fn visit_intersection(&mut self, children: &[ExplainAccessPath]) {
        let mut object = JsonWriter::begin_object(self.out);
        object.field_str("type", "Intersection");
        object.field_with("children", |out| {
            out.push('[');
            for (index, child) in children.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                let mut visitor = ExplainJsonVisitor { out };
                visit_explain_access_path(child, &mut visitor);
            }
            out.push(']');
        });
        object.finish();
    }
}

pub(in crate::db::query::explain) fn write_access_json(
    access: &ExplainAccessPath,
    out: &mut String,
) {
    let mut visitor = ExplainJsonVisitor { out };
    visit_explain_access_path(access, &mut visitor);
}

pub(in crate::db::query::explain) fn access_strategy_label(access: &ExplainAccessPath) -> String {
    struct ExplainLabelVisitor;

    impl AccessPathVisitor<String> for ExplainLabelVisitor {
        fn visit_by_key(&mut self, _key: &Value) -> String {
            "ByKey".to_string()
        }

        fn visit_by_keys(&mut self, _keys: &[Value]) -> String {
            "ByKeys".to_string()
        }

        fn visit_key_range(&mut self, _start: &Value, _end: &Value) -> String {
            "KeyRange".to_string()
        }

        fn visit_index_prefix(
            &mut self,
            name: &'static str,
            _fields: &[&'static str],
            _prefix_len: usize,
            _values: &[Value],
        ) -> String {
            format!("IndexPrefix({name})")
        }

        fn visit_index_multi_lookup(
            &mut self,
            name: &'static str,
            _fields: &[&'static str],
            _values: &[Value],
        ) -> String {
            format!("IndexMultiLookup({name})")
        }

        fn visit_index_range(
            &mut self,
            name: &'static str,
            _fields: &[&'static str],
            _prefix_len: usize,
            _prefix: &[Value],
            _lower: &Bound<Value>,
            _upper: &Bound<Value>,
        ) -> String {
            format!("IndexRange({name})")
        }

        fn visit_full_scan(&mut self) -> String {
            "FullScan".to_string()
        }

        fn visit_union(&mut self, children: &[ExplainAccessPath]) -> String {
            format!("Union({})", children.len())
        }

        fn visit_intersection(&mut self, children: &[ExplainAccessPath]) -> String {
            format!("Intersection({})", children.len())
        }
    }

    let mut visitor = ExplainLabelVisitor;
    visit_explain_access_path(access, &mut visitor)
}

struct ExplainAccessProjection;

impl<K> AccessPlanProjection<K> for ExplainAccessProjection
where
    K: FieldValue,
{
    type Output = ExplainAccessPath;

    fn by_key(&mut self, key: &K) -> Self::Output {
        ExplainAccessPath::ByKey {
            key: key.to_value(),
        }
    }

    fn by_keys(&mut self, keys: &[K]) -> Self::Output {
        ExplainAccessPath::ByKeys {
            keys: keys.iter().map(FieldValue::to_value).collect(),
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
        lower: &Bound<Value>,
        upper: &Bound<Value>,
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

impl ExplainAccessPath {
    pub(in crate::db) fn from_access_plan<K>(access: &AccessPlan<K>) -> Self
    where
        K: FieldValue,
    {
        let mut projection = ExplainAccessProjection;
        project_access_plan(access, &mut projection)
    }
}
