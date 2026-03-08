//! Module: query::access::access_visitor
//! Responsibility: centralized explain-access variant dispatch.
//! Does not own: explain output formatting or hash token policy.
//! Boundary: one shared visitor hook for explain/projection/fingerprint consumers.

use crate::{db::query::explain::ExplainAccessPath, value::Value};
use std::ops::Bound;

///
/// AccessPathVisitor
///
/// Shared visitor contract for `ExplainAccessPath` traversal.
/// Callers implement behavior while variant branching stays centralized.
///

pub(in crate::db::query) trait AccessPathVisitor<R> {
    fn visit_by_key(&mut self, key: &Value) -> R;
    fn visit_by_keys(&mut self, keys: &[Value]) -> R;
    fn visit_key_range(&mut self, start: &Value, end: &Value) -> R;
    fn visit_index_prefix(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) -> R;
    fn visit_index_multi_lookup(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        values: &[Value],
    ) -> R;
    fn visit_index_range(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> R;
    fn visit_full_scan(&mut self) -> R;
    fn visit_union(&mut self, children: &[ExplainAccessPath]) -> R;
    fn visit_intersection(&mut self, children: &[ExplainAccessPath]) -> R;
}

/// Visit one explain access-path node through the canonical visitor surface.
pub(in crate::db::query) fn visit_explain_access_path<R, V>(
    access: &ExplainAccessPath,
    visitor: &mut V,
) -> R
where
    V: AccessPathVisitor<R>,
{
    match access {
        ExplainAccessPath::ByKey { key } => visitor.visit_by_key(key),
        ExplainAccessPath::ByKeys { keys } => visitor.visit_by_keys(keys),
        ExplainAccessPath::KeyRange { start, end } => visitor.visit_key_range(start, end),
        ExplainAccessPath::IndexPrefix {
            name,
            fields,
            prefix_len,
            values,
        } => visitor.visit_index_prefix(name, fields, *prefix_len, values),
        ExplainAccessPath::IndexMultiLookup {
            name,
            fields,
            values,
        } => visitor.visit_index_multi_lookup(name, fields, values),
        ExplainAccessPath::IndexRange {
            name,
            fields,
            prefix_len,
            prefix,
            lower,
            upper,
        } => visitor.visit_index_range(name, fields, *prefix_len, prefix, lower, upper),
        ExplainAccessPath::FullScan => visitor.visit_full_scan(),
        ExplainAccessPath::Union(children) => visitor.visit_union(children),
        ExplainAccessPath::Intersection(children) => visitor.visit_intersection(children),
    }
}
