//! Module: query::plan::semantics::access_projection
//! Responsibility: project access-plan/access-path semantics into diagnostics-facing shapes.
//! Does not own: access-path construction or planner route-selection decisions.
//! Boundary: provides visitor-based projection adapters for explain/diagnostic consumers.

use crate::{
    db::{
        access::{AccessPath, AccessPlan, SemanticIndexAccessContract},
        query::explain::ExplainAccessPath,
    },
    model::index::IndexKeyItemsRef,
    value::Value,
};
use std::{fmt::Write, ops::Bound};

///
/// AccessPlanProjection
///
/// Shared visitor for projecting `AccessPlan` / `AccessPath` into
/// diagnostics-specific representations.
///

pub(in crate::db) trait AccessPlanProjection<K> {
    type Output;

    fn by_key(&mut self, key: &K) -> Self::Output;
    fn by_keys(&mut self, keys: &[K]) -> Self::Output;
    fn key_range(&mut self, start: &K, end: &K) -> Self::Output;
    fn index_prefix(
        &mut self,
        index_name: &str,
        index_fields: &[String],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output;
    fn index_multi_lookup(
        &mut self,
        index_name: &str,
        index_fields: &[String],
        values: &[Value],
    ) -> Self::Output;
    fn index_range(
        &mut self,
        index_name: &str,
        index_fields: &[String],
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Self::Output;
    fn full_scan(&mut self) -> Self::Output;
    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output;
    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output;
}

/// Project an access plan by exhaustively walking canonical access variants.
pub(in crate::db) fn project_access_plan<K, P>(
    plan: &AccessPlan<K>,
    projection: &mut P,
) -> P::Output
where
    P: AccessPlanProjection<K>,
{
    plan.project(projection)
}

impl<K> AccessPlan<K> {
    // Project this plan by recursively visiting all access nodes.
    fn project<P>(&self, projection: &mut P) -> P::Output
    where
        P: AccessPlanProjection<K>,
    {
        match self {
            Self::Path(path) => path.project(projection),
            Self::Union(children) => {
                let child_projections =
                    project_projection_children(children.iter(), projection, Self::project);

                projection.union(child_projections)
            }
            Self::Intersection(children) => {
                let child_projections =
                    project_projection_children(children.iter(), projection, Self::project);

                projection.intersection(child_projections)
            }
        }
    }
}

impl<K> AccessPath<K> {
    // Project one concrete path variant via the shared projection surface.
    fn project<P>(&self, projection: &mut P) -> P::Output
    where
        P: AccessPlanProjection<K>,
    {
        match self {
            Self::ByKey(key) => projection.by_key(key),
            Self::ByKeys(keys) => projection.by_keys(keys),
            Self::KeyRange { start, end } => projection.key_range(start, end),
            Self::IndexPrefix { index, values } => {
                let fields = index_contract_key_fields(index);

                projection.index_prefix(index.name(), fields.as_slice(), values.len(), values)
            }
            Self::IndexMultiLookup { index, values } => {
                let fields = index_contract_key_fields(index);

                projection.index_multi_lookup(index.name(), fields.as_slice(), values)
            }
            Self::IndexRange { spec } => {
                let contract = spec.index();
                let fields = index_contract_key_fields(&contract);

                projection.index_range(
                    contract.name(),
                    fields.as_slice(),
                    spec.prefix_values().len(),
                    spec.prefix_values(),
                    spec.lower(),
                    spec.upper(),
                )
            }
            Self::FullScan => projection.full_scan(),
        }
    }
}

fn index_contract_key_fields(index: &SemanticIndexAccessContract) -> Vec<String> {
    match index.key_items() {
        crate::db::access::SemanticIndexKeyItemsRef::Fields(fields) => fields.to_vec(),
        crate::db::access::SemanticIndexKeyItemsRef::Accepted(items) => items
            .iter()
            .map(|item| item.as_ref().field().to_string())
            .collect(),
        crate::db::access::SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Fields(fields)) => {
            fields.iter().copied().map(str::to_string).collect()
        }
        crate::db::access::SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Items(items)) => {
            items.iter().map(|item| item.field().to_string()).collect()
        }
    }
}

pub(in crate::db) fn project_explain_access_path<P>(
    access: &ExplainAccessPath,
    projection: &mut P,
) -> P::Output
where
    P: AccessPlanProjection<Value>,
{
    match access {
        ExplainAccessPath::ByKey { key } => projection.by_key(key),
        ExplainAccessPath::ByKeys { keys } => projection.by_keys(keys),
        ExplainAccessPath::KeyRange { start, end } => projection.key_range(start, end),
        ExplainAccessPath::IndexPrefix {
            name,
            fields,
            prefix_len,
            values,
        } => projection.index_prefix(name, fields, *prefix_len, values),
        ExplainAccessPath::IndexMultiLookup {
            name,
            fields,
            values,
        } => projection.index_multi_lookup(name, fields, values),
        ExplainAccessPath::IndexRange {
            name,
            fields,
            prefix_len,
            prefix,
            lower,
            upper,
        } => projection.index_range(name, fields, *prefix_len, prefix, lower, upper),
        ExplainAccessPath::FullScan => projection.full_scan(),
        ExplainAccessPath::Union(children) => {
            let child_projections = project_projection_children(
                children.iter(),
                projection,
                project_explain_access_path,
            );

            projection.union(child_projections)
        }
        ExplainAccessPath::Intersection(children) => {
            let child_projections = project_projection_children(
                children.iter(),
                projection,
                project_explain_access_path,
            );

            projection.intersection(child_projections)
        }
    }
}

///
/// AccessStrategyLabelProjection
///
/// Shared projection adapter that renders one stable label for canonical
/// access-plan and explain-access variants from the same projection contract.
/// This keeps access strategy label ownership on one semantic seam instead of
/// duplicating the label ladder in planner and explain consumers.
///

struct AccessStrategyLabelProjection;

impl<K> AccessPlanProjection<K> for AccessStrategyLabelProjection {
    type Output = String;

    fn by_key(&mut self, _key: &K) -> Self::Output {
        "ByKey".to_string()
    }

    fn by_keys(&mut self, _keys: &[K]) -> Self::Output {
        "ByKeys".to_string()
    }

    fn key_range(&mut self, _start: &K, _end: &K) -> Self::Output {
        "KeyRange".to_string()
    }

    fn index_prefix(
        &mut self,
        index_name: &str,
        _index_fields: &[String],
        _prefix_len: usize,
        _values: &[Value],
    ) -> Self::Output {
        let mut label = String::new();
        let _ = write!(&mut label, "IndexPrefix({index_name})");

        label
    }

    fn index_multi_lookup(
        &mut self,
        index_name: &str,
        _index_fields: &[String],
        _values: &[Value],
    ) -> Self::Output {
        let mut label = String::new();
        let _ = write!(&mut label, "IndexMultiLookup({index_name})");

        label
    }

    fn index_range(
        &mut self,
        index_name: &str,
        _index_fields: &[String],
        _prefix_len: usize,
        _prefix: &[Value],
        _lower: &Bound<Value>,
        _upper: &Bound<Value>,
    ) -> Self::Output {
        let mut label = String::new();
        let _ = write!(&mut label, "IndexRange({index_name})");

        label
    }

    fn full_scan(&mut self) -> Self::Output {
        "FullScan".to_string()
    }

    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
        let mut label = String::new();
        let _ = write!(&mut label, "Union({})", children.len());

        label
    }

    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
        let mut label = String::new();
        let _ = write!(&mut label, "Intersection({})", children.len());

        label
    }
}

/// Render one stable planner-owned access label without routing through explain transport.
pub(in crate::db) fn access_plan_label<K>(plan: &AccessPlan<K>) -> String {
    project_access_plan(plan, &mut AccessStrategyLabelProjection)
}

/// Render one stable explain access label from the canonical explain-access DTO.
pub(in crate::db) fn explain_access_strategy_label(access: &ExplainAccessPath) -> String {
    project_explain_access_path(access, &mut AccessStrategyLabelProjection)
}

///
/// ExplainAccessKindProjection
///
/// Shared explain-access classifier for consumers that only need one stable
/// access-kind code from the transport DTO surface.
///

struct ExplainAccessKindProjection;

impl AccessPlanProjection<Value> for ExplainAccessKindProjection {
    type Output = &'static str;

    fn by_key(&mut self, _key: &Value) -> Self::Output {
        "by_key"
    }

    fn by_keys(&mut self, keys: &[Value]) -> Self::Output {
        if keys.is_empty() {
            "empty_access_contract"
        } else {
            "by_keys"
        }
    }

    fn key_range(&mut self, _start: &Value, _end: &Value) -> Self::Output {
        "key_range"
    }

    fn index_prefix(
        &mut self,
        _index_name: &str,
        _index_fields: &[String],
        _prefix_len: usize,
        _values: &[Value],
    ) -> Self::Output {
        "index_prefix"
    }

    fn index_multi_lookup(
        &mut self,
        _index_name: &str,
        _index_fields: &[String],
        _values: &[Value],
    ) -> Self::Output {
        "index_multi_lookup"
    }

    fn index_range(
        &mut self,
        _index_name: &str,
        _index_fields: &[String],
        _prefix_len: usize,
        _prefix: &[Value],
        _lower: &Bound<Value>,
        _upper: &Bound<Value>,
    ) -> Self::Output {
        "index_range"
    }

    fn full_scan(&mut self) -> Self::Output {
        "full_scan"
    }

    fn union(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        "union"
    }

    fn intersection(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        "intersection"
    }
}

/// Classify one explain access DTO into the stable access-kind code used by
/// intent/debug labels without rebuilding a local branch ladder elsewhere.
pub(in crate::db) fn explain_access_kind_label(access: &ExplainAccessPath) -> &'static str {
    project_explain_access_path(access, &mut ExplainAccessKindProjection)
}

// Recurse over one child collection with the caller-owned projection adapter so
// access-plan and explain-path walkers share the same union/intersection child
// traversal contract.
fn project_projection_children<'a, T: 'a, P, I, F, O>(
    children: I,
    projection: &mut P,
    project_child: F,
) -> Vec<O>
where
    I: Iterator<Item = &'a T>,
    F: Fn(&'a T, &mut P) -> O,
{
    children
        .map(|child| project_child(child, projection))
        .collect()
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
