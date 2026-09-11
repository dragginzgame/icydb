//! Module: query::plan::semantics::access_projection
//! Responsibility: project access-plan/access-path semantics into diagnostics-facing shapes.
//! Does not own: access-path construction or planner route-selection decisions.
//! Boundary: provides visitor-based projection adapters for explain/diagnostic consumers.

use crate::{
    db::{
        access::{AccessPath, AccessPlan, SemanticIndexAccessContract},
        query::explain::ExplainAccessPath,
    },
    value::Value,
};
use std::{fmt, ops::Bound};

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
    fn index_prefix<'a>(
        &mut self,
        index_name: &str,
        index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output;
    fn index_multi_lookup<'a>(
        &mut self,
        index_name: &str,
        index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        values: &[Value],
    ) -> Self::Output;
    fn index_branch_set<'a>(
        &mut self,
        index_name: &str,
        index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        fixed_values: &[Value],
        branch_values: &[Value],
    ) -> Self::Output;
    fn index_range<'a>(
        &mut self,
        index_name: &str,
        index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Self::Output;
    fn full_scan(&mut self) -> Self::Output;
    // Borrowed, on-demand children let budgeted collectors admit destination
    // backing before calling `project`. Summaries may skip children; hashing
    // must preserve child-before-parent order.
    fn union<T>(
        &mut self,
        children: &[T],
        project: impl Fn(&T, &mut Self) -> Self::Output,
    ) -> Self::Output;
    fn intersection<T>(
        &mut self,
        children: &[T],
        project: impl Fn(&T, &mut Self) -> Self::Output,
    ) -> Self::Output;
}

/// Project canonical access variants, letting the visitor control child descent.
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
    // Dispatch this node; each visitor decides whether to project its children.
    fn project<P>(&self, projection: &mut P) -> P::Output
    where
        P: AccessPlanProjection<K>,
    {
        match self {
            Self::Path(path) => path.project(projection),
            Self::Union(children) => projection.union(children, Self::project),
            Self::Intersection(children) => projection.intersection(children, Self::project),
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

                projection.index_prefix(index.name(), fields, values.len(), values)
            }
            Self::IndexMultiLookup { index, values } => {
                let fields = index_contract_key_fields(index);

                projection.index_multi_lookup(index.name(), fields, values)
            }
            Self::IndexBranchSet { spec } => {
                let fields = index_contract_key_fields(spec.index_ref());

                projection.index_branch_set(
                    spec.index_ref().name(),
                    fields,
                    spec.fixed_values(),
                    spec.branch_values(),
                )
            }
            Self::IndexRange { spec } => {
                let contract = spec.index();
                let fields = index_contract_key_fields(&contract);

                projection.index_range(
                    contract.name(),
                    fields,
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

fn index_contract_key_fields(
    index: &SemanticIndexAccessContract,
) -> impl ExactSizeIterator<Item = &str> + Clone {
    index.key_items().iter().map(|item| item.as_ref().field())
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
        } => projection.index_prefix(name, fields.iter().map(String::as_str), *prefix_len, values),
        ExplainAccessPath::IndexMultiLookup {
            name,
            fields,
            values,
        } => projection.index_multi_lookup(name, fields.iter().map(String::as_str), values),
        ExplainAccessPath::IndexBranchSet {
            name,
            fields,
            fixed_values,
            branch_values,
            branch_field,
        } => {
            debug_assert_eq!(
                branch_field.as_deref(),
                fields.get(fixed_values.len()).map(String::as_str)
            );
            projection.index_branch_set(
                name,
                fields.iter().map(String::as_str),
                fixed_values,
                branch_values,
            )
        }
        ExplainAccessPath::IndexRange {
            name,
            fields,
            prefix_len,
            prefix,
            lower,
            upper,
        } => projection.index_range(
            name,
            fields.iter().map(String::as_str),
            *prefix_len,
            prefix,
            lower,
            upper,
        ),
        ExplainAccessPath::FullScan => projection.full_scan(),
        ExplainAccessPath::Union(children) => {
            projection.union(children, project_explain_access_path)
        }
        ExplainAccessPath::Intersection(children) => {
            projection.intersection(children, project_explain_access_path)
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

struct AccessStrategyLabelProjection<'a> {
    out: &'a mut dyn fmt::Write,
}

impl<K> AccessPlanProjection<K> for AccessStrategyLabelProjection<'_> {
    type Output = fmt::Result;

    fn by_key(&mut self, _key: &K) -> Self::Output {
        self.out.write_str("ByKey")
    }

    fn by_keys(&mut self, _keys: &[K]) -> Self::Output {
        self.out.write_str("ByKeys")
    }

    fn key_range(&mut self, _start: &K, _end: &K) -> Self::Output {
        self.out.write_str("KeyRange")
    }

    fn index_prefix<'a>(
        &mut self,
        index_name: &str,
        _index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        _prefix_len: usize,
        _values: &[Value],
    ) -> Self::Output {
        write!(self.out, "IndexPrefix({index_name})")
    }

    fn index_multi_lookup<'a>(
        &mut self,
        index_name: &str,
        _index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        _values: &[Value],
    ) -> Self::Output {
        write!(self.out, "IndexMultiLookup({index_name})")
    }

    fn index_branch_set<'a>(
        &mut self,
        index_name: &str,
        _index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        _fixed_values: &[Value],
        _branch_values: &[Value],
    ) -> Self::Output {
        write!(self.out, "IndexBranchSet({index_name})")
    }

    fn index_range<'a>(
        &mut self,
        index_name: &str,
        _index_fields: impl ExactSizeIterator<Item = &'a str> + Clone,
        _prefix_len: usize,
        _prefix: &[Value],
        _lower: &Bound<Value>,
        _upper: &Bound<Value>,
    ) -> Self::Output {
        write!(self.out, "IndexRange({index_name})")
    }

    fn full_scan(&mut self) -> Self::Output {
        self.out.write_str("FullScan")
    }

    fn union<T>(
        &mut self,
        children: &[T],
        _project: impl Fn(&T, &mut Self) -> Self::Output,
    ) -> Self::Output {
        write!(self.out, "Union({})", children.len())
    }

    fn intersection<T>(
        &mut self,
        children: &[T],
        _project: impl Fn(&T, &mut Self) -> Self::Output,
    ) -> Self::Output {
        write!(self.out, "Intersection({})", children.len())
    }
}

/// Render one stable planner-owned access label without routing through explain transport.
#[cfg(feature = "sql")]
pub(in crate::db) fn access_plan_label<K>(plan: &AccessPlan<K>) -> String {
    let mut label = String::new();
    // String writes are infallible; both sources share the streaming visitor.
    let _ = project_access_plan(plan, &mut AccessStrategyLabelProjection { out: &mut label });
    label
}

/// Write a stable access label directly into the caller's fallible destination.
pub(in crate::db) fn write_explain_access_strategy_label(
    access: &ExplainAccessPath,
    out: &mut dyn fmt::Write,
) -> fmt::Result {
    project_explain_access_path(access, &mut AccessStrategyLabelProjection { out })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_projection_labels_only_need_composite_root_and_child_count() {
        let children = [(), (), ()];
        let mut output = String::new();
        let mut projection = AccessStrategyLabelProjection { out: &mut output };
        AccessPlanProjection::<Value>::union(&mut projection, &children, |(), _| {
            panic!("label must not inspect child payloads")
        })
        .unwrap();
        assert_eq!(output, "Union(3)");
        output.clear();
        let mut projection = AccessStrategyLabelProjection { out: &mut output };
        AccessPlanProjection::<Value>::intersection(&mut projection, &children, |(), _| {
            panic!("label must not inspect child payloads")
        })
        .unwrap();
        assert_eq!(output, "Intersection(3)");
    }
}
