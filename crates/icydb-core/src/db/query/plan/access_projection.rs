use crate::{
    db::query::plan::{AccessPath, AccessPlan, ExplainAccessPath},
    value::Value,
};
use std::ops::Bound;

///
/// AccessPlanProjection
///
/// Shared visitor for projecting `AccessPlan` / `AccessPath` into
/// diagnostics-specific representations.
///
pub trait AccessPlanProjection<K> {
    type Output;

    fn by_key(&mut self, key: &K) -> Self::Output;
    fn by_keys(&mut self, keys: &[K]) -> Self::Output;
    fn key_range(&mut self, start: &K, end: &K) -> Self::Output;
    fn index_prefix(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output;
    fn index_range(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
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
pub fn project_access_plan<K, P>(plan: &AccessPlan<K>, projection: &mut P) -> P::Output
where
    P: AccessPlanProjection<K>,
{
    match plan {
        AccessPlan::Path(path) => project_access_path(path, projection),
        AccessPlan::Union(children) => {
            let children = children
                .iter()
                .map(|child| project_access_plan(child, projection))
                .collect();
            projection.union(children)
        }
        AccessPlan::Intersection(children) => {
            let children = children
                .iter()
                .map(|child| project_access_plan(child, projection))
                .collect();
            projection.intersection(children)
        }
    }
}

// Project one concrete path variant via the shared projection surface.
fn project_access_path<K, P>(path: &AccessPath<K>, projection: &mut P) -> P::Output
where
    P: AccessPlanProjection<K>,
{
    match path {
        AccessPath::ByKey(key) => projection.by_key(key),
        AccessPath::ByKeys(keys) => projection.by_keys(keys),
        AccessPath::KeyRange { start, end } => projection.key_range(start, end),
        AccessPath::IndexPrefix { index, values } => {
            projection.index_prefix(index.name, index.fields, values.len(), values)
        }
        AccessPath::IndexRange {
            index,
            prefix,
            lower,
            upper,
        } => projection.index_range(index.name, index.fields, prefix.len(), prefix, lower, upper),
        AccessPath::FullScan => projection.full_scan(),
    }
}

/// Project an explain access path using the same shared access-shape visitor.
pub fn project_explain_access_path<P>(access: &ExplainAccessPath, projection: &mut P) -> P::Output
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
            let children = children
                .iter()
                .map(|child| project_explain_access_path(child, projection))
                .collect();
            projection.union(children)
        }
        ExplainAccessPath::Intersection(children) => {
            let children = children
                .iter()
                .map(|child| project_explain_access_path(child, projection))
                .collect();
            projection.intersection(children)
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{model::index::IndexModel, value::Value};

    const TEST_INDEX_FIELDS: [&str; 2] = ["group", "rank"];
    const TEST_INDEX: IndexModel = IndexModel::new(
        "tests::group_rank",
        "tests::store",
        &TEST_INDEX_FIELDS,
        false,
    );

    #[derive(Default)]
    struct AccessPlanEventProjection {
        events: Vec<&'static str>,
        union_child_counts: Vec<usize>,
        intersection_child_counts: Vec<usize>,
        seen_index: Option<(&'static str, usize, usize, usize)>,
    }

    impl AccessPlanProjection<u64> for AccessPlanEventProjection {
        type Output = ();

        fn by_key(&mut self, _key: &u64) -> Self::Output {
            self.events.push("by_key");
        }

        fn by_keys(&mut self, keys: &[u64]) -> Self::Output {
            self.events.push("by_keys");
            assert_eq!(keys, [2, 3].as_slice());
        }

        fn key_range(&mut self, start: &u64, end: &u64) -> Self::Output {
            self.events.push("key_range");
            assert_eq!((*start, *end), (4, 9));
        }

        fn index_prefix(
            &mut self,
            index_name: &'static str,
            index_fields: &[&'static str],
            prefix_len: usize,
            values: &[Value],
        ) -> Self::Output {
            self.events.push("index_prefix");
            self.seen_index = Some((index_name, index_fields.len(), prefix_len, values.len()));
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
            self.events.push("index_range");
            self.seen_index = Some((index_name, index_fields.len(), prefix_len, prefix.len()));
            assert_eq!(lower, &Bound::Included(Value::Uint(8)));
            assert_eq!(upper, &Bound::Excluded(Value::Uint(12)));
        }

        fn full_scan(&mut self) -> Self::Output {
            self.events.push("full_scan");
        }

        fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
            self.events.push("union");
            self.union_child_counts.push(children.len());
        }

        fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
            self.events.push("intersection");
            self.intersection_child_counts.push(children.len());
        }
    }

    #[test]
    fn project_access_plan_walks_canonical_access_variants() {
        let plan: AccessPlan<u64> = AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::ByKey(1)),
            AccessPlan::path(AccessPath::ByKeys(vec![2, 3])),
            AccessPlan::path(AccessPath::KeyRange { start: 4, end: 9 }),
            AccessPlan::path(AccessPath::IndexPrefix {
                index: TEST_INDEX,
                values: vec![Value::Uint(7)],
            }),
            AccessPlan::path(AccessPath::IndexRange {
                index: TEST_INDEX,
                prefix: vec![Value::Uint(7)],
                lower: Bound::Included(Value::Uint(8)),
                upper: Bound::Excluded(Value::Uint(12)),
            }),
            AccessPlan::Intersection(vec![
                AccessPlan::path(AccessPath::FullScan),
                AccessPlan::path(AccessPath::ByKey(11)),
            ]),
        ]);

        let mut projection = AccessPlanEventProjection::default();
        project_access_plan(&plan, &mut projection);

        assert_eq!(projection.union_child_counts, vec![6]);
        assert_eq!(projection.intersection_child_counts, vec![2]);
        assert_eq!(projection.seen_index, Some((TEST_INDEX.name, 2, 1, 1)));
        assert!(
            projection.events.contains(&"by_key"),
            "projection must visit by-key variants"
        );
        assert!(
            projection.events.contains(&"by_keys"),
            "projection must visit by-keys variants"
        );
        assert!(
            projection.events.contains(&"key_range"),
            "projection must visit key-range variants"
        );
        assert!(
            projection.events.contains(&"index_prefix"),
            "projection must visit index-prefix variants"
        );
        assert!(
            projection.events.contains(&"index_range"),
            "projection must visit index-range variants"
        );
        assert!(
            projection.events.contains(&"full_scan"),
            "projection must visit full-scan variants"
        );
    }

    #[derive(Default)]
    struct ExplainAccessEventProjection {
        events: Vec<&'static str>,
        union_child_counts: Vec<usize>,
        intersection_child_counts: Vec<usize>,
        seen_index: Option<(&'static str, usize, usize, usize)>,
    }

    impl AccessPlanProjection<Value> for ExplainAccessEventProjection {
        type Output = ();

        fn by_key(&mut self, key: &Value) -> Self::Output {
            self.events.push("by_key");
            assert_eq!(key, &Value::Uint(10));
        }

        fn by_keys(&mut self, keys: &[Value]) -> Self::Output {
            self.events.push("by_keys");
            assert_eq!(keys, [Value::Uint(20), Value::Uint(30)].as_slice());
        }

        fn key_range(&mut self, start: &Value, end: &Value) -> Self::Output {
            self.events.push("key_range");
            assert_eq!((start, end), (&Value::Uint(40), &Value::Uint(90)));
        }

        fn index_prefix(
            &mut self,
            index_name: &'static str,
            index_fields: &[&'static str],
            prefix_len: usize,
            values: &[Value],
        ) -> Self::Output {
            self.events.push("index_prefix");
            self.seen_index = Some((index_name, index_fields.len(), prefix_len, values.len()));
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
            self.events.push("index_range");
            self.seen_index = Some((index_name, index_fields.len(), prefix_len, prefix.len()));
            assert_eq!(lower, &Bound::Included(Value::Uint(8)));
            assert_eq!(upper, &Bound::Excluded(Value::Uint(12)));
        }

        fn full_scan(&mut self) -> Self::Output {
            self.events.push("full_scan");
        }

        fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
            self.events.push("union");
            self.union_child_counts.push(children.len());
        }

        fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
            self.events.push("intersection");
            self.intersection_child_counts.push(children.len());
        }
    }

    #[test]
    fn project_explain_access_path_walks_canonical_access_variants() {
        let access = ExplainAccessPath::Union(vec![
            ExplainAccessPath::ByKey {
                key: Value::Uint(10),
            },
            ExplainAccessPath::ByKeys {
                keys: vec![Value::Uint(20), Value::Uint(30)],
            },
            ExplainAccessPath::KeyRange {
                start: Value::Uint(40),
                end: Value::Uint(90),
            },
            ExplainAccessPath::IndexPrefix {
                name: TEST_INDEX.name,
                fields: vec!["group", "rank"],
                prefix_len: 1,
                values: vec![Value::Uint(7)],
            },
            ExplainAccessPath::IndexRange {
                name: TEST_INDEX.name,
                fields: vec!["group", "rank"],
                prefix_len: 1,
                prefix: vec![Value::Uint(7)],
                lower: Bound::Included(Value::Uint(8)),
                upper: Bound::Excluded(Value::Uint(12)),
            },
            ExplainAccessPath::Intersection(vec![
                ExplainAccessPath::FullScan,
                ExplainAccessPath::ByKey {
                    key: Value::Uint(10),
                },
            ]),
        ]);

        let mut projection = ExplainAccessEventProjection::default();
        project_explain_access_path(&access, &mut projection);

        assert_eq!(projection.union_child_counts, vec![6]);
        assert_eq!(projection.intersection_child_counts, vec![2]);
        assert_eq!(projection.seen_index, Some((TEST_INDEX.name, 2, 1, 1)));
        assert!(
            projection.events.contains(&"by_key"),
            "projection must visit by-key variants"
        );
        assert!(
            projection.events.contains(&"by_keys"),
            "projection must visit by-keys variants"
        );
        assert!(
            projection.events.contains(&"key_range"),
            "projection must visit key-range variants"
        );
        assert!(
            projection.events.contains(&"index_prefix"),
            "projection must visit index-prefix variants"
        );
        assert!(
            projection.events.contains(&"index_range"),
            "projection must visit index-range variants"
        );
        assert!(
            projection.events.contains(&"full_scan"),
            "projection must visit full-scan variants"
        );
    }
}
