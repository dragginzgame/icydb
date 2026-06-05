use super::*;
use crate::{model::index::IndexModel, value::Value};

const TEST_INDEX_FIELDS: [&str; 2] = ["group", "rank"];
const TEST_INDEX: IndexModel = IndexModel::generated(
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
    seen_index: Option<(String, usize, usize, usize)>,
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
        index_name: &str,
        index_fields: &[String],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        self.events.push("index_prefix");
        self.seen_index = Some((
            index_name.to_string(),
            index_fields.len(),
            prefix_len,
            values.len(),
        ));
    }

    fn index_multi_lookup(
        &mut self,
        index_name: &str,
        index_fields: &[String],
        values: &[Value],
    ) -> Self::Output {
        self.events.push("index_multi_lookup");
        self.seen_index = Some((index_name.to_string(), index_fields.len(), 1, values.len()));
    }

    fn index_range(
        &mut self,
        index_name: &str,
        index_fields: &[String],
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Self::Output {
        self.events.push("index_range");
        self.seen_index = Some((
            index_name.to_string(),
            index_fields.len(),
            prefix_len,
            prefix.len(),
        ));
        assert_eq!(lower, &Bound::Included(Value::Nat64(8)));
        assert_eq!(upper, &Bound::Excluded(Value::Nat64(12)));
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
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                TEST_INDEX,
            ),
            values: vec![Value::Nat64(7)],
        }),
        AccessPlan::path(AccessPath::IndexMultiLookup {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                TEST_INDEX,
            ),
            values: vec![Value::Nat64(7), Value::Nat64(9)],
        }),
        AccessPlan::path(AccessPath::index_range(
            TEST_INDEX,
            vec![Value::Nat64(7)],
            Bound::Included(Value::Nat64(8)),
            Bound::Excluded(Value::Nat64(12)),
        )),
        AccessPlan::Intersection(vec![
            AccessPlan::path(AccessPath::FullScan),
            AccessPlan::path(AccessPath::ByKey(11)),
        ]),
    ]);

    let mut projection = AccessPlanEventProjection::default();
    project_access_plan(&plan, &mut projection);

    assert_eq!(projection.union_child_counts, vec![7]);
    assert_eq!(projection.intersection_child_counts, vec![2]);
    assert_eq!(
        projection.seen_index,
        Some((TEST_INDEX.name().to_string(), 2, 1, 1))
    );
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
        projection.events.contains(&"index_multi_lookup"),
        "projection must visit index-multi-lookup variants",
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
    seen_index: Option<(String, usize, usize, usize)>,
}

impl AccessPlanProjection<Value> for ExplainAccessEventProjection {
    type Output = ();

    fn by_key(&mut self, key: &Value) -> Self::Output {
        self.events.push("by_key");
        assert_eq!(key, &Value::Nat64(10));
    }

    fn by_keys(&mut self, keys: &[Value]) -> Self::Output {
        self.events.push("by_keys");
        assert_eq!(keys, [Value::Nat64(20), Value::Nat64(30)].as_slice());
    }

    fn key_range(&mut self, start: &Value, end: &Value) -> Self::Output {
        self.events.push("key_range");
        assert_eq!((start, end), (&Value::Nat64(40), &Value::Nat64(90)));
    }

    fn index_prefix(
        &mut self,
        index_name: &str,
        index_fields: &[String],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        self.events.push("index_prefix");
        self.seen_index = Some((
            index_name.to_string(),
            index_fields.len(),
            prefix_len,
            values.len(),
        ));
    }

    fn index_multi_lookup(
        &mut self,
        index_name: &str,
        index_fields: &[String],
        values: &[Value],
    ) -> Self::Output {
        self.events.push("index_multi_lookup");
        self.seen_index = Some((index_name.to_string(), index_fields.len(), 1, values.len()));
    }

    fn index_range(
        &mut self,
        index_name: &str,
        index_fields: &[String],
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Self::Output {
        self.events.push("index_range");
        self.seen_index = Some((
            index_name.to_string(),
            index_fields.len(),
            prefix_len,
            prefix.len(),
        ));
        assert_eq!(lower, &Bound::Included(Value::Nat64(8)));
        assert_eq!(upper, &Bound::Excluded(Value::Nat64(12)));
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
            key: Value::Nat64(10),
        },
        ExplainAccessPath::ByKeys {
            keys: vec![Value::Nat64(20), Value::Nat64(30)],
        },
        ExplainAccessPath::KeyRange {
            start: Value::Nat64(40),
            end: Value::Nat64(90),
        },
        ExplainAccessPath::IndexPrefix {
            name: TEST_INDEX.name().to_string(),
            fields: vec!["group".to_string(), "rank".to_string()],
            prefix_len: 1,
            values: vec![Value::Nat64(7)],
        },
        ExplainAccessPath::IndexMultiLookup {
            name: TEST_INDEX.name().to_string(),
            fields: vec!["group".to_string(), "rank".to_string()],
            values: vec![Value::Nat64(7), Value::Nat64(9)],
        },
        ExplainAccessPath::IndexRange {
            name: TEST_INDEX.name().to_string(),
            fields: vec!["group".to_string(), "rank".to_string()],
            prefix_len: 1,
            prefix: vec![Value::Nat64(7)],
            lower: Bound::Included(Value::Nat64(8)),
            upper: Bound::Excluded(Value::Nat64(12)),
        },
        ExplainAccessPath::Intersection(vec![
            ExplainAccessPath::FullScan,
            ExplainAccessPath::ByKey {
                key: Value::Nat64(10),
            },
        ]),
    ]);

    let mut projection = ExplainAccessEventProjection::default();
    project_explain_access_path(&access, &mut projection);

    assert_eq!(projection.union_child_counts, vec![7]);
    assert_eq!(projection.intersection_child_counts, vec![2]);
    assert_eq!(
        projection.seen_index,
        Some((TEST_INDEX.name().to_string(), 2, 1, 1))
    );
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
        projection.events.contains(&"index_multi_lookup"),
        "projection must visit index-multi-lookup variants",
    );
    assert!(
        projection.events.contains(&"full_scan"),
        "projection must visit full-scan variants"
    );
}

#[test]
fn explain_access_kind_label_projects_stable_access_codes() {
    assert_eq!(
        explain_access_kind_label(&ExplainAccessPath::ByKey {
            key: Value::Nat64(1)
        }),
        "by_key"
    );
    assert_eq!(
        explain_access_kind_label(&ExplainAccessPath::ByKeys { keys: Vec::new() }),
        "empty_access_contract"
    );
    assert_eq!(
        explain_access_kind_label(&ExplainAccessPath::FullScan),
        "full_scan"
    );
}
