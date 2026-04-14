//! Module: db::access::tests::canonical
//! Covers access-canonicalization regressions that must stay aligned with the
//! wider access/fingerprint contract.
//! Does not own: leaf-local canonical comparator helpers.
//! Boundary: keeps owner-level access identity regressions in the access
//! subsystem `tests/` boundary.

use crate::{
    db::{
        MissingRowPolicy,
        access::{AccessPath, AccessPlan, normalize_access_plan_value},
        query::plan::AccessPlannedQuery,
    },
    model::index::IndexModel,
    value::Value,
};
use std::ops::Bound;

const TEST_INDEX_FIELDS: [&str; 2] = ["group", "rank"];
const TEST_INDEX: IndexModel = IndexModel::generated(
    "canonical::group_rank",
    "canonical::store",
    &TEST_INDEX_FIELDS,
    false,
);
const TEST_INDEX_FIELDS_ALT: [&str; 2] = ["group", "score"];
const TEST_INDEX_SAME_NAME_ALT_FIELDS: IndexModel = IndexModel::generated(
    "canonical::group_rank",
    "canonical::store",
    &TEST_INDEX_FIELDS_ALT,
    false,
);

fn index_range_path(
    index: IndexModel,
    lower: Bound<Value>,
    upper: Bound<Value>,
) -> AccessPath<Value> {
    AccessPath::index_range(index, vec![Value::Uint(7)], lower, upper)
}

#[test]
fn canonical_and_fingerprint_align_for_index_range_bound_discriminants() {
    let included = index_range_path(
        TEST_INDEX,
        Bound::Included(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );
    let excluded = index_range_path(
        TEST_INDEX,
        Bound::Excluded(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );

    assert_ne!(
        normalize_access_plan_value(AccessPlan::path(included.clone())),
        normalize_access_plan_value(AccessPlan::path(excluded.clone())),
        "access canonicalization must keep index-range bound discriminants distinct",
    );

    let included_plan: AccessPlannedQuery =
        AccessPlannedQuery::new(included, MissingRowPolicy::Ignore);
    let excluded_plan: AccessPlannedQuery =
        AccessPlannedQuery::new(excluded, MissingRowPolicy::Ignore);

    assert_ne!(
        included_plan.fingerprint(),
        excluded_plan.fingerprint(),
        "fingerprints must stay aligned with canonical bound discriminants",
    );
}

#[test]
fn canonical_and_fingerprint_align_for_index_field_identity() {
    let path_a = index_range_path(
        TEST_INDEX,
        Bound::Included(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );
    let path_b = index_range_path(
        TEST_INDEX_SAME_NAME_ALT_FIELDS,
        Bound::Included(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );

    assert_ne!(
        normalize_access_plan_value(AccessPlan::path(path_a.clone())),
        normalize_access_plan_value(AccessPlan::path(path_b.clone())),
        "access canonicalization must keep index field identity in the semantic shape",
    );

    let plan_a: AccessPlannedQuery = AccessPlannedQuery::new(path_a, MissingRowPolicy::Ignore);
    let plan_b: AccessPlannedQuery = AccessPlannedQuery::new(path_b, MissingRowPolicy::Ignore);

    assert_ne!(
        plan_a.fingerprint(),
        plan_b.fingerprint(),
        "fingerprints must stay aligned with canonical index identity",
    );
}
