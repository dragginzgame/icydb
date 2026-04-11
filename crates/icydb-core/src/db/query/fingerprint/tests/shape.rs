use super::*;

#[test]
fn fingerprint_changes_with_index_choice() {
    const INDEX_FIELDS: [&str; 1] = ["idx_a"];
    const INDEX_A: IndexModel = IndexModel::generated(
        "fingerprint::idx_a",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );
    const INDEX_B: IndexModel = IndexModel::generated(
        "fingerprint::idx_b",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );

    let plan_a: AccessPlannedQuery =
        index_prefix_query(INDEX_A, vec![Value::Text("alpha".to_string())]);
    let plan_b: AccessPlannedQuery =
        index_prefix_query(INDEX_B, vec![Value::Text("alpha".to_string())]);

    assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_changes_with_pagination() {
    let mut plan_a: AccessPlannedQuery = full_scan_query();
    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(10),
        offset: 0,
    });
    plan_b.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(10),
        offset: 1,
    });

    assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_changes_with_delete_limit() {
    let mut plan_a: AccessPlannedQuery = full_scan_query();
    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_a.scalar_plan_mut().mode = QueryMode::Delete(DeleteSpec::new());
    plan_b.scalar_plan_mut().mode = QueryMode::Delete(DeleteSpec::new());
    plan_a.scalar_plan_mut().delete_limit = Some(DeleteLimitSpec {
        limit: Some(2),
        offset: 0,
    });
    plan_b.scalar_plan_mut().delete_limit = Some(DeleteLimitSpec {
        limit: Some(3),
        offset: 0,
    });

    assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_changes_with_distinct_flag() {
    let plan_a: AccessPlannedQuery = full_scan_query();
    let mut plan_b: AccessPlannedQuery = full_scan_query();
    plan_b.scalar_plan_mut().distinct = true;

    assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
}

#[test]
fn fingerprint_changes_when_index_range_bound_discriminant_changes() {
    const INDEX_FIELDS: [&str; 2] = ["group", "rank"];
    const INDEX: IndexModel = IndexModel::generated(
        "fingerprint::group_rank",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );

    let plan_included: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );
    let plan_excluded: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Excluded(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );

    assert_ne!(plan_included.fingerprint(), plan_excluded.fingerprint());
}

#[test]
fn fingerprint_changes_when_index_range_bound_value_changes() {
    const INDEX_FIELDS: [&str; 2] = ["group", "rank"];
    const INDEX: IndexModel = IndexModel::generated(
        "fingerprint::group_rank",
        "fingerprint::store",
        &INDEX_FIELDS,
        false,
    );

    let plan_low_100: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(100)),
        Bound::Excluded(Value::Uint(200)),
    );
    let plan_low_101: AccessPlannedQuery = index_range_query(
        INDEX,
        vec![Value::Uint(7)],
        Bound::Included(Value::Uint(101)),
        Bound::Excluded(Value::Uint(200)),
    );

    assert_ne!(plan_low_100.fingerprint(), plan_low_101.fingerprint());
}
