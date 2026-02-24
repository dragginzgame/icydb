use super::*;

#[test]
fn predicate_slot_coverage_matches_single_index_path_fields() {
    let access = AccessPlan::path(AccessPath::<Ulid>::IndexPrefix {
        index: ROUTE_MATRIX_INDEX_MODELS[0],
        values: vec![Value::Uint(7)],
    });
    let predicate_slots = PredicateFieldSlots::resolve::<RouteMatrixEntity>(&Predicate::eq(
        "rank".to_string(),
        Value::Uint(7),
    ));

    let covered = LoadExecutor::<RouteMatrixEntity>::predicate_slots_fully_covered_by_index_path(
        &access,
        Some(&predicate_slots),
    );

    assert!(
        covered,
        "rank predicate should be covered by rank index path"
    );
}

#[test]
fn predicate_slot_coverage_rejects_non_indexed_predicate_fields() {
    let access = AccessPlan::path(AccessPath::<Ulid>::IndexPrefix {
        index: ROUTE_MATRIX_INDEX_MODELS[0],
        values: vec![Value::Uint(7)],
    });
    let predicate_slots = PredicateFieldSlots::resolve::<RouteMatrixEntity>(&Predicate::eq(
        "label".to_string(),
        Value::Text("x".to_string()),
    ));

    let covered = LoadExecutor::<RouteMatrixEntity>::predicate_slots_fully_covered_by_index_path(
        &access,
        Some(&predicate_slots),
    );

    assert!(
        !covered,
        "label predicate must not be covered by single-field rank index path"
    );
}

#[test]
fn predicate_slot_coverage_requires_index_backed_access_path() {
    let access = AccessPlan::path(AccessPath::<Ulid>::FullScan);
    let predicate_slots = PredicateFieldSlots::resolve::<RouteMatrixEntity>(&Predicate::eq(
        "rank".to_string(),
        Value::Uint(7),
    ));

    let covered = LoadExecutor::<RouteMatrixEntity>::predicate_slots_fully_covered_by_index_path(
        &access,
        Some(&predicate_slots),
    );

    assert!(
        !covered,
        "full-scan access is intentionally out of index-slot coverage scope"
    );
}

#[test]
fn route_capabilities_full_scan_desc_pk_order_reflect_expected_flags() {
    let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(3),
        offset: 2,
    });
    let capabilities =
        LoadExecutor::<RouteMatrixEntity>::derive_route_capabilities(&plan, Direction::Desc, None);

    assert!(capabilities.streaming_access_shape_safe);
    assert!(capabilities.desc_physical_reverse_supported);
    assert!(capabilities.count_pushdown_access_shape_supported);
    assert!(!capabilities.index_range_limit_pushdown_shape_eligible);
    assert!(!capabilities.composite_aggregate_fast_path_eligible);
    assert!(capabilities.bounded_probe_hint_safe);
    assert!(!capabilities.field_min_fast_path_eligible);
    assert!(!capabilities.field_max_fast_path_eligible);
}

#[test]
fn route_capabilities_by_keys_desc_distinct_offset_disable_probe_hint() {
    let mut plan = LogicalPlan::new(
        AccessPath::<Ulid>::ByKeys(vec![
            Ulid::from_u128(7303),
            Ulid::from_u128(7301),
            Ulid::from_u128(7302),
        ]),
        ReadConsistency::MissingOk,
    );
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.distinct = true;
    plan.page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let capabilities =
        LoadExecutor::<RouteMatrixEntity>::derive_route_capabilities(&plan, Direction::Desc, None);

    assert!(capabilities.streaming_access_shape_safe);
    assert!(!capabilities.desc_physical_reverse_supported);
    assert!(!capabilities.count_pushdown_access_shape_supported);
    assert!(!capabilities.index_range_limit_pushdown_shape_eligible);
    assert!(!capabilities.composite_aggregate_fast_path_eligible);
    assert!(!capabilities.bounded_probe_hint_safe);
    assert!(!capabilities.field_min_fast_path_eligible);
    assert!(!capabilities.field_max_fast_path_eligible);
}
