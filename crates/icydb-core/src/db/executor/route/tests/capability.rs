use super::*;

#[test]
fn route_capabilities_full_scan_desc_pk_order_reflect_expected_flags() {
    let mut plan =
        AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
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
    let mut plan = AccessPlannedQuery::new(
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
