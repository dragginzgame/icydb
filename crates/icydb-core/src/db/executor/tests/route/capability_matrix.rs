use super::*;

#[test]
fn route_capabilities_full_scan_desc_pk_order_reflect_expected_flags() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(3),
        offset: 2,
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
    )
    .expect("load route plan should build");

    assert!(route_plan.streaming_access_shape_safe());
    assert!(route_plan.desc_physical_reverse_supported());
    assert!(route_plan.count_pushdown_access_shape_supported());
    assert!(!route_plan.count_pushdown_existing_rows_shape_supported());
    assert!(!route_plan.index_range_limit_pushdown_shape_eligible());
    assert!(!route_plan.composite_aggregate_fast_path_eligible());
    assert!(route_plan.bounded_probe_hint_safe());
    assert!(!route_plan.field_min_fast_path_eligible());
    assert!(!route_plan.field_max_fast_path_eligible());
}

#[test]
fn route_capabilities_by_keys_desc_distinct_offset_disable_probe_hint() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::ByKeys(vec![
            Ulid::from_u128(7303),
            Ulid::from_u128(7301),
            Ulid::from_u128(7302),
        ]),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.scalar_plan_mut().distinct = true;
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
    )
    .expect("load route plan should build");

    assert!(route_plan.streaming_access_shape_safe());
    assert!(!route_plan.desc_physical_reverse_supported());
    assert!(!route_plan.count_pushdown_access_shape_supported());
    assert!(!route_plan.count_pushdown_existing_rows_shape_supported());
    assert!(!route_plan.index_range_limit_pushdown_shape_eligible());
    assert!(!route_plan.composite_aggregate_fast_path_eligible());
    assert!(!route_plan.bounded_probe_hint_safe());
    assert!(!route_plan.field_min_fast_path_eligible());
    assert!(!route_plan.field_max_fast_path_eligible());
}

#[test]
fn route_capabilities_index_range_order_compatible_shape_is_streaming_safe() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
    )
    .expect("load route plan should build");

    assert!(route_plan.streaming_access_shape_safe());
    assert!(route_plan.count_pushdown_existing_rows_shape_supported());
    assert!(route_plan.index_range_limit_pushdown_shape_eligible());
}

#[test]
fn route_capabilities_index_range_without_order_remains_limit_pushdown_eligible() {
    let plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
    )
    .expect("load route plan should build");

    assert!(
        route_plan.index_range_limit_pushdown_shape_eligible(),
        "no-order index-range shapes remain eligible for limit pushdown",
    );
}

#[test]
fn route_capabilities_index_range_with_empty_order_rejects_limit_pushdown_shape() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    // Planner validation rejects empty ORDER BY, but route capability checks
    // must still fail closed if a bypassed plan shape reaches this boundary.
    plan.scalar_plan_mut().order = Some(OrderSpec { fields: Vec::new() });

    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
    )
    .expect("load route plan should build");

    assert!(
        !route_plan.index_range_limit_pushdown_shape_eligible(),
        "empty-order planner-bypass shapes must not be treated as limit-pushdown eligible",
    );
}

#[test]
fn route_capabilities_non_unique_index_prefix_order_requires_post_access_sort() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });

    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
    )
    .expect("load route plan should build");

    assert!(
        !route_plan.streaming_access_shape_safe(),
        "non-unique index-prefix ordering must preserve post-access sorting",
    );
}
