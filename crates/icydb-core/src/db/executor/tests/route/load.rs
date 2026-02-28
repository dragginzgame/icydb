use super::*;

#[test]
fn route_plan_load_uses_route_owned_fast_path_order() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan, None, None, None,
    )
    .expect("load route plan should build");

    assert_eq!(route_plan.fast_path_order(), &LOAD_FAST_PATH_ORDER);
    assert_eq!(route_plan.direction(), Direction::Asc);
    assert_eq!(route_plan.continuation_mode(), ContinuationMode::Initial);
}

#[test]
fn route_matrix_load_pk_desc_with_page_uses_streaming_budget_and_reverse() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(3),
        offset: 2,
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan, None, None, None,
    )
    .expect("load route plan should build");

    assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
    assert_eq!(route_plan.direction(), Direction::Desc);
    assert_eq!(route_plan.continuation_mode(), ContinuationMode::Initial);
    assert_eq!(route_plan.window().effective_offset, 2);
    assert!(route_plan.desc_physical_reverse_supported());
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, Some(6));
    assert!(route_plan.index_range_limit_spec.is_none());
}

#[test]
fn route_matrix_load_index_range_cursor_without_anchor_disables_pushdown() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(20)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let cursor = CursorBoundary { slots: Vec::new() };
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        Some(&cursor),
        None,
        None,
    )
    .expect("load route plan should build");

    assert_eq!(route_plan.execution_mode, ExecutionMode::Materialized);
    assert_eq!(
        route_plan.continuation_mode(),
        ContinuationMode::CursorBoundary
    );
    assert_eq!(route_plan.window().effective_offset, 0);
    assert!(route_plan.desc_physical_reverse_supported());
    assert!(route_plan.index_range_limit_spec.is_none());
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
}

#[test]
fn route_matrix_load_index_range_residual_predicate_allows_small_window_pushdown() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(20)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.predicate = Some(Predicate::eq(
        "label".to_string(),
        Value::Text("keep".to_string()),
    ));
    plan.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan, None, None, None,
    )
    .expect("load route plan should build");

    assert_eq!(
        route_plan.index_range_limit_spec.map(|spec| spec.fetch),
        Some(3),
        "small residual-filter windows should retain index-range limit pushdown",
    );
}

#[test]
fn route_matrix_load_index_range_residual_predicate_large_window_disables_pushdown() {
    let fetch_cap = LoadExecutor::<RouteMatrixEntity>::residual_predicate_pushdown_fetch_cap();
    let limit =
        u32::try_from(fetch_cap).expect("residual pushdown fetch cap should fit within u32");

    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(20)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.predicate = Some(Predicate::eq(
        "label".to_string(),
        Value::Text("keep".to_string()),
    ));
    plan.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.page = Some(PageSpec {
        limit: Some(limit),
        offset: 0,
    });

    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan, None, None, None,
    )
    .expect("load route plan should build");

    assert!(
        route_plan.index_range_limit_spec.is_none(),
        "residual-filter windows above the fetch cap must disable index-range limit pushdown",
    );
}

#[test]
fn route_matrix_load_non_pk_order_disables_scan_budget_hint() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.order = Some(OrderSpec {
        fields: vec![("rank".to_string(), OrderDirection::Desc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(3),
        offset: 2,
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan, None, None, None,
    )
    .expect("load route plan should build");

    assert_eq!(route_plan.execution_mode, ExecutionMode::Materialized);
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
}

#[test]
fn route_matrix_load_by_keys_desc_disables_fallback_fetch_hint_without_reverse_support() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::ByKeys(vec![
            Ulid::from_u128(7203),
            Ulid::from_u128(7201),
            Ulid::from_u128(7202),
        ]),
        MissingRowPolicy::Ignore,
    );
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        None,
        None,
        Some(4),
    )
    .expect("load route plan should build");

    assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(4));
    assert_eq!(
        route_plan.fallback_physical_fetch_hint(Direction::Desc),
        None
    );
    assert_eq!(
        route_plan.fallback_physical_fetch_hint(Direction::Asc),
        Some(4)
    );
}

#[test]
fn route_matrix_load_desc_reverse_support_gate_allows_and_blocks_fetch_hint() {
    let mut reverse_capable =
        AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    reverse_capable.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    let reverse_capable_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
            &reverse_capable,
            None,
            None,
            Some(5),
        )
        .expect("reverse-capable load route should build");
    assert!(reverse_capable_route.desc_physical_reverse_supported());
    assert_eq!(
        reverse_capable_route.scan_hints.physical_fetch_hint,
        Some(5)
    );
    assert_eq!(
        reverse_capable_route.fallback_physical_fetch_hint(Direction::Desc),
        Some(5)
    );

    let mut reverse_blocked = AccessPlannedQuery::new(
        AccessPath::<Ulid>::ByKeys(vec![
            Ulid::from_u128(7_203),
            Ulid::from_u128(7_201),
            Ulid::from_u128(7_202),
        ]),
        MissingRowPolicy::Ignore,
    );
    reverse_blocked.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    let reverse_blocked_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
            &reverse_blocked,
            None,
            None,
            Some(5),
        )
        .expect("reverse-blocked load route should build");
    assert!(!reverse_blocked_route.desc_physical_reverse_supported());
    assert_eq!(
        reverse_blocked_route.scan_hints.physical_fetch_hint,
        Some(5)
    );
    assert_eq!(
        reverse_blocked_route.fallback_physical_fetch_hint(Direction::Desc),
        None
    );
}
