use super::super::{UNIQUE_INDEX_RANGE_INDEX_MODELS, UniqueIndexRangeEntity};
use super::*;
use crate::db::executor::route::RouteShapeKind;

#[test]
fn route_plan_load_uses_route_owned_fast_path_order() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
    )
    .expect("load route plan should build");

    assert_eq!(route_plan.fast_path_order(), &LOAD_FAST_PATH_ORDER);
    assert_eq!(route_plan.direction(), Direction::Asc);
    assert_eq!(route_plan.continuation().mode(), ContinuationMode::Initial);
}

#[test]
fn route_plan_shape_descriptor_matches_route_axes() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
    )
    .expect("load route plan should build");

    let shape = route_plan.shape();
    assert_eq!(shape.route_shape_kind(), RouteShapeKind::LoadScalar);
    assert_eq!(shape.execution_mode_case(), ExecutionModeRouteCase::Load);
    assert_eq!(shape.execution_mode(), RouteExecutionMode::Streaming);
    assert!(shape.is_streaming());
}

#[test]
fn runtime_route_consumers_avoid_direct_execution_mode_field_reads() {
    let runtime_consumers = [
        "src/db/executor/load/execute/mod.rs",
        "src/db/executor/load/execute/fast_path.rs",
        "src/db/executor/load/entrypoints/scalar.rs",
        "src/db/executor/aggregate/mod.rs",
        "src/db/executor/explain/descriptor.rs",
    ];

    for relative_path in runtime_consumers {
        let absolute_path = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), relative_path);
        let source = std::fs::read_to_string(&absolute_path)
            .unwrap_or_else(|err| panic!("failed to read {absolute_path}: {err}"));
        assert!(
            !source.contains("route_plan.execution_mode"),
            "runtime route consumer should use ExecutionRouteShape accessors instead of direct execution_mode field reads: {relative_path}",
        );
    }
}

#[test]
fn route_matrix_load_pk_desc_with_page_uses_streaming_budget_and_reverse() {
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

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Streaming
    );
    assert_eq!(route_plan.direction(), Direction::Desc);
    assert_eq!(route_plan.continuation().mode(), ContinuationMode::Initial);
    assert_eq!(route_plan.continuation().effective_offset(), 2);
    assert!(route_plan.desc_physical_reverse_supported());
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, Some(6));
    assert_eq!(
        route_plan
            .top_n_seek_spec()
            .map(crate::db::executor::route::TopNSeekSpec::fetch),
        Some(6)
    );
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
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let cursor = CursorBoundary { slots: Vec::new() };
    let continuation = ScalarContinuationContext::from_parts(Some(cursor), None);
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &continuation,
        None,
    )
    .expect("load route plan should build");

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Streaming
    );
    assert_eq!(
        route_plan.continuation().mode(),
        ContinuationMode::CursorBoundary
    );
    assert_eq!(route_plan.continuation().effective_offset(), 0);
    assert!(route_plan.desc_physical_reverse_supported());
    assert!(route_plan.index_range_limit_spec.is_none());
    assert!(route_plan.top_n_seek_spec().is_none());
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
    plan.scalar_plan_mut().predicate = Some(Predicate::eq(
        "label".to_string(),
        Value::Text("keep".to_string()),
    ));
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
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
    plan.scalar_plan_mut().predicate = Some(Predicate::eq(
        "label".to_string(),
        Value::Text("keep".to_string()),
    ));
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(limit),
        offset: 0,
    });

    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
    )
    .expect("load route plan should build");

    assert!(
        route_plan.index_range_limit_spec.is_none(),
        "residual-filter windows above the fetch cap must disable index-range limit pushdown",
    );
}

#[test]
fn route_matrix_load_index_range_incompatible_order_disables_limit_pushdown() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(20)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("label".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
    )
    .expect("load route plan should build");

    assert!(
        !route_plan.index_range_limit_pushdown_shape_supported(),
        "index-range LIMIT pushdown shape must be rejected when ORDER BY is not planner-compatible",
    );
    assert!(
        route_plan.index_range_limit_spec.is_none(),
        "incompatible ordered index-range shapes must not derive index-range limit pushdown specs",
    );
    assert!(
        route_plan.top_n_seek_spec().is_none(),
        "incompatible ordered shapes must not derive Top-N seek hints",
    );
}

#[test]
fn route_matrix_load_index_range_missing_pk_tie_break_disables_limit_pushdown() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(20)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("rank".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
    )
    .expect("load route plan should build");

    assert!(
        !route_plan.index_range_limit_pushdown_shape_supported(),
        "index-range LIMIT pushdown shape must be rejected when ORDER BY omits PK tie-break",
    );
    assert!(
        route_plan.index_range_limit_spec.is_none(),
        "missing PK tie-break must disable index-range limit pushdown specs",
    );
    assert!(
        route_plan.top_n_seek_spec().is_none(),
        "missing PK tie-break must disable Top-N seek hints",
    );
}

#[test]
fn route_matrix_load_index_range_mixed_direction_disables_limit_pushdown() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(20)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
    )
    .expect("load route plan should build");

    assert!(
        !route_plan.index_range_limit_pushdown_shape_supported(),
        "index-range LIMIT pushdown shape must be rejected for mixed ORDER BY directions",
    );
    assert!(
        route_plan.index_range_limit_spec.is_none(),
        "mixed ORDER BY directions must disable index-range limit pushdown specs",
    );
    assert!(
        route_plan.top_n_seek_spec().is_none(),
        "mixed ORDER BY directions must disable Top-N seek hints",
    );
}

#[test]
fn route_matrix_load_non_pk_order_disables_scan_budget_hint() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("rank".to_string(), OrderDirection::Desc)],
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

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Materialized
    );
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
}

#[test]
fn route_matrix_load_unique_secondary_order_limit_one_uses_bounded_scan_budget_hint() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::IndexPrefix {
            index: UNIQUE_INDEX_RANGE_INDEX_MODELS[0],
            values: vec![],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("code".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(1),
        offset: 0,
    });
    let route_plan = LoadExecutor::<UniqueIndexRangeEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
        None,
    )
    .expect("secondary-order limit-one route plan should build");

    assert_eq!(
        route_plan.shape().execution_mode(),
        RouteExecutionMode::Streaming
    );
    assert_eq!(route_plan.direction(), Direction::Desc);
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    assert_eq!(
        route_plan.scan_hints.load_scan_budget_hint,
        Some(2),
        "secondary ORDER BY DESC LIMIT 1 should bound access scanning to keep+continuation fetch",
    );
    assert_eq!(
        route_plan
            .top_n_seek_spec()
            .map(crate::db::executor::route::TopNSeekSpec::fetch),
        Some(2)
    );
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
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &plan,
        &initial_scalar_continuation_context(),
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
    reverse_capable.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    let reverse_capable_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
            &reverse_capable,
            &initial_scalar_continuation_context(),
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
    reverse_blocked.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    let reverse_blocked_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
            &reverse_blocked,
            &initial_scalar_continuation_context(),
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
