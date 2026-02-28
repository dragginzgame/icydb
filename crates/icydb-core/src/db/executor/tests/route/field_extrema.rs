use super::*;

#[test]
fn route_matrix_field_extrema_capability_flags_enable_for_eligible_shapes() {
    let mut min_plan =
        AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    min_plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let mut max_plan =
        AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    max_plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });

    let min_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
            &min_plan,
            AggregateSpec::for_target_field(AggregateKind::Min, "id"),
        );
    let max_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
            &max_plan,
            AggregateSpec::for_target_field(AggregateKind::Max, "id"),
        );

    assert!(min_route.field_min_fast_path_eligible());
    assert!(!min_route.field_max_fast_path_eligible());
    assert!(!max_route.field_min_fast_path_eligible());
    assert!(max_route.field_max_fast_path_eligible());
    assert_eq!(min_route.field_min_fast_path_ineligibility_reason(), None);
    assert_eq!(max_route.field_max_fast_path_ineligibility_reason(), None);
}

#[test]
fn route_matrix_field_extrema_capability_rejects_unknown_target_field() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, false);

    let route = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
        &plan,
        AggregateSpec::for_target_field(AggregateKind::Min, "missing_field"),
    );

    assert!(!route.field_min_fast_path_eligible());
    assert!(!route.field_max_fast_path_eligible());
    assert_eq!(
        route.field_min_fast_path_ineligibility_reason(),
        Some(FieldExtremaIneligibilityReason::UnknownTargetField)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_unsupported_field_type() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, false);

    let route = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
        &plan,
        AggregateSpec::for_target_field(AggregateKind::Min, "scores"),
    );

    assert_eq!(
        route.field_min_fast_path_ineligibility_reason(),
        Some(FieldExtremaIneligibilityReason::UnsupportedFieldType)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_distinct_shape() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, true);

    let route = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
        &plan,
        AggregateSpec::for_target_field(AggregateKind::Min, "rank"),
    );

    assert_eq!(
        route.field_min_fast_path_ineligibility_reason(),
        Some(FieldExtremaIneligibilityReason::DistinctNotSupported)
    );
}

#[test]
fn route_matrix_field_extrema_capability_allows_index_predicate_covered_shape() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.predicate = Some(Predicate::eq("rank".to_string(), Value::Uint(12)));
    plan.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.page = Some(PageSpec {
        limit: None,
        offset: 0,
    });

    let route = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
        &plan,
        AggregateSpec::for_target_field(AggregateKind::Min, "rank"),
    );

    assert!(
        route.field_min_fast_path_eligible(),
        "strict index-covered predicate shapes should remain eligible for field-extrema streaming"
    );
    assert_eq!(route.field_min_fast_path_ineligibility_reason(), None);
}

#[test]
fn route_matrix_field_extrema_reason_rejects_offset_shape() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 1, false);

    let route = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
        &plan,
        AggregateSpec::for_target_field(AggregateKind::Min, "rank"),
    );

    assert_eq!(
        route.field_min_fast_path_ineligibility_reason(),
        Some(FieldExtremaIneligibilityReason::OffsetNotSupported)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_composite_access_shape() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    let child_path = AccessPath::<Ulid>::index_range(
        ROUTE_MATRIX_INDEX_MODELS[0],
        vec![],
        Bound::Included(Value::Uint(10)),
        Bound::Excluded(Value::Uint(30)),
    );
    plan.access = AccessPlan::Union(vec![
        AccessPlan::path(child_path.clone()),
        AccessPlan::path(child_path),
    ]);
    plan.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.page = Some(PageSpec {
        limit: Some(4),
        offset: 0,
    });

    let route = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
        &plan,
        AggregateSpec::for_target_field(AggregateKind::Min, "rank"),
    );

    assert_eq!(
        route.field_min_fast_path_ineligibility_reason(),
        Some(FieldExtremaIneligibilityReason::CompositePathNotSupported)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_no_matching_index() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(4),
        offset: 0,
    });

    let route = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
        &plan,
        AggregateSpec::for_target_field(AggregateKind::Min, "rank"),
    );

    assert_eq!(
        route.field_min_fast_path_ineligibility_reason(),
        Some(FieldExtremaIneligibilityReason::NoMatchingIndex)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_page_limit_shape() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(4),
        offset: 0,
    });

    let route = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
        &plan,
        AggregateSpec::for_target_field(AggregateKind::Min, "id"),
    );

    assert_eq!(
        route.field_min_fast_path_ineligibility_reason(),
        Some(FieldExtremaIneligibilityReason::PageLimitNotSupported)
    );
}

#[test]
fn route_matrix_field_target_min_fallback_route_matches_terminal_min() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let terminal_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Min,
        );
    let field_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
            &plan,
            AggregateSpec::for_target_field(AggregateKind::Min, "rank"),
        );

    assert_eq!(terminal_route.execution_mode, ExecutionMode::Streaming);
    assert_eq!(field_route.execution_mode, ExecutionMode::Materialized);
    assert_eq!(field_route.scan_hints.physical_fetch_hint, None);
    assert_eq!(field_route.scan_hints.load_scan_budget_hint, None);
    assert!(field_route.index_range_limit_spec.is_none());
    assert_eq!(
        field_route.aggregate_fold_mode,
        terminal_route.aggregate_fold_mode
    );
    assert!(!field_route.field_min_fast_path_eligible());
    assert!(!field_route.field_max_fast_path_eligible());
}

#[test]
fn route_matrix_field_target_unknown_field_fallback_route_matches_terminal_min() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let terminal_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Min,
        );
    let unknown_field_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
            &plan,
            AggregateSpec::for_target_field(AggregateKind::Min, "missing_field"),
        );

    assert_eq!(terminal_route.execution_mode, ExecutionMode::Streaming);
    assert_eq!(
        unknown_field_route.execution_mode,
        ExecutionMode::Materialized
    );
    assert_eq!(unknown_field_route.scan_hints.physical_fetch_hint, None);
    assert_eq!(unknown_field_route.scan_hints.load_scan_budget_hint, None);
    assert!(unknown_field_route.index_range_limit_spec.is_none());
    assert_eq!(
        unknown_field_route.aggregate_fold_mode,
        terminal_route.aggregate_fold_mode
    );
    assert!(!unknown_field_route.field_min_fast_path_eligible());
    assert!(!unknown_field_route.field_max_fast_path_eligible());
}

#[test]
fn route_matrix_field_target_max_fallback_route_matches_terminal_max_desc() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let terminal_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Max,
        );
    let field_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
            &plan,
            AggregateSpec::for_target_field(AggregateKind::Max, "rank"),
        );

    assert_eq!(terminal_route.execution_mode, ExecutionMode::Streaming);
    assert_eq!(field_route.execution_mode, ExecutionMode::Materialized);
    assert_eq!(field_route.scan_hints.physical_fetch_hint, None);
    assert_eq!(field_route.scan_hints.load_scan_budget_hint, None);
    assert!(field_route.index_range_limit_spec.is_none());
    assert_eq!(
        field_route.aggregate_fold_mode,
        terminal_route.aggregate_fold_mode
    );
    assert!(!field_route.field_min_fast_path_eligible());
    assert!(!field_route.field_max_fast_path_eligible());
}

#[test]
fn route_matrix_field_target_non_extrema_fallback_route_matches_terminal_count() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, MissingRowPolicy::Ignore);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(3),
        offset: 2,
    });

    let terminal_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Count,
        );
    let field_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate_spec(
            &plan,
            AggregateSpec::for_target_field(AggregateKind::Count, "rank"),
        );

    assert_eq!(field_route.execution_mode, terminal_route.execution_mode);
    assert_eq!(
        field_route.scan_hints.physical_fetch_hint,
        terminal_route.scan_hints.physical_fetch_hint
    );
    assert_eq!(
        field_route.scan_hints.load_scan_budget_hint,
        terminal_route.scan_hints.load_scan_budget_hint
    );
    assert_eq!(
        field_route.index_range_limit_spec,
        terminal_route.index_range_limit_spec
    );
    assert_eq!(
        field_route.aggregate_fold_mode,
        terminal_route.aggregate_fold_mode
    );
    assert!(!field_route.field_min_fast_path_eligible());
    assert!(!field_route.field_max_fast_path_eligible());
}
