use super::*;

#[test]
fn route_matrix_field_extrema_capability_flags_enable_for_eligible_shapes() {
    let mut min_plan =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    min_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let mut max_plan =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    max_plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });

    let min_route = build_aggregate_spec_route(&min_plan, aggregate_builder::min_by("id"));
    let max_route = build_aggregate_spec_route(&max_plan, aggregate_builder::max_by("id"));

    assert!(min_route.field_min_fast_path_eligible());
    assert!(!min_route.field_max_fast_path_eligible());
    assert!(!max_route.field_min_fast_path_eligible());
    assert!(max_route.field_max_fast_path_eligible());
    assert_eq!(
        min_route
            .capabilities
            .field_min_fast_path_ineligibility_reason,
        None
    );
    assert_eq!(
        max_route
            .capabilities
            .field_max_fast_path_ineligibility_reason,
        None
    );
}

#[test]
fn route_matrix_field_target_max_pk_shape_enables_single_step_probe_hint() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });

    let route = build_aggregate_spec_route(&plan, aggregate_builder::max_by("id"));

    assert_eq!(route.execution_mode, RouteExecutionMode::Streaming);
    assert!(route.field_max_fast_path_eligible());
    assert_eq!(route.scan_hints.physical_fetch_hint, Some(1));
    assert_eq!(route.aggregate_seek_fetch_hint(), Some(1));
}

#[test]
fn route_matrix_field_extrema_capability_rejects_unknown_target_field() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, false);
    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("missing_field"));

    assert!(!route.field_min_fast_path_eligible());
    assert!(!route.field_max_fast_path_eligible());
    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::UnknownTargetField)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_unsupported_field_type() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, false);
    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("scores"));

    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::UnsupportedFieldType)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_distinct_shape() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 0, true);
    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("rank"));

    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::DistinctNotSupported)
    );
}

#[test]
fn route_matrix_field_extrema_capability_allows_index_predicate_covered_shape() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::eq("rank".to_string(), Value::Uint(12)));
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: None,
        offset: 0,
    });

    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("rank"));

    assert!(
        route.field_min_fast_path_eligible(),
        "strict index-covered predicate shapes should remain eligible for field-extrema streaming",
    );
    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        None
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_offset_shape() {
    let plan = field_extrema_index_range_plan(OrderDirection::Asc, 1, false);
    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("rank"));

    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::OffsetNotSupported)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_composite_access_shape() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    let child_path = AccessPath::<Value>::index_range(
        ROUTE_CAPABILITY_INDEX_MODELS[0],
        vec![],
        Bound::Included(Value::Uint(10)),
        Bound::Excluded(Value::Uint(30)),
    );
    plan.access = AccessPlan::Union(vec![
        AccessPlan::path(child_path.clone()),
        AccessPlan::path(child_path),
    ])
    .into_value_plan();
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(4),
        offset: 0,
    });

    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("rank"));

    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::CompositePathNotSupported)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_no_matching_index() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(4),
        offset: 0,
    });

    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("rank"));

    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::NoMatchingIndex)
    );
}

#[test]
fn route_matrix_field_extrema_reason_rejects_page_limit_shape() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(4),
        offset: 0,
    });

    let route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("id"));

    assert_eq!(
        route.capabilities.field_min_fast_path_ineligibility_reason,
        Some(AggregateFieldExtremaIneligibilityReason::PageLimitNotSupported)
    );
}

#[test]
fn route_matrix_field_target_min_fallback_route_matches_terminal_min() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let terminal_route = build_aggregate_route(&plan, AggregateKind::Min);
    let field_route = build_aggregate_spec_route(&plan, aggregate_builder::min_by("rank"));

    assert_eq!(terminal_route.execution_mode, RouteExecutionMode::Streaming);
    assert_eq!(field_route.execution_mode, RouteExecutionMode::Materialized);
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
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let terminal_route = build_aggregate_route(&plan, AggregateKind::Min);
    let unknown_field_route =
        build_aggregate_spec_route(&plan, aggregate_builder::min_by("missing_field"));

    assert_eq!(terminal_route.execution_mode, RouteExecutionMode::Streaming);
    assert_eq!(
        unknown_field_route.execution_mode,
        RouteExecutionMode::Materialized
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
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });

    let terminal_route = build_aggregate_route(&plan, AggregateKind::Max);
    let field_route = build_aggregate_spec_route(&plan, aggregate_builder::max_by("rank"));

    assert_eq!(terminal_route.execution_mode, RouteExecutionMode::Streaming);
    assert_eq!(field_route.execution_mode, RouteExecutionMode::Materialized);
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
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(3),
        offset: 2,
    });

    let terminal_route = build_aggregate_route(&plan, AggregateKind::Count);
    let field_route = build_aggregate_spec_route(&plan, aggregate_builder::count_by("rank"));

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
