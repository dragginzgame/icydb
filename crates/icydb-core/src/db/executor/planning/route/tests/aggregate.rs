use super::*;

#[test]
fn route_plan_aggregate_uses_route_owned_fast_path_order() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let route_plan = build_aggregate_route(&plan, AggregateKind::Exists);

    assert_eq!(route_plan.fast_path_order(), &AGGREGATE_FAST_PATH_ORDER);
    assert_eq!(route_plan.grouped_observability(), None);
}

#[test]
fn aggregate_route_snapshot_for_scalar_count_is_stable() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let actual = scalar_aggregate_route_snapshot(&plan, crate::db::count());
    let expected = [
        "aggregate_kind=Count".to_string(),
        "grouped=false".to_string(),
        "distinct_mode=false".to_string(),
        "target_field=None".to_string(),
        "route_strategy=AggregateCount".to_string(),
        "execution_mode=Streaming".to_string(),
        "fold_mode=KeysOnly".to_string(),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "scalar COUNT aggregate route snapshot drifted; route strategy/fold mode are stabilized",
    );
}

#[test]
fn aggregate_route_snapshot_for_scalar_sum_field_is_stable() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let actual = scalar_aggregate_route_snapshot(&plan, crate::db::sum("rank"));
    let expected = [
        "aggregate_kind=Sum".to_string(),
        "grouped=false".to_string(),
        "distinct_mode=false".to_string(),
        "target_field=Some(\"rank\")".to_string(),
        "route_strategy=AggregateNonCount".to_string(),
        "execution_mode=Materialized".to_string(),
        "fold_mode=ExistingRows".to_string(),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "scalar SUM(field) aggregate route snapshot drifted; route strategy/fold mode are stabilized",
    );
}

#[test]
fn aggregate_route_snapshot_for_scalar_avg_field_is_stable() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let actual = scalar_aggregate_route_snapshot(&plan, crate::db::avg("rank"));
    let expected = [
        "aggregate_kind=Avg".to_string(),
        "grouped=false".to_string(),
        "distinct_mode=false".to_string(),
        "target_field=Some(\"rank\")".to_string(),
        "route_strategy=AggregateNonCount".to_string(),
        "execution_mode=Materialized".to_string(),
        "fold_mode=ExistingRows".to_string(),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "scalar AVG(field) aggregate route snapshot drifted; route strategy/fold mode are stabilized",
    );
}

#[test]
fn aggregate_route_snapshot_for_grouped_field_aggregates_is_stable() {
    let grouped = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore)
        .into_grouped(GroupSpec {
            group_fields: grouped_field_slots(&["rank"]),
            aggregates: vec![GroupAggregateSpec {
                kind: AggregateKind::Avg,
                target_field: Some("rank".to_string()),
                distinct: false,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        });

    let actual = grouped_aggregate_route_snapshot(&grouped);
    let expected = [
        "grouped=true".to_string(),
        "planner_strategy=GroupedPlanStrategy { family: Hash, aggregate_family: FieldTargetRows, fallback_reason: Some(GroupKeyOrderUnavailable) }".to_string(),
        "aggregate_contracts=[\"Avg:Some(\\\"rank\\\"):false\"]".to_string(),
        "route_strategy=AggregateGrouped".to_string(),
        "execution_mode=Materialized".to_string(),
        "planner_fallback_reason=Some(GroupKeyOrderUnavailable)".to_string(),
        "grouped_execution_mode=HashMaterialized".to_string(),
        "fold_mode=ExistingRows".to_string(),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "grouped field-aggregate route snapshot drifted; grouped planner/route/executor strategy is stabilized",
    );
}

#[test]
fn aggregate_route_strategy_parity_for_scalar_avg_matches_sum_field() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    let sum_route = build_aggregate_spec_route(&plan, crate::db::sum("rank"));
    let avg_route = build_aggregate_spec_route(&plan, crate::db::avg("rank"));

    assert_eq!(avg_route.route_shape_kind(), sum_route.route_shape_kind());
    assert_eq!(avg_route.execution_mode(), sum_route.execution_mode());
    assert_eq!(avg_route.aggregate_fold_mode, sum_route.aggregate_fold_mode);
}
