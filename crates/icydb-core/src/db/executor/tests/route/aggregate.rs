use super::*;
use crate::db::executor::{
    ExecutionPreparation, IndexPredicateCompileMode, compile_index_predicate_program_from_slots,
};

#[test]
fn route_plan_aggregate_uses_route_owned_fast_path_order() {
    let mut plan =
        AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Exists,
    );

    assert_eq!(route_plan.fast_path_order(), &AGGREGATE_FAST_PATH_ORDER);
}

#[test]
fn route_plan_grouped_wrapper_maps_to_grouped_case_materialized_without_fast_paths() {
    let mut base =
        AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
    base.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let grouped = GroupedPlan::from_parts(
        base,
        GroupSpec {
            group_fields: vec!["rank".to_string()],
            aggregates: vec![GroupAggregateSpec {
                kind: GroupAggregateKind::Count,
                target_field: None,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        },
    );

    let route_plan =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_grouped_plan(&grouped);

    assert_eq!(
        route_plan.execution_mode_case(),
        ExecutionModeRouteCase::AggregateGrouped
    );
    assert_eq!(route_plan.execution_mode, ExecutionMode::Materialized);
    assert_eq!(route_plan.continuation_mode(), ContinuationMode::Initial);
    assert_eq!(route_plan.index_range_limit_spec, None);
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
    assert_eq!(route_plan.fast_path_order(), &[]);
}

#[test]
fn route_plan_grouped_wrapper_keeps_blocking_shape_under_tight_budget_config() {
    let mut base =
        AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
    base.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let grouped = GroupedPlan::from_parts(
        base,
        GroupSpec {
            group_fields: vec!["rank".to_string()],
            aggregates: vec![GroupAggregateSpec {
                kind: GroupAggregateKind::Count,
                target_field: None,
            }],
            execution: GroupedExecutionConfig::with_hard_limits(1, 1),
        },
    );

    let route_plan =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_grouped_plan(&grouped);

    assert_eq!(
        route_plan.execution_mode_case(),
        ExecutionModeRouteCase::AggregateGrouped
    );
    assert_eq!(route_plan.execution_mode, ExecutionMode::Materialized);
    assert_eq!(route_plan.continuation_mode(), ContinuationMode::Initial);
    assert_eq!(route_plan.index_range_limit_spec, None);
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
    assert_eq!(route_plan.fast_path_order(), &[]);
}

#[test]
fn route_matrix_aggregate_count_pk_order_is_streaming_keys_only() {
    let mut plan =
        AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(4),
        offset: 2,
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Count,
    );

    assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::KeysOnly
    ));
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(6));
}

#[test]
fn route_matrix_aggregate_fold_mode_contract_maps_non_count_to_existing_rows() {
    let mut plan =
        AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    for kind in [
        AggregateKind::Exists,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan, kind,
            );

        assert!(matches!(
            route_plan.aggregate_fold_mode,
            AggregateFoldMode::ExistingRows
        ));
    }
}

#[test]
fn route_matrix_aggregate_count_secondary_shape_materializes() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        ReadConsistency::MissingOk,
    );
    plan.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Count,
    );

    assert_eq!(route_plan.execution_mode, ExecutionMode::Materialized);
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::KeysOnly
    ));
}

#[test]
fn route_matrix_aggregate_distinct_offset_last_disables_probe_hint() {
    let mut plan =
        AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.distinct = true;
    plan.page = Some(PageSpec {
        limit: Some(3),
        offset: 1,
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Last,
    );

    assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::ExistingRows
    ));
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
}

#[test]
fn route_matrix_aggregate_distinct_offset_disables_bounded_probe_hints_for_terminals() {
    let mut plan =
        AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.distinct = true;
    plan.page = Some(PageSpec {
        limit: Some(3),
        offset: 1,
    });

    for kind in [
        AggregateKind::Count,
        AggregateKind::Exists,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan, kind,
            );

        assert_eq!(
            route_plan.scan_hints.physical_fetch_hint, None,
            "DISTINCT+offset must disable bounded aggregate hints for {kind:?}"
        );
        assert_eq!(
            route_plan.secondary_extrema_probe_fetch_hint(),
            None,
            "DISTINCT+offset must disable secondary extrema probe hints for {kind:?}"
        );
    }
}

#[test]
fn route_matrix_aggregate_by_keys_desc_disables_probe_hint_without_reverse_support() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::ByKeys(vec![
            Ulid::from_u128(7103),
            Ulid::from_u128(7101),
            Ulid::from_u128(7102),
        ]),
        ReadConsistency::MissingOk,
    );
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });
    plan.page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::First,
    );

    assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
    assert!(!route_plan.desc_physical_reverse_supported());
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
}

#[test]
fn route_matrix_aggregate_secondary_extrema_probe_hints_lock_offset_plus_one() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        ReadConsistency::MissingOk,
    );
    plan.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.page = Some(PageSpec {
        limit: None,
        offset: 2,
    });

    let min_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Min,
    );
    let max_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Max,
    );
    assert_eq!(min_asc.scan_hints.physical_fetch_hint, Some(3));
    assert_eq!(max_asc.scan_hints.physical_fetch_hint, None);
    assert_eq!(min_asc.secondary_extrema_probe_fetch_hint(), Some(3));
    assert_eq!(max_asc.secondary_extrema_probe_fetch_hint(), None);

    let first_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::First,
    );
    assert_eq!(
        first_asc.secondary_extrema_probe_fetch_hint(),
        None,
        "secondary extrema probe hints must stay route-owned and Min/Max-only"
    );

    plan.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    let max_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Max,
    );
    let min_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Min,
    );
    assert_eq!(max_desc.scan_hints.physical_fetch_hint, Some(3));
    assert_eq!(min_desc.scan_hints.physical_fetch_hint, None);
    assert_eq!(max_desc.secondary_extrema_probe_fetch_hint(), Some(3));
    assert_eq!(min_desc.secondary_extrema_probe_fetch_hint(), None);
}

#[test]
fn route_matrix_aggregate_index_range_desc_with_window_enables_pushdown_hint() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        ReadConsistency::MissingOk,
    );
    plan.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    plan.page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Last,
    );

    assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
    assert!(route_plan.desc_physical_reverse_supported());
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(3));
    assert_eq!(
        route_plan.index_range_limit_spec.map(|spec| spec.fetch),
        Some(3)
    );
}

#[test]
fn route_matrix_aggregate_count_pushdown_boundary_matrix() {
    let mut full_scan =
        AccessPlannedQuery::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
    full_scan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let full_scan_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &full_scan,
            AggregateKind::Count,
        );
    assert_eq!(full_scan_route.execution_mode, ExecutionMode::Streaming);
    assert!(matches!(
        full_scan_route.aggregate_fold_mode,
        AggregateFoldMode::KeysOnly
    ));

    let mut key_range = AccessPlannedQuery::new(
        AccessPath::<Ulid>::KeyRange {
            start: Ulid::from_u128(1),
            end: Ulid::from_u128(9),
        },
        ReadConsistency::MissingOk,
    );
    key_range.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    let key_range_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &key_range,
            AggregateKind::Count,
        );
    assert_eq!(key_range_route.execution_mode, ExecutionMode::Streaming);
    assert!(matches!(
        key_range_route.aggregate_fold_mode,
        AggregateFoldMode::KeysOnly
    ));

    let mut secondary = AccessPlannedQuery::new(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        ReadConsistency::MissingOk,
    );
    secondary.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let secondary_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &secondary,
            AggregateKind::Count,
        );
    assert_eq!(secondary_route.execution_mode, ExecutionMode::Materialized);
    assert!(matches!(
        secondary_route.aggregate_fold_mode,
        AggregateFoldMode::KeysOnly
    ));

    let mut index_range = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        ReadConsistency::MissingOk,
    );
    index_range.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    index_range.page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let index_range_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &index_range,
            AggregateKind::Count,
        );
    assert_eq!(
        index_range_route.execution_mode,
        ExecutionMode::Materialized
    );
    assert!(index_range_route.index_range_limit_spec.is_none());
    assert!(matches!(
        index_range_route.aggregate_fold_mode,
        AggregateFoldMode::KeysOnly
    ));
}

#[test]
fn route_matrix_secondary_extrema_probe_eligibility_is_min_max_only() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::IndexPrefix {
            index: ROUTE_MATRIX_INDEX_MODELS[0],
            values: vec![Value::Uint(7)],
        },
        ReadConsistency::MissingOk,
    );
    plan.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    plan.page = Some(PageSpec {
        limit: None,
        offset: 2,
    });

    let min_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Min,
    );
    let max_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Max,
    );
    let first_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::First,
    );
    let exists_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Exists,
    );
    let last_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Last,
    );
    assert_eq!(min_asc.secondary_extrema_probe_fetch_hint(), Some(3));
    assert_eq!(max_asc.secondary_extrema_probe_fetch_hint(), None);
    assert_eq!(first_asc.secondary_extrema_probe_fetch_hint(), None);
    assert_eq!(exists_asc.secondary_extrema_probe_fetch_hint(), None);
    assert_eq!(last_asc.secondary_extrema_probe_fetch_hint(), None);

    plan.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Desc),
            ("id".to_string(), OrderDirection::Desc),
        ],
    });
    let min_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Min,
    );
    let max_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
        &plan,
        AggregateKind::Max,
    );
    assert_eq!(min_desc.secondary_extrema_probe_fetch_hint(), None);
    assert_eq!(max_desc.secondary_extrema_probe_fetch_hint(), Some(3));
}

#[test]
fn route_matrix_index_predicate_compile_mode_subset_vs_strict_boundary_is_explicit() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        ReadConsistency::MissingOk,
    );
    plan.predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(12)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
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

    let execution_preparation = ExecutionPreparation::for_plan::<RouteMatrixEntity>(&plan);
    let predicate_slots = execution_preparation
        .compiled_predicate()
        .expect("predicate slots should compile for mixed strict/residual predicate");
    let index_slots = execution_preparation
        .slot_map()
        .expect("index-range plan should expose one resolvable index slot");
    let subset_program = compile_index_predicate_program_from_slots(
        predicate_slots,
        index_slots,
        IndexPredicateCompileMode::ConservativeSubset,
    );
    let strict_program = compile_index_predicate_program_from_slots(
        predicate_slots,
        index_slots,
        IndexPredicateCompileMode::StrictAllOrNone,
    );

    assert!(
        subset_program.is_some(),
        "subset compile mode should keep the strict index-covered rank clause as a safe AND subset",
    );
    assert!(
        strict_program.is_none(),
        "strict compile mode must fail closed when any predicate child is not index-only-safe",
    );
}

#[test]
fn route_matrix_aggregate_strict_compile_uncertainty_forces_materialized_execution_mode() {
    let mut strict_compatible = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        ReadConsistency::MissingOk,
    );
    strict_compatible.predicate = Some(Predicate::eq("rank".to_string(), Value::Uint(12)));
    strict_compatible.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    strict_compatible.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let strict_compatible_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &strict_compatible,
            AggregateKind::Exists,
        );
    assert_eq!(
        strict_compatible_route.execution_mode,
        ExecutionMode::Streaming,
        "strict-compilable secondary predicate shapes should keep aggregate streaming eligibility",
    );

    let mut strict_uncertain = strict_compatible.clone();
    strict_uncertain.predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(12)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    let strict_uncertain_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &strict_uncertain,
            AggregateKind::Exists,
        );
    assert_eq!(
        strict_uncertain_route.execution_mode,
        ExecutionMode::Materialized,
        "aggregate route planning must force materialized execution when strict index compile fails",
    );

    let load_route = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &strict_uncertain,
        None,
        None,
        None,
    )
    .expect("load route plan should build for strict/subset parity boundary shape");
    assert_eq!(
        load_route.execution_mode,
        ExecutionMode::Streaming,
        "load routing should remain streaming for the same shape via conservative subset policy",
    );
}

#[test]
fn route_matrix_strict_vs_subset_decision_logs_are_stable() {
    let mut strict_compatible = AccessPlannedQuery::new(
        AccessPath::<Ulid>::index_range(
            ROUTE_MATRIX_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        ReadConsistency::MissingOk,
    );
    strict_compatible.predicate = Some(Predicate::eq("rank".to_string(), Value::Uint(12)));
    strict_compatible.order = Some(OrderSpec {
        fields: vec![
            ("rank".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    strict_compatible.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let strict_compatible_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &strict_compatible,
            AggregateKind::Exists,
        );
    let mut strict_uncertain = strict_compatible.clone();
    strict_uncertain.predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(12)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    let strict_uncertain_route =
        LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &strict_uncertain,
            AggregateKind::Exists,
        );
    let load_route = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
        &strict_uncertain,
        None,
        None,
        None,
    )
    .expect("load route plan should build for strict/subset log shape");

    let strict_compatible_log = format!(
        "aggregate:mode={:?};fold={:?};fetch={:?};secondary_probe={:?};index_range_limit={};continuation={:?}",
        strict_compatible_route.execution_mode,
        strict_compatible_route.aggregate_fold_mode,
        strict_compatible_route.scan_hints.physical_fetch_hint,
        strict_compatible_route.secondary_extrema_probe_fetch_hint(),
        strict_compatible_route.index_range_limit_fast_path_enabled(),
        strict_compatible_route.continuation_mode(),
    );
    let strict_uncertain_log = format!(
        "aggregate:mode={:?};fold={:?};fetch={:?};secondary_probe={:?};index_range_limit={};continuation={:?}",
        strict_uncertain_route.execution_mode,
        strict_uncertain_route.aggregate_fold_mode,
        strict_uncertain_route.scan_hints.physical_fetch_hint,
        strict_uncertain_route.secondary_extrema_probe_fetch_hint(),
        strict_uncertain_route.index_range_limit_fast_path_enabled(),
        strict_uncertain_route.continuation_mode(),
    );
    let load_log = format!(
        "load:mode={:?};fetch={:?};scan_budget={:?};index_range_limit={};continuation={:?}",
        load_route.execution_mode,
        load_route.scan_hints.physical_fetch_hint,
        load_route.scan_hints.load_scan_budget_hint,
        load_route.index_range_limit_fast_path_enabled(),
        load_route.continuation_mode(),
    );

    assert_eq!(
        strict_compatible_log,
        "aggregate:mode=Streaming;fold=ExistingRows;fetch=Some(1);secondary_probe=None;index_range_limit=true;continuation=Initial",
        "strict-compilable aggregate route decision log should remain stable",
    );
    assert_eq!(
        strict_uncertain_log,
        "aggregate:mode=Materialized;fold=ExistingRows;fetch=Some(1);secondary_probe=None;index_range_limit=false;continuation=Initial",
        "strict-uncertain aggregate route decision log should remain stable",
    );
    assert_eq!(
        load_log,
        "load:mode=Streaming;fetch=None;scan_budget=None;index_range_limit=true;continuation=Initial",
        "subset load route decision log should remain stable for the same shape",
    );
}
