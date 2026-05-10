use super::*;
use crate::db::executor::route::AggregateSeekSpec;

#[test]
fn route_matrix_aggregate_count_pk_order_is_streaming_keys_only() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(4),
        offset: 2,
    });
    let route_plan = build_aggregate_route(&plan, AggregateKind::Count);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Streaming);
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::KeysOnly
    ));
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(6));
}

#[test]
fn route_matrix_aggregate_fold_mode_contract_maps_non_count_to_existing_rows() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    for kind in [
        AggregateKind::Exists,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        let route_plan = build_aggregate_route(&plan, kind);

        assert!(matches!(
            route_plan.aggregate_fold_mode,
            AggregateFoldMode::ExistingRows
        ));
    }
}

#[test]
fn route_matrix_numeric_field_aggregate_fold_mode_contract_maps_sum_avg_to_existing_rows() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    for aggregate_expr in [crate::db::sum("rank"), crate::db::avg("rank")] {
        let route_plan = build_aggregate_spec_route(&plan, aggregate_expr);

        assert!(matches!(
            route_plan.aggregate_fold_mode,
            AggregateFoldMode::ExistingRows
        ));
    }
}

#[test]
fn route_matrix_aggregate_count_secondary_shape_streams_with_existing_rows() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    let route_plan = build_aggregate_route(&plan, AggregateKind::Count);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Streaming);
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::ExistingRows
    ));
}

#[test]
fn route_matrix_aggregate_count_secondary_shape_with_strict_predicate_streams() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::eq("rank".to_string(), Value::Uint(7)));
    let route_plan = build_aggregate_route(&plan, AggregateKind::Count);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Streaming);
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::ExistingRows
    ));
}

#[test]
fn route_matrix_aggregate_count_secondary_shape_with_strict_uncertainty_materializes() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(7)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    let route_plan = build_aggregate_route(&plan, AggregateKind::Count);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Materialized);
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::ExistingRows
    ));
}

#[test]
fn route_matrix_aggregate_distinct_offset_last_disables_probe_hint() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Desc,
        )],
    });
    plan.scalar_plan_mut().distinct = true;
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(3),
        offset: 1,
    });
    let route_plan = build_aggregate_route(&plan, AggregateKind::Last);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Streaming);
    assert!(matches!(
        route_plan.aggregate_fold_mode,
        AggregateFoldMode::ExistingRows
    ));
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
}

#[test]
fn route_matrix_aggregate_distinct_offset_disables_bounded_probe_hints_for_terminals() {
    let mut plan = AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });
    plan.scalar_plan_mut().distinct = true;
    plan.scalar_plan_mut().page = Some(PageSpec {
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
        let route_plan = build_aggregate_route(&plan, kind);

        assert_eq!(
            route_plan.scan_hints.physical_fetch_hint, None,
            "DISTINCT+offset must disable bounded aggregate hints for {kind:?}"
        );
        assert_eq!(
            route_plan.aggregate_seek_fetch_hint(),
            None,
            "DISTINCT+offset must disable secondary extrema probe hints for {kind:?}"
        );
    }
}

#[test]
fn route_matrix_aggregate_by_keys_desc_disables_probe_hint_without_reverse_support() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::ByKeys(vec![
            Value::Ulid(Ulid::from_u128(7103)),
            Value::Ulid(Ulid::from_u128(7101)),
            Value::Ulid(Ulid::from_u128(7102)),
        ]),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Desc,
        )],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = build_aggregate_route(&plan, AggregateKind::First);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Streaming);
    assert!(!route_plan.desc_physical_reverse_supported());
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
}

#[test]
fn route_matrix_aggregate_secondary_extrema_probe_hints_lock_offset_plus_one() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: None,
        offset: 2,
    });

    let min_asc = build_aggregate_route(&plan, AggregateKind::Min);
    let max_asc = build_aggregate_route(&plan, AggregateKind::Max);
    assert_eq!(min_asc.scan_hints.physical_fetch_hint, Some(3));
    assert_eq!(max_asc.scan_hints.physical_fetch_hint, None);
    assert_eq!(min_asc.aggregate_seek_fetch_hint(), Some(3));
    assert_eq!(max_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(
        min_asc.aggregate_seek_spec(),
        Some(AggregateSeekSpec::First { fetch: 3 })
    );
    assert_eq!(max_asc.aggregate_seek_spec(), None);

    let first_asc = build_aggregate_route(&plan, AggregateKind::First);
    assert_eq!(
        first_asc.aggregate_seek_fetch_hint(),
        None,
        "secondary extrema probe hints must stay route-owned and Min/Max-only"
    );

    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
        ],
    });
    let max_desc = build_aggregate_route(&plan, AggregateKind::Max);
    let min_desc = build_aggregate_route(&plan, AggregateKind::Min);
    assert_eq!(max_desc.scan_hints.physical_fetch_hint, Some(3));
    assert_eq!(min_desc.scan_hints.physical_fetch_hint, None);
    assert_eq!(max_desc.aggregate_seek_fetch_hint(), Some(3));
    assert_eq!(min_desc.aggregate_seek_fetch_hint(), None);
    assert_eq!(
        max_desc.aggregate_seek_spec(),
        Some(AggregateSeekSpec::Last { fetch: 3 })
    );
    assert_eq!(min_desc.aggregate_seek_spec(), None);
}

#[test]
fn route_matrix_aggregate_index_range_desc_with_window_enables_pushdown_hint() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let route_plan = build_aggregate_route(&plan, AggregateKind::Last);

    assert_eq!(route_plan.execution_mode, RouteExecutionMode::Streaming);
    assert!(route_plan.desc_physical_reverse_supported());
    assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(3));
    assert_eq!(
        route_plan.index_range_limit_spec.map(|spec| spec.fetch),
        Some(3)
    );
}

#[test]
fn route_matrix_aggregate_count_pushdown_boundary_matrix() {
    let assert_count_route = |plan: &AccessPlannedQuery, expected_fold_mode: AggregateFoldMode| {
        let route_plan = build_aggregate_route(plan, AggregateKind::Count);
        assert_eq!(
            route_plan.execution_mode,
            RouteExecutionMode::Streaming,
            "COUNT pushdown matrix should stay on streaming execution mode",
        );
        assert_eq!(
            route_plan.aggregate_fold_mode, expected_fold_mode,
            "COUNT pushdown matrix should preserve fold-mode contract",
        );

        route_plan
    };

    let mut full_scan =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    full_scan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });
    let _full_scan_route = assert_count_route(&full_scan, AggregateFoldMode::KeysOnly);

    let mut key_range = AccessPlannedQuery::new(
        AccessPath::<Value>::KeyRange {
            start: Value::Ulid(Ulid::from_u128(1)),
            end: Value::Ulid(Ulid::from_u128(9)),
        },
        MissingRowPolicy::Ignore,
    );
    key_range.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });
    let _key_range_route = assert_count_route(&key_range, AggregateFoldMode::KeysOnly);

    let mut secondary = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    secondary.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    let _secondary_route = assert_count_route(&secondary, AggregateFoldMode::ExistingRows);

    let mut index_range = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    index_range.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    index_range.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 1,
    });
    let index_range_route = assert_count_route(&index_range, AggregateFoldMode::ExistingRows);
    assert_eq!(
        index_range_route
            .index_range_limit_spec
            .map(|spec| spec.fetch),
        Some(3),
        "index-range COUNT with page window should inherit bounded pushdown fetch",
    );
}

#[test]
fn route_matrix_secondary_extrema_probe_eligibility_is_min_max_only() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::<Value>::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: None,
        offset: 2,
    });

    let min_asc = build_aggregate_route(&plan, AggregateKind::Min);
    let max_asc = build_aggregate_route(&plan, AggregateKind::Max);
    let first_asc = build_aggregate_route(&plan, AggregateKind::First);
    let exists_asc = build_aggregate_route(&plan, AggregateKind::Exists);
    let last_asc = build_aggregate_route(&plan, AggregateKind::Last);
    assert_eq!(min_asc.aggregate_seek_fetch_hint(), Some(3));
    assert_eq!(max_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(first_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(exists_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(last_asc.aggregate_seek_fetch_hint(), None);
    assert_eq!(
        min_asc.aggregate_seek_spec(),
        Some(AggregateSeekSpec::First { fetch: 3 })
    );
    assert_eq!(max_asc.aggregate_seek_spec(), None);
    assert_eq!(first_asc.aggregate_seek_spec(), None);
    assert_eq!(exists_asc.aggregate_seek_spec(), None);
    assert_eq!(last_asc.aggregate_seek_spec(), None);

    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
        ],
    });
    let min_desc = build_aggregate_route(&plan, AggregateKind::Min);
    let max_desc = build_aggregate_route(&plan, AggregateKind::Max);
    assert_eq!(min_desc.aggregate_seek_fetch_hint(), None);
    assert_eq!(max_desc.aggregate_seek_fetch_hint(), Some(3));
    assert_eq!(min_desc.aggregate_seek_spec(), None);
    assert_eq!(
        max_desc.aggregate_seek_spec(),
        Some(AggregateSeekSpec::Last { fetch: 3 })
    );
}

#[test]
fn route_matrix_index_predicate_compile_mode_subset_vs_strict_boundary_is_explicit() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(12)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    plan.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let finalized = finalized_plan_for_authority(route_capability_authority(), &plan);

    let execution_preparation =
        ExecutionPreparation::from_plan(&finalized, slot_map_for_model_plan(&finalized));
    let predicate_slots = execution_preparation
        .compiled_predicate()
        .expect("predicate slots should compile for mixed strict/residual predicate");
    let index_slots = execution_preparation
        .slot_map()
        .expect("index-range plan should expose one resolvable index slot");
    let subset_program = compile_index_program(
        predicate_slots.executable(),
        index_slots,
        IndexCompilePolicy::ConservativeSubset,
    );
    let strict_program = compile_index_program(
        predicate_slots.executable(),
        index_slots,
        IndexCompilePolicy::StrictAllOrNone,
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
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    strict_compatible.scalar_plan_mut().predicate =
        Some(Predicate::eq("rank".to_string(), Value::Uint(12)));
    strict_compatible.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    strict_compatible.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let strict_compatible_route = build_aggregate_route(&strict_compatible, AggregateKind::Exists);
    assert_eq!(
        strict_compatible_route.execution_mode,
        RouteExecutionMode::Streaming,
        "strict-compilable secondary predicate shapes should keep aggregate streaming eligibility",
    );

    let mut strict_uncertain = strict_compatible.clone();
    strict_uncertain.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(12)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    let strict_uncertain_route = build_aggregate_route(&strict_uncertain, AggregateKind::Exists);
    assert_eq!(
        strict_uncertain_route.execution_mode,
        RouteExecutionMode::Materialized,
        "aggregate route planning must force materialized execution when strict index compile fails",
    );

    let load_route = build_load_route_plan(&strict_uncertain)
        .expect("load route plan should build for strict/subset parity boundary shape");
    assert_eq!(
        load_route.execution_mode,
        RouteExecutionMode::Streaming,
        "load routing should remain streaming for the same shape via conservative subset policy",
    );
}

#[test]
fn route_matrix_aggregate_exists_secondary_order_prefix_shape_stays_materialized() {
    let mut plan = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });

    let route_plan = build_aggregate_route(&plan, AggregateKind::Exists);

    assert_eq!(
        route_plan.execution_mode,
        RouteExecutionMode::Materialized,
        "ordered secondary-prefix EXISTS must stay on the canonical materialized lane",
    );
}

#[test]
fn route_matrix_strict_vs_subset_decision_logs_are_stable() {
    let mut strict_compatible = AccessPlannedQuery::new(
        AccessPath::index_range(
            ROUTE_CAPABILITY_INDEX_MODELS[0],
            vec![],
            Bound::Included(Value::Uint(10)),
            Bound::Excluded(Value::Uint(30)),
        ),
        MissingRowPolicy::Ignore,
    );
    strict_compatible.scalar_plan_mut().predicate =
        Some(Predicate::eq("rank".to_string(), Value::Uint(12)));
    strict_compatible.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    });
    strict_compatible.scalar_plan_mut().page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let strict_compatible_route = build_aggregate_route(&strict_compatible, AggregateKind::Exists);
    let mut strict_uncertain = strict_compatible.clone();
    strict_uncertain.scalar_plan_mut().predicate = Some(Predicate::And(vec![
        Predicate::eq("rank".to_string(), Value::Uint(12)),
        Predicate::TextContains {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        },
    ]));
    let strict_uncertain_route = build_aggregate_route(&strict_uncertain, AggregateKind::Exists);
    let load_route = build_load_route_plan(&strict_uncertain)
        .expect("load route plan should build for strict/subset log shape");

    let strict_compatible_log = format!(
        "aggregate:mode={:?};fold={:?};fetch={:?};secondary_probe={:?};index_range_limit={};continuation={:?}",
        strict_compatible_route.execution_mode,
        strict_compatible_route.aggregate_fold_mode,
        strict_compatible_route.scan_hints.physical_fetch_hint,
        strict_compatible_route.aggregate_seek_fetch_hint(),
        strict_compatible_route.index_range_limit_fast_path_enabled(),
        strict_compatible_route.continuation().mode(),
    );
    let strict_uncertain_log = format!(
        "aggregate:mode={:?};fold={:?};fetch={:?};secondary_probe={:?};index_range_limit={};continuation={:?}",
        strict_uncertain_route.execution_mode,
        strict_uncertain_route.aggregate_fold_mode,
        strict_uncertain_route.scan_hints.physical_fetch_hint,
        strict_uncertain_route.aggregate_seek_fetch_hint(),
        strict_uncertain_route.index_range_limit_fast_path_enabled(),
        strict_uncertain_route.continuation().mode(),
    );
    let load_log = format!(
        "load:mode={:?};fetch={:?};scan_budget={:?};index_range_limit={};continuation={:?}",
        load_route.execution_mode,
        load_route.scan_hints.physical_fetch_hint,
        load_route.scan_hints.load_scan_budget_hint,
        load_route.index_range_limit_fast_path_enabled(),
        load_route.continuation().mode(),
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
