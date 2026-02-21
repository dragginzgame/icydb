use super::*;

#[test]
fn load_distinct_flag_preserves_union_pagination_rows_and_boundaries() {
    setup_pagination_test();

    let rows = [
        (39_201, 7, 10, "g7-r10"),
        (39_202, 7, 20, "g7-r20-a"),
        (39_203, 7, 20, "g7-r20-b"),
        (39_204, 7, 30, "g7-r30"),
        (39_205, 8, 15, "g8-r15"),
    ];
    seed_pushdown_rows(&rows);

    let id1 = Ulid::from_u128(39_201);
    let id2 = Ulid::from_u128(39_202);
    let id3 = Ulid::from_u128(39_203);
    let id4 = Ulid::from_u128(39_204);
    let id5 = Ulid::from_u128(39_205);

    let build_plan = |distinct: bool, limit: u32| {
        ExecutablePlan::<PushdownParityEntity>::new(LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            access: AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id4])),
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id5])),
            ]),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    ("rank".to_string(), OrderDirection::Desc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            distinct,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(limit),
                offset: 0,
            }),
            consistency: ReadConsistency::MissingOk,
        })
    };

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    for limit in [1_u32, 2, 3] {
        let (ids_plain, boundaries_plain) =
            collect_all_pages_from_executable_plan(&load, || build_plan(false, limit), 12);
        let (ids_distinct, boundaries_distinct) =
            collect_all_pages_from_executable_plan(&load, || build_plan(true, limit), 12);

        assert_eq!(
            ids_plain, ids_distinct,
            "distinct on/off should preserve canonical row order for limit={limit}"
        );
        assert_eq!(
            boundaries_plain, boundaries_distinct,
            "distinct on/off should preserve continuation boundaries for limit={limit}"
        );
    }
}

#[test]
fn load_distinct_union_resume_matrix_is_boundary_complete() {
    setup_pagination_test();

    let rows = [
        (39_301, 7, 10, "g7-r10"),
        (39_302, 7, 20, "g7-r20"),
        (39_303, 7, 30, "g7-r30"),
        (39_304, 7, 40, "g7-r40"),
        (39_305, 7, 50, "g7-r50"),
        (39_306, 8, 10, "g8-r10"),
        (39_307, 8, 20, "g8-r20"),
        (39_308, 8, 30, "g8-r30"),
        (39_309, 9, 10, "g9-r10"),
    ];
    seed_pushdown_rows(&rows);

    let id1 = Ulid::from_u128(39_301);
    let id2 = Ulid::from_u128(39_302);
    let id3 = Ulid::from_u128(39_303);
    let id4 = Ulid::from_u128(39_304);
    let id5 = Ulid::from_u128(39_305);
    let id6 = Ulid::from_u128(39_306);
    let id7 = Ulid::from_u128(39_307);
    let id8 = Ulid::from_u128(39_308);
    let id9 = Ulid::from_u128(39_309);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    for (case_name, order_direction) in
        [("asc", OrderDirection::Asc), ("desc", OrderDirection::Desc)]
    {
        let expected_ids = if order_direction == OrderDirection::Asc {
            vec![id1, id2, id3, id4, id5, id6, id7, id8, id9]
        } else {
            vec![id9, id8, id7, id6, id5, id4, id3, id2, id1]
        };

        for limit in [1_u32, 2, 3] {
            let build_plan = || {
                ExecutablePlan::<PushdownParityEntity>::new(LogicalPlan {
                    mode: QueryMode::Load(LoadSpec::new()),
                    access: AccessPlan::Union(vec![
                        AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4, id5])),
                        AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7])),
                        AccessPlan::path(AccessPath::ByKeys(vec![id5, id6, id7, id8, id9])),
                    ]),
                    predicate: None,
                    order: Some(OrderSpec {
                        fields: vec![("id".to_string(), order_direction)],
                    }),
                    distinct: true,
                    delete_limit: None,
                    page: Some(PageSpec {
                        limit: Some(limit),
                        offset: 0,
                    }),
                    consistency: ReadConsistency::MissingOk,
                })
            };

            let (ids, boundaries) = collect_all_pages_from_executable_plan(&load, build_plan, 30);
            assert_eq!(
                ids, expected_ids,
                "case '{case_name}' with limit={limit} should preserve distinct canonical ordering",
            );

            let unique: BTreeSet<Ulid> = ids.iter().copied().collect();
            assert_eq!(
                unique.len(),
                ids.len(),
                "case '{case_name}' with limit={limit} distinct pagination must not duplicate rows",
            );

            let context = format!("case '{case_name}' with limit={limit}");
            assert_resume_suffixes_from_boundaries(
                &load,
                &build_plan,
                &boundaries,
                &expected_ids,
                30,
                &context,
            );
        }
    }
}

#[test]
fn load_distinct_desc_secondary_pushdown_resume_matrix_is_boundary_complete() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(39_401);
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let mut expected_ids = ordered_ids_from_group_rank_index(7);
    expected_ids.reverse();

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);
    for limit in [1_u32, 2, 3] {
        // Phase 1: verify this DISTINCT DESC shape stays on secondary pushdown.
        let (seed_page, seed_trace) = load
            .execute_paged_with_cursor_traced(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(predicate.clone())
                    .order_by_desc("rank")
                    .order_by_desc("id")
                    .distinct()
                    .limit(limit)
                    .plan()
                    .expect("distinct secondary DESC seed plan should build"),
                None,
            )
            .expect("distinct secondary DESC seed page should execute");
        let seed_trace = seed_trace.expect("debug trace should be present");
        assert_eq!(
            seed_trace.optimization,
            Some(ExecutionOptimization::SecondaryOrderPushdown),
            "distinct DESC secondary plan should stay on pushdown for limit={limit}",
        );
        let _ = seed_page;

        // Phase 2: verify full paged traversal and boundary-complete resume suffixes.
        let build_plan = || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by_desc("rank")
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .plan()
                .expect("distinct secondary DESC plan should build")
        };
        let (ids, boundaries) = collect_all_pages_from_executable_plan(&load, build_plan, 20);
        assert_eq!(
            ids, expected_ids,
            "distinct DESC secondary pushdown should preserve canonical ordering for limit={limit}",
        );

        let unique: BTreeSet<Ulid> = ids.iter().copied().collect();
        assert_eq!(
            unique.len(),
            ids.len(),
            "distinct DESC secondary pagination must not emit duplicates for limit={limit}",
        );

        let context = format!("distinct DESC secondary limit={limit}");
        assert_resume_suffixes_from_boundaries(
            &load,
            &build_plan,
            &boundaries,
            &expected_ids,
            20,
            &context,
        );
    }
}

#[test]
fn load_distinct_desc_secondary_fast_path_and_fallback_match_ids_and_boundaries() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(39_501);
    seed_pushdown_rows(&rows);

    let group7_ids = pushdown_group_ids(&rows, 7);
    let predicate = pushdown_group_predicate(7);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);
    for limit in [1_u32, 2, 3] {
        // Phase 1: fast path uses secondary pushdown for this DISTINCT DESC shape.
        let (_fast_seed_page, fast_trace) = load
            .execute_paged_with_cursor_traced(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(predicate.clone())
                    .order_by_desc("rank")
                    .order_by_desc("id")
                    .distinct()
                    .limit(limit)
                    .plan()
                    .expect("distinct DESC fast-path seed plan should build"),
                None,
            )
            .expect("distinct DESC fast-path seed page should execute");
        let fast_trace = fast_trace.expect("debug trace should be present");
        assert_eq!(
            fast_trace.optimization,
            Some(ExecutionOptimization::SecondaryOrderPushdown),
            "distinct DESC fast-path seed execution should select secondary pushdown for limit={limit}",
        );

        // Phase 2: fallback by-ids path should remain non-optimized and semantically identical.
        let (_fallback_seed_page, fallback_trace) = load
            .execute_paged_with_cursor_traced(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .by_ids(group7_ids.iter().copied())
                    .order_by_desc("rank")
                    .order_by_desc("id")
                    .distinct()
                    .limit(limit)
                    .plan()
                    .expect("distinct DESC fallback seed plan should build"),
                None,
            )
            .expect("distinct DESC fallback seed page should execute");
        let fallback_trace = fallback_trace.expect("debug trace should be present");
        assert_eq!(
            fallback_trace.optimization, None,
            "distinct DESC by-ids fallback seed execution should not report fast-path optimization for limit={limit}",
        );

        let build_fast_plan = || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by_desc("rank")
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .plan()
                .expect("distinct DESC fast-path plan should build")
        };
        let build_fallback_plan = || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .by_ids(group7_ids.iter().copied())
                .order_by_desc("rank")
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .plan()
                .expect("distinct DESC fallback plan should build")
        };

        let (fast_ids, fast_boundaries, _fast_tokens) =
            collect_all_pages_from_executable_plan_with_tokens(&load, build_fast_plan, 20);
        let (fallback_ids, fallback_boundaries, _fallback_tokens) =
            collect_all_pages_from_executable_plan_with_tokens(&load, build_fallback_plan, 20);

        assert_eq!(
            fast_ids, fallback_ids,
            "distinct DESC fast-path and fallback ids should match for limit={limit}",
        );
        assert_eq!(
            fast_boundaries, fallback_boundaries,
            "distinct DESC fast-path and fallback boundaries should match for limit={limit}",
        );
    }
}

#[test]
fn load_distinct_desc_index_range_limit_pushdown_resume_matrix_and_fallback_parity() {
    setup_pagination_test();

    let rows = [
        (39_601, 10, "t10-a"),
        (39_602, 10, "t10-b"),
        (39_603, 20, "t20-a"),
        (39_604, 20, "t20-b"),
        (39_605, 25, "t25"),
        (39_606, 28, "t28-a"),
        (39_607, 28, "t28-b"),
        (39_608, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    // Phase 1: compute canonical expected IDs for [10, 30) ordered by (tag DESC, id DESC).
    let mut expected_rows = rows
        .iter()
        .filter(|(_, tag, _)| *tag >= 10 && *tag < 30)
        .map(|(id, tag, _)| (*tag, Ulid::from_u128(*id)))
        .collect::<Vec<_>>();
    expected_rows.sort_by(|(left_tag, left_id), (right_tag, right_id)| {
        right_tag.cmp(left_tag).then_with(|| right_id.cmp(left_id))
    });
    let expected_ids = expected_rows
        .iter()
        .map(|(_, id)| *id)
        .collect::<Vec<Ulid>>();

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    for limit in [1_u32, 2, 3] {
        // Phase 2: verify this DISTINCT DESC shape stays on index-range limit pushdown.
        let (_seed_page, seed_trace) = load
            .execute_paged_with_cursor_traced(
                ExecutablePlan::<IndexedMetricsEntity>::new(LogicalPlan {
                    mode: QueryMode::Load(LoadSpec::new()),
                    access: AccessPlan::path(AccessPath::IndexRange {
                        index: INDEXED_METRICS_INDEX_MODELS[0],
                        prefix: Vec::new(),
                        lower: Bound::Included(Value::Uint(10)),
                        upper: Bound::Excluded(Value::Uint(30)),
                    }),
                    predicate: None,
                    order: Some(OrderSpec {
                        fields: vec![
                            ("tag".to_string(), OrderDirection::Desc),
                            ("id".to_string(), OrderDirection::Desc),
                        ],
                    }),
                    distinct: true,
                    delete_limit: None,
                    page: Some(PageSpec {
                        limit: Some(limit),
                        offset: 0,
                    }),
                    consistency: ReadConsistency::MissingOk,
                }),
                None,
            )
            .expect("distinct DESC index-range seed page should execute");
        let seed_trace = seed_trace.expect("debug trace should be present");
        assert_eq!(
            seed_trace.optimization,
            Some(ExecutionOptimization::IndexRangeLimitPushdown),
            "distinct DESC index-range seed execution should use limit pushdown for limit={limit}",
        );

        // Phase 3: verify paged traversal and boundary-complete resume suffixes.
        let build_fast_plan = || {
            ExecutablePlan::<IndexedMetricsEntity>::new(LogicalPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                access: AccessPlan::path(AccessPath::IndexRange {
                    index: INDEXED_METRICS_INDEX_MODELS[0],
                    prefix: Vec::new(),
                    lower: Bound::Included(Value::Uint(10)),
                    upper: Bound::Excluded(Value::Uint(30)),
                }),
                predicate: None,
                order: Some(OrderSpec {
                    fields: vec![
                        ("tag".to_string(), OrderDirection::Desc),
                        ("id".to_string(), OrderDirection::Desc),
                    ],
                }),
                distinct: true,
                delete_limit: None,
                page: Some(PageSpec {
                    limit: Some(limit),
                    offset: 0,
                }),
                consistency: ReadConsistency::MissingOk,
            })
        };
        let (fast_ids, fast_boundaries) =
            collect_all_pages_from_executable_plan(&load, build_fast_plan, 20);
        assert_eq!(
            fast_ids, expected_ids,
            "distinct DESC index-range pushdown should preserve canonical ordering for limit={limit}",
        );

        let context = format!("distinct DESC index-range limit={limit}");
        assert_resume_suffixes_from_boundaries(
            &load,
            &build_fast_plan,
            &fast_boundaries,
            &expected_ids,
            20,
            &context,
        );

        // Phase 4: fallback by-ids semantics must match IDs and boundaries.
        let build_fallback_plan = || {
            Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .by_ids(expected_ids.iter().copied())
                .order_by_desc("tag")
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .plan()
                .expect("distinct DESC index-range fallback plan should build")
        };
        let (fallback_ids, fallback_boundaries) =
            collect_all_pages_from_executable_plan(&load, build_fallback_plan, 20);
        assert_eq!(
            fast_ids, fallback_ids,
            "distinct DESC index-range fast path and fallback ids should match for limit={limit}",
        );
        assert_eq!(
            fast_boundaries, fallback_boundaries,
            "distinct DESC index-range fast path and fallback boundaries should match for limit={limit}",
        );
    }
}

#[test]
fn load_distinct_desc_pk_fast_path_and_fallback_match_ids_and_boundaries() {
    setup_pagination_test();

    let keys = [
        39_651_u128,
        39_652_u128,
        39_653_u128,
        39_654_u128,
        39_655_u128,
        39_656_u128,
        39_657_u128,
    ];
    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [39_656_u128, 39_651, 39_655, 39_653, 39_657, 39_652, 39_654] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("distinct DESC pk parity seed save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    for limit in [1_u32, 2, 3] {
        // Phase 1: confirm full-scan DESC stays on PK fast path.
        let (_fast_seed_page, fast_trace) = load
            .execute_paged_with_cursor_traced(
                Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                    .order_by_desc("id")
                    .distinct()
                    .limit(limit)
                    .offset(1)
                    .plan()
                    .expect("distinct DESC PK fast-path seed plan should build"),
                None,
            )
            .expect("distinct DESC PK fast-path seed page should execute");
        let fast_trace = fast_trace.expect("debug trace should be present");
        assert_eq!(
            fast_trace.optimization,
            Some(ExecutionOptimization::PrimaryKey),
            "distinct DESC full-scan seed execution should use PK fast path for limit={limit}",
        );

        // Phase 2: by-ids fallback should stay non-optimized.
        let (_fallback_seed_page, fallback_trace) = load
            .execute_paged_with_cursor_traced(
                Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                    .by_ids(keys.into_iter().map(Ulid::from_u128))
                    .order_by_desc("id")
                    .distinct()
                    .limit(limit)
                    .offset(1)
                    .plan()
                    .expect("distinct DESC PK fallback seed plan should build"),
                None,
            )
            .expect("distinct DESC PK fallback seed page should execute");
        let fallback_trace = fallback_trace.expect("debug trace should be present");
        assert_eq!(
            fallback_trace.optimization, None,
            "distinct DESC by-ids seed execution should not report fast-path optimization for limit={limit}",
        );

        // Phase 3: compare full traversal IDs and continuation boundaries.
        let build_fast_plan = || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .offset(1)
                .plan()
                .expect("distinct DESC PK fast-path plan should build")
        };
        let build_fallback_plan = || {
            Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .by_ids(keys.into_iter().map(Ulid::from_u128))
                .order_by_desc("id")
                .distinct()
                .limit(limit)
                .offset(1)
                .plan()
                .expect("distinct DESC PK fallback plan should build")
        };

        let (fast_ids, fast_boundaries, _fast_tokens) =
            collect_all_pages_from_executable_plan_with_tokens(&load, build_fast_plan, 20);
        let (fallback_ids, fallback_boundaries, _fallback_tokens) =
            collect_all_pages_from_executable_plan_with_tokens(&load, build_fallback_plan, 20);
        assert_eq!(
            fast_ids, fallback_ids,
            "distinct DESC PK fast-path and fallback ids should match for limit={limit}",
        );
        assert_eq!(
            fast_boundaries, fallback_boundaries,
            "distinct DESC PK fast-path and fallback boundaries should match for limit={limit}",
        );
    }
}

#[test]
fn load_distinct_offset_fast_path_and_fallback_match_ids_and_boundaries() {
    setup_pagination_test();

    // Secondary-index source fixture.
    let secondary_rows = pushdown_rows_with_group9(42_301);
    seed_pushdown_rows(&secondary_rows);
    let secondary_predicate = pushdown_group_predicate(7);
    let secondary_group_ids = pushdown_group_ids(&secondary_rows, 7);

    // Index-range source fixture.
    let index_rows = [
        (42_401, 10, "t10-a"),
        (42_402, 10, "t10-b"),
        (42_403, 20, "t20-a"),
        (42_404, 20, "t20-b"),
        (42_405, 25, "t25"),
        (42_406, 28, "t28-a"),
        (42_407, 28, "t28-b"),
        (42_408, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&index_rows);

    let load_secondary = LoadExecutor::<PushdownParityEntity>::new(DB, true);
    let load_index_range = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    for (case_name, direction) in [("asc", OrderDirection::Asc), ("desc", OrderDirection::Desc)] {
        // ------------------------------------------------------------------
        // Secondary distinct+offset parity: pushdown vs fallback
        // ------------------------------------------------------------------

        let build_secondary_fast = || {
            let base = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(secondary_predicate.clone())
                .distinct()
                .limit(2)
                .offset(1);
            let ordered = match direction {
                OrderDirection::Asc => base.order_by("rank").order_by("id"),
                OrderDirection::Desc => base.order_by_desc("rank").order_by_desc("id"),
            };

            ordered
                .plan()
                .expect("distinct secondary offset fast-path plan should build")
        };
        let build_secondary_fallback = || {
            let base = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .by_ids(secondary_group_ids.iter().copied())
                .distinct()
                .limit(2)
                .offset(1);
            let ordered = match direction {
                OrderDirection::Asc => base.order_by("rank").order_by("id"),
                OrderDirection::Desc => base.order_by_desc("rank").order_by_desc("id"),
            };

            ordered
                .plan()
                .expect("distinct secondary offset fallback plan should build")
        };

        let (_seed_fast, trace_fast) = load_secondary
            .execute_paged_with_cursor_traced(build_secondary_fast(), None)
            .expect("distinct secondary offset fast-path seed should execute");
        let trace_fast = trace_fast.expect("debug trace should be present");
        assert_eq!(
            trace_fast.optimization,
            Some(ExecutionOptimization::SecondaryOrderPushdown),
            "distinct secondary offset fast path should use pushdown for case={case_name}",
        );

        let (_seed_fallback, trace_fallback) = load_secondary
            .execute_paged_with_cursor_traced(build_secondary_fallback(), None)
            .expect("distinct secondary offset fallback seed should execute");
        let trace_fallback = trace_fallback.expect("debug trace should be present");
        assert_eq!(
            trace_fallback.optimization, None,
            "distinct secondary offset fallback should remain non-optimized for case={case_name}",
        );

        let (secondary_fast_ids, secondary_fast_boundaries, secondary_fast_tokens) =
            collect_all_pages_from_executable_plan_with_tokens(
                &load_secondary,
                build_secondary_fast,
                20,
            );
        let (secondary_fallback_ids, secondary_fallback_boundaries, _secondary_fallback_tokens) =
            collect_all_pages_from_executable_plan_with_tokens(
                &load_secondary,
                build_secondary_fallback,
                20,
            );
        assert_eq!(
            secondary_fast_ids, secondary_fallback_ids,
            "distinct secondary offset fast/fallback ids should match for case={case_name}",
        );
        assert_eq!(
            secondary_fast_boundaries, secondary_fallback_boundaries,
            "distinct secondary offset fast/fallback boundaries should match for case={case_name}",
        );
        assert_resume_suffixes_from_tokens(
            &load_secondary,
            &build_secondary_fast,
            &secondary_fast_tokens,
            &secondary_fast_ids,
            20,
            case_name,
        );

        // ------------------------------------------------------------------
        // Index-range distinct+offset parity: pushdown vs fallback
        // ------------------------------------------------------------------

        let build_index_range_fast = || {
            ExecutablePlan::<IndexedMetricsEntity>::new(LogicalPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                access: AccessPlan::path(AccessPath::IndexRange {
                    index: INDEXED_METRICS_INDEX_MODELS[0],
                    prefix: Vec::new(),
                    lower: Bound::Included(Value::Uint(10)),
                    upper: Bound::Excluded(Value::Uint(30)),
                }),
                predicate: None,
                order: Some(OrderSpec {
                    fields: vec![
                        ("tag".to_string(), direction),
                        ("id".to_string(), direction),
                    ],
                }),
                distinct: true,
                delete_limit: None,
                page: Some(PageSpec {
                    limit: Some(2),
                    offset: 1,
                }),
                consistency: ReadConsistency::MissingOk,
            })
        };

        let mut index_rows_sorted = index_rows
            .iter()
            .filter(|(_, tag, _)| *tag >= 10 && *tag < 30)
            .map(|(id, tag, _)| (*tag, Ulid::from_u128(*id)))
            .collect::<Vec<_>>();
        index_rows_sorted.sort_by(
            |(left_tag, left_id), (right_tag, right_id)| match direction {
                OrderDirection::Asc => left_tag.cmp(right_tag).then_with(|| left_id.cmp(right_id)),
                OrderDirection::Desc => right_tag.cmp(left_tag).then_with(|| right_id.cmp(left_id)),
            },
        );
        let index_candidate_ids = index_rows_sorted
            .iter()
            .map(|(_, id)| *id)
            .collect::<Vec<_>>();

        let build_index_range_fallback = || {
            let base = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
                .by_ids(index_candidate_ids.iter().copied())
                .distinct()
                .limit(2)
                .offset(1);
            let ordered = match direction {
                OrderDirection::Asc => base.order_by("tag").order_by("id"),
                OrderDirection::Desc => base.order_by_desc("tag").order_by_desc("id"),
            };

            ordered
                .plan()
                .expect("distinct index-range offset fallback plan should build")
        };

        let (_seed_fast, trace_fast) = load_index_range
            .execute_paged_with_cursor_traced(build_index_range_fast(), None)
            .expect("distinct index-range offset fast-path seed should execute");
        let trace_fast = trace_fast.expect("debug trace should be present");
        assert_eq!(
            trace_fast.optimization,
            Some(ExecutionOptimization::IndexRangeLimitPushdown),
            "distinct index-range offset fast path should use limit pushdown for case={case_name}",
        );

        let (_seed_fallback, trace_fallback) = load_index_range
            .execute_paged_with_cursor_traced(build_index_range_fallback(), None)
            .expect("distinct index-range offset fallback seed should execute");
        let trace_fallback = trace_fallback.expect("debug trace should be present");
        assert_eq!(
            trace_fallback.optimization, None,
            "distinct index-range offset fallback should remain non-optimized for case={case_name}",
        );

        let (index_fast_ids, index_fast_boundaries, index_fast_tokens) =
            collect_all_pages_from_executable_plan_with_tokens(
                &load_index_range,
                build_index_range_fast,
                20,
            );
        let (index_fallback_ids, index_fallback_boundaries, _index_fallback_tokens) =
            collect_all_pages_from_executable_plan_with_tokens(
                &load_index_range,
                build_index_range_fallback,
                20,
            );
        assert_eq!(
            index_fast_ids, index_fallback_ids,
            "distinct index-range offset fast/fallback ids should match for case={case_name}",
        );
        assert_eq!(
            index_fast_boundaries, index_fallback_boundaries,
            "distinct index-range offset fast/fallback boundaries should match for case={case_name}",
        );
        assert_resume_suffixes_from_tokens(
            &load_index_range,
            &build_index_range_fast,
            &index_fast_tokens,
            &index_fast_ids,
            20,
            case_name,
        );
    }
}

#[test]
fn load_distinct_mixed_direction_secondary_shape_rejects_pushdown_and_matches_fallback() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(39_701);
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    // Phase 1: mixed direction remains non-pushdown, even with DISTINCT.
    let predicate = pushdown_group_predicate(7);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .order_by("id")
        .distinct()
        .explain()
        .expect("distinct mixed-direction explain should build");
    assert!(
        matches!(
            explain.order_pushdown,
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible { .. }
            )
        ),
        "distinct mixed-direction ordering should remain ineligible for secondary pushdown"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);
    for limit in [1_u32, 2, 3] {
        // Phase 2: traced execution should confirm no fast-path optimization.
        let (_index_seed_page, index_seed_trace) = load
            .execute_paged_with_cursor_traced(
                Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                    .filter(predicate.clone())
                    .order_by_desc("rank")
                    .order_by("id")
                    .distinct()
                    .limit(limit)
                    .plan()
                    .expect("distinct mixed-direction index-shape seed plan should build"),
                None,
            )
            .expect("distinct mixed-direction index-shape seed page should execute");
        let index_seed_trace = index_seed_trace.expect("debug trace should be present");
        assert_eq!(
            index_seed_trace.optimization, None,
            "distinct mixed-direction index-shape seed execution should not report fast-path optimization for limit={limit}",
        );

        // Phase 3: index-shape and by-ids fallback must match IDs and boundaries.
        let build_index_shape_plan = || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .order_by_desc("rank")
                .order_by("id")
                .distinct()
                .limit(limit)
                .plan()
                .expect("distinct mixed-direction index-shape plan should build")
        };
        let build_fallback_plan = || {
            Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .by_ids(group7_ids.iter().copied())
                .order_by_desc("rank")
                .order_by("id")
                .distinct()
                .limit(limit)
                .plan()
                .expect("distinct mixed-direction fallback plan should build")
        };

        let (index_shape_ids, index_shape_boundaries) =
            collect_all_pages_from_executable_plan(&load, build_index_shape_plan, 20);
        let (fallback_ids, fallback_boundaries) =
            collect_all_pages_from_executable_plan(&load, build_fallback_plan, 20);
        assert_eq!(
            index_shape_ids, fallback_ids,
            "distinct mixed-direction index-shape and fallback ids should match for limit={limit}",
        );
        assert_eq!(
            index_shape_boundaries, fallback_boundaries,
            "distinct mixed-direction index-shape and fallback boundaries should match for limit={limit}",
        );
    }
}
