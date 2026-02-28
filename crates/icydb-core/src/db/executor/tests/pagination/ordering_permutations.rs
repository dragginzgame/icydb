use super::*;

#[test]
fn load_mixed_direction_resume_matrix_is_boundary_complete() {
    setup_pagination_test();

    let rows = [
        (39_101, 7, 10, "g7-r10"),
        (39_102, 7, 20, "g7-r20-a"),
        (39_103, 7, 20, "g7-r20-b"),
        (39_104, 7, 30, "g7-r30-a"),
        (39_105, 7, 30, "g7-r30-b"),
        (39_106, 7, 40, "g7-r40"),
        (39_107, 8, 20, "g8-r20"),
        (39_108, 8, 30, "g8-r30"),
    ];
    seed_pushdown_rows(&rows);

    let id1 = Ulid::from_u128(39_101);
    let id2 = Ulid::from_u128(39_102);
    let id3 = Ulid::from_u128(39_103);
    let id4 = Ulid::from_u128(39_104);
    let id5 = Ulid::from_u128(39_105);
    let id6 = Ulid::from_u128(39_106);
    let id7 = Ulid::from_u128(39_107);
    let id8 = Ulid::from_u128(39_108);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let assert_resume_case = |case_name: &'static str,
                              filter_group: Option<u32>,
                              group_desc: Option<bool>,
                              rank_desc: bool,
                              id_desc: bool,
                              expected_ids: Vec<Ulid>| {
        let order_query = |limit: u32| {
            let mut query =
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore).limit(limit);

            if let Some(group) = filter_group {
                query = query.filter(pushdown_group_predicate(group));
            }

            if let Some(desc) = group_desc {
                query = if desc {
                    query.order_by_desc("group")
                } else {
                    query.order_by("group")
                };
            }

            query = if rank_desc {
                query.order_by_desc("rank")
            } else {
                query.order_by("rank")
            };

            if id_desc {
                query.order_by_desc("id")
            } else {
                query.order_by("id")
            }
        };

        let base_plan = order_query(16)
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("mixed-direction base plan should build");
        let base_page = load
            .execute_paged_with_cursor(base_plan, None)
            .expect("mixed-direction base page should execute");
        let base_ids = ids_from_items(&base_page.items.0);
        assert_eq!(
            base_ids, expected_ids,
            "case '{case_name}' should preserve mixed-direction canonical ordering",
        );

        for idx in 0..base_page.items.0.len() {
            let boundary_entity = &base_page.items.0[idx].1;
            assert_resume_after_entity(
                || order_query(16),
                boundary_entity,
                expected_ids[idx + 1..].to_vec(),
            );
        }

        for limit in [1_u32, 2, 3] {
            let (paged_ids, _) = collect_all_pages(&load, || order_query(limit), 20);
            assert_eq!(
                paged_ids, expected_ids,
                "case '{case_name}' with limit={limit} paged traversal should match unbounded mixed-direction ordering",
            );

            let unique: BTreeSet<Ulid> = paged_ids.iter().copied().collect();
            assert_eq!(
                unique.len(),
                paged_ids.len(),
                "case '{case_name}' with limit={limit} mixed-direction pagination must not duplicate rows",
            );
        }
    };

    assert_resume_case(
        "rank_desc_id_asc",
        Some(7),
        None,
        true,
        false,
        vec![id6, id4, id5, id2, id3, id1],
    );
    assert_resume_case(
        "rank_asc_id_desc",
        Some(7),
        None,
        false,
        true,
        vec![id1, id3, id2, id5, id4, id6],
    );
    assert_resume_case(
        "rank_desc_id_desc",
        Some(7),
        None,
        true,
        true,
        vec![id6, id5, id4, id3, id2, id1],
    );
    assert_resume_case(
        "rank_asc_id_asc",
        Some(7),
        None,
        false,
        false,
        vec![id1, id2, id3, id4, id5, id6],
    );
    assert_resume_case(
        "group_asc_rank_desc_id_asc",
        None,
        Some(false),
        true,
        false,
        vec![id6, id4, id5, id2, id3, id1, id8, id7],
    );
    assert_resume_case(
        "group_desc_rank_asc_id_desc",
        None,
        Some(true),
        false,
        true,
        vec![id7, id8, id1, id3, id2, id5, id4, id6],
    );
}

#[test]
fn load_mixed_direction_fallback_matches_uniform_fast_path_when_rank_is_unique() {
    setup_pagination_test();

    let rows = pushdown_rows_window(41_901);
    seed_pushdown_rows(&rows);
    let predicate = pushdown_group_predicate(7);

    let id1 = Ulid::from_u128(41_902);
    let id2 = Ulid::from_u128(41_903);
    let id3 = Ulid::from_u128(41_904);
    let expected_ids = vec![id1, id2, id3];

    // Phase 1: mixed-direction shape should remain fallback-only.
    let mixed_explain = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("rank")
        .order_by_desc("id")
        .explain()
        .expect("mixed-direction explain should build");
    assert!(
        matches!(
            mixed_explain.order_pushdown,
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible { .. }
            )
        ),
        "mixed-direction secondary ordering should remain ineligible for pushdown",
    );

    // Phase 2: equivalent uniform-direction shape should be pushdown-eligible.
    let uniform_explain = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("rank")
        .order_by("id")
        .explain()
        .expect("uniform-direction explain should build");
    assert!(
        matches!(
            uniform_explain.order_pushdown,
            ExplainOrderPushdown::EligibleSecondaryIndex { .. }
        ),
        "uniform secondary ordering should remain pushdown-eligible",
    );

    let build_mixed_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate.clone())
            .order_by("rank")
            .order_by_desc("id")
            .limit(2)
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("mixed-direction plan should build")
    };
    let build_uniform_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate.clone())
            .order_by("rank")
            .order_by("id")
            .limit(2)
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("uniform-direction plan should build")
    };

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);

    // Phase 3: residual-filter load routing is materialized; traces should
    // stay fallback-only even when ORDER pushdown eligibility is true.
    let (_seed_mixed, mixed_trace) = load
        .execute_paged_with_cursor_traced(build_mixed_plan(), None)
        .expect("mixed-direction seed page should execute");
    let mixed_trace = mixed_trace.expect("debug trace should be present");
    assert_eq!(
        mixed_trace.optimization, None,
        "mixed-direction execution should remain fallback-only",
    );

    let (_seed_uniform, uniform_trace) = load
        .execute_paged_with_cursor_traced(build_uniform_plan(), None)
        .expect("uniform-direction seed page should execute");
    let uniform_trace = uniform_trace.expect("debug trace should be present");
    assert_eq!(
        uniform_trace.optimization, None,
        "uniform-direction residual-filter execution should remain materialized",
    );

    // Phase 4: ordering + page boundaries must match across both paths.
    let (mixed_ids, mixed_boundaries, mixed_tokens) =
        collect_all_pages_from_executable_plan_with_tokens(&load, build_mixed_plan, 20);
    let (uniform_ids, uniform_boundaries, uniform_tokens) =
        collect_all_pages_from_executable_plan_with_tokens(&load, build_uniform_plan, 20);
    assert_eq!(
        mixed_ids, expected_ids,
        "mixed-direction traversal should preserve expected ordering",
    );
    assert_eq!(
        uniform_ids, expected_ids,
        "uniform-direction traversal should preserve expected ordering",
    );
    assert_eq!(
        mixed_ids, uniform_ids,
        "mixed-direction fallback and uniform pushdown should return identical IDs",
    );
    assert_eq!(
        mixed_boundaries, uniform_boundaries,
        "mixed-direction fallback and uniform pushdown should emit identical boundaries",
    );

    // Phase 5: resume from emitted tokens must be stable on both paths.
    assert_resume_suffixes_from_tokens(
        &load,
        &build_mixed_plan,
        &mixed_tokens,
        &mixed_ids,
        20,
        "mixed-direction fallback resumes",
    );
    assert_resume_suffixes_from_tokens(
        &load,
        &build_uniform_plan,
        &uniform_tokens,
        &uniform_ids,
        20,
        "uniform-direction pushdown resumes",
    );
}

#[test]
fn load_union_child_order_permutation_preserves_rows_and_continuation_boundaries() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [
        37_901_u128,
        37_902,
        37_903,
        37_904,
        37_905,
        37_906,
        37_907,
        37_908,
    ] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("union permutation seed save should succeed");
    }

    let id1 = Ulid::from_u128(37_901);
    let id2 = Ulid::from_u128(37_902);
    let id3 = Ulid::from_u128(37_903);
    let id4 = Ulid::from_u128(37_904);
    let id5 = Ulid::from_u128(37_905);
    let id6 = Ulid::from_u128(37_906);
    let id7 = Ulid::from_u128(37_907);
    let id8 = Ulid::from_u128(37_908);

    let build_union_abc = || {
        ExecutablePlan::<SimpleEntity>::new(AccessPlannedQuery {
            logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: Some(OrderSpec {
                    fields: vec![("id".to_string(), OrderDirection::Desc)],
                }),
                distinct: false,
                delete_limit: None,
                page: Some(PageSpec {
                    limit: Some(2),
                    offset: 0,
                }),
                consistency: MissingRowPolicy::Ignore,
            }),
            access: AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6])),
                AccessPlan::path(AccessPath::ByKeys(vec![id6, id7, id8])),
            ]),
        })
    };
    let build_union_cab = || {
        ExecutablePlan::<SimpleEntity>::new(AccessPlannedQuery {
            logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: Some(OrderSpec {
                    fields: vec![("id".to_string(), OrderDirection::Desc)],
                }),
                distinct: false,
                delete_limit: None,
                page: Some(PageSpec {
                    limit: Some(2),
                    offset: 0,
                }),
                consistency: MissingRowPolicy::Ignore,
            }),
            access: AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id6, id7, id8])),
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6])),
            ]),
        })
    };

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let (ids_abc, boundaries_abc) =
        collect_all_pages_from_executable_plan(&load, build_union_abc, 12);
    let (ids_cab, boundaries_cab) =
        collect_all_pages_from_executable_plan(&load, build_union_cab, 12);

    assert_eq!(
        ids_abc, ids_cab,
        "union child-plan order permutation must not change paged row sequence"
    );
    assert_eq!(
        boundaries_abc, boundaries_cab,
        "union child-plan order permutation must not change continuation boundaries"
    );
}

#[test]
fn load_intersection_child_order_permutation_preserves_rows_and_continuation_boundaries() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [
        38_001_u128,
        38_002,
        38_003,
        38_004,
        38_005,
        38_006,
        38_007,
        38_008,
    ] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("intersection permutation seed save should succeed");
    }

    let id1 = Ulid::from_u128(38_001);
    let id2 = Ulid::from_u128(38_002);
    let id3 = Ulid::from_u128(38_003);
    let id4 = Ulid::from_u128(38_004);
    let id5 = Ulid::from_u128(38_005);
    let id6 = Ulid::from_u128(38_006);
    let id7 = Ulid::from_u128(38_007);
    let id8 = Ulid::from_u128(38_008);

    let build_intersection_abc = || {
        ExecutablePlan::<SimpleEntity>::new(AccessPlannedQuery {
            logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: Some(OrderSpec {
                    fields: vec![("id".to_string(), OrderDirection::Desc)],
                }),
                distinct: false,
                delete_limit: None,
                page: Some(PageSpec {
                    limit: Some(1),
                    offset: 0,
                }),
                consistency: MissingRowPolicy::Ignore,
            }),
            access: AccessPlan::Intersection(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4, id5, id6])),
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7])),
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id4, id5, id6, id8])),
            ]),
        })
    };
    let build_intersection_bca = || {
        ExecutablePlan::<SimpleEntity>::new(AccessPlannedQuery {
            logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: Some(OrderSpec {
                    fields: vec![("id".to_string(), OrderDirection::Desc)],
                }),
                distinct: false,
                delete_limit: None,
                page: Some(PageSpec {
                    limit: Some(1),
                    offset: 0,
                }),
                consistency: MissingRowPolicy::Ignore,
            }),
            access: AccessPlan::Intersection(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7])),
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id4, id5, id6, id8])),
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4, id5, id6])),
            ]),
        })
    };

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let (ids_abc, boundaries_abc) =
        collect_all_pages_from_executable_plan(&load, build_intersection_abc, 12);
    let (ids_bca, boundaries_bca) =
        collect_all_pages_from_executable_plan(&load, build_intersection_bca, 12);

    assert_eq!(
        ids_abc, ids_bca,
        "intersection child-plan order permutation must not change paged row sequence"
    );
    assert_eq!(
        boundaries_abc, boundaries_bca,
        "intersection child-plan order permutation must not change continuation boundaries"
    );
}

#[test]
fn load_union_child_order_permutation_preserves_rows_and_boundaries_under_mixed_direction() {
    setup_pagination_test();

    let rows = [
        (40_001, 7, 30, "g7-r30-a"),
        (40_002, 7, 20, "g7-r20-a"),
        (40_003, 7, 20, "g7-r20-b"),
        (40_004, 7, 10, "g7-r10"),
        (40_005, 7, 30, "g7-r30-b"),
        (40_006, 7, 40, "g7-r40"),
        (40_007, 7, 20, "g7-r20-c"),
        (40_008, 8, 15, "g8-r15"),
    ];
    seed_pushdown_rows(&rows);

    let id1 = Ulid::from_u128(40_001);
    let id2 = Ulid::from_u128(40_002);
    let id3 = Ulid::from_u128(40_003);
    let id4 = Ulid::from_u128(40_004);
    let id5 = Ulid::from_u128(40_005);
    let id6 = Ulid::from_u128(40_006);
    let id7 = Ulid::from_u128(40_007);
    let id8 = Ulid::from_u128(40_008);

    let build_union_abc = || {
        ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
            logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: Some(OrderSpec {
                    fields: vec![
                        ("rank".to_string(), OrderDirection::Desc),
                        ("id".to_string(), OrderDirection::Asc),
                    ],
                }),
                distinct: false,
                delete_limit: None,
                page: Some(PageSpec {
                    limit: Some(2),
                    offset: 0,
                }),
                consistency: MissingRowPolicy::Ignore,
            }),
            access: AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id5, id6])),
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id7, id8])),
            ]),
        })
    };
    let build_union_cab = || {
        ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
            logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: Some(OrderSpec {
                    fields: vec![
                        ("rank".to_string(), OrderDirection::Desc),
                        ("id".to_string(), OrderDirection::Asc),
                    ],
                }),
                distinct: false,
                delete_limit: None,
                page: Some(PageSpec {
                    limit: Some(2),
                    offset: 0,
                }),
                consistency: MissingRowPolicy::Ignore,
            }),
            access: AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id7, id8])),
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4])),
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id5, id6])),
            ]),
        })
    };

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let (ids_abc, boundaries_abc) =
        collect_all_pages_from_executable_plan(&load, build_union_abc, 12);
    let (ids_cab, boundaries_cab) =
        collect_all_pages_from_executable_plan(&load, build_union_cab, 12);

    assert_eq!(
        ids_abc, ids_cab,
        "mixed-direction union child-plan permutation must not change paged row sequence"
    );
    assert_eq!(
        boundaries_abc, boundaries_cab,
        "mixed-direction union child-plan permutation must not change continuation boundaries"
    );
}

#[test]
fn load_intersection_child_order_permutation_preserves_rows_and_boundaries_under_mixed_direction() {
    setup_pagination_test();

    let rows = [
        (40_101, 7, 50, "g7-r50"),
        (40_102, 7, 40, "g7-r40"),
        (40_103, 7, 30, "g7-r30-a"),
        (40_104, 7, 30, "g7-r30-b"),
        (40_105, 7, 20, "g7-r20-a"),
        (40_106, 7, 20, "g7-r20-b"),
        (40_107, 7, 10, "g7-r10"),
        (40_108, 8, 5, "g8-r5"),
    ];
    seed_pushdown_rows(&rows);

    let id1 = Ulid::from_u128(40_101);
    let id2 = Ulid::from_u128(40_102);
    let id3 = Ulid::from_u128(40_103);
    let id4 = Ulid::from_u128(40_104);
    let id5 = Ulid::from_u128(40_105);
    let id6 = Ulid::from_u128(40_106);
    let id7 = Ulid::from_u128(40_107);
    let id8 = Ulid::from_u128(40_108);

    let build_intersection_abc = || {
        ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
            logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: Some(OrderSpec {
                    fields: vec![
                        ("rank".to_string(), OrderDirection::Asc),
                        ("id".to_string(), OrderDirection::Desc),
                    ],
                }),
                distinct: false,
                delete_limit: None,
                page: Some(PageSpec {
                    limit: Some(1),
                    offset: 0,
                }),
                consistency: MissingRowPolicy::Ignore,
            }),
            access: AccessPlan::Intersection(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4, id5, id6])),
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7])),
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id4, id5, id6, id8])),
            ]),
        })
    };
    let build_intersection_bca = || {
        ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
            logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: Some(OrderSpec {
                    fields: vec![
                        ("rank".to_string(), OrderDirection::Asc),
                        ("id".to_string(), OrderDirection::Desc),
                    ],
                }),
                distinct: false,
                delete_limit: None,
                page: Some(PageSpec {
                    limit: Some(1),
                    offset: 0,
                }),
                consistency: MissingRowPolicy::Ignore,
            }),
            access: AccessPlan::Intersection(vec![
                AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7])),
                AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id4, id5, id6, id8])),
                AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id3, id4, id5, id6])),
            ]),
        })
    };

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let (ids_abc, boundaries_abc) =
        collect_all_pages_from_executable_plan(&load, build_intersection_abc, 12);
    let (ids_bca, boundaries_bca) =
        collect_all_pages_from_executable_plan(&load, build_intersection_bca, 12);

    assert_eq!(
        ids_abc, ids_bca,
        "mixed-direction intersection child-plan permutation must not change paged row sequence"
    );
    assert_eq!(
        boundaries_abc, boundaries_bca,
        "mixed-direction intersection child-plan permutation must not change continuation boundaries"
    );
}

#[test]
fn load_union_child_order_permutation_matrix_preserves_rows_and_boundaries_under_mixed_direction() {
    setup_pagination_test();

    let rows = [
        (41_001, 7, 60, "g7-r60"),
        (41_002, 7, 50, "g7-r50-a"),
        (41_003, 7, 50, "g7-r50-b"),
        (41_004, 7, 40, "g7-r40"),
        (41_005, 7, 30, "g7-r30"),
        (41_006, 7, 20, "g7-r20-a"),
        (41_007, 7, 20, "g7-r20-b"),
        (41_008, 8, 70, "g8-r70"),
    ];
    seed_pushdown_rows(&rows);

    let id1 = Ulid::from_u128(41_001);
    let id2 = Ulid::from_u128(41_002);
    let id3 = Ulid::from_u128(41_003);
    let id4 = Ulid::from_u128(41_004);
    let id5 = Ulid::from_u128(41_005);
    let id6 = Ulid::from_u128(41_006);
    let id7 = Ulid::from_u128(41_007);
    let id8 = Ulid::from_u128(41_008);

    for (case_name, rank_direction, id_direction, limit) in [
        (
            "rank_desc_id_asc_limit1",
            OrderDirection::Desc,
            OrderDirection::Asc,
            1,
        ),
        (
            "rank_desc_id_asc_limit3",
            OrderDirection::Desc,
            OrderDirection::Asc,
            3,
        ),
        (
            "rank_asc_id_desc_limit2",
            OrderDirection::Asc,
            OrderDirection::Desc,
            2,
        ),
    ] {
        let build_union_abc = || {
            ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
                logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                    mode: QueryMode::Load(LoadSpec::new()),
                    predicate: None,
                    order: Some(OrderSpec {
                        fields: vec![
                            ("rank".to_string(), rank_direction),
                            ("id".to_string(), id_direction),
                        ],
                    }),
                    distinct: false,
                    delete_limit: None,
                    page: Some(PageSpec {
                        limit: Some(limit),
                        offset: 0,
                    }),
                    consistency: MissingRowPolicy::Ignore,
                }),
                access: AccessPlan::Union(vec![
                    AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id4, id6])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id3, id5, id6, id7])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id8])),
                ]),
            })
        };
        let build_union_cab = || {
            ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
                logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                    mode: QueryMode::Load(LoadSpec::new()),
                    predicate: None,
                    order: Some(OrderSpec {
                        fields: vec![
                            ("rank".to_string(), rank_direction),
                            ("id".to_string(), id_direction),
                        ],
                    }),
                    distinct: false,
                    delete_limit: None,
                    page: Some(PageSpec {
                        limit: Some(limit),
                        offset: 0,
                    }),
                    consistency: MissingRowPolicy::Ignore,
                }),
                access: AccessPlan::Union(vec![
                    AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id8])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id1, id2, id4, id6])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id3, id5, id6, id7])),
                ]),
            })
        };

        let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
        let (ids_abc, boundaries_abc) =
            collect_all_pages_from_executable_plan(&load, build_union_abc, 20);
        let (ids_cab, boundaries_cab) =
            collect_all_pages_from_executable_plan(&load, build_union_cab, 20);

        assert_eq!(
            ids_abc, ids_cab,
            "{case_name}: mixed-direction union child-plan permutation must not change paged row sequence"
        );
        assert_eq!(
            boundaries_abc, boundaries_cab,
            "{case_name}: mixed-direction union child-plan permutation must not change continuation boundaries"
        );
    }
}

#[test]
fn load_intersection_child_order_permutation_matrix_preserves_rows_and_boundaries_under_mixed_direction()
 {
    setup_pagination_test();

    let rows = [
        (41_101, 7, 70, "g7-r70"),
        (41_102, 7, 60, "g7-r60"),
        (41_103, 7, 50, "g7-r50-a"),
        (41_104, 7, 50, "g7-r50-b"),
        (41_105, 7, 40, "g7-r40-a"),
        (41_106, 7, 40, "g7-r40-b"),
        (41_107, 7, 30, "g7-r30"),
        (41_108, 7, 20, "g7-r20"),
        (41_109, 8, 10, "g8-r10"),
        (41_110, 8, 5, "g8-r5"),
    ];
    seed_pushdown_rows(&rows);

    let id1 = Ulid::from_u128(41_101);
    let id2 = Ulid::from_u128(41_102);
    let id3 = Ulid::from_u128(41_103);
    let id4 = Ulid::from_u128(41_104);
    let id5 = Ulid::from_u128(41_105);
    let id6 = Ulid::from_u128(41_106);
    let id7 = Ulid::from_u128(41_107);
    let id8 = Ulid::from_u128(41_108);
    let id9 = Ulid::from_u128(41_109);
    let id10 = Ulid::from_u128(41_110);

    for (case_name, rank_direction, id_direction, limit) in [
        (
            "rank_desc_id_asc_limit1",
            OrderDirection::Desc,
            OrderDirection::Asc,
            1,
        ),
        (
            "rank_asc_id_desc_limit2",
            OrderDirection::Asc,
            OrderDirection::Desc,
            2,
        ),
        (
            "rank_asc_id_desc_limit3",
            OrderDirection::Asc,
            OrderDirection::Desc,
            3,
        ),
    ] {
        let build_intersection_abc = || {
            ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
                logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                    mode: QueryMode::Load(LoadSpec::new()),
                    predicate: None,
                    order: Some(OrderSpec {
                        fields: vec![
                            ("rank".to_string(), rank_direction),
                            ("id".to_string(), id_direction),
                        ],
                    }),
                    distinct: false,
                    delete_limit: None,
                    page: Some(PageSpec {
                        limit: Some(limit),
                        offset: 0,
                    }),
                    consistency: MissingRowPolicy::Ignore,
                }),
                access: AccessPlan::Intersection(vec![
                    AccessPlan::path(AccessPath::ByKeys(vec![
                        id1, id2, id3, id4, id5, id6, id7, id8,
                    ])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7, id9])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id4, id5, id6, id7, id10])),
                ]),
            })
        };
        let build_intersection_bca = || {
            ExecutablePlan::<PushdownParityEntity>::new(AccessPlannedQuery {
                logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                    mode: QueryMode::Load(LoadSpec::new()),
                    predicate: None,
                    order: Some(OrderSpec {
                        fields: vec![
                            ("rank".to_string(), rank_direction),
                            ("id".to_string(), id_direction),
                        ],
                    }),
                    distinct: false,
                    delete_limit: None,
                    page: Some(PageSpec {
                        limit: Some(limit),
                        offset: 0,
                    }),
                    consistency: MissingRowPolicy::Ignore,
                }),
                access: AccessPlan::Intersection(vec![
                    AccessPlan::path(AccessPath::ByKeys(vec![id3, id4, id5, id6, id7, id9])),
                    AccessPlan::path(AccessPath::ByKeys(vec![id2, id3, id4, id5, id6, id7, id10])),
                    AccessPlan::path(AccessPath::ByKeys(vec![
                        id1, id2, id3, id4, id5, id6, id7, id8,
                    ])),
                ]),
            })
        };

        let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
        let (ids_abc, boundaries_abc) =
            collect_all_pages_from_executable_plan(&load, build_intersection_abc, 20);
        let (ids_bca, boundaries_bca) =
            collect_all_pages_from_executable_plan(&load, build_intersection_bca, 20);

        assert_eq!(
            ids_abc, ids_bca,
            "{case_name}: mixed-direction intersection child-plan permutation must not change paged row sequence"
        );
        assert_eq!(
            boundaries_abc, boundaries_bca,
            "{case_name}: mixed-direction intersection child-plan permutation must not change continuation boundaries"
        );
    }
}
