use super::*;

#[test]
fn load_applies_order_and_pagination() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [3_u128, 1_u128, 2_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .offset(1)
        .plan()
        .expect("load plan should build");

    let response = load.execute(plan).expect("load should succeed");
    assert_eq!(response.0.len(), 1, "pagination should return one row");
    assert_eq!(
        response.0[0].1.id,
        Ulid::from_u128(2),
        "pagination should run after canonical ordering by id"
    );
}

#[test]
fn load_offset_pagination_preserves_next_cursor_boundary() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [5_u128, 1_u128, 4_u128, 2_u128, 3_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("offset page plan should build");
    let page_boundary = page_plan
        .plan_cursor(None)
        .expect("offset page boundary should plan");
    let page = load
        .execute_paged_with_cursor(page_plan, page_boundary)
        .expect("offset page should execute");

    let page_ids: Vec<Ulid> = ids_from_items(&page.items.0);
    assert_eq!(
        page_ids,
        vec![Ulid::from_u128(2), Ulid::from_u128(3)],
        "offset pagination should return canonical ordered window"
    );

    let cursor_bytes = page
        .next_cursor
        .as_ref()
        .expect("offset page should emit continuation cursor");
    let token = cursor_bytes.clone();
    let comparison_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("comparison plan should build")
        .into_inner();
    let expected_boundary = comparison_plan
        .cursor_boundary_from_entity(&page.items.0[1].1)
        .expect("expected boundary should build");
    assert_eq!(
        token.boundary(),
        &expected_boundary,
        "next cursor must encode the last returned row for offset pages"
    );
}

#[test]
fn load_cursor_with_offset_applies_offset_once_across_pages() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [6_u128, 1_u128, 5_u128, 2_u128, 4_u128, 3_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    // Phase 1: first page consumes offset before applying limit.
    let page1_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("offset page1 plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("offset page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("offset page1 should execute");
    assert_eq!(
        ids_from_items(&page1.items.0),
        vec![Ulid::from_u128(2), Ulid::from_u128(3)],
        "first page should apply offset once"
    );

    // Phase 2: continuation resumes from cursor boundary without re-applying offset.
    let cursor = page1
        .next_cursor
        .expect("first page should emit continuation cursor");
    let page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("offset page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(
            cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("offset page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("offset page2 should execute");
    assert_eq!(
        ids_from_items(&page2.items.0),
        vec![Ulid::from_u128(4), Ulid::from_u128(5)],
        "continuation page should not re-apply offset"
    );
}

#[test]
fn load_cursor_with_offset_desc_secondary_pushdown_resume_matrix_is_boundary_complete() {
    setup_pagination_test();

    let rows = pushdown_rows_with_group9(42_001);
    seed_pushdown_rows(&rows);
    let predicate = pushdown_group_predicate(7);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, true);
    for (case_name, descending) in [("asc", false), ("desc", true)] {
        let build_plan = || {
            let base = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
                .filter(predicate.clone())
                .limit(2)
                .offset(1);
            let ordered = if descending {
                base.order_by_desc("rank").order_by_desc("id")
            } else {
                base.order_by("rank").order_by("id")
            };

            ordered
                .plan()
                .expect("secondary offset continuation plan should build")
        };

        let (_seed_page, seed_trace) = load
            .execute_paged_with_cursor_traced(build_plan(), None)
            .expect("secondary offset seed page should execute");
        let seed_trace = seed_trace.expect("debug trace should be present");
        assert_eq!(
            seed_trace.optimization,
            Some(ExecutionOptimization::SecondaryOrderPushdown),
            "secondary offset shape should use pushdown for case={case_name}",
        );

        let mut expected_ids = ordered_ids_from_group_rank_index(7);
        if descending {
            expected_ids.reverse();
        }
        let expected_ids = expected_ids.into_iter().skip(1).collect::<Vec<_>>();

        let (ids, _boundaries, tokens) =
            collect_all_pages_from_executable_plan_with_tokens(&load, build_plan, 20);
        assert_eq!(
            ids, expected_ids,
            "secondary offset traversal must preserve canonical order for case={case_name}",
        );
        assert_resume_suffixes_from_tokens(
            &load,
            &build_plan,
            &tokens,
            &expected_ids,
            20,
            case_name,
        );
    }
}

#[test]
fn load_cursor_with_offset_index_range_pushdown_resume_matrix_is_boundary_complete() {
    setup_pagination_test();

    let rows = [
        (42_101, 10, "t10-a"),
        (42_102, 10, "t10-b"),
        (42_103, 20, "t20-a"),
        (42_104, 20, "t20-b"),
        (42_105, 25, "t25"),
        (42_106, 28, "t28-a"),
        (42_107, 28, "t28-b"),
        (42_108, 40, "t40"),
    ];
    seed_indexed_metrics_rows(&rows);

    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, true);
    for (case_name, direction) in [("asc", OrderDirection::Asc), ("desc", OrderDirection::Desc)] {
        let build_plan = || {
            ExecutablePlan::<IndexedMetricsEntity>::new(AccessPlannedQuery {
                logical: LogicalPlan {
                    mode: QueryMode::Load(LoadSpec::new()),
                    predicate: None,
                    order: Some(OrderSpec {
                        fields: vec![
                            ("tag".to_string(), direction),
                            ("id".to_string(), direction),
                        ],
                    }),
                    distinct: false,
                    delete_limit: None,
                    page: Some(PageSpec {
                        limit: Some(2),
                        offset: 1,
                    }),
                    consistency: ReadConsistency::MissingOk,
                },
                access: AccessPlan::path(AccessPath::index_range(
                    INDEXED_METRICS_INDEX_MODELS[0],
                    Vec::new(),
                    Bound::Included(Value::Uint(10)),
                    Bound::Excluded(Value::Uint(30)),
                )),
            })
        };

        let (_seed_page, seed_trace) = load
            .execute_paged_with_cursor_traced(build_plan(), None)
            .expect("index-range offset seed page should execute");
        let seed_trace = seed_trace.expect("debug trace should be present");
        assert_eq!(
            seed_trace.optimization,
            Some(ExecutionOptimization::IndexRangeLimitPushdown),
            "index-range offset shape should use limit pushdown for case={case_name}",
        );

        let mut expected_rows = rows
            .iter()
            .filter(|(_, tag, _)| *tag >= 10 && *tag < 30)
            .map(|(id, tag, _)| (*tag, Ulid::from_u128(*id)))
            .collect::<Vec<_>>();
        expected_rows.sort_by(
            |(left_tag, left_id), (right_tag, right_id)| match direction {
                OrderDirection::Asc => left_tag.cmp(right_tag).then_with(|| left_id.cmp(right_id)),
                OrderDirection::Desc => right_tag.cmp(left_tag).then_with(|| right_id.cmp(left_id)),
            },
        );
        let expected_ids = expected_rows
            .iter()
            .map(|(_, id)| *id)
            .skip(1)
            .collect::<Vec<_>>();

        let (ids, _boundaries, tokens) =
            collect_all_pages_from_executable_plan_with_tokens(&load, build_plan, 20);
        assert_eq!(
            ids, expected_ids,
            "index-range offset traversal must preserve canonical order for case={case_name}",
        );
        assert_resume_suffixes_from_tokens(
            &load,
            &build_plan,
            &tokens,
            &expected_ids,
            20,
            case_name,
        );
    }
}

#[test]
fn load_cursor_with_offset_fallback_resume_matrix_is_boundary_complete() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [42_201_u128, 42_202, 42_203, 42_204, 42_205, 42_206, 42_207] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("fallback offset seed save should succeed");
    }

    let fallback_ids = vec![
        Ulid::from_u128(42_204),
        Ulid::from_u128(42_201),
        Ulid::from_u128(42_207),
        Ulid::from_u128(42_202),
        Ulid::from_u128(42_206),
        Ulid::from_u128(42_203),
        Ulid::from_u128(42_205),
    ];
    let load = LoadExecutor::<SimpleEntity>::new(DB, true);

    for (case_name, descending) in [("asc", false), ("desc", true)] {
        let build_plan = || {
            let base = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
                .by_ids(fallback_ids.iter().copied())
                .limit(2)
                .offset(1);
            let ordered = if descending {
                base.order_by_desc("id")
            } else {
                base.order_by("id")
            };
            ordered
                .plan()
                .expect("fallback offset continuation plan should build")
        };

        let (_seed_page, seed_trace) = load
            .execute_paged_with_cursor_traced(build_plan(), None)
            .expect("fallback offset seed page should execute");
        let seed_trace = seed_trace.expect("debug trace should be present");
        assert_eq!(
            seed_trace.optimization, None,
            "fallback by-ids offset shape should remain non-optimized for case={case_name}",
        );

        let mut expected_ids = fallback_ids.clone();
        expected_ids.sort();
        if descending {
            expected_ids.reverse();
        }
        let expected_ids = expected_ids.into_iter().skip(1).collect::<Vec<_>>();

        let (ids, _boundaries, tokens) =
            collect_all_pages_from_executable_plan_with_tokens(&load, build_plan, 20);
        assert_eq!(
            ids, expected_ids,
            "fallback offset traversal must preserve canonical order for case={case_name}",
        );
        assert_resume_suffixes_from_tokens(
            &load,
            &build_plan,
            &tokens,
            &expected_ids,
            20,
            case_name,
        );
    }
}

#[test]
fn load_cursor_pagination_pk_order_round_trips_across_pages() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [4_u128, 1_u128, 3_u128, 2_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page1_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .plan()
        .expect("pk-order page1 plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("pk-order page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("pk-order page1 should execute");
    let page1_ids: Vec<Ulid> = ids_from_items(&page1.items.0);
    assert_eq!(page1_ids, vec![Ulid::from_u128(1), Ulid::from_u128(2)]);

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("pk-order page1 should emit continuation cursor");
    let page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .plan()
        .expect("pk-order page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(
            cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("pk-order page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("pk-order page2 should execute");
    let page2_ids: Vec<Ulid> = ids_from_items(&page2.items.0);
    assert_eq!(page2_ids, vec![Ulid::from_u128(3), Ulid::from_u128(4)]);
    assert!(
        page2.next_cursor.is_none(),
        "final pk-order page should not emit continuation cursor"
    );
}

#[test]
fn load_cursor_pagination_pk_fast_path_matches_non_fast_post_access_semantics() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let keys = [5_u128, 1_u128, 4_u128, 2_u128, 3_u128];
    for id in keys {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    // Path A: full scan + PK ASC is fast-path eligible.
    let fast_page1_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("fast page1 plan should build");
    let fast_page1_boundary = fast_page1_plan
        .plan_cursor(None)
        .expect("fast page1 boundary should plan");
    let fast_page1 = load
        .execute_paged_with_cursor(fast_page1_plan, fast_page1_boundary)
        .expect("fast page1 should execute");

    // Path B: key-batch access forces non-fast path, but post-access semantics are identical.
    let non_fast_page1_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .by_ids(keys.into_iter().map(Ulid::from_u128))
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("non-fast page1 plan should build");
    let non_fast_page1_boundary = non_fast_page1_plan
        .plan_cursor(None)
        .expect("non-fast page1 boundary should plan");
    let non_fast_page1 = load
        .execute_paged_with_cursor(non_fast_page1_plan, non_fast_page1_boundary)
        .expect("non-fast page1 should execute");

    let fast_page1_ids: Vec<Ulid> = ids_from_items(&fast_page1.items.0);
    let non_fast_page1_ids: Vec<Ulid> = ids_from_items(&non_fast_page1.items.0);
    assert_eq!(
        fast_page1_ids, non_fast_page1_ids,
        "page1 rows should match between fast and non-fast access paths"
    );
    assert_eq!(
        fast_page1.next_cursor.is_some(),
        non_fast_page1.next_cursor.is_some(),
        "page1 cursor presence should match between paths"
    );

    let fast_cursor_page1 = fast_page1
        .next_cursor
        .as_ref()
        .expect("fast page1 should emit continuation cursor");
    let non_fast_cursor_page1 = non_fast_page1
        .next_cursor
        .as_ref()
        .expect("non-fast page1 should emit continuation cursor");
    let fast_cursor_page1_boundary = fast_cursor_page1.boundary().clone();
    let non_fast_cursor_page1_boundary = non_fast_cursor_page1.boundary().clone();
    assert_eq!(
        &fast_cursor_page1_boundary, &non_fast_cursor_page1_boundary,
        "cursor boundaries should match even when signatures differ by access path"
    );

    let fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("fast page2 plan should build");
    let fast_page2_boundary = fast_page2_plan
        .plan_cursor(Some(
            fast_cursor_page1
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("fast page2 boundary should plan");
    let fast_page2 = load
        .execute_paged_with_cursor(fast_page2_plan, fast_page2_boundary)
        .expect("fast page2 should execute");

    let non_fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .by_ids(keys.into_iter().map(Ulid::from_u128))
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("non-fast page2 plan should build");
    let non_fast_page2_boundary = non_fast_page2_plan
        .plan_cursor(Some(
            non_fast_cursor_page1
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("non-fast page2 boundary should plan");
    let non_fast_page2 = load
        .execute_paged_with_cursor(non_fast_page2_plan, non_fast_page2_boundary)
        .expect("non-fast page2 should execute");

    let fast_page2_ids: Vec<Ulid> = ids_from_items(&fast_page2.items.0);
    let non_fast_page2_ids: Vec<Ulid> = ids_from_items(&non_fast_page2.items.0);
    assert_eq!(
        fast_page2_ids, non_fast_page2_ids,
        "page2 rows should match between fast and non-fast access paths"
    );
    assert_eq!(
        fast_page2.next_cursor.is_some(),
        non_fast_page2.next_cursor.is_some(),
        "page2 cursor presence should match between paths"
    );
}

#[test]
fn load_cursor_pagination_pk_fast_path_desc_matches_non_fast_post_access_semantics() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let keys = [5_u128, 1_u128, 4_u128, 2_u128, 3_u128];
    for id in keys {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    // Path A: full scan + PK DESC should use the PK stream fast path.
    let fast_page1_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("fast descending page1 plan should build");
    let fast_page1_boundary = fast_page1_plan
        .plan_cursor(None)
        .expect("fast descending page1 boundary should plan");
    let fast_page1 = load
        .execute_paged_with_cursor(fast_page1_plan, fast_page1_boundary)
        .expect("fast descending page1 should execute");

    // Path B: key-batch access forces non-fast path, but post-access semantics are identical.
    let non_fast_page1_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .by_ids(keys.into_iter().map(Ulid::from_u128))
        .order_by_desc("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("non-fast descending page1 plan should build");
    let non_fast_page1_boundary = non_fast_page1_plan
        .plan_cursor(None)
        .expect("non-fast descending page1 boundary should plan");
    let non_fast_page1 = load
        .execute_paged_with_cursor(non_fast_page1_plan, non_fast_page1_boundary)
        .expect("non-fast descending page1 should execute");

    let fast_page1_ids: Vec<Ulid> = ids_from_items(&fast_page1.items.0);
    let non_fast_page1_ids: Vec<Ulid> = ids_from_items(&non_fast_page1.items.0);
    assert_eq!(
        fast_page1_ids, non_fast_page1_ids,
        "descending page1 rows should match between fast and non-fast access paths"
    );
    assert_eq!(
        fast_page1.next_cursor.is_some(),
        non_fast_page1.next_cursor.is_some(),
        "descending page1 cursor presence should match between paths"
    );

    let fast_cursor_page1 = fast_page1
        .next_cursor
        .as_ref()
        .expect("fast descending page1 should emit continuation cursor");
    let non_fast_cursor_page1 = non_fast_page1
        .next_cursor
        .as_ref()
        .expect("non-fast descending page1 should emit continuation cursor");
    let fast_cursor_page1_boundary = fast_cursor_page1.boundary().clone();
    let non_fast_cursor_page1_boundary = non_fast_cursor_page1.boundary().clone();
    assert_eq!(
        &fast_cursor_page1_boundary, &non_fast_cursor_page1_boundary,
        "descending cursor boundaries should match even when signatures differ by access path"
    );

    let fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("fast descending page2 plan should build");
    let fast_page2_boundary = fast_page2_plan
        .plan_cursor(Some(
            fast_cursor_page1
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("fast descending page2 boundary should plan");
    let fast_page2 = load
        .execute_paged_with_cursor(fast_page2_plan, fast_page2_boundary)
        .expect("fast descending page2 should execute");

    let non_fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .by_ids(keys.into_iter().map(Ulid::from_u128))
        .order_by_desc("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("non-fast descending page2 plan should build");
    let non_fast_page2_boundary = non_fast_page2_plan
        .plan_cursor(Some(
            non_fast_cursor_page1
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("non-fast descending page2 boundary should plan");
    let non_fast_page2 = load
        .execute_paged_with_cursor(non_fast_page2_plan, non_fast_page2_boundary)
        .expect("non-fast descending page2 should execute");

    let fast_page2_ids: Vec<Ulid> = ids_from_items(&fast_page2.items.0);
    let non_fast_page2_ids: Vec<Ulid> = ids_from_items(&non_fast_page2.items.0);
    assert_eq!(
        fast_page2_ids, non_fast_page2_ids,
        "descending page2 rows should match between fast and non-fast access paths"
    );
    assert_eq!(
        fast_page2.next_cursor.is_some(),
        non_fast_page2.next_cursor.is_some(),
        "descending page2 cursor presence should match between paths"
    );
}

#[test]
fn load_cursor_pagination_pk_fast_path_matches_non_fast_with_same_cursor_boundary() {
    setup_pagination_test();

    // Phase 1: seed rows with non-sorted insertion order.
    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let keys = [7_u128, 1_u128, 6_u128, 2_u128, 5_u128, 3_u128, 4_u128];
    for id in keys {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    // Phase 2: capture one canonical cursor boundary from an initial fast-path page.
    let page1_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(3)
        .plan()
        .expect("cursor source plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("cursor source boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("cursor source page should execute");
    let cursor_bytes = page1
        .next_cursor
        .as_ref()
        .expect("cursor source page should emit continuation cursor");
    let shared_boundary = cursor_bytes.boundary().clone();

    // Phase 3: execute page-2 parity checks with the same typed cursor boundary.
    let fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .plan()
        .expect("fast page2 plan should build");
    let fast_page2 = load
        .execute_paged_with_cursor(fast_page2_plan, Some(shared_boundary.clone()))
        .expect("fast page2 should execute");

    let non_fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .by_ids(keys.into_iter().map(Ulid::from_u128))
        .order_by("id")
        .limit(2)
        .plan()
        .expect("non-fast page2 plan should build");
    let non_fast_page2 = load
        .execute_paged_with_cursor(non_fast_page2_plan, Some(shared_boundary))
        .expect("non-fast page2 should execute");

    let fast_ids: Vec<Ulid> = ids_from_items(&fast_page2.items.0);
    let non_fast_ids: Vec<Ulid> = ids_from_items(&non_fast_page2.items.0);
    assert_eq!(
        fast_ids, non_fast_ids,
        "fast and non-fast paths must return identical rows for the same cursor boundary"
    );

    assert_eq!(
        fast_page2.next_cursor.is_some(),
        non_fast_page2.next_cursor.is_some(),
        "cursor presence must match between fast and non-fast paths"
    );

    let fast_next = fast_page2
        .next_cursor
        .as_ref()
        .expect("fast page2 should emit continuation cursor");
    let non_fast_next = non_fast_page2
        .next_cursor
        .as_ref()
        .expect("non-fast page2 should emit continuation cursor");
    let fast_next_boundary = fast_next.boundary().clone();
    let non_fast_next_boundary = non_fast_next.boundary().clone();
    assert_eq!(
        &fast_next_boundary, &non_fast_next_boundary,
        "fast and non-fast paths must emit the same continuation boundary"
    );
}

#[test]
fn load_cursor_pagination_pk_order_key_range_respects_bounds() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1_u128, 2_u128, 3_u128, 4_u128, 5_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let mut page1_logical = AccessPlannedQuery::<Ulid>::new(
        AccessPath::KeyRange {
            start: Ulid::from_u128(2),
            end: Ulid::from_u128(4),
        },
        ReadConsistency::MissingOk,
    );
    page1_logical.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    page1_logical.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page1_plan = ExecutablePlan::<SimpleEntity>::new(page1_logical);
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("pk-range page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("pk-range page1 should execute");
    let page1_ids: Vec<Ulid> = ids_from_items(&page1.items.0);
    assert_eq!(page1_ids, vec![Ulid::from_u128(2), Ulid::from_u128(3)]);

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("pk-range page1 should emit continuation cursor");
    let mut page2_logical = AccessPlannedQuery::<Ulid>::new(
        AccessPath::KeyRange {
            start: Ulid::from_u128(2),
            end: Ulid::from_u128(4),
        },
        ReadConsistency::MissingOk,
    );
    page2_logical.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    page2_logical.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let page2_plan = ExecutablePlan::<SimpleEntity>::new(page2_logical);
    let page2_boundary = page2_plan
        .plan_cursor(Some(
            cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("pk-range page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("pk-range page2 should execute");
    let page2_ids: Vec<Ulid> = ids_from_items(&page2.items.0);
    assert_eq!(page2_ids, vec![Ulid::from_u128(4)]);
    assert!(
        page2.next_cursor.is_none(),
        "final pk-range page should not emit continuation cursor"
    );
}

#[test]
fn load_cursor_pagination_pk_order_key_range_cursor_past_end_returns_empty_page() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1_u128, 2_u128, 3_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let mut logical = AccessPlannedQuery::<Ulid>::new(
        AccessPath::KeyRange {
            start: Ulid::from_u128(1),
            end: Ulid::from_u128(2),
        },
        ReadConsistency::MissingOk,
    );
    logical.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    logical.page = Some(PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let plan = ExecutablePlan::<SimpleEntity>::new(logical);
    let boundary = Some(CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(
            99,
        )))],
    });

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page = load
        .execute_paged_with_cursor(plan, boundary)
        .expect("pk-range cursor past end should execute");

    assert_exhausted_continuation_page!(
        page,
        "cursor beyond range end should produce an empty page",
        "empty page should not emit a continuation cursor",
    );
}

#[test]
fn load_cursor_pagination_pk_order_inverted_key_range_returns_empty_without_scan() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1_u128, 2_u128, 3_u128, 4_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    for (case_name, direction) in [("asc", OrderDirection::Asc), ("desc", OrderDirection::Desc)] {
        let mut logical = AccessPlannedQuery::<Ulid>::new(
            AccessPath::KeyRange {
                start: Ulid::from_u128(4),
                end: Ulid::from_u128(2),
            },
            ReadConsistency::MissingOk,
        );
        logical.order = Some(OrderSpec {
            fields: vec![("id".to_string(), direction)],
        });
        logical.page = Some(PageSpec {
            limit: Some(2),
            offset: 0,
        });
        let plan = ExecutablePlan::<SimpleEntity>::new(logical);

        let (page, trace) = load
            .execute_paged_with_cursor_traced(plan, None)
            .expect("inverted pk-range execution should succeed");
        let trace = trace.expect("debug trace should be present");

        assert_exhausted_continuation_page!(
            page,
            format!("inverted pk-range should return an empty page for case={case_name}"),
            format!("inverted pk-range should not emit a continuation cursor for case={case_name}"),
        );
        assert_eq!(
            trace.optimization,
            Some(ExecutionOptimization::PrimaryKey),
            "inverted pk-range should remain on PK fast path for case={case_name}",
        );
        assert_eq!(
            trace.keys_scanned, 0,
            "inverted pk-range should not scan any keys for case={case_name}",
        );
    }
}

#[test]
fn load_cursor_pagination_pk_fast_path_scan_accounting_tracks_access_candidates() {
    setup_pagination_test();

    let seeded_count = 6_u64;
    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [6_u128, 1_u128, 5_u128, 2_u128, 4_u128, 3_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, true);
    for (case_name, descending) in [("asc", false), ("desc", true)] {
        let base = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
            .limit(2)
            .offset(1);
        let plan = if descending {
            base.order_by_desc("id")
        } else {
            base.order_by("id")
        }
        .plan()
        .expect("pk fast-path budget plan should build");

        let (_page, trace) = load
            .execute_paged_with_cursor_traced(plan, None)
            .expect("pk fast-path budget execution should succeed");
        let trace = trace.expect("debug trace should be present");

        assert_eq!(
            trace.optimization,
            Some(ExecutionOptimization::PrimaryKey),
            "pk trace should report PK fast path for case={case_name}",
        );
        // PK fast-path trace accounting reports access-phase candidate count.
        assert_eq!(
            trace.keys_scanned, seeded_count,
            "pk fast-path trace should count all access candidates for case={case_name}",
        );
    }
}

#[test]
fn load_cursor_pagination_pk_order_missing_slot_is_invariant_violation() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1_u128, 2_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .plan()
        .expect("pk-order plan should build");

    let err = load
        .execute_paged_with_cursor(
            plan,
            Some(CursorBoundary {
                slots: vec![CursorBoundarySlot::Missing],
            }),
        )
        .expect_err("missing pk slot should be rejected by executor invariants");
    assert_eq!(
        err.class,
        ErrorClass::InvariantViolation,
        "missing pk slot should classify as invariant violation"
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Query,
        "missing pk slot should originate from query invariant checks"
    );
    assert!(
        err.message.contains("pk cursor slot must be present"),
        "missing pk slot should return a clear invariant message: {err:?}"
    );
}

#[test]
fn load_cursor_pagination_pk_order_type_mismatch_is_invariant_violation() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1_u128, 2_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .plan()
        .expect("pk-order plan should build");

    let err = load
        .execute_paged_with_cursor(
            plan,
            Some(CursorBoundary {
                slots: vec![CursorBoundarySlot::Present(Value::Text(
                    "not-a-ulid".to_string(),
                ))],
            }),
        )
        .expect_err("pk slot type mismatch should be rejected by executor invariants");
    assert_eq!(
        err.class,
        ErrorClass::InvariantViolation,
        "pk slot mismatch should classify as invariant violation"
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Query,
        "pk slot mismatch should originate from query invariant checks"
    );
    assert!(
        err.message.contains("pk cursor slot type mismatch"),
        "pk slot mismatch should return a clear invariant message: {err:?}"
    );
}

#[test]
fn load_cursor_pagination_pk_order_arity_mismatch_is_invariant_violation() {
    setup_pagination_test();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1_u128, 2_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(1)
        .plan()
        .expect("pk-order plan should build");

    let err = load
        .execute_paged_with_cursor(
            plan,
            Some(CursorBoundary {
                slots: vec![
                    CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(1))),
                    CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(2))),
                ],
            }),
        )
        .expect_err("pk slot arity mismatch should be rejected by executor invariants");
    assert_eq!(
        err.class,
        ErrorClass::InvariantViolation,
        "pk slot arity mismatch should classify as invariant violation"
    );
    assert_eq!(
        err.origin,
        ErrorOrigin::Query,
        "pk slot arity mismatch should originate from query invariant checks"
    );
    assert!(
        err.message
            .contains("pk-ordered continuation boundary must contain exactly 1 slot"),
        "pk slot arity mismatch should return a clear invariant message: {err:?}"
    );
}

#[test]
fn load_cursor_pagination_skips_strictly_before_limit() {
    setup_pagination_test();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(1100),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1],
            label: "r10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1101),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1102),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![3],
            label: "r20-b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1103),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![4],
            label: "r30".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let page1_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("cursor page1 plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("cursor page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("cursor page1 should execute");
    assert_eq!(page1.items.0.len(), 1, "page1 should return one row");
    assert_eq!(page1.items.0[0].1.id, Ulid::from_u128(1100));

    let cursor1 = page1
        .next_cursor
        .as_ref()
        .expect("page1 should emit a continuation cursor");
    let page2_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("cursor page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(
            cursor1
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("cursor page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("cursor page2 should execute");
    assert_eq!(page2.items.0.len(), 1, "page2 should return one row");
    assert_eq!(
        page2.items.0[0].1.id,
        Ulid::from_u128(1101),
        "cursor boundary must be applied before limit using strict ordering"
    );

    let cursor2 = page2
        .next_cursor
        .as_ref()
        .expect("page2 should emit a continuation cursor");
    let page3_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("cursor page3 plan should build");
    let page3_boundary = page3_plan
        .plan_cursor(Some(
            cursor2
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("cursor page3 boundary should plan");
    let page3 = load
        .execute_paged_with_cursor(page3_plan, page3_boundary)
        .expect("cursor page3 should execute");
    assert_eq!(page3.items.0.len(), 1, "page3 should return one row");
    assert_eq!(
        page3.items.0[0].1.id,
        Ulid::from_u128(1102),
        "strict cursor continuation must advance beyond the last returned row"
    );
}

#[test]
fn load_cursor_next_cursor_uses_last_returned_row_boundary() {
    setup_pagination_test();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(1200),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1],
            label: "r10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1201),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1202),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![3],
            label: "r20-b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1203),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![4],
            label: "r30".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let page1_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("cursor next-cursor plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("cursor page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("cursor page1 should execute");
    assert_eq!(page1.items.0.len(), 2, "page1 should return two rows");
    assert_eq!(page1.items.0[0].1.id, Ulid::from_u128(1200));
    assert_eq!(
        page1.items.0[1].1.id,
        Ulid::from_u128(1201),
        "page1 second row should be the PK tie-break winner for rank=20"
    );

    let cursor_bytes = page1
        .next_cursor
        .as_ref()
        .expect("page1 should include next cursor");
    let token = cursor_bytes.clone();
    let comparison_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("comparison plan should build")
        .into_inner();
    let expected_boundary = comparison_plan
        .cursor_boundary_from_entity(&page1.items.0[1].1)
        .expect("expected boundary should build");
    assert_eq!(
        token.boundary(),
        &expected_boundary,
        "next cursor must encode the last returned row boundary"
    );

    let page2_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("cursor page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(
            cursor_bytes
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("cursor page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("cursor page2 should execute");
    let page2_ids: Vec<Ulid> = ids_from_items(&page2.items.0);
    assert_eq!(
        page2_ids,
        vec![Ulid::from_u128(1202), Ulid::from_u128(1203)],
        "page2 should resume strictly after page1's final row"
    );
    assert!(
        page2.next_cursor.is_none(),
        "final page should not emit a continuation cursor"
    );
}

#[test]
fn load_cursor_pagination_desc_order_resumes_strictly_after_boundary() {
    setup_pagination_test();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(1400),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1],
            label: "r10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1401),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1402),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![3],
            label: "r20-b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1403),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![4],
            label: "r30".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let page1_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("descending page1 plan should build");
    let page1_boundary = page1_plan
        .plan_cursor(None)
        .expect("descending page1 boundary should plan");
    let page1 = load
        .execute_paged_with_cursor(page1_plan, page1_boundary)
        .expect("descending page1 should execute");
    let page1_ids: Vec<Ulid> = ids_from_items(&page1.items.0);
    assert_eq!(
        page1_ids,
        vec![Ulid::from_u128(1403), Ulid::from_u128(1401)],
        "descending page1 should apply rank DESC then canonical PK tie-break"
    );

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("descending page1 should emit continuation cursor");
    let page2_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("descending page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(
            cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("descending page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("descending page2 should execute");
    let page2_ids: Vec<Ulid> = ids_from_items(&page2.items.0);
    assert_eq!(
        page2_ids,
        vec![Ulid::from_u128(1402), Ulid::from_u128(1400)],
        "descending continuation must resume strictly after the boundary row"
    );
    assert!(
        page2.next_cursor.is_none(),
        "final descending page should not emit a continuation cursor"
    );
}

#[test]
fn load_desc_order_uses_primary_key_tie_break_for_equal_rank_rows() {
    setup_pagination_test();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(14_500),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![1],
            label: "r30".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(14_503),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20-c".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(14_501),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![3],
            label: "r20-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(14_502),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![4],
            label: "r20-b".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("rank")
        .limit(4)
        .plan()
        .expect("descending tie-break plan should build");
    let page = load
        .execute_paged_with_cursor(plan, None)
        .expect("descending tie-break page should execute");
    let page_ids: Vec<Ulid> = ids_from_items(&page.items.0);

    assert_eq!(
        page_ids,
        vec![
            Ulid::from_u128(14_500),
            Ulid::from_u128(14_501),
            Ulid::from_u128(14_502),
            Ulid::from_u128(14_503),
        ],
        "descending primary comparator must preserve canonical PK tie-break ordering",
    );
}

#[test]
fn load_cursor_rejects_signature_mismatch() {
    setup_pagination_test();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(1300),
            opt_rank: Some(1),
            rank: 1,
            tags: vec![1],
            label: "a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(1301),
            opt_rank: Some(2),
            rank: 2,
            tags: vec![2],
            label: "b".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let asc_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("ascending cursor plan should build");
    let asc_boundary = asc_plan
        .plan_cursor(None)
        .expect("ascending boundary should plan");
    let asc_page = load
        .execute_paged_with_cursor(asc_plan, asc_boundary)
        .expect("ascending cursor page should execute");
    let cursor = asc_page
        .next_cursor
        .expect("ascending page should emit cursor");

    let desc_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("rank")
        .limit(1)
        .plan()
        .expect("descending plan should build");
    let err = desc_plan
        .plan_cursor(Some(
            cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect_err("cursor from different canonical plan should be rejected");
    assert!(
        matches!(
            err,
            PlanError::Cursor(inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
        ),
        "planning should reject plan-signature mismatch"
    );
}
