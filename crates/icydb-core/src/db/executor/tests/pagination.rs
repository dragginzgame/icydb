use super::*;

// Resolve ids directly from the `(group, rank)` index prefix in raw index-key order.
fn ordered_ids_from_group_rank_index(group: u32) -> Vec<Ulid> {
    // Phase 1: read candidate keys from canonical index traversal order.
    let data_keys = DB
        .with_store_registry(|reg| {
            reg.try_get_store(TestDataStore::PATH).and_then(|store| {
                store.with_index(|index_store| {
                    index_store.resolve_data_values::<PushdownParityEntity>(
                        &PUSHDOWN_PARITY_INDEX_MODELS[0],
                        &[Value::Uint(u64::from(group))],
                    )
                })
            })
        })
        .expect("index prefix resolution should succeed");

    // Phase 2: decode typed ids while preserving traversal order.
    data_keys
        .into_iter()
        .map(|data_key| data_key.try_key::<PushdownParityEntity>())
        .collect::<Result<Vec<_>, _>>()
        .expect("resolved index keys should decode to entity ids")
}

type PushdownSeedRow = (u128, u32, u32, &'static str);

fn pushdown_entity((id, group, rank, label): PushdownSeedRow) -> PushdownParityEntity {
    PushdownParityEntity {
        id: Ulid::from_u128(id),
        group,
        rank,
        label: label.to_string(),
    }
}

fn seed_pushdown_rows(rows: &[PushdownSeedRow]) {
    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for row in rows {
        save.insert(pushdown_entity(*row))
            .expect("seed row save should succeed");
    }
}

fn pushdown_group_predicate(group: u32) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(u64::from(group)),
        CoercionId::Strict,
    ))
}

fn pushdown_group_ids(rows: &[PushdownSeedRow], group: u32) -> Vec<Ulid> {
    rows.iter()
        .filter(|(_, row_group, _, _)| *row_group == group)
        .map(|(id, _, _, _)| Ulid::from_u128(*id))
        .collect()
}

fn pushdown_rows_with_group8(prefix: u128) -> [PushdownSeedRow; 5] {
    [
        (prefix + 3, 7, 30, "g7-r30"),
        (prefix + 1, 7, 10, "g7-r10-a"),
        (prefix + 2, 7, 10, "g7-r10-b"),
        (prefix + 4, 8, 5, "g8-r5"),
        (prefix + 5, 7, 20, "g7-r20"),
    ]
}

fn pushdown_rows_with_group9(prefix: u128) -> [PushdownSeedRow; 6] {
    [
        (prefix + 3, 7, 30, "g7-r30"),
        (prefix + 1, 7, 10, "g7-r10-a"),
        (prefix + 2, 7, 10, "g7-r10-b"),
        (prefix + 4, 7, 20, "g7-r20"),
        (prefix + 5, 7, 40, "g7-r40"),
        (prefix + 6, 9, 1, "g9-r1"),
    ]
}

fn pushdown_rows_window(prefix: u128) -> [PushdownSeedRow; 4] {
    [
        (prefix + 1, 7, 10, "g7-r10"),
        (prefix + 2, 7, 20, "g7-r20"),
        (prefix + 3, 7, 30, "g7-r30"),
        (prefix + 4, 9, 1, "g9-r1"),
    ]
}

fn pushdown_rows_trace(prefix: u128) -> [PushdownSeedRow; 2] {
    [(prefix + 1, 7, 10, "g7-r10"), (prefix + 2, 7, 20, "g7-r20")]
}

#[test]
fn load_applies_order_and_pagination() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

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
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

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
        .plan_cursor_boundary(None)
        .expect("offset page boundary should plan");
    let page = load
        .execute_paged(page_plan, page_boundary)
        .expect("offset page should execute");

    let page_ids: Vec<Ulid> = page.items.0.iter().map(|(_, entity)| entity.id).collect();
    assert_eq!(
        page_ids,
        vec![Ulid::from_u128(2), Ulid::from_u128(3)],
        "offset pagination should return canonical ordered window"
    );

    let cursor_bytes = page
        .next_cursor
        .as_ref()
        .expect("offset page should emit continuation cursor");
    let token = ContinuationToken::decode(cursor_bytes.as_slice())
        .expect("continuation cursor should decode");
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
fn load_cursor_pagination_pk_order_round_trips_across_pages() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

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
        .plan_cursor_boundary(None)
        .expect("pk-order page1 boundary should plan");
    let page1 = load
        .execute_paged(page1_plan, page1_boundary)
        .expect("pk-order page1 should execute");
    let page1_ids: Vec<Ulid> = page1.items.0.iter().map(|(_, entity)| entity.id).collect();
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
        .plan_cursor_boundary(Some(cursor.as_slice()))
        .expect("pk-order page2 boundary should plan");
    let page2 = load
        .execute_paged(page2_plan, page2_boundary)
        .expect("pk-order page2 should execute");
    let page2_ids: Vec<Ulid> = page2.items.0.iter().map(|(_, entity)| entity.id).collect();
    assert_eq!(page2_ids, vec![Ulid::from_u128(3), Ulid::from_u128(4)]);
    assert!(
        page2.next_cursor.is_none(),
        "final pk-order page should not emit continuation cursor"
    );
}

#[expect(clippy::too_many_lines)]
#[test]
fn load_cursor_pagination_pk_fast_path_matches_non_fast_post_access_semantics() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

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
        .plan_cursor_boundary(None)
        .expect("fast page1 boundary should plan");
    let fast_page1 = load
        .execute_paged(fast_page1_plan, fast_page1_boundary)
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
        .plan_cursor_boundary(None)
        .expect("non-fast page1 boundary should plan");
    let non_fast_page1 = load
        .execute_paged(non_fast_page1_plan, non_fast_page1_boundary)
        .expect("non-fast page1 should execute");

    let fast_page1_ids: Vec<Ulid> = fast_page1
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
    let non_fast_page1_ids: Vec<Ulid> = non_fast_page1
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
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
    let fast_cursor_page1_token =
        ContinuationToken::decode(fast_cursor_page1.as_slice()).expect("fast cursor should decode");
    let non_fast_cursor_page1_token = ContinuationToken::decode(non_fast_cursor_page1.as_slice())
        .expect("non-fast cursor should decode");
    assert_eq!(
        fast_cursor_page1_token.boundary(),
        non_fast_cursor_page1_token.boundary(),
        "cursor boundaries should match even when signatures differ by access path"
    );

    let fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("fast page2 plan should build");
    let fast_page2_boundary = fast_page2_plan
        .plan_cursor_boundary(Some(fast_cursor_page1.as_slice()))
        .expect("fast page2 boundary should plan");
    let fast_page2 = load
        .execute_paged(fast_page2_plan, fast_page2_boundary)
        .expect("fast page2 should execute");

    let non_fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .by_ids(keys.into_iter().map(Ulid::from_u128))
        .order_by("id")
        .limit(2)
        .offset(1)
        .plan()
        .expect("non-fast page2 plan should build");
    let non_fast_page2_boundary = non_fast_page2_plan
        .plan_cursor_boundary(Some(non_fast_cursor_page1.as_slice()))
        .expect("non-fast page2 boundary should plan");
    let non_fast_page2 = load
        .execute_paged(non_fast_page2_plan, non_fast_page2_boundary)
        .expect("non-fast page2 should execute");

    let fast_page2_ids: Vec<Ulid> = fast_page2
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
    let non_fast_page2_ids: Vec<Ulid> = non_fast_page2
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
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
fn load_cursor_pagination_pk_fast_path_matches_non_fast_with_same_cursor_boundary() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

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
        .plan_cursor_boundary(None)
        .expect("cursor source boundary should plan");
    let page1 = load
        .execute_paged(page1_plan, page1_boundary)
        .expect("cursor source page should execute");
    let cursor_bytes = page1
        .next_cursor
        .as_ref()
        .expect("cursor source page should emit continuation cursor");
    let cursor_token = ContinuationToken::decode(cursor_bytes.as_slice())
        .expect("cursor source token should decode");
    let shared_boundary = cursor_token.boundary().clone();

    // Phase 3: execute page-2 parity checks with the same typed cursor boundary.
    let fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .limit(2)
        .plan()
        .expect("fast page2 plan should build");
    let fast_page2 = load
        .execute_paged(fast_page2_plan, Some(shared_boundary.clone()))
        .expect("fast page2 should execute");

    let non_fast_page2_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .by_ids(keys.into_iter().map(Ulid::from_u128))
        .order_by("id")
        .limit(2)
        .plan()
        .expect("non-fast page2 plan should build");
    let non_fast_page2 = load
        .execute_paged(non_fast_page2_plan, Some(shared_boundary))
        .expect("non-fast page2 should execute");

    let fast_ids: Vec<Ulid> = fast_page2
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
    let non_fast_ids: Vec<Ulid> = non_fast_page2
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
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
    let fast_next_token =
        ContinuationToken::decode(fast_next.as_slice()).expect("fast next cursor should decode");
    let non_fast_next_token = ContinuationToken::decode(non_fast_next.as_slice())
        .expect("non-fast next cursor should decode");
    assert_eq!(
        fast_next_token.boundary(),
        non_fast_next_token.boundary(),
        "fast and non-fast paths must emit the same continuation boundary"
    );
}

#[test]
fn load_cursor_pagination_pk_order_key_range_respects_bounds() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1_u128, 2_u128, 3_u128, 4_u128, 5_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let mut page1_logical = crate::db::query::plan::LogicalPlan::<Ulid>::new(
        crate::db::query::plan::AccessPath::KeyRange {
            start: Ulid::from_u128(2),
            end: Ulid::from_u128(4),
        },
        ReadConsistency::MissingOk,
    );
    page1_logical.order = Some(crate::db::query::plan::OrderSpec {
        fields: vec![(
            "id".to_string(),
            crate::db::query::plan::OrderDirection::Asc,
        )],
    });
    page1_logical.page = Some(crate::db::query::plan::PageSpec {
        limit: Some(2),
        offset: 0,
    });

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page1_plan = crate::db::query::plan::ExecutablePlan::<SimpleEntity>::new(page1_logical);
    let page1_boundary = page1_plan
        .plan_cursor_boundary(None)
        .expect("pk-range page1 boundary should plan");
    let page1 = load
        .execute_paged(page1_plan, page1_boundary)
        .expect("pk-range page1 should execute");
    let page1_ids: Vec<Ulid> = page1.items.0.iter().map(|(_, entity)| entity.id).collect();
    assert_eq!(page1_ids, vec![Ulid::from_u128(2), Ulid::from_u128(3)]);

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("pk-range page1 should emit continuation cursor");
    let mut page2_logical = crate::db::query::plan::LogicalPlan::<Ulid>::new(
        crate::db::query::plan::AccessPath::KeyRange {
            start: Ulid::from_u128(2),
            end: Ulid::from_u128(4),
        },
        ReadConsistency::MissingOk,
    );
    page2_logical.order = Some(crate::db::query::plan::OrderSpec {
        fields: vec![(
            "id".to_string(),
            crate::db::query::plan::OrderDirection::Asc,
        )],
    });
    page2_logical.page = Some(crate::db::query::plan::PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let page2_plan = crate::db::query::plan::ExecutablePlan::<SimpleEntity>::new(page2_logical);
    let page2_boundary = page2_plan
        .plan_cursor_boundary(Some(cursor.as_slice()))
        .expect("pk-range page2 boundary should plan");
    let page2 = load
        .execute_paged(page2_plan, page2_boundary)
        .expect("pk-range page2 should execute");
    let page2_ids: Vec<Ulid> = page2.items.0.iter().map(|(_, entity)| entity.id).collect();
    assert_eq!(page2_ids, vec![Ulid::from_u128(4)]);
    assert!(
        page2.next_cursor.is_none(),
        "final pk-range page should not emit continuation cursor"
    );
}

#[test]
fn load_cursor_pagination_pk_order_key_range_cursor_past_end_returns_empty_page() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1_u128, 2_u128, 3_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let mut logical = crate::db::query::plan::LogicalPlan::<Ulid>::new(
        crate::db::query::plan::AccessPath::KeyRange {
            start: Ulid::from_u128(1),
            end: Ulid::from_u128(2),
        },
        ReadConsistency::MissingOk,
    );
    logical.order = Some(crate::db::query::plan::OrderSpec {
        fields: vec![(
            "id".to_string(),
            crate::db::query::plan::OrderDirection::Asc,
        )],
    });
    logical.page = Some(crate::db::query::plan::PageSpec {
        limit: Some(2),
        offset: 0,
    });
    let plan = crate::db::query::plan::ExecutablePlan::<SimpleEntity>::new(logical);
    let boundary = Some(CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Ulid(Ulid::from_u128(
            99,
        )))],
    });

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let page = load
        .execute_paged(plan, boundary)
        .expect("pk-range cursor past end should execute");

    assert!(
        page.items.0.is_empty(),
        "cursor beyond range end should produce an empty page"
    );
    assert!(
        page.next_cursor.is_none(),
        "empty page should not emit a continuation cursor"
    );
}

#[test]
fn load_cursor_pagination_pk_order_missing_slot_is_invariant_violation() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

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
        .execute_paged(
            plan,
            Some(CursorBoundary {
                slots: vec![CursorBoundarySlot::Missing],
            }),
        )
        .expect_err("missing pk slot should be rejected by executor invariants");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::InvariantViolation,
        "missing pk slot should classify as invariant violation"
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Query,
        "missing pk slot should originate from query invariant checks"
    );
    assert!(
        err.message.contains("pk cursor slot must be present"),
        "missing pk slot should return a clear invariant message: {err:?}"
    );
}

#[test]
fn load_cursor_pagination_pk_order_type_mismatch_is_invariant_violation() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

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
        .execute_paged(
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
        crate::error::ErrorClass::InvariantViolation,
        "pk slot mismatch should classify as invariant violation"
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Query,
        "pk slot mismatch should originate from query invariant checks"
    );
    assert!(
        err.message.contains("pk cursor slot type mismatch"),
        "pk slot mismatch should return a clear invariant message: {err:?}"
    );
}

#[test]
fn load_cursor_pagination_pk_order_arity_mismatch_is_invariant_violation() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

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
        .execute_paged(
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
        crate::error::ErrorClass::InvariantViolation,
        "pk slot arity mismatch should classify as invariant violation"
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Query,
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
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

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
        .plan_cursor_boundary(None)
        .expect("cursor page1 boundary should plan");
    let page1 = load
        .execute_paged(page1_plan, page1_boundary)
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
        .plan_cursor_boundary(Some(cursor1.as_slice()))
        .expect("cursor page2 boundary should plan");
    let page2 = load
        .execute_paged(page2_plan, page2_boundary)
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
        .plan_cursor_boundary(Some(cursor2.as_slice()))
        .expect("cursor page3 boundary should plan");
    let page3 = load
        .execute_paged(page3_plan, page3_boundary)
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
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

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
        .plan_cursor_boundary(None)
        .expect("cursor page1 boundary should plan");
    let page1 = load
        .execute_paged(page1_plan, page1_boundary)
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
    let token = ContinuationToken::decode(cursor_bytes.as_slice())
        .expect("continuation cursor should decode");
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
        .plan_cursor_boundary(Some(cursor_bytes.as_slice()))
        .expect("cursor page2 boundary should plan");
    let page2 = load
        .execute_paged(page2_plan, page2_boundary)
        .expect("cursor page2 should execute");
    let page2_ids: Vec<Ulid> = page2.items.0.iter().map(|(_, entity)| entity.id).collect();
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
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

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
        .plan_cursor_boundary(None)
        .expect("descending page1 boundary should plan");
    let page1 = load
        .execute_paged(page1_plan, page1_boundary)
        .expect("descending page1 should execute");
    let page1_ids: Vec<Ulid> = page1.items.0.iter().map(|(_, entity)| entity.id).collect();
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
        .plan_cursor_boundary(Some(cursor.as_slice()))
        .expect("descending page2 boundary should plan");
    let page2 = load
        .execute_paged(page2_plan, page2_boundary)
        .expect("descending page2 should execute");
    let page2_ids: Vec<Ulid> = page2.items.0.iter().map(|(_, entity)| entity.id).collect();
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
fn load_cursor_rejects_signature_mismatch() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

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
        .plan_cursor_boundary(None)
        .expect("ascending boundary should plan");
    let asc_page = load
        .execute_paged(asc_plan, asc_boundary)
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
        .plan_cursor_boundary(Some(cursor.as_slice()))
        .expect_err("cursor from different canonical plan should be rejected");
    assert!(
        matches!(
            err,
            crate::db::query::plan::PlanError::ContinuationCursorSignatureMismatch { .. }
        ),
        "planning should reject plan-signature mismatch"
    );
}

#[test]
fn load_index_pushdown_eligible_order_matches_index_scan_order() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let rows = pushdown_rows_with_group8(10_000);
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .explain()
        .expect("parity explain should build");
    assert!(
        matches!(
            explain.order_pushdown,
            crate::db::query::plan::ExplainOrderPushdown::EligibleSecondaryIndex {
                index,
                prefix_len
            } if index == PUSHDOWN_PARITY_INDEX_MODELS[0].name && prefix_len == 1
        ),
        "query shape should be pushdown-eligible for group+rank index traversal"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .plan()
        .expect("parity load plan should build");
    let response = load.execute(plan).expect("parity load should execute");
    let actual_ids: Vec<Ulid> = response.0.iter().map(|(_, entity)| entity.id).collect();

    let expected_ids = ordered_ids_from_group_rank_index(7);
    assert_eq!(
        actual_ids, expected_ids,
        "fallback post-access ordering must match canonical index traversal order for eligible plans"
    );
}

#[test]
fn load_index_pushdown_eligible_paged_results_match_index_scan_window() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let rows = pushdown_rows_with_group9(11_000);
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let page1_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("page1 parity plan should build");
    let page1_boundary = page1_plan
        .plan_cursor_boundary(None)
        .expect("page1 parity boundary should plan");
    let page1 = load
        .execute_paged(page1_plan, page1_boundary)
        .expect("page1 parity should execute");
    let page1_ids: Vec<Ulid> = page1.items.0.iter().map(|(_, entity)| entity.id).collect();

    let expected_all = ordered_ids_from_group_rank_index(7);
    let expected_page1: Vec<Ulid> = expected_all.iter().copied().take(2).collect();
    assert_eq!(
        page1_ids, expected_page1,
        "page1 fallback output must match the canonical index-order window"
    );

    let page2_cursor = page1
        .next_cursor
        .as_ref()
        .expect("page1 parity should emit continuation cursor")
        .clone();
    let page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("page2 parity plan should build");
    let page2_boundary = page2_plan
        .plan_cursor_boundary(Some(page2_cursor.as_slice()))
        .expect("page2 parity boundary should plan");
    let page2 = load
        .execute_paged(page2_plan, page2_boundary)
        .expect("page2 parity should execute");
    let page2_ids: Vec<Ulid> = page2.items.0.iter().map(|(_, entity)| entity.id).collect();

    let expected_page2: Vec<Ulid> = expected_all.iter().copied().skip(2).take(2).collect();
    assert_eq!(
        page2_ids, expected_page2,
        "page2 fallback continuation must match the canonical index-order window"
    );
}

#[test]
fn load_index_pushdown_and_fallback_emit_equivalent_cursor_boundaries() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let rows = pushdown_rows_with_group9(12_000);
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let pushdown_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("pushdown plan should build");
    let pushdown_page = load
        .execute_paged(pushdown_plan, None)
        .expect("pushdown page should execute");

    let fallback_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .by_ids(group7_ids.iter().copied())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("fallback plan should build");
    let fallback_page = load
        .execute_paged(fallback_plan, None)
        .expect("fallback page should execute");

    let pushdown_ids: Vec<Ulid> = pushdown_page
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
    let fallback_ids: Vec<Ulid> = fallback_page
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
    assert_eq!(
        pushdown_ids, fallback_ids,
        "pushdown and fallback page windows should match"
    );

    let pushdown_cursor = pushdown_page
        .next_cursor
        .as_ref()
        .expect("pushdown page should emit continuation cursor");
    let fallback_cursor = fallback_page
        .next_cursor
        .as_ref()
        .expect("fallback page should emit continuation cursor");
    let pushdown_token = ContinuationToken::decode(pushdown_cursor.as_slice())
        .expect("pushdown cursor should decode");
    let fallback_token = ContinuationToken::decode(fallback_cursor.as_slice())
        .expect("fallback cursor should decode");
    assert_eq!(
        pushdown_token.boundary(),
        fallback_token.boundary(),
        "pushdown and fallback cursors should encode the same continuation boundary"
    );
}

#[test]
fn load_index_pushdown_and_fallback_resume_equivalently_from_shared_boundary() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let rows = pushdown_rows_with_group9(13_000);
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let seed_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("seed plan should build");
    let seed_page = load
        .execute_paged(seed_plan, None)
        .expect("seed page should execute");
    let seed_cursor = seed_page
        .next_cursor
        .as_ref()
        .expect("seed page should emit continuation cursor");
    let shared_boundary = ContinuationToken::decode(seed_cursor.as_slice())
        .expect("seed cursor should decode")
        .boundary()
        .clone();

    let pushdown_page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("pushdown page2 plan should build");
    let pushdown_page2 = load
        .execute_paged(pushdown_page2_plan, Some(shared_boundary.clone()))
        .expect("pushdown page2 should execute");

    let fallback_page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .by_ids(group7_ids.iter().copied())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("fallback page2 plan should build");
    let fallback_page2 = load
        .execute_paged(fallback_page2_plan, Some(shared_boundary))
        .expect("fallback page2 should execute");

    let pushdown_page2_ids: Vec<Ulid> = pushdown_page2
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
    let fallback_page2_ids: Vec<Ulid> = fallback_page2
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
    assert_eq!(
        pushdown_page2_ids, fallback_page2_ids,
        "pushdown and fallback should return the same rows for a shared continuation boundary"
    );

    let pushdown_next = pushdown_page2
        .next_cursor
        .as_ref()
        .expect("pushdown page2 should emit continuation cursor");
    let fallback_next = fallback_page2
        .next_cursor
        .as_ref()
        .expect("fallback page2 should emit continuation cursor");
    let pushdown_next_token =
        ContinuationToken::decode(pushdown_next.as_slice()).expect("pushdown next should decode");
    let fallback_next_token =
        ContinuationToken::decode(fallback_next.as_slice()).expect("fallback next should decode");
    assert_eq!(
        pushdown_next_token.boundary(),
        fallback_next_token.boundary(),
        "pushdown and fallback page2 cursors should encode identical boundaries"
    );
}

#[test]
fn load_index_desc_order_with_ties_matches_for_index_and_by_ids_paths() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let rows = pushdown_rows_with_group9(14_000);
    seed_pushdown_rows(&rows);
    let group7_ids = pushdown_group_ids(&rows, 7);

    let predicate = pushdown_group_predicate(7);
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .explain()
        .expect("desc explain should build");
    assert!(
        matches!(
            explain.order_pushdown,
            crate::db::query::plan::ExplainOrderPushdown::Matrix(
                crate::db::query::plan::validate::SecondaryOrderPushdownRejection::NonAscendingDirection { field }
            ) if field == "rank"
        ),
        "descending rank order should be ineligible and use fallback execution"
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let index_path_page1_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("index-path desc page1 plan should build");
    let index_path_page1 = load
        .execute_paged(index_path_page1_plan, None)
        .expect("index-path desc page1 should execute");

    let by_ids_page1_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .by_ids(group7_ids.iter().copied())
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("by-ids desc page1 plan should build");
    let by_ids_page1 = load
        .execute_paged(by_ids_page1_plan, None)
        .expect("by-ids desc page1 should execute");

    let index_path_page1_ids: Vec<Ulid> = index_path_page1
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
    let by_ids_page1_ids: Vec<Ulid> = by_ids_page1
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
    assert_eq!(
        index_path_page1_ids, by_ids_page1_ids,
        "descending page1 should match across index-prefix and by-ids paths"
    );

    let shared_boundary = ContinuationToken::decode(
        index_path_page1
            .next_cursor
            .as_ref()
            .expect("index-path desc page1 should emit cursor")
            .as_slice(),
    )
    .expect("index-path desc cursor should decode")
    .boundary()
    .clone();
    let index_path_page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("index-path desc page2 plan should build");
    let index_path_page2 = load
        .execute_paged(index_path_page2_plan, Some(shared_boundary.clone()))
        .expect("index-path desc page2 should execute");

    let by_ids_page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .by_ids(group7_ids.iter().copied())
        .order_by_desc("rank")
        .limit(2)
        .plan()
        .expect("by-ids desc page2 plan should build");
    let by_ids_page2 = load
        .execute_paged(by_ids_page2_plan, Some(shared_boundary))
        .expect("by-ids desc page2 should execute");

    let index_path_page2_ids: Vec<Ulid> = index_path_page2
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
    let by_ids_page2_ids: Vec<Ulid> = by_ids_page2
        .items
        .0
        .iter()
        .map(|(_, entity)| entity.id)
        .collect();
    assert_eq!(
        index_path_page2_ids, by_ids_page2_ids,
        "descending page2 should match across index-prefix and by-ids paths"
    );
}

#[test]
fn load_index_prefix_window_cursor_past_end_returns_empty_page() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let rows = pushdown_rows_window(15_000);
    seed_pushdown_rows(&rows);

    let predicate = pushdown_group_predicate(7);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let page1_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("prefix window page1 plan should build");
    let page1 = load
        .execute_paged(page1_plan, None)
        .expect("prefix window page1 should execute");

    let page1_cursor = page1
        .next_cursor
        .as_ref()
        .expect("prefix window page1 should emit continuation cursor");
    let page2_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("prefix window page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor_boundary(Some(page1_cursor.as_slice()))
        .expect("prefix window page2 boundary should plan");
    let page2 = load
        .execute_paged(page2_plan, page2_boundary)
        .expect("prefix window page2 should execute");
    assert_eq!(page2.items.0.len(), 1, "page2 should return final row only");
    assert!(
        page2.next_cursor.is_none(),
        "final prefix window page should not emit continuation cursor"
    );

    let plan_for_boundary = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("prefix window boundary plan should build");
    let explicit_boundary = plan_for_boundary
        .into_inner()
        .cursor_boundary_from_entity(&page2.items.0[0].1)
        .expect("explicit boundary from terminal row should build");
    let past_end_plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(pushdown_group_predicate(7))
        .order_by("rank")
        .limit(2)
        .plan()
        .expect("past-end plan should build");
    let past_end = load
        .execute_paged(past_end_plan, Some(explicit_boundary))
        .expect("past-end execution should succeed");
    assert!(
        past_end.items.0.is_empty(),
        "cursor boundary at final prefix row should yield an empty continuation page"
    );
    assert!(
        past_end.next_cursor.is_none(),
        "empty continuation page should not emit a cursor"
    );
}

#[test]
fn load_trace_marks_secondary_order_pushdown_outcomes() {
    #[derive(Clone, Copy)]
    enum ExpectedDecision {
        Accepted,
        RejectedNonAscending,
    }

    #[derive(Clone, Copy)]
    struct Case {
        name: &'static str,
        prefix: u128,
        descending: bool,
        expected: ExpectedDecision,
    }

    let cases = [
        Case {
            name: "accepted_ascending",
            prefix: 16_000,
            descending: false,
            expected: ExpectedDecision::Accepted,
        },
        Case {
            name: "rejected_descending",
            prefix: 17_000,
            descending: true,
            expected: ExpectedDecision::RejectedNonAscending,
        },
    ];

    init_commit_store_for_tests().expect("commit store init should succeed");

    for case in cases {
        reset_store();
        seed_pushdown_rows(&pushdown_rows_trace(case.prefix));

        let predicate = pushdown_group_predicate(7);
        let mut query = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate)
            .limit(1);
        query = if case.descending {
            query.order_by_desc("rank")
        } else {
            query.order_by("rank")
        };

        let plan = query
            .plan()
            .expect("trace outcome test plan should build for case");

        let _ = take_trace_events();
        let load =
            LoadExecutor::<PushdownParityEntity>::new(DB, false).with_trace(&TEST_TRACE_SINK);
        let _page = load
            .execute_paged(plan, None)
            .expect("trace outcome execution should succeed for case");
        let events = take_trace_events();

        let matched = events.iter().any(|event| match case.expected {
            ExpectedDecision::Accepted => matches!(
                event,
                QueryTraceEvent::Pushdown {
                    decision: TracePushdownDecision::AcceptedSecondaryIndexOrder,
                    ..
                }
            ),
            ExpectedDecision::RejectedNonAscending => matches!(
                event,
                QueryTraceEvent::Pushdown {
                    decision: TracePushdownDecision::RejectedSecondaryIndexOrder {
                        reason: TracePushdownRejectionReason::NonAscendingDirection
                    },
                    ..
                }
            ),
        });

        assert!(
            matched,
            "trace should emit expected secondary-order pushdown marker for case '{}'",
            case.name
        );
    }
}
