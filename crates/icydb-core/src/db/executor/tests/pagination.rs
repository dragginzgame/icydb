use super::*;

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
