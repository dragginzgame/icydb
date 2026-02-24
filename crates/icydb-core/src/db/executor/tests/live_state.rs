use super::*;

#[test]
#[expect(clippy::too_many_lines)]
fn load_cursor_live_state_reordered_update_can_skip_rows_before_boundary() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(4101),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1],
            label: "r10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(4102),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(4103),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![3],
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
        .expect("page1 plan should build");
    let page1 = load
        .execute_paged_with_cursor(
            page1_plan,
            Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
                .order_by("rank")
                .limit(1)
                .plan()
                .expect("boundary plan should build")
                .plan_cursor(None)
                .expect("page1 boundary should plan"),
        )
        .expect("page1 should execute");
    assert_eq!(page1.items.0.len(), 1, "page1 should return one row");
    assert_eq!(
        page1.items.0[0].1.id,
        Ulid::from_u128(4101),
        "page1 should return the initial lowest-rank row"
    );

    // Reorder one unseen row to before the continuation boundary.
    save.update(PhaseEntity {
        id: Ulid::from_u128(4103),
        opt_rank: Some(5),
        rank: 5,
        tags: vec![3],
        label: "r05".to_string(),
    })
    .expect("reordering update should succeed");

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("page1 should emit continuation cursor");
    let page2_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(
            cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("page2 should execute");
    assert_eq!(page2.items.0.len(), 1, "page2 should return one row");
    assert_eq!(
        page2.items.0[0].1.id,
        Ulid::from_u128(4102),
        "row moved before boundary should not re-enter forward continuation"
    );
    assert!(
        page2.next_cursor.is_none(),
        "updated row moved before the boundary is skipped in this live-state continuation"
    );

    let full_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .plan()
        .expect("full-order plan should build");
    let now = load
        .execute(full_plan)
        .expect("full-order load should succeed");
    assert_eq!(
        now.0[0].1.id,
        Ulid::from_u128(4103),
        "updated row now sorts before the boundary in live state"
    );
}

#[test]
fn load_cursor_live_state_insert_after_boundary_can_appear_on_next_page() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(4201),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1],
            label: "r10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(4202),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(4203),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![3],
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
        .expect("page1 plan should build");
    let page1 = load
        .execute_paged_with_cursor(
            page1_plan,
            Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
                .order_by("rank")
                .limit(1)
                .plan()
                .expect("boundary plan should build")
                .plan_cursor(None)
                .expect("page1 boundary should plan"),
        )
        .expect("page1 should execute");
    assert_eq!(page1.items.0.len(), 1, "page1 should return one row");
    assert_eq!(
        page1.items.0[0].1.id,
        Ulid::from_u128(4201),
        "page1 should return the initial boundary row"
    );

    // Insert a new row that sorts after the boundary and before previously unseen rows.
    save.insert(PhaseEntity {
        id: Ulid::from_u128(4299),
        opt_rank: Some(15),
        rank: 15,
        tags: vec![9],
        label: "r15".to_string(),
    })
    .expect("insert between pages should succeed");

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("page1 should emit continuation cursor");
    let page2_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(
            cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("page2 should execute");
    assert_eq!(page2.items.0.len(), 1, "page2 should return one row");
    assert_eq!(
        page2.items.0[0].1.id,
        Ulid::from_u128(4299),
        "new row inserted after boundary may appear on continuation page"
    );
}

#[test]
fn load_cursor_live_state_delete_between_pages_can_shrink_remaining_results() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(4301),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1],
            label: "r10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(4302),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2],
            label: "r20".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(4303),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![3],
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
        .expect("page1 plan should build");
    let page1 = load
        .execute_paged_with_cursor(
            page1_plan,
            Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
                .order_by("rank")
                .limit(1)
                .plan()
                .expect("boundary plan should build")
                .plan_cursor(None)
                .expect("page1 boundary should plan"),
        )
        .expect("page1 should execute");
    assert_eq!(page1.items.0.len(), 1, "page1 should return one row");
    assert_eq!(
        page1.items.0[0].1.id,
        Ulid::from_u128(4301),
        "page1 should return the initial boundary row"
    );

    // Remove one unseen row between page requests.
    let delete_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(Ulid::from_u128(4302))
        .plan()
        .expect("delete plan should build");
    let delete = DeleteExecutor::<PhaseEntity>::new(DB, false);
    let deleted = delete.execute(delete_plan).expect("delete should succeed");
    assert_eq!(deleted.0.len(), 1, "one row should be removed");
    assert_eq!(
        deleted.0[0].1.id,
        Ulid::from_u128(4302),
        "delete should remove the middle row before continuation"
    );

    let cursor = page1
        .next_cursor
        .as_ref()
        .expect("page1 should emit continuation cursor");
    let page2_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("page2 plan should build");
    let page2_boundary = page2_plan
        .plan_cursor(Some(
            cursor
                .encode()
                .expect("continuation cursor should serialize")
                .as_slice(),
        ))
        .expect("page2 boundary should plan");
    let page2 = load
        .execute_paged_with_cursor(page2_plan, page2_boundary)
        .expect("page2 should execute");
    assert_eq!(page2.items.0.len(), 1, "page2 should return one row");
    assert_eq!(
        page2.items.0[0].1.id,
        Ulid::from_u128(4303),
        "deleted rows must not appear on continuation pages"
    );
    assert!(
        page2.next_cursor.is_none(),
        "deleting unseen rows can reduce remaining continuation pages"
    );
}
