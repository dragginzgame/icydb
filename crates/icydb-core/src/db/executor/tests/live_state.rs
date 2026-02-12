use super::*;

#[test]
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
        .execute_paged(
            page1_plan,
            Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
                .order_by("rank")
                .limit(1)
                .plan()
                .expect("boundary plan should build")
                .plan_cursor_boundary(None)
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
        .plan_cursor_boundary(Some(cursor.as_slice()))
        .expect("page2 boundary should plan");
    let page2 = load
        .execute_paged(page2_plan, page2_boundary)
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
