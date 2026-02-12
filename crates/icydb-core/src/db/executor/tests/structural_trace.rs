use super::*;

#[test]
#[expect(clippy::too_many_lines)]
fn load_structural_guard_emits_post_access_phase_and_stats() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(901),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![1, 3],
            label: "needle alpha".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(902),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![2],
            label: "other".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(903),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![9],
            label: "NEEDLE beta".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(904),
            opt_rank: Some(40),
            rank: 40,
            tags: vec![4],
            label: "needle gamma".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::In,
            Value::List(vec![Value::Uint(20), Value::Uint(30), Value::Uint(40)]),
            CoercionId::Strict,
        )),
        Predicate::TextContainsCi {
            field: "label".to_string(),
            value: Value::Text("needle".to_string()),
        },
    ]);

    let plan_for_stats = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .limit(1)
        .offset(1)
        .plan()
        .expect("structural stats plan should build");
    let plan_for_execute = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(1)
        .offset(1)
        .plan()
        .expect("structural execute plan should build");

    // Structural assertion: post-access stats must report all load phases were applied.
    let logical = plan_for_stats.into_inner();
    let ctx = DB
        .recovered_context::<PhaseEntity>()
        .expect("recovered context should succeed");
    let data_rows = ctx
        .rows_from_access_plan(&logical.access, logical.consistency)
        .expect("access rows should load");
    let mut rows = Context::deserialize_rows(data_rows).expect("rows should deserialize");
    let stats = logical
        .apply_post_access::<PhaseEntity, _>(&mut rows)
        .expect("post-access should apply");
    assert!(stats.filtered, "filter phase should be applied");
    assert!(stats.ordered, "order phase should be applied");
    assert!(stats.paged, "pagination phase should be applied");
    assert!(
        !stats.delete_was_limited,
        "delete limit must remain inactive on load plans"
    );
    assert_eq!(stats.rows_after_filter, 3, "filter should keep three rows");
    assert_eq!(
        stats.rows_after_order, 3,
        "ordering should preserve row count"
    );
    assert_eq!(
        stats.rows_after_page, 1,
        "pagination should trim to one row"
    );
    assert_eq!(
        stats.rows_after_delete_limit, 1,
        "load plans should not apply delete limits"
    );

    // Runtime assertion: executor output and trace phase must both reflect post-access execution.
    let _ = take_trace_events();
    let load = LoadExecutor::<PhaseEntity>::new(DB, false).with_trace(&TEST_TRACE_SINK);
    let response = load
        .execute(plan_for_execute)
        .expect("structural load should execute");
    assert_eq!(response.0.len(), 1, "post-access output should be paged");
    assert_eq!(
        response.0[0].1.rank, 30,
        "paged row should come from filtered+ordered post-access window"
    );

    let events = take_trace_events();
    assert!(
        events.iter().any(|event| matches!(
            event,
            QueryTraceEvent::Phase {
                phase: TracePhase::PostAccess,
                rows: 1,
                ..
            }
        )),
        "trace must include post-access phase with final row count"
    );
}
