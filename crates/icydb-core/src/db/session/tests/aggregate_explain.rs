use super::*;

fn assert_session_aggregate_terminal_plan_semantic_parity(
    left: &crate::db::ExplainAggregateTerminalPlan,
    right: &crate::db::ExplainAggregateTerminalPlan,
) {
    assert_eq!(left.terminal(), right.terminal());
    assert_eq!(left.query().access(), right.query().access());
    assert_eq!(left.query().order_by(), right.query().order_by());
    assert_eq!(left.query().page(), right.query().page());
    assert_eq!(left.query().grouping(), right.query().grouping());
    assert_eq!(
        left.query().order_pushdown(),
        right.query().order_pushdown()
    );
    assert_eq!(left.query().consistency(), right.query().consistency());
    assert_eq!(
        left.execution().aggregation(),
        right.execution().aggregation()
    );
    assert_eq!(
        left.execution().access_strategy(),
        right.execution().access_strategy()
    );
    assert_eq!(
        left.execution().execution_mode(),
        right.execution().execution_mode()
    );
    assert_eq!(
        left.execution().ordering_source(),
        right.execution().ordering_source()
    );
    assert_eq!(left.execution().limit(), right.execution().limit());
    assert_eq!(left.execution().cursor(), right.execution().cursor());
    assert_eq!(
        left.execution().covering_projection(),
        right.execution().covering_projection()
    );
    assert_eq!(
        left.execution_node_descriptor().node_type(),
        right.execution_node_descriptor().node_type()
    );
    assert_eq!(
        left.execution_node_descriptor().execution_mode(),
        right.execution_node_descriptor().execution_mode()
    );
    assert_eq!(
        left.execution_node_descriptor().access_strategy(),
        right.execution_node_descriptor().access_strategy()
    );
    assert_eq!(
        left.execution_node_descriptor().ordering_source(),
        right.execution_node_descriptor().ordering_source()
    );
    assert_eq!(
        left.execution_node_descriptor().limit(),
        right.execution_node_descriptor().limit()
    );
    assert_eq!(
        left.execution_node_descriptor().cursor(),
        right.execution_node_descriptor().cursor()
    );
    assert_eq!(
        left.execution_node_descriptor().covering_scan(),
        right.execution_node_descriptor().covering_scan()
    );
    assert_eq!(
        left.execution_node_descriptor().rows_expected(),
        right.execution_node_descriptor().rows_expected()
    );
}

#[test]
fn session_aggregate_bytes_matrix_matches_execute_window_parity() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_971, 7, 10),
            (8_972, 7, 20),
            (8_973, 7, 35),
            (8_974, 8, 99),
            (8_975, 7, 50),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("rank")
            .offset(1)
            .limit(2)
    };

    let expected_response = load_window()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("baseline execute for session bytes parity should succeed");
    let expected_ids: Vec<_> = expected_response.ids().map(|id| id.key()).collect();

    for (actual_bytes, expected_bytes, context) in [
        (
            load_window()
                .bytes()
                .expect("session bytes terminal should succeed"),
            session_aggregate_persisted_payload_bytes_for_ids(expected_ids),
            "session bytes parity",
        ),
        (
            load_window()
                .bytes_by("rank")
                .expect("session bytes_by(rank) terminal should succeed"),
            session_aggregate_serialized_field_payload_bytes_for_rows(&expected_response, "rank"),
            "session bytes_by(rank) parity",
        ),
    ] {
        assert_eq!(
            actual_bytes, expected_bytes,
            "{context} should match the execute-window payload byte sum",
        );
    }
}

#[test]
fn session_aggregate_bytes_empty_window_matrix_returns_zero() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(&session, &[(8_991, 7, 10), (8_992, 7, 20), (8_993, 8, 99)]);

    for (actual_bytes, context) in [
        (
            session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(999))
                .order_by("rank")
                .bytes()
                .expect("session bytes terminal should succeed for empty windows"),
            "session bytes terminal",
        ),
        (
            session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(999))
                .order_by("rank")
                .bytes_by("rank")
                .expect("session bytes_by(rank) terminal should succeed for empty windows"),
            "session bytes_by(rank) terminal",
        ),
    ] {
        assert_eq!(
            actual_bytes, 0,
            "{context} should return zero for empty windows"
        );
    }
}

#[test]
fn session_aggregate_bytes_by_unknown_field_fails_before_scan_budget_consumption() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_901, 7, 10),
            (8_902, 7, 20),
            (8_903, 7, 30),
            (8_904, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by_desc("id")
            .offset(0)
            .limit(3)
    };

    let (result, scanned_rows) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            load_window().bytes_by("missing_field")
        });
    let Err(err) = result else {
        panic!("session bytes_by(missing_field) should be rejected");
    };

    assert!(
        matches!(err, QueryError::Execute(_)),
        "session unknown-field bytes_by should remain an execute-domain error: {err:?}",
    );
    assert_eq!(
        scanned_rows, 0,
        "session unknown-field bytes_by should fail before scan-budget consumption",
    );
    assert!(
        err.to_string().contains("unknown aggregate target field"),
        "session unknown-field bytes_by should preserve explicit field taxonomy: {err:?}",
    );
}

#[test]
fn session_aggregate_explain_bytes_by_metadata_matrix_projects_materialized_mode() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_905, 7, 20),
            (8_906, 7, 20),
            (8_907, 7, 30),
            (8_908, 8, 20),
        ],
    );

    let filtered_descriptor = session
        .load::<SessionAggregateEntity>()
        .filter(Predicate::And(vec![
            session_aggregate_group_predicate(7),
            Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                CompareOp::Eq,
                Value::from(20u64),
                CoercionId::Strict,
            )),
        ]))
        .explain_bytes_by("rank")
        .expect("session bytes_by explain should succeed for filtered shapes");
    let strict_descriptor = session
        .load_with_consistency::<SessionAggregateEntity>(crate::db::MissingRowPolicy::Error)
        .filter(Predicate::And(vec![
            session_aggregate_group_predicate(7),
            Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                CompareOp::Eq,
                Value::from(20u64),
                CoercionId::Strict,
            )),
        ]))
        .explain_bytes_by("rank")
        .expect("session bytes_by explain should succeed for strict load shapes");

    for (descriptor, context) in [
        (&filtered_descriptor, "filtered session bytes_by explain"),
        (&strict_descriptor, "strict session bytes_by explain"),
    ] {
        assert_eq!(
            descriptor.node_properties().get("terminal_projection_mode"),
            Some(&Value::from("field_materialized")),
            "{context} should fail closed to materialized projection mode",
        );
        assert_eq!(
            descriptor.node_properties().get("terminal_index_only"),
            Some(&Value::from(false)),
            "{context} should project index-only=false",
        );
    }
}

#[test]
fn session_aggregate_explain_bytes_by_unknown_field_fails_before_planning() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(&session, &[(8_914, 7, 10), (8_915, 7, 20)]);

    let result = session
        .load::<SessionAggregateEntity>()
        .filter(session_aggregate_group_predicate(7))
        .explain_bytes_by("missing_field");

    let Err(err) = result else {
        panic!("session bytes_by explain for unknown fields should fail closed");
    };
    assert!(
        matches!(err, QueryError::Execute(_)),
        "session unknown-field bytes_by explain should remain an execute-domain failure: {err:?}"
    );
    assert!(
        err.to_string().contains("unknown aggregate target field"),
        "session unknown-field bytes_by explain should preserve field taxonomy: {err:?}",
    );
}

#[test]
fn session_aggregate_terminal_explain_exists_matrix_preserves_alias_and_route_contracts() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (9_421, 7, 10),
            (9_422, 7, 20),
            (9_423, 7, 30),
            (9_424, 8, 99),
        ],
    );
    let query = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("rank")
            .order_by("id")
    };

    let exists_plan = query()
        .explain_exists()
        .expect("session explain_exists should succeed");
    let not_exists_plan = query()
        .explain_not_exists()
        .expect("session explain_not_exists should succeed");

    assert_eq!(exists_plan.terminal(), AggregateKind::Exists);
    assert!(matches!(
        exists_plan.execution().ordering_source(),
        crate::db::ExplainExecutionOrderingSource::AccessOrder
            | crate::db::ExplainExecutionOrderingSource::Materialized
    ));

    let exists_execution = exists_plan.execution();
    assert_eq!(exists_execution.aggregation(), AggregateKind::Exists);
    assert!(matches!(
        exists_execution.ordering_source(),
        crate::db::ExplainExecutionOrderingSource::AccessOrder
            | crate::db::ExplainExecutionOrderingSource::Materialized
    ));
    assert_eq!(
        exists_execution.access_strategy(),
        exists_plan.query().access()
    );
    assert!(matches!(
        exists_execution.execution_mode(),
        crate::db::ExplainExecutionMode::Streaming | crate::db::ExplainExecutionMode::Materialized
    ));
    assert_eq!(exists_execution.limit(), None);
    assert!(!exists_execution.cursor());
    assert!(
        !exists_execution.covering_projection(),
        "ordered exists explain shape should not mark index-only covering projection",
    );
    let exists_node = exists_plan.execution_node_descriptor();
    assert_eq!(
        exists_node.node_type(),
        crate::db::ExplainExecutionNodeType::AggregateExists
    );
    assert_eq!(
        exists_node.execution_mode(),
        exists_execution.execution_mode()
    );
    assert_eq!(
        exists_node.access_strategy(),
        Some(exists_execution.access_strategy())
    );
    assert_eq!(
        not_exists_plan.terminal(),
        AggregateKind::Exists,
        "not_exists explain alias should remain backed by exists terminal execution",
    );
    assert_session_aggregate_terminal_plan_semantic_parity(&not_exists_plan, &exists_plan);
}

#[test]
fn session_aggregate_exists_explain_hides_non_ready_secondary_indexes_from_planner_visibility() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_session_explain_entities(
        &session,
        &[
            (9_461, 7, 10),
            (9_462, 7, 20),
            (9_463, 7, 30),
            (9_464, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionExplainEntity>()
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "group",
                CompareOp::Eq,
                Value::from(7u64),
                CoercionId::Strict,
            )))
            .order_by("rank")
            .order_by("id")
    };

    let ready_plan = load_window()
        .explain_exists()
        .expect("indexed aggregate exists explain should succeed while the index is ready");
    assert!(
        matches!(
            ready_plan.query().access(),
            ExplainAccessPath::IndexPrefix { name, .. } if *name == "group_rank"
        ),
        "ready aggregate exists planning should keep the composite secondary index visible",
    );
    assert_eq!(
        ready_plan.execution().access_strategy(),
        ready_plan.query().access(),
        "ready aggregate exists execution should inherit the planner-owned access path",
    );

    INDEXED_SESSION_SQL_DB
        .recovered_store(IndexedSessionSqlStore::PATH)
        .expect("indexed SQL store should recover")
        .mark_index_building();

    let building_plan = load_window().explain_exists().expect(
        "aggregate exists explain should still succeed when the shared index becomes building",
    );
    assert!(
        matches!(building_plan.query().access(), ExplainAccessPath::FullScan),
        "non-ready aggregate exists planning must hide the secondary index instead of planning a downgraded shortcut",
    );
    assert_eq!(
        building_plan.execution().access_strategy(),
        building_plan.query().access(),
        "non-ready aggregate exists execution should inherit the fallback planner path",
    );
    assert!(
        load_window()
            .exists()
            .expect("aggregate exists should still execute after planner visibility fallback"),
        "planner visibility fallback must preserve aggregate exists correctness",
    );
}

#[test]
fn session_aggregate_terminal_explain_first_last_preserve_order_shape_parity() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (9_441, 7, 10),
            (9_442, 7, 20),
            (9_443, 7, 30),
            (9_444, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("rank")
            .order_by("id")
    };

    let first_plan = load_window()
        .explain_first()
        .expect("session explain_first should succeed");
    let last_plan = load_window()
        .explain_last()
        .expect("session explain_last should succeed");

    assert_eq!(first_plan.terminal(), AggregateKind::First);
    assert_eq!(last_plan.terminal(), AggregateKind::Last);
    assert_eq!(
        first_plan.execution().ordering_source(),
        crate::db::ExplainExecutionOrderingSource::Materialized,
        "first explain should remain on the materialized ordering source",
    );
    assert_eq!(
        last_plan.execution().ordering_source(),
        crate::db::ExplainExecutionOrderingSource::Materialized,
        "last explain should remain on the materialized ordering source",
    );
    assert_eq!(
        first_plan.query().access(),
        last_plan.query().access(),
        "first vs last explain should preserve access-shape parity for equivalent windows",
    );
    assert_eq!(first_plan.query().order_by(), last_plan.query().order_by());
    assert_eq!(first_plan.query().page(), last_plan.query().page());
    assert_eq!(first_plan.query().grouping(), last_plan.query().grouping());
    assert_eq!(
        first_plan.query().order_pushdown(),
        last_plan.query().order_pushdown()
    );
    assert_eq!(
        first_plan.query().consistency(),
        last_plan.query().consistency()
    );
    assert_eq!(
        first_plan.execution().access_strategy(),
        last_plan.execution().access_strategy(),
    );
    assert_eq!(
        first_plan.execution().execution_mode(),
        last_plan.execution().execution_mode(),
        "first vs last explains should agree on execution-mode classification",
    );
    assert_eq!(
        first_plan.execution().ordering_source(),
        last_plan.execution().ordering_source(),
        "first vs last explains should agree on ordering-source classification",
    );
    assert_eq!(first_plan.execution().limit(), None);
    assert_eq!(last_plan.execution().limit(), None);
    assert!(!first_plan.execution().cursor());
    assert!(!last_plan.execution().cursor());

    let first_node = first_plan.execution_node_descriptor();
    let last_node = last_plan.execution_node_descriptor();
    assert_eq!(
        first_node.node_type(),
        crate::db::ExplainExecutionNodeType::AggregateFirst
    );
    assert_eq!(
        last_node.node_type(),
        crate::db::ExplainExecutionNodeType::AggregateLast
    );
    assert_eq!(first_node.execution_mode(), last_node.execution_mode());
    assert_eq!(first_node.access_strategy(), last_node.access_strategy());
    assert_eq!(first_node.ordering_source(), last_node.ordering_source());
    assert_eq!(first_node.limit(), last_node.limit());
    assert_eq!(first_node.cursor(), last_node.cursor());
    assert_eq!(first_node.covering_scan(), last_node.covering_scan());
    assert_eq!(first_node.rows_expected(), last_node.rows_expected());
}
