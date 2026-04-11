use super::*;

#[test]
fn session_aggregate_bytes_matches_execute_window_persisted_payload_sum() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_951, 7, 10),
            (8_952, 7, 20),
            (8_953, 7, 35),
            (8_954, 8, 99),
            (8_955, 7, 50),
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

    let expected_ids = load_window()
        .execute()
        .expect("baseline execute for session bytes parity should succeed")
        .ids()
        .map(|id| id.key())
        .collect();
    let expected_bytes = session_aggregate_persisted_payload_bytes_for_ids(expected_ids);
    let actual_bytes = load_window()
        .bytes()
        .expect("session bytes terminal should succeed");

    assert_eq!(
        actual_bytes, expected_bytes,
        "session bytes parity should match persisted payload byte sum of the effective window",
    );
}

#[test]
fn session_aggregate_bytes_empty_window_returns_zero() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(&session, &[(8_961, 7, 10), (8_962, 7, 20), (8_963, 8, 99)]);

    let actual_bytes = session
        .load::<SessionAggregateEntity>()
        .filter(session_aggregate_group_predicate(999))
        .order_by("rank")
        .bytes()
        .expect("session bytes terminal should succeed for empty windows");

    assert_eq!(
        actual_bytes, 0,
        "session bytes terminal should return zero for empty windows",
    );
}

#[test]
fn session_aggregate_bytes_by_matches_execute_window_serialized_field_sum() {
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
        .expect("baseline execute for session bytes_by parity should succeed");
    let expected_bytes =
        session_aggregate_serialized_field_payload_bytes_for_rows(&expected_response, "rank");
    let actual_bytes = load_window()
        .bytes_by("rank")
        .expect("session bytes_by(rank) terminal should succeed");

    assert_eq!(
        actual_bytes, expected_bytes,
        "session bytes_by(rank) parity should match serialized field byte sum of the effective window",
    );
}

#[test]
fn session_aggregate_bytes_by_empty_window_returns_zero() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(&session, &[(8_991, 7, 10), (8_992, 7, 20), (8_993, 8, 99)]);

    let actual_bytes = session
        .load::<SessionAggregateEntity>()
        .filter(session_aggregate_group_predicate(999))
        .order_by("rank")
        .bytes_by("rank")
        .expect("session bytes_by(rank) terminal should succeed for empty windows");

    assert_eq!(
        actual_bytes, 0,
        "session bytes_by(rank) terminal should return zero for empty windows",
    );
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
fn session_aggregate_explain_bytes_by_projects_terminal_metadata_for_filtered_shape() {
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

    let descriptor = session
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

    assert_eq!(
        descriptor.node_properties().get("terminal"),
        Some(&Value::from("bytes_by")),
        "session bytes_by explain should project the terminal label",
    );
    assert_eq!(
        descriptor.node_properties().get("terminal_field"),
        Some(&Value::from("rank")),
        "session bytes_by explain should preserve the requested terminal field",
    );
    assert_eq!(
        descriptor.node_properties().get("terminal_projection_mode"),
        Some(&Value::from("field_materialized")),
        "filtered session bytes_by explain should project the current materialized mode label",
    );
    assert_eq!(
        descriptor.node_properties().get("terminal_index_only"),
        Some(&Value::from(false)),
        "filtered session bytes_by explain should project index-only=false under current planner access",
    );
}

#[test]
fn session_aggregate_explain_bytes_by_projects_materialized_mode_for_strict_queries() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(&session, &[(8_911, 7, 20), (8_912, 7, 20), (8_913, 8, 30)]);

    let descriptor = session
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

    assert_eq!(
        descriptor.node_properties().get("terminal_projection_mode"),
        Some(&Value::from("field_materialized")),
        "strict session bytes_by explain should fail closed to materialized projection mode",
    );
    assert_eq!(
        descriptor.node_properties().get("terminal_index_only"),
        Some(&Value::from(false)),
        "strict session bytes_by explain should project index-only=false",
    );
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
fn session_aggregate_terminal_explain_reports_standard_route_for_exists() {
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

    let exists_terminal_plan = session
        .load::<SessionAggregateEntity>()
        .filter(session_aggregate_group_predicate(7))
        .order_by("rank")
        .order_by("id")
        .explain_exists()
        .expect("session explain_exists should succeed");

    assert_eq!(exists_terminal_plan.terminal(), AggregateKind::Exists);
    assert!(matches!(
        exists_terminal_plan.execution().ordering_source(),
        crate::db::ExplainExecutionOrderingSource::AccessOrder
            | crate::db::ExplainExecutionOrderingSource::Materialized
    ));

    let exists_execution = exists_terminal_plan.execution();
    assert_eq!(exists_execution.aggregation(), AggregateKind::Exists);
    assert!(matches!(
        exists_execution.ordering_source(),
        crate::db::ExplainExecutionOrderingSource::AccessOrder
            | crate::db::ExplainExecutionOrderingSource::Materialized
    ));
    assert_eq!(
        exists_execution.access_strategy(),
        exists_terminal_plan.query().access()
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
    let exists_node = exists_terminal_plan.execution_node_descriptor();
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
    assert!(
        exists_node
            .render_text_tree()
            .contains("AggregateExists execution_mode="),
        "text tree should render the standard aggregate node label",
    );
}

#[test]
fn session_aggregate_terminal_explain_not_exists_alias_matches_exists_plan() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (9_431, 7, 10),
            (9_432, 7, 20),
            (9_433, 7, 30),
            (9_434, 8, 99),
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

    assert_eq!(
        not_exists_plan.terminal(),
        AggregateKind::Exists,
        "not_exists explain alias should remain backed by exists terminal execution",
    );
    assert_eq!(
        session_aggregate_terminal_plan_snapshot(&not_exists_plan),
        session_aggregate_terminal_plan_snapshot(&exists_plan),
        "not_exists explain alias must remain plan-identical to exists explain",
    );
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

    mark_indexed_session_sql_index_building();

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
    assert_eq!(
        first_node.node_properties(),
        last_node.node_properties(),
        "first vs last descriptor metadata should remain stable for equivalent windows",
    );
}
