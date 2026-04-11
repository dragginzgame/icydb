use super::*;

#[test]
fn execute_sql_projection_expression_order_query_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so expression order
    // semantics disagree with primary-key order instead of accidentally
    // matching one tie-break-only fallback.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_243_u128, "sam", 10),
            (9_244, "Alex", 20),
            (9_241, "bob", 30),
            (9_242, "zoe", 40),
        ],
    );

    // Phase 2: verify the projection lane keeps the same `LOWER(name), id`
    // ordering contract as the entity lane and the explicit expected window on
    // the matching expression index.
    let sql = "SELECT id, name FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2";
    let projected_rows =
        dispatch_projection_rows::<ExpressionIndexedSessionSqlEntity>(&session, sql)
            .expect("expression-order projection query should execute");
    let entity_rows = session
        .execute_sql::<ExpressionIndexedSessionSqlEntity>(sql)
        .expect("expression-order entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().name.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let expected_rows = vec![
        vec![
            Value::Ulid(Ulid::from_u128(9_244)),
            Value::Text("Alex".to_string()),
        ],
        vec![
            Value::Ulid(Ulid::from_u128(9_241)),
            Value::Text("bob".to_string()),
        ],
    ];

    assert_eq!(
        entity_projected_rows, expected_rows,
        "entity execution must honor the LOWER(name), id ordering contract",
    );
    assert_eq!(
        projected_rows, expected_rows,
        "projection execution must honor the LOWER(name), id ordering contract",
    );
}

#[test]
fn execute_sql_expression_order_index_range_scan_preserves_lower_name_order() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset whose primary-key
    // order disagrees with canonical `LOWER(name), id` traversal.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_243_u128, "sam", 10),
            (9_244, "Alex", 20),
            (9_241, "bob", 30),
            (9_242, "zoe", 40),
        ],
    );

    // Phase 2: lower the expression-order SQL shape to its shared index-range
    // access contract and inspect the raw index scan order directly.
    let plan = session
        .query_from_sql::<ExpressionIndexedSessionSqlEntity>(
            "SELECT id, name FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect("expression-order SQL query should lower")
        .plan()
        .expect("expression-order SQL query should plan")
        .into_inner();
    let lowered_specs =
        lower_index_range_specs(ExpressionIndexedSessionSqlEntity::ENTITY_TAG, &plan.access)
            .expect("expression-order access plan should lower to one raw index range");
    let [spec] = lowered_specs.as_slice() else {
        panic!("expression-order access plan should use exactly one index-range spec");
    };
    let store = INDEXED_SESSION_SQL_DB
        .recovered_store(IndexedSessionSqlStore::PATH)
        .expect("expression-order indexed store should recover");
    let keys = store
        .with_index(|index_store| {
            index_store.resolve_data_values_in_raw_range_limited(
                ExpressionIndexedSessionSqlEntity::ENTITY_TAG,
                spec.index(),
                (spec.lower(), spec.upper()),
                IndexScanContinuationInput::new(None, Direction::Asc),
                3,
                None,
            )
        })
        .expect("expression-order index range scan should succeed");
    let scanned_ids = keys
        .into_iter()
        .map(|key: DataKey| match key.storage_key() {
            StorageKey::Ulid(id) => id,
            other => {
                panic!("expression-order fixture keys should stay on ULID primary keys: {other:?}")
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(
        scanned_ids,
        vec![
            Ulid::from_u128(9_244),
            Ulid::from_u128(9_241),
            Ulid::from_u128(9_243),
        ],
        "raw expression-index range scans must preserve the canonical LOWER(name), id order before later pagination/windowing",
    );
}

#[test]
fn execute_sql_projection_expression_order_desc_query_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: reuse one deterministic mixed-case dataset whose primary-key
    // order disagrees with reverse expression order.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_243_u128, "sam", 10),
            (9_244, "Alex", 20),
            (9_241, "bob", 30),
            (9_242, "zoe", 40),
        ],
    );

    // Phase 2: verify descending expression order stays explicit on both the
    // projection and entity lanes.
    let sql = "SELECT id, name FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) DESC, id DESC LIMIT 2";
    let projected_rows =
        dispatch_projection_rows::<ExpressionIndexedSessionSqlEntity>(&session, sql)
            .expect("descending expression-order projection query should execute");
    let entity_rows = session
        .execute_sql::<ExpressionIndexedSessionSqlEntity>(sql)
        .expect("descending expression-order entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().name.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let expected_rows = vec![
        vec![
            Value::Ulid(Ulid::from_u128(9_242)),
            Value::Text("zoe".to_string()),
        ],
        vec![
            Value::Ulid(Ulid::from_u128(9_243)),
            Value::Text("sam".to_string()),
        ],
    ];

    assert_eq!(
        entity_projected_rows, expected_rows,
        "descending entity execution must honor the LOWER(name), id ordering contract",
    );
    assert_eq!(
        projected_rows, expected_rows,
        "descending projection execution must honor the LOWER(name), id ordering contract",
    );
}

#[test]
fn session_explain_execution_order_only_expression_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so EXPLAIN EXECUTION
    // can prove the expression-index route instead of a materialized fallback.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_251_u128, "sam", 10),
            (9_252, "Alex", 20),
            (9_253, "bob", 30),
        ],
    );

    // Phase 2: require EXPLAIN EXECUTION to surface the shared order-only
    // expression index-range root and access-satisfied ordering markers.
    let descriptor = session
        .query_from_sql::<ExpressionIndexedSessionSqlEntity>(
            "SELECT id, name FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect("expression order-only SQL query should lower")
        .explain_execution()
        .expect("expression order-only SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "expression order-only queries should stay on the shared index-range root",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "expression order-only index-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "expression order-only index-range roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_order_only_expression_key_only_query_uses_covering_read_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so the key-only
    // expression-order sibling can prove true covering eligibility without
    // claiming original `name` reconstruction from the lowered key.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_261_u128, "sam", 10),
            (9_262, "Alex", 20),
            (9_263, "bob", 30),
        ],
    );

    // Phase 2: require the session-backed query-builder explain to reuse the
    // planner-proven covering-read route for the `id`-only projection.
    let descriptor = session
        .query_from_sql::<ExpressionIndexedSessionSqlEntity>(
            "SELECT id FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect("expression key-only order SQL query should lower")
        .explain_execution()
        .expect("expression key-only order SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "expression key-only order queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "expression key-only order queries should expose the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "expression key-only order explain roots should expose the covering-read route label",
    );
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect("expression key-only order explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![Value::Text("id".to_string())])),
        "expression key-only order covering nodes should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "session-backed expression key-only order explain should inherit the planner-proven covering mode",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "expression key-only order explain roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_order_only_expression_key_only_desc_query_uses_covering_read_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so the descending
    // key-only expression-order sibling stays on the same honest covering
    // family.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_264_u128, "sam", 10),
            (9_265, "Alex", 20),
            (9_266, "bob", 30),
        ],
    );

    // Phase 2: require the session-backed query-builder explain to surface the
    // covering route and the planner-proven existing-row mode.
    let descriptor = session
        .query_from_sql::<ExpressionIndexedSessionSqlEntity>(
            "SELECT id FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        )
        .expect("descending expression key-only order SQL query should lower")
        .explain_execution()
        .expect("descending expression key-only order SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending expression key-only order queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending expression key-only order queries should expose the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "descending expression key-only order explain roots should expose the covering-read route label",
    );
    let projection_node = explain_execution_find_first_node(
        &descriptor,
        ExplainExecutionNodeType::CoveringRead,
    )
    .expect("descending expression key-only order explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![Value::Text("id".to_string())])),
        "descending expression key-only order covering nodes should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "descending session-backed expression key-only order explain should inherit the planner-proven covering mode",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending expression key-only order explain roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_expression_key_only_strict_text_range_query_uses_covering_read_route()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so the bounded
    // expression-key sibling can prove true covering eligibility without
    // claiming original `name` reconstruction from the lowered key.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_267_u128, "sam", 10),
            (9_268, "Alex", 20),
            (9_269, "amy", 30),
            (9_270, "bob", 40),
        ],
    );

    // Phase 2: require the session-backed query-builder explain to reuse the
    // planner-proven covering-read route for the bounded `id`-only projection.
    let descriptor = session
        .query_from_sql::<ExpressionIndexedSessionSqlEntity>(
            "SELECT id FROM ExpressionIndexedSessionSqlEntity WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect("expression key-only strict text-range SQL query should lower")
        .explain_execution()
        .expect("expression key-only strict text-range SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "expression key-only strict text-range queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "expression key-only strict text-range queries should expose the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "expression key-only strict text-range explain roots should expose the covering-read route label",
    );
    let projection_node = explain_execution_find_first_node(
        &descriptor,
        ExplainExecutionNodeType::CoveringRead,
    )
    .expect("expression key-only strict text-range explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![Value::Text("id".to_string())])),
        "expression key-only strict text-range covering nodes should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "session-backed expression key-only strict text-range explain should inherit the planner-proven covering mode",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "expression key-only strict text-range explain roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_expression_key_only_strict_text_range_desc_query_uses_covering_read_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so the descending
    // bounded expression-key sibling stays on the same honest covering family.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_271_u128, "sam", 10),
            (9_272, "Alex", 20),
            (9_273, "amy", 30),
            (9_274, "bob", 40),
        ],
    );

    // Phase 2: require the session-backed query-builder explain to surface the
    // covering route and the planner-proven existing-row mode.
    let descriptor = session
        .query_from_sql::<ExpressionIndexedSessionSqlEntity>(
            "SELECT id FROM ExpressionIndexedSessionSqlEntity WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        )
        .expect("descending expression key-only strict text-range SQL query should lower")
        .explain_execution()
        .expect(
            "descending expression key-only strict text-range SQL explain_execution should succeed",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending expression key-only strict text-range queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending expression key-only strict text-range queries should expose the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "descending expression key-only strict text-range explain roots should expose the covering-read route label",
    );
    let projection_node = explain_execution_find_first_node(
        &descriptor,
        ExplainExecutionNodeType::CoveringRead,
    )
    .expect(
        "descending expression key-only strict text-range explain tree should emit a covering-read node",
    );
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![Value::Text("id".to_string())])),
        "descending expression key-only strict text-range covering nodes should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "descending session-backed expression key-only strict text-range explain should inherit the planner-proven covering mode",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending expression key-only strict text-range explain roots should report access-satisfied ordering",
    );
}

#[test]
fn session_sql_expression_order_without_matching_index_stays_fail_closed() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>(
            "SELECT id, name FROM SessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect_err("expression order without one matching index should fail closed");

    assert!(
        err.to_string()
            .contains("expression ORDER BY requires a matching index-backed access order"),
        "expression-order failures should preserve the explicit fail-closed policy message",
    );
}
