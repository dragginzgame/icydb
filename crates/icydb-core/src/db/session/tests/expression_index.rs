use super::*;

// Seed one deterministic mixed-case dataset for expression-index routing
// checks where `LOWER(name)` order differs from primary-key order.
fn seed_expression_order_fixture(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &str, u64)],
) {
    seed_expression_indexed_session_sql_entities(session, rows);
}

// Assert the shared non-covering order-only expression route contract.
fn assert_expression_order_index_range_descriptor(
    descriptor: &ExplainExecutionNodeDescriptor,
    context: &str,
) {
    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "{context} should stay on the shared index-range root",
    );
    assert!(
        explain_execution_find_first_node(
            descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "{context} should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "{context} should report access-satisfied ordering",
    );
}

// Assert the shared covering-read route contract for key-only expression-index
// routes, parameterized only by the specific query spelling under test.
fn assert_expression_covering_read_descriptor(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    context: &str,
) {
    let descriptor = session
        .query_from_sql::<ExpressionIndexedSessionSqlEntity>(sql)
        .expect("expression covering SQL query should lower")
        .explain_execution()
        .expect("expression covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "{context} should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "{context} should expose the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "{context} should expose the covering-read route label",
    );
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect("expression covering explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![Value::Text("id".to_string())])),
        "{context} should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "{context} should inherit the planner-proven covering mode",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "{context} should report access-satisfied ordering",
    );
}

const fn expression_covering_read_queries(desc: bool) -> [(&'static str, &'static str); 2] {
    if desc {
        [
            (
                "descending expression key-only order queries",
                "SELECT id FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            ),
            (
                "descending expression key-only strict text-range queries",
                "SELECT id FROM ExpressionIndexedSessionSqlEntity WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            ),
        ]
    } else {
        [
            (
                "expression key-only order queries",
                "SELECT id FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            ),
            (
                "expression key-only strict text-range queries",
                "SELECT id FROM ExpressionIndexedSessionSqlEntity WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            ),
        ]
    }
}

fn assert_expression_covering_read_route(session: &DbSession<SessionSqlCanister>, desc: bool) {
    for (context, sql) in expression_covering_read_queries(desc) {
        assert_expression_covering_read_descriptor(session, sql, context);
    }
}

#[test]
fn execute_sql_projection_expression_order_query_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so expression order
    // semantics disagree with primary-key order instead of accidentally
    // matching one tie-break-only fallback.
    seed_expression_order_fixture(
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
    seed_expression_order_fixture(
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
    seed_expression_order_fixture(
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
    seed_expression_order_fixture(
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

    assert_expression_order_index_range_descriptor(&descriptor, "expression order-only queries");
}

#[test]
fn session_explain_execution_expression_key_only_covering_routes_stay_on_covering_family() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so both the
    // order-only and bounded text-range key-only siblings can prove covering
    // eligibility without claiming original `name` reconstruction.
    seed_expression_order_fixture(
        &session,
        &[
            (9_261_u128, "sam", 10),
            (9_262, "Alex", 20),
            (9_263, "amy", 30),
            (9_264, "bob", 40),
        ],
    );

    // Phase 2: require the ascending key-only expression routes to stay on
    // the same planner-proven covering-read family.
    assert_expression_covering_read_route(&session, false);
}

#[test]
fn session_explain_execution_expression_key_only_desc_covering_routes_stay_on_covering_family() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so the descending
    // order-only and bounded text-range key-only siblings stay on the same
    // honest covering family.
    seed_expression_order_fixture(
        &session,
        &[
            (9_265_u128, "sam", 10),
            (9_266, "Alex", 20),
            (9_267, "amy", 30),
            (9_268, "bob", 40),
        ],
    );

    // Phase 2: require the descending key-only expression routes to surface
    // the covering route and planner-proven existing-row mode consistently.
    assert_expression_covering_read_route(&session, true);
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
