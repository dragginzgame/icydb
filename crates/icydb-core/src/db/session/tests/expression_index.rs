use super::*;
use crate::db::session::sql::with_sql_projection_materialization_metrics;

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
    let descriptor =
        lower_select_query_for_tests::<ExpressionIndexedSessionSqlEntity>(&session, sql)
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
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect("expression covering explain tree should emit a covering-read node");
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
fn execute_sql_projection_expression_order_matrix_matches_entity_rows() {
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

    // Phase 2: verify both ascending and descending expression order keep the
    // same `LOWER(name), id` ordering contract on the projection and entity lanes.
    for (sql, expected_rows, context) in [
        (
            "SELECT id, name FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            vec![
                vec![
                    Value::Ulid(Ulid::from_u128(9_244)),
                    Value::Text("Alex".to_string()),
                ],
                vec![
                    Value::Ulid(Ulid::from_u128(9_241)),
                    Value::Text("bob".to_string()),
                ],
            ],
            "ascending expression order",
        ),
        (
            "SELECT id, name FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            vec![
                vec![
                    Value::Ulid(Ulid::from_u128(9_242)),
                    Value::Text("zoe".to_string()),
                ],
                vec![
                    Value::Ulid(Ulid::from_u128(9_243)),
                    Value::Text("sam".to_string()),
                ],
            ],
            "descending expression order",
        ),
    ] {
        let projected_rows =
            statement_projection_rows::<ExpressionIndexedSessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| panic!("{context} projection query should execute: {err:?}"));
        let entity_rows =
            execute_scalar_select_for_tests::<ExpressionIndexedSessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| panic!("{context} entity query should execute: {err:?}"));
        let entity_projected_rows = entity_rows
            .iter()
            .map(|row| {
                vec![
                    Value::Ulid(row.id().key()),
                    Value::Text(row.entity_ref().name.clone()),
                ]
            })
            .collect::<Vec<_>>();

        assert_eq!(
            entity_projected_rows, expected_rows,
            "{context} entity execution must honor the LOWER(name), id ordering contract",
        );
        assert_eq!(
            projected_rows, expected_rows,
            "{context} projection execution must honor the LOWER(name), id ordering contract",
        );
    }
}

#[test]
fn execute_sql_projection_expression_order_pk_plus_row_field_uses_sparse_sql_path() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so the expression
    // index order disagrees with primary-key order and the SQL lane must rely
    // on the `LOWER(name), id` access path for its page window.
    seed_expression_order_fixture(
        &session,
        &[
            (9_253_u128, "sam", 10),
            (9_254, "Alex", 20),
            (9_251, "bob", 30),
            (9_252, "zoe", 40),
        ],
    );

    // Phase 2: prove the SQL projection lane now admits the sparse
    // index-backed projection path even when it returns only the primary key
    // plus one uncovered row-backed field.
    let sql = "SELECT id, age FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2";
    let (projected_rows, metrics) = with_sql_projection_materialization_metrics(|| {
        statement_projection_rows::<ExpressionIndexedSessionSqlEntity>(&session, sql)
            .expect("expression-order pk-plus-row projection query should execute")
    });
    let entity_rows =
        execute_scalar_select_for_tests::<ExpressionIndexedSessionSqlEntity>(&session, sql)
            .expect("expression-order pk-plus-row entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Uint(row.entity_ref().age),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(projected_rows, entity_projected_rows);
    assert_eq!(
        metrics.hybrid_covering_path_hits, 1,
        "expression-order pk-plus-row projection should use the sparse SQL-side index-backed path",
    );
    assert_eq!(
        metrics.hybrid_covering_index_field_accesses, 0,
        "expression-order pk-plus-row projection should not materialize projected index component values",
    );
    assert_eq!(
        metrics.hybrid_covering_row_field_accesses, 2,
        "expression-order pk-plus-row projection should sparse-read one uncovered field per emitted row",
    );
    assert_eq!(
        metrics.data_rows_path_hits, 0,
        "expression-order pk-plus-row projection should bypass the generic data-row path",
    );
    assert_eq!(
        metrics.slot_rows_path_hits, 0,
        "expression-order pk-plus-row projection should bypass retained slot rows",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn execute_sql_projection_expression_order_key_only_covering_query_avoids_store_gets() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so the `LOWER(name),
    // id` index order stays observable instead of collapsing onto primary-key
    // order by accident.
    seed_expression_order_fixture(
        &session,
        &[
            (9_263_u128, "sam", 10),
            (9_264, "Alex", 20),
            (9_261, "bob", 30),
            (9_262, "zoe", 40),
        ],
    );

    // Phase 2: require the SQL projection lane to keep the planner-proven
    // covering read route fully row-store-free for one key-only expression
    // order query.
    let (_result, attribution) = session
        .execute_sql_query_with_attribution::<ExpressionIndexedSessionSqlEntity>(
            "SELECT id FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect("expression-order key-only covering query should execute");

    assert_eq!(
        attribution.store_get_calls, 0,
        "expression-order key-only covering queries should avoid row-store get() calls",
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
    let plan = lower_select_query_for_tests::<ExpressionIndexedSessionSqlEntity>(&session,
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
    let descriptor = lower_select_query_for_tests::<ExpressionIndexedSessionSqlEntity>(&session,
            "SELECT id, name FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect("expression order-only SQL query should lower")
        .explain_execution()
        .expect("expression order-only SQL explain_execution should succeed");

    assert_expression_order_index_range_descriptor(&descriptor, "expression order-only queries");
}

#[test]
fn session_explain_execution_expression_key_only_covering_route_matrix_stays_on_covering_family() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so both ascending and
    // descending key-only siblings stay on the same honest covering family.
    seed_expression_order_fixture(
        &session,
        &[
            (9_261_u128, "sam", 10),
            (9_262, "Alex", 20),
            (9_263, "amy", 30),
            (9_264, "bob", 40),
        ],
    );

    // Phase 2: require the ascending and descending key-only expression
    // routes to surface the covering route consistently.
    for desc in [false, true] {
        assert_expression_covering_read_route(&session, desc);
    }
}

#[test]
fn session_sql_expression_order_without_matching_index_stays_fail_closed() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_scalar_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT id, name FROM SessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
    )
    .expect_err("expression order without one matching index should fail closed");

    assert!(
        err.to_string()
            .contains("expression ORDER BY requires a matching index-backed access order"),
        "expression-order failures should preserve the explicit fail-closed policy message",
    );
}
