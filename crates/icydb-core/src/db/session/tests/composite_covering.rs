use super::*;

#[test]
fn session_explain_execution_order_only_composite_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset so the SQL lane
    // can prove planner-selected order-only access on the live `(code, serial)` index.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221_u128, "alpha", 2),
            (9_222, "alpha", 1),
            (9_223, "beta", 1),
        ],
    );

    // Phase 2: require EXPLAIN EXECUTION to surface the shared order-only
    // composite index-range root and covering-read route.
    let descriptor = session
        .query_from_sql::<CompositeIndexedSessionSqlEntity>(
            "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 2",
        )
        .expect("composite order-only covering SQL query should lower")
        .explain_execution()
        .expect("composite order-only covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "order-only composite secondary queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "order-only composite coverable projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "order-only composite explain roots should expose the covering-read route label",
    );
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect("order-only composite explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "session-backed order-only composite covering nodes should inherit the planner-proven covering mode",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "order-only composite index-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "order-only composite index-range roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_order_only_composite_desc_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset so the SQL lane
    // can prove planner-selected descending order-only access on the live
    // `(code, serial)` index instead of materializing a reverse sort.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231_u128, "alpha", 2),
            (9_232, "alpha", 1),
            (9_233, "beta", 1),
        ],
    );

    // Phase 2: require EXPLAIN EXECUTION to surface the shared descending
    // order-only composite index-range root and covering-read route.
    let descriptor = session
        .query_from_sql::<CompositeIndexedSessionSqlEntity>(
            "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code DESC, serial DESC, id DESC LIMIT 2",
        )
        .expect("descending composite order-only covering SQL query should lower")
        .explain_execution()
        .expect("descending composite order-only covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending order-only composite secondary queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending order-only composite coverable projections should keep the explicit covering-read route",
    );
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect(
                "descending order-only composite explain tree should emit a covering-read node",
            );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "descending session-backed order-only composite covering nodes should inherit the planner-proven covering mode",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "descending order-only composite explain roots should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending order-only composite index-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending order-only composite index-range roots should report access-satisfied ordering",
    );
}

#[test]
fn execute_sql_projection_index_coverable_multi_component_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset so the SQL
    // projection lane must decode both indexed components from one secondary
    // `(code, serial)` access path.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_201_u128, "alpha", 2),
            (9_202, "alpha", 1),
            (9_203, "beta", 1),
        ],
    );

    // Phase 2: verify the projection lane returns the same `(id, code,
    // serial)` rows as the entity lane for a direct composite covering query.
    let sql = "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 2";
    let projected_rows =
        dispatch_projection_rows::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("multi-component covering projection query should execute");
    let entity_rows = session
        .execute_sql::<CompositeIndexedSessionSqlEntity>(sql)
        .expect("multi-component covering entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.entity_ref().id),
                Value::Text(row.entity_ref().code.clone()),
                Value::Uint(row.entity_ref().serial),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
}
