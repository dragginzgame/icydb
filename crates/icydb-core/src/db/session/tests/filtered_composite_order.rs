use super::*;

#[test]
fn execute_sql_projection_filtered_composite_order_only_covering_query_returns_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset where
    // the guarded `tier = 'gold'` equality prefix should expose one ordered
    // `handle` suffix window without needing an extra bounded text predicate.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the projection lane to return only the guarded
    // equality-prefix subset under the `ORDER BY handle, id` suffix shape.
    let sql = "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect("filtered composite order-only covering projection query should execute");

    assert_eq!(
        projected_rows,
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bravo".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bristle".to_string()),
            ],
        ],
        "guarded filtered composite order-only queries should return only rows admitted by the equality-prefix filtered window",
    );
}

#[test]
fn execute_sql_projection_filtered_composite_order_only_desc_covering_query_returns_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset where
    // reverse traversal still depends on the same guarded `tier = 'gold'`
    // equality prefix before ordering by the `handle` suffix.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require reverse ordered projection rows from the same guarded
    // equality-prefix composite window.
    let sql = "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect(
            "descending filtered composite order-only covering projection query should execute",
        );

    assert_eq!(
        projected_rows,
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("charlie".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bristle".to_string()),
            ],
        ],
        "descending guarded filtered composite order-only queries should return the reverse equality-prefix window",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_composite_covering_query_uses_index_prefix_access()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset so
    // EXPLAIN EXECUTION can prove the guarded equality-prefix query uses the
    // composite filtered secondary index instead of materializing rows.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );
    // Phase 2: require the guarded composite order-only SQL lane to surface
    // the shared index-prefix covering route with access-satisfied suffix ordering.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite order-only covering SQL query should lower")
        .explain_execution()
        .expect("filtered composite order-only covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "guarded filtered composite-order queries should stay on the shared index-prefix root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "guarded filtered composite-order coverable projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "guarded filtered composite-order explain roots should expose the covering-read route label",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "guarded filtered composite-order explain roots should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "guarded filtered composite-order explain roots should expose the concrete equality-prefix value",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "guarded filtered composite-order roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered composite-order roots should report access-satisfied suffix ordering",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_composite_desc_covering_query_uses_index_prefix_access()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset so
    // reverse EXPLAIN EXECUTION can prove the same guarded equality-prefix
    // route instead of a materialized reverse sort.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the descending guarded composite order-only SQL lane
    // to surface the shared index-prefix covering route.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
        )
        .expect("descending filtered composite order-only covering SQL query should lower")
        .explain_execution()
        .expect("descending filtered composite order-only covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "descending guarded filtered composite-order queries should stay on the shared index-prefix root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending guarded filtered composite-order coverable projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "descending guarded filtered composite-order explain roots should expose the covering-read route label",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "descending guarded filtered composite-order explain roots should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "descending guarded filtered composite-order explain roots should expose the concrete equality-prefix value",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending guarded filtered composite-order roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_none(),
        "descending guarded filtered composite-order roots should fail closed on reverse suffix streaming safety",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_composite_desc_offset_query_stays_on_materialized_boundary()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset where
    // the `tier = 'gold'` equality prefix keeps the suffix order meaningful
    // even though reverse offset paging must still stay on the shared
    // materialized boundary.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the descending offset composite filtered order-only
    // shape to keep the index-prefix route but stop before Top-N derivation.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
        )
        .expect("descending filtered composite order-only offset SQL query should lower")
        .explain_execution()
        .expect(
            "descending filtered composite order-only offset SQL explain_execution should succeed",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "descending filtered composite order-only offset queries should keep the shared index-prefix root",
    );
    assert_eq!(
        descriptor.execution_mode(),
        crate::db::ExplainExecutionMode::Materialized,
        "descending filtered composite order-only offset queries should stay on the materialized boundary",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending filtered composite order-only offset projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "descending filtered composite order-only offset roots should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "descending filtered composite order-only offset roots should expose the concrete equality-prefix value",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending filtered composite order-only offset roots should still report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_some(),
        "descending filtered composite order-only offset roots should stay on the materialized boundary sort contract",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "descending filtered composite order-only offset roots must not derive Top-N seek",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_none(),
        "descending filtered composite order-only offset roots must not report direct access-satisfied ordering",
    );
    let limit_node = explain_execution_find_first_node(
        &descriptor,
        ExplainExecutionNodeType::LimitOffset,
    )
    .expect("descending filtered composite order-only offset roots should expose one limit node");
    assert_eq!(
        limit_node.node_properties().get("offset"),
        Some(&Value::Uint(1)),
        "descending filtered composite order-only offset roots should expose the retained offset window",
    );
}
