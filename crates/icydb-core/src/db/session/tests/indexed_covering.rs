use super::*;

// Seed the canonical ordered secondary-index fixture for direct covering
// `ORDER BY name` tests.
fn seed_indexed_covering_order_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_indexed_session_sql_entities(
        session,
        &[("carol", 10), ("alice", 20), ("bob", 30), ("dora", 40)],
    );
}

// Assert the shared covering index-range contract for basic order-only
// covering routes.
fn assert_indexed_covering_descriptor(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    context: &str,
) {
    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(sql)
        .expect("order-only covering SQL query should lower")
        .explain_execution()
        .expect("order-only covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "{context} should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "{context} should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "{context} should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "{context} should report secondary order pushdown",
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

#[test]
fn execute_sql_projection_index_coverable_primary_key_and_prefix_field_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic equality-prefix dataset on the indexed
    // `name` field so the projection lane can stay on the same query shape as
    // the hot canister attribution benchmark.
    seed_indexed_session_sql_entities(
        &session,
        &[("alice", 10), ("alice", 20), ("bob", 30), ("carol", 40)],
    );

    // Phase 2: verify the projection lane returns the same `(id, name)` row
    // as the entity lane for an index-covered equality-prefix query.
    let sql =
        "SELECT id, name FROM IndexedSessionSqlEntity WHERE name = 'alice' ORDER BY id LIMIT 1";
    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(&session, sql)
        .expect("index-covered projection query should execute");
    let entity_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(sql)
        .expect("index-covered entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.entity_ref().id),
                Value::Text(row.entity_ref().name.clone()),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
}

#[test]
fn execute_sql_projection_index_coverable_secondary_order_field_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic ordered secondary-index dataset on the
    // indexed `name` field so the projection lane can stay on the same
    // coverable order-by-name shape tracked by PocketIC attribution.
    seed_indexed_covering_order_fixture(&session);

    // Phase 2: verify the projection lane returns the same ordered `name`
    // row as the entity lane for a direct secondary-index covering query.
    let sql = "SELECT name FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 1";
    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(&session, sql)
        .expect("secondary-order covering projection query should execute");
    let entity_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(sql)
        .expect("secondary-order covering entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| vec![Value::Text(row.entity_ref().name.clone())])
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
}

#[test]
fn execute_sql_projection_index_coverable_secondary_order_field_with_offset_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic ordered secondary-index dataset so the
    // covering projection lane can validate post-filter pagination against the
    // entity lane on the same index-ordered shape.
    seed_indexed_covering_order_fixture(&session);

    // Phase 2: verify the projection lane preserves the same ordered page
    // window as the entity lane for a direct secondary-index covering query.
    let sql = "SELECT name FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 2 OFFSET 1";
    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(&session, sql)
        .expect("secondary-order covering projection page query should execute");
    let entity_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(sql)
        .expect("secondary-order covering entity page query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| vec![Value::Text(row.entity_ref().name.clone())])
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
}

#[test]
fn session_explain_execution_order_only_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic secondary-order dataset so the SQL lane
    // can prove planner-selected order-only index access instead of a
    // materialized full scan that merely returns the same first row.
    seed_indexed_covering_order_fixture(&session);

    // Phase 2: require EXPLAIN EXECUTION to surface the shared planner/runtime
    // order-only index-range path for one coverable `ORDER BY name, id` query.
    assert_indexed_covering_descriptor(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 1",
        "order-only single-field secondary queries",
    );
}

#[test]
fn execute_sql_projection_order_only_filtered_covering_query_returns_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset where the
    // lexicographically earliest row is inactive so the guarded query can only
    // stay correct if it respects the filtered-index predicate.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "charlie", true, 30),
            (9_204, "delta", false, 40),
        ],
    );

    // Phase 2: require the projection lane to return only the guarded active
    // subset under the order-only `ORDER BY name, id` shape.
    let sql = "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY name ASC, id ASC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect("filtered order-only covering projection query should execute");

    assert_eq!(
        projected_rows,
        vec![
            vec![Value::Text("bravo".to_string())],
            vec![Value::Text("charlie".to_string())],
        ],
        "guarded order-only covering queries should return only rows admitted by the filtered index predicate",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset so EXPLAIN
    // EXECUTION can prove the guarded query uses the filtered secondary index
    // instead of one materialized full scan.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "charlie", true, 30),
            (9_204, "delta", false, 40),
        ],
    );

    // Phase 2: require the guarded order-only SQL lane to surface the shared
    // planner/runtime index-range covering route.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("filtered order-only covering SQL query should lower")
        .explain_execution()
        .expect("filtered order-only covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "guarded filtered-order queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "guarded filtered-order coverable projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "guarded filtered-order explain roots should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "guarded filtered-order index-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered-order index-range roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_desc_residual_query_fails_closed_before_top_n() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite dataset where the
    // descending `handle` order uses the `tier, handle` index, but the extra
    // `age >= 20` predicate must remain residual.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "amber", false, "gold", "bramble", 10),
            (9_222, "bravo", true, "gold", "bravo", 20),
            (9_223, "charlie", true, "gold", "bristle", 30),
            (9_224, "delta", false, "silver", "brisk", 40),
            (9_225, "echo", true, "silver", "Brisk", 50),
        ],
    );

    // Phase 2: require the residual descending filtered composite order-only
    // shape to keep the secondary-prefix route while failing closed before
    // Top-N derivation.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND age >= 20 ORDER BY handle DESC, id DESC LIMIT 2",
        )
        .expect("descending filtered composite residual order-only SQL query should lower")
        .explain_execution()
        .expect(
            "descending filtered composite residual order-only SQL explain_execution should succeed",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "descending filtered composite residual order-only queries should keep the filtered index-prefix root",
    );
    assert_eq!(
        descriptor.execution_mode(),
        crate::db::ExplainExecutionMode::Materialized,
        "descending filtered composite residual order-only queries should stay materialized",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(false),
        "descending filtered composite residual order-only projections should materialize rows because the residual filter needs non-index fields",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "descending filtered composite residual order-only roots should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "descending filtered composite residual order-only roots should expose the concrete equality-prefix value",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::ResidualPredicateFilter
        )
        .is_some(),
        "descending filtered composite residual order-only roots should expose the residual filter stage",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending filtered composite residual order-only roots should still report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_some(),
        "descending filtered composite residual order-only roots should fail closed to a materialized sort",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "descending filtered composite residual order-only roots must not derive Top-N seek",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_none(),
        "descending filtered composite residual order-only roots must not report access-satisfied ordering after failing closed",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_query_without_guard_falls_back_to_full_scan() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset so the unguarded
    // order-only query would be observably wrong if it silently reused the
    // filtered secondary index without proving the guard predicate.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "charlie", true, 30),
            (9_204, "delta", false, 40),
        ],
    );

    // Phase 2: require the unguarded `ORDER BY name, id` query to stay on the
    // fail-closed full-scan path instead of silently borrowing the filtered
    // index order.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT name FROM FilteredIndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("unguarded filtered-order SQL query should lower")
        .explain_execution()
        .expect("unguarded filtered-order SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::FullScan,
        "unguarded filtered-order queries must fail closed to the full-scan root",
    );
    assert_ne!(
        descriptor.covering_scan(),
        Some(true),
        "unguarded filtered-order queries must not claim the covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("materialized".to_string())),
        "unguarded filtered-order explains must surface the explicit materialized fallback route",
    );
}
