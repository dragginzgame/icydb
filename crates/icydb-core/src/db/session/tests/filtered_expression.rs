use super::*;

#[test]
fn execute_sql_projection_filtered_expression_order_only_covering_query_returns_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered dataset where the active rows
    // include one mixed-case `handle` value so `ORDER BY LOWER(handle)` has one
    // real expression-ordering contract to preserve.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231, "alpha", false, "gold", "bramble", 10),
            (9_232, "bravo-user", true, "gold", "bravo", 20),
            (9_233, "bristle-user", true, "gold", "bristle", 30),
            (9_234, "brisk-user", true, "silver", "Brisk", 40),
            (9_235, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the projection lane to keep the guarded active-only
    // window on the filtered `LOWER(handle)` route.
    let sql = "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect("filtered expression order-only projection query should execute");
    let entity_rows = session
        .execute_sql::<FilteredIndexedSessionSqlEntity>(sql)
        .expect("filtered expression order-only entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().handle.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let expected_rows = vec![
        vec![
            Value::Ulid(Ulid::from_u128(9_232)),
            Value::Text("bravo".to_string()),
        ],
        vec![
            Value::Ulid(Ulid::from_u128(9_234)),
            Value::Text("Brisk".to_string()),
        ],
    ];

    assert_eq!(
        entity_projected_rows, expected_rows,
        "guarded filtered expression order-only entity queries should preserve the canonical LOWER(handle) window",
    );
    assert_eq!(
        projected_rows, expected_rows,
        "guarded filtered expression order-only projection queries should preserve the canonical LOWER(handle) window",
    );
}

#[test]
fn execute_sql_projection_filtered_expression_order_only_desc_covering_query_returns_guarded_rows()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the same mixed-case filtered dataset so reverse
    // `LOWER(handle)` traversal keeps the same guarded route.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231, "alpha", false, "gold", "bramble", 10),
            (9_232, "bravo-user", true, "gold", "bravo", 20),
            (9_233, "bristle-user", true, "gold", "bristle", 30),
            (9_234, "brisk-user", true, "silver", "Brisk", 40),
            (9_235, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require reverse ordered projection rows from the same guarded
    // filtered expression window.
    let sql = "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect("descending filtered expression order-only projection query should execute");
    let entity_rows = session
        .execute_sql::<FilteredIndexedSessionSqlEntity>(sql)
        .expect("descending filtered expression order-only entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().handle.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let expected_rows = vec![
        vec![
            Value::Ulid(Ulid::from_u128(9_235)),
            Value::Text("charlie".to_string()),
        ],
        vec![
            Value::Ulid(Ulid::from_u128(9_233)),
            Value::Text("bristle".to_string()),
        ],
    ];

    assert_eq!(
        entity_projected_rows, expected_rows,
        "descending guarded filtered expression order-only entity queries should preserve the reverse LOWER(handle) window",
    );
    assert_eq!(
        projected_rows, expected_rows,
        "descending guarded filtered expression order-only projection queries should preserve the reverse LOWER(handle) window",
    );
}

#[test]
fn execute_sql_projection_filtered_expression_equivalent_prefix_forms_match_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one mixed-case filtered dataset so the casefold
    // `STARTS_WITH(LOWER(handle), ...)` spellings share one real guarded route.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231, "alpha", false, "gold", "bramble", 10),
            (9_232, "bravo-user", true, "gold", "bravo", 20),
            (9_233, "bristle-user", true, "gold", "bristle", 30),
            (9_234, "brisk-user", true, "silver", "Brisk", 40),
            (9_235, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the accepted filtered expression prefix spellings to
    // keep one guarded projection result set.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
    )
    .expect("filtered expression LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
    )
    .expect("filtered expression STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
    )
    .expect("filtered expression text-range projection should execute");
    let entity_rows = session
        .execute_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered expression LIKE prefix entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().handle.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let expected_rows = vec![
        vec![
            Value::Ulid(Ulid::from_u128(9_232)),
            Value::Text("bravo".to_string()),
        ],
        vec![
            Value::Ulid(Ulid::from_u128(9_234)),
            Value::Text("Brisk".to_string()),
        ],
    ];

    assert_eq!(
        starts_with_rows, like_rows,
        "guarded filtered expression STARTS_WITH and LIKE prefix projections should stay in parity",
    );
    assert_eq!(
        range_rows, like_rows,
        "guarded filtered expression text-range and LIKE prefix projections should stay in parity",
    );
    assert_eq!(
        entity_projected_rows, expected_rows,
        "guarded filtered expression prefix entity queries should preserve the canonical LOWER(handle) window",
    );
    assert_eq!(
        like_rows, expected_rows,
        "guarded filtered expression prefix projection queries should preserve the canonical LOWER(handle) window",
    );
}

#[test]
fn execute_sql_projection_filtered_expression_equivalent_desc_prefix_forms_match_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the same mixed-case filtered dataset so reverse casefold
    // prefix traversal stays on the same guarded route.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231, "alpha", false, "gold", "bramble", 10),
            (9_232, "bravo-user", true, "gold", "bravo", 20),
            (9_233, "bristle-user", true, "gold", "bristle", 30),
            (9_234, "brisk-user", true, "silver", "Brisk", 40),
            (9_235, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the accepted descending filtered expression prefix
    // spellings to keep one reverse guarded projection result set.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered expression LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered expression STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered expression text-range projection should execute");
    let entity_rows = session
        .execute_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect("descending filtered expression LIKE prefix entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().handle.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let expected_rows = vec![
        vec![
            Value::Ulid(Ulid::from_u128(9_233)),
            Value::Text("bristle".to_string()),
        ],
        vec![
            Value::Ulid(Ulid::from_u128(9_234)),
            Value::Text("Brisk".to_string()),
        ],
    ];

    assert_eq!(
        starts_with_rows, like_rows,
        "descending guarded filtered expression STARTS_WITH and LIKE prefix projections should stay in parity",
    );
    assert_eq!(
        range_rows, like_rows,
        "descending guarded filtered expression text-range and LIKE prefix projections should stay in parity",
    );
    assert_eq!(
        entity_projected_rows, expected_rows,
        "descending guarded filtered expression prefix entity queries should preserve the reverse LOWER(handle) window",
    );
    assert_eq!(
        like_rows, expected_rows,
        "descending guarded filtered expression prefix projection queries should preserve the reverse LOWER(handle) window",
    );
}

#[test]
fn execute_sql_filtered_expression_index_range_scan_preserves_lower_handle_order() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered dataset whose canonical
    // `LOWER(handle), id` order differs from primary-key order.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: lower the shared filtered expression-order SQL shape and
    // inspect the raw index-range scan order directly.
    let (entries_in_range_keys, scanned_ids) =
        inspect_filtered_expression_order_only_raw_scan(&session);

    assert_eq!(
        entries_in_range_keys,
        vec![
            (
                StorageKey::Ulid(Ulid::from_u128(9_232)),
                vec![StorageKey::Ulid(Ulid::from_u128(9_232))]
            ),
            (
                StorageKey::Ulid(Ulid::from_u128(9_234)),
                vec![StorageKey::Ulid(Ulid::from_u128(9_234))]
            ),
            (
                StorageKey::Ulid(Ulid::from_u128(9_233)),
                vec![StorageKey::Ulid(Ulid::from_u128(9_233))]
            ),
            (
                StorageKey::Ulid(Ulid::from_u128(9_235)),
                vec![StorageKey::Ulid(Ulid::from_u128(9_235))]
            ),
        ],
        "filtered expression raw bounds must isolate the expression index instead of bleeding into sibling filtered indexes",
    );
    assert_eq!(
        scanned_ids,
        vec![
            Ulid::from_u128(9_232),
            Ulid::from_u128(9_234),
            Ulid::from_u128(9_233),
            Ulid::from_u128(9_235),
        ],
        "raw filtered expression index-range scans must preserve canonical LOWER(handle), id order before later pagination/windowing",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_expression_covering_query_uses_index_range_access()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one mixed-case filtered dataset so EXPLAIN EXECUTION can
    // prove the guarded `LOWER(handle)` order-only route.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231, "alpha", false, "gold", "bramble", 10),
            (9_232, "bravo-user", true, "gold", "bravo", 20),
            (9_233, "bristle-user", true, "gold", "bristle", 30),
            (9_234, "brisk-user", true, "silver", "Brisk", 40),
            (9_235, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the guarded filtered expression order-only SQL lane to
    // surface the shared index-range covering route.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered expression order-only SQL query should lower")
        .explain_execution()
        .expect("filtered expression order-only SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "guarded filtered expression-order queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("materialized".to_string())),
        "guarded filtered expression-order explain roots should expose the materialized route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "guarded filtered expression-order roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered expression-order roots should report access-satisfied LOWER(handle) ordering",
    );
}

#[test]
fn session_explain_execution_filtered_expression_prefix_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one mixed-case filtered dataset so EXPLAIN EXECUTION can
    // prove the guarded casefold prefix route stays on the same expression index.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231, "alpha", false, "gold", "bramble", 10),
            (9_232, "bravo-user", true, "gold", "bravo", 20),
            (9_233, "bristle-user", true, "gold", "bristle", 30),
            (9_234, "brisk-user", true, "silver", "Brisk", 40),
            (9_235, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the guarded filtered expression prefix SQL lane to
    // surface the shared index-range covering route and access-satisfied order.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered expression prefix SQL query should lower")
        .explain_execution()
        .expect("filtered expression prefix SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "guarded filtered expression-prefix queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("materialized".to_string())),
        "guarded filtered expression-prefix explain roots should expose the materialized route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "guarded filtered expression-prefix roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered expression-prefix roots should report access-satisfied LOWER(handle) ordering",
    );
}

#[test]
fn session_explain_execution_filtered_expression_text_range_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the guarded mixed-case dataset so EXPLAIN EXECUTION can
    // prove explicit casefold bounds stay on the same expression index route.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: require the guarded filtered expression text-range lane to stay
    // on the shared index-range root with access-satisfied ordering.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered expression text-range SQL query should lower")
        .explain_execution()
        .expect("filtered expression text-range SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "guarded filtered expression text-range queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("materialized".to_string())),
        "guarded filtered expression text-range explain roots should expose the materialized route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "guarded filtered expression text-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered expression text-range roots should report access-satisfied LOWER(handle) ordering",
    );
}
