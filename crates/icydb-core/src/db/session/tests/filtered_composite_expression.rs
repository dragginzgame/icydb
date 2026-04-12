use super::*;

// Seed the canonical guarded filtered-composite expression fixture used by the
// parity and EXPLAIN route checks in this file.
fn seed_filtered_composite_expression_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_filtered_expression_indexed_session_sql_entities(session);
}

// Run the three admitted bounded casefold spellings for one guarded composite
// expression prefix shape and return their projected rows.
fn filtered_composite_expression_prefix_spellings(
    session: &DbSession<SessionSqlCanister>,
    descending: bool,
) -> (ProjectedRows, ProjectedRows, ProjectedRows) {
    let order = if descending {
        "DESC, id DESC"
    } else {
        "ASC, id ASC"
    };
    let like_sql = format!(
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity \
         WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' \
         ORDER BY LOWER(handle) {order} LIMIT 2"
    );
    let starts_with_sql = format!(
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity \
         WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') \
         ORDER BY LOWER(handle) {order} LIMIT 2"
    );
    let range_sql = format!(
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity \
         WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' \
         ORDER BY LOWER(handle) {order} LIMIT 2"
    );

    let like_rows =
        statement_projection_rows::<FilteredIndexedSessionSqlEntity>(session, &like_sql)
            .expect("filtered composite expression LIKE prefix projection should execute");
    let starts_with_rows =
        statement_projection_rows::<FilteredIndexedSessionSqlEntity>(session, &starts_with_sql)
            .expect("filtered composite expression STARTS_WITH projection should execute");
    let range_rows =
        statement_projection_rows::<FilteredIndexedSessionSqlEntity>(session, &range_sql)
            .expect("filtered composite expression text-range projection should execute");

    (like_rows, starts_with_rows, range_rows)
}

// Return the canonical guarded equality-prefix projection rows for the
// bounded `br*` suffix window in either direction.
fn filtered_composite_expression_prefix_expected_rows(descending: bool) -> ProjectedRows {
    if descending {
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bristle".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bravo".to_string()),
            ],
        ]
    } else {
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bravo".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bristle".to_string()),
            ],
        ]
    }
}

// Return the canonical guarded equality-prefix order-only rows for the full
// projection surface in either direction.
fn filtered_composite_expression_order_only_expected_rows(descending: bool) -> ProjectedRows {
    if descending {
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("charlie".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bristle".to_string()),
            ],
        ]
    } else {
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bravo".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bristle".to_string()),
            ],
        ]
    }
}

// Assert the shared materialized filtered-composite expression route contract
// for full projection shapes.
fn assert_filtered_composite_expression_materialized_descriptor(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_node: ExplainExecutionNodeType,
    context: &str,
) {
    let descriptor = session
        .lower_sql_query_for_tests::<FilteredIndexedSessionSqlEntity>(sql)
        .expect("filtered composite expression SQL query should lower")
        .explain_execution()
        .expect("filtered composite expression SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        expected_node,
        "{context} should stay on the shared routed root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(false),
        "{context} should materialize original handle values instead of claiming a covering route",
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
        "{context} should report access-satisfied LOWER(handle) ordering",
    );
}

// Assert the shared covering-read contract for the narrower filtered composite
// strict text-range projection.
fn assert_filtered_composite_expression_covering_descriptor(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    context: &str,
) {
    let descriptor = session
        .lower_sql_query_for_tests::<FilteredIndexedSessionSqlEntity>(sql)
        .expect("filtered composite expression covering SQL query should lower")
        .explain_execution()
        .expect("filtered composite expression covering SQL explain_execution should succeed");

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
            .expect(
                "filtered composite expression covering explain should emit a covering-read node",
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
        "{context} should report access-satisfied LOWER(handle) ordering",
    );
}

const fn filtered_composite_expression_materialized_queries()
-> [(&'static str, &'static str, ExplainExecutionNodeType); 3] {
    [
        (
            "guarded filtered composite expression order-only queries",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            ExplainExecutionNodeType::IndexPrefixScan,
        ),
        (
            "guarded filtered composite expression-prefix queries",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            ExplainExecutionNodeType::IndexRangeScan,
        ),
        (
            "guarded filtered composite expression text-range queries",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            ExplainExecutionNodeType::IndexRangeScan,
        ),
    ]
}

const fn filtered_composite_expression_covering_queries(
    desc: bool,
) -> [(&'static str, &'static str); 1] {
    if desc {
        [(
            "descending guarded filtered composite expression key-only strict text-range queries",
            "SELECT id, tier FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )]
    } else {
        [(
            "guarded filtered composite expression key-only strict text-range queries",
            "SELECT id, tier FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )]
    }
}

#[test]
fn execute_sql_projection_filtered_composite_expression_order_only_matrix_returns_guarded_rows() {
    let cases = [
        (
            "ascending filtered composite expression order-only projection query",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            false,
        ),
        (
            "descending filtered composite expression order-only projection query",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            true,
        ),
    ];

    for (context, sql, desc) in cases {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();

        // Phase 1: seed the canonical mixed-case filtered expression dataset so
        // the guarded `tier = 'gold'` window traverses one `LOWER(handle)` suffix.
        seed_filtered_composite_expression_fixture(&session);

        // Phase 2: require the projection lane to keep the guarded equality-prefix
        // window on the filtered composite `tier, LOWER(handle)` route.
        let projected_rows =
            statement_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| panic!("{context} should execute: {err}"));

        assert_eq!(
            projected_rows,
            filtered_composite_expression_order_only_expected_rows(desc),
            "{context} should preserve the guarded LOWER(handle) suffix window",
        );
    }
}

#[test]
fn execute_sql_projection_filtered_composite_expression_order_only_pagination_matches_entity_rows()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one larger mixed-case filtered dataset so the guarded
    // composite expression route has enough admitted rows to exercise several
    // LIMIT/OFFSET windows while still carrying inactive and wrong-tier noise.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_281, "amber-user", true, "gold", "Amber", 10),
            (9_282, "alpha-user", false, "gold", "alpha", 20),
            (9_283, "bravo-user", true, "gold", "bravo", 30),
            (9_284, "bravo-shadow", true, "silver", "Bravo", 40),
            (9_285, "charlie-user", true, "gold", "CHARLIE", 50),
            (9_286, "delta-user", true, "gold", "delta", 60),
            (9_287, "echo-user", true, "gold", "Echo", 70),
            (9_288, "foxtrot-user", false, "gold", "foxtrot", 80),
            (9_289, "golf-user", true, "gold", "golf", 90),
            (9_290, "hotel-user", true, "gold", "Hotel", 100),
            (9_291, "india-user", true, "gold", "india", 110),
        ],
    );

    // Phase 2: derive the canonical full ordered entity result so each paged
    // projection window can be checked against the same structural order.
    let base_sql = "SELECT id, tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC";
    let full_entity_rows = session
        .execute_scalar_sql_for_tests::<FilteredIndexedSessionSqlEntity>(base_sql)
        .expect("filtered composite expression baseline entity query should execute");
    let expected_projected_rows = full_entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().tier.clone()),
                Value::Text(row.entity_ref().handle.clone()),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(
        expected_projected_rows.len(),
        8,
        "the guarded composite expression fixture should admit eight ordered gold rows",
    );

    // Phase 3: compare several paged windows against both the entity lane and
    // the expected ordered prefix so pagination cannot silently skip or repeat rows.
    let mut concatenated_projection_pages = Vec::new();
    for (offset, limit) in [(0_u64, 3_u64), (3, 3), (6, 3)] {
        let paged_sql = format!("{base_sql} LIMIT {limit} OFFSET {offset}");
        let projected_rows =
            statement_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, &paged_sql)
                .expect("filtered composite expression paged projection query should execute");
        let paged_entity_rows = session
            .execute_scalar_sql_for_tests::<FilteredIndexedSessionSqlEntity>(&paged_sql)
            .expect("filtered composite expression paged entity query should execute");
        let expected_page = paged_entity_rows
            .iter()
            .map(|row| {
                vec![
                    Value::Ulid(row.id().key()),
                    Value::Text(row.entity_ref().tier.clone()),
                    Value::Text(row.entity_ref().handle.clone()),
                ]
            })
            .collect::<Vec<_>>();

        assert_eq!(
            projected_rows, expected_page,
            "paged composite expression projections must match the entity lane for LIMIT {limit} OFFSET {offset}",
        );

        concatenated_projection_pages.extend(projected_rows);
    }

    assert_eq!(
        concatenated_projection_pages, expected_projected_rows,
        "concatenated paged composite expression projections must preserve the canonical ordered guarded result set without missing or repeated rows",
    );
}

#[test]
fn execute_sql_projection_filtered_composite_expression_prefix_matrix_matches_guarded_rows() {
    for (context, desc) in [
        (
            "ascending filtered composite expression prefix projections",
            false,
        ),
        (
            "descending filtered composite expression prefix projections",
            true,
        ),
    ] {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();

        // Phase 1: seed the guarded mixed-case dataset so each direction stays
        // on the same equality-prefix route family.
        seed_filtered_composite_expression_fixture(&session);

        // Phase 2: require the admitted bounded casefold spellings to keep one
        // guarded equality-prefix projection result set in that direction.
        let (like_rows, starts_with_rows, range_rows) =
            filtered_composite_expression_prefix_spellings(&session, desc);

        assert_eq!(
            starts_with_rows, like_rows,
            "{context} should keep STARTS_WITH and LIKE in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "{context} should keep text-range and LIKE in parity",
        );
        assert_eq!(
            like_rows,
            filtered_composite_expression_prefix_expected_rows(desc),
            "{context} should preserve the guarded LOWER(handle) equality-prefix window",
        );
    }
}

#[test]
fn session_explain_execution_filtered_composite_expression_materialized_route_matrix_is_stable() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one guarded mixed-case dataset so the order-only, prefix,
    // and bounded text-range spellings all share the same expression family.
    seed_filtered_composite_expression_fixture(&session);

    // Phase 2: require each admitted full-projection spelling to preserve the
    // shared materialized route contract.
    for (context, sql, expected_node) in filtered_composite_expression_materialized_queries() {
        assert_filtered_composite_expression_materialized_descriptor(
            &session,
            sql,
            expected_node,
            context,
        );
    }
}

#[test]
fn session_explain_execution_filtered_composite_expression_prefix_key_only_keeps_bounded_route_parity()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the guarded mixed-case dataset so both projection shapes
    // share the same filtered composite expression route family.
    seed_filtered_composite_expression_fixture(&session);

    // Phase 2: collect the fuller materialized sibling and the narrower
    // key-only covering sibling from the same guarded prefix shape.
    let full_descriptor = session
        .lower_sql_query_for_tests::<FilteredIndexedSessionSqlEntity>(
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite expression prefix full-projection SQL query should lower")
        .explain_execution()
        .expect(
            "filtered composite expression prefix full-projection SQL explain_execution should succeed",
        );
    let key_only_descriptor = session
        .lower_sql_query_for_tests::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, tier FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite expression prefix key-only SQL query should lower")
        .explain_execution()
        .expect(
            "filtered composite expression prefix key-only SQL explain_execution should succeed",
        );

    // Phase 3: require the shared bounded route contract to stay in parity
    // even though the projection surface differs.
    assert_eq!(
        full_descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "filtered composite expression prefix full-projection roots should stay on the shared index-range root",
    );
    assert_eq!(
        key_only_descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "filtered composite expression prefix key-only roots should stay on the shared index-range root",
    );
    assert_eq!(
        full_descriptor.node_properties().get("fetch"),
        key_only_descriptor.node_properties().get("fetch"),
        "filtered composite expression prefix siblings should keep the same bounded fetch contract at the scan root",
    );
    assert_eq!(
        explain_execution_find_first_node(&full_descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        explain_execution_find_first_node(&key_only_descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "filtered composite expression prefix siblings should either both derive Top-N seek or both fail closed before it",
    );
    assert_eq!(
        explain_execution_find_first_node(
            &full_descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        explain_execution_find_first_node(
            &key_only_descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "filtered composite expression prefix siblings should either both derive index-range limit pushdown or both stay off that fast path",
    );
    assert_eq!(
        explain_execution_find_first_node(
            &full_descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        explain_execution_find_first_node(
            &key_only_descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "filtered composite expression prefix siblings should keep the same access-satisfied ordering contract",
    );
}

#[test]
fn execute_sql_statement_filtered_composite_expression_prefix_key_only_keeps_trace_scan_parity() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session().debug();

    // Phase 1: seed the guarded mixed-case dataset so both projection shapes
    // execute against the same filtered composite expression route.
    seed_filtered_composite_expression_fixture(&session);

    // Phase 2: execute the fuller materialized sibling and the narrower
    // key-only covering sibling with trace enabled.
    let full_query = session
        .lower_sql_query_for_tests::<FilteredIndexedSessionSqlEntity>(
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite expression prefix full-projection SQL query should lower");
    let key_only_query = session
        .lower_sql_query_for_tests::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, tier FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite expression prefix key-only SQL query should lower");
    let full_execution = session
        .execute_load_query_paged_with_trace(&full_query, None)
        .expect(
            "filtered composite expression prefix full-projection traced execution should succeed",
        );
    let key_only_execution = session
        .execute_load_query_paged_with_trace(&key_only_query, None)
        .expect("filtered composite expression prefix key-only traced execution should succeed");
    let full_trace = full_execution
        .execution_trace()
        .expect("filtered composite expression prefix full-projection execution should emit trace");
    let key_only_trace = key_only_execution
        .execution_trace()
        .expect("filtered composite expression prefix key-only execution should emit trace");

    // Phase 3: require the narrowed projection not to widen access traversal
    // or lose the same coarse optimization label.
    assert_eq!(
        full_trace.optimization(),
        key_only_trace.optimization(),
        "filtered composite expression prefix siblings should keep the same coarse execution optimization label",
    );
    assert_eq!(
        full_trace.keys_scanned(),
        key_only_trace.keys_scanned(),
        "filtered composite expression prefix siblings should scan the same bounded key count",
    );
}

#[test]
fn session_explain_execution_filtered_composite_expression_covering_route_matrix_is_stable() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one guarded mixed-case dataset so the ascending and
    // descending key-only bounded siblings stay on the same covering family.
    seed_filtered_composite_expression_fixture(&session);

    // Phase 2: require the narrower key-only text-range surface to keep the
    // planner-proven covering-read contract in both directions.
    for desc in [false, true] {
        for (context, sql) in filtered_composite_expression_covering_queries(desc) {
            assert_filtered_composite_expression_covering_descriptor(&session, sql, context);
        }
    }
}
