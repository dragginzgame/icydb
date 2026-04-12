use super::*;

#[test]
fn session_explain_execution_order_only_composite_covering_matrix_uses_index_range_access() {
    let cases = [
        (
            "ascending composite order-only covering SQL query",
            vec![
                (9_221_u128, "alpha", 2),
                (9_222, "alpha", 1),
                (9_223, "beta", 1),
            ],
            "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 2",
        ),
        (
            "descending composite order-only covering SQL query",
            vec![
                (9_231_u128, "alpha", 2),
                (9_232, "alpha", 1),
                (9_233, "beta", 1),
            ],
            "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code DESC, serial DESC, id DESC LIMIT 2",
        ),
    ];

    for (context, seed_rows, sql) in cases {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();

        // Phase 1: seed one deterministic composite-index dataset so the SQL lane
        // proves planner-selected order-only access on the live `(code, serial)` index.
        seed_composite_indexed_session_sql_entities(&session, seed_rows.as_slice());

        // Phase 2: require EXPLAIN EXECUTION to surface the shared order-only
        // composite index-range root and covering-read route.
        let descriptor = session
            .query_from_sql::<CompositeIndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} should lower: {err}"))
            .explain_execution()
            .unwrap_or_else(|err| panic!("{context} should explain_execution: {err}"));

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
        let projection_node =
            explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
                .unwrap_or_else(|| panic!("{context} should emit a covering-read node"));
        assert_eq!(
            projection_node.node_properties().get("existing_row_mode"),
            Some(&Value::Text("planner_proven".to_string())),
            "{context} should inherit the planner-proven covering mode",
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
