use super::*;

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "this matrix test locks several related authority-label removals on one shared surface contract"
)]
fn secondary_route_surfaces_stay_off_removed_authority_labels_matrix() {
    let explain_cases = [
        (
            "secondary non-covering explain",
            vec![
                (9_220_u128, "alice", 10_u64),
                (9_221, "bob", 20),
                (9_222, "carol", 30),
            ],
            "EXPLAIN EXECUTION SELECT age FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 2",
            Some("cov_read_route=Text(\"materialized\")"),
        ),
        (
            "secondary covering explain",
            vec![
                (9_226_u128, "alice", 10_u64),
                (9_227, "bob", 20),
                (9_228, "carol", 30),
            ],
            "EXPLAIN EXECUTION SELECT id, name FROM IndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
            Some("existing_row_mode=Text(\"planner_proven\")"),
        ),
    ];
    let descriptor_cases = [
        (
            "secondary non-covering descriptor json",
            vec![
                (9_223_u128, "alice", 10_u64),
                (9_224, "bob", 20),
                (9_225, "carol", 30),
            ],
            "SELECT age FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 2",
            false,
        ),
        (
            "secondary covering descriptor json",
            vec![
                (9_229_u128, "alice", 10_u64),
                (9_230, "bob", 20),
                (9_231, "carol", 30),
            ],
            "SELECT id, name FROM IndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
            true,
        ),
    ];

    for (context, seed_rows, sql, required_token) in explain_cases {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();

        for (id, name, age) in seed_rows {
            session
                .insert(IndexedSessionSqlEntity {
                    id: Ulid::from_u128(id),
                    name: name.to_string(),
                    age,
                })
                .unwrap_or_else(|err| panic!("{context} fixture insert should succeed: {err}"));
        }

        let surface = dispatch_explain_sql::<IndexedSessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should execute: {err}"));

        if let Some(token) = required_token {
            assert!(
                surface.contains(token),
                "{context} should keep its required route token: {surface}",
            );
        }
        assert!(
            !surface.contains("authority_decision")
                && !surface.contains("authority_reason")
                && !surface.contains("index_state"),
            "{context} should stay off the removed authority-label surface: {surface}",
        );
        assert!(
            !surface.contains("witness_validated")
                && !surface.contains("storage_existence_witness"),
            "{context} must not surface legacy authority labels: {surface}",
        );
    }

    for (context, seed_rows, sql, expect_covering) in descriptor_cases {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();

        for (id, name, age) in seed_rows {
            session
                .insert(IndexedSessionSqlEntity {
                    id: Ulid::from_u128(id),
                    name: name.to_string(),
                    age,
                })
                .unwrap_or_else(|err| panic!("{context} fixture insert should succeed: {err}"));
        }

        let descriptor =
            store_backed_execution_descriptor_for_sql::<IndexedSessionSqlEntity>(&session, sql);

        assert_eq!(
            descriptor.covering_scan(),
            Some(expect_covering),
            "{context} should preserve the typed covering-route contract",
        );
        if expect_covering {
            let projection_node = explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::CoveringRead,
            )
            .unwrap_or_else(|| panic!("{context} should emit a covering-read node"));
            assert_eq!(
                projection_node.node_properties().get("existing_row_mode"),
                Some(&Value::from("planner_proven")),
                "{context} should preserve planner-proven existing-row mode",
            );
        } else {
            assert!(
                explain_execution_find_first_node(
                    &descriptor,
                    ExplainExecutionNodeType::CoveringRead,
                )
                .is_none(),
                "{context} should stay off the covering-read route",
            );
        }

        let descriptor_json = descriptor.render_json_canonical();
        assert!(
            !descriptor_json.contains("authority_decision")
                && !descriptor_json.contains("authority_reason")
                && !descriptor_json.contains("index_state"),
            "{context} should stay off the removed authority-label json surface: {descriptor_json}",
        );
        assert!(
            !descriptor_json.contains("witness_validated")
                && !descriptor_json.contains("storage_existence_witness"),
            "{context} must not surface legacy authority labels in descriptor json: {descriptor_json}",
        );
    }
}

#[test]
fn execute_sql_dispatch_explain_execution_secondary_covering_order_field_building_index_becomes_planner_invisible()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (id, name, age) in [
        (9_460_u128, "alice", 10_u64),
        (9_461, "bob", 20),
        (9_462, "carol", 30),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                age,
            })
            .expect("indexed SQL building-state explain fixture insert should succeed");
    }
    mark_indexed_session_sql_index_building();

    let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT id, name FROM IndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("building-index secondary covering EXPLAIN EXECUTION should execute");

    assert!(
        explain.contains("FullScan")
            && explain.contains("OrderByMaterializedSort")
            && !explain.contains("CoveringRead")
            && !explain.contains("existing_row_mode")
            && !explain.contains("authority_decision")
            && !explain.contains("authority_reason")
            && !explain.contains("index_state"),
        "building indexes must disappear from planner visibility and explain as a materialized full-scan fallback: {explain}",
    );
    assert!(
        !explain.contains("witness_validated") && !explain.contains("storage_existence_witness"),
        "building indexes must not leave legacy secondary covering labels behind once they are planner-invisible: {explain}",
    );

    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT id, name FROM IndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("building-index secondary covering query should execute");

    assert_eq!(
        projected_rows,
        vec![
            vec![
                Value::Ulid(Ulid::from_u128(9_460)),
                Value::Text("alice".to_string()),
            ],
            vec![
                Value::Ulid(Ulid::from_u128(9_461)),
                Value::Text("bob".to_string()),
            ],
        ],
        "planner-invisibility fallback should preserve the same ordered query output",
    );
}
