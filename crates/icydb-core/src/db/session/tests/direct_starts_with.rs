use super::*;

// Seed the canonical uppercase-prefix dataset used by the direct
// STARTS_WITH/LIKE/text-range parity checks on the generic indexed session
// fixture surface.
fn seed_direct_starts_with_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_indexed_session_sql_entities(
        session,
        &[
            ("Sable", 10),
            ("Saffron", 20),
            ("Sierra", 30),
            ("Slate", 40),
            ("Summit", 50),
            ("Atlas", 60),
        ],
    );
}

// Assert the shared non-covering expression-index route for direct lower/upper
// STARTS_WITH and explicit text-range spellings.
fn assert_direct_casefold_expression_route(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    context: &str,
) {
    let descriptor = lower_select_query_for_tests::<IndexedSessionSqlEntity>(&session, sql)
        .unwrap_or_else(|err| panic!("{context} should lower: {err}"))
        .explain_execution()
        .unwrap_or_else(|err| panic!("{context} should explain_execution: {err}"));

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "{context} should keep the shared expression index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(false),
        "{context} should keep the non-covering materialized route",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::ResidualFilter)
            .is_some(),
        "{context} should keep the residual filter stage",
    );
}

#[test]
fn execute_sql_direct_starts_with_family_matrix_matches_indexed_like_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset under the same
    // secondary text index used by the strict LIKE prefix regression.
    seed_direct_starts_with_fixture(&session);

    // Phase 2: prove the direct spelling stays aligned with the established
    // strict LIKE and explicit text-range paths on both public lanes.
    let projection_direct_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name ASC",
    )
    .expect("direct STARTS_WITH projection should execute");
    let projection_like_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC",
    )
    .expect("strict LIKE prefix projection should execute");
    let projection_range_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC",
    )
    .expect("strict text-range projection should execute");

    assert_eq!(
        projection_direct_rows, projection_like_rows,
        "direct STARTS_WITH projection should match the established strict LIKE prefix result set",
    );
    assert_eq!(
        projection_direct_rows, projection_range_rows,
        "direct STARTS_WITH projection should match the equivalent strict text-range result set",
    );

    let entity_direct_names = execute_scalar_select_for_tests::<IndexedSessionSqlEntity>(
        &session,
        "SELECT * FROM IndexedSessionSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name ASC",
    )
    .expect("direct STARTS_WITH entity query should execute")
    .iter()
    .map(|row| row.entity_ref().name.clone())
    .collect::<Vec<_>>();
    let entity_like_names = execute_scalar_select_for_tests::<IndexedSessionSqlEntity>(
        &session,
        "SELECT * FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC",
    )
    .expect("strict LIKE prefix entity query should execute")
    .iter()
    .map(|row| row.entity_ref().name.clone())
    .collect::<Vec<_>>();
    let entity_range_names = execute_scalar_select_for_tests::<IndexedSessionSqlEntity>(
        &session,
        "SELECT * FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC",
    )
    .expect("strict text-range entity query should execute")
    .iter()
    .map(|row| row.entity_ref().name.clone())
    .collect::<Vec<_>>();

    assert_eq!(
        entity_direct_names, entity_like_names,
        "direct STARTS_WITH entity rows should match strict LIKE prefix entity rows",
    );
    assert_eq!(
        entity_direct_names, entity_range_names,
        "direct STARTS_WITH entity rows should match strict text-range entity rows",
    );

    // Phase 3: keep the accepted `UPPER(name)` entity family aligned with the
    // established casefold LIKE path on the same seeded fixture.
    let upper_like_rows = execute_scalar_select_for_tests::<IndexedSessionSqlEntity>(
        &session,
        "SELECT * FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC",
    )
    .expect("UPPER(field) LIKE entity query should execute");
    let upper_cases = [
        (
            "SELECT * FROM IndexedSessionSqlEntity WHERE STARTS_WITH(UPPER(name), 'S') ORDER BY name ASC",
            "direct UPPER(field) STARTS_WITH entity rows",
        ),
        (
            "SELECT * FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC",
            "UPPER(field) ordered text-range entity rows",
        ),
    ];

    for (sql, context) in upper_cases {
        let actual_rows = execute_scalar_select_for_tests::<IndexedSessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should execute: {err}"));

        assert_eq!(actual_rows.len(), upper_like_rows.len());
        for (actual_row, like_row) in actual_rows.iter().zip(upper_like_rows.iter()) {
            assert_eq!(
                actual_row.entity_ref(),
                like_row.entity_ref(),
                "{context} should match the established casefold LIKE prefix entity rows",
            );
        }
    }
}

#[test]
fn execute_sql_projection_direct_lower_prefix_matrix_matches_indexed_like_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_direct_starts_with_fixture(&session);

    let like_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC",
    )
    .expect("LOWER(field) LIKE projection should execute");

    let cases = [
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(LOWER(name), 's') ORDER BY name ASC",
            "direct LOWER(field) STARTS_WITH projection",
        ),
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC",
            "LOWER(field) ordered text-range projection",
        ),
    ];

    for (sql, context) in cases {
        let actual_rows = statement_projection_rows::<IndexedSessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should execute: {err}"));

        assert_eq!(
            actual_rows, like_rows,
            "{context} should match the established casefold LIKE prefix result set",
        );
    }
}

#[test]
fn execute_sql_not_like_prefix_matrix_matches_negated_prefix_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_direct_starts_with_fixture(&session);

    let strict_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name NOT LIKE 'S%' ORDER BY name ASC",
    )
    .expect("strict NOT LIKE prefix projection should execute");
    let lower_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) NOT LIKE 's%' ORDER BY name ASC",
    )
    .expect("LOWER(field) NOT LIKE projection should execute");
    let upper_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE UPPER(name) NOT LIKE 'S%' ORDER BY name ASC",
    )
    .expect("UPPER(field) NOT LIKE projection should execute");
    let expected_rows = vec![vec![Value::Text("Atlas".to_string())]];

    assert_eq!(
        strict_rows, expected_rows,
        "strict NOT LIKE prefix projection should keep only rows outside the bounded strict prefix family",
    );
    assert_eq!(
        lower_rows, expected_rows,
        "LOWER(field) NOT LIKE projection should keep only rows outside the bounded casefold prefix family",
    );
    assert_eq!(
        upper_rows, expected_rows,
        "UPPER(field) NOT LIKE projection should keep only rows outside the bounded casefold prefix family",
    );
}

#[test]
fn execute_sql_ilike_prefix_matrix_matches_casefold_prefix_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_direct_starts_with_fixture(&session);

    let plain_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name ILIKE 's%' ORDER BY name ASC",
    )
    .expect("ILIKE prefix projection should execute");
    let lower_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC",
    )
    .expect("LOWER(field) LIKE projection should execute");

    assert_eq!(
        plain_rows, lower_rows,
        "ILIKE prefix projection should match the bounded casefold prefix family",
    );
}

#[test]
fn execute_sql_not_ilike_prefix_matrix_matches_negated_casefold_prefix_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_direct_starts_with_fixture(&session);

    let plain_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name NOT ILIKE 's%' ORDER BY name ASC",
    )
    .expect("NOT ILIKE prefix projection should execute");
    let lower_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) NOT LIKE 's%' ORDER BY name ASC",
    )
    .expect("LOWER(field) NOT LIKE projection should execute");
    let expected_rows = vec![vec![Value::Text("Atlas".to_string())]];

    assert_eq!(
        plain_rows, expected_rows,
        "NOT ILIKE prefix projection should keep only rows outside the bounded casefold prefix family",
    );
    assert_eq!(
        plain_rows, lower_rows,
        "NOT ILIKE prefix projection should match the established negated casefold LIKE result set",
    );
}

#[test]
fn session_explain_execution_direct_casefold_equivalent_prefix_matrix_preserves_expression_index_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_direct_starts_with_fixture(&session);

    let cases = [
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC",
            "LOWER(field) LIKE explain route",
        ),
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(LOWER(name), 's') ORDER BY name ASC",
            "direct LOWER(field) STARTS_WITH explain route",
        ),
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC",
            "LOWER(field) ordered text-range explain route",
        ),
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC",
            "UPPER(field) LIKE explain route",
        ),
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(UPPER(name), 'S') ORDER BY name ASC",
            "direct UPPER(field) STARTS_WITH explain route",
        ),
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC",
            "UPPER(field) ordered text-range explain route",
        ),
    ];

    for (sql, context) in cases {
        assert_direct_casefold_expression_route(&session, sql, context);
    }
}

#[test]
fn execute_sql_delete_direct_starts_with_family_matches_indexed_like_delete_rows() {
    // Phase 1: define the accepted direct predicate family and the established
    // equivalent bounded LIKE spellings they should continue to match.
    let cases = [
        (
            "DELETE FROM IndexedSessionSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name ASC LIMIT 2",
            "DELETE FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC LIMIT 2",
            "strict direct STARTS_WITH delete",
        ),
        (
            "DELETE FROM IndexedSessionSqlEntity WHERE STARTS_WITH(LOWER(name), 's') ORDER BY name ASC LIMIT 2",
            "DELETE FROM IndexedSessionSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC LIMIT 2",
            "direct LOWER(field) STARTS_WITH delete",
        ),
        (
            "DELETE FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC LIMIT 2",
            "DELETE FROM IndexedSessionSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC LIMIT 2",
            "direct LOWER(field) ordered text-range delete",
        ),
        (
            "DELETE FROM IndexedSessionSqlEntity WHERE STARTS_WITH(UPPER(name), 'S') ORDER BY name ASC LIMIT 2",
            "DELETE FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC LIMIT 2",
            "direct UPPER(field) STARTS_WITH delete",
        ),
        (
            "DELETE FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC LIMIT 2",
            "DELETE FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC LIMIT 2",
            "direct UPPER(field) ordered text-range delete",
        ),
    ];

    // Phase 2: run the direct and LIKE deletes against separate fresh seeds so
    // both the deleted rows and surviving rows must stay identical.
    for (direct_sql, like_sql, context) in cases {
        let run_delete = |sql: &str| {
            reset_indexed_session_sql_store();
            let session = indexed_sql_session();
            seed_direct_starts_with_fixture(&session);

            let deleted_names = statement_projection_rows::<IndexedSessionSqlEntity>(
                &session,
                format!("{sql} RETURNING name").as_str(),
            )
            .expect("indexed STARTS_WITH/LIKE delete should execute")
            .into_iter()
            .map(|row| {
                let [Value::Text(name)] = row.as_slice() else {
                    panic!("indexed delete returning should yield one projected name column");
                };
                name.clone()
            })
            .collect::<Vec<_>>();
            let remaining_names = session
                .load::<IndexedSessionSqlEntity>()
                .order_term(crate::db::asc("name"))
                .execute()
                .and_then(crate::db::LoadQueryResult::into_rows)
                .expect("post-delete indexed load should succeed")
                .iter()
                .map(|row| row.entity_ref().name.clone())
                .collect::<Vec<_>>();

            (deleted_names, remaining_names)
        };

        let direct = run_delete(direct_sql);
        let like = run_delete(like_sql);

        assert_eq!(
            direct, like,
            "bounded direct STARTS_WITH delete should match the established LIKE delete semantics: {context}",
        );
    }
}
