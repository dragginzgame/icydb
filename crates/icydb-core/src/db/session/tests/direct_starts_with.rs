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
    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(sql)
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
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::ResidualPredicateFilter
        )
        .is_some(),
        "{context} should keep the residual filter stage",
    );
}

#[test]
fn execute_sql_projection_direct_starts_with_matches_indexed_like_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset under the same
    // secondary text index used by the strict LIKE prefix regression.
    seed_direct_starts_with_fixture(&session);

    // Phase 2: prove the new direct spelling returns the same indexed
    // projection rows as the established strict LIKE prefix path.
    let direct_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name ASC",
    )
    .expect("direct STARTS_WITH projection should execute");
    let like_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC",
    )
    .expect("strict LIKE prefix projection should execute");
    let range_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC",
    )
    .expect("strict text-range projection should execute");

    assert_eq!(
        direct_rows, like_rows,
        "direct STARTS_WITH projection should match the established strict LIKE prefix result set",
    );
    assert_eq!(
        direct_rows, range_rows,
        "direct STARTS_WITH projection should match the equivalent strict text-range result set",
    );
}

#[test]
fn execute_sql_entity_direct_starts_with_matches_indexed_like_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset under the same
    // secondary text index used by the strict LIKE prefix regression.
    seed_direct_starts_with_fixture(&session);

    // Phase 2: prove the direct spelling keeps entity-row execution aligned
    // with the established strict LIKE prefix path.
    let direct_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name ASC",
        )
        .expect("direct STARTS_WITH entity query should execute");
    let like_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC",
        )
        .expect("strict LIKE prefix entity query should execute");
    let range_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC",
        )
        .expect("strict text-range entity query should execute");

    assert_eq!(direct_rows.len(), like_rows.len());
    for (direct, like) in direct_rows.iter().zip(like_rows.iter()) {
        assert_eq!(
            direct.entity_ref(),
            like.entity_ref(),
            "direct STARTS_WITH entity rows should match strict LIKE prefix entity rows",
        );
    }
    assert_eq!(direct_rows.len(), range_rows.len());
    for (direct, range) in direct_rows.iter().zip(range_rows.iter()) {
        assert_eq!(
            direct.entity_ref(),
            range.entity_ref(),
            "direct STARTS_WITH entity rows should match strict text-range entity rows",
        );
    }
}

#[test]
fn execute_sql_projection_direct_lower_prefix_matrix_matches_indexed_like_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_direct_starts_with_fixture(&session);

    let like_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
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
        let actual_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should execute: {err}"));

        assert_eq!(
            actual_rows, like_rows,
            "{context} should match the established casefold LIKE prefix result set",
        );
    }
}

#[test]
fn session_explain_execution_direct_lower_equivalent_prefix_forms_preserve_expression_index_route()
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
    ];

    for (sql, context) in cases {
        assert_direct_casefold_expression_route(&session, sql, context);
    }
}

#[test]
fn execute_sql_entity_direct_upper_prefix_matrix_matches_indexed_like_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_direct_starts_with_fixture(&session);

    let like_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC",
        )
        .expect("UPPER(field) LIKE entity query should execute");

    let cases = [
        (
            "SELECT * FROM IndexedSessionSqlEntity WHERE STARTS_WITH(UPPER(name), 'S') ORDER BY name ASC",
            "direct UPPER(field) STARTS_WITH entity rows",
        ),
        (
            "SELECT * FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC",
            "UPPER(field) ordered text-range entity rows",
        ),
    ];

    for (sql, context) in cases {
        let actual_rows = session
            .execute_sql::<IndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} should execute: {err}"));

        assert_eq!(actual_rows.len(), like_rows.len());
        for (actual_row, like_row) in actual_rows.iter().zip(like_rows.iter()) {
            assert_eq!(
                actual_row.entity_ref(),
                like_row.entity_ref(),
                "{context} should match the established casefold LIKE prefix entity rows",
            );
        }
    }
}

#[test]
fn session_explain_execution_direct_upper_equivalent_prefix_forms_preserve_expression_index_route()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_direct_starts_with_fixture(&session);

    let cases = [
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

            let deleted_rows = session
                .execute_sql::<IndexedSessionSqlEntity>(sql)
                .expect("indexed STARTS_WITH/LIKE delete should execute");
            let deleted_names = deleted_rows
                .iter()
                .map(|row| row.entity_ref().name.clone())
                .collect::<Vec<_>>();
            let remaining_names = session
                .load::<IndexedSessionSqlEntity>()
                .order_by("name")
                .execute()
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
