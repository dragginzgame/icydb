use super::*;

// Seed the canonical uppercase-prefix dataset used by the strict indexed
// prefix parity and EXPLAIN route checks on the generic indexed session
// fixture surface.
fn seed_indexed_prefix_fixture(session: &DbSession<SessionSqlCanister>) {
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

const fn indexed_prefix_covering_queries(desc: bool) -> [(&'static str, &'static str); 3] {
    if desc {
        [
            (
                "descending strict LIKE prefix",
                "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name DESC, id DESC LIMIT 2",
            ),
            (
                "descending direct STARTS_WITH",
                "SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name DESC, id DESC LIMIT 2",
            ),
            (
                "descending strict text range",
                "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name DESC, id DESC LIMIT 2",
            ),
        ]
    } else {
        [
            (
                "strict LIKE prefix",
                "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC, id ASC LIMIT 2",
            ),
            (
                "direct STARTS_WITH",
                "SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name ASC, id ASC LIMIT 2",
            ),
            (
                "strict text range",
                "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC, id ASC LIMIT 2",
            ),
        ]
    }
}

// Assert the shared covering index-range route contract for the admitted
// strict prefix spellings.
fn assert_indexed_prefix_covering_descriptor(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    context: &str,
) {
    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(sql)
        .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
        .explain_execution()
        .unwrap_or_else(|err| panic!("{context} SQL explain_execution should succeed: {err:?}"));

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "{context} queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "{context} projections should keep the explicit covering-read route",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "{context} roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "{context} roots should report access-satisfied ordering",
    );
}

fn assert_indexed_prefix_covering_route(session: &DbSession<SessionSqlCanister>, desc: bool) {
    for (context, sql) in indexed_prefix_covering_queries(desc) {
        assert_indexed_prefix_covering_descriptor(session, sql, context);
    }
}

#[test]
fn execute_sql_projection_indexed_prefix_matrix_matches_covering_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one mixed prefix dataset under a real secondary text index.
    seed_indexed_prefix_fixture(&session);

    // Phase 2: materialize the shared accepted spellings and require them to
    // preserve the same canonical covering result set.
    let strict_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC, id ASC",
    )
    .expect("strict indexed LIKE projection should execute");
    let casefold_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC, id ASC",
    )
    .expect("casefold LIKE projection should execute");
    let range_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC, id ASC",
    )
    .expect("strict text-range projection should execute");

    let expected_rows = vec![
        vec![Value::Text("Sable".to_string())],
        vec![Value::Text("Saffron".to_string())],
        vec![Value::Text("Sierra".to_string())],
        vec![Value::Text("Slate".to_string())],
        vec![Value::Text("Summit".to_string())],
    ];

    assert_eq!(
        strict_rows, expected_rows,
        "strict indexed LIKE prefix projection must return the matching secondary-index rows",
    );
    assert_eq!(
        strict_rows, casefold_rows,
        "strict indexed LIKE prefix execution must match the casefold fallback result set for already-uppercase prefixes",
    );
    assert_eq!(
        range_rows, expected_rows,
        "ordered strict text ranges must return the matching secondary-index rows",
    );
    assert_eq!(
        range_rows, strict_rows,
        "ordered strict text ranges must stay in parity with the equivalent strict LIKE prefix route",
    );
}

#[test]
fn execute_sql_entity_indexed_prefix_matrix_matches_projection_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset under the same
    // secondary text index used by the projection regression.
    seed_indexed_prefix_fixture(&session);

    // Phase 2: verify both admitted strict prefix spellings agree with the
    // projection surface on the same ordered covering route.
    let cases = [
        (
            "strict LIKE prefix entity query without explicit id tie-break",
            "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC",
            "SELECT * FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC",
        ),
        (
            "strict LIKE prefix entity query",
            "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC, id ASC",
            "SELECT * FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC, id ASC",
        ),
        (
            "strict text-range entity query",
            "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC, id ASC",
            "SELECT * FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC, id ASC",
        ),
    ];

    for (context, projection_sql, entity_sql) in cases {
        let projected_rows =
            statement_projection_rows::<IndexedSessionSqlEntity>(&session, projection_sql)
                .unwrap_or_else(|err| panic!("{context} projection should execute: {err}"));
        let entity_rows = session
            .execute_sql::<IndexedSessionSqlEntity>(entity_sql)
            .unwrap_or_else(|err| panic!("{context} should execute: {err}"));
        let entity_projected_names = entity_rows
            .iter()
            .map(|row| vec![Value::Text(row.entity_ref().name.clone())])
            .collect::<Vec<_>>();

        assert_eq!(entity_projected_names, projected_rows);
    }
}

#[test]
fn session_explain_execution_equivalent_strict_prefix_matrix_preserves_covering_route() {
    for desc in [false, true] {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();

        // Phase 1: seed one deterministic uppercase-prefix dataset so the
        // accepted strict spellings all run against the same indexed row set.
        seed_indexed_prefix_fixture(&session);

        // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
        // ranges to preserve the same covering index-range route in either direction.
        assert_indexed_prefix_covering_route(&session, desc);
    }
}
