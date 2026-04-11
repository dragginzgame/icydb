use super::*;

#[test]
fn execute_sql_projection_strict_like_prefix_matches_indexed_covering_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one mixed prefix dataset under a real secondary text index.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: execute the strict indexed LIKE projection and compare with the
    // casefold fallback shape that already succeeds in the reported repro.
    let strict_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%'",
    )
    .expect("strict indexed LIKE projection should execute");
    let casefold_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%'",
    )
    .expect("casefold LIKE projection should execute");

    let expected_rows = vec![
        vec![Value::Text("Sonja She-Devil".to_string())],
        vec![Value::Text("Stamm Bladecaster".to_string())],
        vec![Value::Text("Syra Child of Nature".to_string())],
        vec![Value::Text("Sir Edward Lion".to_string())],
        vec![Value::Text("Sethra Bhoaghail".to_string())],
    ];

    assert_eq!(
        strict_rows, expected_rows,
        "strict indexed LIKE prefix projection must return the matching secondary-index rows",
    );
    assert_eq!(
        strict_rows, casefold_rows,
        "strict indexed LIKE prefix execution must match the casefold fallback result set for already-uppercase prefixes",
    );
}

#[test]
fn execute_sql_entity_strict_like_prefix_matches_projection_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset under the same
    // secondary text index used by the projection regression.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: verify entity-row execution agrees with the projection surface
    // for the repaired strict LIKE prefix path.
    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC",
    )
    .expect("strict LIKE prefix projection should execute");
    let entity_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC",
        )
        .expect("strict LIKE prefix entity query should execute");
    let entity_projected_names = entity_rows
        .iter()
        .map(|row| vec![Value::Text(row.entity_ref().name.clone())])
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_names, projected_rows);
}

#[test]
fn session_explain_execution_strict_like_prefix_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic prefix dataset so EXPLAIN EXECUTION can
    // lock the bounded secondary-index route for the repaired strict LIKE path.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: require the ordered-and-limited strict LIKE query to stay on
    // the shared index-range covering route instead of regressing to a
    // materialized full scan.
    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("strict LIKE prefix covering SQL query should lower")
        .explain_execution()
        .expect("strict LIKE prefix covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "ordered strict LIKE prefix queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "ordered strict LIKE prefix projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "ordered strict LIKE prefix explain roots should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "ordered strict LIKE prefix roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "ordered strict LIKE prefix roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_strict_like_prefix_desc_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic prefix dataset so the descending strict
    // LIKE route cannot silently fall back to a reverse materialized sort.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: require the descending ordered-and-limited strict LIKE query to
    // stay on the same bounded index-range covering route.
    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name DESC, id DESC LIMIT 2",
        )
        .expect("descending strict LIKE prefix covering SQL query should lower")
        .explain_execution()
        .expect("descending strict LIKE prefix covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending strict LIKE prefix queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending strict LIKE prefix projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "descending strict LIKE prefix explain roots should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending strict LIKE prefix roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending strict LIKE prefix roots should report access-satisfied ordering",
    );
}

#[test]
fn execute_sql_projection_strict_text_range_matches_indexed_covering_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one mixed prefix dataset under the same real secondary
    // text index used by the strict LIKE regression so bounded range forms can
    // be checked against the already-proven prefix route.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: execute the equivalent ordered strict text range and require it
    // to match the repaired strict LIKE prefix result exactly.
    let range_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC, id ASC",
    )
    .expect("strict text-range projection should execute");
    let prefix_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC, id ASC",
    )
    .expect("strict LIKE prefix projection should execute");

    let expected_rows = vec![
        vec![Value::Text("Sethra Bhoaghail".to_string())],
        vec![Value::Text("Sir Edward Lion".to_string())],
        vec![Value::Text("Sonja She-Devil".to_string())],
        vec![Value::Text("Stamm Bladecaster".to_string())],
        vec![Value::Text("Syra Child of Nature".to_string())],
    ];

    assert_eq!(
        range_rows, expected_rows,
        "ordered strict text ranges must return the matching secondary-index rows",
    );
    assert_eq!(
        range_rows, prefix_rows,
        "ordered strict text ranges must stay in parity with the equivalent strict LIKE prefix route",
    );
}

#[test]
fn execute_sql_entity_strict_text_range_matches_projection_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset under the same
    // secondary text index used by the explicit text-range regression.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: verify entity-row execution agrees with the projection surface
    // for the explicit bounded text-range form.
    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC, id ASC",
    )
    .expect("strict text-range projection should execute");
    let entity_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC, id ASC",
        )
        .expect("strict text-range entity query should execute");
    let entity_projected_names = entity_rows
        .iter()
        .map(|row| vec![Value::Text(row.entity_ref().name.clone())])
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_names, projected_rows);
}

#[test]
fn session_explain_execution_strict_text_range_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic prefix dataset so EXPLAIN EXECUTION can
    // lock the bounded secondary-index route for the explicit text-range form.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: require the ordered-and-limited explicit text range to stay on
    // the shared index-range covering route instead of regressing to one
    // materialized full scan.
    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("strict text-range covering SQL query should lower")
        .explain_execution()
        .expect("strict text-range covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "ordered strict text-range queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "ordered strict text-range projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "ordered strict text-range explain roots should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "ordered strict text-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "ordered strict text-range roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_strict_text_range_desc_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic prefix dataset so the descending
    // explicit text-range route cannot silently fall back to a reverse
    // materialized sort.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: require the descending ordered-and-limited explicit text range
    // to stay on the same bounded index-range covering route.
    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name DESC, id DESC LIMIT 2",
        )
        .expect("descending strict text-range covering SQL query should lower")
        .explain_execution()
        .expect("descending strict text-range covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending strict text-range queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending strict text-range projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "descending strict text-range explain roots should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending strict text-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending strict text-range roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_equivalent_strict_prefix_forms_preserve_covering_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset so the three
    // accepted strict spellings all run against the same indexed row set.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to preserve the same covering index-range route on one ordered
    // limited window.
    let queries = [
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
    ];

    for (context, sql) in queries {
        let descriptor = session
            .query_from_sql::<IndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
            .explain_execution()
            .unwrap_or_else(|err| {
                panic!("{context} SQL explain_execution should succeed: {err:?}")
            });

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
        assert_eq!(
            descriptor.node_properties().get("cov_read_route"),
            Some(&Value::Text("covering_read".to_string())),
            "{context} explain roots should expose the covering-read route label",
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
}

#[test]
fn session_explain_execution_equivalent_desc_strict_prefix_forms_preserve_covering_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset so the three
    // accepted descending spellings all run against the same indexed row set.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to preserve the same reverse covering index-range route.
    let queries = [
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
    ];

    for (context, sql) in queries {
        let descriptor = session
            .query_from_sql::<IndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
            .explain_execution()
            .unwrap_or_else(|err| {
                panic!("{context} SQL explain_execution should succeed: {err:?}")
            });

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
        assert_eq!(
            descriptor.node_properties().get("cov_read_route"),
            Some(&Value::Text("covering_read".to_string())),
            "{context} explain roots should expose the covering-read route label",
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
}
