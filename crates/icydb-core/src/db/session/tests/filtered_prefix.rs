use super::*;

fn seed_filtered_prefix_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_filtered_indexed_session_sql_entities(
        session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "bristle", true, 30),
            (9_204, "broom", false, 40),
            (9_205, "charlie", true, 50),
        ],
    );
}

fn seed_filtered_composite_prefix_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_filtered_composite_indexed_session_sql_entities(
        session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );
}

const fn filtered_prefix_queries(desc: bool) -> [(&'static str, &'static str); 3] {
    if desc {
        [
            (
                "descending filtered strict LIKE prefix",
                "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 2",
            ),
            (
                "descending filtered direct STARTS_WITH",
                "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name DESC, id DESC LIMIT 2",
            ),
            (
                "descending filtered strict text range",
                "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name DESC, id DESC LIMIT 2",
            ),
        ]
    } else {
        [
            (
                "filtered strict LIKE prefix",
                "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 2",
            ),
            (
                "filtered direct STARTS_WITH",
                "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name ASC, id ASC LIMIT 2",
            ),
            (
                "filtered strict text range",
                "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name ASC, id ASC LIMIT 2",
            ),
        ]
    }
}

const fn filtered_composite_prefix_queries(desc: bool) -> [(&'static str, &'static str); 3] {
    if desc {
        [
            (
                "descending filtered composite strict LIKE prefix",
                "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
            ),
            (
                "descending filtered composite direct STARTS_WITH",
                "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle DESC, id DESC LIMIT 2",
            ),
            (
                "descending filtered composite strict text range",
                "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle DESC, id DESC LIMIT 2",
            ),
        ]
    } else {
        [
            (
                "filtered composite strict LIKE prefix",
                "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
            ),
            (
                "filtered composite direct STARTS_WITH",
                "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle ASC, id ASC LIMIT 2",
            ),
            (
                "filtered composite strict text range",
                "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle ASC, id ASC LIMIT 2",
            ),
        ]
    }
}

fn assert_filtered_prefix_projection_parity(
    session: &DbSession<SessionSqlCanister>,
    desc: bool,
    expected_rows: Vec<Vec<Value>>,
) {
    let mut rows = Vec::new();
    for (context, sql) in filtered_prefix_queries(desc) {
        rows.push(
            statement_projection_rows::<FilteredIndexedSessionSqlEntity>(session, sql)
                .unwrap_or_else(|err| panic!("{context} projection should execute: {err}")),
        );
    }

    assert_eq!(rows[0], expected_rows);
    assert_eq!(rows[1], rows[0]);
    assert_eq!(rows[2], rows[0]);
}

fn assert_filtered_prefix_covering_route(session: &DbSession<SessionSqlCanister>, desc: bool) {
    for (context, sql) in filtered_prefix_queries(desc) {
        let descriptor =
            lower_select_query_for_tests::<FilteredIndexedSessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
                .explain_execution()
                .unwrap_or_else(|err| {
                    panic!("{context} SQL explain_execution should succeed: {err:?}")
                });

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan
        );
        assert_eq!(descriptor.covering_scan(), Some(true));
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::SecondaryOrderPushdown
            )
            .is_some(),
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )
            .is_some(),
        );
    }
}

fn assert_filtered_composite_prefix_projection_parity(
    session: &DbSession<SessionSqlCanister>,
    desc: bool,
    expected_rows: Vec<Vec<Value>>,
) {
    let mut rows = Vec::new();
    for (context, sql) in filtered_composite_prefix_queries(desc) {
        rows.push(
            statement_projection_rows::<FilteredIndexedSessionSqlEntity>(session, sql)
                .unwrap_or_else(|err| panic!("{context} projection should execute: {err}")),
        );
    }

    assert_eq!(rows[0], expected_rows);
    assert_eq!(rows[1], rows[0]);
    assert_eq!(rows[2], rows[0]);
}

fn assert_filtered_composite_covering_route(session: &DbSession<SessionSqlCanister>, desc: bool) {
    for (context, sql) in filtered_composite_prefix_queries(desc) {
        let descriptor =
            lower_select_query_for_tests::<FilteredIndexedSessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
                .explain_execution()
                .unwrap_or_else(|err| {
                    panic!("{context} SQL explain_execution should succeed: {err:?}")
                });

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan
        );
        assert_eq!(descriptor.covering_scan(), Some(true));
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::SecondaryOrderPushdown
            )
            .is_some(),
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )
            .is_some(),
        );
    }
}

#[test]
fn execute_sql_projection_filtered_strict_prefix_matrix_matches_guarded_rows() {
    let cases = [
        ("filtered simple asc", false, false),
        ("filtered simple desc", false, true),
        ("filtered composite asc", true, false),
        ("filtered composite desc", true, true),
    ];

    for (context, composite, desc) in cases {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();

        match (composite, desc) {
            (false, false) => {
                seed_filtered_prefix_fixture(&session);
                assert_filtered_prefix_projection_parity(
                    &session,
                    false,
                    vec![
                        vec![Value::Text("bravo".to_string())],
                        vec![Value::Text("bristle".to_string())],
                    ],
                );
            }
            (false, true) => {
                seed_filtered_prefix_fixture(&session);
                assert_filtered_prefix_projection_parity(
                    &session,
                    true,
                    vec![
                        vec![Value::Text("bristle".to_string())],
                        vec![Value::Text("bravo".to_string())],
                    ],
                );
            }
            (true, false) => {
                seed_filtered_composite_prefix_fixture(&session);
                assert_filtered_composite_prefix_projection_parity(
                    &session,
                    false,
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
                );
            }
            (true, true) => {
                seed_filtered_composite_prefix_fixture(&session);
                assert_filtered_composite_prefix_projection_parity(
                    &session,
                    true,
                    vec![
                        vec![
                            Value::Text("gold".to_string()),
                            Value::Text("bristle".to_string()),
                        ],
                        vec![
                            Value::Text("gold".to_string()),
                            Value::Text("bravo".to_string()),
                        ],
                    ],
                );
            }
        }

        let _ = context;
    }
}

#[test]
fn session_explain_execution_filtered_strict_prefix_matrix_preserves_covering_route() {
    let cases = [
        ("filtered simple asc", false, false),
        ("filtered simple desc", false, true),
        ("filtered composite asc", true, false),
        ("filtered composite desc", true, true),
    ];

    for (_context, composite, desc) in cases {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();

        match (composite, desc) {
            (false, false) => {
                seed_filtered_prefix_fixture(&session);
                assert_filtered_prefix_covering_route(&session, false);
            }
            (false, true) => {
                seed_filtered_prefix_fixture(&session);
                assert_filtered_prefix_covering_route(&session, true);
            }
            (true, false) => {
                seed_filtered_composite_prefix_fixture(&session);
                assert_filtered_composite_covering_route(&session, false);
            }
            (true, true) => {
                seed_filtered_composite_prefix_fixture(&session);
                assert_filtered_composite_covering_route(&session, true);
            }
        }
    }
}
