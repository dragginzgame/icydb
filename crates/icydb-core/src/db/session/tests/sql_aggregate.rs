use super::*;

// Execute one aggregate SQL case and assert the scalar aggregate value stays
// stable for that query spelling.
fn assert_sql_aggregate_value_case(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected: Value,
    context: &str,
) {
    let rows = statement_projection_rows::<SessionSqlEntity>(session, sql)
        .unwrap_or_else(|err| panic!("{context} aggregate SQL should execute: {err}"));
    let actual = rows
        .into_iter()
        .next()
        .and_then(|mut row| if row.len() == 1 { row.pop() } else { None })
        .unwrap_or_else(|| panic!("{context} aggregate SQL should emit one scalar value"));

    assert_eq!(
        actual, expected,
        "{context} should preserve aggregate value"
    );
}

// Execute one global aggregate EXPLAIN case and assert the public surface keeps
// the expected stable tokens.
fn assert_global_aggregate_explain_case(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    tokens: &[&str],
    require_json_object: bool,
    context: &str,
) {
    let explain = statement_explain_sql::<SessionSqlEntity>(session, sql)
        .unwrap_or_else(|err| panic!("{context} explain SQL should succeed: {err}"));

    if require_json_object {
        assert!(
            explain.starts_with('{') && explain.ends_with('}'),
            "{context} should render one JSON object payload",
        );
    }

    assert_explain_contains_tokens(explain.as_str(), tokens, context);
}

#[test]
fn execute_sql_aggregate_basic_value_matrix_matches_expected_values() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("aggregate-a", 20), ("aggregate-b", 32)]);

    let cases = [
        (
            "count star",
            "SELECT COUNT(*) FROM SessionSqlEntity",
            Value::Uint(2),
        ),
        (
            "count field",
            "SELECT COUNT(age) FROM SessionSqlEntity",
            Value::Uint(2),
        ),
        (
            "sum",
            "SELECT SUM(age) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(52u64)),
        ),
        (
            "avg",
            "SELECT AVG(age) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(26u64)),
        ),
        (
            "min",
            "SELECT MIN(age) FROM SessionSqlEntity",
            Value::Uint(20),
        ),
        (
            "max",
            "SELECT MAX(age) FROM SessionSqlEntity",
            Value::Uint(32),
        ),
        (
            "qualified sum",
            "SELECT SUM(SessionSqlEntity.age) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(52u64)),
        ),
        (
            "empty sum",
            "SELECT SUM(age) FROM SessionSqlEntity WHERE age < 0",
            Value::Null,
        ),
        (
            "empty min",
            "SELECT MIN(age) FROM SessionSqlEntity WHERE age < 0",
            Value::Null,
        ),
        (
            "empty max",
            "SELECT MAX(age) FROM SessionSqlEntity WHERE age < 0",
            Value::Null,
        ),
    ];

    for (context, sql, expected) in cases {
        assert_sql_aggregate_value_case(&session, sql, expected, context);
    }
}

#[test]
fn execute_sql_aggregate_distinct_value_matrix_matches_expected_values() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("aggregate-distinct-a", 20),
            ("aggregate-distinct-b", 20),
            ("aggregate-distinct-c", 32),
        ],
    );

    let cases = [
        (
            "distinct count",
            "SELECT COUNT(DISTINCT age) FROM SessionSqlEntity",
            Value::Uint(2),
        ),
        (
            "distinct sum",
            "SELECT SUM(DISTINCT age) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(52u64)),
        ),
        (
            "distinct avg",
            "SELECT AVG(DISTINCT age) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(26u64)),
        ),
        (
            "distinct min",
            "SELECT MIN(DISTINCT age) FROM SessionSqlEntity",
            Value::Uint(20),
        ),
        (
            "distinct max",
            "SELECT MAX(DISTINCT age) FROM SessionSqlEntity",
            Value::Uint(32),
        ),
    ];

    for (context, sql, expected) in cases {
        assert_sql_aggregate_value_case(&session, sql, expected, context);
    }
}

#[test]
fn execute_sql_aggregate_window_matrix_returns_expected_values() {
    // Phase 1: keep the aggregate window semantics table-driven so bounded
    // windows and offset-empty windows stay covered under one contract.
    let cases = [
        (
            "bounded aggregate window",
            vec![
                ("window-a", 10_u64),
                ("window-b", 20_u64),
                ("window-c", 30_u64),
            ],
            vec![
                (
                    "SELECT COUNT(*) FROM SessionSqlEntity ORDER BY age DESC LIMIT 2 OFFSET 1",
                    Value::Uint(2),
                ),
                (
                    "SELECT SUM(age) FROM SessionSqlEntity ORDER BY age DESC LIMIT 1 OFFSET 1",
                    Value::Decimal(crate::types::Decimal::from(20u64)),
                ),
                (
                    "SELECT AVG(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 2 OFFSET 1",
                    Value::Decimal(crate::types::Decimal::from(25u64)),
                ),
            ],
        ),
        (
            "offset beyond bounded window",
            vec![("beyond-window-a", 10_u64), ("beyond-window-b", 20_u64)],
            vec![
                (
                    "SELECT COUNT(*) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
                    Value::Uint(0),
                ),
                (
                    "SELECT SUM(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
                    Value::Null,
                ),
                (
                    "SELECT AVG(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
                    Value::Null,
                ),
                (
                    "SELECT MIN(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
                    Value::Null,
                ),
                (
                    "SELECT MAX(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
                    Value::Null,
                ),
            ],
        ),
    ];

    // Phase 2: seed and assert each aggregate window case independently so
    // setup stays local to the expected window semantics.
    for (context, seed_rows, assertions) in cases {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_sql_entities(&session, seed_rows.as_slice());

        for (sql, expected) in assertions {
            assert_sql_aggregate_value_case(&session, sql, expected, context);
        }
    }
}

#[test]
fn execute_sql_statement_global_aggregate_payload_matrix_preserves_projection_labels() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_columns, context) in [
        (
            "SELECT COUNT(*) FROM SessionSqlEntity",
            vec!["COUNT(*)".to_string()],
            "plain global aggregate statement payload",
        ),
        (
            "SELECT COUNT(*) AS total_rows FROM SessionSqlEntity",
            vec!["total_rows".to_string()],
            "aliased global aggregate statement payload",
        ),
    ] {
        let payload = session
            .execute_sql_statement::<SessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

        let SqlStatementResult::Projection {
            columns,
            rows,
            row_count,
        } = payload
        else {
            panic!("{context} should return projection payload");
        };

        assert_eq!(
            columns, expected_columns,
            "{context} should preserve aggregate projection labels",
        );
        assert_eq!(
            rows,
            vec![vec![Value::Uint(0)]],
            "{context} should preserve empty-store scalar aggregate value",
        );
        assert_eq!(
            row_count, 1,
            "{context} should expose one scalar aggregate row",
        );
    }
}

#[test]
fn execute_sql_aggregate_matrix_queries_match_expected_values() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows used by aggregate matrix queries.
    seed_session_sql_entities(
        &session,
        &[
            ("agg-matrix-a", 10),
            ("agg-matrix-b", 10),
            ("agg-matrix-c", 20),
            ("agg-matrix-d", 30),
            ("agg-matrix-e", 30),
            ("agg-matrix-f", 30),
        ],
    );

    // Phase 2: execute table-driven aggregate SQL cases.
    let cases = vec![
        ("SELECT COUNT(*) FROM SessionSqlEntity", Value::Uint(6)),
        (
            "SELECT SUM(age) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(130_u64)),
        ),
        (
            "SELECT AVG(age) FROM SessionSqlEntity ORDER BY age DESC LIMIT 2",
            Value::Decimal(crate::types::Decimal::from(30_u64)),
        ),
        (
            "SELECT MIN(age) FROM SessionSqlEntity WHERE age >= 20",
            Value::Uint(20),
        ),
        (
            "SELECT MAX(age) FROM SessionSqlEntity WHERE age <= 20",
            Value::Uint(20),
        ),
        (
            "SELECT COUNT(*) FROM SessionSqlEntity WHERE age < 0",
            Value::Uint(0),
        ),
        (
            "SELECT SUM(age) FROM SessionSqlEntity WHERE age < 0",
            Value::Null,
        ),
    ];

    // Phase 3: assert aggregate outputs for each SQL input.
    for (sql, expected_value) in cases {
        assert_sql_aggregate_value_case(&session, sql, expected_value, sql);
    }
}

#[test]
fn sql_aggregate_unknown_target_field_matrix_stays_fail_closed() {
    reset_session_sql_store();
    let session = sql_session();

    let cases = [
        (
            statement_projection_rows::<SessionSqlEntity>(
                &session,
                "SELECT SUM(missing_field) FROM SessionSqlEntity",
            )
            .map(|_| ()),
            "execute_sql_aggregate unknown target field",
        ),
        (
            statement_explain_sql::<SessionSqlEntity>(
                &session,
                "EXPLAIN EXECUTION SELECT SUM(missing_field) FROM SessionSqlEntity",
            )
            .map(|_| ()),
            "global aggregate EXPLAIN unknown target field",
        ),
    ];

    for (result, context) in cases {
        let err = result.expect_err("unknown aggregate target field should fail");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "{context} should map to unsupported execution error boundary",
        );
    }
}

#[test]
fn explain_sql_global_aggregate_surface_matrix_returns_expected_tokens() {
    reset_session_sql_store();
    let session = sql_session();

    let cases = [
        (
            "logical aggregate explain",
            "EXPLAIN SELECT COUNT(*) FROM SessionSqlEntity",
            vec!["mode=Load", "access="],
            false,
        ),
        (
            "execution aggregate explain",
            "EXPLAIN EXECUTION SELECT COUNT(*) FROM SessionSqlEntity",
            vec!["AggregateCount execution_mode=", "node_id=0"],
            false,
        ),
        (
            "json aggregate explain",
            "EXPLAIN JSON SELECT COUNT(*) FROM SessionSqlEntity",
            vec!["\"mode\":{\"type\":\"Load\""],
            true,
        ),
    ];

    for (context, sql, tokens, require_json_object) in cases {
        assert_global_aggregate_explain_case(
            &session,
            sql,
            tokens.as_slice(),
            require_json_object,
            context,
        );
    }

    let qualified = statement_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN JSON SELECT SUM(SessionSqlEntity.age) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21",
    )
    .expect("qualified global aggregate EXPLAIN JSON should succeed");
    let unqualified = statement_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN JSON SELECT SUM(age) FROM SessionSqlEntity WHERE age >= 21",
    )
    .expect("unqualified global aggregate EXPLAIN JSON should succeed");

    assert_eq!(
        qualified, unqualified,
        "qualified identifiers should normalize to the same global aggregate EXPLAIN JSON output",
    );
}
