use super::*;

// Execute one aggregate SQL case and assert the scalar aggregate value stays
// stable for that query spelling.
fn assert_sql_aggregate_value_case(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected: Value,
    context: &str,
) {
    let actual = session
        .execute_sql_aggregate::<SessionSqlEntity>(sql)
        .unwrap_or_else(|err| panic!("{context} aggregate SQL should execute: {err}"));

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
    let explain = dispatch_explain_sql::<SessionSqlEntity>(session, sql)
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
fn execute_sql_aggregate_honors_order_limit_offset_window() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "window-a".to_string(),
            age: 10,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "window-b".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "window-c".to_string(),
            age: 30,
        })
        .expect("seed insert should succeed");

    let count = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT COUNT(*) FROM SessionSqlEntity ORDER BY age DESC LIMIT 2 OFFSET 1",
        )
        .expect("COUNT(*) SQL aggregate window execution should succeed");
    let sum = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT SUM(age) FROM SessionSqlEntity ORDER BY age DESC LIMIT 1 OFFSET 1",
        )
        .expect("SUM(field) SQL aggregate window execution should succeed");
    let avg = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT AVG(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 2 OFFSET 1",
        )
        .expect("AVG(field) SQL aggregate window execution should succeed");

    assert_eq!(count, Value::Uint(2));
    assert_eq!(sum, Value::Decimal(crate::types::Decimal::from(20u64)));
    assert_eq!(avg, Value::Decimal(crate::types::Decimal::from(25u64)));
}

#[test]
fn execute_sql_aggregate_offset_beyond_window_returns_empty_aggregate_semantics() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed a small scalar window.
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "beyond-window-a".to_string(),
            age: 10,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "beyond-window-b".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");

    // Phase 2: execute aggregates where OFFSET removes all visible rows.
    let count = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT COUNT(*) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
        )
        .expect("COUNT(*) aggregate with offset beyond window should execute");
    let sum = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT SUM(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
        )
        .expect("SUM aggregate with offset beyond window should execute");
    let avg = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT AVG(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
        )
        .expect("AVG aggregate with offset beyond window should execute");
    let min = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT MIN(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
        )
        .expect("MIN aggregate with offset beyond window should execute");
    let max = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT MAX(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 10",
        )
        .expect("MAX aggregate with offset beyond window should execute");

    // Phase 3: assert empty-window aggregate semantics.
    assert_eq!(count, Value::Uint(0));
    assert_eq!(sum, Value::Null);
    assert_eq!(avg, Value::Null);
    assert_eq!(min, Value::Null);
    assert_eq!(max, Value::Null);
}

#[test]
fn execute_sql_dispatch_returns_projection_payload_for_global_aggregate_execution() {
    reset_session_sql_store();
    let session = sql_session();

    let payload = session
        .execute_sql_dispatch::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
        .expect(
            "execute_sql_dispatch should execute global aggregate SQL through projection payload",
        );

    let SqlDispatchResult::Projection {
        columns,
        rows,
        row_count,
    } = payload
    else {
        panic!(
            "execute_sql_dispatch should return one projection payload for global aggregate SQL"
        );
    };

    assert_eq!(
        columns,
        vec!["COUNT(*)".to_string()],
        "global aggregate dispatch payload should preserve aggregate projection label",
    );
    assert_eq!(
        rows,
        vec![vec![Value::Uint(0)]],
        "global aggregate dispatch payload should preserve empty-store scalar aggregate value",
    );
    assert_eq!(
        row_count, 1,
        "global aggregate dispatch payload should expose one scalar aggregate row",
    );
}

#[test]
fn execute_sql_dispatch_global_aggregate_alias_overrides_output_label() {
    reset_session_sql_store();
    let session = sql_session();

    let payload = session
        .execute_sql_dispatch::<SessionSqlEntity>(
            "SELECT COUNT(*) AS total_rows FROM SessionSqlEntity",
        )
        .expect("aliased global aggregate dispatch should succeed");

    let SqlDispatchResult::Projection { columns, .. } = payload else {
        panic!("global aggregate dispatch should return projection payload");
    };

    assert_eq!(columns, vec!["total_rows".to_string()]);
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
        let actual_value = session
            .execute_sql_aggregate::<SessionSqlEntity>(sql)
            .expect("aggregate matrix SQL execution should succeed");

        assert_eq!(actual_value, expected_value, "aggregate matrix case: {sql}");
    }
}

#[test]
fn execute_sql_aggregate_rejects_unsupported_aggregate_shapes() {
    reset_session_sql_store();
    let session = sql_session();
    let sql = "SELECT age FROM SessionSqlEntity";
    let err = session
        .execute_sql_aggregate::<SessionSqlEntity>(sql)
        .expect_err("unsupported SQL aggregate shape should fail closed");
    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "unsupported SQL aggregate shape should map to unsupported execution error boundary: {sql}",
    );
    assert!(
        err.to_string()
            .contains("execute_sql_aggregate requires constrained global aggregate SELECT"),
        "execute_sql_aggregate should preserve a constrained aggregate-surface boundary message: {sql}",
    );
}

#[test]
fn execute_sql_aggregate_rejects_grouped_select_execution_in_current_lane() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
        )
        .expect_err("grouped SQL should stay fail-closed for execute_sql_aggregate");

    assert!(
        err.to_string()
            .contains("execute_sql_aggregate rejects grouped SELECT"),
        "execute_sql_aggregate should preserve explicit grouped-entrypoint guidance",
    );
}

#[test]
fn execute_sql_aggregate_rejects_non_aggregate_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    let cases = [
        (
            "EXPLAIN SELECT COUNT(*) FROM SessionSqlEntity",
            "execute_sql_aggregate rejects EXPLAIN",
        ),
        (
            "DESCRIBE SessionSqlEntity",
            "execute_sql_aggregate rejects DESCRIBE",
        ),
        (
            "SHOW INDEXES SessionSqlEntity",
            "execute_sql_aggregate rejects SHOW INDEXES",
        ),
        (
            "SHOW COLUMNS SessionSqlEntity",
            "execute_sql_aggregate rejects SHOW COLUMNS",
        ),
        (
            "SHOW ENTITIES",
            "execute_sql_aggregate rejects SHOW ENTITIES",
        ),
        (
            "DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1",
            "execute_sql_aggregate rejects DELETE",
        ),
        (
            "INSERT INTO SessionSqlEntity (id, name, age) VALUES (1, 'Ada', 21)",
            "execute_sql_aggregate rejects INSERT",
        ),
        (
            "INSERT INTO SessionSqlEntity (name, age) SELECT name, age FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
            "execute_sql_aggregate rejects INSERT",
        ),
        (
            "UPDATE SessionSqlEntity SET age = 22 WHERE id = 1",
            "execute_sql_aggregate rejects UPDATE",
        ),
    ];

    for (sql, expected) in cases {
        let err = session
            .execute_sql_aggregate::<SessionSqlEntity>(sql)
            .expect_err(
                "non-aggregate statement lanes should stay fail-closed for execute_sql_aggregate",
            );
        assert!(
            err.to_string().contains(expected),
            "execute_sql_aggregate should preserve a surface-local lane boundary message: {sql}",
        );
    }
}

#[test]
fn execute_sql_aggregate_rejects_unknown_target_field() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_aggregate::<SessionSqlEntity>(
            "SELECT SUM(missing_field) FROM SessionSqlEntity",
        )
        .expect_err("unknown aggregate target field should fail");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "unknown aggregate target field should map to unsupported execution error boundary",
    );
}

#[test]
fn explain_sql_json_qualified_aggregate_matches_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let qualified = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN JSON SELECT SUM(SessionSqlEntity.age) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21",
    )
    .expect("qualified global aggregate EXPLAIN JSON should succeed");
    let unqualified = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN JSON SELECT SUM(age) FROM SessionSqlEntity WHERE age >= 21",
    )
    .expect("unqualified global aggregate EXPLAIN JSON should succeed");

    assert_eq!(
        qualified, unqualified,
        "qualified identifiers should normalize to the same global aggregate EXPLAIN JSON output",
    );
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
}

#[test]
fn explain_sql_global_aggregate_rejects_unknown_target_field() {
    reset_session_sql_store();
    let session = sql_session();

    let err = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT SUM(missing_field) FROM SessionSqlEntity",
    )
    .expect_err("global aggregate SQL explain should reject unknown target fields");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "global aggregate SQL explain should map unknown target field to unsupported execution error boundary",
    );
}
