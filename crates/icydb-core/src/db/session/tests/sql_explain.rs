use super::*;

#[test]
fn explain_sql_plan_matrix_queries_include_expected_tokens() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define table-driven EXPLAIN plan SQL cases.
    let cases = vec![
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
            vec!["mode=Load", "access="],
        ),
        (
            "EXPLAIN SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC",
            vec!["mode=Load", "distinct=true"],
        ),
        (
            "EXPLAIN SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec!["mode=Load", "grouping=Grouped"],
        ),
        (
            "EXPLAIN DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1",
            vec!["mode=Delete", "access="],
        ),
        (
            "EXPLAIN SELECT COUNT(*) FROM SessionSqlEntity",
            vec!["mode=Load", "access="],
        ),
    ];

    // Phase 2: execute each EXPLAIN plan query and assert stable output tokens.
    for (sql, tokens) in cases {
        let explain = dispatch_explain_sql::<SessionSqlEntity>(&session, sql)
            .expect("EXPLAIN plan matrix query should succeed");
        assert_explain_contains_tokens(explain.as_str(), tokens.as_slice(), sql);
    }
}

#[test]
fn explain_sql_execution_matrix_queries_include_expected_tokens() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define table-driven EXPLAIN EXECUTION SQL cases.
    let cases = vec![
        (
            "EXPLAIN EXECUTION SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
            vec!["node_id=0", "layer="],
        ),
        (
            "EXPLAIN EXECUTION SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec!["node_id=0", "execution_mode="],
        ),
        (
            "EXPLAIN EXECUTION SELECT COUNT(*) FROM SessionSqlEntity",
            vec!["AggregateCount execution_mode=", "node_id=0"],
        ),
    ];

    // Phase 2: execute each EXPLAIN EXECUTION query and assert stable output tokens.
    for (sql, tokens) in cases {
        let explain = dispatch_explain_sql::<SessionSqlEntity>(&session, sql)
            .expect("EXPLAIN EXECUTION matrix query should succeed");
        assert_explain_contains_tokens(explain.as_str(), tokens.as_slice(), sql);
    }
}

#[test]
fn explain_sql_json_matrix_queries_include_expected_tokens() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define table-driven EXPLAIN JSON SQL cases.
    let cases = vec![
        (
            "EXPLAIN JSON SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
            vec!["\"mode\":{\"type\":\"Load\"", "\"access\":"],
        ),
        (
            "EXPLAIN JSON SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC",
            vec!["\"mode\":{\"type\":\"Load\"", "\"distinct\":true"],
        ),
        (
            "EXPLAIN JSON SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec!["\"mode\":{\"type\":\"Load\"", "\"grouping\""],
        ),
        (
            "EXPLAIN JSON DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1",
            vec!["\"mode\":{\"type\":\"Delete\"", "\"access\":"],
        ),
        (
            "EXPLAIN JSON SELECT COUNT(*) FROM SessionSqlEntity",
            vec!["\"mode\":{\"type\":\"Load\"", "\"access\":"],
        ),
    ];

    // Phase 2: execute each EXPLAIN JSON query and assert stable output tokens.
    for (sql, tokens) in cases {
        let explain = dispatch_explain_sql::<SessionSqlEntity>(&session, sql)
            .expect("EXPLAIN JSON matrix query should succeed");
        assert!(
            explain.starts_with('{') && explain.ends_with('}'),
            "explain JSON matrix output should be one JSON object payload: {sql}",
        );
        assert_explain_contains_tokens(explain.as_str(), tokens.as_slice(), sql);
    }
}

#[test]
fn explain_sql_execution_returns_descriptor_text() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
    )
    .expect("EXPLAIN EXECUTION should succeed");

    assert!(
        explain.contains("node_id=0"),
        "execution explain output should include the root descriptor node id",
    );
    assert!(
        explain.contains("layer="),
        "execution explain output should include execution layer annotations",
    );
}

#[test]
fn explain_sql_plan_returns_logical_plan_text() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
    )
    .expect("EXPLAIN should succeed");

    assert!(
        explain.contains("mode=Load"),
        "logical explain text should include query mode projection",
    );
    assert!(
        explain.contains("access="),
        "logical explain text should include projected access shape",
    );
}

#[test]
fn explain_sql_delete_rejects_non_casefold_wrapped_direct_starts_with() {
    reset_session_sql_store();
    let session = sql_session();

    let err = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN DELETE FROM SessionSqlEntity WHERE STARTS_WITH(TRIM(name), 'Al') ORDER BY age ASC LIMIT 1",
    )
    .expect_err("non-casefold direct STARTS_WITH delete EXPLAIN should stay fail-closed");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "EXPLAIN DELETE should reject non-casefold wrapped direct STARTS_WITH",
    );
    assert_sql_unsupported_feature_detail(
        err,
        "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers",
    );
}

#[test]
fn explain_json_sql_delete_rejects_non_casefold_wrapped_direct_starts_with() {
    reset_session_sql_store();
    let session = sql_session();

    let err = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN JSON DELETE FROM SessionSqlEntity WHERE STARTS_WITH(TRIM(name), 'Al') ORDER BY age ASC LIMIT 1",
    )
    .expect_err("non-casefold direct STARTS_WITH JSON delete EXPLAIN should stay fail-closed");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "EXPLAIN JSON DELETE should reject non-casefold wrapped direct STARTS_WITH",
    );
    assert_sql_unsupported_feature_detail(
        err,
        "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers",
    );
}

#[test]
fn explain_sql_delete_direct_starts_with_family_matches_like_output() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: compare the accepted direct family against the established LIKE
    // family so EXPLAIN stays honest and surface-coherent for delete routes too.
    let cases = [
        (
            "EXPLAIN DELETE FROM IndexedSessionSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name ASC LIMIT 2",
            "EXPLAIN DELETE FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC LIMIT 2",
            "strict direct STARTS_WITH delete explain",
        ),
        (
            "EXPLAIN DELETE FROM IndexedSessionSqlEntity WHERE STARTS_WITH(LOWER(name), 's') ORDER BY name ASC LIMIT 2",
            "EXPLAIN DELETE FROM IndexedSessionSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC LIMIT 2",
            "direct LOWER(field) STARTS_WITH delete explain",
        ),
        (
            "EXPLAIN DELETE FROM IndexedSessionSqlEntity WHERE STARTS_WITH(UPPER(name), 'S') ORDER BY name ASC LIMIT 2",
            "EXPLAIN DELETE FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC LIMIT 2",
            "direct UPPER(field) STARTS_WITH delete explain",
        ),
    ];

    // Phase 2: assert the logical plan text remains the same across both
    // spellings, proving the accepted direct family reuses the same delete path.
    for (direct_sql, like_sql, context) in cases {
        let direct = dispatch_explain_sql::<IndexedSessionSqlEntity>(&session, direct_sql)
            .expect("direct STARTS_WITH delete EXPLAIN should succeed");
        let like = dispatch_explain_sql::<IndexedSessionSqlEntity>(&session, like_sql)
            .expect("LIKE delete EXPLAIN should succeed");

        assert_eq!(
            direct, like,
            "bounded direct STARTS_WITH delete EXPLAIN should match the established LIKE path: {context}",
        );
    }
}

#[test]
fn explain_sql_plan_qualified_identifiers_match_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let qualified = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT * \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             ORDER BY SessionSqlEntity.age DESC LIMIT 1",
    )
    .expect("qualified EXPLAIN plan SQL should succeed");
    let unqualified = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT * \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             ORDER BY age DESC LIMIT 1",
    )
    .expect("unqualified EXPLAIN plan SQL should succeed");

    assert_eq!(
        qualified, unqualified,
        "qualified identifiers should normalize to the same logical EXPLAIN plan output",
    );
}

#[test]
fn explain_sql_execution_qualified_identifiers_match_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let qualified = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT SessionSqlEntity.name \
             FROM SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             ORDER BY SessionSqlEntity.age DESC LIMIT 1",
    )
    .expect("qualified EXPLAIN execution SQL should succeed");
    let unqualified = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT name \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             ORDER BY age DESC LIMIT 1",
    )
    .expect("unqualified EXPLAIN execution SQL should succeed");

    assert_eq!(
        qualified, unqualified,
        "qualified identifiers should normalize to the same execution EXPLAIN descriptor output",
    );
}

#[test]
fn explain_sql_plan_select_distinct_star_marks_distinct_true() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC",
    )
    .expect("EXPLAIN SELECT DISTINCT * should succeed");

    assert!(
        explain.contains("distinct=true"),
        "logical explain text should preserve scalar distinct intent",
    );
}

#[test]
fn explain_sql_execution_select_distinct_star_returns_execution_descriptor_text() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
    )
    .expect("EXPLAIN EXECUTION SELECT DISTINCT * should succeed");

    assert!(
        explain.contains("node_id=0"),
        "execution explain output should include the root descriptor node id",
    );
}

#[test]
fn explain_sql_json_returns_logical_plan_json() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN JSON SELECT * FROM SessionSqlEntity ORDER BY age LIMIT 1",
    )
    .expect("EXPLAIN JSON should succeed");

    assert!(
        explain.starts_with('{') && explain.ends_with('}'),
        "logical explain JSON should render one JSON object payload",
    );
    assert!(
        explain.contains("\"mode\":{\"type\":\"Load\""),
        "logical explain JSON should expose structured query mode metadata",
    );
    assert!(
        explain.contains("\"access\":"),
        "logical explain JSON should include projected access metadata",
    );
}

#[test]
fn explain_sql_json_select_distinct_star_marks_distinct_true() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN JSON SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC",
    )
    .expect("EXPLAIN JSON SELECT DISTINCT * should succeed");

    assert!(
        explain.contains("\"distinct\":true"),
        "logical explain JSON should preserve scalar distinct intent",
    );
}

#[test]
fn explain_sql_json_delete_returns_logical_delete_mode() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN JSON DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1",
    )
    .expect("EXPLAIN JSON DELETE should succeed");

    assert!(
        explain.contains("\"mode\":{\"type\":\"Delete\""),
        "logical explain JSON should expose delete query mode metadata",
    );
}

#[test]
fn explain_sql_rejects_distinct_without_pk_projection_in_current_slice() {
    reset_session_sql_store();
    let session = sql_session();

    let err = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT DISTINCT age FROM SessionSqlEntity",
    )
    .expect_err("EXPLAIN SELECT DISTINCT without PK projection should remain fail-closed");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "unsupported DISTINCT explain shape should map to unsupported execution error boundary",
    );
}

#[test]
fn explain_sql_supports_computed_text_projection_in_dispatch_lane() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT TRIM(name) FROM SessionSqlEntity ORDER BY age LIMIT 1",
    );

    let explain = explain
        .expect("EXPLAIN should support computed text projection on the narrowed dispatch lane");
    assert!(
        explain.contains("mode=Load"),
        "computed text projection explain should still render the base load plan",
    );
    assert!(
        explain.contains("access="),
        "computed text projection explain should still expose the routed access shape",
    );
}

#[test]
fn explain_sql_rejects_non_explain_statements() {
    reset_session_sql_store();
    let session = sql_session();

    let err = dispatch_explain_sql::<SessionSqlEntity>(&session, "SELECT * FROM SessionSqlEntity")
        .expect_err("explain_sql must reject non-EXPLAIN statements");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "non-EXPLAIN input must fail as unsupported explain usage",
    );
}
