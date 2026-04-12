use super::*;

// Execute one identifier-normalization EXPLAIN pair and assert both spellings
// collapse onto the same public output.
fn assert_explain_identifier_normalization_case(
    session: &DbSession<SessionSqlCanister>,
    lhs_sql: &str,
    rhs_sql: &str,
    context: &str,
) {
    let lhs = dispatch_explain_sql::<SessionSqlEntity>(session, lhs_sql)
        .unwrap_or_else(|err| panic!("{context} left-hand SQL should succeed: {err}"));
    let rhs = dispatch_explain_sql::<SessionSqlEntity>(session, rhs_sql)
        .unwrap_or_else(|err| panic!("{context} right-hand SQL should succeed: {err}"));

    assert_eq!(
        lhs, rhs,
        "{context} identifier spelling should normalize to the same EXPLAIN output",
    );
}

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
fn explain_sql_rejects_join_as_explicit_unsupported_feature() {
    reset_session_sql_store();
    let session = sql_session();

    let err = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT * FROM SessionSqlEntity JOIN other ON SessionSqlEntity.id = other.id",
    )
    .expect_err("EXPLAIN should reject JOIN as an explicit unsupported SQL feature");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "EXPLAIN JOIN should fail through the unsupported SQL boundary",
    );
    assert_sql_unsupported_feature_detail(err, "JOIN");
}

#[test]
fn explain_json_sql_rejects_join_as_explicit_unsupported_feature() {
    reset_session_sql_store();
    let session = sql_session();

    let err = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN JSON SELECT * FROM SessionSqlEntity JOIN other ON SessionSqlEntity.id = other.id",
    )
    .expect_err("EXPLAIN JSON should reject JOIN as an explicit unsupported SQL feature");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "EXPLAIN JSON JOIN should fail through the unsupported SQL boundary",
    );
    assert_sql_unsupported_feature_detail(err, "JOIN");
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
fn explain_sql_delete_direct_upper_text_range_preserves_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(
        &session,
        "EXPLAIN DELETE FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC LIMIT 2",
    )
    .expect("direct UPPER(field) ordered text-range delete EXPLAIN should succeed");

    assert_explain_contains_tokens(
        explain.as_str(),
        &[
            "mode=Delete",
            "access=IndexRange",
            "predicate=And([Compare",
            "op: Lt, value: Text(\"T\")",
            "op: Gte, value: Text(\"S\")",
            "id: TextCasefold",
        ],
        "direct UPPER(field) ordered text-range delete EXPLAIN should preserve the shared expression index-range route",
    );
    assert!(
        !explain.contains("access=FullScan"),
        "direct UPPER(field) ordered text-range delete EXPLAIN must not fall back to full scan: {explain}",
    );
}

#[test]
fn explain_sql_delete_direct_lower_text_range_preserves_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(
        &session,
        "EXPLAIN DELETE FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC LIMIT 2",
    )
    .expect("direct LOWER(field) ordered text-range delete EXPLAIN should succeed");

    assert_explain_contains_tokens(
        explain.as_str(),
        &[
            "mode=Delete",
            "access=IndexRange",
            "predicate=And([Compare",
            "op: Lt, value: Text(\"t\")",
            "op: Gte, value: Text(\"s\")",
            "id: TextCasefold",
        ],
        "direct LOWER(field) ordered text-range delete EXPLAIN should preserve the shared expression index-range route",
    );
    assert!(
        !explain.contains("access=FullScan"),
        "direct LOWER(field) ordered text-range delete EXPLAIN must not fall back to full scan: {explain}",
    );
}

#[test]
fn explain_json_sql_direct_upper_text_range_preserves_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(
        &session,
        "EXPLAIN JSON SELECT name FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC",
    )
    .expect("direct UPPER(field) ordered text-range JSON EXPLAIN should succeed");

    assert!(
        explain.starts_with('{') && explain.ends_with('}'),
        "direct UPPER(field) ordered text-range JSON EXPLAIN should be one JSON object payload",
    );
    assert_explain_contains_tokens(
        explain.as_str(),
        &[
            "\"mode\":{\"type\":\"Load\"",
            "\"access\":{\"type\":\"IndexRange\"",
            "\"predicate\":\"And([Compare",
            "id: TextCasefold",
        ],
        "direct UPPER(field) ordered text-range JSON EXPLAIN should preserve the shared expression index-range route",
    );
    assert!(
        !explain.contains("\"type\":\"FullScan\""),
        "direct UPPER(field) ordered text-range JSON EXPLAIN must not fall back to full scan: {explain}",
    );
}

#[test]
fn explain_json_sql_direct_lower_text_range_preserves_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(
        &session,
        "EXPLAIN JSON SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC",
    )
    .expect("direct LOWER(field) ordered text-range JSON EXPLAIN should succeed");

    assert!(
        explain.starts_with('{') && explain.ends_with('}'),
        "direct LOWER(field) ordered text-range JSON EXPLAIN should be one JSON object payload",
    );
    assert_explain_contains_tokens(
        explain.as_str(),
        &[
            "\"mode\":{\"type\":\"Load\"",
            "\"access\":{\"type\":\"IndexRange\"",
            "\"predicate\":\"And([Compare",
            "id: TextCasefold",
        ],
        "direct LOWER(field) ordered text-range JSON EXPLAIN should preserve the shared expression index-range route",
    );
    assert!(
        !explain.contains("\"type\":\"FullScan\""),
        "direct LOWER(field) ordered text-range JSON EXPLAIN must not fall back to full scan: {explain}",
    );
}

#[test]
fn explain_json_sql_direct_upper_equivalent_prefix_forms_preserve_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let cases = [
        (
            "EXPLAIN JSON SELECT name FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC",
            "direct UPPER(field) LIKE JSON explain route",
        ),
        (
            "EXPLAIN JSON SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(UPPER(name), 'S') ORDER BY name ASC",
            "direct UPPER(field) STARTS_WITH JSON explain route",
        ),
        (
            "EXPLAIN JSON SELECT name FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC",
            "direct UPPER(field) ordered text-range JSON explain route",
        ),
    ];

    for (sql, context) in cases {
        let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

        assert!(
            explain.starts_with('{') && explain.ends_with('}'),
            "{context} should be one JSON object payload",
        );
        assert_explain_contains_tokens(
            explain.as_str(),
            &[
                "\"mode\":{\"type\":\"Load\"",
                "\"access\":{\"type\":\"IndexRange\"",
            ],
            context,
        );
        assert!(
            !explain.contains("\"type\":\"FullScan\""),
            "{context} must not fall back to full scan: {explain}",
        );
    }
}

#[test]
fn explain_json_sql_direct_lower_equivalent_prefix_forms_preserve_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let cases = [
        (
            "EXPLAIN JSON SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC",
            "direct LOWER(field) LIKE JSON explain route",
        ),
        (
            "EXPLAIN JSON SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(LOWER(name), 's') ORDER BY name ASC",
            "direct LOWER(field) STARTS_WITH JSON explain route",
        ),
        (
            "EXPLAIN JSON SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC",
            "direct LOWER(field) ordered text-range JSON explain route",
        ),
    ];

    for (sql, context) in cases {
        let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

        assert!(
            explain.starts_with('{') && explain.ends_with('}'),
            "{context} should be one JSON object payload",
        );
        assert_explain_contains_tokens(
            explain.as_str(),
            &[
                "\"mode\":{\"type\":\"Load\"",
                "\"access\":{\"type\":\"IndexRange\"",
            ],
            context,
        );
        assert!(
            !explain.contains("\"type\":\"FullScan\""),
            "{context} must not fall back to full scan: {explain}",
        );
    }
}

#[test]
fn explain_json_sql_delete_direct_upper_text_range_preserves_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(
        &session,
        "EXPLAIN JSON DELETE FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC LIMIT 2",
    )
    .expect("direct UPPER(field) ordered text-range JSON delete EXPLAIN should succeed");

    assert!(
        explain.starts_with('{') && explain.ends_with('}'),
        "direct UPPER(field) ordered text-range JSON delete EXPLAIN should be one JSON object payload",
    );
    assert_explain_contains_tokens(
        explain.as_str(),
        &[
            "\"mode\":{\"type\":\"Delete\"",
            "\"access\":{\"type\":\"IndexRange\"",
            "\"predicate\":\"And([Compare",
            "id: TextCasefold",
        ],
        "direct UPPER(field) ordered text-range JSON delete EXPLAIN should preserve the shared expression index-range route",
    );
    assert!(
        !explain.contains("\"type\":\"FullScan\""),
        "direct UPPER(field) ordered text-range JSON delete EXPLAIN must not fall back to full scan: {explain}",
    );
}

#[test]
fn explain_json_sql_delete_direct_lower_text_range_preserves_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(
        &session,
        "EXPLAIN JSON DELETE FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC LIMIT 2",
    )
    .expect("direct LOWER(field) ordered text-range JSON delete EXPLAIN should succeed");

    assert!(
        explain.starts_with('{') && explain.ends_with('}'),
        "direct LOWER(field) ordered text-range JSON delete EXPLAIN should be one JSON object payload",
    );
    assert_explain_contains_tokens(
        explain.as_str(),
        &[
            "\"mode\":{\"type\":\"Delete\"",
            "\"access\":{\"type\":\"IndexRange\"",
            "\"predicate\":\"And([Compare",
            "id: TextCasefold",
        ],
        "direct LOWER(field) ordered text-range JSON delete EXPLAIN should preserve the shared expression index-range route",
    );
    assert!(
        !explain.contains("\"type\":\"FullScan\""),
        "direct LOWER(field) ordered text-range JSON delete EXPLAIN must not fall back to full scan: {explain}",
    );
}

#[test]
fn explain_json_sql_delete_direct_upper_equivalent_prefix_forms_preserve_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let cases = [
        (
            "EXPLAIN JSON DELETE FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC LIMIT 2",
            "direct UPPER(field) LIKE JSON delete explain route",
        ),
        (
            "EXPLAIN JSON DELETE FROM IndexedSessionSqlEntity WHERE STARTS_WITH(UPPER(name), 'S') ORDER BY name ASC LIMIT 2",
            "direct UPPER(field) STARTS_WITH JSON delete explain route",
        ),
        (
            "EXPLAIN JSON DELETE FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC LIMIT 2",
            "direct UPPER(field) ordered text-range JSON delete explain route",
        ),
    ];

    for (sql, context) in cases {
        let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

        assert!(
            explain.starts_with('{') && explain.ends_with('}'),
            "{context} should be one JSON object payload",
        );
        assert_explain_contains_tokens(
            explain.as_str(),
            &[
                "\"mode\":{\"type\":\"Delete\"",
                "\"access\":{\"type\":\"IndexRange\"",
            ],
            context,
        );
        assert!(
            !explain.contains("\"type\":\"FullScan\""),
            "{context} must not fall back to full scan: {explain}",
        );
    }
}

#[test]
fn explain_json_sql_delete_direct_lower_equivalent_prefix_forms_preserve_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let cases = [
        (
            "EXPLAIN JSON DELETE FROM IndexedSessionSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC LIMIT 2",
            "direct LOWER(field) LIKE JSON delete explain route",
        ),
        (
            "EXPLAIN JSON DELETE FROM IndexedSessionSqlEntity WHERE STARTS_WITH(LOWER(name), 's') ORDER BY name ASC LIMIT 2",
            "direct LOWER(field) STARTS_WITH JSON delete explain route",
        ),
        (
            "EXPLAIN JSON DELETE FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC LIMIT 2",
            "direct LOWER(field) ordered text-range JSON delete explain route",
        ),
    ];

    for (sql, context) in cases {
        let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

        assert!(
            explain.starts_with('{') && explain.ends_with('}'),
            "{context} should be one JSON object payload",
        );
        assert_explain_contains_tokens(
            explain.as_str(),
            &[
                "\"mode\":{\"type\":\"Delete\"",
                "\"access\":{\"type\":\"IndexRange\"",
            ],
            context,
        );
        assert!(
            !explain.contains("\"type\":\"FullScan\""),
            "{context} must not fall back to full scan: {explain}",
        );
    }
}

#[test]
fn explain_sql_identifier_normalization_matrix_matches_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let cases = [
        (
            "logical explain qualified identifiers",
            "EXPLAIN SELECT * \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             ORDER BY SessionSqlEntity.age DESC LIMIT 1",
            "EXPLAIN SELECT * \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             ORDER BY age DESC LIMIT 1",
        ),
        (
            "execution explain qualified identifiers",
            "EXPLAIN EXECUTION SELECT SessionSqlEntity.name \
             FROM SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             ORDER BY SessionSqlEntity.age DESC LIMIT 1",
            "EXPLAIN EXECUTION SELECT name \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             ORDER BY age DESC LIMIT 1",
        ),
        (
            "execution explain table alias identifiers",
            "EXPLAIN EXECUTION SELECT alias.name \
             FROM SessionSqlEntity alias \
             WHERE alias.age >= 21 \
             ORDER BY alias.age DESC LIMIT 1",
            "EXPLAIN EXECUTION SELECT name \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             ORDER BY age DESC LIMIT 1",
        ),
    ];

    for (context, lhs_sql, rhs_sql) in cases {
        assert_explain_identifier_normalization_case(&session, lhs_sql, rhs_sql, context);
    }
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
fn explain_sql_supports_distinct_without_pk_projection() {
    reset_session_sql_store();
    let session = sql_session();

    let explain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT DISTINCT age FROM SessionSqlEntity",
    )
    .expect("EXPLAIN SELECT DISTINCT without PK projection should succeed");

    assert!(
        explain.contains("distinct=true"),
        "EXPLAIN SELECT DISTINCT without PK projection should preserve scalar distinct intent",
    );
}

#[test]
fn explain_sql_grouped_top_level_distinct_matches_plain_grouped_output() {
    reset_session_sql_store();
    let session = sql_session();

    let distinct_explain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT DISTINCT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
    )
    .expect("EXPLAIN should support top-level grouped SELECT DISTINCT");
    let plain_explain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
    )
    .expect("EXPLAIN should support plain grouped aggregate projection");

    assert_eq!(
        distinct_explain, plain_explain,
        "top-level grouped SELECT DISTINCT should normalize to the same logical EXPLAIN output as the non-DISTINCT form",
    );
}

#[test]
fn explain_sql_projection_alias_matches_unaliased_plan_output() {
    reset_session_sql_store();
    let session = sql_session();

    let aliased = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT name AS display_name FROM SessionSqlEntity ORDER BY age LIMIT 1",
    )
    .expect("EXPLAIN should accept projection aliases");
    let plain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT name FROM SessionSqlEntity ORDER BY age LIMIT 1",
    )
    .expect("EXPLAIN should accept the unaliased projection");

    assert_eq!(
        aliased, plain,
        "projection aliases should stay presentation-only and not affect EXPLAIN output",
    );
}

#[test]
fn explain_sql_order_by_field_alias_matches_canonical_plan_output() {
    reset_session_sql_store();
    let session = sql_session();

    let aliased = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT name AS display_name FROM SessionSqlEntity ORDER BY display_name ASC LIMIT 1",
    )
    .expect("EXPLAIN should accept ORDER BY field aliases");
    let canonical = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT name FROM SessionSqlEntity ORDER BY name ASC LIMIT 1",
    )
    .expect("EXPLAIN should accept the canonical field ORDER BY target");

    assert_eq!(
        aliased, canonical,
        "ORDER BY field aliases should normalize away before EXPLAIN output is rendered",
    );
}

#[test]
fn explain_sql_rejects_order_by_alias_for_unsupported_target_family() {
    reset_session_sql_store();
    let session = sql_session();

    let err = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT TRIM(name) AS trimmed_name FROM SessionSqlEntity ORDER BY trimmed_name ASC LIMIT 1",
    )
    .expect_err("EXPLAIN should keep unsupported ORDER BY alias targets fail-closed");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "unsupported ORDER BY alias targets must fail at the EXPLAIN SQL boundary",
    );
    assert!(
        err.to_string()
            .contains("ORDER BY alias 'trimmed_name' does not resolve to a supported order target"),
        "unsupported ORDER BY alias failure should explain the narrowed alias-order boundary",
    );
}

#[test]
fn explain_sql_order_by_lower_alias_matches_canonical_plan_output() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let aliased = dispatch_explain_sql::<ExpressionIndexedSessionSqlEntity>(
        &session,
        "EXPLAIN SELECT LOWER(name) AS normalized_name FROM ExpressionIndexedSessionSqlEntity ORDER BY normalized_name ASC LIMIT 1",
    )
    .expect("EXPLAIN should accept ORDER BY LOWER(field) aliases on the computed projection lane");
    let canonical = dispatch_explain_sql::<ExpressionIndexedSessionSqlEntity>(
        &session,
        "EXPLAIN SELECT LOWER(name) FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC LIMIT 1",
    )
    .expect("EXPLAIN should accept the canonical LOWER(field) order target");

    assert_eq!(
        aliased, canonical,
        "ORDER BY LOWER(field) aliases should normalize away before EXPLAIN output is rendered",
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
fn explain_sql_grouped_computed_text_projection_matches_base_grouped_output() {
    reset_session_sql_store();
    let session = sql_session();

    let computed_explain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT TRIM(name), COUNT(*) \
         FROM SessionSqlEntity \
         GROUP BY name \
         ORDER BY name ASC LIMIT 10",
    )
    .expect("EXPLAIN should support grouped computed text projection on the session-owned lane");
    let base_explain = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT name, COUNT(*) \
         FROM SessionSqlEntity \
         GROUP BY name \
         ORDER BY name ASC LIMIT 10",
    )
    .expect("EXPLAIN should support the rewritten base grouped query");

    assert_eq!(
        computed_explain, base_explain,
        "grouped computed SQL projection explain should stay on the rewritten base grouped query",
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
