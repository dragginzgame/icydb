use super::*;

// Execute one identifier-normalization EXPLAIN pair and assert both spellings
// collapse onto the same public output.
fn assert_explain_identifier_normalization_case(
    session: &DbSession<SessionSqlCanister>,
    lhs_sql: &str,
    rhs_sql: &str,
    context: &str,
) {
    let lhs = statement_explain_sql::<SessionSqlEntity>(session, lhs_sql)
        .unwrap_or_else(|err| panic!("{context} left-hand SQL should succeed: {err}"));
    let rhs = statement_explain_sql::<SessionSqlEntity>(session, rhs_sql)
        .unwrap_or_else(|err| panic!("{context} right-hand SQL should succeed: {err}"));

    assert_eq!(
        lhs, rhs,
        "{context} identifier spelling should normalize to the same EXPLAIN output",
    );
}

// Execute one EXPLAIN equivalence pair and assert both SQL spellings preserve
// the same public explain output.
fn assert_explain_equivalence_case<E>(
    session: &DbSession<SessionSqlCanister>,
    left_sql: &str,
    right_sql: &str,
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister> + crate::traits::EntityValue,
{
    let left = statement_explain_sql::<E>(session, left_sql)
        .unwrap_or_else(|err| panic!("{context} left SQL should succeed: {err}"));
    let right = statement_explain_sql::<E>(session, right_sql)
        .unwrap_or_else(|err| panic!("{context} right SQL should succeed: {err}"));

    assert_eq!(
        left, right,
        "{context} should normalize to the same EXPLAIN output",
    );
}

fn assert_explain_json_index_range_case(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    tokens: &[&str],
    context: &str,
) {
    let explain = statement_explain_sql::<IndexedSessionSqlEntity>(session, sql)
        .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

    assert!(
        explain.starts_with('{') && explain.ends_with('}'),
        "{context} should be one JSON object payload",
    );
    assert_explain_contains_tokens(explain.as_str(), tokens, context);
    assert!(
        !explain.contains("\"type\":\"FullScan\""),
        "{context} must not fall back to full scan: {explain}",
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
        let explain = statement_explain_sql::<SessionSqlEntity>(&session, sql)
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
        (
            "EXPLAIN EXECUTION DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1",
            vec!["node_id=0", "layer="],
        ),
    ];

    // Phase 2: execute each EXPLAIN EXECUTION query and assert stable output tokens.
    for (sql, tokens) in cases {
        let explain = statement_explain_sql::<SessionSqlEntity>(&session, sql)
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
        let explain = statement_explain_sql::<SessionSqlEntity>(&session, sql)
            .expect("EXPLAIN JSON matrix query should succeed");
        assert!(
            explain.starts_with('{') && explain.ends_with('}'),
            "explain JSON matrix output should be one JSON object payload: {sql}",
        );
        assert_explain_contains_tokens(explain.as_str(), tokens.as_slice(), sql);
    }
}

#[test]
fn explain_sql_delete_rejection_matrix_preserves_unsupported_feature_detail() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, feature, context) in [
        (
            "EXPLAIN DELETE FROM SessionSqlEntity WHERE STARTS_WITH(TRIM(name), 'Al') ORDER BY age ASC LIMIT 1",
            "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers",
            "EXPLAIN DELETE non-casefold wrapped STARTS_WITH",
        ),
        (
            "EXPLAIN JSON DELETE FROM SessionSqlEntity WHERE STARTS_WITH(TRIM(name), 'Al') ORDER BY age ASC LIMIT 1",
            "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers",
            "EXPLAIN JSON DELETE non-casefold wrapped STARTS_WITH",
        ),
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity JOIN other ON SessionSqlEntity.id = other.id",
            "JOIN",
            "EXPLAIN JOIN",
        ),
        (
            "EXPLAIN JSON SELECT * FROM SessionSqlEntity JOIN other ON SessionSqlEntity.id = other.id",
            "JOIN",
            "EXPLAIN JSON JOIN",
        ),
    ] {
        let err = statement_explain_sql::<SessionSqlEntity>(&session, sql)
            .expect_err("unsupported EXPLAIN feature should stay fail-closed");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "{context} should fail through the unsupported SQL boundary",
        );
        assert_sql_unsupported_feature_detail(err, feature);
    }
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
        let direct = statement_explain_sql::<IndexedSessionSqlEntity>(&session, direct_sql)
            .expect("direct STARTS_WITH delete EXPLAIN should succeed");
        let like = statement_explain_sql::<IndexedSessionSqlEntity>(&session, like_sql)
            .expect("LIKE delete EXPLAIN should succeed");

        assert_eq!(
            direct, like,
            "bounded direct STARTS_WITH delete EXPLAIN should match the established LIKE path: {context}",
        );
    }
}

#[test]
fn explain_sql_delete_direct_text_range_matrix_preserves_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (sql, tokens, context) in [
        (
            "EXPLAIN DELETE FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC LIMIT 2",
            &[
                "mode=Delete",
                "access=IndexRange",
                "predicate=And([Compare",
                "op: Lt, value: Text(\"T\")",
                "op: Gte, value: Text(\"S\")",
                "id: TextCasefold",
            ][..],
            "direct UPPER(field) ordered text-range delete EXPLAIN",
        ),
        (
            "EXPLAIN DELETE FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC LIMIT 2",
            &[
                "mode=Delete",
                "access=IndexRange",
                "predicate=And([Compare",
                "op: Lt, value: Text(\"t\")",
                "op: Gte, value: Text(\"s\")",
                "id: TextCasefold",
            ][..],
            "direct LOWER(field) ordered text-range delete EXPLAIN",
        ),
    ] {
        let explain = statement_explain_sql::<IndexedSessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

        assert_explain_contains_tokens(
            explain.as_str(),
            tokens,
            &format!("{context} should preserve the shared expression index-range route"),
        );
        assert!(
            !explain.contains("access=FullScan"),
            "{context} must not fall back to full scan: {explain}",
        );
    }
}

#[test]
fn explain_json_sql_direct_text_range_matrix_preserves_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (sql, context) in [
        (
            "EXPLAIN JSON SELECT name FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC",
            "direct UPPER(field) ordered text-range JSON EXPLAIN",
        ),
        (
            "EXPLAIN JSON SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC",
            "direct LOWER(field) ordered text-range JSON EXPLAIN",
        ),
    ] {
        let explain = statement_explain_sql::<IndexedSessionSqlEntity>(&session, sql)
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
                "\"predicate\":\"And([Compare",
                "id: TextCasefold",
            ],
            &format!("{context} should preserve the shared expression index-range route"),
        );
        assert!(
            !explain.contains("\"type\":\"FullScan\""),
            "{context} must not fall back to full scan: {explain}",
        );
    }
}

#[test]
fn explain_json_sql_direct_equivalent_prefix_matrix_preserves_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (sql, context) in [
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
    ] {
        assert_explain_json_index_range_case(
            &session,
            sql,
            &[
                "\"mode\":{\"type\":\"Load\"",
                "\"access\":{\"type\":\"IndexRange\"",
            ],
            context,
        );
    }
}

#[test]
fn explain_json_sql_delete_direct_text_range_matrix_preserves_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (sql, context) in [
        (
            "EXPLAIN JSON DELETE FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC LIMIT 2",
            "direct UPPER(field) ordered text-range JSON delete EXPLAIN",
        ),
        (
            "EXPLAIN JSON DELETE FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC LIMIT 2",
            "direct LOWER(field) ordered text-range JSON delete EXPLAIN",
        ),
    ] {
        let explain = statement_explain_sql::<IndexedSessionSqlEntity>(&session, sql)
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
                "\"predicate\":\"And([Compare",
                "id: TextCasefold",
            ],
            &format!("{context} should preserve the shared expression index-range route"),
        );
        assert!(
            !explain.contains("\"type\":\"FullScan\""),
            "{context} must not fall back to full scan: {explain}",
        );
    }
}

#[test]
fn explain_json_sql_delete_direct_equivalent_prefix_matrix_preserves_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (sql, context) in [
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
    ] {
        assert_explain_json_index_range_case(
            &session,
            sql,
            &[
                "\"mode\":{\"type\":\"Delete\"",
                "\"access\":{\"type\":\"IndexRange\"",
            ],
            context,
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
fn explain_sql_distinct_surface_matrix_returns_expected_tokens() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, tokens, context) in [
        (
            "EXPLAIN EXECUTION SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
            &["node_id=0"][..],
            "execution explain distinct star",
        ),
        (
            "EXPLAIN SELECT DISTINCT age FROM SessionSqlEntity",
            &["distinct=true"][..],
            "logical explain distinct scalar projection",
        ),
    ] {
        let explain = statement_explain_sql::<SessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));
        assert_explain_contains_tokens(explain.as_str(), tokens, context);
    }
}

#[test]
fn explain_sql_alias_normalization_matrix_matches_canonical_plan_output() {
    reset_session_sql_store();
    let session = sql_session();
    for (aliased_sql, canonical_sql, context) in [
        (
            "EXPLAIN SELECT name AS display_name FROM SessionSqlEntity ORDER BY age LIMIT 1",
            "EXPLAIN SELECT name FROM SessionSqlEntity ORDER BY age LIMIT 1",
            "projection aliases",
        ),
        (
            "EXPLAIN SELECT name AS display_name FROM SessionSqlEntity ORDER BY display_name ASC LIMIT 1",
            "EXPLAIN SELECT name FROM SessionSqlEntity ORDER BY name ASC LIMIT 1",
            "ORDER BY field aliases",
        ),
    ] {
        assert_session_sql_alias_matches_canonical::<String>(
            &session,
            statement_explain_sql::<SessionSqlEntity>,
            aliased_sql,
            canonical_sql,
            context,
        );
    }
    reset_indexed_session_sql_store();
    let indexed_session = indexed_sql_session();
    assert_session_sql_alias_matches_canonical::<String>(
        &indexed_session,
        statement_explain_sql::<ExpressionIndexedSessionSqlEntity>,
        "EXPLAIN SELECT LOWER(name) AS normalized_name FROM ExpressionIndexedSessionSqlEntity ORDER BY normalized_name ASC LIMIT 1",
        "EXPLAIN SELECT LOWER(name) FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC LIMIT 1",
        "ORDER BY LOWER(field) aliases",
    );
}

#[test]
fn explain_sql_rejects_order_by_alias_for_unsupported_target_family() {
    reset_session_sql_store();
    let session = sql_session();

    assert_session_sql_order_by_alias_unsupported::<String>(
        &session,
        statement_explain_sql::<SessionSqlEntity>,
        "EXPLAIN SELECT TRIM(name) AS trimmed_name FROM SessionSqlEntity ORDER BY trimmed_name ASC LIMIT 1",
        "unsupported ORDER BY alias targets",
    );
}

#[test]
fn explain_sql_accepts_order_by_bounded_numeric_aliases() {
    reset_session_sql_store();
    let session = sql_session();

    for sql in [
        "EXPLAIN SELECT age + 1 AS next_age FROM SessionSqlEntity ORDER BY next_age ASC LIMIT 1",
        "EXPLAIN SELECT ROUND(age / 3, 2) AS rounded_age FROM SessionSqlEntity ORDER BY rounded_age DESC LIMIT 1",
    ] {
        let explain = statement_explain_sql::<SessionSqlEntity>(&session, sql)
            .expect("bounded numeric ORDER BY aliases should explain");

        assert!(
            explain.contains("mode=Load"),
            "bounded numeric ORDER BY alias explain should still render the base load plan",
        );
        assert!(
            explain.contains("access="),
            "bounded numeric ORDER BY alias explain should still render one routed access shape",
        );
    }

    for sql in [
        "EXPLAIN SELECT rank + rank AS total FROM SessionAggregateEntity ORDER BY total ASC LIMIT 1",
        "EXPLAIN SELECT ROUND(rank + rank, 2) AS rounded_total FROM SessionAggregateEntity ORDER BY rounded_total DESC LIMIT 1",
    ] {
        let explain = statement_explain_sql::<SessionAggregateEntity>(&session, sql)
            .expect("bounded numeric ORDER BY aliases should explain");

        assert!(
            explain.contains("mode=Load"),
            "bounded numeric ORDER BY alias explain should still render the base load plan",
        );
        assert!(
            explain.contains("access="),
            "bounded numeric ORDER BY alias explain should still render one routed access shape",
        );
    }
}

#[test]
fn explain_sql_accepts_direct_bounded_numeric_order_terms() {
    reset_session_sql_store();
    let session = sql_session();

    for sql in [
        "EXPLAIN SELECT age FROM SessionSqlEntity ORDER BY age + 1 ASC LIMIT 1",
        "EXPLAIN SELECT age FROM SessionSqlEntity ORDER BY ROUND(age / 3, 2) DESC LIMIT 1",
    ] {
        let explain = statement_explain_sql::<SessionSqlEntity>(&session, sql)
            .expect("direct bounded numeric ORDER BY terms should explain");

        assert!(
            explain.contains("mode=Load"),
            "direct bounded numeric ORDER BY explain should still render the base load plan",
        );
        assert!(
            explain.contains("access="),
            "direct bounded numeric ORDER BY explain should still render one routed access shape",
        );
    }
}

#[test]
fn explain_sql_computed_text_projection_matrix_preserves_surface_contracts() {
    reset_session_sql_store();
    let session = sql_session();

    let scalar_explain = statement_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT TRIM(name) FROM SessionSqlEntity ORDER BY age LIMIT 1",
    )
    .expect("EXPLAIN should support computed text projection on the narrowed statement lane");
    assert!(
        scalar_explain.contains("mode=Load"),
        "computed text projection explain should still render the base load plan",
    );
    assert!(
        scalar_explain.contains("access="),
        "computed text projection explain should still expose the routed access shape",
    );

    let grouped_err = statement_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT TRIM(name), COUNT(*) \
         FROM SessionSqlEntity \
         GROUP BY name \
         ORDER BY name ASC LIMIT 10",
    )
    .expect_err("EXPLAIN should stay fail-closed for grouped computed text projection");
    assert!(
        matches!(
            grouped_err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "grouped computed SQL projection explain should remain rejected",
    );

    let (left_sql, right_sql, context) = (
        "EXPLAIN SELECT DISTINCT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
        "EXPLAIN SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
        "top-level grouped SELECT DISTINCT explain",
    );
    assert_explain_equivalence_case::<SessionSqlEntity>(&session, left_sql, right_sql, context);
}

#[test]
fn explain_sql_rejects_non_explain_statements() {
    reset_session_sql_store();
    let session = sql_session();

    let err = statement_explain_sql::<SessionSqlEntity>(&session, "SELECT * FROM SessionSqlEntity")
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

#[test]
fn explain_sql_field_to_field_predicate_stays_visible_in_predicate_tree() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let explain = statement_explain_sql::<SessionDeterministicRangeEntity>(
        &session,
        "EXPLAIN JSON SELECT label \
         FROM SessionDeterministicRangeEntity \
         WHERE tier = 'gold' AND score > 18 AND handle > label \
         ORDER BY score ASC, id ASC",
    )
    .expect("mixed literal and field-to-field EXPLAIN JSON should succeed");

    assert_explain_contains_tokens(
        explain.as_str(),
        &[
            "\"predicate\":\"And([Compare",
            "CompareFields { left_field: \\\"handle\\\", op: Gt, right_field: \\\"label\\\"",
        ],
        "field-to-field explain should keep the compare-fields predicate shape visible",
    );
}
