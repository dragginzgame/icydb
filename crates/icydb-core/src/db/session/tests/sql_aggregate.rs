use super::*;

#[test]
fn global_aggregate_value_matrix_matches_expected_values() {
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
        assert_session_sql_scalar_value::<SessionSqlEntity>(&session, sql, expected, context);
    }
}

#[test]
fn global_aggregate_distinct_value_matrix_matches_expected_values() {
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
        assert_session_sql_scalar_value::<SessionSqlEntity>(&session, sql, expected, context);
    }
}

#[test]
fn global_aggregate_expression_input_value_matrix_matches_expected_values() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[("aggregate-expr-a", 20), ("aggregate-expr-b", 32)],
    );

    let cases = [
        (
            "count literal expression",
            "SELECT COUNT(1) FROM SessionSqlEntity",
            Value::Uint(2),
        ),
        (
            "sum arithmetic expression",
            "SELECT SUM(age + 1) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(54u64)),
        ),
        (
            "avg arithmetic expression",
            "SELECT AVG(age + 1) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(27u64)),
        ),
        (
            "avg chained arithmetic expression",
            "SELECT AVG(age + 1 * 2) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(28u64)),
        ),
        (
            "sum constant-folded arithmetic expression",
            "SELECT SUM(2 * 3) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(12u64)),
        ),
        (
            "avg constant-folded round expression",
            "SELECT AVG(ROUND(2 * 3, 1)) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(6u64)),
        ),
        (
            "avg parenthesized arithmetic expression",
            "SELECT ROUND(AVG((age + age) / 2), 2) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::new(2600, 2)),
        ),
        (
            "bounded sum arithmetic expression",
            "SELECT SUM(age + 1) FROM SessionSqlEntity ORDER BY age DESC LIMIT 1 OFFSET 0",
            Value::Decimal(crate::types::Decimal::from(33u64)),
        ),
    ];

    for (context, sql, expected) in cases {
        assert_session_sql_scalar_value::<SessionSqlEntity>(&session, sql, expected, context);
    }
}

#[test]
fn global_post_aggregate_expression_value_matrix_matches_expected_values() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[("aggregate-post-a", 20), ("aggregate-post-b", 21)],
    );

    let cases = [
        (
            "rounded avg",
            "SELECT ROUND(AVG(age), 0) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(21_u64)),
        ),
        (
            "count plus one",
            "SELECT COUNT(*) + 1 FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(3_u64)),
        ),
        (
            "max minus min",
            "SELECT MAX(age) - MIN(age) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(1_u64)),
        ),
    ];

    for (context, sql, expected) in cases {
        assert_session_sql_scalar_value::<SessionSqlEntity>(&session, sql, expected, context);
    }
}

#[test]
fn global_aggregate_case_expression_matrix_matches_expected_values() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[("aggregate-case-a", 20), ("aggregate-case-b", 32)],
    );

    let cases = [
        (
            "sum searched case indicator",
            "SELECT SUM(CASE WHEN age >= 30 THEN 1 ELSE 0 END) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(1_u64)),
        ),
        (
            "avg searched case branch value",
            "SELECT AVG(CASE WHEN age >= 30 THEN age ELSE 0 END) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(16_u64)),
        ),
        (
            "global searched case having",
            "SELECT COUNT(*) \
             FROM SessionSqlEntity \
             HAVING CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1",
            Value::Uint(2),
        ),
    ];

    for (context, sql, expected) in cases {
        assert_session_sql_scalar_value::<SessionSqlEntity>(&session, sql, expected, context);
    }
}

#[test]
fn global_aggregate_filter_value_matrix_matches_expected_values() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("aggregate-filter-a", 20),
            ("aggregate-filter-b", 32),
            ("aggregate-filter-c", 40),
        ],
    );

    let cases = [
        (
            "filtered count star",
            "SELECT COUNT(*) FILTER (WHERE age >= 30) FROM SessionSqlEntity",
            Value::Uint(2),
        ),
        (
            "filtered count field",
            "SELECT COUNT(age) FILTER (WHERE age >= 30) FROM SessionSqlEntity",
            Value::Uint(2),
        ),
        (
            "filtered sum",
            "SELECT SUM(age) FILTER (WHERE age >= 30) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(72_u64)),
        ),
        (
            "filtered avg",
            "SELECT AVG(age) FILTER (WHERE age >= 30) FROM SessionSqlEntity",
            Value::Decimal(crate::types::Decimal::from(36_u64)),
        ),
        (
            "filtered count false window",
            "SELECT COUNT(*) FILTER (WHERE age < 0) FROM SessionSqlEntity",
            Value::Uint(0),
        ),
        (
            "filtered sum empty window",
            "SELECT SUM(age) FILTER (WHERE age < 0) FROM SessionSqlEntity",
            Value::Null,
        ),
    ];

    for (context, sql, expected) in cases {
        assert_session_sql_scalar_value::<SessionSqlEntity>(&session, sql, expected, context);
    }
}

#[test]
fn global_aggregate_filter_case_null_conditions_fall_through_to_later_arms() {
    reset_session_sql_store();
    let session = sql_session();

    seed_nullable_session_sql_entities(
        &session,
        &[
            ("aggregate-filter-case-a", None),
            ("aggregate-filter-case-b", Some("bravo")),
            ("aggregate-filter-case-c", Some("charlie")),
        ],
    );

    assert_session_sql_scalar_value::<SessionNullableSqlEntity>(
        &session,
        "SELECT COUNT(*) FILTER ( \
         WHERE CASE \
           WHEN nickname = 'bravo' THEN TRUE \
           WHEN nickname IS NOT NULL THEN TRUE \
           ELSE FALSE \
         END \
       ) FROM SessionNullableSqlEntity",
        Value::Uint(2),
        "global aggregate FILTER should treat NULL searched-CASE conditions as false and continue to later arms",
    );
}

#[test]
fn global_aggregate_filter_mixed_projection_payload_matches_expected_values() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("aggregate-filter-mixed-a", 20),
            ("aggregate-filter-mixed-b", 32),
            ("aggregate-filter-mixed-c", 40),
        ],
    );

    // Phase 1: execute one mixed global aggregate projection that keeps
    // filtered and unfiltered aggregates in the same reduced row.
    let payload = execute_sql_statement_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) FILTER (WHERE age >= 30) AS filtered_rows, \
         COUNT(*) AS total_rows, \
         SUM(age) FILTER (WHERE age >= 30) AS filtered_sum \
         FROM SessionSqlEntity",
    )
    .expect("mixed filtered and unfiltered global aggregate projection should execute");

    let SqlStatementResult::Projection {
        columns,
        fixed_scales,
        rows,
        row_count,
    } = payload
    else {
        panic!(
            "mixed filtered and unfiltered global aggregate projection should return projection payload"
        );
    };

    // Phase 2: require one stable reduced row with outward labels preserved
    // across both filtered and unfiltered aggregate terminals.
    assert_eq!(
        columns,
        vec![
            "filtered_rows".to_string(),
            "total_rows".to_string(),
            "filtered_sum".to_string(),
        ],
        "mixed filtered and unfiltered global aggregate projection should preserve outward labels",
    );
    assert_eq!(
        fixed_scales,
        vec![None, None, None],
        "mixed filtered and unfiltered global aggregate projection should preserve fixed-scale metadata",
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Uint(2),
            Value::Uint(3),
            Value::Decimal(crate::types::Decimal::from(72_u64)),
        ]],
        "mixed filtered and unfiltered global aggregate projection should preserve distinct filtered and unfiltered aggregate values in the same reduced row",
    );
    assert_eq!(
        row_count, 1,
        "mixed filtered and unfiltered global aggregate projection should expose one reduced row",
    );
}

#[test]
fn global_aggregate_filter_rejection_matrix_stays_fail_closed() {
    reset_session_sql_store();
    let session = sql_session();

    let cases = [
        (
            "DISTINCT + FILTER",
            "SELECT COUNT(DISTINCT age) FILTER (WHERE age >= 30) FROM SessionSqlEntity",
            "unsupported SQL SELECT projection",
        ),
        (
            "alias inside FILTER",
            "SELECT COUNT(*) FILTER (WHERE total_rows > 1) AS total_rows FROM SessionSqlEntity",
            "unknown expression field 'total_rows'",
        ),
    ];

    for (context, sql, expected_message) in cases {
        let err = statement_projection_rows::<SessionSqlEntity>(&session, sql)
            .expect_err("out-of-scope aggregate FILTER shape should stay rejected");

        assert!(
            err.to_string().contains(expected_message),
            "{context} should preserve its fail-closed SQL surface detail",
        );
    }
}

#[test]
fn global_aggregate_having_returns_single_row_when_predicate_matches() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[("aggregate-having-a", 20), ("aggregate-having-b", 21)],
    );

    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) FROM SessionSqlEntity HAVING COUNT(*) > 1",
        Value::Uint(2),
        "global aggregate HAVING should preserve its single reduced row when the predicate matches",
    );
}

#[test]
fn global_aggregate_having_alias_returns_single_row_when_predicate_matches() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("aggregate-having-alias-a", 20),
            ("aggregate-having-alias-b", 21),
        ],
    );

    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) AS total_rows \
         FROM SessionSqlEntity \
         HAVING total_rows > 1",
        Value::Uint(2),
        "aliased global aggregate HAVING should execute through the same single reduced row path",
    );
}

#[test]
fn global_aggregate_having_null_semantics_stay_distinct() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("aggregate-having-null-a", 20),
            ("aggregate-having-null-b", 21),
        ],
    );

    let is_null_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) FROM SessionSqlEntity HAVING COUNT(*) IS NULL",
    )
    .expect("global aggregate HAVING IS NULL should execute");
    let eq_null_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) FROM SessionSqlEntity HAVING COUNT(*) = NULL",
    )
    .expect("global aggregate HAVING = NULL should execute");
    let ne_null_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) FROM SessionSqlEntity HAVING COUNT(*) != NULL",
    )
    .expect("global aggregate HAVING != NULL should execute");

    assert!(
        is_null_rows.is_empty(),
        "global aggregate HAVING IS NULL should reject the implicit group for non-null COUNT(*)",
    );
    assert!(
        eq_null_rows.is_empty(),
        "global aggregate HAVING = NULL should stay unknown and reject the implicit group",
    );
    assert!(
        ne_null_rows.is_empty(),
        "global aggregate HAVING != NULL should stay unknown and reject the implicit group",
    );
}

#[test]
fn global_aggregate_having_not_null_alias_preserves_single_row() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("aggregate-having-not-null-alias-a", 20),
            ("aggregate-having-not-null-alias-b", 21),
        ],
    );

    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) AS c \
         FROM SessionSqlEntity \
         HAVING c IS NOT NULL",
        Value::Uint(2),
        "aliased global aggregate HAVING IS NOT NULL should preserve the implicit group for non-null COUNT(*)",
    );
}

#[test]
fn global_aggregate_having_returns_empty_projection_when_predicate_fails() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("aggregate-having-fail-a", 20)]);

    let payload = execute_sql_statement_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT ROUND(AVG(age), 0) AS avg_rounded \
         FROM SessionSqlEntity \
         HAVING AVG(age) > 30",
    )
    .expect("global aggregate HAVING should execute through the shared post-aggregate evaluator");

    let SqlStatementResult::Projection {
        columns,
        fixed_scales,
        rows,
        row_count,
    } = payload
    else {
        panic!("global aggregate HAVING failure should still return projection payload");
    };

    assert_eq!(
        columns,
        vec!["avg_rounded".to_string()],
        "global aggregate HAVING should preserve output labels even when it filters away the implicit group",
    );
    assert_eq!(
        fixed_scales,
        vec![Some(0)],
        "global aggregate HAVING should preserve fixed-scale metadata on empty payloads",
    );
    assert!(
        rows.is_empty(),
        "global aggregate HAVING should filter away the implicit group when the predicate fails",
    );
    assert_eq!(
        row_count, 0,
        "global aggregate HAVING should expose zero rows when the implicit group is rejected",
    );
}

#[test]
fn global_aggregate_output_order_alias_is_inert_for_singleton_result() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("aggregate-order-alias-a", 20),
            ("aggregate-order-alias-b", 22),
        ],
    );

    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT AVG(age) AS avg_age FROM SessionSqlEntity ORDER BY avg_age DESC",
        Value::Decimal(crate::types::Decimal::from(21_u64)),
        "singleton global aggregate output ordering should execute as an inert no-op instead of misclassifying the alias as a base-row field",
    );
}

#[test]
fn global_aggregate_wrapped_output_order_alias_is_inert_for_singleton_result() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("aggregate-order-wrap-a", 20),
            ("aggregate-order-wrap-b", 22),
        ],
    );

    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT ROUND(AVG(age), 2) AS avg_age FROM SessionSqlEntity ORDER BY avg_age DESC",
        Value::Decimal(crate::types::Decimal::new(2100, 2)),
        "singleton wrapped global aggregate output ordering should execute as an inert no-op instead of misclassifying the alias as a base-row field",
    );
}

// This parity test is intentionally table-shaped so whole-window and bounded
// aggregate equivalence stay on one readable contract table.
#[expect(
    clippy::too_many_lines,
    reason = "aggregate SQL/fluent parity matrix is intentionally table-shaped"
)]
#[test]
fn global_aggregate_sql_matches_canonical_fluent_terminals() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("aggregate-window-a", 20),
            ("aggregate-window-b", 20),
            ("aggregate-window-c", 32),
            ("aggregate-window-d", 40),
        ],
    );

    // Phase 1: prove whole-window aggregate SQL shapes against their
    // canonical fluent terminal representations.
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) FROM SessionSqlEntity",
        Value::Uint(u64::from(
            session
                .load::<SessionSqlEntity>()
                .count()
                .expect("fluent count should succeed"),
        )),
        "COUNT(*)",
    );
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT SUM(age) FROM SessionSqlEntity",
        session
            .load::<SessionSqlEntity>()
            .sum_by("age")
            .expect("fluent sum_by(age) should succeed")
            .map_or(Value::Null, Value::Decimal),
        "SUM(age)",
    );
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT AVG(age) FROM SessionSqlEntity",
        session
            .load::<SessionSqlEntity>()
            .avg_by("age")
            .expect("fluent avg_by(age) should succeed")
            .map_or(Value::Null, Value::Decimal),
        "AVG(age)",
    );
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(DISTINCT age) FROM SessionSqlEntity",
        Value::Uint(u64::from(
            session
                .load::<SessionSqlEntity>()
                .count_distinct_by("age")
                .expect("fluent count_distinct_by(age) should succeed"),
        )),
        "COUNT(DISTINCT age)",
    );
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT SUM(DISTINCT age) FROM SessionSqlEntity",
        session
            .load::<SessionSqlEntity>()
            .sum_distinct_by("age")
            .expect("fluent sum_distinct_by(age) should succeed")
            .map_or(Value::Null, Value::Decimal),
        "SUM(DISTINCT age)",
    );
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT AVG(DISTINCT age) FROM SessionSqlEntity",
        session
            .load::<SessionSqlEntity>()
            .avg_distinct_by("age")
            .expect("fluent avg_distinct_by(age) should succeed")
            .map_or(Value::Null, Value::Decimal),
        "AVG(DISTINCT age)",
    );

    // Phase 2: prove bounded aggregate windows against the same fluent
    // aggregate terminals over equivalent ordered windows.
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) FROM SessionSqlEntity ORDER BY age DESC LIMIT 2 OFFSET 1",
        Value::Uint(u64::from(
            session
                .load::<SessionSqlEntity>()
                .order_term(crate::db::desc("age"))
                .limit(2)
                .offset(1)
                .count()
                .expect("bounded fluent count should succeed"),
        )),
        "bounded COUNT(*) window",
    );
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT SUM(age) FROM SessionSqlEntity ORDER BY age DESC LIMIT 2 OFFSET 1",
        session
            .load::<SessionSqlEntity>()
            .order_term(crate::db::desc("age"))
            .limit(2)
            .offset(1)
            .sum_by("age")
            .expect("bounded fluent sum_by(age) should succeed")
            .map_or(Value::Null, Value::Decimal),
        "bounded SUM(age) window",
    );
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT AVG(age) FROM SessionSqlEntity ORDER BY age ASC LIMIT 2 OFFSET 1",
        session
            .load::<SessionSqlEntity>()
            .order_term(crate::db::asc("age"))
            .limit(2)
            .offset(1)
            .avg_by("age")
            .expect("bounded fluent avg_by(age) should succeed")
            .map_or(Value::Null, Value::Decimal),
        "bounded AVG(age) window",
    );
}

#[test]
fn global_aggregate_count_star_reuses_shared_query_plan_cache_with_fluent_count() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("count-cache-a", 10),
            ("count-cache-b", 20),
            ("count-cache-c", 30),
            ("count-cache-d", 40),
        ],
    );

    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "new session should start with an empty shared query-plan cache",
    );
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) FROM SessionSqlEntity ORDER BY age DESC LIMIT 2 OFFSET 1",
        Value::Uint(2),
        "COUNT(*) SQL should execute through the shared count-terminal route",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "COUNT(*) SQL should populate one shared query-plan cache entry",
    );

    let fluent_count = session
        .load::<SessionSqlEntity>()
        .order_term(crate::db::desc("age"))
        .limit(2)
        .offset(1)
        .count()
        .expect("equivalent fluent count should succeed");
    assert_eq!(fluent_count, 2);
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "equivalent fluent count should reuse the shared query-plan cache entry populated by SQL COUNT(*)",
    );
}

#[test]
fn global_aggregate_count_non_nullable_field_reuses_shared_query_plan_cache_with_fluent_count() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("count-field-cache-a", 10),
            ("count-field-cache-b", 20),
            ("count-field-cache-c", 30),
            ("count-field-cache-d", 40),
        ],
    );

    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "new session should start with an empty shared query-plan cache",
    );
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(name) FROM SessionSqlEntity ORDER BY age DESC LIMIT 2 OFFSET 1",
        Value::Uint(2),
        "COUNT(non-null field) SQL should execute through the shared count-terminal route",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "COUNT(non-null field) SQL should populate one shared query-plan cache entry",
    );

    let fluent_count = session
        .load::<SessionSqlEntity>()
        .order_term(crate::db::desc("age"))
        .limit(2)
        .offset(1)
        .count()
        .expect("equivalent fluent count should succeed");
    assert_eq!(fluent_count, 2);
    assert_eq!(
        session.query_plan_cache_len(),
        1,
        "equivalent fluent count should reuse the shared query-plan cache entry populated by COUNT(non-null field)",
    );
}

#[test]
fn fluent_helper_terminals_map_to_admitted_sql_query_terms() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("helper-a", 20),
            ("helper-b", 20),
            ("helper-c", 32),
            ("helper-d", 40),
        ],
    );

    // Phase 1: prove existence helpers are only ergonomic sugar over admitted
    // SQL aggregate query terms, not separate capability lanes.
    let existing_count = statement_projection_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) FROM SessionSqlEntity",
    )
    .expect("COUNT(*) SQL should execute for exists() parity");
    assert_eq!(
        session
            .load::<SessionSqlEntity>()
            .exists()
            .expect("fluent exists() should succeed"),
        matches!(existing_count, Value::Uint(count) if count > 0),
        "exists() should match COUNT(*) > 0 over the same SQL window",
    );

    let missing_count = statement_projection_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) FROM SessionSqlEntity WHERE name = 'missing-helper'",
    )
    .expect("empty COUNT(*) SQL should execute for not_exists() parity");
    assert_eq!(
        session
            .load::<SessionSqlEntity>()
            .filter(crate::db::FieldRef::new("name").eq("missing-helper"))
            .not_exists()
            .expect("fluent not_exists() should succeed"),
        matches!(missing_count, Value::Uint(0)),
        "not_exists() should match COUNT(*) == 0 over the same SQL window",
    );

    // Phase 2: prove the order-sensitive id helpers map onto ordinary ordered
    // SQL projection windows instead of requiring a separate SQL helper family.
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT id FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
        session
            .load::<SessionSqlEntity>()
            .min()
            .expect("fluent min() should succeed")
            .map_or(Value::Null, |id| Value::Ulid(id.key())),
        "min()",
    );
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT id FROM SessionSqlEntity ORDER BY id DESC LIMIT 1",
        session
            .load::<SessionSqlEntity>()
            .max()
            .expect("fluent max() should succeed")
            .map_or(Value::Null, |id| Value::Ulid(id.key())),
        "max()",
    );
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT id FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        session
            .load::<SessionSqlEntity>()
            .min_by("age")
            .expect("fluent min_by(age) should succeed")
            .map_or(Value::Null, |id| Value::Ulid(id.key())),
        "min_by(age)",
    );
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT id FROM SessionSqlEntity ORDER BY age DESC, id ASC LIMIT 1",
        session
            .load::<SessionSqlEntity>()
            .max_by("age")
            .expect("fluent max_by(age) should succeed")
            .map_or(Value::Null, |id| Value::Ulid(id.key())),
        "max_by(age)",
    );
    assert_session_sql_scalar_value::<SessionSqlEntity>(
        &session,
        "SELECT id FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1 OFFSET 1",
        session
            .load::<SessionSqlEntity>()
            .nth_by("age", 1)
            .expect("fluent nth_by(age, 1) should succeed")
            .map_or(Value::Null, |id| Value::Ulid(id.key())),
        "nth_by(age, 1)",
    );
}

#[test]
fn global_aggregate_window_matrix_returns_expected_values() {
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
            assert_session_sql_scalar_value::<SessionSqlEntity>(&session, sql, expected, context);
        }
    }
}

#[test]
fn execute_sql_statement_global_aggregate_payload_matrix_preserves_projection_labels() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_columns, expected_fixed_scales, expected_rows, context) in [
        (
            "SELECT COUNT(*) FROM SessionSqlEntity",
            vec!["COUNT(*)".to_string()],
            vec![None],
            vec![vec![Value::Uint(0)]],
            "plain global aggregate statement payload",
        ),
        (
            "SELECT COUNT(*) AS total_rows FROM SessionSqlEntity",
            vec!["total_rows".to_string()],
            vec![None],
            vec![vec![Value::Uint(0)]],
            "aliased global aggregate statement payload",
        ),
        (
            "SELECT MIN(age) AS youngest, MAX(age) AS oldest FROM SessionSqlEntity",
            vec!["youngest".to_string(), "oldest".to_string()],
            vec![None, None],
            vec![vec![Value::Null, Value::Null]],
            "multi-terminal aliased global aggregate statement payload",
        ),
        (
            "SELECT COUNT(1), SUM(age + 1), AVG(age + 1) FROM SessionSqlEntity",
            vec![
                "COUNT(1)".to_string(),
                "SUM(age + 1)".to_string(),
                "AVG(age + 1)".to_string(),
            ],
            vec![None, None, None],
            vec![vec![Value::Uint(0), Value::Null, Value::Null]],
            "expression-input global aggregate statement payload",
        ),
        (
            "SELECT ROUND(AVG(age), 4) AS avg_rounded, COUNT(*) + 1 AS count_plus_one, MAX(age) - MIN(age) AS spread FROM SessionSqlEntity",
            vec![
                "avg_rounded".to_string(),
                "count_plus_one".to_string(),
                "spread".to_string(),
            ],
            vec![Some(4), None, None],
            vec![vec![
                Value::Null,
                Value::Decimal(crate::types::Decimal::from(1_u64)),
                Value::Null,
            ]],
            "post-aggregate global aggregate statement payload",
        ),
    ] {
        let payload = execute_sql_statement_for_tests::<SessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

        let SqlStatementResult::Projection {
            columns,
            fixed_scales,
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
            fixed_scales, expected_fixed_scales,
            "{context} should preserve fixed-scale metadata",
        );
        assert_eq!(
            rows, expected_rows,
            "{context} should preserve empty-store aggregate values",
        );
        assert_eq!(
            row_count, 1,
            "{context} should expose one scalar aggregate row",
        );
    }
}

#[test]
fn global_aggregate_matrix_queries_match_expected_values() {
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
        assert_session_sql_scalar_value::<SessionSqlEntity>(&session, sql, expected_value, sql);
    }
}

#[test]
fn global_aggregate_multi_terminal_query_returns_expected_projection_row() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("multi-aggregate-a", 10),
            ("multi-aggregate-b", 20),
            ("multi-aggregate-c", 30),
        ],
    );

    assert_eq!(
        statement_projection_columns::<SessionSqlEntity>(
            &session,
            "SELECT MIN(age) AS youngest, MAX(age) AS oldest FROM SessionSqlEntity",
        )
        .expect("multi-terminal global aggregate columns should load"),
        vec!["youngest".to_string(), "oldest".to_string()],
        "multi-terminal global aggregate SQL should preserve both aggregate labels",
    );
    assert_eq!(
        statement_projection_rows::<SessionSqlEntity>(
            &session,
            "SELECT MIN(age), MAX(age) FROM SessionSqlEntity",
        )
        .expect("multi-terminal global aggregate row should load"),
        vec![vec![Value::Uint(10), Value::Uint(30)]],
        "multi-terminal global aggregate SQL should emit one row with both reduced values",
    );
}

#[test]
fn global_aggregate_duplicate_terminals_preserve_duplicate_output_columns() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("duplicate-aggregate-a", 10),
            ("duplicate-aggregate-b", 20),
            ("duplicate-aggregate-c", 30),
        ],
    );

    assert_eq!(
        statement_projection_columns::<SessionSqlEntity>(
            &session,
            "SELECT COUNT(age), COUNT(age), SUM(age), COUNT(age) FROM SessionSqlEntity",
        )
        .expect("duplicate global aggregate columns should load"),
        vec![
            "COUNT(age)".to_string(),
            "COUNT(age)".to_string(),
            "SUM(age)".to_string(),
            "COUNT(age)".to_string(),
        ],
        "duplicate global aggregate SQL should preserve duplicate outward labels",
    );
    assert_eq!(
        statement_projection_rows::<SessionSqlEntity>(
            &session,
            "SELECT COUNT(age), COUNT(age), SUM(age), COUNT(age) FROM SessionSqlEntity",
        )
        .expect("duplicate global aggregate row should load"),
        vec![vec![
            Value::Uint(3),
            Value::Uint(3),
            Value::Decimal(crate::types::Decimal::from(60_u64)),
            Value::Uint(3),
        ]],
        "duplicate global aggregate SQL should fan unique reduced values back out into original projection order",
    );
}

#[test]
fn global_aggregate_duplicate_terminals_preserve_duplicate_alias_columns() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("duplicate-alias-a", 10),
            ("duplicate-alias-b", 20),
            ("duplicate-alias-c", 30),
        ],
    );

    assert_eq!(
        statement_projection_columns::<SessionSqlEntity>(
            &session,
            "SELECT COUNT(age) AS first_count, COUNT(age) AS second_count, COUNT(age) AS third_count FROM SessionSqlEntity",
        )
        .expect("duplicate aliased global aggregate columns should load"),
        vec![
            "first_count".to_string(),
            "second_count".to_string(),
            "third_count".to_string(),
        ],
        "duplicate global aggregate SQL should preserve outward alias labels after terminal dedupe",
    );
    assert_eq!(
        statement_projection_rows::<SessionSqlEntity>(
            &session,
            "SELECT COUNT(age) AS first_count, COUNT(age) AS second_count, COUNT(age) AS third_count FROM SessionSqlEntity",
        )
        .expect("duplicate aliased global aggregate row should load"),
        vec![vec![Value::Uint(3), Value::Uint(3), Value::Uint(3)]],
        "duplicate aliased global aggregate SQL should fan one reduced value back out to every aliased output slot",
    );
}

#[test]
fn global_aggregate_distinct_terminals_do_not_collapse_into_plain_count_outputs() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("distinct-aggregate-a", 10),
            ("distinct-aggregate-b", 10),
            ("distinct-aggregate-c", 30),
        ],
    );

    assert_eq!(
        statement_projection_rows::<SessionSqlEntity>(
            &session,
            "SELECT COUNT(age), COUNT(DISTINCT age), COUNT(age) FROM SessionSqlEntity",
        )
        .expect("distinct and non-distinct aggregate row should load"),
        vec![vec![Value::Uint(3), Value::Uint(2), Value::Uint(3)]],
        "COUNT(DISTINCT age) should stay separate from plain COUNT(age) while exact duplicates still fan out",
    );
}

#[test]
fn global_aggregate_qualified_and_unqualified_duplicates_preserve_same_outputs() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("qualified-aggregate-a", 10),
            ("qualified-aggregate-b", 20),
            ("qualified-aggregate-c", 30),
        ],
    );

    assert_eq!(
        statement_projection_rows::<SessionSqlEntity>(
            &session,
            "SELECT COUNT(age), COUNT(SessionSqlEntity.age), COUNT(age) FROM SessionSqlEntity",
        )
        .expect("qualified and unqualified duplicate aggregate row should load"),
        vec![vec![Value::Uint(3), Value::Uint(3), Value::Uint(3)]],
        "qualified and unqualified duplicate aggregate terminals should normalize to the same reduced output",
    );
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
            "global aggregate statement unknown target field",
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
        assert_session_sql_explain_tokens::<SessionSqlEntity>(
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

    let multi_execution = statement_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT MIN(age), MAX(age) FROM SessionSqlEntity",
    )
    .expect("multi-terminal global aggregate EXPLAIN EXECUTION should succeed");
    assert!(
        multi_execution.contains("AggregateMin execution_mode="),
        "multi-terminal global aggregate EXPLAIN EXECUTION should render the MIN terminal route",
    );
    assert!(
        multi_execution.contains("AggregateMax execution_mode="),
        "multi-terminal global aggregate EXPLAIN EXECUTION should render the MAX terminal route",
    );

    let expression_execution = statement_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT SUM(age + 1), COUNT(1) FROM SessionSqlEntity",
    )
    .expect("expression-input global aggregate EXPLAIN EXECUTION should succeed");
    assert!(
        expression_execution.contains("AggregateSum execution_mode="),
        "expression-input global aggregate EXPLAIN EXECUTION should render the SUM terminal route",
    );
    assert!(
        expression_execution.contains("AggregateCount execution_mode="),
        "expression-input global aggregate EXPLAIN EXECUTION should render the COUNT terminal route",
    );
}

#[test]
fn explain_sql_global_aggregate_filter_execution_surfaces_filter_shape() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: execute one filtered global aggregate EXPLAIN EXECUTION query
    // and require the terminal descriptor to keep the planner-owned filter
    // expression visible on the public execution surface.
    let explain = statement_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT COUNT(*) FILTER (WHERE age >= 30) FROM SessionSqlEntity",
    )
    .expect("filtered global aggregate EXPLAIN EXECUTION should succeed");

    assert_explain_contains_tokens(
        explain.as_str(),
        &[
            "AggregateCount execution_mode=",
            "filter_expr=Text(\"age >= 30\")",
        ],
        "filtered global aggregate EXPLAIN EXECUTION should keep filter shape visible",
    );
}
