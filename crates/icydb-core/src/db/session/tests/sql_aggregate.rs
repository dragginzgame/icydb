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
                .order_by_desc("age")
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
            .order_by_desc("age")
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
            .order_by("age")
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
        .order_by_desc("age")
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
        .order_by_desc("age")
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
            .filter(Predicate::eq(
                "name".to_string(),
                "missing-helper".to_string().into(),
            ))
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

    for (sql, expected_columns, expected_rows, context) in [
        (
            "SELECT COUNT(*) FROM SessionSqlEntity",
            vec!["COUNT(*)".to_string()],
            vec![vec![Value::Uint(0)]],
            "plain global aggregate statement payload",
        ),
        (
            "SELECT COUNT(*) AS total_rows FROM SessionSqlEntity",
            vec!["total_rows".to_string()],
            vec![vec![Value::Uint(0)]],
            "aliased global aggregate statement payload",
        ),
        (
            "SELECT MIN(age) AS youngest, MAX(age) AS oldest FROM SessionSqlEntity",
            vec!["youngest".to_string(), "oldest".to_string()],
            vec![vec![Value::Null, Value::Null]],
            "multi-terminal aliased global aggregate statement payload",
        ),
    ] {
        let payload = execute_sql_statement_for_tests::<SessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

        let SqlStatementResult::Projection {
            columns,
            rows,
            row_count,
            ..
        } = payload
        else {
            panic!("{context} should return projection payload");
        };

        assert_eq!(
            columns, expected_columns,
            "{context} should preserve aggregate projection labels",
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
}
