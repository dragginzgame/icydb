use super::*;

#[test]
fn execute_sql_grouped_rejects_computed_text_projection_in_current_lane() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_grouped::<SessionSqlEntity>("SELECT TRIM(name) FROM SessionSqlEntity", None)
        .expect_err(
            "execute_sql_grouped should keep computed text projection on the dispatch-owned lane",
        );

    assert!(
        err.to_string()
            .contains("execute_sql_grouped rejects computed text projection"),
        "execute_sql_grouped should reject computed text projection with an actionable boundary message",
    );
}

#[test]
fn query_from_sql_select_grouped_aggregate_projection_lowers_to_grouped_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
        )
        .expect("grouped aggregate projection SQL query should lower");
    assert!(
        query.has_grouping(),
        "grouped aggregate SQL projection lowering should produce grouped query intent",
    );
}

#[test]
fn execute_sql_grouped_rejects_global_aggregate_execution_in_current_lane() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_grouped::<SessionSqlEntity>(
            "SELECT COUNT(*) FROM SessionSqlEntity",
            None,
        )
        .expect_err(
            "execute_sql_grouped should keep global aggregate execution on the dedicated aggregate lane",
        );

    assert!(
        err.to_string()
            .contains("execute_sql_grouped rejects global aggregate SELECT"),
        "execute_sql_grouped should preserve the dedicated aggregate-lane boundary message",
    );
}

#[test]
fn execute_sql_grouped_matrix_queries_match_expected_grouped_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows used by grouped matrix queries.
    seed_session_sql_entities(
        &session,
        &[
            ("group-matrix-a", 10),
            ("group-matrix-b", 10),
            ("group-matrix-c", 20),
            ("group-matrix-d", 30),
            ("group-matrix-e", 30),
            ("group-matrix-f", 30),
        ],
    );

    // Phase 2: execute table-driven grouped SQL cases.
    let cases = vec![
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec![(10_u64, 2_u64), (20_u64, 1_u64), (30_u64, 3_u64)],
        ),
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             WHERE age >= 20 \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec![(20_u64, 1_u64), (30_u64, 3_u64)],
        ),
        (
            "SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 20 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age ASC LIMIT 10",
            vec![(20_u64, 1_u64), (30_u64, 3_u64)],
        ),
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             HAVING COUNT(*) > 1 \
             ORDER BY age ASC LIMIT 10",
            vec![(10_u64, 2_u64), (30_u64, 3_u64)],
        ),
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             HAVING COUNT(*) IS NULL \
             ORDER BY age ASC LIMIT 10",
            vec![],
        ),
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             HAVING COUNT(*) IS NOT NULL \
             ORDER BY age ASC LIMIT 10",
            vec![(10_u64, 2_u64), (20_u64, 1_u64), (30_u64, 3_u64)],
        ),
    ];

    // Phase 3: assert grouped row payloads for each SQL input.
    for (sql, expected_rows) in cases {
        let execution = session
            .execute_sql_grouped::<SessionSqlEntity>(sql, None)
            .expect("grouped matrix SQL execution should succeed");
        let actual_rows = execution
            .rows()
            .iter()
            .map(|row| {
                (
                    row.group_key()[0].clone(),
                    row.aggregate_values()[0].clone(),
                )
            })
            .collect::<Vec<_>>();
        let expected_values = expected_rows
            .iter()
            .map(|(group_key, count)| (Value::Uint(*group_key), Value::Uint(*count)))
            .collect::<Vec<_>>();

        assert!(
            execution.continuation_cursor().is_none(),
            "grouped matrix cases should fully materialize under LIMIT 10: {sql}",
        );
        assert_eq!(actual_rows, expected_values, "grouped matrix case: {sql}");
    }
}

#[test]
fn execute_sql_projection_rejects_grouped_aggregate_sql() {
    reset_session_sql_store();
    let session = sql_session();

    let err = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
    )
    .expect_err("projection SQL API should reject grouped aggregate SQL intent");

    assert!(
        err.to_string()
            .contains("execute_sql_dispatch rejects grouped SELECT execution"),
        "projection SQL API must preserve explicit grouped dispatch-lane guidance",
    );
}

#[test]
fn execute_sql_grouped_select_count_returns_grouped_aggregate_row() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "aggregate-a".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "aggregate-b".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "aggregate-c".to_string(),
            age: 32,
        })
        .expect("seed insert should succeed");

    let execution = session
        .execute_sql_grouped::<SessionSqlEntity>(
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age ORDER BY age ASC LIMIT 10",
            None,
        )
        .expect("grouped SQL aggregate execution should succeed");

    assert!(
        execution.continuation_cursor().is_none(),
        "single-page grouped aggregate execution should not emit continuation cursor",
    );
    assert_eq!(execution.rows().len(), 2);
    assert_eq!(execution.rows()[0].group_key(), [Value::Uint(20)]);
    assert_eq!(execution.rows()[0].aggregate_values(), [Value::Uint(2)]);
    assert_eq!(execution.rows()[1].group_key(), [Value::Uint(32)]);
    assert_eq!(execution.rows()[1].aggregate_values(), [Value::Uint(1)]);
}

#[test]
fn execute_sql_grouped_select_count_with_qualified_identifiers_executes() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "qualified-group-a".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "qualified-group-b".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "qualified-group-c".to_string(),
            age: 32,
        })
        .expect("seed insert should succeed");

    let execution = session
        .execute_sql_grouped::<SessionSqlEntity>(
            "SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 20 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age ASC LIMIT 10",
            None,
        )
        .expect("qualified grouped SQL aggregate execution should succeed");

    assert!(execution.continuation_cursor().is_none());
    assert_eq!(execution.rows().len(), 2);
    assert_eq!(execution.rows()[0].group_key(), [Value::Uint(20)]);
    assert_eq!(execution.rows()[0].aggregate_values(), [Value::Uint(2)]);
    assert_eq!(execution.rows()[1].group_key(), [Value::Uint(32)]);
    assert_eq!(execution.rows()[1].aggregate_values(), [Value::Uint(1)]);
}

#[test]
fn execute_sql_grouped_limit_window_emits_cursor_and_resumes_next_group_page() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed three grouped-key buckets with deterministic counts.
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-a".to_string(),
            age: 10,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-b".to_string(),
            age: 10,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-c".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-d".to_string(),
            age: 30,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-e".to_string(),
            age: 30,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-f".to_string(),
            age: 30,
        })
        .expect("seed insert should succeed");

    // Phase 2: execute the first grouped page and capture continuation cursor.
    let sql = "SELECT age, COUNT(*) \
               FROM SessionSqlEntity \
               GROUP BY age \
               ORDER BY age ASC LIMIT 1";
    let first_page = session
        .execute_sql_grouped::<SessionSqlEntity>(sql, None)
        .expect("first grouped SQL page should execute");
    assert_eq!(first_page.rows().len(), 1);
    assert_eq!(first_page.rows()[0].group_key(), [Value::Uint(10)]);
    assert_eq!(first_page.rows()[0].aggregate_values(), [Value::Uint(2)]);
    let cursor_one = crate::db::encode_cursor(
        first_page
            .continuation_cursor()
            .expect("first grouped SQL page should emit continuation cursor"),
    );

    // Phase 3: resume to second grouped page and capture next cursor.
    let second_page = session
        .execute_sql_grouped::<SessionSqlEntity>(sql, Some(cursor_one.as_str()))
        .expect("second grouped SQL page should execute");
    assert_eq!(second_page.rows().len(), 1);
    assert_eq!(second_page.rows()[0].group_key(), [Value::Uint(20)]);
    assert_eq!(second_page.rows()[0].aggregate_values(), [Value::Uint(1)]);
    let cursor_two = crate::db::encode_cursor(
        second_page
            .continuation_cursor()
            .expect("second grouped SQL page should emit continuation cursor"),
    );

    // Phase 4: resume final grouped page and assert no further continuation.
    let third_page = session
        .execute_sql_grouped::<SessionSqlEntity>(sql, Some(cursor_two.as_str()))
        .expect("third grouped SQL page should execute");
    assert_eq!(third_page.rows().len(), 1);
    assert_eq!(third_page.rows()[0].group_key(), [Value::Uint(30)]);
    assert_eq!(third_page.rows()[0].aggregate_values(), [Value::Uint(3)]);
    assert!(
        third_page.continuation_cursor().is_none(),
        "last grouped SQL page should not emit continuation cursor",
    );
}

#[test]
fn execute_sql_grouped_rejects_invalid_cursor_token_payload() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: execute one grouped query with an invalid cursor token payload.
    let err = session
        .execute_sql_grouped::<SessionSqlEntity>(
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 1",
            Some("zz"),
        )
        .expect_err("grouped SQL should fail closed on invalid cursor token payload");

    // Phase 2: assert decode failures stay in cursor-plan error taxonomy.
    assert_query_error_is_cursor_plan(err, |inner| {
        matches!(inner, CursorPlanError::InvalidContinuationCursor { .. })
    });
}

#[test]
fn execute_sql_grouped_rejects_cursor_token_from_different_query_signature() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed grouped buckets and capture one valid continuation cursor.
    seed_session_sql_entities(
        &session,
        &[
            ("cursor-signature-a", 10),
            ("cursor-signature-b", 20),
            ("cursor-signature-c", 30),
        ],
    );
    let first_page = session
        .execute_sql_grouped::<SessionSqlEntity>(
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 1",
            None,
        )
        .expect("first grouped SQL page should execute");
    let cursor = crate::db::encode_cursor(
        first_page
            .continuation_cursor()
            .expect("first grouped SQL page should emit continuation cursor"),
    );

    // Phase 2: replay cursor against a signature-incompatible grouped SQL shape.
    let err = session
        .execute_sql_grouped::<SessionSqlEntity>(
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age DESC LIMIT 1",
            Some(cursor.as_str()),
        )
        .expect_err("grouped SQL should reject cursor tokens from incompatible query signatures");

    // Phase 3: assert mismatch stays in cursor-plan signature error taxonomy.
    assert_query_error_is_cursor_plan(err, |inner| {
        matches!(
            inner,
            CursorPlanError::ContinuationCursorSignatureMismatch { .. }
        )
    });
}

#[test]
fn execute_sql_grouped_rejects_scalar_sql_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_grouped::<SessionSqlEntity>("SELECT name FROM SessionSqlEntity", None)
        .expect_err("grouped SQL API should reject non-grouped SQL queries");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "grouped SQL API should fail closed for non-grouped SQL shapes",
    );
}

#[test]
fn execute_sql_rejects_grouped_sql_intent_without_grouped_api() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age")
        .expect_err("scalar SQL API should reject grouped SQL intent");

    assert!(
        err.to_string()
            .contains("execute_sql rejects grouped SELECT"),
        "scalar SQL API must preserve grouped explicit-entrypoint guidance",
    );
}

#[test]
fn execute_sql_dispatch_rejects_grouped_sql_execution_in_current_lane() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_dispatch::<SessionSqlEntity>(
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
        )
        .expect_err("dispatch SQL API should reject grouped SQL execution");

    assert!(
        err.to_string()
            .contains("execute_sql_dispatch rejects grouped SELECT execution"),
        "dispatch SQL API must preserve grouped explicit-entrypoint guidance",
    );
}

#[test]
fn execute_sql_grouped_rejects_delete_execution_in_current_lane() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_grouped::<SessionSqlEntity>(
            "DELETE FROM SessionSqlEntity ORDER BY id LIMIT 1",
            None,
        )
        .expect_err("grouped SQL API should reject DELETE execution");

    assert!(
        err.to_string()
            .contains("execute_sql_grouped rejects DELETE"),
        "grouped SQL API must preserve explicit DELETE lane guidance",
    );
}

#[test]
fn execute_sql_rejects_unsupported_group_by_projection_shape() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity GROUP BY age")
        .expect_err("group-by projection mismatch should fail closed");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "unsupported grouped SQL projection shapes should fail at reduced lowering boundary",
    );
}

#[test]
fn explain_sql_plan_grouped_qualified_identifiers_match_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let qualified = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age DESC LIMIT 2 OFFSET 1",
    )
    .expect("qualified grouped EXPLAIN plan SQL should succeed");
    let unqualified = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             GROUP BY age \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
    )
    .expect("unqualified grouped EXPLAIN plan SQL should succeed");

    assert_eq!(
        qualified, unqualified,
        "qualified grouped identifiers should normalize to the same logical EXPLAIN plan output",
    );
}

#[test]
fn explain_sql_execution_grouped_qualified_identifiers_match_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let qualified = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age DESC LIMIT 2 OFFSET 1",
    )
    .expect("qualified grouped EXPLAIN execution SQL should succeed");
    let unqualified = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             GROUP BY age \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
    )
    .expect("unqualified grouped EXPLAIN execution SQL should succeed");

    assert_eq!(
        qualified, unqualified,
        "qualified grouped identifiers should normalize to the same execution EXPLAIN descriptor output",
    );
}

#[test]
fn explain_sql_json_grouped_qualified_identifiers_match_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let qualified = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN JSON SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age DESC LIMIT 2 OFFSET 1",
    )
    .expect("qualified grouped EXPLAIN JSON SQL should succeed");
    let unqualified = dispatch_explain_sql::<SessionSqlEntity>(
        &session,
        "EXPLAIN JSON SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             GROUP BY age \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
    )
    .expect("unqualified grouped EXPLAIN JSON SQL should succeed");

    assert_eq!(
        qualified, unqualified,
        "qualified grouped identifiers should normalize to the same EXPLAIN JSON output",
    );
}
