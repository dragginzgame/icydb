use super::*;

#[test]
fn execute_sql_scalar_matrix_queries_match_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows for scalar matrix cases.
    seed_session_sql_entities(
        &session,
        &[
            ("scalar-matrix-a", 10),
            ("scalar-matrix-b", 20),
            ("scalar-matrix-c", 30),
            ("scalar-matrix-d", 40),
        ],
    );

    // Phase 2: execute table-driven scalar SQL cases.
    let cases = vec![
        (
            "SELECT * \
             FROM SessionSqlEntity \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
            vec![
                ("scalar-matrix-c".to_string(), 30_u64),
                ("scalar-matrix-b".to_string(), 20_u64),
            ],
        ),
        (
            "SELECT * \
             FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age ASC LIMIT 2",
            vec![
                ("scalar-matrix-b".to_string(), 20_u64),
                ("scalar-matrix-c".to_string(), 30_u64),
            ],
        ),
        (
            "SELECT DISTINCT * \
             FROM SessionSqlEntity \
             WHERE age >= 30 \
             ORDER BY age DESC",
            vec![
                ("scalar-matrix-d".to_string(), 40_u64),
                ("scalar-matrix-c".to_string(), 30_u64),
            ],
        ),
        (
            "SELECT * \
             FROM public.SessionSqlEntity \
             WHERE age < 25 \
             ORDER BY age ASC",
            vec![
                ("scalar-matrix-a".to_string(), 10_u64),
                ("scalar-matrix-b".to_string(), 20_u64),
            ],
        ),
        (
            "SELECT * \
             FROM SessionSqlEntity \
             ORDER BY age ASC LIMIT 1 OFFSET 2",
            vec![("scalar-matrix-c".to_string(), 30_u64)],
        ),
    ];

    // Phase 3: assert scalar row payload order and values for each query.
    for (sql, expected_rows) in cases {
        let actual_rows = execute_sql_name_age_rows(&session, sql);
        assert_eq!(actual_rows, expected_rows, "scalar matrix case: {sql}");
    }
}

#[test]
fn execute_sql_rejects_aggregate_projection_in_current_slice() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_scalar_sql_for_tests::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
        .expect_err("global aggregate SQL projection should remain lowering-gated");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "global aggregate SQL projection should fail at reduced lowering boundary",
    );
    assert!(
        err.to_string()
            .contains("execute_sql rejects global aggregate SELECT"),
        "execute_sql should preserve the dedicated aggregate-lane boundary message",
    );
}
