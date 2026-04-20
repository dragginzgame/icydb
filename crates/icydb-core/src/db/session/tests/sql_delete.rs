use super::*;

type NameAgeRows = Vec<(String, u64)>;

// Seed the canonical minor/adult delete fixture used by the ordered delete
// boundary checks in this file.
fn seed_delete_minor_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_session_sql_entities(
        session,
        &[("first-minor", 16), ("second-minor", 17), ("adult", 42)],
    );
}

// Seed the canonical offset-aware delete fixture used by the ordered delete
// window checks in this file.
fn seed_delete_offset_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_session_sql_entities(
        session,
        &[
            ("first-minor", 16),
            ("second-minor", 17),
            ("third-minor", 18),
            ("adult", 42),
        ],
    );
}

// Run one SQL DELETE statement with explicit `RETURNING name, age` and return
// the deleted rows as `(name, age)` tuples in response order.
fn execute_sql_delete_returning_name_age_rows(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> NameAgeRows {
    let returning_sql = format!("{sql} RETURNING name, age");

    statement_projection_rows::<SessionSqlEntity>(session, returning_sql.as_str())
        .unwrap_or_else(|err| {
            panic!("DELETE SQL statement execution should execute with RETURNING: {err:?}")
        })
        .into_iter()
        .map(|row| {
            let [Value::Text(name), Value::Uint(age)] = row.as_slice() else {
                panic!("DELETE RETURNING name, age should preserve two-column value rows");
            };
            (name.clone(), *age)
        })
        .collect::<Vec<_>>()
}

// Load the remaining rows after a delete through one stable age-ordered
// session surface.
fn remaining_session_name_age_rows(session: &DbSession<SessionSqlCanister>) -> NameAgeRows {
    execute_sql_name_age_rows(session, "SELECT * FROM SessionSqlEntity ORDER BY age ASC")
}

// Run one SQL DELETE statement through unified statement and return only the
// affected-row count from the traditional mutation result surface.
fn execute_sql_statement_delete_count(session: &DbSession<SessionSqlCanister>, sql: &str) -> u32 {
    let payload = execute_sql_statement_for_tests::<SessionSqlEntity>(session, sql)
        .unwrap_or_else(|err| panic!("DELETE SQL statement execution should execute: {err:?}"));

    match payload {
        SqlStatementResult::Count { row_count } => row_count,
        other => {
            panic!("DELETE SQL statement execution should return count payload, got {other:?}")
        }
    }
}

#[test]
fn execute_sql_delete_ordered_window_matrix_honors_delete_shape() {
    let cases = [
        (
            "ordered limit",
            "minor",
            "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1",
            vec![("first-minor".to_string(), 16)],
            vec![("second-minor".to_string(), 17), ("adult".to_string(), 42)],
        ),
        (
            "ordered offset then limit",
            "offset",
            "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1 OFFSET 1",
            vec![("second-minor".to_string(), 17)],
            vec![
                ("first-minor".to_string(), 16),
                ("third-minor".to_string(), 18),
                ("adult".to_string(), 42),
            ],
        ),
        (
            "single-table alias",
            "minor",
            "DELETE FROM SessionSqlEntity alias \
             WHERE alias.age < 20 \
             ORDER BY alias.age ASC LIMIT 1",
            vec![("first-minor".to_string(), 16)],
            vec![("second-minor".to_string(), 17), ("adult".to_string(), 42)],
        ),
    ];

    for (context, fixture, sql, expected_deleted, expected_remaining) in cases {
        reset_session_sql_store();
        let session = sql_session();

        match fixture {
            "minor" => seed_delete_minor_fixture(&session),
            "offset" => seed_delete_offset_fixture(&session),
            _ => unreachable!("delete ordered window matrix uses fixed fixtures"),
        }

        let deleted = execute_sql_delete_returning_name_age_rows(&session, sql);
        let remaining = remaining_session_name_age_rows(&session);

        assert_eq!(
            deleted, expected_deleted,
            "{context} should preserve deleted-row ordering",
        );
        assert_eq!(
            remaining, expected_remaining,
            "{context} should preserve remaining-row semantics",
        );
    }
}

#[test]
fn execute_sql_delete_ulid_string_literal_predicate_removes_matching_row() {
    reset_session_sql_store();
    let session = sql_session();
    let target_id = Ulid::from_u128(9_921);
    let other_id = Ulid::from_u128(9_922);
    let sql = format!("DELETE FROM SessionSqlEntity WHERE id = '{target_id}'");

    session
        .insert(SessionSqlEntity {
            id: target_id,
            name: "delete-target".to_string(),
            age: 21,
        })
        .expect("target ULID delete seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: other_id,
            name: "delete-other".to_string(),
            age: 22,
        })
        .expect("other ULID delete seed insert should succeed");

    let row_count = execute_sql_statement_delete_count(&session, sql.as_str());
    let remaining = remaining_session_name_age_rows(&session);

    assert_eq!(row_count, 1, "quoted ULID delete should affect one row");
    assert_eq!(remaining, vec![("delete-other".to_string(), 22)]);
}

#[test]
fn scalar_select_helper_rejects_delete_lane_on_typed_entity_surface() {
    reset_session_sql_store();
    let session = sql_session();

    for sql in [
        "DELETE FROM SessionSqlEntity WHERE age < 20",
        "DELETE FROM SessionSqlEntity WHERE age < 20 RETURNING id",
    ] {
        let err = execute_scalar_select_for_tests::<SessionSqlEntity>(&session, sql)
            .expect_err("scalar SELECT helper DELETE should stay off the entity-response surface");

        assert!(
            err.to_string()
                .contains("scalar SELECT helper rejects DELETE; use execute_sql_update::<E>()"),
            "scalar SELECT helper DELETE should preserve explicit fluent guidance",
        );
    }
}

#[test]
fn fluent_delete_returns_count_without_materializing_deleted_rows() {
    reset_session_sql_store();
    let session = sql_session();
    seed_delete_minor_fixture(&session);

    let row_count = session
        .delete::<SessionSqlEntity>()
        .order_term(crate::db::asc("age"))
        .limit(1)
        .execute()
        .expect("fluent delete should return count payload");
    let remaining = remaining_session_name_age_rows(&session);

    assert_eq!(row_count, 1);
    assert_eq!(
        remaining,
        vec![("second-minor".to_string(), 17), ("adult".to_string(), 42)],
        "fluent delete should still honor ordered delete semantics while returning count only",
    );
}

#[test]
fn execute_sql_statement_delete_returns_count_without_returning() {
    reset_session_sql_store();
    let session = sql_session();
    seed_delete_minor_fixture(&session);

    let row_count = execute_sql_statement_delete_count(
        &session,
        "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1",
    );
    let remaining = remaining_session_name_age_rows(&session);

    assert_eq!(row_count, 1, "bare DELETE should return affected-row count");
    assert_eq!(
        remaining,
        vec![("second-minor".to_string(), 17), ("adult".to_string(), 42)],
        "bare DELETE should still apply the ordered delete window",
    );
}

#[test]
fn execute_sql_statement_delete_returning_projection_matrix_projects_deleted_rows() {
    for (sql, expect_full_row, context) in [
        (
            "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1 RETURNING name, age",
            false,
            "DELETE RETURNING field list",
        ),
        (
            "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1 RETURNING *",
            true,
            "DELETE RETURNING star",
        ),
    ] {
        reset_session_sql_store();
        let session = sql_session();
        seed_delete_minor_fixture(&session);

        let rows = statement_projection_rows::<SessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should return deleted rows: {err:?}"));
        let remaining = remaining_session_name_age_rows(&session);

        assert_eq!(rows.len(), 1, "{context} should emit one deleted row");

        if expect_full_row {
            assert_eq!(
                rows[0].len(),
                3,
                "{context} should preserve full entity field width",
            );
            assert!(
                matches!(rows[0][0], Value::Ulid(_)),
                "{context} should preserve the generated primary key slot",
            );
            assert_eq!(
                rows[0][1..],
                [Value::Text("first-minor".to_string()), Value::Uint(16),],
                "{context} should preserve the deleted name and age in field order",
            );
        } else {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("first-minor".to_string()),
                    Value::Uint(16)
                ]],
                "{context} should project only the requested deleted-row fields",
            );
        }

        assert_eq!(
            remaining,
            vec![("second-minor".to_string(), 17), ("adult".to_string(), 42)],
            "{context} should preserve delete side effects",
        );
    }
}

#[test]
fn execute_sql_delete_searched_case_where_matches_expected_deleted_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic delete matrix for the searched-CASE
    // scalar WHERE seam shared with load execution.
    seed_session_sql_entities(
        &session,
        &[
            ("delete-case-a", 10),
            ("delete-case-b", 20),
            ("delete-case-c", 30),
            ("delete-case-d", 40),
        ],
    );

    // Phase 2: require delete post-access filtering to preserve the same
    // searched-CASE row semantics as scalar load execution.
    let deleted = execute_sql_delete_returning_name_age_rows(
        &session,
        "DELETE FROM SessionSqlEntity \
         WHERE CASE WHEN age >= 30 THEN TRUE ELSE age = 20 END \
         ORDER BY age ASC",
    );
    let remaining = remaining_session_name_age_rows(&session);

    assert_eq!(
        deleted,
        vec![
            ("delete-case-b".to_string(), 20),
            ("delete-case-c".to_string(), 30),
            ("delete-case-d".to_string(), 40),
        ],
        "searched CASE delete WHERE should keep the same row admission semantics as scalar load execution",
    );
    assert_eq!(
        remaining,
        vec![("delete-case-a".to_string(), 10)],
        "searched CASE delete WHERE should leave only the non-matching row behind",
    );
}

#[test]
fn execute_sql_delete_matrix_queries_match_deleted_and_remaining_rows() {
    // Phase 1: define one shared seed dataset and table-driven DELETE cases.
    let seed_rows = [
        ("delete-matrix-a", 10_u64),
        ("delete-matrix-b", 20_u64),
        ("delete-matrix-c", 30_u64),
        ("delete-matrix-d", 40_u64),
    ];
    let cases = vec![
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age ASC LIMIT 1",
            vec![("delete-matrix-b".to_string(), 20_u64)],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-c".to_string(), 30_u64),
                ("delete-matrix-d".to_string(), 40_u64),
            ],
        ),
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age DESC LIMIT 2",
            vec![
                ("delete-matrix-d".to_string(), 40_u64),
                ("delete-matrix-c".to_string(), 30_u64),
            ],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-b".to_string(), 20_u64),
            ],
        ),
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age ASC LIMIT 1 OFFSET 1",
            vec![("delete-matrix-c".to_string(), 30_u64)],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-b".to_string(), 20_u64),
                ("delete-matrix-d".to_string(), 40_u64),
            ],
        ),
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 100 \
             ORDER BY age ASC LIMIT 1",
            vec![],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-b".to_string(), 20_u64),
                ("delete-matrix-c".to_string(), 30_u64),
                ("delete-matrix-d".to_string(), 40_u64),
            ],
        ),
    ];

    // Phase 2: execute each DELETE case from a fresh seeded store.
    for (sql, expected_deleted, expected_remaining) in cases {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_sql_entities(&session, &seed_rows);

        let deleted_rows = execute_sql_delete_returning_name_age_rows(&session, sql);
        let remaining_rows = remaining_session_name_age_rows(&session);

        assert_eq!(
            deleted_rows, expected_deleted,
            "delete matrix deleted rows: {sql}"
        );
        assert_eq!(
            remaining_rows, expected_remaining,
            "delete matrix remaining rows: {sql}",
        );
    }
}
