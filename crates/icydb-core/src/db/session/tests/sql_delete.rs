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

// Run one SQL DELETE statement and return the deleted rows as `(name, age)`
// tuples in response order.
fn execute_sql_delete_name_age_rows(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> NameAgeRows {
    session
        .execute_sql::<SessionSqlEntity>(sql)
        .unwrap_or_else(|err| panic!("DELETE SQL should execute: {err:?}"))
        .iter()
        .map(|row| (row.entity_ref().name.clone(), row.entity_ref().age))
        .collect::<Vec<_>>()
}

// Load the remaining rows after a delete through one stable age-ordered
// session surface.
fn remaining_session_name_age_rows(session: &DbSession<SessionSqlCanister>) -> NameAgeRows {
    execute_sql_name_age_rows(session, "SELECT * FROM SessionSqlEntity ORDER BY age ASC")
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

        let deleted = execute_sql_delete_name_age_rows(&session, sql);
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
fn execute_sql_delete_rejects_returning_clause() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("DELETE FROM SessionSqlEntity WHERE age < 20 RETURNING id")
        .expect_err("DELETE RETURNING should stay fail-closed");

    assert_sql_unsupported_feature_detail(err, "RETURNING");
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

        let deleted_rows = execute_sql_delete_name_age_rows(&session, sql);
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
