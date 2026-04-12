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
fn execute_sql_delete_honors_predicate_order_and_limit() {
    reset_session_sql_store();
    let session = sql_session();

    seed_delete_minor_fixture(&session);

    let deleted = execute_sql_delete_name_age_rows(
        &session,
        "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1",
    );
    let remaining = remaining_session_name_age_rows(&session);

    assert_eq!(
        deleted,
        vec![("first-minor".to_string(), 16)],
        "ordered delete should remove the youngest matching row first",
    );
    assert_eq!(
        remaining,
        vec![("second-minor".to_string(), 17), ("adult".to_string(), 42),],
        "delete window semantics should preserve non-deleted rows",
    );
}

#[test]
fn execute_sql_delete_honors_ordered_offset_then_limit() {
    reset_session_sql_store();
    let session = sql_session();

    seed_delete_offset_fixture(&session);

    let deleted = execute_sql_delete_name_age_rows(
        &session,
        "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1 OFFSET 1",
    );
    let remaining = remaining_session_name_age_rows(&session);

    assert_eq!(
        deleted,
        vec![("second-minor".to_string(), 17)],
        "ordered delete offset should skip the first matching row before applying LIMIT",
    );
    assert_eq!(
        remaining,
        vec![
            ("first-minor".to_string(), 16),
            ("third-minor".to_string(), 18),
            ("adult".to_string(), 42),
        ],
        "ordered delete offset should preserve skipped and non-matching rows",
    );
}

#[test]
fn execute_sql_delete_accepts_single_table_alias() {
    reset_session_sql_store();
    let session = sql_session();

    seed_delete_minor_fixture(&session);

    let deleted = execute_sql_delete_name_age_rows(
        &session,
        "DELETE FROM SessionSqlEntity alias \
         WHERE alias.age < 20 \
         ORDER BY alias.age ASC LIMIT 1",
    );

    assert_eq!(
        deleted,
        vec![("first-minor".to_string(), 16)],
        "ordered delete with one qualifying field and one table alias should remove the youngest matching row first",
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
