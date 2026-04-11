use super::*;

#[test]
fn execute_sql_delete_honors_predicate_order_and_limit() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "first-minor".to_string(),
            age: 16,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "second-minor".to_string(),
            age: 17,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "adult".to_string(),
            age: 42,
        })
        .expect("seed insert should succeed");

    let deleted = session
        .execute_sql::<SessionSqlEntity>(
            "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1",
        )
        .expect("DELETE should execute");

    assert_eq!(deleted.count(), 1, "delete limit should remove one row");
    assert_eq!(
        deleted
            .iter()
            .next()
            .expect("deleted row should exist")
            .entity_ref()
            .age,
        16,
        "ordered delete should remove the youngest matching row first",
    );

    let remaining = session
        .load::<SessionSqlEntity>()
        .order_by("age")
        .execute()
        .expect("post-delete load should succeed");
    let remaining_ages = remaining
        .iter()
        .map(|row| row.entity_ref().age)
        .collect::<Vec<_>>();

    assert_eq!(
        remaining_ages,
        vec![17, 42],
        "delete window semantics should preserve non-deleted rows",
    );
}

#[test]
fn execute_sql_delete_honors_ordered_offset_then_limit() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "first-minor".to_string(),
            age: 16,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "second-minor".to_string(),
            age: 17,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "third-minor".to_string(),
            age: 18,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "adult".to_string(),
            age: 42,
        })
        .expect("seed insert should succeed");

    let deleted = session
        .execute_sql::<SessionSqlEntity>(
            "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1 OFFSET 1",
        )
        .expect("DELETE with OFFSET should execute");

    assert_eq!(deleted.count(), 1, "delete window should remove one row");
    assert_eq!(
        deleted
            .iter()
            .next()
            .expect("deleted row should exist")
            .entity_ref()
            .age,
        17,
        "ordered delete offset should skip the first matching row before applying LIMIT",
    );

    let remaining =
        execute_sql_name_age_rows(&session, "SELECT * FROM SessionSqlEntity ORDER BY age ASC");

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

        let deleted = session
            .execute_sql::<SessionSqlEntity>(sql)
            .expect("delete matrix SQL execution should succeed");
        let deleted_rows = deleted
            .iter()
            .map(|row| (row.entity_ref().name.clone(), row.entity_ref().age))
            .collect::<Vec<_>>();
        let remaining_rows =
            execute_sql_name_age_rows(&session, "SELECT * FROM SessionSqlEntity ORDER BY age ASC");

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
