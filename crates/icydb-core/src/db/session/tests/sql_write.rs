use super::*;

#[test]
fn execute_sql_dispatch_insert_returns_full_row_projection_payload() {
    reset_session_sql_store();
    let session = sql_session();

    let columns = dispatch_projection_columns::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
    )
    .expect("SQL INSERT dispatch should return one projection payload");
    let rows = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (2, 'Bea', 22)",
    )
    .expect("SQL INSERT dispatch should return one value row");

    assert_eq!(columns, vec!["id", "name", "age"]);
    assert_eq!(
        rows,
        vec![vec![
            Value::Uint(2),
            Value::Text("Bea".to_string()),
            Value::Uint(22),
        ]],
    );
}

#[test]
fn execute_sql_dispatch_insert_with_multiple_values_tuples_returns_rows_in_input_order() {
    reset_session_sql_store();
    let session = sql_session();

    let rows = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity (id, name, age) \
         VALUES (2, 'Bea', 22), (3, 'Cid', 23)",
    )
    .expect("multi-row SQL INSERT dispatch should return value rows");
    let persisted = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
    )
    .expect("post-insert SQL projection should succeed");

    assert_eq!(
        rows,
        vec![
            vec![
                Value::Uint(2),
                Value::Text("Bea".to_string()),
                Value::Uint(22),
            ],
            vec![
                Value::Uint(3),
                Value::Text("Cid".to_string()),
                Value::Uint(23),
            ],
        ],
    );
    assert_eq!(persisted, rows);
}

#[test]
fn execute_sql_dispatch_insert_without_column_list_uses_canonical_field_order() {
    reset_session_sql_store();
    let session = sql_session();

    let rows = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity VALUES (4, 'Dee', 24)",
    )
    .expect("SQL INSERT without column list should use canonical field order");

    assert_eq!(
        rows,
        vec![vec![
            Value::Uint(4),
            Value::Text("Dee".to_string()),
            Value::Uint(24),
        ]],
    );
}

#[test]
fn execute_sql_dispatch_insert_without_column_list_accepts_multiple_values_tuples() {
    reset_session_sql_store();
    let session = sql_session();

    let rows = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity VALUES (4, 'Dee', 24), (5, 'Eli', 25)",
    )
    .expect("multi-row SQL INSERT without column list should use canonical field order");

    assert_eq!(
        rows,
        vec![
            vec![
                Value::Uint(4),
                Value::Text("Dee".to_string()),
                Value::Uint(24),
            ],
            vec![
                Value::Uint(5),
                Value::Text("Eli".to_string()),
                Value::Uint(25),
            ],
        ],
    );
}

#[test]
fn execute_sql_dispatch_update_returns_full_row_projection_payload() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlWriteEntity {
            id: 1,
            name: "Ada".to_string(),
            age: 21,
        })
        .expect("typed setup insert should succeed");

    let rows = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET name = 'Bea', age = 22 WHERE id = 1",
    )
    .expect("SQL UPDATE dispatch should return one value row");
    let persisted = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
    )
    .expect("post-update SQL projection should succeed");

    assert_eq!(
        rows,
        vec![vec![
            Value::Uint(1),
            Value::Text("Bea".to_string()),
            Value::Uint(22),
        ]],
    );
    assert_eq!(persisted, rows);
}

#[test]
fn execute_sql_dispatch_update_accepts_single_table_alias() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlWriteEntity {
            id: 1,
            name: "Ada".to_string(),
            age: 21,
        })
        .expect("typed setup insert should succeed");

    let rows = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity s SET s.name = 'Bea', s.age = 22 WHERE s.id = 1",
    )
    .expect("SQL UPDATE with one table alias should succeed");

    assert_eq!(
        rows,
        vec![vec![
            Value::Uint(1),
            Value::Text("Bea".to_string()),
            Value::Uint(22),
        ]],
    );
}

#[test]
fn execute_sql_dispatch_update_with_non_primary_key_predicate_updates_matching_rows() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlWriteEntity {
            id: 1,
            name: "Ada".to_string(),
            age: 21,
        })
        .expect("typed setup insert should succeed");
    session
        .insert(SessionSqlWriteEntity {
            id: 2,
            name: "Bea".to_string(),
            age: 21,
        })
        .expect("typed setup insert should succeed");
    session
        .insert(SessionSqlWriteEntity {
            id: 3,
            name: "Cid".to_string(),
            age: 30,
        })
        .expect("typed setup insert should succeed");

    let rows = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21",
    )
    .expect("SQL UPDATE with non-primary-key predicate should succeed");
    let persisted = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
    )
    .expect("post-update SQL projection should succeed");

    assert_eq!(
        rows,
        vec![
            vec![
                Value::Uint(1),
                Value::Text("Ada".to_string()),
                Value::Uint(22),
            ],
            vec![
                Value::Uint(2),
                Value::Text("Bea".to_string()),
                Value::Uint(22),
            ],
        ],
    );
    assert_eq!(
        persisted,
        vec![
            vec![
                Value::Uint(1),
                Value::Text("Ada".to_string()),
                Value::Uint(22),
            ],
            vec![
                Value::Uint(2),
                Value::Text("Bea".to_string()),
                Value::Uint(22),
            ],
            vec![
                Value::Uint(3),
                Value::Text("Cid".to_string()),
                Value::Uint(30),
            ],
        ],
    );
}

#[test]
fn execute_sql_dispatch_insert_requires_primary_key_column() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_dispatch::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (name, age) VALUES ('Ada', 21)",
        )
        .expect_err("SQL INSERT without explicit primary key should stay fail-closed");

    assert!(
        err.to_string()
            .contains("SQL INSERT requires primary key column 'id'"),
        "INSERT without primary key should keep an actionable boundary message",
    );
}

#[test]
fn execute_sql_dispatch_insert_rejects_insert_select() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_dispatch::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) \
             SELECT id, name, age FROM SessionSqlWriteEntity",
        )
        .expect_err("SQL INSERT SELECT should stay fail-closed");

    assert_sql_unsupported_feature_detail(err, "INSERT ... SELECT");
}

#[test]
fn execute_sql_dispatch_insert_rejects_tuple_length_mismatch() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_dispatch::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21), (2, 'Bea')",
        )
        .expect_err("SQL INSERT with tuple length mismatch should stay fail-closed");

    assert!(
        err.to_string()
            .contains("INSERT column list and VALUES tuple length must match"),
        "INSERT tuple length mismatch should keep an actionable parser boundary message",
    );
}

#[test]
fn execute_sql_dispatch_update_requires_where_predicate() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_dispatch::<SessionSqlWriteEntity>("UPDATE SessionSqlWriteEntity SET age = 22")
        .expect_err("SQL UPDATE without WHERE predicate should stay fail-closed");

    assert!(
        err.to_string()
            .contains("SQL UPDATE requires WHERE predicate"),
        "UPDATE without WHERE predicate should keep an actionable boundary message",
    );
}

#[test]
fn execute_sql_dispatch_update_rejects_order_limit_and_offset() {
    reset_session_sql_store();
    let session = sql_session();

    let cases = [
        (
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 ORDER BY id",
            "UPDATE ORDER BY",
        ),
        (
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 LIMIT 1",
            "UPDATE LIMIT",
        ),
        (
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 OFFSET 1",
            "UPDATE OFFSET",
        ),
    ];

    for (sql, feature) in cases {
        let err = session
            .execute_sql_dispatch::<SessionSqlWriteEntity>(sql)
            .expect_err("unsupported UPDATE windowing/modifier shape should stay fail-closed");
        assert_sql_unsupported_feature_detail(err, feature);
    }
}

#[test]
fn execute_sql_dispatch_update_rejects_primary_key_mutation() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlWriteEntity {
            id: 1,
            name: "Ada".to_string(),
            age: 21,
        })
        .expect("typed setup insert should succeed");

    let err = session
        .execute_sql_dispatch::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET id = 2, age = 22 WHERE id = 1",
        )
        .expect_err("SQL UPDATE primary-key mutation should stay fail-closed");

    assert!(
        err.to_string()
            .contains("SQL UPDATE does not allow primary key mutation"),
        "UPDATE primary-key mutation should keep an actionable boundary message",
    );
}
