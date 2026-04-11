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
fn execute_sql_dispatch_update_requires_primary_key_equality_predicate() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_dispatch::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21",
        )
        .expect_err("SQL UPDATE without primary-key equality should stay fail-closed");

    assert!(
        err.to_string()
            .contains("SQL UPDATE requires WHERE id = literal"),
        "UPDATE without primary-key equality should keep an actionable boundary message",
    );
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
