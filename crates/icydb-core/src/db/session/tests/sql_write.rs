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
fn execute_sql_dispatch_insert_accepts_single_table_alias() {
    reset_session_sql_store();
    let session = sql_session();

    let rows = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity s (id, name, age) VALUES (6, 'Fae', 26)",
    )
    .expect("SQL INSERT with one table alias should succeed");

    assert_eq!(
        rows,
        vec![vec![
            Value::Uint(6),
            Value::Text("Fae".to_string()),
            Value::Uint(26),
        ]],
    );
}

#[test]
fn execute_sql_dispatch_insert_with_generated_ulid_primary_key_accepts_missing_pk_column() {
    reset_session_sql_store();
    let session = sql_session();

    let rows = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21)",
    )
    .expect("SQL INSERT should synthesize one Ulid primary key when the target entity owns it");
    let persisted = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name, age FROM SessionSqlEntity ORDER BY name ASC",
    )
    .expect("post-insert SQL projection should succeed");

    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Ulid(_)));
    assert_eq!(
        rows[0][1..],
        [Value::Text("Ada".to_string()), Value::Uint(21),],
    );
    assert_eq!(
        persisted,
        vec![vec![Value::Text("Ada".to_string()), Value::Uint(21)]],
    );
}

#[test]
fn execute_sql_dispatch_insert_with_generated_ulid_primary_key_accepts_positional_omission() {
    reset_session_sql_store();
    let session = sql_session();

    let rows = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "INSERT INTO SessionSqlEntity VALUES ('Bea', 22)",
    )
    .expect("positional SQL INSERT should synthesize one Ulid primary key when omitted");

    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Ulid(_)));
    assert_eq!(
        rows[0][1..],
        [Value::Text("Bea".to_string()), Value::Uint(22),],
    );
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
fn execute_sql_dispatch_update_with_order_limit_and_offset_updates_one_ordered_window() {
    reset_session_sql_store();
    let session = sql_session();
    for (id, name, age) in [
        (1, "Ada", 21_u64),
        (2, "Bea", 30_u64),
        (3, "Cid", 25_u64),
        (4, "Dee", 40_u64),
    ] {
        session
            .insert(SessionSqlWriteEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("typed setup insert should succeed");
    }

    let rows = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 99 WHERE age >= 21 ORDER BY age DESC LIMIT 2 OFFSET 1",
    )
    .expect("SQL UPDATE ordered window should succeed");
    let persisted = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
    )
    .expect("post-update SQL projection should succeed");

    assert_eq!(
        rows,
        vec![
            vec![
                Value::Uint(2),
                Value::Text("Bea".to_string()),
                Value::Uint(99),
            ],
            vec![
                Value::Uint(3),
                Value::Text("Cid".to_string()),
                Value::Uint(99),
            ],
        ],
    );
    assert_eq!(
        persisted,
        vec![
            vec![
                Value::Uint(1),
                Value::Text("Ada".to_string()),
                Value::Uint(21),
            ],
            vec![
                Value::Uint(2),
                Value::Text("Bea".to_string()),
                Value::Uint(99),
            ],
            vec![
                Value::Uint(3),
                Value::Text("Cid".to_string()),
                Value::Uint(99),
            ],
            vec![
                Value::Uint(4),
                Value::Text("Dee".to_string()),
                Value::Uint(40),
            ],
        ],
    );
}

#[test]
fn execute_sql_dispatch_update_with_limit_and_offset_uses_primary_key_order_fallback() {
    reset_session_sql_store();
    let session = sql_session();
    for (id, name, age) in [(1, "Ada", 21_u64), (2, "Bea", 21_u64), (3, "Cid", 21_u64)] {
        session
            .insert(SessionSqlWriteEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("typed setup insert should succeed");
    }

    let rows = dispatch_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 LIMIT 1 OFFSET 1",
    )
    .expect("SQL UPDATE window without ORDER BY should use deterministic primary-key fallback");

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
fn execute_sql_dispatch_insert_select_with_generated_ulid_primary_key_copies_rows() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(1),
            name: "Ada".to_string(),
            age: 21,
        })
        .expect("typed setup insert should succeed");

    let rows = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "INSERT INTO SessionSqlEntity (name, age) \
         SELECT name, age FROM SessionSqlEntity WHERE name = 'Ada' ORDER BY id ASC LIMIT 1",
    )
    .expect("SQL INSERT SELECT should copy one generated-key row on the typed dispatch lane");
    let persisted = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name, age FROM SessionSqlEntity WHERE name = 'Ada' ORDER BY age ASC LIMIT 10",
    )
    .expect("post-insert-select SQL projection should succeed");

    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Ulid(_)));
    assert_ne!(rows[0][0], Value::Ulid(Ulid::from_u128(1)));
    assert_eq!(
        rows[0][1..],
        [Value::Text("Ada".to_string()), Value::Uint(21),],
    );
    assert_eq!(
        persisted,
        vec![
            vec![Value::Text("Ada".to_string()), Value::Uint(21)],
            vec![Value::Text("Ada".to_string()), Value::Uint(21)],
        ],
    );
}

#[test]
fn execute_sql_dispatch_insert_select_accepts_scalar_computed_projection() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(1),
            name: "Ada".to_string(),
            age: 21,
        })
        .expect("typed setup insert should succeed");

    let rows = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "INSERT INTO SessionSqlEntity (name, age) \
         SELECT LOWER(name), age FROM SessionSqlEntity WHERE name = 'Ada' ORDER BY id ASC LIMIT 1",
    )
    .expect("INSERT SELECT should reuse the admitted scalar computed-projection lane");
    let persisted = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name, age FROM SessionSqlEntity ORDER BY name ASC LIMIT 10",
    )
    .expect("post-insert-select SQL projection should succeed");

    assert!(
        matches!(rows[0][0], Value::Ulid(_)),
        "computed INSERT SELECT should still synthesize one generated Ulid primary key",
    );
    assert_eq!(
        rows[0][1..],
        [Value::Text("ada".to_string()), Value::Uint(21),],
    );
    assert_eq!(
        persisted,
        vec![
            vec![Value::Text("Ada".to_string()), Value::Uint(21)],
            vec![Value::Text("ada".to_string()), Value::Uint(21)],
        ],
    );
}

#[test]
fn execute_sql_dispatch_insert_select_rejects_aggregate_source_projection() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(1),
            name: "Ada".to_string(),
            age: 21,
        })
        .expect("typed setup insert should succeed");

    let err = session
        .execute_sql_dispatch::<SessionSqlEntity>(
            "INSERT INTO SessionSqlEntity (name) \
             SELECT COUNT(*) FROM SessionSqlEntity",
        )
        .expect_err("INSERT SELECT aggregate source should stay fail-closed");

    assert!(
        err.to_string()
            .contains("SQL INSERT SELECT does not support aggregate source projection"),
        "INSERT SELECT aggregate source should keep an actionable boundary message",
    );
}

#[test]
fn execute_sql_dispatch_insert_select_rejects_grouped_source_projection() {
    reset_session_sql_store();
    let session = sql_session();
    for (id, name, age) in [
        (Ulid::from_u128(1), "Ada", 21_u64),
        (Ulid::from_u128(2), "Bea", 22_u64),
    ] {
        session
            .insert(SessionSqlEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("typed setup insert should succeed");
    }

    let err = session
        .execute_sql_dispatch::<SessionSqlEntity>(
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT name, COUNT(*) FROM SessionSqlEntity GROUP BY name",
        )
        .expect_err("INSERT SELECT grouped source should stay fail-closed");

    assert!(
        err.to_string()
            .contains("SQL INSERT SELECT requires scalar SELECT source"),
        "INSERT SELECT grouped source should keep an actionable scalar-source boundary message",
    );
}

#[test]
fn execute_sql_dispatch_insert_select_rejects_unsupported_computed_source_shape() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(1),
            name: "Ada".to_string(),
            age: 21,
        })
        .expect("typed setup insert should succeed");

    let err = session
        .execute_sql_dispatch::<SessionSqlEntity>(
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT DISTINCT LOWER(name), age FROM SessionSqlEntity WHERE name = 'Ada'",
        )
        .expect_err("INSERT SELECT unsupported computed source shape should stay fail-closed");

    assert!(
        err.to_string()
            .contains("computed SQL projection currently supports only scalar SELECT field lists"),
        "INSERT SELECT unsupported computed source shape should keep the computed-lane boundary message",
    );
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
fn execute_sql_dispatch_update_rejects_invalid_window_clause_order() {
    reset_session_sql_store();
    let session = sql_session();

    let cases = [
        (
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 LIMIT 1 ORDER BY id",
            "ORDER BY must appear before LIMIT/OFFSET in UPDATE",
        ),
        (
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 OFFSET 1 LIMIT 1",
            "LIMIT must appear before OFFSET in UPDATE",
        ),
    ];

    for (sql, message) in cases {
        let err = session
            .execute_sql_dispatch::<SessionSqlWriteEntity>(sql)
            .expect_err("invalid UPDATE window clause order should stay fail-closed");
        assert!(
            err.to_string().contains(message),
            "invalid UPDATE window clause ordering should keep an actionable parser boundary message",
        );
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
