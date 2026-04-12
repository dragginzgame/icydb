use super::*;

#[test]
fn execute_sql_dispatch_single_row_insert_matrix_returns_projection_payload() {
    let cases = [
        (
            "explicit-column insert",
            Some("INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)"),
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (2, 'Bea', 22)",
            vec![vec![
                Value::Uint(2),
                Value::Text("Bea".to_string()),
                Value::Uint(22),
            ]],
        ),
        (
            "single-table-alias insert",
            None,
            "INSERT INTO SessionSqlWriteEntity s (id, name, age) VALUES (6, 'Fae', 26)",
            vec![vec![
                Value::Uint(6),
                Value::Text("Fae".to_string()),
                Value::Uint(26),
            ]],
        ),
        (
            "canonical-order insert",
            None,
            "INSERT INTO SessionSqlWriteEntity VALUES (4, 'Dee', 24)",
            vec![vec![
                Value::Uint(4),
                Value::Text("Dee".to_string()),
                Value::Uint(24),
            ]],
        ),
    ];

    for (context, columns_sql, row_sql, expected_rows) in cases {
        reset_session_sql_store();
        let session = sql_session();

        if let Some(columns_sql) = columns_sql {
            let columns =
                dispatch_projection_columns::<SessionSqlWriteEntity>(&session, columns_sql)
                    .unwrap_or_else(|err| {
                        panic!("{context} should return projection payload: {err}")
                    });
            assert_eq!(columns, vec!["id", "name", "age"]);
        }

        let rows = dispatch_projection_rows::<SessionSqlWriteEntity>(&session, row_sql)
            .unwrap_or_else(|err| panic!("{context} should return one value row: {err}"));

        assert_eq!(
            rows, expected_rows,
            "{context} should preserve returned row values"
        );
    }
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
fn execute_sql_dispatch_insert_with_schema_generated_primary_key_matrix_accepts_omission() {
    let cases = [
        (
            "named-column omission",
            "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21)",
            "Ada",
            21_u64,
            true,
        ),
        (
            "positional omission",
            "INSERT INTO SessionSqlEntity VALUES ('Bea', 22)",
            "Bea",
            22_u64,
            false,
        ),
    ];

    for (context, sql, expected_name, expected_age, check_persisted) in cases {
        reset_session_sql_store();
        let session = sql_session();

        let rows =
            dispatch_projection_rows::<SessionSqlEntity>(&session, sql).unwrap_or_else(|err| {
                panic!("{context} should synthesize one schema-generated Ulid: {err}")
            });

        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0][0], Value::Ulid(_)));
        assert_eq!(
            rows[0][1..],
            [
                Value::Text(expected_name.to_string()),
                Value::Uint(expected_age),
            ],
        );

        if check_persisted {
            let persisted = dispatch_projection_rows::<SessionSqlEntity>(
                &session,
                "SELECT name, age FROM SessionSqlEntity ORDER BY name ASC",
            )
            .unwrap_or_else(|err| panic!("{context} post-insert projection should succeed: {err}"));
            assert_eq!(
                persisted,
                vec![vec![
                    Value::Text(expected_name.to_string()),
                    Value::Uint(expected_age)
                ]],
            );
        }
    }
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
fn execute_sql_dispatch_insert_rejects_omitted_non_generated_fields() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_dispatch::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name) VALUES (1, 'Ada')",
        )
        .expect_err("SQL INSERT should not consume omitted non-generated field defaults");

    assert!(
        err.to_string()
            .contains("SQL INSERT requires explicit values for non-generated fields age"),
        "INSERT should keep an actionable omitted-field boundary message",
    );
}

#[test]
fn execute_sql_dispatch_insert_rejects_explicit_managed_timestamp_fields() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_dispatch::<SessionSqlManagedWriteEntity>(
            "INSERT INTO SessionSqlManagedWriteEntity (id, name, created_at) VALUES (1, 'Ada', 0)",
        )
        .expect_err("SQL INSERT should reject explicit writes to managed timestamp fields");
    let err_text = err.to_string();

    assert!(
        err_text
            .contains("SQL INSERT does not allow explicit writes to managed field 'created_at'"),
        "INSERT should keep an actionable managed-field boundary message: {err_text}",
    );
}

#[test]
fn execute_sql_dispatch_insert_synthesizes_schema_generated_non_primary_fields() {
    reset_session_sql_store();
    let session = sql_session();

    let rows = dispatch_projection_rows::<SessionSqlGeneratedFieldEntity>(
        &session,
        "INSERT INTO SessionSqlGeneratedFieldEntity (id, name) VALUES (1, 'Ada')",
    )
    .expect("SQL INSERT should synthesize omitted schema-generated non-primary fields");
    let positional_rows = dispatch_projection_rows::<SessionSqlGeneratedFieldEntity>(
        &session,
        "INSERT INTO SessionSqlGeneratedFieldEntity VALUES (2, 'Bea')",
    )
    .expect("positional SQL INSERT should omit schema-generated non-primary fields by width");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Uint(1));
    assert!(matches!(rows[0][1], Value::Ulid(_)));
    assert_eq!(rows[0][2], Value::Text("Ada".to_string()));
    assert_eq!(positional_rows.len(), 1);
    assert_eq!(positional_rows[0][0], Value::Uint(2));
    assert!(matches!(positional_rows[0][1], Value::Ulid(_)));
    assert_eq!(positional_rows[0][2], Value::Text("Bea".to_string()));
}

#[test]
fn execute_sql_dispatch_single_row_update_matrix_returns_projection_payload() {
    let cases = [
        (
            "plain update",
            "UPDATE SessionSqlWriteEntity SET name = 'Bea', age = 22 WHERE id = 1",
            true,
        ),
        (
            "aliased update",
            "UPDATE SessionSqlWriteEntity s SET s.name = 'Bea', s.age = 22 WHERE s.id = 1",
            false,
        ),
    ];

    for (context, sql, check_persisted) in cases {
        reset_session_sql_store();
        let session = sql_session();
        session
            .insert(SessionSqlWriteEntity {
                id: 1,
                name: "Ada".to_string(),
                age: 21,
            })
            .unwrap_or_else(|err| panic!("{context} setup insert should succeed: {err}"));

        let rows = dispatch_projection_rows::<SessionSqlWriteEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should return one value row: {err}"));

        assert_eq!(
            rows,
            vec![vec![
                Value::Uint(1),
                Value::Text("Bea".to_string()),
                Value::Uint(22),
            ]],
        );

        if check_persisted {
            let persisted = dispatch_projection_rows::<SessionSqlWriteEntity>(
                &session,
                "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
            )
            .unwrap_or_else(|err| panic!("{context} post-update projection should succeed: {err}"));
            assert_eq!(persisted, rows);
        }
    }
}

#[test]
fn execute_sql_dispatch_update_rejects_explicit_managed_timestamp_fields() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .execute_sql_dispatch::<SessionSqlManagedWriteEntity>(
            "INSERT INTO SessionSqlManagedWriteEntity (id, name) VALUES (1, 'Ada')",
        )
        .expect("setup insert should succeed");

    let err = session
        .execute_sql_dispatch::<SessionSqlManagedWriteEntity>(
            "UPDATE SessionSqlManagedWriteEntity SET updated_at = 0 WHERE id = 1",
        )
        .expect_err("SQL UPDATE should reject explicit writes to managed timestamp fields");
    let err_text = err.to_string();

    assert!(
        err_text
            .contains("SQL UPDATE does not allow explicit writes to managed field 'updated_at'"),
        "UPDATE should keep an actionable managed-field boundary message: {err_text}",
    );
}

#[test]
fn execute_sql_dispatch_insert_synthesizes_managed_timestamp_fields() {
    reset_session_sql_store();
    let session = sql_session();

    let rows = dispatch_projection_rows::<SessionSqlManagedWriteEntity>(
        &session,
        "INSERT INTO SessionSqlManagedWriteEntity (id, name) VALUES (1, 'Ada')",
    )
    .expect("SQL INSERT should synthesize managed timestamp fields");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Uint(1));
    assert_eq!(rows[0][1], Value::Text("Ada".to_string()));
    assert!(matches!(rows[0][2], Value::Timestamp(_)));
    assert!(matches!(rows[0][3], Value::Timestamp(_)));
}

#[test]
fn execute_sql_dispatch_update_refreshes_managed_updated_at_field() {
    reset_session_sql_store();
    let session = sql_session();
    let inserted = dispatch_projection_rows::<SessionSqlManagedWriteEntity>(
        &session,
        "INSERT INTO SessionSqlManagedWriteEntity (id, name) VALUES (1, 'Ada')",
    )
    .expect("setup insert should succeed");
    let inserted_updated_at = inserted[0][3].clone();

    let rows = dispatch_projection_rows::<SessionSqlManagedWriteEntity>(
        &session,
        "UPDATE SessionSqlManagedWriteEntity SET name = 'Bea' WHERE id = 1",
    )
    .expect("SQL UPDATE should refresh managed updated_at");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Uint(1));
    assert_eq!(rows[0][1], Value::Text("Bea".to_string()));
    assert!(matches!(rows[0][2], Value::Timestamp(_)));
    assert!(matches!(rows[0][3], Value::Timestamp(_)));
    assert_ne!(
        rows[0][3], inserted_updated_at,
        "managed updated_at should be refreshed on update",
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
        .expect_err("SQL INSERT without explicit primary key should stay fail-closed when the schema does not mark it generated");

    assert!(
        err.to_string()
            .contains("SQL INSERT requires primary key column 'id'"),
        "INSERT without primary key should keep an actionable boundary message",
    );
}

#[test]
fn execute_sql_dispatch_insert_select_with_schema_generated_primary_key_copies_rows() {
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
    .expect(
        "SQL INSERT SELECT should copy one schema-generated-key row on the typed dispatch lane",
    );
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
        "computed INSERT SELECT should still synthesize one schema-generated Ulid primary key",
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
fn execute_sql_dispatch_insert_select_rejection_matrix_preserves_boundary_messages() {
    let cases = [
        (
            "aggregate source",
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT COUNT(*), COUNT(*) FROM SessionSqlEntity",
            "SQL INSERT SELECT does not support aggregate source projection",
            vec![(Ulid::from_u128(1), "Ada", 21_u64)],
        ),
        (
            "grouped source",
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT name, COUNT(*) FROM SessionSqlEntity GROUP BY name",
            "SQL INSERT SELECT requires scalar SELECT source",
            vec![
                (Ulid::from_u128(1), "Ada", 21_u64),
                (Ulid::from_u128(2), "Bea", 22_u64),
            ],
        ),
        (
            "unsupported computed source shape",
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT DISTINCT LOWER(name), age FROM SessionSqlEntity WHERE name = 'Ada'",
            "computed SQL projection currently supports only scalar SELECT field lists",
            vec![(Ulid::from_u128(1), "Ada", 21_u64)],
        ),
    ];

    for (context, sql, expected_message, seed_rows) in cases {
        reset_session_sql_store();
        let session = sql_session();

        for (id, name, age) in seed_rows {
            session
                .insert(SessionSqlEntity {
                    id,
                    name: name.to_string(),
                    age,
                })
                .unwrap_or_else(|err| panic!("{context} setup insert should succeed: {err}"));
        }

        let err = session
            .execute_sql_dispatch::<SessionSqlEntity>(sql)
            .expect_err("INSERT SELECT unsupported source shape should stay fail-closed");

        assert!(
            err.to_string().contains(expected_message),
            "{context} should keep an actionable boundary message",
        );
    }
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
fn execute_sql_dispatch_insert_rejects_returning_clause() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_dispatch::<SessionSqlEntity>(
            "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21) RETURNING id",
        )
        .expect_err("SQL INSERT RETURNING should stay fail-closed");

    assert_sql_unsupported_feature_detail(err, "RETURNING");
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
fn execute_sql_dispatch_update_rejects_returning_clause() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_dispatch::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING id",
        )
        .expect_err("SQL UPDATE RETURNING should stay fail-closed");

    assert_sql_unsupported_feature_detail(err, "RETURNING");
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
