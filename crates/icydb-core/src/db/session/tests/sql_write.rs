use super::*;
use crate::db::{MutationMode, UpdatePatch};

#[test]
fn execute_sql_statement_single_row_insert_matrix_returns_count_without_returning() {
    let cases = [
        (
            "explicit-column insert",
            Some(
                "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21) RETURNING id",
            ),
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (2, 'Bea', 22)",
            1_u32,
        ),
        (
            "single-table-alias insert",
            None,
            "INSERT INTO SessionSqlWriteEntity s (id, name, age) VALUES (6, 'Fae', 26)",
            1_u32,
        ),
        (
            "canonical-order insert",
            None,
            "INSERT INTO SessionSqlWriteEntity VALUES (4, 'Dee', 24)",
            1_u32,
        ),
    ];

    for (context, columns_sql, row_sql, expected_row_count) in cases {
        reset_session_sql_store();
        let session = sql_session();

        if let Some(columns_sql) = columns_sql {
            let columns =
                statement_projection_columns::<SessionSqlWriteEntity>(&session, columns_sql)
                    .unwrap_or_else(|err| {
                        panic!("{context} should return projection payload: {err}")
                    });
            assert_eq!(columns, vec!["id"]);
        }

        let payload = session
            .execute_sql_statement::<SessionSqlWriteEntity>(row_sql)
            .unwrap_or_else(|err| panic!("{context} should return count payload: {err}"));
        let SqlStatementResult::Count { row_count } = payload else {
            panic!("{context} should return count payload");
        };
        assert_eq!(
            row_count, expected_row_count,
            "{context} should follow traditional SQL count semantics without RETURNING",
        );
    }
}

#[test]
fn execute_sql_statement_multi_row_insert_matrix_returns_count_without_returning() {
    for (sql, expected_row_count, check_persisted, context) in [
        (
            "INSERT INTO SessionSqlWriteEntity (id, name, age) \
             VALUES (2, 'Bea', 22), (3, 'Cid', 23)",
            2_u32,
            true,
            "explicit-column multi-row insert",
        ),
        (
            "INSERT INTO SessionSqlWriteEntity VALUES (4, 'Dee', 24), (5, 'Eli', 25)",
            2_u32,
            false,
            "canonical-order multi-row insert",
        ),
    ] {
        reset_session_sql_store();
        let session = sql_session();

        let payload = session
            .execute_sql_statement::<SessionSqlWriteEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} should return count payload: {err}"));
        let SqlStatementResult::Count { row_count } = payload else {
            panic!("{context} should return count payload");
        };
        assert_eq!(
            row_count, expected_row_count,
            "{context} should follow traditional SQL count semantics without RETURNING",
        );

        if check_persisted {
            let persisted = statement_projection_rows::<SessionSqlWriteEntity>(
                &session,
                "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
            )
            .unwrap_or_else(|err| panic!("{context} post-insert projection should succeed: {err}"));
            assert_eq!(
                persisted.len(),
                usize::try_from(expected_row_count).unwrap_or(usize::MAX),
                "{context} should persist the counted insert rows",
            );
        }
    }
}

#[test]
fn execute_sql_statement_insert_with_schema_generated_primary_key_matrix_accepts_omission() {
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

        let rows = statement_projection_rows::<SessionSqlEntity>(
            &session,
            match sql {
                "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21)" => {
                    "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21) RETURNING *"
                }
                "INSERT INTO SessionSqlEntity VALUES ('Bea', 22)" => {
                    "INSERT INTO SessionSqlEntity VALUES ('Bea', 22) RETURNING *"
                }
                _ => unreachable!("generated-key insert matrix uses fixed SQL cases"),
            },
        )
        .unwrap_or_else(|err| {
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
            let persisted = statement_projection_rows::<SessionSqlEntity>(
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
fn execute_sql_statement_insert_rejects_missing_required_fields_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_message, context) in [
        (
            "INSERT INTO SessionSqlWriteEntity (id, name) VALUES (1, 'Ada')",
            "SQL INSERT requires explicit values for non-generated fields age",
            "missing non-generated field",
        ),
        (
            "INSERT INTO SessionSqlWriteEntity (name, age) VALUES ('Ada', 21)",
            "SQL INSERT requires primary key column 'id'",
            "missing primary key field",
        ),
    ] {
        let err = session
            .execute_sql_statement::<SessionSqlWriteEntity>(sql)
            .expect_err("missing required INSERT fields should stay fail-closed");

        assert!(
            err.to_string().contains(expected_message),
            "{context} should keep an actionable boundary message",
        );
    }
}

#[test]
fn execute_sql_statement_write_rejects_explicit_managed_timestamp_fields_matrix() {
    let cases = [
        (
            "INSERT INTO SessionSqlManagedWriteEntity (id, name, created_at) VALUES (1, 'Ada', 0)",
            "SQL INSERT does not allow explicit writes to managed field 'created_at'",
            "INSERT explicit managed timestamp write",
            false,
        ),
        (
            "UPDATE SessionSqlManagedWriteEntity SET updated_at = 0 WHERE id = 1",
            "SQL UPDATE does not allow explicit writes to managed field 'updated_at'",
            "UPDATE explicit managed timestamp write",
            true,
        ),
    ];

    for (sql, expected_message, context, seed_row) in cases {
        reset_session_sql_store();
        let session = sql_session();

        if seed_row {
            session
                .insert(SessionSqlManagedWriteEntity {
                    id: 1,
                    name: "Ada".to_string(),
                    created_at: Timestamp::from_nanos(1),
                    updated_at: Timestamp::from_nanos(1),
                })
                .unwrap_or_else(|err| panic!("{context} setup insert should succeed: {err}"));
        }

        let err = session
            .execute_sql_statement::<SessionSqlManagedWriteEntity>(sql)
            .expect_err("managed timestamp writes should stay fail-closed");
        let err_text = err.to_string();

        assert!(
            err_text.contains(expected_message),
            "{context} should keep an actionable managed-field boundary message: {err_text}",
        );
    }
}

#[test]
fn execute_sql_statement_insert_rejects_explicit_generated_fields_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_message, context) in [
        (
            "INSERT INTO SessionSqlGeneratedTimestampEntity (id, created_on_insert, name) VALUES (1, 7, 'Ada')",
            "SQL INSERT does not allow explicit writes to generated field 'created_on_insert'",
            "named-column generated timestamp insert",
        ),
        (
            "INSERT INTO SessionSqlGeneratedTimestampEntity VALUES (2, 9, 'Bea')",
            "SQL INSERT does not allow explicit writes to generated field 'created_on_insert'",
            "positional generated timestamp insert",
        ),
    ] {
        let err = session
            .execute_sql_statement::<SessionSqlGeneratedTimestampEntity>(sql)
            .expect_err("insert-generated fields should stay system-owned on SQL INSERT");
        let err_text = err.to_string();

        assert!(
            err_text.contains(expected_message),
            "{context} should keep an actionable generated-field boundary message: {err_text}",
        );
    }
}

#[test]
fn execute_sql_statement_insert_synthesizes_schema_generated_fields_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    for (named_rows, positional_rows, generated_kind, context) in [
        (
            statement_projection_rows::<SessionSqlGeneratedFieldEntity>(
                &session,
                "INSERT INTO SessionSqlGeneratedFieldEntity (id, name) VALUES (1, 'Ada') RETURNING *",
            )
            .expect("SQL INSERT should synthesize omitted schema-generated non-primary fields"),
            statement_projection_rows::<SessionSqlGeneratedFieldEntity>(
                &session,
                "INSERT INTO SessionSqlGeneratedFieldEntity VALUES (2, 'Bea') RETURNING *",
            )
            .expect(
                "positional SQL INSERT should omit schema-generated non-primary fields by width",
            ),
            "ulid",
            "schema-generated non-primary field",
        ),
        (
            statement_projection_rows::<SessionSqlGeneratedTimestampEntity>(
                &session,
                "INSERT INTO SessionSqlGeneratedTimestampEntity (id, name) VALUES (1, 'Ada') RETURNING *",
            )
            .expect("SQL INSERT should synthesize omitted schema-generated timestamp fields"),
            statement_projection_rows::<SessionSqlGeneratedTimestampEntity>(
                &session,
                "INSERT INTO SessionSqlGeneratedTimestampEntity VALUES (2, 'Bea') RETURNING *",
            )
            .expect("positional SQL INSERT should omit schema-generated timestamp fields by width"),
            "timestamp",
            "schema-generated timestamp field",
        ),
    ] {
        assert_eq!(
            named_rows.len(),
            1,
            "{context} named insert should return one row"
        );
        assert_eq!(
            positional_rows.len(),
            1,
            "{context} positional insert should return one row",
        );
        assert_eq!(named_rows[0][0], Value::Uint(1));
        assert_eq!(positional_rows[0][0], Value::Uint(2));
        match generated_kind {
            "ulid" => {
                assert!(
                    matches!(named_rows[0][1], Value::Ulid(_)),
                    "{context} named insert should synthesize a Ulid field",
                );
                assert!(
                    matches!(positional_rows[0][1], Value::Ulid(_)),
                    "{context} positional insert should synthesize a Ulid field",
                );
            }
            "timestamp" => {
                assert!(
                    matches!(named_rows[0][1], Value::Timestamp(_)),
                    "{context} named insert should synthesize a timestamp field",
                );
                assert!(
                    matches!(positional_rows[0][1], Value::Timestamp(_)),
                    "{context} positional insert should synthesize a timestamp field",
                );
            }
            other => panic!("unexpected generated field kind: {other}"),
        }
        assert_eq!(named_rows[0][2], Value::Text("Ada".to_string()));
        assert_eq!(positional_rows[0][2], Value::Text("Bea".to_string()));
    }
}

#[test]
fn structural_create_rejects_explicit_generated_insert_fields_matrix() {
    let cases = [
        (
            MutationMode::Insert,
            1_u64,
            UpdatePatch::new()
                .set_field(
                    SessionSqlGeneratedTimestampEntity::MODEL,
                    "id",
                    Value::Uint(1),
                )
                .expect("generated timestamp structural insert should resolve id")
                .set_field(
                    SessionSqlGeneratedTimestampEntity::MODEL,
                    "created_on_insert",
                    Value::Timestamp(Timestamp::from_nanos(7)),
                )
                .expect("generated timestamp structural insert should resolve generated field")
                .set_field(
                    SessionSqlGeneratedTimestampEntity::MODEL,
                    "name",
                    Value::Text("Ada".to_string()),
                )
                .expect("generated timestamp structural insert should resolve name"),
            "created_on_insert",
            "structural insert explicit generated timestamp",
        ),
        (
            MutationMode::Replace,
            2_u64,
            UpdatePatch::new()
                .set_field(
                    SessionSqlGeneratedTimestampEntity::MODEL,
                    "id",
                    Value::Uint(2),
                )
                .expect("generated timestamp structural replace should resolve id")
                .set_field(
                    SessionSqlGeneratedTimestampEntity::MODEL,
                    "created_on_insert",
                    Value::Timestamp(Timestamp::from_nanos(9)),
                )
                .expect("generated timestamp structural replace should resolve generated field")
                .set_field(
                    SessionSqlGeneratedTimestampEntity::MODEL,
                    "name",
                    Value::Text("Bea".to_string()),
                )
                .expect("generated timestamp structural replace should resolve name"),
            "created_on_insert",
            "structural replace-on-missing explicit generated timestamp",
        ),
    ];

    for (mode, key, patch, field_name, context) in cases {
        reset_session_sql_store();
        let session = sql_session();

        let err = session
            .mutate_structural::<SessionSqlGeneratedTimestampEntity>(key, patch, mode)
            .expect_err("structural create lanes should reject explicit insert-generated fields");

        assert_eq!(err.class(), ErrorClass::Unsupported);
        assert!(
            err.message
                .contains("generated field may not be explicitly written"),
            "{context} should preserve the generated-field ownership message: {}",
            err.message,
        );
        assert!(
            err.message.contains(field_name),
            "{context} should name the rejected generated field: {}",
            err.message,
        );
    }
}

#[test]
fn execute_sql_statement_update_rejects_explicit_generated_fields_matrix() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlGeneratedTimestampEntity {
            id: 1,
            created_on_insert: Timestamp::from_nanos(1),
            name: "Ada".to_string(),
        })
        .expect("generated timestamp update setup insert should succeed");

    let err = session
        .execute_sql_statement::<SessionSqlGeneratedTimestampEntity>(
            "UPDATE SessionSqlGeneratedTimestampEntity SET created_on_insert = 7 WHERE id = 1",
        )
        .expect_err("insert-generated fields should stay system-owned on SQL UPDATE");
    let err_text = err.to_string();

    assert!(
        err_text.contains(
            "SQL UPDATE does not allow explicit writes to generated field 'created_on_insert'",
        ),
        "SQL UPDATE should keep the generated-field ownership boundary explicit: {err_text}",
    );
}

#[test]
fn structural_rewrite_rejects_explicit_generated_insert_fields_matrix() {
    let cases = [
        (
            MutationMode::Update,
            "structural update explicit generated timestamp",
        ),
        (
            MutationMode::Replace,
            "structural replace-existing explicit generated timestamp",
        ),
    ];

    for (mode, context) in cases {
        reset_session_sql_store();
        let session = sql_session();
        session
            .insert(SessionSqlGeneratedTimestampEntity {
                id: 1,
                created_on_insert: Timestamp::from_nanos(1),
                name: "Ada".to_string(),
            })
            .unwrap_or_else(|err| panic!("{context} setup insert should succeed: {err}"));

        let patch = UpdatePatch::new()
            .set_field(
                SessionSqlGeneratedTimestampEntity::MODEL,
                "created_on_insert",
                Value::Timestamp(Timestamp::from_nanos(9)),
            )
            .expect("generated timestamp structural rewrite should resolve generated field");
        let err = session
            .mutate_structural::<SessionSqlGeneratedTimestampEntity>(1, patch, mode)
            .expect_err("structural rewrites should reject explicit insert-generated fields");

        assert_eq!(err.class(), ErrorClass::Unsupported);
        assert!(
            err.message
                .contains("generated field may not be explicitly written"),
            "{context} should preserve the generated-field ownership message: {}",
            err.message,
        );
        assert!(
            err.message.contains("created_on_insert"),
            "{context} should name the rejected generated field: {}",
            err.message,
        );
    }
}

#[test]
fn execute_sql_statement_single_row_update_matrix_returns_count_without_returning() {
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

        let payload = session
            .execute_sql_statement::<SessionSqlWriteEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} should return count payload: {err}"));
        let SqlStatementResult::Count { row_count } = payload else {
            panic!("{context} should return count payload");
        };
        assert_eq!(
            row_count, 1,
            "{context} should follow traditional SQL count semantics without RETURNING",
        );

        if check_persisted {
            let persisted = statement_projection_rows::<SessionSqlWriteEntity>(
                &session,
                "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
            )
            .unwrap_or_else(|err| panic!("{context} post-update projection should succeed: {err}"));
            assert_eq!(
                persisted,
                vec![vec![
                    Value::Uint(1),
                    Value::Text("Bea".to_string()),
                    Value::Uint(22),
                ]],
            );
        }
    }
}

#[test]
fn execute_sql_statement_signed_numeric_write_matrix_widens_parser_literals() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlSignedWriteEntity { id: 1, delta: -5 })
        .expect("signed write setup insert should succeed");

    let payload = session
        .execute_sql_statement::<SessionSqlSignedWriteEntity>(
            "UPDATE SessionSqlSignedWriteEntity SET delta = 7 WHERE id = 1",
        )
        .expect("signed SQL UPDATE should widen parser literals onto signed field contracts");
    let SqlStatementResult::Count { row_count } = payload else {
        panic!("signed SQL UPDATE should return count payload without RETURNING");
    };
    assert_eq!(row_count, 1);

    let persisted = statement_projection_rows::<SessionSqlSignedWriteEntity>(
        &session,
        "SELECT id, delta FROM SessionSqlSignedWriteEntity ORDER BY id ASC",
    )
    .expect("signed post-update projection should succeed");

    assert_eq!(persisted, vec![vec![Value::Int(1), Value::Int(7)]]);
}

#[test]
fn execute_sql_statement_signed_numeric_insert_widens_parser_literals() {
    reset_session_sql_store();
    let session = sql_session();

    let payload = session
        .execute_sql_statement::<SessionSqlSignedWriteEntity>(
            "INSERT INTO SessionSqlSignedWriteEntity (id, delta) VALUES (2, 9)",
        )
        .expect("signed SQL INSERT should widen parser literals onto signed field contracts");
    let SqlStatementResult::Count { row_count } = payload else {
        panic!("signed SQL INSERT should return count payload without RETURNING");
    };
    assert_eq!(row_count, 1);

    let persisted = statement_projection_rows::<SessionSqlSignedWriteEntity>(
        &session,
        "SELECT id, delta FROM SessionSqlSignedWriteEntity ORDER BY id ASC",
    )
    .expect("signed post-insert projection should succeed");

    assert_eq!(persisted, vec![vec![Value::Int(2), Value::Int(9)]]);
}

#[test]
fn execute_sql_statement_rejects_incompatible_assignment_literal_for_signed_field() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlSignedWriteEntity { id: 1, delta: -5 })
        .expect("signed write setup insert should succeed");

    let err = session
        .execute_sql_statement::<SessionSqlSignedWriteEntity>(
            "UPDATE SessionSqlSignedWriteEntity SET delta = 'Ada' WHERE id = 1",
        )
        .expect_err("signed field assignment should stay fail-closed for incompatible literals");

    assert!(
        err.to_string()
            .contains("invalid literal for field 'delta': literal type does not match field type"),
        "incompatible signed assignment should keep the literal-type boundary explicit",
    );
}

#[test]
fn execute_sql_statement_update_with_non_primary_key_predicate_updates_matching_rows() {
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

    let payload = session
        .execute_sql_statement::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21",
        )
        .expect("SQL UPDATE with non-primary-key predicate should succeed");
    let SqlStatementResult::Count { row_count } = payload else {
        panic!("SQL UPDATE with non-primary-key predicate should return count payload");
    };
    let persisted = statement_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
    )
    .expect("post-update SQL projection should succeed");

    assert_eq!(row_count, 2);
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
fn execute_sql_statement_update_with_order_limit_and_offset_updates_one_ordered_window() {
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

    let payload = session
        .execute_sql_statement::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 99 WHERE age >= 21 ORDER BY age DESC LIMIT 2 OFFSET 1",
        )
        .expect("SQL UPDATE ordered window should succeed");
    let SqlStatementResult::Count { row_count } = payload else {
        panic!("SQL UPDATE ordered window should return count payload");
    };
    let persisted = statement_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
    )
    .expect("post-update SQL projection should succeed");

    assert_eq!(row_count, 2);
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
fn execute_sql_statement_update_with_limit_and_offset_uses_primary_key_order_fallback() {
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

    let payload = session
        .execute_sql_statement::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 LIMIT 1 OFFSET 1",
        )
        .expect("SQL UPDATE window without ORDER BY should use deterministic primary-key fallback");
    let SqlStatementResult::Count { row_count } = payload else {
        panic!("SQL UPDATE window without ORDER BY should return count payload");
    };
    assert_eq!(row_count, 1);

    let persisted = statement_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
    )
    .expect("post-update SQL projection should succeed");

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
                Value::Uint(22),
            ],
            vec![
                Value::Uint(3),
                Value::Text("Cid".to_string()),
                Value::Uint(21),
            ],
        ],
    );
}

#[test]
fn execute_sql_statement_write_rejects_entity_mismatch_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, sql_entity, context) in [
        (
            "INSERT INTO SessionSqlGeneratedFieldEntity (id, name) VALUES (1, 'Ada')",
            "SessionSqlGeneratedFieldEntity",
            "insert entity mismatch",
        ),
        (
            "UPDATE SessionSqlGeneratedTimestampEntity SET name = 'Ada' WHERE id = 1",
            "SessionSqlGeneratedTimestampEntity",
            "update entity mismatch",
        ),
    ] {
        let err = session
            .execute_sql_statement::<SessionSqlWriteEntity>(sql)
            .expect_err("write statement should keep typed entity matching fail-closed");
        let err_text = err.to_string();

        assert!(
            err_text.contains(&format!(
                "SQL entity '{sql_entity}' does not match requested entity type 'SessionSqlWriteEntity'"
            )),
            "{context} should keep the typed write route mismatch boundary explicit",
        );
    }
}

#[test]
fn execute_sql_statement_insert_select_matrix_accepts_supported_source_shapes() {
    for (_sql, expected_inserted_name, persisted_sql, expected_persisted, context) in [
        (
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT name, age FROM SessionSqlEntity WHERE name = 'Ada' ORDER BY id ASC LIMIT 1",
            "Ada",
            "SELECT name, age FROM SessionSqlEntity WHERE name = 'Ada' ORDER BY age ASC LIMIT 10",
            vec![
                vec![Value::Text("Ada".to_string()), Value::Uint(21)],
                vec![Value::Text("Ada".to_string()), Value::Uint(21)],
            ],
            "plain INSERT SELECT",
        ),
        (
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT LOWER(name), age FROM SessionSqlEntity WHERE name = 'Ada' ORDER BY id ASC LIMIT 1",
            "ada",
            "SELECT name, age FROM SessionSqlEntity ORDER BY name ASC LIMIT 10",
            vec![
                vec![Value::Text("Ada".to_string()), Value::Uint(21)],
                vec![Value::Text("ada".to_string()), Value::Uint(21)],
            ],
            "computed INSERT SELECT",
        ),
    ] {
        reset_session_sql_store();
        let session = sql_session();
        session
            .insert(SessionSqlEntity {
                id: Ulid::from_u128(1),
                name: "Ada".to_string(),
                age: 21,
            })
            .unwrap_or_else(|err| panic!("{context} setup insert should succeed: {err}"));

        let returning_sql = match context {
            "plain INSERT SELECT" => {
                "INSERT INTO SessionSqlEntity (name, age) \
                 SELECT name, age FROM SessionSqlEntity WHERE name = 'Ada' ORDER BY id ASC LIMIT 1 RETURNING *"
            }
            "computed INSERT SELECT" => {
                "INSERT INTO SessionSqlEntity (name, age) \
                 SELECT LOWER(name), age FROM SessionSqlEntity WHERE name = 'Ada' ORDER BY id ASC LIMIT 1 RETURNING *"
            }
            other => panic!("unexpected INSERT SELECT context: {other}"),
        };

        let rows = statement_projection_rows::<SessionSqlEntity>(&session, returning_sql)
            .unwrap_or_else(|err| panic!("{context} should succeed with RETURNING: {err}"));
        let persisted = statement_projection_rows::<SessionSqlEntity>(&session, persisted_sql)
            .unwrap_or_else(|err| {
                panic!("{context} post-insert-select projection should succeed: {err}")
            });

        assert_eq!(rows.len(), 1, "{context} should insert one row");
        assert!(
            matches!(rows[0][0], Value::Ulid(_)),
            "{context} should synthesize one schema-generated Ulid primary key",
        );
        assert_ne!(
            rows[0][0],
            Value::Ulid(Ulid::from_u128(1)),
            "{context} should allocate a fresh generated primary key",
        );
        assert_eq!(
            rows[0][1..],
            [
                Value::Text(expected_inserted_name.to_string()),
                Value::Uint(21)
            ],
            "{context} should preserve the projected source payload",
        );
        assert_eq!(
            persisted, expected_persisted,
            "{context} should persist the expected post-insert rows",
        );
    }
}

#[test]
fn execute_sql_statement_insert_select_rejection_matrix_preserves_boundary_messages() {
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
            .execute_sql_statement::<SessionSqlEntity>(sql)
            .expect_err("INSERT SELECT unsupported source shape should stay fail-closed");

        assert!(
            err.to_string().contains(expected_message),
            "{context} should keep an actionable boundary message",
        );
    }
}

#[test]
fn execute_sql_statement_write_rejects_incompatible_primary_key_literal() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_statement::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (-1, 'Ada', 21)",
        )
        .expect_err("unsigned SQL insert key boundary should stay fail-closed for signed literals");

    assert!(
        err.to_string().contains(
            "SQL write primary key literal for 'id' is not compatible with entity key type"
        ),
        "incompatible primary-key literal should keep the reduced-SQL boundary explicit",
    );
}

#[test]
fn execute_sql_statement_insert_rejects_tuple_length_mismatch() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_statement::<SessionSqlWriteEntity>(
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
fn execute_sql_statement_insert_and_update_returning_projection_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    let inserted = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21) RETURNING name, age",
    )
    .expect("SQL INSERT RETURNING should return projection rows");
    assert_eq!(
        inserted,
        vec![vec![Value::Text("Ada".to_string()), Value::Uint(21)]],
        "SQL INSERT RETURNING field list should project requested inserted fields",
    );

    session
        .insert(SessionSqlWriteEntity {
            id: 1,
            name: "Ada".to_string(),
            age: 21,
        })
        .expect("typed update setup insert should succeed");
    let updated = statement_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING *",
    )
    .expect("SQL UPDATE RETURNING should return projection rows");
    assert_eq!(
        updated,
        vec![vec![
            Value::Uint(1),
            Value::Text("Ada".to_string()),
            Value::Uint(22),
        ]],
        "SQL UPDATE RETURNING star should preserve full entity field order",
    );
}

#[test]
fn execute_sql_statement_write_rejects_unsupported_returning_projection_matrix() {
    reset_session_sql_store();
    for (entity_kind, sql) in [
        (
            "insert",
            "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21) RETURNING LOWER(name)",
        ),
        (
            "update",
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING LOWER(name)",
        ),
    ] {
        let session = sql_session();
        let err = match entity_kind {
            "insert" => session
                .execute_sql_statement::<SessionSqlEntity>(sql)
                .expect_err("unsupported INSERT RETURNING projection should stay fail-closed"),
            "update" => {
                session
                    .insert(SessionSqlWriteEntity {
                        id: 1,
                        name: "Ada".to_string(),
                        age: 21,
                    })
                    .expect("unsupported UPDATE RETURNING setup insert should succeed");
                session
                    .execute_sql_statement::<SessionSqlWriteEntity>(sql)
                    .expect_err("unsupported UPDATE RETURNING projection should stay fail-closed")
            }
            other => panic!("unexpected write RETURNING case: {other}"),
        };

        assert!(
            err.to_string().contains(
                "SQL function namespace beyond supported aggregate or scalar text projection forms"
            ),
            "{entity_kind} RETURNING should preserve the parser-owned unsupported feature detail",
        );
    }
}

#[test]
fn execute_sql_statement_update_requires_where_predicate() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql_statement::<SessionSqlWriteEntity>("UPDATE SessionSqlWriteEntity SET age = 22")
        .expect_err("SQL UPDATE without WHERE predicate should stay fail-closed");

    assert!(
        err.to_string()
            .contains("SQL UPDATE requires WHERE predicate"),
        "UPDATE without WHERE predicate should keep an actionable boundary message",
    );
}

#[test]
fn execute_sql_statement_update_rejects_invalid_window_clause_order() {
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
            .execute_sql_statement::<SessionSqlWriteEntity>(sql)
            .expect_err("invalid UPDATE window clause order should stay fail-closed");
        assert!(
            err.to_string().contains(message),
            "invalid UPDATE window clause ordering should keep an actionable parser boundary message",
        );
    }
}

#[test]
fn execute_sql_statement_update_rejects_primary_key_mutation() {
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
        .execute_sql_statement::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET id = 2, age = 22 WHERE id = 1",
        )
        .expect_err("SQL UPDATE primary-key mutation should stay fail-closed");

    assert!(
        err.to_string()
            .contains("SQL UPDATE does not allow primary key mutation"),
        "UPDATE primary-key mutation should keep an actionable boundary message",
    );
}
