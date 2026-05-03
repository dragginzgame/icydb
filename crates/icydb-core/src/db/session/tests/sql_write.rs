use super::*;
use crate::{
    db::{MutationMode, StructuralPatch},
    error::InternalError,
    metrics::sink::SqlWriteKind,
};

// Execute one write statement through the statement SQL boundary and assert it
// returns the canonical count payload for non-RETURNING write forms.
fn assert_statement_count<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_row_count: u32,
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    let payload = execute_sql_statement_for_tests::<E>(session, sql)
        .unwrap_or_else(|err| panic!("{context} should return count payload: {err}"));
    let SqlStatementResult::Count { row_count } = payload else {
        panic!("{context} should return count payload");
    };

    assert_eq!(
        row_count, expected_row_count,
        "{context} should follow traditional SQL count semantics without RETURNING",
    );
}

// Execute one write statement that must stay fail-closed and assert the
// surfaced error text keeps one actionable boundary message.
fn assert_statement_error_contains<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_message: &str,
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    let err = execute_sql_statement_for_tests::<E>(session, sql)
        .expect_err("write statement should stay fail-closed");
    let err_text = err.to_string();

    assert!(
        err_text.contains(expected_message),
        "{context} should keep an actionable boundary message: {err_text}",
    );
}

// Execute one signed write statement that widens parser literals and assert it
// returns the canonical count payload plus the expected persisted signed rows.
fn assert_signed_write_count_and_rows(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_rows: &[Vec<Value>],
    context: &str,
) {
    assert_statement_count::<SessionSqlSignedWriteEntity>(session, sql, 1, context);

    let persisted = statement_projection_rows::<SessionSqlSignedWriteEntity>(
        session,
        "SELECT id, delta FROM SessionSqlSignedWriteEntity ORDER BY id ASC",
    )
    .unwrap_or_else(|err| panic!("{context} post-write projection should succeed: {err}"));

    assert_eq!(
        persisted, expected_rows,
        "{context} should persist the widened signed literal values",
    );
}

// Execute one write statement with RETURNING through the projection-row helper
// and assert the projected value rows stay stable for the requested surface.
fn assert_statement_returning_rows<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_rows: &[Vec<Value>],
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    let rows = statement_projection_rows::<E>(session, sql)
        .unwrap_or_else(|err| panic!("{context} should return projection rows: {err}"));

    assert_eq!(
        rows, expected_rows,
        "{context} should preserve the requested RETURNING projection rows",
    );
}

// Seed one deterministic `SessionSqlWriteEntity` cohort so write-boundary tests
// can share the same setup path without repeating row literals inline.
fn seed_write_entities(session: &DbSession<SessionSqlCanister>, rows: &[(u64, &str, u64)]) {
    for (id, name, age) in rows {
        session
            .insert(SessionSqlWriteEntity {
                id: *id,
                name: (*name).to_string(),
                age: *age,
            })
            .expect("typed setup insert should succeed");
    }
}

fn captured_sql_write_events(
    events: &[MetricsEvent],
) -> Vec<(&'static str, SqlWriteKind, u64, u64, u64)> {
    events
        .iter()
        .filter_map(|event| match event {
            MetricsEvent::SqlWrite {
                entity_path,
                kind,
                matched_rows,
                mutated_rows,
                returning_rows,
            } => Some((
                *entity_path,
                *kind,
                *matched_rows,
                *mutated_rows,
                *returning_rows,
            )),
            _ => None,
        })
        .collect()
}

// Read back the canonical `SessionSqlWriteEntity` ordered row surface used by
// the SQL write tests that assert persisted post-write state.
fn persisted_write_rows(session: &DbSession<SessionSqlCanister>) -> Vec<Vec<Value>> {
    statement_projection_rows::<SessionSqlWriteEntity>(
        session,
        "SELECT id, name, age FROM SessionSqlWriteEntity ORDER BY id ASC",
    )
    .expect("post-write SQL projection should succeed")
}

// Execute one `SessionSqlWriteEntity` UPDATE statement and assert both the
// returned count payload and the persisted ordered row surface stay stable.
fn assert_write_update_count_and_rows(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_row_count: u32,
    expected_rows: &[Vec<Value>],
    context: &str,
) {
    assert_statement_count::<SessionSqlWriteEntity>(session, sql, expected_row_count, context);

    let persisted = persisted_write_rows(session);
    assert_eq!(
        persisted, expected_rows,
        "{context} should preserve the expected persisted write rows",
    );
}

// Execute one SQL statement that returns a single unsigned id column and decode
// it into the compact key list used by update/delete target convergence tests.
fn statement_uint_ids<E>(session: &DbSession<SessionSqlCanister>, sql: &str) -> Vec<u64>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    statement_projection_rows::<E>(session, sql)
        .unwrap_or_else(|err| panic!("id-returning SQL should succeed: {err}"))
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Uint(id)] => *id,
            other => panic!("id-returning SQL should emit one uint id column, got {other:?}"),
        })
        .collect()
}

// Run one selector-shaped statement against a fresh deterministic write fixture
// so SELECT, UPDATE RETURNING, and DELETE RETURNING can be compared without
// mutation side effects leaking between surfaces.
fn write_selector_ids(sql: &str) -> Vec<u64> {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(
        &session,
        &[
            (1, "Ada", 21),
            (2, "Bea", 30),
            (3, "Cid", 25),
            (4, "Dee", 40),
        ],
    );

    statement_uint_ids::<SessionSqlWriteEntity>(&session, sql)
}

// Compare selector keys while allowing explicitly unordered SQL surfaces to
// differ in row order but never in the target key set.
fn assert_selector_ids_match(
    mut expected: Vec<u64>,
    mut actual: Vec<u64>,
    ordered: bool,
    context: &str,
) {
    if !ordered {
        expected.sort_unstable();
        actual.sort_unstable();
    }

    assert_eq!(
        actual, expected,
        "{context} should select the same target ids"
    );
}

// Seed one generated-timestamp row so SQL and structural rewrite tests can
// share the same persisted setup without restating the entity literal.
fn seed_generated_timestamp_entity(
    session: &DbSession<SessionSqlCanister>,
    id: u64,
    name: &str,
    created_on_insert_nanos: i64,
) {
    session
        .insert(SessionSqlGeneratedTimestampEntity {
            id,
            created_on_insert: Timestamp::from_nanos(created_on_insert_nanos),
            name: name.to_string(),
        })
        .expect("generated timestamp setup insert should succeed");
}

// Build one structural insert/replace patch that explicitly writes the
// generated timestamp field so generated-field rejection matrices can share it.
fn generated_timestamp_insert_patch(
    id: u64,
    name: &str,
    created_on_insert_nanos: i64,
    context: &str,
) -> StructuralPatch {
    StructuralPatch::new()
        .set_field(
            SessionSqlGeneratedTimestampEntity::MODEL,
            "id",
            Value::Uint(id),
        )
        .unwrap_or_else(|err| panic!("{context} should resolve id: {err}"))
        .set_field(
            SessionSqlGeneratedTimestampEntity::MODEL,
            "created_on_insert",
            Value::Timestamp(Timestamp::from_nanos(created_on_insert_nanos)),
        )
        .unwrap_or_else(|err| panic!("{context} should resolve generated field: {err}"))
        .set_field(
            SessionSqlGeneratedTimestampEntity::MODEL,
            "name",
            Value::Text(name.to_string()),
        )
        .unwrap_or_else(|err| panic!("{context} should resolve name: {err}"))
}

// Assert one structural generated-field rejection keeps the Unsupported class
// and names the ownership-protected generated field.
fn assert_structural_generated_field_rejection(
    err: &InternalError,
    field_name: &str,
    context: &str,
) {
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

// Execute one supported `INSERT ... SELECT ... RETURNING *` statement and
// assert it synthesizes a fresh primary key while preserving the projected row
// payload and the expected persisted post-insert surface.
fn assert_insert_select_returning_and_persisted_rows(
    session: &DbSession<SessionSqlCanister>,
    returning_sql: &str,
    persisted_sql: &str,
    expected_inserted_name: &str,
    expected_persisted: &[Vec<Value>],
    context: &str,
) {
    let rows = statement_projection_rows::<SessionSqlEntity>(session, returning_sql)
        .unwrap_or_else(|err| panic!("{context} should succeed with RETURNING: {err}"));
    let persisted = statement_projection_rows::<SessionSqlEntity>(session, persisted_sql)
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

        assert_statement_count::<SessionSqlWriteEntity>(
            &session,
            row_sql,
            expected_row_count,
            context,
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

        assert_statement_count::<SessionSqlWriteEntity>(&session, sql, expected_row_count, context);

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
fn execute_sql_statement_multi_row_insert_late_failure_is_statement_atomic() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(2, "Existing", 20)]);

    execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity (id, name, age) \
         VALUES (1, 'Ada', 21), (2, 'Dup', 22)",
    )
    .expect_err("late duplicate-key INSERT failure should reject the whole statement");

    assert_eq!(
        persisted_write_rows(&session),
        vec![vec![
            Value::Uint(2),
            Value::Text("Existing".to_string()),
            Value::Uint(20),
        ]],
        "late INSERT failure must not commit the earlier row",
    );
}

#[test]
fn execute_sql_statement_multi_row_insert_duplicate_keys_is_statement_atomic() {
    reset_session_sql_store();
    let session = sql_session();

    execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity (id, name, age) \
         VALUES (1, 'Ada', 21), (1, 'Dup', 22)",
    )
    .expect_err("duplicate keys inside one INSERT statement should fail atomically");

    assert!(
        persisted_write_rows(&session).is_empty(),
        "duplicate-key INSERT must commit zero rows",
    );
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
        assert_statement_error_contains::<SessionSqlWriteEntity>(
            &session,
            sql,
            expected_message,
            context,
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

        assert_statement_error_contains::<SessionSqlManagedWriteEntity>(
            &session,
            sql,
            expected_message,
            context,
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
        assert_statement_error_contains::<SessionSqlGeneratedTimestampEntity>(
            &session,
            sql,
            expected_message,
            context,
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
            generated_timestamp_insert_patch(1, "Ada", 7, "generated timestamp structural insert"),
            "created_on_insert",
            "structural insert explicit generated timestamp",
        ),
        (
            MutationMode::Replace,
            2_u64,
            generated_timestamp_insert_patch(2, "Bea", 9, "generated timestamp structural replace"),
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

        assert_structural_generated_field_rejection(&err, field_name, context);
    }
}

#[test]
fn execute_sql_statement_update_rejects_explicit_generated_fields_matrix() {
    reset_session_sql_store();
    let session = sql_session();
    seed_generated_timestamp_entity(&session, 1, "Ada", 1);

    let err = execute_sql_statement_for_tests::<SessionSqlGeneratedTimestampEntity>(
        &session,
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
        seed_generated_timestamp_entity(&session, 1, "Ada", 1);

        let patch = StructuralPatch::new()
            .set_field(
                SessionSqlGeneratedTimestampEntity::MODEL,
                "created_on_insert",
                Value::Timestamp(Timestamp::from_nanos(9)),
            )
            .expect("generated timestamp structural rewrite should resolve generated field");
        let err = session
            .mutate_structural::<SessionSqlGeneratedTimestampEntity>(1, patch, mode)
            .expect_err("structural rewrites should reject explicit insert-generated fields");

        assert_structural_generated_field_rejection(&err, "created_on_insert", context);
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
        seed_write_entities(&session, &[(1, "Ada", 21)]);

        assert_statement_count::<SessionSqlWriteEntity>(&session, sql, 1, context);

        if check_persisted {
            let persisted = persisted_write_rows(&session);
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
fn execute_sql_statement_write_metrics_capture_sql_boundary_shape() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 30)]);
    seed_session_sql_entities(&session, &[("Ada", 21)]);

    let sink = SessionMetricsCaptureSink::default();
    with_metrics_sink(&sink, || {
        execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
            &session,
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (3, 'Cid', 31)",
        )
        .expect("SQL INSERT should succeed");
        execute_sql_statement_for_tests::<SessionSqlEntity>(
            &session,
            "INSERT INTO SessionSqlEntity (name, age) SELECT name, age FROM SessionSqlEntity WHERE name = 'Ada' RETURNING *",
        )
        .expect("SQL INSERT SELECT RETURNING should succeed");
        execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
            &session,
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age >= 21 RETURNING id",
        )
        .expect("SQL UPDATE RETURNING should succeed");
        execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
            &session,
            "DELETE FROM SessionSqlWriteEntity WHERE id = 1 RETURNING id",
        )
        .expect("SQL DELETE RETURNING should succeed");
    });

    assert_eq!(
        captured_sql_write_events(&sink.into_events()),
        vec![
            (SessionSqlWriteEntity::PATH, SqlWriteKind::Insert, 1, 1, 0),
            (SessionSqlEntity::PATH, SqlWriteKind::InsertSelect, 1, 1, 1,),
            (SessionSqlWriteEntity::PATH, SqlWriteKind::Update, 3, 3, 3),
            (SessionSqlWriteEntity::PATH, SqlWriteKind::Delete, 1, 1, 1),
        ],
    );
}

#[test]
fn execute_sql_statement_signed_numeric_write_matrix_widens_parser_literals() {
    let cases = [
        (
            "signed SQL UPDATE",
            Some((1_i64, -5_i64)),
            "UPDATE SessionSqlSignedWriteEntity SET delta = 7 WHERE id = 1",
            vec![vec![Value::Int(1), Value::Int(7)]],
        ),
        (
            "signed SQL INSERT",
            None,
            "INSERT INTO SessionSqlSignedWriteEntity (id, delta) VALUES (2, 9)",
            vec![vec![Value::Int(2), Value::Int(9)]],
        ),
    ];

    for (context, seed_row, sql, expected_rows) in cases {
        reset_session_sql_store();
        let session = sql_session();

        if let Some((id, delta)) = seed_row {
            session
                .insert(SessionSqlSignedWriteEntity { id, delta })
                .unwrap_or_else(|err| panic!("{context} setup insert should succeed: {err}"));
        }

        assert_signed_write_count_and_rows(&session, sql, expected_rows.as_slice(), context);
    }
}

#[test]
fn execute_sql_statement_rejects_incompatible_assignment_literal_for_signed_field() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlSignedWriteEntity { id: 1, delta: -5 })
        .expect("signed write setup insert should succeed");

    let err = execute_sql_statement_for_tests::<SessionSqlSignedWriteEntity>(
        &session,
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
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 30)]);

    assert_write_update_count_and_rows(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21",
        2,
        &[
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
        "SQL UPDATE with non-primary-key predicate",
    );
}

#[test]
fn execute_sql_statement_update_with_order_limit_and_offset_updates_one_ordered_window() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(
        &session,
        &[
            (1, "Ada", 21),
            (2, "Bea", 30),
            (3, "Cid", 25),
            (4, "Dee", 40),
        ],
    );

    assert_write_update_count_and_rows(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 99 WHERE age >= 21 ORDER BY age DESC LIMIT 2 OFFSET 1",
        2,
        &[
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
        "SQL UPDATE ordered window",
    );
}

#[test]
fn execute_sql_statement_update_with_limit_and_offset_uses_primary_key_order_fallback() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21), (2, "Bea", 21), (3, "Cid", 21)]);

    assert_write_update_count_and_rows(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE age = 21 LIMIT 1 OFFSET 1",
        1,
        &[
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
        "SQL UPDATE window without ORDER BY",
    );
}

#[test]
fn execute_sql_statement_update_selector_converges_with_select_and_delete_targets() {
    for (clause, ordered, context) in [
        ("WHERE age = 21", false, "WHERE predicate"),
        (
            "WHERE age >= 21 ORDER BY age ASC",
            true,
            "ORDER BY ASC selector",
        ),
        (
            "WHERE age >= 21 ORDER BY age DESC",
            true,
            "ORDER BY DESC selector",
        ),
        (
            "WHERE age >= 21 ORDER BY id ASC LIMIT 2",
            true,
            "LIMIT selector",
        ),
        (
            "WHERE age >= 21 ORDER BY id ASC LIMIT 2 OFFSET 1",
            true,
            "OFFSET selector",
        ),
        (
            "WHERE age >= 21 ORDER BY age DESC LIMIT 2",
            true,
            "WHERE ORDER BY LIMIT selector",
        ),
    ] {
        let select_ids =
            write_selector_ids(&format!("SELECT id FROM SessionSqlWriteEntity {clause}"));
        let update_ids = write_selector_ids(&format!(
            "UPDATE SessionSqlWriteEntity SET age = 99 {clause} RETURNING id"
        ));
        let delete_ids = write_selector_ids(&format!(
            "DELETE FROM SessionSqlWriteEntity {clause} RETURNING id"
        ));

        assert_selector_ids_match(select_ids.clone(), update_ids, ordered, context);
        assert_selector_ids_match(select_ids, delete_ids, ordered, context);
    }
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
        assert_statement_error_contains::<SessionSqlWriteEntity>(
            &session,
            sql,
            &format!(
                "SQL entity '{sql_entity}' does not match requested entity type 'SessionSqlWriteEntity'"
            ),
            context,
        );
    }
}

#[test]
fn execute_sql_statement_insert_select_matrix_accepts_supported_source_shapes() {
    for (returning_sql, expected_inserted_name, persisted_sql, expected_persisted, context) in [
        (
            "INSERT INTO SessionSqlEntity (name, age) \
             SELECT name, age FROM SessionSqlEntity WHERE name = 'Ada' ORDER BY id ASC LIMIT 1 RETURNING *",
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
             SELECT LOWER(name), age FROM SessionSqlEntity WHERE name = 'Ada' ORDER BY id ASC LIMIT 1 RETURNING *",
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
        seed_session_sql_entities(&session, &[("Ada", 21)]);

        assert_insert_select_returning_and_persisted_rows(
            &session,
            returning_sql,
            persisted_sql,
            expected_inserted_name,
            expected_persisted.as_slice(),
            context,
        );
    }
}

#[test]
fn execute_sql_statement_insert_select_late_failure_is_statement_atomic() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(
        &session,
        &[(1, "Ada", 21), (2, "Bea", 22), (12, "Existing", 32)],
    );

    execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity (id, name, age) \
         SELECT id + 10, name, age FROM SessionSqlWriteEntity WHERE id <= 2 ORDER BY id ASC",
    )
    .expect_err("late INSERT SELECT conflict should reject the whole statement");

    assert_eq!(
        persisted_write_rows(&session),
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
                Value::Uint(12),
                Value::Text("Existing".to_string()),
                Value::Uint(32),
            ],
        ],
        "late INSERT SELECT failure must not commit the earlier projected row",
    );
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

        assert_statement_error_contains::<SessionSqlEntity>(
            &session,
            sql,
            expected_message,
            context,
        );
    }
}

#[test]
fn execute_sql_statement_update_unique_conflict_is_statement_atomic() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_unique_prefix_offset_session_entities(
        &session,
        &[(1, "gold", "alpha", "first"), (2, "gold", "beta", "second")],
    );

    execute_sql_statement_for_tests::<SessionUniquePrefixOffsetEntity>(
        &session,
        "UPDATE SessionUniquePrefixOffsetEntity SET handle = 'shared' WHERE tier = 'gold' ORDER BY id ASC",
    )
    .expect_err("same-batch unique-index UPDATE conflict should fail atomically");

    let persisted = statement_projection_rows::<SessionUniquePrefixOffsetEntity>(
        &session,
        "SELECT tier, handle, note FROM SessionUniquePrefixOffsetEntity ORDER BY id ASC",
    )
    .expect("post-update projection should succeed");
    assert_eq!(
        persisted,
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("alpha".to_string()),
                Value::Text("first".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("beta".to_string()),
                Value::Text("second".to_string()),
            ],
        ],
        "late UPDATE unique conflict must not commit the earlier matched row",
    );
}

#[test]
fn execute_sql_statement_insert_strong_relation_same_statement_target_stays_committed_only() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlSelfRelationEntity {
            id: 1,
            parent: None,
        })
        .expect("committed nullable root setup should save");

    assert_statement_count::<SessionSqlSelfRelationEntity>(
        &session,
        "INSERT INTO SessionSqlSelfRelationEntity (id, parent) VALUES (2, 1)",
        1,
        "committed strong relation target insert",
    );

    execute_sql_statement_for_tests::<SessionSqlSelfRelationEntity>(
        &session,
        "INSERT INTO SessionSqlSelfRelationEntity (id, parent) VALUES (3, 1), (4, 3)",
    )
    .expect_err("same-statement strong relation target should still be rejected");

    let persisted = statement_projection_rows::<SessionSqlSelfRelationEntity>(
        &session,
        "SELECT id, parent FROM SessionSqlSelfRelationEntity ORDER BY id ASC",
    )
    .expect("post-relation projection should succeed");
    assert_eq!(
        persisted,
        vec![
            vec![Value::Uint(1), Value::Null],
            vec![Value::Uint(2), Value::Uint(1)],
        ],
        "same-statement relation failure must not persist the staged parent or child",
    );
}

#[test]
fn execute_sql_statement_write_rejects_incompatible_primary_key_literal() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
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

    let err = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
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

    assert_statement_returning_rows::<SessionSqlEntity>(
        &session,
        "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21) RETURNING name, age",
        &[vec![Value::Text("Ada".to_string()), Value::Uint(21)]],
        "SQL INSERT RETURNING field list",
    );

    seed_write_entities(&session, &[(1, "Ada", 21)]);

    assert_statement_returning_rows::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1 RETURNING *",
        &[vec![
            Value::Uint(1),
            Value::Text("Ada".to_string()),
            Value::Uint(22),
        ]],
        "SQL UPDATE RETURNING star",
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
            "insert" => execute_sql_statement_for_tests::<SessionSqlEntity>(&session, sql)
                .expect_err("unsupported INSERT RETURNING projection should stay fail-closed"),
            "update" => {
                seed_write_entities(&session, &[(1, "Ada", 21)]);
                execute_sql_statement_for_tests::<SessionSqlWriteEntity>(&session, sql)
                    .expect_err("unsupported UPDATE RETURNING projection should stay fail-closed")
            }
            other => panic!("unexpected write RETURNING case: {other}"),
        };

        assert!(
            err.to_string().contains(
                "SQL function namespace beyond supported aggregate or scalar function forms"
            ),
            "{entity_kind} RETURNING should preserve the parser-owned unsupported feature detail",
        );
    }
}

#[test]
fn execute_sql_statement_update_requires_where_predicate() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22",
    )
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
        assert_statement_error_contains::<SessionSqlWriteEntity>(
            &session,
            sql,
            message,
            "invalid UPDATE window clause ordering",
        );
    }
}

#[test]
fn execute_sql_statement_update_rejects_primary_key_mutation() {
    reset_session_sql_store();
    let session = sql_session();
    seed_write_entities(&session, &[(1, "Ada", 21)]);

    let err = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET id = 2, age = 22 WHERE id = 1",
    )
    .expect_err("SQL UPDATE primary-key mutation should stay fail-closed");

    assert!(
        err.to_string()
            .contains("SQL UPDATE does not allow primary key mutation"),
        "UPDATE primary-key mutation should keep an actionable boundary message",
    );
}
