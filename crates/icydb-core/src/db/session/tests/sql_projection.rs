use super::*;

// Seed the shared text-function projection fixture used by the computed
// projection tests in this file.
fn seed_projection_text_fixture(session: &DbSession<SessionSqlCanister>) {
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "  Ada  ".to_string(),
            age: 33,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "\tBob".to_string(),
            age: 21,
        })
        .expect("seed insert should succeed");
}

// Seed the deterministic ordered projection fixture used by the matrix/window
// checks in this file.
fn seed_projection_window_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_session_sql_entities(
        session,
        &[
            ("matrix-a", 10),
            ("matrix-b", 20),
            ("matrix-c", 30),
            ("matrix-d", 40),
        ],
    );
}

// Execute one projection SQL query and assert both the derived column labels
// and the projected rows against one explicit expected surface.
fn assert_projection_columns_and_rows(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_columns: &[&str],
    expected_rows: ProjectedRows,
    context: &str,
) {
    let columns = statement_projection_columns::<SessionSqlEntity>(session, sql)
        .unwrap_or_else(|err| panic!("{context} projection columns should derive: {err:?}"));
    let rows = statement_projection_rows::<SessionSqlEntity>(session, sql)
        .unwrap_or_else(|err| panic!("{context} projection rows should execute: {err:?}"));

    assert_eq!(
        columns,
        expected_columns
            .iter()
            .map(|column| (*column).to_string())
            .collect::<Vec<_>>(),
        "{context} should expose the expected projection column labels",
    );
    assert_eq!(
        rows, expected_rows,
        "{context} should expose the expected projection row payloads",
    );
}

// Assert that one SQL surface still derives the exact public projection
// column labels expected by the session boundary.
fn assert_projection_columns(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_columns: &[&str],
    context: &str,
) {
    let columns = statement_projection_columns::<SessionSqlEntity>(session, sql)
        .unwrap_or_else(|err| panic!("{context} projection columns should derive: {err:?}"));

    assert_eq!(
        columns,
        expected_columns
            .iter()
            .map(|column| (*column).to_string())
            .collect::<Vec<_>>(),
        "{context} should expose the expected projection column labels",
    );
}

// Assert that one single-column SQL computed projection stays aligned with the
// shared fluent text-projection terminal over the same ordered response window.
fn assert_sql_projection_matches_fluent_text_projection(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    projection: &impl crate::db::ValueProjectionExpr,
    context: &str,
) {
    let sql_rows = statement_projection_rows::<SessionSqlEntity>(session, sql)
        .unwrap_or_else(|err| panic!("{context} SQL projection should execute: {err:?}"));
    let fluent_values = session
        .load::<SessionSqlEntity>()
        .order_by_desc("age")
        .project_values(projection)
        .unwrap_or_else(|err| panic!("{context} fluent projection should execute: {err:?}"));

    let sql_values = sql_rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .next()
                .expect("single-column SQL projection row should contain one value")
        })
        .collect::<Vec<_>>();

    assert_eq!(
        fluent_values, sql_values,
        "{context} fluent projection should stay aligned with the SQL projection values",
    );
}

#[test]
fn execute_sql_projection_scalar_addition_matches_fluent_numeric_projection() {
    reset_session_sql_store();
    let session = sql_session();

    seed_projection_window_fixture(&session);

    assert_sql_projection_matches_fluent_text_projection(
        &session,
        "SELECT age + 1 FROM SessionSqlEntity ORDER BY age DESC LIMIT 4",
        &crate::db::add("age", 1_u64),
        "scalar arithmetic projection",
    );

    assert_projection_columns_and_rows(
        &session,
        "SELECT age + 1 FROM SessionSqlEntity ORDER BY age ASC LIMIT 2",
        &["age + 1"],
        vec![
            vec![Value::Decimal(
                crate::types::Decimal::from_u128(11).expect("11 decimal"),
            )],
            vec![Value::Decimal(
                crate::types::Decimal::from_u128(21).expect("21 decimal"),
            )],
        ],
        "scalar arithmetic projection rows",
    );
}

#[test]
fn execute_sql_select_field_projection_currently_returns_entity_shaped_rows() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projected-row".to_string(),
            age: 29,
        })
        .expect("seed insert should succeed");

    let response = execute_scalar_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT name FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
    )
    .expect("field-list SQL projection should execute");
    let row = response
        .iter()
        .next()
        .expect("field-list SQL projection response should contain one row");

    assert_eq!(
        row.entity_ref().name,
        "projected-row",
        "field-list SQL projection should still return entity rows in this baseline",
    );
    assert_eq!(
        row.entity_ref().age,
        29,
        "field-list SQL projection should preserve full entity payload until projection response shaping is introduced",
    );
}

#[test]
fn sql_projection_columns_matrix_matches_expected_labels() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_columns, context) in [
        (
            "SELECT name, age FROM SessionSqlEntity",
            &["name", "age"][..],
            "field-list projection columns",
        ),
        (
            "SELECT TRIM(name) AS trimmed_name, age years FROM SessionSqlEntity",
            &["trimmed_name", "years"][..],
            "aliased projection columns",
        ),
        (
            "SELECT * FROM SessionSqlEntity",
            &["id", "name", "age"][..],
            "star projection columns",
        ),
    ] {
        assert_projection_columns(&session, sql, expected_columns, context);
    }
}

#[test]
fn execute_sql_projection_order_by_alias_matrix_matches_canonical_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(&session, &[("bravo", 20), ("alpha", 30), ("charlie", 40)]);

    assert_session_sql_alias_matches_canonical::<Vec<Vec<Value>>>(
        &session,
        statement_projection_rows::<SessionSqlEntity>,
        "SELECT name AS display_name FROM SessionSqlEntity ORDER BY display_name ASC LIMIT 3",
        "SELECT name FROM SessionSqlEntity ORDER BY name ASC LIMIT 3",
        "ORDER BY field aliases",
    );

    reset_indexed_session_sql_store();
    let indexed_session = indexed_sql_session();

    seed_expression_indexed_session_sql_entities(
        &indexed_session,
        &[
            (9_243_u128, "sam", 10),
            (9_244, "Alex", 20),
            (9_241, "bob", 30),
        ],
    );

    assert_session_sql_alias_matches_canonical::<Vec<Vec<Value>>>(
        &indexed_session,
        statement_projection_rows::<ExpressionIndexedSessionSqlEntity>,
        "SELECT LOWER(name) AS normalized_name FROM ExpressionIndexedSessionSqlEntity ORDER BY normalized_name ASC LIMIT 3",
        "SELECT LOWER(name) FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC LIMIT 3",
        "ORDER BY LOWER(field) aliases",
    );
}

#[test]
fn execute_sql_projection_rejects_order_by_alias_for_unsupported_target_family() {
    reset_session_sql_store();
    let session = sql_session();

    assert_session_sql_order_by_alias_unsupported::<Vec<Vec<Value>>>(
        &session,
        statement_projection_rows::<SessionSqlEntity>,
        "SELECT TRIM(name) AS trimmed_name FROM SessionSqlEntity ORDER BY trimmed_name ASC LIMIT 2",
        "unsupported ORDER BY alias targets",
    );
}

#[test]
fn execute_sql_projection_select_field_list_returns_projection_shaped_rows() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-surface".to_string(),
            age: 33,
        })
        .expect("seed insert should succeed");

    let response = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
    )
    .expect("projection SQL execution should succeed");
    let row = response
        .first()
        .expect("projection SQL response should contain one row");

    assert_eq!(response.len(), 1);
    assert_eq!(
        row.as_slice(),
        [Value::Text("projection-surface".to_string())],
        "projection SQL response should carry only projected field values in declaration order",
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "table-driven computed projection matrix"
)]
fn execute_sql_projection_computed_function_matrix_runs_from_session_boundary() {
    reset_session_sql_store();
    let session = sql_session();

    seed_projection_text_fixture(&session);

    for (sql, expected_columns, expected_rows, context) in [
        (
            "SELECT TRIM(name), LTRIM(name), RTRIM(name), LOWER(name), UPPER(name), LENGTH(name), age FROM SessionSqlEntity ORDER BY age DESC",
            &[
                "TRIM(name)",
                "LTRIM(name)",
                "RTRIM(name)",
                "LOWER(name)",
                "UPPER(name)",
                "LENGTH(name)",
                "age",
            ][..],
            vec![
                vec![
                    Value::Text("Ada".to_string()),
                    Value::Text("Ada  ".to_string()),
                    Value::Text("  Ada".to_string()),
                    Value::Text("  ada  ".to_string()),
                    Value::Text("  ADA  ".to_string()),
                    Value::Uint(7),
                    Value::Uint(33),
                ],
                vec![
                    Value::Text("Bob".to_string()),
                    Value::Text("Bob".to_string()),
                    Value::Text("\tBob".to_string()),
                    Value::Text("\tbob".to_string()),
                    Value::Text("\tBOB".to_string()),
                    Value::Uint(4),
                    Value::Uint(21),
                ],
            ],
            "computed trim/case/length projections",
        ),
        (
            "SELECT LEFT(name, 2), RIGHT(name, 3), LEFT(name, NULL) FROM SessionSqlEntity ORDER BY age DESC",
            &["LEFT(name, 2)", "RIGHT(name, 3)", "LEFT(name, NULL)"][..],
            vec![
                vec![
                    Value::Text("  ".to_string()),
                    Value::Text("a  ".to_string()),
                    Value::Null,
                ],
                vec![
                    Value::Text("\tB".to_string()),
                    Value::Text("Bob".to_string()),
                    Value::Null,
                ],
            ],
            "left/right projections",
        ),
        (
            "SELECT STARTS_WITH(name, ' '), ENDS_WITH(name, 'b'), CONTAINS(name, 'da'), POSITION('da', name), POSITION(NULL, name) FROM SessionSqlEntity ORDER BY age DESC",
            &[
                "STARTS_WITH(name, ' ')",
                "ENDS_WITH(name, 'b')",
                "CONTAINS(name, 'da')",
                "POSITION('da', name)",
                "POSITION(NULL, name)",
            ][..],
            vec![
                vec![
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Uint(4),
                    Value::Null,
                ],
                vec![
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Uint(0),
                    Value::Null,
                ],
            ],
            "text predicate projections",
        ),
        (
            "SELECT REPLACE(name, 'A', 'E'), REPLACE(name, NULL, 'x') FROM SessionSqlEntity ORDER BY age DESC",
            &["REPLACE(name, 'A', 'E')", "REPLACE(name, NULL, 'x')"][..],
            vec![
                vec![Value::Text("  Eda  ".to_string()), Value::Null],
                vec![Value::Text("\tBob".to_string()), Value::Null],
            ],
            "replace projections",
        ),
        (
            "SELECT SUBSTRING(name, 3, 3), SUBSTRING(name, 3), SUBSTRING(name, NULL, 2) FROM SessionSqlEntity ORDER BY age DESC",
            &[
                "SUBSTRING(name, 3, 3)",
                "SUBSTRING(name, 3)",
                "SUBSTRING(name, NULL, 2)",
            ][..],
            vec![
                vec![
                    Value::Text("Ada".to_string()),
                    Value::Text("Ada  ".to_string()),
                    Value::Null,
                ],
                vec![
                    Value::Text("ob".to_string()),
                    Value::Text("ob".to_string()),
                    Value::Null,
                ],
            ],
            "substring projections",
        ),
    ] {
        assert_projection_columns_and_rows(&session, sql, expected_columns, expected_rows, context);
    }
}

#[test]
fn fluent_text_projection_terminals_match_sql_projection_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    seed_projection_text_fixture(&session);

    for (sql, projection, context) in [
        (
            "SELECT TRIM(name) FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::trim("name"),
            "TRIM(name) parity",
        ),
        (
            "SELECT LTRIM(name) FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::ltrim("name"),
            "LTRIM(name) parity",
        ),
        (
            "SELECT RTRIM(name) FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::rtrim("name"),
            "RTRIM(name) parity",
        ),
        (
            "SELECT LOWER(name) FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::lower("name"),
            "LOWER(name) parity",
        ),
        (
            "SELECT UPPER(name) FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::upper("name"),
            "UPPER(name) parity",
        ),
        (
            "SELECT LENGTH(name) FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::length("name"),
            "LENGTH(name) parity",
        ),
        (
            "SELECT LEFT(name, 2) FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::left("name", 2_i64),
            "LEFT(name, 2) parity",
        ),
        (
            "SELECT RIGHT(name, 3) FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::right("name", 3_i64),
            "RIGHT(name, 3) parity",
        ),
        (
            "SELECT STARTS_WITH(name, ' ') FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::starts_with("name", " "),
            "STARTS_WITH(name, ' ') parity",
        ),
        (
            "SELECT ENDS_WITH(name, 'b') FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::ends_with("name", "b"),
            "ENDS_WITH(name, 'b') parity",
        ),
        (
            "SELECT CONTAINS(name, 'da') FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::contains("name", "da"),
            "CONTAINS(name, 'da') parity",
        ),
        (
            "SELECT POSITION('da', name) FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::position("name", "da"),
            "POSITION('da', name) parity",
        ),
        (
            "SELECT REPLACE(name, 'A', 'E') FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::replace("name", "A", "E"),
            "REPLACE(name, 'A', 'E') parity",
        ),
        (
            "SELECT SUBSTRING(name, 3, 3) FROM SessionSqlEntity ORDER BY age DESC",
            crate::db::substring_with_length("name", 3_i64, 3_i64),
            "SUBSTRING(name, 3, 3) parity",
        ),
    ] {
        assert_sql_projection_matches_fluent_text_projection(&session, sql, &projection, context);
    }
}

#[test]
fn fluent_text_projection_first_and_last_values_match_sql_ordered_windows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_projection_text_fixture(&session);

    let projection = crate::db::lower("name");
    let sql_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT LOWER(name) FROM SessionSqlEntity ORDER BY age ASC",
    )
    .expect("LOWER(name) SQL projection should execute");
    let expected = sql_rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .next()
                .expect("single-column SQL projection row should contain one value")
        })
        .collect::<Vec<_>>();

    let first_value = session
        .load::<SessionSqlEntity>()
        .order_by("age")
        .project_first_value(&projection)
        .expect("fluent first projected value should execute");
    let last_value = session
        .load::<SessionSqlEntity>()
        .order_by("age")
        .project_last_value(&projection)
        .expect("fluent last projected value should execute");

    assert_eq!(
        first_value,
        expected.first().cloned(),
        "first projected fluent value should match the first ordered SQL projection value",
    );
    assert_eq!(
        last_value,
        expected.last().cloned(),
        "last projected fluent value should match the last ordered SQL projection value",
    );
}

#[test]
fn execute_sql_projection_select_star_returns_all_fields_in_model_order() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-star".to_string(),
            age: 41,
        })
        .expect("seed insert should succeed");

    let response = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT * FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
    )
    .expect("projection SQL star execution should succeed");
    let row = response
        .first()
        .expect("projection SQL star response should contain one row");

    assert_eq!(response.len(), 1);
    assert_eq!(
        row.len(),
        3,
        "SELECT * projection response should include all model fields",
    );
    assert!(matches!(row[0], Value::Ulid(_)));
    assert_eq!(row[1], Value::Text("projection-star".to_string()));
    assert_eq!(row[2], Value::Uint(41));
}

#[test]
fn execute_sql_projection_qualified_identifier_matrix_executes() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "schema-qualified".to_string(),
            age: 41,
        })
        .expect("seed insert should succeed");

    for (sql, expect_full_row, expected_name, expected_age, context) in [
        (
            "SELECT * FROM public.SessionSqlEntity ORDER BY age ASC LIMIT 1",
            true,
            "schema-qualified",
            41,
            "schema-qualified entity SQL",
        ),
        (
            "SELECT SessionSqlEntity.name \
             FROM SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 40 \
             ORDER BY SessionSqlEntity.age DESC LIMIT 1",
            false,
            "schema-qualified",
            41,
            "table-qualified projection SQL",
        ),
        (
            "SELECT alias.name \
             FROM SessionSqlEntity alias \
             WHERE alias.age >= 40 \
             ORDER BY alias.age DESC LIMIT 1",
            false,
            "schema-qualified",
            41,
            "table-alias projection SQL",
        ),
    ] {
        let rows = statement_projection_rows::<SessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should execute: {err:?}"));

        assert_eq!(rows.len(), 1, "{context} should return one row");

        if expect_full_row {
            assert!(
                matches!(rows[0][0], Value::Ulid(_)),
                "{context} should preserve the generated primary key slot",
            );
            assert_eq!(
                rows[0][1..],
                [
                    Value::Text(expected_name.to_string()),
                    Value::Uint(expected_age),
                ],
                "{context} should preserve full entity field order",
            );
            continue;
        }

        assert_eq!(
            rows,
            vec![vec![Value::Text(expected_name.to_string())]],
            "{context} should preserve the projected field value",
        );
    }
}

#[test]
fn execute_sql_projection_delete_returns_deleted_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("projection-delete-a", 10_u64),
            ("projection-delete-b", 20_u64),
            ("projection-delete-c", 30_u64),
        ],
    );

    let projection = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1 RETURNING *",
    )
    .expect("projection SQL execution should support DELETE RETURNING statements");
    let rows = projection;

    assert!(
        rows.len() == 1,
        "delete projection should return exactly one deleted row",
    );
    assert!(
        matches!(rows[0].first(), Some(Value::Ulid(_))),
        "delete projection should expose the deleted row id in the first projected column",
    );
    assert_eq!(
        &rows[0][1..],
        &[
            Value::Text("projection-delete-a".to_string()),
            Value::Uint(10)
        ],
        "delete projection should return the deleted entity fields in declared model order",
    );
}

#[test]
fn execute_sql_select_field_projection_unknown_field_fails_with_plan_error() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_scalar_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT missing_field FROM SessionSqlEntity",
    )
    .expect_err("unknown projected fields should fail planner validation");

    assert!(
        matches!(err, QueryError::Plan(_)),
        "unknown projected fields should surface planner-domain query errors: {err:?}",
    );
}

#[test]
fn execute_sql_select_distinct_star_executes() {
    reset_session_sql_store();
    let session = sql_session();

    let id_a = Ulid::generate();
    let id_b = Ulid::generate();
    session
        .insert(SessionSqlEntity {
            id: id_a,
            name: "distinct-a".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: id_b,
            name: "distinct-b".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");

    let response = execute_scalar_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC",
    )
    .expect("SELECT DISTINCT * should execute");
    assert_eq!(response.len(), 2);
}

#[test]
fn execute_sql_projection_distinct_matrix_matches_expected_rows() {
    for (seed_rows, sql, expected_rows, expect_pk_rows, context) in [
        (
            vec![("distinct-pk-a", 25_u64), ("distinct-pk-b", 25_u64)],
            "SELECT DISTINCT id, age FROM SessionSqlEntity ORDER BY id ASC",
            vec![],
            true,
            "SELECT DISTINCT field-list with PK",
        ),
        (
            vec![
                ("distinct-no-pk-a", 25_u64),
                ("distinct-no-pk-b", 25_u64),
                ("distinct-no-pk-c", 30_u64),
            ],
            "SELECT DISTINCT age FROM SessionSqlEntity ORDER BY age ASC",
            vec![vec![Value::Uint(25)], vec![Value::Uint(30)]],
            false,
            "SELECT DISTINCT without PK in projection",
        ),
        (
            vec![
                ("distinct-window-a", 25_u64),
                ("distinct-window-b", 25_u64),
                ("distinct-window-c", 30_u64),
                ("distinct-window-d", 35_u64),
            ],
            "SELECT DISTINCT age FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 1",
            vec![vec![Value::Uint(30)]],
            false,
            "SELECT DISTINCT without PK projection paging",
        ),
    ] {
        reset_session_sql_store();
        let session = sql_session();

        seed_session_sql_entities(&session, &seed_rows);

        let response = statement_projection_rows::<SessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should execute: {err:?}"));

        if expect_pk_rows {
            assert_eq!(
                response.len(),
                2,
                "{context} should return one row per distinct id"
            );
            assert_eq!(
                response[0].len(),
                2,
                "{context} should keep both projected columns"
            );
            assert!(
                matches!(response[0][0], Value::Ulid(_))
                    && matches!(response[1][0], Value::Ulid(_)),
                "{context} should keep the primary key in the first projected column",
            );
            assert_eq!(
                response
                    .iter()
                    .map(|row| row[1].clone())
                    .collect::<Vec<_>>(),
                vec![Value::Uint(25), Value::Uint(25)],
                "{context} should preserve the distinct field payloads",
            );
            continue;
        }

        assert_eq!(
            response, expected_rows,
            "{context} should match expected rows"
        );
    }
}

#[test]
fn execute_sql_projection_matrix_queries_match_expected_projected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows used by matrix projections.
    seed_projection_window_fixture(&session);

    // Phase 2: execute table-driven projection SQL cases.
    let cases = vec![
        (
            "SELECT name, age \
             FROM SessionSqlEntity \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
            vec![
                vec![Value::Text("matrix-c".to_string()), Value::Uint(30)],
                vec![Value::Text("matrix-b".to_string()), Value::Uint(20)],
            ],
        ),
        (
            "SELECT age \
             FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age ASC LIMIT 2",
            vec![vec![Value::Uint(20)], vec![Value::Uint(30)]],
        ),
        (
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE age < 25 \
             ORDER BY age ASC",
            vec![
                vec![Value::Text("matrix-a".to_string())],
                vec![Value::Text("matrix-b".to_string())],
            ],
        ),
    ];

    // Phase 3: assert projected row payloads for each SQL input.
    for (sql, expected_rows) in cases {
        let response = statement_projection_rows::<SessionSqlEntity>(&session, sql)
            .expect("projection matrix SQL execution should succeed");
        let actual_rows = response;

        assert_eq!(actual_rows, expected_rows, "projection matrix case: {sql}");
    }
}
