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
    let columns = dispatch_projection_columns::<SessionSqlEntity>(session, sql)
        .unwrap_or_else(|err| panic!("{context} projection columns should derive: {err:?}"));
    let rows = dispatch_projection_rows::<SessionSqlEntity>(session, sql)
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

// Assert that one field-alias surface still normalizes to the same projected
// rows as the equivalent canonical SQL spelling.
fn assert_projection_alias_matches_canonical<E>(
    session: &DbSession<SessionSqlCanister>,
    aliased_sql: &str,
    canonical_sql: &str,
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister> + crate::traits::EntityValue,
{
    let aliased_rows = dispatch_projection_rows::<E>(session, aliased_sql)
        .unwrap_or_else(|err| panic!("{context} aliased SQL should execute: {err:?}"));
    let canonical_rows = dispatch_projection_rows::<E>(session, canonical_sql)
        .unwrap_or_else(|err| panic!("{context} canonical SQL should execute: {err:?}"));

    assert_eq!(
        aliased_rows, canonical_rows,
        "{context} should normalize onto the same scalar execution order",
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

    let response = session
        .execute_sql::<SessionSqlEntity>(
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
fn sql_projection_columns_select_field_list_returns_canonical_labels() {
    reset_session_sql_store();
    let session = sql_session();

    let columns = dispatch_projection_columns::<SessionSqlEntity>(
        &session,
        "SELECT name, age FROM SessionSqlEntity",
    )
    .expect("field-list SQL projection columns should derive");

    assert_eq!(columns, vec!["name".to_string(), "age".to_string()]);
}

#[test]
fn sql_projection_columns_select_aliases_override_parser_owned_output_labels() {
    reset_session_sql_store();
    let session = sql_session();

    let columns = dispatch_projection_columns::<SessionSqlEntity>(
        &session,
        "SELECT TRIM(name) AS trimmed_name, age years FROM SessionSqlEntity",
    )
    .expect("aliased SQL projection columns should derive");

    assert_eq!(
        columns,
        vec!["trimmed_name".to_string(), "years".to_string()],
    );
}

#[test]
fn execute_sql_projection_order_by_field_alias_matches_canonical_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(&session, &[("bravo", 20), ("alpha", 30), ("charlie", 40)]);

    assert_projection_alias_matches_canonical::<SessionSqlEntity>(
        &session,
        "SELECT name AS display_name FROM SessionSqlEntity ORDER BY display_name ASC LIMIT 3",
        "SELECT name FROM SessionSqlEntity ORDER BY name ASC LIMIT 3",
        "ORDER BY field aliases",
    );
}

#[test]
fn execute_sql_projection_order_by_lower_alias_matches_canonical_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_243_u128, "sam", 10),
            (9_244, "Alex", 20),
            (9_241, "bob", 30),
        ],
    );

    assert_projection_alias_matches_canonical::<ExpressionIndexedSessionSqlEntity>(
        &session,
        "SELECT LOWER(name) AS normalized_name FROM ExpressionIndexedSessionSqlEntity ORDER BY normalized_name ASC LIMIT 3",
        "SELECT LOWER(name) FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC LIMIT 3",
        "ORDER BY LOWER(field) aliases",
    );
}

#[test]
fn execute_sql_projection_rejects_order_by_alias_for_unsupported_target_family() {
    reset_session_sql_store();
    let session = sql_session();

    let err = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT TRIM(name) AS trimmed_name FROM SessionSqlEntity ORDER BY trimmed_name ASC LIMIT 2",
    )
    .expect_err("ORDER BY aliases should stay fail-closed for unsupported target families");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "unsupported ORDER BY alias targets must fail at the session SQL boundary",
    );
    assert!(
        err.to_string()
            .contains("ORDER BY alias 'trimmed_name' does not resolve to a supported order target"),
        "unsupported ORDER BY alias failure should explain the narrowed alias-order boundary",
    );
}

#[test]
fn sql_projection_columns_select_star_returns_entity_model_order() {
    reset_session_sql_store();
    let session = sql_session();

    let columns =
        dispatch_projection_columns::<SessionSqlEntity>(&session, "SELECT * FROM SessionSqlEntity")
            .expect("star SQL projection columns should derive");

    assert_eq!(
        columns,
        vec!["id".to_string(), "name".to_string(), "age".to_string()]
    );
}

#[test]
fn sql_projection_columns_delete_returns_entity_model_order() {
    reset_session_sql_store();
    let session = sql_session();

    let columns = dispatch_projection_columns::<SessionSqlEntity>(
        &session,
        "DELETE FROM SessionSqlEntity WHERE age > 10",
    )
    .expect("delete SQL columns should derive from full entity row shape");

    assert_eq!(
        columns,
        vec!["id".to_string(), "name".to_string(), "age".to_string()],
        "delete SQL should project full entity columns in model order",
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

    let response = dispatch_projection_rows::<SessionSqlEntity>(
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
fn execute_sql_projection_trim_ltrim_rtrim_lower_upper_and_length_dispatch_from_session_boundary() {
    reset_session_sql_store();
    let session = sql_session();

    seed_projection_text_fixture(&session);

    assert_projection_columns_and_rows(
        &session,
        "SELECT TRIM(name), LTRIM(name), RTRIM(name), LOWER(name), UPPER(name), LENGTH(name), age FROM SessionSqlEntity ORDER BY age DESC",
        &[
            "TRIM(name)",
            "LTRIM(name)",
            "RTRIM(name)",
            "LOWER(name)",
            "UPPER(name)",
            "LENGTH(name)",
            "age",
        ],
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
    );
}

#[test]
fn execute_sql_projection_left_and_right_dispatch_from_session_boundary() {
    reset_session_sql_store();
    let session = sql_session();

    seed_projection_text_fixture(&session);

    assert_projection_columns_and_rows(
        &session,
        "SELECT LEFT(name, 2), RIGHT(name, 3), LEFT(name, NULL) FROM SessionSqlEntity ORDER BY age DESC",
        &["LEFT(name, 2)", "RIGHT(name, 3)", "LEFT(name, NULL)"],
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
    );
}

#[test]
fn execute_sql_projection_starts_ends_and_position_dispatch_from_session_boundary() {
    reset_session_sql_store();
    let session = sql_session();

    seed_projection_text_fixture(&session);

    assert_projection_columns_and_rows(
        &session,
        "SELECT STARTS_WITH(name, ' '), ENDS_WITH(name, 'b'), CONTAINS(name, 'da'), POSITION('da', name), POSITION(NULL, name) FROM SessionSqlEntity ORDER BY age DESC",
        &[
            "STARTS_WITH(name, ' ')",
            "ENDS_WITH(name, 'b')",
            "CONTAINS(name, 'da')",
            "POSITION('da', name)",
            "POSITION(NULL, name)",
        ],
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
    );
}

#[test]
fn execute_sql_projection_replace_dispatch_from_session_boundary() {
    reset_session_sql_store();
    let session = sql_session();

    seed_projection_text_fixture(&session);

    assert_projection_columns_and_rows(
        &session,
        "SELECT REPLACE(name, 'A', 'E'), REPLACE(name, NULL, 'x') FROM SessionSqlEntity ORDER BY age DESC",
        &["REPLACE(name, 'A', 'E')", "REPLACE(name, NULL, 'x')"],
        vec![
            vec![Value::Text("  Eda  ".to_string()), Value::Null],
            vec![Value::Text("\tBob".to_string()), Value::Null],
        ],
        "replace projections",
    );
}

#[test]
fn execute_sql_projection_substring_dispatch_from_session_boundary() {
    reset_session_sql_store();
    let session = sql_session();

    seed_projection_text_fixture(&session);

    assert_projection_columns_and_rows(
        &session,
        "SELECT SUBSTRING(name, 3, 3), SUBSTRING(name, 3), SUBSTRING(name, NULL, 2) FROM SessionSqlEntity ORDER BY age DESC",
        &[
            "SUBSTRING(name, 3, 3)",
            "SUBSTRING(name, 3)",
            "SUBSTRING(name, NULL, 2)",
        ],
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

    let response = dispatch_projection_rows::<SessionSqlEntity>(
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
fn execute_sql_select_schema_qualified_entity_executes() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "schema-qualified".to_string(),
            age: 41,
        })
        .expect("seed insert should succeed");

    let response = session
        .execute_sql::<SessionSqlEntity>(
            "SELECT * FROM public.SessionSqlEntity ORDER BY age ASC LIMIT 1",
        )
        .expect("schema-qualified entity SQL should execute");

    assert_eq!(response.len(), 1);
}

#[test]
fn execute_sql_projection_select_qualified_field_forms_execute() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "qualified-projection".to_string(),
            age: 42,
        })
        .expect("seed insert should succeed");

    for (sql, context) in [
        (
            "SELECT SessionSqlEntity.name \
             FROM SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 40 \
             ORDER BY SessionSqlEntity.age DESC LIMIT 1",
            "table-qualified projection SQL",
        ),
        (
            "SELECT alias.name \
             FROM SessionSqlEntity alias \
             WHERE alias.age >= 40 \
             ORDER BY alias.age DESC LIMIT 1",
            "table-alias projection SQL",
        ),
    ] {
        let response = dispatch_projection_rows::<SessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should execute: {err:?}"));
        let row = response
            .first()
            .unwrap_or_else(|| panic!("{context} response should contain one row"));

        assert_eq!(response.len(), 1, "{context} should return one row");
        assert_eq!(
            row,
            &[Value::Text("qualified-projection".to_string())],
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

    let projection = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1",
    )
    .expect("projection SQL execution should support delete statements");
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

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT missing_field FROM SessionSqlEntity")
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

    let response = session
        .execute_sql::<SessionSqlEntity>("SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC")
        .expect("SELECT DISTINCT * should execute");
    assert_eq!(response.len(), 2);
}

#[test]
fn execute_sql_projection_select_distinct_with_pk_field_list_executes() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(&session, &[("distinct-pk-a", 25), ("distinct-pk-b", 25)]);

    let response = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT DISTINCT id, age FROM SessionSqlEntity ORDER BY id ASC",
    )
    .expect("SELECT DISTINCT field-list with PK should execute");
    assert_eq!(response.len(), 2);
    assert_eq!(response[0].len(), 2);
}

#[test]
fn execute_sql_projection_select_distinct_without_pk_projection_executes() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("distinct-no-pk-a", 25),
            ("distinct-no-pk-b", 25),
            ("distinct-no-pk-c", 30),
        ],
    );

    let response = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT DISTINCT age FROM SessionSqlEntity ORDER BY age ASC",
    )
    .expect("SELECT DISTINCT without PK in projection should execute");

    assert_eq!(response, vec![vec![Value::Uint(25)], vec![Value::Uint(30)]]);
}

#[test]
fn execute_sql_projection_select_distinct_without_pk_projection_applies_page_after_dedup() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("distinct-window-a", 25),
            ("distinct-window-b", 25),
            ("distinct-window-c", 30),
            ("distinct-window-d", 35),
        ],
    );

    let response = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT DISTINCT age FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 1",
    )
    .expect("SELECT DISTINCT without PK projection should page after dedup");

    assert_eq!(response, vec![vec![Value::Uint(30)]]);
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
        let response = dispatch_projection_rows::<SessionSqlEntity>(&session, sql)
            .expect("projection matrix SQL execution should succeed");
        let actual_rows = response;

        assert_eq!(actual_rows, expected_rows, "projection matrix case: {sql}");
    }
}
