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

// Reset the shared SQL store and seed the shared text fixture used by the
// computed text projection surfaces in this file.
fn seeded_projection_text_session() -> DbSession<SessionSqlCanister> {
    reset_session_sql_store();
    let session = sql_session();

    seed_projection_text_fixture(&session);

    session
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

// Reset the shared SQL store and seed the deterministic ordered fixture used
// by the bounded numeric projection matrices in this file.
fn seeded_projection_window_session() -> DbSession<SessionSqlCanister> {
    reset_session_sql_store();
    let session = sql_session();

    seed_projection_window_fixture(&session);

    session
}

// Reset the shared SQL store and seed the bounded ORDER BY fixture rows used
// by the alias/direct numeric ordering checks in this file.
fn seeded_projection_bounded_order_session() -> DbSession<SessionSqlCanister> {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(&session, &[("bravo", 20), ("alpha", 30), ("charlie", 40)]);
    seed_projection_alias_order_aggregate_fixture(&session);

    session
}

// Seed the aggregate rows used by the bounded computed ORDER BY coverage in
// this file.
fn seed_projection_alias_order_aggregate_fixture(session: &DbSession<SessionSqlCanister>) {
    for (group, rank, label) in [(3_u64, 10_u64, "gamma"), (1, 20, "alpha"), (2, 40, "beta")] {
        session
            .insert(SessionAggregateEntity {
                id: Ulid::generate(),
                group,
                rank,
                label: label.to_string(),
            })
            .expect("seed aggregate row insert should succeed");
    }
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

// Execute one row-producing projection SQL statement for the requested entity
// type and assert the public row payload stays exactly as expected.
fn assert_projection_rows_match<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_rows: ProjectedRows,
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    let rows = statement_projection_rows::<E>(session, sql)
        .unwrap_or_else(|err| panic!("{context} projection rows should execute: {err:?}"));

    assert_eq!(
        rows, expected_rows,
        "{context} should materialize the expected projection row payloads",
    );
}

// Run one table of projection row assertions against the requested entity
// surface so nearby matrix tests can share the same assertion loop.
fn assert_projection_row_case_matrix<E>(
    session: &DbSession<SessionSqlCanister>,
    cases: &[(&str, ProjectedRows, &str)],
) where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    for (sql, expected_rows, context) in cases {
        assert_projection_rows_match::<E>(session, sql, expected_rows.clone(), context);
    }
}

// Collect the first scalar value from each projected SQL row so fluent
// projection terminals can compare against the public SQL surface directly.
fn statement_projection_values(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    context: &str,
) -> Vec<Value> {
    statement_projection_rows::<SessionSqlEntity>(session, sql)
        .unwrap_or_else(|err| panic!("{context} SQL projection should execute: {err:?}"))
        .into_iter()
        .map(|row| {
            row.into_iter()
                .next()
                .expect("single-column SQL projection row should contain one value")
        })
        .collect::<Vec<_>>()
}

// Assert that one single-column SQL computed projection stays aligned with the
// shared fluent bounded value-projection terminal over the same ordered
// response window.
fn assert_sql_projection_matches_fluent_value_projection(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    projection: &impl crate::db::ValueProjectionExpr,
    context: &str,
) {
    let sql_values = statement_projection_values(session, sql, context);
    let fluent_values = session
        .load::<SessionSqlEntity>()
        .order_term(crate::db::desc("age"))
        .project_values(projection)
        .unwrap_or_else(|err| panic!("{context} fluent projection should execute: {err:?}"));

    assert_eq!(
        fluent_values, sql_values,
        "{context} fluent projection should stay aligned with the SQL projection values",
    );
}

// Route one shared text-function parity case through the corresponding fluent
// bounded projection helper so the parity matrix stays table-driven.
fn assert_text_projection_case_matches_fluent(
    session: &DbSession<SessionSqlCanister>,
    case: TextProjectionCase,
    sql: &str,
    context: &str,
) {
    match case {
        TextProjectionCase::Trim => assert_sql_projection_matches_fluent_value_projection(
            session,
            sql,
            &crate::db::trim("name"),
            context,
        ),
        TextProjectionCase::Ltrim => assert_sql_projection_matches_fluent_value_projection(
            session,
            sql,
            &crate::db::ltrim("name"),
            context,
        ),
        TextProjectionCase::Rtrim => assert_sql_projection_matches_fluent_value_projection(
            session,
            sql,
            &crate::db::rtrim("name"),
            context,
        ),
        TextProjectionCase::Lower => assert_sql_projection_matches_fluent_value_projection(
            session,
            sql,
            &crate::db::lower("name"),
            context,
        ),
        TextProjectionCase::Upper => assert_sql_projection_matches_fluent_value_projection(
            session,
            sql,
            &crate::db::upper("name"),
            context,
        ),
        TextProjectionCase::Length => assert_sql_projection_matches_fluent_value_projection(
            session,
            sql,
            &crate::db::length("name"),
            context,
        ),
        TextProjectionCase::LeftTwo => assert_sql_projection_matches_fluent_value_projection(
            session,
            sql,
            &crate::db::left("name", 2_i64),
            context,
        ),
        TextProjectionCase::RightThree => assert_sql_projection_matches_fluent_value_projection(
            session,
            sql,
            &crate::db::right("name", 3_i64),
            context,
        ),
        TextProjectionCase::StartsWithSpace => {
            assert_sql_projection_matches_fluent_value_projection(
                session,
                sql,
                &crate::db::starts_with("name", " "),
                context,
            );
        }
        TextProjectionCase::EndsWithB => assert_sql_projection_matches_fluent_value_projection(
            session,
            sql,
            &crate::db::ends_with("name", "b"),
            context,
        ),
        TextProjectionCase::ContainsDa => assert_sql_projection_matches_fluent_value_projection(
            session,
            sql,
            &crate::db::contains("name", "da"),
            context,
        ),
        TextProjectionCase::PositionDa => assert_sql_projection_matches_fluent_value_projection(
            session,
            sql,
            &crate::db::position("name", "da"),
            context,
        ),
        TextProjectionCase::ReplaceAWithE => assert_sql_projection_matches_fluent_value_projection(
            session,
            sql,
            &crate::db::replace("name", "A", "E"),
            context,
        ),
        TextProjectionCase::SubstringThreeThree => {
            assert_sql_projection_matches_fluent_value_projection(
                session,
                sql,
                &crate::db::substring_with_length("name", 3_i64, 3_i64),
                context,
            );
        }
    }
}

#[derive(Clone, Copy)]
enum TextProjectionCase {
    Trim,
    Ltrim,
    Rtrim,
    Lower,
    Upper,
    Length,
    LeftTwo,
    RightThree,
    StartsWithSpace,
    EndsWithB,
    ContainsDa,
    PositionDa,
    ReplaceAWithE,
    SubstringThreeThree,
}

#[derive(Clone, Copy)]
enum NumericProjectionCase {
    AddOne,
    SubOne,
    MulTwo,
    DivTwo,
    RoundDivThree,
    RoundAge,
}

#[expect(clippy::too_many_lines)]
#[test]
fn execute_sql_projection_scalar_numeric_projection_matrix_matches_fluent_and_rows() {
    let session = seeded_projection_window_session();

    // Keep the bounded numeric SQL helpers aligned with the fluent
    // value-projection terminals over the same ordered window.
    for (sql, case, context) in [
        (
            "SELECT age + 1 FROM SessionSqlEntity ORDER BY age DESC LIMIT 4",
            NumericProjectionCase::AddOne,
            "scalar arithmetic projection",
        ),
        (
            "SELECT age - 1 FROM SessionSqlEntity ORDER BY age DESC LIMIT 4",
            NumericProjectionCase::SubOne,
            "scalar subtraction projection",
        ),
        (
            "SELECT age * 2 FROM SessionSqlEntity ORDER BY age DESC LIMIT 4",
            NumericProjectionCase::MulTwo,
            "scalar multiplication projection",
        ),
        (
            "SELECT age / 2 FROM SessionSqlEntity ORDER BY age DESC LIMIT 4",
            NumericProjectionCase::DivTwo,
            "scalar division projection",
        ),
        (
            "SELECT ROUND(age / 3, 2) FROM SessionSqlEntity ORDER BY age DESC LIMIT 4",
            NumericProjectionCase::RoundDivThree,
            "scalar round projection over bounded arithmetic expression",
        ),
        (
            "SELECT ROUND(age, 2) FROM SessionSqlEntity ORDER BY age DESC LIMIT 4",
            NumericProjectionCase::RoundAge,
            "scalar round projection over plain field",
        ),
    ] {
        match case {
            NumericProjectionCase::AddOne => assert_sql_projection_matches_fluent_value_projection(
                &session,
                sql,
                &crate::db::add("age", 1_u64),
                context,
            ),
            NumericProjectionCase::SubOne => assert_sql_projection_matches_fluent_value_projection(
                &session,
                sql,
                &crate::db::sub("age", 1_u64),
                context,
            ),
            NumericProjectionCase::MulTwo => assert_sql_projection_matches_fluent_value_projection(
                &session,
                sql,
                &crate::db::mul("age", 2_u64),
                context,
            ),
            NumericProjectionCase::DivTwo => assert_sql_projection_matches_fluent_value_projection(
                &session,
                sql,
                &crate::db::div("age", 2_u64),
                context,
            ),
            NumericProjectionCase::RoundDivThree => {
                assert_sql_projection_matches_fluent_value_projection(
                    &session,
                    sql,
                    &crate::db::round_expr(&crate::db::div("age", 3_u64), 2),
                    context,
                );
            }
            NumericProjectionCase::RoundAge => {
                assert_sql_projection_matches_fluent_value_projection(
                    &session,
                    sql,
                    &crate::db::round("age", 2),
                    context,
                );
            }
        }
    }

    // Assert the public projection columns and projected row payloads stay
    // stable across the bounded numeric surfaces that materialize values.
    for (sql, expected_columns, expected_rows, context) in [
        (
            "SELECT age + 1 FROM SessionSqlEntity ORDER BY age ASC LIMIT 2",
            &["age + 1"][..],
            vec![
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(11).expect("11 decimal"),
                )],
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(21).expect("21 decimal"),
                )],
            ],
            "scalar arithmetic projection rows",
        ),
        (
            "SELECT age - 1 FROM SessionSqlEntity ORDER BY age DESC LIMIT 4",
            &["age - 1"][..],
            vec![
                vec![Value::Decimal(
                    crate::types::Decimal::from_i128(39).expect("39 decimal"),
                )],
                vec![Value::Decimal(
                    crate::types::Decimal::from_i128(29).expect("29 decimal"),
                )],
                vec![Value::Decimal(
                    crate::types::Decimal::from_i128(19).expect("19 decimal"),
                )],
                vec![Value::Decimal(
                    crate::types::Decimal::from_i128(9).expect("9 decimal"),
                )],
            ],
            "scalar subtraction projection",
        ),
        (
            "SELECT age * 2 FROM SessionSqlEntity ORDER BY age DESC LIMIT 4",
            &["age * 2"][..],
            vec![
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(80).expect("80 decimal"),
                )],
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(60).expect("60 decimal"),
                )],
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(40).expect("40 decimal"),
                )],
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(20).expect("20 decimal"),
                )],
            ],
            "scalar multiplication projection",
        ),
        (
            "SELECT age / 2 FROM SessionSqlEntity ORDER BY age DESC LIMIT 4",
            &["age / 2"][..],
            vec![
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(20).expect("20 decimal"),
                )],
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(15).expect("15 decimal"),
                )],
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(10).expect("10 decimal"),
                )],
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(5).expect("5 decimal"),
                )],
            ],
            "scalar division projection",
        ),
        (
            "SELECT ROUND(age / 3, 2) FROM SessionSqlEntity ORDER BY age ASC LIMIT 2",
            &["ROUND(age / 3, 2)"][..],
            vec![
                vec![Value::Decimal(crate::types::Decimal::new(333, 2))],
                vec![Value::Decimal(crate::types::Decimal::new(667, 2))],
            ],
            "scalar round projection rows",
        ),
    ] {
        assert_projection_columns_and_rows(&session, sql, expected_columns, expected_rows, context);
    }
}

#[test]
fn execute_sql_projection_scalar_field_to_field_numeric_projection_rows_match_expected_surface() {
    let session = seeded_projection_window_session();

    for (sql, expected_columns, expected_rows, context) in [
        (
            "SELECT age + age AS total FROM SessionSqlEntity ORDER BY age ASC LIMIT 2",
            &["total"][..],
            vec![
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(20).expect("20 decimal"),
                )],
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(40).expect("40 decimal"),
                )],
            ],
            "scalar field-to-field arithmetic projection rows",
        ),
        (
            "SELECT ROUND(age + age, 2) AS total FROM SessionSqlEntity ORDER BY age ASC LIMIT 2",
            &["total"][..],
            vec![
                vec![Value::Decimal(crate::types::Decimal::new(2000, 2))],
                vec![Value::Decimal(crate::types::Decimal::new(4000, 2))],
            ],
            "scalar round over field-to-field arithmetic projection rows",
        ),
    ] {
        assert_projection_columns_and_rows(&session, sql, expected_columns, expected_rows, context);
    }
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
fn execute_sql_projection_order_by_supported_scalar_text_aliases_match_canonical_rows() {
    reset_session_sql_store();
    let session = sql_session();

    for (alias_sql, canonical_sql, context) in [
        (
            "SELECT TRIM(name) AS trimmed_name FROM SessionSqlEntity ORDER BY trimmed_name ASC LIMIT 2",
            "SELECT TRIM(name) FROM SessionSqlEntity ORDER BY TRIM(name) ASC LIMIT 2",
            "ORDER BY TRIM alias",
        ),
        (
            "SELECT LTRIM(name) AS left_trimmed_name FROM SessionSqlEntity ORDER BY left_trimmed_name ASC LIMIT 2",
            "SELECT LTRIM(name) FROM SessionSqlEntity ORDER BY LTRIM(name) ASC LIMIT 2",
            "ORDER BY LTRIM alias",
        ),
        (
            "SELECT RTRIM(name) AS right_trimmed_name FROM SessionSqlEntity ORDER BY right_trimmed_name ASC LIMIT 2",
            "SELECT RTRIM(name) FROM SessionSqlEntity ORDER BY RTRIM(name) ASC LIMIT 2",
            "ORDER BY RTRIM alias",
        ),
        (
            "SELECT LENGTH(name) AS name_len FROM SessionSqlEntity ORDER BY name_len DESC LIMIT 2",
            "SELECT LENGTH(name) FROM SessionSqlEntity ORDER BY LENGTH(name) DESC LIMIT 2",
            "ORDER BY LENGTH alias",
        ),
        (
            "SELECT LEFT(name, 2) AS short_name FROM SessionSqlEntity ORDER BY short_name ASC LIMIT 2",
            "SELECT LEFT(name, 2) FROM SessionSqlEntity ORDER BY LEFT(name, 2) ASC LIMIT 2",
            "ORDER BY LEFT alias",
        ),
        (
            "SELECT TRIM(name) AS trimmed_name, ROUND((age + age) / (age + 1), 2) AS normalized_age FROM SessionSqlEntity ORDER BY trimmed_name ASC, normalized_age DESC LIMIT 2",
            "SELECT TRIM(name), ROUND((age + age) / (age + 1), 2) FROM SessionSqlEntity ORDER BY TRIM(name) ASC, ROUND((age + age) / (age + 1), 2) DESC LIMIT 2",
            "mixed TRIM plus nested ROUND alias ordering",
        ),
    ] {
        assert_session_sql_alias_matches_canonical::<Vec<Vec<Value>>>(
            &session,
            statement_projection_rows::<SessionSqlEntity>,
            alias_sql,
            canonical_sql,
            context,
        );
    }
}

#[test]
fn execute_sql_projection_order_by_bounded_numeric_aliases_runs_from_session_boundary() {
    let session = seeded_projection_bounded_order_session();

    assert_projection_row_case_matrix::<SessionSqlEntity>(
        &session,
        &[
            (
                "SELECT name, age + 1 AS next_age FROM SessionSqlEntity ORDER BY next_age ASC LIMIT 3",
                vec![
                    vec![
                        Value::Text("bravo".to_string()),
                        Value::Decimal(crate::types::Decimal::new(21, 0)),
                    ],
                    vec![
                        Value::Text("alpha".to_string()),
                        Value::Decimal(crate::types::Decimal::new(31, 0)),
                    ],
                    vec![
                        Value::Text("charlie".to_string()),
                        Value::Decimal(crate::types::Decimal::new(41, 0)),
                    ],
                ],
                "ORDER BY arithmetic alias",
            ),
            (
                "SELECT name, ROUND(age / 3, 2) AS rounded_age FROM SessionSqlEntity ORDER BY rounded_age DESC LIMIT 3",
                vec![
                    vec![
                        Value::Text("charlie".to_string()),
                        Value::Decimal(crate::types::Decimal::new(1333, 2)),
                    ],
                    vec![
                        Value::Text("alpha".to_string()),
                        Value::Decimal(crate::types::Decimal::new(10, 0)),
                    ],
                    vec![
                        Value::Text("bravo".to_string()),
                        Value::Decimal(crate::types::Decimal::new(667, 2)),
                    ],
                ],
                "ORDER BY ROUND alias",
            ),
        ],
    );

    assert_projection_row_case_matrix::<SessionAggregateEntity>(
        &session,
        &[
            (
                "SELECT label, rank + rank AS total FROM SessionAggregateEntity ORDER BY total ASC LIMIT 3",
                vec![
                    vec![
                        Value::Text("gamma".to_string()),
                        Value::Decimal(crate::types::Decimal::new(20, 0)),
                    ],
                    vec![
                        Value::Text("alpha".to_string()),
                        Value::Decimal(crate::types::Decimal::new(40, 0)),
                    ],
                    vec![
                        Value::Text("beta".to_string()),
                        Value::Decimal(crate::types::Decimal::new(80, 0)),
                    ],
                ],
                "ORDER BY field-to-field arithmetic alias",
            ),
            (
                "SELECT label, ROUND(rank + rank, 2) AS rounded_total FROM SessionAggregateEntity ORDER BY rounded_total DESC LIMIT 3",
                vec![
                    vec![
                        Value::Text("beta".to_string()),
                        Value::Decimal(crate::types::Decimal::new(80, 0)),
                    ],
                    vec![
                        Value::Text("alpha".to_string()),
                        Value::Decimal(crate::types::Decimal::new(40, 0)),
                    ],
                    vec![
                        Value::Text("gamma".to_string()),
                        Value::Decimal(crate::types::Decimal::new(20, 0)),
                    ],
                ],
                "ORDER BY ROUND(field + field) alias",
            ),
        ],
    );
}

#[test]
fn execute_sql_projection_direct_bounded_numeric_order_terms_run_from_session_boundary() {
    let session = seeded_projection_bounded_order_session();

    assert_projection_row_case_matrix::<SessionSqlEntity>(
        &session,
        &[
            (
                "SELECT name, age FROM SessionSqlEntity ORDER BY age + 1 ASC LIMIT 3",
                vec![
                    vec![Value::Text("bravo".to_string()), Value::Uint(20)],
                    vec![Value::Text("alpha".to_string()), Value::Uint(30)],
                    vec![Value::Text("charlie".to_string()), Value::Uint(40)],
                ],
                "direct ORDER BY arithmetic terms",
            ),
            (
                "SELECT name, age FROM SessionSqlEntity ORDER BY ROUND(age / 3, 2) DESC LIMIT 3",
                vec![
                    vec![Value::Text("charlie".to_string()), Value::Uint(40)],
                    vec![Value::Text("alpha".to_string()), Value::Uint(30)],
                    vec![Value::Text("bravo".to_string()), Value::Uint(20)],
                ],
                "direct ORDER BY ROUND terms",
            ),
        ],
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
    let session = seeded_projection_text_session();

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
    let session = seeded_projection_text_session();

    for (sql, case, context) in [
        (
            "SELECT TRIM(name) FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::Trim,
            "TRIM(name) parity",
        ),
        (
            "SELECT LTRIM(name) FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::Ltrim,
            "LTRIM(name) parity",
        ),
        (
            "SELECT RTRIM(name) FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::Rtrim,
            "RTRIM(name) parity",
        ),
        (
            "SELECT LOWER(name) FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::Lower,
            "LOWER(name) parity",
        ),
        (
            "SELECT UPPER(name) FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::Upper,
            "UPPER(name) parity",
        ),
        (
            "SELECT LENGTH(name) FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::Length,
            "LENGTH(name) parity",
        ),
        (
            "SELECT LEFT(name, 2) FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::LeftTwo,
            "LEFT(name, 2) parity",
        ),
        (
            "SELECT RIGHT(name, 3) FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::RightThree,
            "RIGHT(name, 3) parity",
        ),
        (
            "SELECT STARTS_WITH(name, ' ') FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::StartsWithSpace,
            "STARTS_WITH(name, ' ') parity",
        ),
        (
            "SELECT ENDS_WITH(name, 'b') FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::EndsWithB,
            "ENDS_WITH(name, 'b') parity",
        ),
        (
            "SELECT CONTAINS(name, 'da') FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::ContainsDa,
            "CONTAINS(name, 'da') parity",
        ),
        (
            "SELECT POSITION('da', name) FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::PositionDa,
            "POSITION('da', name) parity",
        ),
        (
            "SELECT REPLACE(name, 'A', 'E') FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::ReplaceAWithE,
            "REPLACE(name, 'A', 'E') parity",
        ),
        (
            "SELECT SUBSTRING(name, 3, 3) FROM SessionSqlEntity ORDER BY age DESC",
            TextProjectionCase::SubstringThreeThree,
            "SUBSTRING(name, 3, 3) parity",
        ),
    ] {
        assert_text_projection_case_matches_fluent(&session, case, sql, context);
    }
}

#[test]
fn fluent_text_projection_first_and_last_values_match_sql_ordered_windows() {
    let session = seeded_projection_text_session();

    let projection = crate::db::lower("name");
    let expected = statement_projection_values(
        &session,
        "SELECT LOWER(name) FROM SessionSqlEntity ORDER BY age ASC",
        "LOWER(name) ordered SQL projection",
    );

    let first_value = session
        .load::<SessionSqlEntity>()
        .order_term(crate::db::asc("age"))
        .project_first_value(&projection)
        .expect("fluent first projected value should execute");
    let last_value = session
        .load::<SessionSqlEntity>()
        .order_term(crate::db::asc("age"))
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
fn execute_sql_projection_searched_case_matrix_matches_expected_values() {
    let session = seeded_projection_window_session();

    assert_projection_row_case_matrix::<SessionSqlEntity>(
        &session,
        &[
            (
                "SELECT CASE WHEN age >= 30 THEN 'senior' ELSE 'junior' END \
                 FROM SessionSqlEntity \
                 ORDER BY age ASC",
                vec![
                    vec![Value::Text("junior".to_string())],
                    vec![Value::Text("junior".to_string())],
                    vec![Value::Text("senior".to_string())],
                    vec![Value::Text("senior".to_string())],
                ],
                "searched CASE scalar projection values",
            ),
            (
                "SELECT CASE WHEN age >= 30 THEN 'senior' END \
                 FROM SessionSqlEntity \
                 ORDER BY age ASC",
                vec![
                    vec![Value::Null],
                    vec![Value::Null],
                    vec![Value::Text("senior".to_string())],
                    vec![Value::Text("senior".to_string())],
                ],
                "searched CASE without ELSE should project planner-normalized NULL fallback values",
            ),
        ],
    );
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
fn execute_sql_projection_ulid_string_literal_predicate_matches_single_row() {
    reset_session_sql_store();
    let session = sql_session();
    let target_id = Ulid::from_u128(9_911);
    let other_id = Ulid::from_u128(9_912);
    let sql = format!("SELECT name FROM SessionSqlEntity WHERE id = '{target_id}'");

    session
        .insert(SessionSqlEntity {
            id: target_id,
            name: "ulid-target".to_string(),
            age: 21,
        })
        .expect("target ULID seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: other_id,
            name: "ulid-other".to_string(),
            age: 22,
        })
        .expect("other ULID seed insert should succeed");

    let rows = statement_projection_rows::<SessionSqlEntity>(&session, sql.as_str())
        .expect("quoted ULID projection predicate should execute");

    assert_eq!(rows, vec![vec![Value::Text("ulid-target".to_string())]]);
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
