use super::*;

// Assert that one representative SQL surface stays fail-closed for a matrix of
// statement lanes that belong to some other surface.
fn assert_sql_surface_rejects_statement_lanes<T, F>(cases: &[(&str, &str)], mut execute: F)
where
    F: FnMut(&str) -> Result<T, QueryError>,
{
    for (sql, context) in cases {
        assert_unsupported_sql_surface_result(execute(sql), context);
    }
}

// Assert that one representative SQL surface keeps its own user-facing lane
// rejection message for each disallowed statement family.
fn assert_sql_surface_rejects_statement_lanes_with_message<T, F>(
    cases: &[(&str, &str)],
    mut execute: F,
    surface: &str,
) where
    F: FnMut(&str) -> Result<T, QueryError>,
{
    for (sql, expected) in cases {
        let Err(err) = execute(sql) else {
            panic!("{surface} should reject the non-owned lane: {sql}");
        };
        assert!(
            err.to_string().contains(expected),
            "{surface} should preserve a surface-local lane boundary message: {sql}",
        );
    }
}

// Assert that one unsupported SQL feature is surfaced with the same parser
// detail label through the selected SQL surface.
fn assert_sql_surface_preserves_unsupported_feature_detail<T, F>(mut execute: F)
where
    F: FnMut(&str) -> Result<T, QueryError>,
{
    for (sql, feature) in unsupported_sql_feature_cases() {
        let Err(err) = execute(sql) else {
            panic!("unsupported SQL feature should fail through the SQL surface");
        };
        assert_sql_unsupported_feature_detail(err, feature);
    }
}

// Assert one specific unsupported SQL feature label is preserved through one
// selected non-EXPLAIN SQL surface.
fn assert_specific_sql_unsupported_feature_detail<T, F>(
    sql: &str,
    feature: &'static str,
    mut execute: F,
) where
    F: FnMut(&str) -> Result<T, QueryError>,
{
    let Err(err) = execute(sql) else {
        panic!("unsupported SQL feature should fail through the selected SQL surface");
    };
    assert_sql_unsupported_feature_detail(err, feature);
}

// Assert one SQL statement route surface against the expected route value and
// the derived route classification helpers.
fn assert_sql_statement_route_case(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_route: SqlStatementRoute,
    expected_entity: &str,
    flags: (bool, bool, bool, bool, bool),
    context: &str,
) {
    let route = session
        .sql_statement_route(sql)
        .unwrap_or_else(|err| panic!("{context} should parse: {err:?}"));

    assert_eq!(route, expected_route, "{context} should classify the route");
    assert_eq!(
        route.entity(),
        expected_entity,
        "{context} should preserve the entity surface"
    );
    assert_eq!(
        route.is_explain(),
        flags.0,
        "{context} explain flag should match"
    );
    assert_eq!(
        route.is_describe(),
        flags.1,
        "{context} describe flag should match"
    );
    assert_eq!(
        route.is_show_indexes(),
        flags.2,
        "{context} show-indexes flag should match",
    );
    assert_eq!(
        route.is_show_columns(),
        flags.3,
        "{context} show-columns flag should match",
    );
    assert_eq!(
        route.is_show_entities(),
        flags.4,
        "{context} show-entities flag should match",
    );
}

// This query-surface matrix keeps every non-query statement rejection on one
// outward contract table so the boundary stays easy to audit.
#[expect(
    clippy::too_many_lines,
    reason = "query-surface rejection matrix is intentionally tabular"
)]
#[test]
fn sql_query_surfaces_reject_non_query_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: keep the lowered query entrypoint fail-closed for every
    // non-query statement family.
    assert_sql_surface_rejects_statement_lanes(
        &[
            (
                "EXPLAIN SELECT * FROM SessionSqlEntity",
                "query_from_sql must reject EXPLAIN statements",
            ),
            (
                "DESCRIBE SessionSqlEntity",
                "query_from_sql must reject DESCRIBE statements",
            ),
            (
                "SHOW INDEXES SessionSqlEntity",
                "query_from_sql must reject SHOW INDEXES statements",
            ),
            (
                "SHOW COLUMNS SessionSqlEntity",
                "query_from_sql must reject SHOW COLUMNS statements",
            ),
            (
                "SHOW ENTITIES",
                "query_from_sql must reject SHOW ENTITIES statements",
            ),
            (
                "INSERT INTO SessionSqlEntity (id, name, age) VALUES (1, 'Ada', 21)",
                "query_from_sql must reject INSERT statements",
            ),
            (
                "INSERT INTO SessionSqlEntity (name, age) SELECT name, age FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
                "query_from_sql must reject INSERT statements",
            ),
            (
                "UPDATE SessionSqlEntity SET age = 22 WHERE id = 1",
                "query_from_sql must reject UPDATE statements",
            ),
        ],
        |sql| session.lower_sql_query_for_tests::<SessionSqlEntity>(sql),
    );

    // Phase 2: require both executable query entrypoints to preserve their
    // own surface-local lane boundary messages for the same non-query SQL.
    let message_cases = [
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity",
            "execute_sql rejects EXPLAIN",
        ),
        ("DESCRIBE SessionSqlEntity", "execute_sql rejects DESCRIBE"),
        (
            "SHOW INDEXES SessionSqlEntity",
            "execute_sql rejects SHOW INDEXES",
        ),
        (
            "SHOW COLUMNS SessionSqlEntity",
            "execute_sql rejects SHOW COLUMNS",
        ),
        ("SHOW ENTITIES", "execute_sql rejects SHOW ENTITIES"),
        (
            "INSERT INTO SessionSqlEntity (id, name, age) VALUES (1, 'Ada', 21)",
            "execute_sql rejects INSERT",
        ),
        (
            "INSERT INTO SessionSqlEntity (name, age) SELECT name, age FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
            "execute_sql rejects INSERT",
        ),
        (
            "UPDATE SessionSqlEntity SET age = 22 WHERE id = 1",
            "execute_sql rejects UPDATE",
        ),
    ];
    assert_sql_surface_rejects_statement_lanes_with_message(
        &message_cases,
        |sql| session.execute_scalar_sql_for_tests::<SessionSqlEntity>(sql),
        "execute_sql",
    );

    let grouped_cases = [
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity",
            "execute_sql_grouped rejects EXPLAIN",
        ),
        (
            "DESCRIBE SessionSqlEntity",
            "execute_sql_grouped rejects DESCRIBE",
        ),
        (
            "SHOW INDEXES SessionSqlEntity",
            "execute_sql_grouped rejects SHOW INDEXES",
        ),
        (
            "SHOW COLUMNS SessionSqlEntity",
            "execute_sql_grouped rejects SHOW COLUMNS",
        ),
        ("SHOW ENTITIES", "execute_sql_grouped rejects SHOW ENTITIES"),
        (
            "INSERT INTO SessionSqlEntity (id, name, age) VALUES (1, 'Ada', 21)",
            "execute_sql_grouped rejects INSERT",
        ),
        (
            "INSERT INTO SessionSqlEntity (name, age) SELECT name, age FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
            "execute_sql_grouped rejects INSERT",
        ),
        (
            "UPDATE SessionSqlEntity SET age = 22 WHERE id = 1",
            "execute_sql_grouped rejects UPDATE",
        ),
    ];

    assert_sql_surface_rejects_statement_lanes_with_message(
        &grouped_cases,
        |sql| session.execute_grouped_sql_for_tests::<SessionSqlEntity>(sql, None),
        "execute_sql_grouped",
    );
}

#[test]
fn query_from_sql_projection_lowering_matrix_normalizes_to_scalar_fields() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_field_names, context) in [
        (
            "SELECT name, age FROM SessionSqlEntity",
            vec!["name".to_string(), "age".to_string()],
            "field-list SQL projection",
        ),
        (
            "SELECT alias.name \
             FROM SessionSqlEntity alias \
             WHERE alias.age >= 21 \
             ORDER BY alias.age DESC LIMIT 1",
            vec!["name".to_string()],
            "single-table alias SQL projection",
        ),
    ] {
        let query = session
            .lower_sql_query_for_tests::<SessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} should lower: {err}"));
        let projection = query
            .plan()
            .unwrap_or_else(|err| panic!("{context} plan should build: {err}"))
            .projection_spec();
        let field_names = projection
            .fields()
            .map(|field| match field {
                ProjectionField::Scalar {
                    expr: Expr::Field(field),
                    alias: None,
                } => field.as_str().to_string(),
                other @ ProjectionField::Scalar { .. } => {
                    panic!("{context} should lower to canonical field exprs: {other:?}")
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(
            field_names, expected_field_names,
            "{context} should normalize to scalar field selection",
        );
    }
}

#[test]
fn sql_surface_computed_text_projection_rejection_matrix_preserves_lane_messages() {
    reset_session_sql_store();
    let session = sql_session();

    let query_err = session
        .lower_sql_query_for_tests::<SessionSqlEntity>("SELECT TRIM(name) FROM SessionSqlEntity")
        .expect_err(
            "query_from_sql should stay on the structural lowered-query lane and reject computed text projection forms",
        );
    assert!(
        query_err
            .to_string()
            .contains("query_from_sql does not accept computed text projection"),
        "query_from_sql should reject computed text projection with an actionable boundary message",
    );

    let execute_err = session
        .execute_scalar_sql_for_tests::<SessionSqlEntity>("SELECT TRIM(name) FROM SessionSqlEntity")
        .expect_err("execute_sql should keep computed text projection on the statement-owned lane");
    assert!(
        execute_err
            .to_string()
            .contains("execute_sql rejects computed text projection"),
        "execute_sql should reject computed text projection with an actionable boundary message",
    );
}

#[test]
fn sql_statement_route_matrix_classifies_supported_surfaces() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_route, entity, flags, context) in [
        (
            "SELECT * FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
            SqlStatementRoute::Query {
                entity: "SessionSqlEntity".to_string(),
            },
            "SessionSqlEntity",
            (false, false, false, false, false),
            "select SQL statement",
        ),
        (
            "INSERT INTO SessionSqlEntity (id, name, age) VALUES (1, 'Ada', 21)",
            SqlStatementRoute::Insert {
                entity: "SessionSqlEntity".to_string(),
            },
            "SessionSqlEntity",
            (false, false, false, false, false),
            "insert SQL statement",
        ),
        (
            "UPDATE SessionSqlEntity SET age = 22 WHERE id = 1",
            SqlStatementRoute::Update {
                entity: "SessionSqlEntity".to_string(),
            },
            "SessionSqlEntity",
            (false, false, false, false, false),
            "update SQL statement",
        ),
        (
            "DESCRIBE public.SessionSqlEntity",
            SqlStatementRoute::Describe {
                entity: "public.SessionSqlEntity".to_string(),
            },
            "public.SessionSqlEntity",
            (false, true, false, false, false),
            "describe SQL statement",
        ),
        (
            "SHOW INDEXES public.SessionSqlEntity",
            SqlStatementRoute::ShowIndexes {
                entity: "public.SessionSqlEntity".to_string(),
            },
            "public.SessionSqlEntity",
            (false, false, true, false, false),
            "show indexes SQL statement",
        ),
        (
            "SHOW COLUMNS public.SessionSqlEntity",
            SqlStatementRoute::ShowColumns {
                entity: "public.SessionSqlEntity".to_string(),
            },
            "public.SessionSqlEntity",
            (false, false, false, true, false),
            "show columns SQL statement",
        ),
        (
            "SHOW ENTITIES",
            SqlStatementRoute::ShowEntities,
            "",
            (false, false, false, false, true),
            "show entities SQL statement",
        ),
        (
            "EXPLAIN JSON DELETE FROM SessionSqlEntity WHERE age > 20 LIMIT 1",
            SqlStatementRoute::Explain {
                entity: "SessionSqlEntity".to_string(),
            },
            "SessionSqlEntity",
            (true, false, false, false, false),
            "explain SQL statement",
        ),
    ] {
        assert_sql_statement_route_case(&session, sql, expected_route, entity, flags, context);
    }
}

#[test]
fn execute_sql_rejects_quoted_identifiers_in_reduced_parser() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_scalar_sql_for_tests::<SessionSqlEntity>("SELECT \"name\" FROM SessionSqlEntity")
        .expect_err("quoted identifiers should be rejected by reduced SQL parser");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "quoted identifiers should fail closed through unsupported SQL boundary",
    );
}

#[test]
fn sql_metadata_surfaces_match_typed_payloads() {
    reset_session_sql_store();
    let session = sql_session();

    let describe_from_sql =
        statement_describe_sql::<SessionSqlEntity>(&session, "DESCRIBE SessionSqlEntity")
            .expect("describe_sql should succeed");
    let show_indexes_from_sql =
        statement_show_indexes_sql::<SessionSqlEntity>(&session, "SHOW INDEXES SessionSqlEntity")
            .expect("show_indexes_sql should succeed");
    let show_columns_from_sql =
        statement_show_columns_sql::<SessionSqlEntity>(&session, "SHOW COLUMNS SessionSqlEntity")
            .expect("show_columns_sql should succeed");
    let show_entities_from_sql = statement_show_entities_sql(&session, "SHOW ENTITIES")
        .expect("show_entities_sql should succeed");

    assert_eq!(
        describe_from_sql,
        session.describe_entity::<SessionSqlEntity>(),
        "describe_sql should project through canonical describe_entity payload",
    );
    assert_eq!(
        show_indexes_from_sql,
        session.show_indexes::<SessionSqlEntity>(),
        "show_indexes_sql should project through canonical show_indexes payload",
    );
    assert_eq!(
        show_columns_from_sql,
        session.show_columns::<SessionSqlEntity>(),
        "show_columns_sql should project through canonical show_columns payload",
    );
    assert_eq!(
        show_entities_from_sql,
        session.show_entities(),
        "show_entities_sql should project through canonical show_entities payload",
    );
}

// This metadata/explain matrix keeps every non-owned statement rejection on
// one outward surface contract instead of splitting the statement families.
#[expect(
    clippy::too_many_lines,
    reason = "metadata and explain rejection matrix is intentionally tabular"
)]
#[test]
fn sql_metadata_and_explain_surfaces_reject_non_owned_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    assert_sql_surface_rejects_statement_lanes(
        &[
            (
                "SELECT * FROM SessionSqlEntity",
                "describe_sql should reject SELECT statements",
            ),
            (
                "EXPLAIN SELECT * FROM SessionSqlEntity",
                "describe_sql should reject EXPLAIN statements",
            ),
            (
                "SHOW INDEXES SessionSqlEntity",
                "describe_sql should reject SHOW INDEXES statements",
            ),
            (
                "SHOW COLUMNS SessionSqlEntity",
                "describe_sql should reject SHOW COLUMNS statements",
            ),
            (
                "SHOW ENTITIES",
                "describe_sql should reject SHOW ENTITIES statements",
            ),
        ],
        |sql| statement_describe_sql::<SessionSqlEntity>(&session, sql),
    );
    assert_sql_surface_rejects_statement_lanes(
        &[
            (
                "SELECT * FROM SessionSqlEntity",
                "show_indexes_sql should reject SELECT statements",
            ),
            (
                "EXPLAIN SELECT * FROM SessionSqlEntity",
                "show_indexes_sql should reject EXPLAIN statements",
            ),
            (
                "DESCRIBE SessionSqlEntity",
                "show_indexes_sql should reject DESCRIBE statements",
            ),
            (
                "SHOW COLUMNS SessionSqlEntity",
                "show_indexes_sql should reject SHOW COLUMNS statements",
            ),
            (
                "SHOW ENTITIES",
                "show_indexes_sql should reject SHOW ENTITIES statements",
            ),
        ],
        |sql| statement_show_indexes_sql::<SessionSqlEntity>(&session, sql),
    );
    assert_sql_surface_rejects_statement_lanes(
        &[
            (
                "SELECT * FROM SessionSqlEntity",
                "show_columns_sql should reject SELECT statements",
            ),
            (
                "EXPLAIN SELECT * FROM SessionSqlEntity",
                "show_columns_sql should reject EXPLAIN statements",
            ),
            (
                "DESCRIBE SessionSqlEntity",
                "show_columns_sql should reject DESCRIBE statements",
            ),
            (
                "SHOW INDEXES SessionSqlEntity",
                "show_columns_sql should reject SHOW INDEXES statements",
            ),
            (
                "SHOW ENTITIES",
                "show_columns_sql should reject SHOW ENTITIES statements",
            ),
        ],
        |sql| statement_show_columns_sql::<SessionSqlEntity>(&session, sql),
    );
    assert_sql_surface_rejects_statement_lanes(
        &[
            (
                "SELECT * FROM SessionSqlEntity",
                "show_entities_sql should reject SELECT statements",
            ),
            (
                "EXPLAIN SELECT * FROM SessionSqlEntity",
                "show_entities_sql should reject EXPLAIN statements",
            ),
            (
                "DESCRIBE SessionSqlEntity",
                "show_entities_sql should reject DESCRIBE statements",
            ),
            (
                "SHOW INDEXES SessionSqlEntity",
                "show_entities_sql should reject SHOW INDEXES statements",
            ),
            (
                "SHOW COLUMNS SessionSqlEntity",
                "show_entities_sql should reject SHOW COLUMNS statements",
            ),
        ],
        |sql| statement_show_entities_sql(&session, sql),
    );
    assert_sql_surface_rejects_statement_lanes(
        &[
            (
                "DESCRIBE SessionSqlEntity",
                "explain_sql should reject DESCRIBE statements",
            ),
            (
                "SHOW INDEXES SessionSqlEntity",
                "explain_sql should reject SHOW INDEXES statements",
            ),
            (
                "SHOW COLUMNS SessionSqlEntity",
                "explain_sql should reject SHOW COLUMNS statements",
            ),
            (
                "SHOW ENTITIES",
                "explain_sql should reject SHOW ENTITIES statements",
            ),
        ],
        |sql| statement_explain_sql::<SessionSqlEntity>(&session, sql),
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "this matrix test intentionally proves one shared unsupported-feature contract across several SQL surfaces"
)]
fn sql_surfaces_preserve_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    assert_sql_surface_preserves_unsupported_feature_detail(|sql| session.sql_statement_route(sql));
    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        session.lower_sql_query_for_tests::<SessionSqlEntity>(sql)
    });
    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        session.execute_scalar_sql_for_tests::<SessionSqlEntity>(sql)
    });
    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        statement_projection_rows::<SessionSqlEntity>(&session, sql)
    });
    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        session.execute_grouped_sql_for_tests::<SessionSqlEntity>(sql, None)
    });
    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        let explain_sql = format!("EXPLAIN {sql}");
        statement_explain_sql::<SessionSqlEntity>(&session, explain_sql.as_str())
    });
    assert_specific_sql_unsupported_feature_detail(
        "DELETE FROM SessionSqlEntity WHERE STARTS_WITH(TRIM(name), 'Al') ORDER BY age ASC LIMIT 1",
        "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers",
        |sql| session.lower_sql_query_for_tests::<SessionSqlEntity>(sql),
    );
    let sql = "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21) RETURNING id";

    assert_unsupported_sql_surface_result(
        session.lower_sql_query_for_tests::<SessionSqlEntity>(sql),
        "query_from_sql should reject INSERT lane even when RETURNING is present",
    );
    assert_unsupported_sql_surface_result(
        session.execute_scalar_sql_for_tests::<SessionSqlEntity>(sql),
        "execute_sql should reject INSERT lane even when RETURNING is present",
    );
    let returning_rows = statement_projection_rows::<SessionSqlEntity>(&session, sql)
        .expect("statement execution should admit INSERT RETURNING");
    assert_eq!(returning_rows.len(), 1);
    assert_eq!(returning_rows[0].len(), 1);
    assert!(
        matches!(returning_rows[0][0], Value::Ulid(_)),
        "statement INSERT RETURNING should project the requested generated id",
    );
    assert_unsupported_sql_surface_result(
        session.execute_grouped_sql_for_tests::<SessionSqlEntity>(sql, None),
        "execute_sql_grouped should reject INSERT lane even when RETURNING is present",
    );

    let update_returning_sql =
        "UPDATE SessionSqlEntity SET age = 22 WHERE name = 'Ada' RETURNING id, age";

    assert_unsupported_sql_surface_result(
        session.lower_sql_query_for_tests::<SessionSqlEntity>(update_returning_sql),
        "query_from_sql should reject UPDATE lane even when RETURNING is present",
    );
    assert_unsupported_sql_surface_result(
        session.execute_scalar_sql_for_tests::<SessionSqlEntity>(update_returning_sql),
        "execute_sql should reject UPDATE lane even when RETURNING is present",
    );
    let updated_rows =
        statement_projection_rows::<SessionSqlEntity>(&session, update_returning_sql)
            .expect("statement execution should admit UPDATE RETURNING");
    assert_eq!(updated_rows.len(), 1);
    assert_eq!(updated_rows[0].len(), 2);
    assert!(
        matches!(updated_rows[0][0], Value::Ulid(_)),
        "statement UPDATE RETURNING should project the requested generated id",
    );
    assert_eq!(
        updated_rows[0][1],
        Value::Uint(22),
        "statement UPDATE RETURNING should project the updated scalar field",
    );
    assert_unsupported_sql_surface_result(
        session.execute_grouped_sql_for_tests::<SessionSqlEntity>(update_returning_sql, None),
        "execute_sql_grouped should reject UPDATE lane even when RETURNING is present",
    );

    let delete_returning_sql =
        "DELETE FROM SessionSqlEntity WHERE age > 20 ORDER BY age ASC LIMIT 1 RETURNING id";

    let query_from_err = session
        .lower_sql_query_for_tests::<SessionSqlEntity>(delete_returning_sql)
        .map(|_| ())
        .expect_err("query_from_sql should reject DELETE RETURNING");
    assert!(
        query_from_err
            .to_string()
            .contains("DELETE RETURNING; use delete::<E>().returning..."),
        "query_from_sql should preserve explicit DELETE RETURNING guidance",
    );

    let execute_sql_err = session
        .execute_scalar_sql_for_tests::<SessionSqlEntity>(delete_returning_sql)
        .map(|_| ())
        .expect_err("execute_sql should reject DELETE entirely");
    assert!(
        execute_sql_err
            .to_string()
            .contains("execute_sql rejects DELETE; use delete::<E>()"),
        "execute_sql should preserve explicit fluent delete guidance",
    );

    let grouped_err = session
        .execute_grouped_sql_for_tests::<SessionSqlEntity>(delete_returning_sql, None)
        .map(|_| ())
        .expect_err("execute_sql_grouped should still reject DELETE at the grouped surface");
    assert!(
        grouped_err
            .to_string()
            .contains("execute_sql_grouped rejects DELETE"),
        "grouped SQL surface should preserve its own lane boundary before RETURNING guidance",
    );
}

#[test]
fn execute_sql_statement_admits_supported_single_entity_read_shapes() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("ada", 21), ("bob", 21), ("carol", 32)]);

    let scalar = session
        .execute_sql_statement::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("execute_sql_statement should admit scalar SELECT");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
    } = scalar
    else {
        panic!("execute_sql_statement scalar SELECT should emit projection rows");
    };
    assert_eq!(columns, vec!["name".to_string()]);
    assert_eq!(rows, vec![vec![Value::Text("ada".to_string())]]);
    assert_eq!(row_count, 1);

    let grouped = session
        .execute_sql_statement::<SessionSqlEntity>(
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
        )
        .expect("execute_sql_statement should admit grouped SELECT");
    let SqlStatementResult::Grouped {
        columns, row_count, ..
    } = grouped
    else {
        panic!("execute_sql_statement grouped SELECT should emit grouped rows");
    };
    assert_eq!(columns, vec!["age".to_string(), "COUNT(*)".to_string()]);
    assert_eq!(row_count, 2);

    let aggregate = session
        .execute_sql_statement::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
        .expect("execute_sql_statement should admit global aggregate SELECT");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
    } = aggregate
    else {
        panic!("execute_sql_statement aggregate SELECT should emit projection rows");
    };
    assert_eq!(columns, vec!["COUNT(*)".to_string()]);
    assert_eq!(rows, vec![vec![Value::Uint(3)]]);
    assert_eq!(row_count, 1);
}

#[test]
fn execute_sql_statement_admits_supported_single_entity_mutation_shapes() {
    reset_session_sql_store();
    let session = sql_session();

    let insert = session
        .execute_sql_statement::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
        )
        .expect("execute_sql_statement should admit INSERT");
    let SqlStatementResult::Count { row_count } = insert else {
        panic!("execute_sql_statement INSERT should emit count payload");
    };
    assert_eq!(row_count, 1);

    let update = session
        .execute_sql_statement::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
        )
        .expect("execute_sql_statement should admit UPDATE");
    let SqlStatementResult::Count { row_count } = update else {
        panic!("execute_sql_statement UPDATE should emit count payload");
    };
    assert_eq!(row_count, 1);

    let delete = session
        .execute_sql_statement::<SessionSqlWriteEntity>(
            "DELETE FROM SessionSqlWriteEntity WHERE name = 'Ada' RETURNING name",
        )
        .expect("execute_sql_statement should admit DELETE RETURNING");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
    } = delete
    else {
        panic!("execute_sql_statement DELETE RETURNING should emit projection rows");
    };
    assert_eq!(columns, vec!["name".to_string()]);
    assert_eq!(rows, vec![vec![Value::Text("Ada".to_string())]]);
    assert_eq!(row_count, 1);
}

#[test]
fn execute_sql_query_rejects_supported_single_entity_mutation_shapes() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected, context) in [
        (
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
            "execute_sql_query rejects INSERT; use execute_sql_update::<E>()",
            "query SQL surface should reject INSERT",
        ),
        (
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
            "execute_sql_query rejects UPDATE; use execute_sql_update::<E>()",
            "query SQL surface should reject UPDATE",
        ),
        (
            "DELETE FROM SessionSqlWriteEntity WHERE id = 1",
            "execute_sql_query rejects DELETE; use execute_sql_update::<E>()",
            "query SQL surface should reject DELETE",
        ),
    ] {
        let err = session
            .execute_sql_query::<SessionSqlWriteEntity>(sql)
            .expect_err(context);
        assert!(
            err.to_string().contains(expected),
            "{context} should preserve the query-to-update guidance",
        );
    }
}

#[test]
fn execute_sql_query_admits_supported_single_entity_read_shapes() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("ada", 21), ("bob", 21), ("carol", 32)]);

    let scalar = session
        .execute_sql_query::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("execute_sql_query should admit scalar SELECT");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
    } = scalar
    else {
        panic!("execute_sql_query scalar SELECT should emit projection rows");
    };
    assert_eq!(columns, vec!["name".to_string()]);
    assert_eq!(rows, vec![vec![Value::Text("ada".to_string())]]);
    assert_eq!(row_count, 1);

    let grouped = session
        .execute_sql_query::<SessionSqlEntity>(
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
        )
        .expect("execute_sql_query should admit grouped SELECT");
    let SqlStatementResult::Grouped {
        columns, row_count, ..
    } = grouped
    else {
        panic!("execute_sql_query grouped SELECT should emit grouped rows");
    };
    assert_eq!(columns, vec!["age".to_string(), "COUNT(*)".to_string()]);
    assert_eq!(row_count, 2);

    let aggregate = session
        .execute_sql_query::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
        .expect("execute_sql_query should admit global aggregate SELECT");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
    } = aggregate
    else {
        panic!("execute_sql_query aggregate SELECT should emit projection rows");
    };
    assert_eq!(columns, vec!["COUNT(*)".to_string()]);
    assert_eq!(rows, vec![vec![Value::Uint(3)]]);
    assert_eq!(row_count, 1);
}

#[test]
fn execute_sql_update_admits_supported_single_entity_mutation_shapes() {
    reset_session_sql_store();
    let session = sql_session();

    let insert = session
        .execute_sql_update::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
        )
        .expect("execute_sql_update should admit INSERT");
    let SqlStatementResult::Count { row_count } = insert else {
        panic!("execute_sql_update INSERT should emit count payload");
    };
    assert_eq!(row_count, 1);

    let update = session
        .execute_sql_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
        )
        .expect("execute_sql_update should admit UPDATE");
    let SqlStatementResult::Count { row_count } = update else {
        panic!("execute_sql_update UPDATE should emit count payload");
    };
    assert_eq!(row_count, 1);

    let delete = session
        .execute_sql_update::<SessionSqlWriteEntity>(
            "DELETE FROM SessionSqlWriteEntity WHERE name = 'Ada' RETURNING name",
        )
        .expect("execute_sql_update should admit DELETE RETURNING");
    let SqlStatementResult::Projection {
        columns,
        rows,
        row_count,
    } = delete
    else {
        panic!("execute_sql_update DELETE RETURNING should emit projection rows");
    };
    assert_eq!(columns, vec!["name".to_string()]);
    assert_eq!(rows, vec![vec![Value::Text("Ada".to_string())]]);
    assert_eq!(row_count, 1);
}
