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
                "SQL query lowering must reject EXPLAIN statements",
            ),
            (
                "DESCRIBE SessionSqlEntity",
                "SQL query lowering must reject DESCRIBE statements",
            ),
            (
                "SHOW INDEXES SessionSqlEntity",
                "SQL query lowering must reject SHOW INDEXES statements",
            ),
            (
                "SHOW COLUMNS SessionSqlEntity",
                "SQL query lowering must reject SHOW COLUMNS statements",
            ),
            (
                "SHOW ENTITIES",
                "SQL query lowering must reject SHOW ENTITIES statements",
            ),
            (
                "INSERT INTO SessionSqlEntity (id, name, age) VALUES (1, 'Ada', 21)",
                "SQL query lowering must reject INSERT statements",
            ),
            (
                "INSERT INTO SessionSqlEntity (name, age) SELECT name, age FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
                "SQL query lowering must reject INSERT statements",
            ),
            (
                "UPDATE SessionSqlEntity SET age = 22 WHERE id = 1",
                "SQL query lowering must reject UPDATE statements",
            ),
        ],
        |sql| lower_select_query_for_tests::<SessionSqlEntity>(&session, sql),
    );

    // Phase 2: require both executable query entrypoints to preserve their
    // own surface-local lane boundary messages for the same non-query SQL.
    let message_cases = [
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity",
            "scalar SELECT helper rejects EXPLAIN",
        ),
        (
            "DESCRIBE SessionSqlEntity",
            "scalar SELECT helper rejects DESCRIBE",
        ),
        (
            "SHOW INDEXES SessionSqlEntity",
            "scalar SELECT helper rejects SHOW INDEXES",
        ),
        (
            "SHOW COLUMNS SessionSqlEntity",
            "scalar SELECT helper rejects SHOW COLUMNS",
        ),
        (
            "SHOW ENTITIES",
            "scalar SELECT helper rejects SHOW ENTITIES",
        ),
        (
            "INSERT INTO SessionSqlEntity (id, name, age) VALUES (1, 'Ada', 21)",
            "scalar SELECT helper rejects INSERT",
        ),
        (
            "INSERT INTO SessionSqlEntity (name, age) SELECT name, age FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
            "scalar SELECT helper rejects INSERT",
        ),
        (
            "UPDATE SessionSqlEntity SET age = 22 WHERE id = 1",
            "scalar SELECT helper rejects UPDATE",
        ),
    ];
    assert_sql_surface_rejects_statement_lanes_with_message(
        &message_cases,
        |sql| execute_scalar_select_for_tests::<SessionSqlEntity>(&session, sql),
        "scalar SELECT helper",
    );

    let grouped_cases = [
        (
            "EXPLAIN SELECT * FROM SessionSqlEntity",
            "grouped SELECT helper rejects EXPLAIN",
        ),
        (
            "DESCRIBE SessionSqlEntity",
            "grouped SELECT helper rejects DESCRIBE",
        ),
        (
            "SHOW INDEXES SessionSqlEntity",
            "grouped SELECT helper rejects SHOW INDEXES",
        ),
        (
            "SHOW COLUMNS SessionSqlEntity",
            "grouped SELECT helper rejects SHOW COLUMNS",
        ),
        (
            "SHOW ENTITIES",
            "grouped SELECT helper rejects SHOW ENTITIES",
        ),
        (
            "INSERT INTO SessionSqlEntity (id, name, age) VALUES (1, 'Ada', 21)",
            "grouped SELECT helper rejects INSERT",
        ),
        (
            "INSERT INTO SessionSqlEntity (name, age) SELECT name, age FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
            "grouped SELECT helper rejects INSERT",
        ),
        (
            "UPDATE SessionSqlEntity SET age = 22 WHERE id = 1",
            "grouped SELECT helper rejects UPDATE",
        ),
    ];

    assert_sql_surface_rejects_statement_lanes_with_message(
        &grouped_cases,
        |sql| execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None),
        "grouped SELECT helper",
    );
}

#[test]
fn sql_query_lowering_projection_matrix_normalizes_to_scalar_fields() {
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
        let query = lower_select_query_for_tests::<SessionSqlEntity>(&session, sql)
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

    let query_err = lower_select_query_for_tests::<SessionSqlEntity>(&session, "SELECT TRIM(name) FROM SessionSqlEntity")
        .expect_err(
            "SQL query lowering should stay on the structural lowered-query lane and reject computed text projection forms",
        );
    assert!(
        query_err
            .to_string()
            .contains("SQL query lowering does not accept computed text projection"),
        "SQL query lowering should reject computed text projection with an actionable boundary message",
    );

    let execute_err = execute_scalar_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT TRIM(name) FROM SessionSqlEntity",
    )
    .expect_err(
        "scalar SELECT helper should keep computed text projection on the statement-owned lane",
    );
    assert!(
        execute_err
            .to_string()
            .contains("scalar SELECT helper rejects computed text projection"),
        "scalar SELECT helper should reject computed text projection with an actionable boundary message",
    );
}

#[test]
fn execute_sql_rejects_quoted_identifiers_in_reduced_parser() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_scalar_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT \"name\" FROM SessionSqlEntity",
    )
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
    let show_tables_from_sql = statement_show_entities_sql(&session, "SHOW TABLES")
        .expect("show tables alias should succeed");

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
    assert_eq!(
        session.show_tables(),
        session.show_entities(),
        "typed show_tables helper should stay a direct alias of show_entities",
    );
    assert_eq!(
        show_tables_from_sql,
        session.show_entities(),
        "SHOW TABLES should stay a direct alias of SHOW ENTITIES",
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

    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        parse_sql_statement_for_tests(&session, sql).map(|_| ())
    });
    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        lower_select_query_for_tests::<SessionSqlEntity>(&session, sql)
    });
    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        execute_scalar_select_for_tests::<SessionSqlEntity>(&session, sql)
    });
    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        statement_projection_rows::<SessionSqlEntity>(&session, sql)
    });
    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None)
    });
    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        let explain_sql = format!("EXPLAIN {sql}");
        statement_explain_sql::<SessionSqlEntity>(&session, explain_sql.as_str())
    });
    assert_specific_sql_unsupported_feature_detail(
        "DELETE FROM SessionSqlEntity WHERE STARTS_WITH(TRIM(name), 'Al') ORDER BY age ASC LIMIT 1",
        "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers",
        |sql| lower_select_query_for_tests::<SessionSqlEntity>(&session, sql),
    );
    let sql = "INSERT INTO SessionSqlEntity (name, age) VALUES ('Ada', 21) RETURNING id";

    assert_unsupported_sql_surface_result(
        lower_select_query_for_tests::<SessionSqlEntity>(&session, sql),
        "SQL query lowering should reject INSERT lane even when RETURNING is present",
    );
    assert_unsupported_sql_surface_result(
        execute_scalar_select_for_tests::<SessionSqlEntity>(&session, sql),
        "scalar SELECT helper should reject INSERT lane even when RETURNING is present",
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
        execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None),
        "grouped SELECT helper should reject INSERT lane even when RETURNING is present",
    );

    let update_returning_sql =
        "UPDATE SessionSqlEntity SET age = 22 WHERE name = 'Ada' RETURNING id, age";

    assert_unsupported_sql_surface_result(
        lower_select_query_for_tests::<SessionSqlEntity>(&session, update_returning_sql),
        "SQL query lowering should reject UPDATE lane even when RETURNING is present",
    );
    assert_unsupported_sql_surface_result(
        execute_scalar_select_for_tests::<SessionSqlEntity>(&session, update_returning_sql),
        "scalar SELECT helper should reject UPDATE lane even when RETURNING is present",
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
        execute_grouped_select_for_tests::<SessionSqlEntity>(&session, update_returning_sql, None),
        "grouped SELECT helper should reject UPDATE lane even when RETURNING is present",
    );

    let delete_returning_sql =
        "DELETE FROM SessionSqlEntity WHERE age > 20 ORDER BY age ASC LIMIT 1 RETURNING id";

    let query_from_err =
        lower_select_query_for_tests::<SessionSqlEntity>(&session, delete_returning_sql)
            .map(|_| ())
            .expect_err("SQL query lowering should reject DELETE RETURNING");
    assert!(
        query_from_err
            .to_string()
            .contains("DELETE RETURNING; use execute_sql_update::<E>()"),
        "SQL query lowering should preserve explicit DELETE RETURNING guidance",
    );

    let execute_sql_err =
        execute_scalar_select_for_tests::<SessionSqlEntity>(&session, delete_returning_sql)
            .map(|_| ())
            .expect_err("scalar SELECT helper should reject DELETE entirely");
    assert!(
        execute_sql_err
            .to_string()
            .contains("scalar SELECT helper rejects DELETE; use execute_sql_update::<E>()"),
        "scalar SELECT helper should preserve explicit fluent delete guidance",
    );

    let grouped_err =
        execute_grouped_select_for_tests::<SessionSqlEntity>(&session, delete_returning_sql, None)
            .map(|_| ())
            .expect_err("grouped SELECT helper should still reject DELETE at the grouped surface");
    assert!(
        grouped_err
            .to_string()
            .contains("grouped SELECT helper rejects DELETE"),
        "grouped SQL surface should preserve its own lane boundary before RETURNING guidance",
    );
}

#[test]
fn execute_sql_statement_admits_supported_single_entity_read_shapes() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("ada", 21), ("bob", 21), ("carol", 32)]);

    let scalar = execute_sql_statement_for_tests::<SessionSqlEntity>(
        &session,
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

    let grouped = execute_sql_statement_for_tests::<SessionSqlEntity>(
        &session,
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

    let aggregate = execute_sql_statement_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT COUNT(*) FROM SessionSqlEntity",
    )
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

    let insert = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
    )
    .expect("execute_sql_statement should admit INSERT");
    let SqlStatementResult::Count { row_count } = insert else {
        panic!("execute_sql_statement INSERT should emit count payload");
    };
    assert_eq!(row_count, 1);

    let update = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
        "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
    )
    .expect("execute_sql_statement should admit UPDATE");
    let SqlStatementResult::Count { row_count } = update else {
        panic!("execute_sql_statement UPDATE should emit count payload");
    };
    assert_eq!(row_count, 1);

    let delete = execute_sql_statement_for_tests::<SessionSqlWriteEntity>(
        &session,
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
fn compile_sql_query_and_execute_compiled_preserve_supported_read_families() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("ada", 21), ("bob", 21), ("carol", 32)]);

    let scalar = session
        .compile_sql_query::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("scalar SELECT should compile");
    assert!(
        matches!(
            scalar,
            crate::db::session::sql::CompiledSqlCommand::Select(_)
        ),
        "scalar SELECT should compile to lowered SELECT artifact",
    );
    let SqlStatementResult::Projection { row_count, .. } = session
        .execute_compiled_sql::<SessionSqlEntity>(&scalar)
        .expect("compiled scalar SELECT should execute")
    else {
        panic!("compiled scalar SELECT should emit projection rows");
    };
    assert_eq!(row_count, 1);

    let grouped = session
        .compile_sql_query::<SessionSqlEntity>(
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
        )
        .expect("grouped SELECT should compile");
    assert!(
        matches!(
            grouped,
            crate::db::session::sql::CompiledSqlCommand::Select(_)
        ),
        "grouped SELECT should stay on the lowered SELECT artifact family",
    );
    let SqlStatementResult::Grouped { row_count, .. } = session
        .execute_compiled_sql::<SessionSqlEntity>(&grouped)
        .expect("compiled grouped SELECT should execute")
    else {
        panic!("compiled grouped SELECT should emit grouped rows");
    };
    assert_eq!(row_count, 2);

    let aggregate = session
        .compile_sql_query::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
        .expect("global aggregate SELECT should compile");
    assert!(
        matches!(
            aggregate,
            crate::db::session::sql::CompiledSqlCommand::GlobalAggregate { .. }
        ),
        "global aggregate SELECT should compile to dedicated aggregate artifact",
    );
    let SqlStatementResult::Projection { row_count, .. } = session
        .execute_compiled_sql::<SessionSqlEntity>(&aggregate)
        .expect("compiled aggregate SELECT should execute")
    else {
        panic!("compiled aggregate SELECT should emit projection rows");
    };
    assert_eq!(row_count, 1);

    let explain = session
        .compile_sql_query::<SessionSqlEntity>(
            "EXPLAIN SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("EXPLAIN SELECT should compile");
    assert!(
        matches!(
            explain,
            crate::db::session::sql::CompiledSqlCommand::Explain(_)
        ),
        "EXPLAIN SELECT should compile to lowered explain artifact",
    );
    let SqlStatementResult::Explain(rendered) = session
        .execute_compiled_sql::<SessionSqlEntity>(&explain)
        .expect("compiled EXPLAIN should execute")
    else {
        panic!("compiled EXPLAIN should emit explain text");
    };
    assert!(
        !rendered.is_empty(),
        "compiled EXPLAIN should render a non-empty explain payload",
    );
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

#[test]
fn compile_sql_update_and_execute_compiled_preserve_supported_mutation_families() {
    reset_session_sql_store();
    let session = sql_session();

    let insert = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
        )
        .expect("INSERT should compile");
    assert!(
        matches!(
            insert,
            crate::db::session::sql::CompiledSqlCommand::Insert(_)
        ),
        "INSERT should compile to prepared INSERT artifact",
    );
    let SqlStatementResult::Count { row_count } = session
        .execute_compiled_sql::<SessionSqlWriteEntity>(&insert)
        .expect("compiled INSERT should execute")
    else {
        panic!("compiled INSERT should emit count payload");
    };
    assert_eq!(row_count, 1);

    let update = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
        )
        .expect("UPDATE should compile");
    assert!(
        matches!(
            update,
            crate::db::session::sql::CompiledSqlCommand::Update(_)
        ),
        "UPDATE should compile to prepared UPDATE artifact",
    );
    let SqlStatementResult::Count { row_count } = session
        .execute_compiled_sql::<SessionSqlWriteEntity>(&update)
        .expect("compiled UPDATE should execute")
    else {
        panic!("compiled UPDATE should emit count payload");
    };
    assert_eq!(row_count, 1);

    let delete = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "DELETE FROM SessionSqlWriteEntity WHERE name = 'Ada' RETURNING name",
        )
        .expect("DELETE RETURNING should compile through update surface");
    assert!(
        matches!(
            delete,
            crate::db::session::sql::CompiledSqlCommand::Delete { .. }
        ),
        "DELETE RETURNING should compile to lowered DELETE artifact",
    );
    let SqlStatementResult::Projection { row_count, .. } = session
        .execute_compiled_sql::<SessionSqlWriteEntity>(&delete)
        .expect("compiled DELETE RETURNING should execute")
    else {
        panic!("compiled DELETE RETURNING should emit projection rows");
    };
    assert_eq!(row_count, 1);
}

#[test]
fn sql_compile_cache_keeps_query_and_update_surfaces_separate() {
    reset_session_sql_store();
    let session = sql_session();

    let insert_sql = "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)";
    let insert = session
        .compile_sql_update::<SessionSqlWriteEntity>(insert_sql)
        .expect("update surface should compile INSERT into the session-local cache");
    assert!(
        matches!(
            insert,
            crate::db::session::sql::CompiledSqlCommand::Insert(_)
        ),
        "update surface should cache the INSERT artifact under the update lane only",
    );

    let err = session
        .compile_sql_query::<SessionSqlWriteEntity>(insert_sql)
        .expect_err("query surface must not reuse the cached update artifact");
    assert!(
        err.to_string()
            .contains("execute_sql_query rejects INSERT; use execute_sql_update::<E>()"),
        "query surface should preserve its own lane boundary after an update-surface cache fill",
    );
}

#[test]
fn sql_compile_cache_covers_query_surface_read_explain_and_metadata_families() {
    reset_session_sql_store();
    let session = sql_session();

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "new SQL session should start with an empty compiled-command cache",
    );

    let scalar = session
        .compile_sql_query::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("scalar SELECT should compile into the query-surface cache");
    assert!(
        matches!(
            scalar,
            crate::db::session::sql::CompiledSqlCommand::Select(_)
        ),
        "scalar SELECT should stay on the lowered SELECT artifact family",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "first query-surface compile should populate one cache entry",
    );

    let scalar_repeat = session
        .compile_sql_query::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("same scalar SELECT should compile from the existing cache entry");
    assert!(
        matches!(
            scalar_repeat,
            crate::db::session::sql::CompiledSqlCommand::Select(_)
        ),
        "repeated scalar SELECT should still resolve to the same artifact family",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "repeating one identical query-surface compile must not grow the cache",
    );

    let explain = session
        .compile_sql_query::<SessionSqlEntity>(
            "EXPLAIN SELECT name FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 1",
        )
        .expect("EXPLAIN should compile into the query-surface cache");
    assert!(
        matches!(
            explain,
            crate::db::session::sql::CompiledSqlCommand::Explain(_)
        ),
        "EXPLAIN should use its dedicated compiled artifact family",
    );

    let describe = session
        .compile_sql_query::<SessionSqlEntity>("DESCRIBE SessionSqlEntity")
        .expect("DESCRIBE should compile into the query-surface cache");
    assert!(
        matches!(
            describe,
            crate::db::session::sql::CompiledSqlCommand::DescribeEntity
        ),
        "DESCRIBE should cache its dedicated metadata artifact",
    );

    let show_indexes = session
        .compile_sql_query::<SessionSqlEntity>("SHOW INDEXES SessionSqlEntity")
        .expect("SHOW INDEXES should compile into the query-surface cache");
    assert!(
        matches!(
            show_indexes,
            crate::db::session::sql::CompiledSqlCommand::ShowIndexesEntity
        ),
        "SHOW INDEXES should cache its dedicated metadata artifact",
    );

    let show_columns = session
        .compile_sql_query::<SessionSqlEntity>("SHOW COLUMNS SessionSqlEntity")
        .expect("SHOW COLUMNS should compile into the query-surface cache");
    assert!(
        matches!(
            show_columns,
            crate::db::session::sql::CompiledSqlCommand::ShowColumnsEntity
        ),
        "SHOW COLUMNS should cache its dedicated metadata artifact",
    );

    let show_entities = session
        .compile_sql_query::<SessionSqlEntity>("SHOW ENTITIES")
        .expect("SHOW ENTITIES should compile into the query-surface cache");
    assert!(
        matches!(
            show_entities,
            crate::db::session::sql::CompiledSqlCommand::ShowEntities
        ),
        "SHOW ENTITIES should cache its dedicated metadata artifact",
    );

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        6,
        "query-surface cache should retain distinct entries for SELECT, EXPLAIN, and metadata families",
    );
}

#[test]
fn sql_compile_cache_covers_insert_update_and_delete_mutation_families() {
    reset_session_sql_store();
    let session = sql_session();

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "new SQL session should start with an empty compiled-command cache",
    );

    let insert = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
        )
        .expect("INSERT should compile into the update-surface cache");
    assert!(
        matches!(
            insert,
            crate::db::session::sql::CompiledSqlCommand::Insert(_)
        ),
        "INSERT should cache the prepared INSERT artifact",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "first update-surface compile should populate one cache entry",
    );

    let insert_repeat = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "INSERT INTO SessionSqlWriteEntity (id, name, age) VALUES (1, 'Ada', 21)",
        )
        .expect("same INSERT should compile from the existing update-surface cache entry");
    assert!(
        matches!(
            insert_repeat,
            crate::db::session::sql::CompiledSqlCommand::Insert(_)
        ),
        "repeated INSERT should stay on the prepared INSERT artifact family",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        1,
        "repeating one identical update-surface compile must not grow the cache",
    );

    let update = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "UPDATE SessionSqlWriteEntity SET age = 22 WHERE id = 1",
        )
        .expect("UPDATE should compile into the update-surface cache");
    assert!(
        matches!(
            update,
            crate::db::session::sql::CompiledSqlCommand::Update(_)
        ),
        "UPDATE should cache the prepared UPDATE artifact",
    );

    let delete = session
        .compile_sql_update::<SessionSqlWriteEntity>(
            "DELETE FROM SessionSqlWriteEntity WHERE name = 'Ada' RETURNING name",
        )
        .expect("DELETE RETURNING should compile into the update-surface cache");
    assert!(
        matches!(
            delete,
            crate::db::session::sql::CompiledSqlCommand::Delete { .. }
        ),
        "DELETE RETURNING should cache the lowered DELETE artifact",
    );

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        3,
        "update-surface cache should retain distinct entries for INSERT, UPDATE, and DELETE",
    );
}
