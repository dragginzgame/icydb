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

#[test]
fn query_from_sql_rejects_non_query_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define statement lanes that must stay outside query_from_sql.
    let cases = [
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
    ];

    // Phase 2: assert each lane remains fail-closed through unsupported execution.
    assert_sql_surface_rejects_statement_lanes(&cases, |sql| {
        session.query_from_sql::<SessionSqlEntity>(sql)
    });
}

#[test]
fn query_from_sql_delete_rejects_non_casefold_wrapped_direct_starts_with() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .query_from_sql::<SessionSqlEntity>(
            "DELETE FROM SessionSqlEntity WHERE STARTS_WITH(TRIM(name), 'Al') ORDER BY age ASC LIMIT 1",
        )
        .expect_err("non-casefold direct STARTS_WITH delete should stay fail-closed");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "query_from_sql should reject non-casefold wrapped direct STARTS_WITH delete",
    );
    assert_sql_unsupported_feature_detail(
        err,
        "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers",
    );
}

#[test]
fn execute_sql_rejects_non_query_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    let cases = [
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
        &cases,
        |sql| session.execute_sql::<SessionSqlEntity>(sql),
        "execute_sql",
    );
}

#[test]
fn execute_sql_grouped_rejects_non_query_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    let cases = [
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
        &cases,
        |sql| session.execute_sql_grouped::<SessionSqlEntity>(sql, None),
        "execute_sql_grouped",
    );
}

#[test]
fn query_from_sql_select_field_projection_lowers_to_scalar_field_selection() {
    reset_session_sql_store();
    let session = sql_session();

    let query = session
        .query_from_sql::<SessionSqlEntity>("SELECT name, age FROM SessionSqlEntity")
        .expect("field-list SQL query should lower");
    let projection = query
        .plan()
        .expect("field-list SQL plan should build")
        .projection_spec();
    let field_names = projection
        .fields()
        .map(|field| match field {
            ProjectionField::Scalar {
                expr: Expr::Field(field),
                alias: None,
            } => field.as_str().to_string(),
            other @ ProjectionField::Scalar { .. } => {
                panic!("field-list SQL projection should lower to plain field exprs: {other:?}")
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(field_names, vec!["name".to_string(), "age".to_string()]);
}

#[test]
fn query_from_sql_rejects_computed_text_projection_in_current_lane() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .query_from_sql::<SessionSqlEntity>("SELECT TRIM(name) FROM SessionSqlEntity")
        .expect_err(
            "query_from_sql should stay on the structural lowered-query lane and reject computed text projection forms",
        );

    assert!(
        err.to_string()
            .contains("query_from_sql does not accept computed text projection"),
        "query_from_sql should reject computed text projection with an actionable boundary message",
    );
}

#[test]
fn execute_sql_rejects_computed_text_projection_in_current_lane() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT TRIM(name) FROM SessionSqlEntity")
        .expect_err("execute_sql should keep computed text projection on the dispatch-owned lane");

    assert!(
        err.to_string()
            .contains("execute_sql rejects computed text projection"),
        "execute_sql should reject computed text projection with an actionable boundary message",
    );
}

#[test]
fn sql_statement_route_select_classifies_query_entity() {
    reset_session_sql_store();
    let session = sql_session();

    let route = session
        .sql_statement_route("SELECT * FROM SessionSqlEntity ORDER BY age ASC LIMIT 1")
        .expect("select SQL statement should parse");

    assert_eq!(
        route,
        SqlStatementRoute::Query {
            entity: "SessionSqlEntity".to_string(),
        }
    );
    assert_eq!(route.entity(), "SessionSqlEntity");
    assert!(!route.is_explain());
    assert!(!route.is_describe());
    assert!(!route.is_show_indexes());
    assert!(!route.is_show_columns());
    assert!(!route.is_show_entities());
}

#[test]
fn sql_statement_route_insert_classifies_entity() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_route, entity, flags, context) in [
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
    ] {
        assert_sql_statement_route_case(&session, sql, expected_route, entity, flags, context);
    }
}

#[test]
fn query_from_sql_accepts_single_table_alias_and_normalizes_to_canonical_fields() {
    reset_session_sql_store();
    let session = sql_session();

    let query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT alias.name \
             FROM SessionSqlEntity alias \
             WHERE alias.age >= 21 \
             ORDER BY alias.age DESC LIMIT 1",
        )
        .expect("single-table alias SQL query should lower");
    let projection = query
        .plan()
        .expect("single-table alias SQL plan should build")
        .projection_spec();
    let field_names = projection
        .fields()
        .map(|field| match field {
            ProjectionField::Scalar {
                expr: Expr::Field(field),
                alias: None,
            } => field.as_str().to_string(),
            other @ ProjectionField::Scalar { .. } => {
                panic!("single-table alias SQL projection should lower to canonical field exprs: {other:?}")
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(field_names, vec!["name".to_string()]);
}

#[test]
fn execute_sql_rejects_quoted_identifiers_in_reduced_parser() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT \"name\" FROM SessionSqlEntity")
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
fn sql_statement_route_show_entities_classifies_surface() {
    reset_session_sql_store();
    let session = sql_session();

    let route = session
        .sql_statement_route("SHOW ENTITIES")
        .expect("show entities SQL statement should parse");

    assert_eq!(route, SqlStatementRoute::ShowEntities);
    assert!(route.is_show_entities());
    assert_eq!(route.entity(), "");
    assert!(!route.is_show_indexes());
    assert!(!route.is_show_columns());
    assert!(!route.is_describe());
    assert!(!route.is_explain());
}

#[test]
fn sql_statement_route_explain_classifies_wrapped_entity() {
    reset_session_sql_store();
    let session = sql_session();

    let route = session
        .sql_statement_route("EXPLAIN JSON DELETE FROM SessionSqlEntity WHERE age > 20 LIMIT 1")
        .expect("explain SQL statement should parse");

    assert_eq!(
        route,
        SqlStatementRoute::Explain {
            entity: "SessionSqlEntity".to_string(),
        }
    );
    assert_eq!(route.entity(), "SessionSqlEntity");
    assert!(route.is_explain());
    assert!(!route.is_describe());
    assert!(!route.is_show_indexes());
    assert!(!route.is_show_columns());
    assert!(!route.is_show_entities());
}

#[test]
fn describe_sql_returns_same_payload_as_describe_entity() {
    reset_session_sql_store();
    let session = sql_session();

    let from_sql = dispatch_describe_sql::<SessionSqlEntity>(&session, "DESCRIBE SessionSqlEntity")
        .expect("describe_sql should succeed");
    let from_typed = session.describe_entity::<SessionSqlEntity>();

    assert_eq!(
        from_sql, from_typed,
        "describe_sql should project through canonical describe_entity payload",
    );
}

#[test]
fn describe_sql_rejects_non_describe_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define lanes that must remain outside describe_sql.
    let cases = [
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
    ];

    // Phase 2: assert each non-describe lane remains fail-closed.
    assert_sql_surface_rejects_statement_lanes(&cases, |sql| {
        dispatch_describe_sql::<SessionSqlEntity>(&session, sql)
    });
}

#[test]
fn show_indexes_sql_returns_same_payload_as_show_indexes() {
    reset_session_sql_store();
    let session = sql_session();

    let from_sql =
        dispatch_show_indexes_sql::<SessionSqlEntity>(&session, "SHOW INDEXES SessionSqlEntity")
            .expect("show_indexes_sql should succeed");
    let from_typed = session.show_indexes::<SessionSqlEntity>();

    assert_eq!(
        from_sql, from_typed,
        "show_indexes_sql should project through canonical show_indexes payload",
    );
}

#[test]
fn show_indexes_sql_rejects_non_show_indexes_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define lanes that must remain outside show_indexes_sql.
    let cases = [
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
    ];

    // Phase 2: assert each non-show-indexes lane remains fail-closed.
    assert_sql_surface_rejects_statement_lanes(&cases, |sql| {
        dispatch_show_indexes_sql::<SessionSqlEntity>(&session, sql)
    });
}

#[test]
fn show_columns_sql_returns_same_payload_as_show_columns() {
    reset_session_sql_store();
    let session = sql_session();

    let from_sql =
        dispatch_show_columns_sql::<SessionSqlEntity>(&session, "SHOW COLUMNS SessionSqlEntity")
            .expect("show_columns_sql should succeed");
    let from_typed = session.show_columns::<SessionSqlEntity>();

    assert_eq!(
        from_sql, from_typed,
        "show_columns_sql should project through canonical show_columns payload",
    );
}

#[test]
fn show_columns_sql_rejects_non_show_columns_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define lanes that must remain outside show_columns_sql.
    let cases = [
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
    ];

    // Phase 2: assert each non-show-columns lane remains fail-closed.
    assert_sql_surface_rejects_statement_lanes(&cases, |sql| {
        dispatch_show_columns_sql::<SessionSqlEntity>(&session, sql)
    });
}

#[test]
fn show_entities_sql_returns_runtime_entity_names() {
    reset_session_sql_store();
    let session = sql_session();

    let entities = dispatch_show_entities_sql(&session, "SHOW ENTITIES")
        .expect("show_entities_sql should succeed");

    assert_eq!(
        entities,
        session.show_entities(),
        "show_entities_sql should project through canonical show_entities payload",
    );
}

#[test]
fn show_entities_sql_rejects_non_show_entities_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define lanes that must remain outside show_entities_sql.
    let cases = [
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
    ];

    // Phase 2: assert each non-show-entities lane remains fail-closed.
    assert_sql_surface_rejects_statement_lanes(&cases, |sql| {
        dispatch_show_entities_sql(&session, sql)
    });
}

#[test]
fn explain_sql_rejects_non_explain_statement_lanes_matrix() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: define lanes that must remain outside explain_sql.
    let cases = [
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
    ];

    // Phase 2: assert each non-explain lane remains fail-closed.
    assert_sql_surface_rejects_statement_lanes(&cases, |sql| {
        dispatch_explain_sql::<SessionSqlEntity>(&session, sql)
    });
}

#[test]
fn sql_statement_route_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    assert_sql_surface_preserves_unsupported_feature_detail(|sql| session.sql_statement_route(sql));
}

#[test]
fn query_from_sql_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        session.query_from_sql::<SessionSqlEntity>(sql)
    });
}

#[test]
fn execute_sql_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        session.execute_sql::<SessionSqlEntity>(sql)
    });
}

#[test]
fn execute_sql_projection_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        dispatch_projection_rows::<SessionSqlEntity>(&session, sql)
    });
}

#[test]
fn execute_sql_grouped_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        session.execute_sql_grouped::<SessionSqlEntity>(sql, None)
    });
}

#[test]
fn execute_sql_aggregate_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        session.execute_sql_aggregate::<SessionSqlEntity>(sql)
    });
}

#[test]
fn explain_sql_preserves_parser_unsupported_feature_detail_labels() {
    reset_session_sql_store();
    let session = sql_session();

    assert_sql_surface_preserves_unsupported_feature_detail(|sql| {
        let explain_sql = format!("EXPLAIN {sql}");
        dispatch_explain_sql::<SessionSqlEntity>(&session, explain_sql.as_str())
    });
}
