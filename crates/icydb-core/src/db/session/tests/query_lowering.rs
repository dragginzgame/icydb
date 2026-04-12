use super::*;

// Lower one SQL query and require it to match the same normalized planned
// intent as an explicit fluent query.
fn assert_query_lowering_matches_fluent_intent(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    fluent_query: crate::db::query::intent::Query<SessionSqlEntity>,
    context: &str,
) {
    let sql_query = session
        .lower_sql_query_for_tests::<SessionSqlEntity>(sql)
        .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err}"));

    assert_eq!(
        sql_query
            .plan()
            .unwrap_or_else(|err| panic!("{context} SQL plan should build: {err}"))
            .into_inner(),
        fluent_query
            .plan()
            .unwrap_or_else(|err| panic!("{context} fluent plan should build: {err}"))
            .into_inner(),
        "{context} must produce identical normalized planned intent",
    );
}

#[test]
fn query_from_sql_rejects_global_aggregate_execution_in_current_lane() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .lower_sql_query_for_tests::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
        .expect_err(
            "query_from_sql should keep global aggregate execution on the dedicated aggregate lane",
        );

    assert!(
        err.to_string()
            .contains("query_from_sql rejects global aggregate SELECT"),
        "query_from_sql should reject global aggregate execution with an aggregate-lane boundary message",
    );
}

#[test]
fn query_from_sql_strict_prefix_matrix_lowers_to_strict_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let cases = [
        (
            "SELECT * FROM SessionSqlEntity WHERE name LIKE 'Al%'",
            "plain LIKE 'prefix%' SQL lowering",
        ),
        (
            "SELECT * FROM SessionSqlEntity WHERE STARTS_WITH(name, 'Al')",
            "direct STARTS_WITH SQL lowering",
        ),
    ];

    for (sql, context) in cases {
        let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
            crate::db::predicate::MissingRowPolicy::Ignore,
        )
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("Al".to_string()),
            CoercionId::Strict,
        )));

        assert_query_lowering_matches_fluent_intent(&session, sql, fluent_query, context);
    }
}

#[test]
fn query_from_sql_casefold_prefix_matrix_lowers_to_casefold_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let cases = [
        (
            "SELECT * FROM SessionSqlEntity WHERE STARTS_WITH(LOWER(name), 'Al')",
            "direct LOWER(field) STARTS_WITH SQL lowering",
            "Al",
        ),
        (
            "SELECT * FROM SessionSqlEntity WHERE STARTS_WITH(UPPER(name), 'AL')",
            "direct UPPER(field) STARTS_WITH SQL lowering",
            "AL",
        ),
        (
            "SELECT * FROM SessionSqlEntity WHERE LOWER(name) LIKE 'Al%'",
            "LOWER(field) LIKE 'prefix%' SQL lowering",
            "Al",
        ),
        (
            "SELECT * FROM SessionSqlEntity WHERE UPPER(name) LIKE 'AL%'",
            "UPPER(field) LIKE 'prefix%' SQL lowering",
            "AL",
        ),
    ];

    for (sql, context, prefix) in cases {
        let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
            crate::db::predicate::MissingRowPolicy::Ignore,
        )
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text(prefix.to_string()),
            CoercionId::TextCasefold,
        )));

        assert_query_lowering_matches_fluent_intent(&session, sql, fluent_query, context);
    }
}

#[test]
fn query_from_sql_casefold_ordered_bounds_matrix_lowers_to_casefold_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let cases = [
        (
            "SELECT * FROM SessionSqlEntity WHERE UPPER(name) >= 'AL' AND UPPER(name) < 'AM'",
            "UPPER(field) ordered text bounds lowering",
            "AL",
            "AM",
        ),
        (
            "SELECT * FROM SessionSqlEntity WHERE LOWER(name) >= 'al' AND LOWER(name) < 'am'",
            "LOWER(field) ordered text bounds lowering",
            "al",
            "am",
        ),
    ];

    for (sql, context, lower_bound, upper_bound) in cases {
        let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
            crate::db::predicate::MissingRowPolicy::Ignore,
        )
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Gte,
                Value::Text(lower_bound.to_string()),
                CoercionId::TextCasefold,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Lt,
                Value::Text(upper_bound.to_string()),
                CoercionId::TextCasefold,
            )),
        ]));

        assert_query_lowering_matches_fluent_intent(&session, sql, fluent_query, context);
    }
}
