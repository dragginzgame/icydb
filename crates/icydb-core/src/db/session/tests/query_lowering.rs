use super::*;

#[test]
fn query_from_sql_rejects_global_aggregate_execution_in_current_lane() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .query_from_sql::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
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
fn query_from_sql_strict_like_prefix_lowers_to_strict_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>("SELECT * FROM SessionSqlEntity WHERE name LIKE 'Al%'")
        .expect("strict LIKE prefix SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("Al".to_string()),
        CoercionId::Strict,
    )));

    assert_eq!(
        sql_query
            .plan()
            .expect("strict LIKE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent strict starts-with plan should build")
            .into_inner(),
        "plain LIKE 'prefix%' SQL lowering and fluent strict starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn query_from_sql_direct_starts_with_lowers_to_strict_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE STARTS_WITH(name, 'Al')",
        )
        .expect("direct STARTS_WITH SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("Al".to_string()),
        CoercionId::Strict,
    )));

    assert_eq!(
        sql_query
            .plan()
            .expect("direct STARTS_WITH SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent strict starts-with plan should build")
            .into_inner(),
        "direct STARTS_WITH SQL lowering and fluent strict starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn query_from_sql_direct_lower_starts_with_lowers_to_casefold_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE STARTS_WITH(LOWER(name), 'Al')",
        )
        .expect("direct LOWER(field) STARTS_WITH SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("Al".to_string()),
        CoercionId::TextCasefold,
    )));

    assert_eq!(
        sql_query
            .plan()
            .expect("direct LOWER(field) STARTS_WITH SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold starts-with plan should build")
            .into_inner(),
        "direct LOWER(field) STARTS_WITH SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn query_from_sql_direct_upper_starts_with_lowers_to_casefold_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE STARTS_WITH(UPPER(name), 'AL')",
        )
        .expect("direct UPPER(field) STARTS_WITH SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("AL".to_string()),
        CoercionId::TextCasefold,
    )));

    assert_eq!(
        sql_query
            .plan()
            .expect("direct UPPER(field) STARTS_WITH SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold starts-with plan should build")
            .into_inner(),
        "direct UPPER(field) STARTS_WITH SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn query_from_sql_lower_like_prefix_lowers_to_casefold_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE LOWER(name) LIKE 'Al%'",
        )
        .expect("LOWER(field) LIKE prefix SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("Al".to_string()),
        CoercionId::TextCasefold,
    )));

    assert_eq!(
        sql_query
            .plan()
            .expect("LOWER(field) LIKE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold starts-with plan should build")
            .into_inner(),
        "LOWER(field) LIKE 'prefix%' SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn query_from_sql_upper_like_prefix_lowers_to_casefold_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE UPPER(name) LIKE 'AL%'",
        )
        .expect("UPPER(field) LIKE prefix SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("AL".to_string()),
        CoercionId::TextCasefold,
    )));

    assert_eq!(
        sql_query
            .plan()
            .expect("UPPER(field) LIKE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold starts-with plan should build")
            .into_inner(),
        "UPPER(field) LIKE 'prefix%' SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn query_from_sql_upper_text_range_lowers_to_casefold_ordered_bounds_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE UPPER(name) >= 'AL' AND UPPER(name) < 'AM'",
        )
        .expect("UPPER(field) ordered text-range SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Gte,
            Value::Text("AL".to_string()),
            CoercionId::TextCasefold,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Lt,
            Value::Text("AM".to_string()),
            CoercionId::TextCasefold,
        )),
    ]));

    assert_eq!(
        sql_query
            .plan()
            .expect("UPPER(field) ordered text-range SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold ordered bounds plan should build")
            .into_inner(),
        "UPPER(field) ordered text bounds must lower onto the same normalized casefold range intent as the fluent query",
    );
}

#[test]
fn query_from_sql_lower_text_range_lowers_to_casefold_ordered_bounds_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE LOWER(name) >= 'al' AND LOWER(name) < 'am'",
        )
        .expect("LOWER(field) ordered text-range SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Gte,
            Value::Text("al".to_string()),
            CoercionId::TextCasefold,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Lt,
            Value::Text("am".to_string()),
            CoercionId::TextCasefold,
        )),
    ]));

    assert_eq!(
        sql_query
            .plan()
            .expect("LOWER(field) ordered text-range SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold ordered bounds plan should build")
            .into_inner(),
        "LOWER(field) ordered text bounds must lower onto the same normalized casefold range intent as the fluent query",
    );
}
