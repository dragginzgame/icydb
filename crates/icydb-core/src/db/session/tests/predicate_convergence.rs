use super::*;
use crate::db::{
    FieldRef,
    predicate::{CoercionId, CompareOp, ComparePredicate, PredicateProgram},
};

// Project entity rows into stable names so SQL, fluent, direct-predicate, and
// full-scan comparisons stay focused on predicate semantics rather than row DTO
// details.
fn session_names(response: EntityResponse<SessionSqlEntity>) -> Vec<String> {
    response
        .iter()
        .map(|row| row.entity_ref().name.clone())
        .collect()
}

// Project nullable entity rows into stable names for NULL-semantics tests.
fn nullable_names(response: EntityResponse<SessionNullableSqlEntity>) -> Vec<String> {
    response
        .iter()
        .map(|row| row.entity_ref().name.clone())
        .collect()
}

// Project indexed entity rows into stable `(name, age)` pairs so ready-index
// and hidden-index execution can compare the same public result contract.
fn indexed_name_age_rows(response: EntityResponse<IndexedSessionSqlEntity>) -> Vec<(String, u64)> {
    response
        .iter()
        .map(|row| (row.entity_ref().name.clone(), row.entity_ref().age))
        .collect()
}

// Project expression-indexed entity rows into stable `(name, age)` pairs.
fn expression_name_age_rows(
    response: EntityResponse<ExpressionIndexedSessionSqlEntity>,
) -> Vec<(String, u64)> {
    response
        .iter()
        .map(|row| (row.entity_ref().name.clone(), row.entity_ref().age))
        .collect()
}

// Execute one SQL scalar select over the indexed fixture and return comparable
// row values.
fn indexed_sql_rows(session: &DbSession<SessionSqlCanister>, sql: &str) -> Vec<(String, u64)> {
    indexed_name_age_rows(
        execute_scalar_select_for_tests::<IndexedSessionSqlEntity>(&session, sql)
            .expect("indexed predicate convergence SQL should execute"),
    )
}

// Execute one SQL scalar select over the expression-indexed fixture and return
// comparable row values.
fn expression_sql_rows(session: &DbSession<SessionSqlCanister>, sql: &str) -> Vec<(String, u64)> {
    expression_name_age_rows(
        execute_scalar_select_for_tests::<ExpressionIndexedSessionSqlEntity>(&session, sql)
            .expect("expression-index predicate convergence SQL should execute"),
    )
}

// Hide secondary indexes without clearing the underlying rows so the same SQL
// statement can be compared through index pushdown and full-scan fallback.
fn hide_indexed_session_indexes() {
    INDEXED_SESSION_SQL_DB
        .recovered_store(IndexedSessionSqlStore::PATH)
        .expect("indexed SQL store should recover")
        .mark_index_building();
}

#[test]
fn predicate_sql_and_fluent_filters_converge_on_scalar_rows() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[("Alpha", 10), ("bravo", 20), ("CHARLIE", 30), ("delta", 40)],
    );

    let cases = [
        (
            "numeric range",
            "SELECT * FROM SessionSqlEntity WHERE age >= 20 ORDER BY age ASC",
            FieldRef::new("age").gte(20_u64),
        ),
        (
            "text casefold",
            "SELECT * FROM SessionSqlEntity WHERE LOWER(name) = 'alpha' ORDER BY age ASC",
            FieldRef::new("name").text_eq_ci("alpha"),
        ),
        (
            "membership",
            "SELECT * FROM SessionSqlEntity WHERE age IN (20, 40) ORDER BY age ASC",
            FieldRef::new("age").in_list([20_u64, 40_u64]),
        ),
    ];

    for (context, sql, filter) in cases {
        let sql_rows = session_names(
            execute_scalar_select_for_tests::<SessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| panic!("{context} SQL should execute: {err}")),
        );
        let fluent_rows = session_names(
            session
                .load::<SessionSqlEntity>()
                .filter(filter)
                .order_term(crate::db::asc("age"))
                .execute()
                .and_then(crate::db::LoadQueryResult::into_rows)
                .unwrap_or_else(|err| panic!("{context} fluent query should execute: {err}")),
        );

        assert_eq!(
            sql_rows, fluent_rows,
            "{context} SQL and fluent predicate filters should return the same rows",
        );
    }
}

#[test]
fn predicate_optional_null_converges_but_sql_eq_null_stays_unknown_false() {
    reset_session_sql_store();
    let session = sql_session();
    seed_nullable_session_sql_entities(
        &session,
        &[("explicit-null", None), ("present", Some("present"))],
    );

    let fluent_null_rows = nullable_names(
        session
            .load::<SessionNullableSqlEntity>()
            .filter(FieldRef::new("nickname").is_null())
            .execute()
            .and_then(crate::db::LoadQueryResult::into_rows)
            .expect("fluent IS NULL query should execute"),
    );
    let sql_is_null_rows = nullable_names(
        execute_scalar_select_for_tests::<SessionNullableSqlEntity>(
            &session,
            "SELECT * FROM SessionNullableSqlEntity WHERE nickname IS NULL",
        )
        .expect("SQL IS NULL query should execute"),
    );
    let sql_eq_null_rows = nullable_names(
        execute_scalar_select_for_tests::<SessionNullableSqlEntity>(
            &session,
            "SELECT * FROM SessionNullableSqlEntity WHERE nickname = NULL",
        )
        .expect("SQL = NULL query should execute"),
    );
    let rejected_direct_null = session.execute_query(
        &Query::<SessionNullableSqlEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
            Predicate::Compare(ComparePredicate::with_coercion(
                "nickname",
                CompareOp::Eq,
                Value::Null,
                CoercionId::Strict,
            )),
        ),
    );

    let expected_null_rows = vec!["explicit-null".to_string()];
    assert_eq!(
        fluent_null_rows, expected_null_rows,
        "fluent IS NULL should match explicit nullable rows",
    );
    assert_eq!(
        sql_is_null_rows, expected_null_rows,
        "SQL IS NULL should match explicit nullable rows",
    );
    assert!(
        sql_eq_null_rows.is_empty(),
        "SQL = NULL should keep SQL UNKNOWN/false WHERE semantics",
    );
    assert!(
        rejected_direct_null.is_err(),
        "query validation currently rejects direct Compare(field, NULL); runtime direct NULL equality is locked in predicate runtime tests",
    );
}

#[test]
fn predicate_index_pushdown_and_full_scan_paths_return_same_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("alpha", 10), ("bravo", 20), ("charlie", 30), ("delta", 40)],
    );

    let queries = [
        (
            "secondary equality",
            "SELECT * FROM IndexedSessionSqlEntity WHERE name = 'bravo' ORDER BY name ASC, id ASC",
        ),
        (
            "secondary range",
            "SELECT * FROM IndexedSessionSqlEntity WHERE name >= 'bravo' AND name < 'delta' ORDER BY name ASC, id ASC",
        ),
        (
            "partial AND residual",
            "SELECT * FROM IndexedSessionSqlEntity WHERE name >= 'bravo' AND name < 'delta' AND age > 20 ORDER BY name ASC, id ASC",
        ),
        (
            "non-pushdown OR fallback",
            "SELECT * FROM IndexedSessionSqlEntity WHERE name = 'alpha' OR age = 40 ORDER BY name ASC, id ASC",
        ),
    ];
    let ready_results = queries
        .iter()
        .map(|(context, sql)| (*context, indexed_sql_rows(&session, sql)))
        .collect::<Vec<_>>();

    hide_indexed_session_indexes();

    for ((context, sql), (_, ready_rows)) in queries.iter().zip(ready_results.iter()) {
        assert_eq!(
            indexed_sql_rows(&session, sql),
            *ready_rows,
            "{context} index-visible and index-hidden execution should return identical rows",
        );
    }
}

#[test]
fn predicate_text_casefold_expression_index_matches_full_scan() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_811, "Alpha", 10),
            (9_812, "bravo", 20),
            (9_813, "ALPINE", 30),
        ],
    );

    let sql = "SELECT * FROM ExpressionIndexedSessionSqlEntity \
               WHERE LOWER(name) >= 'alp' AND LOWER(name) < 'alq' \
               ORDER BY id ASC";
    let ready_rows = expression_sql_rows(&session, sql);

    hide_indexed_session_indexes();

    assert_eq!(
        expression_sql_rows(&session, sql),
        ready_rows,
        "TextCasefold expression-index execution should match full-scan fallback rows",
    );
}

#[test]
fn predicate_and_projection_comparisons_match_for_shared_supported_cases() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("cmp-alpha", 20)]);

    let projection_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT age = 20, age > 10, name < 'z' FROM SessionSqlEntity WHERE name = 'cmp-alpha'",
    )
    .expect("comparison projection SQL should execute");
    let [projection_row] = projection_rows.as_slice() else {
        panic!("comparison projection should return exactly one row");
    };
    let values = [
        (1_usize, Value::Text("cmp-alpha".to_string())),
        (2_usize, Value::Nat(20)),
    ];

    let predicate_cases = [
        (
            Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Eq,
                Value::Nat(20),
                CoercionId::NumericWiden,
            )),
            Value::Bool(true),
        ),
        (
            Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Gt,
                Value::Nat(10),
                CoercionId::NumericWiden,
            )),
            Value::Bool(true),
        ),
        (
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Lt,
                Value::Text("z".to_string()),
                CoercionId::Strict,
            )),
            Value::Bool(true),
        ),
    ];

    for ((predicate, expected), projected) in predicate_cases.into_iter().zip(projection_row) {
        let program = PredicateProgram::compile_for_model_only(SessionSqlEntity::MODEL, &predicate);
        let mut read_slot = |slot| {
            values
                .iter()
                .find_map(|(candidate, value)| (*candidate == slot).then_some(value))
        };

        assert_eq!(projected, &expected);
        assert_eq!(
            Value::Bool(program.eval_with_slot_value_ref_reader(&mut read_slot)),
            *projected,
            "predicate and SQL projection comparison should match for shared supported cases",
        );
    }
}

#[test]
fn predicate_documents_unsupported_ne_projection_drift() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("cmp-drift", 20)]);

    let projection_result = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name != age FROM SessionSqlEntity WHERE name = 'cmp-drift'",
    );
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::Ne,
        Value::Nat(20),
        CoercionId::Strict,
    ));
    let program = PredicateProgram::compile_for_model_only(SessionSqlEntity::MODEL, &predicate);
    let values = [(1_usize, Value::Text("cmp-drift".to_string()))];
    let mut read_slot = |slot| {
        values
            .iter()
            .find_map(|(candidate, value)| (*candidate == slot).then_some(value))
    };

    assert!(
        projection_result.is_err(),
        "projection validation currently rejects nonnumeric cross-variant != before execution",
    );
    assert!(
        !program.eval_with_slot_value_ref_reader(&mut read_slot),
        "direct predicate strict != currently treats unsupported cross-variant comparison as false",
    );
}
