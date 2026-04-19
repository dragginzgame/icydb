use super::*;
use crate::{db::session::sql::PreparedSqlParameterTypeFamily, value::Value};

#[test]
fn prepare_sql_query_collects_where_compare_contract_and_executes_bound_values() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity WHERE age > ? ORDER BY age ASC",
        )
        .expect("prepared SQL WHERE compare should prepare");

    assert_eq!(
        prepared.parameter_count(),
        1,
        "one WHERE placeholder should freeze one parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "age compare should freeze one numeric parameter contract",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(&prepared, &[Value::Uint(15)])
        .expect("prepared SQL execution should bind one numeric threshold");

    let crate::db::session::sql::SqlStatementResult::Projection { rows, .. } = result else {
        panic!("prepared SQL scalar SELECT should emit one projection payload");
    };

    assert_eq!(
        rows,
        vec![
            vec![Value::Text("Bea".to_string())],
            vec![Value::Text("Cid".to_string())],
        ],
        "bound WHERE threshold should reuse the prepared shape while filtering with the supplied runtime value",
    );
}

#[test]
fn prepare_sql_query_collects_having_compare_contract_and_executes_bound_values() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Ada", 11), ("Bea", 20)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM SessionSqlEntity \
             GROUP BY name \
             HAVING COUNT(*) > ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared SQL HAVING compare should prepare");

    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "COUNT(*) HAVING compare should freeze one numeric parameter contract",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(&prepared, &[Value::Uint(1)])
        .expect("prepared SQL grouped execution should bind one HAVING threshold");

    let crate::db::session::sql::SqlStatementResult::Grouped { rows, .. } = result else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        rows.len(),
        1,
        "only one grouped row should survive HAVING COUNT(*) > 1"
    );
    assert_eq!(
        rows[0].group_key(),
        &[Value::Text("Ada".to_string())],
        "the duplicated name group should stay present after binding the HAVING threshold",
    );
    assert_eq!(
        rows[0].aggregate_values(),
        &[Value::Uint(2)],
        "the prepared HAVING threshold should keep the duplicated name group only",
    );
}

#[test]
fn prepare_sql_query_rejects_parameterized_projection_positions() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT age + ? FROM SessionSqlEntity ORDER BY age ASC",
        )
        .expect_err("projection parameters should stay out of the initial 0.98 v1 surface");

    assert!(
        err.to_string()
            .contains("parameterized SELECT projection is not supported"),
        "projection parameter rejection should explain the unsupported v1 placement: {err}",
    );
}

#[test]
fn execute_prepared_sql_query_rejects_type_mismatched_bindings() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20)]);
    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity WHERE age > ? ORDER BY age ASC",
        )
        .expect("prepared SQL WHERE compare should prepare");

    let err = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Text("not-a-number".to_string())],
        )
        .expect_err("numeric WHERE parameter contract should reject text bindings");

    assert!(
        err.to_string()
            .contains("does not match the required Numeric contract"),
        "bind validation should fail before execution with the frozen numeric contract: {err}",
    );
}

#[test]
fn prepare_sql_query_rejects_non_field_compare_parameter_positions() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity WHERE LOWER(name) = ? ORDER BY age ASC",
        )
        .expect_err(
            "function-backed compare predicates should stay outside the initial 0.98 v1 surface",
        );

    assert!(
        err.to_string().contains(
            "only field-compare and aggregate-compare WHERE parameter positions are supported"
        ),
        "compare-position rejection should explain the admitted v1 surface boundary: {err}",
    );
}

#[test]
fn execute_prepared_sql_query_does_not_alias_raw_sql_compiled_cache_across_bindings() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity WHERE age > ? ORDER BY age ASC",
        )
        .expect("prepared SQL WHERE compare should prepare");

    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "prepared SQL v1 should not populate the raw SQL compiled-command cache during prepare",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "prepared SQL v1 should not populate the structural query-plan cache before execution",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlEntity>(&prepared, &[Value::Uint(15)])
        .expect("first prepared execution should bind the lower threshold");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        first_rows,
        vec![
            vec![Value::Text("Bea".to_string())],
            vec![Value::Text("Cid".to_string())],
        ],
        "the first prepared execution should honor the first bound threshold",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "prepared SQL v1 should continue bypassing the raw SQL compiled-command cache after execution",
    );
    assert!(
        session.query_plan_cache_len() >= 1,
        "the first prepared execution should still lower onto the shared structural planning boundary",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlEntity>(&prepared, &[Value::Uint(25)])
        .expect("second prepared execution should bind a different threshold");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        second_rows,
        vec![vec![Value::Text("Cid".to_string())]],
        "the second prepared execution should reflect the new bound threshold instead of reusing the old literal",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "prepared SQL v1 should still bypass the raw SQL compiled-command cache on repeat execution",
    );
}

#[test]
fn execute_prepared_sql_query_allows_null_bindings_in_compare_positions() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity WHERE age > ? ORDER BY age ASC",
        )
        .expect("prepared SQL WHERE compare should prepare");

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(&prepared, &[Value::Null])
        .expect("NULL compare bindings should preserve ordinary SQL evaluation semantics");
    let crate::db::session::sql::SqlStatementResult::Projection { rows, .. } = result else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert!(
        rows.is_empty(),
        "comparing against NULL should not admit any rows through the ordinary SQL boolean boundary",
    );
}
