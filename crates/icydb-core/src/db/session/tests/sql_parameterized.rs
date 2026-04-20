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
fn execute_prepared_sql_query_templates_min_aggregate_compare_contracts() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Ada", 11), ("Bea", 20)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name, MIN(age) AS youngest \
             FROM SessionSqlEntity \
             GROUP BY name \
             HAVING MIN(age) > ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared SQL MIN(age) HAVING compare should prepare");

    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "MIN(age) HAVING compare should freeze one numeric parameter contract",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "prepared SQL template-capable MIN(age) HAVING shapes should not touch the shared structural query-plan cache during prepare",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(&prepared, &[Value::Uint(10)])
        .expect("prepared SQL grouped MIN(age) execution should bind one HAVING threshold");

    let crate::db::session::sql::SqlStatementResult::Grouped { rows, .. } = result else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        rows.len(),
        1,
        "only one grouped row should survive HAVING MIN(age) > 10",
    );
    assert_eq!(
        rows[0].group_key(),
        &[Value::Text("Bea".to_string())],
        "the group with the larger minimum age should survive the bound HAVING threshold",
    );
    assert_eq!(
        rows[0].aggregate_values(),
        &[Value::Uint(20)],
        "the grouped MIN(age) value should stay visible after prepared execution binding",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-backed MIN(age) prepared execution should stay off the shared structural query-plan cache",
    );
}

#[test]
fn execute_prepared_sql_query_templates_text_min_aggregate_compare_contracts() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Ada", 11), ("Bea", 20)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name, MIN(name) AS first_name \
             FROM SessionSqlEntity \
             GROUP BY name \
             HAVING MIN(name) > ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared SQL MIN(name) HAVING compare should prepare");

    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "MIN(name) HAVING compare should freeze one text parameter contract",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "prepared SQL template-capable MIN(name) HAVING shapes should not touch the shared structural query-plan cache during prepare",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Text("Ada".to_string())],
        )
        .expect("prepared SQL grouped MIN(name) execution should bind one HAVING threshold");

    let crate::db::session::sql::SqlStatementResult::Grouped { rows, .. } = result else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        rows.len(),
        1,
        "only one grouped row should survive HAVING MIN(name) > 'Ada'",
    );
    assert_eq!(
        rows[0].group_key(),
        &[Value::Text("Bea".to_string())],
        "the lexically later grouped name should survive the bound MIN(name) threshold",
    );
    assert_eq!(
        rows[0].aggregate_values(),
        &[Value::Text("Bea".to_string())],
        "the grouped MIN(name) value should stay visible after prepared execution binding",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-backed MIN(name) prepared execution should stay off the shared structural query-plan cache",
    );
}

#[test]
fn execute_prepared_sql_query_templates_text_max_aggregate_compare_contracts() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 10), ("Cid", 20)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT age, MAX(name) AS last_name \
             FROM SessionSqlEntity \
             GROUP BY age \
             HAVING MAX(name) < ? \
             ORDER BY age ASC \
             LIMIT 10",
        )
        .expect("prepared SQL MAX(name) HAVING compare should prepare");

    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "MAX(name) HAVING compare should freeze one text parameter contract",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "prepared SQL template-capable MAX(name) HAVING shapes should not touch the shared structural query-plan cache during prepare",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Text("Cid".to_string())],
        )
        .expect("prepared SQL grouped MAX(name) execution should bind one HAVING threshold");

    let crate::db::session::sql::SqlStatementResult::Grouped { rows, .. } = result else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        rows.len(),
        1,
        "only one grouped row should survive HAVING MAX(name) < 'Cid'",
    );
    assert_eq!(
        rows[0].group_key(),
        &[Value::Uint(10)],
        "the earlier age bucket should survive when its lexically greatest name stays below the bound",
    );
    assert_eq!(
        rows[0].aggregate_values(),
        &[Value::Text("Bea".to_string())],
        "the grouped MAX(name) value should stay visible after prepared execution binding",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-backed MAX(name) prepared execution should stay off the shared structural query-plan cache",
    );
}

#[test]
fn execute_prepared_sql_query_templates_max_aggregate_compare_contracts() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Ada", 11), ("Bea", 20)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name, MAX(age) AS oldest \
             FROM SessionSqlEntity \
             GROUP BY name \
             HAVING MAX(age) < ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared SQL MAX(age) HAVING compare should prepare");

    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "MAX(age) HAVING compare should freeze one numeric parameter contract",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "prepared SQL template-capable MAX(age) HAVING shapes should not touch the shared structural query-plan cache during prepare",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(&prepared, &[Value::Uint(20)])
        .expect("prepared SQL grouped MAX(age) execution should bind one HAVING threshold");

    let crate::db::session::sql::SqlStatementResult::Grouped { rows, .. } = result else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        rows.len(),
        1,
        "only one grouped row should survive HAVING MAX(age) < 20",
    );
    assert_eq!(
        rows[0].group_key(),
        &[Value::Text("Ada".to_string())],
        "the group with the smaller maximum age should survive the bound HAVING threshold",
    );
    assert_eq!(
        rows[0].aggregate_values(),
        &[Value::Uint(11)],
        "the grouped MAX(age) value should stay visible after prepared execution binding",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-backed MAX(age) prepared execution should stay off the shared structural query-plan cache",
    );
}

#[test]
fn execute_prepared_sql_query_templates_grouped_mixed_having_compare_contracts() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Ada", 11), ("Bea", 20)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM SessionSqlEntity \
             GROUP BY name \
             HAVING COUNT(*) > ? AND name > ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared SQL grouped mixed HAVING compare contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "mixed grouped HAVING compare shapes should freeze two parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "COUNT(*) HAVING compare should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "group-field HAVING compare should freeze one text parameter contract",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-capable grouped mixed HAVING shapes should not touch the shared structural query-plan cache during prepare",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(0), Value::Text("Ada".to_string())],
        )
        .expect("grouped mixed prepared execution should bind both HAVING thresholds");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        first_rows.len(),
        1,
        "the first mixed grouped execution should keep one row after both HAVING thresholds",
    );
    assert_eq!(
        first_rows[0].group_key(),
        &[Value::Text("Bea".to_string())],
        "the text HAVING threshold should keep only the lexically later grouped key",
    );
    assert_eq!(
        first_rows[0].aggregate_values(),
        &[Value::Uint(1)],
        "the grouped aggregate should stay visible after the mixed HAVING bind",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "mixed grouped template-backed execution should stay off the shared structural query-plan cache",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(1), Value::Text("A".to_string())],
        )
        .expect("repeat grouped mixed prepared execution should bind different HAVING thresholds");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        second_rows.len(),
        1,
        "the second mixed grouped execution should still keep one grouped row",
    );
    assert_eq!(
        second_rows[0].group_key(),
        &[Value::Text("Ada".to_string())],
        "the numeric HAVING threshold should now keep the duplicated grouped key instead",
    );
    assert_eq!(
        second_rows[0].aggregate_values(),
        &[Value::Uint(2)],
        "the repeated grouped execution should reflect the newly bound aggregate threshold",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "repeat mixed grouped template-backed execution should keep the shared structural query-plan cache untouched",
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
        "prepared SQL 0.98.1 templates should still bypass the shared structural query-plan cache before execution",
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
        "prepared SQL 0.98.1 should continue bypassing the raw SQL compiled-command cache after execution",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "prepared SQL 0.98.1 template execution should not populate the shared structural query-plan cache",
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
        "prepared SQL 0.98.1 should still bypass the raw SQL compiled-command cache on repeat execution",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "repeat prepared SQL template execution should keep the shared structural query-plan cache untouched",
    );
}

#[test]
fn execute_prepared_sql_query_templates_mixed_numeric_and_text_compare_contracts() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE age > ? AND name > ? \
             ORDER BY age ASC",
        )
        .expect("prepared SQL mixed WHERE compare contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "mixed numeric/text WHERE compare shapes should freeze two parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "age compare should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "name compare should freeze one text parameter contract",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-capable mixed numeric/text prepared shapes should not touch the shared structural query-plan cache during prepare",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(15), Value::Text("B".to_string())],
        )
        .expect("mixed prepared SQL execution should bind both compare thresholds");
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
        "the first mixed execution should honor both the numeric and text bound thresholds",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "mixed template-backed execution should stay off the shared structural query-plan cache",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(15), Value::Text("C".to_string())],
        )
        .expect("repeat mixed prepared SQL execution should bind the second text threshold");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        second_rows,
        vec![vec![Value::Text("Cid".to_string())]],
        "the second mixed execution should reflect the new text threshold without leaving the template lane",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "repeat mixed template-backed execution should keep the shared structural query-plan cache untouched",
    );
}

#[test]
fn execute_prepared_sql_query_falls_back_when_template_text_sentinel_collides_with_user_literal() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE age > ? \
               AND (name = '__icydb_prepared_param_text_1__' OR name > ?) \
             ORDER BY age ASC",
        )
        .expect(
            "prepared SQL mixed compare shape with one sentinel collision literal should prepare",
        );

    assert_eq!(
        prepared.parameter_count(),
        2,
        "the mixed collision shape should still freeze two parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "the second compare slot should still freeze one text parameter contract",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(15), Value::Text("Bea".to_string())],
        )
        .expect("collision-guarded prepared execution should still bind both thresholds");
    let crate::db::session::sql::SqlStatementResult::Projection { rows, .. } = result else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        rows,
        vec![vec![Value::Text("Cid".to_string())]],
        "the user-authored sentinel literal must stay intact instead of being rebound to the runtime text parameter",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "collision fallback should still bypass the raw SQL compiled-command cache",
    );
}

#[test]
fn execute_prepared_sql_query_falls_back_when_template_numeric_sentinel_collides_with_user_literal()
{
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE age >= ? \
               AND (age = 18446744073709551615 OR name > ?) \
             ORDER BY age ASC",
        )
        .expect("prepared SQL mixed compare shape with one numeric sentinel collision literal should prepare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "the mixed numeric collision shape should still freeze two parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "the first compare slot should still freeze one numeric parameter contract",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(20), Value::Text("Cid".to_string())],
        )
        .expect("collision-guarded prepared execution should still bind both thresholds");
    let crate::db::session::sql::SqlStatementResult::Projection { rows, .. } = result else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert!(
        rows.is_empty(),
        "the user-authored numeric sentinel literal must stay intact instead of being rebound to the runtime numeric parameter",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "numeric collision fallback should still bypass the raw SQL compiled-command cache",
    );
}

#[test]
fn execute_prepared_sql_query_bool_compare_contracts_preserve_fallback_results() {
    reset_session_sql_store();
    let session = sql_session();

    for (label, active, archived) in [
        ("bool-a", true, false),
        ("bool-b", false, false),
        ("bool-c", true, true),
    ] {
        session
            .insert(SessionSqlBoolCompareEntity {
                id: Ulid::generate(),
                label: label.to_string(),
                active,
                archived,
            })
            .expect("bool compare fixture insert should succeed");
    }

    let prepared = session
        .prepare_sql_query::<SessionSqlBoolCompareEntity>(
            "SELECT label \
             FROM SessionSqlBoolCompareEntity \
             WHERE active = ? \
             ORDER BY label ASC",
        )
        .expect("prepared SQL bool WHERE compare should prepare");

    assert_eq!(
        prepared.parameter_count(),
        1,
        "bool WHERE compare shapes should freeze one parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Bool,
        "bool WHERE compare should freeze one bool parameter contract",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "bool prepared shapes should not touch the shared structural query-plan cache during prepare",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlBoolCompareEntity>(&prepared, &[Value::Bool(true)])
        .expect("bool prepared execution should bind the true threshold");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        first_rows,
        vec![
            vec![Value::Text("bool-a".to_string())],
            vec![Value::Text("bool-c".to_string())],
        ],
        "the first bool prepared execution should honor the true threshold",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "bool prepared execution should still bypass the raw SQL compiled-command cache on the fallback path",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlBoolCompareEntity>(&prepared, &[Value::Bool(false)])
        .expect("bool prepared execution should bind the false threshold");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        second_rows,
        vec![vec![Value::Text("bool-b".to_string())]],
        "the second bool prepared execution should reflect the false threshold instead of reusing the first binding",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "repeat bool prepared execution should keep bypassing the raw SQL compiled-command cache",
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

#[test]
fn execute_prepared_sql_query_grouped_null_binding_preserves_having_semantics() {
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
        .expect("prepared SQL grouped HAVING compare should prepare");

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(&prepared, &[Value::Null])
        .expect("NULL grouped HAVING bindings should preserve ordinary SQL evaluation semantics");
    let crate::db::session::sql::SqlStatementResult::Grouped { rows, .. } = result else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert!(
        rows.is_empty(),
        "HAVING COUNT(*) > NULL should not admit any grouped rows through the ordinary SQL boolean boundary",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "prepared SQL grouped NULL fallback should still bypass the raw SQL compiled-command cache",
    );
}

#[test]
fn execute_prepared_sql_query_grouped_falls_back_when_template_text_sentinel_collides_with_user_literal()
 {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Ada", 11), ("Bea", 20)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM SessionSqlEntity \
             GROUP BY name \
             HAVING COUNT(*) > ? \
               AND (name = '__icydb_prepared_param_text_1__' OR name > ?) \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared SQL grouped mixed shape with one text sentinel collision literal should prepare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "the grouped collision shape should still freeze two parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "the second grouped HAVING slot should still freeze one text parameter contract",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(0), Value::Text("Ada".to_string())],
        )
        .expect("grouped collision-guarded prepared execution should still bind both thresholds");
    let crate::db::session::sql::SqlStatementResult::Grouped { rows, .. } = result else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        rows.len(),
        1,
        "the grouped collision fallback should still keep only one row",
    );
    assert_eq!(
        rows[0].group_key(),
        &[Value::Text("Bea".to_string())],
        "the user-authored sentinel literal must stay intact instead of being rebound inside grouped HAVING",
    );
    assert_eq!(
        rows[0].aggregate_values(),
        &[Value::Uint(1)],
        "the grouped collision fallback should preserve the correct aggregate payload",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "grouped collision fallback should still bypass the raw SQL compiled-command cache",
    );
}
