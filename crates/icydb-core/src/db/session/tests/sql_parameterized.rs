use super::*;
use crate::{
    db::session::sql::{PreparedSqlExecutionTemplateKind, PreparedSqlParameterTypeFamily},
    value::Value,
};

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
#[expect(
    clippy::too_many_lines,
    reason = "grouped prepared SQL lane coverage keeps both executions and cache assertions in one contract test"
)]
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
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicGrouped),
        "grouped compare-family HAVING queries should move onto the symbolic grouped template lane in 0.99",
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
fn prepare_sql_query_expression_owned_where_parameters_fall_back_outside_template_lanes() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE COALESCE(NULLIF(age, ?), 99) = ? \
             ORDER BY age ASC",
        )
        .expect(
            "expression-owned WHERE parameter positions should still prepare for bound-SQL fallback",
        );

    assert_eq!(
        prepared.parameter_count(),
        2,
        "the expression-owned WHERE shape should still freeze both parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "NULLIF(age, ?) should still infer a numeric contract from the surrounding field-owned expression",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "the outer compare target should still infer a numeric right-hand contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        None,
        "general expression-owned WHERE semantics must stay off the prepared template lanes",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(20), Value::Uint(99)],
        )
        .expect(
            "prepared fallback execution should bind the first expression-owned WHERE thresholds",
        );
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        first_rows,
        vec![vec![Value::Text("Bea".to_string())]],
        "the first fallback execution should honor the parameterized COALESCE/NULLIF WHERE semantics",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlEntity>(&prepared, &[Value::Uint(10), Value::Uint(99)])
        .expect("prepared fallback execution should bind a second expression-owned WHERE threshold pair");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        second_rows,
        vec![vec![Value::Text("Ada".to_string())]],
        "repeat fallback execution should re-evaluate the expression-owned WHERE semantics with the new bindings",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "expression-owned prepared fallback should still bypass the raw SQL compiled-command cache",
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
fn prepare_sql_query_mixed_predicate_owned_and_expression_owned_where_contracts_stay_fallback_only()
{
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE name = ? AND COALESCE(age, ?) > 10 \
             ORDER BY age ASC",
        )
        .expect("mixed predicate-owned and expression-owned WHERE contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "the mixed WHERE shape should still freeze both parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "the direct compare slot should keep its text-family contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "the expression-owned COALESCE slot should derive a numeric-family contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        None,
        "mixed WHERE shapes with expression-owned semantics must stay off prepared template lanes",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Text("Bea".to_string()), Value::Uint(0)],
        )
        .expect(
            "mixed prepared fallback should bind both predicate-owned and expression-owned slots",
        );
    let crate::db::session::sql::SqlStatementResult::Projection { rows, .. } = result else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        rows,
        vec![vec![Value::Text("Bea".to_string())]],
        "the mixed fallback query should preserve both direct-compare and expression-owned WHERE semantics",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "mixed expression-owned prepared fallback should still bypass the raw SQL compiled-command cache",
    );
}

#[test]
fn prepare_sql_query_text_expression_owned_where_contracts_derive_from_shared_family_rules() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE LOWER(TRIM(COALESCE(name, ?))) = ? \
             ORDER BY age ASC",
        )
        .expect("text expression-owned WHERE contracts should prepare through fallback");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "the text expression-owned WHERE shape should still freeze both parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "COALESCE(name, ?) should derive a text-family fallback contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "the outer LOWER/TRIM compare target should keep one text-family contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        None,
        "text expression-owned WHERE semantics must stay off prepared template lanes",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[
                Value::Text("unused".to_string()),
                Value::Text("bea".to_string()),
            ],
        )
        .expect("text prepared fallback should bind both text-family thresholds");
    let crate::db::session::sql::SqlStatementResult::Projection { rows, .. } = result else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        rows,
        vec![vec![Value::Text("Bea".to_string())]],
        "the text fallback query should preserve LOWER/TRIM/COALESCE WHERE semantics",
    );
}

#[test]
fn prepare_sql_query_boolean_wrapper_expression_owned_where_contracts_stay_fallback_only() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE STARTS_WITH(COALESCE(name, ?), ?) \
             ORDER BY age ASC",
        )
        .expect("boolean-wrapper expression-owned WHERE contracts should prepare through fallback");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "the boolean-wrapper expression-owned WHERE shape should still freeze both parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "COALESCE(name, ?) should still derive a text-family fallback contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "the STARTS_WITH prefix slot should keep one text-family contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        None,
        "boolean-wrapper expression-owned WHERE semantics must stay off prepared template lanes",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[
                Value::Text("unused".to_string()),
                Value::Text("B".to_string()),
            ],
        )
        .expect("boolean-wrapper prepared fallback should bind both text-family slots");
    let crate::db::session::sql::SqlStatementResult::Projection { rows, .. } = result else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        rows,
        vec![vec![Value::Text("Bea".to_string())]],
        "the boolean-wrapper fallback query should preserve STARTS_WITH(COALESCE(...), ...) semantics",
    );
}

#[test]
fn prepare_sql_query_all_param_coalesce_where_contracts_inherit_outer_compare_family() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE COALESCE(?, ?) = name \
             ORDER BY age ASC",
        )
        .expect("all-parameter COALESCE compare should inherit its text contract from the outer compare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "the all-parameter COALESCE compare should still freeze both fallback contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "the first COALESCE slot should inherit the outer text compare family",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "the second COALESCE slot should inherit the outer text compare family",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        None,
        "all-parameter COALESCE compare semantics must stay off prepared template lanes",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Null, Value::Text("Bea".to_string())],
        )
        .expect("fallback execution should bind all-parameter COALESCE compare slots");
    let crate::db::session::sql::SqlStatementResult::Projection { rows, .. } = result else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        rows,
        vec![vec![Value::Text("Bea".to_string())]],
        "the all-parameter COALESCE compare should preserve outer compare semantics under fallback execution",
    );
}

#[test]
fn prepare_sql_query_fixed_result_function_where_rejects_incompatible_outer_compare_family() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE ABS(?) = name \
             ORDER BY age ASC",
        )
        .expect_err("fixed-result numeric function compare should fail closed against one text outer contract");
}

#[test]
fn prepare_sql_query_all_param_nullif_where_contracts_inherit_outer_compare_family() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE NULLIF(?, ?) = name \
             ORDER BY age ASC",
        )
        .expect(
            "all-parameter NULLIF compare should inherit its text contract from the outer compare",
        );

    assert_eq!(
        prepared.parameter_count(),
        2,
        "the all-parameter NULLIF compare should still freeze both fallback contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "the first NULLIF slot should inherit the outer text compare family",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "the second NULLIF slot should inherit the outer text compare family",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        None,
        "all-parameter NULLIF compare semantics must stay off prepared template lanes",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[
                Value::Text("unused".to_string()),
                Value::Text("unused".to_string()),
            ],
        )
        .expect("fallback execution should bind all-parameter NULLIF compare slots");
    let crate::db::session::sql::SqlStatementResult::Projection { rows, .. } = result else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert!(
        rows.is_empty(),
        "the all-parameter NULLIF compare should preserve outer compare semantics under fallback execution",
    );
}

#[test]
fn prepare_sql_query_case_result_where_contracts_inherit_outer_compare_family() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE CASE WHEN name = 'Ada' THEN ? ELSE ? END = name \
             ORDER BY age ASC",
        )
        .expect(
            "searched CASE compare should inherit its text result contract from the outer compare",
        );

    assert_eq!(
        prepared.parameter_count(),
        2,
        "the searched CASE compare should still freeze both fallback result contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "the CASE then-branch slot should inherit the outer text compare family",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "the CASE else-branch slot should inherit the outer text compare family",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        None,
        "searched CASE compare semantics must stay off prepared template lanes",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[
                Value::Text("Ada".to_string()),
                Value::Text("unused".to_string()),
            ],
        )
        .expect("fallback execution should bind searched CASE compare result slots");
    let crate::db::session::sql::SqlStatementResult::Projection { rows, .. } = result else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        rows,
        vec![vec![Value::Text("Ada".to_string())]],
        "the searched CASE compare should preserve outer compare semantics under fallback execution",
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
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicScalar),
        "simple scalar mixed compare queries should move onto the symbolic scalar template lane in 0.99",
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
fn execute_prepared_sql_query_templates_indexed_scalar_compare_contracts() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<IndexedSessionSqlEntity>(
            "SELECT name \
             FROM IndexedSessionSqlEntity \
             WHERE name = ? \
             ORDER BY id ASC \
             LIMIT 1",
        )
        .expect("prepared indexed SQL compare contract should prepare");

    assert_eq!(
        prepared.parameter_count(),
        1,
        "indexed scalar compare shape should freeze one parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "indexed text compare should freeze one text parameter contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicScalar),
        "indexed scalar compare queries should stay on the symbolic scalar lane after the first access-template slice",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-capable indexed scalar shapes should not touch the shared structural query-plan cache during prepare",
    );

    let first = session
        .execute_prepared_sql_query::<IndexedSessionSqlEntity>(
            &prepared,
            &[Value::Text("Ada".to_string())],
        )
        .expect("indexed prepared execution should bind the first text lookup");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared indexed SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        first_rows,
        vec![vec![Value::Text("Ada".to_string())]],
        "the first indexed prepared execution should honor the first bound lookup value",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "indexed symbolic template execution should stay off the shared structural query-plan cache",
    );

    let second = session
        .execute_prepared_sql_query::<IndexedSessionSqlEntity>(
            &prepared,
            &[Value::Text("Cid".to_string())],
        )
        .expect("indexed prepared execution should bind the second text lookup");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared indexed SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        second_rows,
        vec![vec![Value::Text("Cid".to_string())]],
        "the second indexed prepared execution should reflect the new bound lookup value instead of reusing the first access payload",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "repeat indexed symbolic template execution should keep the shared structural query-plan cache untouched",
    );
}

#[test]
fn execute_prepared_sql_query_indexed_range_contracts_preserve_fallback_results() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("Ada", 10), ("Bea", 20), ("Cid", 30), ("Dora", 40)],
    );

    let prepared = session
        .prepare_sql_query::<IndexedSessionSqlEntity>(
            "SELECT name \
             FROM IndexedSessionSqlEntity \
             WHERE name >= ? AND name < ? \
             ORDER BY name ASC, id ASC \
             LIMIT 2",
        )
        .expect("prepared indexed range SQL compare contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "indexed range shape should freeze two parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "indexed range lower bound should freeze one text parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "indexed range upper bound should freeze one text parameter contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::Legacy),
        "indexed range prepared queries should stay on the legacy lane until secondary range access payloads move onto symbolic slot ownership",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-capable indexed range shapes should not touch the shared structural query-plan cache during prepare",
    );

    let first = session
        .execute_prepared_sql_query::<IndexedSessionSqlEntity>(
            &prepared,
            &[Value::Text("A".to_string()), Value::Text("D".to_string())],
        )
        .expect("indexed range prepared execution should bind the first text range");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared indexed range SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        first_rows,
        vec![
            vec![Value::Text("Ada".to_string())],
            vec![Value::Text("Bea".to_string())],
        ],
        "the first indexed range prepared execution should still honor both bound range endpoints on the fallback lane",
    );

    let second = session
        .execute_prepared_sql_query::<IndexedSessionSqlEntity>(
            &prepared,
            &[Value::Text("B".to_string()), Value::Text("E".to_string())],
        )
        .expect("indexed range prepared execution should bind the second text range");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared indexed range SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        second_rows,
        vec![
            vec![Value::Text("Bea".to_string())],
            vec![Value::Text("Cid".to_string())],
        ],
        "the second indexed range prepared execution should reflect the new bound range endpoints on the fallback lane too",
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "grouped prepared secondary-range fallback coverage keeps both executions and lane assertions in one contract test"
)]
fn execute_prepared_sql_query_grouped_indexed_range_contracts_preserve_fallback_results() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("Ada", 10), ("Ada", 20), ("Bea", 30), ("Cid", 40)],
    );

    let prepared = session
        .prepare_sql_query::<IndexedSessionSqlEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM IndexedSessionSqlEntity \
             WHERE name >= ? AND name < ? \
             GROUP BY name \
             HAVING COUNT(*) > ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared grouped indexed range SQL compare contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        3,
        "grouped indexed range shape should freeze three parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "grouped indexed range lower bound should freeze one text parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "grouped indexed range upper bound should freeze one text parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[2].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "grouped indexed range HAVING threshold should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::Legacy),
        "grouped indexed range prepared queries should stay on the legacy lane until secondary range access payloads move onto symbolic slot ownership",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-capable grouped indexed range shapes should not touch the shared structural query-plan cache during prepare",
    );

    let first = session
        .execute_prepared_sql_query::<IndexedSessionSqlEntity>(
            &prepared,
            &[
                Value::Text("A".to_string()),
                Value::Text("C".to_string()),
                Value::Uint(0),
            ],
        )
        .expect("grouped indexed range prepared execution should bind the first thresholds");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared grouped indexed range SQL should emit grouped rows");
    };

    assert_eq!(
        first_rows
            .iter()
            .map(|row| row.group_key().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Text("Ada".to_string())],
            vec![Value::Text("Bea".to_string())],
        ],
        "the first grouped indexed range prepared execution should honor both bound range endpoints on the fallback lane",
    );

    let second = session
        .execute_prepared_sql_query::<IndexedSessionSqlEntity>(
            &prepared,
            &[
                Value::Text("B".to_string()),
                Value::Text("D".to_string()),
                Value::Uint(0),
            ],
        )
        .expect("grouped indexed range prepared execution should bind the second thresholds");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared grouped indexed range SQL should emit grouped rows");
    };

    assert_eq!(
        second_rows
            .iter()
            .map(|row| row.group_key().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Text("Bea".to_string())],
            vec![Value::Text("Cid".to_string())],
        ],
        "the second grouped indexed range prepared execution should reflect the rebound range endpoints on the fallback lane too",
    );
}

#[test]
fn execute_prepared_sql_query_templates_primary_key_lookup_contracts() {
    reset_session_sql_store();
    let session = sql_session();

    for (id, name) in [(10_u64, "Ada"), (20_u64, "Bea"), (30_u64, "Cid")] {
        session
            .insert(SessionSqlManagedWriteEntity {
                id,
                name: name.to_string(),
                created_at: Timestamp::from_millis(id.cast_signed()),
                updated_at: Timestamp::from_millis(id.cast_signed()),
            })
            .expect("managed-write fixture insert should succeed");
    }

    let prepared = session
        .prepare_sql_query::<SessionSqlManagedWriteEntity>(
            "SELECT name \
             FROM SessionSqlManagedWriteEntity \
             WHERE id = ? \
             ORDER BY id ASC \
             LIMIT 1",
        )
        .expect("prepared primary-key SQL compare contract should prepare");

    assert_eq!(
        prepared.parameter_count(),
        1,
        "primary-key lookup shape should freeze one parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "primary-key lookup should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicScalar),
        "primary-key lookup queries should stay on the symbolic scalar lane after the access-template follow-through",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-capable primary-key lookup shapes should not touch the shared structural query-plan cache during prepare",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlManagedWriteEntity>(&prepared, &[Value::Uint(10)])
        .expect("primary-key prepared execution should bind the first lookup id");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared primary-key SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        first_rows,
        vec![vec![Value::Text("Ada".to_string())]],
        "the first primary-key prepared execution should honor the first bound lookup id",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlManagedWriteEntity>(&prepared, &[Value::Uint(30)])
        .expect("primary-key prepared execution should bind the second lookup id");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared primary-key SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        second_rows,
        vec![vec![Value::Text("Cid".to_string())]],
        "the second primary-key prepared execution should reflect the new bound lookup id without falling back to sentinel access rebinding",
    );
}

#[test]
fn execute_prepared_sql_query_primary_key_range_contracts_on_write_entity() {
    reset_session_sql_store();
    let session = sql_session();

    for (id, name, age) in [
        (10_u64, "Ada", 10_u64),
        (20_u64, "Bea", 20_u64),
        (30_u64, "Cid", 30_u64),
        (40_u64, "Dora", 40_u64),
    ] {
        session
            .insert(SessionSqlWriteEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("write fixture insert should succeed");
    }

    let prepared = session
        .prepare_sql_query::<SessionSqlWriteEntity>(
            "SELECT name \
             FROM SessionSqlWriteEntity \
             WHERE id >= ? AND id < ? \
             ORDER BY id ASC \
             LIMIT 2",
        )
        .expect("prepared primary-key range SQL compare contract should prepare");

    let direct_rows = statement_projection_rows::<SessionSqlWriteEntity>(
        &session,
        "SELECT name \
         FROM SessionSqlWriteEntity \
         WHERE id >= 10 AND id < 35 \
         ORDER BY id ASC \
         LIMIT 2",
    )
    .expect("direct primary-key range SQL should execute");

    assert_eq!(
        direct_rows,
        vec![
            vec![Value::Text("Ada".to_string())],
            vec![Value::Text("Bea".to_string())],
        ],
        "the ordinary SQL path should prove the write-entity primary-key range semantics before the prepared path is compared against it",
    );

    assert_eq!(
        prepared.parameter_count(),
        2,
        "primary-key range shape should freeze two parameter contracts",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicScalar),
        "primary-key range queries should stay on the symbolic scalar lane once ordered exemplar bindings keep the frozen key-range access payload valid",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlWriteEntity>(
            &prepared,
            &[Value::Uint(10), Value::Uint(35)],
        )
        .expect("primary-key range prepared execution should bind the first range");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared primary-key range SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        first_rows,
        vec![
            vec![Value::Text("Ada".to_string())],
            vec![Value::Text("Bea".to_string())],
        ],
        "the first primary-key range prepared execution should honor both bound range endpoints",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlWriteEntity>(
            &prepared,
            &[Value::Uint(20), Value::Uint(50)],
        )
        .expect("primary-key range prepared execution should bind the second range");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared primary-key range SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        second_rows,
        vec![
            vec![Value::Text("Bea".to_string())],
            vec![Value::Text("Cid".to_string())],
        ],
        "the second primary-key range prepared execution should reflect the rebound range endpoints instead of reusing the first symbolic key-range payload",
    );
}

#[test]
fn execute_prepared_sql_query_templates_grouped_primary_key_lookup_contracts() {
    reset_session_sql_store();
    let session = sql_session();

    for (id, name) in [(10_u64, "Ada"), (20_u64, "Bea"), (30_u64, "Cid")] {
        session
            .insert(SessionSqlManagedWriteEntity {
                id,
                name: name.to_string(),
                created_at: Timestamp::from_millis(id.cast_signed()),
                updated_at: Timestamp::from_millis(id.cast_signed()),
            })
            .expect("managed-write fixture insert should succeed");
    }

    let prepared = session
        .prepare_sql_query::<SessionSqlManagedWriteEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM SessionSqlManagedWriteEntity \
             WHERE id = ? \
             GROUP BY name \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared grouped primary-key SQL compare contract should prepare");

    assert_eq!(
        prepared.parameter_count(),
        1,
        "grouped primary-key lookup shape should freeze one parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "grouped primary-key lookup should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicGrouped),
        "grouped primary-key lookup queries should stay on the symbolic grouped lane after the access-template follow-through",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-capable grouped primary-key lookup shapes should not touch the shared structural query-plan cache during prepare",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlManagedWriteEntity>(&prepared, &[Value::Uint(10)])
        .expect("grouped primary-key prepared execution should bind the first lookup id");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared grouped primary-key SQL should emit grouped rows");
    };

    assert_eq!(
        first_rows.len(),
        1,
        "the first grouped primary-key prepared execution should return one grouped row",
    );
    assert_eq!(
        first_rows[0].group_key(),
        &[Value::Text("Ada".to_string())],
        "the first grouped primary-key prepared execution should honor the first bound lookup id",
    );
    assert_eq!(
        first_rows[0].aggregate_values(),
        &[Value::Uint(1)],
        "the first grouped primary-key prepared execution should keep the grouped count for the rebound key lookup",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlManagedWriteEntity>(&prepared, &[Value::Uint(30)])
        .expect("grouped primary-key prepared execution should bind the second lookup id");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared grouped primary-key SQL should emit grouped rows");
    };

    assert_eq!(
        second_rows.len(),
        1,
        "the second grouped primary-key prepared execution should still return one grouped row",
    );
    assert_eq!(
        second_rows[0].group_key(),
        &[Value::Text("Cid".to_string())],
        "the second grouped primary-key prepared execution should reflect the rebound key lookup instead of reusing the first key payload",
    );
    assert_eq!(
        second_rows[0].aggregate_values(),
        &[Value::Uint(1)],
        "the second grouped primary-key prepared execution should keep the grouped count for the rebound key lookup",
    );
}

#[test]
fn execute_prepared_sql_query_templates_grouped_primary_key_range_contracts() {
    reset_session_sql_store();
    let session = sql_session();

    for (id, name, age) in [
        (10_u64, "Ada", 10_u64),
        (20_u64, "Bea", 20_u64),
        (30_u64, "Cid", 30_u64),
        (40_u64, "Dora", 40_u64),
    ] {
        session
            .insert(SessionSqlWriteEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("write-entity fixture insert should succeed");
    }

    let prepared = session
        .prepare_sql_query::<SessionSqlWriteEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM SessionSqlWriteEntity \
             WHERE id >= ? AND id < ? \
             GROUP BY name \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared grouped primary-key range SQL compare contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "grouped primary-key range shape should freeze two parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "grouped primary-key range lower bound should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "grouped primary-key range upper bound should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicGrouped),
        "grouped primary-key range queries should stay on the symbolic grouped lane once grouped access rebinding owns key ranges too",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlWriteEntity>(
            &prepared,
            &[Value::Uint(10), Value::Uint(35)],
        )
        .expect("grouped primary-key range prepared execution should bind the first bounds");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared grouped primary-key range SQL should emit grouped rows");
    };

    assert_eq!(
        first_rows
            .iter()
            .map(|row| row.group_key().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Text("Ada".to_string())],
            vec![Value::Text("Bea".to_string())],
            vec![Value::Text("Cid".to_string())],
        ],
        "the first grouped primary-key range execution should honor the first rebound key bounds",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlWriteEntity>(
            &prepared,
            &[Value::Uint(20), Value::Uint(50)],
        )
        .expect("grouped primary-key range prepared execution should bind the second bounds");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared grouped primary-key range SQL should emit grouped rows");
    };

    assert_eq!(
        second_rows
            .iter()
            .map(|row| row.group_key().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Text("Bea".to_string())],
            vec![Value::Text("Cid".to_string())],
            vec![Value::Text("Dora".to_string())],
        ],
        "the second grouped primary-key range execution should reflect the rebound key bounds instead of reusing the first range payload",
    );
}

#[test]
fn execute_prepared_sql_query_templates_grouped_primary_key_range_and_having_contracts() {
    reset_session_sql_store();
    let session = sql_session();

    for (id, name, age) in [
        (10_u64, "Ada", 10_u64),
        (20_u64, "Bea", 20_u64),
        (30_u64, "Cid", 30_u64),
        (40_u64, "Dora", 40_u64),
    ] {
        session
            .insert(SessionSqlWriteEntity {
                id,
                name: name.to_string(),
                age,
            })
            .expect("write-entity fixture insert should succeed");
    }

    let prepared = session
        .prepare_sql_query::<SessionSqlWriteEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM SessionSqlWriteEntity \
             WHERE id >= ? AND id < ? \
             GROUP BY name \
             HAVING COUNT(*) > ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared grouped primary-key range SQL WHERE+HAVING contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        3,
        "grouped primary-key range WHERE+HAVING shape should freeze three parameter contracts",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicGrouped),
        "grouped primary-key range WHERE+HAVING queries should stay on the symbolic grouped lane",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlWriteEntity>(
            &prepared,
            &[Value::Uint(10), Value::Uint(35), Value::Uint(0)],
        )
        .expect(
            "grouped primary-key range prepared execution should bind the first WHERE+HAVING thresholds",
        );
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared grouped primary-key range SQL should emit grouped rows");
    };

    assert_eq!(
        first_rows
            .iter()
            .map(|row| row.group_key().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Text("Ada".to_string())],
            vec![Value::Text("Bea".to_string())],
            vec![Value::Text("Cid".to_string())],
        ],
        "the first grouped primary-key range WHERE+HAVING execution should honor the first rebound key bounds",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlWriteEntity>(
            &prepared,
            &[Value::Uint(20), Value::Uint(50), Value::Uint(1)],
        )
        .expect(
            "grouped primary-key range prepared execution should bind the second WHERE+HAVING thresholds",
        );
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared grouped primary-key range SQL should emit grouped rows");
    };

    assert!(
        second_rows.is_empty(),
        "the rebound HAVING threshold should be able to eliminate every grouped row on the symbolic grouped range lane",
    );
}

#[test]
fn execute_prepared_sql_query_templates_grouped_primary_key_lookup_and_having_contracts() {
    reset_session_sql_store();
    let session = sql_session();

    for (id, name) in [(10_u64, "Ada"), (20_u64, "Bea"), (30_u64, "Cid")] {
        session
            .insert(SessionSqlManagedWriteEntity {
                id,
                name: name.to_string(),
                created_at: Timestamp::from_millis(id.cast_signed()),
                updated_at: Timestamp::from_millis(id.cast_signed()),
            })
            .expect("managed-write fixture insert should succeed");
    }

    let prepared = session
        .prepare_sql_query::<SessionSqlManagedWriteEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM SessionSqlManagedWriteEntity \
             WHERE id = ? \
             GROUP BY name \
             HAVING COUNT(*) > ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared grouped primary-key SQL WHERE+HAVING contract should prepare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "grouped primary-key WHERE+HAVING shape should freeze two parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "grouped primary-key WHERE compare should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "grouped primary-key HAVING compare should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicGrouped),
        "grouped primary-key WHERE+HAVING queries should stay on the symbolic grouped lane",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlManagedWriteEntity>(
            &prepared,
            &[Value::Uint(10), Value::Uint(0)],
        )
        .expect(
            "grouped primary-key prepared execution should bind the first WHERE+HAVING thresholds",
        );
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared grouped primary-key SQL should emit grouped rows");
    };

    assert_eq!(
        first_rows.len(),
        1,
        "the first grouped primary-key prepared execution should keep one grouped row",
    );
    assert_eq!(
        first_rows[0].group_key(),
        &[Value::Text("Ada".to_string())],
        "the first grouped primary-key prepared execution should honor the first rebound key lookup",
    );
    assert_eq!(
        first_rows[0].aggregate_values(),
        &[Value::Uint(1)],
        "the first grouped primary-key prepared execution should keep the grouped count after the rebound key lookup",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlManagedWriteEntity>(
            &prepared,
            &[Value::Uint(30), Value::Uint(1)],
        )
        .expect(
            "grouped primary-key prepared execution should bind the second WHERE+HAVING thresholds",
        );
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared grouped primary-key SQL should emit grouped rows");
    };

    assert_eq!(
        second_rows,
        Vec::new(),
        "the second grouped primary-key prepared execution should apply the rebound HAVING threshold on top of the rebound key lookup",
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn execute_prepared_sql_query_templates_grouped_bool_where_and_having_compare_contracts() {
    reset_session_sql_store();
    let session = sql_session();

    for (label, active, archived) in [
        ("bool-a", true, false),
        ("bool-a", true, true),
        ("bool-b", false, false),
    ] {
        session
            .insert(SessionSqlBoolCompareEntity {
                id: Ulid::generate(),
                label: label.to_string(),
                active,
                archived,
            })
            .expect("grouped bool compare fixture insert should succeed");
    }

    let prepared = session
        .prepare_sql_query::<SessionSqlBoolCompareEntity>(
            "SELECT label, COUNT(*) AS total_count \
             FROM SessionSqlBoolCompareEntity \
             WHERE active = ? \
             GROUP BY label \
             HAVING COUNT(*) > ? \
             ORDER BY label ASC \
             LIMIT 10",
        )
        .expect("prepared grouped bool WHERE+HAVING compare contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "grouped bool WHERE+HAVING compare shape should freeze two parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Bool,
        "grouped bool WHERE compare should freeze one bool parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "grouped HAVING compare should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicGrouped),
        "grouped bool WHERE+HAVING queries should stay on the symbolic grouped lane in 0.99",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-capable grouped bool WHERE+HAVING shapes should not touch the shared structural query-plan cache during prepare",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlBoolCompareEntity>(
            &prepared,
            &[Value::Bool(true), Value::Uint(1)],
        )
        .expect("grouped bool prepared execution should bind the first WHERE+HAVING thresholds");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared grouped bool SQL should emit grouped rows");
    };

    assert_eq!(
        first_rows.len(),
        1,
        "the first grouped bool prepared execution should keep the duplicated true group only",
    );
    assert_eq!(
        first_rows[0].group_key(),
        &[Value::Text("bool-a".to_string())],
        "the first grouped bool prepared execution should bind the bool WHERE threshold on the symbolic grouped lane",
    );
    assert_eq!(
        first_rows[0].aggregate_values(),
        &[Value::Uint(2)],
        "the first grouped bool prepared execution should keep the grouped count after rebound bool filtering",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlBoolCompareEntity>(
            &prepared,
            &[Value::Bool(false), Value::Uint(0)],
        )
        .expect("grouped bool prepared execution should bind the second WHERE+HAVING thresholds");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared grouped bool SQL should emit grouped rows");
    };

    assert_eq!(
        second_rows.len(),
        1,
        "the second grouped bool prepared execution should keep the rebound false group when the HAVING threshold allows it",
    );
    assert_eq!(
        second_rows[0].group_key(),
        &[Value::Text("bool-b".to_string())],
        "the second grouped bool prepared execution should reflect the new bool WHERE binding instead of reusing the first grouped filter",
    );
    assert_eq!(
        second_rows[0].aggregate_values(),
        &[Value::Uint(1)],
        "the second grouped bool prepared execution should recompute grouped counts from the rebound bool predicate",
    );
}

#[test]
fn execute_prepared_sql_query_templates_grouped_where_and_having_compare_contracts() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[("Ada", 10), ("Ada", 30), ("Bea", 20), ("Cid", 40)],
    );

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM SessionSqlEntity \
             WHERE age > ? \
             GROUP BY name \
             HAVING COUNT(*) > ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared SQL grouped WHERE+HAVING compare contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "grouped WHERE+HAVING compare shape should freeze two parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "grouped scalar WHERE compare should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "grouped HAVING compare should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicGrouped),
        "grouped compare-family WHERE+HAVING queries should stay on the symbolic grouped lane in 0.99",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-capable grouped WHERE+HAVING shapes should not touch the shared structural query-plan cache during prepare",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(15), Value::Uint(0)],
        )
        .expect("grouped prepared execution should bind the first WHERE+HAVING thresholds");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        first_rows.len(),
        3,
        "the first grouped prepared execution should keep every group with at least one row above the bound WHERE threshold",
    );
    assert_eq!(
        first_rows[0].group_key(),
        &[Value::Text("Ada".to_string())],
        "the first grouped prepared execution should keep the Ada group after filtering age > 15",
    );
    assert_eq!(
        first_rows[0].aggregate_values(),
        &[Value::Uint(1)],
        "the first grouped prepared execution should recount Ada after the bound WHERE threshold",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(15), Value::Uint(1)],
        )
        .expect("grouped prepared execution should bind the second WHERE+HAVING thresholds");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        second_rows,
        Vec::new(),
        "the second grouped prepared execution should apply the new HAVING threshold on top of the rebound grouped WHERE predicate",
    );
}

#[test]
fn execute_prepared_sql_query_templates_grouped_where_only_compare_contracts() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[("Ada", 10), ("Ada", 30), ("Bea", 20), ("Cid", 40)],
    );

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM SessionSqlEntity \
             WHERE age > ? \
             GROUP BY name \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared SQL grouped WHERE-only compare contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        1,
        "grouped WHERE-only compare shape should freeze one parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "grouped scalar WHERE compare should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicGrouped),
        "grouped compare-family WHERE-only queries should stay on the symbolic grouped lane in 0.99",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlEntity>(&prepared, &[Value::Uint(15)])
        .expect("grouped prepared execution should bind the first WHERE-only threshold");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        first_rows.len(),
        3,
        "the first grouped prepared execution should keep every group with one row above the bound WHERE threshold",
    );
    assert_eq!(
        first_rows[0].group_key(),
        &[Value::Text("Ada".to_string())],
        "the first grouped prepared execution should keep Ada after filtering age > 15",
    );
    assert_eq!(
        first_rows[0].aggregate_values(),
        &[Value::Uint(1)],
        "the first grouped prepared execution should recount Ada after the bound WHERE threshold",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlEntity>(&prepared, &[Value::Uint(25)])
        .expect("grouped prepared execution should bind the second WHERE-only threshold");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        second_rows.len(),
        2,
        "the second grouped prepared execution should rebuild grouped rows from the rebound WHERE threshold",
    );
    assert_eq!(
        second_rows[0].group_key(),
        &[Value::Text("Ada".to_string())],
        "the second grouped prepared execution should still keep Ada through the rebound WHERE threshold",
    );
    assert_eq!(
        second_rows[1].group_key(),
        &[Value::Text("Cid".to_string())],
        "the second grouped prepared execution should drop Bea and keep Cid after rebinding the grouped WHERE predicate",
    );
}

#[test]
fn prepare_sql_query_expression_owned_having_parameters_fall_back_outside_template_lanes() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Ada", 11), ("Bea", 20)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM SessionSqlEntity \
             GROUP BY name \
             HAVING COALESCE(NULLIF(COUNT(*), ?), 99) = ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect(
            "expression-owned HAVING parameter positions should still prepare for bound-SQL fallback",
        );

    assert_eq!(
        prepared.parameter_count(),
        2,
        "the expression-owned HAVING shape should still freeze both parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "NULLIF(COUNT(*), ?) should still infer a numeric contract from the surrounding aggregate-owned expression",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "the outer HAVING compare target should still infer a numeric right-hand contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        None,
        "general expression-owned HAVING semantics must stay off the prepared template lanes",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(2), Value::Uint(99)],
        )
        .expect(
            "prepared fallback execution should bind the first expression-owned HAVING thresholds",
        );
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        first_rows.len(),
        1,
        "the first fallback execution should keep one grouped row",
    );
    assert_eq!(
        first_rows[0].group_key(),
        &[Value::Text("Ada".to_string())],
        "the first fallback execution should honor the parameterized COALESCE/NULLIF HAVING semantics",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(1), Value::Uint(99)],
        )
        .expect(
            "prepared fallback execution should bind a second expression-owned HAVING threshold pair",
        );
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        second_rows.len(),
        1,
        "repeat fallback execution should keep one grouped row",
    );
    assert_eq!(
        second_rows[0].group_key(),
        &[Value::Text("Bea".to_string())],
        "repeat fallback execution should re-evaluate the expression-owned HAVING semantics with the new bindings",
    );
    assert_eq!(
        session.sql_compiled_command_cache_len(),
        0,
        "expression-owned prepared HAVING fallback should still bypass the raw SQL compiled-command cache",
    );
}

#[test]
fn prepare_sql_query_case_result_having_contracts_inherit_outer_compare_family() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Ada", 11), ("Bea", 20)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM SessionSqlEntity \
             GROUP BY name \
             HAVING CASE WHEN COUNT(*) > 1 THEN ? ELSE COUNT(*) END = ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared SQL CASE-result HAVING compare should inherit its numeric contract from the outer compare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "the CASE-result HAVING compare should still freeze both fallback contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "the CASE result slot should inherit the outer numeric compare family",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "the HAVING compare slot should keep one numeric-family contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        None,
        "CASE-result HAVING compare semantics must stay off prepared template lanes",
    );

    let result = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(99), Value::Uint(99)],
        )
        .expect("fallback execution should bind the CASE-result HAVING slots");
    let crate::db::session::sql::SqlStatementResult::Grouped { rows, .. } = result else {
        panic!("prepared grouped SQL should emit grouped rows");
    };

    assert_eq!(
        rows.len(),
        1,
        "the CASE-result HAVING compare should keep one grouped row",
    );
    assert_eq!(
        rows[0].group_key(),
        &[Value::Text("Ada".to_string())],
        "the CASE-result HAVING compare should preserve outer compare semantics under fallback execution",
    );
}

#[test]
fn prepare_sql_query_grouped_having_shared_contract_inference_keeps_distinct_lane_gating() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Ada", 11), ("Bea", 20)]);

    // Compare-family grouped HAVING should stay on the symbolic grouped template lane.
    let compare_prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM SessionSqlEntity \
             GROUP BY name \
             HAVING COUNT(*) > ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("grouped compare-family HAVING should prepare on the symbolic grouped lane");

    assert_eq!(
        compare_prepared.parameter_count(),
        1,
        "grouped compare-family HAVING should freeze one numeric compare contract",
    );
    assert_eq!(
        compare_prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "COUNT(*) > ? should keep the numeric compare contract on the grouped template lane",
    );
    assert_eq!(
        compare_prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicGrouped),
        "compare-family grouped HAVING should remain eligible for symbolic grouped templates",
    );

    // Expression-owned grouped HAVING should reuse the same numeric family inference model,
    // but it must stay on fallback execution instead of silently widening grouped templates.
    let fallback_prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM SessionSqlEntity \
             GROUP BY name \
             HAVING COALESCE(NULLIF(COUNT(*), ?), 99) = ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("expression-owned grouped HAVING should prepare on the fallback lane");

    assert_eq!(
        fallback_prepared.parameter_count(),
        2,
        "expression-owned grouped HAVING should still freeze both fallback contracts",
    );
    assert_eq!(
        fallback_prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "NULLIF(COUNT(*), ?) should inherit the grouped numeric family under shared fallback inference",
    );
    assert_eq!(
        fallback_prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "the outer grouped compare should keep the same numeric-family contract under fallback inference",
    );
    assert_eq!(
        fallback_prepared.template_kind_for_test(),
        None,
        "expression-owned grouped HAVING must stay off template lanes even though contract inference is shared",
    );
}

#[test]
fn execute_prepared_sql_query_templates_grouped_indexed_where_and_having_compare_contracts() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Ada", 10), ("Ada", 20), ("Bea", 30)]);

    let prepared = session
        .prepare_sql_query::<IndexedSessionSqlEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM IndexedSessionSqlEntity \
             WHERE name = ? \
             GROUP BY name \
             HAVING COUNT(*) > ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared indexed grouped WHERE+HAVING compare contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "indexed grouped WHERE+HAVING compare shape should freeze two parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "indexed grouped scalar WHERE compare should freeze one text parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "indexed grouped HAVING compare should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicGrouped),
        "indexed grouped compare-family WHERE+HAVING queries should stay on the symbolic grouped lane when the planner selects one symbolic index-prefix access path",
    );
    assert_eq!(
        session.query_plan_cache_len(),
        0,
        "template-capable indexed grouped WHERE+HAVING shapes should not touch the shared structural query-plan cache during prepare",
    );

    let first = session
        .execute_prepared_sql_query::<IndexedSessionSqlEntity>(
            &prepared,
            &[Value::Text("Ada".to_string()), Value::Uint(1)],
        )
        .expect("indexed grouped prepared execution should bind the first WHERE+HAVING thresholds");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared indexed grouped SQL should emit grouped rows");
    };

    assert_eq!(
        first_rows.len(),
        1,
        "the first indexed grouped prepared execution should keep the duplicated indexed name group",
    );
    assert_eq!(
        first_rows[0].group_key(),
        &[Value::Text("Ada".to_string())],
        "the first indexed grouped prepared execution should bind the indexed WHERE value into the selected access payload",
    );
    assert_eq!(
        first_rows[0].aggregate_values(),
        &[Value::Uint(2)],
        "the first indexed grouped prepared execution should keep the grouped count after rebound indexed filtering",
    );

    let second = session
        .execute_prepared_sql_query::<IndexedSessionSqlEntity>(
            &prepared,
            &[Value::Text("Bea".to_string()), Value::Uint(0)],
        )
        .expect(
            "indexed grouped prepared execution should bind the second WHERE+HAVING thresholds",
        );
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared indexed grouped SQL should emit grouped rows");
    };

    assert_eq!(
        second_rows.len(),
        1,
        "the second indexed grouped prepared execution should keep the rebound singleton indexed name group when the HAVING threshold allows it",
    );
    assert_eq!(
        second_rows[0].group_key(),
        &[Value::Text("Bea".to_string())],
        "the second indexed grouped prepared execution should reflect the new indexed WHERE binding instead of reusing the first access payload",
    );
    assert_eq!(
        second_rows[0].aggregate_values(),
        &[Value::Uint(1)],
        "the second indexed grouped prepared execution should recompute grouped counts from the rebound indexed access path",
    );
}

#[test]
fn execute_prepared_sql_query_grouped_indexed_where_predicate_and_having_contracts_preserve_fallback_results()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Ada", 10), ("Ada", 20), ("Bea", 30)]);

    let prepared = session
        .prepare_sql_query::<IndexedSessionSqlEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM IndexedSessionSqlEntity \
             WHERE name = ? AND age > ? \
             GROUP BY name \
             HAVING COUNT(*) > ? \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared indexed grouped access+predicate+HAVING contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        3,
        "indexed grouped access+predicate+HAVING shape should freeze three parameter contracts",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "indexed grouped access compare should freeze one text parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[1].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "indexed grouped scalar WHERE compare should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[2].type_family(),
        PreparedSqlParameterTypeFamily::Numeric,
        "indexed grouped HAVING compare should freeze one numeric parameter contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::Legacy),
        "indexed grouped access+predicate+HAVING queries should stay on the legacy lane until grouped access and grouped residual filter rebinding can coexist safely",
    );

    let first = session
        .execute_prepared_sql_query::<IndexedSessionSqlEntity>(
            &prepared,
            &[Value::Text("Ada".to_string()), Value::Uint(15), Value::Uint(0)],
        )
        .expect("indexed grouped prepared execution should bind the first access+predicate+HAVING thresholds");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared indexed grouped SQL should emit grouped rows");
    };

    assert_eq!(
        first_rows.len(),
        1,
        "the first indexed grouped prepared execution should keep one grouped row after indexed access, rebound WHERE filtering, and HAVING on the fallback lane",
    );
    assert_eq!(
        first_rows[0].group_key(),
        &[Value::Text("Ada".to_string())],
        "the first indexed grouped prepared execution should bind the indexed access value and keep the surviving grouped key",
    );
    assert_eq!(
        first_rows[0].aggregate_values(),
        &[Value::Uint(1)],
        "the first indexed grouped prepared execution should recompute grouped counts after the rebound residual WHERE predicate on the fallback lane",
    );

    let second = session
        .execute_prepared_sql_query::<IndexedSessionSqlEntity>(
            &prepared,
            &[Value::Text("Ada".to_string()), Value::Uint(15), Value::Uint(1)],
        )
        .expect("indexed grouped prepared execution should bind the second access+predicate+HAVING thresholds");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared indexed grouped SQL should emit grouped rows");
    };

    assert_eq!(
        second_rows,
        Vec::new(),
        "the second indexed grouped prepared execution should apply the rebound HAVING threshold after the same rebound access and WHERE filtering on the fallback lane",
    );
}

#[test]
fn execute_prepared_sql_query_templates_grouped_indexed_where_only_compare_contracts() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Ada", 10), ("Ada", 20), ("Bea", 30)]);

    let prepared = session
        .prepare_sql_query::<IndexedSessionSqlEntity>(
            "SELECT name, COUNT(*) AS total_count \
             FROM IndexedSessionSqlEntity \
             WHERE name = ? \
             GROUP BY name \
             ORDER BY name ASC \
             LIMIT 10",
        )
        .expect("prepared indexed grouped WHERE-only compare contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        1,
        "indexed grouped WHERE-only compare shape should freeze one parameter contract",
    );
    assert_eq!(
        prepared.parameter_contracts()[0].type_family(),
        PreparedSqlParameterTypeFamily::Text,
        "indexed grouped scalar WHERE compare should freeze one text parameter contract",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicGrouped),
        "indexed grouped compare-family WHERE-only queries should stay on the symbolic grouped lane when the planner selects one symbolic index-prefix access path",
    );

    let first = session
        .execute_prepared_sql_query::<IndexedSessionSqlEntity>(
            &prepared,
            &[Value::Text("Ada".to_string())],
        )
        .expect("indexed grouped prepared execution should bind the first WHERE-only threshold");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: first_rows, ..
    } = first
    else {
        panic!("prepared indexed grouped SQL should emit grouped rows");
    };

    assert_eq!(
        first_rows.len(),
        1,
        "the first indexed grouped prepared execution should keep the duplicated indexed name group",
    );
    assert_eq!(
        first_rows[0].group_key(),
        &[Value::Text("Ada".to_string())],
        "the first indexed grouped prepared execution should bind the indexed WHERE value into the selected access payload",
    );
    assert_eq!(
        first_rows[0].aggregate_values(),
        &[Value::Uint(2)],
        "the first indexed grouped prepared execution should keep the grouped count after rebound indexed filtering",
    );

    let second = session
        .execute_prepared_sql_query::<IndexedSessionSqlEntity>(
            &prepared,
            &[Value::Text("Bea".to_string())],
        )
        .expect("indexed grouped prepared execution should bind the second WHERE-only threshold");
    let crate::db::session::sql::SqlStatementResult::Grouped {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared indexed grouped SQL should emit grouped rows");
    };

    assert_eq!(
        second_rows.len(),
        1,
        "the second indexed grouped prepared execution should rebuild grouped rows from the rebound indexed WHERE value",
    );
    assert_eq!(
        second_rows[0].group_key(),
        &[Value::Text("Bea".to_string())],
        "the second indexed grouped prepared execution should reflect the rebound indexed WHERE value instead of reusing the first access payload",
    );
    assert_eq!(
        second_rows[0].aggregate_values(),
        &[Value::Uint(1)],
        "the second indexed grouped prepared execution should keep the grouped count for the rebound indexed name",
    );
}

#[test]
fn execute_prepared_sql_query_templates_mixed_or_compare_contracts() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("Ada", 10), ("Bea", 20), ("Cid", 30)]);

    let prepared = session
        .prepare_sql_query::<SessionSqlEntity>(
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE age > ? OR name > ? \
             ORDER BY age ASC",
        )
        .expect("prepared SQL mixed OR compare contracts should prepare");

    assert_eq!(
        prepared.parameter_count(),
        2,
        "mixed OR compare shapes should freeze two parameter contracts",
    );
    assert_eq!(
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicScalar),
        "simple scalar OR compare queries should stay on the symbolic scalar template lane in 0.99",
    );

    let first = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(25), Value::Text("B".to_string())],
        )
        .expect("mixed OR prepared SQL execution should bind both compare thresholds");
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
        "the first mixed OR execution should honor both the numeric and text bound thresholds",
    );

    let second = session
        .execute_prepared_sql_query::<SessionSqlEntity>(
            &prepared,
            &[Value::Uint(35), Value::Text("C".to_string())],
        )
        .expect("repeat mixed OR prepared SQL execution should bind different thresholds");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: second_rows, ..
    } = second
    else {
        panic!("prepared SQL scalar SELECT should emit projection rows");
    };

    assert_eq!(
        second_rows,
        vec![vec![Value::Text("Cid".to_string())]],
        "the repeated mixed OR execution should reflect the newly bound thresholds on the symbolic scalar lane",
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
fn execute_prepared_sql_query_templates_bool_compare_contracts() {
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
        prepared.template_kind_for_test(),
        Some(PreparedSqlExecutionTemplateKind::SymbolicScalar),
        "simple scalar bool prepared queries should stay on the symbolic scalar template lane",
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
        "bool prepared execution should bypass the raw SQL compiled-command cache on the symbolic template path",
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

#[expect(
    clippy::too_many_lines,
    reason = "prepared truth-wrapper parity matrix intentionally proves one semantic boundary"
)]
#[test]
fn prepare_sql_query_zero_param_truth_wrappers_match_non_prepared_canonical_shape() {
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

    // Phase 1: prepare the zero-parameter truth-wrapper spellings without
    // widening the prepared template lane.
    let prepared_is_true = session
        .prepare_sql_query::<SessionSqlBoolCompareEntity>(
            "SELECT label \
             FROM SessionSqlBoolCompareEntity \
             WHERE active IS TRUE \
             ORDER BY label ASC",
        )
        .expect("zero-parameter IS TRUE query should prepare");
    let prepared_is_false = session
        .prepare_sql_query::<SessionSqlBoolCompareEntity>(
            "SELECT label \
             FROM SessionSqlBoolCompareEntity \
             WHERE active IS FALSE \
             ORDER BY label ASC",
        )
        .expect("zero-parameter IS FALSE query should prepare");

    assert_eq!(
        prepared_is_true.parameter_count(),
        0,
        "zero-parameter IS TRUE query should not freeze any parameter contracts",
    );
    assert_eq!(
        prepared_is_false.parameter_count(),
        0,
        "zero-parameter IS FALSE query should not freeze any parameter contracts",
    );
    assert_eq!(
        prepared_is_true.template_kind_for_test(),
        None,
        "zero-parameter truth-wrapper queries should stay off prepared template lanes",
    );
    assert_eq!(
        prepared_is_false.template_kind_for_test(),
        None,
        "zero-parameter false-wrapper queries should stay off prepared template lanes",
    );

    // Phase 2: lower the prepared statements after empty binding and require
    // the same canonical planner identity as the non-prepared truth forms.
    let prepared_true_query = lower_select_statement_for_tests::<SessionSqlBoolCompareEntity>(
        prepared_is_true
            .statement_for_test()
            .bind_literals(&[])
            .expect("zero-parameter IS TRUE prepared statement should bind an empty literal set"),
    )
    .expect("prepared IS TRUE statement should lower");
    let bare_query = lower_select_query_for_tests::<SessionSqlBoolCompareEntity>(
        &session,
        "SELECT label \
         FROM SessionSqlBoolCompareEntity \
         WHERE active \
         ORDER BY label ASC",
    )
    .expect("bare bool truth query should lower");
    let prepared_false_query = lower_select_statement_for_tests::<SessionSqlBoolCompareEntity>(
        prepared_is_false
            .statement_for_test()
            .bind_literals(&[])
            .expect("zero-parameter IS FALSE prepared statement should bind an empty literal set"),
    )
    .expect("prepared IS FALSE statement should lower");
    let not_query = lower_select_query_for_tests::<SessionSqlBoolCompareEntity>(
        &session,
        "SELECT label \
         FROM SessionSqlBoolCompareEntity \
         WHERE NOT active \
         ORDER BY label ASC",
    )
    .expect("NOT bool truth query should lower");

    assert_eq!(
        prepared_true_query.structural().structural_cache_key(),
        bare_query.structural().structural_cache_key(),
        "prepared IS TRUE must lower onto the same structural truth-condition identity as the non-prepared bare bool filter",
    );
    assert_eq!(
        prepared_true_query
            .plan()
            .expect("prepared IS TRUE plan should build")
            .into_inner()
            .fingerprint(),
        bare_query
            .plan()
            .expect("bare bool truth plan should build")
            .into_inner()
            .fingerprint(),
        "prepared IS TRUE must share the same semantic plan fingerprint as the non-prepared bare bool filter",
    );
    assert_eq!(
        prepared_false_query.structural().structural_cache_key(),
        not_query.structural().structural_cache_key(),
        "prepared IS FALSE must lower onto the same structural truth-condition identity as the non-prepared NOT filter",
    );
    assert_eq!(
        prepared_false_query
            .plan()
            .expect("prepared IS FALSE plan should build")
            .into_inner()
            .fingerprint(),
        not_query
            .plan()
            .expect("NOT bool truth plan should build")
            .into_inner()
            .fingerprint(),
        "prepared IS FALSE must share the same semantic plan fingerprint as the non-prepared NOT filter",
    );

    // Phase 3: execution should preserve the same outward rows as the matching
    // non-prepared truth-condition spellings.
    let prepared_true_result = session
        .execute_prepared_sql_query::<SessionSqlBoolCompareEntity>(&prepared_is_true, &[])
        .expect("zero-parameter IS TRUE prepared execution should succeed");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: prepared_true_rows,
        ..
    } = prepared_true_result
    else {
        panic!("prepared IS TRUE query should emit one projection payload");
    };
    let bare_rows = statement_projection_rows::<SessionSqlBoolCompareEntity>(
        &session,
        "SELECT label \
         FROM SessionSqlBoolCompareEntity \
         WHERE active \
         ORDER BY label ASC",
    )
    .expect("non-prepared bare bool truth query should execute");

    assert_eq!(
        prepared_true_rows, bare_rows,
        "prepared IS TRUE execution should preserve the same outward row set as the non-prepared bare bool truth condition",
    );

    let prepared_false_result = session
        .execute_prepared_sql_query::<SessionSqlBoolCompareEntity>(&prepared_is_false, &[])
        .expect("zero-parameter IS FALSE prepared execution should succeed");
    let crate::db::session::sql::SqlStatementResult::Projection {
        rows: prepared_false_rows,
        ..
    } = prepared_false_result
    else {
        panic!("prepared IS FALSE query should emit one projection payload");
    };
    let not_rows = statement_projection_rows::<SessionSqlBoolCompareEntity>(
        &session,
        "SELECT label \
         FROM SessionSqlBoolCompareEntity \
         WHERE NOT active \
         ORDER BY label ASC",
    )
    .expect("non-prepared NOT bool truth query should execute");

    assert_eq!(
        prepared_false_rows, not_rows,
        "prepared IS FALSE execution should preserve the same outward row set as the non-prepared NOT bool truth condition",
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
