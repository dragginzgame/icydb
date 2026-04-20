use super::*;
use crate::db::query::explain::{ExplainExecutionNodeType, ExplainGrouping};

// Execute one indexed grouped SQL case, assert the fully materialized ordered
// grouped contract, and project rows into a compact assertion shape.
fn execute_indexed_grouped_case(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    context: &str,
) -> Vec<(Value, Vec<Value>)> {
    let execution =
        execute_grouped_select_for_tests::<IndexedSessionSqlEntity>(&session, sql, None)
            .unwrap_or_else(|err| panic!("{context} SQL execution should succeed: {err}"));

    assert!(
        execution.continuation_cursor().is_none(),
        "{context} should fully materialize under LIMIT 10",
    );

    grouped_result_rows(&execution)
}

// Reset the indexed SQL store and seed one deterministic indexed grouped
// cohort so grouped aggregate matrix tests can share the same setup path.
fn seeded_indexed_grouped_session(rows: &[(&'static str, u64)]) -> DbSession<SessionSqlCanister> {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    seed_indexed_session_sql_entities(&session, rows);

    session
}

// Execute one table of indexed grouped SQL cases and assert the compact
// grouped row payload stays stable for every case in the matrix.
type IndexedGroupedCase<'a> = (&'a str, &'a str, Vec<(Value, Vec<Value>)>);

fn assert_indexed_grouped_case_matrix(
    session: &DbSession<SessionSqlCanister>,
    cases: &[IndexedGroupedCase<'_>],
    failure_suffix: &str,
) {
    for (label, sql, expected_rows) in cases {
        let actual_rows = execute_indexed_grouped_case(session, sql, label);

        assert_eq!(actual_rows, *expected_rows, "{label} {failure_suffix}");
    }
}

// Execute one table of simple grouped COUNT(*) SQL cases and assert the fully
// materialized grouped key/count rows stay stable across each spelling.
type GroupedCountCase<'a> = (&'a str, &'a str, Vec<(Value, Value)>);

fn assert_grouped_count_case_matrix(
    session: &DbSession<SessionSqlCanister>,
    cases: &[GroupedCountCase<'_>],
    failure_suffix: &str,
) {
    for (label, sql, expected_rows) in cases {
        let execution = execute_grouped_select_for_tests::<SessionSqlEntity>(session, sql, None)
            .unwrap_or_else(|err| panic!("{label} should succeed: {err}"));
        let actual_rows = execution
            .rows()
            .iter()
            .map(|row| {
                (
                    row.group_key()[0].clone(),
                    row.aggregate_values()[0].clone(),
                )
            })
            .collect::<Vec<_>>();

        assert!(
            execution.continuation_cursor().is_none(),
            "{label} should fully materialize under LIMIT 10",
        );
        assert_eq!(actual_rows, *expected_rows, "{label} {failure_suffix}");
    }
}

// Project one grouped execution payload into the compact public row shape used
// by the grouped helper and statement-lane assertions in this file.
fn grouped_result_rows(execution: &PagedGroupedExecutionWithTrace) -> Vec<(Value, Vec<Value>)> {
    execution
        .rows()
        .iter()
        .map(|row| (row.group_key()[0].clone(), row.aggregate_values().to_vec()))
        .collect()
}

// Assert that one grouped/session boundary error stays in the Unsupported lane
// and optionally preserves one actionable message fragment from that boundary.
fn assert_unsupported_query_error(err: QueryError, expected_message: Option<&str>, context: &str) {
    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "{context} should stay on the Unsupported query boundary",
    );

    if let Some(expected_message) = expected_message {
        assert!(
            err.to_string().contains(expected_message),
            "{context} should preserve explicit boundary guidance",
        );
    }
}

// Execute one grouped SQL statement execution case and assert the grouped payload surface
// stays stable across different projection shapes.
fn assert_grouped_statement_payload_case(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_columns: &[&str],
    expected_rows: &[(Value, Vec<Value>)],
    expected_row_count: u32,
    context: &str,
) {
    let payload = execute_sql_statement_for_tests::<SessionSqlEntity>(session, sql)
        .unwrap_or_else(|err| panic!("{context} should execute through statement SQL: {err}"));

    let SqlStatementResult::Grouped {
        columns,
        rows,
        row_count,
        next_cursor,
        ..
    } = payload
    else {
        panic!("{context} should return grouped payload");
    };

    assert_eq!(
        columns,
        expected_columns
            .iter()
            .map(|column| (*column).to_string())
            .collect::<Vec<_>>(),
        "{context} should preserve grouped projection labels",
    );
    assert_eq!(
        row_count, expected_row_count,
        "{context} should report grouped row count",
    );
    assert!(
        next_cursor.is_none(),
        "{context} should not emit cursor for fully materialized page",
    );

    let actual_rows = rows
        .iter()
        .map(|row| (row.group_key()[0].clone(), row.aggregate_values().to_vec()))
        .collect::<Vec<_>>();
    assert_eq!(
        actual_rows, expected_rows,
        "{context} should preserve grouped row payload values",
    );
}

// Execute one qualified-vs-unqualified grouped EXPLAIN pair and assert both
// surfaces normalize onto the same public output.
fn assert_grouped_qualified_identifier_explain_case(
    session: &DbSession<SessionSqlCanister>,
    qualified_sql: &str,
    unqualified_sql: &str,
    context: &str,
) {
    let qualified = statement_explain_sql::<SessionSqlEntity>(session, qualified_sql)
        .unwrap_or_else(|err| panic!("{context} qualified SQL should succeed: {err}"));
    let unqualified = statement_explain_sql::<SessionSqlEntity>(session, unqualified_sql)
        .unwrap_or_else(|err| panic!("{context} unqualified SQL should succeed: {err}"));

    assert_eq!(
        qualified, unqualified,
        "{context} qualified grouped identifiers should normalize to the same public output",
    );
}

// Execute one grouped-row equivalence pair and assert both SQL spellings keep
// the same grouped payload rows and paging state.
fn assert_grouped_row_equivalence_case(
    session: &DbSession<SessionSqlCanister>,
    left_sql: &str,
    right_sql: &str,
    context: &str,
) {
    let left = execute_grouped_select_for_tests::<SessionSqlEntity>(&session, left_sql, None)
        .unwrap_or_else(|err| panic!("{context} left SQL should execute: {err}"));
    let right = execute_grouped_select_for_tests::<SessionSqlEntity>(&session, right_sql, None)
        .unwrap_or_else(|err| panic!("{context} right SQL should execute: {err}"));

    assert_eq!(
        left.rows(),
        right.rows(),
        "{context} should normalize onto the same grouped rows",
    );
    assert_eq!(
        left.continuation_cursor(),
        right.continuation_cursor(),
        "{context} should preserve the same grouped paging state",
    );
}

// Assert one indexed grouped SQL explain/execution pair stays on the ordered
// grouped public contract.
fn assert_indexed_grouped_ordered_public_case(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    context: &str,
    expect_grouped_node_contract: bool,
) {
    let query = lower_select_query_for_tests::<IndexedSessionSqlEntity>(&session, sql)
        .unwrap_or_else(|err| panic!("{context} should lower: {err}"));
    let explain = query
        .explain()
        .unwrap_or_else(|err| panic!("{context} logical explain should succeed: {err}"));

    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            strategy: "ordered_group",
            fallback_reason: None,
            ..
        }
    ));

    let descriptor = query
        .explain_execution()
        .unwrap_or_else(|err| panic!("{context} execution explain should succeed: {err}"));
    assert_eq!(
        descriptor
            .node_properties()
            .get("grouped_plan_fallback_reason"),
        Some(&Value::from("none")),
        "{context} execution explain root should stay on the ordered grouped planner path",
    );

    if expect_grouped_node_contract {
        let grouped_node = explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::GroupedAggregateOrderedMaterialized,
        )
        .unwrap_or_else(|| {
            panic!("{context} should emit an explicit ordered grouped aggregate node")
        });
        assert_eq!(
            grouped_node
                .node_properties()
                .get("grouped_plan_fallback_reason"),
            Some(&Value::from("none")),
            "{context} grouped aggregate node should inherit the same no-fallback planner state",
        );
    }
}

#[test]
fn grouped_select_helper_rejection_matrix_preserves_lane_boundary_messages() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_message, context, expect_unsupported_variant) in [
        (
            "SELECT TRIM(name) FROM SessionSqlEntity",
            "grouped SELECT helper rejects scalar text-specific computed projection",
            "text-specific computed projection",
            false,
        ),
        (
            "SELECT COUNT(*) FROM SessionSqlEntity",
            "grouped SELECT helper rejects global aggregate SELECT",
            "global aggregate execution",
            false,
        ),
        (
            "SELECT TRIM(name), COUNT(*) FROM SessionSqlEntity GROUP BY name",
            "grouped SELECT helper rejects grouped text-specific computed projection",
            "grouped text-specific computed projection",
            false,
        ),
        (
            "DELETE FROM SessionSqlEntity ORDER BY id LIMIT 1",
            "grouped SELECT helper rejects DELETE",
            "delete execution",
            false,
        ),
        (
            "SELECT name FROM SessionSqlEntity",
            "",
            "non-grouped scalar SQL",
            true,
        ),
    ] {
        let err = execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None)
            .expect_err("grouped lane rejection matrix should stay fail-closed");

        if expect_unsupported_variant {
            assert!(
                matches!(
                    err,
                    QueryError::Execute(
                        crate::db::query::intent::QueryExecutionError::Unsupported(_)
                    )
                ),
                "{context} should fail closed for unsupported grouped-lane shapes",
            );
        } else {
            assert!(
                err.to_string().contains(expected_message),
                "{context} should preserve the actionable grouped-lane boundary message",
            );
        }
    }
}

#[test]
fn grouped_select_lowering_explain_and_execution_project_grouped_fallback_publicly() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("grouped-explain-a", 20),
            ("grouped-explain-b", 20),
            ("grouped-explain-c", 32),
        ],
    );

    let query = lower_select_query_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age ORDER BY age ASC LIMIT 10",
    )
    .expect("grouped explain SQL query should lower");
    assert!(
        query.has_grouping(),
        "grouped aggregate SQL projection lowering should produce grouped query intent",
    );
    let explain = query
        .explain()
        .expect("grouped logical explain should succeed");

    assert!(matches!(
        explain.grouping(),
        ExplainGrouping::Grouped {
            fallback_reason: Some("group_key_order_unavailable"),
            ..
        }
    ));

    let descriptor = query
        .explain_execution()
        .expect("grouped execution explain should succeed");
    assert_eq!(
        descriptor
            .node_properties()
            .get("grouped_plan_fallback_reason"),
        Some(&Value::from("group_key_order_unavailable")),
        "grouped execution explain root should surface the planner-owned grouped fallback reason",
    );

    let grouped_node = explain_execution_find_first_node(
        &descriptor,
        ExplainExecutionNodeType::GroupedAggregateHashMaterialized,
    )
    .expect("grouped execution explain should emit an explicit grouped aggregate node");
    assert_eq!(
        grouped_node
            .node_properties()
            .get("grouped_plan_fallback_reason"),
        Some(&Value::from("group_key_order_unavailable")),
        "grouped aggregate node should inherit the same planner-owned grouped fallback reason",
    );
}

#[test]
fn grouped_select_lowering_execution_surfaces_residual_filter_expr_for_searched_case_where() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(
        &session,
        &[
            ("grouped-explain-case-a", 10),
            ("grouped-explain-case-b", 20),
            ("grouped-explain-case-c", 30),
            ("grouped-explain-case-d", 40),
        ],
    );

    let query = lower_select_query_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, COUNT(*) \
         FROM SessionSqlEntity \
         WHERE CASE WHEN age >= 30 THEN TRUE ELSE age = 20 END \
         GROUP BY age \
         ORDER BY age ASC LIMIT 10",
    )
    .expect("grouped searched CASE explain_execution SQL should lower");

    let descriptor = query
        .explain_execution()
        .expect("grouped searched CASE execution explain should succeed");
    let residual_node = explain_execution_find_first_node(
        &descriptor,
        ExplainExecutionNodeType::ResidualPredicateFilter,
    )
    .expect("grouped searched CASE execution explain should emit a residual predicate node");

    assert_eq!(
        residual_node.filter_expr(),
        Some("CASE WHEN age >= 30 THEN TRUE ELSE age = 20 END"),
        "grouped execution explain should expose the semantic grouped WHERE expression separately from the residual predicate contract",
    );
    assert!(
        residual_node.residual_predicate().is_some(),
        "grouped execution explain should still expose the derived grouped residual predicate contract",
    );
}

#[test]
fn grouped_select_lowering_indexed_grouped_ordered_explain_matrix_projects_ordered_group_publicly()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("alpha", 10), ("alpha", 20), ("bravo", 30), ("charlie", 40)],
    );

    let cases = [
        (
            "ordered grouped COUNT(*)",
            "SELECT name, COUNT(*) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            false,
            false,
        ),
        (
            "ordered grouped COUNT(field)",
            "SELECT name, COUNT(age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            false,
            false,
        ),
        (
            "ordered grouped SUM(field)",
            "SELECT name, SUM(age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            false,
            false,
        ),
        (
            "ordered grouped AVG(field)",
            "SELECT name, AVG(age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            false,
            false,
        ),
        (
            "ordered grouped MIN(field)",
            "SELECT name, MIN(age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            false,
            false,
        ),
        (
            "ordered grouped MAX(field)",
            "SELECT name, MAX(age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            false,
            false,
        ),
        (
            "ordered grouped mixed COUNT(*) + SUM(field)",
            "SELECT name, COUNT(*), SUM(age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            false,
            false,
        ),
    ];

    for (context, sql, expect_grouped_node_contract, _) in cases {
        assert_indexed_grouped_ordered_public_case(
            &session,
            sql,
            context,
            expect_grouped_node_contract,
        );
    }
}

// This is an intentionally table-driven grouped aggregate matrix. Keeping the
// admitted ordered grouped cases inline makes the outward SQL contract easier
// to audit than splitting them across many tiny helpers.
#[test]
#[expect(clippy::too_many_lines)]
fn grouped_select_helper_indexed_aggregate_matrix_preserves_ordered_group_rows() {
    // Phase 1: seed one deterministic duplicate-free cohort for the plain
    // ordered grouped aggregate matrix.
    let session = seeded_indexed_grouped_session(&[
        ("alpha", 10),
        ("alpha", 20),
        ("bravo", 30),
        ("charlie", 40),
        ("charlie", 50),
    ]);

    // Phase 2: execute the ordered grouped aggregate matrix and assert each
    // aggregate shape stays on the same public grouped-row contract.
    let cases = [
        (
            "COUNT(*)",
            "SELECT name, COUNT(*) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![
                (Value::Text("alpha".to_string()), vec![Value::Uint(2)]),
                (Value::Text("bravo".to_string()), vec![Value::Uint(1)]),
                (Value::Text("charlie".to_string()), vec![Value::Uint(2)]),
            ],
        ),
        (
            "COUNT(age)",
            "SELECT name, COUNT(age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![
                (Value::Text("alpha".to_string()), vec![Value::Uint(2)]),
                (Value::Text("bravo".to_string()), vec![Value::Uint(1)]),
                (Value::Text("charlie".to_string()), vec![Value::Uint(2)]),
            ],
        ),
        (
            "SUM(age)",
            "SELECT name, SUM(age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![
                (
                    Value::Text("alpha".to_string()),
                    vec![Value::Decimal(crate::types::Decimal::from(30_u64))],
                ),
                (
                    Value::Text("bravo".to_string()),
                    vec![Value::Decimal(crate::types::Decimal::from(30_u64))],
                ),
                (
                    Value::Text("charlie".to_string()),
                    vec![Value::Decimal(crate::types::Decimal::from(90_u64))],
                ),
            ],
        ),
        (
            "AVG(age)",
            "SELECT name, AVG(age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![
                (
                    Value::Text("alpha".to_string()),
                    vec![Value::Decimal(crate::types::Decimal::from(15_u64))],
                ),
                (
                    Value::Text("bravo".to_string()),
                    vec![Value::Decimal(crate::types::Decimal::from(30_u64))],
                ),
                (
                    Value::Text("charlie".to_string()),
                    vec![Value::Decimal(crate::types::Decimal::from(45_u64))],
                ),
            ],
        ),
        (
            "MIN(age)",
            "SELECT name, MIN(age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![
                (Value::Text("alpha".to_string()), vec![Value::Uint(10)]),
                (Value::Text("bravo".to_string()), vec![Value::Uint(30)]),
                (Value::Text("charlie".to_string()), vec![Value::Uint(40)]),
            ],
        ),
        (
            "MIN(DISTINCT age)",
            "SELECT name, MIN(DISTINCT age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![
                (Value::Text("alpha".to_string()), vec![Value::Uint(10)]),
                (Value::Text("bravo".to_string()), vec![Value::Uint(30)]),
                (Value::Text("charlie".to_string()), vec![Value::Uint(40)]),
            ],
        ),
        (
            "MAX(age)",
            "SELECT name, MAX(age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![
                (Value::Text("alpha".to_string()), vec![Value::Uint(20)]),
                (Value::Text("bravo".to_string()), vec![Value::Uint(30)]),
                (Value::Text("charlie".to_string()), vec![Value::Uint(50)]),
            ],
        ),
        (
            "COUNT(*) + SUM(age)",
            "SELECT name, COUNT(*), SUM(age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![
                (
                    Value::Text("alpha".to_string()),
                    vec![
                        Value::Uint(2),
                        Value::Decimal(crate::types::Decimal::from(30_u64)),
                    ],
                ),
                (
                    Value::Text("bravo".to_string()),
                    vec![
                        Value::Uint(1),
                        Value::Decimal(crate::types::Decimal::from(30_u64)),
                    ],
                ),
                (
                    Value::Text("charlie".to_string()),
                    vec![
                        Value::Uint(2),
                        Value::Decimal(crate::types::Decimal::from(90_u64)),
                    ],
                ),
            ],
        ),
    ];

    assert_indexed_grouped_case_matrix(
        &session,
        &cases,
        "should preserve grouped-key order on the admitted ordered grouped lane",
    );
}

#[test]
fn grouped_select_helper_indexed_distinct_aggregate_matrix_preserves_ordered_group_rows() {
    // Phase 1: seed one deterministic duplicate-heavy cohort for the distinct
    // aggregate matrix on the ordered grouped lane.
    let session = seeded_indexed_grouped_session(&[
        ("alpha", 10),
        ("alpha", 10),
        ("alpha", 20),
        ("bravo", 30),
        ("charlie", 40),
        ("charlie", 50),
        ("charlie", 50),
    ]);

    // Phase 2: execute the distinct aggregate matrix and assert the public
    // grouped rows keep both ordering and per-group dedupe semantics.
    let cases = [
        (
            "COUNT(DISTINCT age)",
            "SELECT name, COUNT(DISTINCT age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![
                (Value::Text("alpha".to_string()), vec![Value::Uint(2)]),
                (Value::Text("bravo".to_string()), vec![Value::Uint(1)]),
                (Value::Text("charlie".to_string()), vec![Value::Uint(2)]),
            ],
        ),
        (
            "SUM(DISTINCT age)",
            "SELECT name, SUM(DISTINCT age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![
                (
                    Value::Text("alpha".to_string()),
                    vec![Value::Decimal(crate::types::Decimal::from(30_u64))],
                ),
                (
                    Value::Text("bravo".to_string()),
                    vec![Value::Decimal(crate::types::Decimal::from(30_u64))],
                ),
                (
                    Value::Text("charlie".to_string()),
                    vec![Value::Decimal(crate::types::Decimal::from(90_u64))],
                ),
            ],
        ),
        (
            "AVG(DISTINCT age)",
            "SELECT name, AVG(DISTINCT age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![
                (
                    Value::Text("alpha".to_string()),
                    vec![Value::Decimal(crate::types::Decimal::from(15_u64))],
                ),
                (
                    Value::Text("bravo".to_string()),
                    vec![Value::Decimal(crate::types::Decimal::from(30_u64))],
                ),
                (
                    Value::Text("charlie".to_string()),
                    vec![Value::Decimal(crate::types::Decimal::from(45_u64))],
                ),
            ],
        ),
        (
            "MIN(DISTINCT age)",
            "SELECT name, MIN(DISTINCT age) \
             FROM IndexedSessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![
                (Value::Text("alpha".to_string()), vec![Value::Uint(10)]),
                (Value::Text("bravo".to_string()), vec![Value::Uint(30)]),
                (Value::Text("charlie".to_string()), vec![Value::Uint(40)]),
            ],
        ),
    ];

    assert_indexed_grouped_case_matrix(
        &session,
        &cases,
        "should preserve ordered grouped rows after per-group DISTINCT dedupe",
    );
}

#[test]
fn grouped_select_lowering_indexed_filtered_grouped_ordered_explain_matrix_projects_ordered_group_publicly()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("alpha", 10), ("alpha", 20), ("bravo", 30), ("charlie", 40)],
    );

    let cases = [
        (
            "filtered ordered grouped COUNT(*)",
            "SELECT name, COUNT(*) \
             FROM IndexedSessionSqlEntity \
             WHERE name = 'alpha' \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            false,
            false,
        ),
        (
            "filtered ordered grouped SUM(field)",
            "SELECT name, SUM(age) \
             FROM IndexedSessionSqlEntity \
             WHERE name = 'alpha' \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            false,
            false,
        ),
        (
            "filtered ordered grouped AVG(field)",
            "SELECT name, AVG(age) \
             FROM IndexedSessionSqlEntity \
             WHERE name = 'alpha' \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            false,
            false,
        ),
    ];

    for (context, sql, expect_grouped_node_contract, _) in cases {
        assert_indexed_grouped_ordered_public_case(
            &session,
            sql,
            context,
            expect_grouped_node_contract,
        );
    }
}

#[test]
fn grouped_select_helper_indexed_filtered_aggregate_matrix_preserves_ordered_group_rows() {
    // Phase 1: seed one deterministic filtered cohort for the ordered grouped
    // aggregate matrix on the admitted index-backed filter path.
    let session = seeded_indexed_grouped_session(&[
        ("alpha", 10),
        ("alpha", 20),
        ("bravo", 30),
        ("charlie", 40),
        ("delta", 50),
    ]);

    // Phase 2: execute the filtered grouped aggregate matrix and assert the
    // grouped public surface stays stable after the index-backed filter.
    let cases = [
        (
            "filtered COUNT(*)",
            "SELECT name, COUNT(*) \
             FROM IndexedSessionSqlEntity \
             WHERE name = 'alpha' \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![(Value::Text("alpha".to_string()), vec![Value::Uint(2)])],
        ),
        (
            "filtered SUM(age)",
            "SELECT name, SUM(age) \
             FROM IndexedSessionSqlEntity \
             WHERE name = 'alpha' \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![(
                Value::Text("alpha".to_string()),
                vec![Value::Decimal(crate::types::Decimal::from(30_u64))],
            )],
        ),
        (
            "filtered AVG(age)",
            "SELECT name, AVG(age) \
             FROM IndexedSessionSqlEntity \
             WHERE name = 'alpha' \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec![(
                Value::Text("alpha".to_string()),
                vec![Value::Decimal(crate::types::Decimal::from(15_u64))],
            )],
        ),
    ];

    assert_indexed_grouped_case_matrix(
        &session,
        &cases,
        "should preserve grouped-key order on the admitted ordered grouped lane",
    );
}

#[test]
fn grouped_select_helper_matrix_queries_match_expected_grouped_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows used by grouped matrix queries.
    seed_session_sql_entities(
        &session,
        &[
            ("group-matrix-a", 10),
            ("group-matrix-b", 10),
            ("group-matrix-c", 20),
            ("group-matrix-d", 30),
            ("group-matrix-e", 30),
            ("group-matrix-f", 30),
        ],
    );

    // Phase 2: execute table-driven grouped SQL cases.
    let cases = [
        (
            "grouped COUNT(*)",
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec![
                (Value::Uint(10), Value::Uint(2)),
                (Value::Uint(20), Value::Uint(1)),
                (Value::Uint(30), Value::Uint(3)),
            ],
        ),
        (
            "filtered grouped COUNT(*)",
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             WHERE age >= 20 \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec![
                (Value::Uint(20), Value::Uint(1)),
                (Value::Uint(30), Value::Uint(3)),
            ],
        ),
        (
            "qualified filtered grouped COUNT(*)",
            "SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 20 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age ASC LIMIT 10",
            vec![
                (Value::Uint(20), Value::Uint(1)),
                (Value::Uint(30), Value::Uint(3)),
            ],
        ),
        (
            "grouped HAVING count threshold",
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             HAVING COUNT(*) > 1 \
             ORDER BY age ASC LIMIT 10",
            vec![
                (Value::Uint(10), Value::Uint(2)),
                (Value::Uint(30), Value::Uint(3)),
            ],
        ),
        (
            "grouped HAVING null count",
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             HAVING COUNT(*) IS NULL \
             ORDER BY age ASC LIMIT 10",
            vec![],
        ),
        (
            "grouped HAVING non-null count",
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             HAVING COUNT(*) IS NOT NULL \
             ORDER BY age ASC LIMIT 10",
            vec![
                (Value::Uint(10), Value::Uint(2)),
                (Value::Uint(20), Value::Uint(1)),
                (Value::Uint(30), Value::Uint(3)),
            ],
        ),
    ];

    // Phase 3: assert grouped row payloads for each SQL input.
    assert_grouped_count_case_matrix(
        &session,
        &cases,
        "should preserve grouped key/count rows on the grouped helper lane",
    );
}

#[test]
fn grouped_select_helper_filter_aggregate_matrix_matches_expected_grouped_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows so aggregate FILTER can prove that
    // groups stay present even when one aggregate admits zero rows.
    seed_session_sql_entities(
        &session,
        &[
            ("group-filter-a", 10),
            ("group-filter-b", 10),
            ("group-filter-c", 20),
            ("group-filter-d", 30),
            ("group-filter-e", 30),
            ("group-filter-f", 30),
        ],
    );

    // Phase 2: execute one grouped filtered aggregate statement and assert
    // the per-group aggregate values preserve SQL FILTER semantics.
    let sql = "SELECT age, \
               COUNT(*) FILTER (WHERE age >= 20), \
               SUM(age) FILTER (WHERE age >= 20) \
               FROM SessionSqlEntity \
               GROUP BY age \
               ORDER BY age ASC LIMIT 10";
    let execution = execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None)
        .expect("grouped aggregate FILTER should execute through the grouped runtime");

    assert!(
        execution.continuation_cursor().is_none(),
        "grouped aggregate FILTER should fully materialize under LIMIT 10",
    );
    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (Value::Uint(10), vec![Value::Uint(0), Value::Null]),
            (
                Value::Uint(20),
                vec![
                    Value::Uint(1),
                    Value::Decimal(crate::types::Decimal::from(20_u64)),
                ],
            ),
            (
                Value::Uint(30),
                vec![
                    Value::Uint(3),
                    Value::Decimal(crate::types::Decimal::from(90_u64)),
                ],
            ),
        ],
        "grouped aggregate FILTER should keep all groups while admitting rows only into the filtered aggregate terminals",
    );
}

#[test]
fn grouped_select_helper_filter_having_order_and_mixed_projection_matrix_matches_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows so one grouped query can prove filtered
    // aggregates, unfiltered aggregates, HAVING, and ORDER BY all compose on
    // the same grouped execution path.
    seed_session_sql_entities(
        &session,
        &[
            ("group-filter-mixed-a", 10),
            ("group-filter-mixed-b", 10),
            ("group-filter-mixed-c", 20),
            ("group-filter-mixed-d", 30),
            ("group-filter-mixed-e", 30),
            ("group-filter-mixed-f", 30),
        ],
    );

    // Phase 2: execute one grouped query that keeps one filtered aggregate,
    // one unfiltered aggregate, grouped HAVING, and grouped ORDER BY on the
    // same filtered aggregate expression surface.
    let sql = "SELECT age, \
               COUNT(*) FILTER (WHERE age >= 20), \
               COUNT(*), \
               SUM(age) FILTER (WHERE age >= 20) \
               FROM SessionSqlEntity \
               GROUP BY age \
               HAVING COUNT(*) FILTER (WHERE age >= 20) > 0 \
               ORDER BY COUNT(*) FILTER (WHERE age >= 20) DESC, age ASC LIMIT 10";
    let execution = execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None)
        .expect("grouped aggregate FILTER should compose with HAVING, ORDER BY, and mixed aggregate projection");

    assert!(
        execution.continuation_cursor().is_none(),
        "grouped aggregate FILTER composition query should fully materialize under LIMIT 10",
    );
    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (
                Value::Uint(30),
                vec![
                    Value::Uint(3),
                    Value::Uint(3),
                    Value::Decimal(crate::types::Decimal::from(90_u64)),
                ],
            ),
            (
                Value::Uint(20),
                vec![
                    Value::Uint(1),
                    Value::Uint(1),
                    Value::Decimal(crate::types::Decimal::from(20_u64)),
                ],
            ),
        ],
        "grouped aggregate FILTER should preserve filtered-vs-unfiltered aggregate values while HAVING and ORDER BY consume the same filtered aggregate meaning",
    );
}

#[test]
fn grouped_select_helper_filtered_aggregate_on_non_group_field_supports_alias_having_and_order() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows with repeated names so one grouped
    // query can filter aggregates on a non-grouped source field while still
    // grouping, HAVING, and ordering by the grouped alias.
    seed_session_sql_entities(
        &session,
        &[
            ("alpha", 10),
            ("alpha", 20),
            ("alpha", 30),
            ("beta", 10),
            ("beta", 40),
            ("gamma", 25),
            ("gamma", 35),
            ("gamma", 45),
        ],
    );

    // Phase 2: require aggregate FILTER to keep the non-grouped source-field
    // slots needed by grouped reducer evaluation instead of only the grouped
    // key and aggregate input slots.
    let sql = "SELECT name, \
               COUNT(*) FILTER (WHERE age >= 20) AS filtered_count \
               FROM SessionSqlEntity \
               GROUP BY name \
               HAVING filtered_count > 1 \
               ORDER BY filtered_count DESC, name ASC LIMIT 10";
    let execution = execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None)
        .expect("grouped aggregate FILTER on one non-grouped field should compose with alias HAVING and ORDER BY");

    assert!(
        execution.continuation_cursor().is_none(),
        "grouped aggregate FILTER on one non-grouped field should fully materialize under LIMIT 10",
    );
    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (Value::Text("gamma".to_string()), vec![Value::Uint(3)]),
            (Value::Text("alpha".to_string()), vec![Value::Uint(2)]),
        ],
        "grouped aggregate FILTER should preserve source-field access for non-grouped filter expressions",
    );
}

#[test]
fn grouped_select_helper_filtered_count_rows_on_non_group_field_uses_generic_reducers() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one minimal grouped cohort where the grouped key and the
    // filtered count source field diverge, so a dedicated `COUNT(*)` fold that
    // ignores aggregate FILTER would flatten every group to the same row count.
    seed_session_sql_entities(
        &session,
        &[
            ("alpha", 5),
            ("alpha", 15),
            ("alpha", 25),
            ("beta", 8),
            ("beta", 18),
            ("gamma", 30),
            ("gamma", 40),
        ],
    );

    // Phase 2: require grouped `COUNT(*) FILTER (...)` to respect the
    // aggregate-local predicate even when the grouped route would otherwise be
    // admissible for the dedicated grouped count fast path.
    let sql = "SELECT name, \
               COUNT(*) FILTER (WHERE age > 10) AS strong_count \
               FROM SessionSqlEntity \
               GROUP BY name \
               ORDER BY name ASC LIMIT 10";
    let execution = execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None)
        .expect("grouped filtered COUNT(*) on one non-grouped field should execute");

    assert!(
        execution.continuation_cursor().is_none(),
        "grouped filtered COUNT(*) should fully materialize under LIMIT 10",
    );
    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (Value::Text("alpha".to_string()), vec![Value::Uint(2)]),
            (Value::Text("beta".to_string()), vec![Value::Uint(1)]),
            (Value::Text("gamma".to_string()), vec![Value::Uint(2)]),
        ],
        "grouped filtered COUNT(*) must not flatten onto the dedicated grouped count path",
    );
}

#[test]
fn grouped_select_helper_filtered_aggregate_order_alias_supports_unary_not_bool_filters() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed repeated grouped keys plus mixed boolean rows so grouped
    // aggregate FILTER can prove alias ordering survives a unary NOT filter on
    // one non-grouped boolean source field.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (1, "mage", false, 30),
            (2, "mage", true, 10),
            (3, "warrior", false, 20),
            (4, "warrior", false, 15),
            (5, "cleric", true, 40),
        ],
    );

    // Phase 2: require grouped alias ORDER BY to normalize and execute over
    // the canonical filtered aggregate term instead of failing on the rewritten
    // `SUM(age) FILTER (WHERE NOT active)` order target.
    let sql = "SELECT name, \
               SUM(age) FILTER (WHERE NOT active) AS inactive_age_sum \
               FROM FilteredIndexedSessionSqlEntity \
               GROUP BY name \
               ORDER BY inactive_age_sum DESC, name ASC LIMIT 5";
    let execution =
        execute_grouped_select_for_tests::<FilteredIndexedSessionSqlEntity>(&session, sql, None)
            .expect(
                "grouped filtered aggregate ORDER BY alias should admit unary NOT boolean filters",
            );

    assert!(
        execution.continuation_cursor().is_none(),
        "grouped filtered aggregate ORDER BY alias should fully materialize under LIMIT 5",
    );
    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (Value::Text("cleric".to_string()), vec![Value::Null]),
            (
                Value::Text("warrior".to_string()),
                vec![Value::Decimal(crate::types::Decimal::from(35_u64))],
            ),
            (
                Value::Text("mage".to_string()),
                vec![Value::Decimal(crate::types::Decimal::from(30_u64))],
            ),
        ],
        "grouped filtered aggregate ORDER BY aliases should rank on the same filtered aggregate semantics even when the filter uses unary NOT over a boolean field",
    );
}

#[test]
fn grouped_select_helper_filtered_aggregate_order_alias_supports_null_test_boolean_compositions() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed repeated grouped keys plus nullable rows so grouped
    // aggregate FILTER can prove alias ordering survives normalized null-test
    // function calls composed with one ordinary boolean comparison.
    seed_nullable_session_sql_entities(
        &session,
        &[
            ("alpha", Some("captain")),
            ("alpha", None),
            ("bravo", Some("chief")),
            ("bravo", Some("guide")),
            ("charlie", None),
        ],
    );

    // Phase 2: require grouped alias ORDER BY to normalize and execute over
    // the canonical filtered aggregate term instead of rejecting the rewritten
    // `COUNT(*) FILTER (WHERE IS_NOT_NULL(nickname) AND name >= 'alpha')`
    // order target during grouped Top-K admission.
    let sql = "SELECT name, \
               COUNT(*) FILTER (WHERE nickname IS NOT NULL AND name >= 'alpha') AS named_count \
               FROM SessionNullableSqlEntity \
               GROUP BY name \
               ORDER BY named_count DESC, name ASC LIMIT 10";
    let execution =
        execute_grouped_select_for_tests::<SessionNullableSqlEntity>(&session, sql, None).expect(
            "grouped filtered aggregate ORDER BY alias should admit null-test boolean compositions",
        );

    assert!(
        execution.continuation_cursor().is_none(),
        "grouped filtered aggregate ORDER BY alias should fully materialize under LIMIT 10",
    );
    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (Value::Text("bravo".to_string()), vec![Value::Uint(2)]),
            (Value::Text("alpha".to_string()), vec![Value::Uint(1)]),
            (Value::Text("charlie".to_string()), vec![Value::Uint(0)]),
        ],
        "grouped filtered aggregate ORDER BY aliases should rank on the same filtered aggregate semantics when the filter uses null tests plus boolean composition",
    );
}

#[test]
fn grouped_select_helper_filtered_aggregate_order_alias_supports_offset() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed repeated grouped keys plus mixed boolean rows so grouped
    // aggregate FILTER can prove alias ordering still composes with the
    // grouped Top-K offset window.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (1, "mage", false, 30),
            (2, "mage", true, 10),
            (3, "warrior", false, 20),
            (4, "warrior", false, 15),
            (5, "cleric", true, 40),
        ],
    );

    // Phase 2: require grouped alias ORDER BY to keep the canonical filtered
    // aggregate meaning while the grouped bounded window skips the first row.
    let sql = "SELECT name, \
               SUM(age) FILTER (WHERE NOT active) AS inactive_age_sum \
               FROM FilteredIndexedSessionSqlEntity \
               GROUP BY name \
               ORDER BY inactive_age_sum DESC, name ASC LIMIT 1 OFFSET 1";
    let execution =
        execute_grouped_select_for_tests::<FilteredIndexedSessionSqlEntity>(&session, sql, None)
            .expect("grouped filtered aggregate ORDER BY alias with OFFSET should execute");

    assert!(
        execution.continuation_cursor().is_none(),
        "grouped filtered aggregate ORDER BY alias with OFFSET should stay on the grouped Top-K lane, which still suppresses continuation cursors",
    );
    assert_eq!(
        grouped_result_rows(&execution),
        vec![(
            Value::Text("warrior".to_string()),
            vec![Value::Decimal(crate::types::Decimal::from(35_u64))],
        )],
        "grouped filtered aggregate ORDER BY aliases with OFFSET should skip the first ranked row and keep the same filtered aggregate ordering semantics",
    );
}

#[test]
fn execute_sql_projection_rejects_grouped_aggregate_sql() {
    reset_session_sql_store();
    let session = sql_session();

    let err = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
    )
    .expect_err("projection row helper should reject grouped statement payloads");

    assert!(
        err.to_string()
            .contains("projection row SQL only supports value-row SQL projection payloads"),
        "projection row helper must preserve its value-row-only contract for grouped payloads",
    );
}

#[test]
fn grouped_select_helper_executes_field_to_field_predicate() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (score, handle, label) in [
        (10_u64, "mango", "apple"),
        (10_u64, "omega", "beta"),
        (20_u64, "alpha", "zebra"),
        (20_u64, "same", "same"),
    ] {
        session
            .insert(SessionDeterministicRangeEntity {
                id: Ulid::generate(),
                tier: "gold".to_string(),
                score,
                handle: handle.to_string(),
                label: label.to_string(),
            })
            .expect("deterministic grouped field-compare fixture insert should succeed");
    }

    let execution = execute_grouped_select_for_tests::<SessionDeterministicRangeEntity>(
        &session,
        "SELECT score, COUNT(*) \
         FROM SessionDeterministicRangeEntity \
         WHERE handle > label \
         GROUP BY score \
         ORDER BY score ASC LIMIT 10",
        None,
    )
    .expect("grouped field-to-field predicate SQL should execute");

    assert!(
        execution.continuation_cursor().is_none(),
        "grouped field-to-field predicate query should fully materialize under LIMIT 10",
    );
    assert_eq!(
        grouped_result_rows(&execution),
        vec![(Value::Uint(10), vec![Value::Uint(2)])],
        "grouped field-to-field predicate should filter rows before grouped aggregation using the shared residual predicate path",
    );
}

#[test]
fn grouped_select_helper_executes_searched_case_where_expression() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed one deterministic grouped WHERE matrix that keeps grouped
    // aggregation honest after searched CASE filtering.
    seed_session_sql_entities(
        &session,
        &[
            ("grouped-case-a", 10),
            ("grouped-case-b", 20),
            ("grouped-case-c", 30),
            ("grouped-case-d", 40),
        ],
    );

    // Phase 2: require grouped pre-aggregate WHERE to evaluate searched CASE
    // filters through the same scalar expression seam as scalar load/delete.
    let execution = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, COUNT(*) \
         FROM SessionSqlEntity \
         WHERE CASE WHEN age >= 30 THEN TRUE ELSE age = 20 END \
         GROUP BY age \
         ORDER BY age ASC LIMIT 10",
        None,
    )
    .expect("grouped searched CASE WHERE SQL should execute");

    assert!(
        execution.continuation_cursor().is_none(),
        "grouped searched CASE WHERE query should fully materialize under LIMIT 10",
    );
    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (Value::Uint(20), vec![Value::Uint(1)]),
            (Value::Uint(30), vec![Value::Uint(1)]),
            (Value::Uint(40), vec![Value::Uint(1)]),
        ],
        "grouped searched CASE WHERE should filter rows through the unified scalar expression seam before grouped aggregation",
    );
}

#[test]
fn grouped_select_helper_count_matrix_returns_expected_grouped_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("qualified-group-a", 20),
            ("qualified-group-b", 20),
            ("qualified-group-c", 32),
        ],
    );

    let cases = [
        (
            "canonical grouped count SQL",
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age ORDER BY age ASC LIMIT 10",
            vec![
                (Value::Uint(20), Value::Uint(2)),
                (Value::Uint(32), Value::Uint(1)),
            ],
        ),
        (
            "qualified grouped count SQL",
            "SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 20 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age ASC LIMIT 10",
            vec![
                (Value::Uint(20), Value::Uint(2)),
                (Value::Uint(32), Value::Uint(1)),
            ],
        ),
    ];

    assert_grouped_count_case_matrix(
        &session,
        &cases,
        "should preserve the canonical grouped count rows",
    );
}

#[test]
fn grouped_select_helper_limit_window_emits_cursor_and_resumes_next_group_page() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed three grouped-key buckets with deterministic counts.
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-a".to_string(),
            age: 10,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-b".to_string(),
            age: 10,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-c".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-d".to_string(),
            age: 30,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-e".to_string(),
            age: 30,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "group-page-f".to_string(),
            age: 30,
        })
        .expect("seed insert should succeed");

    // Phase 2: execute the first grouped page and capture continuation cursor.
    let sql = "SELECT age, COUNT(*) \
               FROM SessionSqlEntity \
               GROUP BY age \
               ORDER BY age ASC LIMIT 1";
    let first_page = execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None)
        .expect("first grouped SQL page should execute");
    assert_eq!(first_page.rows().len(), 1);
    assert_eq!(first_page.rows()[0].group_key(), [Value::Uint(10)]);
    assert_eq!(first_page.rows()[0].aggregate_values(), [Value::Uint(2)]);
    let cursor_one = crate::db::encode_cursor(
        first_page
            .continuation_cursor()
            .expect("first grouped SQL page should emit continuation cursor"),
    );

    // Phase 3: resume to second grouped page and capture next cursor.
    let second_page = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        sql,
        Some(cursor_one.as_str()),
    )
    .expect("second grouped SQL page should execute");
    assert_eq!(second_page.rows().len(), 1);
    assert_eq!(second_page.rows()[0].group_key(), [Value::Uint(20)]);
    assert_eq!(second_page.rows()[0].aggregate_values(), [Value::Uint(1)]);
    let cursor_two = crate::db::encode_cursor(
        second_page
            .continuation_cursor()
            .expect("second grouped SQL page should emit continuation cursor"),
    );

    // Phase 4: resume final grouped page and assert no further continuation.
    let third_page = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        sql,
        Some(cursor_two.as_str()),
    )
    .expect("third grouped SQL page should execute");
    assert_eq!(third_page.rows().len(), 1);
    assert_eq!(third_page.rows()[0].group_key(), [Value::Uint(30)]);
    assert_eq!(third_page.rows()[0].aggregate_values(), [Value::Uint(3)]);
    assert!(
        third_page.continuation_cursor().is_none(),
        "last grouped SQL page should not emit continuation cursor",
    );
}

#[test]
fn grouped_select_helper_multi_aggregate_having_offset_limit_cursor_resumes_consistently() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed five grouped buckets so HAVING leaves three qualifying
    // groups after aggregate finalization.
    seed_session_sql_entities(
        &session,
        &[
            ("grouped-having-page-a", 10),
            ("grouped-having-page-b", 10),
            ("grouped-having-page-c", 20),
            ("grouped-having-page-d", 30),
            ("grouped-having-page-e", 30),
            ("grouped-having-page-f", 30),
            ("grouped-having-page-g", 40),
            ("grouped-having-page-h", 50),
            ("grouped-having-page-i", 50),
            ("grouped-having-page-j", 50),
            ("grouped-having-page-k", 50),
        ],
    );

    // Phase 2: execute one multi-aggregate grouped page that requires HAVING,
    // offset, bounded selection, and continuation cursor construction.
    let sql = "SELECT age, COUNT(*), SUM(age) \
               FROM SessionSqlEntity \
               GROUP BY age \
               HAVING COUNT(*) > 1 \
               ORDER BY age ASC LIMIT 1 OFFSET 1";
    let first_page = execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None)
        .expect("first multi-aggregate grouped SQL page should execute");
    assert_eq!(first_page.rows().len(), 1);
    assert_eq!(first_page.rows()[0].group_key(), [Value::Uint(30)]);
    assert_eq!(
        first_page.rows()[0].aggregate_values(),
        [
            Value::Uint(3),
            Value::Decimal(crate::types::Decimal::from(90_u64)),
        ],
    );
    let first_cursor = crate::db::encode_cursor(
        first_page
            .continuation_cursor()
            .expect("first multi-aggregate grouped page should emit continuation cursor"),
    );

    // Phase 3: resume after the offset-qualified page and assert the next
    // qualifying grouped row continues from the prior canonical group key.
    let second_page = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        sql,
        Some(first_cursor.as_str()),
    )
    .expect("second multi-aggregate grouped SQL page should execute");
    assert_eq!(second_page.rows().len(), 1);
    assert_eq!(second_page.rows()[0].group_key(), [Value::Uint(50)]);
    assert_eq!(
        second_page.rows()[0].aggregate_values(),
        [
            Value::Uint(4),
            Value::Decimal(crate::types::Decimal::from(200_u64)),
        ],
    );
    assert!(
        second_page.continuation_cursor().is_none(),
        "final multi-aggregate grouped page should not emit another continuation cursor",
    );
}

#[test]
fn grouped_select_helper_cursor_rejection_matrix_preserves_cursor_plan_taxonomy() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed grouped buckets and capture one valid continuation cursor
    // for the signature-mismatch arm of the cursor rejection matrix.
    seed_session_sql_entities(
        &session,
        &[
            ("cursor-signature-a", 10),
            ("cursor-signature-b", 20),
            ("cursor-signature-c", 30),
        ],
    );
    let first_page = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 1",
        None,
    )
    .expect("first grouped SQL page should execute");
    let cursor = crate::db::encode_cursor(
        first_page
            .continuation_cursor()
            .expect("first grouped SQL page should emit continuation cursor"),
    );

    // Phase 2: assert both decode and signature failures stay mapped onto the
    // cursor-plan error taxonomy.
    for (cursor, context, expect_invalid_payload) in [
        (Some("zz"), "invalid grouped cursor token payload", true),
        (
            Some(cursor.as_str()),
            "grouped cursor token from incompatible query signature",
            false,
        ),
    ] {
        let sql = match context {
            "invalid grouped cursor token payload" => {
                "SELECT age, COUNT(*) \
                 FROM SessionSqlEntity \
                 GROUP BY age \
                 ORDER BY age ASC LIMIT 1"
            }
            "grouped cursor token from incompatible query signature" => {
                "SELECT age, COUNT(*) \
                 FROM SessionSqlEntity \
                 GROUP BY age \
                 ORDER BY age DESC LIMIT 1"
            }
            _ => unreachable!("grouped cursor rejection matrix is fixed"),
        };
        let err = execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, cursor)
            .expect_err("grouped cursor rejection matrix should stay fail-closed");

        if expect_invalid_payload {
            assert_query_error_is_cursor_plan(err, |inner| {
                matches!(inner, CursorPlanError::InvalidContinuationCursor { .. })
            });
        } else {
            assert_query_error_is_cursor_plan(err, |inner| {
                matches!(
                    inner,
                    CursorPlanError::ContinuationCursorSignatureMismatch { .. }
                )
            });
        }
    }
}

#[test]
fn grouped_select_helper_executes_bounded_aggregate_order_top_k_rows() {
    let session = seeded_indexed_grouped_session(&[
        ("alpha", 10),
        ("alpha", 20),
        ("bravo", 30),
        ("charlie", 40),
        ("delta", 50),
    ]);
    let execution = execute_grouped_select_for_tests::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name, AVG(age) \
         FROM IndexedSessionSqlEntity \
         GROUP BY name \
         ORDER BY AVG(age) DESC, name ASC LIMIT 2",
        None,
    )
    .expect("grouped aggregate ORDER BY should execute through bounded Top-K finalize");

    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (
                Value::Text("delta".to_string()),
                vec![Value::Decimal(crate::types::Decimal::from(50_u64))],
            ),
            (
                Value::Text("charlie".to_string()),
                vec![Value::Decimal(crate::types::Decimal::from(40_u64))],
            ),
        ],
        "grouped aggregate ORDER BY should emit the highest aggregate-ranked rows first",
    );
    assert!(
        execution.continuation_cursor().is_none(),
        "grouped aggregate ORDER BY should not expose grouped continuation cursors in this release",
    );
}

#[test]
fn grouped_select_helper_executes_bounded_aggregate_order_top_k_alias_rows() {
    let session = seeded_indexed_grouped_session(&[
        ("alpha", 10),
        ("alpha", 20),
        ("bravo", 30),
        ("charlie", 40),
        ("delta", 50),
    ]);

    let execution = execute_grouped_select_for_tests::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name, AVG(age) AS avg_age \
         FROM IndexedSessionSqlEntity \
         GROUP BY name \
         ORDER BY avg_age DESC, name ASC LIMIT 2",
        None,
    )
    .expect("grouped aggregate ORDER BY alias should execute through bounded Top-K finalize");

    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (
                Value::Text("delta".to_string()),
                vec![Value::Decimal(crate::types::Decimal::from(50_u64))],
            ),
            (
                Value::Text("charlie".to_string()),
                vec![Value::Decimal(crate::types::Decimal::from(40_u64))],
            ),
        ],
        "grouped aggregate ORDER BY aliases should rank by the same aggregate values as direct terms",
    );
    assert!(
        execution.continuation_cursor().is_none(),
        "grouped aggregate ORDER BY aliases should not expose grouped continuation cursors in this release",
    );
}

#[test]
fn grouped_select_helper_executes_bounded_aggregate_input_order_top_k_alias_rows() {
    let session = seeded_indexed_grouped_session(&[
        ("alpha", 10),
        ("alpha", 20),
        ("bravo", 30),
        ("charlie", 40),
        ("delta", 50),
    ]);

    let execution = execute_grouped_select_for_tests::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name, AVG(age + 1) AS avg_plus_one \
         FROM IndexedSessionSqlEntity \
         GROUP BY name \
         ORDER BY avg_plus_one DESC, name ASC LIMIT 2",
        None,
    )
    .expect("grouped aggregate input ORDER BY alias should execute through bounded Top-K finalize");

    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (
                Value::Text("delta".to_string()),
                vec![Value::Decimal(crate::types::Decimal::new(51, 0))],
            ),
            (
                Value::Text("charlie".to_string()),
                vec![Value::Decimal(crate::types::Decimal::new(41, 0))],
            ),
        ],
        "grouped aggregate input ORDER BY aliases should rank by the same aggregate values as direct terms",
    );
    assert!(
        execution.continuation_cursor().is_none(),
        "grouped aggregate input ORDER BY aliases should not expose grouped continuation cursors in this release",
    );
}

#[test]
fn grouped_select_helper_executes_bounded_wrapped_aggregate_input_order_top_k_alias_rows() {
    let session = seeded_indexed_grouped_session(&[
        ("alpha", 10),
        ("alpha", 20),
        ("bravo", 30),
        ("charlie", 40),
        ("delta", 50),
    ]);

    let execution = execute_grouped_select_for_tests::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name, ROUND(AVG(age + 1 * 2), 2) AS avg_boosted \
         FROM IndexedSessionSqlEntity \
         GROUP BY name \
         ORDER BY avg_boosted DESC, name ASC LIMIT 2",
        None,
    )
    .expect(
        "wrapped grouped aggregate input ORDER BY alias should execute through bounded Top-K finalize",
    );

    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (
                Value::Text("delta".to_string()),
                vec![Value::Decimal(crate::types::Decimal::new(5200, 2))],
            ),
            (
                Value::Text("charlie".to_string()),
                vec![Value::Decimal(crate::types::Decimal::new(4200, 2))],
            ),
        ],
        "wrapped grouped aggregate input ORDER BY aliases should rank by the same aggregate values as canonical direct terms",
    );
    assert!(
        execution.continuation_cursor().is_none(),
        "wrapped grouped aggregate input ORDER BY aliases should not expose grouped continuation cursors in this release",
    );
}

#[test]
fn grouped_select_helper_executes_parenthesized_wrapped_aggregate_input_order_top_k_alias_rows() {
    let session = seeded_indexed_grouped_session(&[
        ("alpha", 10),
        ("alpha", 20),
        ("bravo", 30),
        ("charlie", 40),
        ("delta", 50),
    ]);

    let execution = execute_grouped_select_for_tests::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name, ROUND(AVG((age + age) / 2), 2) AS avg_balanced \
         FROM IndexedSessionSqlEntity \
         GROUP BY name \
         ORDER BY avg_balanced DESC, name ASC LIMIT 2",
        None,
    )
    .expect(
        "parenthesized wrapped grouped aggregate input ORDER BY alias should execute through bounded Top-K finalize",
    );

    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (
                Value::Text("delta".to_string()),
                vec![Value::Decimal(crate::types::Decimal::new(5000, 2))],
            ),
            (
                Value::Text("charlie".to_string()),
                vec![Value::Decimal(crate::types::Decimal::new(4000, 2))],
            ),
        ],
        "parenthesized wrapped grouped aggregate input ORDER BY aliases should preserve the requested arithmetic precedence in grouped Top-K ranking",
    );
    assert!(
        execution.continuation_cursor().is_none(),
        "parenthesized wrapped grouped aggregate input ORDER BY aliases should not expose grouped continuation cursors in this release",
    );
}

#[test]
fn grouped_select_helper_executes_case_aggregate_input_order_top_k_alias_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("case-top-k-a", 10),
            ("case-top-k-b", 10),
            ("case-top-k-c", 20),
            ("case-top-k-d", 20),
            ("case-top-k-e", 20),
            ("case-top-k-f", 30),
        ],
    );

    let execution = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, SUM(CASE WHEN age > 10 THEN 1 ELSE 0 END) AS high_count \
         FROM SessionSqlEntity \
         GROUP BY age \
         ORDER BY high_count DESC, age ASC LIMIT 2",
        None,
    )
    .expect("grouped searched CASE aggregate input ORDER BY alias should execute through bounded Top-K finalize");

    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (
                Value::Uint(20),
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(3).expect("3 decimal"),
                )],
            ),
            (
                Value::Uint(30),
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(1).expect("1 decimal"),
                )],
            ),
        ],
        "grouped searched CASE aggregate input ORDER BY aliases should rank by the same aggregate values as canonical direct terms",
    );
    assert!(
        execution.continuation_cursor().is_none(),
        "grouped searched CASE aggregate input ORDER BY aliases should not expose grouped continuation cursors in this release",
    );
}

#[test]
fn grouped_select_helper_executes_aggregate_order_top_k_alias_with_field_compare_predicate() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (tier, score, handle, label) in [
        ("alpha", 10_u64, "mango", "apple"),
        ("alpha", 20_u64, "omega", "beta"),
        ("alpha", 30_u64, "same", "same"),
        ("bravo", 40_u64, "zulu", "able"),
        ("bravo", 50_u64, "charlie", "zebra"),
        ("bravo", 60_u64, "yankee", "bravo"),
    ] {
        session
            .insert(SessionDeterministicRangeEntity {
                id: Ulid::generate(),
                tier: tier.to_string(),
                score,
                handle: handle.to_string(),
                label: label.to_string(),
            })
            .expect("deterministic grouped Top-K field-compare fixture insert should succeed");
    }

    let execution = execute_grouped_select_for_tests::<SessionDeterministicRangeEntity>(
        &session,
        "SELECT tier, ROUND(AVG(score), 2) AS avg_score \
         FROM SessionDeterministicRangeEntity \
         WHERE handle > label \
         GROUP BY tier \
         ORDER BY avg_score DESC, tier ASC LIMIT 2",
        None,
    )
    .expect(
        "grouped aggregate ORDER BY alias should still execute through bounded Top-K finalize when a field-to-field residual predicate is present",
    );

    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (
                Value::Text("bravo".to_string()),
                vec![Value::Decimal(crate::types::Decimal::new(5000, 2))],
            ),
            (
                Value::Text("alpha".to_string()),
                vec![Value::Decimal(crate::types::Decimal::new(1500, 2))],
            ),
        ],
        "grouped aggregate ORDER BY alias should rank surviving grouped rows even when the underlying WHERE clause uses the shared field-to-field residual predicate path",
    );
    assert!(
        execution.continuation_cursor().is_none(),
        "grouped aggregate ORDER BY alias with field-to-field predicate should not expose grouped continuation cursors in this release",
    );
}

#[test]
fn execute_sql_scalar_api_rejection_matrix_preserves_grouped_boundary_contracts() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_message, context) in [
        (
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
            Some("scalar SELECT helper rejects grouped SELECT"),
            "grouped SELECT on scalar API",
        ),
        (
            "SELECT COUNT(*) FROM SessionSqlEntity GROUP BY age",
            None,
            "group-by projection mismatch on scalar API",
        ),
        (
            "SELECT age, TRIM(name), COUNT(*) FROM SessionSqlEntity GROUP BY age",
            None,
            "grouped computed projection widening on scalar API",
        ),
    ] {
        let err = execute_scalar_select_for_tests::<SessionSqlEntity>(&session, sql)
            .expect_err("scalar API grouped-shape matrix should stay fail-closed");

        assert_unsupported_query_error(err, expected_message, context);
    }
}

// This grouped payload matrix is intentionally kept as one table-driven surface
// contract so grouped statement labels and rows stay audited together.
#[test]
fn execute_sql_statement_grouped_payload_matrix() {
    reset_session_sql_store();
    let session = sql_session();
    let sql = "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age ORDER BY age ASC LIMIT 10";
    let expected_rows = vec![
        (Value::Uint(20), vec![Value::Uint(2)]),
        (Value::Uint(32), vec![Value::Uint(1)]),
    ];

    seed_session_sql_entities(
        &session,
        &[
            ("aggregate-a", 20),
            ("aggregate-b", 20),
            ("aggregate-c", 32),
        ],
    );

    assert_grouped_statement_payload_case(
        &session,
        sql,
        &["age", "COUNT(*)"],
        expected_rows.as_slice(),
        2,
        "statement grouped SQL",
    );
}

#[test]
fn execute_sql_statement_grouped_computed_projection_matrix_succeeds() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            (" alpha ", 20),
            (" alpha ", 21),
            ("beta", 30),
            ("gamma  ", 40),
        ],
    );

    for (sql, expected_columns, expected_rows, context) in [
        (
            "SELECT name, TRIM(name), COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec!["name", "TRIM(name)", "COUNT(*)"],
            vec![
                (
                    vec![Value::Text(" alpha ".into()), Value::Text("alpha".into())],
                    vec![Value::Uint(2)],
                ),
                (
                    vec![Value::Text("beta".into()), Value::Text("beta".into())],
                    vec![Value::Uint(1)],
                ),
                (
                    vec![Value::Text("gamma  ".into()), Value::Text("gamma".into())],
                    vec![Value::Uint(1)],
                ),
            ],
            "grouped statement SQL direct+text-specific computed projection",
        ),
        (
            "SELECT TRIM(name) AS trimmed_name, COUNT(*) total \
             FROM SessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec!["trimmed_name", "total"],
            vec![
                (vec![Value::Text("alpha".into())], vec![Value::Uint(2)]),
                (vec![Value::Text("beta".into())], vec![Value::Uint(1)]),
                (vec![Value::Text("gamma".into())], vec![Value::Uint(1)]),
            ],
            "grouped statement SQL computed-only text projection",
        ),
    ] {
        let payload = execute_sql_statement_for_tests::<SessionSqlEntity>(&session, sql)
            .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));
        let SqlStatementResult::Grouped {
            columns,
            rows,
            row_count,
            next_cursor,
            ..
        } = payload
        else {
            panic!("{context} should return grouped payload");
        };

        assert_eq!(
            columns,
            expected_columns
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>(),
            "{context} should preserve grouped projection labels",
        );
        assert_eq!(row_count, 3, "{context} should preserve grouped row count");
        assert!(next_cursor.is_none(), "{context} should fully materialize");

        let actual_rows = rows
            .iter()
            .map(|row| (row.group_key().to_vec(), row.aggregate_values().to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(
            actual_rows, expected_rows,
            "{context} should preserve computed grouped row payloads",
        );
    }
}

#[test]
fn execute_sql_statement_grouped_projection_unknown_field_stays_specific() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_sql_statement_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT agge, AVG(age) FROM SessionSqlEntity GROUP BY age",
    )
    .expect_err("grouped projection typo should fail field resolution");

    assert!(
        err.to_string().contains("unknown field 'agge'"),
        "grouped projection typo should stay a field-resolution error: {err}",
    );
}

#[test]
fn execute_sql_statement_grouped_filter_alias_unknown_field_stays_specific() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_sql_statement_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, \
         COUNT(*) FILTER (WHERE total_count > 0) AS total_count \
         FROM SessionSqlEntity \
         GROUP BY age \
         ORDER BY age ASC LIMIT 10",
    )
    .expect_err("grouped FILTER alias leakage should fail field resolution before execution");

    assert!(
        err.to_string().contains("unknown field 'total_count'"),
        "grouped FILTER alias leakage should stay a field-resolution error instead of tripping executor invariants: {err}",
    );
}

#[test]
fn execute_sql_statement_grouped_filter_alias_unknown_field_inside_case_stays_specific() {
    reset_session_sql_store();
    let session = sql_session();

    let err = execute_sql_statement_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, \
         COUNT(*) FILTER ( \
           WHERE CASE \
             WHEN total_count > 0 THEN TRUE \
             ELSE FALSE \
           END \
         ) AS total_count \
         FROM SessionSqlEntity \
         GROUP BY age \
         ORDER BY age ASC LIMIT 10",
    )
    .expect_err(
        "grouped FILTER alias leakage inside CASE should fail field resolution before execution",
    );

    assert!(
        err.to_string().contains("unknown field 'total_count'"),
        "grouped FILTER alias leakage inside CASE should stay a field-resolution error instead of tripping executor invariants: {err}",
    );
}

#[test]
fn grouped_select_pagination_preserves_cursor_with_extra_group_projection_columns() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("grouped-a", 10),
            ("grouped-b", 10),
            ("grouped-c", 20),
            ("grouped-d", 30),
            ("grouped-e", 30),
        ],
    );

    let sql =
        "SELECT age, age + 1, COUNT(*) FROM SessionSqlEntity GROUP BY age ORDER BY age ASC LIMIT 1";
    let first_page = execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None)
        .expect("first grouped computed-projection page should succeed");
    assert_eq!(first_page.rows()[0].group_key()[0], Value::Uint(10));
    assert_eq!(
        first_page.rows()[0].group_key()[1].cmp_numeric(&Value::Uint(11)),
        Some(std::cmp::Ordering::Equal),
    );
    assert_eq!(first_page.rows()[0].aggregate_values(), [Value::Uint(2)]);
    let first_cursor = first_page
        .continuation_cursor()
        .expect("first grouped computed-projection page should emit cursor");

    let second_page = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        sql,
        Some(&crate::db::encode_cursor(first_cursor)),
    )
    .expect("second grouped computed-projection page should succeed");
    assert_eq!(second_page.rows()[0].group_key()[0], Value::Uint(20));
    assert_eq!(
        second_page.rows()[0].group_key()[1].cmp_numeric(&Value::Uint(21)),
        Some(std::cmp::Ordering::Equal),
    );
    assert_eq!(second_page.rows()[0].aggregate_values(), [Value::Uint(1)]);
}

#[test]
fn grouped_select_helper_equivalent_row_matrix_matches_canonical_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("alpha", 20),
            ("beta", 20),
            ("gamma", 30),
            ("delta", 40),
            ("grouped-distinct-a", 10),
            ("grouped-distinct-b", 10),
            ("grouped-distinct-c", 20),
            ("grouped-distinct-d", 30),
            ("grouped-distinct-e", 30),
            ("grouped-distinct-f", 30),
        ],
    );

    for (left_sql, right_sql, context) in [
        (
            "SELECT age years, COUNT(*) total \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY years ASC LIMIT 10",
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            "grouped ORDER BY field aliases",
        ),
        (
            "SELECT DISTINCT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            "top-level grouped SELECT DISTINCT",
        ),
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age + 1 ASC LIMIT 10",
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            "grouped ORDER BY additive group-key expressions",
        ),
        (
            "SELECT age + 1 AS next_age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY next_age ASC LIMIT 10",
            "SELECT age + 1, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            "grouped ORDER BY additive computed aliases",
        ),
        (
            "SELECT age, COUNT(*) AS total_count \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY total_count + 1 DESC, age ASC LIMIT 10",
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY COUNT(*) + 1 DESC, age ASC LIMIT 10",
            "grouped ORDER BY aggregate aliases inside larger arithmetic expressions",
        ),
    ] {
        assert_grouped_row_equivalence_case(&session, left_sql, right_sql, context);
    }
}

#[test]
fn grouped_select_allows_post_aggregate_projection_expressions() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(&session, &[("alpha", 10), ("bravo", 10), ("charlie", 20)]);

    let execution = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, COUNT(*) + MAX(age), ROUND(AVG(age), 2) \
         FROM SessionSqlEntity \
         GROUP BY age \
         ORDER BY age ASC LIMIT 10",
        None,
    )
    .expect("grouped post-aggregate projection expressions should execute");

    assert!(
        execution.continuation_cursor().is_none(),
        "grouped post-aggregate projection expressions should fully materialize under LIMIT 10",
    );
    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (
                Value::Uint(10),
                vec![
                    Value::Decimal(crate::types::Decimal::from_u128(12).expect("12 decimal"),),
                    Value::Decimal(crate::types::Decimal::new(1000, 2)),
                ],
            ),
            (
                Value::Uint(20),
                vec![
                    Value::Decimal(crate::types::Decimal::from_u128(21).expect("21 decimal"),),
                    Value::Decimal(crate::types::Decimal::new(2000, 2)),
                ],
            ),
        ],
        "grouped post-aggregate projection expressions should materialize computed aggregate outputs on the aggregate side",
    );
}

#[test]
fn grouped_statement_sql_preserves_fixed_scale_for_post_aggregate_round_projection() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(&session, &[("alpha", 12), ("bravo", 12), ("charlie", 14)]);

    let payload = execute_sql_statement_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, ROUND(AVG(age), 4) \
         FROM SessionSqlEntity \
         GROUP BY age \
         ORDER BY age ASC LIMIT 10",
    )
    .expect("grouped ROUND projection statement SQL should execute");

    let SqlStatementResult::Grouped {
        columns,
        fixed_scales,
        rows,
        row_count,
        next_cursor,
    } = payload
    else {
        panic!("grouped ROUND projection statement SQL should return grouped payload");
    };

    assert_eq!(
        columns,
        vec!["age".to_string(), "ROUND(AVG(age), 4)".to_string()],
        "grouped ROUND projection should preserve grouped projection labels",
    );
    assert_eq!(
        fixed_scales,
        vec![None, Some(4)],
        "grouped ROUND projection should carry fixed display scale into grouped SQL packaging",
    );
    assert_eq!(
        row_count, 2,
        "grouped ROUND projection should preserve grouped row count"
    );
    assert!(
        next_cursor.is_none(),
        "grouped ROUND projection should fully materialize"
    );
    assert_eq!(
        rows.iter()
            .map(|row| (row.group_key().to_vec(), row.aggregate_values().to_vec()))
            .collect::<Vec<_>>(),
        vec![
            (
                vec![Value::Uint(12)],
                vec![Value::Decimal(crate::types::Decimal::new(120_000, 4))],
            ),
            (
                vec![Value::Uint(14)],
                vec![Value::Decimal(crate::types::Decimal::new(140_000, 4))],
            ),
        ],
        "grouped ROUND projection should preserve rounded decimal payload values",
    );
}

#[test]
fn grouped_select_reuses_repeated_aggregate_leaf_outputs() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(&session, &[("alpha", 10), ("bravo", 10), ("charlie", 20)]);

    let execution = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, COUNT(*) + COUNT(*)          FROM SessionSqlEntity          GROUP BY age          ORDER BY age ASC LIMIT 10",
        None,
    )
    .expect("grouped repeated aggregate leaf projection expressions should execute");

    assert!(
        execution.continuation_cursor().is_none(),
        "grouped repeated aggregate leaf projection expressions should fully materialize under LIMIT 10",
    );
    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (
                Value::Uint(10),
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(4).expect("4 decimal"),
                )],
            ),
            (
                Value::Uint(20),
                vec![Value::Decimal(
                    crate::types::Decimal::from_u128(2).expect("2 decimal"),
                )],
            ),
        ],
        "grouped repeated aggregate leaf projection expressions should reuse one grouped aggregate output slot",
    );
}

#[test]
fn grouped_select_allows_searched_case_projection_and_having() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(&session, &[("alpha", 10), ("bravo", 10), ("charlie", 20)]);

    let projection_execution = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, CASE WHEN COUNT(*) > 1 THEN 'multi' ELSE 'single' END \
         FROM SessionSqlEntity \
         GROUP BY age \
         ORDER BY age ASC LIMIT 10",
        None,
    )
    .expect("grouped searched CASE projection should execute");

    assert_eq!(
        grouped_result_rows(&projection_execution),
        vec![
            (Value::Uint(10), vec![Value::Text("multi".to_string())],),
            (Value::Uint(20), vec![Value::Text("single".to_string())],),
        ],
        "grouped searched CASE projection should evaluate over finalized grouped outputs",
    );

    let having_execution = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, COUNT(*) \
         FROM SessionSqlEntity \
         GROUP BY age \
         HAVING CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1 \
         ORDER BY age ASC LIMIT 10",
        None,
    )
    .expect("grouped searched CASE HAVING should execute");

    assert_eq!(
        grouped_result_rows(&having_execution),
        vec![(Value::Uint(10), vec![Value::Uint(2)])],
        "grouped searched CASE HAVING should filter on finalized grouped outputs",
    );
}

#[test]
fn grouped_select_allows_post_aggregate_having_expressions() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(&session, &[("alpha", 10), ("bravo", 10), ("charlie", 20)]);

    let execution = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, COUNT(*) \
         FROM SessionSqlEntity \
         GROUP BY age \
         HAVING ROUND(AVG(age), 2) >= 10 AND COUNT(*) + 1 > 1 \
         ORDER BY age ASC LIMIT 10",
        None,
    )
    .expect("grouped post-aggregate HAVING expressions should execute");

    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (Value::Uint(10), vec![Value::Uint(2)]),
            (Value::Uint(20), vec![Value::Uint(1)]),
        ],
        "grouped post-aggregate HAVING expressions should filter on finalized grouped outputs",
    );
}

#[test]
fn grouped_select_allows_post_aggregate_having_aliases() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("having-alias-a", 10),
            ("having-alias-b", 10),
            ("having-alias-c", 20),
            ("having-alias-d", 20),
            ("having-alias-e", 20),
        ],
    );

    let execution = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, SUM(CASE WHEN age > 10 THEN 1 ELSE 0 END) AS high_count \
         FROM SessionSqlEntity \
         GROUP BY age \
         HAVING high_count > 2 \
         ORDER BY age ASC LIMIT 10",
        None,
    )
    .expect("grouped HAVING aggregate aliases should execute");

    assert_eq!(
        grouped_result_rows(&execution),
        vec![(
            Value::Uint(20),
            vec![Value::Decimal(
                crate::types::Decimal::from_u128(3).expect("3 decimal"),
            )],
        )],
        "grouped HAVING aggregate aliases should filter on the same finalized post-aggregate value as the canonical expression form",
    );
}

#[test]
fn grouped_select_executes_aggregate_input_expressions() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("alpha", 10), ("bravo", 10), ("charlie", 20)]);

    let execution = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, AVG(age + 1) \
         FROM SessionSqlEntity \
         GROUP BY age \
         ORDER BY age ASC LIMIT 10",
        None,
    )
    .expect("grouped aggregate input expressions should execute once grouped runtime widens");

    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (
                Value::Uint(10),
                vec![Value::Decimal(crate::types::Decimal::new(110_000, 4))],
            ),
            (
                Value::Uint(20),
                vec![Value::Decimal(crate::types::Decimal::new(210_000, 4))],
            ),
        ],
        "grouped aggregate input expressions should execute over per-row values before grouped reduction",
    );
}

#[test]
fn grouped_select_repeated_aggregate_input_leaves_reuse_one_grouped_output_slot() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("alpha", 10), ("bravo", 10), ("charlie", 20)]);

    let execution = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, AVG(age + 1) + AVG(age + 1) \
         FROM SessionSqlEntity \
         GROUP BY age \
         ORDER BY age ASC LIMIT 10",
        None,
    )
    .expect("repeated grouped aggregate-input leaves should execute once grouped runtime widens");

    assert_eq!(
        grouped_result_rows(&execution),
        vec![
            (
                Value::Uint(10),
                vec![Value::Decimal(crate::types::Decimal::new(220_000, 4))],
            ),
            (
                Value::Uint(20),
                vec![Value::Decimal(crate::types::Decimal::new(420_000, 4))],
            ),
        ],
        "repeated grouped aggregate-input leaves should reuse one grouped aggregate output slot instead of changing grouped result semantics",
    );
}

#[test]
fn grouped_select_rejects_non_preserving_computed_order() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(&session, &[("alpha", 10), ("beta", 20), ("gamma", 30)]);

    let err = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT age, COUNT(*) \
         FROM SessionSqlEntity \
         GROUP BY age \
         ORDER BY age + age ASC LIMIT 10",
        None,
    )
    .expect_err("grouped non-preserving computed ORDER BY should stay fail-closed");

    assert!(matches!(
        err,
        QueryError::Plan(inner)
            if matches!(
                inner.as_ref(),
                crate::db::query::plan::validate::PlanError::Policy(policy)
                    if matches!(
                        policy.as_ref(),
                        crate::db::query::plan::validate::PlanPolicyError::Group(group)
                            if matches!(
                                group.as_ref(),
                                crate::db::query::plan::validate::GroupPlanError::OrderExpressionNotAdmissible { term } if term == "age + age"
                            )
                    )
            )
    ));
}

#[test]
fn grouped_select_additive_desc_order_preserves_rows_and_cursor_progression() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("alpha", 10),
            ("beta", 10),
            ("gamma", 20),
            ("delta", 30),
            ("epsilon", 30),
            ("zeta", 40),
        ],
    );

    let sql = "SELECT age, age + 1, COUNT(*) FROM SessionSqlEntity GROUP BY age ORDER BY age + 1 DESC LIMIT 2";
    let first_page = execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None)
        .expect("first grouped computed-desc page should succeed");

    assert_eq!(first_page.rows()[0].group_key()[0], Value::Uint(40));
    assert_eq!(first_page.rows()[1].group_key()[0], Value::Uint(30));

    let first_cursor = crate::db::encode_cursor(
        first_page
            .continuation_cursor()
            .expect("first grouped computed-desc page should emit cursor"),
    );
    let second_page = execute_grouped_select_for_tests::<SessionSqlEntity>(
        &session,
        sql,
        Some(first_cursor.as_str()),
    )
    .expect("second grouped computed-desc page should succeed");

    assert_eq!(second_page.rows()[0].group_key()[0], Value::Uint(20));
    assert_eq!(second_page.rows()[1].group_key()[0], Value::Uint(10));
    assert!(
        second_page.continuation_cursor().is_none(),
        "second grouped computed-desc page should fully exhaust the grouped result set",
    );
}

#[test]
fn explain_sql_grouped_qualified_identifier_matrix_matches_unqualified_output() {
    reset_session_sql_store();
    let session = sql_session();

    let cases = [
        (
            "grouped logical explain",
            "EXPLAIN SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age DESC LIMIT 2 OFFSET 1",
            "EXPLAIN SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             GROUP BY age \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
        ),
        (
            "grouped execution explain",
            "EXPLAIN EXECUTION SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age DESC LIMIT 2 OFFSET 1",
            "EXPLAIN EXECUTION SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             GROUP BY age \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
        ),
        (
            "grouped json explain",
            "EXPLAIN JSON SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 21 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age DESC LIMIT 2 OFFSET 1",
            "EXPLAIN JSON SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             WHERE age >= 21 \
             GROUP BY age \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
        ),
    ];

    for (context, qualified_sql, unqualified_sql) in cases {
        assert_grouped_qualified_identifier_explain_case(
            &session,
            qualified_sql,
            unqualified_sql,
            context,
        );
    }
}

#[cfg(feature = "diagnostics")]
#[test]
fn execute_sql_grouped_query_with_attribution_reports_grouped_phase_split() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("ada", 21), ("bob", 21), ("carol", 32)]);

    let (_result, attribution) = session
        .execute_sql_query_with_attribution::<SessionSqlEntity>(
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age ORDER BY age LIMIT 10",
        )
        .expect("grouped SQL attribution query should execute");

    assert!(
        attribution.executor_local_instructions
            >= attribution
                .grouped_stream_local_instructions
                .saturating_add(attribution.grouped_fold_local_instructions)
                .saturating_add(attribution.grouped_finalize_local_instructions),
        "grouped SQL executor totals should remain at least as large as the grouped phase split",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn execute_fluent_grouped_query_with_attribution_reports_grouped_phase_split() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_sql_entities(&session, &[("ada", 21), ("bob", 21), ("carol", 32)]);

    let query = session
        .load::<SessionSqlEntity>()
        .group_by("age")
        .expect("group_by(age) should resolve")
        .aggregate(crate::db::count())
        .order_term(crate::db::asc("age"))
        .limit(10);
    let (_result, attribution) = session
        .execute_query_result_with_attribution(query.query())
        .expect("grouped fluent attribution query should execute");

    assert_eq!(
        attribution.runtime_local_instructions,
        attribution
            .grouped_stream_local_instructions
            .saturating_add(attribution.grouped_fold_local_instructions),
        "grouped fluent runtime totals should equal grouped stream plus fold work",
    );
    assert_eq!(
        attribution.finalize_local_instructions, attribution.grouped_finalize_local_instructions,
        "grouped fluent finalize totals should equal the grouped finalize phase",
    );
    assert_eq!(
        attribution.direct_data_row_scan_local_instructions, 0,
        "grouped fluent attribution should not populate scalar direct-row counters",
    );
}
