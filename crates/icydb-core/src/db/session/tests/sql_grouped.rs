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
            "grouped SELECT helper rejects scalar computed text projection",
            "computed text projection",
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
            "grouped SELECT helper rejects grouped computed text projection",
            "grouped computed text projection",
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
fn grouped_select_helper_rejects_field_to_field_predicate_in_current_slice() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let err = execute_grouped_select_for_tests::<SessionDeterministicRangeEntity>(
        &session,
        "SELECT score, COUNT(*) \
         FROM SessionDeterministicRangeEntity \
         WHERE handle > label \
         GROUP BY score \
         ORDER BY score ASC LIMIT 10",
        None,
    )
    .expect_err("grouped field-to-field predicate SQL should remain fail-closed");

    assert!(
        err.to_string()
            .contains("grouped predicates do not support field-to-field comparisons"),
        "grouped SQL helper should preserve the grouped predicate policy boundary message",
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
            "grouped statement SQL direct+computed text projection",
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
    ] {
        assert_grouped_row_equivalence_case(&session, left_sql, right_sql, context);
    }
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
                                crate::db::query::plan::validate::GroupPlanError::OrderPrefixNotAlignedWithGroupKeys
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

#[cfg(feature = "perf-attribution")]
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

#[cfg(feature = "perf-attribution")]
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
        .order_by("age")
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
