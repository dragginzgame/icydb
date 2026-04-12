use super::*;
use crate::db::query::explain::{ExplainExecutionNodeType, ExplainGrouping};

// Execute one indexed grouped SQL case, assert the fully materialized ordered
// grouped contract, and project rows into a compact assertion shape.
fn execute_indexed_grouped_case(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    context: &str,
) -> Vec<(Value, Vec<Value>)> {
    let execution = session
        .execute_sql_grouped::<IndexedSessionSqlEntity>(sql, None)
        .unwrap_or_else(|err| panic!("{context} SQL execution should succeed: {err}"));

    assert!(
        execution.continuation_cursor().is_none(),
        "{context} should fully materialize under LIMIT 10",
    );

    execution
        .rows()
        .iter()
        .map(|row| (row.group_key()[0].clone(), row.aggregate_values().to_vec()))
        .collect()
}

// Execute one grouped SQL dispatch case and assert the grouped payload surface
// stays stable across different projection shapes.
fn assert_grouped_dispatch_payload_case(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_columns: &[&str],
    expected_rows: &[(Value, Vec<Value>)],
    expected_row_count: u32,
    context: &str,
) {
    let payload = session
        .execute_sql_dispatch::<SessionSqlEntity>(sql)
        .unwrap_or_else(|err| panic!("{context} should execute through dispatch SQL: {err}"));

    let SqlDispatchResult::Grouped {
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
    let qualified = dispatch_explain_sql::<SessionSqlEntity>(session, qualified_sql)
        .unwrap_or_else(|err| panic!("{context} qualified SQL should succeed: {err}"));
    let unqualified = dispatch_explain_sql::<SessionSqlEntity>(session, unqualified_sql)
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
    let left = session
        .execute_sql_grouped::<SessionSqlEntity>(left_sql, None)
        .unwrap_or_else(|err| panic!("{context} left SQL should execute: {err}"));
    let right = session
        .execute_sql_grouped::<SessionSqlEntity>(right_sql, None)
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
    expect_route_outcome: bool,
) {
    let query = session
        .query_from_sql::<IndexedSessionSqlEntity>(sql)
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
    assert_eq!(
        descriptor.node_properties().get("grouped_execution_mode"),
        Some(&Value::from("ordered_materialized")),
        "{context} execution explain root should surface the ordered grouped execution strategy",
    );

    if expect_route_outcome {
        assert_eq!(
            descriptor.node_properties().get("grouped_route_outcome"),
            Some(&Value::from("materialized_fallback")),
            "{context} execution explain root should surface the grouped route outcome",
        );
    }

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
fn execute_sql_grouped_rejection_matrix_preserves_lane_boundary_messages() {
    reset_session_sql_store();
    let session = sql_session();

    for (sql, expected_message, context, expect_unsupported_variant) in [
        (
            "SELECT TRIM(name) FROM SessionSqlEntity",
            "execute_sql_grouped rejects computed text projection",
            "computed text projection",
            false,
        ),
        (
            "SELECT COUNT(*) FROM SessionSqlEntity",
            "execute_sql_grouped rejects global aggregate SELECT",
            "global aggregate execution",
            false,
        ),
        (
            "DELETE FROM SessionSqlEntity ORDER BY id LIMIT 1",
            "execute_sql_grouped rejects DELETE",
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
        let err = session
            .execute_sql_grouped::<SessionSqlEntity>(sql, None)
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
fn query_from_sql_grouped_explain_and_execution_project_grouped_fallback_publicly() {
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

    let query = session
        .query_from_sql::<SessionSqlEntity>(
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
    assert_eq!(
        descriptor.node_properties().get("grouped_route_outcome"),
        Some(&Value::from("materialized_fallback")),
        "grouped execution explain root should surface the grouped route outcome",
    );
    assert_eq!(
        descriptor.node_properties().get("grouped_execution_mode"),
        Some(&Value::from("hash_materialized")),
        "grouped execution explain root should surface the grouped execution strategy",
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
fn query_from_sql_indexed_grouped_ordered_explain_matrix_projects_ordered_group_publicly() {
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

    for (context, sql, expect_grouped_node_contract, expect_route_outcome) in cases {
        assert_indexed_grouped_ordered_public_case(
            &session,
            sql,
            context,
            expect_grouped_node_contract,
            expect_route_outcome,
        );
    }
}

// This is an intentionally table-driven grouped aggregate matrix. Keeping the
// admitted ordered grouped cases inline makes the outward SQL contract easier
// to audit than splitting them across many tiny helpers.
#[test]
#[expect(clippy::too_many_lines)]
fn execute_sql_grouped_indexed_aggregate_matrix_preserves_ordered_group_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic duplicate-free cohort for the plain
    // ordered grouped aggregate matrix.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("alpha", 10),
            ("alpha", 20),
            ("bravo", 30),
            ("charlie", 40),
            ("charlie", 50),
        ],
    );

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

    for (label, sql, expected_rows) in cases {
        let actual_rows = execute_indexed_grouped_case(&session, sql, label);

        assert_eq!(
            actual_rows, expected_rows,
            "{label} should preserve grouped-key order on the admitted ordered grouped lane",
        );
    }
}

#[test]
fn execute_sql_grouped_indexed_distinct_aggregate_matrix_preserves_ordered_group_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic duplicate-heavy cohort for the distinct
    // aggregate matrix on the ordered grouped lane.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("alpha", 10),
            ("alpha", 10),
            ("alpha", 20),
            ("bravo", 30),
            ("charlie", 40),
            ("charlie", 50),
            ("charlie", 50),
        ],
    );

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

    for (label, sql, expected_rows) in cases {
        let actual_rows = execute_indexed_grouped_case(&session, sql, label);

        assert_eq!(
            actual_rows, expected_rows,
            "{label} should preserve ordered grouped rows after per-group DISTINCT dedupe",
        );
    }
}

#[test]
fn query_from_sql_indexed_filtered_grouped_ordered_explain_matrix_projects_ordered_group_publicly()
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

    for (context, sql, expect_grouped_node_contract, expect_route_outcome) in cases {
        assert_indexed_grouped_ordered_public_case(
            &session,
            sql,
            context,
            expect_grouped_node_contract,
            expect_route_outcome,
        );
    }
}

#[test]
fn execute_sql_grouped_indexed_filtered_aggregate_matrix_preserves_ordered_group_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered cohort for the ordered grouped
    // aggregate matrix on the admitted index-backed filter path.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("alpha", 10),
            ("alpha", 20),
            ("bravo", 30),
            ("charlie", 40),
            ("delta", 50),
        ],
    );

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

    for (label, sql, expected_rows) in cases {
        let actual_rows = execute_indexed_grouped_case(&session, sql, label);

        assert_eq!(
            actual_rows, expected_rows,
            "{label} should preserve grouped-key order on the admitted ordered grouped lane",
        );
    }
}

#[test]
fn execute_sql_grouped_matrix_queries_match_expected_grouped_rows() {
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
    let cases = vec![
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec![(10_u64, 2_u64), (20_u64, 1_u64), (30_u64, 3_u64)],
        ),
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             WHERE age >= 20 \
             GROUP BY age \
             ORDER BY age ASC LIMIT 10",
            vec![(20_u64, 1_u64), (30_u64, 3_u64)],
        ),
        (
            "SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 20 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age ASC LIMIT 10",
            vec![(20_u64, 1_u64), (30_u64, 3_u64)],
        ),
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             HAVING COUNT(*) > 1 \
             ORDER BY age ASC LIMIT 10",
            vec![(10_u64, 2_u64), (30_u64, 3_u64)],
        ),
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             HAVING COUNT(*) IS NULL \
             ORDER BY age ASC LIMIT 10",
            vec![],
        ),
        (
            "SELECT age, COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY age \
             HAVING COUNT(*) IS NOT NULL \
             ORDER BY age ASC LIMIT 10",
            vec![(10_u64, 2_u64), (20_u64, 1_u64), (30_u64, 3_u64)],
        ),
    ];

    // Phase 3: assert grouped row payloads for each SQL input.
    for (sql, expected_rows) in cases {
        let execution = session
            .execute_sql_grouped::<SessionSqlEntity>(sql, None)
            .expect("grouped matrix SQL execution should succeed");
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
        let expected_values = expected_rows
            .iter()
            .map(|(group_key, count)| (Value::Uint(*group_key), Value::Uint(*count)))
            .collect::<Vec<_>>();

        assert!(
            execution.continuation_cursor().is_none(),
            "grouped matrix cases should fully materialize under LIMIT 10: {sql}",
        );
        assert_eq!(actual_rows, expected_values, "grouped matrix case: {sql}");
    }
}

#[test]
fn execute_sql_projection_rejects_grouped_aggregate_sql() {
    reset_session_sql_store();
    let session = sql_session();

    let err = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age",
    )
    .expect_err("projection row helper should reject grouped dispatch payloads");

    assert!(
        err.to_string()
            .contains("projection row dispatch only supports value-row SQL projection payloads"),
        "projection row helper must preserve its value-row-only contract for grouped payloads",
    );
}

#[test]
fn execute_sql_grouped_count_matrix_returns_expected_grouped_rows() {
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

    for (sql, context) in [
        (
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age ORDER BY age ASC LIMIT 10",
            "canonical grouped count SQL",
        ),
        (
            "SELECT SessionSqlEntity.age, COUNT(*) \
             FROM public.SessionSqlEntity \
             WHERE SessionSqlEntity.age >= 20 \
             GROUP BY SessionSqlEntity.age \
             ORDER BY SessionSqlEntity.age ASC LIMIT 10",
            "qualified grouped count SQL",
        ),
    ] {
        let execution = session
            .execute_sql_grouped::<SessionSqlEntity>(sql, None)
            .unwrap_or_else(|err| panic!("{context} should succeed: {err}"));

        assert!(
            execution.continuation_cursor().is_none(),
            "{context} should fully materialize under LIMIT 10",
        );
        assert_eq!(
            execution.rows().len(),
            2,
            "{context} should return two groups"
        );
        assert_eq!(execution.rows()[0].group_key(), [Value::Uint(20)]);
        assert_eq!(execution.rows()[0].aggregate_values(), [Value::Uint(2)]);
        assert_eq!(execution.rows()[1].group_key(), [Value::Uint(32)]);
        assert_eq!(execution.rows()[1].aggregate_values(), [Value::Uint(1)]);
    }
}

#[test]
fn execute_sql_grouped_limit_window_emits_cursor_and_resumes_next_group_page() {
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
    let first_page = session
        .execute_sql_grouped::<SessionSqlEntity>(sql, None)
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
    let second_page = session
        .execute_sql_grouped::<SessionSqlEntity>(sql, Some(cursor_one.as_str()))
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
    let third_page = session
        .execute_sql_grouped::<SessionSqlEntity>(sql, Some(cursor_two.as_str()))
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
fn execute_sql_grouped_multi_aggregate_having_offset_limit_cursor_resumes_consistently() {
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
    let first_page = session
        .execute_sql_grouped::<SessionSqlEntity>(sql, None)
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
    let second_page = session
        .execute_sql_grouped::<SessionSqlEntity>(sql, Some(first_cursor.as_str()))
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
fn execute_sql_grouped_cursor_rejection_matrix_preserves_cursor_plan_taxonomy() {
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
    let first_page = session
        .execute_sql_grouped::<SessionSqlEntity>(
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
        let err = session
            .execute_sql_grouped::<SessionSqlEntity>(sql, cursor)
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
            Some("execute_sql rejects grouped SELECT"),
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
        let err = session
            .execute_sql::<SessionSqlEntity>(sql)
            .expect_err("scalar API grouped-shape matrix should stay fail-closed");

        assert!(
            matches!(
                err,
                QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                    _
                ))
            ),
            "{context} should fail at the reduced scalar lowering boundary",
        );
        if let Some(expected_message) = expected_message {
            assert!(
                err.to_string().contains(expected_message),
                "{context} should preserve explicit grouped entrypoint guidance",
            );
        }
    }
}

// This grouped payload matrix is intentionally kept as one table-driven surface
// contract so grouped dispatch labels, rows, and aliases stay audited together.
#[expect(
    clippy::too_many_lines,
    reason = "grouped payload matrix coverage is table-driven"
)]
#[test]
fn execute_sql_dispatch_grouped_payload_matrix() {
    let cases = [
        (
            "dispatch grouped SQL",
            "SELECT age, COUNT(*) FROM SessionSqlEntity GROUP BY age ORDER BY age ASC LIMIT 10",
            vec!["age", "COUNT(*)"],
            vec![
                (Value::Uint(20), vec![Value::Uint(2)]),
                (Value::Uint(32), vec![Value::Uint(1)]),
            ],
            2u32,
            false,
        ),
        (
            "dispatch grouped aliased computed SQL",
            "SELECT TRIM(name) AS trimmed_name, COUNT(*) total \
             FROM SessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec!["trimmed_name", "total"],
            vec![
                (Value::from("alpha"), vec![Value::Uint(2)]),
                (Value::from("beta"), vec![Value::Uint(1)]),
                (Value::from("gamma"), vec![Value::Uint(1)]),
            ],
            3u32,
            false,
        ),
        (
            "dispatch grouped computed SQL",
            "SELECT TRIM(name), COUNT(*) \
             FROM SessionSqlEntity \
             GROUP BY name \
             ORDER BY name ASC LIMIT 10",
            vec!["TRIM(name)", "COUNT(*)"],
            vec![
                (Value::from("alpha"), vec![Value::Uint(2)]),
                (Value::from("beta"), vec![Value::Uint(1)]),
                (Value::from("gamma"), vec![Value::Uint(1)]),
            ],
            3u32,
            true,
        ),
    ];

    for (context, sql, expected_columns, expected_rows, expected_row_count, check_grouped_api) in
        cases
    {
        reset_session_sql_store();
        let session = sql_session();

        match context {
            "dispatch grouped SQL" => {
                session
                    .insert(SessionSqlEntity {
                        id: Ulid::generate(),
                        name: "aggregate-a".to_string(),
                        age: 20,
                    })
                    .expect("seed insert should succeed");
                session
                    .insert(SessionSqlEntity {
                        id: Ulid::generate(),
                        name: "aggregate-b".to_string(),
                        age: 20,
                    })
                    .expect("seed insert should succeed");
                session
                    .insert(SessionSqlEntity {
                        id: Ulid::generate(),
                        name: "aggregate-c".to_string(),
                        age: 32,
                    })
                    .expect("seed insert should succeed");
            }
            "dispatch grouped aliased computed SQL" | "dispatch grouped computed SQL" => {
                seed_session_sql_entities(
                    &session,
                    &[
                        (" alpha ", 20),
                        (" alpha ", 21),
                        ("beta", 30),
                        ("gamma  ", 40),
                    ],
                );
            }
            _ => unreachable!("grouped payload matrix is fixed"),
        }

        assert_grouped_dispatch_payload_case(
            &session,
            sql,
            expected_columns.as_slice(),
            expected_rows.as_slice(),
            expected_row_count,
            context,
        );

        if check_grouped_api {
            let grouped = session
                .execute_sql_grouped::<SessionSqlEntity>(sql, None)
                .unwrap_or_else(|err| {
                    panic!("{context} should execute through grouped SQL lane too: {err}")
                });
            let grouped_rows = grouped
                .rows()
                .iter()
                .map(|row| {
                    (
                        row.group_key()[0].clone(),
                        vec![row.aggregate_values()[0].clone()],
                    )
                })
                .collect::<Vec<_>>();

            assert!(
                grouped.continuation_cursor().is_none(),
                "{context} grouped SQL lane should fully materialize under LIMIT 10",
            );
            assert_eq!(
                grouped_rows, expected_rows,
                "{context} grouped SQL lane should preserve grouped key/value payloads",
            );
        }
    }
}

#[test]
fn execute_sql_grouped_equivalent_row_matrix_matches_canonical_rows() {
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
    ] {
        assert_grouped_row_equivalence_case(&session, left_sql, right_sql, context);
    }
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
