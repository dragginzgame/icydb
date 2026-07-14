use super::*;

// Seed the canonical filtered composite-order fixture used by the guarded
// equality-prefix order-only tests in this file.
fn seed_filtered_composite_order_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_filtered_composite_indexed_session_sql_entities(
        session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );
}

// Assert the shared index-prefix covering contract for guarded filtered
// composite order-only routes.
fn assert_filtered_composite_order_descriptor(
    descriptor: &ExplainExecutionNodeDescriptor,
    expect_access_satisfied: bool,
    context: &str,
) {
    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "{context} should stay on the shared index-prefix root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "{context} should keep the explicit covering-read route",
    );
    assert!(
        explain_execution_find_first_node(
            descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "{context} should report secondary order pushdown",
    );
    assert_eq!(
        explain_execution_find_first_node(
            descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        expect_access_satisfied,
        "{context} should keep the expected access-satisfied ordering contract",
    );
}

const fn filtered_composite_order_explain_queries() -> [(&'static str, &'static str, bool, bool); 3]
{
    [
        (
            "guarded filtered composite-order queries",
            "SELECT id, tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
            true,
            false,
        ),
        (
            "descending guarded filtered composite-order queries",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
            false,
            false,
        ),
        (
            "descending filtered composite order-only offset roots",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
            false,
            true,
        ),
    ]
}

#[test]
fn execute_sql_projection_filtered_composite_order_only_matrix_returns_guarded_rows() {
    let cases = [
        (
            "ascending filtered composite order-only covering projection query",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
            vec![
                vec![
                    Value::Text("gold".to_string()),
                    Value::Text("bravo".to_string()),
                ],
                vec![
                    Value::Text("gold".to_string()),
                    Value::Text("bristle".to_string()),
                ],
            ],
        ),
        (
            "descending filtered composite order-only covering projection query",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
            vec![
                vec![
                    Value::Text("gold".to_string()),
                    Value::Text("charlie".to_string()),
                ],
                vec![
                    Value::Text("gold".to_string()),
                    Value::Text("bristle".to_string()),
                ],
            ],
        ),
    ];

    for (context, sql, expected_rows) in cases {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();

        // Phase 1: seed one deterministic filtered composite-index dataset so
        // the guarded `tier = 'gold'` equality prefix exposes one ordered suffix window.
        seed_filtered_composite_order_fixture(&session);

        // Phase 2: require the projection lane to return only the guarded
        // equality-prefix subset under that ordered `handle, id` suffix shape.
        let projected_rows =
            statement_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| panic!("{context} should execute: {err}"));

        assert_eq!(
            projected_rows, expected_rows,
            "{context} should preserve the guarded equality-prefix window",
        );
    }
}

#[test]
fn filtered_composite_order_only_pushdown_matches_forced_full_scan_fallback() {
    let cases = [
        (
            "ascending equality-prefix ordered suffix",
            "SELECT id, tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 3",
        ),
        (
            "descending equality-prefix ordered suffix",
            "SELECT id, tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 3",
        ),
    ];

    for (context, sql) in cases {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();
        seed_filtered_composite_order_fixture(&session);

        let pushed_query =
            lower_select_query_for_tests::<FilteredIndexedSessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| panic!("{context} should lower with indexes: {err:?}"));
        let pushed_descriptor = session
            .explain_query_execution_with_visible_indexes(&pushed_query)
            .unwrap_or_else(|err| panic!("{context} should explain with indexes: {err:?}"));
        assert_eq!(
            pushed_descriptor.node_type(),
            ExplainExecutionNodeType::IndexPrefixScan,
            "{context} should use the equality-prefix index route before fallback",
        );
        let pushed_rows =
            statement_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| panic!("{context} should execute with indexes: {err:?}"));

        hide_indexed_session_indexes();

        let fallback_query =
            lower_select_query_for_tests::<FilteredIndexedSessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| {
                    panic!("{context} should lower with hidden indexes: {err:?}")
                });
        let fallback_descriptor = session
            .explain_query_execution_with_visible_indexes(&fallback_query)
            .unwrap_or_else(|err| panic!("{context} should explain with hidden indexes: {err:?}"));
        assert!(
            explain_execution_find_first_node(
                &fallback_descriptor,
                ExplainExecutionNodeType::FullScan
            )
            .is_some(),
            "{context} hidden-index route should fall back to a full scan:\n{}",
            fallback_descriptor.render_text_tree(),
        );
        assert!(
            explain_execution_find_first_node(
                &fallback_descriptor,
                ExplainExecutionNodeType::OrderByMaterializedSort
            )
            .is_some(),
            "{context} hidden-index route should retain materialized ordering:\n{}",
            fallback_descriptor.render_text_tree(),
        );
        let fallback_rows =
            statement_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| {
                    panic!("{context} should execute with hidden indexes: {err:?}")
                });

        assert_eq!(
            pushed_rows, fallback_rows,
            "{context} pushdown route must match forced full-scan fallback row identity and order",
        );
    }
}

#[cfg(feature = "diagnostics")]
#[test]
fn execute_sql_projection_filtered_composite_order_only_pushdown_keeps_reads_bounded() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_filtered_composite_order_fixture(&session);
    let sql = "SELECT tier, handle \
               FROM FilteredIndexedSessionSqlEntity \
               WHERE active = true AND tier = 'gold' \
               ORDER BY handle ASC, id ASC \
               LIMIT 2";

    let (result, attribution) = session
        .execute_trusted_sql_query_with_attribution::<FilteredIndexedSessionSqlEntity>(sql)
        .unwrap_or_else(|err| {
            panic!("filtered composite order-only query should execute: {err:?}")
        });
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("filtered composite order-only query should return projection rows");
    };

    assert_eq!(
        rows.iter()
            .map(|row| runtime_outputs(row))
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bravo".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bristle".to_string()),
            ],
        ],
        "filtered composite order-only query should preserve the guarded ordered suffix",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "covering filtered composite order-only query should not hydrate row-store records",
    );
    assert_eq!(
        attribution.index_store_range_scan_calls, 1,
        "filtered composite order-only query should use one bounded index range",
    );
    assert!(
        attribution.index_store_entry_reads <= 3,
        "filtered composite order-only query should read no more than LIMIT plus lookahead entries, got {attribution:?}",
    );
}

#[test]
fn session_explain_execution_filtered_composite_order_matrix_is_stable() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset so the
    // guarded equality-prefix covering and materialized-boundary variants all
    // share the same route family under one matrix.
    seed_filtered_composite_order_fixture(&session);

    // Phase 2: keep the covering vs boundary distinction explicit while
    // removing the repetitive one-wrapper-per-query shape.
    for (context, sql, expect_access_satisfied, expect_offset_boundary) in
        filtered_composite_order_explain_queries()
    {
        let descriptor =
            lower_select_query_for_tests::<FilteredIndexedSessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
                .explain_execution()
                .unwrap_or_else(|err| {
                    panic!("{context} SQL explain_execution should succeed: {err:?}")
                });

        if expect_offset_boundary {
            assert_eq!(
                descriptor.node_type(),
                ExplainExecutionNodeType::IndexPrefixScan,
                "{context} should keep the shared index-prefix root",
            );
            assert_eq!(
                descriptor.execution_mode(),
                crate::db::ExplainExecutionMode::Materialized,
                "{context} should stay on the materialized boundary",
            );
            assert_eq!(
                descriptor.covering_scan(),
                Some(true),
                "{context} projections should keep the explicit covering-read route",
            );
            assert!(
                explain_execution_find_first_node(
                    &descriptor,
                    ExplainExecutionNodeType::SecondaryOrderPushdown
                )
                .is_some(),
                "{context} should still report secondary order pushdown",
            );
            assert!(
                explain_execution_find_first_node(
                    &descriptor,
                    ExplainExecutionNodeType::OrderByMaterializedSort
                )
                .is_some(),
                "{context} should stay on the materialized boundary sort contract",
            );
            assert!(
                explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
                    .is_none(),
                "{context} must not derive Top-N seek",
            );
            assert!(
                explain_execution_find_first_node(
                    &descriptor,
                    ExplainExecutionNodeType::OrderByAccessSatisfied
                )
                .is_none(),
                "{context} must not report direct access-satisfied ordering",
            );
            let limit_node = explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::LimitOffset,
            )
            .unwrap_or_else(|| panic!("{context} should expose one limit node"));
            assert_eq!(
                limit_node.node_properties().get("offset"),
                Some(&Value::Nat64(1)),
                "{context} should expose the retained offset window",
            );
        } else {
            assert_filtered_composite_order_descriptor(
                &descriptor,
                expect_access_satisfied,
                context,
            );
        }
    }
}
