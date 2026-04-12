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
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "{context} should expose the covering-read route label",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "{context} should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "{context} should expose the concrete equality-prefix value",
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
fn execute_sql_projection_filtered_composite_order_only_covering_query_returns_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset where
    // the guarded `tier = 'gold'` equality prefix should expose one ordered
    // `handle` suffix window without needing an extra bounded text predicate.
    seed_filtered_composite_order_fixture(&session);

    // Phase 2: require the projection lane to return only the guarded
    // equality-prefix subset under the `ORDER BY handle, id` suffix shape.
    let sql = "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect("filtered composite order-only covering projection query should execute");

    assert_eq!(
        projected_rows,
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
        "guarded filtered composite order-only queries should return only rows admitted by the equality-prefix filtered window",
    );
}

#[test]
fn execute_sql_projection_filtered_composite_order_only_desc_covering_query_returns_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset where
    // reverse traversal still depends on the same guarded `tier = 'gold'`
    // equality prefix before ordering by the `handle` suffix.
    seed_filtered_composite_order_fixture(&session);

    // Phase 2: require reverse ordered projection rows from the same guarded
    // equality-prefix composite window.
    let sql = "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect(
            "descending filtered composite order-only covering projection query should execute",
        );

    assert_eq!(
        projected_rows,
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
        "descending guarded filtered composite order-only queries should return the reverse equality-prefix window",
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
        let descriptor = session
            .query_from_sql::<FilteredIndexedSessionSqlEntity>(sql)
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
            assert_eq!(
                descriptor.node_properties().get("prefix_len"),
                Some(&Value::Uint(1)),
                "{context} should report one equality-prefix slot",
            );
            assert_eq!(
                descriptor.node_properties().get("prefix_values"),
                Some(&Value::List(vec![Value::Text("gold".to_string())])),
                "{context} should expose the concrete equality-prefix value",
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
                Some(&Value::Uint(1)),
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
