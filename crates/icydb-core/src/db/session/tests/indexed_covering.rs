use super::*;

// Seed the canonical ordered secondary-index fixture for direct covering
// `ORDER BY name` tests.
fn seed_indexed_covering_order_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_indexed_session_sql_entities(
        session,
        &[("carol", 10), ("alice", 20), ("bob", 30), ("dora", 40)],
    );
}

// Assert the shared covering index-range contract for both plain and filtered
// order-only covering routes.
fn assert_covering_index_range_descriptor<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister> + crate::traits::EntityValue,
{
    let descriptor = session
        .query_from_sql::<E>(sql)
        .expect("order-only covering SQL query should lower")
        .explain_execution()
        .expect("order-only covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "{context} should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "{context} should keep the explicit covering-read route",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "{context} should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "{context} should report access-satisfied ordering",
    );
}

// Enumerate the small set of projected row shapes used by the covering
// projection parity checks in this file.
#[derive(Clone, Copy)]
enum CoveringProjectionShape {
    IdAndName,
    NameOnly,
}

// Assert that one SQL surface keeps projection rows in parity with the entity
// lane after projecting one explicit set of fields.
fn assert_projection_matches_entity_rows(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    shape: CoveringProjectionShape,
    context: &str,
) {
    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(session, sql)
        .unwrap_or_else(|err| panic!("{context} projection query should execute: {err:?}"));
    let entity_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(sql)
        .unwrap_or_else(|err| panic!("{context} entity query should execute: {err:?}"));
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| match shape {
            CoveringProjectionShape::IdAndName => vec![
                Value::Ulid(row.entity_ref().id),
                Value::Text(row.entity_ref().name.clone()),
            ],
            CoveringProjectionShape::NameOnly => {
                vec![Value::Text(row.entity_ref().name.clone())]
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(
        entity_projected_rows, projected_rows,
        "{context} should keep projection and entity lanes in parity",
    );
}

#[test]
fn execute_sql_projection_index_covering_matrix_matches_entity_rows() {
    reset_indexed_session_sql_store();
    // Phase 1: run one equality-prefix and two order-only covering shapes
    // through the same projection/entity parity contract.
    for (seed, sql, context, shape) in [
        (
            "equality_prefix",
            "SELECT id, name FROM IndexedSessionSqlEntity WHERE name = 'alice' ORDER BY id LIMIT 1",
            "index-covered equality-prefix projection",
            CoveringProjectionShape::IdAndName,
        ),
        (
            "order_only",
            "SELECT name FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 1",
            "secondary-order covering projection",
            CoveringProjectionShape::NameOnly,
        ),
        (
            "order_only",
            "SELECT name FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 2 OFFSET 1",
            "secondary-order covering projection page",
            CoveringProjectionShape::NameOnly,
        ),
    ] {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();

        match seed {
            "equality_prefix" => seed_indexed_session_sql_entities(
                &session,
                &[("alice", 10), ("alice", 20), ("bob", 30), ("carol", 40)],
            ),
            "order_only" => seed_indexed_covering_order_fixture(&session),
            other => panic!("unexpected covering seed family: {other}"),
        }

        assert_projection_matches_entity_rows(&session, sql, shape, context);
    }
}

#[test]
fn session_explain_execution_covering_query_matrix_uses_index_range_access() {
    // Phase 1: run both the plain and filtered covering shapes through the
    // same root route contract.
    for case in ["plain", "filtered"] {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();

        match case {
            "plain" => {
                seed_indexed_covering_order_fixture(&session);
                assert_covering_index_range_descriptor::<IndexedSessionSqlEntity>(
                    &session,
                    "SELECT name FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 1",
                    "order-only single-field secondary queries",
                );
            }
            "filtered" => {
                seed_filtered_indexed_session_sql_entities(
                    &session,
                    &[
                        (9_201, "amber", false, 10),
                        (9_202, "bravo", true, 20),
                        (9_203, "charlie", true, 30),
                        (9_204, "delta", false, 40),
                    ],
                );
                assert_covering_index_range_descriptor::<FilteredIndexedSessionSqlEntity>(
                    &session,
                    "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
                    "guarded filtered-order queries",
                );
            }
            other => panic!("unexpected covering explain case: {other}"),
        }
    }
}

#[test]
fn execute_sql_projection_order_only_filtered_covering_query_returns_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset where the
    // lexicographically earliest row is inactive so the guarded query can only
    // stay correct if it respects the filtered-index predicate.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "charlie", true, 30),
            (9_204, "delta", false, 40),
        ],
    );

    // Phase 2: require the projection lane to return only the guarded active
    // subset under the order-only `ORDER BY name, id` shape.
    let sql = "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY name ASC, id ASC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect("filtered order-only covering projection query should execute");

    assert_eq!(
        projected_rows,
        vec![
            vec![Value::Text("bravo".to_string())],
            vec![Value::Text("charlie".to_string())],
        ],
        "guarded order-only covering queries should return only rows admitted by the filtered index predicate",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_desc_residual_query_fails_closed_before_top_n() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite dataset where the
    // descending `handle` order uses the `tier, handle` index, but the extra
    // `age >= 20` predicate must remain residual.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "amber", false, "gold", "bramble", 10),
            (9_222, "bravo", true, "gold", "bravo", 20),
            (9_223, "charlie", true, "gold", "bristle", 30),
            (9_224, "delta", false, "silver", "brisk", 40),
            (9_225, "echo", true, "silver", "Brisk", 50),
        ],
    );

    // Phase 2: require the residual descending filtered composite order-only
    // shape to keep the secondary-prefix route while failing closed before
    // Top-N derivation.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND age >= 20 ORDER BY handle DESC, id DESC LIMIT 2",
        )
        .expect("descending filtered composite residual order-only SQL query should lower")
        .explain_execution()
        .expect(
            "descending filtered composite residual order-only SQL explain_execution should succeed",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "descending filtered composite residual order-only queries should keep the filtered index-prefix root",
    );
    assert_eq!(
        descriptor.execution_mode(),
        crate::db::ExplainExecutionMode::Materialized,
        "descending filtered composite residual order-only queries should stay materialized",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(false),
        "descending filtered composite residual order-only projections should materialize rows because the residual filter needs non-index fields",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::ResidualPredicateFilter
        )
        .is_some(),
        "descending filtered composite residual order-only roots should expose the residual filter stage",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending filtered composite residual order-only roots should still report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_some(),
        "descending filtered composite residual order-only roots should fail closed to a materialized sort",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "descending filtered composite residual order-only roots must not derive Top-N seek",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_none(),
        "descending filtered composite residual order-only roots must not report access-satisfied ordering after failing closed",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_query_without_guard_falls_back_to_full_scan() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset so the unguarded
    // order-only query would be observably wrong if it silently reused the
    // filtered secondary index without proving the guard predicate.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "charlie", true, 30),
            (9_204, "delta", false, 40),
        ],
    );

    // Phase 2: require the unguarded `ORDER BY name, id` query to stay on the
    // fail-closed full-scan path instead of silently borrowing the filtered
    // index order.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT name FROM FilteredIndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("unguarded filtered-order SQL query should lower")
        .explain_execution()
        .expect("unguarded filtered-order SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::FullScan,
        "unguarded filtered-order queries must fail closed to the full-scan root",
    );
    assert_ne!(
        descriptor.covering_scan(),
        Some(true),
        "unguarded filtered-order queries must not claim the covering-read route",
    );
}
