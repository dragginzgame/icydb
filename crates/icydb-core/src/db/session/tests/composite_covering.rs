use super::*;
use crate::db::session::sql::with_sql_projection_materialization_metrics;

#[test]
fn session_explain_execution_order_only_composite_covering_matrix_uses_index_range_access() {
    let cases = [
        (
            "ascending composite order-only covering SQL query",
            vec![
                (9_221_u128, "alpha", 2),
                (9_222, "alpha", 1),
                (9_223, "beta", 1),
            ],
            "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 2",
        ),
        (
            "descending composite order-only covering SQL query",
            vec![
                (9_231_u128, "alpha", 2),
                (9_232, "alpha", 1),
                (9_233, "beta", 1),
            ],
            "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code DESC, serial DESC, id DESC LIMIT 2",
        ),
    ];

    for (context, seed_rows, sql) in cases {
        reset_indexed_session_sql_store();
        let session = indexed_sql_session();

        // Phase 1: seed one deterministic composite-index dataset so the SQL lane
        // proves planner-selected order-only access on the live `(code, serial)` index.
        seed_composite_indexed_session_sql_entities(&session, seed_rows.as_slice());

        // Phase 2: require EXPLAIN EXECUTION to surface the shared order-only
        // composite index-range root and covering-read route.
        let descriptor =
            lower_select_query_for_tests::<CompositeIndexedSessionSqlEntity>(&session, sql)
                .unwrap_or_else(|err| panic!("{context} should lower: {err}"))
                .explain_execution()
                .unwrap_or_else(|err| panic!("{context} should explain_execution: {err}"));

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
        let projection_node =
            explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
                .unwrap_or_else(|| panic!("{context} should emit a covering-read node"));
        assert_eq!(
            projection_node.node_properties().get("existing_row_mode"),
            Some(&Value::Text("planner_proven".to_string())),
            "{context} should inherit the planner-proven covering mode",
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
}

#[test]
fn execute_sql_projection_index_coverable_multi_component_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset so the SQL
    // projection lane must decode both indexed components from one secondary
    // `(code, serial)` access path.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_201_u128, "alpha", 2),
            (9_202, "alpha", 1),
            (9_203, "beta", 1),
        ],
    );

    // Phase 2: verify the projection lane returns the same `(id, code,
    // serial)` rows as the entity lane for a direct composite covering query.
    let sql = "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 2";
    let projected_rows =
        statement_projection_rows::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("multi-component covering projection query should execute");
    let entity_rows =
        execute_scalar_select_for_tests::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("multi-component covering entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.entity_ref().id),
                Value::Text(row.entity_ref().code.clone()),
                Value::Uint(row.entity_ref().serial),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
}

#[test]
fn execute_sql_projection_hybrid_covering_projection_mixes_covering_and_row_fields() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset so the SQL
    // projection lane can read `code` and `serial` from the covering index
    // while sparse-decoding `note` from row storage.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_301_u128, "alpha", 2),
            (9_302, "alpha", 1),
            (9_303, "beta", 1),
        ],
    );

    // Phase 2: require the SQL projection lane to preserve row parity while
    // taking the dedicated hybrid covering path instead of the generic
    // structural row materialization path.
    let sql = "SELECT id, code, serial, note FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 2";
    let (projected_rows, metrics) = with_sql_projection_materialization_metrics(|| {
        statement_projection_rows::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("hybrid composite covering projection query should execute")
    });
    let entity_rows =
        execute_scalar_select_for_tests::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("hybrid composite covering entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.entity_ref().id),
                Value::Text(row.entity_ref().code.clone()),
                Value::Uint(row.entity_ref().serial),
                Value::Text(row.entity_ref().note.clone()),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
    assert_eq!(
        metrics.hybrid_covering_path_hits, 1,
        "hybrid composite covering projection should use the SQL-side mixed covering path",
    );
    assert_eq!(
        metrics.hybrid_covering_row_field_accesses, 2,
        "hybrid composite covering projection should cap sparse row-backed field reads to the final SQL page window when index order already satisfies the query order",
    );
    assert_eq!(
        metrics.data_rows_path_hits, 0,
        "hybrid composite covering projection should bypass the generic data-row path",
    );
    assert_eq!(
        metrics.slot_rows_path_hits, 0,
        "hybrid composite covering projection should bypass retained slot rows",
    );
}

#[test]
fn execute_sql_projection_hybrid_covering_projection_skips_offset_before_index_projection() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset where the query
    // has to skip the first index-ordered row before emitting the LIMIT window.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_321_u128, "alpha", 2),
            (9_322, "alpha", 1),
            (9_323, "beta", 1),
        ],
    );

    // Phase 2: require row parity with the entity path while proving the
    // hybrid covering projector does not decode/project index components for
    // rows discarded by OFFSET after row-presence filtering.
    let sql = "SELECT id, code, serial, note FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 1 OFFSET 1";
    let (projected_rows, metrics) = with_sql_projection_materialization_metrics(|| {
        statement_projection_rows::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("offset hybrid composite covering projection query should execute")
    });
    let entity_rows =
        execute_scalar_select_for_tests::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("offset hybrid composite covering entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.entity_ref().id),
                Value::Text(row.entity_ref().code.clone()),
                Value::Uint(row.entity_ref().serial),
                Value::Text(row.entity_ref().note.clone()),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
    assert_eq!(
        metrics.hybrid_covering_path_hits, 1,
        "offset hybrid covering projection should use the SQL-side mixed covering path",
    );
    assert_eq!(
        metrics.hybrid_covering_row_field_accesses, 1,
        "offset hybrid covering projection should materialize row-backed projected field values only for retained output rows",
    );
    assert_eq!(
        metrics.hybrid_covering_index_field_accesses, 2,
        "offset hybrid covering projection should decode projected index fields only for retained output rows",
    );
}

#[test]
fn execute_sql_projection_hybrid_covering_projection_admits_pk_plus_row_field_only() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset so the SQL
    // projection lane can satisfy ordering from the `(code, serial)` index
    // while sparse-decoding only the uncovered `note` field from row storage.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_311_u128, "alpha", 2),
            (9_312, "alpha", 1),
            (9_313, "beta", 1),
        ],
    );

    // Phase 2: prove the SQL projection lane admits the sparse row-backed
    // path even when no projected index component is returned to the caller.
    let sql = "SELECT id, note FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 2";
    let (projected_rows, metrics) = with_sql_projection_materialization_metrics(|| {
        statement_projection_rows::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("pk-plus-row-field covering projection query should execute")
    });
    let entity_rows =
        execute_scalar_select_for_tests::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("pk-plus-row-field entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.entity_ref().id),
                Value::Text(row.entity_ref().note.clone()),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
    assert_eq!(
        metrics.hybrid_covering_path_hits, 1,
        "pk-plus-row-field covering projection should use the SQL-side sparse index-backed path",
    );
    assert_eq!(
        metrics.hybrid_covering_index_field_accesses, 0,
        "pk-plus-row-field covering projection should not materialize projected index component values",
    );
    assert_eq!(
        metrics.hybrid_covering_row_field_accesses, 2,
        "pk-plus-row-field covering projection should sparse-read one uncovered field per emitted row",
    );
    assert_eq!(
        metrics.data_rows_path_hits, 0,
        "pk-plus-row-field covering projection should bypass the generic data-row path",
    );
    assert_eq!(
        metrics.slot_rows_path_hits, 0,
        "pk-plus-row-field covering projection should bypass retained slot rows",
    );
}
