use super::*;
use crate::db::{
    ExecutionAccessPathVariant, FieldRef, PagedLoadExecution,
    session::sql::with_sql_projection_materialization_metrics,
};

type SessionKeyAgeRows = Vec<(Ulid, u64)>;
type GroupedAgeCountRows = Vec<(Value, Vec<Value>)>;

// Project one entity response into the minimal key/order assertion shape shared
// by SQL-lowered and fluent scalar convergence tests.
fn entity_response_key_age_rows(response: &EntityResponse<SessionSqlEntity>) -> SessionKeyAgeRows {
    response
        .iter()
        .map(|row| (row.id().key(), row.entity_ref().age))
        .collect()
}

// Project one paged execution response into the same key/order assertion shape.
fn paged_key_age_rows(page: &PagedLoadExecution<SessionSqlEntity>) -> SessionKeyAgeRows {
    entity_response_key_age_rows(page.response())
}

// Project one grouped execution response into `(group_key, aggregate_values)`
// rows so SQL and fluent grouped pages compare the same public payload shape.
fn grouped_age_count_rows(page: &PagedGroupedExecutionWithTrace) -> GroupedAgeCountRows {
    page.rows()
        .iter()
        .map(|row| {
            (
                runtime_output(row.group_key()[0].clone()),
                runtime_outputs(row.aggregate_values()),
            )
        })
        .collect()
}

// Insert fixed primary-key rows so delete target tests can reset and replay the
// exact same logical table before comparing mutating SQL and fluent surfaces.
fn seed_fixed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &'static str, u64)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(id, name, age)| SessionSqlEntity {
            id: Ulid::from_u128(id),
            name: name.to_string(),
            age,
        },
        "fixed convergence seed",
    );
}

// Extract one `RETURNING id` projection into the same primary-key shape emitted
// by entity responses.
fn projection_ulid_keys(rows: Vec<Vec<Value>>) -> Vec<Ulid> {
    rows.into_iter()
        .map(|row| {
            let [Value::Ulid(id)] = row.as_slice() else {
                panic!("RETURNING id should emit exactly one ULID value");
            };
            *id
        })
        .collect()
}

// Project runtime trace access families onto EXPLAIN execution-node families
// so convergence tests compare the same route fact through both observability
// surfaces without snapshot-locking every descriptor property.
const fn explain_node_type_for_trace_variant(
    variant: ExecutionAccessPathVariant,
) -> ExplainExecutionNodeType {
    match variant {
        ExecutionAccessPathVariant::ByKey => ExplainExecutionNodeType::ByKeyLookup,
        ExecutionAccessPathVariant::ByKeys => ExplainExecutionNodeType::ByKeysLookup,
        ExecutionAccessPathVariant::KeyRange => ExplainExecutionNodeType::PrimaryKeyRangeScan,
        ExecutionAccessPathVariant::IndexPrefix => ExplainExecutionNodeType::IndexPrefixScan,
        ExecutionAccessPathVariant::IndexMultiLookup => ExplainExecutionNodeType::IndexMultiLookup,
        ExecutionAccessPathVariant::IndexRange => ExplainExecutionNodeType::IndexRangeScan,
        ExecutionAccessPathVariant::FullScan => ExplainExecutionNodeType::FullScan,
        ExecutionAccessPathVariant::Union => ExplainExecutionNodeType::Union,
        ExecutionAccessPathVariant::Intersection => ExplainExecutionNodeType::Intersection,
    }
}

// Compare one EXPLAIN descriptor tree with one execution trace route family.
fn assert_explain_descriptor_matches_trace_route(
    descriptor: &ExplainExecutionNodeDescriptor,
    variant: ExecutionAccessPathVariant,
    context: &str,
) {
    let expected = explain_node_type_for_trace_variant(variant);

    assert!(
        explain_execution_find_first_node(descriptor, expected).is_some(),
        "{context}: EXPLAIN should contain the execution trace route family {expected:?}",
    );
}

// Lower, explain, and execute one SQL query, then require EXPLAIN and runtime
// trace route facts to agree on the same access family.
fn assert_sql_explain_route_matches_execution_trace<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected_variant: ExecutionAccessPathVariant,
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    let query = lower_select_query_for_tests::<E>(session, sql)
        .expect("route convergence SQL should lower");
    let explain = session
        .explain_query_execution_with_visible_indexes(&query)
        .expect("route convergence EXPLAIN descriptor should build");
    let execution = session
        .execute_load_query_paged_with_trace(&query, None)
        .expect("route convergence traced execution should run");
    let trace = execution
        .execution_trace()
        .expect("debug route convergence execution should emit a trace");

    assert_eq!(
        trace.access_path_variant(),
        expected_variant,
        "{context}: traced execution route should match the expected fixture route",
    );
    assert_explain_descriptor_matches_trace_route(&explain, trace.access_path_variant(), context);
}

#[test]
fn sql_and_fluent_scalar_execution_match_keys_order_paging_and_cursor() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed enough ordered matches to force a continuation boundary.
    seed_session_sql_entities(
        &session,
        &[
            ("scalar-conv-a", 10),
            ("scalar-conv-b", 20),
            ("scalar-conv-c", 30),
            ("scalar-conv-d", 40),
            ("scalar-conv-e", 50),
        ],
    );

    // Phase 2: compare the full scalar row order through SQL lowering and the
    // fluent entity response before exercising cursor paging.
    let sql = "SELECT * \
               FROM SessionSqlEntity \
               WHERE age >= 20 \
               ORDER BY age ASC, id ASC \
               LIMIT 2";
    let sql_query = lower_select_query_for_tests::<SessionSqlEntity>(&session, sql)
        .expect("scalar convergence SQL should lower");
    let sql_rows = execute_scalar_select_for_tests::<SessionSqlEntity>(&session, sql)
        .expect("scalar convergence SQL should execute");
    let fluent_rows = session
        .load::<SessionSqlEntity>()
        .filter(FieldRef::new("age").gte(20_u64))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .execute()
        .expect("scalar convergence fluent query should execute")
        .into_rows()
        .expect("scalar convergence fluent query should return scalar rows");
    assert_eq!(
        entity_response_key_age_rows(&sql_rows),
        entity_response_key_age_rows(&fluent_rows),
        "SQL and fluent scalar execution should emit the same keyed row order",
    );

    // Phase 3: compare initial cursor pages and require the continuation token
    // boundary to be byte-for-byte identical.
    let sql_first = session
        .execute_load_query_paged_with_trace(&sql_query, None)
        .expect("scalar convergence first SQL page should execute")
        .into_execution();
    let fluent_first = session
        .load::<SessionSqlEntity>()
        .filter(FieldRef::new("age").gte(20_u64))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .execute_paged()
        .expect("scalar convergence first fluent page should execute");
    assert_eq!(
        paged_key_age_rows(&sql_first),
        paged_key_age_rows(&fluent_first),
        "SQL and fluent first pages should emit the same keys and order",
    );
    assert_eq!(
        sql_first.continuation_cursor(),
        fluent_first.continuation_cursor(),
        "SQL and fluent first pages should mint the same continuation cursor",
    );

    // Phase 4: resume both surfaces from their cursor and compare the next page.
    let cursor = crate::db::encode_cursor(
        sql_first
            .continuation_cursor()
            .expect("first scalar page should emit a continuation cursor"),
    );
    let sql_second = session
        .execute_load_query_paged_with_trace(&sql_query, Some(cursor.as_str()))
        .expect("scalar convergence second SQL page should execute")
        .into_execution();
    let fluent_second = session
        .load::<SessionSqlEntity>()
        .filter(FieldRef::new("age").gte(20_u64))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .cursor(cursor)
        .execute_paged()
        .expect("scalar convergence second fluent page should execute");
    assert_eq!(
        paged_key_age_rows(&sql_second),
        paged_key_age_rows(&fluent_second),
        "SQL and fluent continuation pages should emit the same keys and order",
    );
    assert_eq!(
        sql_second.continuation_cursor(),
        fluent_second.continuation_cursor(),
        "SQL and fluent continuation pages should preserve the same cursor state",
    );
}

#[test]
fn sql_and_fluent_grouped_execution_match_groups_aggregates_and_cursor() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed three grouped buckets and force one group per page.
    seed_session_sql_entities(
        &session,
        &[
            ("group-conv-a", 10),
            ("group-conv-b", 10),
            ("group-conv-c", 20),
            ("group-conv-d", 30),
            ("group-conv-e", 30),
            ("group-conv-f", 30),
        ],
    );

    // Phase 2: execute the first grouped page through SQL and fluent surfaces.
    let sql = "SELECT age, COUNT(*) \
               FROM SessionSqlEntity \
               GROUP BY age \
               ORDER BY age ASC \
               LIMIT 1";
    let sql_first = execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, None)
        .expect("grouped convergence first SQL page should execute");
    let fluent_query = session
        .load::<SessionSqlEntity>()
        .group_by("age")
        .expect("grouped convergence fluent group_by should resolve")
        .aggregate(crate::db::count())
        .order_term(crate::db::asc("age"))
        .limit(1);
    let fluent_first = session
        .execute_grouped(fluent_query.query(), None)
        .expect("grouped convergence first fluent page should execute");
    assert_eq!(
        grouped_age_count_rows(&sql_first),
        grouped_age_count_rows(&fluent_first),
        "SQL and fluent grouped first pages should emit the same groups and aggregates",
    );
    assert_eq!(
        sql_first.continuation_cursor(),
        fluent_first.continuation_cursor(),
        "SQL and fluent grouped first pages should mint the same continuation cursor",
    );

    // Phase 3: resume both grouped surfaces from the same cursor and compare
    // the next grouped payload and cursor state.
    let cursor = crate::db::encode_cursor(
        sql_first
            .continuation_cursor()
            .expect("first grouped page should emit a continuation cursor"),
    );
    let sql_second =
        execute_grouped_select_for_tests::<SessionSqlEntity>(&session, sql, Some(cursor.as_str()))
            .expect("grouped convergence second SQL page should execute");
    let fluent_second = session
        .execute_grouped(fluent_query.query(), Some(cursor.as_str()))
        .expect("grouped convergence second fluent page should execute");
    assert_eq!(
        grouped_age_count_rows(&sql_second),
        grouped_age_count_rows(&fluent_second),
        "SQL and fluent grouped continuation pages should emit the same groups and aggregates",
    );
    assert_eq!(
        sql_second.continuation_cursor(),
        fluent_second.continuation_cursor(),
        "SQL and fluent grouped continuation pages should preserve the same cursor state",
    );
}

#[test]
fn delete_target_keys_match_scalar_execution_keys_for_same_predicate() {
    let rows = [
        (13_301, "delete-conv-a", 10),
        (13_302, "delete-conv-b", 20),
        (13_303, "delete-conv-c", 30),
        (13_304, "delete-conv-d", 40),
    ];

    // Phase 1: establish the non-mutating scalar key stream baseline.
    reset_session_sql_store();
    let session = sql_session();
    seed_fixed_session_sql_entities(&session, &rows);
    let scalar_keys = session
        .load::<SessionSqlEntity>()
        .filter(FieldRef::new("age").lt(30_u64))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .execute()
        .expect("delete convergence scalar baseline should execute")
        .into_rows()
        .expect("delete convergence scalar baseline should return entity rows")
        .ids()
        .map(|id| id.key())
        .collect::<Vec<_>>();

    // Phase 2: replay the same table and require SQL DELETE RETURNING to target
    // the exact scalar baseline keys.
    reset_session_sql_store();
    let session = sql_session();
    seed_fixed_session_sql_entities(&session, &rows);
    let sql_delete_keys = projection_ulid_keys(
        statement_projection_rows::<SessionSqlEntity>(
            &session,
            "DELETE FROM SessionSqlEntity \
             WHERE age < 30 \
             ORDER BY age ASC, id ASC \
             LIMIT 2 \
             RETURNING id",
        )
        .expect("delete convergence SQL DELETE RETURNING should execute"),
    );
    assert_eq!(
        sql_delete_keys, scalar_keys,
        "SQL DELETE target keys should match scalar execution keys for the same predicate",
    );

    // Phase 3: replay again and require fluent delete materialization to target
    // the same keys before future delete path unification work.
    reset_session_sql_store();
    let session = sql_session();
    seed_fixed_session_sql_entities(&session, &rows);
    let fluent_delete_keys = session
        .delete::<SessionSqlEntity>()
        .filter(FieldRef::new("age").lt(30_u64))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .limit(2)
        .execute_rows()
        .expect("delete convergence fluent delete rows should execute")
        .ids()
        .map(|id| id.key())
        .collect::<Vec<_>>();
    assert_eq!(
        fluent_delete_keys, scalar_keys,
        "fluent delete target keys should match scalar execution keys for the same predicate",
    );
}

#[test]
fn sql_distinct_projection_matches_logical_distinct_over_scalar_stream() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed duplicate projected names while keeping full rows distinct.
    seed_fixed_session_sql_entities(
        &session,
        &[
            (13_401, "distinct-alpha", 10),
            (13_402, "distinct-alpha", 20),
            (13_403, "distinct-beta", 30),
            (13_404, "distinct-beta", 40),
            (13_405, "distinct-gamma", 50),
        ],
    );

    // Phase 2: execute SQL DISTINCT and independently derive logical DISTINCT
    // from the ordinary ordered scalar stream.
    let sql_distinct_rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT DISTINCT name \
         FROM SessionSqlEntity \
         ORDER BY name ASC",
    )
    .expect("SQL DISTINCT convergence projection should execute");
    let scalar_rows = execute_scalar_select_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT * FROM SessionSqlEntity ORDER BY name ASC, id ASC",
    )
    .expect("logical DISTINCT scalar stream should execute");
    let mut logical_distinct_rows = Vec::new();
    for row in &scalar_rows {
        let value = Value::Text(row.entity_ref().name.clone());
        if !logical_distinct_rows
            .iter()
            .any(|existing: &Vec<Value>| existing[0] == value)
        {
            logical_distinct_rows.push(vec![value]);
        }
    }

    assert_eq!(
        sql_distinct_rows, logical_distinct_rows,
        "SQL DISTINCT should equal logical DISTINCT over the ordered scalar stream",
    );
}

#[test]
fn sql_distinct_projection_uses_shared_structural_execution_before_dedup() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed an indexed prefix route whose non-DISTINCT form can use a
    // SQL-side covering projection shortcut.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sable", 10),
            ("Sable", 20),
            ("Saffron", 30),
            ("Summit", 40),
            ("Atlas", 50),
        ],
    );

    // Phase 2: require DISTINCT to run through the shared scalar retained-slot
    // executor before SQL projection deduplication and final paging.
    let (projected_rows, metrics) = with_sql_projection_materialization_metrics(|| {
        statement_projection_rows::<IndexedSessionSqlEntity>(
            &session,
            "SELECT DISTINCT name \
             FROM IndexedSessionSqlEntity \
             WHERE STARTS_WITH(name, 'S') \
             ORDER BY name ASC \
             LIMIT 2",
        )
    });
    let projected_rows =
        projected_rows.expect("DISTINCT structural convergence projection should execute");

    assert_eq!(
        projected_rows,
        vec![
            vec![Value::Text("Sable".to_string())],
            vec![Value::Text("Saffron".to_string())],
        ],
        "DISTINCT should keep post-dedup paging semantics while using the shared executor",
    );
    assert_eq!(
        metrics.hybrid_covering_path_hits, 0,
        "DISTINCT should not take the SQL-side hybrid covering projection fork",
    );
    assert!(
        metrics.slot_rows_path_hits > 0,
        "DISTINCT should materialize through retained slot rows from shared scalar execution",
    );
}

#[test]
fn explain_route_matches_executed_trace_route_structurally() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session().debug();

    // Phase 1: seed one indexed prefix fixture so explain and execution should
    // both choose the same secondary-prefix route family.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sable", 10),
            ("Saffron", 20),
            ("Summit", 30),
            ("Atlas", 40),
        ],
    );
    let sql = "SELECT * \
               FROM IndexedSessionSqlEntity \
               WHERE STARTS_WITH(name, 'S') \
               ORDER BY name ASC, id ASC \
               LIMIT 2";
    let query = lower_select_query_for_tests::<IndexedSessionSqlEntity>(&session, sql)
        .expect("explain convergence SQL should lower");

    // Phase 2: compare the explain route family to the actual traced execution
    // route without snapshot-locking the full descriptor tree.
    let explain = session
        .explain_query_execution_with_visible_indexes(&query)
        .expect("explain convergence descriptor should build");
    let execution = session
        .execute_load_query_paged_with_trace(&query, None)
        .expect("explain convergence traced execution should run");
    let trace = execution
        .execution_trace()
        .expect("debug execution should emit an execution trace");

    assert_eq!(
        trace.access_path_variant(),
        ExecutionAccessPathVariant::IndexRange,
        "executed trace should use the indexed range route",
    );
    assert!(
        explain_execution_find_first_node(&explain, ExplainExecutionNodeType::IndexRangeScan)
            .is_some(),
        "EXPLAIN should describe the same indexed range route family as execution",
    );
}

#[test]
fn explain_route_facts_match_execution_trace_route_matrix() {
    reset_session_sql_store();
    let session = sql_session().debug();
    seed_fixed_session_sql_entities(
        &session,
        &[
            (13_501, "explain-trace-a", 10),
            (13_502, "explain-trace-b", 20),
            (13_503, "explain-trace-c", 30),
        ],
    );

    // Phase 1: cover the plain scalar route where both EXPLAIN and execution
    // should agree on a full scan.
    assert_sql_explain_route_matches_execution_trace::<SessionSqlEntity>(
        &session,
        "SELECT * FROM SessionSqlEntity ORDER BY age ASC, id ASC",
        ExecutionAccessPathVariant::FullScan,
        "full scan",
    );

    // Phase 2: cover the primary-key singleton route through SQL lowering so
    // route facts match the same surface users inspect with EXPLAIN.
    let target_id = Ulid::from_u128(13_501);
    let by_key_sql =
        format!("SELECT * FROM SessionSqlEntity WHERE id = '{target_id}' ORDER BY id ASC");
    assert_sql_explain_route_matches_execution_trace::<SessionSqlEntity>(
        &session,
        by_key_sql.as_str(),
        ExecutionAccessPathVariant::ByKey,
        "primary-key lookup",
    );

    // Phase 3: cover the secondary index route where the planner/runtime route
    // agreement depends on the visible-index boundary.
    reset_indexed_session_sql_store();
    let indexed_session = indexed_sql_session().debug();
    seed_indexed_session_sql_entities(
        &indexed_session,
        &[
            ("Sable", 10),
            ("Saffron", 20),
            ("Summit", 30),
            ("Atlas", 40),
        ],
    );
    assert_sql_explain_route_matches_execution_trace::<IndexedSessionSqlEntity>(
        &indexed_session,
        "SELECT * \
         FROM IndexedSessionSqlEntity \
         WHERE STARTS_WITH(name, 'S') \
         ORDER BY name ASC, id ASC \
         LIMIT 2",
        ExecutionAccessPathVariant::IndexRange,
        "secondary index range",
    );
}
