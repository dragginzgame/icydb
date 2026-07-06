use super::*;
use crate::db::{
    ExecutionAccessPathVariant, FieldRef, PagedLoadExecution,
    session::sql::with_sql_projection_materialization_metrics,
};

type SessionKeyAgeRows = Vec<(Ulid, u64)>;
type IndexedSessionNameKeyRows = Vec<(String, Ulid)>;
type GroupedAgeCountRows = Vec<(Value, Vec<Value>)>;

const PHYSICAL_STREAM_CHUNK_TEST_ROWS: usize = 150;
const PHYSICAL_STREAM_CHUNK_PAGE: u32 = 70;

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

// Project just primary keys from one paged execution response.
fn paged_keys(page: &PagedLoadExecution<SessionSqlEntity>) -> Vec<Ulid> {
    paged_key_age_rows(page)
        .into_iter()
        .map(|(id, _age)| id)
        .collect()
}

fn encoded_scalar_cursor(page: &PagedLoadExecution<SessionSqlEntity>, context: &str) -> String {
    crate::db::encode_cursor(
        page.continuation_cursor()
            .unwrap_or_else(|| panic!("{context} should emit a continuation cursor")),
    )
}

fn insert_fixed_session_sql_entity(
    session: &DbSession<SessionSqlCanister>,
    id: u128,
    name: &'static str,
    age: u64,
) {
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(id),
            name: name.to_string(),
            age,
        })
        .unwrap_or_else(|err| panic!("fixed cursor mutation insert should succeed: {err}"));
}

fn delete_fixed_session_sql_entity(session: &DbSession<SessionSqlCanister>, id: u128) {
    let deleted = session
        .delete::<SessionSqlEntity>()
        .filter(FieldRef::new("id").eq(Ulid::from_u128(id)))
        .order_term(crate::db::asc("id"))
        .limit(1)
        .execute()
        .unwrap_or_else(|err| panic!("fixed cursor mutation delete should succeed: {err}"));
    assert_eq!(
        deleted, 1,
        "fixed cursor mutation delete should remove exactly one row",
    );
}

fn indexed_page_name_key_rows(
    page: &PagedLoadExecution<IndexedSessionSqlEntity>,
) -> IndexedSessionNameKeyRows {
    page.response()
        .iter()
        .map(|row| (row.entity_ref().name.clone(), row.id().key()))
        .collect()
}

fn encoded_indexed_scalar_cursor(
    page: &PagedLoadExecution<IndexedSessionSqlEntity>,
    context: &str,
) -> String {
    crate::db::encode_cursor(
        page.continuation_cursor()
            .unwrap_or_else(|| panic!("{context} should emit a continuation cursor")),
    )
}

fn insert_fixed_indexed_session_sql_entity(
    session: &DbSession<SessionSqlCanister>,
    id: u128,
    name: &'static str,
    age: u64,
) {
    session
        .insert(IndexedSessionSqlEntity {
            id: Ulid::from_u128(id),
            name: name.to_string(),
            age,
        })
        .unwrap_or_else(|err| panic!("fixed indexed cursor insert should succeed: {err}"));
}

fn update_fixed_indexed_session_sql_entity(
    session: &DbSession<SessionSqlCanister>,
    id: u128,
    name: &'static str,
    age: u64,
) {
    session
        .update(IndexedSessionSqlEntity {
            id: Ulid::from_u128(id),
            name: name.to_string(),
            age,
        })
        .unwrap_or_else(|err| panic!("fixed indexed cursor update should succeed: {err}"));
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

// Seed enough deterministic primary-key rows to cross multiple physical stream
// chunks and return the exact key order expected from primary-key traversal.
fn seed_chunked_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    base: u128,
    count: usize,
    name_prefix: &str,
) -> Vec<Ulid> {
    let ids = (0..count)
        .map(|offset| Ulid::from_u128(base + offset as u128))
        .collect::<Vec<_>>();

    insert_session_fixture_rows(
        session,
        ids.iter().enumerate(),
        |(offset, id)| SessionSqlEntity {
            id: *id,
            name: format!("{name_prefix}-{offset:03}"),
            age: offset as u64,
        },
        "chunked primary stream seed",
    );

    ids
}

// Seed enough deterministic indexed rows to force raw-index stream chunks.
// Names are unique so physical index traversal order is observable directly.
fn seed_chunked_indexed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    base: u128,
    count: usize,
    name_prefix: &str,
) -> Vec<String> {
    let names = (0..count)
        .map(|offset| format!("{name_prefix}-{offset:03}"))
        .collect::<Vec<_>>();

    insert_session_fixture_rows(
        session,
        names.iter().enumerate(),
        |(offset, name)| IndexedSessionSqlEntity {
            id: Ulid::from_u128(base + offset as u128),
            name: name.clone(),
            age: offset as u64,
        },
        "chunked indexed stream seed",
    );

    names
}

// Seed indexed rows with more than one chunk of distinct raw index entries and
// duplicate projected names so DISTINCT has to dedupe after streaming.
fn seed_chunked_indexed_distinct_rows(
    session: &DbSession<SessionSqlCanister>,
    base: u128,
    distinct_count: usize,
    name_prefix: &str,
) -> Vec<String> {
    let names = (0..distinct_count)
        .map(|offset| format!("{name_prefix}-{offset:03}"))
        .collect::<Vec<_>>();

    insert_session_fixture_rows(
        session,
        names.iter().enumerate().flat_map(|(offset, name)| {
            [(offset * 2, name.clone()), (offset * 2 + 1, name.clone())]
        }),
        |(offset, name)| IndexedSessionSqlEntity {
            id: Ulid::from_u128(base + offset as u128),
            name,
            age: offset as u64,
        },
        "chunked indexed distinct stream seed",
    );

    names
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

// Extract one `SELECT name` projection into plain text values.
fn projection_text_values(rows: Vec<Vec<Value>>) -> Vec<String> {
    rows.into_iter()
        .map(|row| {
            let [Value::Text(value)] = row.as_slice() else {
                panic!("text projection should emit exactly one Text value");
            };
            value.clone()
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
        ExecutionAccessPathVariant::IndexBranchSet => ExplainExecutionNodeType::IndexBranchSet,
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
    let stats = trace
        .execution_stats()
        .expect("debug route convergence execution should emit execution stats");
    assert!(
        stats.keys_streamed() > 0,
        "{context}: execution stats should record streamed physical keys",
    );
    assert_eq!(
        stats.rows_scanned_pre_filter(),
        trace.keys_scanned(),
        "{context}: execution stats pre-filter rows should match trace keys scanned",
    );
    assert!(
        stats.rows_after_predicate() <= stats.rows_scanned_pre_filter(),
        "{context}: predicate-filtered rows cannot exceed pre-filter rows",
    );
    assert_eq!(
        stats.rows_after_projection(),
        trace.rows_returned(),
        "{context}: projected row count should match returned trace rows",
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
        .trusted_read_unchecked()
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
        .trusted_read_unchecked()
        .filter(FieldRef::new("age").gte(20_u64))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .execute_paged(crate::db::PageRequest::first(2))
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
        .trusted_read_unchecked()
        .filter(FieldRef::new("age").gte(20_u64))
        .order_term(crate::db::asc("age"))
        .order_term(crate::db::asc("id"))
        .execute_paged(crate::db::PageRequest::next(2, cursor))
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
fn primary_key_stream_resume_crosses_chunk_boundary_without_gaps() {
    reset_session_sql_store();
    let session = sql_session();
    let expected_ids = seed_chunked_session_sql_entities(
        &session,
        13_600,
        PHYSICAL_STREAM_CHUNK_TEST_ROWS,
        "primary-stream",
    );
    let sql = format!(
        "SELECT * \
         FROM SessionSqlEntity \
         ORDER BY id ASC \
         LIMIT {PHYSICAL_STREAM_CHUNK_PAGE}"
    );
    let query = lower_select_query_for_tests::<SessionSqlEntity>(&session, sql.as_str())
        .expect("primary stream chunk SQL should lower");

    // Phase 1: read the first page across the 64-key physical chunk boundary.
    let first = session
        .execute_load_query_paged_with_trace(&query, None)
        .expect("first primary stream chunk page should execute")
        .into_execution();
    let first_cursor = crate::db::encode_cursor(
        first
            .continuation_cursor()
            .expect("first chunked primary page should emit a cursor"),
    );
    let first_ids = paged_key_age_rows(&first)
        .into_iter()
        .map(|(id, _age)| id)
        .collect::<Vec<_>>();
    assert_eq!(
        first_ids,
        expected_ids[..PHYSICAL_STREAM_CHUNK_PAGE as usize],
        "first primary stream page should contain the first ordered key window",
    );

    // Phase 2: resume past the first page and require no duplicate or skipped
    // primary keys across the physical chunk boundary.
    let second = session
        .execute_load_query_paged_with_trace(&query, Some(first_cursor.as_str()))
        .expect("second primary stream chunk page should execute")
        .into_execution();
    let second_cursor = crate::db::encode_cursor(
        second
            .continuation_cursor()
            .expect("second chunked primary page should emit a cursor"),
    );
    let second_ids = paged_key_age_rows(&second)
        .into_iter()
        .map(|(id, _age)| id)
        .collect::<Vec<_>>();
    assert_eq!(
        second_ids,
        expected_ids
            [PHYSICAL_STREAM_CHUNK_PAGE as usize..(PHYSICAL_STREAM_CHUNK_PAGE as usize * 2)],
        "second primary stream page should continue without gaps or duplicates",
    );

    // Phase 3: resume once more and consume the tail rows that do not fill a
    // complete logical page.
    let third = session
        .execute_load_query_paged_with_trace(&query, Some(second_cursor.as_str()))
        .expect("third primary stream chunk page should execute")
        .into_execution();
    let third_ids = paged_key_age_rows(&third)
        .into_iter()
        .map(|(id, _age)| id)
        .collect::<Vec<_>>();
    assert_eq!(
        third_ids,
        expected_ids[(PHYSICAL_STREAM_CHUNK_PAGE as usize * 2)..],
        "tail primary stream page should contain every remaining ordered key",
    );
    assert_eq!(
        third.continuation_cursor(),
        None,
        "tail primary stream page should not emit a continuation cursor",
    );
}

#[test]
fn primary_key_ordered_stream_matches_materialized_full_scan_oracle() {
    reset_session_sql_store();
    let session = sql_session();
    seed_fixed_session_sql_entities(
        &session,
        &[
            (10, "primary-oracle-10", 10),
            (20, "primary-oracle-20", 20),
            (30, "primary-oracle-30", 30),
            (40, "primary-oracle-40", 40),
        ],
    );
    let pushed_sql = "SELECT id FROM SessionSqlEntity ORDER BY id ASC LIMIT 3";
    let materialized_sql = "SELECT id FROM SessionSqlEntity ORDER BY age ASC, id ASC LIMIT 3";

    let pushed_descriptor = lower_select_query_for_tests::<SessionSqlEntity>(&session, pushed_sql)
        .expect("primary ordered stream SQL should lower")
        .explain_execution()
        .expect("primary ordered stream SQL should explain execution");
    assert!(
        explain_execution_find_first_node(
            &pushed_descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "primary ordered stream should prove access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &pushed_descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "primary ordered stream must not use the materialized sort oracle path",
    );

    let materialized_descriptor =
        lower_select_query_for_tests::<SessionSqlEntity>(&session, materialized_sql)
            .expect("primary materialized oracle SQL should lower")
            .explain_execution()
            .expect("primary materialized oracle SQL should explain execution");
    assert!(
        explain_execution_find_first_node(
            &materialized_descriptor,
            ExplainExecutionNodeType::FullScan
        )
        .is_some(),
        "primary materialized oracle should use a full scan",
    );
    assert!(
        explain_execution_find_first_node(
            &materialized_descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_some(),
        "primary materialized oracle should retain the post-access sort",
    );

    let pushed_rows = statement_projection_rows::<SessionSqlEntity>(&session, pushed_sql)
        .expect("primary ordered stream SQL should execute");
    let materialized_rows =
        statement_projection_rows::<SessionSqlEntity>(&session, materialized_sql)
            .expect("primary materialized oracle SQL should execute");

    assert_eq!(
        pushed_rows, materialized_rows,
        "primary ordered stream must match a materialized full-scan oracle with the same deterministic key order",
    );
}

#[test]
fn primary_key_cursor_resume_skips_deleted_boundary_and_unseen_rows() {
    reset_session_sql_store();
    let session = sql_session();
    seed_fixed_session_sql_entities(
        &session,
        &[
            (10, "cursor-delete-10", 10),
            (20, "cursor-delete-20", 20),
            (30, "cursor-delete-30", 30),
            (40, "cursor-delete-40", 40),
            (50, "cursor-delete-50", 50),
        ],
    );
    let query = lower_select_query_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT * FROM SessionSqlEntity ORDER BY id ASC LIMIT 2",
    )
    .expect("primary cursor deletion SQL should lower");

    let first = session
        .execute_load_query_paged_with_trace(&query, None)
        .expect("primary cursor deletion first page should execute")
        .into_execution();
    assert_eq!(
        paged_keys(&first),
        vec![Ulid::from_u128(10), Ulid::from_u128(20)],
        "first page should establish a stable primary-key cursor boundary",
    );
    let cursor = encoded_scalar_cursor(&first, "primary cursor deletion first page");

    delete_fixed_session_sql_entity(&session, 20);
    delete_fixed_session_sql_entity(&session, 30);

    let second = session
        .execute_load_query_paged_with_trace(&query, Some(cursor.as_str()))
        .expect("primary cursor deletion second page should execute")
        .into_execution();
    assert_eq!(
        paged_keys(&second),
        vec![Ulid::from_u128(40), Ulid::from_u128(50)],
        "resume should use the cursor key boundary rather than replaying or requiring the deleted row",
    );
    assert_eq!(
        second.continuation_cursor(),
        None,
        "deleted unseen rows should not force a phantom continuation cursor",
    );
}

#[test]
fn primary_key_cursor_resume_places_inserted_rows_by_boundary() {
    reset_session_sql_store();
    let session = sql_session();
    seed_fixed_session_sql_entities(
        &session,
        &[
            (10, "cursor-insert-10", 10),
            (20, "cursor-insert-20", 20),
            (40, "cursor-insert-40", 40),
            (50, "cursor-insert-50", 50),
        ],
    );
    let query = lower_select_query_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT * FROM SessionSqlEntity ORDER BY id ASC LIMIT 2",
    )
    .expect("primary cursor insertion SQL should lower");

    let first = session
        .execute_load_query_paged_with_trace(&query, None)
        .expect("primary cursor insertion first page should execute")
        .into_execution();
    assert_eq!(
        paged_keys(&first),
        vec![Ulid::from_u128(10), Ulid::from_u128(20)],
        "first page should establish the insertion boundary after id 20",
    );
    let first_cursor = encoded_scalar_cursor(&first, "primary cursor insertion first page");

    insert_fixed_session_sql_entity(&session, 15, "cursor-insert-before", 15);
    insert_fixed_session_sql_entity(&session, 30, "cursor-insert-after", 30);
    insert_fixed_session_sql_entity(&session, 45, "cursor-insert-later", 45);

    let second = session
        .execute_load_query_paged_with_trace(&query, Some(first_cursor.as_str()))
        .expect("primary cursor insertion second page should execute")
        .into_execution();
    assert_eq!(
        paged_keys(&second),
        vec![Ulid::from_u128(30), Ulid::from_u128(40)],
        "resume should ignore new rows before the boundary and include new rows after it in order",
    );
    let second_cursor = encoded_scalar_cursor(&second, "primary cursor insertion second page");

    let third = session
        .execute_load_query_paged_with_trace(&query, Some(second_cursor.as_str()))
        .expect("primary cursor insertion third page should execute")
        .into_execution();
    assert_eq!(
        paged_keys(&third),
        vec![Ulid::from_u128(45), Ulid::from_u128(50)],
        "subsequent resume should continue through rows inserted after the previous boundary",
    );
    assert_eq!(
        third.continuation_cursor(),
        None,
        "tail page after inserted rows should finish without a continuation cursor",
    );
}

#[test]
fn primary_key_desc_cursor_resume_places_inserted_rows_by_boundary() {
    reset_session_sql_store();
    let session = sql_session();
    seed_fixed_session_sql_entities(
        &session,
        &[
            (10, "cursor-desc-10", 10),
            (20, "cursor-desc-20", 20),
            (40, "cursor-desc-40", 40),
            (50, "cursor-desc-50", 50),
        ],
    );
    let query = lower_select_query_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT * FROM SessionSqlEntity ORDER BY id DESC LIMIT 2",
    )
    .expect("primary DESC cursor insertion SQL should lower");

    let first = session
        .execute_load_query_paged_with_trace(&query, None)
        .expect("primary DESC cursor insertion first page should execute")
        .into_execution();
    assert_eq!(
        paged_keys(&first),
        vec![Ulid::from_u128(50), Ulid::from_u128(40)],
        "first DESC page should establish the insertion boundary before id 40",
    );
    let cursor = encoded_scalar_cursor(&first, "primary DESC cursor insertion first page");

    insert_fixed_session_sql_entity(&session, 60, "cursor-desc-before", 60);
    insert_fixed_session_sql_entity(&session, 35, "cursor-desc-after-high", 35);
    insert_fixed_session_sql_entity(&session, 30, "cursor-desc-after-low", 30);
    delete_fixed_session_sql_entity(&session, 20);
    delete_fixed_session_sql_entity(&session, 10);

    let second = session
        .execute_load_query_paged_with_trace(&query, Some(cursor.as_str()))
        .expect("primary DESC cursor insertion second page should execute")
        .into_execution();
    assert_eq!(
        paged_keys(&second),
        vec![Ulid::from_u128(35), Ulid::from_u128(30)],
        "DESC resume should ignore new rows before the boundary and include new rows after it",
    );
    assert_eq!(
        second.continuation_cursor(),
        None,
        "DESC tail page after inserted rows should finish without a continuation cursor",
    );
}

#[test]
fn secondary_index_stream_spans_multiple_chunks_in_final_order() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let names = seed_chunked_indexed_session_sql_entities(
        &session,
        13_800,
        PHYSICAL_STREAM_CHUNK_TEST_ROWS,
        "SecondaryStream",
    );

    let rows = execute_scalar_select_for_tests::<IndexedSessionSqlEntity>(
        &session,
        "SELECT * \
         FROM IndexedSessionSqlEntity \
         WHERE STARTS_WITH(name, 'SecondaryStream') \
         ORDER BY name ASC, id ASC \
         LIMIT 130",
    )
    .expect("secondary stream chunk select should execute");
    let actual_names = rows
        .iter()
        .map(|row| row.entity_ref().name.clone())
        .collect::<Vec<_>>();

    assert_eq!(
        actual_names,
        names[..130],
        "secondary index final-order stream should span raw-index chunks without gaps",
    );
}

#[test]
fn secondary_index_cursor_resume_preserves_tie_breakers_after_insert_and_update() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    for (id, name, age) in [
        (100_u128, "alpha", 10_u64),
        (200, "bravo", 20),
        (300, "bravo", 30),
        (500, "delta", 50),
    ] {
        insert_fixed_indexed_session_sql_entity(&session, id, name, age);
    }
    let query = lower_select_query_for_tests::<IndexedSessionSqlEntity>(
        &session,
        "SELECT * FROM IndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("secondary cursor mutation SQL should lower");

    let first = session
        .execute_load_query_paged_with_trace(&query, None)
        .expect("secondary cursor mutation first page should execute")
        .into_execution();
    assert_eq!(
        indexed_page_name_key_rows(&first),
        vec![
            ("alpha".to_string(), Ulid::from_u128(100)),
            ("bravo".to_string(), Ulid::from_u128(200)),
        ],
        "first page should use the secondary value and PK tie-breaker as its boundary",
    );
    let first_cursor =
        encoded_indexed_scalar_cursor(&first, "secondary cursor mutation first page");

    insert_fixed_indexed_session_sql_entity(&session, 150, "alpha", 15);
    insert_fixed_indexed_session_sql_entity(&session, 250, "bravo", 25);
    update_fixed_indexed_session_sql_entity(&session, 500, "aardvark", 55);
    update_fixed_indexed_session_sql_entity(&session, 100, "charlie", 11);

    let second = session
        .execute_load_query_paged_with_trace(&query, Some(first_cursor.as_str()))
        .expect("secondary cursor mutation second page should execute")
        .into_execution();
    assert_eq!(
        indexed_page_name_key_rows(&second),
        vec![
            ("bravo".to_string(), Ulid::from_u128(250)),
            ("bravo".to_string(), Ulid::from_u128(300)),
        ],
        "resume should skip new/updated rows that now sort before the cursor and include equal-key rows after the PK tie-breaker",
    );
    let second_cursor =
        encoded_indexed_scalar_cursor(&second, "secondary cursor mutation second page");

    let third = session
        .execute_load_query_paged_with_trace(&query, Some(second_cursor.as_str()))
        .expect("secondary cursor mutation third page should execute")
        .into_execution();
    assert_eq!(
        indexed_page_name_key_rows(&third),
        vec![("charlie".to_string(), Ulid::from_u128(100))],
        "a row updated after the cursor boundary should appear once at its new ordered position",
    );
    assert_eq!(
        third.continuation_cursor(),
        None,
        "secondary cursor mutation tail page should finish without a continuation cursor",
    );
}

#[test]
fn delete_targets_match_chunked_scalar_key_stream() {
    // Phase 1: establish the chunked scalar key-stream baseline.
    reset_session_sql_store();
    let session = sql_session();
    let expected_ids = seed_chunked_session_sql_entities(
        &session,
        14_000,
        PHYSICAL_STREAM_CHUNK_TEST_ROWS,
        "delete-stream",
    );
    let scalar_keys = session
        .load::<SessionSqlEntity>()
        .trusted_read_unchecked()
        .filter(FieldRef::new("age").lt(130_u64))
        .order_term(crate::db::asc("id"))
        .limit(100)
        .execute()
        .expect("chunked delete scalar baseline should execute")
        .into_rows()
        .expect("chunked delete scalar baseline should return entity rows")
        .ids()
        .map(|id| id.key())
        .collect::<Vec<_>>();
    assert_eq!(
        scalar_keys,
        expected_ids[..100],
        "scalar baseline should span more than one primary stream chunk",
    );

    // Phase 2: replay the same table and require SQL DELETE RETURNING to consume
    // the same shared chunked key-stream target set.
    reset_session_sql_store();
    let session = sql_session();
    seed_chunked_session_sql_entities(
        &session,
        14_000,
        PHYSICAL_STREAM_CHUNK_TEST_ROWS,
        "delete-stream",
    );
    let sql_delete_keys = projection_ulid_keys(
        statement_projection_rows::<SessionSqlEntity>(
            &session,
            "DELETE FROM SessionSqlEntity \
             WHERE age < 130 \
             ORDER BY id ASC \
             LIMIT 100 \
             RETURNING id",
        )
        .expect("chunked SQL DELETE RETURNING should execute"),
    );

    assert_eq!(
        sql_delete_keys, scalar_keys,
        "chunked SQL DELETE target keys should match scalar execution keys",
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
        .trusted_read_unchecked()
        .group_by("age")
        .expect("grouped convergence fluent group_by should resolve")
        .aggregate(crate::db::count())
        .order_term(crate::db::asc("age"))
        .limit(1);
    let fluent_first = session
        .execute_grouped(fluent_query.query(), None)
        .expect("grouped convergence first fluent page should execute");
    assert_eq!(
        sql_first.rows(),
        fluent_first.rows(),
        "SQL and fluent grouped first pages should preserve identical public GroupedRow order",
    );
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
        sql_second.rows(),
        fluent_second.rows(),
        "SQL and fluent grouped continuation pages should preserve identical public GroupedRow order",
    );
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
fn fluent_rows_only_execution_rejects_grouped_plan_without_blocking_grouped_surface() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("rows-only-group-a", 10),
            ("rows-only-group-b", 10),
            ("rows-only-group-c", 20),
        ],
    );

    let grouped_query = session
        .load::<SessionSqlEntity>()
        .trusted_read_unchecked()
        .group_by("age")
        .expect("rows-only grouped fixture should resolve group_by")
        .aggregate(crate::db::count())
        .order_term(crate::db::asc("age"))
        .limit(1);

    assert!(
        grouped_query.execute_rows().is_err(),
        "rows-only fluent execution should reject grouped plans before grouped executor dispatch",
    );

    let grouped = session
        .execute_grouped(grouped_query.query(), None)
        .expect("explicit grouped execution should still admit the same grouped query");
    assert_eq!(
        grouped.rows().len(),
        1,
        "explicit grouped execution should preserve the grouped page contract",
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "this convergence test keeps scalar and grouped trace-preservation checks in one response-finalization contract"
)]
fn paged_response_finalization_preserves_cursor_presence_and_trace_independently() {
    reset_session_sql_store();
    let seed_session = sql_session();

    // Phase 1: seed enough scalar rows and grouped buckets so both first-page
    // response surfaces must emit continuation cursors under LIMIT 1.
    seed_session_sql_entities(
        &seed_session,
        &[
            ("paged-finalize-a", 10),
            ("paged-finalize-b", 10),
            ("paged-finalize-c", 20),
            ("paged-finalize-d", 30),
        ],
    );

    let plain_session = sql_session();
    let traced_session = sql_session().debug();

    // Phase 2: compare scalar page finalization with and without trace data.
    let scalar_sql = "SELECT * \
                      FROM SessionSqlEntity \
                      WHERE age >= 10 \
                      ORDER BY age ASC, id ASC \
                      LIMIT 1";
    let scalar_query = lower_select_query_for_tests::<SessionSqlEntity>(&plain_session, scalar_sql)
        .expect("scalar finalization SQL should lower");
    let scalar_plain = plain_session
        .execute_load_query_paged_with_trace(&scalar_query, None)
        .expect("plain scalar page should execute");
    let scalar_traced = traced_session
        .execute_load_query_paged_with_trace(&scalar_query, None)
        .expect("traced scalar page should execute");
    assert!(
        scalar_traced.execution_trace().is_some(),
        "debug scalar page should attach execution trace",
    );
    assert_eq!(
        entity_response_key_age_rows(scalar_plain.response()),
        entity_response_key_age_rows(scalar_traced.response()),
        "scalar trace attachment must not alter page rows",
    );
    assert_eq!(
        scalar_plain.continuation_cursor(),
        scalar_traced.continuation_cursor(),
        "scalar trace attachment must not alter cursor bytes",
    );
    assert!(
        scalar_traced.continuation_cursor().is_some(),
        "scalar LIMIT 1 first page should emit a continuation cursor",
    );

    let scalar_traced_rows = entity_response_key_age_rows(scalar_traced.response());
    let scalar_traced_cursor = scalar_traced.continuation_cursor().map(<[u8]>::to_vec);
    let scalar_untraced = scalar_traced.into_execution();
    assert_eq!(
        entity_response_key_age_rows(scalar_untraced.response()),
        scalar_traced_rows,
        "dropping scalar trace must preserve rows",
    );
    assert_eq!(
        scalar_untraced.continuation_cursor().map(<[u8]>::to_vec),
        scalar_traced_cursor,
        "dropping scalar trace must preserve cursor bytes",
    );

    // Phase 3: compare grouped page finalization with and without trace data.
    let grouped_sql = "SELECT age, COUNT(*) \
                       FROM SessionSqlEntity \
                       GROUP BY age \
                       ORDER BY age ASC \
                       LIMIT 1";
    let grouped_plain =
        execute_grouped_select_for_tests::<SessionSqlEntity>(&plain_session, grouped_sql, None)
            .expect("plain grouped page should execute");
    let grouped_traced =
        execute_grouped_select_for_tests::<SessionSqlEntity>(&traced_session, grouped_sql, None)
            .expect("traced grouped page should execute");
    assert!(
        grouped_traced.execution_trace().is_some(),
        "debug grouped page should attach execution trace",
    );
    assert_eq!(
        grouped_plain.rows(),
        grouped_traced.rows(),
        "grouped trace attachment must not alter public GroupedRow order",
    );
    assert_eq!(
        grouped_plain.continuation_cursor(),
        grouped_traced.continuation_cursor(),
        "grouped trace attachment must not alter cursor bytes",
    );
    assert!(
        grouped_traced.continuation_cursor().is_some(),
        "grouped LIMIT 1 first page should emit a continuation cursor",
    );
    assert_eq!(
        scalar_untraced.continuation_cursor().is_some(),
        grouped_traced.continuation_cursor().is_some(),
        "scalar and grouped first pages should agree on cursor presence under LIMIT 1",
    );

    let grouped_traced_rows = grouped_traced.rows().to_vec();
    let grouped_traced_cursor = grouped_traced.continuation_cursor().map(<[u8]>::to_vec);
    let grouped_untraced = grouped_traced.into_execution();
    assert_eq!(
        grouped_untraced.rows(),
        grouped_traced_rows.as_slice(),
        "dropping grouped trace must preserve rows",
    );
    assert_eq!(
        grouped_untraced.continuation_cursor().map(<[u8]>::to_vec),
        grouped_traced_cursor,
        "dropping grouped trace must preserve cursor bytes",
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
        .trusted_read_unchecked()
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
fn sql_distinct_projection_dedupes_chunked_secondary_stream() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let expected_names = seed_chunked_indexed_distinct_rows(&session, 14_200, 75, "DistinctStream");

    let distinct_rows = statement_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT DISTINCT name \
         FROM IndexedSessionSqlEntity \
         WHERE STARTS_WITH(name, 'DistinctStream') \
         ORDER BY name ASC",
    )
    .expect("chunked DISTINCT projection should execute");

    assert_eq!(
        projection_text_values(distinct_rows),
        expected_names,
        "DISTINCT should dedupe after consuming a secondary stream spanning multiple raw-index chunks",
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
    assert_eq!(
        metrics.distinct_candidate_rows, 3,
        "bounded DISTINCT should stop projecting once LIMIT distinct rows are observed",
    );
    assert_eq!(
        metrics.distinct_bounded_stop_hits, 1,
        "bounded DISTINCT should record one early-stop event for LIMIT 2",
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
