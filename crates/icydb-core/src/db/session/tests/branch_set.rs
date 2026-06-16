use super::*;

const BRANCH_COLLECTION: &str = "01KV5N439P0000000000000000";
const OTHER_COLLECTION: &str = "01KV5N439P1111111111111111";
const BRANCH_LIMIT: usize = 3;
const BRANCH_FETCH: u64 = 4;
#[cfg(feature = "diagnostics")]
const BRANCH_HEAD_MERGE_READ_CAP: u64 = BRANCH_FETCH + 1;

fn branch_target_sql(select: &str, limit: usize) -> String {
    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id = '{BRANCH_COLLECTION}' \
           AND stage IN ('Draft', 'Review') \
         ORDER BY id ASC \
         LIMIT {limit}",
    )
}

fn branch_target_or_sql(select: &str, limit: usize) -> String {
    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id = '{BRANCH_COLLECTION}' \
           AND (stage = 'Draft' OR stage = 'Review') \
         ORDER BY id ASC \
         LIMIT {limit}",
    )
}

fn branch_target_duplicate_literal_sql(select: &str, limit: usize) -> String {
    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id = '{BRANCH_COLLECTION}' \
           AND stage IN ('Draft', 'Draft', 'Review') \
         ORDER BY id ASC \
         LIMIT {limit}",
    )
}

fn branch_target_over_cap_sql(select: &str, limit: usize) -> String {
    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id = '{BRANCH_COLLECTION}' \
           AND stage IN (\
             'Draft', 'Review', 'Published', 'Archived', 'Queued', \
             'Rejected', 'Minted', 'Burned', 'Frozen'\
           ) \
         ORDER BY id ASC \
         LIMIT {limit}",
    )
}

fn branch_target_over_cap_sparse_sql(select: &str, limit: usize) -> String {
    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id = '{BRANCH_COLLECTION}' \
           AND stage IN (\
             'Draft', 'Review', 'MissingA', 'MissingB', 'MissingC', \
             'MissingD', 'MissingE', 'MissingF', 'MissingG'\
           ) \
         ORDER BY id ASC \
         LIMIT {limit}",
    )
}

fn seed_branch_set_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_branch_indexed_session_sql_entities(
        session,
        &[
            (9_090, BRANCH_COLLECTION, "Draft", "draft-090"),
            (9_095, BRANCH_COLLECTION, "Review", "review-095"),
            (9_100, BRANCH_COLLECTION, "Review", "review-100"),
            (9_105, BRANCH_COLLECTION, "Draft", "draft-105"),
            (9_110, BRANCH_COLLECTION, "Published", "published-110"),
            (9_115, OTHER_COLLECTION, "Draft", "other-draft-115"),
            (9_120, BRANCH_COLLECTION, "Draft", "draft-120"),
            (9_125, BRANCH_COLLECTION, "Review", "review-125"),
            (9_130, BRANCH_COLLECTION, "Draft", "draft-130"),
            (9_135, BRANCH_COLLECTION, "Review", "review-135"),
            (9_140, BRANCH_COLLECTION, "Queued", "queued-140"),
            (9_145, OTHER_COLLECTION, "Review", "other-review-145"),
            (9_150, BRANCH_COLLECTION, "Draft", "draft-150"),
            (9_155, BRANCH_COLLECTION, "Review", "review-155"),
            (9_160, BRANCH_COLLECTION, "Archived", "archived-160"),
            (9_165, OTHER_COLLECTION, "Draft", "other-draft-165"),
            (9_170, BRANCH_COLLECTION, "Draft", "draft-170"),
            (9_175, BRANCH_COLLECTION, "Review", "review-175"),
            (9_180, BRANCH_COLLECTION, "Rejected", "rejected-180"),
            (9_185, OTHER_COLLECTION, "Review", "other-review-185"),
        ],
    );
}

fn branch_descriptor(sql: &str) -> ExplainExecutionNodeDescriptor {
    let session = indexed_sql_session();
    lower_select_query_for_tests::<BranchIndexedSessionSqlEntity>(&session, sql)
        .unwrap_or_else(|err| panic!("branch-set SQL should lower: {err:?}"))
        .explain_execution()
        .unwrap_or_else(|err| panic!("branch-set SQL should explain execution: {err:?}"))
}

fn assert_target_branch_route(descriptor: &ExplainExecutionNodeDescriptor) {
    let branch_node =
        explain_execution_find_first_node(descriptor, ExplainExecutionNodeType::IndexBranchSet)
            .expect("target shape should expose one branch-aware route node");

    assert_eq!(
        branch_node.access_strategy(),
        Some(&ExplainAccessPath::IndexBranchSet {
            name: "collection_stage_id".to_string(),
            fields: vec![
                "collection_id".to_string(),
                "stage".to_string(),
                "id".to_string(),
            ],
            fixed_values: vec![Value::Text(BRANCH_COLLECTION.to_string())],
            branch_values: vec![
                Value::Text("Draft".to_string()),
                Value::Text("Review".to_string()),
            ],
        }),
        "branch route should carry index identity, fixed prefix, and exact branch values",
    );
    assert!(
        !explain_execution_contains_node_type(
            descriptor,
            ExplainExecutionNodeType::IndexMultiLookup
        ),
        "target shape must not degrade to the unordered multi-lookup route",
    );
    assert!(
        !explain_execution_contains_node_type(descriptor, ExplainExecutionNodeType::FullScan),
        "target shape must not use a full collection scan",
    );
    assert!(
        !explain_execution_contains_node_type(
            descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort,
        ),
        "target shape must not materialize-sort the admitted branch route",
    );
    assert!(
        explain_execution_contains_node_type(
            descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied,
        ),
        "target branch route should prove global primary-key order",
    );
}

fn assert_target_top_n_fetch(descriptor: &ExplainExecutionNodeDescriptor) {
    let top_n = explain_execution_find_first_node(descriptor, ExplainExecutionNodeType::TopNSeek)
        .expect("target branch route should preserve TopN seek");

    assert_eq!(
        top_n.node_properties().get("fetch"),
        Some(&Value::from(BRANCH_FETCH)),
        "limit paging should request exactly limit + 1 rows for has_more",
    );
}

fn expected_branch_rows(limit: usize) -> Vec<Vec<Value>> {
    expected_branch_ids(limit)
        .into_iter()
        .map(|id| vec![Value::Ulid(id)])
        .collect()
}

fn expected_branch_ids(limit: usize) -> Vec<Ulid> {
    [
        9_090_u128, 9_095, 9_100, 9_105, 9_120, 9_125, 9_130, 9_135, 9_150, 9_155, 9_170, 9_175,
    ]
    .into_iter()
    .take(limit)
    .map(Ulid::from_u128)
    .collect()
}

fn paged_branch_ids(
    page: &crate::db::PagedLoadExecution<BranchIndexedSessionSqlEntity>,
) -> Vec<Ulid> {
    page.iter().map(|row| row.id().key()).collect()
}

#[cfg(feature = "diagnostics")]
fn with_store_and_index_reads<T>(run: impl FnOnce() -> T) -> (T, u64, u64) {
    let store_gets_before = crate::db::data::DataStore::current_get_call_count();
    let index_entries_before = crate::db::index::IndexStore::current_entry_read_count();
    let result = run();
    let store_gets =
        crate::db::data::DataStore::current_get_call_count().saturating_sub(store_gets_before);
    let index_entries = crate::db::index::IndexStore::current_entry_read_count()
        .saturating_sub(index_entries_before);

    (result, store_gets, index_entries)
}

#[test]
fn session_branch_set_sql_admits_branch_route_and_strips_residual_predicates() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_sql("id", BRANCH_LIMIT);
    let descriptor = branch_descriptor(sql.as_str());

    assert_target_branch_route(&descriptor);
    assert_target_top_n_fetch(&descriptor);
    assert!(
        !explain_execution_contains_node_type(
            &descriptor,
            ExplainExecutionNodeType::ResidualFilter
        ),
        "access-proven collection_id and stage predicates must not remain as residual filters",
    );
}

#[test]
fn session_branch_set_sql_admits_or_of_equality_branch_shape() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_or_sql("id", BRANCH_LIMIT);
    let descriptor = branch_descriptor(sql.as_str());

    assert_target_branch_route(&descriptor);
    assert_target_top_n_fetch(&descriptor);
}

#[test]
fn session_branch_set_sql_large_in_falls_back_and_keeps_residual_stage_predicate() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_over_cap_sql("id", BRANCH_LIMIT);
    let descriptor = branch_descriptor(sql.as_str());
    let rendered = descriptor.render_text_tree();

    assert!(
        !explain_execution_contains_node_type(
            &descriptor,
            ExplainExecutionNodeType::IndexBranchSet
        ),
        "over-cap IN lists should not use the branch-aware route",
    );
    assert!(
        rendered.contains("stage"),
        "fallback route must retain the unproven stage predicate: {rendered}",
    );
}

#[test]
fn session_branch_set_sql_over_cap_fallback_filters_before_primary_key_limit() {
    const OVER_CAP_LIMIT: usize = 8;

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_over_cap_sparse_sql("id", OVER_CAP_LIMIT);
    let descriptor = branch_descriptor(sql.as_str());
    let rendered = descriptor.render_text_tree();

    assert!(
        !explain_execution_contains_node_type(
            &descriptor,
            ExplainExecutionNodeType::IndexBranchSet
        ),
        "over-cap sparse IN list should stay on the fallback route",
    );
    assert!(
        rendered.contains("stage"),
        "fallback route must retain the sparse stage predicate: {rendered}",
    );
    assert!(
        explain_execution_contains_node_type(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        ),
        "fallback route must materialize-sort before applying the page limit",
    );
    assert!(
        !explain_execution_contains_node_type(&descriptor, ExplainExecutionNodeType::TopNSeek),
        "fallback route must not pre-limit before residual filtering and primary-key sorting",
    );

    let rows = statement_projection_rows::<BranchIndexedSessionSqlEntity>(&session, sql.as_str())
        .unwrap_or_else(|err| panic!("over-cap fallback projection should execute: {err:?}"));

    assert_eq!(
        rows,
        expected_branch_rows(OVER_CAP_LIMIT),
        "over-cap fallback should apply residual stage filtering before the global primary-key LIMIT",
    );
}

#[test]
fn session_branch_set_sql_rows_match_full_filter_primary_key_sort() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_sql("id", BRANCH_LIMIT);

    let rows = statement_projection_rows::<BranchIndexedSessionSqlEntity>(&session, sql.as_str())
        .unwrap_or_else(|err| panic!("branch-set projection should execute: {err:?}"));

    assert_eq!(
        rows,
        expected_branch_rows(BRANCH_LIMIT),
        "branch merge should match full filter plus global primary-key ASC sort",
    );
}

#[test]
fn session_branch_set_sql_cursor_continuation_resumes_branch_streams_after_boundary() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_sql("*", BRANCH_LIMIT);
    let descriptor = branch_descriptor(sql.as_str());
    let query =
        lower_select_query_for_tests::<BranchIndexedSessionSqlEntity>(&session, sql.as_str())
            .unwrap_or_else(|err| panic!("branch-set paged SQL should lower: {err:?}"));

    assert_target_branch_route(&descriptor);
    assert_target_top_n_fetch(&descriptor);

    let first = session
        .execute_load_query_paged_with_trace(&query, None)
        .unwrap_or_else(|err| panic!("first branch-set page should execute: {err:?}"))
        .into_execution();

    assert_eq!(
        paged_branch_ids(&first),
        expected_branch_ids(BRANCH_LIMIT),
        "first branch-set page should match the first global primary-key window",
    );

    let cursor = crate::db::encode_cursor(
        first
            .continuation_cursor()
            .expect("first branch-set page should emit a continuation cursor"),
    );
    let second = session
        .execute_load_query_paged_with_trace(&query, Some(cursor.as_str()))
        .unwrap_or_else(|err| panic!("second branch-set page should execute: {err:?}"))
        .into_execution();

    assert_eq!(
        paged_branch_ids(&second),
        expected_branch_ids(BRANCH_LIMIT * 2)
            .into_iter()
            .skip(BRANCH_LIMIT)
            .collect::<Vec<_>>(),
        "second branch-set page should resume after the prior page boundary",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_cursor_continuation_does_not_replay_branch_prefix_entries() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_sql("*", BRANCH_LIMIT);
    let query =
        lower_select_query_for_tests::<BranchIndexedSessionSqlEntity>(&session, sql.as_str())
            .unwrap_or_else(|err| panic!("branch-set paged SQL should lower: {err:?}"));

    let first = session
        .execute_load_query_paged_with_trace(&query, None)
        .unwrap_or_else(|err| panic!("first branch-set page should execute: {err:?}"))
        .into_execution();
    let cursor = crate::db::encode_cursor(
        first
            .continuation_cursor()
            .expect("first branch-set page should emit a continuation cursor"),
    );

    let (second_with_trace, second_store_gets, second_entry_reads) =
        with_store_and_index_reads(|| {
            session
                .execute_load_query_paged_with_trace(&query, Some(cursor.as_str()))
                .unwrap_or_else(|err| panic!("second branch-set page should execute: {err:?}"))
        });
    let second = second_with_trace.into_execution();

    assert_eq!(
        paged_branch_ids(&second),
        expected_branch_ids(BRANCH_LIMIT * 2)
            .into_iter()
            .skip(BRANCH_LIMIT)
            .collect::<Vec<_>>(),
        "resumed branch page should still emit the next primary-key window",
    );
    assert!(
        (BRANCH_LIMIT as u64..=BRANCH_FETCH).contains(&second_store_gets),
        "resumed SELECT * branch page should hydrate only returned rows plus lookahead, got {second_store_gets} row-store gets",
    );

    let branch_match_count = expected_branch_ids(usize::MAX).len() as u64;
    let branch_stream_entry_cap = BRANCH_FETCH * 2;
    let remaining_after_first_page = branch_match_count.saturating_sub(BRANCH_LIMIT as u64);
    assert!(
        second_entry_reads <= branch_stream_entry_cap,
        "resumed branch page should read at most the page lookahead per branch stream, got {second_entry_reads} reads for cap {branch_stream_entry_cap}",
    );
    assert!(
        second_entry_reads < remaining_after_first_page,
        "resumed branch page should not scan the full remaining suffix, got {second_entry_reads} reads for {remaining_after_first_page} remaining matches",
    );
}

#[test]
fn session_branch_set_sql_duplicate_branch_literals_do_not_duplicate_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_duplicate_literal_sql("id", 6);

    let rows = statement_projection_rows::<BranchIndexedSessionSqlEntity>(&session, sql.as_str())
        .unwrap_or_else(|err| panic!("duplicate-branch projection should execute: {err:?}"));

    assert_eq!(
        rows,
        expected_branch_rows(6),
        "duplicate IN literals must not duplicate primary keys in the merged branch output",
    );
}

#[test]
fn session_branch_set_sql_explain_output_identifies_branch_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let explain_sql = format!(
        "EXPLAIN EXECUTION {}",
        branch_target_sql("id", BRANCH_LIMIT)
    );

    let explain =
        statement_explain_sql::<BranchIndexedSessionSqlEntity>(&session, explain_sql.as_str())
            .unwrap_or_else(|err| panic!("branch-set EXPLAIN EXECUTION should run: {err:?}"));

    assert!(
        explain.contains("IndexBranchSet"),
        "EXPLAIN should identify the branch-aware route: {explain}",
    );
    assert!(
        explain.contains("collection_stage_id"),
        "EXPLAIN should identify the selected composite index: {explain}",
    );
    assert!(
        explain.contains("OrderByAccessSatisfied"),
        "EXPLAIN should identify order satisfied by access: {explain}",
    );
    assert!(
        !explain.contains("OrderByMaterializedSort"),
        "EXPLAIN should not report materialized sorting for the admitted branch route: {explain}",
    );
}

#[test]
fn session_branch_set_sql_plan_metrics_identify_branch_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_sql("id", BRANCH_LIMIT);
    let sink = SessionMetricsCaptureSink::default();

    with_metrics_sink(&sink, || {
        execute_scalar_select_for_tests::<BranchIndexedSessionSqlEntity>(&session, sql.as_str())
    })
    .unwrap_or_else(|err| panic!("branch-set load should execute with metrics: {err:?}"));
    let events = sink.into_events();

    assert!(
        events.iter().any(|event| {
            matches!(
                event,
                MetricsEvent::Plan {
                    entity_path: BranchIndexedSessionSqlEntity::PATH,
                    kind: PlanKind::IndexBranchSet,
                    ..
                }
            )
        }),
        "target branch SQL should emit an IndexBranchSet plan metric: {events:?}",
    );
    assert!(
        !events.iter().any(|event| {
            matches!(
                event,
                MetricsEvent::Plan {
                    entity_path: BranchIndexedSessionSqlEntity::PATH,
                    kind: PlanKind::IndexMultiLookup | PlanKind::FullScan,
                    ..
                }
            )
        }),
        "target branch SQL should not be attributed as multi-lookup or full-scan: {events:?}",
    );
}

#[test]
fn session_branch_set_sql_plan_hash_and_verbose_cache_keep_branch_identity() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let branch_sql = branch_target_sql("id", BRANCH_LIMIT);
    let branch_query = lower_select_query_for_tests::<BranchIndexedSessionSqlEntity>(
        &session,
        branch_sql.as_str(),
    )
    .unwrap_or_else(|err| panic!("branch SQL should lower for plan hash: {err:?}"));
    let repeat_query = lower_select_query_for_tests::<BranchIndexedSessionSqlEntity>(
        &session,
        branch_sql.as_str(),
    )
    .unwrap_or_else(|err| panic!("repeat branch SQL should lower for plan hash: {err:?}"));
    let over_cap_sql = branch_target_over_cap_sql("id", BRANCH_LIMIT);
    let over_cap_query = lower_select_query_for_tests::<BranchIndexedSessionSqlEntity>(
        &session,
        over_cap_sql.as_str(),
    )
    .unwrap_or_else(|err| panic!("over-cap SQL should lower for plan hash: {err:?}"));

    assert_eq!(
        branch_query
            .plan_hash_hex()
            .expect("branch plan hash should derive"),
        repeat_query
            .plan_hash_hex()
            .expect("repeat branch plan hash should derive"),
        "identical branch-route SQL should keep stable plan hash identity",
    );
    assert_ne!(
        branch_query
            .plan_hash_hex()
            .expect("branch plan hash should derive"),
        over_cap_query
            .plan_hash_hex()
            .expect("over-cap plan hash should derive"),
        "branch-route plan identity should not collapse onto an over-cap fallback shape",
    );

    let verbose_sql = format!("EXPLAIN EXECUTION VERBOSE {branch_sql}");
    let first =
        statement_explain_sql::<BranchIndexedSessionSqlEntity>(&session, verbose_sql.as_str())
            .unwrap_or_else(|err| panic!("first branch verbose explain should run: {err:?}"));
    let second =
        statement_explain_sql::<BranchIndexedSessionSqlEntity>(&session, verbose_sql.as_str())
            .unwrap_or_else(|err| panic!("second branch verbose explain should run: {err:?}"));

    assert!(
        first.contains("IndexBranchSet")
            && first.contains("diag.s.semantic_reuse_artifact=shared_prepared_query_plan")
            && first.contains("diag.s.semantic_reuse=miss"),
        "first verbose explain should expose branch route and a cache miss: {first}",
    );
    assert!(
        second.contains("IndexBranchSet")
            && second.contains("diag.s.semantic_reuse_artifact=shared_prepared_query_plan")
            && second.contains("diag.s.semantic_reuse=hit"),
        "second verbose explain should expose branch route and a cache hit: {second}",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_key_only_projection_is_covered_and_bounded() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_sql("id", BRANCH_LIMIT);

    let (_result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("covered branch SQL should execute: {err:?}"));

    assert_eq!(
        attribution.store_get_calls, 0,
        "key-only branch projection should be satisfied from the composite index",
    );
    assert!(
        attribution.index_store_entry_reads <= BRANCH_HEAD_MERGE_READ_CAP,
        "covered branch stream should pull branch heads lazily, got {attribution:?}",
    );
    assert_eq!(
        attribution.scalar_aggregate, None,
        "default page-shaped branch query must not invoke an exact count path",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_noncovered_projection_hydrates_only_bounded_page_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_sql("id, title", BRANCH_LIMIT);

    let (_result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("non-covered branch SQL should execute: {err:?}"));

    assert!(
        (BRANCH_LIMIT as u64..=BRANCH_FETCH).contains(&attribution.store_get_calls),
        "non-covered branch projection should hydrate only returned rows plus lookahead, got {attribution:?}",
    );
    assert!(
        attribution.index_store_entry_reads <= BRANCH_HEAD_MERGE_READ_CAP,
        "non-covered branch stream should pull branch heads lazily, got {attribution:?}",
    );
    assert_eq!(
        attribution.scalar_aggregate, None,
        "non-covered default page-shaped branch query must not invoke count",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_count_covered_predicate_uses_prefix_cardinality() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = format!(
        "SELECT COUNT(*) \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id = '{BRANCH_COLLECTION}' \
           AND stage IN ('Draft', 'Review')",
    );

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("covered branch COUNT SQL should execute: {err:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("covered branch COUNT SQL should return a projection row");
    };

    assert_eq!(
        rows,
        vec![outputs(vec![Value::Nat64(
            expected_branch_ids(usize::MAX).len() as u64
        )])],
        "covered branch COUNT should match the full branch predicate result",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "synchronized branch COUNT should use exact prefix cardinality without row probes",
    );
    assert_eq!(
        attribution.index_store_entry_reads, 0,
        "synchronized branch COUNT should not scan index entries",
    );
    let scalar_aggregate = attribution
        .scalar_aggregate
        .expect("prefix-cardinality COUNT should report its terminal source");
    assert_eq!(
        scalar_aggregate.sink_mode.as_deref(),
        Some("IndexPrefixCardinality"),
        "covered branch COUNT should attribute the exact metadata source",
    );
    assert_eq!(
        scalar_aggregate.terminal_count, 1,
        "covered branch COUNT should report one terminal",
    );
    assert_eq!(
        scalar_aggregate.rows_ingested, 0,
        "metadata COUNT should not ingest rows through the buffered reducer",
    );
}
