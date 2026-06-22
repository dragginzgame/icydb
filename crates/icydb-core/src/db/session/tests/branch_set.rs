use super::*;

const BRANCH_COLLECTION: &str = "01KV5N439P0000000000000000";
const OTHER_COLLECTION: &str = "01KV5N439P1111111111111111";
#[cfg(feature = "diagnostics")]
const SKEW_BRANCH_COLLECTION: &str = "01KV5N439P2222222222222222";
const BRANCH_LIMIT: usize = 3;
const BRANCH_FETCH: u64 = 4;
#[cfg(feature = "diagnostics")]
const BRANCH_HEAD_MERGE_READ_CAP: u64 = BRANCH_FETCH + 1;
#[cfg(feature = "diagnostics")]
const BRANCH_INDEX_RESIDUAL_READ_CAP: u64 = 10;
#[cfg(feature = "diagnostics")]
const SPARSE_COLLECTION_CHILD_PREFIX_RANGE_CAP: u64 = 12;

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

#[cfg(feature = "diagnostics")]
fn skew_branch_target_sql(select: &str, limit: usize) -> String {
    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id = '{SKEW_BRANCH_COLLECTION}' \
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

#[cfg(feature = "diagnostics")]
fn branch_target_index_residual_sql(select: &str, limit: usize) -> String {
    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id = '{BRANCH_COLLECTION}' \
           AND stage IN ('Draft', 'Review') \
           AND stage != 'Review' \
         ORDER BY id ASC \
         LIMIT {limit}",
    )
}

fn branch_target_wide_sql(select: &str, limit: usize) -> String {
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

#[cfg(feature = "diagnostics")]
fn branch_target_wide_sparse_sql(select: &str, limit: usize) -> String {
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

fn branch_target_over_cap_sql(select: &str, limit: usize) -> String {
    let stages = branch_target_over_cap_stage_literals();

    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id = '{BRANCH_COLLECTION}' \
           AND stage IN ({stages}) \
         ORDER BY id ASC \
         LIMIT {limit}",
    )
}

fn branch_target_over_cap_sparse_sql(select: &str, limit: usize) -> String {
    let stages = branch_target_over_cap_stage_literals();

    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id = '{BRANCH_COLLECTION}' \
           AND stage IN ({stages}) \
         ORDER BY id ASC \
         LIMIT {limit}",
    )
}

fn branch_target_over_cap_stage_literals() -> String {
    let mut stages = vec!["'Draft'".to_string(), "'Review'".to_string()];
    stages.extend(
        (0..crate::db::access::MAX_INDEX_BRANCH_SET_VALUES)
            .map(|index| format!("'Missing{index:02}'")),
    );
    stages.join(", ")
}

#[cfg(feature = "diagnostics")]
fn sparse_collection_sql(select: &str, limit: usize) -> String {
    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id IN (\
             '{BRANCH_COLLECTION}', \
             'missing-collection-000', \
             'missing-collection-001', \
             'missing-collection-002', \
             'missing-collection-003', \
             'missing-collection-004'\
         ) \
         ORDER BY id ASC \
         LIMIT {limit}",
    )
}

#[cfg(feature = "diagnostics")]
fn missing_sparse_collection_sql(select: &str, limit: usize) -> String {
    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id IN (\
             'missing-collection-000', \
             'missing-collection-001', \
             'missing-collection-002', \
             'missing-collection-003', \
             'missing-collection-004'\
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

#[cfg(feature = "diagnostics")]
fn seed_skew_branch_set_fixture(session: &DbSession<SessionSqlCanister>) {
    seed_branch_indexed_session_sql_entities(
        session,
        &[
            (10_000, SKEW_BRANCH_COLLECTION, "Draft", "draft-000"),
            (10_010, SKEW_BRANCH_COLLECTION, "Draft", "draft-010"),
            (10_020, SKEW_BRANCH_COLLECTION, "Draft", "draft-020"),
            (10_030, SKEW_BRANCH_COLLECTION, "Draft", "draft-030"),
            (10_040, SKEW_BRANCH_COLLECTION, "Draft", "draft-040"),
            (10_050, SKEW_BRANCH_COLLECTION, "Draft", "draft-050"),
            (10_060, SKEW_BRANCH_COLLECTION, "Draft", "draft-060"),
            (10_070, SKEW_BRANCH_COLLECTION, "Draft", "draft-070"),
            (10_500, SKEW_BRANCH_COLLECTION, "Review", "review-500"),
            (10_510, SKEW_BRANCH_COLLECTION, "Review", "review-510"),
            (10_520, SKEW_BRANCH_COLLECTION, "Review", "review-520"),
            (10_530, SKEW_BRANCH_COLLECTION, "Review", "review-530"),
            (10_540, SKEW_BRANCH_COLLECTION, "Review", "review-540"),
        ],
    );
}

fn branch_descriptor(sql: &str) -> ExplainExecutionNodeDescriptor {
    let session = indexed_sql_session();
    let query = lower_select_query_for_tests::<BranchIndexedSessionSqlEntity>(&session, sql)
        .unwrap_or_else(|err| panic!("branch-set SQL should lower: {err:?}"));

    session
        .explain_query_execution_with_visible_indexes(&query)
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

#[cfg(feature = "diagnostics")]
fn assert_pruned_draft_prefix_route(descriptor: &ExplainExecutionNodeDescriptor) {
    let prefix_node =
        explain_execution_find_first_node(descriptor, ExplainExecutionNodeType::IndexPrefixScan)
            .expect("pruned branch shape should expose one index-prefix route node");

    assert_eq!(
        prefix_node.access_strategy(),
        Some(&ExplainAccessPath::IndexPrefix {
            name: "collection_stage_id".to_string(),
            fields: vec![
                "collection_id".to_string(),
                "stage".to_string(),
                "id".to_string(),
            ],
            prefix_len: 2,
            values: vec![
                Value::Text(BRANCH_COLLECTION.to_string()),
                Value::Text("Draft".to_string()),
            ],
        }),
        "pruned branch route should collapse to the surviving Draft prefix",
    );
    assert!(
        !explain_execution_contains_node_type(descriptor, ExplainExecutionNodeType::IndexBranchSet),
        "single surviving branch should not stay represented as IndexBranchSet",
    );
}

fn expected_branch_rows(limit: usize) -> Vec<Vec<Value>> {
    expected_branch_ids(limit)
        .into_iter()
        .map(|id| vec![Value::Ulid(id)])
        .collect()
}

#[cfg(feature = "diagnostics")]
fn expected_branch_id_title_rows(limit: usize) -> Vec<Vec<Value>> {
    [
        (9_090_u128, "draft-090"),
        (9_095, "review-095"),
        (9_100, "review-100"),
        (9_105, "draft-105"),
        (9_120, "draft-120"),
        (9_125, "review-125"),
        (9_130, "draft-130"),
        (9_135, "review-135"),
        (9_150, "draft-150"),
        (9_155, "review-155"),
        (9_170, "draft-170"),
        (9_175, "review-175"),
    ]
    .into_iter()
    .take(limit)
    .map(|(id, title)| {
        vec![
            Value::Ulid(Ulid::from_u128(id)),
            Value::Text(title.to_string()),
        ]
    })
    .collect()
}

#[cfg(feature = "diagnostics")]
fn expected_branch_id_title_output_rows(limit: usize) -> Vec<Vec<crate::value::OutputValue>> {
    expected_branch_id_title_rows(limit)
        .into_iter()
        .map(outputs)
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

#[cfg(feature = "diagnostics")]
fn expected_collection_ulids(limit: usize) -> Vec<Ulid> {
    [
        9_090_u128, 9_095, 9_100, 9_105, 9_110, 9_120, 9_125, 9_130, 9_135, 9_140, 9_150, 9_155,
        9_160, 9_170, 9_175, 9_180,
    ]
    .into_iter()
    .take(limit)
    .map(Ulid::from_u128)
    .collect()
}

#[cfg(feature = "diagnostics")]
fn expected_collection_ids(limit: usize) -> Vec<Vec<crate::value::OutputValue>> {
    expected_collection_ulids(limit)
        .into_iter()
        .map(|id| outputs(vec![Value::Ulid(id)]))
        .collect()
}

#[cfg(feature = "diagnostics")]
fn expected_collection_id_title_output_rows(limit: usize) -> Vec<Vec<crate::value::OutputValue>> {
    [
        (9_090_u128, "draft-090"),
        (9_095, "review-095"),
        (9_100, "review-100"),
        (9_105, "draft-105"),
        (9_110, "published-110"),
        (9_120, "draft-120"),
        (9_125, "review-125"),
        (9_130, "draft-130"),
        (9_135, "review-135"),
        (9_140, "queued-140"),
        (9_150, "draft-150"),
        (9_155, "review-155"),
        (9_160, "archived-160"),
        (9_170, "draft-170"),
        (9_175, "review-175"),
        (9_180, "rejected-180"),
    ]
    .into_iter()
    .take(limit)
    .map(|(id, title)| {
        outputs(vec![
            Value::Ulid(Ulid::from_u128(id)),
            Value::Text(title.to_string()),
        ])
    })
    .collect()
}

#[cfg(feature = "diagnostics")]
fn expected_skew_branch_rows(limit: usize) -> Vec<Vec<crate::value::OutputValue>> {
    [10_000_u128, 10_010, 10_020, 10_030, 10_040, 10_050]
        .into_iter()
        .take(limit)
        .map(|id| outputs(vec![Value::Ulid(Ulid::from_u128(id))]))
        .collect()
}

#[cfg(feature = "diagnostics")]
fn expected_draft_branch_rows(limit: usize) -> Vec<Vec<Value>> {
    [9_090_u128, 9_105, 9_120, 9_130, 9_150, 9_170]
        .into_iter()
        .take(limit)
        .map(|id| {
            vec![
                Value::Ulid(Ulid::from_u128(id)),
                Value::Text("Draft".to_string()),
            ]
        })
        .collect()
}

#[cfg(feature = "diagnostics")]
fn expected_draft_branch_output_rows(limit: usize) -> Vec<Vec<crate::value::OutputValue>> {
    expected_draft_branch_rows(limit)
        .into_iter()
        .map(outputs)
        .collect()
}

fn paged_branch_ids(
    page: &crate::db::PagedLoadExecution<BranchIndexedSessionSqlEntity>,
) -> Vec<Ulid> {
    page.iter().map(|row| row.id().key()).collect()
}

#[cfg(feature = "diagnostics")]
fn response_branch_ids(
    rows: &crate::db::EntityResponse<BranchIndexedSessionSqlEntity>,
) -> Vec<Ulid> {
    rows.ids().map(|id| id.key()).collect()
}

#[cfg(feature = "diagnostics")]
fn fluent_branch_target_query(limit: u32) -> Query<BranchIndexedSessionSqlEntity> {
    Query::<BranchIndexedSessionSqlEntity>::new(MissingRowPolicy::Ignore)
        .filter(crate::db::query::builder::FieldRef::new("collection_id").eq(BRANCH_COLLECTION))
        .filter(crate::db::query::builder::FieldRef::new("stage").in_list(["Draft", "Review"]))
        .order_term(crate::db::asc("id"))
        .limit(limit)
}

#[cfg(feature = "diagnostics")]
fn fluent_sparse_collection_query(limit: u32) -> Query<BranchIndexedSessionSqlEntity> {
    Query::<BranchIndexedSessionSqlEntity>::new(MissingRowPolicy::Ignore)
        .filter(
            crate::db::query::builder::FieldRef::new("collection_id").in_list([
                BRANCH_COLLECTION,
                "missing-collection-000",
                "missing-collection-001",
                "missing-collection-002",
                "missing-collection-003",
                "missing-collection-004",
            ]),
        )
        .order_term(crate::db::asc("id"))
        .limit(limit)
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
fn session_branch_set_sql_admits_nine_branch_route_under_current_cap() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_wide_sql("id", BRANCH_LIMIT);
    let descriptor = branch_descriptor(sql.as_str());
    let branch_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::IndexBranchSet)
            .unwrap_or_else(|| {
                panic!(
                    "nine-value IN list should stay inside the branch-set cap:\n{}",
                    descriptor.render_text_tree()
                )
            });
    let Some(ExplainAccessPath::IndexBranchSet { branch_values, .. }) =
        branch_node.access_strategy()
    else {
        panic!("wide branch route should expose branch values");
    };

    assert_eq!(
        branch_values.len(),
        9,
        "wide branch route should preserve every exact branch literal",
    );
    assert!(
        !explain_execution_contains_node_type(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort,
        ),
        "wide branch route should keep primary-key ordering without materialized sort",
    );
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

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_over_cap_covering_fallback_does_not_prelimit_prefix_stream() {
    const OVER_CAP_LIMIT: usize = 8;

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_over_cap_sparse_sql("id", OVER_CAP_LIMIT);

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("over-cap covered fallback SQL should execute: {err:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("over-cap covered fallback SQL should return projection rows");
    };

    assert_eq!(
        rows.iter()
            .map(|row| runtime_outputs(row))
            .collect::<Vec<_>>(),
        expected_branch_rows(OVER_CAP_LIMIT),
        "covered over-cap fallback must filter and sort before the page limit",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "key-only over-cap fallback should evaluate fully indexable residual predicates from index keys",
    );
    assert!(
        attribution.index_store_entry_reads > OVER_CAP_LIMIT as u64,
        "over-cap fallback must not cap prefix traversal at the page limit before filtering, got {attribution:?}",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_over_cap_hybrid_fallback_hydrates_only_page_rows_after_filter_sort() {
    const OVER_CAP_LIMIT: usize = 8;

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_over_cap_sparse_sql("id, title", OVER_CAP_LIMIT);

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("over-cap hybrid fallback SQL should execute: {err:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("over-cap hybrid fallback SQL should return projection rows");
    };

    assert_eq!(
        rows,
        expected_branch_id_title_output_rows(OVER_CAP_LIMIT),
        "hybrid over-cap fallback must filter and sort before hydrating row-backed fields",
    );
    assert_eq!(
        attribution.store_get_calls, OVER_CAP_LIMIT as u64,
        "hybrid over-cap fallback should hydrate only final page rows, got {attribution:?}",
    );
    assert!(
        attribution.index_store_entry_reads > OVER_CAP_LIMIT as u64,
        "hybrid over-cap fallback must still scan enough index entries to filter before the page limit, got {attribution:?}",
    );
    assert_eq!(
        attribution.scalar_aggregate, None,
        "hybrid over-cap fallback default page path must not invoke count",
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
fn session_branch_set_sql_index_route_matches_forced_full_scan_fallback() {
    let limit = BRANCH_LIMIT * 2;

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_sql("id", limit);
    let ready_descriptor = branch_descriptor(sql.as_str());

    assert_target_branch_route(&ready_descriptor);

    let ready_rows =
        statement_projection_rows::<BranchIndexedSessionSqlEntity>(&session, sql.as_str())
            .unwrap_or_else(|err| panic!("branch-set indexed projection should execute: {err:?}"));

    hide_indexed_session_indexes();

    let fallback_descriptor = branch_descriptor(sql.as_str());
    assert!(
        !explain_execution_contains_node_type(
            &fallback_descriptor,
            ExplainExecutionNodeType::IndexBranchSet
        ),
        "forced fallback must not reuse the branch-aware route:\n{}",
        fallback_descriptor.render_text_tree(),
    );
    assert!(
        explain_execution_contains_node_type(
            &fallback_descriptor,
            ExplainExecutionNodeType::FullScan
        ),
        "forced fallback should route through a full scan:\n{}",
        fallback_descriptor.render_text_tree(),
    );

    let fallback_rows =
        statement_projection_rows::<BranchIndexedSessionSqlEntity>(&session, sql.as_str())
            .unwrap_or_else(|err| panic!("branch-set fallback projection should execute: {err:?}"));

    assert_eq!(
        ready_rows,
        expected_branch_rows(limit),
        "ready branch route should match the canonical filtered primary-key window",
    );
    assert_eq!(
        fallback_rows, ready_rows,
        "branch-aware index route and forced full-scan fallback should return identical rows",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_skewed_branch_refill_preserves_order_and_stops_at_lookahead() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_skew_branch_set_fixture(&session);
    let sql = skew_branch_target_sql("id", BRANCH_LIMIT);
    let descriptor = branch_descriptor(sql.as_str());

    assert!(
        explain_execution_contains_node_type(&descriptor, ExplainExecutionNodeType::IndexBranchSet),
        "skewed branch query should stay on the branch-aware route",
    );
    assert!(
        !explain_execution_contains_node_type(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort,
        ),
        "skewed branch route should preserve primary-key order without materialized sort",
    );
    assert_target_top_n_fetch(&descriptor);

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("skewed branch SQL should execute: {err:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("skewed branch SQL should return projection rows");
    };

    assert_eq!(
        rows,
        expected_skew_branch_rows(BRANCH_LIMIT),
        "branch chunk refill should preserve global primary-key order",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "key-only skewed branch projection should stay row-store-free",
    );
    assert!(
        attribution.index_store_entry_reads > BRANCH_FETCH,
        "skewed branch page should force a second pull from the leading branch, got {attribution:?}",
    );
    assert!(
        attribution.index_store_entry_reads <= BRANCH_FETCH + 2,
        "skewed branch page should stop after the page lookahead refill, got {attribution:?}",
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
fn session_branch_set_sql_wide_sparse_route_prunes_empty_branch_streams() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_wide_sparse_sql("id", BRANCH_LIMIT);
    let descriptor = branch_descriptor(sql.as_str());

    assert!(
        explain_execution_contains_node_type(&descriptor, ExplainExecutionNodeType::IndexBranchSet),
        "sparse wide branch list should stay inside the branch-set route:\n{}",
        descriptor.render_text_tree(),
    );

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("sparse wide branch SQL should execute: {err:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("sparse wide branch SQL should return projection rows");
    };

    assert_eq!(
        rows.iter()
            .map(|row| runtime_outputs(row))
            .collect::<Vec<_>>(),
        expected_branch_rows(BRANCH_LIMIT),
        "sparse wide branch route should keep branch-merged primary-key order",
    );
    assert!(
        attribution.index_store_entry_reads <= BRANCH_HEAD_MERGE_READ_CAP,
        "empty sparse branch prefixes should be pruned before stream merge, got {attribution:?}",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_index_residual_covering_projection_stays_row_store_free() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_index_residual_sql("id, stage", BRANCH_LIMIT);
    let descriptor = branch_descriptor(sql.as_str());

    assert_pruned_draft_prefix_route(&descriptor);
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "pruned index-residual projection should stay on covering reads",
    );

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("index-residual covered branch SQL should execute: {err:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("index-residual covered branch SQL should return projection rows");
    };

    assert_eq!(
        rows,
        expected_draft_branch_output_rows(BRANCH_LIMIT),
        "index-covered residual predicates must filter before the branch LIMIT",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "index-residual covered prefix projection should avoid row-store reads",
    );
    assert!(
        attribution.index_store_entry_reads <= BRANCH_INDEX_RESIDUAL_READ_CAP,
        "index-residual pruned prefix stream should remain bounded by the page, got {attribution:?}",
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
fn session_branch_set_fluent_full_entity_page_uses_lazy_branch_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let query = fluent_branch_target_query(
        u32::try_from(BRANCH_LIMIT).expect("branch test limit should fit into u32"),
    );
    let descriptor = query
        .explain_execution()
        .expect("fluent branch target should explain execution");

    assert!(
        explain_execution_contains_node_type(&descriptor, ExplainExecutionNodeType::IndexBranchSet),
        "fluent branch target should use the branch-aware route",
    );

    let (result, attribution) = session
        .execute_query_result_with_attribution(&query)
        .expect("fluent branch target should execute");
    let crate::db::LoadQueryResult::Rows(page) = result else {
        panic!("fluent branch target should return scalar rows");
    };

    assert_eq!(
        response_branch_ids(&page),
        expected_branch_ids(BRANCH_LIMIT),
        "fluent branch target should match globally sorted branch-set rows",
    );
    assert!(
        (BRANCH_LIMIT as u64..=BRANCH_FETCH).contains(&attribution.store_get_calls),
        "fluent full-entity branch page should hydrate only returned rows plus lookahead, got {attribution:?}",
    );
    assert!(
        attribution.index_store_entry_reads <= BRANCH_HEAD_MERGE_READ_CAP,
        "fluent branch stream should pull branch heads lazily, got {attribution:?}",
    );
    assert!(
        attribution.direct_data_row.is_some(),
        "fluent branch target should report scalar row attribution",
    );
    assert_eq!(
        attribution.grouped, None,
        "fluent branch target page must not invoke grouped/count work",
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
    assert_eq!(
        attribution.cache.shared_query_plan_hits, 0,
        "direct metadata COUNT should not hit the shared prepared-plan cache",
    );
    assert_eq!(
        attribution.cache.shared_query_plan_misses, 0,
        "direct metadata COUNT should not build a shared prepared-plan cache entry",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_sparse_in_count_uses_direct_prefix_cardinality() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = format!(
        "SELECT COUNT(*) \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id IN (\
             '{BRANCH_COLLECTION}', \
             'missing-collection-000', \
             'missing-collection-001', \
             'missing-collection-002', \
             'missing-collection-003', \
             'missing-collection-004'\
         )",
    );

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("sparse collection COUNT SQL should execute: {err:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("sparse collection COUNT SQL should return a projection row");
    };

    assert_eq!(
        rows,
        vec![outputs(vec![Value::Nat64(16)])],
        "sparse collection COUNT should include only the existing collection prefix",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "sparse collection COUNT should use metadata without row probes",
    );
    assert_eq!(
        attribution.index_store_entry_reads, 0,
        "sparse collection COUNT should not scan index entries",
    );
    let scalar_aggregate = attribution
        .scalar_aggregate
        .expect("sparse collection COUNT should report its terminal source");
    assert_eq!(
        scalar_aggregate.sink_mode.as_deref(),
        Some("IndexPrefixCardinality"),
        "sparse collection COUNT should attribute the exact metadata source",
    );
    assert_eq!(
        attribution.cache.shared_query_plan_hits, 0,
        "direct sparse COUNT should not hit the shared prepared-plan cache",
    );
    assert_eq!(
        attribution.cache.shared_query_plan_misses, 0,
        "direct sparse COUNT should not build a shared prepared-plan cache entry",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_sparse_in_key_only_page_uses_covering_multi_lookup() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = sparse_collection_sql("id", 8);
    let query = lower_select_query_for_tests::<BranchIndexedSessionSqlEntity>(&session, &sql)
        .unwrap_or_else(|err| panic!("sparse collection page SQL should lower: {err:?}"));
    let descriptor = session
        .explain_query_execution_with_visible_indexes(&query)
        .unwrap_or_else(|err| panic!("sparse collection page SQL should explain: {err:?}"));

    assert!(
        explain_execution_contains_node_type(
            &descriptor,
            ExplainExecutionNodeType::IndexMultiLookup
        ),
        "sparse collection page should use the multi-lookup route: {descriptor:?}",
    );
    assert!(
        !explain_execution_contains_node_type(&descriptor, ExplainExecutionNodeType::FullScan),
        "sparse collection page should not use a full scan: {descriptor:?}",
    );

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("sparse collection page SQL should execute: {err:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("sparse collection page SQL should return projection rows");
    };

    assert_eq!(
        rows,
        expected_collection_ids(8),
        "sparse collection page should match primary-key sorted collection rows",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "key-only sparse collection page should stay on covering index payloads",
    );
    assert!(
        attribution.index_store_entry_reads <= 16,
        "sparse collection page should prune missing prefixes and scan only existing collection entries, got {attribution:?}",
    );
    assert!(
        attribution.index_store_range_scan_calls <= SPARSE_COLLECTION_CHILD_PREFIX_RANGE_CAP,
        "sparse collection page should expand only bounded non-empty child prefixes, got {attribution:?}",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_sparse_in_key_only_empty_expansion_returns_empty_page() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = missing_sparse_collection_sql("id", 8);

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("missing sparse collection page SQL should execute: {err:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("missing sparse collection page SQL should return projection rows");
    };

    assert!(
        rows.is_empty(),
        "missing sparse collection page should return no rows",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "missing key-only sparse collection page should stay row-store-free",
    );
    assert_eq!(
        attribution.index_store_entry_reads, 0,
        "missing sparse collection page should not scan index entries after empty child-prefix expansion",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_sparse_in_hybrid_page_expands_child_prefix_streams() {
    const LIMIT: usize = 8;

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = sparse_collection_sql("id, title", LIMIT);
    let query = lower_select_query_for_tests::<BranchIndexedSessionSqlEntity>(&session, &sql)
        .unwrap_or_else(|err| panic!("sparse collection hybrid SQL should lower: {err:?}"));
    let descriptor = session
        .explain_query_execution_with_visible_indexes(&query)
        .unwrap_or_else(|err| panic!("sparse collection hybrid SQL should explain: {err:?}"));

    assert!(
        explain_execution_contains_node_type(
            &descriptor,
            ExplainExecutionNodeType::IndexMultiLookup
        ),
        "sparse collection hybrid page should use the multi-lookup route: {descriptor:?}",
    );
    assert!(
        !explain_execution_contains_node_type(&descriptor, ExplainExecutionNodeType::FullScan),
        "sparse collection hybrid page should not use a full scan: {descriptor:?}",
    );

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("sparse collection hybrid SQL should execute: {err:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("sparse collection hybrid SQL should return projection rows");
    };

    assert_eq!(
        rows,
        expected_collection_id_title_output_rows(LIMIT),
        "sparse collection hybrid page should match primary-key sorted collection rows",
    );
    assert_eq!(
        attribution.store_get_calls, LIMIT as u64,
        "hybrid sparse collection page should hydrate only returned row-backed fields, got {attribution:?}",
    );
    assert_eq!(
        attribution
            .hybrid_covering
            .expect("sparse collection hybrid page should report hybrid covering")
            .row_field_accesses,
        LIMIT as u64,
        "hybrid sparse collection page should read one row-backed field per returned row",
    );
    assert!(
        attribution.index_store_entry_reads <= 16,
        "sparse collection hybrid page should prune missing prefixes and scan only existing collection entries, got {attribution:?}",
    );
    assert!(
        attribution.index_store_range_scan_calls <= SPARSE_COLLECTION_CHILD_PREFIX_RANGE_CAP,
        "sparse collection hybrid page should expand only bounded non-empty child prefixes, got {attribution:?}",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_fluent_sparse_in_full_entity_page_expands_child_prefix_streams() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let query = fluent_sparse_collection_query(8);
    let descriptor = query
        .explain_execution()
        .expect("sparse fluent page should explain execution");

    assert!(
        explain_execution_contains_node_type(
            &descriptor,
            ExplainExecutionNodeType::IndexMultiLookup
        ),
        "sparse fluent page should use the multi-lookup route: {descriptor:?}",
    );
    assert!(
        !explain_execution_contains_node_type(&descriptor, ExplainExecutionNodeType::FullScan),
        "sparse fluent page should not use a full scan: {descriptor:?}",
    );
    assert!(
        !explain_execution_contains_node_type(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort,
        ),
        "child-prefix-expanded sparse fluent page should not require a materialized sort: {descriptor:?}",
    );
    assert!(
        explain_execution_contains_node_type(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied,
        ),
        "child-prefix-expanded sparse fluent page should prove primary-key order: {descriptor:?}",
    );

    let (result, attribution) = session
        .execute_query_result_with_attribution(&query)
        .expect("sparse fluent page should execute");
    let crate::db::LoadQueryResult::Rows(page) = result else {
        panic!("sparse fluent page should return scalar rows");
    };

    assert_eq!(
        response_branch_ids(&page),
        expected_collection_ulids(8),
        "sparse fluent page should match primary-key sorted collection rows",
    );
    assert!(
        (8..=9).contains(&attribution.store_get_calls),
        "sparse fluent full-entity page should hydrate only returned rows plus lookahead, got {attribution:?}",
    );
    assert!(
        attribution.index_store_range_scan_calls > 1,
        "sparse fluent page should expand the parent collection prefix into child prefix streams, got {attribution:?}",
    );
    assert!(
        attribution.index_store_range_scan_calls <= 12,
        "sparse fluent page should keep metadata probes and child-prefix streams bounded, got {attribution:?}",
    );
    assert!(
        attribution.index_store_entry_reads <= 24,
        "sparse fluent child-prefix streams should stay bounded by the page, got {attribution:?}",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_fluent_count_covered_predicate_reports_prefix_cardinality() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);

    let (count, attribution) = session
        .load::<BranchIndexedSessionSqlEntity>()
        .filter(crate::db::query::builder::FieldRef::new("collection_id").eq(BRANCH_COLLECTION))
        .filter(crate::db::query::builder::FieldRef::new("stage").in_list(["Draft", "Review"]))
        .count_with_attribution()
        .unwrap_or_else(|err| panic!("covered branch fluent COUNT should execute: {err:?}"));

    assert_eq!(
        count,
        u32::try_from(expected_branch_ids(usize::MAX).len())
            .expect("branch count should fit into u32"),
        "fluent covered COUNT should match the full branch predicate result",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "fluent synchronized branch COUNT should use exact prefix cardinality without row probes",
    );
    assert_eq!(
        attribution.index_store_entry_reads, 0,
        "fluent synchronized branch COUNT should not scan index entries",
    );
    let scalar_aggregate = attribution
        .scalar_aggregate
        .expect("fluent prefix-cardinality COUNT should report its terminal source");
    assert_eq!(
        scalar_aggregate.sink_mode.as_deref(),
        Some("IndexPrefixCardinality"),
        "fluent covered branch COUNT should attribute the exact metadata source",
    );
    assert_eq!(
        scalar_aggregate.terminal_count, 1,
        "fluent covered branch COUNT should report one terminal",
    );
    assert_eq!(
        scalar_aggregate.rows_ingested, 0,
        "fluent metadata COUNT should not ingest rows through the buffered reducer",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_fluent_exists_reports_existing_rows_terminal_attribution() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let query = fluent_branch_target_query(
        u32::try_from(BRANCH_LIMIT).expect("branch test limit should fit into u32"),
    );

    let (exists, attribution) = session
        .load::<BranchIndexedSessionSqlEntity>()
        .filter(crate::db::query::builder::FieldRef::new("collection_id").eq(BRANCH_COLLECTION))
        .filter(crate::db::query::builder::FieldRef::new("stage").in_list(["Draft", "Review"]))
        .order_term(crate::db::asc("id"))
        .limit(u32::try_from(BRANCH_LIMIT).expect("branch test limit should fit into u32"))
        .exists_with_attribution()
        .unwrap_or_else(|err| panic!("covered branch fluent EXISTS should execute: {err:?}"));

    assert!(
        exists,
        "fluent EXISTS should find a row for the branch predicate",
    );
    let descriptor = query
        .explain_execution()
        .expect("fluent branch EXISTS baseline should explain");
    assert!(
        explain_execution_contains_node_type(&descriptor, ExplainExecutionNodeType::IndexBranchSet),
        "fluent EXISTS baseline should retain the branch-aware route shape",
    );
    assert!(
        (BRANCH_LIMIT as u64..=BRANCH_FETCH).contains(&attribution.store_get_calls),
        "fluent branch EXISTS should hydrate only the bounded effective window, got {attribution:?}",
    );
    assert!(
        attribution.index_store_entry_reads <= BRANCH_HEAD_MERGE_READ_CAP,
        "fluent branch EXISTS should keep branch traversal bounded, got {attribution:?}",
    );
    let scalar_aggregate = attribution
        .scalar_aggregate
        .expect("fluent EXISTS should report its terminal source");
    assert_eq!(
        scalar_aggregate.sink_mode.as_deref(),
        Some("KernelAggregate"),
        "fluent EXISTS should attribute the kernel aggregate terminal source",
    );
    assert_eq!(
        scalar_aggregate.terminal_count, 1,
        "fluent EXISTS should report one terminal",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_count_duplicate_branch_literals_use_unique_prefix_cardinality() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = format!(
        "SELECT COUNT(*) \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id = '{BRANCH_COLLECTION}' \
           AND stage IN ('Draft', 'Draft', 'Review')",
    );

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("duplicate-literal branch COUNT SQL should execute: {err:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("duplicate-literal branch COUNT SQL should return a projection row");
    };

    assert_eq!(
        rows,
        vec![outputs(vec![Value::Nat64(
            expected_branch_ids(usize::MAX).len() as u64
        )])],
        "duplicate branch literals must not double-count exact prefix cardinality",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "duplicate-literal branch COUNT should stay row-store-free",
    );
    assert_eq!(
        attribution.index_store_entry_reads, 0,
        "duplicate-literal branch COUNT should stay index-scan-free",
    );
    let scalar_aggregate = attribution
        .scalar_aggregate
        .expect("duplicate-literal prefix-cardinality COUNT should report its terminal source");
    assert_eq!(
        scalar_aggregate.sink_mode.as_deref(),
        Some("IndexPrefixCardinality"),
        "duplicate-literal branch COUNT should still use exact metadata",
    );
    assert_eq!(
        scalar_aggregate.rows_ingested, 0,
        "duplicate-literal metadata COUNT should not ingest rows through the buffered reducer",
    );
}
