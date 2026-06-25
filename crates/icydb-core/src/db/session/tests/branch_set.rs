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
#[cfg(feature = "diagnostics")]
const SPARSE_COLLECTION_CHILD_PREFIX_OVER_CAP: usize = 40;
#[cfg(feature = "diagnostics")]
const SPARSE_COLLECTION_CHILD_PREFIX_COMBINED_OVER_CAP_PARENTS: usize =
    crate::db::access::MAX_INDEX_BRANCH_SET_VALUES;
#[cfg(feature = "diagnostics")]
const SPARSE_COLLECTION_CHILD_PREFIX_COMBINED_OVER_CAP_STAGES: [&str; 3] =
    ["Draft", "Review", "Queued"];
#[cfg(feature = "diagnostics")]
const SPARSE_COLLECTION_CHILD_PREFIX_EXACT_CAP_PARENTS: usize = 11;
#[cfg(feature = "diagnostics")]
const SPARSE_COLLECTION_IDS: [&str; 6] = [
    BRANCH_COLLECTION,
    "missing-collection-000",
    "missing-collection-001",
    "missing-collection-002",
    "missing-collection-003",
    "missing-collection-004",
];
#[cfg(feature = "diagnostics")]
const MISSING_SPARSE_COLLECTION_IDS: [&str; 5] = [
    "missing-collection-000",
    "missing-collection-001",
    "missing-collection-002",
    "missing-collection-003",
    "missing-collection-004",
];

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
    sparse_collection_ordered_sql(select, "ASC", limit)
}

#[cfg(feature = "diagnostics")]
fn sparse_collection_desc_sql(select: &str, limit: usize) -> String {
    sparse_collection_ordered_sql(select, "DESC", limit)
}

#[cfg(feature = "diagnostics")]
fn sparse_collection_ordered_sql(select: &str, direction: &str, limit: usize) -> String {
    let collections = sparse_collection_literal_list();

    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id IN ({collections}) \
         ORDER BY id {direction} \
         LIMIT {limit}",
    )
}

#[cfg(feature = "diagnostics")]
fn sparse_collection_count_sql() -> String {
    let collections = sparse_collection_literal_list();

    format!(
        "SELECT COUNT(*) \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id IN ({collections})",
    )
}

#[cfg(feature = "diagnostics")]
fn missing_sparse_collection_count_sql() -> String {
    let collections = missing_sparse_collection_literal_list();

    format!(
        "SELECT COUNT(*) \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id IN ({collections})",
    )
}

#[cfg(feature = "diagnostics")]
fn sparse_collection_literal_list() -> String {
    SPARSE_COLLECTION_IDS
        .into_iter()
        .map(|collection_id| format!("'{collection_id}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(feature = "diagnostics")]
fn missing_sparse_collection_sql(select: &str, limit: usize) -> String {
    let collections = missing_sparse_collection_literal_list();

    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id IN ({collections}) \
         ORDER BY id ASC \
         LIMIT {limit}",
    )
}

#[cfg(feature = "diagnostics")]
fn missing_sparse_collection_literal_list() -> String {
    MISSING_SPARSE_COLLECTION_IDS
        .into_iter()
        .map(|collection_id| format!("'{collection_id}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(feature = "diagnostics")]
fn combined_child_prefix_over_cap_sql(select: &str, limit: usize) -> String {
    let collections = combined_child_prefix_collection_ids()
        .into_iter()
        .map(|collection_id| format!("'{collection_id}'"))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id IN ({collections}) \
         ORDER BY id ASC \
         LIMIT {limit}",
    )
}

#[cfg(feature = "diagnostics")]
fn combined_child_prefix_exact_cap_with_missing_sql(select: &str, limit: usize) -> String {
    let collections = combined_child_prefix_exact_cap_with_missing_collection_ids()
        .into_iter()
        .map(|collection_id| format!("'{collection_id}'"))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "SELECT {select} \
         FROM BranchIndexedSessionSqlEntity \
         WHERE collection_id IN ({collections}) \
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

#[cfg(feature = "diagnostics")]
fn combined_child_prefix_parent_base_id(parent_index: usize) -> u128 {
    let parent_offset = u128::try_from(parent_index).expect("test index should fit");
    if parent_index + 1 == SPARSE_COLLECTION_CHILD_PREFIX_COMBINED_OVER_CAP_PARENTS {
        21_000
    } else {
        30_000 + parent_offset * 100
    }
}

#[cfg(feature = "diagnostics")]
fn combined_child_prefix_row_id(parent_index: usize, stage_index: usize) -> u128 {
    combined_child_prefix_parent_base_id(parent_index)
        + u128::try_from(stage_index).expect("test index should fit")
}

#[cfg(feature = "diagnostics")]
fn combined_child_prefix_collection_id(parent_index: usize) -> String {
    format!("cap-parent-{parent_index:02}")
}

#[cfg(feature = "diagnostics")]
fn combined_child_prefix_collection_ids() -> Vec<String> {
    (0..SPARSE_COLLECTION_CHILD_PREFIX_COMBINED_OVER_CAP_PARENTS)
        .map(combined_child_prefix_collection_id)
        .collect()
}

#[cfg(feature = "diagnostics")]
fn combined_child_prefix_exact_cap_with_missing_collection_ids() -> Vec<String> {
    let mut collection_ids = (0..SPARSE_COLLECTION_CHILD_PREFIX_EXACT_CAP_PARENTS)
        .map(combined_child_prefix_collection_id)
        .collect::<Vec<_>>();
    collection_ids.extend((0..5).map(|index| format!("cap-missing-{index:02}")));
    collection_ids
}

#[cfg(feature = "diagnostics")]
fn combined_child_prefix_title(parent_index: usize, stage_index: usize) -> String {
    format!("cap-parent-{parent_index:02}-{stage_index:02}")
}

#[cfg(feature = "diagnostics")]
fn seed_combined_child_prefix_over_cap_fixture(session: &DbSession<SessionSqlCanister>) {
    let rows = (0..SPARSE_COLLECTION_CHILD_PREFIX_COMBINED_OVER_CAP_PARENTS)
        .flat_map(|parent_index| {
            SPARSE_COLLECTION_CHILD_PREFIX_COMBINED_OVER_CAP_STAGES
                .iter()
                .enumerate()
                .map(move |(stage_index, stage)| {
                    let id = combined_child_prefix_row_id(parent_index, stage_index);
                    (
                        id,
                        combined_child_prefix_collection_id(parent_index),
                        (*stage).to_string(),
                        combined_child_prefix_title(parent_index, stage_index),
                    )
                })
        })
        .collect::<Vec<_>>();

    insert_session_fixture_rows(
        session,
        rows,
        |(id, collection_id, stage, title)| BranchIndexedSessionSqlEntity {
            id: Ulid::from_u128(id),
            collection_id,
            stage,
            title,
        },
        "combined child-prefix over-cap branch indexed SQL",
    );
}

#[cfg(feature = "diagnostics")]
fn seed_child_prefix_over_cap_fixture(session: &DbSession<SessionSqlCanister>) {
    let rows = (0..SPARSE_COLLECTION_CHILD_PREFIX_OVER_CAP)
        .map(|index| {
            (
                20_000 + u128::try_from(index).expect("test index should fit") * 10,
                BRANCH_COLLECTION.to_string(),
                format!("Stage{index:02}"),
                format!("cap-{index:02}"),
            )
        })
        .collect::<Vec<_>>();

    insert_session_fixture_rows(
        session,
        rows,
        |(id, collection_id, stage, title)| BranchIndexedSessionSqlEntity {
            id: Ulid::from_u128(id),
            collection_id,
            stage,
            title,
        },
        "child-prefix over-cap branch indexed SQL",
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

fn target_branch_access_path() -> ExplainAccessPath {
    ExplainAccessPath::IndexBranchSet {
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
        branch_field: Some("stage".to_string()),
        ordered_suffix: "primary_key_asc".to_string(),
    }
}

fn assert_target_branch_route(descriptor: &ExplainExecutionNodeDescriptor) {
    let branch_node =
        explain_execution_find_first_node(descriptor, ExplainExecutionNodeType::IndexBranchSet)
            .expect("target shape should expose one branch-aware route node");

    assert_eq!(
        branch_node.access_strategy(),
        Some(&target_branch_access_path()),
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
fn expected_collection_ids_desc(limit: usize) -> Vec<Vec<crate::value::OutputValue>> {
    let mut ids = expected_collection_ulids(usize::MAX);
    ids.reverse();
    ids.into_iter()
        .take(limit)
        .map(|id| outputs(vec![Value::Ulid(id)]))
        .collect()
}

#[cfg(feature = "diagnostics")]
fn expected_child_prefix_over_cap_ids(limit: usize) -> Vec<Vec<crate::value::OutputValue>> {
    (0..SPARSE_COLLECTION_CHILD_PREFIX_OVER_CAP)
        .take(limit)
        .map(|index| {
            outputs(vec![Value::Ulid(Ulid::from_u128(
                20_000 + u128::try_from(index).expect("test index should fit") * 10,
            ))])
        })
        .collect()
}

#[cfg(feature = "diagnostics")]
fn combined_child_prefix_sorted_rows() -> Vec<(u128, String, String)> {
    let mut rows = (0..SPARSE_COLLECTION_CHILD_PREFIX_COMBINED_OVER_CAP_PARENTS)
        .flat_map(|parent_index| {
            (0..SPARSE_COLLECTION_CHILD_PREFIX_COMBINED_OVER_CAP_STAGES.len()).map(
                move |stage_index| {
                    (
                        combined_child_prefix_row_id(parent_index, stage_index),
                        combined_child_prefix_collection_id(parent_index),
                        combined_child_prefix_title(parent_index, stage_index),
                    )
                },
            )
        })
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|(id, _, _)| *id);

    rows
}

#[cfg(feature = "diagnostics")]
fn combined_child_prefix_exact_cap_sorted_rows() -> Vec<(u128, String, String)> {
    let mut rows = (0..SPARSE_COLLECTION_CHILD_PREFIX_EXACT_CAP_PARENTS)
        .flat_map(|parent_index| {
            (0..SPARSE_COLLECTION_CHILD_PREFIX_COMBINED_OVER_CAP_STAGES.len()).map(
                move |stage_index| {
                    (
                        combined_child_prefix_row_id(parent_index, stage_index),
                        combined_child_prefix_collection_id(parent_index),
                        combined_child_prefix_title(parent_index, stage_index),
                    )
                },
            )
        })
        .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|(id, _, _)| *id);

    rows
}

#[cfg(feature = "diagnostics")]
fn combined_child_prefix_fixture_entry_count() -> u64 {
    u64::try_from(
        SPARSE_COLLECTION_CHILD_PREFIX_COMBINED_OVER_CAP_PARENTS
            * SPARSE_COLLECTION_CHILD_PREFIX_COMBINED_OVER_CAP_STAGES.len(),
    )
    .expect("fixture size should fit")
}

#[cfg(feature = "diagnostics")]
fn expected_combined_child_prefix_over_cap_ids(
    limit: usize,
) -> Vec<Vec<crate::value::OutputValue>> {
    expected_combined_child_prefix_over_cap_ulids(limit)
        .into_iter()
        .map(|id| outputs(vec![Value::Ulid(id)]))
        .collect()
}

#[cfg(feature = "diagnostics")]
fn expected_combined_child_prefix_exact_cap_ids(
    limit: usize,
) -> Vec<Vec<crate::value::OutputValue>> {
    combined_child_prefix_exact_cap_sorted_rows()
        .into_iter()
        .take(limit)
        .map(|(id, _, _)| outputs(vec![Value::Ulid(Ulid::from_u128(id))]))
        .collect()
}

#[cfg(feature = "diagnostics")]
fn expected_combined_child_prefix_over_cap_ulids(limit: usize) -> Vec<Ulid> {
    combined_child_prefix_sorted_rows()
        .into_iter()
        .take(limit)
        .map(|(id, _, _)| Ulid::from_u128(id))
        .collect()
}

#[cfg(feature = "diagnostics")]
fn expected_combined_child_prefix_over_cap_id_collection_rows(
    limit: usize,
) -> Vec<Vec<crate::value::OutputValue>> {
    combined_child_prefix_sorted_rows()
        .into_iter()
        .take(limit)
        .map(|(id, collection_id, _)| {
            outputs(vec![
                Value::Ulid(Ulid::from_u128(id)),
                Value::Text(collection_id),
            ])
        })
        .collect()
}

#[cfg(feature = "diagnostics")]
fn expected_combined_child_prefix_over_cap_id_collection_title_rows(
    limit: usize,
) -> Vec<Vec<crate::value::OutputValue>> {
    combined_child_prefix_sorted_rows()
        .into_iter()
        .take(limit)
        .map(|(id, collection_id, title)| {
            outputs(vec![
                Value::Ulid(Ulid::from_u128(id)),
                Value::Text(collection_id),
                Value::Text(title),
            ])
        })
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
fn assert_fluent_prefix_cardinality_terminal(
    attribution: &crate::db::FluentTerminalExecutionAttribution,
    context: &str,
) {
    assert_eq!(
        attribution.store_get_calls, 0,
        "{context} should use metadata without row probes",
    );
    assert_eq!(
        attribution.index_store_entry_reads, 0,
        "{context} should not scan index entries",
    );
    assert_eq!(
        attribution.index_store_range_scan_calls, 0,
        "{context} should not open index ranges for missing prefixes",
    );
    let scalar_aggregate = attribution
        .scalar_aggregate
        .as_ref()
        .unwrap_or_else(|| panic!("{context} should report its terminal source"));
    assert_eq!(
        scalar_aggregate.sink_mode.as_deref(),
        Some("IndexPrefixCardinality"),
        "{context} should attribute the exact metadata source",
    );
    assert_eq!(
        scalar_aggregate.terminal_count, 1,
        "{context} should report one terminal",
    );
    assert_eq!(
        scalar_aggregate.rows_ingested, 0,
        "{context} should not ingest rows through the buffered reducer",
    );
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
fn sparse_collection_filter_expr() -> crate::db::FilterExpr {
    crate::db::query::builder::FieldRef::new("collection_id").in_list(SPARSE_COLLECTION_IDS)
}

#[cfg(feature = "diagnostics")]
fn missing_sparse_collection_filter_expr() -> crate::db::FilterExpr {
    crate::db::query::builder::FieldRef::new("collection_id").in_list(MISSING_SPARSE_COLLECTION_IDS)
}

#[cfg(feature = "diagnostics")]
fn fluent_sparse_collection_query(limit: u32) -> Query<BranchIndexedSessionSqlEntity> {
    Query::<BranchIndexedSessionSqlEntity>::new(MissingRowPolicy::Ignore)
        .filter(sparse_collection_filter_expr())
        .order_term(crate::db::asc("id"))
        .limit(limit)
}

#[cfg(feature = "diagnostics")]
fn fluent_combined_child_prefix_over_cap_query(limit: u32) -> Query<BranchIndexedSessionSqlEntity> {
    Query::<BranchIndexedSessionSqlEntity>::new(MissingRowPolicy::Ignore)
        .filter(
            crate::db::query::builder::FieldRef::new("collection_id")
                .in_list(combined_child_prefix_collection_ids()),
        )
        .order_term(crate::db::asc("id"))
        .limit(limit)
}

#[cfg(feature = "diagnostics")]
fn execute_fluent_sparse_collection_page(
    session: &DbSession<SessionSqlCanister>,
    limit: usize,
    cursor: Option<&str>,
) -> crate::db::PagedLoadExecution<BranchIndexedSessionSqlEntity> {
    let query = session
        .load::<BranchIndexedSessionSqlEntity>()
        .filter(sparse_collection_filter_expr())
        .order_term(crate::db::asc("id"))
        .limit(u32::try_from(limit).expect("sparse collection test limit should fit into u32"));
    let query = if let Some(cursor) = cursor {
        query.cursor(cursor)
    } else {
        query
    };

    query
        .execute_paged()
        .unwrap_or_else(|err| panic!("sparse collection fluent page should execute: {err:?}"))
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

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_and_fluent_share_branch_route_identity() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = branch_target_sql("id", BRANCH_LIMIT);
    let sql_descriptor = branch_descriptor(sql.as_str());
    let fluent_query = fluent_branch_target_query(
        u32::try_from(BRANCH_LIMIT).expect("branch test limit should fit into u32"),
    );
    let fluent_descriptor = fluent_query
        .explain_execution()
        .expect("fluent branch target should explain execution");

    assert_target_branch_route(&sql_descriptor);
    assert_target_branch_route(&fluent_descriptor);

    let sql_branch = explain_execution_find_first_node(
        &sql_descriptor,
        ExplainExecutionNodeType::IndexBranchSet,
    )
    .expect("SQL target should expose one branch route node");
    let fluent_branch = explain_execution_find_first_node(
        &fluent_descriptor,
        ExplainExecutionNodeType::IndexBranchSet,
    )
    .expect("fluent target should expose one branch route node");

    assert_eq!(
        sql_branch.access_strategy(),
        fluent_branch.access_strategy(),
        "equivalent SQL and fluent shapes should share the same branch route identity",
    );

    let sql_rows =
        statement_projection_rows::<BranchIndexedSessionSqlEntity>(&session, sql.as_str())
            .unwrap_or_else(|err| panic!("branch-set SQL projection should execute: {err:?}"));
    let (result, attribution) = session
        .execute_query_result_with_attribution(&fluent_query)
        .expect("fluent branch target should execute");
    let crate::db::LoadQueryResult::Rows(page) = result else {
        panic!("fluent branch target should return scalar rows");
    };
    let fluent_rows = response_branch_ids(&page)
        .into_iter()
        .map(|id| vec![Value::Ulid(id)])
        .collect::<Vec<_>>();

    assert_eq!(
        sql_rows, fluent_rows,
        "equivalent SQL and fluent branch shapes should return the same primary-key page",
    );
    assert_eq!(
        attribution.grouped, None,
        "fluent branch page convergence check must not invoke grouped/count work",
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
    let sql = sparse_collection_count_sql();

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
fn session_branch_set_sql_missing_sparse_in_count_uses_empty_prefix_cardinality() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = missing_sparse_collection_count_sql();

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| {
            panic!("missing sparse collection COUNT SQL should execute: {err:?}")
        });
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("missing sparse collection COUNT SQL should return a projection row");
    };

    assert_eq!(
        rows,
        vec![outputs(vec![Value::Nat64(0)])],
        "missing sparse collection COUNT should return zero from empty prefixes",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "missing sparse collection COUNT should use metadata without row probes",
    );
    assert_eq!(
        attribution.index_store_entry_reads, 0,
        "missing sparse collection COUNT should not scan index entries",
    );
    assert_eq!(
        attribution.index_store_range_scan_calls, 0,
        "missing sparse collection COUNT should not open index ranges",
    );
    let scalar_aggregate = attribution
        .scalar_aggregate
        .expect("missing sparse collection COUNT should report its terminal source");
    assert_eq!(
        scalar_aggregate.sink_mode.as_deref(),
        Some("IndexPrefixCardinality"),
        "missing sparse collection COUNT should attribute the exact metadata source",
    );
    assert_eq!(
        scalar_aggregate.rows_ingested, 0,
        "missing sparse metadata COUNT should not ingest rows through the buffered reducer",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_fluent_sparse_in_count_uses_prefix_cardinality() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);

    let (count, attribution) = session
        .load::<BranchIndexedSessionSqlEntity>()
        .filter(sparse_collection_filter_expr())
        .count_with_attribution()
        .unwrap_or_else(|err| panic!("sparse collection fluent COUNT should execute: {err:?}"));

    assert_eq!(
        count, 16,
        "fluent sparse collection COUNT should include only the existing collection prefix",
    );
    assert_fluent_prefix_cardinality_terminal(&attribution, "fluent sparse collection COUNT");
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_fluent_missing_sparse_in_count_uses_empty_prefix_cardinality() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);

    let (count, attribution) = session
        .load::<BranchIndexedSessionSqlEntity>()
        .filter(missing_sparse_collection_filter_expr())
        .count_with_attribution()
        .unwrap_or_else(|err| {
            panic!("missing sparse collection fluent COUNT should execute: {err:?}")
        });

    assert_eq!(
        count, 0,
        "fluent missing sparse collection COUNT should return zero from empty prefixes",
    );
    assert_fluent_prefix_cardinality_terminal(
        &attribution,
        "fluent missing sparse collection COUNT",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_fluent_sparse_in_exists_uses_prefix_cardinality() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);

    let (exists, attribution) = session
        .load::<BranchIndexedSessionSqlEntity>()
        .filter(sparse_collection_filter_expr())
        .exists_with_attribution()
        .unwrap_or_else(|err| panic!("sparse collection fluent EXISTS should execute: {err:?}"));

    assert!(
        exists,
        "fluent sparse collection EXISTS should find the existing collection prefix",
    );
    assert_fluent_prefix_cardinality_terminal(&attribution, "fluent sparse collection EXISTS");
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_fluent_missing_sparse_in_exists_uses_empty_prefix_cardinality() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);

    let (exists, attribution) = session
        .load::<BranchIndexedSessionSqlEntity>()
        .filter(missing_sparse_collection_filter_expr())
        .exists_with_attribution()
        .unwrap_or_else(|err| {
            panic!("missing sparse collection fluent EXISTS should execute: {err:?}")
        });

    assert!(
        !exists,
        "fluent missing sparse collection EXISTS should return false from empty prefixes",
    );
    assert_fluent_prefix_cardinality_terminal(
        &attribution,
        "fluent missing sparse collection EXISTS",
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
fn session_branch_set_sql_sparse_in_child_prefix_over_cap_falls_back_safely() {
    const LIMIT: usize = 8;

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_child_prefix_over_cap_fixture(&session);
    let sql = sparse_collection_sql("id", LIMIT);

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| {
            panic!("over-cap sparse collection page SQL should execute: {err:?}")
        });
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("over-cap sparse collection page SQL should return projection rows");
    };

    assert_eq!(
        rows,
        expected_child_prefix_over_cap_ids(LIMIT),
        "over-cap sparse collection page should fall back without losing primary-key order",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "over-cap key-only sparse collection fallback should stay row-store-free",
    );
    assert!(
        attribution.index_store_entry_reads >= SPARSE_COLLECTION_CHILD_PREFIX_OVER_CAP as u64,
        "over-cap sparse collection fallback should materialize the parent prefix instead of pretending the capped child-prefix expansion succeeded, got {attribution:?}",
    );
    assert_eq!(
        attribution.scalar_aggregate, None,
        "over-cap sparse collection default page path must not invoke count",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_sparse_in_exact_cap_ignores_trailing_empty_parent_prefixes() {
    const LIMIT: usize = 32;
    const EXACT_CHILD_PREFIX_COUNT: u64 = 33;

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_combined_child_prefix_over_cap_fixture(&session);
    let sql = combined_child_prefix_exact_cap_with_missing_sql("id", LIMIT);

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| {
            panic!("exact-cap sparse collection page SQL should execute: {err:?}")
        });
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("exact-cap sparse collection page SQL should return projection rows");
    };

    assert_eq!(
        rows,
        expected_combined_child_prefix_exact_cap_ids(LIMIT),
        "exact-cap child-prefix expansion should keep rows from every non-empty child prefix",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "exact-cap key-only sparse collection expansion should stay row-store-free",
    );
    assert!(
        attribution.index_store_range_scan_calls >= EXACT_CHILD_PREFIX_COUNT,
        "exact-cap sparse collection route should expand child prefixes instead of falling back to parent-prefix scans, got {attribution:?}",
    );
    assert_eq!(
        attribution.scalar_aggregate, None,
        "exact-cap sparse collection default page path must not invoke count",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_sparse_in_combined_child_prefix_over_cap_falls_back_completely() {
    const LIMIT: usize = 8;

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_combined_child_prefix_over_cap_fixture(&session);
    let sql = combined_child_prefix_over_cap_sql("id", LIMIT);

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| {
            panic!("combined over-cap sparse collection page SQL should execute: {err:?}")
        });
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("combined over-cap sparse collection page SQL should return projection rows");
    };

    assert_eq!(
        rows,
        expected_combined_child_prefix_over_cap_ids(LIMIT),
        "combined over-cap child-prefix fallback must not lose rows from prefixes beyond the expansion cap",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "combined over-cap key-only sparse collection fallback should stay row-store-free",
    );
    assert!(
        attribution.index_store_entry_reads >= combined_child_prefix_fixture_entry_count(),
        "combined over-cap sparse collection fallback should materialize the complete parent-prefix route, got {attribution:?}",
    );
    assert_eq!(
        attribution.scalar_aggregate, None,
        "combined over-cap sparse collection default page path must not invoke count",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_sparse_in_combined_child_prefix_over_cap_decodes_index_components() {
    const LIMIT: usize = 8;

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_combined_child_prefix_over_cap_fixture(&session);
    let sql = combined_child_prefix_over_cap_sql("id, collection_id", LIMIT);

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| {
            panic!("combined over-cap sparse component projection SQL should execute: {err:?}")
        });
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("combined over-cap sparse component projection SQL should return projection rows");
    };

    assert_eq!(
        rows,
        expected_combined_child_prefix_over_cap_id_collection_rows(LIMIT),
        "combined over-cap component fallback must decode index fields after complete primary-key sorting",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "combined over-cap index-component projection should stay row-store-free",
    );
    assert!(
        attribution.index_store_entry_reads >= combined_child_prefix_fixture_entry_count(),
        "combined over-cap component fallback should scan the complete parent-prefix route, got {attribution:?}",
    );
    assert_eq!(
        attribution.scalar_aggregate, None,
        "combined over-cap component projection default page path must not invoke count",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_sparse_in_combined_child_prefix_over_cap_hydrates_page_rows() {
    const LIMIT: usize = 8;

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_combined_child_prefix_over_cap_fixture(&session);
    let sql = combined_child_prefix_over_cap_sql("id, collection_id, title", LIMIT);

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| {
            panic!("combined over-cap sparse hybrid projection SQL should execute: {err:?}")
        });
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("combined over-cap sparse hybrid projection SQL should return projection rows");
    };

    assert_eq!(
        rows,
        expected_combined_child_prefix_over_cap_id_collection_title_rows(LIMIT),
        "combined over-cap hybrid fallback must sort before hydrating row-backed fields",
    );
    assert_eq!(
        attribution.store_get_calls, LIMIT as u64,
        "combined over-cap hybrid fallback should hydrate only final page rows, got {attribution:?}",
    );
    let hybrid = attribution
        .hybrid_covering
        .expect("combined over-cap hybrid fallback should report hybrid covering");
    assert_eq!(
        hybrid.path_hits, 1,
        "combined over-cap hybrid fallback should report one hybrid covering path hit",
    );
    assert_eq!(
        hybrid.index_field_accesses, LIMIT as u64,
        "combined over-cap hybrid fallback should decode one index field per returned row",
    );
    assert_eq!(
        hybrid.row_field_accesses, LIMIT as u64,
        "combined over-cap hybrid fallback should read one row-backed field per returned row",
    );
    assert!(
        attribution.index_store_entry_reads >= combined_child_prefix_fixture_entry_count(),
        "combined over-cap hybrid fallback should scan the complete parent-prefix route, got {attribution:?}",
    );
    assert_eq!(
        attribution.scalar_aggregate, None,
        "combined over-cap hybrid projection default page path must not invoke count",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_fluent_sparse_in_combined_child_prefix_over_cap_hydrates_page_rows() {
    const LIMIT: usize = 8;

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_combined_child_prefix_over_cap_fixture(&session);
    let query = fluent_combined_child_prefix_over_cap_query(
        u32::try_from(LIMIT).expect("combined over-cap test limit should fit"),
    );

    let (result, attribution) = session
        .execute_query_result_with_attribution(&query)
        .expect("combined over-cap sparse fluent page should execute");
    let crate::db::LoadQueryResult::Rows(page) = result else {
        panic!("combined over-cap sparse fluent page should return scalar rows");
    };

    assert_eq!(
        response_branch_ids(&page),
        expected_combined_child_prefix_over_cap_ulids(LIMIT),
        "combined over-cap fluent fallback must preserve primary-key order",
    );
    assert!(
        (LIMIT as u64..=u64::try_from(LIMIT + 1).expect("test limit should fit"))
            .contains(&attribution.store_get_calls),
        "combined over-cap fluent fallback should hydrate only returned rows plus lookahead, got {attribution:?}",
    );
    assert!(
        attribution.index_store_entry_reads >= combined_child_prefix_fixture_entry_count(),
        "combined over-cap fluent fallback should scan the complete parent-prefix route, got {attribution:?}",
    );
    assert!(
        attribution.direct_data_row.is_some(),
        "combined over-cap fluent fallback should report scalar row attribution",
    );
    assert_eq!(
        attribution.grouped, None,
        "combined over-cap fluent fallback must not invoke grouped/count work",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_sparse_in_cursor_continuation_resumes_expanded_child_prefixes() {
    const LIMIT: usize = 8;

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = sparse_collection_sql("*", LIMIT);
    let query = lower_select_query_for_tests::<BranchIndexedSessionSqlEntity>(&session, &sql)
        .unwrap_or_else(|err| panic!("sparse collection paged SQL should lower: {err:?}"));

    let first = session
        .execute_load_query_paged_with_trace(&query, None)
        .unwrap_or_else(|err| panic!("first sparse collection page should execute: {err:?}"))
        .into_execution();

    assert_eq!(
        paged_branch_ids(&first),
        expected_collection_ulids(LIMIT),
        "first sparse expanded page should match the first primary-key window",
    );
    let fluent_first = execute_fluent_sparse_collection_page(&session, LIMIT, None);
    assert_eq!(
        paged_branch_ids(&first),
        paged_branch_ids(&fluent_first),
        "SQL and fluent sparse expanded first pages should share key order",
    );

    let cursor = crate::db::encode_cursor(
        first
            .continuation_cursor()
            .expect("first sparse expanded page should emit a continuation cursor"),
    );
    let fluent_cursor = crate::db::encode_cursor(
        fluent_first
            .continuation_cursor()
            .expect("first fluent sparse expanded page should emit a continuation cursor"),
    );
    let range_scans_before = crate::db::index::IndexStore::current_range_scan_call_count();
    let (second_with_trace, second_store_gets, second_entry_reads) =
        with_store_and_index_reads(|| {
            session
                .execute_load_query_paged_with_trace(&query, Some(cursor.as_str()))
                .unwrap_or_else(|err| {
                    panic!("second sparse collection page should execute: {err:?}")
                })
        });
    let second_range_scans = crate::db::index::IndexStore::current_range_scan_call_count()
        .saturating_sub(range_scans_before);
    let second = second_with_trace.into_execution();
    let fluent_second =
        execute_fluent_sparse_collection_page(&session, LIMIT, Some(&fluent_cursor));

    assert_eq!(
        paged_branch_ids(&second),
        expected_collection_ulids(LIMIT * 2)
            .into_iter()
            .skip(LIMIT)
            .collect::<Vec<_>>(),
        "second sparse expanded page should resume after the prior primary-key boundary",
    );
    assert_eq!(
        paged_branch_ids(&second),
        paged_branch_ids(&fluent_second),
        "SQL and fluent sparse expanded continuation pages should share key order",
    );
    assert!(
        second.continuation_cursor().is_none(),
        "second sparse expanded page should exhaust the fixture window",
    );
    assert!(
        fluent_second.continuation_cursor().is_none(),
        "fluent sparse expanded continuation page should exhaust the fixture window",
    );
    assert!(
        (LIMIT as u64..=u64::try_from(LIMIT + 1).expect("test limit should fit"))
            .contains(&second_store_gets),
        "resumed sparse SELECT * page should hydrate only returned rows plus lookahead, got {second_store_gets} row-store gets",
    );
    assert!(
        second_entry_reads < expected_collection_ulids(usize::MAX).len() as u64,
        "resumed sparse expanded page should not replay all collection-prefix entries, got {second_entry_reads} entry reads",
    );
    assert!(
        second_range_scans <= SPARSE_COLLECTION_CHILD_PREFIX_RANGE_CAP,
        "resumed sparse expanded page should keep child-prefix streams bounded, got {second_range_scans} range scans",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_sparse_in_verbose_explain_identifies_child_prefix_expansion() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = sparse_collection_sql("id", 8);
    let explain_sql = format!("EXPLAIN EXECUTION VERBOSE {sql}");

    let explain =
        statement_explain_sql::<BranchIndexedSessionSqlEntity>(&session, explain_sql.as_str())
            .unwrap_or_else(|err| {
                panic!("sparse collection verbose EXPLAIN should execute: {err:?}")
            });

    assert!(
        explain.contains("IndexMultiLookup"),
        "sparse IN route should remain visibly multi-lookup: {explain}",
    );
    assert!(
        explain.contains("diag.r.index_prefix_child_expansion=true"),
        "sparse IN route should report child-prefix expansion: {explain}",
    );
    assert!(
        explain.contains("diag.r.index_prefix_child_expansion_target=fetch(2)"),
        "sparse IN route should report the expanded composite prefix length: {explain}",
    );
    assert!(
        explain.contains("diag.r.index_prefix_child_expansion_cap=fetch(32)"),
        "sparse IN route should report the child-prefix expansion cap: {explain}",
    );

    let wider_sql = sparse_collection_sql("id", 50);
    let wider_explain_sql = format!("EXPLAIN EXECUTION VERBOSE {wider_sql}");
    let wider_explain = statement_explain_sql::<BranchIndexedSessionSqlEntity>(
        &session,
        wider_explain_sql.as_str(),
    )
    .unwrap_or_else(|err| {
        panic!("wider sparse collection verbose EXPLAIN should execute: {err:?}")
    });

    assert!(
        wider_explain.contains("diag.r.index_prefix_child_expansion_cap=fetch(51)"),
        "bounded sparse IN route should adapt the child-prefix expansion cap to the page lookahead window: {wider_explain}",
    );
}

#[cfg(feature = "diagnostics")]
#[test]
fn session_branch_set_sql_sparse_in_desc_does_not_use_asc_child_prefix_expansion() {
    const LIMIT: usize = 8;

    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_branch_set_fixture(&session);
    let sql = sparse_collection_desc_sql("id", LIMIT);
    let query = lower_select_query_for_tests::<BranchIndexedSessionSqlEntity>(&session, &sql)
        .unwrap_or_else(|err| panic!("sparse collection DESC SQL should lower: {err:?}"));
    let descriptor = session
        .explain_query_execution_with_visible_indexes(&query)
        .unwrap_or_else(|err| panic!("sparse collection DESC SQL should explain: {err:?}"));

    assert!(
        !explain_execution_contains_node_type(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied,
        ),
        "DESC sparse IN route must not reuse the ASC child-prefix order proof: {descriptor:?}",
    );
    assert!(
        explain_execution_contains_node_type(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort,
        ),
        "DESC sparse IN route should stay materialized until reverse child-prefix expansion is designed: {descriptor:?}",
    );

    let explain_sql = format!("EXPLAIN EXECUTION VERBOSE {sql}");
    let explain =
        statement_explain_sql::<BranchIndexedSessionSqlEntity>(&session, explain_sql.as_str())
            .unwrap_or_else(|err| {
                panic!("sparse collection DESC verbose EXPLAIN should execute: {err:?}")
            });

    assert!(
        !explain.contains("diag.r.index_prefix_child_expansion=true"),
        "DESC sparse IN route must not report ASC-only child-prefix expansion: {explain}",
    );

    let (result, attribution) = session
        .execute_sql_query_with_attribution::<BranchIndexedSessionSqlEntity>(sql.as_str())
        .unwrap_or_else(|err| panic!("sparse collection DESC SQL should execute: {err:?}"));
    let SqlStatementResult::Projection { rows, .. } = result else {
        panic!("sparse collection DESC SQL should return projection rows");
    };

    assert_eq!(
        rows,
        expected_collection_ids_desc(LIMIT),
        "DESC sparse IN fallback should return the global primary-key DESC window",
    );
    assert_eq!(
        attribution.store_get_calls, 0,
        "DESC key-only sparse IN fallback should stay row-store-free",
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
