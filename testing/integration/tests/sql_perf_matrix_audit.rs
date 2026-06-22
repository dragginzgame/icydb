use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashSet},
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error, ErrorOrigin,
    db::{SqlQueryExecutionAttribution, sql::SqlQueryResult},
    diagnostic::{DiagnosticCode, ErrorClass},
};
use icydb_testing_integration::{install_fixture_canister, reset_icydb_fixtures};
use serde::{Deserialize, Serialize};

const DEFAULT_MATRIX_LIMIT: usize = 300;
const DEFAULT_RANDOM_CASE_COUNT: usize = 300;
const DEFAULT_TOP_N: usize = 20;
const DEFAULT_RANDOM_SEED: u64 = 0x1cdb_0182_0000_0001;

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    attribution: SqlQueryExecutionAttribution,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatrixSurface {
    Account,
    Blob,
    HeapUser,
    JournaledUser,
    Token,
    User,
}

impl MatrixSurface {
    const fn label(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Blob => "blob",
            Self::HeapUser => "heap_user",
            Self::JournaledUser => "journaled_user",
            Self::Token => "token",
            Self::User => "user",
        }
    }

    const fn table(self) -> &'static str {
        match self {
            Self::Account => "PerfAuditAccount",
            Self::Blob => "PerfAuditBlob",
            Self::HeapUser => "PerfAuditHeapUser",
            Self::JournaledUser => "PerfAuditJournaledUser",
            Self::Token => "PerfAuditToken",
            Self::User => "PerfAuditUser",
        }
    }

    const fn query_method(self) -> &'static str {
        match self {
            Self::Account => "query_account_with_perf",
            Self::Blob => "query_blob_with_perf",
            Self::HeapUser => "query_heap_user_with_perf",
            Self::JournaledUser => "query_journaled_user_with_perf",
            Self::Token => "query_token_with_perf",
            Self::User => "query_user_with_perf",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatrixSource {
    Deterministic,
    Random,
}

impl MatrixSource {
    const fn label(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::Random => "random",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatrixMode {
    Deterministic,
    Random,
}

impl MatrixMode {
    const fn label(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::Random => "random",
        }
    }

    const fn title(self) -> &'static str {
        match self {
            Self::Deterministic => "SQL Perf Deterministic Matrix",
            Self::Random => "SQL Perf Random Matrix",
        }
    }

    const fn default_report_stem(self) -> &'static str {
        match self {
            Self::Deterministic => "sql_perf_deterministic_matrix",
            Self::Random => "sql_perf_random_matrix",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SqlFragment {
    key: &'static str,
    sql: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MatrixScenario {
    key: String,
    source: MatrixSource,
    surface: MatrixSurface,
    family: String,
    sql: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
struct MatrixOutcome {
    result_kind: &'static str,
    entity: String,
    row_count: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
struct MatrixSample {
    key: String,
    source: String,
    surface: String,
    family: String,
    sql: String,
    compile_local_instructions: u64,
    compile_cache_key_local_instructions: u64,
    compile_cache_lookup_local_instructions: u64,
    compile_parse_local_instructions: u64,
    compile_parse_tokenize_local_instructions: u64,
    compile_parse_select_local_instructions: u64,
    compile_parse_expr_local_instructions: u64,
    compile_parse_predicate_local_instructions: u64,
    compile_aggregate_lane_check_local_instructions: u64,
    compile_prepare_local_instructions: u64,
    compile_lower_local_instructions: u64,
    compile_bind_local_instructions: u64,
    compile_cache_insert_local_instructions: u64,
    execute_local_instructions: u64,
    planner_local_instructions: u64,
    planner_schema_info_local_instructions: u64,
    planner_prepare_local_instructions: u64,
    planner_cache_key_local_instructions: u64,
    planner_cache_lookup_local_instructions: u64,
    planner_plan_build_local_instructions: u64,
    planner_cache_insert_local_instructions: u64,
    store_local_instructions: u64,
    executor_local_instructions: u64,
    grouped_stream_local_instructions: u64,
    grouped_fold_local_instructions: u64,
    grouped_finalize_local_instructions: u64,
    scalar_aggregate_base_row_local_instructions: u64,
    scalar_aggregate_reducer_fold_local_instructions: u64,
    scalar_aggregate_expression_evaluations: u64,
    scalar_aggregate_filter_evaluations: u64,
    scalar_aggregate_rows_ingested: u64,
    scalar_aggregate_terminal_count: u64,
    scalar_aggregate_unique_input_expr_count: u64,
    scalar_aggregate_unique_filter_expr_count: u64,
    scalar_aggregate_sink_mode: Option<String>,
    pure_covering_decode_local_instructions: u64,
    pure_covering_row_assembly_local_instructions: u64,
    hybrid_covering_path_hits: u64,
    hybrid_covering_index_field_accesses: u64,
    hybrid_covering_row_field_accesses: u64,
    direct_data_row_scan_local_instructions: u64,
    direct_data_row_key_stream_local_instructions: u64,
    direct_data_row_row_read_local_instructions: u64,
    direct_data_row_key_encode_local_instructions: u64,
    direct_data_row_store_get_local_instructions: u64,
    direct_data_row_order_window_local_instructions: u64,
    direct_data_row_page_window_local_instructions: u64,
    kernel_row_scan_local_instructions: u64,
    kernel_row_key_stream_local_instructions: u64,
    kernel_row_row_read_local_instructions: u64,
    kernel_row_order_window_local_instructions: u64,
    kernel_row_page_window_local_instructions: u64,
    kernel_row_retained_layout_hits: u64,
    kernel_row_retained_slot_values: u64,
    kernel_row_retained_octet_length_values: u64,
    data_store_get_calls: u64,
    index_store_get_calls: u64,
    index_store_range_scan_calls: u64,
    index_store_entry_reads: u64,
    output_blob_values: u64,
    output_blob_bytes: u64,
    output_blob_hex_bytes: u64,
    sql_compiled_command_hits: u64,
    sql_compiled_command_misses: u64,
    shared_query_plan_hits: u64,
    shared_query_plan_misses: u64,
    total_local_instructions: u64,
    outcome: MatrixOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct MatrixFailure {
    key: String,
    source: String,
    surface: String,
    family: String,
    sql: String,
    code: u16,
    diagnostic_code: u16,
    diagnostic_label: &'static str,
    class: String,
    origin: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct MatrixReport {
    matrix_mode: String,
    generated_scenario_count: usize,
    executed_scenario_count: usize,
    failed_scenario_count: usize,
    matrix_limit: usize,
    scenario_key_filter: Option<String>,
    random_seed: Option<u64>,
    random_case_count: usize,
    samples: Vec<MatrixSample>,
    failures: Vec<MatrixFailure>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Lcg {
    state: u64,
}

impl Lcg {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    const fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.state
    }

    fn index(&mut self, len: usize) -> usize {
        let len = u64::try_from(len).expect("matrix option count should fit u64");
        usize::try_from(self.next() % len).expect("matrix option index should fit usize")
    }

    fn choose<'a, T>(&mut self, values: &'a [T]) -> &'a T {
        &values[self.index(values.len())]
    }
}

fn deterministic_matrix() -> Vec<MatrixScenario> {
    let mut scenarios = Vec::new();
    let mut user_scenarios = select_matrix(
        MatrixSurface::User,
        &user_projections(),
        &user_predicates(),
        &user_orders(),
        &[1, 3, 10],
    );
    if !user_scenarios.is_empty() {
        scenarios.extend(user_scenarios.drain(..1));
    }
    scenarios.extend(token_branch_route_hotspot_matrix());
    scenarios.extend(user_scenarios);
    scenarios.extend(select_matrix(
        MatrixSurface::Account,
        &account_projections(),
        &account_predicates(),
        &account_orders(),
        &[1, 3, 10],
    ));
    scenarios.extend(select_matrix(
        MatrixSurface::Blob,
        &blob_projections(),
        &blob_predicates(),
        &blob_orders(),
        &[1, 3, 10],
    ));
    scenarios.extend(storage_backend_mirror_matrix());
    scenarios.extend(aggregate_and_metadata_matrix());

    scenarios
}

const TOKEN_TARGET_COLLECTION: &str = "01KV5N439P0000000000000000";
const TOKEN_BRANCH_STAGES: &str = "'Draft', 'Review'";
const TOKEN_BRANCH_STAGES_WITH_DUPLICATE: &str = "'Draft', 'Draft', 'Review'";
const TOKEN_BRANCH_STAGES_WIDE: &str =
    "'Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden'";
const TOKEN_BRANCH_STAGES_OVER_CAP: &str = "'Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07'";

fn token_branch_route_hotspot_matrix() -> Vec<MatrixScenario> {
    vec![
        scenario(
            "token.collection_stage_id.branch_set.page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.page_only",
            token_branch_page_sql("id", TOKEN_BRANCH_STAGES, 50),
        ),
        scenario(
            "token.collection_stage_id.branch_set.covering_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.covering_page_only",
            token_branch_page_sql("id, collection_id, stage", TOKEN_BRANCH_STAGES, 50),
        ),
        scenario(
            "token.collection_stage_id.branch_set.noncovered_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.noncovered_page_only",
            token_branch_page_sql("id, title", TOKEN_BRANCH_STAGES, 50),
        ),
        scenario(
            "token.collection_stage_id.branch_set.full_entity.limit50",
            MatrixSurface::Token,
            "route.branch_set.full_entity",
            token_branch_page_sql("*", TOKEN_BRANCH_STAGES, 50),
        ),
        scenario(
            "token.collection_stage_id.branch_set.index_residual_covering.limit3",
            MatrixSurface::Token,
            "route.branch_set.index_residual_covering",
            token_branch_page_sql_with_extra_predicate(
                "id, stage",
                TOKEN_BRANCH_STAGES,
                "stage != 'Review'",
                3,
            ),
        ),
        scenario(
            "token.collection_stage_id.branch_set.count",
            MatrixSurface::Token,
            "route.branch_set.count",
            token_branch_count_sql(TOKEN_BRANCH_STAGES),
        ),
        scenario(
            "token.collection_stage_id.branch_set.duplicate_count",
            MatrixSurface::Token,
            "route.branch_set.duplicate_count",
            token_branch_count_sql(TOKEN_BRANCH_STAGES_WITH_DUPLICATE),
        ),
        scenario(
            "token.collection_stage_id.branch_set.wide_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.wide_page_only",
            token_branch_page_sql("id", TOKEN_BRANCH_STAGES_WIDE, 50),
        ),
        scenario(
            "token.collection_stage_id.branch_set.wide_noncovered_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_set.wide_noncovered_page_only",
            token_branch_page_sql("id, title", TOKEN_BRANCH_STAGES_WIDE, 50),
        ),
        scenario(
            "token.collection_stage_id.overcap_fallback.page_only.limit50",
            MatrixSurface::Token,
            "route.branch_over_cap.page_only",
            token_branch_page_sql("id", TOKEN_BRANCH_STAGES_OVER_CAP, 50),
        ),
        scenario(
            "token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50",
            MatrixSurface::Token,
            "route.branch_over_cap.noncovered_page_only",
            token_branch_page_sql("id, title", TOKEN_BRANCH_STAGES_OVER_CAP, 50),
        ),
        scenario(
            "token.collection_id.sparse_in.page_only.limit50",
            MatrixSurface::Token,
            "route.sparse_in.page_only",
            token_sparse_collection_in_page_sql(250, 50),
        ),
        scenario(
            "token.collection_id.sparse_in.count",
            MatrixSurface::Token,
            "route.sparse_in.count",
            token_sparse_collection_in_count_sql(250),
        ),
    ]
}

fn token_branch_page_sql(projection: &str, stages: &str, limit: u32) -> String {
    format!(
        "SELECT {projection} FROM PerfAuditToken WHERE collection_id = '{TOKEN_TARGET_COLLECTION}' AND stage IN ({stages}) ORDER BY id ASC LIMIT {limit}"
    )
}

fn token_branch_page_sql_with_extra_predicate(
    projection: &str,
    stages: &str,
    extra_predicate: &str,
    limit: u32,
) -> String {
    format!(
        "SELECT {projection} FROM PerfAuditToken WHERE collection_id = '{TOKEN_TARGET_COLLECTION}' AND stage IN ({stages}) AND {extra_predicate} ORDER BY id ASC LIMIT {limit}"
    )
}

fn token_branch_count_sql(stages: &str) -> String {
    format!(
        "SELECT COUNT(*) FROM PerfAuditToken WHERE collection_id = '{TOKEN_TARGET_COLLECTION}' AND stage IN ({stages})"
    )
}

fn token_sparse_collection_in_filter(missing_count: usize) -> String {
    let mut collections = format!("'{TOKEN_TARGET_COLLECTION}'");
    for index in 0..missing_count {
        let _ = write!(collections, ", 'missing-collection-{index:03}'");
    }

    format!("collection_id IN ({collections})")
}

fn token_sparse_collection_in_page_sql(missing_count: usize, limit: u32) -> String {
    let filter = token_sparse_collection_in_filter(missing_count);

    format!("SELECT id FROM PerfAuditToken WHERE {filter} ORDER BY id ASC LIMIT {limit}")
}

fn token_sparse_collection_in_count_sql(missing_count: usize) -> String {
    let filter = token_sparse_collection_in_filter(missing_count);

    format!("SELECT COUNT(*) FROM PerfAuditToken WHERE {filter}")
}

fn select_matrix(
    surface: MatrixSurface,
    projections: &[SqlFragment],
    predicates: &[SqlFragment],
    orders: &[SqlFragment],
    limits: &[u32],
) -> Vec<MatrixScenario> {
    let mut scenarios = Vec::new();

    for projection in projections {
        for predicate in predicates {
            for order in orders {
                for limit in limits {
                    let key = format!(
                        "{}.select.{}.{}.{}.limit{}",
                        surface.label(),
                        projection.key,
                        predicate.key,
                        order.key,
                        limit
                    );
                    let family =
                        format!("select.{}.{}.{}", projection.key, predicate.key, order.key);
                    let sql = select_sql(
                        surface.table(),
                        projection.sql,
                        predicate.sql,
                        order.sql,
                        *limit,
                    );

                    scenarios.push(MatrixScenario {
                        key,
                        source: MatrixSource::Deterministic,
                        surface,
                        family,
                        sql,
                    });
                }
            }
        }
    }

    scenarios
}

fn select_sql(table: &str, projection: &str, predicate: &str, order: &str, limit: u32) -> String {
    let where_clause = if predicate.is_empty() {
        String::new()
    } else {
        format!(" WHERE {predicate}")
    };
    let order_clause = if order.is_empty() {
        String::new()
    } else {
        format!(" ORDER BY {order}")
    };

    format!("SELECT {projection} FROM {table}{where_clause}{order_clause} LIMIT {limit}")
}

fn user_projections() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk",
            sql: "id",
        },
        SqlFragment {
            key: "narrow",
            sql: "id, name",
        },
        SqlFragment {
            key: "wide",
            sql: "id, name, age, age_nat, rank, active",
        },
        SqlFragment {
            key: "numeric_expr",
            sql: "id, age + rank AS total",
        },
        SqlFragment {
            key: "text_expr",
            sql: "id, LOWER(name) AS lower_name",
        },
    ]
}

fn user_predicates() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "all",
            sql: "",
        },
        SqlFragment {
            key: "pk_range",
            sql: "id >= 2",
        },
        SqlFragment {
            key: "age_range",
            sql: "age >= 24 AND age < 40",
        },
        SqlFragment {
            key: "name_prefix",
            sql: "name LIKE 'A%'",
        },
        SqlFragment {
            key: "lower_name_prefix",
            sql: "LOWER(name) LIKE 'a%'",
        },
        SqlFragment {
            key: "active_true",
            sql: "active = true",
        },
        SqlFragment {
            key: "age_in",
            sql: "age IN (24, 31, 43)",
        },
        SqlFragment {
            key: "field_compare",
            sql: "age > rank",
        },
    ]
}

fn user_orders() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk_asc",
            sql: "id ASC",
        },
        SqlFragment {
            key: "pk_desc",
            sql: "id DESC",
        },
        SqlFragment {
            key: "age_asc",
            sql: "age ASC, id ASC",
        },
        SqlFragment {
            key: "age_desc",
            sql: "age DESC, id DESC",
        },
        SqlFragment {
            key: "name_asc",
            sql: "name ASC, id ASC",
        },
        SqlFragment {
            key: "lower_name_asc",
            sql: "LOWER(name) ASC, id ASC",
        },
        SqlFragment {
            key: "numeric_expr_asc",
            sql: "age + rank ASC, id ASC",
        },
    ]
}

fn account_projections() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk",
            sql: "id",
        },
        SqlFragment {
            key: "narrow",
            sql: "id, handle",
        },
        SqlFragment {
            key: "wide",
            sql: "id, handle, tier, active, score",
        },
        SqlFragment {
            key: "text_expr",
            sql: "id, LOWER(handle) AS lower_handle",
        },
    ]
}

fn account_predicates() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "all",
            sql: "",
        },
        SqlFragment {
            key: "active_true",
            sql: "active = true",
        },
        SqlFragment {
            key: "tier_gold_active",
            sql: "tier = 'gold' AND active = true",
        },
        SqlFragment {
            key: "handle_prefix_active",
            sql: "handle LIKE 'a%' AND active = true",
        },
        SqlFragment {
            key: "lower_handle_prefix_active",
            sql: "LOWER(handle) LIKE 'a%' AND active = true",
        },
        SqlFragment {
            key: "score_range",
            sql: "score >= 20",
        },
    ]
}

fn account_orders() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk_asc",
            sql: "id ASC",
        },
        SqlFragment {
            key: "handle_asc",
            sql: "handle ASC, id ASC",
        },
        SqlFragment {
            key: "handle_desc",
            sql: "handle DESC, id DESC",
        },
        SqlFragment {
            key: "lower_handle_asc",
            sql: "LOWER(handle) ASC, id ASC",
        },
        SqlFragment {
            key: "tier_handle_asc",
            sql: "tier ASC, handle ASC, id ASC",
        },
    ]
}

fn blob_projections() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk",
            sql: "id",
        },
        SqlFragment {
            key: "metadata",
            sql: "id, label, bucket",
        },
        SqlFragment {
            key: "lengths",
            sql: "id, label, OCTET_LENGTH(thumbnail), OCTET_LENGTH(chunk)",
        },
        SqlFragment {
            key: "thumbnail",
            sql: "id, label, thumbnail",
        },
        SqlFragment {
            key: "payload",
            sql: "id, label, thumbnail, chunk",
        },
    ]
}

fn blob_predicates() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "all",
            sql: "",
        },
        SqlFragment {
            key: "bucket_eq",
            sql: "bucket = 10",
        },
        SqlFragment {
            key: "bucket_range",
            sql: "bucket >= 10 AND bucket < 40",
        },
        SqlFragment {
            key: "label_prefix",
            sql: "label LIKE 'blob-%'",
        },
    ]
}

fn blob_orders() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk_asc",
            sql: "id ASC",
        },
        SqlFragment {
            key: "bucket_asc",
            sql: "bucket ASC, id ASC",
        },
        SqlFragment {
            key: "bucket_label_asc",
            sql: "bucket ASC, label ASC, id ASC",
        },
        SqlFragment {
            key: "label_asc",
            sql: "label ASC, id ASC",
        },
    ]
}

fn storage_backend_mirror_matrix() -> Vec<MatrixScenario> {
    let mut scenarios = Vec::new();
    for surface in [MatrixSurface::HeapUser, MatrixSurface::JournaledUser] {
        scenarios.extend(select_matrix(
            surface,
            &storage_mirror_projections(),
            &storage_mirror_predicates(),
            &storage_mirror_orders(),
            &[1, 3, 10],
        ));
    }
    scenarios
}

fn storage_mirror_projections() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk",
            sql: "id",
        },
        SqlFragment {
            key: "narrow",
            sql: "id, name",
        },
        SqlFragment {
            key: "wide",
            sql: "id, name, age",
        },
    ]
}

fn storage_mirror_predicates() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "all",
            sql: "",
        },
        SqlFragment {
            key: "pk_range",
            sql: "id >= 2",
        },
        SqlFragment {
            key: "age_range",
            sql: "age >= 24 AND age < 40",
        },
        SqlFragment {
            key: "name_range",
            sql: "name >= 'a'",
        },
    ]
}

fn storage_mirror_orders() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk_asc",
            sql: "id ASC",
        },
        SqlFragment {
            key: "pk_desc",
            sql: "id DESC",
        },
        SqlFragment {
            key: "age_asc",
            sql: "age ASC, id ASC",
        },
        SqlFragment {
            key: "name_asc",
            sql: "name ASC, id ASC",
        },
    ]
}

fn aggregate_and_metadata_matrix() -> Vec<MatrixScenario> {
    vec![
        scenario(
            "user.aggregate.count_all",
            MatrixSurface::User,
            "aggregate.count",
            "SELECT COUNT(*) FROM PerfAuditUser",
        ),
        scenario(
            "user.aggregate.count_active",
            MatrixSurface::User,
            "aggregate.count",
            "SELECT COUNT(*) FROM PerfAuditUser WHERE active = true",
        ),
        scenario(
            "user.aggregate.count_age_in",
            MatrixSurface::User,
            "aggregate.count_in",
            "SELECT COUNT(*) FROM PerfAuditUser WHERE age IN (24, 31, 43)",
        ),
        scenario(
            "user.aggregate.group_age_count",
            MatrixSurface::User,
            "aggregate.grouped",
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
        ),
        scenario(
            "user.aggregate.group_active_avg_age",
            MatrixSurface::User,
            "aggregate.grouped",
            "SELECT active, AVG(age) FROM PerfAuditUser GROUP BY active ORDER BY active ASC LIMIT 10",
        ),
        scenario(
            "user.aggregate.group_age_having_alias",
            MatrixSurface::User,
            "aggregate.grouped_having",
            "SELECT age, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS high_count FROM PerfAuditUser GROUP BY age HAVING high_count > 0 ORDER BY high_count DESC, age ASC LIMIT 5",
        ),
        scenario(
            "account.aggregate.group_tier_count",
            MatrixSurface::Account,
            "aggregate.grouped",
            "SELECT tier, COUNT(*) FROM PerfAuditAccount WHERE active = true GROUP BY tier ORDER BY tier ASC LIMIT 10",
        ),
        scenario(
            "account.aggregate.count_active_tier_in",
            MatrixSurface::Account,
            "aggregate.count_in",
            "SELECT COUNT(*) FROM PerfAuditAccount WHERE active = true AND tier IN ('gold', 'silver')",
        ),
        scenario(
            "blob.aggregate.count_bucket",
            MatrixSurface::Blob,
            "aggregate.count",
            "SELECT COUNT(*) FROM PerfAuditBlob WHERE bucket = 10",
        ),
        scenario(
            "user.metadata.explain_pk_limit",
            MatrixSurface::User,
            "metadata.explain",
            "EXPLAIN SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
        ),
        scenario(
            "user.metadata.describe",
            MatrixSurface::User,
            "metadata.describe",
            "DESCRIBE PerfAuditUser",
        ),
        scenario(
            "user.metadata.show_columns",
            MatrixSurface::User,
            "metadata.show_columns",
            "SHOW COLUMNS PerfAuditUser",
        ),
        scenario(
            "user.metadata.show_indexes",
            MatrixSurface::User,
            "metadata.show_indexes",
            "SHOW INDEXES FROM PerfAuditUser",
        ),
        scenario(
            "user.metadata.show_entities",
            MatrixSurface::User,
            "metadata.show_entities",
            "SHOW ENTITIES",
        ),
    ]
}

fn scenario(
    key: impl Into<String>,
    surface: MatrixSurface,
    family: impl Into<String>,
    sql: impl Into<String>,
) -> MatrixScenario {
    MatrixScenario {
        key: key.into(),
        source: MatrixSource::Deterministic,
        surface,
        family: family.into(),
        sql: sql.into(),
    }
}

fn random_matrix(seed: u64, case_count: usize) -> Vec<MatrixScenario> {
    let mut rng = Lcg::new(seed);
    (0..case_count)
        .map(|index| random_scenario(&mut rng, seed, index))
        .collect()
}

fn random_scenario(rng: &mut Lcg, seed: u64, index: usize) -> MatrixScenario {
    let surface = *rng.choose(&[
        MatrixSurface::User,
        MatrixSurface::Account,
        MatrixSurface::Blob,
        MatrixSurface::Token,
    ]);
    let key = format!("random.{seed:016x}.{index:04}.{}", surface.label());

    match surface {
        MatrixSurface::Account => {
            let predicate = random_account_predicate(rng);
            random_select_scenario(
                rng,
                key,
                surface,
                &account_projections(),
                predicate,
                &account_orders(),
            )
        }
        MatrixSurface::Blob => {
            let predicate = random_blob_predicate(rng);
            random_select_scenario(
                rng,
                key,
                surface,
                &blob_projections(),
                predicate,
                &blob_orders(),
            )
        }
        MatrixSurface::HeapUser | MatrixSurface::JournaledUser => {
            let predicate = random_storage_mirror_predicate(rng);
            random_select_scenario(
                rng,
                key,
                surface,
                &storage_mirror_projections(),
                predicate,
                &storage_mirror_orders(),
            )
        }
        MatrixSurface::Token => random_token_route_hotspot_scenario(rng, key),
        MatrixSurface::User => {
            let predicate = random_user_predicate(rng);
            random_select_scenario(
                rng,
                key,
                surface,
                &user_projections(),
                predicate,
                &user_orders(),
            )
        }
    }
}

fn random_token_route_hotspot_scenario(rng: &mut Lcg, key: String) -> MatrixScenario {
    let token_scenarios = token_branch_route_hotspot_matrix();
    let mut scenario = rng.choose(&token_scenarios).clone();
    scenario.key = key;
    scenario.source = MatrixSource::Random;
    scenario.family = format!("random.{}", scenario.family);
    scenario
}

fn random_select_scenario(
    rng: &mut Lcg,
    key: String,
    surface: MatrixSurface,
    projections: &[SqlFragment],
    predicate: String,
    orders: &[SqlFragment],
) -> MatrixScenario {
    let projection = rng.choose(projections);
    let order = rng.choose(orders);
    let limit = *rng.choose(&[1, 2, 3, 5, 10]);
    let sql = select_sql(
        surface.table(),
        projection.sql,
        predicate.as_str(),
        order.sql,
        limit,
    );

    MatrixScenario {
        key,
        source: MatrixSource::Random,
        surface,
        family: format!("random.{}.{}", projection.key, order.key),
        sql,
    }
}

fn random_storage_mirror_predicate(rng: &mut Lcg) -> String {
    match rng.index(4) {
        0 => String::new(),
        1 => format!("id >= {}", rng.choose(&[1, 2, 3, 4])),
        2 => {
            let low = *rng.choose(&[18, 24, 30, 35]);
            let high = low + *rng.choose(&[5, 10, 20]);
            format!("age >= {low} AND age < {high}")
        }
        _ => "name >= 'a'".to_string(),
    }
}

fn random_user_predicate(rng: &mut Lcg) -> String {
    match rng.index(8) {
        0 => String::new(),
        1 => format!("id >= {}", rng.choose(&[1, 2, 3, 4])),
        2 => {
            let low = *rng.choose(&[18, 24, 30, 35]);
            let high = low + *rng.choose(&[5, 10, 20]);
            format!("age >= {low} AND age < {high}")
        }
        3 => format!("name LIKE '{}%'", rng.choose(&["A", "B", "C", "D"])),
        4 => format!("LOWER(name) LIKE '{}%'", rng.choose(&["a", "b", "c", "d"])),
        5 => format!("active = {}", rng.choose(&["true", "false"])),
        6 => format!(
            "age IN ({}, {}, {})",
            rng.choose(&[18, 24, 30]),
            rng.choose(&[31, 35, 40]),
            rng.choose(&[43, 45, 50])
        ),
        _ => "age > rank".to_string(),
    }
}

fn random_account_predicate(rng: &mut Lcg) -> String {
    match rng.index(6) {
        0 => String::new(),
        1 => "active = true".to_string(),
        2 => format!(
            "tier = '{}' AND active = true",
            rng.choose(&["free", "gold", "pro"])
        ),
        3 => format!(
            "handle LIKE '{}%' AND active = true",
            rng.choose(&["a", "b", "c"])
        ),
        4 => format!(
            "LOWER(handle) LIKE '{}%' AND active = true",
            rng.choose(&["a", "b", "c"])
        ),
        _ => format!("score >= {}", rng.choose(&[10, 20, 30, 40])),
    }
}

fn random_blob_predicate(rng: &mut Lcg) -> String {
    match rng.index(4) {
        0 => String::new(),
        1 => format!("bucket = {}", rng.choose(&[10, 20, 30, 40])),
        2 => {
            let low = *rng.choose(&[10, 20, 30]);
            let high = low + *rng.choose(&[10, 20]);
            format!("bucket >= {low} AND bucket < {high}")
        }
        _ => "label LIKE 'blob-%'".to_string(),
    }
}

fn generated_matrix(mode: MatrixMode) -> Vec<MatrixScenario> {
    match mode {
        MatrixMode::Deterministic => deterministic_matrix(),
        MatrixMode::Random => random_matrix(random_seed(), random_case_count()),
    }
}

fn filter_matrix_scenarios(
    scenarios: Vec<MatrixScenario>,
    scenario_key_filter: Option<&str>,
) -> Vec<MatrixScenario> {
    let Some(filter) = scenario_key_filter else {
        return scenarios;
    };
    let requested_keys = filter
        .split(',')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .collect::<Vec<_>>();
    assert!(
        !requested_keys.is_empty(),
        "ICYDB_SQL_PERF_MATRIX_KEYS should contain one or more comma-separated scenario keys",
    );

    let requested = requested_keys.iter().copied().collect::<HashSet<_>>();
    let mut found = HashSet::new();
    let selected = scenarios
        .into_iter()
        .filter(|scenario| {
            let keep = requested.contains(scenario.key.as_str());
            if keep {
                found.insert(scenario.key.clone());
            }
            keep
        })
        .collect::<Vec<_>>();
    let missing = requested_keys
        .into_iter()
        .filter(|key| !found.contains(*key))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "ICYDB_SQL_PERF_MATRIX_KEYS contained unknown scenario key(s): {}",
        missing.join(", "),
    );

    selected
}

fn matrix_limit(total: usize) -> usize {
    match env::var("ICYDB_SQL_PERF_MATRIX_LIMIT") {
        Ok(value) if value == "all" => total,
        Ok(value) => value
            .parse::<usize>()
            .expect("ICYDB_SQL_PERF_MATRIX_LIMIT should be a positive integer or 'all'")
            .min(total),
        Err(_) => DEFAULT_MATRIX_LIMIT.min(total),
    }
}

fn matrix_mode() -> MatrixMode {
    if let Ok(value) = env::var("ICYDB_SQL_PERF_MATRIX_MODE") {
        return parse_matrix_mode(&value);
    }

    assert!(
        env::var_os("ICYDB_SQL_PERF_MATRIX_RANDOM_CASES").is_none()
            && env::var_os("ICYDB_SQL_PERF_MATRIX_SEED").is_none(),
        "set ICYDB_SQL_PERF_MATRIX_MODE=random before using random matrix controls"
    );
    MatrixMode::Deterministic
}

fn parse_matrix_mode(value: &str) -> MatrixMode {
    match value {
        "deterministic" => MatrixMode::Deterministic,
        "random" => MatrixMode::Random,
        other => panic!(
            "ICYDB_SQL_PERF_MATRIX_MODE should be 'deterministic' or 'random', got '{other}'"
        ),
    }
}

fn random_case_count() -> usize {
    env::var("ICYDB_SQL_PERF_MATRIX_RANDOM_CASES").map_or(DEFAULT_RANDOM_CASE_COUNT, |value| {
        value
            .parse::<usize>()
            .expect("ICYDB_SQL_PERF_MATRIX_RANDOM_CASES should be a positive integer")
    })
}

fn random_seed() -> u64 {
    env::var("ICYDB_SQL_PERF_MATRIX_SEED").map_or(DEFAULT_RANDOM_SEED, |value| {
        value
            .parse::<u64>()
            .expect("ICYDB_SQL_PERF_MATRIX_SEED should be an unsigned integer")
    })
}

fn top_n() -> usize {
    env::var("ICYDB_SQL_PERF_MATRIX_TOP").map_or(DEFAULT_TOP_N, |value| {
        value
            .parse::<usize>()
            .expect("ICYDB_SQL_PERF_MATRIX_TOP should be a positive integer")
    })
}

fn matrix_scenario_key_filter() -> Option<String> {
    env::var("ICYDB_SQL_PERF_MATRIX_KEYS")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn install_sql_perf_canister_fixture() -> StandaloneCanisterFixture {
    install_fixture_canister("sql_perf")
}

fn query_surface_with_perf(
    fixture: &StandaloneCanisterFixture,
    scenario: &MatrixScenario,
) -> Result<SqlQueryPerfResult, Error> {
    fixture
        .query_call(scenario.surface.query_method(), (scenario.sql.clone(),))
        .unwrap_or_else(|err| panic!("{} should decode: {err}", scenario.surface.query_method()))
}

fn summarize_perf_outcome(result: &SqlQueryResult) -> MatrixOutcome {
    match result {
        SqlQueryResult::Count { entity, row_count } => MatrixOutcome {
            result_kind: "count",
            entity: entity.clone(),
            row_count: usize::try_from(*row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Projection(rows) => MatrixOutcome {
            result_kind: "projection",
            entity: rows.entity.clone(),
            row_count: usize::try_from(rows.row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Grouped(rows) => MatrixOutcome {
            result_kind: "grouped",
            entity: rows.entity.clone(),
            row_count: usize::try_from(rows.row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Explain { entity, .. } => MatrixOutcome {
            result_kind: "explain",
            entity: entity.clone(),
            row_count: 1,
        },
        SqlQueryResult::Describe(entity) => MatrixOutcome {
            result_kind: "describe",
            entity: entity.entity_name().to_string(),
            row_count: entity.fields().len(),
        },
        SqlQueryResult::ShowIndexes { entity, indexes } => MatrixOutcome {
            result_kind: "show_indexes",
            entity: entity.clone(),
            row_count: indexes.len(),
        },
        SqlQueryResult::ShowColumns { entity, columns } => MatrixOutcome {
            result_kind: "show_columns",
            entity: entity.clone(),
            row_count: columns.len(),
        },
        SqlQueryResult::ShowEntities { entities, .. } => MatrixOutcome {
            result_kind: "show_entities",
            entity: String::new(),
            row_count: entities.len(),
        },
        SqlQueryResult::ShowStores { stores, .. } => MatrixOutcome {
            result_kind: "show_stores",
            entity: String::new(),
            row_count: stores.len(),
        },
        SqlQueryResult::ShowMemory { memory } => MatrixOutcome {
            result_kind: "show_memory",
            entity: String::new(),
            row_count: memory.len(),
        },
        SqlQueryResult::Ddl { entity, .. } => MatrixOutcome {
            result_kind: "__icydb_ddl",
            entity: entity.clone(),
            row_count: 1,
        },
    }
}

fn sample_scenario(
    fixture: &StandaloneCanisterFixture,
    scenario: &MatrixScenario,
) -> Result<MatrixSample, Box<MatrixFailure>> {
    let perf = query_surface_with_perf(fixture, scenario)
        .map_err(|err| Box::new(matrix_failure_from_error(scenario, err)))?;

    Ok(matrix_sample_from_perf(scenario, &perf))
}

fn matrix_sample_from_perf(scenario: &MatrixScenario, perf: &SqlQueryPerfResult) -> MatrixSample {
    let attribution = &perf.attribution;
    let mut sample = MatrixSample {
        key: scenario.key.clone(),
        source: scenario.source.label().to_string(),
        surface: scenario.surface.label().to_string(),
        family: scenario.family.clone(),
        sql: scenario.sql.clone(),
        outcome: summarize_perf_outcome(&perf.result),
        ..MatrixSample::default()
    };

    fill_matrix_compile_sample(&mut sample, attribution);
    fill_matrix_execution_sample(&mut sample, attribution);
    fill_matrix_grouped_sample(&mut sample, attribution);
    fill_matrix_scalar_aggregate_sample(&mut sample, attribution);
    fill_matrix_projection_path_sample(&mut sample, attribution);
    fill_matrix_store_output_cache_sample(&mut sample, attribution);

    sample
}

const fn fill_matrix_compile_sample(
    sample: &mut MatrixSample,
    attribution: &SqlQueryExecutionAttribution,
) {
    let compile = attribution.compile;

    sample.compile_local_instructions = attribution.compile_local_instructions;
    sample.compile_cache_key_local_instructions = compile.cache_key_local_instructions;
    sample.compile_cache_lookup_local_instructions = compile.cache_lookup_local_instructions;
    sample.compile_parse_local_instructions = compile.parse_local_instructions;
    sample.compile_parse_tokenize_local_instructions = compile.parse_tokenize_local_instructions;
    sample.compile_parse_select_local_instructions = compile.parse_select_local_instructions;
    sample.compile_parse_expr_local_instructions = compile.parse_expr_local_instructions;
    sample.compile_parse_predicate_local_instructions = compile.parse_predicate_local_instructions;
    sample.compile_aggregate_lane_check_local_instructions =
        compile.aggregate_lane_check_local_instructions;
    sample.compile_prepare_local_instructions = compile.prepare_local_instructions;
    sample.compile_lower_local_instructions = compile.lower_local_instructions;
    sample.compile_bind_local_instructions = compile.bind_local_instructions;
    sample.compile_cache_insert_local_instructions = compile.cache_insert_local_instructions;
}

const fn fill_matrix_execution_sample(
    sample: &mut MatrixSample,
    attribution: &SqlQueryExecutionAttribution,
) {
    let execution = attribution.execution;

    sample.execute_local_instructions = attribution.execute_local_instructions;
    sample.planner_local_instructions = execution.planner_local_instructions;
    sample.planner_schema_info_local_instructions =
        execution.planner_schema_info_local_instructions;
    sample.planner_prepare_local_instructions = execution.planner_prepare_local_instructions;
    sample.planner_cache_key_local_instructions = execution.planner_cache_key_local_instructions;
    sample.planner_cache_lookup_local_instructions =
        execution.planner_cache_lookup_local_instructions;
    sample.planner_plan_build_local_instructions = execution.planner_plan_build_local_instructions;
    sample.planner_cache_insert_local_instructions =
        execution.planner_cache_insert_local_instructions;
    sample.store_local_instructions = execution.store_local_instructions;
    sample.executor_local_instructions = execution.executor_local_instructions;
    sample.total_local_instructions = attribution.total_local_instructions;
}

const fn fill_matrix_grouped_sample(
    sample: &mut MatrixSample,
    attribution: &SqlQueryExecutionAttribution,
) {
    let Some(grouped) = attribution.grouped else {
        return;
    };

    sample.grouped_stream_local_instructions = grouped.stream_local_instructions;
    sample.grouped_fold_local_instructions = grouped.fold_local_instructions;
    sample.grouped_finalize_local_instructions = grouped.finalize_local_instructions;
}

fn fill_matrix_scalar_aggregate_sample(
    sample: &mut MatrixSample,
    attribution: &SqlQueryExecutionAttribution,
) {
    let Some(aggregate) = &attribution.scalar_aggregate else {
        return;
    };

    sample.scalar_aggregate_base_row_local_instructions = aggregate.base_row_local_instructions;
    sample.scalar_aggregate_reducer_fold_local_instructions =
        aggregate.reducer_fold_local_instructions;
    sample.scalar_aggregate_expression_evaluations = aggregate.expression_evaluations;
    sample.scalar_aggregate_filter_evaluations = aggregate.filter_evaluations;
    sample.scalar_aggregate_rows_ingested = aggregate.rows_ingested;
    sample.scalar_aggregate_terminal_count = aggregate.terminal_count;
    sample.scalar_aggregate_unique_input_expr_count = aggregate.unique_input_expr_count;
    sample.scalar_aggregate_unique_filter_expr_count = aggregate.unique_filter_expr_count;
    sample
        .scalar_aggregate_sink_mode
        .clone_from(&aggregate.sink_mode);
}

const fn fill_matrix_projection_path_sample(
    sample: &mut MatrixSample,
    attribution: &SqlQueryExecutionAttribution,
) {
    if let Some(pure_covering) = attribution.pure_covering {
        sample.pure_covering_decode_local_instructions = pure_covering.decode_local_instructions;
        sample.pure_covering_row_assembly_local_instructions =
            pure_covering.row_assembly_local_instructions;
    }

    if let Some(hybrid) = attribution.hybrid_covering {
        sample.hybrid_covering_path_hits = hybrid.path_hits;
        sample.hybrid_covering_index_field_accesses = hybrid.index_field_accesses;
        sample.hybrid_covering_row_field_accesses = hybrid.row_field_accesses;
    }

    if let Some(direct) = attribution.direct_data_row {
        sample.direct_data_row_scan_local_instructions = direct.scan_local_instructions;
        sample.direct_data_row_key_stream_local_instructions = direct.key_stream_local_instructions;
        sample.direct_data_row_row_read_local_instructions = direct.row_read_local_instructions;
        sample.direct_data_row_key_encode_local_instructions = direct.key_encode_local_instructions;
        sample.direct_data_row_store_get_local_instructions = direct.store_get_local_instructions;
        sample.direct_data_row_order_window_local_instructions =
            direct.order_window_local_instructions;
        sample.direct_data_row_page_window_local_instructions =
            direct.page_window_local_instructions;
    }

    if let Some(kernel) = attribution.kernel_row {
        sample.kernel_row_scan_local_instructions = kernel.scan_local_instructions;
        sample.kernel_row_key_stream_local_instructions = kernel.key_stream_local_instructions;
        sample.kernel_row_row_read_local_instructions = kernel.row_read_local_instructions;
        sample.kernel_row_order_window_local_instructions = kernel.order_window_local_instructions;
        sample.kernel_row_page_window_local_instructions = kernel.page_window_local_instructions;
        sample.kernel_row_retained_layout_hits = kernel.retained_layout_hits;
        sample.kernel_row_retained_slot_values = kernel.retained_slot_values;
        sample.kernel_row_retained_octet_length_values = kernel.retained_octet_length_values;
    }
}

const fn fill_matrix_store_output_cache_sample(
    sample: &mut MatrixSample,
    attribution: &SqlQueryExecutionAttribution,
) {
    sample.data_store_get_calls = attribution.store_get_calls;
    sample.index_store_get_calls = attribution.index_store_get_calls;
    sample.index_store_range_scan_calls = attribution.index_store_range_scan_calls;
    sample.index_store_entry_reads = attribution.index_store_entry_reads;
    sample.output_blob_values = attribution.output_blob.projected_values;
    sample.output_blob_bytes = attribution.output_blob.projected_bytes;
    sample.output_blob_hex_bytes = attribution.output_blob.rendered_hex_bytes;
    sample.sql_compiled_command_hits = attribution.cache.sql_compiled_command_hits;
    sample.sql_compiled_command_misses = attribution.cache.sql_compiled_command_misses;
    sample.shared_query_plan_hits = attribution.cache.shared_query_plan_hits;
    sample.shared_query_plan_misses = attribution.cache.shared_query_plan_misses;
}

fn matrix_failure_from_error(scenario: &MatrixScenario, err: Error) -> MatrixFailure {
    let diagnostic_code = err.diagnostic_code();
    MatrixFailure {
        key: scenario.key.clone(),
        source: scenario.source.label().to_string(),
        surface: scenario.surface.label().to_string(),
        family: scenario.family.clone(),
        sql: scenario.sql.clone(),
        code: err.code().raw(),
        diagnostic_code: diagnostic_code.error_code().raw(),
        diagnostic_label: diagnostic_label(diagnostic_code),
        class: error_class_label(err.class()).to_string(),
        origin: format!("{:?}", err.origin()),
    }
}

const fn diagnostic_label(code: DiagnosticCode) -> &'static str {
    match code {
        DiagnosticCode::QueryValidate => "QueryValidate",
        DiagnosticCode::QueryIntent => "QueryIntent",
        DiagnosticCode::QueryPlan => "QueryPlan",
        DiagnosticCode::QueryAccessRequirement => "QueryAccessRequirement",
        DiagnosticCode::QueryUnorderedPagination => "QueryUnorderedPagination",
        DiagnosticCode::QueryInvalidContinuationCursor => "QueryInvalidContinuationCursor",
        DiagnosticCode::QueryNotFound => "QueryNotFound",
        DiagnosticCode::QueryNotUnique => "QueryNotUnique",
        DiagnosticCode::QueryNumericOverflow => "QueryNumericOverflow",
        DiagnosticCode::QueryNumericNotRepresentable => "QueryNumericNotRepresentable",
        DiagnosticCode::QueryUnknownAggregateTargetField => "QueryUnknownAggregateTargetField",
        DiagnosticCode::QueryUnsupportedProjection => "QueryUnsupportedProjection",
        DiagnosticCode::QueryResultShapeMismatch => "QueryResultShapeMismatch",
        DiagnosticCode::QueryUnsupportedSqlFeature => "QueryUnsupportedSqlFeature",
        DiagnosticCode::QuerySqlSurfaceMismatch => "QuerySqlSurfaceMismatch",
        DiagnosticCode::QuerySqlWriteBoundary => "QuerySqlWriteBoundary",
        DiagnosticCode::SchemaDdlAdmission => "SchemaDdlAdmission",
        DiagnosticCode::StoreNotFound => "StoreNotFound",
        DiagnosticCode::StoreCorruption => "StoreCorruption",
        DiagnosticCode::StoreInvariantViolation => "StoreInvariantViolation",
        DiagnosticCode::RuntimeCorruption => "RuntimeCorruption",
        DiagnosticCode::RuntimeIncompatiblePersistedFormat => "RuntimeIncompatiblePersistedFormat",
        DiagnosticCode::RuntimeInvariantViolation => "RuntimeInvariantViolation",
        DiagnosticCode::RuntimeConflict => "RuntimeConflict",
        DiagnosticCode::RuntimeNotFound => "RuntimeNotFound",
        DiagnosticCode::RuntimeUnsupported => "RuntimeUnsupported",
        DiagnosticCode::RuntimeInternal => "RuntimeInternal",
    }
}

const fn error_class_label(class: ErrorClass) -> &'static str {
    match class {
        ErrorClass::Query => "Query",
        ErrorClass::Corruption => "Corruption",
        ErrorClass::IncompatiblePersistedFormat => "IncompatiblePersistedFormat",
        ErrorClass::NotFound => "NotFound",
        ErrorClass::Internal => "Internal",
        ErrorClass::Conflict => "Conflict",
        ErrorClass::Unsupported => "Unsupported",
        ErrorClass::InvariantViolation => "InvariantViolation",
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("integration crate should live two levels below workspace root")
        .to_path_buf()
}

fn report_stem(mode: MatrixMode) -> PathBuf {
    env::var("ICYDB_SQL_PERF_MATRIX_OUT").map_or_else(
        |_| {
            workspace_root()
                .join("artifacts/perf-audit")
                .join(mode.default_report_stem())
        },
        PathBuf::from,
    )
}

fn write_matrix_reports(report: &MatrixReport) {
    let stem = report_stem(matrix_mode_from_report(report));
    if let Some(parent) = stem.parent() {
        fs::create_dir_all(parent)
            .unwrap_or_else(|err| panic!("matrix report directory should be created: {err}"));
    }

    let json_path = stem.with_extension("json");
    let md_path = stem.with_extension("md");
    let json = serde_json::to_string_pretty(report).expect("matrix report should serialize");
    fs::write(&json_path, json)
        .unwrap_or_else(|err| panic!("matrix JSON report should write: {err}"));
    fs::write(&md_path, matrix_markdown(report))
        .unwrap_or_else(|err| panic!("matrix Markdown report should write: {err}"));

    println!("matrix JSON: {}", json_path.display());
    println!("matrix Markdown: {}", md_path.display());
}

fn matrix_markdown(report: &MatrixReport) -> String {
    let mut output = String::new();
    let mode = matrix_mode_from_report(report);
    writeln!(output, "# {}", mode.title()).expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(output, "- matrix mode: {}", report.matrix_mode)
        .expect("write to string should succeed");
    writeln!(
        output,
        "- generated scenarios: {}",
        report.generated_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- executed scenarios: {}",
        report.executed_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- failed scenarios: {}",
        report.failed_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(output, "- matrix limit: {}", report.matrix_limit)
        .expect("write to string should succeed");
    if let Some(filter) = &report.scenario_key_filter {
        writeln!(output, "- scenario key filter: {filter}")
            .expect("write to string should succeed");
    }
    if let Some(seed) = report.random_seed {
        writeln!(output, "- random seed: {seed}").expect("write to string should succeed");
        writeln!(output, "- random cases: {}", report.random_case_count)
            .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");

    append_instruction_hotspot_tables(&mut output, &report.samples);
    append_storage_backend_comparison_table(&mut output, &report.samples);
    append_failure_table(&mut output, &report.failures);

    output
}

fn append_instruction_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_ranked_table(
        output,
        "Top Total Instructions",
        ranked_by(samples, |sample| sample.total_local_instructions),
    );
    append_ranked_table(
        output,
        "Top Compile Instructions",
        ranked_by(samples, |sample| sample.compile_local_instructions),
    );
    append_compile_phase_table(
        output,
        ranked_by(samples, |sample| sample.compile_local_instructions),
    );
    append_ranked_table(
        output,
        "Top Execute Instructions",
        ranked_by(samples, |sample| sample.execute_local_instructions),
    );
    append_ranked_table(
        output,
        "Top Store Instructions",
        ranked_by(samples, |sample| sample.store_local_instructions),
    );
    append_ranked_table(
        output,
        "Top Executor Instructions",
        ranked_by(samples, |sample| sample.executor_local_instructions),
    );
    append_ranked_table(
        output,
        "Top Data Store Gets",
        ranked_by(samples, |sample| sample.data_store_get_calls),
    );
    append_ranked_table(
        output,
        "Top Index Store Gets",
        ranked_by(samples, |sample| sample.index_store_get_calls),
    );
    append_ranked_table(
        output,
        "Top Index Store Range Scans",
        ranked_by(samples, |sample| sample.index_store_range_scan_calls),
    );
    append_ranked_table(
        output,
        "Top Index Store Entry Reads",
        ranked_by(samples, |sample| sample.index_store_entry_reads),
    );
    append_blob_output_table(
        output,
        "Top Blob Output Bytes",
        ranked_by(samples, |sample| sample.output_blob_bytes),
    );
    append_pure_covering_hotspot_tables(output, samples);
    append_hybrid_covering_hotspot_tables(output, samples);
    append_direct_data_row_hotspot_tables(output, samples);
    append_kernel_row_hotspot_tables(output, samples);
    append_main_fixture_hotspot_tables(output, samples);
}

fn append_pure_covering_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_pure_covering_table(
        output,
        "Top Pure Covering Decode Instructions",
        ranked_by(samples, |sample| {
            sample.pure_covering_decode_local_instructions
        }),
        |sample| sample.pure_covering_decode_local_instructions,
    );
    append_pure_covering_table(
        output,
        "Top Pure Covering Row Assembly Instructions",
        ranked_by(samples, |sample| {
            sample.pure_covering_row_assembly_local_instructions
        }),
        |sample| sample.pure_covering_row_assembly_local_instructions,
    );
}

fn append_hybrid_covering_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_hybrid_covering_table(
        output,
        "Top Hybrid Covering Row Field Accesses",
        ranked_by(samples, |sample| sample.hybrid_covering_row_field_accesses),
        |sample| sample.hybrid_covering_row_field_accesses,
    );
    append_hybrid_covering_table(
        output,
        "Top Hybrid Covering Index Field Accesses",
        ranked_by(samples, |sample| {
            sample.hybrid_covering_index_field_accesses
        }),
        |sample| sample.hybrid_covering_index_field_accesses,
    );
}

fn append_direct_data_row_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_direct_data_row_table(
        output,
        "Top Direct Data-Row Scan Instructions",
        ranked_by(samples, |sample| {
            sample.direct_data_row_scan_local_instructions
        }),
    );
    append_direct_data_row_table(
        output,
        "Top Direct Data-Row Row-Read Instructions",
        ranked_by(samples, |sample| {
            sample.direct_data_row_row_read_local_instructions
        }),
    );
    append_direct_data_row_table(
        output,
        "Top Direct Data-Row Order-Window Instructions",
        ranked_by(samples, |sample| {
            sample.direct_data_row_order_window_local_instructions
        }),
    );
}

fn append_kernel_row_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_kernel_row_table(
        output,
        "Top Kernel Row Scan Instructions",
        ranked_by(samples, |sample| sample.kernel_row_scan_local_instructions),
    );
    append_kernel_row_table(
        output,
        "Top Kernel Row Row-Read Instructions",
        ranked_by(samples, |sample| {
            sample.kernel_row_row_read_local_instructions
        }),
    );
    append_kernel_row_table(
        output,
        "Top Kernel Row Order-Window Instructions",
        ranked_by(samples, |sample| {
            sample.kernel_row_order_window_local_instructions
        }),
    );
    append_kernel_row_table(
        output,
        "Top Kernel Row Retained Layout Hits",
        ranked_by(samples, |sample| sample.kernel_row_retained_layout_hits),
    );
    append_kernel_row_table(
        output,
        "Top Kernel Row Retained Slot Values",
        ranked_by(samples, |sample| sample.kernel_row_retained_slot_values),
    );
    append_kernel_row_table(
        output,
        "Top Kernel Row Retained Length Values",
        ranked_by(samples, |sample| {
            sample.kernel_row_retained_octet_length_values
        }),
    );
}

fn append_main_fixture_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    if !samples
        .iter()
        .any(|sample| !sample_is_storage_mirror(sample))
    {
        return;
    }

    append_ranked_table(
        output,
        "Top Main Fixture Total Instructions",
        ranked_main_fixture_by(samples, |sample| sample.total_local_instructions),
    );
    append_ranked_table(
        output,
        "Top Main Fixture Data Store Gets",
        ranked_main_fixture_by(samples, |sample| sample.data_store_get_calls),
    );
    append_ranked_table(
        output,
        "Top Main Fixture Index Store Gets",
        ranked_main_fixture_by(samples, |sample| sample.index_store_get_calls),
    );
    append_ranked_table(
        output,
        "Top Main Fixture Index Store Range Scans",
        ranked_main_fixture_by(samples, |sample| sample.index_store_range_scan_calls),
    );
    append_ranked_table(
        output,
        "Top Main Fixture Index Store Entry Reads",
        ranked_main_fixture_by(samples, |sample| sample.index_store_entry_reads),
    );
    append_main_fixture_covering_hotspot_tables(output, samples);
    append_main_fixture_execution_hotspot_tables(output, samples);
}

fn append_main_fixture_covering_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_pure_covering_table(
        output,
        "Top Main Fixture Pure Covering Decode Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.pure_covering_decode_local_instructions
        }),
        |sample| sample.pure_covering_decode_local_instructions,
    );
    append_pure_covering_table(
        output,
        "Top Main Fixture Pure Covering Row Assembly Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.pure_covering_row_assembly_local_instructions
        }),
        |sample| sample.pure_covering_row_assembly_local_instructions,
    );
    append_hybrid_covering_table(
        output,
        "Top Main Fixture Hybrid Covering Row Field Accesses",
        ranked_main_fixture_by(samples, |sample| sample.hybrid_covering_row_field_accesses),
        |sample| sample.hybrid_covering_row_field_accesses,
    );
    append_hybrid_covering_table(
        output,
        "Top Main Fixture Hybrid Covering Index Field Accesses",
        ranked_main_fixture_by(samples, |sample| {
            sample.hybrid_covering_index_field_accesses
        }),
        |sample| sample.hybrid_covering_index_field_accesses,
    );
}

fn append_main_fixture_execution_hotspot_tables(output: &mut String, samples: &[MatrixSample]) {
    append_direct_data_row_table(
        output,
        "Top Main Fixture Direct Data-Row Scan Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.direct_data_row_scan_local_instructions
        }),
    );
    append_direct_data_row_table(
        output,
        "Top Main Fixture Direct Data-Row Row-Read Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.direct_data_row_row_read_local_instructions
        }),
    );
    append_direct_data_row_table(
        output,
        "Top Main Fixture Direct Data-Row Order-Window Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.direct_data_row_order_window_local_instructions
        }),
    );
    append_kernel_row_table(
        output,
        "Top Main Fixture Kernel Row Scan Instructions",
        ranked_main_fixture_by(samples, |sample| sample.kernel_row_scan_local_instructions),
    );
    append_kernel_row_table(
        output,
        "Top Main Fixture Kernel Row Row-Read Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.kernel_row_row_read_local_instructions
        }),
    );
    append_kernel_row_table(
        output,
        "Top Main Fixture Kernel Row Order-Window Instructions",
        ranked_main_fixture_by(samples, |sample| {
            sample.kernel_row_order_window_local_instructions
        }),
    );
    append_kernel_row_table(
        output,
        "Top Main Fixture Kernel Row Retained Layout Hits",
        ranked_main_fixture_by(samples, |sample| sample.kernel_row_retained_layout_hits),
    );
    append_kernel_row_table(
        output,
        "Top Main Fixture Kernel Row Retained Slot Values",
        ranked_main_fixture_by(samples, |sample| sample.kernel_row_retained_slot_values),
    );
    append_kernel_row_table(
        output,
        "Top Main Fixture Kernel Row Retained Length Values",
        ranked_main_fixture_by(samples, |sample| {
            sample.kernel_row_retained_octet_length_values
        }),
    );
}

fn matrix_mode_from_report(report: &MatrixReport) -> MatrixMode {
    match report.matrix_mode.as_str() {
        "deterministic" => MatrixMode::Deterministic,
        "random" => MatrixMode::Random,
        other => panic!("matrix report mode should be known, got '{other}'"),
    }
}

fn ranked_by<F>(samples: &[MatrixSample], key: F) -> Vec<&MatrixSample>
where
    F: Fn(&MatrixSample) -> u64,
{
    let mut ranked = samples.iter().collect::<Vec<_>>();
    ranked.sort_by_key(|sample| Reverse(key(sample)));
    ranked.truncate(top_n());
    ranked
}

fn ranked_main_fixture_by<F>(samples: &[MatrixSample], key: F) -> Vec<&MatrixSample>
where
    F: Fn(&MatrixSample) -> u64,
{
    let mut ranked = samples
        .iter()
        .filter(|sample| !sample_is_storage_mirror(sample))
        .collect::<Vec<_>>();
    ranked.sort_by_key(|sample| Reverse(key(sample)));
    ranked.truncate(top_n());
    ranked
}

fn sample_is_storage_mirror(sample: &MatrixSample) -> bool {
    sample.surface == MatrixSurface::HeapUser.label()
        || sample.surface == MatrixSurface::JournaledUser.label()
}

fn append_ranked_table(output: &mut String, title: &str, samples: Vec<&MatrixSample>) {
    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Total | Compile | Execute | Planner | Store | Executor | data_store.get | index_store.get | index_store.ranges | index_store.entries | Rows | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )
    .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.total_local_instructions,
            sample.compile_local_instructions,
            sample.execute_local_instructions,
            sample.planner_local_instructions,
            sample.store_local_instructions,
            sample.executor_local_instructions,
            sample.data_store_get_calls,
            sample.index_store_get_calls,
            sample.index_store_range_scan_calls,
            sample.index_store_entry_reads,
            sample.outcome.row_count,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_blob_output_table(output: &mut String, title: &str, samples: Vec<&MatrixSample>) {
    let samples = samples
        .into_iter()
        .filter(|sample| sample.output_blob_bytes > 0)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return;
    }

    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Blob Values | Blob Bytes | Blob Hex Bytes | Total | Rows | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---:|---:|---:|---:|---:|---|")
        .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.output_blob_values,
            sample.output_blob_bytes,
            sample.output_blob_hex_bytes,
            sample.total_local_instructions,
            sample.outcome.row_count,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_compile_phase_table(output: &mut String, samples: Vec<&MatrixSample>) {
    let samples = samples
        .into_iter()
        .filter(|sample| sample.compile_local_instructions > 0)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return;
    }

    writeln!(output, "## Top Compile Phase Instructions").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Compile | Cache Key | Cache Lookup | Parse | Tokenize | Select | Expr | Predicate | Aggregate Check | Prepare | Lower | Bind | Cache Insert | Total | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )
    .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.compile_local_instructions,
            sample.compile_cache_key_local_instructions,
            sample.compile_cache_lookup_local_instructions,
            sample.compile_parse_local_instructions,
            sample.compile_parse_tokenize_local_instructions,
            sample.compile_parse_select_local_instructions,
            sample.compile_parse_expr_local_instructions,
            sample.compile_parse_predicate_local_instructions,
            sample.compile_aggregate_lane_check_local_instructions,
            sample.compile_prepare_local_instructions,
            sample.compile_lower_local_instructions,
            sample.compile_bind_local_instructions,
            sample.compile_cache_insert_local_instructions,
            sample.total_local_instructions,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_pure_covering_table<F>(
    output: &mut String,
    title: &str,
    samples: Vec<&MatrixSample>,
    metric: F,
) where
    F: Fn(&MatrixSample) -> u64,
{
    let samples = samples
        .into_iter()
        .filter(|sample| metric(sample) > 0)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return;
    }

    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Decode | Row Assembly | Total | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---:|---:|---:|---|").expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.pure_covering_decode_local_instructions,
            sample.pure_covering_row_assembly_local_instructions,
            sample.total_local_instructions,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_hybrid_covering_table<F>(
    output: &mut String,
    title: &str,
    samples: Vec<&MatrixSample>,
    metric: F,
) where
    F: Fn(&MatrixSample) -> u64,
{
    let samples = samples
        .into_iter()
        .filter(|sample| metric(sample) > 0)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return;
    }

    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Path Hits | Index Fields | Row Fields | Data Store Get | Total | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---:|---:|---:|---:|---:|---|")
        .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.hybrid_covering_path_hits,
            sample.hybrid_covering_index_field_accesses,
            sample.hybrid_covering_row_field_accesses,
            sample.data_store_get_calls,
            sample.total_local_instructions,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_direct_data_row_table(output: &mut String, title: &str, samples: Vec<&MatrixSample>) {
    let samples = samples
        .into_iter()
        .filter(|sample| sample.direct_data_row_scan_local_instructions > 0)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return;
    }

    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Scan | Key Stream | Row Read | Key Encode | Data Store Get | Order Window | Page Window | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---:|---:|---:|---:|---:|---:|---:|---|")
        .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.direct_data_row_scan_local_instructions,
            sample.direct_data_row_key_stream_local_instructions,
            sample.direct_data_row_row_read_local_instructions,
            sample.direct_data_row_key_encode_local_instructions,
            sample.direct_data_row_store_get_local_instructions,
            sample.direct_data_row_order_window_local_instructions,
            sample.direct_data_row_page_window_local_instructions,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_kernel_row_table(output: &mut String, title: &str, samples: Vec<&MatrixSample>) {
    let samples = samples
        .into_iter()
        .filter(|sample| sample.kernel_row_scan_local_instructions > 0)
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return;
    }

    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Scan | Key Stream | Row Read | Order Window | Page Window | Retained Layouts | Retained Values | Length Values | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )
    .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.kernel_row_scan_local_instructions,
            sample.kernel_row_key_stream_local_instructions,
            sample.kernel_row_row_read_local_instructions,
            sample.kernel_row_order_window_local_instructions,
            sample.kernel_row_page_window_local_instructions,
            sample.kernel_row_retained_layout_hits,
            sample.kernel_row_retained_slot_values,
            sample.kernel_row_retained_octet_length_values,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_storage_backend_comparison_table(output: &mut String, samples: &[MatrixSample]) {
    let heap_samples = storage_samples_by_suffix(samples, MatrixSurface::HeapUser, "heap_user.");
    let journaled_samples =
        storage_samples_by_suffix(samples, MatrixSurface::JournaledUser, "journaled_user.");

    let mut rows = heap_samples
        .iter()
        .filter_map(|(suffix, heap)| {
            let heap = *heap;
            let journaled = *journaled_samples.get(suffix)?;

            Some((suffix.as_str(), heap, journaled))
        })
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return;
    }

    rows.sort_by_key(|(_, heap, journaled)| {
        Reverse(absolute_delta(
            journaled.total_local_instructions,
            heap.total_local_instructions,
        ))
    });
    rows.truncate(top_n());

    writeln!(output, "## Heap vs Journaled Unindexed Storage Mirror")
        .expect("write to string should succeed");
    writeln!(
        output,
        "Mirror entities expose only the primary-key index; field predicate/order scenarios are intentional unindexed scan baselines."
    )
        .expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Heap Total | Journaled Total | Journaled Delta | Journaled Ratio | Heap Store | Journaled Store | SQL |",
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---:|---:|---:|---:|---:|---:|---|")
        .expect("write to string should succeed");
    for (suffix, heap, journaled) in rows {
        writeln!(
            output,
            "| `{suffix}` | {} | {} | {} | {} | {} | {} | `{}` |",
            heap.total_local_instructions,
            journaled.total_local_instructions,
            signed_delta(
                journaled.total_local_instructions,
                heap.total_local_instructions
            ),
            ratio_text(
                journaled.total_local_instructions,
                heap.total_local_instructions
            ),
            heap.store_local_instructions,
            journaled.store_local_instructions,
            journaled.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn storage_samples_by_suffix<'a>(
    samples: &'a [MatrixSample],
    surface: MatrixSurface,
    prefix: &str,
) -> BTreeMap<String, &'a MatrixSample> {
    samples
        .iter()
        .filter(|sample| sample.surface == surface.label())
        .filter_map(|sample| {
            sample
                .key
                .strip_prefix(prefix)
                .map(|suffix| (suffix.to_string(), sample))
        })
        .collect()
}

const fn absolute_delta(value: u64, baseline: u64) -> u64 {
    value.abs_diff(baseline)
}

fn signed_delta(value: u64, baseline: u64) -> String {
    if value >= baseline {
        format!("+{}", value - baseline)
    } else {
        format!("-{}", baseline - value)
    }
}

fn ratio_text(value: u64, baseline: u64) -> String {
    if baseline == 0 {
        return "n/a".to_string();
    }

    let scaled = value.saturating_mul(100) / baseline;
    format!("{}.{:02}x", scaled / 100, scaled % 100)
}

fn append_failure_table(output: &mut String, failures: &[MatrixFailure]) {
    if failures.is_empty() {
        return;
    }

    writeln!(output, "## Failed Generated Scenarios").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Code | Diagnostic | Class | Origin | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---:|---|---|---|---|").expect("write to string should succeed");
    for failure in failures.iter().take(top_n()) {
        writeln!(
            output,
            "| `{}` | {} | {} | {} ({}) | {} | {} | `{}` |",
            failure.key,
            failure.surface,
            failure.code,
            failure.diagnostic_label,
            failure.diagnostic_code,
            failure.class,
            failure.origin,
            failure.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

#[test]
fn sql_perf_matrix_failures_use_stable_diagnostic_labels() {
    let scenario = scenario(
        "user.failure.query_plan",
        MatrixSurface::User,
        "failure.query_plan",
        "SELECT id FROM PerfAuditUser ORDER BY unsupported_expression",
    );
    let failure = matrix_failure_from_error(
        &scenario,
        Error::from_code(DiagnosticCode::QueryPlan, ErrorOrigin::Query),
    );

    assert_eq!(failure.code, 3);
    assert_eq!(failure.diagnostic_code, 3);
    assert_eq!(failure.diagnostic_label, "QueryPlan");
    assert_eq!(failure.class, "Query");
    assert_eq!(failure.origin, "Query");
}

fn print_matrix_summary(report: &MatrixReport) {
    println!("{}", matrix_markdown(report));
}

#[test]
fn sql_perf_generated_matrix_has_stable_shape() {
    let deterministic = deterministic_matrix();
    assert!(
        deterministic.len() >= 1_000,
        "deterministic matrix should be broad enough to hunt hotspots; got {}",
        deterministic.len(),
    );
    assert_eq!(
        deterministic.first().map(|scenario| scenario.key.as_str()),
        Some("user.select.pk.all.pk_asc.limit1"),
    );

    let mut keys = HashSet::new();
    for scenario in &deterministic {
        assert!(
            keys.insert(scenario.key.as_str()),
            "duplicate generated scenario key '{}'",
            scenario.key,
        );
        assert!(
            scenario.sql.starts_with("SELECT")
                || scenario.sql.starts_with("EXPLAIN")
                || scenario.sql.starts_with("DESCRIBE")
                || scenario.sql.starts_with("SHOW"),
            "generated scenario '{}' should use supported SQL syntax",
            scenario.key,
        );
    }
}

#[test]
fn sql_perf_generated_matrix_includes_branch_route_hotspots() {
    let deterministic = deterministic_matrix();
    let scenarios_by_key = deterministic
        .iter()
        .enumerate()
        .map(|(position, scenario)| (scenario.key.as_str(), (position, scenario)))
        .collect::<BTreeMap<_, _>>();
    let expected_keys = [
        "token.collection_stage_id.branch_set.page_only.limit50",
        "token.collection_stage_id.branch_set.covering_page_only.limit50",
        "token.collection_stage_id.branch_set.noncovered_page_only.limit50",
        "token.collection_stage_id.branch_set.full_entity.limit50",
        "token.collection_stage_id.branch_set.index_residual_covering.limit3",
        "token.collection_stage_id.branch_set.count",
        "token.collection_stage_id.branch_set.duplicate_count",
        "token.collection_stage_id.branch_set.wide_page_only.limit50",
        "token.collection_stage_id.branch_set.wide_noncovered_page_only.limit50",
        "token.collection_stage_id.overcap_fallback.page_only.limit50",
        "token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50",
    ];

    for key in expected_keys {
        let (position, scenario) = scenarios_by_key
            .get(key)
            .unwrap_or_else(|| panic!("deterministic matrix should include route hotspot {key}"));
        assert!(
            *position < DEFAULT_MATRIX_LIMIT,
            "route hotspot {key} should run inside the default matrix window; position={position}"
        );
        assert_eq!(scenario.surface, MatrixSurface::Token);
        assert!(
            scenario.family.starts_with("route."),
            "route hotspot {key} should be grouped under route families"
        );
        assert!(
            scenario.sql.contains("FROM PerfAuditToken"),
            "route hotspot {key} should target the token fixture"
        );
        assert!(
            scenario
                .sql
                .contains("collection_id = '01KV5N439P0000000000000000'"),
            "route hotspot {key} should keep the fixed collection prefix"
        );
    }

    let branch = scenarios_by_key
        .get("token.collection_stage_id.branch_set.page_only.limit50")
        .expect("branch-set route hotspot should exist")
        .1;
    assert!(
        branch.sql.contains("stage IN ('Draft', 'Review')"),
        "branch-set route hotspot should use the small exact stage set"
    );
    assert!(
        branch.sql.contains("ORDER BY id ASC LIMIT 50"),
        "branch-set route hotspot should preserve the primary-key page order"
    );

    let wide_branch = scenarios_by_key
        .get("token.collection_stage_id.branch_set.wide_page_only.limit50")
        .expect("wide branch-set route hotspot should exist")
        .1;
    assert!(
        wide_branch.sql.contains(
            "stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden')"
        ),
        "wide branch-set hotspot should cover the admitted nine-branch route"
    );

    let over_cap = scenarios_by_key
        .get("token.collection_stage_id.overcap_fallback.page_only.limit50")
        .expect("over-cap route hotspot should exist")
        .1;
    assert!(
        over_cap.sql.contains(
            "stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07')"
        ),
        "over-cap route hotspot should exceed the branch-set admission cap"
    );

    assert_sparse_collection_in_route_hotspots(&scenarios_by_key);
}

fn assert_sparse_collection_in_route_hotspots(
    scenarios_by_key: &BTreeMap<&str, (usize, &MatrixScenario)>,
) {
    let (sparse_position, sparse_in) = scenarios_by_key
        .get("token.collection_id.sparse_in.page_only.limit50")
        .expect("sparse collection IN route hotspot should exist")
        .to_owned();
    assert!(
        sparse_position < DEFAULT_MATRIX_LIMIT,
        "sparse collection IN hotspot should run inside the default matrix window; position={sparse_position}"
    );
    assert!(
        sparse_in.sql.contains("collection_id IN"),
        "sparse collection IN hotspot should exercise the index multi-lookup route"
    );
    assert!(
        sparse_in.sql.contains("missing-collection-249"),
        "sparse collection IN hotspot should include 250 missing prefixes"
    );
    assert!(
        sparse_in.sql.contains("ORDER BY id ASC LIMIT 50"),
        "sparse collection IN hotspot should preserve the primary-key page order"
    );

    let (sparse_count_position, sparse_count) = scenarios_by_key
        .get("token.collection_id.sparse_in.count")
        .expect("sparse collection IN count hotspot should exist")
        .to_owned();
    assert!(
        sparse_count_position < DEFAULT_MATRIX_LIMIT,
        "sparse collection IN count hotspot should run inside the default matrix window; position={sparse_count_position}"
    );
    assert!(
        sparse_count.sql.contains("SELECT COUNT(*)"),
        "sparse collection IN count hotspot should exercise count terminal routing"
    );
    assert!(
        sparse_count.sql.contains("missing-collection-249"),
        "sparse collection IN count hotspot should include 250 missing prefixes"
    );
}

#[test]
fn sql_perf_matrix_exact_key_filter_selects_known_scenarios() {
    let deterministic = deterministic_matrix();
    let selected = filter_matrix_scenarios(
        deterministic,
        Some(
            "user.select.pk.all.pk_asc.limit1,\
             journaled_user.select.wide.name_range.age_asc.limit10",
        ),
    );
    let keys = selected
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        keys,
        vec![
            "user.select.pk.all.pk_asc.limit1",
            "journaled_user.select.wide.name_range.age_asc.limit10",
        ],
    );
}

#[test]
fn sql_perf_random_matrix_has_seeded_stable_shape() {
    let random = random_matrix(DEFAULT_RANDOM_SEED, 20);
    assert_eq!(random.len(), 20);
    assert_eq!(
        random.first().map(|scenario| scenario.key.as_str()),
        Some("random.1cdb018200000001.0000.blob"),
    );

    let mut keys = HashSet::new();
    for scenario in &random {
        assert_eq!(scenario.source, MatrixSource::Random);
        assert!(
            keys.insert(scenario.key.as_str()),
            "duplicate random scenario key '{}'",
            scenario.key,
        );
        assert!(
            scenario.sql.starts_with("SELECT"),
            "random scenario '{}' should use supported SELECT syntax",
            scenario.key,
        );
    }
    assert!(
        random
            .iter()
            .any(|scenario| scenario.surface == MatrixSurface::Token),
        "seeded random matrix should include token IN/branch route pressure"
    );
}

#[test]
fn sql_perf_matrix_storage_backend_comparison_pairs_all_storage_mirrors() {
    let samples = vec![
        storage_matrix_sample("heap_user.select.pk.all.pk_asc.limit1", "heap_user", 80, 10),
        storage_matrix_sample(
            "journaled_user.select.pk.all.pk_asc.limit1",
            "journaled_user",
            70,
            12,
        ),
    ];
    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        generated_scenario_count: samples.len(),
        executed_scenario_count: samples.len(),
        failed_scenario_count: 0,
        matrix_limit: samples.len(),
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples,
        failures: Vec::new(),
    };

    let markdown = matrix_markdown(&report);

    assert!(
        markdown.contains("Heap vs Journaled Unindexed Storage Mirror"),
        "storage mirror report should include the comparison table",
    );
    assert!(
        markdown.contains("intentional unindexed scan baselines"),
        "storage mirror report should label field predicate/order cases as unindexed baselines",
    );
    assert!(
        markdown.contains("Heap Total"),
        "storage mirror report should include heap totals",
    );
    assert!(
        markdown.contains("| `select.pk.all.pk_asc.limit1` | 80 | 70 | -10 | 0.87x | 10 | 12 |"),
        "storage mirror report should pair heap and journaled by scenario suffix",
    );
}

#[test]
fn sql_perf_matrix_main_fixture_hotspots_exclude_storage_mirror_baselines() {
    let samples = vec![
        storage_matrix_sample(
            "heap_user.select.pk.all.pk_asc.limit1",
            "heap_user",
            800,
            100,
        ),
        storage_matrix_sample(
            "journaled_user.select.pk.all.pk_asc.limit1",
            "journaled_user",
            700,
            120,
        ),
        {
            let mut sample = main_fixture_sample_with_kernel_scan(
                "user.select.pk.all.pk_asc.limit1",
                "user",
                90,
                5,
                "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
            );
            sample.kernel_row_retained_layout_hits = 1;
            sample.kernel_row_retained_slot_values = 3;
            sample.kernel_row_retained_octet_length_values = 1;
            sample
        },
    ];
    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        generated_scenario_count: samples.len(),
        executed_scenario_count: samples.len(),
        failed_scenario_count: 0,
        matrix_limit: samples.len(),
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples,
        failures: Vec::new(),
    };

    let markdown = matrix_markdown(&report);
    let main_fixture_total_section = markdown
        .split("## Top Main Fixture Total Instructions")
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .expect("main fixture total hotspot section should render");

    assert!(
        main_fixture_total_section.contains("user.select.pk.all.pk_asc.limit1"),
        "main fixture hotspot section should keep ordinary fixture scenarios",
    );
    assert!(
        !main_fixture_total_section.contains("heap_user"),
        "main fixture hotspot section should exclude heap storage mirror baselines",
    );
    assert!(
        !main_fixture_total_section.contains("journaled_user"),
        "main fixture hotspot section should exclude journaled storage mirror baselines",
    );

    let main_fixture_kernel_section =
        matrix_markdown_section(&markdown, "Top Main Fixture Kernel Row Scan Instructions");
    assert!(
        !main_fixture_kernel_section.contains("heap_user"),
        "main fixture kernel-row hotspot section should exclude heap storage mirror baselines",
    );
    assert!(
        !main_fixture_kernel_section.contains("journaled_user"),
        "main fixture kernel-row hotspot section should exclude journaled storage mirror baselines",
    );

    assert_main_fixture_kernel_retained_hotspot_sections(&markdown);
}

fn assert_main_fixture_kernel_retained_hotspot_sections(markdown: &str) {
    const SCENARIO_KEY: &str = "user.select.pk.all.pk_asc.limit1";
    const SCENARIO_ROW: &str =
        "| `user.select.pk.all.pk_asc.limit1` | user | 90 | 0 | 5 | 0 | 0 | 1 | 3 | 1 |";

    let main_fixture_kernel_section =
        matrix_markdown_section(markdown, "Top Main Fixture Kernel Row Scan Instructions");
    assert!(
        main_fixture_kernel_section.contains(SCENARIO_KEY),
        "main fixture kernel-row hotspot section should keep ordinary fixture scenarios",
    );
    assert!(
        main_fixture_kernel_section.contains("Retained Values"),
        "main fixture kernel-row hotspot section should expose retained-slot footprint columns",
    );
    assert!(
        main_fixture_kernel_section.contains("Length Values"),
        "main fixture kernel-row hotspot section should expose byte-length retained-slot columns",
    );

    let main_fixture_retained_section =
        matrix_markdown_section(markdown, "Top Main Fixture Kernel Row Retained Slot Values");
    assert!(
        main_fixture_retained_section.contains(SCENARIO_KEY),
        "main fixture retained-slot hotspot section should rank ordinary fixture scenarios",
    );
    assert!(
        main_fixture_retained_section.contains(SCENARIO_ROW),
        "main fixture retained-slot hotspot section should expose retained layout/value counts",
    );

    let main_fixture_length_section = matrix_markdown_section(
        markdown,
        "Top Main Fixture Kernel Row Retained Length Values",
    );
    assert!(
        main_fixture_length_section.contains(SCENARIO_KEY),
        "main fixture retained byte-length hotspot section should rank ordinary fixture scenarios",
    );
    assert!(
        main_fixture_length_section.contains(SCENARIO_ROW),
        "main fixture retained byte-length hotspot section should expose retained length counts",
    );
}

fn matrix_markdown_section<'a>(markdown: &'a str, title: &str) -> &'a str {
    markdown
        .split(&format!("## {title}"))
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .unwrap_or_else(|| panic!("matrix markdown section should render: {title}"))
}

#[test]
fn sql_perf_matrix_reports_compile_phase_hotspots() {
    let mut sample = report_matrix_sample(
        "token.collection_stage_id.overcap_fallback.page_only.limit50",
        "token",
        240,
        10,
        "SELECT id FROM PerfAuditToken WHERE collection_id = '01KV5N439P0000000000000000' AND stage IN ('Draft', 'Review', 'Hold') ORDER BY id ASC LIMIT 50",
    );
    sample.compile_local_instructions = 120;
    sample.compile_cache_key_local_instructions = 7;
    sample.compile_cache_lookup_local_instructions = 5;
    sample.compile_parse_local_instructions = 30;
    sample.compile_parse_tokenize_local_instructions = 11;
    sample.compile_parse_select_local_instructions = 8;
    sample.compile_parse_expr_local_instructions = 4;
    sample.compile_parse_predicate_local_instructions = 7;
    sample.compile_aggregate_lane_check_local_instructions = 3;
    sample.compile_prepare_local_instructions = 13;
    sample.compile_lower_local_instructions = 17;
    sample.compile_bind_local_instructions = 19;
    sample.compile_cache_insert_local_instructions = 26;

    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        generated_scenario_count: 1,
        executed_scenario_count: 1,
        failed_scenario_count: 0,
        matrix_limit: 1,
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples: vec![sample],
        failures: Vec::new(),
    };

    let markdown = matrix_markdown(&report);
    let compile_phase_section = markdown
        .split("## Top Compile Phase Instructions")
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .expect("compile phase hotspot section should render");

    assert!(
        compile_phase_section
            .contains("token.collection_stage_id.overcap_fallback.page_only.limit50"),
        "compile phase section should include scenarios with compile attribution",
    );
    assert!(
        compile_phase_section.contains("| `token.collection_stage_id.overcap_fallback.page_only.limit50` | token | 120 | 7 | 5 | 30 | 11 | 8 | 4 | 7 | 3 | 13 | 17 | 19 | 26 | 240 |"),
        "compile phase section should expose cache, parse, prepare, lower, bind, and insert costs",
    );
}

#[test]
fn sql_perf_matrix_reports_pure_covering_hotspots() {
    let samples = vec![main_fixture_sample_with_pure_covering(
        "user.select.pk.id_only.pk_asc.limit1",
        "user",
        120,
        75,
        35,
        "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
    )];
    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        generated_scenario_count: samples.len(),
        executed_scenario_count: samples.len(),
        failed_scenario_count: 0,
        matrix_limit: samples.len(),
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples,
        failures: Vec::new(),
    };

    let markdown = matrix_markdown(&report);
    let pure_covering_decode_section = markdown
        .split("## Top Pure Covering Decode Instructions")
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .expect("pure covering decode hotspot section should render");

    assert!(
        pure_covering_decode_section.contains("user.select.pk.id_only.pk_asc.limit1"),
        "pure covering decode section should include scenarios with decode attribution",
    );
    assert!(
        pure_covering_decode_section
            .contains("| `user.select.pk.id_only.pk_asc.limit1` | user | 75 | 35 | 120 |"),
        "pure covering decode section should expose decode, row assembly, and total costs",
    );

    let main_fixture_row_assembly_section = markdown
        .split("## Top Main Fixture Pure Covering Row Assembly Instructions")
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .expect("main fixture pure covering row assembly section should render");

    assert!(
        main_fixture_row_assembly_section.contains("user.select.pk.id_only.pk_asc.limit1"),
        "main fixture pure covering section should include ordinary fixture scenarios",
    );
}

#[test]
fn sql_perf_matrix_reports_hybrid_covering_hotspots() {
    let samples = vec![main_fixture_sample_with_hybrid_covering(
        "token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50",
        "token",
        240,
        1,
        0,
        50,
        "SELECT id, title FROM PerfAuditToken ORDER BY id ASC LIMIT 50",
    )];
    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        generated_scenario_count: samples.len(),
        executed_scenario_count: samples.len(),
        failed_scenario_count: 0,
        matrix_limit: samples.len(),
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples,
        failures: Vec::new(),
    };

    let markdown = matrix_markdown(&report);
    let hybrid_row_section = markdown
        .split("## Top Hybrid Covering Row Field Accesses")
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .expect("hybrid covering row-field hotspot section should render");

    assert!(
        hybrid_row_section
            .contains("token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50"),
        "hybrid covering section should include scenarios with row-backed field attribution",
    );
    assert!(
        hybrid_row_section.contains("| `token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50` | token | 1 | 0 | 50 | 50 | 240 |"),
        "hybrid covering section should expose path hits, field accesses, row gets, and total costs",
    );
}

fn storage_matrix_sample(key: &str, surface: &str, total: u64, store: u64) -> MatrixSample {
    report_matrix_sample(
        key,
        surface,
        total,
        store,
        "SELECT id FROM PerfAuditHeapUser ORDER BY id ASC LIMIT 1",
    )
}

fn report_matrix_sample(
    key: &str,
    surface: &str,
    total: u64,
    store: u64,
    sql: &str,
) -> MatrixSample {
    MatrixSample {
        key: key.to_string(),
        source: MatrixSource::Deterministic.label().to_string(),
        surface: surface.to_string(),
        family: "select.pk.all.pk_asc".to_string(),
        sql: sql.to_string(),
        compile_local_instructions: 1,
        compile_cache_key_local_instructions: 0,
        compile_cache_lookup_local_instructions: 0,
        compile_parse_local_instructions: 0,
        compile_parse_tokenize_local_instructions: 0,
        compile_parse_select_local_instructions: 0,
        compile_parse_expr_local_instructions: 0,
        compile_parse_predicate_local_instructions: 0,
        compile_aggregate_lane_check_local_instructions: 0,
        compile_prepare_local_instructions: 0,
        compile_lower_local_instructions: 0,
        compile_bind_local_instructions: 0,
        compile_cache_insert_local_instructions: 0,
        execute_local_instructions: total.saturating_sub(1),
        planner_local_instructions: 0,
        planner_schema_info_local_instructions: 0,
        planner_prepare_local_instructions: 0,
        planner_cache_key_local_instructions: 0,
        planner_cache_lookup_local_instructions: 0,
        planner_plan_build_local_instructions: 0,
        planner_cache_insert_local_instructions: 0,
        store_local_instructions: store,
        executor_local_instructions: total.saturating_sub(store),
        grouped_stream_local_instructions: 0,
        grouped_fold_local_instructions: 0,
        grouped_finalize_local_instructions: 0,
        scalar_aggregate_base_row_local_instructions: 0,
        scalar_aggregate_reducer_fold_local_instructions: 0,
        scalar_aggregate_expression_evaluations: 0,
        scalar_aggregate_filter_evaluations: 0,
        scalar_aggregate_rows_ingested: 0,
        scalar_aggregate_terminal_count: 0,
        scalar_aggregate_unique_input_expr_count: 0,
        scalar_aggregate_unique_filter_expr_count: 0,
        scalar_aggregate_sink_mode: None,
        pure_covering_decode_local_instructions: 0,
        pure_covering_row_assembly_local_instructions: 0,
        hybrid_covering_path_hits: 0,
        hybrid_covering_index_field_accesses: 0,
        hybrid_covering_row_field_accesses: 0,
        direct_data_row_scan_local_instructions: 0,
        direct_data_row_key_stream_local_instructions: 0,
        direct_data_row_row_read_local_instructions: 0,
        direct_data_row_key_encode_local_instructions: 0,
        direct_data_row_store_get_local_instructions: 0,
        direct_data_row_order_window_local_instructions: 0,
        direct_data_row_page_window_local_instructions: 0,
        kernel_row_scan_local_instructions: 0,
        kernel_row_key_stream_local_instructions: 0,
        kernel_row_row_read_local_instructions: 0,
        kernel_row_order_window_local_instructions: 0,
        kernel_row_page_window_local_instructions: 0,
        kernel_row_retained_layout_hits: 0,
        kernel_row_retained_slot_values: 0,
        kernel_row_retained_octet_length_values: 0,
        data_store_get_calls: 1,
        index_store_get_calls: 0,
        index_store_range_scan_calls: 0,
        index_store_entry_reads: 0,
        output_blob_values: 0,
        output_blob_bytes: 0,
        output_blob_hex_bytes: 0,
        sql_compiled_command_hits: 0,
        sql_compiled_command_misses: 1,
        shared_query_plan_hits: 0,
        shared_query_plan_misses: 1,
        total_local_instructions: total,
        outcome: MatrixOutcome {
            result_kind: "projection",
            entity: "PerfAuditHeapUser".to_string(),
            row_count: 1,
        },
    }
}

fn main_fixture_sample_with_kernel_scan(
    key: &str,
    surface: &str,
    total: u64,
    store: u64,
    sql: &str,
) -> MatrixSample {
    let mut sample = report_matrix_sample(key, surface, total, store, sql);
    sample.kernel_row_scan_local_instructions = total;
    sample.kernel_row_row_read_local_instructions = store;
    sample
}

fn main_fixture_sample_with_pure_covering(
    key: &str,
    surface: &str,
    total: u64,
    decode: u64,
    row_assembly: u64,
    sql: &str,
) -> MatrixSample {
    let mut sample = report_matrix_sample(key, surface, total, 0, sql);
    sample.pure_covering_decode_local_instructions = decode;
    sample.pure_covering_row_assembly_local_instructions = row_assembly;
    sample
}

fn main_fixture_sample_with_hybrid_covering(
    key: &str,
    surface: &str,
    total: u64,
    path_hits: u64,
    index_fields: u64,
    row_fields: u64,
    sql: &str,
) -> MatrixSample {
    let mut sample = report_matrix_sample(key, surface, total, 0, sql);
    sample.hybrid_covering_path_hits = path_hits;
    sample.hybrid_covering_index_field_accesses = index_fields;
    sample.hybrid_covering_row_field_accesses = row_fields;
    sample.data_store_get_calls = row_fields;
    sample
}

#[test]
fn sql_perf_matrix_reports_index_range_scan_hotspots() {
    let mut sample = report_matrix_sample(
        "token.collection_id.sparse_in.page_only.limit50",
        "token",
        240,
        30,
        "SELECT id FROM PerfAuditToken WHERE collection_id IN ('01KV5N439P0000000000000000', 'missing-collection-000') ORDER BY id ASC LIMIT 50",
    );
    sample.index_store_range_scan_calls = 251;
    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        generated_scenario_count: 1,
        executed_scenario_count: 1,
        failed_scenario_count: 0,
        matrix_limit: 1,
        scenario_key_filter: None,
        random_seed: None,
        random_case_count: 0,
        samples: vec![sample],
        failures: Vec::new(),
    };

    let markdown = matrix_markdown(&report);
    let range_scan_section = markdown
        .split("## Top Index Store Range Scans")
        .nth(1)
        .and_then(|tail| tail.split("##").next())
        .expect("index range-scan hotspot section should render");

    assert!(
        range_scan_section.contains("token.collection_id.sparse_in.page_only.limit50"),
        "range-scan hotspot section should include sparse IN scenarios",
    );
    assert!(
        range_scan_section.contains("| 251 |"),
        "range-scan hotspot section should expose index range traversal counts",
    );
}

#[test]
#[ignore = "expensive PocketIC hotspot scan; run manually with --ignored --nocapture"]
fn sql_perf_generated_matrix_reports_hotspots() {
    let fixture = install_sql_perf_canister_fixture();
    reset_icydb_fixtures(&fixture);

    let mode = matrix_mode();
    let scenarios = generated_matrix(mode);
    let generated_scenario_count = scenarios.len();
    let scenario_key_filter = matrix_scenario_key_filter();
    let scenarios = filter_matrix_scenarios(scenarios, scenario_key_filter.as_deref());
    let matrix_limit = matrix_limit(scenarios.len());
    let selected = scenarios.into_iter().take(matrix_limit).collect::<Vec<_>>();
    let mut samples = Vec::new();
    let mut failures = Vec::new();
    for scenario in &selected {
        match sample_scenario(&fixture, scenario) {
            Ok(sample) => samples.push(sample),
            Err(failure) => failures.push(*failure),
        }
    }
    let random_case_count = if mode == MatrixMode::Random {
        random_case_count()
    } else {
        0
    };

    let report = MatrixReport {
        matrix_mode: mode.label().to_string(),
        generated_scenario_count,
        executed_scenario_count: samples.len(),
        failed_scenario_count: failures.len(),
        matrix_limit,
        scenario_key_filter,
        random_seed: (mode == MatrixMode::Random).then(random_seed),
        random_case_count,
        samples,
        failures,
    };

    write_matrix_reports(&report);
    print_matrix_summary(&report);
}
