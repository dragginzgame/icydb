use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error,
    db::{QueryExecutionAttribution, SqlQueryExecutionAttribution, sql::SqlQueryResult},
};
use icydb_testing_integration::{
    install_fixture_canister, reset_icydb_fixtures, upgrade_fixture_canister,
};
use serde::Deserialize;

// Mirror the dedicated perf-audit query envelope so the testkit can decode the
// query result plus the compile/execute instruction split from the canister.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    attribution: SqlQueryExecutionAttribution,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlTotalOnlyPerfResult {
    result: SqlQueryResult,
    instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct FluentTotalOnlyPerfResult {
    row_count: u32,
    instructions: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct FluentQueryPerfOutcome {
    result_kind: String,
    entity: String,
    row_count: u32,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct FluentQueryPerfResult {
    outcome: FluentQueryPerfOutcome,
    attribution: QueryExecutionAttribution,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct StorageWritePerfResult {
    first_insert_local_instructions: u64,
    steady_insert_avg_local_instructions: u64,
    steady_update_avg_local_instructions: u64,
    steady_delete_avg_local_instructions: u64,
    write_then_read_back_local_instructions: u64,
    read_back_rows: u32,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct ConstraintActivationPerfResult {
    no_check: StorageWritePerfResult,
    add_check_local_instructions: u64,
    add_check_rows_scanned: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlWriteMaterializationPerfResult {
    local_instructions: [u64; 4],
    rows: [u32; 4],
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct ResumableUpdatePerfResult {
    prepare_local_instructions: u64,
    forward_local_instructions: Vec<u64>,
    verify_local_instructions: Vec<u64>,
    forward_keys_scanned: u32,
    verify_keys_scanned: u32,
    rows_updated: u32,
}

const SQL_WRITE_MATERIALIZATION_METRICS: [&str; 4] = [
    "update count",
    "update returning",
    "delete count",
    "delete returning",
];
const SQL_WRITE_MATERIALIZATION_BUDGET: u64 = 750_000_000;
const RESUMABLE_UPDATE_STEP_BUDGET: u64 = 2_000_000_000;

#[derive(Clone, Copy, Debug)]
enum SqlPerfSurface {
    Account,
    Blob,
    Token,
    User,
}

impl SqlPerfSurface {
    const fn label(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Blob => "blob",
            Self::Token => "token",
            Self::User => "user",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct SqlPerfScenario {
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    sql: &'static str,
    query_loop_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SqlPerfOutcome {
    result_kind: &'static str,
    entity: String,
    row_count: usize,
}

#[derive(Clone, Debug, PartialEq)]
struct SqlPerfScenarioSample {
    scenario_key: String,
    compile_local_instructions: u64,
    compile_phases: SqlPerfCompilePhases,
    execute_local_instructions: u64,
    grouped_count_row_materialization_local_instructions: u64,
    grouped_count_group_lookup_local_instructions: u64,
    hybrid_covering_path_hits: u64,
    hybrid_covering_index_field_accesses: u64,
    hybrid_covering_row_field_accesses: u64,
    data_store_get_calls: u64,
    index_store_get_calls: u64,
    index_store_range_scan_calls: u64,
    index_store_entry_reads: u64,
    sql_compiled_command_cache_hits: u64,
    sql_compiled_command_cache_misses: u64,
    shared_query_plan_cache_hits: u64,
    shared_query_plan_cache_misses: u64,
    local_instructions: u64,
    outcome: SqlPerfOutcome,
}

const fn scenario(
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    sql: &'static str,
) -> SqlPerfScenario {
    SqlPerfScenario {
        scenario_key,
        surface,
        sql,
        query_loop_count: 1,
    }
}

const fn repeat_scenario(
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    sql: &'static str,
    query_loop_count: usize,
) -> SqlPerfScenario {
    SqlPerfScenario {
        scenario_key,
        surface,
        sql,
        query_loop_count,
    }
}

fn install_sql_perf_canister_fixture() -> StandaloneCanisterFixture {
    install_fixture_canister("sql_perf")
}

fn reset_sql_perf_fixtures(fixture: &StandaloneCanisterFixture) {
    // Clear retained state from an earlier scenario batch and reload the
    // deterministic perf fixture window before sampling.
    reset_icydb_fixtures(fixture);
}

fn load_journaled_reentry_probe_fixture(fixture: &StandaloneCanisterFixture) {
    let result: Result<(), Error> = fixture
        .update_call("load_journaled_reentry_probe_fixture", ())
        .expect("journaled reentry probe fixture load should decode");

    result.expect("journaled reentry probe fixture load should succeed");
}

fn query_surface_with_perf(
    fixture: &StandaloneCanisterFixture,
    surface: SqlPerfSurface,
    sql: &str,
    query_loop_count: usize,
) -> Result<SqlQueryPerfResult, Error> {
    match surface {
        SqlPerfSurface::User if query_loop_count == 1 => fixture
            .query_call("query_user_with_perf", (sql.to_string(),))
            .expect("query_user_with_perf should decode"),
        SqlPerfSurface::User => fixture
            .query_call(
                "query_user_loop_with_perf",
                (
                    sql.to_string(),
                    u32::try_from(query_loop_count)
                        .expect("query loop count should fit into canister argument"),
                ),
            )
            .expect("query_user_loop_with_perf should decode"),
        SqlPerfSurface::Account if query_loop_count == 1 => fixture
            .query_call("query_account_with_perf", (sql.to_string(),))
            .expect("query_account_with_perf should decode"),
        SqlPerfSurface::Account => fixture
            .query_call(
                "query_account_loop_with_perf",
                (
                    sql.to_string(),
                    u32::try_from(query_loop_count)
                        .expect("query loop count should fit into canister argument"),
                ),
            )
            .expect("query_account_loop_with_perf should decode"),
        SqlPerfSurface::Blob if query_loop_count == 1 => fixture
            .query_call("query_blob_with_perf", (sql.to_string(),))
            .expect("query_blob_with_perf should decode"),
        SqlPerfSurface::Blob => fixture
            .query_call(
                "query_blob_loop_with_perf",
                (
                    sql.to_string(),
                    u32::try_from(query_loop_count)
                        .expect("query loop count should fit into canister argument"),
                ),
            )
            .expect("query_blob_loop_with_perf should decode"),
        SqlPerfSurface::Token if query_loop_count == 1 => fixture
            .query_call("query_token_with_perf", (sql.to_string(),))
            .expect("query_token_with_perf should decode"),
        SqlPerfSurface::Token => fixture
            .query_call(
                "query_token_loop_with_perf",
                (
                    sql.to_string(),
                    u32::try_from(query_loop_count)
                        .expect("query loop count should fit into canister argument"),
                ),
            )
            .expect("query_token_loop_with_perf should decode"),
    }
}

fn warm_query_surface_with_perf(
    fixture: &StandaloneCanisterFixture,
    surface: SqlPerfSurface,
    sql: &str,
) -> Result<SqlQueryPerfResult, Error> {
    match surface {
        SqlPerfSurface::User => fixture
            .update_call("warm_user_query_with_perf", (sql.to_string(),))
            .expect("warm_user_query_with_perf should decode"),
        SqlPerfSurface::Account => fixture
            .update_call("warm_account_query_with_perf", (sql.to_string(),))
            .expect("warm_account_query_with_perf should decode"),
        SqlPerfSurface::Blob => fixture
            .update_call("warm_blob_query_with_perf", (sql.to_string(),))
            .expect("warm_blob_query_with_perf should decode"),
        SqlPerfSurface::Token => fixture
            .update_call("warm_token_query_with_perf", (sql.to_string(),))
            .expect("warm_token_query_with_perf should decode"),
    }
}

fn summarize_perf_outcome(result: &SqlQueryResult) -> SqlPerfOutcome {
    match result {
        SqlQueryResult::Count { entity, row_count } => SqlPerfOutcome {
            result_kind: "count",
            entity: entity.clone(),
            row_count: usize::try_from(*row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Projection(rows) => SqlPerfOutcome {
            result_kind: "projection",
            entity: rows.entity.clone(),
            row_count: usize::try_from(rows.row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Grouped(rows) => SqlPerfOutcome {
            result_kind: "grouped",
            entity: rows.entity.clone(),
            row_count: usize::try_from(rows.row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Explain { entity, .. } => SqlPerfOutcome {
            result_kind: "explain",
            entity: entity.clone(),
            row_count: 1,
        },
        SqlQueryResult::Describe(entity) => SqlPerfOutcome {
            result_kind: "describe",
            entity: entity.entity_name().to_string(),
            row_count: entity.fields().len(),
        },
        SqlQueryResult::ShowIndexes { entity, indexes } => SqlPerfOutcome {
            result_kind: "show_indexes",
            entity: entity.clone(),
            row_count: indexes.len(),
        },
        SqlQueryResult::ShowConstraints {
            entity,
            constraints,
        } => SqlPerfOutcome {
            result_kind: "show_constraints",
            entity: entity.clone(),
            row_count: constraints.len(),
        },
        SqlQueryResult::ShowColumns { entity, columns } => SqlPerfOutcome {
            result_kind: "show_columns",
            entity: entity.clone(),
            row_count: columns.len(),
        },
        SqlQueryResult::ShowEntities { entities, .. } => SqlPerfOutcome {
            result_kind: "show_entities",
            entity: String::new(),
            row_count: entities.len(),
        },
        SqlQueryResult::ShowStores { stores, .. } => SqlPerfOutcome {
            result_kind: "show_stores",
            entity: String::new(),
            row_count: stores.len(),
        },
        SqlQueryResult::ShowMemory { memory } => SqlPerfOutcome {
            result_kind: "show_memory",
            entity: String::new(),
            row_count: memory.len(),
        },
        SqlQueryResult::Ddl { entity, .. } => SqlPerfOutcome {
            result_kind: "icydb_ddl",
            entity: entity.clone(),
            row_count: 1,
        },
    }
}

fn rendered_projection_rows(result: SqlQueryResult) -> Vec<Vec<String>> {
    match result {
        SqlQueryResult::Projection(rows) => rows.rendered_rows(),
        other => panic!("expected projection payload, got {other:?}"),
    }
}

// SqlPerfCompilePhases keeps exact compile attribution together for the
// focused route diagnostics that remain in this target. Repeated sampling and
// statistical aggregation belong to the P2 confirmation harness.
#[derive(Clone, Debug, Eq, PartialEq)]
struct SqlPerfCompilePhases {
    cache_key: u64,
    cache_lookup: u64,
    parse: u64,
    tokenize: u64,
    select: u64,
    expr: u64,
    predicate: u64,
    aggregate_check: u64,
    prepare: u64,
    lower: u64,
    bind: u64,
    cache_insert: u64,
}

impl SqlPerfCompilePhases {
    const fn from_attribution(attribution: &SqlQueryExecutionAttribution) -> Self {
        let compile = &attribution.compile;

        Self {
            cache_key: compile.cache_key_local_instructions,
            cache_lookup: compile.cache_lookup_local_instructions,
            parse: compile.parse_local_instructions,
            tokenize: compile.parse_tokenize_local_instructions,
            select: compile.parse_select_local_instructions,
            expr: compile.parse_expr_local_instructions,
            predicate: compile.parse_predicate_local_instructions,
            aggregate_check: compile.aggregate_lane_check_local_instructions,
            prepare: compile.prepare_local_instructions,
            lower: compile.lower_local_instructions,
            bind: compile.bind_local_instructions,
            cache_insert: compile.cache_insert_local_instructions,
        }
    }
}

fn build_sql_perf_scenario_sample(
    scenario: SqlPerfScenario,
    sample: SqlQueryPerfResult,
) -> SqlPerfScenarioSample {
    let attribution = &sample.attribution;
    let grouped_count = attribution.grouped.map(|grouped| grouped.count);
    let hybrid = attribution.hybrid_covering;

    SqlPerfScenarioSample {
        scenario_key: scenario.scenario_key.to_string(),
        compile_local_instructions: attribution.compile_local_instructions,
        compile_phases: SqlPerfCompilePhases::from_attribution(attribution),
        execute_local_instructions: attribution.execute_local_instructions,
        grouped_count_row_materialization_local_instructions: grouped_count
            .map_or(0, |count| count.row_materialization_local_instructions),
        grouped_count_group_lookup_local_instructions: grouped_count
            .map_or(0, |count| count.group_lookup_local_instructions),
        hybrid_covering_path_hits: hybrid.map_or(0, |hybrid| hybrid.path_hits),
        hybrid_covering_index_field_accesses: hybrid
            .map_or(0, |hybrid| hybrid.index_field_accesses),
        hybrid_covering_row_field_accesses: hybrid.map_or(0, |hybrid| hybrid.row_field_accesses),
        data_store_get_calls: attribution.store_get_calls,
        index_store_get_calls: attribution.index_store_get_calls,
        index_store_range_scan_calls: attribution.index_store_range_scan_calls,
        index_store_entry_reads: attribution.index_store_entry_reads,
        sql_compiled_command_cache_hits: attribution.cache.sql_compiled_command_hits,
        sql_compiled_command_cache_misses: attribution.cache.sql_compiled_command_misses,
        shared_query_plan_cache_hits: attribution.cache.shared_query_plan_hits,
        shared_query_plan_cache_misses: attribution.cache.shared_query_plan_misses,
        local_instructions: attribution.total_local_instructions,
        outcome: summarize_perf_outcome(&sample.result),
    }
}

// sample_perf_scenario captures one exact focused result. P2 owns repeated
// cold/warm sampling, stability checks, and summary statistics.
fn sample_perf_scenario(
    fixture: &StandaloneCanisterFixture,
    scenario: SqlPerfScenario,
) -> SqlPerfScenarioSample {
    let sample = query_surface_with_perf(
        fixture,
        scenario.surface,
        scenario.sql,
        scenario.query_loop_count,
    )
    .unwrap_or_else(|err| {
        panic!(
            "perf scenario '{}' on '{}' should succeed: {err}",
            scenario.scenario_key,
            scenario.surface.label(),
        )
    });

    build_sql_perf_scenario_sample(scenario, sample)
}

const TOKEN_BRANCH_SET_PAGE_SQL: &str = "\
SELECT id \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review') \
ORDER BY id ASC \
LIMIT 3";

const TOKEN_BRANCH_SET_NONCOVERED_PAGE_SQL: &str = "\
SELECT id, title \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review') \
ORDER BY id ASC \
LIMIT 3";

const TOKEN_BRANCH_SET_INDEX_RESIDUAL_PAGE_SQL: &str = "\
SELECT id, stage \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review') \
  AND stage != 'Review' \
ORDER BY id ASC \
LIMIT 3";

const TOKEN_BRANCH_SET_COUNT_SQL: &str = "\
SELECT COUNT(*) \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review')";

const TOKEN_BRANCH_SET_DUPLICATE_COUNT_SQL: &str = "\
SELECT COUNT(*) \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Draft', 'Review')";

const TOKEN_BRANCH_SET_PAGE_LIMIT50_SQL: &str = "\
SELECT id \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_BRANCH_SET_NONCOVERED_PAGE_LIMIT50_SQL: &str = "\
SELECT id, title \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_BRANCH_SET_WIDE_PAGE_LIMIT50_SQL: &str = "\
SELECT id \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_BRANCH_SET_WIDE_NONCOVERED_PAGE_LIMIT50_SQL: &str = "\
SELECT id, title \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_BRANCH_SET_OVERCAP_FALLBACK_LIMIT50_SQL: &str = "\
SELECT id \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_BRANCH_SET_LARGE_IN_FALLBACK_LIMIT50_SQL: &str = "\
SELECT id \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07', 'Missing08', 'Missing09', 'Missing10', 'Missing11', 'Missing12', 'Missing13', 'Missing14', 'Missing15', 'Missing16', 'Missing17', 'Missing18', 'Missing19', 'Missing20', 'Missing21', 'Missing22', 'Missing23', 'Missing24', 'Missing25', 'Missing26', 'Missing27', 'Missing28', 'Missing29', 'Missing30') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_BRANCH_SET_OVERCAP_FALLBACK_NONCOVERED_LIMIT50_SQL: &str = "\
SELECT id, title \
FROM PerfAuditToken \
WHERE collection_id = '01KV5N439P0000000000000000' \
  AND stage IN ('Draft', 'Review', 'Hold', 'Minted', 'Frozen', 'Burned', 'Listed', 'Sold', 'Hidden', 'Missing00', 'Missing01', 'Missing02', 'Missing03', 'Missing04', 'Missing05', 'Missing06', 'Missing07') \
ORDER BY id ASC \
LIMIT 50";

const TOKEN_COLLECTION_SPARSE_IN_LIMIT50_SQL: &str = "\
SELECT id \
FROM PerfAuditToken \
WHERE collection_id IN ('01KV5N439P0000000000000000', 'missing-collection-000', 'missing-collection-001', 'missing-collection-002', 'missing-collection-003', 'missing-collection-004', 'missing-collection-005', 'missing-collection-006', 'missing-collection-007', 'missing-collection-008', 'missing-collection-009', 'missing-collection-010', 'missing-collection-011', 'missing-collection-012', 'missing-collection-013', 'missing-collection-014', 'missing-collection-015', 'missing-collection-016', 'missing-collection-017', 'missing-collection-018', 'missing-collection-019', 'missing-collection-020', 'missing-collection-021', 'missing-collection-022', 'missing-collection-023', 'missing-collection-024', 'missing-collection-025', 'missing-collection-026', 'missing-collection-027', 'missing-collection-028', 'missing-collection-029', 'missing-collection-030') \
ORDER BY id ASC \
LIMIT 50";
const TOKEN_COLLECTION_FULL_ENTITY_FLUENT_SCENARIO: &str =
    "token.collection_id.full_entity.limit300";
const TOKEN_COLLECTION_FULL_ENTITY_ROWS: u32 = 256;
const TOKEN_COLLECTION_REPEAT_LOAD_RUNS: u32 = 50;

fn token_branch_set_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "token.collection_stage_id.branch_set.page_only.limit3",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_PAGE_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.noncovered_page_only.limit3",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_NONCOVERED_PAGE_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.index_residual_covering.limit3",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_INDEX_RESIDUAL_PAGE_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.count",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_COUNT_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.duplicate_count",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_DUPLICATE_COUNT_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_PAGE_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.noncovered_page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_NONCOVERED_PAGE_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.wide_page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_WIDE_PAGE_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_stage_id.branch_set.wide_noncovered_page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_WIDE_NONCOVERED_PAGE_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_stage_id.overcap_fallback.page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_OVERCAP_FALLBACK_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_stage_id.large_in_fallback.page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_LARGE_IN_FALLBACK_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_id.sparse_in.page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_COLLECTION_SPARSE_IN_LIMIT50_SQL,
        ),
        scenario(
            "token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50",
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_OVERCAP_FALLBACK_NONCOVERED_LIMIT50_SQL,
        ),
    ]
}

fn repeated_query_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        repeat_scenario(
            "repeat.user.pk.order_only.asc.limit1.runs10",
            SqlPerfSurface::User,
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
            10,
        ),
        repeat_scenario(
            "repeat.user.pk.order_only.asc.limit2.runs10",
            SqlPerfSurface::User,
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
            10,
        ),
        repeat_scenario(
            "repeat.user.name.lower.order_only.asc.limit3.runs10",
            SqlPerfSurface::User,
            "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.grouped.age_count.limit10.runs10",
            SqlPerfSurface::User,
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
            10,
        ),
        repeat_scenario(
            "repeat.user.age.order_only.asc.limit3.runs10",
            SqlPerfSurface::User,
            "SELECT id, age FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.distinct.age.order_only.asc.limit3.runs10",
            SqlPerfSurface::User,
            "SELECT DISTINCT age FROM PerfAuditUser ORDER BY age ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.case_where.order_id.limit3.runs10",
            SqlPerfSurface::User,
            "SELECT id, name FROM PerfAuditUser WHERE CASE WHEN age >= 30 THEN TRUE ELSE active END ORDER BY id ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.age_plus_rank.direct_order.asc.limit3.runs10",
            SqlPerfSurface::User,
            "SELECT id, age FROM PerfAuditUser ORDER BY age + rank ASC, id ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.grouped.age_count.no_order.runs10",
            SqlPerfSurface::User,
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age",
            10,
        ),
        repeat_scenario(
            "repeat.user.grouped.case_sum.having_alias.order.limit5.runs10",
            SqlPerfSurface::User,
            "SELECT age, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS high_count FROM PerfAuditUser GROUP BY age HAVING high_count > 0 ORDER BY high_count DESC, age ASC LIMIT 5",
            10,
        ),
        repeat_scenario(
            "repeat.account.active.lower.order_handle.asc.limit3.runs10",
            SqlPerfSurface::Account,
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
            10,
        ),
    ]
}

fn print_branch_set_perf_sample(label: &str, sample: &SqlPerfScenarioSample) {
    let scenario = sample.scenario_key.as_str();
    let rows = sample.outcome.row_count;
    let compile = sample.compile_local_instructions;
    let compile_phases = &sample.compile_phases;
    let compile_key = compile_phases.cache_key;
    let compile_lookup = compile_phases.cache_lookup;
    let parse = compile_phases.parse;
    let tokenize = compile_phases.tokenize;
    let select = compile_phases.select;
    let expr = compile_phases.expr;
    let predicate = compile_phases.predicate;
    let aggregate_check = compile_phases.aggregate_check;
    let prepare = compile_phases.prepare;
    let lower = compile_phases.lower;
    let bind = compile_phases.bind;
    let cache_insert = compile_phases.cache_insert;
    let execute = sample.execute_local_instructions;
    let total = sample.local_instructions;
    let data_gets = sample.data_store_get_calls;
    let index_gets = sample.index_store_get_calls;
    let index_ranges = sample.index_store_range_scan_calls;
    let index_entries = sample.index_store_entry_reads;
    let grouped_count_rows = sample.grouped_count_row_materialization_local_instructions;
    let grouped_count_lookup = sample.grouped_count_group_lookup_local_instructions;
    let hybrid_hits = sample.hybrid_covering_path_hits;
    let hybrid_index_fields = sample.hybrid_covering_index_field_accesses;
    let hybrid_row_fields = sample.hybrid_covering_row_field_accesses;
    let sql_hits = sample.sql_compiled_command_cache_hits;
    let sql_misses = sample.sql_compiled_command_cache_misses;
    let shared_hits = sample.shared_query_plan_cache_hits;
    let shared_misses = sample.shared_query_plan_cache_misses;

    println!(
        "branch-set perf {label}: scenario={scenario} rows={rows} compile={compile} compile_key={compile_key} compile_lookup={compile_lookup} parse={parse} tokenize={tokenize} select={select} expr={expr} predicate={predicate} agg_check={aggregate_check} prepare={prepare} lower={lower} bind={bind} cache_insert={cache_insert} execute={execute} total={total} data_gets={data_gets} index_gets={index_gets} index_ranges={index_ranges} index_entries={index_entries} grouped_count_rows={grouped_count_rows} grouped_count_lookup={grouped_count_lookup} hybrid_hits={hybrid_hits} hybrid_index_fields={hybrid_index_fields} hybrid_row_fields={hybrid_row_fields} sql_hits={sql_hits} sql_misses={sql_misses} shared_hits={shared_hits} shared_misses={shared_misses}",
    );
}

// WarmCacheContractCase keeps one update-then-query cache contract case
// together so the IC testkit audit can prove that a warm update call feeds the
// later compiled-plus-shared query cache path across more than one query family.
struct WarmCacheContractCase {
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    sql: &'static str,
}

// sql_perf_scenario_by_key resolves one focused cache or route contract.
fn sql_perf_scenario_by_key(scenario_key: &str) -> SqlPerfScenario {
    token_branch_set_scenarios()
        .into_iter()
        .chain(repeated_query_scenarios())
        .find(|scenario| scenario.scenario_key == scenario_key)
        .unwrap_or_else(|| panic!("sql perf scenario '{scenario_key}' should exist"))
}

// assert_repeat_scenario_keeps_compiled_and_shared_cache_path checks one exact
// in-call repeat contract; P2 owns repetition across independent canisters.
fn assert_repeat_scenario_keeps_compiled_and_shared_cache_path(
    fixture: &StandaloneCanisterFixture,
    scenario: SqlPerfScenario,
) {
    let repeated_hits =
        u64::try_from(scenario.query_loop_count.saturating_sub(1)).expect("loop count should fit");
    let sample = sample_perf_scenario(fixture, scenario);

    assert_eq!(
        sample.sql_compiled_command_cache_hits, repeated_hits,
        "scenario '{}' should keep SQL compiled-command hits for every repeated pass",
        sample.scenario_key,
    );
    assert_eq!(
        sample.sql_compiled_command_cache_misses, 1,
        "scenario '{}' should keep exactly one cold SQL compiled-command miss",
        sample.scenario_key,
    );
    assert_eq!(
        sample.shared_query_plan_cache_hits, repeated_hits,
        "scenario '{}' should surface shared lower query-plan hits on every repeated pass",
        sample.scenario_key,
    );
    assert_eq!(
        sample.shared_query_plan_cache_misses, 1,
        "scenario '{}' should touch the shared lower query-plan cache only once as cold-fill support",
        sample.scenario_key,
    );
}

// assert_update_warm_persists_compiled_and_shared_cache_path proves that an update-side
// warm call still fills the compiled-command cache and the shared lower
// query-plan cache for the later query-side call.
fn assert_update_warm_persists_compiled_and_shared_cache_path(
    fixture: &StandaloneCanisterFixture,
    case: WarmCacheContractCase,
) {
    let warm =
        warm_query_surface_with_perf(fixture, case.surface, case.sql).unwrap_or_else(|err| {
            panic!(
                "update warm cache contract scenario '{}' should succeed: {err}",
                case.scenario_key,
            )
        });

    // Phase 1: the update-side warm call should populate the compiled-command
    // cache and touch the shared lower cache once for cold fill.
    assert_eq!(
        warm.attribution.cache.sql_compiled_command_misses, 1,
        "scenario '{}' should populate the SQL compiled-command cache on the update warm pass",
        case.scenario_key,
    );
    assert_eq!(
        warm.attribution.cache.shared_query_plan_misses, 1,
        "scenario '{}' should touch the shared lower query-plan cache only once during the update warm cold fill",
        case.scenario_key,
    );

    // Phase 2: the later query call should stay entirely on the compiled SQL
    // hit path plus the shared lower query-plan hit path.
    let query = query_surface_with_perf(fixture, case.surface, case.sql, 1).unwrap_or_else(|err| {
        panic!(
            "query cache contract scenario '{}' should succeed after update warm: {err}",
            case.scenario_key,
        )
    });
    assert_eq!(
        query.attribution.cache.sql_compiled_command_hits, 1,
        "scenario '{}' should reuse the compiled SQL artifact warmed by the update call",
        case.scenario_key,
    );
    assert_eq!(
        query.attribution.cache.sql_compiled_command_misses, 0,
        "scenario '{}' should not recompile the warmed SQL artifact on the later query call",
        case.scenario_key,
    );
    assert_eq!(
        query.attribution.cache.shared_query_plan_hits, 1,
        "scenario '{}' should reuse the warmed shared lower query-plan cache on the later query call",
        case.scenario_key,
    );
    assert_eq!(
        query.attribution.cache.shared_query_plan_misses, 0,
        "scenario '{}' should not rebuild the lower shared query plan on the later query call",
        case.scenario_key,
    );
}

const HEAP_PRIMARY_LIMIT_ONE_SQL: &str =
    "SELECT id, name FROM PerfAuditHeapUser ORDER BY id ASC LIMIT 1";
const JOURNALED_PRIMARY_LIMIT_ONE_SQL: &str =
    "SELECT id, name FROM PerfAuditJournaledUser ORDER BY id ASC LIMIT 1";
const JOURNALED_UPGRADE_REENTRY_BUDGET: u64 = 5_000_000_000;

fn query_sql_limit_one_with_perf(
    fixture: &StandaloneCanisterFixture,
    method: &str,
    sql: &str,
    decode_expectation: &str,
    success_expectation: &str,
) -> SqlQueryPerfResult {
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .query_call(method, (sql.to_string(),))
        .expect(decode_expectation);

    result.expect(success_expectation)
}

fn query_sql_loop_limit_one_with_perf(
    fixture: &StandaloneCanisterFixture,
    method: &str,
    sql: &str,
    query_loop_count: u32,
    decode_expectation: &str,
    success_expectation: &str,
) -> SqlQueryPerfResult {
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .query_call(method, (sql.to_string(), query_loop_count))
        .expect(decode_expectation);

    result.expect(success_expectation)
}

fn warm_sql_limit_one_with_perf(
    fixture: &StandaloneCanisterFixture,
    method: &str,
    sql: &str,
    decode_expectation: &str,
    success_expectation: &str,
) {
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .update_call(method, (sql.to_string(),))
        .expect(decode_expectation);

    result.expect(success_expectation);
}

fn print_sql_limit_one_attribution(label: &str, perf: &SqlQueryPerfResult) {
    let attribution = &perf.attribution;
    let execution = &attribution.execution;
    let cache = &attribution.cache;

    println!(
        "{label}: compile={} plan_lookup={} planner={} store={} executor_invocation={} executor={} response_finalize={} execute={} response_decode={} total={} compiled_hits={} compiled_misses={} shared_hits={} shared_misses={}",
        attribution.compile_local_instructions,
        attribution.plan_lookup_local_instructions,
        execution.planner_local_instructions,
        execution.store_local_instructions,
        execution.executor_invocation_local_instructions,
        execution.executor_local_instructions,
        execution.response_finalization_local_instructions,
        attribution.execute_local_instructions,
        attribution.response_decode_local_instructions,
        attribution.total_local_instructions,
        cache.sql_compiled_command_hits,
        cache.sql_compiled_command_misses,
        cache.shared_query_plan_hits,
        cache.shared_query_plan_misses,
    );
}

fn print_storage_read_comparison(
    label: &str,
    heap: &SqlQueryPerfResult,
    journaled: &SqlQueryPerfResult,
) {
    println!(
        "{label}: heap_total={} journaled_total={} total_delta={} total_ratio={} heap_compile={} journaled_compile={} compile_delta={} heap_execute={} journaled_execute={} execute_delta={} heap_store={} journaled_store={} store_delta={} heap_executor={} journaled_executor={} executor_delta={} heap_data_store_gets={} journaled_data_store_gets={}",
        heap.attribution.total_local_instructions,
        journaled.attribution.total_local_instructions,
        signed_instruction_delta(
            journaled.attribution.total_local_instructions,
            heap.attribution.total_local_instructions,
        ),
        instruction_ratio_text(
            journaled.attribution.total_local_instructions,
            heap.attribution.total_local_instructions,
        ),
        heap.attribution.compile_local_instructions,
        journaled.attribution.compile_local_instructions,
        signed_instruction_delta(
            journaled.attribution.compile_local_instructions,
            heap.attribution.compile_local_instructions,
        ),
        heap.attribution.execute_local_instructions,
        journaled.attribution.execute_local_instructions,
        signed_instruction_delta(
            journaled.attribution.execute_local_instructions,
            heap.attribution.execute_local_instructions,
        ),
        heap.attribution.execution.store_local_instructions,
        journaled.attribution.execution.store_local_instructions,
        signed_instruction_delta(
            journaled.attribution.execution.store_local_instructions,
            heap.attribution.execution.store_local_instructions,
        ),
        heap.attribution.execution.executor_local_instructions,
        journaled.attribution.execution.executor_local_instructions,
        signed_instruction_delta(
            journaled.attribution.execution.executor_local_instructions,
            heap.attribution.execution.executor_local_instructions,
        ),
        heap.attribution.store_get_calls,
        journaled.attribution.store_get_calls,
    );
}

fn signed_instruction_delta(value: u64, baseline: u64) -> String {
    if value >= baseline {
        format!("+{}", value - baseline)
    } else {
        format!("-{}", baseline - value)
    }
}

fn instruction_ratio_text(value: u64, baseline: u64) -> String {
    if baseline == 0 {
        return "n/a".to_string();
    }

    let scaled = value.saturating_mul(100) / baseline;
    format!("{}.{:02}x", scaled / 100, scaled % 100)
}

fn print_cached_journaled_sql_limit_one_attribution(perf: &SqlQueryPerfResult) {
    let attribution = &perf.attribution;
    let compile = &attribution.compile;
    let execution = &attribution.execution;
    let cache = &attribution.cache;

    println!(
        "journaled cached limit1 attribution: compile={} cache_key={} cache_lookup={} parse={} prepare={} lower={} bind={} plan_lookup={} planner={} store={} executor_invocation={} executor={} response_finalize={} execute={} response_decode={} total={} pure={:?} compiled_hits={} compiled_misses={} shared_hits={} shared_misses={}",
        attribution.compile_local_instructions,
        compile.cache_key_local_instructions,
        compile.cache_lookup_local_instructions,
        compile.parse_local_instructions,
        compile.prepare_local_instructions,
        compile.lower_local_instructions,
        compile.bind_local_instructions,
        attribution.plan_lookup_local_instructions,
        execution.planner_local_instructions,
        execution.store_local_instructions,
        execution.executor_invocation_local_instructions,
        execution.executor_local_instructions,
        execution.response_finalization_local_instructions,
        attribution.execute_local_instructions,
        attribution.response_decode_local_instructions,
        attribution.total_local_instructions,
        attribution.pure_covering,
        cache.sql_compiled_command_hits,
        cache.sql_compiled_command_misses,
        cache.shared_query_plan_hits,
        cache.shared_query_plan_misses,
    );
}

fn print_fluent_limit_one_attribution(label: &str, perf: &FluentQueryPerfResult) {
    let attribution = &perf.attribution;

    println!(
        "{label} fluent attributed limit1: compile={} compile_schema={} compile_info={} compile_prepare={} compile_key={} compile_lookup={} compile_plan={} compile_insert={} plan_lookup={} executor_invocation={} load_plan={} row_layout={} continuation={} handoff={} route_plan={} runtime_prepare={} runtime={} finalize={} response_finalize={} response_decode={} execute={} total={} shared_hits={} shared_misses={} direct={:?}",
        attribution.compile_local_instructions,
        attribution.compile_schema_catalog_local_instructions,
        attribution.compile_schema_info_local_instructions,
        attribution.compile_prepare_local_instructions,
        attribution.compile_cache_key_local_instructions,
        attribution.compile_cache_lookup_local_instructions,
        attribution.compile_plan_build_local_instructions,
        attribution.compile_cache_insert_local_instructions,
        attribution.plan_lookup_local_instructions,
        attribution.executor_invocation_local_instructions,
        attribution.load_plan_local_instructions,
        attribution.row_layout_local_instructions,
        attribution.continuation_signature_local_instructions,
        attribution.scalar_runtime_handoff_local_instructions,
        attribution.route_plan_local_instructions,
        attribution.runtime_prepare_local_instructions,
        attribution.runtime_local_instructions,
        attribution.finalize_local_instructions,
        attribution.response_finalization_local_instructions,
        attribution.response_decode_local_instructions,
        attribution.execute_local_instructions,
        attribution.total_local_instructions,
        attribution.shared_query_plan_cache_hits,
        attribution.shared_query_plan_cache_misses,
        attribution.direct_data_row,
    );
}

fn print_fluent_repeat_load_attribution(label: &str, perf: &FluentQueryPerfResult) {
    let attribution = &perf.attribution;

    println!(
        "{label} fluent repeat load: rows={} avg_compile={} avg_execute={} avg_total={} avg_data_gets={} avg_index_ranges={} avg_index_entries={} shared_hits={} shared_misses={}",
        perf.outcome.row_count,
        attribution.compile_local_instructions,
        attribution.execute_local_instructions,
        attribution.total_local_instructions,
        attribution.store_get_calls,
        attribution.index_store_range_scan_calls,
        attribution.index_store_entry_reads,
        attribution.shared_query_plan_cache_hits,
        attribution.shared_query_plan_cache_misses,
    );
}

fn assert_storage_primary_limit_one_stays_bounded(label: &str, perf: &SqlQueryPerfResult) {
    let outcome = summarize_perf_outcome(&perf.result);

    assert_eq!(
        outcome.row_count, 1,
        "{label} primary-key LIMIT 1 perf query should return one row",
    );
    assert!(
        perf.attribution.execution.store_local_instructions < 1_000_000,
        "{label} primary-key LIMIT 1 store phase should stay bounded, got {}",
        perf.attribution.execution.store_local_instructions,
    );
}

fn assert_cached_primary_limit_one_stays_bounded(
    label: &str,
    cached: &SqlQueryPerfResult,
    cold: &SqlQueryPerfResult,
) {
    assert_eq!(
        cached.attribution.cache.sql_compiled_command_hits, 1,
        "{label} cached LIMIT 1 should reuse the compiled SQL artifact",
    );
    assert_eq!(
        cached.attribution.cache.sql_compiled_command_misses, 0,
        "{label} cached LIMIT 1 should not recompile",
    );
    assert_eq!(
        cached.attribution.cache.shared_query_plan_hits, 1,
        "{label} cached LIMIT 1 should reuse the prepared query plan",
    );
    assert_eq!(
        cached.attribution.cache.shared_query_plan_misses, 0,
        "{label} cached LIMIT 1 should not rebuild the prepared query plan",
    );
    assert!(
        cached.attribution.compile_local_instructions < 500_000,
        "{label} cached LIMIT 1 should not reload/re-fingerprint accepted schema before cache hit, got {}",
        cached.attribution.compile_local_instructions,
    );
    assert!(
        cached.attribution.plan_lookup_local_instructions < 100_000,
        "{label} cached LIMIT 1 should not re-enter the expensive plan lookup path, got {}",
        cached.attribution.plan_lookup_local_instructions,
    );
    assert!(
        cached.attribution.total_local_instructions
            <= cold
                .attribution
                .total_local_instructions
                .saturating_mul(2)
                .saturating_div(3),
        "{label} cached LIMIT 1 should stay materially below cold query cost, cold={} cached={}",
        cold.attribution.total_local_instructions,
        cached.attribution.total_local_instructions,
    );
    assert!(
        cached
            .attribution
            .execution
            .executor_invocation_local_instructions
            < 1_000_000,
        "{label} cached LIMIT 1 should not re-run recovery/schema reconciliation in executor prep, got {}",
        cached
            .attribution
            .execution
            .executor_invocation_local_instructions,
    );
    assert!(
        cached.attribution.total_local_instructions < 1_000_000,
        "{label} cached LIMIT 1 should stay bounded after caches are warm, got {}",
        cached.attribution.total_local_instructions,
    );
}

fn query_journaled_total_only_limit_one_perf(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
) -> SqlTotalOnlyPerfResult {
    let result: Result<SqlTotalOnlyPerfResult, Error> = fixture
        .query_call("query_journaled_user_total_only_perf", (sql.to_string(),))
        .expect("journaled total-only LIMIT 1 perf query should decode");

    result.expect("journaled total-only LIMIT 1 perf query should succeed")
}

fn query_heap_total_only_limit_one_perf(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
) -> SqlTotalOnlyPerfResult {
    let result: Result<SqlTotalOnlyPerfResult, Error> = fixture
        .query_call("query_heap_user_total_only_perf", (sql.to_string(),))
        .expect("heap total-only LIMIT 1 perf query should decode");

    result.expect("heap total-only LIMIT 1 perf query should succeed")
}

fn assert_journaled_total_only_limit_one_variants_stay_bounded(
    fixture: &StandaloneCanisterFixture,
) {
    let total_only =
        query_journaled_total_only_limit_one_perf(fixture, JOURNALED_PRIMARY_LIMIT_ONE_SQL);
    println!(
        "journaled total-only limit1 attribution: total={}",
        total_only.instructions,
    );

    for (label, sql) in [
        (
            "journaled total-only id limit1",
            "SELECT id FROM PerfAuditJournaledUser ORDER BY id ASC LIMIT 1",
        ),
        (
            "journaled total-only name limit1",
            "SELECT name FROM PerfAuditJournaledUser ORDER BY id ASC LIMIT 1",
        ),
    ] {
        let variant = query_journaled_total_only_limit_one_perf(fixture, sql);
        println!("{label}: total={}", variant.instructions);
        assert!(
            variant.instructions < 1_000_000,
            "{label} should stay under the warmed LIMIT 1 budget, got {}",
            variant.instructions,
        );
    }
}

fn assert_heap_total_only_limit_one_variants_stay_bounded(fixture: &StandaloneCanisterFixture) {
    let total_only = query_heap_total_only_limit_one_perf(fixture, HEAP_PRIMARY_LIMIT_ONE_SQL);
    println!(
        "heap total-only limit1 attribution: total={}",
        total_only.instructions,
    );

    for (label, sql) in [
        (
            "heap total-only id limit1",
            "SELECT id FROM PerfAuditHeapUser ORDER BY id ASC LIMIT 1",
        ),
        (
            "heap total-only name limit1",
            "SELECT name FROM PerfAuditHeapUser ORDER BY id ASC LIMIT 1",
        ),
    ] {
        let variant = query_heap_total_only_limit_one_perf(fixture, sql);
        println!("{label}: total={}", variant.instructions);
        assert!(
            variant.instructions < 1_000_000,
            "{label} should stay under the warmed LIMIT 1 budget, got {}",
            variant.instructions,
        );
    }
}

fn query_journaled_fluent_total_only_limit_one_perf(
    fixture: &StandaloneCanisterFixture,
) -> FluentTotalOnlyPerfResult {
    let result: Result<FluentTotalOnlyPerfResult, Error> = fixture
        .query_call("query_journaled_user_fluent_total_only_perf", ())
        .expect("journaled fluent total-only LIMIT 1 perf query should decode");

    result.expect("journaled fluent total-only LIMIT 1 perf query should succeed")
}

fn measure_journaled_guarded_reentry_total_only_perf(
    fixture: &StandaloneCanisterFixture,
) -> FluentTotalOnlyPerfResult {
    let result: Result<FluentTotalOnlyPerfResult, Error> = fixture
        .update_call("measure_journaled_reentry_perf", ())
        .expect("journaled guarded reentry perf update should decode");

    result.expect("journaled guarded reentry perf update should succeed")
}

fn query_heap_fluent_total_only_limit_one_perf(
    fixture: &StandaloneCanisterFixture,
) -> FluentTotalOnlyPerfResult {
    let result: Result<FluentTotalOnlyPerfResult, Error> = fixture
        .query_call("query_heap_user_fluent_total_only_perf", ())
        .expect("heap fluent total-only LIMIT 1 perf query should decode");

    result.expect("heap fluent total-only LIMIT 1 perf query should succeed")
}

fn query_journaled_fluent_attributed_limit_one_perf(
    fixture: &StandaloneCanisterFixture,
) -> FluentQueryPerfResult {
    let result: Result<FluentQueryPerfResult, Error> = fixture
        .query_call("query_journaled_user_fluent_with_perf", ())
        .expect("journaled fluent attributed LIMIT 1 perf query should decode");

    result.expect("journaled fluent attributed LIMIT 1 perf query should succeed")
}

fn query_heap_fluent_attributed_limit_one_perf(
    fixture: &StandaloneCanisterFixture,
) -> FluentQueryPerfResult {
    let result: Result<FluentQueryPerfResult, Error> = fixture
        .query_call("query_heap_user_fluent_with_perf", ())
        .expect("heap fluent attributed LIMIT 1 perf query should decode");

    result.expect("heap fluent attributed LIMIT 1 perf query should succeed")
}

fn query_token_fluent_loop_with_perf(
    fixture: &StandaloneCanisterFixture,
    scenario: &str,
    query_loop_count: u32,
) -> FluentQueryPerfResult {
    let result: Result<FluentQueryPerfResult, Error> = fixture
        .query_call(
            "query_token_fluent_loop_with_perf",
            (scenario.to_string(), query_loop_count),
        )
        .expect("token fluent loop perf query should decode");

    result.expect("token fluent loop perf query should succeed")
}

fn assert_journaled_fluent_limit_one_reports(fixture: &StandaloneCanisterFixture) {
    let fluent_total = query_journaled_fluent_total_only_limit_one_perf(fixture);
    println!(
        "journaled fluent total-only limit1 attribution: total={}",
        fluent_total.instructions,
    );

    let fluent_attributed = query_journaled_fluent_attributed_limit_one_perf(fixture);
    print_fluent_limit_one_attribution("journaled", &fluent_attributed);
}

fn assert_heap_fluent_limit_one_reports(fixture: &StandaloneCanisterFixture) {
    let fluent_total = query_heap_fluent_total_only_limit_one_perf(fixture);
    println!(
        "heap fluent total-only limit1 attribution: total={}",
        fluent_total.instructions,
    );

    let fluent_attributed = query_heap_fluent_attributed_limit_one_perf(fixture);
    print_fluent_limit_one_attribution("heap", &fluent_attributed);
}

fn measure_storage_write_matrix(
    fixture: &StandaloneCanisterFixture,
    method: &str,
    label: &str,
) -> StorageWritePerfResult {
    let result: Result<StorageWritePerfResult, Error> = fixture
        .update_call(method, ())
        .unwrap_or_else(|err| panic!("{label} write matrix perf result should decode: {err}"));

    result.unwrap_or_else(|err| panic!("{label} write matrix perf endpoint should succeed: {err}"))
}

fn print_storage_write_matrix(label: &str, result: &StorageWritePerfResult) {
    println!(
        "{label} write matrix: first_insert={} steady_insert_avg={} steady_update_avg={} steady_delete_avg={} write_then_read_back={} read_back_rows={}",
        result.first_insert_local_instructions,
        result.steady_insert_avg_local_instructions,
        result.steady_update_avg_local_instructions,
        result.steady_delete_avg_local_instructions,
        result.write_then_read_back_local_instructions,
        result.read_back_rows,
    );
}

fn assert_storage_write_matrix_stays_bounded(label: &str, result: &StorageWritePerfResult) {
    assert_eq!(
        result.read_back_rows, 1,
        "{label} write-then-read-back should return exactly one row",
    );

    for (metric, instructions, budget) in [
        (
            "first insert",
            result.first_insert_local_instructions,
            30_000_000,
        ),
        (
            "steady insert avg",
            result.steady_insert_avg_local_instructions,
            25_000_000,
        ),
        (
            "steady update avg",
            result.steady_update_avg_local_instructions,
            25_000_000,
        ),
        (
            "steady delete avg",
            result.steady_delete_avg_local_instructions,
            150_000_000,
        ),
        (
            "write then read back",
            result.write_then_read_back_local_instructions,
            100_000_000,
        ),
    ] {
        assert!(
            instructions < budget,
            "{label} {metric} should stay bounded, got {instructions} >= {budget}",
        );
    }
}

fn assert_storage_write_matrix_reports(fixture: &StandaloneCanisterFixture) {
    let heap = measure_storage_write_matrix(fixture, "measure_heap_user_write_matrix_perf", "heap");
    let journaled = measure_storage_write_matrix(
        fixture,
        "measure_journaled_user_write_matrix_perf",
        "journaled",
    );

    print_storage_write_matrix("heap", &heap);
    print_storage_write_matrix("journaled", &journaled);

    assert_storage_write_matrix_stays_bounded("heap", &heap);
    assert_storage_write_matrix_stays_bounded("journaled", &journaled);
}

fn measure_sql_write_materialization_matrix(
    fixture: &StandaloneCanisterFixture,
    method: &str,
    label: &str,
) -> SqlWriteMaterializationPerfResult {
    let result: Result<SqlWriteMaterializationPerfResult, Error> =
        fixture.update_call(method, ()).unwrap_or_else(|err| {
            panic!("{label} SQL write materialization result should decode: {err}")
        });

    result.unwrap_or_else(|err| {
        panic!("{label} SQL write materialization endpoint should succeed: {err}")
    })
}

fn print_sql_write_materialization_matrix(label: &str, result: &SqlWriteMaterializationPerfResult) {
    println!(
        "{label} SQL write materialization: update_count={} update_returning={} delete_count={} delete_returning={} rows=[{},{},{},{}]",
        result.local_instructions[0],
        result.local_instructions[1],
        result.local_instructions[2],
        result.local_instructions[3],
        result.rows[0],
        result.rows[1],
        result.rows[2],
        result.rows[3],
    );
}

fn assert_sql_write_materialization_matrix_stays_bounded(
    label: &str,
    result: &SqlWriteMaterializationPerfResult,
) {
    for (metric, rows) in SQL_WRITE_MATERIALIZATION_METRICS
        .iter()
        .copied()
        .zip(result.rows)
    {
        assert_eq!(
            rows, 32,
            "{label} {metric} should cover the broad fixture window"
        );
    }

    for (metric, instructions) in SQL_WRITE_MATERIALIZATION_METRICS
        .iter()
        .copied()
        .zip(result.local_instructions)
    {
        assert!(
            instructions < SQL_WRITE_MATERIALIZATION_BUDGET,
            "{label} SQL write materialization {metric} should stay bounded, got {instructions} >= {SQL_WRITE_MATERIALIZATION_BUDGET}",
        );
    }
}

fn assert_sql_write_materialization_matrix_reports(fixture: &StandaloneCanisterFixture) {
    let heap = measure_sql_write_materialization_matrix(
        fixture,
        "measure_heap_user_sql_write_materialization_perf",
        "heap",
    );
    let journaled = measure_sql_write_materialization_matrix(
        fixture,
        "measure_journaled_user_sql_write_materialization_perf",
        "journaled",
    );

    print_sql_write_materialization_matrix("heap", &heap);
    print_sql_write_materialization_matrix("journaled", &journaled);

    assert_sql_write_materialization_matrix_stays_bounded("heap", &heap);
    assert_sql_write_materialization_matrix_stays_bounded("journaled", &journaled);
}

fn measure_resumable_update(fixture: &StandaloneCanisterFixture) -> ResumableUpdatePerfResult {
    let result: Result<ResumableUpdatePerfResult, Error> = fixture
        .update_call("measure_journaled_user_resumable_update_perf", ())
        .expect("resumable update perf result should decode");

    result.expect("resumable update perf endpoint should succeed")
}

fn assert_resumable_update_perf_stays_bounded(result: &ResumableUpdatePerfResult) {
    assert!(result.prepare_local_instructions > 0);
    assert_eq!(result.forward_local_instructions.len(), 8);
    assert_eq!(result.verify_local_instructions.len(), 2);
    assert_eq!(result.forward_keys_scanned, 512);
    assert_eq!(result.verify_keys_scanned, 512);
    assert_eq!(result.rows_updated, 512);
    for instructions in result
        .forward_local_instructions
        .iter()
        .chain(&result.verify_local_instructions)
    {
        assert!(
            *instructions < RESUMABLE_UPDATE_STEP_BUDGET,
            "resumable UPDATE step should stay bounded, got {instructions} >= {RESUMABLE_UPDATE_STEP_BUDGET}",
        );
    }
}

struct StorageLimitOneReadSamples {
    heap: SqlQueryPerfResult,
    journaled: SqlQueryPerfResult,
}

fn assert_storage_cold_limit_one_reports(
    fixture: &StandaloneCanisterFixture,
) -> StorageLimitOneReadSamples {
    let heap = query_sql_limit_one_with_perf(
        fixture,
        "query_heap_user_with_perf",
        HEAP_PRIMARY_LIMIT_ONE_SQL,
        "heap primary LIMIT 1 perf query should decode",
        "heap primary LIMIT 1 perf query should succeed",
    );
    let journaled = query_sql_limit_one_with_perf(
        fixture,
        "query_journaled_user_with_perf",
        JOURNALED_PRIMARY_LIMIT_ONE_SQL,
        "journaled primary LIMIT 1 perf query should decode",
        "journaled primary LIMIT 1 perf query should succeed",
    );

    print_sql_limit_one_attribution("heap limit1 attribution", &heap);
    print_sql_limit_one_attribution("journaled limit1 attribution", &journaled);
    print_storage_read_comparison("heap vs journaled primary LIMIT 1", &heap, &journaled);
    assert_storage_primary_limit_one_stays_bounded("heap", &heap);
    assert_storage_primary_limit_one_stays_bounded("journaled", &journaled);

    StorageLimitOneReadSamples { heap, journaled }
}

fn assert_heap_cached_limit_one_reports(
    fixture: &StandaloneCanisterFixture,
    heap: &SqlQueryPerfResult,
) {
    warm_sql_limit_one_with_perf(
        fixture,
        "warm_heap_user_query_with_perf",
        HEAP_PRIMARY_LIMIT_ONE_SQL,
        "heap warm LIMIT 1 perf query should decode",
        "heap warm LIMIT 1 perf query should succeed",
    );
    let cached_heap = query_sql_limit_one_with_perf(
        fixture,
        "query_heap_user_with_perf",
        HEAP_PRIMARY_LIMIT_ONE_SQL,
        "heap cached LIMIT 1 perf query should decode",
        "heap cached LIMIT 1 perf query should succeed",
    );
    print_sql_limit_one_attribution("heap cached limit1 attribution", &cached_heap);
    assert_cached_primary_limit_one_stays_bounded("heap", &cached_heap, heap);

    let heap_looped = query_sql_loop_limit_one_with_perf(
        fixture,
        "query_heap_user_loop_with_perf",
        HEAP_PRIMARY_LIMIT_ONE_SQL,
        10,
        "heap loop LIMIT 1 perf query should decode",
        "heap loop LIMIT 1 perf query should succeed",
    );
    print_sql_limit_one_attribution("heap loop limit1 attribution", &heap_looped);
}

fn assert_journaled_cached_limit_one_reports(
    fixture: &StandaloneCanisterFixture,
    journaled: &SqlQueryPerfResult,
) {
    warm_sql_limit_one_with_perf(
        fixture,
        "warm_journaled_user_query_with_perf",
        JOURNALED_PRIMARY_LIMIT_ONE_SQL,
        "journaled warm LIMIT 1 perf query should decode",
        "journaled warm LIMIT 1 perf query should succeed",
    );
    let cached = query_sql_limit_one_with_perf(
        fixture,
        "query_journaled_user_with_perf",
        JOURNALED_PRIMARY_LIMIT_ONE_SQL,
        "journaled cached LIMIT 1 perf query should decode",
        "journaled cached LIMIT 1 perf query should succeed",
    );
    print_cached_journaled_sql_limit_one_attribution(&cached);
    assert_cached_primary_limit_one_stays_bounded("journaled", &cached, journaled);

    let looped = query_sql_loop_limit_one_with_perf(
        fixture,
        "query_journaled_user_loop_with_perf",
        JOURNALED_PRIMARY_LIMIT_ONE_SQL,
        10,
        "journaled loop LIMIT 1 perf query should decode",
        "journaled loop LIMIT 1 perf query should succeed",
    );
    print_sql_limit_one_attribution("journaled loop limit1 attribution", &looped);
}

fn assert_storage_total_and_fluent_limit_one_reports(fixture: &StandaloneCanisterFixture) {
    assert_heap_total_only_limit_one_variants_stay_bounded(fixture);
    assert_journaled_total_only_limit_one_variants_stay_bounded(fixture);
    assert_heap_fluent_limit_one_reports(fixture);
    assert_journaled_fluent_limit_one_reports(fixture);
}

fn assert_journaled_guarded_reentry_perf_stays_bounded(
    label: &str,
    perf: &FluentTotalOnlyPerfResult,
) {
    assert_eq!(
        perf.row_count, 1,
        "{label} guarded reentry probe should return one journaled row",
    );
    assert!(
        perf.instructions > 0,
        "{label} guarded reentry probe should report positive instructions",
    );
    assert!(
        perf.instructions < JOURNALED_UPGRADE_REENTRY_BUDGET,
        "{label} guarded reentry probe should stay below the regression budget, got {} >= {}",
        perf.instructions,
        JOURNALED_UPGRADE_REENTRY_BUDGET,
    );
}

#[test]
fn sql_perf_update_warm_persists_compiled_and_shared_cache_across_calls() {
    let fixture = install_sql_perf_canister_fixture();

    for case in [
        WarmCacheContractCase {
            scenario_key: "user.pk.order_only.asc.limit1.warm_after_update",
            surface: SqlPerfSurface::User,
            sql: "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
        },
        WarmCacheContractCase {
            scenario_key: "user.pk.order_only.asc.limit2.warm_after_update",
            surface: SqlPerfSurface::User,
            sql: "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
        },
        WarmCacheContractCase {
            scenario_key: "user.name.lower.order_only.asc.limit3.warm_after_update",
            surface: SqlPerfSurface::User,
            sql: "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
        },
        WarmCacheContractCase {
            scenario_key: "user.age.order_only.asc.limit2.warm_after_update",
            surface: SqlPerfSurface::User,
            sql: "SELECT id, age FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 2",
        },
        WarmCacheContractCase {
            scenario_key: "user.grouped.age_count.limit10.warm_after_update",
            surface: SqlPerfSurface::User,
            sql: "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
        },
        WarmCacheContractCase {
            scenario_key: "user.grouped.case_sum.having_alias.order.limit5.warm_after_update",
            surface: SqlPerfSurface::User,
            sql: "SELECT age, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS high_count FROM PerfAuditUser GROUP BY age HAVING high_count > 0 ORDER BY high_count DESC, age ASC LIMIT 5",
        },
        WarmCacheContractCase {
            scenario_key: "account.active.lower.order_handle.asc.limit3.warm_after_update",
            surface: SqlPerfSurface::Account,
            sql: "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
        },
    ] {
        reset_sql_perf_fixtures(&fixture);
        assert_update_warm_persists_compiled_and_shared_cache_path(&fixture, case);
    }
}

#[test]
fn sql_perf_journaled_primary_limit_one_stays_bounded() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let read_samples = assert_storage_cold_limit_one_reports(&fixture);
    assert_heap_cached_limit_one_reports(&fixture, &read_samples.heap);
    assert_journaled_cached_limit_one_reports(&fixture, &read_samples.journaled);
    assert_storage_total_and_fluent_limit_one_reports(&fixture);
    assert_storage_write_matrix_reports(&fixture);
    assert_sql_write_materialization_matrix_reports(&fixture);
}

#[test]
fn sql_perf_journaled_check_write_cost_is_measured() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let result: Result<ConstraintActivationPerfResult, Error> = fixture
        .update_call("measure_journaled_user_constraint_write_perf", ())
        .expect("constraint write perf result should decode");
    let result = result.expect("constraint write perf endpoint should succeed");
    let checked: Result<StorageWritePerfResult, Error> = fixture
        .update_call("measure_journaled_user_checked_write_perf", ())
        .expect("checked write perf result should decode");
    let checked = checked.expect("checked write perf endpoint should succeed");

    print_storage_write_matrix("journaled no-check", &result.no_check);
    print_storage_write_matrix("journaled checked", &checked);
    println!(
        "journaled ADD CHECK: instructions={} rows_scanned={}",
        result.add_check_local_instructions, result.add_check_rows_scanned,
    );
    assert_eq!(result.add_check_rows_scanned, 0);
    assert!(result.add_check_local_instructions > 0);
    for (metric, without_check, with_check) in [
        (
            "steady insert",
            result.no_check.steady_insert_avg_local_instructions,
            checked.steady_insert_avg_local_instructions,
        ),
        (
            "steady update",
            result.no_check.steady_update_avg_local_instructions,
            checked.steady_update_avg_local_instructions,
        ),
        (
            "steady delete",
            result.no_check.steady_delete_avg_local_instructions,
            checked.steady_delete_avg_local_instructions,
        ),
        (
            "write then read back",
            result.no_check.write_then_read_back_local_instructions,
            checked.write_then_read_back_local_instructions,
        ),
    ] {
        let limit = without_check.saturating_mul(105) / 100;
        assert!(
            with_check <= limit,
            "journaled {metric} check overhead should stay within 5%, got {with_check} > {limit} from {without_check}",
        );
    }
}

#[test]
fn sql_perf_resumable_update_steps_stay_bounded() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let result = measure_resumable_update(&fixture);
    println!(
        "resumable UPDATE: prepare={} forward={:?} verify={:?} forward_keys={} verify_keys={} updated={}",
        result.prepare_local_instructions,
        result.forward_local_instructions,
        result.verify_local_instructions,
        result.forward_keys_scanned,
        result.verify_keys_scanned,
        result.rows_updated,
    );
    assert_resumable_update_perf_stays_bounded(&result);
}

#[test]
fn sql_perf_journaled_upgrade_guarded_reentry_stays_bounded() {
    let fixture = install_sql_perf_canister_fixture();
    load_journaled_reentry_probe_fixture(&fixture);

    upgrade_fixture_canister(&fixture, "sql_perf");

    let first = measure_journaled_guarded_reentry_total_only_perf(&fixture);
    let second = measure_journaled_guarded_reentry_total_only_perf(&fixture);

    println!(
        "journaled guarded reentry after upgrade: first_total={} second_total={} first_rows={} second_rows={}",
        first.instructions, second.instructions, first.row_count, second.row_count,
    );

    assert_journaled_guarded_reentry_perf_stays_bounded("first", &first);
    assert_journaled_guarded_reentry_perf_stays_bounded("second", &second);
}

#[test]
fn sql_perf_repeated_query_contracts_keep_compiled_and_shared_cache_path() {
    let fixture = install_sql_perf_canister_fixture();

    // Every repeated call should keep the same compiled-plus-shared cache path,
    // including guarded, grouped, DISTINCT, CASE, and expression-order variants.
    for scenario in repeated_query_scenarios() {
        reset_sql_perf_fixtures(&fixture);
        assert_repeat_scenario_keeps_compiled_and_shared_cache_path(&fixture, scenario);
    }
}

#[test]
fn sql_perf_repeated_same_indexed_load_reports_full_reload_pressure() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let repeat = query_token_fluent_loop_with_perf(
        &fixture,
        TOKEN_COLLECTION_FULL_ENTITY_FLUENT_SCENARIO,
        TOKEN_COLLECTION_REPEAT_LOAD_RUNS,
    );
    print_fluent_repeat_load_attribution("token.collection_id full entity", &repeat);
    let attribution = &repeat.attribution;
    let repeated_hits = u64::from(TOKEN_COLLECTION_REPEAT_LOAD_RUNS.saturating_sub(1));

    assert_eq!(
        repeat.outcome.result_kind, "rows",
        "repeated collection load should return full entity rows",
    );
    assert_eq!(
        repeat.outcome.entity, "PerfAuditToken",
        "repeated collection load should target the token fixture",
    );
    assert_eq!(
        repeat.outcome.row_count, TOKEN_COLLECTION_FULL_ENTITY_ROWS,
        "repeated collection load should cover the Toko-sized target collection",
    );
    assert_eq!(
        attribution.shared_query_plan_cache_hits, repeated_hits,
        "same-call repeated collection load should reuse the prepared plan after the first pass",
    );
    assert_eq!(
        attribution.shared_query_plan_cache_misses, 1,
        "same-call repeated collection load should cold-fill the prepared plan once",
    );
    assert_eq!(
        attribution.index_store_range_scan_calls, 1,
        "each averaged repeated collection load should still perform one indexed range traversal",
    );
    assert!(
        attribution.index_store_entry_reads >= u64::from(TOKEN_COLLECTION_FULL_ENTITY_ROWS),
        "each averaged repeated collection load should still walk the full target collection index, got {} entries",
        attribution.index_store_entry_reads,
    );
    assert!(
        attribution.store_get_calls >= u64::from(TOKEN_COLLECTION_FULL_ENTITY_ROWS),
        "each averaged repeated collection load should still hydrate the full target collection, got {} row reads",
        attribution.store_get_calls,
    );
}

#[test]
fn sql_perf_membership_queries_report_compile_subphase_breakdown() {
    let fixture = install_sql_perf_canister_fixture();

    for (scenario_key, sql) in [
        (
            "user.age.in.limit3",
            "SELECT id, age FROM PerfAuditUser WHERE age IN (24, 31, 43) ORDER BY age ASC, id ASC LIMIT 3",
        ),
        (
            "user.age.not_in.limit3",
            "SELECT id, age FROM PerfAuditUser WHERE age NOT IN (24, 31, 43) ORDER BY id ASC LIMIT 3",
        ),
    ] {
        reset_sql_perf_fixtures(&fixture);
        let perf =
            query_surface_with_perf(&fixture, SqlPerfSurface::User, sql, 1).unwrap_or_else(|err| {
                panic!("membership scenario '{scenario_key}' should succeed: {err}")
            });

        println!(
            "{scenario_key}: compile={} key={} lookup={} parse={} tokenize={} select={} expr={} predicate={} agg_check={} prepare={} lower={} bind={} cache_insert={} execute={} total={}",
            perf.attribution.compile_local_instructions,
            perf.attribution.compile.cache_key_local_instructions,
            perf.attribution.compile.cache_lookup_local_instructions,
            perf.attribution.compile.parse_local_instructions,
            perf.attribution.compile.parse_tokenize_local_instructions,
            perf.attribution.compile.parse_select_local_instructions,
            perf.attribution.compile.parse_expr_local_instructions,
            perf.attribution.compile.parse_predicate_local_instructions,
            perf.attribution
                .compile
                .aggregate_lane_check_local_instructions,
            perf.attribution.compile.prepare_local_instructions,
            perf.attribution.compile.lower_local_instructions,
            perf.attribution.compile.bind_local_instructions,
            perf.attribution.compile.cache_insert_local_instructions,
            perf.attribution.execute_local_instructions,
            perf.attribution.total_local_instructions,
        );

        assert!(
            perf.attribution.compile_local_instructions > 0,
            "membership scenario '{scenario_key}' should report positive compile cost",
        );
        assert_eq!(
            perf.attribution.compile.parse_local_instructions,
            perf.attribution
                .compile
                .parse_tokenize_local_instructions
                .saturating_add(perf.attribution.compile.parse_select_local_instructions)
                .saturating_add(perf.attribution.compile.parse_expr_local_instructions)
                .saturating_add(perf.attribution.compile.parse_predicate_local_instructions),
            "membership scenario '{scenario_key}' should keep parse subphases exhaustive",
        );
    }
}

#[test]
fn sql_perf_blob_metadata_query_stays_on_covering_index() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let explain = query_surface_with_perf(
        &fixture,
        SqlPerfSurface::Blob,
        "EXPLAIN EXECUTION SELECT id, label, bucket \
         FROM PerfAuditBlob \
         WHERE bucket = 10 \
         ORDER BY bucket ASC, label ASC, id ASC \
         LIMIT 3",
        1,
    )
    .expect("blob scalar metadata EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain { explain, .. } = explain.result else {
        panic!("blob scalar metadata EXPLAIN EXECUTION should return explain output");
    };

    let perf = query_surface_with_perf(
        &fixture,
        SqlPerfSurface::Blob,
        "SELECT id, label, bucket \
         FROM PerfAuditBlob \
         WHERE bucket = 10 \
         ORDER BY bucket ASC, label ASC, id ASC \
         LIMIT 3",
        1,
    )
    .expect("blob scalar metadata query should succeed");

    assert_eq!(
        perf.attribution.store_get_calls, 0,
        "blob scalar metadata query should stay on the covering index and avoid row-store get() calls: {explain}",
    );
    assert!(
        perf.attribution.pure_covering.is_some(),
        "blob scalar metadata query should report the pure covering attribution lane",
    );
    assert_eq!(
        perf.attribution.output_blob.projected_bytes, 0,
        "blob scalar metadata query should not project blob payload bytes",
    );
}

#[test]
fn sql_perf_token_branch_set_page_is_bounded_and_page_only() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let explain = query_surface_with_perf(
        &fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_PAGE_SQL}").as_str(),
        1,
    )
    .expect("token branch-set EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain { explain, .. } = explain.result else {
        panic!("token branch-set EXPLAIN EXECUTION should return explain output");
    };

    assert!(
        explain.contains("IndexBranchSet"),
        "token branch-set EXPLAIN should expose the branch-aware route: {explain}",
    );
    assert!(
        !explain.contains("OrderByMaterializedSort"),
        "token branch-set EXPLAIN must not materialize-sort the page route: {explain}",
    );

    for (scenario_key, min_store_gets, max_store_gets) in [
        (
            "token.collection_stage_id.branch_set.page_only.limit3",
            0,
            0,
        ),
        (
            "token.collection_stage_id.branch_set.noncovered_page_only.limit3",
            3,
            4,
        ),
    ] {
        let sample = sample_perf_scenario(&fixture, sql_perf_scenario_by_key(scenario_key));
        print_branch_set_perf_sample("page-only", &sample);

        assert_eq!(
            sample.outcome.result_kind, "projection",
            "token branch-set audit row '{scenario_key}' should remain a page/projection query, not count",
        );
        assert_eq!(
            sample.outcome.row_count, 3,
            "token branch-set audit row '{scenario_key}' should return the requested page size",
        );
        assert!(
            (min_store_gets..=max_store_gets).contains(&sample.data_store_get_calls),
            "token branch-set audit row '{scenario_key}' should keep row-store get() calls bounded, got {}: {explain}",
            sample.data_store_get_calls,
        );
        assert!(
            sample.index_store_entry_reads <= 8,
            "token branch-set audit row '{scenario_key}' should keep index traversal bounded by branch fetch, got {}: {explain}",
            sample.index_store_entry_reads,
        );
        assert_eq!(
            sample.grouped_count_row_materialization_local_instructions, 0,
            "token branch-set default page query '{scenario_key}' must not invoke grouped/count materialization",
        );
        assert_eq!(
            sample.grouped_count_group_lookup_local_instructions, 0,
            "token branch-set default page query '{scenario_key}' must not invoke grouped/count lookup work",
        );
    }
}

#[test]
fn sql_perf_token_branch_set_limit50_pressure_beats_overcap_fallback() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    assert_token_branch_set_limit50_explain_contract(&fixture);
    assert_token_branch_set_limit50_fallback_rows_match(&fixture);

    let branch = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_stage_id.branch_set.page_only.limit50"),
    );
    let wide_branch = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_stage_id.branch_set.wide_page_only.limit50"),
    );
    let fallback = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_stage_id.overcap_fallback.page_only.limit50"),
    );
    let large_in_fallback = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_stage_id.large_in_fallback.page_only.limit50"),
    );
    let sparse_collection_in = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_id.sparse_in.page_only.limit50"),
    );
    print_branch_set_perf_sample("limit50 branch", &branch);
    print_branch_set_perf_sample("limit50 wide branch", &wide_branch);
    print_branch_set_perf_sample("limit50 overcap fallback", &fallback);
    print_branch_set_perf_sample("limit50 large IN fallback", &large_in_fallback);
    print_branch_set_perf_sample("limit50 sparse collection IN", &sparse_collection_in);
    let execute_delta = i128::from(fallback.execute_local_instructions)
        - i128::from(branch.execute_local_instructions);
    let total_delta =
        i128::from(fallback.local_instructions) - i128::from(branch.local_instructions);
    println!("branch-set perf limit50 saved: execute={execute_delta} total={total_delta}");

    assert_token_branch_set_limit50_pressure_contract(
        &branch,
        &wide_branch,
        &fallback,
        &large_in_fallback,
        &sparse_collection_in,
    );
}

fn assert_token_branch_set_limit50_pressure_contract(
    branch: &SqlPerfScenarioSample,
    wide_branch: &SqlPerfScenarioSample,
    fallback: &SqlPerfScenarioSample,
    large_in_fallback: &SqlPerfScenarioSample,
    sparse_collection_in: &SqlPerfScenarioSample,
) {
    assert_eq!(
        branch.outcome.row_count, 50,
        "branch-set LIMIT 50 pressure query should return the requested page size",
    );
    assert_eq!(
        fallback.outcome, branch.outcome,
        "over-cap fallback comparator should return the same page result as the branch route",
    );
    assert_eq!(
        large_in_fallback.outcome, branch.outcome,
        "large-IN fallback comparator should return the same page result as the branch route",
    );
    assert_eq!(
        wide_branch.outcome, branch.outcome,
        "wide branch-set comparator should return the same page result as the small branch route",
    );
    assert_eq!(
        branch.data_store_get_calls, 0,
        "covered branch-set LIMIT 50 pressure query should avoid row-store get() calls",
    );
    assert_eq!(
        wide_branch.data_store_get_calls, 0,
        "covered wide branch-set LIMIT 50 pressure query should avoid row-store get() calls",
    );
    assert!(
        branch.index_store_entry_reads <= 128,
        "branch-set LIMIT 50 should keep index traversal bounded by the merged page, got {}",
        branch.index_store_entry_reads,
    );
    assert!(
        branch.execute_local_instructions < fallback.execute_local_instructions,
        "branch-set LIMIT 50 should execute cheaper than the over-cap fallback; branch={} fallback={}",
        branch.execute_local_instructions,
        fallback.execute_local_instructions,
    );
    assert!(
        wide_branch.execute_local_instructions < fallback.execute_local_instructions,
        "wide branch-set LIMIT 50 should execute cheaper than the over-cap fallback; wide={} fallback={}",
        wide_branch.execute_local_instructions,
        fallback.execute_local_instructions,
    );
    assert_eq!(
        large_in_fallback.data_store_get_calls, 0,
        "covered large-IN fallback should avoid row-store get() calls",
    );
    assert!(
        large_in_fallback.index_store_entry_reads <= 320,
        "large-IN fallback should stay bounded to the fixed collection prefix, got {}",
        large_in_fallback.index_store_entry_reads,
    );
    assert_eq!(
        sparse_collection_in.outcome.row_count, 50,
        "sparse collection IN audit row should return the requested page size",
    );
    assert!(
        sparse_collection_in.index_store_range_scan_calls <= 16,
        "sparse collection IN should expand only bounded non-empty child prefixes, got {} range scans",
        sparse_collection_in.index_store_range_scan_calls,
    );
    assert!(
        sparse_collection_in.index_store_entry_reads <= 128,
        "sparse collection IN should read bounded child-prefix entries, got {}",
        sparse_collection_in.index_store_entry_reads,
    );
}

fn assert_branch_set_count_sample_uses_prefix_cardinality(
    label: &str,
    sample: &SqlPerfScenarioSample,
) {
    assert_eq!(
        sample.outcome.result_kind, "projection",
        "{label} branch-set COUNT audit row should return SQL projection output",
    );
    assert_eq!(
        sample.outcome.row_count, 1,
        "{label} branch-set COUNT audit row should return one aggregate row",
    );
    assert_eq!(
        sample.data_store_get_calls, 0,
        "{label} branch COUNT should avoid row-store get() calls",
    );
    assert_eq!(
        sample.index_store_entry_reads, 0,
        "{label} branch COUNT should use prefix-cardinality metadata without scanning index entries",
    );
}

#[test]
fn sql_perf_token_branch_set_changed_queries_stay_bounded() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    assert_token_branch_set_index_residual_explain_contract(&fixture);

    let residual = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key(
            "token.collection_stage_id.branch_set.index_residual_covering.limit3",
        ),
    );
    print_branch_set_perf_sample("index residual covering", &residual);
    assert_eq!(
        residual.outcome.result_kind, "projection",
        "branch-set index-residual audit row should remain a projection page query",
    );
    assert_eq!(
        residual.outcome.row_count, 3,
        "branch-set index-residual audit row should return the requested page size",
    );
    assert_eq!(
        residual.data_store_get_calls, 0,
        "index-residual covered branch query should stay row-store-free",
    );
    assert!(
        residual.index_store_entry_reads <= 16,
        "index-residual branch query should keep index traversal bounded by lazy branch heads, got {}",
        residual.index_store_entry_reads,
    );
    assert_eq!(
        residual.grouped_count_row_materialization_local_instructions, 0,
        "index-residual page query must not invoke grouped/count materialization",
    );
    assert_eq!(
        residual.grouped_count_group_lookup_local_instructions, 0,
        "index-residual page query must not invoke grouped/count lookup work",
    );

    let count = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_stage_id.branch_set.count"),
    );
    print_branch_set_perf_sample("count", &count);
    assert_branch_set_count_sample_uses_prefix_cardinality("plain", &count);

    let duplicate_count = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key("token.collection_stage_id.branch_set.duplicate_count"),
    );
    print_branch_set_perf_sample("duplicate count", &duplicate_count);
    assert_branch_set_count_sample_uses_prefix_cardinality("duplicate", &duplicate_count);

    let duplicate_count_query = query_surface_with_perf(
        &fixture,
        SqlPerfSurface::Token,
        TOKEN_BRANCH_SET_DUPLICATE_COUNT_SQL,
        1,
    )
    .expect("token duplicate-literal branch COUNT should succeed");
    let scalar_aggregate = duplicate_count_query
        .attribution
        .scalar_aggregate
        .expect("duplicate branch COUNT should report scalar aggregate attribution");
    assert_eq!(
        scalar_aggregate.sink_mode.as_deref(),
        Some("IndexPrefixCardinality"),
        "duplicate branch COUNT should attribute the metadata-backed terminal source",
    );
    assert_eq!(
        scalar_aggregate.rows_ingested, 0,
        "duplicate branch COUNT should not ingest rows through the buffered reducer",
    );
    assert_eq!(
        scalar_aggregate.terminal_count, 1,
        "duplicate branch COUNT should report one scalar aggregate terminal",
    );
}

#[test]
fn sql_perf_token_hybrid_covering_hotspot_counters_are_attributed() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let explain = query_surface_with_perf(
        &fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_OVERCAP_FALLBACK_NONCOVERED_LIMIT50_SQL}")
            .as_str(),
        1,
    )
    .expect("token hybrid over-cap EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain { explain, .. } = explain.result else {
        panic!("token hybrid over-cap EXPLAIN EXECUTION should return explain output");
    };
    assert!(
        explain.contains("cov_read_kind=Text(\"hybrid_covering\")"),
        "hybrid over-cap EXPLAIN should expose the hybrid covering route kind: {explain}",
    );
    assert!(
        explain.contains("covering_kind=Text(\"hybrid_covering\")"),
        "hybrid over-cap EXPLAIN should expose the hybrid covering terminal: {explain}",
    );
    assert!(
        explain.contains("existing_row_mode=Text(\"planner_proven\")"),
        "hybrid over-cap route should keep the planner-proven row-presence contract visible: {explain}",
    );

    let sample = sample_perf_scenario(
        &fixture,
        sql_perf_scenario_by_key(
            "token.collection_stage_id.overcap_fallback.noncovered_page_only.limit50",
        ),
    );
    print_branch_set_perf_sample("overcap hybrid covering", &sample);

    assert_eq!(
        sample.outcome.result_kind, "projection",
        "hybrid over-cap audit row should remain a projection page query",
    );
    assert_eq!(
        sample.outcome.row_count, 50,
        "hybrid over-cap audit row should return the requested page size",
    );
    assert_eq!(
        sample.hybrid_covering_path_hits, 1,
        "hybrid over-cap audit row should report the hybrid covering path",
    );
    assert_eq!(
        sample.hybrid_covering_row_field_accesses, 50,
        "hybrid over-cap audit row should read one row-backed field per returned row",
    );
    assert_eq!(
        sample.data_store_get_calls, 50,
        "hybrid over-cap audit row should hydrate only returned rows after filtering, sorting, and windowing",
    );
    assert!(
        sample.index_store_entry_reads > sample.data_store_get_calls,
        "hybrid over-cap audit row should still attribute the pre-window index scan separately",
    );
}

fn assert_token_branch_set_limit50_explain_contract(fixture: &StandaloneCanisterFixture) {
    let branch_explain = query_surface_with_perf(
        fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_PAGE_LIMIT50_SQL}").as_str(),
        1,
    )
    .expect("token branch-set LIMIT 50 EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain {
        explain: branch_explain,
        ..
    } = branch_explain.result
    else {
        panic!("token branch-set LIMIT 50 EXPLAIN EXECUTION should return explain output");
    };
    assert!(
        branch_explain.contains("IndexBranchSet"),
        "token branch-set LIMIT 50 EXPLAIN should expose the branch-aware route: {branch_explain}",
    );
    assert!(
        !branch_explain.contains("OrderByMaterializedSort"),
        "token branch-set LIMIT 50 EXPLAIN must not materialize-sort the page route: {branch_explain}",
    );

    let wide_branch_explain = query_surface_with_perf(
        fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_WIDE_PAGE_LIMIT50_SQL}").as_str(),
        1,
    )
    .expect("token wide branch-set LIMIT 50 EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain {
        explain: wide_branch_explain,
        ..
    } = wide_branch_explain.result
    else {
        panic!("token wide branch-set LIMIT 50 EXPLAIN EXECUTION should return explain output");
    };
    assert!(
        wide_branch_explain.contains("IndexBranchSet"),
        "token wide branch-set LIMIT 50 EXPLAIN should expose the branch-aware route: {wide_branch_explain}",
    );
    assert!(
        !wide_branch_explain.contains("OrderByMaterializedSort"),
        "token wide branch-set LIMIT 50 EXPLAIN must not materialize-sort the page route: {wide_branch_explain}",
    );

    let fallback_explain = query_surface_with_perf(
        fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_OVERCAP_FALLBACK_LIMIT50_SQL}").as_str(),
        1,
    )
    .expect("token over-cap fallback LIMIT 50 EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain {
        explain: fallback_explain,
        ..
    } = fallback_explain.result
    else {
        panic!("token over-cap fallback LIMIT 50 EXPLAIN EXECUTION should return explain output");
    };
    assert!(
        !fallback_explain.contains("IndexBranchSet"),
        "token over-cap fallback LIMIT 50 should not be admitted as IndexBranchSet: {fallback_explain}",
    );
    assert!(
        fallback_explain.contains("OrderByMaterializedSort"),
        "token over-cap fallback LIMIT 50 should materialize-sort after rejecting the branch route: {fallback_explain}",
    );

    let large_in_fallback_explain = query_surface_with_perf(
        fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_LARGE_IN_FALLBACK_LIMIT50_SQL}").as_str(),
        1,
    )
    .expect("token large-IN fallback LIMIT 50 EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain {
        explain: large_in_fallback_explain,
        ..
    } = large_in_fallback_explain.result
    else {
        panic!("token large-IN fallback LIMIT 50 EXPLAIN EXECUTION should return explain output");
    };
    assert!(
        !large_in_fallback_explain.contains("IndexBranchSet"),
        "token large-IN fallback LIMIT 50 should not be admitted as IndexBranchSet: {large_in_fallback_explain}",
    );
    assert!(
        large_in_fallback_explain.contains("OrderByMaterializedSort"),
        "token large-IN fallback LIMIT 50 should remain on the over-cap fallback route: {large_in_fallback_explain}",
    );
}

fn assert_token_branch_set_index_residual_explain_contract(fixture: &StandaloneCanisterFixture) {
    let residual_explain = query_surface_with_perf(
        fixture,
        SqlPerfSurface::Token,
        format!("EXPLAIN EXECUTION {TOKEN_BRANCH_SET_INDEX_RESIDUAL_PAGE_SQL}").as_str(),
        1,
    )
    .expect("token branch-set index-residual EXPLAIN EXECUTION should succeed");
    let SqlQueryResult::Explain { explain, .. } = residual_explain.result else {
        panic!("token branch-set index-residual EXPLAIN EXECUTION should return explain output");
    };
    assert!(
        explain.contains("IndexPrefix"),
        "token branch-set index-residual EXPLAIN should expose the pruned prefix route: {explain}",
    );
    assert!(
        !explain.contains("IndexBranchSet"),
        "token branch-set index-residual EXPLAIN should prune the rejected branch before route execution: {explain}",
    );
    assert!(
        !explain.contains("OrderByMaterializedSort"),
        "token branch-set index-residual EXPLAIN must not materialize-sort the page route: {explain}",
    );
    assert!(
        explain.contains("covering_scan=true"),
        "token branch-set index-residual EXPLAIN should stay on the covering scan lane: {explain}",
    );
}

fn assert_token_branch_set_limit50_fallback_rows_match(fixture: &StandaloneCanisterFixture) {
    let branch_rows = rendered_projection_rows(
        query_surface_with_perf(
            fixture,
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_PAGE_LIMIT50_SQL,
            1,
        )
        .expect("token branch-set LIMIT 50 query should succeed")
        .result,
    );
    let fallback_rows = rendered_projection_rows(
        query_surface_with_perf(
            fixture,
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_OVERCAP_FALLBACK_LIMIT50_SQL,
            1,
        )
        .expect("token over-cap fallback LIMIT 50 query should succeed")
        .result,
    );
    assert_eq!(
        fallback_rows, branch_rows,
        "over-cap fallback comparator should return the same first page as the branch route",
    );
    let large_in_fallback_rows = rendered_projection_rows(
        query_surface_with_perf(
            fixture,
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_LARGE_IN_FALLBACK_LIMIT50_SQL,
            1,
        )
        .expect("token large-IN fallback LIMIT 50 query should succeed")
        .result,
    );
    assert_eq!(
        large_in_fallback_rows, branch_rows,
        "large-IN fallback comparator should return the same first page as the branch route",
    );
    let wide_branch_rows = rendered_projection_rows(
        query_surface_with_perf(
            fixture,
            SqlPerfSurface::Token,
            TOKEN_BRANCH_SET_WIDE_PAGE_LIMIT50_SQL,
            1,
        )
        .expect("token wide branch-set LIMIT 50 query should succeed")
        .result,
    );
    assert_eq!(
        wide_branch_rows, branch_rows,
        "wide branch-set comparator should return the same first page as the small branch route",
    );
}

#[test]
fn sql_perf_explain_queries_report_phase_breakdown() {
    let fixture = install_sql_perf_canister_fixture();

    for (scenario_key, sql) in [
        (
            "user.explain.lower.order.limit1",
            "EXPLAIN SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 1",
        ),
        (
            "user.explain_execution.lower.order.limit1",
            "EXPLAIN EXECUTION SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 1",
        ),
        (
            "user.explain_json.lower.order.limit1",
            "EXPLAIN JSON SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 1",
        ),
    ] {
        reset_sql_perf_fixtures(&fixture);
        let perf =
            query_surface_with_perf(&fixture, SqlPerfSurface::User, sql, 1).unwrap_or_else(|err| {
                panic!("explain scenario '{scenario_key}' should succeed: {err}")
            });

        println!(
            "{scenario_key}: compile={} planner={} store={} executor={} execute={} total={}",
            perf.attribution.compile_local_instructions,
            perf.attribution.execution.planner_local_instructions,
            perf.attribution.execution.store_local_instructions,
            perf.attribution.execution.executor_local_instructions,
            perf.attribution.execute_local_instructions,
            perf.attribution.total_local_instructions,
        );

        assert!(
            perf.attribution.total_local_instructions > 0,
            "explain scenario '{scenario_key}' should report positive total cost",
        );
    }
}

#[test]
// Prints the parser/compile subphase breakdown for the canonical shared-floor
// rows, so the long literal scenario table stays visible in one place.
#[expect(clippy::too_many_lines)]
fn sql_perf_shared_floor_queries_report_phase_breakdown() {
    let fixture = install_sql_perf_canister_fixture();

    for (scenario_key, sql, query_loop_count) in [
        (
            "user.pk.key_only.asc.limit1",
            "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
            1,
        ),
        (
            "user.pk.order_only.asc.limit1",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
            1,
        ),
        (
            "user.pk.order_only.asc.limit2",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
            1,
        ),
        (
            "user.name.lower.order_only.asc.limit3",
            "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
            1,
        ),
        (
            "user.grouped.age_count.limit10",
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
            1,
        ),
        (
            "user.age.in.limit3",
            "SELECT id, age FROM PerfAuditUser WHERE age IN (24, 31, 43) ORDER BY age ASC, id ASC LIMIT 3",
            1,
        ),
        (
            "user.age.not_in.limit3",
            "SELECT id, age FROM PerfAuditUser WHERE age NOT IN (24, 31, 43) ORDER BY id ASC LIMIT 3",
            1,
        ),
        (
            "repeat.user.pk.order_only.asc.limit1.runs10",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
            10,
        ),
        (
            "repeat.user.pk.order_only.asc.limit2.runs10",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
            10,
        ),
        (
            "repeat.user.name.lower.order_only.asc.limit3.runs10",
            "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
            10,
        ),
        (
            "repeat.user.grouped.age_count.limit10.runs10",
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
            10,
        ),
    ] {
        reset_sql_perf_fixtures(&fixture);
        let perf = query_surface_with_perf(&fixture, SqlPerfSurface::User, sql, query_loop_count)
            .unwrap_or_else(|err| {
                panic!("shared floor scenario '{scenario_key}' should succeed: {err}")
            });

        println!(
            "{scenario_key}: compile={} key={} lookup={} parse={} tokenize={} select={} expr={} predicate={} agg_check={} prepare={} lower={} bind={} planner={} store={} executor={} execute={} total={} pure={:?} compiled_hits={} compiled_misses={} shared_hits={} shared_misses={}",
            perf.attribution.compile_local_instructions,
            perf.attribution.compile.cache_key_local_instructions,
            perf.attribution.compile.cache_lookup_local_instructions,
            perf.attribution.compile.parse_local_instructions,
            perf.attribution.compile.parse_tokenize_local_instructions,
            perf.attribution.compile.parse_select_local_instructions,
            perf.attribution.compile.parse_expr_local_instructions,
            perf.attribution.compile.parse_predicate_local_instructions,
            perf.attribution
                .compile
                .aggregate_lane_check_local_instructions,
            perf.attribution.compile.prepare_local_instructions,
            perf.attribution.compile.lower_local_instructions,
            perf.attribution.compile.bind_local_instructions,
            perf.attribution.execution.planner_local_instructions,
            perf.attribution.execution.store_local_instructions,
            perf.attribution.execution.executor_local_instructions,
            perf.attribution.execute_local_instructions,
            perf.attribution.total_local_instructions,
            perf.attribution.pure_covering,
            perf.attribution.cache.sql_compiled_command_hits,
            perf.attribution.cache.sql_compiled_command_misses,
            perf.attribution.cache.shared_query_plan_hits,
            perf.attribution.cache.shared_query_plan_misses,
        );

        assert!(
            perf.attribution.total_local_instructions > 0,
            "shared floor scenario '{scenario_key}' should report positive total cost",
        );
        let parse_subphase_total = perf
            .attribution
            .compile
            .parse_tokenize_local_instructions
            .saturating_add(perf.attribution.compile.parse_select_local_instructions)
            .saturating_add(perf.attribution.compile.parse_expr_local_instructions)
            .saturating_add(perf.attribution.compile.parse_predicate_local_instructions);
        let parse_rounding_gap = perf
            .attribution
            .compile
            .parse_local_instructions
            .abs_diff(parse_subphase_total);
        assert!(
            parse_rounding_gap <= 2,
            "shared floor scenario '{scenario_key}' should keep parse subphases exhaustive apart from averaged rounding, got parse={} subphases={parse_subphase_total}",
            perf.attribution.compile.parse_local_instructions,
        );
    }
}
