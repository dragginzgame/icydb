use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error,
    db::{QueryExecutionAttribution, SqlQueryExecutionAttribution, sql::SqlQueryResult},
};
use icydb_testing_integration::{install_fixture_canister, reset_icydb_fixtures};
use serde::{Deserialize, Serialize};

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

#[derive(Clone, Copy, Debug)]
enum SqlPerfSurface {
    Account,
    Blob,
    User,
}

impl SqlPerfSurface {
    const fn label(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Blob => "blob",
            Self::User => "user",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SqlPerfSampleMode {
    QueryOnly,
    WarmThenQuery,
}

#[derive(Clone, Copy, Debug)]
struct SqlPerfScenario {
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    index_family: &'static str,
    query_family: &'static str,
    sql: &'static str,
    sample_count: usize,
    query_loop_count: usize,
    sample_mode: SqlPerfSampleMode,
    isolated_fixture: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SqlPerfBaselineRow {
    scenario_key: String,
    #[serde(default)]
    avg_compile_local_instructions: u64,
    #[serde(default)]
    avg_execute_local_instructions: u64,
    avg_local_instructions: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SqlPerfOutcome {
    result_kind: &'static str,
    entity: String,
    row_count: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct SqlPerfScenarioSample {
    scenario_key: String,
    surface: String,
    index_family: String,
    query_family: String,
    sql: String,
    query_loop_count: usize,
    baseline_avg_compile_local_instructions: Option<u64>,
    baseline_avg_execute_local_instructions: Option<u64>,
    baseline_avg_local_instructions: Option<u64>,
    avg_compile_local_instructions: u64,
    avg_execute_local_instructions: u64,
    avg_grouped_stream_local_instructions: u64,
    avg_grouped_fold_local_instructions: u64,
    avg_grouped_finalize_local_instructions: u64,
    avg_grouped_count_borrowed_hash_computations: u64,
    avg_grouped_count_bucket_candidate_checks: u64,
    avg_grouped_count_existing_group_hits: u64,
    avg_grouped_count_new_group_inserts: u64,
    avg_grouped_count_row_materialization_local_instructions: u64,
    avg_grouped_count_group_lookup_local_instructions: u64,
    avg_grouped_count_existing_group_update_local_instructions: u64,
    avg_grouped_count_new_group_insert_local_instructions: u64,
    avg_data_store_get_calls: u64,
    avg_sql_compiled_command_cache_hits: u64,
    avg_sql_compiled_command_cache_misses: u64,
    avg_shared_query_plan_cache_hits: u64,
    avg_shared_query_plan_cache_misses: u64,
    first_local_instructions: u64,
    min_local_instructions: u64,
    max_local_instructions: u64,
    total_local_instructions: u64,
    avg_local_instructions: u64,
    avg_local_instructions_delta: Option<i64>,
    avg_local_instructions_delta_percent_bps: Option<i64>,
    outcome_stable: bool,
    outcome: SqlPerfOutcome,
}

const fn scenario(
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    index_family: &'static str,
    query_family: &'static str,
    sql: &'static str,
) -> SqlPerfScenario {
    SqlPerfScenario {
        scenario_key,
        surface,
        index_family,
        query_family,
        sql,
        sample_count: 5,
        query_loop_count: 1,
        sample_mode: SqlPerfSampleMode::QueryOnly,
        isolated_fixture: false,
    }
}

const fn repeat_scenario(
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    index_family: &'static str,
    query_family: &'static str,
    sql: &'static str,
    query_loop_count: usize,
) -> SqlPerfScenario {
    SqlPerfScenario {
        scenario_key,
        surface,
        index_family,
        query_family,
        sql,
        sample_count: 5,
        query_loop_count,
        sample_mode: SqlPerfSampleMode::QueryOnly,
        isolated_fixture: false,
    }
}

const fn parity_scenario(
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    index_family: &'static str,
    query_family: &'static str,
    sql: &'static str,
    sample_mode: SqlPerfSampleMode,
) -> SqlPerfScenario {
    SqlPerfScenario {
        scenario_key,
        surface,
        index_family,
        query_family,
        sql,
        sample_count: 1,
        query_loop_count: 1,
        sample_mode,
        isolated_fixture: true,
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
            result_kind: "__icydb_ddl",
            entity: entity.clone(),
            row_count: 1,
        },
    }
}

fn baseline_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("sql_perf_audit_baseline.json")
}

fn load_baseline_rows() -> HashMap<String, SqlPerfBaselineRow> {
    let path = baseline_path();
    let Ok(raw) = fs::read_to_string(&path) else {
        return HashMap::new();
    };
    if raw.trim().is_empty() {
        return HashMap::new();
    }
    let rows: Vec<SqlPerfBaselineRow> = serde_json::from_str(&raw).unwrap_or_else(|err| {
        panic!(
            "sql perf baseline should parse at '{}': {err}",
            path.display()
        )
    });

    rows.into_iter()
        .map(|row| (row.scenario_key.clone(), row))
        .collect()
}

fn baseline_rows_from_samples(samples: &[SqlPerfScenarioSample]) -> Vec<SqlPerfBaselineRow> {
    samples
        .iter()
        .map(|sample| SqlPerfBaselineRow {
            scenario_key: sample.scenario_key.clone(),
            avg_compile_local_instructions: sample.avg_compile_local_instructions,
            avg_execute_local_instructions: sample.avg_execute_local_instructions,
            avg_local_instructions: sample.avg_local_instructions,
        })
        .collect()
}

fn maybe_write_blessed_baseline(samples: &[SqlPerfScenarioSample]) {
    if std::env::var_os("SQL_PERF_AUDIT_BLESS").is_none() {
        return;
    }

    let path = baseline_path();
    let rows = baseline_rows_from_samples(samples);
    let json = serde_json::to_string_pretty(&rows)
        .expect("sql perf baseline rows should serialize to pretty JSON");
    fs::write(&path, json).unwrap_or_else(|err| {
        panic!(
            "sql perf baseline should write to '{}': {err}",
            path.display()
        )
    });
}

fn average_u64(samples: &[u64]) -> u64 {
    samples.iter().copied().sum::<u64>() / u64::try_from(samples.len()).unwrap_or(1)
}

fn delta_percent_bps(current: u64, previous: u64) -> Option<i64> {
    if previous == 0 {
        return None;
    }

    let delta = i128::from(current) - i128::from(previous);
    let scaled = delta
        .saturating_mul(10_000)
        .checked_div(i128::from(previous))
        .expect("previous should be non-zero");

    Some(i64::try_from(scaled).expect("delta percent basis points should fit i64"))
}

// GroupedCountRawSamples keeps one scenario's grouped-count submetrics together so the
// SQL perf harness can record and average that subfamily through one owner.
struct GroupedCountRawSamples {
    borrowed_hash: Vec<u64>,
    bucket_checks: Vec<u64>,
    existing_hits: Vec<u64>,
    new_inserts: Vec<u64>,
    row_materialization_local_instructions: Vec<u64>,
    group_lookup_local_instructions: Vec<u64>,
    existing_group_update_local_instructions: Vec<u64>,
    new_group_insert_local_instructions: Vec<u64>,
}

impl GroupedCountRawSamples {
    fn with_capacity(sample_count: usize) -> Self {
        Self {
            borrowed_hash: Vec::with_capacity(sample_count),
            bucket_checks: Vec::with_capacity(sample_count),
            existing_hits: Vec::with_capacity(sample_count),
            new_inserts: Vec::with_capacity(sample_count),
            row_materialization_local_instructions: Vec::with_capacity(sample_count),
            group_lookup_local_instructions: Vec::with_capacity(sample_count),
            existing_group_update_local_instructions: Vec::with_capacity(sample_count),
            new_group_insert_local_instructions: Vec::with_capacity(sample_count),
        }
    }

    fn record(&mut self, attribution: &SqlQueryExecutionAttribution) {
        let count = attribution.grouped.map(|grouped| grouped.count);

        self.borrowed_hash
            .push(count.map_or(0, |count| count.borrowed_hash_computations));
        self.bucket_checks
            .push(count.map_or(0, |count| count.bucket_candidate_checks));
        self.existing_hits
            .push(count.map_or(0, |count| count.existing_group_hits));
        self.new_inserts
            .push(count.map_or(0, |count| count.new_group_inserts));
        self.row_materialization_local_instructions
            .push(count.map_or(0, |count| count.row_materialization_local_instructions));
        self.group_lookup_local_instructions
            .push(count.map_or(0, |count| count.group_lookup_local_instructions));
        self.existing_group_update_local_instructions
            .push(count.map_or(0, |count| count.existing_group_update_local_instructions));
        self.new_group_insert_local_instructions
            .push(count.map_or(0, |count| count.new_group_insert_local_instructions));
    }
}

struct GroupedCountSampleAverages {
    borrowed_hash_computations: u64,
    bucket_candidate_checks: u64,
    existing_group_hits: u64,
    new_group_inserts: u64,
    row_materialization_local_instructions: u64,
    group_lookup_local_instructions: u64,
    existing_group_update_local_instructions: u64,
    new_group_insert_local_instructions: u64,
}

impl GroupedCountRawSamples {
    fn average(&self) -> GroupedCountSampleAverages {
        GroupedCountSampleAverages {
            borrowed_hash_computations: average_u64(&self.borrowed_hash),
            bucket_candidate_checks: average_u64(&self.bucket_checks),
            existing_group_hits: average_u64(&self.existing_hits),
            new_group_inserts: average_u64(&self.new_inserts),
            row_materialization_local_instructions: average_u64(
                &self.row_materialization_local_instructions,
            ),
            group_lookup_local_instructions: average_u64(&self.group_lookup_local_instructions),
            existing_group_update_local_instructions: average_u64(
                &self.existing_group_update_local_instructions,
            ),
            new_group_insert_local_instructions: average_u64(
                &self.new_group_insert_local_instructions,
            ),
        }
    }
}

// SqlPerfRawSamples keeps one scenario's repeated raw counters together so the
// report builder can collapse them without passing a long list of slices.
struct SqlPerfRawSamples {
    compile_samples: Vec<u64>,
    execute_samples: Vec<u64>,
    grouped_stream_samples: Vec<u64>,
    grouped_fold_samples: Vec<u64>,
    grouped_finalize_samples: Vec<u64>,
    grouped_count: GroupedCountRawSamples,
    data_store_get_call_samples: Vec<u64>,
    sql_compiled_command_cache_hit_samples: Vec<u64>,
    sql_compiled_command_cache_miss_samples: Vec<u64>,
    shared_query_plan_cache_hit_samples: Vec<u64>,
    shared_query_plan_cache_miss_samples: Vec<u64>,
    instruction_samples: Vec<u64>,
    outcomes: Vec<SqlPerfOutcome>,
}

impl SqlPerfRawSamples {
    fn with_capacity(sample_count: usize) -> Self {
        Self {
            compile_samples: Vec::with_capacity(sample_count),
            execute_samples: Vec::with_capacity(sample_count),
            grouped_stream_samples: Vec::with_capacity(sample_count),
            grouped_fold_samples: Vec::with_capacity(sample_count),
            grouped_finalize_samples: Vec::with_capacity(sample_count),
            grouped_count: GroupedCountRawSamples::with_capacity(sample_count),
            data_store_get_call_samples: Vec::with_capacity(sample_count),
            sql_compiled_command_cache_hit_samples: Vec::with_capacity(sample_count),
            sql_compiled_command_cache_miss_samples: Vec::with_capacity(sample_count),
            shared_query_plan_cache_hit_samples: Vec::with_capacity(sample_count),
            shared_query_plan_cache_miss_samples: Vec::with_capacity(sample_count),
            instruction_samples: Vec::with_capacity(sample_count),
            outcomes: Vec::with_capacity(sample_count),
        }
    }

    fn record(&mut self, sample: SqlQueryPerfResult) {
        self.compile_samples
            .push(sample.attribution.compile_local_instructions);
        self.execute_samples
            .push(sample.attribution.execute_local_instructions);
        let grouped = sample.attribution.grouped;
        self.grouped_stream_samples
            .push(grouped.map_or(0, |grouped| grouped.stream_local_instructions));
        self.grouped_fold_samples
            .push(grouped.map_or(0, |grouped| grouped.fold_local_instructions));
        self.grouped_finalize_samples
            .push(grouped.map_or(0, |grouped| grouped.finalize_local_instructions));
        self.grouped_count.record(&sample.attribution);
        self.data_store_get_call_samples
            .push(sample.attribution.store_get_calls);
        self.sql_compiled_command_cache_hit_samples
            .push(sample.attribution.cache.sql_compiled_command_hits);
        self.sql_compiled_command_cache_miss_samples
            .push(sample.attribution.cache.sql_compiled_command_misses);
        self.shared_query_plan_cache_hit_samples
            .push(sample.attribution.cache.shared_query_plan_hits);
        self.shared_query_plan_cache_miss_samples
            .push(sample.attribution.cache.shared_query_plan_misses);
        self.instruction_samples
            .push(sample.attribution.total_local_instructions);
        self.outcomes.push(summarize_perf_outcome(&sample.result));
    }
}

fn build_sql_perf_scenario_sample(
    baseline: &HashMap<String, SqlPerfBaselineRow>,
    scenario: SqlPerfScenario,
    raw: &SqlPerfRawSamples,
) -> SqlPerfScenarioSample {
    let avg_compile_local_instructions = average_u64(&raw.compile_samples);
    let avg_execute_local_instructions = average_u64(&raw.execute_samples);
    let avg_grouped_stream_local_instructions = average_u64(&raw.grouped_stream_samples);
    let avg_grouped_fold_local_instructions = average_u64(&raw.grouped_fold_samples);
    let avg_grouped_finalize_local_instructions = average_u64(&raw.grouped_finalize_samples);
    let grouped_count = raw.grouped_count.average();
    let avg_grouped_count_borrowed_hash_computations = grouped_count.borrowed_hash_computations;
    let avg_grouped_count_bucket_candidate_checks = grouped_count.bucket_candidate_checks;
    let avg_grouped_count_existing_group_hits = grouped_count.existing_group_hits;
    let avg_grouped_count_new_group_inserts = grouped_count.new_group_inserts;
    let avg_grouped_count_row_materialization_local_instructions =
        grouped_count.row_materialization_local_instructions;
    let avg_grouped_count_group_lookup_local_instructions =
        grouped_count.group_lookup_local_instructions;
    let avg_grouped_count_existing_group_update_local_instructions =
        grouped_count.existing_group_update_local_instructions;
    let avg_grouped_count_new_group_insert_local_instructions =
        grouped_count.new_group_insert_local_instructions;
    let avg_data_store_get_calls = average_u64(&raw.data_store_get_call_samples);
    let avg_sql_compiled_command_cache_hits =
        average_u64(&raw.sql_compiled_command_cache_hit_samples);
    let avg_sql_compiled_command_cache_misses =
        average_u64(&raw.sql_compiled_command_cache_miss_samples);
    let avg_shared_query_plan_cache_hits = average_u64(&raw.shared_query_plan_cache_hit_samples);
    let avg_shared_query_plan_cache_misses = average_u64(&raw.shared_query_plan_cache_miss_samples);
    let first_local_instructions = raw.instruction_samples[0];
    let min_local_instructions = raw.instruction_samples.iter().copied().min().unwrap_or(0);
    let max_local_instructions = raw.instruction_samples.iter().copied().max().unwrap_or(0);
    let total_local_instructions = raw.instruction_samples.iter().copied().sum::<u64>();
    let avg_local_instructions = average_u64(&raw.instruction_samples);
    let outcome = raw.outcomes[0].clone();
    let outcome_stable = raw.outcomes.iter().all(|candidate| candidate == &outcome);
    let baseline_row = baseline.get(scenario.scenario_key);
    let baseline_avg_compile_local_instructions =
        baseline_row.map(|row| row.avg_compile_local_instructions);
    let baseline_avg_execute_local_instructions =
        baseline_row.map(|row| row.avg_execute_local_instructions);
    let baseline_avg_local_instructions = baseline_row.map(|row| row.avg_local_instructions);
    let avg_local_instructions_delta = baseline_avg_local_instructions.map(|previous| {
        i64::try_from(avg_local_instructions).expect("instruction count should fit i64")
            - i64::try_from(previous).expect("instruction count should fit i64")
    });
    let avg_local_instructions_delta_percent_bps = baseline_avg_local_instructions
        .and_then(|previous| delta_percent_bps(avg_local_instructions, previous));

    SqlPerfScenarioSample {
        scenario_key: scenario.scenario_key.to_string(),
        surface: scenario.surface.label().to_string(),
        index_family: scenario.index_family.to_string(),
        query_family: scenario.query_family.to_string(),
        sql: scenario.sql.to_string(),
        query_loop_count: scenario.query_loop_count,
        baseline_avg_compile_local_instructions,
        baseline_avg_execute_local_instructions,
        baseline_avg_local_instructions,
        avg_compile_local_instructions,
        avg_execute_local_instructions,
        avg_grouped_stream_local_instructions,
        avg_grouped_fold_local_instructions,
        avg_grouped_finalize_local_instructions,
        avg_grouped_count_borrowed_hash_computations,
        avg_grouped_count_bucket_candidate_checks,
        avg_grouped_count_existing_group_hits,
        avg_grouped_count_new_group_inserts,
        avg_grouped_count_row_materialization_local_instructions,
        avg_grouped_count_group_lookup_local_instructions,
        avg_grouped_count_existing_group_update_local_instructions,
        avg_grouped_count_new_group_insert_local_instructions,
        avg_data_store_get_calls,
        avg_sql_compiled_command_cache_hits,
        avg_sql_compiled_command_cache_misses,
        avg_shared_query_plan_cache_hits,
        avg_shared_query_plan_cache_misses,
        first_local_instructions,
        min_local_instructions,
        max_local_instructions,
        total_local_instructions,
        avg_local_instructions,
        avg_local_instructions_delta,
        avg_local_instructions_delta_percent_bps,
        outcome_stable,
        outcome,
    }
}

fn sample_perf_scenario(
    fixture: &StandaloneCanisterFixture,
    baseline: &HashMap<String, SqlPerfBaselineRow>,
    scenario: SqlPerfScenario,
) -> SqlPerfScenarioSample {
    let mut raw = SqlPerfRawSamples::with_capacity(scenario.sample_count);

    // Phase 1: sample the same scenario repeatedly against one stable fixture.
    // Each sample can optionally run the SQL multiple times inside one
    // canister call so the audit can measure a real session-local cache.
    for _ in 0..scenario.sample_count {
        let isolated_fixture;
        let active_fixture = if scenario.isolated_fixture {
            isolated_fixture = install_sql_perf_canister_fixture();
            reset_sql_perf_fixtures(&isolated_fixture);

            &isolated_fixture
        } else {
            fixture
        };
        let sample = match scenario.sample_mode {
            SqlPerfSampleMode::QueryOnly => query_surface_with_perf(
                active_fixture,
                scenario.surface,
                scenario.sql,
                scenario.query_loop_count,
            ),
            SqlPerfSampleMode::WarmThenQuery => {
                warm_query_surface_with_perf(active_fixture, scenario.surface, scenario.sql)
                    .unwrap_or_else(|err| {
                        panic!(
                            "warm perf scenario '{}' on '{}' should succeed: {err}",
                            scenario.scenario_key,
                            scenario.surface.label(),
                        )
                    });
                query_surface_with_perf(
                    active_fixture,
                    scenario.surface,
                    scenario.sql,
                    scenario.query_loop_count,
                )
            }
        }
        .unwrap_or_else(|err| {
            panic!(
                "perf scenario '{}' on '{}' should succeed: {err}",
                scenario.scenario_key,
                scenario.surface.label(),
            )
        });
        raw.record(sample);
    }

    build_sql_perf_scenario_sample(baseline, scenario, &raw)
}

// Keeps the core shared-floor SQL audit rows in one contiguous list so
// baseline drift is easy to review and edit without chasing helper indirection.
#[expect(clippy::too_many_lines)]
fn user_primary_and_age_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "user.pk.key_only.asc.limit1",
            SqlPerfSurface::User,
            "primary_key",
            "scalar_projection",
            "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
        ),
        scenario(
            "user.pk.order_only.asc.limit1",
            SqlPerfSurface::User,
            "primary_key",
            "scalar_projection",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
        ),
        scenario(
            "user.pk.order_only.asc.limit2",
            SqlPerfSurface::User,
            "primary_key",
            "scalar_projection",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
        ),
        scenario(
            "user.pk.order_only.desc.limit2",
            SqlPerfSurface::User,
            "primary_key",
            "scalar_projection",
            "SELECT id, name FROM PerfAuditUser ORDER BY id DESC LIMIT 2",
        ),
        scenario(
            "user.pk.range.asc.limit2",
            SqlPerfSurface::User,
            "primary_key",
            "primary_range",
            "SELECT id, name FROM PerfAuditUser WHERE id >= 2 ORDER BY id ASC LIMIT 2",
        ),
        scenario(
            "user.name.eq.order_id.limit1",
            SqlPerfSurface::User,
            "secondary_name_eq",
            "scalar_projection",
            "SELECT id, name FROM PerfAuditUser WHERE name = 'Alice' ORDER BY id ASC LIMIT 1",
        ),
        scenario(
            "user.age.order_only.asc.limit3",
            SqlPerfSurface::User,
            "secondary_age_id",
            "scalar_projection",
            "SELECT id, age FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 3",
        ),
        parity_scenario(
            "user.age.order_only.asc.limit2.cold_query",
            SqlPerfSurface::User,
            "secondary_age_id",
            "parity_cold_query",
            "SELECT id, age FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 2",
            SqlPerfSampleMode::QueryOnly,
        ),
        parity_scenario(
            "user.age.order_only.asc.limit2.warm_after_update",
            SqlPerfSurface::User,
            "secondary_age_id",
            "parity_warm_after_update",
            "SELECT id, age FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 2",
            SqlPerfSampleMode::WarmThenQuery,
        ),
        scenario(
            "user.age.order_only.desc.limit3",
            SqlPerfSurface::User,
            "secondary_age_id",
            "scalar_projection",
            "SELECT id, age FROM PerfAuditUser ORDER BY age DESC, id DESC LIMIT 3",
        ),
        scenario(
            "user.age.range.order.limit3",
            SqlPerfSurface::User,
            "secondary_age_id",
            "bounded_range",
            "SELECT id, age FROM PerfAuditUser WHERE age >= 24 AND age < 32 ORDER BY age ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.age.between.limit3",
            SqlPerfSurface::User,
            "secondary_age_id",
            "between_literal",
            "SELECT id, age FROM PerfAuditUser WHERE age BETWEEN 24 AND 31 ORDER BY age ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.age.not_between.limit3",
            SqlPerfSurface::User,
            "residual_not_between",
            "not_between_literal",
            "SELECT id, age FROM PerfAuditUser WHERE age NOT BETWEEN 24 AND 31 ORDER BY id ASC LIMIT 3",
        ),
        scenario(
            "user.scalar_arithmetic.age_minus_one.limit3",
            SqlPerfSurface::User,
            "secondary_age_id",
            "scalar_arithmetic",
            "SELECT age - 1 FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.scalar_round.age_div3.limit3",
            SqlPerfSurface::User,
            "secondary_age_id",
            "scalar_round",
            "SELECT ROUND(age / 3, 2) FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 3",
        ),
    ]
}

fn user_name_scenarios() -> Vec<SqlPerfScenario> {
    let mut scenarios = Vec::new();
    scenarios.extend(user_name_raw_order_scenarios());
    scenarios.extend(user_name_expression_order_scenarios());
    scenarios.extend(user_name_casefold_predicate_scenarios());

    scenarios
}

fn user_name_raw_order_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "user.name.order_only.asc.limit3",
            SqlPerfSurface::User,
            "secondary_name",
            "order_only",
            "SELECT id, name FROM PerfAuditUser ORDER BY name ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.name.covering_key_only.asc.limit3",
            SqlPerfSurface::User,
            "secondary_name",
            "covering_order_only",
            "SELECT id FROM PerfAuditUser ORDER BY name ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.name.materialized_rank.asc.limit3",
            SqlPerfSurface::User,
            "secondary_name",
            "materialized_order_only",
            "SELECT id, rank FROM PerfAuditUser ORDER BY name ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.name.order_only.desc.limit3",
            SqlPerfSurface::User,
            "secondary_name",
            "order_only",
            "SELECT id, name FROM PerfAuditUser ORDER BY name DESC, id DESC LIMIT 3",
        ),
        scenario(
            "user.name.range.limit3",
            SqlPerfSurface::User,
            "secondary_name",
            "ordered_range",
            "SELECT id, name FROM PerfAuditUser WHERE name >= 'A' AND name < 'd' ORDER BY name ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.name.like_prefix.limit3",
            SqlPerfSurface::User,
            "secondary_name",
            "prefix_like",
            "SELECT id, name FROM PerfAuditUser WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.name.not_like_prefix.limit3",
            SqlPerfSurface::User,
            "residual_not_like",
            "negated_prefix_like",
            "SELECT id, name FROM PerfAuditUser WHERE name NOT LIKE 'A%' ORDER BY id ASC LIMIT 3",
        ),
    ]
}

fn user_name_expression_order_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "user.name.lower.order_only.asc.limit3",
            SqlPerfSurface::User,
            "expression_lower_name",
            "expression_order_only",
            "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.name.lower.covering_key_only.asc.limit3",
            SqlPerfSurface::User,
            "expression_lower_name",
            "expression_covering_order_only",
            "SELECT id FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.name.lower.materialized_rank.asc.limit3",
            SqlPerfSurface::User,
            "expression_lower_name",
            "expression_materialized_order_only",
            "SELECT id, rank FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.name.lower.order_only.desc.limit3",
            SqlPerfSurface::User,
            "expression_lower_name",
            "expression_order_only",
            "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) DESC, id DESC LIMIT 3",
        ),
        scenario(
            "user.name.lower.range.limit3",
            SqlPerfSurface::User,
            "expression_lower_name",
            "expression_ordered_range",
            "SELECT id, name FROM PerfAuditUser WHERE LOWER(name) >= 'a' AND LOWER(name) < 'c' ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.name.lower.like_prefix.limit3",
            SqlPerfSurface::User,
            "expression_lower_name",
            "expression_casefold_prefix",
            "SELECT id, name FROM PerfAuditUser WHERE LOWER(name) LIKE 'a%' ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.name.lower.not_like_prefix.limit3",
            SqlPerfSurface::User,
            "residual_not_like_casefold",
            "expression_negated_casefold_prefix",
            "SELECT id, name FROM PerfAuditUser WHERE LOWER(name) NOT LIKE 'a%' ORDER BY id ASC LIMIT 3",
        ),
        scenario(
            "user.age.plus_one.alias_order.asc.limit3",
            SqlPerfSurface::User,
            "materialized_computed_alias_order",
            "computed_alias_order",
            "SELECT id, age + 1 AS next_age FROM PerfAuditUser ORDER BY next_age ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.age_plus_rank.alias_order.asc.limit3",
            SqlPerfSurface::User,
            "materialized_computed_alias_order",
            "computed_alias_order",
            "SELECT id, age + rank AS total FROM PerfAuditUser ORDER BY total ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.age_plus_rank.direct_order.asc.limit3",
            SqlPerfSurface::User,
            "materialized_computed_order",
            "computed_order",
            "SELECT id, age + rank FROM PerfAuditUser ORDER BY age + rank ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.age_div3_round.direct_order.desc.limit3",
            SqlPerfSurface::User,
            "materialized_computed_order",
            "computed_round_order",
            "SELECT id, age FROM PerfAuditUser ORDER BY ROUND(age / 3, 2) DESC, id DESC LIMIT 3",
        ),
    ]
}

fn user_name_casefold_predicate_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "user.name.ilike_prefix.limit3",
            SqlPerfSurface::User,
            "casefold_predicate_only",
            "casefold_prefix",
            "SELECT id, name FROM PerfAuditUser WHERE name ILIKE 'a%' ORDER BY id ASC LIMIT 3",
        ),
        scenario(
            "user.name.not_ilike_prefix.limit3",
            SqlPerfSurface::User,
            "residual_negated_casefold",
            "negated_casefold_prefix",
            "SELECT id, name FROM PerfAuditUser WHERE name NOT ILIKE 'a%' ORDER BY id ASC LIMIT 3",
        ),
    ]
}

fn user_predicate_and_metadata_scenarios() -> Vec<SqlPerfScenario> {
    let mut scenarios = Vec::new();
    scenarios.extend(user_bool_predicate_scenarios());
    scenarios.extend(user_field_predicate_scenarios());
    scenarios.extend(user_membership_and_between_scenarios());
    scenarios.extend(user_aggregate_and_metadata_scenarios());

    scenarios
}

fn user_bool_predicate_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "user.active.is_true.order_id.limit4",
            SqlPerfSurface::User,
            "primary_key",
            "bool_predicate",
            "SELECT id, active FROM PerfAuditUser WHERE active IS TRUE ORDER BY id ASC LIMIT 4",
        ),
        scenario(
            "user.active.is_not_true.order_id.limit4",
            SqlPerfSurface::User,
            "primary_key",
            "bool_predicate",
            "SELECT id, active FROM PerfAuditUser WHERE active IS NOT TRUE ORDER BY id ASC LIMIT 4",
        ),
    ]
}

fn user_field_predicate_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "user.field_compare.age_gt_rank.limit3",
            SqlPerfSurface::User,
            "residual_field_compare",
            "field_compare",
            "SELECT id, name FROM PerfAuditUser WHERE age > rank ORDER BY age ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.field_compare.age_gt_rank.lower_order.limit3",
            SqlPerfSurface::User,
            "expression_lower_name",
            "field_compare_expression_order",
            "SELECT id, name FROM PerfAuditUser WHERE age > rank ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.field_compare.age_eq_age_nat.limit3",
            SqlPerfSurface::User,
            "residual_mixed_field_compare",
            "mixed_field_compare",
            "SELECT id, name FROM PerfAuditUser WHERE age = age_nat ORDER BY age ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.field_compare.age_eq_rank.limit3",
            SqlPerfSurface::User,
            "residual_field_compare",
            "field_compare",
            "SELECT id, name FROM PerfAuditUser WHERE age = rank ORDER BY age ASC, id ASC LIMIT 3",
        ),
    ]
}

fn user_membership_and_between_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "user.age.in.limit3",
            SqlPerfSurface::User,
            "secondary_age_id",
            "in_membership",
            "SELECT id, age FROM PerfAuditUser WHERE age IN (24, 31, 43) ORDER BY age ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.age.in.computed_order.limit3",
            SqlPerfSurface::User,
            "materialized_computed_order",
            "in_membership_computed_order",
            "SELECT id, age FROM PerfAuditUser WHERE age IN (24, 31, 43) ORDER BY age + rank ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.age.not_in.limit3",
            SqlPerfSurface::User,
            "residual_not_in",
            "not_in_membership",
            "SELECT id, age FROM PerfAuditUser WHERE age NOT IN (24, 31, 43) ORDER BY id ASC LIMIT 3",
        ),
        scenario(
            "user.field_between.rank_age_age.limit3",
            SqlPerfSurface::User,
            "residual_field_between",
            "field_between",
            "SELECT id, name FROM PerfAuditUser WHERE rank BETWEEN age AND age ORDER BY age ASC, id ASC LIMIT 3",
        ),
        scenario(
            "user.field_not_between.rank_age_age.limit3",
            SqlPerfSurface::User,
            "residual_field_between",
            "field_not_between",
            "SELECT id, name FROM PerfAuditUser WHERE rank NOT BETWEEN age AND age ORDER BY id ASC LIMIT 3",
        ),
    ]
}

fn user_aggregate_and_metadata_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "user.count.active_true",
            SqlPerfSurface::User,
            "aggregate_count",
            "count",
            "SELECT COUNT(*) FROM PerfAuditUser WHERE active = true",
        ),
        scenario(
            "user.grouped.age_count.limit10",
            SqlPerfSurface::User,
            "grouped_no_special_index",
            "grouped_count",
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
        ),
        parity_scenario(
            "user.grouped.case_sum.having_alias.order.limit5.cold_query",
            SqlPerfSurface::User,
            "grouped_no_special_index",
            "parity_cold_query",
            "SELECT age, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS high_count FROM PerfAuditUser GROUP BY age HAVING high_count > 0 ORDER BY high_count DESC, age ASC LIMIT 5",
            SqlPerfSampleMode::QueryOnly,
        ),
        parity_scenario(
            "user.grouped.case_sum.having_alias.order.limit5.warm_after_update",
            SqlPerfSurface::User,
            "grouped_no_special_index",
            "parity_warm_after_update",
            "SELECT age, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS high_count FROM PerfAuditUser GROUP BY age HAVING high_count > 0 ORDER BY high_count DESC, age ASC LIMIT 5",
            SqlPerfSampleMode::WarmThenQuery,
        ),
        scenario(
            "user.explain.lower.order.limit1",
            SqlPerfSurface::User,
            "expression_lower_name",
            "explain",
            "EXPLAIN SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 1",
        ),
        scenario(
            "user.explain_execution.lower.order.limit1",
            SqlPerfSurface::User,
            "expression_lower_name",
            "explain_execution",
            "EXPLAIN EXECUTION SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 1",
        ),
        scenario(
            "user.explain_json.lower.order.limit1",
            SqlPerfSurface::User,
            "expression_lower_name",
            "explain_json",
            "EXPLAIN JSON SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 1",
        ),
        scenario(
            "user.describe",
            SqlPerfSurface::User,
            "metadata",
            "describe",
            "DESCRIBE PerfAuditUser",
        ),
        scenario(
            "user.show_indexes",
            SqlPerfSurface::User,
            "metadata",
            "show_indexes",
            "SHOW INDEXES FROM PerfAuditUser",
        ),
        scenario(
            "user.show_columns",
            SqlPerfSurface::User,
            "metadata",
            "show_columns",
            "SHOW COLUMNS PerfAuditUser",
        ),
        scenario(
            "user.show_entities",
            SqlPerfSurface::User,
            "metadata",
            "show_entities",
            "SHOW ENTITIES",
        ),
    ]
}

fn account_order_scenarios() -> Vec<SqlPerfScenario> {
    let mut scenarios = Vec::new();
    scenarios.extend(account_order_handle_scenarios());
    scenarios.extend(account_lower_handle_order_scenarios());

    scenarios
}

fn account_order_handle_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "account.active.order_handle.asc.limit3",
            SqlPerfSurface::Account,
            "filtered_handle_active_only",
            "guarded_order_only",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY handle ASC, id ASC LIMIT 3",
        ),
        scenario(
            "account.active.order_handle.asc.limit2.offset1",
            SqlPerfSurface::Account,
            "filtered_handle_active_only",
            "guarded_order_offset",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY handle ASC, id ASC LIMIT 2 OFFSET 1",
        ),
        scenario(
            "account.active.order_handle.desc.limit3",
            SqlPerfSurface::Account,
            "filtered_handle_active_only",
            "guarded_order_only",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY handle DESC, id DESC LIMIT 3",
        ),
        scenario(
            "account.active.ilike_br.limit3",
            SqlPerfSurface::Account,
            "guarded_casefold_predicate_only",
            "guarded_casefold_prefix",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true AND handle ILIKE 'br%' ORDER BY id ASC LIMIT 3",
        ),
        scenario(
            "account.active.not_ilike_br.limit3",
            SqlPerfSurface::Account,
            "guarded_casefold_residual",
            "guarded_negated_casefold_prefix",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true AND handle NOT ILIKE 'br%' ORDER BY id ASC LIMIT 3",
        ),
        scenario(
            "account.active.handle_prefix.limit3",
            SqlPerfSurface::Account,
            "filtered_handle_active_only",
            "guarded_prefix",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 3",
        ),
    ]
}

fn account_lower_handle_order_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "account.active.lower.order_handle.asc.limit3",
            SqlPerfSurface::Account,
            "filtered_lower_handle_active_only",
            "guarded_expression_order_only",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
        ),
        parity_scenario(
            "account.active.lower.order_handle.asc.limit3.cold_query",
            SqlPerfSurface::Account,
            "filtered_lower_handle_active_only",
            "parity_cold_query",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
            SqlPerfSampleMode::QueryOnly,
        ),
        parity_scenario(
            "account.active.lower.order_handle.asc.limit3.warm_after_update",
            SqlPerfSurface::Account,
            "filtered_lower_handle_active_only",
            "parity_warm_after_update",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
            SqlPerfSampleMode::WarmThenQuery,
        ),
        scenario(
            "account.active.lower.covering_key_only.asc.limit3",
            SqlPerfSurface::Account,
            "filtered_lower_handle_active_only",
            "guarded_expression_covering_order_only",
            "SELECT id FROM PerfAuditAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
        ),
        scenario(
            "account.active.lower.materialized_score.asc.limit3",
            SqlPerfSurface::Account,
            "filtered_lower_handle_active_only",
            "guarded_expression_materialized_order_only",
            "SELECT id, score FROM PerfAuditAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
        ),
        scenario(
            "account.active.lower.order_handle.asc.limit2.offset1",
            SqlPerfSurface::Account,
            "filtered_lower_handle_active_only",
            "guarded_expression_order_offset",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2 OFFSET 1",
        ),
        scenario(
            "account.active.lower.order_handle.desc.limit3",
            SqlPerfSurface::Account,
            "filtered_lower_handle_active_only",
            "guarded_expression_order_only",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 3",
        ),
        scenario(
            "account.active.lower.handle_prefix.limit3",
            SqlPerfSurface::Account,
            "filtered_lower_handle_active_only",
            "guarded_expression_prefix",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
        ),
    ]
}

fn account_tier_and_metadata_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "account.tier.in.limit3",
            SqlPerfSurface::Account,
            "filtered_tier_handle_active_only",
            "in_membership",
            "SELECT id, tier FROM PerfAuditAccount WHERE active = true AND tier IN ('gold', 'silver') ORDER BY tier ASC, handle ASC, id ASC LIMIT 3",
        ),
        scenario(
            "account.tier.not_in.limit3",
            SqlPerfSurface::Account,
            "residual_not_in",
            "not_in_membership",
            "SELECT id, tier FROM PerfAuditAccount WHERE active = true AND tier NOT IN ('gold', 'silver') ORDER BY id ASC LIMIT 3",
        ),
        scenario(
            "account.tier_gold.order_handle.limit3",
            SqlPerfSurface::Account,
            "filtered_tier_handle_active_only",
            "guarded_prefix_order_only",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 3",
        ),
        scenario(
            "account.tier_gold.lower.order_handle.limit3",
            SqlPerfSurface::Account,
            "filtered_tier_lower_handle_active_only",
            "guarded_expression_order_only",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
        ),
        scenario(
            "account.tier_gold.lower.handle_prefix.limit3",
            SqlPerfSurface::Account,
            "filtered_tier_lower_handle_active_only",
            "guarded_expression_prefix",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
        ),
        scenario(
            "account.tier_gold.handle_prefix.limit3",
            SqlPerfSurface::Account,
            "filtered_tier_handle_active_only",
            "guarded_prefix",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 3",
        ),
        scenario(
            "account.count.active_true",
            SqlPerfSurface::Account,
            "aggregate_count",
            "count",
            "SELECT COUNT(*) FROM PerfAuditAccount WHERE active = true",
        ),
        scenario(
            "account.describe",
            SqlPerfSurface::Account,
            "metadata",
            "describe",
            "DESCRIBE PerfAuditAccount",
        ),
        scenario(
            "account.show_indexes",
            SqlPerfSurface::Account,
            "metadata",
            "show_indexes",
            "SHOW INDEXES FROM PerfAuditAccount",
        ),
    ]
}

fn blob_payload_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        scenario(
            "blob.bucket.lengths.asc.limit3",
            SqlPerfSurface::Blob,
            "secondary_bucket_id",
            "blob_byte_length_projection",
            "SELECT id, label, OCTET_LENGTH(thumbnail), OCTET_LENGTH(chunk) FROM PerfAuditBlob WHERE bucket = 10 ORDER BY bucket ASC, id ASC LIMIT 3",
        ),
        scenario(
            "blob.bucket.thumbnail_payload.asc.limit3",
            SqlPerfSurface::Blob,
            "secondary_bucket_id",
            "blob_thumbnail_payload_projection",
            "SELECT id, label, thumbnail FROM PerfAuditBlob WHERE bucket = 10 ORDER BY bucket ASC, id ASC LIMIT 3",
        ),
        scenario(
            "blob.bucket.chunk_payload.asc.limit2",
            SqlPerfSurface::Blob,
            "secondary_bucket_id",
            "blob_chunk_payload_projection",
            "SELECT id, label, chunk FROM PerfAuditBlob WHERE bucket = 10 ORDER BY bucket ASC, id ASC LIMIT 2",
        ),
        scenario(
            "blob.bucket.full_payload.asc.limit2",
            SqlPerfSurface::Blob,
            "secondary_bucket_id",
            "blob_full_payload_projection",
            "SELECT id, label, thumbnail, chunk FROM PerfAuditBlob WHERE bucket = 10 ORDER BY bucket ASC, id ASC LIMIT 2",
        ),
        repeat_scenario(
            "repeat.blob.bucket.lengths.asc.limit3.runs10",
            SqlPerfSurface::Blob,
            "secondary_bucket_id",
            "blob_byte_length_repeat",
            "SELECT id, label, OCTET_LENGTH(thumbnail), OCTET_LENGTH(chunk) FROM PerfAuditBlob WHERE bucket = 10 ORDER BY bucket ASC, id ASC LIMIT 3",
            10,
        ),
    ]
}

fn repeated_query_scenarios() -> Vec<SqlPerfScenario> {
    let mut scenarios = Vec::new();
    scenarios.extend(repeated_query_baseline_scenarios());
    scenarios.extend(repeated_query_boundary_scenarios());

    scenarios
}

fn repeated_query_baseline_scenarios() -> Vec<SqlPerfScenario> {
    let mut scenarios = vec![
        repeat_scenario(
            "repeat.user.pk.order_only.asc.limit1.runs10",
            SqlPerfSurface::User,
            "primary_key",
            "repeat_baseline",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
            10,
        ),
        repeat_scenario(
            "repeat.user.pk.order_only.asc.limit2.runs10",
            SqlPerfSurface::User,
            "primary_key",
            "repeat_baseline",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
            10,
        ),
        repeat_scenario(
            "repeat.user.name.lower.order_only.asc.limit3.runs10",
            SqlPerfSurface::User,
            "expression_lower_name",
            "repeat_baseline",
            "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.grouped.age_count.limit10.runs10",
            SqlPerfSurface::User,
            "grouped_no_special_index",
            "repeat_baseline",
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
            10,
        ),
    ];

    // The 100-run SQL repeat rows are useful for manual deep-cache audits, but
    // they can exceed the IC test runner's single-message instruction cap as the runtime
    // schema authority surface grows. Keep default CI on the 10-run cache story
    // and require an explicit opt-in for the long-loop rows.
    if std::env::var_os("SQL_PERF_AUDIT_LONG_REPEAT").is_some() {
        scenarios.extend([
            repeat_scenario(
                "repeat.user.pk.order_only.asc.limit2.runs100",
                SqlPerfSurface::User,
                "primary_key",
                "repeat_baseline",
                "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
                100,
            ),
            repeat_scenario(
                "repeat.user.name.lower.order_only.asc.limit3.runs100",
                SqlPerfSurface::User,
                "expression_lower_name",
                "repeat_baseline",
                "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
                100,
            ),
            repeat_scenario(
                "repeat.user.grouped.age_count.limit10.runs100",
                SqlPerfSurface::User,
                "grouped_no_special_index",
                "repeat_baseline",
                "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
                100,
            ),
        ]);
    }

    scenarios
}

fn repeated_query_boundary_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        repeat_scenario(
            "repeat.user.age.order_only.asc.limit3.runs10",
            SqlPerfSurface::User,
            "secondary_age_id",
            "repeat_boundary",
            "SELECT id, age FROM PerfAuditUser ORDER BY age ASC, id ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.distinct.age.order_only.asc.limit3.runs10",
            SqlPerfSurface::User,
            "distinct_projection",
            "repeat_boundary",
            "SELECT DISTINCT age FROM PerfAuditUser ORDER BY age ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.case_where.order_id.limit3.runs10",
            SqlPerfSurface::User,
            "repeat_boundary",
            "repeat_boundary",
            "SELECT id, name FROM PerfAuditUser WHERE CASE WHEN age >= 30 THEN TRUE ELSE active END ORDER BY id ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.age_plus_rank.direct_order.asc.limit3.runs10",
            SqlPerfSurface::User,
            "materialized_computed_order",
            "repeat_boundary",
            "SELECT id, age FROM PerfAuditUser ORDER BY age + rank ASC, id ASC LIMIT 3",
            10,
        ),
        repeat_scenario(
            "repeat.user.grouped.age_count.no_order.runs10",
            SqlPerfSurface::User,
            "grouped_no_special_index",
            "repeat_boundary",
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age",
            10,
        ),
        repeat_scenario(
            "repeat.user.grouped.case_sum.having_alias.order.limit5.runs10",
            SqlPerfSurface::User,
            "grouped_no_special_index",
            "repeat_boundary",
            "SELECT age, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS high_count FROM PerfAuditUser GROUP BY age HAVING high_count > 0 ORDER BY high_count DESC, age ASC LIMIT 5",
            10,
        ),
        repeat_scenario(
            "repeat.account.active.lower.order_handle.asc.limit3.runs10",
            SqlPerfSurface::Account,
            "filtered_lower_handle_active_only",
            "repeat_boundary",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
            10,
        ),
    ]
}

fn sql_perf_scenarios() -> Vec<SqlPerfScenario> {
    let mut scenarios = Vec::new();
    scenarios.extend(user_primary_and_age_scenarios());
    scenarios.extend(user_name_scenarios());
    scenarios.extend(user_predicate_and_metadata_scenarios());
    scenarios.extend(account_order_scenarios());
    scenarios.extend(account_tier_and_metadata_scenarios());
    scenarios.extend(blob_payload_scenarios());
    scenarios.extend(repeated_query_scenarios());

    scenarios
}

fn print_perf_report(samples: &[SqlPerfScenarioSample]) {
    println!(
        "| Scenario | Runs | Avg Compile | Avg Execute | Grouped Stream | Grouped Fold | Grouped Finalize | GCount Hash | GCount Buckets | GCount Hits | GCount Inserts | GCount Read | GCount Lookup | GCount Update | GCount Admit | Avg data_store.get() | SQL Compile Hits | SQL Compile Misses | Shared Hits | Shared Misses | Avg Instructions | Delta | Delta % | Query |"
    );
    println!(
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    );

    for sample in samples {
        let delta_text = sample
            .avg_local_instructions_delta
            .map_or_else(|| "N/A".to_string(), |delta| format!("{delta:+}"));
        let delta_percent_text = sample.avg_local_instructions_delta_percent_bps.map_or_else(
            || "N/A".to_string(),
            |delta_bps| format!("{:+}.{:02}%", delta_bps / 100, delta_bps.abs() % 100),
        );

        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.scenario_key,
            sample.query_loop_count,
            sample.avg_compile_local_instructions,
            sample.avg_execute_local_instructions,
            sample.avg_grouped_stream_local_instructions,
            sample.avg_grouped_fold_local_instructions,
            sample.avg_grouped_finalize_local_instructions,
            sample.avg_grouped_count_borrowed_hash_computations,
            sample.avg_grouped_count_bucket_candidate_checks,
            sample.avg_grouped_count_existing_group_hits,
            sample.avg_grouped_count_new_group_inserts,
            sample.avg_grouped_count_row_materialization_local_instructions,
            sample.avg_grouped_count_group_lookup_local_instructions,
            sample.avg_grouped_count_existing_group_update_local_instructions,
            sample.avg_grouped_count_new_group_insert_local_instructions,
            sample.avg_data_store_get_calls,
            sample.avg_sql_compiled_command_cache_hits,
            sample.avg_sql_compiled_command_cache_misses,
            sample.avg_shared_query_plan_cache_hits,
            sample.avg_shared_query_plan_cache_misses,
            sample.avg_local_instructions,
            delta_text,
            delta_percent_text,
            sample.sql,
        );
    }

    println!(
        "{}",
        serde_json::to_string_pretty(samples)
            .expect("perf harness samples should serialize to JSON")
    );
}

// RepeatCacheContractCase keeps one representative repeated-SELECT contract
// case together so the IC testkit audit can assert the final two-layer repeat
// path directly instead of relying only on printed report inspection.
struct RepeatCacheContractCase {
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    sql: &'static str,
    query_loop_count: usize,
}

// WarmCacheContractCase keeps one update-then-query cache contract case
// together so the IC testkit audit can prove that a warm update call feeds the
// later compiled-plus-shared query cache path across more than one query family.
struct WarmCacheContractCase {
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    sql: &'static str,
}

// ScenarioSampleCacheExpectation names the expected cache-attribution shape for
// one sampled report row so the audit can assert the printed cohort directly.
struct ScenarioSampleCacheExpectation {
    scenario_key: &'static str,
    sql_compiled_command_cache_hits: u64,
    sql_compiled_command_cache_misses: u64,
    shared_query_plan_cache_hits: u64,
    shared_query_plan_cache_misses: u64,
}

// assert_repeat_query_uses_compiled_and_shared_cache_path proves that the ordinary
// repeated SQL SELECT path now reuses only the compiled-command cache above
// the shared lower query-plan cache.
fn assert_repeat_query_uses_compiled_and_shared_cache_path(
    fixture: &StandaloneCanisterFixture,
    case: RepeatCacheContractCase,
) {
    let result = query_surface_with_perf(fixture, case.surface, case.sql, case.query_loop_count)
        .unwrap_or_else(|err| {
            panic!(
                "repeat cache contract scenario '{}' should succeed: {err}",
                case.scenario_key,
            )
        });
    let repeated_hits =
        u64::try_from(case.query_loop_count.saturating_sub(1)).expect("loop count should fit u64");

    // Phase 1: the first pass should compile once and populate only the shared
    // lower query-plan cache during the cold entry.
    assert_eq!(
        result.attribution.cache.sql_compiled_command_misses, 1,
        "scenario '{}' should miss the SQL compiled-command cache exactly once on the cold pass",
        case.scenario_key,
    );
    assert_eq!(
        result.attribution.cache.shared_query_plan_misses, 1,
        "scenario '{}' should touch the shared lower query-plan cache exactly once on the cold pass",
        case.scenario_key,
    );

    // Phase 2: every later in-call repeat should stay on compiled-command
    // hits plus shared lower query-plan hits.
    assert_eq!(
        result.attribution.cache.sql_compiled_command_hits, repeated_hits,
        "scenario '{}' should reuse the compiled SQL artifact on every repeated pass",
        case.scenario_key,
    );
    assert_eq!(
        result.attribution.cache.shared_query_plan_hits, repeated_hits,
        "scenario '{}' should reuse the shared lower query-plan cache on every repeated pass",
        case.scenario_key,
    );
}

// sql_perf_scenario_by_key resolves one named audit row from the canonical
// scenario table so row-level contract tests assert against the same sampled
// cohort used by the printed report.
fn sql_perf_scenario_by_key(scenario_key: &str) -> SqlPerfScenario {
    sql_perf_scenarios()
        .into_iter()
        .find(|scenario| scenario.scenario_key == scenario_key)
        .unwrap_or_else(|| panic!("sql perf scenario '{scenario_key}' should exist"))
}

// assert_scenario_sample_cache_expectation checks that one sampled report row
// still carries the intended final cache attribution instead of drifting
// into a different visible cache story.
fn assert_scenario_sample_cache_expectation(
    fixture: &StandaloneCanisterFixture,
    baseline: &HashMap<String, SqlPerfBaselineRow>,
    expectation: ScenarioSampleCacheExpectation,
) {
    let sample = sample_perf_scenario(
        fixture,
        baseline,
        sql_perf_scenario_by_key(expectation.scenario_key),
    );

    assert_eq!(
        sample.avg_sql_compiled_command_cache_hits, expectation.sql_compiled_command_cache_hits,
        "scenario '{}' should keep the expected compiled-command cache hits in the sampled report row",
        expectation.scenario_key,
    );
    assert_eq!(
        sample.avg_sql_compiled_command_cache_misses, expectation.sql_compiled_command_cache_misses,
        "scenario '{}' should keep the expected compiled-command cache misses in the sampled report row",
        expectation.scenario_key,
    );
    assert_eq!(
        sample.avg_shared_query_plan_cache_hits, expectation.shared_query_plan_cache_hits,
        "scenario '{}' should keep the expected shared lower query-plan cache hits in the sampled report row",
        expectation.scenario_key,
    );
    assert_eq!(
        sample.avg_shared_query_plan_cache_misses, expectation.shared_query_plan_cache_misses,
        "scenario '{}' should keep the expected shared lower query-plan cache misses in the sampled report row",
        expectation.scenario_key,
    );
}

// assert_repeat_scenario_sample_keeps_compiled_and_shared_cache_story proves that one
// sampled repeat row in the printed audit cohort now shows compiled SQL cache
// hits plus shared lower query-plan hits.
fn assert_repeat_scenario_sample_keeps_compiled_and_shared_cache_story(
    fixture: &StandaloneCanisterFixture,
    baseline: &HashMap<String, SqlPerfBaselineRow>,
    scenario: SqlPerfScenario,
) {
    let repeated_hits =
        u64::try_from(scenario.query_loop_count.saturating_sub(1)).expect("loop count should fit");
    let sample = sample_perf_scenario(fixture, baseline, scenario);

    assert_eq!(
        sample.avg_sql_compiled_command_cache_hits, repeated_hits,
        "scenario '{}' should keep SQL compiled-command hits for every repeated pass",
        sample.scenario_key,
    );
    assert_eq!(
        sample.avg_sql_compiled_command_cache_misses, 1,
        "scenario '{}' should keep exactly one cold SQL compiled-command miss in the sampled repeat row",
        sample.scenario_key,
    );
    assert_eq!(
        sample.avg_shared_query_plan_cache_hits, repeated_hits,
        "scenario '{}' should surface shared lower query-plan hits on every repeated pass",
        sample.scenario_key,
    );
    assert_eq!(
        sample.avg_shared_query_plan_cache_misses, 1,
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
        &attribution.pure_covering,
        cache.sql_compiled_command_hits,
        cache.sql_compiled_command_misses,
        cache.shared_query_plan_hits,
        cache.shared_query_plan_misses,
    );
}

fn print_fluent_limit_one_attribution(label: &str, perf: &FluentQueryPerfResult) {
    let attribution = &perf.attribution;

    println!(
        "{label} fluent attributed limit1: compile={} plan_lookup={} executor_invocation={} load_plan={} row_layout={} continuation={} handoff={} route_plan={} runtime_prepare={} runtime={} finalize={} response_finalize={} response_decode={} execute={} total={} shared_hits={} shared_misses={} direct={:?}",
        attribution.compile_local_instructions,
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
        &attribution.direct_data_row,
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
            25_000_000,
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

#[test]
#[ignore = "manual PocketIC perf report; correctness/cache contracts stay in focused tests"]
fn sql_perf_audit_harness_reports_instruction_samples() {
    let fixture = install_sql_perf_canister_fixture();
    let baseline = load_baseline_rows();
    reset_sql_perf_fixtures(&fixture);

    let samples = sql_perf_scenarios()
        .into_iter()
        .map(|scenario| sample_perf_scenario(&fixture, &baseline, scenario))
        .collect::<Vec<_>>();

    maybe_write_blessed_baseline(&samples);

    for sample in &samples {
        assert!(
            sample.first_local_instructions > 0,
            "scenario '{}' should report a positive first instruction delta",
            sample.scenario_key,
        );
        assert!(
            sample.avg_compile_local_instructions > 0,
            "scenario '{}' should report a positive average compile instruction delta",
            sample.scenario_key,
        );
        assert!(
            sample.avg_execute_local_instructions > 0,
            "scenario '{}' should report a positive average execute instruction delta",
            sample.scenario_key,
        );
        assert!(
            sample.min_local_instructions > 0,
            "scenario '{}' should report a positive min instruction delta",
            sample.scenario_key,
        );
        assert!(
            sample.outcome_stable,
            "scenario '{}' should keep a stable summarized result across repeats",
            sample.scenario_key,
        );
    }

    print_perf_report(&samples);
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
            scenario_key: "user.grouped.age_count.limit10.warm_after_update",
            surface: SqlPerfSurface::User,
            sql: "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
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
fn sql_perf_repeat_queries_stay_on_the_compiled_and_shared_cache_path() {
    let fixture = install_sql_perf_canister_fixture();

    for case in [
        RepeatCacheContractCase {
            scenario_key: "repeat.user.pk.order_only.asc.limit1.runs10",
            surface: SqlPerfSurface::User,
            sql: "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
            query_loop_count: 10,
        },
        RepeatCacheContractCase {
            scenario_key: "repeat.user.pk.order_only.asc.limit2.runs10",
            surface: SqlPerfSurface::User,
            sql: "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
            query_loop_count: 10,
        },
        RepeatCacheContractCase {
            scenario_key: "repeat.user.name.lower.order_only.asc.limit3.runs10",
            surface: SqlPerfSurface::User,
            sql: "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
            query_loop_count: 10,
        },
        RepeatCacheContractCase {
            scenario_key: "repeat.user.grouped.age_count.limit10.runs10",
            surface: SqlPerfSurface::User,
            sql: "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
            query_loop_count: 10,
        },
    ] {
        reset_sql_perf_fixtures(&fixture);
        assert_repeat_query_uses_compiled_and_shared_cache_path(&fixture, case);
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
}

#[test]
fn sql_perf_named_report_rows_keep_the_compiled_and_shared_cache_story() {
    let fixture = install_sql_perf_canister_fixture();
    let baseline = HashMap::new();
    reset_sql_perf_fixtures(&fixture);

    // Phase 1: every sampled repeat row should keep the same compiled-plus-shared
    // cache story, even for guarded, grouped, DISTINCT, CASE, and expression-order
    // variants that could otherwise blur attribution.
    for scenario in repeated_query_scenarios() {
        reset_sql_perf_fixtures(&fixture);
        assert_repeat_scenario_sample_keeps_compiled_and_shared_cache_story(
            &fixture, &baseline, scenario,
        );
    }

    // Phase 2: the isolated cold/warm parity rows should keep showing the
    // intended transition from one cold fill to one later compiled/shared hit path.
    for expectation in [
        ScenarioSampleCacheExpectation {
            scenario_key: "user.age.order_only.asc.limit2.cold_query",
            sql_compiled_command_cache_hits: 0,
            sql_compiled_command_cache_misses: 1,
            shared_query_plan_cache_hits: 0,
            shared_query_plan_cache_misses: 1,
        },
        ScenarioSampleCacheExpectation {
            scenario_key: "user.age.order_only.asc.limit2.warm_after_update",
            sql_compiled_command_cache_hits: 1,
            sql_compiled_command_cache_misses: 0,
            shared_query_plan_cache_hits: 1,
            shared_query_plan_cache_misses: 0,
        },
        ScenarioSampleCacheExpectation {
            scenario_key: "user.grouped.case_sum.having_alias.order.limit5.cold_query",
            sql_compiled_command_cache_hits: 0,
            sql_compiled_command_cache_misses: 1,
            shared_query_plan_cache_hits: 0,
            shared_query_plan_cache_misses: 1,
        },
        ScenarioSampleCacheExpectation {
            scenario_key: "user.grouped.case_sum.having_alias.order.limit5.warm_after_update",
            sql_compiled_command_cache_hits: 1,
            sql_compiled_command_cache_misses: 0,
            shared_query_plan_cache_hits: 1,
            shared_query_plan_cache_misses: 0,
        },
        ScenarioSampleCacheExpectation {
            scenario_key: "account.active.lower.order_handle.asc.limit3.cold_query",
            sql_compiled_command_cache_hits: 0,
            sql_compiled_command_cache_misses: 1,
            shared_query_plan_cache_hits: 0,
            shared_query_plan_cache_misses: 1,
        },
        ScenarioSampleCacheExpectation {
            scenario_key: "account.active.lower.order_handle.asc.limit3.warm_after_update",
            sql_compiled_command_cache_hits: 1,
            sql_compiled_command_cache_misses: 0,
            shared_query_plan_cache_hits: 1,
            shared_query_plan_cache_misses: 0,
        },
    ] {
        assert_scenario_sample_cache_expectation(&fixture, &baseline, expectation);
    }
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
fn sql_perf_explain_queries_report_phase_breakdown() {
    let fixture = install_sql_perf_canister_fixture();
    let baseline = load_baseline_rows();

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
        let baseline_row = baseline
            .get(scenario_key)
            .unwrap_or_else(|| panic!("baseline should contain '{scenario_key}'"));
        let compile_delta = i128::from(perf.attribution.compile_local_instructions)
            - i128::from(baseline_row.avg_compile_local_instructions);
        let execute_delta = i128::from(perf.attribution.execute_local_instructions)
            - i128::from(baseline_row.avg_execute_local_instructions);
        let total_delta = i128::from(perf.attribution.total_local_instructions)
            - i128::from(baseline_row.avg_local_instructions);

        println!(
            "{scenario_key}: compile={} baseline_compile={} delta_compile={} planner={} store={} executor={} execute={} baseline_execute={} delta_execute={} total={} baseline_total={} delta_total={}",
            perf.attribution.compile_local_instructions,
            baseline_row.avg_compile_local_instructions,
            compile_delta,
            perf.attribution.execution.planner_local_instructions,
            perf.attribution.execution.store_local_instructions,
            perf.attribution.execution.executor_local_instructions,
            perf.attribution.execute_local_instructions,
            baseline_row.avg_execute_local_instructions,
            execute_delta,
            perf.attribution.total_local_instructions,
            baseline_row.avg_local_instructions,
            total_delta,
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
    let baseline = load_baseline_rows();

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
        let baseline_row = baseline
            .get(scenario_key)
            .unwrap_or_else(|| panic!("baseline should contain '{scenario_key}'"));
        let compile_delta = i128::from(perf.attribution.compile_local_instructions)
            - i128::from(baseline_row.avg_compile_local_instructions);
        let execute_delta = i128::from(perf.attribution.execute_local_instructions)
            - i128::from(baseline_row.avg_execute_local_instructions);
        let total_delta = i128::from(perf.attribution.total_local_instructions)
            - i128::from(baseline_row.avg_local_instructions);

        println!(
            "{scenario_key}: compile={} baseline_compile={} delta_compile={} key={} lookup={} parse={} tokenize={} select={} expr={} predicate={} agg_check={} prepare={} lower={} bind={} planner={} store={} executor={} execute={} baseline_execute={} delta_execute={} total={} baseline_total={} delta_total={} pure={:?} compiled_hits={} compiled_misses={} shared_hits={} shared_misses={}",
            perf.attribution.compile_local_instructions,
            baseline_row.avg_compile_local_instructions,
            compile_delta,
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
            baseline_row.avg_execute_local_instructions,
            execute_delta,
            perf.attribution.total_local_instructions,
            baseline_row.avg_local_instructions,
            total_delta,
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
