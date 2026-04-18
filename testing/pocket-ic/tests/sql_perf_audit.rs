use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use candid::CandidType;
use canic_testkit::pic::{StandaloneCanisterFixture, install_prebuilt_canister};
use icydb::{
    Error,
    db::{SqlQueryExecutionAttribution, sql::SqlQueryResult},
};
use icydb_testing_integration::build_canister;
use serde::{Deserialize, Serialize};

// Mirror the dedicated perf-audit query envelope so PocketIC can decode the
// query result plus the compile/execute instruction split from the canister.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    attribution: SqlQueryExecutionAttribution,
}

#[derive(Clone, Copy, Debug)]
enum SqlPerfSurface {
    User,
    Account,
}

impl SqlPerfSurface {
    const fn label(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Account => "account",
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
    avg_store_get_calls: u64,
    avg_sql_compiled_command_cache_hits: u64,
    avg_sql_compiled_command_cache_misses: u64,
    avg_sql_select_plan_cache_hits: u64,
    avg_sql_select_plan_cache_misses: u64,
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
    let wasm_path =
        build_canister("sql_perf").expect("sql_perf canister should build for PocketIC tests");
    let wasm = fs::read(&wasm_path)
        .unwrap_or_else(|err| panic!("failed to read built sql_perf canister wasm: {err}"));

    install_prebuilt_canister(
        wasm,
        candid::encode_args(()).expect("encode empty init args"),
    )
}

fn reset_sql_perf_fixtures(fixture: &StandaloneCanisterFixture) {
    // Phase 1: clear any retained state from an earlier scenario batch.
    let reset: Result<(), Error> = fixture
        .pic()
        .update_call(fixture.canister_id(), "fixtures_reset", ())
        .expect("fixtures_reset should decode");
    reset.expect("fixtures_reset should succeed");

    // Phase 2: reload the deterministic perf fixture window before sampling.
    let load: Result<(), Error> = fixture
        .pic()
        .update_call(fixture.canister_id(), "fixtures_load_default", ())
        .expect("fixtures_load_default should decode");
    load.expect("fixtures_load_default should succeed");
}

fn query_surface_with_perf(
    fixture: &StandaloneCanisterFixture,
    surface: SqlPerfSurface,
    sql: &str,
    query_loop_count: usize,
) -> Result<SqlQueryPerfResult, Error> {
    match surface {
        SqlPerfSurface::User if query_loop_count == 1 => fixture
            .pic()
            .query_call(
                fixture.canister_id(),
                "query_user_with_perf",
                (sql.to_string(),),
            )
            .expect("query_user_with_perf should decode"),
        SqlPerfSurface::User => fixture
            .pic()
            .query_call(
                fixture.canister_id(),
                "query_user_loop_with_perf",
                (
                    sql.to_string(),
                    u32::try_from(query_loop_count)
                        .expect("query loop count should fit into canister argument"),
                ),
            )
            .expect("query_user_loop_with_perf should decode"),
        SqlPerfSurface::Account if query_loop_count == 1 => fixture
            .pic()
            .query_call(
                fixture.canister_id(),
                "query_account_with_perf",
                (sql.to_string(),),
            )
            .expect("query_account_with_perf should decode"),
        SqlPerfSurface::Account => fixture
            .pic()
            .query_call(
                fixture.canister_id(),
                "query_account_loop_with_perf",
                (
                    sql.to_string(),
                    u32::try_from(query_loop_count)
                        .expect("query loop count should fit into canister argument"),
                ),
            )
            .expect("query_account_loop_with_perf should decode"),
    }
}

fn warm_query_surface_with_perf(
    fixture: &StandaloneCanisterFixture,
    surface: SqlPerfSurface,
    sql: &str,
) -> Result<SqlQueryPerfResult, Error> {
    match surface {
        SqlPerfSurface::User => fixture
            .pic()
            .update_call(
                fixture.canister_id(),
                "warm_user_query_with_perf",
                (sql.to_string(),),
            )
            .expect("warm_user_query_with_perf should decode"),
        SqlPerfSurface::Account => fixture
            .pic()
            .update_call(
                fixture.canister_id(),
                "warm_account_query_with_perf",
                (sql.to_string(),),
            )
            .expect("warm_account_query_with_perf should decode"),
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
        SqlQueryResult::ShowEntities { entities } => SqlPerfOutcome {
            result_kind: "show_entities",
            entity: String::new(),
            row_count: entities.len(),
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
        self.borrowed_hash
            .push(attribution.grouped_count_borrowed_hash_computations);
        self.bucket_checks
            .push(attribution.grouped_count_bucket_candidate_checks);
        self.existing_hits
            .push(attribution.grouped_count_existing_group_hits);
        self.new_inserts
            .push(attribution.grouped_count_new_group_inserts);
        self.row_materialization_local_instructions
            .push(attribution.grouped_count_row_materialization_local_instructions);
        self.group_lookup_local_instructions
            .push(attribution.grouped_count_group_lookup_local_instructions);
        self.existing_group_update_local_instructions
            .push(attribution.grouped_count_existing_group_update_local_instructions);
        self.new_group_insert_local_instructions
            .push(attribution.grouped_count_new_group_insert_local_instructions);
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
    store_get_call_samples: Vec<u64>,
    sql_compiled_command_cache_hit_samples: Vec<u64>,
    sql_compiled_command_cache_miss_samples: Vec<u64>,
    sql_select_plan_cache_hit_samples: Vec<u64>,
    sql_select_plan_cache_miss_samples: Vec<u64>,
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
            store_get_call_samples: Vec::with_capacity(sample_count),
            sql_compiled_command_cache_hit_samples: Vec::with_capacity(sample_count),
            sql_compiled_command_cache_miss_samples: Vec::with_capacity(sample_count),
            sql_select_plan_cache_hit_samples: Vec::with_capacity(sample_count),
            sql_select_plan_cache_miss_samples: Vec::with_capacity(sample_count),
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
        self.grouped_stream_samples
            .push(sample.attribution.grouped_stream_local_instructions);
        self.grouped_fold_samples
            .push(sample.attribution.grouped_fold_local_instructions);
        self.grouped_finalize_samples
            .push(sample.attribution.grouped_finalize_local_instructions);
        self.grouped_count.record(&sample.attribution);
        self.store_get_call_samples
            .push(sample.attribution.store_get_calls);
        self.sql_compiled_command_cache_hit_samples
            .push(sample.attribution.sql_compiled_command_cache_hits);
        self.sql_compiled_command_cache_miss_samples
            .push(sample.attribution.sql_compiled_command_cache_misses);
        self.sql_select_plan_cache_hit_samples
            .push(sample.attribution.sql_select_plan_cache_hits);
        self.sql_select_plan_cache_miss_samples
            .push(sample.attribution.sql_select_plan_cache_misses);
        self.shared_query_plan_cache_hit_samples
            .push(sample.attribution.shared_query_plan_cache_hits);
        self.shared_query_plan_cache_miss_samples
            .push(sample.attribution.shared_query_plan_cache_misses);
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
    let avg_store_get_calls = average_u64(&raw.store_get_call_samples);
    let avg_sql_compiled_command_cache_hits =
        average_u64(&raw.sql_compiled_command_cache_hit_samples);
    let avg_sql_compiled_command_cache_misses =
        average_u64(&raw.sql_compiled_command_cache_miss_samples);
    let avg_sql_select_plan_cache_hits = average_u64(&raw.sql_select_plan_cache_hit_samples);
    let avg_sql_select_plan_cache_misses = average_u64(&raw.sql_select_plan_cache_miss_samples);
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
        avg_store_get_calls,
        avg_sql_compiled_command_cache_hits,
        avg_sql_compiled_command_cache_misses,
        avg_sql_select_plan_cache_hits,
        avg_sql_select_plan_cache_misses,
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

fn user_primary_and_age_scenarios() -> Vec<SqlPerfScenario> {
    vec![
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
            "SHOW INDEXES PerfAuditUser",
        ),
        scenario(
            "user.show_columns",
            SqlPerfSurface::User,
            "metadata",
            "show_columns",
            "SHOW COLUMNS PerfAuditUser",
        ),
        scenario(
            "user.show_tables",
            SqlPerfSurface::User,
            "metadata",
            "show_tables",
            "SHOW TABLES",
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
            "SHOW INDEXES PerfAuditAccount",
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
    vec![
        repeat_scenario(
            "repeat.user.pk.order_only.asc.limit2.runs10",
            SqlPerfSurface::User,
            "primary_key",
            "repeat_baseline",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
            10,
        ),
        repeat_scenario(
            "repeat.user.pk.order_only.asc.limit2.runs100",
            SqlPerfSurface::User,
            "primary_key",
            "repeat_baseline",
            "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2",
            100,
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
            "repeat.user.name.lower.order_only.asc.limit3.runs100",
            SqlPerfSurface::User,
            "expression_lower_name",
            "repeat_baseline",
            "SELECT id, name FROM PerfAuditUser ORDER BY LOWER(name) ASC, id ASC LIMIT 3",
            100,
        ),
        repeat_scenario(
            "repeat.user.grouped.age_count.limit10.runs10",
            SqlPerfSurface::User,
            "grouped_no_special_index",
            "repeat_baseline",
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
            10,
        ),
        repeat_scenario(
            "repeat.user.grouped.age_count.limit10.runs100",
            SqlPerfSurface::User,
            "grouped_no_special_index",
            "repeat_baseline",
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
            100,
        ),
    ]
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
    scenarios.extend(repeated_query_scenarios());

    scenarios
}

fn print_perf_report(samples: &[SqlPerfScenarioSample]) {
    println!(
        "| Scenario | Runs | Avg Compile | Avg Execute | Grouped Stream | Grouped Fold | Grouped Finalize | GCount Hash | GCount Buckets | GCount Hits | GCount Inserts | GCount Read | GCount Lookup | GCount Update | GCount Admit | Avg store.get() | SQL Compile Hits | SQL Compile Misses | SQL Select Hits | SQL Select Misses | Shared Hits | Shared Misses | Avg Instructions | Delta | Delta % | Query |"
    );
    println!(
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
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
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
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
            sample.avg_store_get_calls,
            sample.avg_sql_compiled_command_cache_hits,
            sample.avg_sql_compiled_command_cache_misses,
            sample.avg_sql_select_plan_cache_hits,
            sample.avg_sql_select_plan_cache_misses,
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
// case together so the PocketIC audit can assert the SQL-owned repeat path
// directly instead of relying only on printed report inspection.
struct RepeatCacheContractCase {
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    sql: &'static str,
    query_loop_count: usize,
}

// WarmCacheContractCase keeps one update-then-query cache contract case
// together so the PocketIC audit can prove that a warm update call feeds the
// later SQL-owned query cache path across more than one query family.
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
    sql_select_plan_cache_hits: u64,
    sql_select_plan_cache_misses: u64,
    shared_query_plan_cache_hits: u64,
    shared_query_plan_cache_misses: u64,
}

// assert_repeat_query_uses_sql_owned_cache_path proves that the ordinary
// repeated SQL SELECT path still reuses the compiled-command and SQL
// prepared-select caches, while the lower shared query-plan cache remains a
// one-time support boundary during the cold fill.
fn assert_repeat_query_uses_sql_owned_cache_path(
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

    // Phase 1: the first pass should compile and prepare the SQL-owned
    // artifacts exactly once for the cold entry.
    assert_eq!(
        result.attribution.sql_compiled_command_cache_misses, 1,
        "scenario '{}' should miss the SQL compiled-command cache exactly once on the cold pass",
        case.scenario_key,
    );
    assert_eq!(
        result.attribution.sql_select_plan_cache_misses, 1,
        "scenario '{}' should miss the SQL select-plan cache exactly once on the cold pass",
        case.scenario_key,
    );
    assert_eq!(
        result.attribution.shared_query_plan_cache_misses, 1,
        "scenario '{}' should touch the shared lower query-plan cache exactly once while filling the SQL-owned prepared-select boundary",
        case.scenario_key,
    );

    // Phase 2: every later in-call repeat should stay on the SQL-owned cache
    // path rather than leaning on the lower shared query-plan cache.
    assert_eq!(
        result.attribution.sql_compiled_command_cache_hits, repeated_hits,
        "scenario '{}' should reuse the compiled SQL artifact on every repeated pass",
        case.scenario_key,
    );
    assert_eq!(
        result.attribution.sql_select_plan_cache_hits, repeated_hits,
        "scenario '{}' should reuse the SQL prepared-select artifact on every repeated pass",
        case.scenario_key,
    );
    assert_eq!(
        result.attribution.shared_query_plan_cache_hits, 0,
        "scenario '{}' should not treat the shared lower query-plan cache as the repeat-path owner",
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
// still carries the intended SQL-owned cache attribution instead of drifting
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
        sample.avg_sql_select_plan_cache_hits, expectation.sql_select_plan_cache_hits,
        "scenario '{}' should keep the expected SQL select-plan cache hits in the sampled report row",
        expectation.scenario_key,
    );
    assert_eq!(
        sample.avg_sql_select_plan_cache_misses, expectation.sql_select_plan_cache_misses,
        "scenario '{}' should keep the expected SQL select-plan cache misses in the sampled report row",
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

// assert_repeat_scenario_sample_keeps_sql_owned_cache_story proves that one
// sampled repeat row in the printed audit cohort still uses the SQL-owned
// caches for every repeated pass and never surfaces shared lower cache hits.
fn assert_repeat_scenario_sample_keeps_sql_owned_cache_story(
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
        sample.avg_sql_select_plan_cache_hits, repeated_hits,
        "scenario '{}' should keep SQL prepared-select hits for every repeated pass",
        sample.scenario_key,
    );
    assert_eq!(
        sample.avg_sql_select_plan_cache_misses, 1,
        "scenario '{}' should keep exactly one cold SQL prepared-select miss in the sampled repeat row",
        sample.scenario_key,
    );
    assert_eq!(
        sample.avg_shared_query_plan_cache_hits, 0,
        "scenario '{}' should not surface shared lower query-plan hits on the repeat path",
        sample.scenario_key,
    );
    assert_eq!(
        sample.avg_shared_query_plan_cache_misses, 1,
        "scenario '{}' should touch the shared lower query-plan cache only once as cold-fill support",
        sample.scenario_key,
    );
}

// assert_update_warm_persists_sql_owned_cache_path proves that an update-side
// warm call still fills the SQL-owned caches for the later query-side call,
// while the shared lower query-plan cache remains cold-fill support work
// rather than becoming the visible query reuse layer.
fn assert_update_warm_persists_sql_owned_cache_path(
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

    // Phase 1: the update-side warm call should populate the SQL-owned caches
    // and touch the lower shared cache only once for cold-fill support.
    assert_eq!(
        warm.attribution.sql_compiled_command_cache_misses, 1,
        "scenario '{}' should populate the SQL compiled-command cache on the update warm pass",
        case.scenario_key,
    );
    assert_eq!(
        warm.attribution.sql_select_plan_cache_misses, 1,
        "scenario '{}' should populate the SQL select-plan cache on the update warm pass",
        case.scenario_key,
    );
    assert_eq!(
        warm.attribution.shared_query_plan_cache_misses, 1,
        "scenario '{}' should touch the shared lower query-plan cache only once during the update warm cold fill",
        case.scenario_key,
    );

    // Phase 2: the later query call should stay entirely on the SQL-owned hit
    // path instead of leaning on the lower shared cache.
    let query = query_surface_with_perf(fixture, case.surface, case.sql, 1).unwrap_or_else(|err| {
        panic!(
            "query cache contract scenario '{}' should succeed after update warm: {err}",
            case.scenario_key,
        )
    });
    assert_eq!(
        query.attribution.sql_compiled_command_cache_hits, 1,
        "scenario '{}' should reuse the compiled SQL artifact warmed by the update call",
        case.scenario_key,
    );
    assert_eq!(
        query.attribution.sql_compiled_command_cache_misses, 0,
        "scenario '{}' should not recompile the warmed SQL artifact on the later query call",
        case.scenario_key,
    );
    assert_eq!(
        query.attribution.sql_select_plan_cache_hits, 1,
        "scenario '{}' should reuse the SQL prepared-select artifact warmed by the update call",
        case.scenario_key,
    );
    assert_eq!(
        query.attribution.sql_select_plan_cache_misses, 0,
        "scenario '{}' should not rebuild the warmed SQL prepared-select artifact on the later query call",
        case.scenario_key,
    );
    assert_eq!(
        query.attribution.shared_query_plan_cache_hits, 0,
        "scenario '{}' should not surface shared lower query-plan hits on the later query call",
        case.scenario_key,
    );
    assert_eq!(
        query.attribution.shared_query_plan_cache_misses, 0,
        "scenario '{}' should not rebuild the lower shared query plan on the later query call",
        case.scenario_key,
    );
}

#[test]
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
fn sql_perf_update_warm_persists_query_cache_across_calls() {
    let fixture = install_sql_perf_canister_fixture();

    for case in [
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
        assert_update_warm_persists_sql_owned_cache_path(&fixture, case);
    }
}

#[test]
fn sql_perf_repeat_queries_stay_on_the_sql_owned_cache_path() {
    let fixture = install_sql_perf_canister_fixture();

    for case in [
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
        assert_repeat_query_uses_sql_owned_cache_path(&fixture, case);
    }
}

#[test]
fn sql_perf_named_report_rows_keep_the_sql_owned_cache_story() {
    let fixture = install_sql_perf_canister_fixture();
    let baseline = HashMap::new();
    reset_sql_perf_fixtures(&fixture);

    // Phase 1: every sampled repeat row should keep the same SQL-owned cache
    // story, even for guarded, grouped, DISTINCT, CASE, and expression-order
    // variants that could otherwise blur attribution.
    for scenario in repeated_query_scenarios() {
        reset_sql_perf_fixtures(&fixture);
        assert_repeat_scenario_sample_keeps_sql_owned_cache_story(&fixture, &baseline, scenario);
    }

    // Phase 2: the isolated cold/warm parity rows should keep showing the
    // intended transition from one cold fill to one later SQL-owned hit path.
    for expectation in [
        ScenarioSampleCacheExpectation {
            scenario_key: "user.age.order_only.asc.limit2.cold_query",
            sql_compiled_command_cache_hits: 0,
            sql_compiled_command_cache_misses: 1,
            sql_select_plan_cache_hits: 0,
            sql_select_plan_cache_misses: 1,
            shared_query_plan_cache_hits: 0,
            shared_query_plan_cache_misses: 1,
        },
        ScenarioSampleCacheExpectation {
            scenario_key: "user.age.order_only.asc.limit2.warm_after_update",
            sql_compiled_command_cache_hits: 1,
            sql_compiled_command_cache_misses: 0,
            sql_select_plan_cache_hits: 1,
            sql_select_plan_cache_misses: 0,
            shared_query_plan_cache_hits: 0,
            shared_query_plan_cache_misses: 0,
        },
        ScenarioSampleCacheExpectation {
            scenario_key: "user.grouped.case_sum.having_alias.order.limit5.cold_query",
            sql_compiled_command_cache_hits: 0,
            sql_compiled_command_cache_misses: 1,
            sql_select_plan_cache_hits: 0,
            sql_select_plan_cache_misses: 1,
            shared_query_plan_cache_hits: 0,
            shared_query_plan_cache_misses: 1,
        },
        ScenarioSampleCacheExpectation {
            scenario_key: "user.grouped.case_sum.having_alias.order.limit5.warm_after_update",
            sql_compiled_command_cache_hits: 1,
            sql_compiled_command_cache_misses: 0,
            sql_select_plan_cache_hits: 1,
            sql_select_plan_cache_misses: 0,
            shared_query_plan_cache_hits: 0,
            shared_query_plan_cache_misses: 0,
        },
        ScenarioSampleCacheExpectation {
            scenario_key: "account.active.lower.order_handle.asc.limit3.cold_query",
            sql_compiled_command_cache_hits: 0,
            sql_compiled_command_cache_misses: 1,
            sql_select_plan_cache_hits: 0,
            sql_select_plan_cache_misses: 1,
            shared_query_plan_cache_hits: 0,
            shared_query_plan_cache_misses: 1,
        },
        ScenarioSampleCacheExpectation {
            scenario_key: "account.active.lower.order_handle.asc.limit3.warm_after_update",
            sql_compiled_command_cache_hits: 1,
            sql_compiled_command_cache_misses: 0,
            sql_select_plan_cache_hits: 1,
            sql_select_plan_cache_misses: 0,
            shared_query_plan_cache_hits: 0,
            shared_query_plan_cache_misses: 0,
        },
    ] {
        assert_scenario_sample_cache_expectation(&fixture, &baseline, expectation);
    }
}
