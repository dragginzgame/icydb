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
struct SqlPerfScenario {
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    index_family: &'static str,
    query_family: &'static str,
    sql: &'static str,
    sample_count: usize,
    query_loop_count: usize,
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

// SqlPerfRawSamples keeps one scenario's repeated raw counters together so the
// report builder can collapse them without passing a long list of slices.
struct SqlPerfRawSamples {
    compile_samples: Vec<u64>,
    execute_samples: Vec<u64>,
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
            "account.active.lower.order_handle.asc.limit3",
            SqlPerfSurface::Account,
            "filtered_lower_handle_active_only",
            "guarded_expression_order_only",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 3",
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
        scenario(
            "account.active.handle_prefix.limit3",
            SqlPerfSurface::Account,
            "filtered_handle_active_only",
            "guarded_prefix",
            "SELECT id, handle FROM PerfAuditAccount WHERE active = true AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 3",
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
        "| Scenario | Runs | Avg Compile | Avg Execute | SQL Compile Hits | SQL Compile Misses | SQL Select Hits | SQL Select Misses | Shared Hits | Shared Misses | Avg Instructions | Delta | Delta % | Query |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|");

    for sample in samples {
        let delta_text = sample
            .avg_local_instructions_delta
            .map_or_else(|| "N/A".to_string(), |delta| format!("{delta:+}"));
        let delta_percent_text = sample.avg_local_instructions_delta_percent_bps.map_or_else(
            || "N/A".to_string(),
            |delta_bps| format!("{:+}.{:02}%", delta_bps / 100, delta_bps.abs() % 100),
        );

        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.scenario_key,
            sample.query_loop_count,
            sample.avg_compile_local_instructions,
            sample.avg_execute_local_instructions,
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
    reset_sql_perf_fixtures(&fixture);

    let sql = "SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 2";
    let warm = warm_query_surface_with_perf(&fixture, SqlPerfSurface::User, sql)
        .expect("update warm SQL query should succeed");
    assert_eq!(
        warm.attribution.sql_compiled_command_cache_misses, 1,
        "the update warm call should populate the SQL compiled-command cache on its cold pass",
    );
    assert_eq!(
        warm.attribution.sql_select_plan_cache_misses, 1,
        "the update warm call should populate the SQL select-plan cache on its cold pass",
    );

    let query = query_surface_with_perf(&fixture, SqlPerfSurface::User, sql, 1)
        .expect("query call should succeed after update warm");
    assert_eq!(
        query.attribution.sql_compiled_command_cache_hits, 1,
        "the later query call should reuse the compiled SQL artifact warmed by the update call",
    );
    assert_eq!(
        query.attribution.sql_compiled_command_cache_misses, 0,
        "the later query call should not recompile the warmed SQL artifact",
    );
    assert_eq!(
        query.attribution.sql_select_plan_cache_hits, 1,
        "the later query call should reuse the SQL select plan warmed by the update call",
    );
    assert_eq!(
        query.attribution.sql_select_plan_cache_misses, 0,
        "the later query call should not rebuild the warmed SQL select plan",
    );
}
