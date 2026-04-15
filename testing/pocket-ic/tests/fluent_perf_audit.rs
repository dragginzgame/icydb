use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use candid::CandidType;
use canic_testkit::pic::{StandaloneCanisterFixture, install_prebuilt_canister};
use icydb::{Error, db::QueryExecutionAttribution};
use icydb_testing_integration::build_canister;
use serde::{Deserialize, Serialize};

// Dedicated reduced fluent audit outcome keeps the recurring baseline stable
// without serializing full row payloads through the perf harness.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FluentQueryPerfOutcome {
    result_kind: String,
    entity: String,
    row_count: u32,
}

// One fluent perf sample carries the reduced outcome plus the shared
// compile/execute attribution split reported by the canister endpoint.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct FluentQueryPerfResult {
    outcome: FluentQueryPerfOutcome,
    attribution: QueryExecutionAttribution,
}

#[derive(Clone, Copy, Debug)]
enum FluentPerfSurface {
    User,
    Account,
}

impl FluentPerfSurface {
    const fn label(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Account => "account",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum FluentPerfSampleMode {
    QueryOnly,
    WarmThenQuery,
}

// Scenario metadata separates the stable baseline/report key from the actual
// canister dispatcher key so repeat rows can share one underlying query shape.
#[derive(Clone, Copy, Debug)]
struct FluentPerfScenario {
    scenario_key: &'static str,
    canister_scenario_key: &'static str,
    surface: FluentPerfSurface,
    query_family: &'static str,
    query_label: &'static str,
    sample_count: usize,
    query_loop_count: usize,
    sample_mode: FluentPerfSampleMode,
    isolated_fixture: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FluentPerfBaselineRow {
    scenario_key: String,
    #[serde(default)]
    avg_compile_local_instructions: u64,
    #[serde(default)]
    avg_runtime_local_instructions: u64,
    #[serde(default)]
    avg_finalize_local_instructions: u64,
    #[serde(default)]
    avg_direct_data_row_scan_local_instructions: u64,
    #[serde(default)]
    avg_direct_data_row_key_stream_local_instructions: u64,
    #[serde(default)]
    avg_direct_data_row_row_read_local_instructions: u64,
    #[serde(default)]
    avg_direct_data_row_key_encode_local_instructions: u64,
    #[serde(default)]
    avg_direct_data_row_store_get_local_instructions: u64,
    #[serde(default)]
    avg_direct_data_row_order_window_local_instructions: u64,
    #[serde(default)]
    avg_direct_data_row_page_window_local_instructions: u64,
    #[serde(default)]
    avg_response_decode_local_instructions: u64,
    #[serde(default)]
    avg_execute_local_instructions: u64,
    avg_local_instructions: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct FluentPerfScenarioSample {
    scenario_key: String,
    surface: String,
    query_family: String,
    query_label: String,
    query_loop_count: usize,
    baseline_avg_compile_local_instructions: Option<u64>,
    baseline_avg_runtime_local_instructions: Option<u64>,
    baseline_avg_finalize_local_instructions: Option<u64>,
    baseline_avg_direct_data_row_scan_local_instructions: Option<u64>,
    baseline_avg_direct_data_row_key_stream_local_instructions: Option<u64>,
    baseline_avg_direct_data_row_row_read_local_instructions: Option<u64>,
    baseline_avg_direct_data_row_key_encode_local_instructions: Option<u64>,
    baseline_avg_direct_data_row_store_get_local_instructions: Option<u64>,
    baseline_avg_direct_data_row_order_window_local_instructions: Option<u64>,
    baseline_avg_direct_data_row_page_window_local_instructions: Option<u64>,
    baseline_avg_response_decode_local_instructions: Option<u64>,
    baseline_avg_execute_local_instructions: Option<u64>,
    baseline_avg_local_instructions: Option<u64>,
    avg_compile_local_instructions: u64,
    avg_runtime_local_instructions: u64,
    avg_finalize_local_instructions: u64,
    avg_direct_data_row_scan_local_instructions: u64,
    avg_direct_data_row_key_stream_local_instructions: u64,
    avg_direct_data_row_row_read_local_instructions: u64,
    avg_direct_data_row_key_encode_local_instructions: u64,
    avg_direct_data_row_store_get_local_instructions: u64,
    avg_direct_data_row_order_window_local_instructions: u64,
    avg_direct_data_row_page_window_local_instructions: u64,
    avg_response_decode_local_instructions: u64,
    avg_execute_local_instructions: u64,
    avg_shared_query_plan_cache_hits: u64,
    avg_shared_query_plan_cache_misses: u64,
    avg_local_instructions: u64,
    avg_local_instructions_delta: Option<i64>,
    avg_local_instructions_delta_percent_bps: Option<i64>,
    outcome_stable: bool,
    outcome: FluentQueryPerfOutcome,
}

const fn same_key_scenario(
    scenario_key: &'static str,
    surface: FluentPerfSurface,
    query_family: &'static str,
    query_label: &'static str,
) -> FluentPerfScenario {
    FluentPerfScenario {
        scenario_key,
        canister_scenario_key: scenario_key,
        surface,
        query_family,
        query_label,
        sample_count: 5,
        query_loop_count: 1,
        sample_mode: FluentPerfSampleMode::QueryOnly,
        isolated_fixture: false,
    }
}

const fn repeat_scenario(
    scenario_key: &'static str,
    canister_scenario_key: &'static str,
    surface: FluentPerfSurface,
    query_family: &'static str,
    query_label: &'static str,
    query_loop_count: usize,
) -> FluentPerfScenario {
    FluentPerfScenario {
        scenario_key,
        canister_scenario_key,
        surface,
        query_family,
        query_label,
        sample_count: 5,
        query_loop_count,
        sample_mode: FluentPerfSampleMode::QueryOnly,
        isolated_fixture: false,
    }
}

const fn parity_scenario(
    scenario_key: &'static str,
    canister_scenario_key: &'static str,
    surface: FluentPerfSurface,
    query_family: &'static str,
    query_label: &'static str,
    sample_mode: FluentPerfSampleMode,
) -> FluentPerfScenario {
    FluentPerfScenario {
        scenario_key,
        canister_scenario_key,
        surface,
        query_family,
        query_label,
        sample_count: 1,
        query_loop_count: 1,
        sample_mode,
        isolated_fixture: true,
    }
}

fn average_u64(samples: &[u64]) -> u64 {
    samples.iter().copied().sum::<u64>() / u64::try_from(samples.len()).unwrap_or(1)
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
    let reset: Result<(), Error> = fixture
        .pic()
        .update_call(fixture.canister_id(), "fixtures_reset", ())
        .expect("fixtures_reset should decode");
    reset.expect("fixtures_reset should succeed");

    let load: Result<(), Error> = fixture
        .pic()
        .update_call(fixture.canister_id(), "fixtures_load_default", ())
        .expect("fixtures_load_default should decode");
    load.expect("fixtures_load_default should succeed");
}

fn query_fluent_surface_with_perf(
    fixture: &StandaloneCanisterFixture,
    surface: FluentPerfSurface,
    scenario_key: &str,
    query_loop_count: usize,
) -> Result<FluentQueryPerfResult, Error> {
    match surface {
        FluentPerfSurface::User if query_loop_count == 1 => fixture
            .pic()
            .query_call(
                fixture.canister_id(),
                "query_user_fluent_with_perf",
                (scenario_key.to_string(),),
            )
            .expect("query_user_fluent_with_perf should decode"),
        FluentPerfSurface::User => fixture
            .pic()
            .query_call(
                fixture.canister_id(),
                "query_user_fluent_loop_with_perf",
                (
                    scenario_key.to_string(),
                    u32::try_from(query_loop_count)
                        .expect("query loop count should fit into canister argument"),
                ),
            )
            .expect("query_user_fluent_loop_with_perf should decode"),
        FluentPerfSurface::Account if query_loop_count == 1 => fixture
            .pic()
            .query_call(
                fixture.canister_id(),
                "query_account_fluent_with_perf",
                (scenario_key.to_string(),),
            )
            .expect("query_account_fluent_with_perf should decode"),
        FluentPerfSurface::Account => fixture
            .pic()
            .query_call(
                fixture.canister_id(),
                "query_account_fluent_loop_with_perf",
                (
                    scenario_key.to_string(),
                    u32::try_from(query_loop_count)
                        .expect("query loop count should fit into canister argument"),
                ),
            )
            .expect("query_account_fluent_loop_with_perf should decode"),
    }
}

fn warm_fluent_surface_with_perf(
    fixture: &StandaloneCanisterFixture,
    surface: FluentPerfSurface,
    scenario_key: &str,
) -> Result<FluentQueryPerfResult, Error> {
    match surface {
        FluentPerfSurface::User => fixture
            .pic()
            .update_call(
                fixture.canister_id(),
                "warm_user_fluent_with_perf",
                (scenario_key.to_string(),),
            )
            .expect("warm_user_fluent_with_perf should decode"),
        FluentPerfSurface::Account => fixture
            .pic()
            .update_call(
                fixture.canister_id(),
                "warm_account_fluent_with_perf",
                (scenario_key.to_string(),),
            )
            .expect("warm_account_fluent_with_perf should decode"),
    }
}

fn baseline_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("fluent_perf_audit_baseline.json")
}

fn load_baseline_rows() -> HashMap<String, FluentPerfBaselineRow> {
    let path = baseline_path();
    let Ok(raw) = fs::read_to_string(&path) else {
        return HashMap::new();
    };
    if raw.trim().is_empty() {
        return HashMap::new();
    }
    let rows: Vec<FluentPerfBaselineRow> = serde_json::from_str(&raw).unwrap_or_else(|err| {
        panic!(
            "fluent perf baseline should parse at '{}': {err}",
            path.display()
        )
    });

    rows.into_iter()
        .map(|row| (row.scenario_key.clone(), row))
        .collect()
}

fn maybe_write_blessed_baseline(samples: &[FluentPerfScenarioSample]) {
    if std::env::var_os("FLUENT_PERF_AUDIT_BLESS").is_none() {
        return;
    }

    let path = baseline_path();
    let rows = samples
        .iter()
        .map(|sample| FluentPerfBaselineRow {
            scenario_key: sample.scenario_key.clone(),
            avg_compile_local_instructions: sample.avg_compile_local_instructions,
            avg_runtime_local_instructions: sample.avg_runtime_local_instructions,
            avg_finalize_local_instructions: sample.avg_finalize_local_instructions,
            avg_direct_data_row_scan_local_instructions: sample
                .avg_direct_data_row_scan_local_instructions,
            avg_direct_data_row_key_stream_local_instructions: sample
                .avg_direct_data_row_key_stream_local_instructions,
            avg_direct_data_row_row_read_local_instructions: sample
                .avg_direct_data_row_row_read_local_instructions,
            avg_direct_data_row_key_encode_local_instructions: sample
                .avg_direct_data_row_key_encode_local_instructions,
            avg_direct_data_row_store_get_local_instructions: sample
                .avg_direct_data_row_store_get_local_instructions,
            avg_direct_data_row_order_window_local_instructions: sample
                .avg_direct_data_row_order_window_local_instructions,
            avg_direct_data_row_page_window_local_instructions: sample
                .avg_direct_data_row_page_window_local_instructions,
            avg_response_decode_local_instructions: sample.avg_response_decode_local_instructions,
            avg_execute_local_instructions: sample.avg_execute_local_instructions,
            avg_local_instructions: sample.avg_local_instructions,
        })
        .collect::<Vec<_>>();
    let json = serde_json::to_string_pretty(&rows)
        .expect("fluent perf baseline rows should serialize to pretty JSON");
    fs::write(&path, json).unwrap_or_else(|err| {
        panic!(
            "fluent perf baseline should write to '{}': {err}",
            path.display()
        )
    });
}

#[expect(clippy::too_many_lines)]
fn sample_perf_scenario(
    fixture: &StandaloneCanisterFixture,
    baseline: &HashMap<String, FluentPerfBaselineRow>,
    scenario: FluentPerfScenario,
) -> FluentPerfScenarioSample {
    let mut compile_samples = Vec::with_capacity(scenario.sample_count);
    let mut runtime_samples = Vec::with_capacity(scenario.sample_count);
    let mut finalize_samples = Vec::with_capacity(scenario.sample_count);
    let mut direct_data_row_scan_samples = Vec::with_capacity(scenario.sample_count);
    let mut direct_data_row_key_stream_samples = Vec::with_capacity(scenario.sample_count);
    let mut direct_data_row_row_read_samples = Vec::with_capacity(scenario.sample_count);
    let mut direct_data_row_key_encode_samples = Vec::with_capacity(scenario.sample_count);
    let mut direct_data_row_store_get_samples = Vec::with_capacity(scenario.sample_count);
    let mut direct_data_row_order_window_samples = Vec::with_capacity(scenario.sample_count);
    let mut direct_data_row_page_window_samples = Vec::with_capacity(scenario.sample_count);
    let mut response_decode_samples = Vec::with_capacity(scenario.sample_count);
    let mut execute_samples = Vec::with_capacity(scenario.sample_count);
    let mut shared_query_plan_cache_hit_samples = Vec::with_capacity(scenario.sample_count);
    let mut shared_query_plan_cache_miss_samples = Vec::with_capacity(scenario.sample_count);
    let mut total_samples = Vec::with_capacity(scenario.sample_count);
    let mut outcomes = Vec::with_capacity(scenario.sample_count);

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
            FluentPerfSampleMode::QueryOnly => query_fluent_surface_with_perf(
                active_fixture,
                scenario.surface,
                scenario.canister_scenario_key,
                scenario.query_loop_count,
            ),
            FluentPerfSampleMode::WarmThenQuery => {
                warm_fluent_surface_with_perf(
                    active_fixture,
                    scenario.surface,
                    scenario.canister_scenario_key,
                )
                .unwrap_or_else(|err| {
                    panic!(
                        "warm fluent perf scenario '{}' on '{}' should succeed: {err}",
                        scenario.scenario_key,
                        scenario.surface.label(),
                    )
                });
                query_fluent_surface_with_perf(
                    active_fixture,
                    scenario.surface,
                    scenario.canister_scenario_key,
                    scenario.query_loop_count,
                )
            }
        }
        .unwrap_or_else(|err| {
            panic!(
                "fluent perf scenario '{}' on '{}' should succeed: {err}",
                scenario.scenario_key,
                scenario.surface.label(),
            )
        });
        compile_samples.push(sample.attribution.compile_local_instructions);
        runtime_samples.push(sample.attribution.runtime_local_instructions);
        finalize_samples.push(sample.attribution.finalize_local_instructions);
        direct_data_row_scan_samples
            .push(sample.attribution.direct_data_row_scan_local_instructions);
        direct_data_row_key_stream_samples.push(
            sample
                .attribution
                .direct_data_row_key_stream_local_instructions,
        );
        direct_data_row_row_read_samples.push(
            sample
                .attribution
                .direct_data_row_row_read_local_instructions,
        );
        direct_data_row_key_encode_samples.push(
            sample
                .attribution
                .direct_data_row_key_encode_local_instructions,
        );
        direct_data_row_store_get_samples.push(
            sample
                .attribution
                .direct_data_row_store_get_local_instructions,
        );
        direct_data_row_order_window_samples.push(
            sample
                .attribution
                .direct_data_row_order_window_local_instructions,
        );
        direct_data_row_page_window_samples.push(
            sample
                .attribution
                .direct_data_row_page_window_local_instructions,
        );
        response_decode_samples.push(sample.attribution.response_decode_local_instructions);
        execute_samples.push(sample.attribution.execute_local_instructions);
        shared_query_plan_cache_hit_samples.push(sample.attribution.shared_query_plan_cache_hits);
        shared_query_plan_cache_miss_samples
            .push(sample.attribution.shared_query_plan_cache_misses);
        total_samples.push(sample.attribution.total_local_instructions);
        outcomes.push(sample.outcome);
    }

    let first_outcome = outcomes
        .first()
        .cloned()
        .expect("scenario should sample once");
    let outcome_stable = outcomes.iter().all(|outcome| *outcome == first_outcome);
    let avg_compile_local_instructions = average_u64(&compile_samples);
    let avg_runtime_local_instructions = average_u64(&runtime_samples);
    let avg_finalize_local_instructions = average_u64(&finalize_samples);
    let avg_direct_data_row_scan_local_instructions = average_u64(&direct_data_row_scan_samples);
    let avg_direct_data_row_key_stream_local_instructions =
        average_u64(&direct_data_row_key_stream_samples);
    let avg_direct_data_row_row_read_local_instructions =
        average_u64(&direct_data_row_row_read_samples);
    let avg_direct_data_row_key_encode_local_instructions =
        average_u64(&direct_data_row_key_encode_samples);
    let avg_direct_data_row_store_get_local_instructions =
        average_u64(&direct_data_row_store_get_samples);
    let avg_direct_data_row_order_window_local_instructions =
        average_u64(&direct_data_row_order_window_samples);
    let avg_direct_data_row_page_window_local_instructions =
        average_u64(&direct_data_row_page_window_samples);
    let avg_response_decode_local_instructions = average_u64(&response_decode_samples);
    let avg_execute_local_instructions = average_u64(&execute_samples);
    let avg_shared_query_plan_cache_hits = average_u64(&shared_query_plan_cache_hit_samples);
    let avg_shared_query_plan_cache_misses = average_u64(&shared_query_plan_cache_miss_samples);
    let avg_local_instructions = average_u64(&total_samples);
    let baseline_row = baseline.get(scenario.scenario_key);
    let avg_local_instructions_delta = baseline_row.map(|row| {
        i64::try_from(avg_local_instructions).unwrap_or(i64::MAX)
            - i64::try_from(row.avg_local_instructions).unwrap_or(i64::MAX)
    });
    let avg_local_instructions_delta_percent_bps = baseline_row.and_then(|row| {
        if row.avg_local_instructions == 0 {
            return None;
        }

        Some(
            i64::try_from(
                avg_local_instructions.saturating_mul(10_000) / row.avg_local_instructions,
            )
            .unwrap_or(i64::MAX)
                - 10_000,
        )
    });

    FluentPerfScenarioSample {
        scenario_key: scenario.scenario_key.to_string(),
        surface: scenario.surface.label().to_string(),
        query_family: scenario.query_family.to_string(),
        query_label: scenario.query_label.to_string(),
        query_loop_count: scenario.query_loop_count,
        baseline_avg_compile_local_instructions: baseline_row
            .map(|row| row.avg_compile_local_instructions),
        baseline_avg_runtime_local_instructions: baseline_row
            .map(|row| row.avg_runtime_local_instructions),
        baseline_avg_finalize_local_instructions: baseline_row
            .map(|row| row.avg_finalize_local_instructions),
        baseline_avg_direct_data_row_scan_local_instructions: baseline_row
            .map(|row| row.avg_direct_data_row_scan_local_instructions),
        baseline_avg_direct_data_row_key_stream_local_instructions: baseline_row
            .map(|row| row.avg_direct_data_row_key_stream_local_instructions),
        baseline_avg_direct_data_row_row_read_local_instructions: baseline_row
            .map(|row| row.avg_direct_data_row_row_read_local_instructions),
        baseline_avg_direct_data_row_key_encode_local_instructions: baseline_row
            .map(|row| row.avg_direct_data_row_key_encode_local_instructions),
        baseline_avg_direct_data_row_store_get_local_instructions: baseline_row
            .map(|row| row.avg_direct_data_row_store_get_local_instructions),
        baseline_avg_direct_data_row_order_window_local_instructions: baseline_row
            .map(|row| row.avg_direct_data_row_order_window_local_instructions),
        baseline_avg_direct_data_row_page_window_local_instructions: baseline_row
            .map(|row| row.avg_direct_data_row_page_window_local_instructions),
        baseline_avg_response_decode_local_instructions: baseline_row
            .map(|row| row.avg_response_decode_local_instructions),
        baseline_avg_execute_local_instructions: baseline_row
            .map(|row| row.avg_execute_local_instructions),
        baseline_avg_local_instructions: baseline_row.map(|row| row.avg_local_instructions),
        avg_compile_local_instructions,
        avg_runtime_local_instructions,
        avg_finalize_local_instructions,
        avg_direct_data_row_scan_local_instructions,
        avg_direct_data_row_key_stream_local_instructions,
        avg_direct_data_row_row_read_local_instructions,
        avg_direct_data_row_key_encode_local_instructions,
        avg_direct_data_row_store_get_local_instructions,
        avg_direct_data_row_order_window_local_instructions,
        avg_direct_data_row_page_window_local_instructions,
        avg_response_decode_local_instructions,
        avg_execute_local_instructions,
        avg_shared_query_plan_cache_hits,
        avg_shared_query_plan_cache_misses,
        avg_local_instructions,
        avg_local_instructions_delta,
        avg_local_instructions_delta_percent_bps,
        outcome_stable,
        outcome: first_outcome,
    }
}

fn user_fluent_scenarios() -> Vec<FluentPerfScenario> {
    vec![
        same_key_scenario(
            "user.id.order_only.asc.limit2",
            FluentPerfSurface::User,
            "ordered_scalar",
            r#"db().load::<PerfAuditUser>().order_by("id").limit(2)"#,
        ),
        same_key_scenario(
            "user.age.order_only.asc.limit3",
            FluentPerfSurface::User,
            "ordered_scalar",
            r#"db().load::<PerfAuditUser>().order_by("age").order_by("id").limit(3)"#,
        ),
        parity_scenario(
            "user.age.order_only.asc.limit2.cold_query",
            "user.age.order_only.asc.limit2.parity",
            FluentPerfSurface::User,
            "parity_cold_query",
            r#"db().load::<PerfAuditUser>().order_by("age").order_by("id").limit(2)"#,
            FluentPerfSampleMode::QueryOnly,
        ),
        parity_scenario(
            "user.age.order_only.asc.limit2.warm_after_update",
            "user.age.order_only.asc.limit2.parity",
            FluentPerfSurface::User,
            "parity_warm_after_update",
            r#"db().load::<PerfAuditUser>().order_by("age").order_by("id").limit(2)"#,
            FluentPerfSampleMode::WarmThenQuery,
        ),
        same_key_scenario(
            "user.active_true.order_age.limit3",
            FluentPerfSurface::User,
            "predicate_scalar",
            r#"db().load::<PerfAuditUser>().filter(FieldRef::new("active").eq(true)).order_by("age").order_by("id").limit(3)"#,
        ),
        same_key_scenario(
            "user.field_compare.age_eq_age_nat.limit3",
            FluentPerfSurface::User,
            "field_compare",
            r#"db().load::<PerfAuditUser>().filter(FieldRef::new("age").eq_field("age_nat")).order_by("age").order_by("id").limit(3)"#,
        ),
        same_key_scenario(
            "user.field_between.rank_age_age.limit3",
            FluentPerfSurface::User,
            "field_between",
            r#"db().load::<PerfAuditUser>().filter(FieldRef::new("rank").between_fields("age", "age")).order_by("age").order_by("id").limit(3)"#,
        ),
        same_key_scenario(
            "user.rank.in_list.limit3",
            FluentPerfSurface::User,
            "membership",
            r#"db().load::<PerfAuditUser>().filter(FieldRef::new("rank").in_list([17, 28, 30])).order_by("age").order_by("id").limit(3)"#,
        ),
        same_key_scenario(
            "user.grouped.age_count.limit10",
            FluentPerfSurface::User,
            "grouped",
            r#"db().load::<PerfAuditUser>().group_by("age")?.aggregate(count()).order_by("age").limit(10)"#,
        ),
        repeat_scenario(
            "repeat.user.age.order_only.asc.limit3.runs10",
            "user.age.order_only.asc.limit3",
            FluentPerfSurface::User,
            "repeat_ordered_scalar",
            r#"db().load::<PerfAuditUser>().order_by("age").order_by("id").limit(3)"#,
            10,
        ),
        repeat_scenario(
            "repeat.user.age.order_only.asc.limit3.runs100",
            "user.age.order_only.asc.limit3",
            FluentPerfSurface::User,
            "repeat_ordered_scalar",
            r#"db().load::<PerfAuditUser>().order_by("age").order_by("id").limit(3)"#,
            100,
        ),
        repeat_scenario(
            "repeat.user.grouped.age_count.limit10.runs100",
            "user.grouped.age_count.limit10",
            FluentPerfSurface::User,
            "repeat_grouped",
            r#"db().load::<PerfAuditUser>().group_by("age")?.aggregate(count()).order_by("age").limit(10)"#,
            100,
        ),
    ]
}

fn account_fluent_scenarios() -> Vec<FluentPerfScenario> {
    vec![
        same_key_scenario(
            "account.active_true.order_handle.asc.limit3",
            FluentPerfSurface::Account,
            "predicate_scalar",
            r#"db().load::<PerfAuditAccount>().filter(FieldRef::new("active").eq(true)).order_by("handle").order_by("id").limit(3)"#,
        ),
        same_key_scenario(
            "account.gold_active.order_handle.asc.limit3",
            FluentPerfSurface::Account,
            "compound_predicate",
            r#"db().load::<PerfAuditAccount>().filter(Predicate::and(vec![FieldRef::new("active").eq(true), FieldRef::new("tier").eq("gold")])).order_by("handle").order_by("id").limit(3)"#,
        ),
        same_key_scenario(
            "account.score_gte_75.order_score.limit3",
            FluentPerfSurface::Account,
            "range_scalar",
            r#"db().load::<PerfAuditAccount>().filter(FieldRef::new("score").gte(75)).order_by("score").order_by("id").limit(3)"#,
        ),
        repeat_scenario(
            "repeat.account.active_true.order_handle.asc.limit3.runs100",
            "account.active_true.order_handle.asc.limit3",
            FluentPerfSurface::Account,
            "repeat_predicate_scalar",
            r#"db().load::<PerfAuditAccount>().filter(FieldRef::new("active").eq(true)).order_by("handle").order_by("id").limit(3)"#,
            100,
        ),
    ]
}

fn fluent_perf_scenarios() -> Vec<FluentPerfScenario> {
    let mut scenarios = user_fluent_scenarios();
    scenarios.extend(account_fluent_scenarios());
    scenarios
}

#[test]
fn fluent_perf_audit_harness_reports_instruction_samples() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let baseline = load_baseline_rows();
    let scenarios = fluent_perf_scenarios();
    let samples = scenarios
        .into_iter()
        .map(|scenario| sample_perf_scenario(&fixture, &baseline, scenario))
        .collect::<Vec<_>>();

    for sample in &samples {
        println!(
            "{} | {} | runs={} | compile={} | runtime={} | direct_scan={} | direct_key={} | direct_read={} | direct_encode={} | direct_store={} | direct_order={} | direct_page={} | finalize={} | decode={} | execute={} | cache_hits={} | cache_misses={} | total={} | delta={:?} | delta_bps={:?}",
            sample.scenario_key,
            sample.query_label,
            sample.query_loop_count,
            sample.avg_compile_local_instructions,
            sample.avg_runtime_local_instructions,
            sample.avg_direct_data_row_scan_local_instructions,
            sample.avg_direct_data_row_key_stream_local_instructions,
            sample.avg_direct_data_row_row_read_local_instructions,
            sample.avg_direct_data_row_key_encode_local_instructions,
            sample.avg_direct_data_row_store_get_local_instructions,
            sample.avg_direct_data_row_order_window_local_instructions,
            sample.avg_direct_data_row_page_window_local_instructions,
            sample.avg_finalize_local_instructions,
            sample.avg_response_decode_local_instructions,
            sample.avg_execute_local_instructions,
            sample.avg_shared_query_plan_cache_hits,
            sample.avg_shared_query_plan_cache_misses,
            sample.avg_local_instructions,
            sample.avg_local_instructions_delta,
            sample.avg_local_instructions_delta_percent_bps,
        );
        assert!(
            sample.outcome_stable,
            "fluent perf outcome should stay stable"
        );
        assert!(
            sample.avg_local_instructions > 0,
            "fluent perf scenario should report positive instructions",
        );
    }

    maybe_write_blessed_baseline(&samples);
}

#[test]
fn fluent_perf_update_warm_persists_query_cache_across_calls() {
    let fixture = install_sql_perf_canister_fixture();
    reset_sql_perf_fixtures(&fixture);

    let scenario = "user.age.order_only.asc.limit3";
    let warm = warm_fluent_surface_with_perf(&fixture, FluentPerfSurface::User, scenario)
        .expect("update warm fluent query should succeed");
    assert!(
        warm.attribution
            .shared_query_plan_cache_hits
            .saturating_add(warm.attribution.shared_query_plan_cache_misses)
            > 0,
        "the update warm call should exercise the shared lower query-plan cache before the later query call",
    );

    let query = query_fluent_surface_with_perf(&fixture, FluentPerfSurface::User, scenario, 1)
        .expect("query call should succeed after update warm");
    assert_eq!(
        query.attribution.shared_query_plan_cache_hits, 1,
        "the later query call should reuse the shared lower query-plan cache warmed by the update call",
    );
    assert_eq!(
        query.attribution.shared_query_plan_cache_misses, 0,
        "the later query call should not rebuild the warmed shared lower query plan",
    );
}
