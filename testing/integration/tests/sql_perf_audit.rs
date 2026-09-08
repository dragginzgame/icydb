//! Total-only instruction coverage for the SQL performance actor.

use candid::CandidType;
use icydb::{Error, db::sql::SqlQueryResult};
use icydb_testing_integration::{install_fixture_canister, reset_icydb_fixtures};
use serde::Deserialize;

#[derive(CandidType, Debug, Deserialize)]
struct ReadTotalOnlyPerfResult {
    row_count: u32,
    instructions: u64,
}

// Keep SQL syntax below the input-depth limit while varying associative width.
fn balanced_sql_terms(terms: &[String], operator: &str) -> String {
    if terms.len() == 1 {
        return terms[0].clone();
    }
    let middle = terms.len() / 2;
    format!(
        "({} {operator} {})",
        balanced_sql_terms(&terms[..middle], operator),
        balanced_sql_terms(&terms[middle..], operator)
    )
}

// Discharge setup/background messages and deferred query charges outside each
// cycle-balance interval. Do not advance IC time or start an auto-progress loop.
fn settle_measurement_rounds(fixture: &ic_testkit::pic::StandaloneCanisterFixture) {
    for _ in 0..64 {
        fixture.pocket_ic().tick();
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    instructions: u64,
}

fn query_perf(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    method: &str,
    sql: &str,
) -> SqlQueryPerfResult {
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid(method, (sql.to_string(),))
        .unwrap_or_else(|error| panic!("{method} should decode: {error}"));

    result.unwrap_or_else(|error| panic!("{method} should execute: {error}"))
}

#[test]
fn sql_perf_reports_total_instructions_for_query_warmup_and_loop_execution() {
    let fixture = install_fixture_canister("sql_perf");
    reset_icydb_fixtures(&fixture);
    let sql = "SELECT COUNT(*) FROM PerfAuditUser";

    let cold = query_perf(&fixture, "query_user_with_perf", sql);
    let warmed: Result<SqlQueryPerfResult, Error> = fixture
        .update_candid("warm_user_query_with_perf", (sql.to_string(),))
        .expect("warm SQL measurement should decode");
    let warmed = warmed.expect("warm SQL measurement should execute");
    let looped: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid("query_user_loop_with_perf", (sql.to_string(), 8_u32))
        .expect("looped SQL measurement should decode");
    let looped = looped.expect("looped SQL measurement should execute");

    assert_eq!(cold.result, warmed.result);
    assert_eq!(cold.result, looped.result);
    assert!(cold.instructions > 0);
    assert!(warmed.instructions > 0);
    assert!(looped.instructions > 0);
}

#[test]
fn sql_perf_total_measurement_covers_distinct_entity_surfaces() {
    let fixture = install_fixture_canister("sql_perf");
    reset_icydb_fixtures(&fixture);

    for (method, sql) in [
        (
            "query_heap_user_with_perf",
            "SELECT COUNT(*) FROM PerfAuditHeapUser",
        ),
        (
            "query_journaled_user_with_perf",
            "SELECT COUNT(*) FROM PerfAuditJournaledUser",
        ),
        (
            "query_token_with_perf",
            "SELECT COUNT(*) FROM PerfAuditToken",
        ),
    ] {
        let sample = query_perf(&fixture, method, sql);
        assert!(sample.instructions > 0, "{method}");
    }
}

#[test]
fn sql_blob_scalar_query_executes_equality_and_octet_length() {
    let fixture = install_fixture_canister("sql_perf");
    reset_icydb_fixtures(&fixture);

    let sample = query_perf(
        &fixture,
        "query_blob_with_perf",
        "SELECT id, label, bucket, OCTET_LENGTH(chunk) FROM PerfAuditBlob \
         WHERE bucket = 10 ORDER BY bucket ASC, label ASC, id ASC LIMIT 3",
    );
    let SqlQueryResult::Projection(output) = sample.result else {
        panic!("blob scalar query should return a projection");
    };

    assert_eq!(output.row_count, 3);
    assert!(output.rows.iter().all(|row| row.len() == 4));
}

fn preparation_measurement_wasm() -> Vec<u8> {
    use icydb_testing_integration::{
        CanisterBuildOptions, CanisterBuildProfile, CanisterCandidExportMode, CanisterSqlMode,
        CanisterWasmProfile, build_canister_with_options,
    };
    let path = std::env::var_os("ICYDB_PREPARATION_WASM").map_or_else(
        || {
            build_canister_with_options(
                "sql_perf",
                CanisterBuildOptions {
                    profile: CanisterWasmProfile::WasmRelease,
                    sql_mode: CanisterSqlMode::Enabled,
                    candid_export: CanisterCandidExportMode::Enabled,
                    build_profile: CanisterBuildProfile::LocalTest,
                },
            )
            .expect("canonical audit actor should build")
        },
        std::path::PathBuf::from,
    );
    let module = std::fs::read(&path).expect("audit artifact should be readable");
    println!(
        "preparation_wasm path={} raw_bytes={}",
        path.display(),
        module.len()
    );
    module
}

/// Manual matched-artifact probe: queries do not persist cold cache changes;
/// updates warm caches and expose actual whole-call charged cycles separately.
#[test]
#[ignore = "manual wasm-release preparation instruction and cycle measurement"]
fn preparation_wasm_cost_matrix() {
    let module = preparation_measurement_wasm();
    for terms in [4_u32, 16, 64, 128] {
        for (shape, name) in [(0_u8, "and"), (1, "or"), (2, "in")] {
            // A preceding shape must not change this sample's allocator/cache
            // history. Reuse bytes, not the running canister, between cells.
            let fixture = icydb_testing_integration::install_prebuilt_fixture_canister(
                "sql_perf",
                module.clone(),
            );
            reset_icydb_fixtures(&fixture);
            let values = (0..terms).rev().collect::<Vec<_>>();
            let predicate = match shape {
                0 => balanced_sql_terms(
                    &values
                        .iter()
                        .map(|n| format!("age >= -{n}"))
                        .collect::<Vec<_>>(),
                    "AND",
                ),
                1 => balanced_sql_terms(
                    &values
                        .iter()
                        .map(|n| format!("age = {n}"))
                        .collect::<Vec<_>>(),
                    "OR",
                ),
                _ => format!(
                    "age IN ({})",
                    values
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            };
            let sql =
                format!("SELECT id FROM PerfAuditUser WHERE {predicate} ORDER BY id LIMIT 100");
            settle_measurement_rounds(&fixture);
            let cold = query_perf(&fixture, "query_user_with_perf", &sql);
            let SqlQueryResult::Projection(ref rows) = cold.result else {
                panic!("measurement should project fixture rows");
            };
            let expected_rows = rows.row_count;
            let balance = || fixture.pocket_ic().cycle_balance(fixture.canister_id());
            settle_measurement_rounds(&fixture);
            let before = balance();
            let warmed: Result<SqlQueryPerfResult, Error> = fixture
                .update_candid("warm_user_query_with_perf", (sql.clone(),))
                .expect("SQL update measurement should decode");
            let cold_cycles = before
                .checked_sub(balance())
                .expect("update charges cycles");
            assert_eq!(
                cold.result,
                warmed.expect("SQL update should execute").result
            );
            for repeat in 0..3 {
                settle_measurement_rounds(&fixture);
                let before = balance();
                let warm: Result<SqlQueryPerfResult, Error> = fixture
                    .update_candid("warm_user_query_with_perf", (sql.clone(),))
                    .expect("warm SQL measurement should decode");
                let warm_cycles = before
                    .checked_sub(balance())
                    .expect("update charges cycles");
                let warm = warm.expect("warm SQL should execute");
                assert_eq!(cold.result, warm.result);
                settle_measurement_rounds(&fixture);
                let before = balance();
                let typed: Result<ReadTotalOnlyPerfResult, Error> = fixture
                    .update_candid("measure_typed_filter_preparation", (shape, terms))
                    .expect("typed measurement should decode");
                let typed_cycles = before
                    .checked_sub(balance())
                    .expect("update charges cycles");
                let typed = typed.expect("typed filter should execute");
                assert_eq!(typed.row_count, expected_rows);
                assert!(cold.instructions > 0 && warm.instructions > 0 && typed.instructions > 0);
                println!(
                    "preparation_cost shape={name} terms={terms} repeat={repeat} rows={expected_rows} sql_cold_instructions={} sql_cold_cycles={cold_cycles} sql_warm_instructions={} sql_warm_cycles={warm_cycles} typed_instructions={} typed_cycles={typed_cycles}",
                    cold.instructions, warm.instructions, typed.instructions
                );
            }
        }
    }
}

/// Matched frozen-module comparison for scalar big-integer filter conversion.
#[test]
#[ignore = "manual wasm-release big-integer instruction and cycle measurement"]
fn big_literal_wasm_cost_matrix() {
    let module = preparation_measurement_wasm();
    for digits in [20_u32, 80, 300] {
        for negative in [false, true] {
            let fixture = icydb_testing_integration::install_prebuilt_fixture_canister(
                "sql_perf",
                module.clone(),
            );
            // A fresh installed fixture has no rows in the dedicated big-literal
            // entity. Cold/warm calls still resolve and normalize accepted types.
            for repeat in 0..4 {
                settle_measurement_rounds(&fixture);
                let before = fixture.pocket_ic().cycle_balance(fixture.canister_id());
                let sample: Result<ReadTotalOnlyPerfResult, Error> = fixture
                    .update_candid("measure_big_literal_preparation", (negative, digits))
                    .expect("big-literal measurement should decode");
                let cycles = before
                    .checked_sub(fixture.pocket_ic().cycle_balance(fixture.canister_id()))
                    .expect("update charges cycles");
                let sample = sample.expect("big-literal query should prepare and execute");
                assert_eq!(sample.row_count, 0);
                assert!(sample.instructions > 0);
                println!(
                    "big_literal_cost negative={negative} digits={digits} repeat={repeat} instructions={} cycles={cycles}",
                    sample.instructions
                );
            }
        }
    }
}

/// Matched frozen-module comparison for accepted big-integer write admission.
#[test]
#[ignore = "manual wasm-release big-integer write instruction and cycle measurement"]
fn big_integer_write_wasm_cost_matrix() {
    let module = preparation_measurement_wasm();
    for digits in [20_u32, 80, 300] {
        let fixture = icydb_testing_integration::install_prebuilt_fixture_canister(
            "sql_perf",
            module.clone(),
        );
        for repeat in 0_i32..4 {
            settle_measurement_rounds(&fixture);
            let before = fixture.pocket_ic().cycle_balance(fixture.canister_id());
            let sample: Result<ReadTotalOnlyPerfResult, Error> = fixture
                .update_candid("measure_big_integer_write", (digits, repeat))
                .expect("big-integer write measurement should decode");
            let cycles = before
                .checked_sub(fixture.pocket_ic().cycle_balance(fixture.canister_id()))
                .expect("update charges cycles");
            let sample = sample.expect("big-integer write should succeed");
            assert_eq!(sample.row_count, 1);
            assert!(sample.instructions > 0);
            println!(
                "big_integer_write_cost digits={digits} repeat={repeat} instructions={} cycles={cycles}",
                sample.instructions
            );
        }
    }
}
