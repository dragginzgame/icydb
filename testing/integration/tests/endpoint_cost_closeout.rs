use std::{env, fs, path::PathBuf};

use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{Error, db::sql::SqlQueryPerfResult};
use icydb_testing_integration::install_prebuilt_fixture_canister;

const SAMPLE_CALLS: u64 = 32;
const SQL: &str = "SELECT name FROM SqlTestUser ORDER BY age ASC LIMIT 2";

#[test]
#[ignore = "release closeout probe comparing explicit SQL endpoint instructions"]
fn explicit_sql_endpoint_reports_baseline_and_current_instruction_costs() {
    let baseline = install("ICYDB_ENDPOINT_BASELINE_WASM");
    let current = install("ICYDB_ENDPOINT_CURRENT_WASM");

    let baseline_instructions = measure_sql_queries(&baseline);
    let current_instructions = measure_sql_queries(&current);
    let delta = i128::from(current_instructions) - i128::from(baseline_instructions);

    assert!(baseline_instructions > 0);
    assert!(current_instructions > 0);
    println!(
        "SQL endpoint local instructions over {SAMPLE_CALLS} calls: baseline={baseline_instructions} current={current_instructions} delta={delta} baseline_avg={} current_avg={}",
        baseline_instructions / SAMPLE_CALLS,
        current_instructions / SAMPLE_CALLS,
    );
}

fn install(variable: &str) -> StandaloneCanisterFixture {
    let path = env::var_os(variable).map_or_else(
        || panic!("{variable} must name one raw test_sql Wasm"),
        PathBuf::from,
    );
    let wasm = fs::read(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));

    install_prebuilt_fixture_canister("sql", wasm)
}

fn measure_sql_queries(fixture: &StandaloneCanisterFixture) -> u64 {
    let loaded: Result<(), Error> = fixture
        .update_call("icydb_fixtures_load", ())
        .expect("fixture load response should decode");
    loaded.expect("fixture load should succeed");

    let warm: Result<SqlQueryPerfResult, Error> = fixture
        .query_call("icydb_query", (SQL.to_string(),))
        .expect("warm SQL response should decode");
    warm.expect("warm SQL query should succeed");

    let mut instructions = 0_u64;
    for _ in 0..SAMPLE_CALLS {
        let result: Result<SqlQueryPerfResult, Error> = fixture
            .query_call("icydb_query", (SQL.to_string(),))
            .expect("SQL response should decode");
        let result = result.expect("SQL query should succeed");
        instructions = instructions
            .checked_add(result.instructions)
            .expect("sample instruction total should fit u64");
    }

    instructions
}
