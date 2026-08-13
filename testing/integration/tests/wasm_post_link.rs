use std::{env, fs};

use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{Error, db::sql::SqlQueryResult};
use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterCandidExportMode, CanisterSqlMode,
    CanisterWasmProfile, build_fixture_canister_wasm_stages_with_options,
    deliver_fixture_startup_watchdog, install_prebuilt_fixture_canister, reset_icydb_fixtures,
    wasm_optimizer::optimize_deployable_wasm,
};
use serde::Deserialize;

#[derive(CandidType, Debug, Deserialize, Eq, PartialEq)]
struct SchemaApplicationPerfResult {
    local_instructions: u64,
    reconcile_checks: u64,
    first_create: u64,
    exact_match: u64,
}

#[derive(CandidType, Debug, Deserialize, Eq, PartialEq)]
struct SqlTotalOnlyPerfResult {
    result: SqlQueryResult,
    instructions: u64,
}

const fn wasm_release_test_options() -> CanisterBuildOptions {
    CanisterBuildOptions {
        profile: CanisterWasmProfile::WasmRelease,
        sql_mode: CanisterSqlMode::Enabled,
        candid_export: CanisterCandidExportMode::Enabled,
        build_profile: CanisterBuildProfile::LocalTest,
    }
}

fn schema_application_update(fixture: &StandaloneCanisterFixture) -> SchemaApplicationPerfResult {
    let result: Result<SchemaApplicationPerfResult, Error> = fixture
        .update_candid("measure_schema_application_update", ())
        .expect("schema-application update result should decode");
    result.expect("schema-application update should succeed")
}

fn schema_application_query(fixture: &StandaloneCanisterFixture) -> SchemaApplicationPerfResult {
    let result: Result<SchemaApplicationPerfResult, Error> = fixture
        .query_candid("measure_schema_application_query", ())
        .expect("schema-application query result should decode");
    result.expect("schema-application query should succeed")
}

fn query_user(fixture: &StandaloneCanisterFixture) -> SqlQueryResult {
    let result: Result<SqlQueryResult, Error> = fixture
        .query_candid(
            "query_user",
            ("SELECT id, name, age FROM PerfAuditUser ORDER BY id".to_string(),),
        )
        .expect("SQL query result should decode");
    result.expect("SQL query should succeed")
}

fn query_user_instructions(fixture: &StandaloneCanisterFixture) -> SqlTotalOnlyPerfResult {
    let result: Result<SqlTotalOnlyPerfResult, Error> = fixture
        .query_candid(
            "query_user_total_only_perf",
            ("SELECT id, name, age FROM PerfAuditUser ORDER BY id".to_string(),),
        )
        .expect("SQL instruction result should decode");
    result.expect("SQL instruction query should succeed")
}

#[test]
fn canonical_post_link_wasm_is_deterministic_and_upgrade_safe() {
    let (compiler_emitted, final_deployable) =
        build_fixture_canister_wasm_stages_with_options("sql_perf", wasm_release_test_options());
    let compiler_len = compiler_emitted.len();
    let final_len = final_deployable.len();
    assert!(
        final_len * 10_000 <= compiler_len * 9_300,
        "post-link optimization requires at least a 7% raw-Wasm reduction"
    );

    let test_dir = env::temp_dir().join(format!("icydb-post-link-{}", std::process::id()));
    fs::create_dir_all(&test_dir).expect("post-link test directory should be creatable");
    let compiler_path = test_dir.join("compiler.wasm");
    let first_path = test_dir.join("first.wasm");
    let second_path = test_dir.join("second.wasm");
    fs::write(&compiler_path, &compiler_emitted).expect("compiler Wasm should be writable");
    optimize_deployable_wasm(&compiler_path, &first_path)
        .expect("first canonical transform should succeed");
    optimize_deployable_wasm(&compiler_path, &second_path)
        .expect("second canonical transform should succeed");
    assert_eq!(
        fs::read(&first_path).expect("first transformed Wasm should be readable"),
        final_deployable
    );
    assert_eq!(
        fs::read(&second_path).expect("second transformed Wasm should be readable"),
        final_deployable
    );

    let fixture = install_prebuilt_fixture_canister("sql_perf", compiler_emitted);
    let initialized = schema_application_update(&fixture);
    assert_eq!(initialized.first_create, 0);
    assert_eq!(initialized.exact_match, 1);
    reset_icydb_fixtures(&fixture);
    let before_upgrade_rows = query_user(&fixture);
    let before_upgrade_exact = schema_application_query(&fixture);
    let before_upgrade_query = query_user_instructions(&fixture);
    assert_eq!(before_upgrade_exact.first_create, 0);
    assert_eq!(before_upgrade_exact.exact_match, 1);

    let optimized_fixture = install_prebuilt_fixture_canister("sql_perf", final_deployable.clone());
    let optimized_initialized = schema_application_update(&optimized_fixture);
    assert_eq!(optimized_initialized.first_create, 0);
    assert_eq!(optimized_initialized.exact_match, 1);
    reset_icydb_fixtures(&optimized_fixture);
    let optimized_exact = schema_application_query(&optimized_fixture);
    let optimized_query = query_user_instructions(&optimized_fixture);
    assert_eq!(optimized_query.result, before_upgrade_query.result);
    assert!(
        optimized_query.instructions <= before_upgrade_query.instructions.saturating_mul(101) / 100,
        "post-link ordered-query instructions must stay within 1%: compiler={}, optimized={}",
        before_upgrade_query.instructions,
        optimized_query.instructions,
    );
    assert!(
        optimized_exact.local_instructions
            <= before_upgrade_exact.local_instructions.saturating_mul(101) / 100,
        "post-link schema exact-reentry instructions must stay within 1%"
    );

    fixture
        .pocket_ic()
        .upgrade_canister(
            fixture.canister_id(),
            final_deployable,
            candid::encode_args(()).expect("empty upgrade args should encode"),
            None,
        )
        .expect("compiler-to-post-link upgrade should succeed");
    deliver_fixture_startup_watchdog(&fixture);

    let after_upgrade_rows = query_user(&fixture);
    assert_eq!(after_upgrade_rows, before_upgrade_rows);
    let after_upgrade_exact = schema_application_query(&fixture);
    assert_eq!(after_upgrade_exact.first_create, 0);
    assert_eq!(after_upgrade_exact.exact_match, 1);

    reset_icydb_fixtures(&fixture);
    assert_eq!(query_user(&fixture), before_upgrade_rows);

    println!(
        "post-link sql_perf: compiler={} final={} reduction_bps={} compiler_exact={} optimized_exact={} post_upgrade_exact={} compiler_query={} optimized_query={}",
        compiler_len,
        final_len,
        (compiler_len - final_len) * 10_000 / compiler_len,
        before_upgrade_exact.local_instructions,
        optimized_exact.local_instructions,
        after_upgrade_exact.local_instructions,
        before_upgrade_query.instructions,
        optimized_query.instructions,
    );

    drop(fs::remove_dir_all(test_dir));
}
