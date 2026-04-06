use candid::{Principal, encode_one};
use canic_testkit::pic::{
    Pic, PicStartError, StandaloneCanisterFixture, StandaloneCanisterFixtureError,
    try_acquire_pic_serial_guard, try_install_prebuilt_canister_with_cycles, try_pic,
};
use icydb::db::sql::{SqlQueryResult, SqlQueryRowsOutput};
use icydb_testing_integration::build_canister;
use serde::Serialize;
use std::{fs, sync::OnceLock};

const INIT_CYCLES: u128 = 2_000_000_000_000;
const ANY_PROJECTION_VALUE: &str = "<any>";
const SQL_PERF_PROBE_SQL_ENV: &str = "ICYDB_SQL_PERF_PROBE_SQL";
const SQL_PERF_PROBE_SURFACE_ENV: &str = "ICYDB_SQL_PERF_PROBE_SURFACE";
const SQL_PERF_PROBE_CURSOR_ENV: &str = "ICYDB_SQL_PERF_PROBE_CURSOR";
const SQL_PERF_PROBE_REPEAT_ENV: &str = "ICYDB_SQL_PERF_PROBE_REPEAT_COUNT";
const DEFAULT_SQL_PERF_PROBE_SQL: &str = "SELECT id, level, class_name FROM Character ORDER BY level ASC, class_name ASC, id ASC LIMIT 2";
static QUICKSTART_CANISTER_WASM: OnceLock<Vec<u8>> = OnceLock::new();

fn build_quickstart_canister_wasm() -> Vec<u8> {
    QUICKSTART_CANISTER_WASM
        .get_or_init(|| {
            let wasm_path = build_canister("quickstart").expect("build quickstart canister");
            fs::read(&wasm_path).unwrap_or_else(|err| {
                panic!(
                    "failed to read built canister wasm at {}: {err}",
                    wasm_path.display()
                )
            })
        })
        .clone()
}

// Skip cleanly when PocketIC is unavailable locally instead of panicking.
//
// The new canic-testkit `try_*` startup APIs classify startup failures for us,
// so the suite no longer needs its own `POCKET_IC_BIN` preflight. Missing
// binaries or blocked auto-downloads are local setup gaps rather than test
// failures; other startup errors still panic because they indicate a broken
// runtime we should notice.
const fn should_skip_pic_start(err: &PicStartError) -> bool {
    matches!(
        err,
        PicStartError::BinaryUnavailable { .. } | PicStartError::DownloadFailed { .. }
    )
}

// Emit the shared skip message for one local PocketIC availability gap.
fn skip_sql_canister_test(reason: impl std::fmt::Display) {
    eprintln!("skipping canic-testkit-backed SQL canister integration test: {reason}");
}

// Install the quickstart fixture canister into one existing Pic instance.
//
// Keep the bridge narrow: this suite still owns the repo-specific wasm build
// path and the quickstart canister's empty init-arg contract, but the actual
// installation now goes through canic-testkit's generic public install helper.
fn install_quickstart_canister(pic: &Pic) -> Principal {
    let wasm = build_quickstart_canister_wasm();

    pic.try_create_and_install_with_args(
        wasm,
        encode_one(()).expect("encode init args"),
        INIT_CYCLES,
    )
    .unwrap_or_else(|err| panic!("failed to install quickstart canister: {err}"))
}

// Install one quickstart canister into a fresh canic-testkit fixture.
//
// This is the common integration-test shape: one fresh Pic, one real
// quickstart canister, public update/query calls only. Keep it on the public
// prebuilt-install helper so the suite stays testkit-first.
fn install_fresh_quickstart_fixture() -> Option<StandaloneCanisterFixture> {
    match try_install_prebuilt_canister_with_cycles(
        build_quickstart_canister_wasm(),
        encode_one(()).expect("encode init args"),
        INIT_CYCLES,
    ) {
        Ok(fixture) => Some(fixture),
        Err(StandaloneCanisterFixtureError::Start(err)) if should_skip_pic_start(&err) => {
            skip_sql_canister_test(err);
            None
        }
        Err(err) => panic!("failed to install quickstart fixture: {err}"),
    }
}

// Execute one unit-shaped update call and assert the canister returned `Ok(())`.
fn expect_unit_update_ok(pic: &Pic, canister_id: Principal, method: &str) {
    let response: Result<(), icydb::Error> = pic
        .update_call(canister_id, method, ())
        .unwrap_or_else(|err| panic!("{method} update call should succeed: {err}"));
    assert!(response.is_ok(), "{method} returned error: {response:?}");
}

// Load the default fixture dataset and assert the update call returned `Ok(())`.
fn load_default_fixtures(pic: &Pic, canister_id: Principal) {
    expect_unit_update_ok(pic, canister_id, "fixtures_load_default");
}

// Reset the default fixture dataset and assert the update call returned `Ok(())`.
fn reset_fixtures(pic: &Pic, canister_id: Principal) {
    expect_unit_update_ok(pic, canister_id, "fixtures_reset");
}

// Execute one canic-testkit-backed integration test body against a fresh
// Pic instance. Keeping the lifecycle per-test matches the harness contract
// and avoids reusing one shared underlying PocketIC process across the whole
// test binary.
fn run_with_pic(test_body: impl FnOnce(&Pic)) {
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

    let _serial_guard = try_acquire_pic_serial_guard()
        .unwrap_or_else(|err| panic!("failed to acquire PocketIC serial guard: {err}"));
    let pic = match try_pic() {
        Ok(pic) => pic,
        Err(err) if should_skip_pic_start(&err) => {
            skip_sql_canister_test(err);
            return;
        }
        Err(err) => panic!("failed to start PocketIC: {err}"),
    };
    let test_result = catch_unwind(AssertUnwindSafe(|| test_body(&pic)));
    drop(pic);

    if let Err(test_panic) = test_result {
        resume_unwind(test_panic);
    }
}

// Execute one integration test body against the dominant fixture shape: a
// fresh Pic with one installed quickstart canister.
fn run_with_quickstart_canister(test_body: impl FnOnce(&Pic, Principal)) {
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

    let Some(fixture) = install_fresh_quickstart_fixture() else {
        return;
    };
    let test_result = catch_unwind(AssertUnwindSafe(|| {
        test_body(&fixture.pic, fixture.canister_id);
    }));
    drop(fixture);

    if let Err(test_panic) = test_result {
        resume_unwind(test_panic);
    }
}

// Execute one integration test body against the common loaded-fixture shape:
// a fresh Pic, one installed quickstart canister, and the default dataset.
fn run_with_loaded_quickstart_canister(test_body: impl FnOnce(&Pic, Principal)) {
    run_with_quickstart_canister(|pic, canister_id| {
        load_default_fixtures(pic, canister_id);
        test_body(pic, canister_id);
    });
}

fn query_result(
    pic: &Pic,
    canister_id: Principal,
    sql: &str,
) -> Result<SqlQueryResult, icydb::Error> {
    pic.query_call(canister_id, "query", (sql.to_string(),))
        .expect("query call should return encoded Result")
}

fn query_projection_rows(
    pic: &Pic,
    canister_id: Principal,
    sql: &str,
    context: &str,
) -> SqlQueryRowsOutput {
    let payload = query_result(pic, canister_id, sql).expect(context);
    match payload {
        SqlQueryResult::Projection(rows) => rows,
        other => panic!("{context}: expected Projection payload, got {other:?}"),
    }
}

fn query_explain_text(pic: &Pic, canister_id: Principal, sql: &str, context: &str) -> String {
    let payload = query_result(pic, canister_id, sql).expect(context);

    match payload {
        SqlQueryResult::Explain { explain, .. } => explain,
        other => panic!("{context}: expected Explain payload, got {other:?}"),
    }
}

// Assert one projected row window against the public SQL payload contract.
//
// Keeping this comparison centralized avoids re-deriving entity, column,
// cardinality, and row-shape expectations at every individual canister test.
fn assert_projection_window(
    rows: &SqlQueryRowsOutput,
    entity: &str,
    columns: &[&str],
    expected_rows: &[&[&str]],
    context: &str,
) {
    let expected_columns = columns
        .iter()
        .map(|column| (*column).to_string())
        .collect::<Vec<_>>();

    assert_eq!(rows.entity, entity, "{context}: unexpected entity");
    assert_eq!(
        rows.columns, expected_columns,
        "{context}: unexpected columns"
    );
    assert_eq!(
        rows.row_count,
        u32::try_from(expected_rows.len()).expect("expected row window length should fit in u32"),
        "{context}: unexpected row_count"
    );
    assert_eq!(
        rows.rows.len(),
        expected_rows.len(),
        "{context}: unexpected row window length"
    );

    for (actual_row, expected_row) in rows.rows.iter().zip(expected_rows.iter()) {
        assert_eq!(
            actual_row.len(),
            expected_row.len(),
            "{context}: unexpected row width",
        );

        for (actual_value, expected_value) in actual_row.iter().zip(expected_row.iter()) {
            if *expected_value == ANY_PROJECTION_VALUE {
                continue;
            }

            assert_eq!(
                actual_value, expected_value,
                "{context}: unexpected row value",
            );
        }
    }
}

// Assert one EXPLAIN payload against a shared route-token contract.
//
// Keeping this logic centralized avoids re-deriving the surface tag, entity,
// required route tokens, and forbidden route tokens at each individual
// canister EXPLAIN test.
fn assert_explain_route(
    payload: SqlQueryResult,
    entity: &str,
    required_tokens: &[&str],
    forbidden_tokens: &[&str],
    context: &str,
) {
    let explain_lines = payload.render_lines();

    assert_eq!(
        explain_lines.first().map(String::as_str),
        Some("surface=explain"),
        "{context}: explain output should be tagged as explain surface",
    );

    match payload {
        SqlQueryResult::Explain {
            entity: actual_entity,
            explain,
        } => {
            assert_eq!(actual_entity, entity, "{context}: unexpected entity");

            for token in required_tokens {
                assert!(
                    explain.contains(token),
                    "{context}: missing explain token `{token}` in {explain}",
                );
            }

            for token in forbidden_tokens {
                assert!(
                    !explain.contains(token),
                    "{context}: unexpected explain token `{token}` in {explain}",
                );
            }
        }
        other => panic!("{context}: expected Explain payload, got {other:?}"),
    }
}

//
// SqlPerfSurface
//
// Mirror of the quickstart canister perf-surface enum used for Candid decode
// and request construction in canic-testkit-backed integration tests.
//

#[derive(candid::CandidType, Clone, Copy, Debug, candid::Deserialize, Serialize)]
enum SqlPerfSurface {
    GeneratedDispatch,
    TypedDispatchUser,
    TypedDispatchCharacter,
    TypedDispatchActiveUser,
    TypedQueryFromSqlUserExecute,
    TypedExecuteSqlUser,
    TypedInsertUser,
    TypedInsertManyAtomicUser10,
    TypedInsertManyAtomicUser100,
    TypedInsertManyAtomicUser1000,
    TypedInsertManyNonAtomicUser10,
    TypedInsertManyNonAtomicUser100,
    TypedInsertManyNonAtomicUser1000,
    TypedUpdateUser,
    FluentDeleteUserOrderIdLimit1Count,
    FluentDeletePerfUserCount,
    TypedExecuteSqlGroupedUser,
    TypedExecuteSqlGroupedUserSecondPage,
    TypedExecuteSqlAggregateUser,
    FluentLoadUserOrderIdLimit2,
    FluentLoadUserNameEqLimit1,
    FluentPagedUserOrderIdLimit2FirstPage,
    FluentPagedUserOrderIdLimit2SecondPage,
    FluentPagedUserOrderIdLimit2InvalidCursor,
}

//
// SqlPerfAttributionSurface
//
// Mirror of the quickstart canister SQL attribution surface enum used by the
// canic-testkit-backed perf attribution test.
//

#[derive(candid::CandidType, Clone, Copy, Debug, candid::Deserialize, Serialize)]
enum SqlPerfAttributionSurface {
    GeneratedDispatch,
    TypedDispatchUser,
    TypedDispatchCharacter,
    TypedDispatchActiveUser,
    TypedGroupedUser,
    TypedGroupedUserSecondPage,
}

//
// SqlPerfRequest
//
// One integration-test request into the quickstart canister perf harness.
// This keeps scenario identity explicit in the test runner instead of hiding
// request shape inside inline Candid tuples.
//

#[derive(candid::CandidType, Clone, Debug, candid::Deserialize, Serialize)]
struct SqlPerfRequest {
    surface: SqlPerfSurface,
    sql: String,
    cursor_token: Option<String>,
    repeat_count: u32,
}

//
// SqlPerfAttributionRequest
//
// One integration-test request into the quickstart canister SQL attribution
// endpoint.
//

#[derive(candid::CandidType, Clone, Debug, candid::Deserialize, Serialize)]
struct SqlPerfAttributionRequest {
    surface: SqlPerfAttributionSurface,
    sql: String,
    cursor_token: Option<String>,
}

//
// SqlPerfOutcome
//
// Compact quickstart perf-harness outcome mirror used by integration tests.
// The audit collector only needs stable surface kind and cardinality metadata
// here; full SQL payload inspection remains in the main SQL integration tests.
//

#[derive(candid::CandidType, Clone, Debug, candid::Deserialize, Serialize)]
struct SqlPerfOutcome {
    success: bool,
    result_kind: String,
    entity: Option<String>,
    row_count: Option<u32>,
    detail_count: Option<u32>,
    has_cursor: Option<bool>,
    rendered_value: Option<String>,
    error_kind: Option<String>,
    error_origin: Option<String>,
    error_message: Option<String>,
}

//
// SqlPerfSample
//
// One repeated wasm-side instruction sample returned by the quickstart
// canister perf harness.
//

#[derive(candid::CandidType, Clone, Debug, candid::Deserialize, Serialize)]
struct SqlPerfSample {
    surface: SqlPerfSurface,
    sql: String,
    cursor_token: Option<String>,
    repeat_count: u32,
    first_local_instructions: u64,
    min_local_instructions: u64,
    max_local_instructions: u64,
    total_local_instructions: u64,
    avg_local_instructions: u64,
    outcome_stable: bool,
    outcome: SqlPerfOutcome,
}

//
// SqlPerfAttributionSample
//
// One fixed-cost SQL query attribution sample returned by the quickstart
// canister perf harness.
//

#[derive(candid::CandidType, Clone, Debug, candid::Deserialize, Serialize)]
struct SqlPerfAttributionSample {
    surface: SqlPerfAttributionSurface,
    sql: String,
    parse_local_instructions: u64,
    route_local_instructions: u64,
    lower_local_instructions: u64,
    dispatch_local_instructions: u64,
    execute_local_instructions: u64,
    wrapper_local_instructions: u64,
    total_local_instructions: u64,
    outcome: SqlPerfOutcome,
}

//
// SqlPerfScenario
//
// One named audit scenario captured through the quickstart canister perf
// harness.
//

#[derive(Clone, Debug, Serialize)]
struct SqlPerfScenario {
    scenario_key: &'static str,
    request: SqlPerfRequest,
}

//
// SqlPerfScenarioRow
//
// Serializable row pairing one stable scenario identity with one measured
// quickstart perf-harness sample.
//

#[derive(Clone, Debug, Serialize)]
struct SqlPerfScenarioRow {
    scenario_key: &'static str,
    sample: SqlPerfSample,
}

fn sql_perf_sample(pic: &Pic, canister_id: Principal, request: &SqlPerfRequest) -> SqlPerfSample {
    let response: Result<SqlPerfSample, icydb::Error> = pic
        .query_call(canister_id, "sql_perf", (request,))
        .expect("sql_perf query call should return encoded Result");

    response.expect("sql_perf should succeed for integration scenario")
}

fn sql_perf_attribution_sample(
    pic: &Pic,
    canister_id: Principal,
    request: &SqlPerfAttributionRequest,
) -> SqlPerfAttributionSample {
    let response: Result<SqlPerfAttributionSample, icydb::Error> = pic
        .query_call(canister_id, "sql_perf_attribution", (request,))
        .expect("sql_perf_attribution query call should return encoded Result");

    response.expect("sql_perf_attribution should succeed for integration scenario")
}

fn optional_non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn normalized_perf_probe_surface_key() -> Option<String> {
    optional_non_empty_env(SQL_PERF_PROBE_SURFACE_ENV).map(|value| value.to_ascii_lowercase())
}

fn sql_perf_probe_sql() -> String {
    optional_non_empty_env(SQL_PERF_PROBE_SQL_ENV)
        .unwrap_or_else(|| DEFAULT_SQL_PERF_PROBE_SQL.to_string())
}

fn sql_perf_probe_cursor_token() -> Option<String> {
    optional_non_empty_env(SQL_PERF_PROBE_CURSOR_ENV)
}

fn sql_perf_probe_repeat_count() -> u32 {
    let Some(raw_repeat_count) = optional_non_empty_env(SQL_PERF_PROBE_REPEAT_ENV) else {
        return 5;
    };

    raw_repeat_count.parse::<u32>().unwrap_or_else(|err| {
        panic!(
            "{SQL_PERF_PROBE_REPEAT_ENV} must parse as a positive u32 repeat count, got '{raw_repeat_count}': {err}"
        )
    })
}

fn sql_perf_probe_sample_surface() -> SqlPerfSurface {
    let Some(surface_key) = normalized_perf_probe_surface_key() else {
        return SqlPerfSurface::GeneratedDispatch;
    };

    match surface_key.as_str() {
        "generated" | "generateddispatch" | "generated_dispatch" => {
            SqlPerfSurface::GeneratedDispatch
        }
        "typeddispatchuser" | "typed_dispatch_user" => SqlPerfSurface::TypedDispatchUser,
        "typeddispatchcharacter" | "typed_dispatch_character" => {
            SqlPerfSurface::TypedDispatchCharacter
        }
        "typeddispatchactiveuser" | "typed_dispatch_active_user" => {
            SqlPerfSurface::TypedDispatchActiveUser
        }
        "typedqueryfromsqluserexecute" | "typed_query_from_sql_user_execute" => {
            SqlPerfSurface::TypedQueryFromSqlUserExecute
        }
        "typedexecutesqluser" | "typed_execute_sql_user" => SqlPerfSurface::TypedExecuteSqlUser,
        "typedinsertuser" | "typed_insert_user" => SqlPerfSurface::TypedInsertUser,
        "typedinsertmanyatomicuser10" | "typed_insert_many_atomic_user_10" => {
            SqlPerfSurface::TypedInsertManyAtomicUser10
        }
        "typedinsertmanyatomicuser100" | "typed_insert_many_atomic_user_100" => {
            SqlPerfSurface::TypedInsertManyAtomicUser100
        }
        "typedinsertmanyatomicuser1000" | "typed_insert_many_atomic_user_1000" => {
            SqlPerfSurface::TypedInsertManyAtomicUser1000
        }
        "typedinsertmanynonatomicuser10" | "typed_insert_many_non_atomic_user_10" => {
            SqlPerfSurface::TypedInsertManyNonAtomicUser10
        }
        "typedinsertmanynonatomicuser100" | "typed_insert_many_non_atomic_user_100" => {
            SqlPerfSurface::TypedInsertManyNonAtomicUser100
        }
        "typedinsertmanynonatomicuser1000" | "typed_insert_many_non_atomic_user_1000" => {
            SqlPerfSurface::TypedInsertManyNonAtomicUser1000
        }
        "typedupdateuser" | "typed_update_user" => SqlPerfSurface::TypedUpdateUser,
        "fluentdeleteuserorderidlimit1count" | "fluent_delete_user_order_id_limit_1_count" => {
            SqlPerfSurface::FluentDeleteUserOrderIdLimit1Count
        }
        "fluentdeleteperfusercount" | "fluent_delete_perf_user_count" => {
            SqlPerfSurface::FluentDeletePerfUserCount
        }
        "typedexecutesqlgroupeduser" | "typed_execute_sql_grouped_user" => {
            SqlPerfSurface::TypedExecuteSqlGroupedUser
        }
        "typedexecutesqlgroupedusersecondpage" | "typed_execute_sql_grouped_user_second_page" => {
            SqlPerfSurface::TypedExecuteSqlGroupedUserSecondPage
        }
        "typedexecutesqlaggregateuser" | "typed_execute_sql_aggregate_user" => {
            SqlPerfSurface::TypedExecuteSqlAggregateUser
        }
        "fluentloaduserorderidlimit2" | "fluent_load_user_order_id_limit_2" => {
            SqlPerfSurface::FluentLoadUserOrderIdLimit2
        }
        "fluentloadusernameeqlimit1" | "fluent_load_user_name_eq_limit_1" => {
            SqlPerfSurface::FluentLoadUserNameEqLimit1
        }
        "fluentpageduserorderidlimit2firstpage"
        | "fluent_paged_user_order_id_limit_2_first_page" => {
            SqlPerfSurface::FluentPagedUserOrderIdLimit2FirstPage
        }
        "fluentpageduserorderidlimit2secondpage"
        | "fluent_paged_user_order_id_limit_2_second_page" => {
            SqlPerfSurface::FluentPagedUserOrderIdLimit2SecondPage
        }
        "fluentpageduserorderidlimit2invalidcursor"
        | "fluent_paged_user_order_id_limit_2_invalid_cursor" => {
            SqlPerfSurface::FluentPagedUserOrderIdLimit2InvalidCursor
        }
        _ => panic!(
            "unsupported {SQL_PERF_PROBE_SURFACE_ENV} value '{surface_key}' for sql perf sample probe"
        ),
    }
}

fn sql_perf_probe_attribution_surface() -> SqlPerfAttributionSurface {
    let Some(surface_key) = normalized_perf_probe_surface_key() else {
        return SqlPerfAttributionSurface::GeneratedDispatch;
    };

    match surface_key.as_str() {
        "generated" | "generateddispatch" | "generated_dispatch" => {
            SqlPerfAttributionSurface::GeneratedDispatch
        }
        "typeddispatchuser" | "typed_dispatch_user" => SqlPerfAttributionSurface::TypedDispatchUser,
        "typeddispatchcharacter" | "typed_dispatch_character" => {
            SqlPerfAttributionSurface::TypedDispatchCharacter
        }
        "typeddispatchactiveuser" | "typed_dispatch_active_user" => {
            SqlPerfAttributionSurface::TypedDispatchActiveUser
        }
        "typedgroupeduser" | "typed_grouped_user" => SqlPerfAttributionSurface::TypedGroupedUser,
        "typedgroupedusersecondpage" | "typed_grouped_user_second_page" => {
            SqlPerfAttributionSurface::TypedGroupedUserSecondPage
        }
        _ => panic!(
            "unsupported {SQL_PERF_PROBE_SURFACE_ENV} value '{surface_key}' for sql perf attribution probe"
        ),
    }
}

fn run_sql_perf_scenarios(pic: &Pic, scenarios: Vec<SqlPerfScenario>) -> Vec<SqlPerfScenarioRow> {
    let mut rows = Vec::with_capacity(scenarios.len());

    for scenario in scenarios {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

        let sample = sql_perf_sample(pic, canister_id, &scenario.request);
        assert_positive_perf_sample(scenario.scenario_key, &sample);

        rows.push(SqlPerfScenarioRow {
            scenario_key: scenario.scenario_key,
            sample,
        });
    }

    rows
}

fn sql_perf_scenario(
    scenario_key: &'static str,
    surface: SqlPerfSurface,
    sql: &str,
    repeat_count: u32,
) -> SqlPerfScenario {
    SqlPerfScenario {
        scenario_key,
        request: SqlPerfRequest {
            surface,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count,
        },
    }
}

// Assert one repeated perf sample stays structurally sane before more
// scenario-specific entity and route checks are layered on top.
fn assert_positive_perf_sample(label: &str, sample: &SqlPerfSample) {
    assert!(
        sample.first_local_instructions > 0,
        "first instruction sample must be positive for {label}: {sample:?}",
    );
    assert!(
        sample.min_local_instructions > 0,
        "min instruction sample must be positive for {label}: {sample:?}",
    );
    assert!(
        sample.max_local_instructions >= sample.min_local_instructions,
        "max must be >= min for {label}: {sample:?}",
    );
    assert!(
        sample.total_local_instructions >= sample.first_local_instructions,
        "total must cover the first run for {label}: {sample:?}",
    );
    assert!(
        sample.outcome_stable,
        "repeated outcome must stay stable for {label}: {sample:?}",
    );
}

// Keep scalar attribution focused on a small representative SELECT cohort so
// read-path tuning does not overfit one especially friendly benchmark query.
const SCALAR_SELECT_ATTRIBUTION_CASES: &[(&str, &str, SqlPerfAttributionSurface, &str, u32)] = &[
    (
        "user_name_eq_limit1",
        "SELECT id, name FROM User WHERE name = 'alice' ORDER BY id LIMIT 1",
        SqlPerfAttributionSurface::TypedDispatchUser,
        "User",
        1,
    ),
    (
        "user_full_row_limit2",
        "SELECT * FROM User ORDER BY id LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchUser,
        "User",
        2,
    ),
    (
        "user_name_order_name_limit1",
        "SELECT name FROM User ORDER BY name ASC LIMIT 1",
        SqlPerfAttributionSurface::TypedDispatchUser,
        "User",
        1,
    ),
    (
        "user_age_order_id_limit1",
        "SELECT age FROM User ORDER BY id ASC LIMIT 1",
        SqlPerfAttributionSurface::TypedDispatchUser,
        "User",
        1,
    ),
    (
        "user_primary_key_covering_id_limit1",
        "SELECT id FROM User ORDER BY id ASC LIMIT 1",
        SqlPerfAttributionSurface::TypedDispatchUser,
        "User",
        1,
    ),
    (
        "user_secondary_covering_name_limit2_asc",
        "SELECT id, name FROM User ORDER BY name ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchUser,
        "User",
        2,
    ),
    (
        "user_secondary_covering_name_limit2_desc",
        "SELECT id, name FROM User ORDER BY name DESC, id DESC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchUser,
        "User",
        2,
    ),
    (
        "user_secondary_covering_name_strict_range_limit2_asc",
        "SELECT id, name FROM User WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchUser,
        "User",
        2,
    ),
    (
        "user_secondary_covering_name_strict_range_limit2_desc",
        "SELECT id, name FROM User WHERE name >= 'a' AND name < 'c' ORDER BY name DESC, id DESC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchUser,
        "User",
        2,
    ),
    (
        "character_order_only_composite_limit2_asc",
        "SELECT id, level, class_name FROM Character ORDER BY level ASC, class_name ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCharacter,
        "Character",
        2,
    ),
    (
        "character_order_only_composite_limit2_desc",
        "SELECT id, level, class_name FROM Character ORDER BY level DESC, class_name DESC, id DESC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCharacter,
        "Character",
        2,
    ),
    (
        "character_numeric_equality_level20_limit2_asc",
        "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCharacter,
        "Character",
        2,
    ),
    (
        "character_numeric_equality_level20_limit2_desc",
        "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name DESC, id DESC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCharacter,
        "Character",
        2,
    ),
    (
        "active_user_filtered_order_only_name_limit2_asc",
        "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_composite_expression_order_only_handle_limit2_asc",
        "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_composite_expression_key_only_order_only_tier_limit2_asc",
        "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_composite_expression_key_only_order_only_tier_limit2_desc",
        "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_composite_expression_key_only_strict_range_tier_limit2_asc",
        "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_composite_expression_key_only_direct_starts_with_tier_limit2_asc",
        "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
];

// Keep non-User ordered covering perf parity focused on the read shapes that
// drove the recent `0.68` planner and route work.
const NON_USER_ORDERED_COVERING_PERF_CASES: &[(&str, &str, SqlPerfSurface, &str, u32)] = &[
    (
        "character_order_only_composite.level_class_id_limit2.asc",
        "SELECT id, level, class_name FROM Character ORDER BY level ASC, class_name ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchCharacter,
        "Character",
        2,
    ),
    (
        "character_order_only_composite.level_class_id_limit2.desc",
        "SELECT id, level, class_name FROM Character ORDER BY level DESC, class_name DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchCharacter,
        "Character",
        2,
    ),
    (
        "character_numeric_equality.level_eq20_class_id_limit2.asc",
        "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchCharacter,
        "Character",
        2,
    ),
    (
        "character_numeric_equality.level_eq20_class_id_limit2.desc",
        "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchCharacter,
        "Character",
        2,
    ),
    (
        "character_numeric_equality_bounded_class_name.level_eq20_class_bd_limit2.asc",
        "SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchCharacter,
        "Character",
        2,
    ),
    (
        "character_numeric_equality_bounded_class_name.level_eq20_class_bd_limit2.desc",
        "SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchCharacter,
        "Character",
        2,
    ),
    (
        "active_user_filtered_order_only_name_limit2.asc",
        "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_order_only_name_limit2.desc",
        "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_composite_order_only_handle_limit2.asc",
        "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_composite_order_only_handle_limit2.desc",
        "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_composite_expression_order_only_handle_limit2.asc",
        "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_composite_expression_order_only_handle_limit2.desc",
        "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_composite_expression_key_only_order_only_tier_limit2.asc",
        "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_composite_expression_key_only_order_only_tier_limit2.desc",
        "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_composite_expression_key_only_strict_range_tier_limit2.asc",
        "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
    (
        "active_user_filtered_composite_expression_key_only_direct_starts_with_tier_limit2.asc",
        "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchActiveUser,
        "ActiveUser",
        2,
    ),
];

// Assert one scalar attribution sample stays structurally sane for perf
// reporting across the representative SELECT cohort.
fn assert_positive_scalar_attribution_sample(
    label: &str,
    sample: &SqlPerfAttributionSample,
    expect_positive_route: bool,
) {
    assert!(
        sample.outcome.success,
        "{label} attribution must keep the representative SELECT successful: {sample:?}",
    );
    assert!(
        sample.parse_local_instructions > 0,
        "{label} parse phase must be positive: {sample:?}",
    );
    assert!(
        sample.lower_local_instructions > 0,
        "{label} lower phase must be positive: {sample:?}",
    );
    assert!(
        sample.execute_local_instructions > 0,
        "{label} execute phase must be positive: {sample:?}",
    );
    assert!(
        sample.total_local_instructions
            >= sample.parse_local_instructions
                + sample.route_local_instructions
                + sample.lower_local_instructions
                + sample.dispatch_local_instructions
                + sample.execute_local_instructions
                + sample.wrapper_local_instructions,
        "{label} total must cover every attributed phase: {sample:?}",
    );

    if expect_positive_route {
        assert!(
            sample.route_local_instructions > 0,
            "{label} attribution must report positive authority routing cost: {sample:?}",
        );
    } else {
        assert_eq!(
            sample.route_local_instructions, 0,
            "{label} attribution should not report dynamic route-authority cost: {sample:?}",
        );
    }
}

// Compare one generated-vs-typed perf pair for the same query shape without
// pretending the instruction totals themselves must match exactly.
fn assert_matching_perf_outcomes(
    scenario_key: &str,
    generated: &SqlPerfSample,
    typed: &SqlPerfSample,
    expected_entity: &str,
    expected_row_count: u32,
) {
    assert_positive_perf_sample(&format!("generated.{scenario_key}"), generated);
    assert_positive_perf_sample(&format!("typed.{scenario_key}"), typed);

    assert!(
        generated.outcome.success,
        "generated.{scenario_key} perf sample must succeed: {generated:?}",
    );
    assert!(
        typed.outcome.success,
        "typed.{scenario_key} perf sample must succeed: {typed:?}",
    );
    assert_eq!(
        generated.outcome.result_kind, typed.outcome.result_kind,
        "{scenario_key}: generated and typed result kinds must match",
    );
    assert_eq!(
        generated.outcome.entity.as_deref(),
        Some(expected_entity),
        "{scenario_key}: generated perf sample should stay on the expected entity route",
    );
    assert_eq!(
        typed.outcome.entity.as_deref(),
        Some(expected_entity),
        "{scenario_key}: typed perf sample should stay on the expected entity route",
    );
    assert_eq!(
        generated.outcome.row_count,
        Some(expected_row_count),
        "{scenario_key}: generated perf sample should return the requested window size",
    );
    assert_eq!(
        typed.outcome.row_count,
        Some(expected_row_count),
        "{scenario_key}: typed perf sample should return the requested window size",
    );
    assert_eq!(
        generated.outcome.detail_count, typed.outcome.detail_count,
        "{scenario_key}: generated and typed projection detail counts must match",
    );
    assert_eq!(
        generated.outcome.has_cursor, typed.outcome.has_cursor,
        "{scenario_key}: generated and typed cursor behavior must match",
    );
    assert_eq!(
        generated.outcome.error_kind, typed.outcome.error_kind,
        "{scenario_key}: generated and typed error kinds must match",
    );
    assert_eq!(
        generated.outcome.error_origin, typed.outcome.error_origin,
        "{scenario_key}: generated and typed error origins must match",
    );
}

fn select_operation_repeat_scenarios() -> Vec<SqlPerfScenario> {
    let sql = "SELECT id, name FROM User WHERE name = 'alice' ORDER BY id LIMIT 1";

    vec![
        sql_perf_scenario(
            "select.generated.dispatch.user_name_eq_limit.x1",
            SqlPerfSurface::GeneratedDispatch,
            sql,
            1,
        ),
        sql_perf_scenario(
            "select.generated.dispatch.user_name_eq_limit.x10",
            SqlPerfSurface::GeneratedDispatch,
            sql,
            10,
        ),
        sql_perf_scenario(
            "select.generated.dispatch.user_name_eq_limit.x100",
            SqlPerfSurface::GeneratedDispatch,
            sql,
            100,
        ),
        sql_perf_scenario(
            "select.typed.dispatch.user_name_eq_limit.x1",
            SqlPerfSurface::TypedDispatchUser,
            sql,
            1,
        ),
        sql_perf_scenario(
            "select.typed.dispatch.user_name_eq_limit.x10",
            SqlPerfSurface::TypedDispatchUser,
            sql,
            10,
        ),
        sql_perf_scenario(
            "select.typed.dispatch.user_name_eq_limit.x100",
            SqlPerfSurface::TypedDispatchUser,
            sql,
            100,
        ),
    ]
}

fn insert_operation_repeat_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        sql_perf_scenario(
            "insert.typed.user_single.x1",
            SqlPerfSurface::TypedInsertUser,
            "INSERT User",
            1,
        ),
        sql_perf_scenario(
            "insert.typed.user_single.x10",
            SqlPerfSurface::TypedInsertUser,
            "INSERT User",
            10,
        ),
        sql_perf_scenario(
            "insert.typed.user_single.x100",
            SqlPerfSurface::TypedInsertUser,
            "INSERT User",
            100,
        ),
    ]
}

fn update_operation_repeat_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        sql_perf_scenario(
            "update.typed.user_single.x1",
            SqlPerfSurface::TypedUpdateUser,
            "UPDATE User",
            1,
        ),
        sql_perf_scenario(
            "update.typed.user_single.x10",
            SqlPerfSurface::TypedUpdateUser,
            "UPDATE User",
            10,
        ),
        sql_perf_scenario(
            "update.typed.user_single.x100",
            SqlPerfSurface::TypedUpdateUser,
            "UPDATE User",
            100,
        ),
    ]
}

fn delete_operation_repeat_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        sql_perf_scenario(
            "delete.fluent.user_single.count.x1",
            SqlPerfSurface::FluentDeletePerfUserCount,
            "DELETE PERF User COUNT",
            1,
        ),
        sql_perf_scenario(
            "delete.fluent.user_single.count.x10",
            SqlPerfSurface::FluentDeletePerfUserCount,
            "DELETE PERF User COUNT",
            10,
        ),
        sql_perf_scenario(
            "delete.fluent.user_single.count.x100",
            SqlPerfSurface::FluentDeletePerfUserCount,
            "DELETE PERF User COUNT",
            100,
        ),
    ]
}

fn sql_operation_repeat_scenarios() -> Vec<SqlPerfScenario> {
    let mut scenarios = Vec::new();
    scenarios.extend(select_operation_repeat_scenarios());
    scenarios.extend(insert_operation_repeat_scenarios());
    scenarios.extend(update_operation_repeat_scenarios());
    scenarios.extend(delete_operation_repeat_scenarios());

    scenarios
}

// Read the stable entity name from one metadata-lane SQL payload.
const fn metadata_entity_name(payload: &SqlQueryResult) -> Option<&str> {
    match payload {
        SqlQueryResult::Describe(description) => Some(description.entity_name()),
        SqlQueryResult::ShowIndexes { entity, .. } | SqlQueryResult::ShowColumns { entity, .. } => {
            Some(entity.as_str())
        }
        _ => None,
    }
}

// Run one normalized metadata-lane SQL statement and assert the stable entity name.
fn assert_metadata_entity_name(
    pic: &Pic,
    canister_id: Principal,
    sql: &str,
    expected_entity: &str,
    context: &str,
) {
    let payload = query_result(pic, canister_id, sql).expect(context);
    assert_eq!(
        metadata_entity_name(&payload),
        Some(expected_entity),
        "{context}",
    );
}

#[test]
fn sql_canister_smoke_flow() {
    run_with_quickstart_canister(|pic, canister_id| {
        let entities: Vec<String> = pic
            .query_call(canister_id, "sql_entities", ())
            .expect("sql_entities query call should succeed");
        assert!(entities.iter().any(|name| name == "User"));
        assert!(entities.iter().any(|name| name == "ActiveUser"));
        assert!(entities.iter().any(|name| name == "Order"));
        assert!(entities.iter().any(|name| name == "Character"));

        let show_entities_payload = query_result(pic, canister_id, "SHOW ENTITIES")
            .expect("SHOW ENTITIES query should return an Ok payload");
        let show_entities_lines = show_entities_payload.render_lines();
        assert_eq!(
            show_entities_lines.first().map(String::as_str),
            Some("surface=entities"),
            "SHOW ENTITIES output should be tagged as entity-list surface",
        );
        match show_entities_payload {
            SqlQueryResult::ShowEntities {
                entities: show_entities,
            } => {
                assert!(
                    show_entities.iter().any(|entity| entity == "User"),
                    "SHOW ENTITIES payload should include User",
                );
                assert!(
                    show_entities.iter().any(|entity| entity == "ActiveUser"),
                    "SHOW ENTITIES payload should include ActiveUser",
                );
                assert!(
                    show_entities.iter().any(|entity| entity == "Order"),
                    "SHOW ENTITIES payload should include Order",
                );
                assert!(
                    show_entities.iter().any(|entity| entity == "Character"),
                    "SHOW ENTITIES payload should include Character",
                );
            }
            other => panic!("SHOW ENTITIES should return ShowEntities payload, got {other:?}"),
        }

        load_default_fixtures(pic, canister_id);

        let explain_payload = query_result(
            pic,
            canister_id,
            "EXPLAIN SELECT name FROM User ORDER BY name LIMIT 1",
        )
        .expect("EXPLAIN query should return an Ok payload");
        assert_explain_route(
            explain_payload,
            "User",
            &[],
            &[],
            "EXPLAIN query should return a User explain payload",
        );

        let query_sql = "SELECT name FROM User ORDER BY name LIMIT 1";
        let projection =
            query_projection_rows(pic, canister_id, query_sql, "query endpoint should project");
        assert_eq!(projection.entity, "User");
        assert_eq!(projection.row_count, 1);
        assert_eq!(projection.columns, vec!["name".to_string()]);
        assert_eq!(projection.rows, vec![vec!["alice".to_string()]]);

        let projection_lines = query_result(pic, canister_id, query_sql)
            .expect("projection query should return an Ok payload")
            .render_lines();
        assert!(
            projection_lines
                .first()
                .is_some_and(|line| line.contains("surface=projection")),
            "projection output should be tagged as projection surface",
        );
        assert!(
            projection_lines.iter().any(|line| line.contains("alice")),
            "projection output should include projected row values",
        );

        reset_fixtures(pic, canister_id);
    });
}

#[test]
fn sql_canister_query_lane_supports_user_secondary_covering_order_only_projection_windows() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let asc_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User ORDER BY name ASC, id ASC LIMIT 2",
            "ascending User secondary covering projection should return projected rows",
        );
        assert_projection_window(
            &asc_rows,
            "User",
            &["id", "name"],
            &[
                &[ANY_PROJECTION_VALUE, "alice"],
                &[ANY_PROJECTION_VALUE, "bob"],
            ],
            "ascending User secondary covering projection should preserve ordered rows",
        );

        let desc_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User ORDER BY name DESC, id DESC LIMIT 2",
            "descending User secondary covering projection should return projected rows",
        );
        assert_projection_window(
            &desc_rows,
            "User",
            &["id", "name"],
            &[
                &[ANY_PROJECTION_VALUE, "charlie"],
                &[ANY_PROJECTION_VALUE, "bob"],
            ],
            "descending User secondary covering projection should preserve ordered rows",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_secondary_covering_witness_validated_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM User ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("query User secondary covering EXPLAIN EXECUTION should return an Ok payload");
        assert_explain_route(
            payload,
            "User",
            &[
                "CoveringRead",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "name",
            ],
            &["row_check_required"],
            "User secondary covering EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_secondary_covering_equality_witness_validated_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM User WHERE name = 'alice' ORDER BY id LIMIT 1",
        )
        .expect(
            "query User secondary covering equality EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "User",
            &[
                "CoveringRead",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "name",
            ],
            &["row_check_required"],
            "User secondary covering equality EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_secondary_covering_equality_desc_witness_validated_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM User WHERE name = 'alice' ORDER BY id DESC LIMIT 1",
        )
        .expect(
            "query User secondary covering equality desc EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "User",
            &[
                "CoveringRead",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "name",
            ],
            &["row_check_required"],
            "User secondary covering equality desc EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_user_secondary_covering_strict_range_projection_window() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2",
            "ascending User secondary covering range projection should return projected rows",
        );
        assert_projection_window(
            &rows,
            "User",
            &["id", "name"],
            &[
                &[ANY_PROJECTION_VALUE, "alice"],
                &[ANY_PROJECTION_VALUE, "bob"],
            ],
            "ascending User secondary covering range projection should preserve ordered rows",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_secondary_covering_strict_range_witness_validated_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM User WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect(
            "query User secondary covering range EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "User",
            &[
                "CoveringRead",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "name",
            ],
            &["row_check_required"],
            "User secondary covering range EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_user_secondary_covering_strict_range_desc_projection_window() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE name >= 'a' AND name < 'c' ORDER BY name DESC, id DESC LIMIT 2",
            "descending User secondary covering range projection should return projected rows",
        );
        assert_projection_window(
            &rows,
            "User",
            &["id", "name"],
            &[
                &[ANY_PROJECTION_VALUE, "bob"],
                &[ANY_PROJECTION_VALUE, "alice"],
            ],
            "descending User secondary covering range projection should preserve ordered rows",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_secondary_covering_strict_range_desc_witness_validated_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM User WHERE name >= 'a' AND name < 'c' ORDER BY name DESC, id DESC LIMIT 2",
        )
        .expect(
            "query User secondary covering desc range EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "User",
            &[
                "CoveringRead",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "name",
            ],
            &["row_check_required"],
            "User secondary covering desc range EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_delete_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let deleted_rows = query_projection_rows(
            pic,
            canister_id,
            "DELETE FROM User ORDER BY id LIMIT 1",
            "query DELETE should return deleted projection rows",
        );

        assert_eq!(deleted_rows.entity, "User");
        assert_eq!(deleted_rows.row_count, 1);
        assert_eq!(deleted_rows.rows.len(), 1);
        assert!(
            !deleted_rows.columns.is_empty(),
            "DELETE projection should keep canonical entity columns",
        );
    });
}

#[test]
fn sql_canister_query_lane_delete_direct_starts_with_family_matches_like_rows() {
    run_with_quickstart_canister(|pic, canister_id| {
        // Phase 1: compare the accepted direct family against the established
        // LIKE forms on the generated query/delete boundary.
        let cases = [
            (
                "DELETE FROM User WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 1",
                "DELETE FROM User WHERE name LIKE 'a%' ORDER BY id LIMIT 1",
                "generated strict direct STARTS_WITH delete",
            ),
            (
                "DELETE FROM User WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 1",
                "DELETE FROM User WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 1",
                "generated direct LOWER(field) STARTS_WITH delete",
            ),
            (
                "DELETE FROM User WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
                "DELETE FROM User WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 1",
                "generated direct LOWER(field) ordered text-range delete",
            ),
            (
                "DELETE FROM User WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 1",
                "DELETE FROM User WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 1",
                "generated direct UPPER(field) STARTS_WITH delete",
            ),
            (
                "DELETE FROM User WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
                "DELETE FROM User WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 1",
                "generated direct UPPER(field) ordered text-range delete",
            ),
        ];

        // Phase 2: execute both spellings against fresh fixtures so the deleted
        // row payload remains semantically identical on the generated canister
        // surface. Fixture reloads mint fresh ids, so compare stable
        // non-identity columns instead of the raw full-row payload.
        for (direct_sql, like_sql, context) in cases {
            reset_fixtures(pic, canister_id);
            load_default_fixtures(pic, canister_id);
            let direct = query_projection_rows(
                pic,
                canister_id,
                direct_sql,
                "generated direct STARTS_WITH delete should return projection rows",
            );

            reset_fixtures(pic, canister_id);
            load_default_fixtures(pic, canister_id);
            let like = query_projection_rows(
                pic,
                canister_id,
                like_sql,
                "generated LIKE delete should return projection rows",
            );

            assert_eq!(
                direct.columns, like.columns,
                "generated direct STARTS_WITH delete should keep canonical delete columns: {context}",
            );
            let stable_delete_rows = |rows: &SqlQueryRowsOutput| {
                let id_index = rows
                    .columns
                    .iter()
                    .position(|column| column == "id")
                    .expect("generated delete projection should expose canonical id column");

                rows.rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .enumerate()
                            .filter(|(index, _)| *index != id_index)
                            .map(|(_, value)| value.clone())
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                stable_delete_rows(&direct),
                stable_delete_rows(&like),
                "generated direct STARTS_WITH delete should match the established LIKE delete payload aside from regenerated ids: {context}",
            );
        }
    });
}

#[test]
fn sql_canister_query_lane_delete_rejects_non_casefold_wrapped_direct_starts_with() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let err = query_result(
            pic,
            canister_id,
            "DELETE FROM User WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 1",
        )
        .expect_err("generated direct STARTS_WITH delete wrapper should fail closed");

        assert!(
            matches!(
                err.kind(),
                icydb::error::ErrorKind::Runtime(icydb::error::RuntimeErrorKind::Unsupported)
            ),
            "generated direct STARTS_WITH delete wrapper should map to Runtime::Unsupported: {err:?}",
        );
        assert!(
            err.message().contains(
                "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers"
            ),
            "generated direct STARTS_WITH delete wrapper should preserve the stable unsupported-feature detail: {err:?}",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_computed_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT LOWER(name) FROM User ORDER BY id LIMIT 2",
            "query computed projection should return projected rows",
        );

        assert_eq!(rows.entity, "User");
        assert_eq!(rows.columns, vec!["LOWER(name)".to_string()]);
        assert_eq!(
            rows.rows,
            vec![vec!["alice".to_string()], vec!["bob".to_string()],],
        );
        assert_eq!(rows.row_count, 2);
    });
}

#[test]
fn sql_canister_query_lane_supports_user_expression_order_covering_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one expression-order User projection so the
        // generated SQL lane proves the new LOWER(name) secondary order path.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            "query User expression-order covering projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the expected
        // projected User window and column order.
        assert_eq!(rows.entity, "User");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0][1],
            "alice".to_string(),
            "expression-order User query should start from the lowercased first row",
        );
        assert_eq!(
            rows.rows[1][1],
            "bob".to_string(),
            "expression-order User query should keep stable lowercased ordering",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_expression_order_covering_route() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the new expression
        // order-only User projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM User ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query User expression-order covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "User",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "materialized",
                "LOWER(name)",
                "proj_fields",
                "id",
                "name",
            ],
            &[],
            "User expression-order EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_user_expression_order_desc_covering_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one descending expression-order User projection so
        // reverse traversal stays locked in the generated SQL harness.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            "query User descending expression-order covering projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the expected
        // descending projected User window.
        assert_eq!(rows.entity, "User");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0][1],
            "charlie".to_string(),
            "descending expression-order User query should start from the last lowercased row",
        );
        assert_eq!(
            rows.rows[1][1],
            "bob".to_string(),
            "descending expression-order User query should keep stable reverse lowercased ordering",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_expression_order_desc_covering_route() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending
        // expression order-only User projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM User ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query User descending expression-order covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "User",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "materialized",
                "LOWER(name)",
                "proj_fields",
                "id",
                "name",
            ],
            &[],
            "descending User expression-order EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_user_primary_key_covering_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id FROM User ORDER BY id ASC LIMIT 1",
            "query User PK-only covering projection should return projected rows",
        );

        assert_eq!(rows.entity, "User");
        assert_eq!(rows.columns, vec!["id".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].len(), 1);
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_primary_key_covering_route() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id FROM User ORDER BY id ASC LIMIT 1",
        )
        .expect("query User PK-only covering EXPLAIN EXECUTION should return an Ok payload");
        assert_explain_route(
            payload,
            "User",
            &[
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "primary_key",
                "existing_row_mode",
                "planner_proven",
                "id",
            ],
            &["row_check_required"],
            "User PK-only covering EXPLAIN EXECUTION should expose the planner-proven covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_character_covering_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one direct indexed Character projection on the
        // generated query surface so dynamic entity routing still reaches the
        // shared covering-read lane.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Character WHERE name = 'Alex Ander' ORDER BY id ASC LIMIT 1",
            "query Character covering projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the expected
        // Character row through the index-backed covering projection lane.
        assert_eq!(rows.entity, "Character");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][1], "Alex Ander".to_string());
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_character_covering_read_route() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the same indexed
        // Character covering projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name = 'Alex Ander' ORDER BY id ASC LIMIT 1",
        )
        .expect("query Character covering EXPLAIN EXECUTION should return an Ok payload");
        assert_explain_route(
            payload,
            "Character",
            &[
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "name",
            ],
            &["row_check_required"],
            "Character covering EXPLAIN EXECUTION should expose the explicit covering-read route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_character_order_only_composite_covering_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one order-only composite Character projection so
        // dynamic entity routing reaches the shared planner fallback instead of
        // materializing a full scan by accident.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, level, class_name FROM Character ORDER BY level ASC, class_name ASC, id ASC LIMIT 2",
            "query Character order-only composite covering projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns one projected
        // Character result window with the expected composite covering shape.
        assert_eq!(rows.entity, "Character");
        assert_eq!(
            rows.columns,
            vec![
                "id".to_string(),
                "level".to_string(),
                "class_name".to_string()
            ]
        );
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_character_order_only_composite_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the order-only
        // Character composite covering projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, level, class_name FROM Character ORDER BY level ASC, class_name ASC, id ASC LIMIT 2",
        )
        .expect(
            "query Character order-only composite covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Character",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "row_check_required",
                "id",
                "level",
                "class_name",
            ],
            &[],
            "Character order-only composite EXPLAIN EXECUTION should expose the index-range covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_character_order_only_composite_desc_covering_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one descending order-only composite Character
        // projection so the generated SQL lane proves reverse index traversal
        // instead of a materialized full-row reverse sort.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, level, class_name FROM Character ORDER BY level DESC, class_name DESC, id DESC LIMIT 2",
            "query Character descending order-only composite covering projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the expected
        // descending composite window, not merely the correct row count.
        assert_eq!(rows.entity, "Character");
        assert_eq!(
            rows.columns,
            vec![
                "id".to_string(),
                "level".to_string(),
                "class_name".to_string()
            ]
        );
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0][1..],
            ["20".to_string(), "Cleric".to_string()],
            "descending composite Character query should start from the highest level/class tuple",
        );
        assert_eq!(
            rows.rows[1][1..],
            ["20".to_string(), "Bard".to_string()],
            "descending composite Character query should keep the second highest level/class tuple",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_character_order_only_composite_desc_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending
        // order-only Character composite covering projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, level, class_name FROM Character ORDER BY level DESC, class_name DESC, id DESC LIMIT 2",
        )
        .expect(
            "query Character descending order-only composite covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Character",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "id",
                "level",
                "class_name",
            ],
            &[],
            "descending Character order-only composite EXPLAIN EXECUTION should expose the index-range covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_character_numeric_equality_covering_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one numeric-equality Character projection so the
        // generated SQL lane proves the strict uint equality shape reaches the
        // same composite equality-prefix covering route as typed SQL.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name ASC, id ASC LIMIT 2",
            "query Character numeric-equality covering projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the expected
        // narrowed level window instead of broad composite traversal.
        assert_projection_window(
            &rows,
            "Character",
            &["id", "level", "class_name"],
            &[
                &[ANY_PROJECTION_VALUE, "20", "Bard"],
                &[ANY_PROJECTION_VALUE, "20", "Cleric"],
            ],
            "Character numeric-equality covering projection should preserve the equality-prefix ordered window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_character_numeric_equality_covering_route() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the Character
        // numeric-equality composite covering projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name ASC, id ASC LIMIT 2",
        )
        .expect(
            "query Character numeric-equality covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Character",
            &[
                "CoveringRead",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "level",
                "class_name",
            ],
            &["row_check_required"],
            "Character numeric-equality EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_character_numeric_equality_desc_covering_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one descending numeric-equality Character
        // projection so the generated SQL lane proves reverse suffix order on
        // the narrowed level prefix.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name DESC, id DESC LIMIT 2",
            "query descending Character numeric-equality covering projection should return projected rows",
        );

        // Phase 2: assert the reverse equality-prefix window stays ordered on
        // the suffix field instead of materializing and resorting elsewhere.
        assert_projection_window(
            &rows,
            "Character",
            &["id", "level", "class_name"],
            &[
                &[ANY_PROJECTION_VALUE, "20", "Cleric"],
                &[ANY_PROJECTION_VALUE, "20", "Bard"],
            ],
            "descending Character numeric-equality covering projection should preserve the reverse equality-prefix ordered window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_character_numeric_equality_desc_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending
        // Character numeric-equality composite covering projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending Character numeric-equality covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Character",
            &[
                "CoveringRead",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "level",
                "class_name",
            ],
            &["row_check_required"],
            "descending Character numeric-equality EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_character_numeric_equality_class_name_strict_text_range_covering_projection()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one bounded suffix-range Character projection so
        // the generated SQL lane proves the existing composite bounded-range
        // witness family handles strict text bounds on the suffix field.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name ASC, id ASC LIMIT 2",
            "query Character numeric-equality bounded class_name covering projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the bounded
        // suffix window rather than the broader numeric-equality cohort.
        assert_projection_window(
            &rows,
            "Character",
            &["id", "level", "class_name"],
            &[
                &[ANY_PROJECTION_VALUE, "20", "Bard"],
                &[ANY_PROJECTION_VALUE, "20", "Cleric"],
            ],
            "Character numeric-equality bounded class_name covering projection should preserve the bounded suffix ordered window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_character_numeric_equality_class_name_strict_text_range_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the bounded suffix
        // Character numeric-equality composite covering projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name ASC, id ASC LIMIT 2",
        )
        .expect(
            "query Character numeric-equality bounded class_name covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Character",
            &[
                "CoveringRead",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "level",
                "class_name",
            ],
            &["row_check_required"],
            "Character numeric-equality bounded class_name EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_character_numeric_equality_class_name_strict_text_range_desc_covering_projection()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one descending bounded suffix-range Character
        // projection so the generated SQL lane proves reverse suffix order on
        // the same narrowed level prefix.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name DESC, id DESC LIMIT 2",
            "query descending Character numeric-equality bounded class_name covering projection should return projected rows",
        );

        // Phase 2: assert the reverse bounded suffix window stays ordered on
        // the suffix field instead of broadening or materializing elsewhere.
        assert_projection_window(
            &rows,
            "Character",
            &["id", "level", "class_name"],
            &[
                &[ANY_PROJECTION_VALUE, "20", "Cleric"],
                &[ANY_PROJECTION_VALUE, "20", "Bard"],
            ],
            "descending Character numeric-equality bounded class_name covering projection should preserve the reverse bounded suffix ordered window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_character_numeric_equality_class_name_strict_text_range_desc_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending bounded
        // suffix Character numeric-equality composite covering projection.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending Character numeric-equality bounded class_name covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Character",
            &[
                "CoveringRead",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "level",
                "class_name",
            ],
            &["row_check_required"],
            "descending Character numeric-equality bounded class_name EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_order_only_covering_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one filtered-index guarded order-only projection so
        // the generated SQL lane reaches the guarded secondary-index route.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
            "query ActiveUser filtered order-only covering projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the guarded
        // filtered-index window instead of falling back to materialized rows.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "name"],
            &[
                &[ANY_PROJECTION_VALUE, "bravo"],
                &[ANY_PROJECTION_VALUE, "charlie"],
            ],
            "ActiveUser filtered order-only covering projection should expose the guarded filtered-index window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_order_only_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded filtered
        // order-only ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect(
            "query ActiveUser filtered order-only covering EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // index-range and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "id",
                "name",
            ],
            &[],
            "ActiveUser filtered order-only EXPLAIN EXECUTION should expose the index-range covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_order_only_desc_covering_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one descending filtered-index guarded order-only
        // projection so reverse traversal stays locked in the generated lane.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
            "query descending ActiveUser filtered order-only covering projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the expected
        // descending filtered-index window.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "name"],
            &[
                &[ANY_PROJECTION_VALUE, "echo"],
                &[ANY_PROJECTION_VALUE, "charlie"],
            ],
            "descending ActiveUser filtered order-only covering projection should expose the reverse filtered-index window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_order_only_desc_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // filtered order-only ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending ActiveUser filtered order-only covering EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse index-range and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "id",
                "name",
            ],
            &[],
            "descending ActiveUser filtered order-only EXPLAIN EXECUTION should expose the index-range covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_strict_like_prefix_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one guarded filtered-index strict prefix projection
        // so the generated SQL lane reaches the bounded filtered route.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
            "query ActiveUser filtered strict LIKE prefix projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the guarded
        // bounded window on the ActiveUser filtered index.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "name"],
            &[&[ANY_PROJECTION_VALUE, "bravo"]],
            "ActiveUser filtered strict LIKE prefix projection should expose the bounded filtered window",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_equivalent_strict_prefix_forms_match_active_user_projection_rows()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute the three accepted guarded strict prefix spellings
        // against the same ordered ActiveUser projection window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
            "ActiveUser filtered strict LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM ActiveUser WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name ASC, id ASC LIMIT 1",
            "ActiveUser filtered direct STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM ActiveUser WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name ASC, id ASC LIMIT 1",
            "ActiveUser filtered strict text-range predicate should return projected rows",
        );

        // Phase 2: keep the canister query lane pinned to one shared filtered
        // result set across the equivalent strict prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "ActiveUser filtered direct STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "ActiveUser filtered strict text-range and LIKE prefix query rows should stay in parity",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_strict_like_prefix_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded filtered
        // strict-prefix ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
        )
        .expect(
            "query ActiveUser filtered strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // bounded index-range and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "covering_read",
                "cov_read_route",
                "OrderByAccessSatisfied",
                "proj_fields",
                "id",
                "name",
            ],
            &[],
            "ActiveUser filtered strict LIKE prefix EXPLAIN EXECUTION should expose the bounded covering index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_strict_like_prefix_desc_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one descending guarded filtered-index strict prefix
        // projection so the generated SQL lane reaches the reverse bounded route.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
            "query descending ActiveUser filtered strict LIKE prefix projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the guarded
        // reverse bounded window on the ActiveUser filtered index.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "name"],
            &[&[ANY_PROJECTION_VALUE, "bravo"]],
            "descending ActiveUser filtered strict LIKE prefix projection should expose the reverse bounded filtered window",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_equivalent_desc_strict_prefix_forms_match_active_user_projection_rows()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute the three accepted descending guarded strict prefix
        // spellings against the same reverse ActiveUser projection window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
            "descending ActiveUser filtered strict LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM ActiveUser WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name DESC, id DESC LIMIT 1",
            "descending ActiveUser filtered direct STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM ActiveUser WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name DESC, id DESC LIMIT 1",
            "descending ActiveUser filtered strict text-range predicate should return projected rows",
        );

        // Phase 2: keep the reverse canister query lane pinned to one shared
        // filtered result set across the equivalent strict prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "descending ActiveUser filtered direct STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "descending ActiveUser filtered strict text-range and LIKE prefix query rows should stay in parity",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_strict_like_prefix_desc_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // filtered strict-prefix ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
        )
        .expect(
            "query descending ActiveUser filtered strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse bounded index-range and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "covering_read",
                "cov_read_route",
                "OrderByAccessSatisfied",
                "proj_fields",
                "id",
                "name",
            ],
            &[],
            "descending ActiveUser filtered strict LIKE prefix EXPLAIN EXECUTION should expose the reverse bounded covering index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_rejects_grouped_sql_execution() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let err = query_result(
            pic,
            canister_id,
            "SELECT age, COUNT(*) FROM User GROUP BY age",
        )
        .expect_err("query grouped SQL execution should fail closed");

        assert!(
            matches!(
                err.kind(),
                icydb::error::ErrorKind::Runtime(icydb::error::RuntimeErrorKind::Unsupported)
            ),
            "grouped SQL execution should map to Runtime::Unsupported: {err:?}",
        );
        assert!(
            err.message()
                .contains("generated SQL query surface rejects grouped SELECT execution"),
            "grouped SQL execution should preserve explicit generated grouped-lane guidance: {err:?}",
        );
        assert!(
            err.message().contains("execute_sql_grouped(...)"),
            "grouped SQL execution should preserve explicit grouped entrypoint guidance: {err:?}",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_grouped_explain() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN SELECT age, COUNT(*) FROM User GROUP BY age",
        )
        .expect("query grouped EXPLAIN should return an Ok payload");
        assert_explain_route(
            payload,
            "User",
            &[],
            &[],
            "grouped EXPLAIN should return a User explain payload",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_delete_direct_starts_with_family_matches_like_output() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: compare the accepted direct family against the established
        // LIKE delete explain outputs on the generated query surface.
        let cases = [
            (
                "EXPLAIN DELETE FROM User WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 1",
                "EXPLAIN DELETE FROM User WHERE name LIKE 'a%' ORDER BY id LIMIT 1",
                "generated strict direct STARTS_WITH delete explain",
            ),
            (
                "EXPLAIN DELETE FROM User WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 1",
                "EXPLAIN DELETE FROM User WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 1",
                "generated direct LOWER(field) STARTS_WITH delete explain",
            ),
            (
                "EXPLAIN DELETE FROM User WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 1",
                "EXPLAIN DELETE FROM User WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 1",
                "generated direct UPPER(field) STARTS_WITH delete explain",
            ),
        ];

        // Phase 2: assert the generated canister query surface emits the same
        // logical explain payload for both spellings.
        for (direct_sql, like_sql, context) in cases {
            let direct = query_result(pic, canister_id, direct_sql)
                .expect("generated direct STARTS_WITH delete EXPLAIN should succeed");
            let like = query_result(pic, canister_id, like_sql)
                .expect("generated LIKE delete EXPLAIN should succeed");

            match (direct, like) {
                (
                    SqlQueryResult::Explain {
                        entity: direct_entity,
                        explain: direct_explain,
                    },
                    SqlQueryResult::Explain {
                        entity: like_entity,
                        explain: like_explain,
                    },
                ) => {
                    assert_eq!(
                        direct_entity, like_entity,
                        "generated direct STARTS_WITH delete EXPLAIN should keep the same entity payload: {context}",
                    );
                    assert_eq!(
                        direct_explain, like_explain,
                        "generated direct STARTS_WITH delete EXPLAIN should match the established LIKE explain output: {context}",
                    );
                }
                (direct_other, like_other) => panic!(
                    "generated delete EXPLAIN parity case should return Explain payloads, got direct={direct_other:?} like={like_other:?}"
                ),
            }
        }
    });
}

#[test]
fn sql_canister_query_lane_explain_delete_direct_upper_text_range_preserves_index_range_route() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN DELETE FROM User WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
        )
        .expect("generated direct UPPER(field) ordered text-range delete EXPLAIN should succeed");
        assert_explain_route(
            payload,
            "User",
            &[
                "mode=Delete",
                "access=IndexRange",
                "User|LOWER(name)",
                "lower: Included(Text(\"a\"))",
                "upper: Excluded(Text(\"b\"))",
            ],
            &["access=FullScan"],
            "generated direct UPPER(field) ordered text-range delete explain should preserve the shared expression index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_json_direct_upper_text_range_preserves_index_range_route() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN JSON SELECT id, name FROM User WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
        )
        .expect("generated direct UPPER(field) ordered text-range JSON EXPLAIN should succeed");
        assert_explain_route(
            payload,
            "User",
            &[
                "\"mode\":{\"type\":\"Load\"",
                "\"access\":{\"type\":\"IndexRange\"",
                "\"predicate\":\"And([Compare",
                "id: TextCasefold",
            ],
            &["\"type\":\"FullScan\""],
            "generated direct UPPER(field) ordered text-range JSON explain should preserve the shared expression index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_json_equivalent_direct_upper_prefix_forms_preserve_index_range_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let cases = [
            (
                "EXPLAIN JSON SELECT id, name FROM User WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 2",
                "direct UPPER(field) LIKE JSON explain route",
            ),
            (
                "EXPLAIN JSON SELECT id, name FROM User WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
                "direct UPPER(field) STARTS_WITH JSON explain route",
            ),
            (
                "EXPLAIN JSON SELECT id, name FROM User WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
                "direct UPPER(field) ordered text-range JSON explain route",
            ),
        ];

        for (sql, context) in cases {
            let payload = query_result(pic, canister_id, sql)
                .unwrap_or_else(|err| panic!("{context} should return an Ok payload: {err}"));
            assert_explain_route(
                payload,
                "User",
                &[
                    "\"mode\":{\"type\":\"Load\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                ],
                &["\"type\":\"FullScan\""],
                context,
            );
        }
    });
}

#[test]
fn sql_canister_query_lane_explain_json_delete_direct_upper_text_range_preserves_index_range_route()
{
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN JSON DELETE FROM User WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
        )
        .expect("generated direct UPPER(field) ordered text-range JSON delete EXPLAIN should succeed");
        assert_explain_route(
            payload,
            "User",
            &[
                "\"mode\":{\"type\":\"Delete\"",
                "\"access\":{\"type\":\"IndexRange\"",
                "\"predicate\":\"And([Compare",
                "id: TextCasefold",
            ],
            &["\"type\":\"FullScan\""],
            "generated direct UPPER(field) ordered text-range JSON delete explain should preserve the shared expression index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_json_delete_direct_upper_equivalent_prefix_forms_preserve_index_range_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let cases = [
            (
                "EXPLAIN JSON DELETE FROM User WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 1",
                "direct UPPER(field) LIKE JSON delete explain route",
            ),
            (
                "EXPLAIN JSON DELETE FROM User WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 1",
                "direct UPPER(field) STARTS_WITH JSON delete explain route",
            ),
            (
                "EXPLAIN JSON DELETE FROM User WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
                "direct UPPER(field) ordered text-range JSON delete explain route",
            ),
        ];

        for (sql, context) in cases {
            let payload = query_result(pic, canister_id, sql)
                .unwrap_or_else(|err| panic!("{context} should return an Ok payload: {err}"));
            assert_explain_route(
                payload,
                "User",
                &[
                    "\"mode\":{\"type\":\"Delete\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                ],
                &["\"type\":\"FullScan\""],
                context,
            );
        }
    });
}

#[test]
fn sql_canister_query_lane_explain_delete_rejects_non_casefold_wrapped_direct_starts_with() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let err = query_result(
            pic,
            canister_id,
            "EXPLAIN DELETE FROM User WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 1",
        )
        .expect_err("generated direct STARTS_WITH delete EXPLAIN wrapper should fail closed");

        assert!(
            matches!(
                err.kind(),
                icydb::error::ErrorKind::Runtime(icydb::error::RuntimeErrorKind::Unsupported)
            ),
            "generated direct STARTS_WITH delete EXPLAIN wrapper should map to Runtime::Unsupported: {err:?}",
        );
        assert!(
            err.message().contains(
                "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers"
            ),
            "generated direct STARTS_WITH delete EXPLAIN wrapper should preserve the stable unsupported-feature detail: {err:?}",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_json_delete_rejects_non_casefold_wrapped_direct_starts_with() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let err = query_result(
            pic,
            canister_id,
            "EXPLAIN JSON DELETE FROM User WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 1",
        )
        .expect_err("generated direct STARTS_WITH JSON delete EXPLAIN wrapper should fail closed");

        assert!(
            matches!(
                err.kind(),
                icydb::error::ErrorKind::Runtime(icydb::error::RuntimeErrorKind::Unsupported)
            ),
            "generated direct STARTS_WITH JSON delete EXPLAIN wrapper should map to Runtime::Unsupported: {err:?}",
        );
        assert!(
            err.message().contains(
                "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers"
            ),
            "generated direct STARTS_WITH JSON delete EXPLAIN wrapper should preserve the stable unsupported-feature detail: {err:?}",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_strict_like_prefix_predicate() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
            "query strict LIKE prefix predicate should return projected Character rows",
        );

        assert_eq!(rows.entity, "Character");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0][1], "Alex Ander".to_string());
        assert_eq!(rows.rows[1][1], "Astroth Slaemworth".to_string());
    });
}

#[test]
fn sql_canister_query_lane_supports_strict_like_prefix_desc_predicate() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
            "query descending strict LIKE prefix predicate should return projected Character rows",
        );

        assert_eq!(rows.entity, "Character");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0][1], "Azizi Johari".to_string());
        assert_eq!(rows.rows[1][1], "Astroth Slaemworth".to_string());
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_composite_strict_like_prefix_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one guarded composite filtered strict-prefix
        // projection so the generated SQL lane reaches the equality-prefix
        // plus bounded-suffix route.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
            "query ActiveUser filtered composite strict LIKE prefix projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the bounded suffix
        // window on the composite filtered ActiveUser index.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
            ],
            "ActiveUser filtered composite strict LIKE prefix projection should expose the bounded composite window",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_composite_equivalent_strict_prefix_forms_match_active_user_projection_rows()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute the three accepted guarded composite strict prefix
        // spellings against the same equality-prefix ActiveUser projection window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
            "ActiveUser filtered composite strict LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle ASC, id ASC LIMIT 2",
            "ActiveUser filtered composite direct STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle ASC, id ASC LIMIT 2",
            "ActiveUser filtered composite strict text-range predicate should return projected rows",
        );

        // Phase 2: keep the canister query lane pinned to one shared composite
        // filtered result set across the equivalent strict prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "ActiveUser filtered composite direct STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "ActiveUser filtered composite strict text-range and LIKE prefix query rows should stay in parity",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_strict_like_prefix_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded composite
        // filtered strict-prefix ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
        )
        .expect(
            "query ActiveUser filtered composite strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // composite index-range and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "covering_read",
                "cov_read_route",
                "existing_row_mode",
                "witness_validated",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "OrderByAccessSatisfied",
                "proj_fields",
                "tier",
                "handle",
            ],
            &["row_check_required"],
            "ActiveUser filtered composite strict LIKE prefix EXPLAIN EXECUTION should expose the bounded covering index-range route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_composite_strict_like_prefix_desc_projection()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one descending guarded composite filtered
        // strict-prefix projection so the generated SQL lane reaches the
        // reverse equality-prefix plus bounded-suffix route.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
            "query descending ActiveUser filtered composite strict LIKE prefix projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the reverse
        // bounded suffix window on the composite filtered ActiveUser index.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
            ],
            "descending ActiveUser filtered composite strict LIKE prefix projection should expose the reverse bounded composite window",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_composite_equivalent_desc_strict_prefix_forms_match_active_user_projection_rows()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute the three accepted descending guarded composite
        // strict prefix spellings against the same reverse equality-prefix window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
            "descending ActiveUser filtered composite strict LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle DESC, id DESC LIMIT 2",
            "descending ActiveUser filtered composite direct STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle DESC, id DESC LIMIT 2",
            "descending ActiveUser filtered composite strict text-range predicate should return projected rows",
        );

        // Phase 2: keep the reverse canister query lane pinned to one shared
        // composite filtered result set across the equivalent strict prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "descending ActiveUser filtered composite direct STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "descending ActiveUser filtered composite strict text-range and LIKE prefix query rows should stay in parity",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_strict_like_prefix_desc_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // composite filtered strict-prefix ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending ActiveUser filtered composite strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse composite index-range and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "covering_read",
                "cov_read_route",
                "existing_row_mode",
                "witness_validated",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "OrderByAccessSatisfied",
                "proj_fields",
                "tier",
                "handle",
            ],
            &["row_check_required"],
            "descending ActiveUser filtered composite strict LIKE prefix EXPLAIN EXECUTION should expose the reverse bounded covering index-range route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_composite_order_only_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one guarded composite filtered order-only
        // projection so the generated SQL lane reaches the equality-prefix
        // suffix-order route without an extra bounded text predicate.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
            "query ActiveUser filtered composite order-only projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the ordered
        // equality-prefix window on the composite filtered ActiveUser index.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
            ],
            "ActiveUser filtered composite order-only projection should expose the ordered equality-prefix window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_order_only_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded composite
        // filtered order-only ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
        )
        .expect(
            "query ActiveUser filtered composite order-only EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // composite index-prefix and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexPrefixScan",
                "covering_read",
                "cov_read_route",
                "existing_row_mode",
                "witness_validated",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "OrderByAccessSatisfied",
                "proj_fields",
                "tier",
                "handle",
            ],
            &["row_check_required"],
            "ActiveUser filtered composite order-only EXPLAIN EXECUTION should expose the covering index-prefix route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_composite_order_only_desc_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one descending guarded composite filtered
        // order-only projection so reverse suffix traversal stays pinned.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
            "query descending ActiveUser filtered composite order-only projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the reverse
        // ordered equality-prefix window on the composite filtered index.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
            ],
            "descending ActiveUser filtered composite order-only projection should expose the reverse equality-prefix window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_order_only_desc_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // composite filtered order-only ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending ActiveUser filtered composite order-only EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse composite index-prefix and covering-read labels while
        // failing closed to a materialized sort on the non-unique suffix.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexPrefixScan",
                "covering_read",
                "cov_read_route",
                "existing_row_mode",
                "witness_validated",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "OrderByMaterializedSort",
                "proj_fields",
                "tier",
                "handle",
            ],
            &["TopNSeek", "OrderByAccessSatisfied", "row_check_required"],
            "descending ActiveUser filtered composite order-only EXPLAIN EXECUTION should expose the reverse covering index-prefix route with one equality prefix and a fail-closed materialized sort without TopN",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_composite_order_only_desc_offset_projection()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one descending guarded composite filtered offset
        // projection so the materialized-boundary route stays pinned on the
        // existing equality-prefix index path.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
            "query descending ActiveUser filtered composite order-only offset projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the reverse
        // equality-prefix window while honoring the retained offset.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "tier", "handle"],
            &[&[ANY_PROJECTION_VALUE, "gold", "bravo"]],
            "descending ActiveUser filtered composite order-only offset projection should expose the retained one-row offset window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_order_only_desc_offset_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // composite filtered offset order-only ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
        )
        .expect(
            "query descending ActiveUser filtered composite order-only offset EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane keeps the index-prefix
        // route, stays on the materialized boundary, and suppresses Top-N.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexPrefixScan",
                "covering_read",
                "cov_read_route",
                "existing_row_mode",
                "witness_validated",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "OrderByMaterializedSort",
                "offset=Uint(1)",
                "proj_fields",
                "tier",
                "handle",
            ],
            &["TopNSeek", "OrderByAccessSatisfied", "row_check_required"],
            "descending ActiveUser filtered composite order-only offset EXPLAIN EXECUTION should expose the materialized-boundary index-prefix route without TopN",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_composite_desc_residual_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one descending guarded composite residual
        // projection so the generated SQL lane proves the `tier, handle` route
        // still owns ordering while `name >= 'a'` remains residual.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND name >= 'a' ORDER BY handle DESC, id DESC LIMIT 2",
            "query descending ActiveUser filtered composite residual projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the reverse
        // equality-prefix window while preserving the residual filter result.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
            ],
            "descending ActiveUser filtered composite residual projection should preserve the reverse equality-prefix window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_desc_residual_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // composite residual ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND name >= 'a' ORDER BY handle DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending ActiveUser filtered composite residual EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane fails closed to the
        // materialized residual route and suppresses Top-N.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "materialized",
                "ResidualPredicateFilter",
                "OrderByMaterializedSort",
                "proj_fields",
                "tier",
                "handle",
            ],
            &["TopNSeek", "OrderByAccessSatisfied"],
            "descending ActiveUser filtered composite residual EXPLAIN EXECUTION should expose the fail-closed materialized residual route without TopN",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_expression_order_only_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one filtered expression-order ActiveUser
        // projection so the generated SQL lane proves the guarded
        // `LOWER(handle)` secondary order path.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM ActiveUser WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "query ActiveUser filtered expression-order projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the expected
        // guarded `LOWER(handle)` window and column order.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "bravo"],
                &[ANY_PROJECTION_VALUE, "Brisk"],
            ],
            "ActiveUser filtered expression order-only projection should expose the guarded LOWER(handle) window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_expression_order_only_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded filtered
        // expression-order ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, handle FROM ActiveUser WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query ActiveUser filtered expression-order EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // expression index-range and materialized route labels from the shared descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "materialized",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "id",
                "handle",
            ],
            &[],
            "ActiveUser filtered expression-order EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_expression_order_only_desc_projection() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one descending filtered expression-order
        // ActiveUser projection so reverse traversal stays locked in the
        // generated SQL harness.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM ActiveUser WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "query descending ActiveUser filtered expression-order projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the expected
        // descending guarded `LOWER(handle)` window.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "bristle"],
                &[ANY_PROJECTION_VALUE, "Brisk"],
            ],
            "descending ActiveUser filtered expression order-only projection should expose the reverse LOWER(handle) window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_expression_order_only_desc_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending
        // filtered expression-order ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, handle FROM ActiveUser WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending ActiveUser filtered expression-order EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse expression index-range and materialized labels from the
        // shared execution descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "materialized",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "id",
                "handle",
            ],
            &[],
            "descending ActiveUser filtered expression-order EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_expression_equivalent_prefix_forms_match_active_user_projection_rows()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute the accepted guarded expression prefix spellings
        // against the same ordered ActiveUser projection window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "ActiveUser filtered expression LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM ActiveUser WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "ActiveUser filtered expression STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "ActiveUser filtered expression text-range predicate should return projected rows",
        );

        // Phase 2: keep the canister query lane pinned to one shared filtered
        // result set across the equivalent expression prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "ActiveUser filtered expression STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "ActiveUser filtered expression text-range and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(like_rows.entity, "ActiveUser");
        assert_eq!(
            like_rows.columns,
            vec!["id".to_string(), "handle".to_string()]
        );
        assert_eq!(like_rows.row_count, 2);
        assert_eq!(like_rows.rows.len(), 2);
        assert_eq!(like_rows.rows[0][1], "bravo".to_string());
        assert_eq!(like_rows.rows[1][1], "Brisk".to_string());
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_expression_strict_like_prefix_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded filtered
        // expression strict-prefix ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query ActiveUser filtered expression strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // expression index-range and materialized route labels from the shared descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "materialized",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "id",
                "handle",
            ],
            &[],
            "ActiveUser filtered expression strict LIKE prefix EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_expression_strict_text_range_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query ActiveUser filtered expression strict text-range EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "materialized",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "id",
                "handle",
            ],
            &[],
            "ActiveUser filtered expression strict text-range EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_expression_equivalent_desc_prefix_forms_match_active_user_projection_rows()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute the accepted descending guarded expression prefix
        // spellings against the same reverse ActiveUser projection window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "descending ActiveUser filtered expression LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM ActiveUser WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "descending ActiveUser filtered expression STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "descending ActiveUser filtered expression text-range predicate should return projected rows",
        );

        // Phase 2: keep the reverse canister query lane pinned to one shared
        // filtered result set across the equivalent expression prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "descending ActiveUser filtered expression STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "descending ActiveUser filtered expression text-range and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(like_rows.entity, "ActiveUser");
        assert_eq!(
            like_rows.columns,
            vec!["id".to_string(), "handle".to_string()]
        );
        assert_eq!(like_rows.row_count, 2);
        assert_eq!(like_rows.rows.len(), 2);
        assert_eq!(like_rows.rows[0][1], "bristle".to_string());
        assert_eq!(like_rows.rows[1][1], "Brisk".to_string());
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_expression_strict_like_prefix_desc_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending
        // filtered expression strict-prefix ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending ActiveUser filtered expression strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse expression index-range and materialized labels from the shared
        // execution descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "materialized",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "id",
                "handle",
            ],
            &[],
            "descending ActiveUser filtered expression strict LIKE prefix EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_expression_strict_text_range_desc_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending ActiveUser filtered expression strict text-range EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "materialized",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "id",
                "handle",
            ],
            &[],
            "descending ActiveUser filtered expression strict text-range EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_composite_expression_order_only_projection()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one guarded composite expression order-only
        // projection so the generated SQL lane proves the equality-prefix
        // `tier, LOWER(handle)` route.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "query ActiveUser filtered composite expression order-only projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the canonical
        // guarded `LOWER(handle)` suffix window on the gold tier.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
            ],
            "ActiveUser filtered composite expression order-only projection should expose the guarded LOWER(handle) suffix window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_expression_order_only_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded composite
        // expression order-only ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query ActiveUser filtered composite expression order-only EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // index-prefix and materialized route labels from the shared descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexPrefixScan",
                "cov_read_route",
                "materialized",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "tier",
                "handle",
            ],
            &[],
            "ActiveUser filtered composite expression order-only EXPLAIN EXECUTION should expose the materialized index-prefix route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_composite_expression_key_only_order_only_projection()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "query ActiveUser filtered composite expression key-only order-only projection should return projected rows",
        );

        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "tier"],
            &[
                &[ANY_PROJECTION_VALUE, "gold"],
                &[ANY_PROJECTION_VALUE, "gold"],
            ],
            "ActiveUser filtered composite expression key-only order-only projection should expose the guarded covering window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_expression_key_only_order_only_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query ActiveUser filtered composite expression key-only order-only EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexPrefixScan",
                "covering_read",
                "cov_read_route",
                "existing_row_mode",
                "witness_validated",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "id",
                "tier",
            ],
            &["row_check_required"],
            "ActiveUser filtered composite expression key-only order-only EXPLAIN EXECUTION should expose the witness-backed covering route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_expression_key_only_order_only_desc_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending ActiveUser filtered composite expression key-only order-only EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexPrefixScan",
                "covering_read",
                "cov_read_route",
                "existing_row_mode",
                "witness_validated",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "LOWER(handle)",
                "OrderByMaterializedSort",
                "proj_fields",
                "id",
                "tier",
            ],
            &["row_check_required"],
            "descending ActiveUser filtered composite expression key-only order-only EXPLAIN EXECUTION should expose the witness-backed covering route with one equality prefix and a fail-closed materialized sort",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_composite_expression_key_only_strict_text_range_projection()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "query ActiveUser filtered composite expression key-only strict text-range projection should return projected rows",
        );

        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "tier"],
            &[
                &[ANY_PROJECTION_VALUE, "gold"],
                &[ANY_PROJECTION_VALUE, "gold"],
            ],
            "ActiveUser filtered composite expression key-only strict text-range projection should expose the guarded covering window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_expression_key_only_strict_text_range_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query ActiveUser filtered composite expression key-only strict text-range EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "covering_read",
                "cov_read_route",
                "existing_row_mode",
                "witness_validated",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "id",
                "tier",
            ],
            &["row_check_required"],
            "ActiveUser filtered composite expression key-only strict text-range EXPLAIN EXECUTION should expose the witness-backed covering route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_composite_expression_key_only_equivalent_direct_prefix_forms_match_projection_rows()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "query ActiveUser filtered composite expression key-only LIKE prefix projection should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "query ActiveUser filtered composite expression key-only STARTS_WITH projection should return projected rows",
        );

        assert_eq!(
            starts_with_rows, like_rows,
            "ActiveUser filtered composite expression key-only STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_projection_window(
            &like_rows,
            "ActiveUser",
            &["id", "tier"],
            &[
                &[ANY_PROJECTION_VALUE, "gold"],
                &[ANY_PROJECTION_VALUE, "gold"],
            ],
            "ActiveUser filtered composite expression key-only direct prefix projection should expose the guarded covering window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_expression_key_only_direct_starts_with_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query ActiveUser filtered composite expression key-only direct STARTS_WITH EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "covering_read",
                "cov_read_route",
                "existing_row_mode",
                "witness_validated",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "id",
                "tier",
            ],
            &["row_check_required"],
            "ActiveUser filtered composite expression key-only direct STARTS_WITH EXPLAIN EXECUTION should expose the witness-backed covering route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_active_user_filtered_composite_expression_order_only_desc_projection()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute one descending guarded composite expression
        // order-only projection so reverse `LOWER(handle)` traversal stays pinned.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "query descending ActiveUser filtered composite expression order-only projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the reverse
        // guarded `LOWER(handle)` suffix window on the gold tier.
        assert_projection_window(
            &rows,
            "ActiveUser",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
            ],
            "descending ActiveUser filtered composite expression order-only projection should expose the reverse LOWER(handle) suffix window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_expression_order_only_desc_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // composite expression order-only ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending ActiveUser filtered composite expression order-only EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse index-prefix and materialized labels while failing closed
        // to a materialized sort on the non-unique suffix.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexPrefixScan",
                "cov_read_route",
                "materialized",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "LOWER(handle)",
                "OrderByMaterializedSort",
                "proj_fields",
                "tier",
                "handle",
            ],
            &[],
            "descending ActiveUser filtered composite expression order-only EXPLAIN EXECUTION should expose the reverse materialized index-prefix route with one equality prefix and a fail-closed materialized sort",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_composite_expression_equivalent_strict_prefix_forms_match_active_user_projection_rows()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute the accepted guarded composite expression prefix
        // spellings against the same equality-prefix ActiveUser window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "ActiveUser filtered composite expression LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "ActiveUser filtered composite expression STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "ActiveUser filtered composite expression text-range predicate should return projected rows",
        );

        // Phase 2: keep the canister query lane pinned to one shared composite
        // expression result set across the equivalent strict prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "ActiveUser filtered composite expression STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "ActiveUser filtered composite expression text-range and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(like_rows.entity, "ActiveUser");
        assert_eq!(
            like_rows.columns,
            vec!["id".to_string(), "tier".to_string(), "handle".to_string()]
        );
        assert_eq!(like_rows.row_count, 2);
        assert_eq!(like_rows.rows.len(), 2);
        assert_eq!(like_rows.rows[0][2], "bravo".to_string());
        assert_eq!(like_rows.rows[1][2], "bristle".to_string());
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_expression_strict_like_prefix_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded composite
        // expression strict-prefix ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query ActiveUser filtered composite expression strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // index-range and materialized route labels from the shared descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "materialized",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "tier",
                "handle",
            ],
            &[],
            "ActiveUser filtered composite expression strict LIKE prefix EXPLAIN EXECUTION should expose the materialized index-range route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_expression_strict_text_range_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query ActiveUser filtered composite expression strict text-range EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "materialized",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "tier",
                "handle",
            ],
            &[],
            "ActiveUser filtered composite expression strict text-range EXPLAIN EXECUTION should expose the materialized index-range route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_composite_expression_equivalent_desc_strict_prefix_forms_match_active_user_projection_rows()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute the accepted descending guarded composite
        // expression prefix spellings against the same reverse equality-prefix window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "descending ActiveUser filtered composite expression LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "descending ActiveUser filtered composite expression STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "descending ActiveUser filtered composite expression text-range predicate should return projected rows",
        );

        // Phase 2: keep the reverse canister query lane pinned to one shared
        // composite expression result set across the equivalent prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "descending ActiveUser filtered composite expression STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "descending ActiveUser filtered composite expression text-range and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(like_rows.entity, "ActiveUser");
        assert_eq!(
            like_rows.columns,
            vec!["id".to_string(), "tier".to_string(), "handle".to_string()]
        );
        assert_eq!(like_rows.row_count, 2);
        assert_eq!(like_rows.rows.len(), 2);
        assert_eq!(like_rows.rows[0][2], "bristle".to_string());
        assert_eq!(like_rows.rows[1][2], "bravo".to_string());
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_expression_strict_like_prefix_desc_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // composite expression strict-prefix ActiveUser projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending ActiveUser filtered composite expression strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse index-range and materialized labels from the shared descriptor.
        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "materialized",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "tier",
                "handle",
            ],
            &[],
            "descending ActiveUser filtered composite expression strict LIKE prefix EXPLAIN EXECUTION should expose the reverse materialized index-range route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_active_user_filtered_composite_expression_strict_text_range_desc_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending ActiveUser filtered composite expression strict text-range EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "ActiveUser",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "materialized",
                "prefix_len",
                "Uint(1)",
                "prefix_values",
                "gold",
                "LOWER(handle)",
                "OrderByAccessSatisfied",
                "proj_fields",
                "tier",
                "handle",
            ],
            &[],
            "descending ActiveUser filtered composite expression strict text-range EXPLAIN EXECUTION should expose the reverse materialized index-range route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_strict_text_range_predicate() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
            "query strict text-range predicate should return projected Character rows",
        );

        assert_eq!(rows.entity, "Character");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0][1], "Alex Ander".to_string());
        assert_eq!(rows.rows[1][1], "Astroth Slaemworth".to_string());
    });
}

#[test]
fn sql_canister_query_lane_supports_strict_text_range_desc_predicate() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
            "query descending strict text-range predicate should return projected Character rows",
        );

        assert_eq!(rows.entity, "Character");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0][1], "Azizi Johari".to_string());
        assert_eq!(rows.rows[1][1], "Astroth Slaemworth".to_string());
    });
}

#[test]
fn sql_canister_query_lane_equivalent_strict_prefix_forms_match_character_projection_rows() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute the three accepted strict text-prefix spellings
        // against the same ordered Character projection window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
            "Character strict LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Character WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2",
            "Character direct STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
            "Character strict text-range predicate should return projected rows",
        );

        // Phase 2: keep the canister query lane pinned to one shared result set
        // across the equivalent strict prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "Character direct STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "Character strict text-range and LIKE prefix query rows should stay in parity",
        );
    });
}

#[test]
fn sql_canister_query_lane_equivalent_desc_strict_prefix_forms_match_character_projection_rows() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute the three accepted descending strict text-prefix
        // spellings against the same reverse Character projection window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
            "descending Character strict LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Character WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2",
            "descending Character direct STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
            "descending Character strict text-range predicate should return projected rows",
        );

        // Phase 2: keep the reverse canister query lane pinned to one shared
        // result set across the equivalent strict prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "descending Character direct STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "descending Character strict text-range and LIKE prefix query rows should stay in parity",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_direct_starts_with_predicate() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 2",
            "query direct STARTS_WITH predicate should return projected rows",
        );

        assert_eq!(rows.entity, "User");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][1], "alice".to_string());
    });
}

#[test]
fn sql_canister_query_lane_supports_direct_lower_starts_with_predicate() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
            "query direct LOWER(field) STARTS_WITH predicate should return projected rows",
        );

        assert_eq!(rows.entity, "User");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][1], "alice".to_string());
    });
}

#[test]
fn sql_canister_query_lane_supports_direct_lower_strict_text_range_predicate() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
            "query direct LOWER(field) ordered text-range predicate should return projected rows",
        );

        assert_eq!(rows.entity, "User");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][1], "alice".to_string());
    });
}

#[test]
fn sql_canister_query_lane_equivalent_direct_lower_prefix_forms_match_projection_rows() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 2",
            "query direct LOWER(field) LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
            "query direct LOWER(field) STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
            "query direct LOWER(field) ordered text-range predicate should return projected rows",
        );

        assert_eq!(
            starts_with_rows, like_rows,
            "direct LOWER(field) STARTS_WITH and LIKE prefix canister query rows should stay identical",
        );
        assert_eq!(
            range_rows, like_rows,
            "direct LOWER(field) ordered text-range and LIKE prefix canister query rows should stay identical",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_direct_lower_strict_text_range_index_range_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM User WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
        )
        .expect(
            "query direct LOWER(field) ordered text-range EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "User",
            &[
                "IndexRangeScan",
                "OrderByMaterializedSort",
                "proj_fields",
                "id",
                "name",
            ],
            &["FullScan"],
            "direct LOWER(field) ordered text-range EXPLAIN EXECUTION should preserve the shared expression index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_equivalent_direct_lower_prefix_forms_preserve_index_range_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let cases = [
            (
                "EXPLAIN EXECUTION SELECT id, name FROM User WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 2",
                "direct LOWER(field) LIKE prefix EXPLAIN EXECUTION route",
            ),
            (
                "EXPLAIN EXECUTION SELECT id, name FROM User WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
                "direct LOWER(field) STARTS_WITH EXPLAIN EXECUTION route",
            ),
            (
                "EXPLAIN EXECUTION SELECT id, name FROM User WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
                "direct LOWER(field) ordered text-range EXPLAIN EXECUTION route",
            ),
        ];

        for (sql, context) in cases {
            let payload = query_result(pic, canister_id, sql)
                .unwrap_or_else(|err| panic!("{context} should return an Ok payload: {err}"));
            assert_explain_route(
                payload,
                "User",
                &[
                    "IndexRangeScan",
                    "OrderByMaterializedSort",
                    "proj_fields",
                    "id",
                    "name",
                ],
                &["FullScan"],
                context,
            );
        }
    });
}

#[test]
fn sql_canister_query_lane_explain_delete_direct_lower_text_range_preserves_index_range_route() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN DELETE FROM User WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
        )
        .expect("generated direct LOWER(field) ordered text-range delete EXPLAIN should succeed");
        assert_explain_route(
            payload,
            "User",
            &[
                "mode=Delete",
                "access=IndexRange",
                "User|LOWER(name)",
                "lower: Included(Text(\"a\"))",
                "upper: Excluded(Text(\"b\"))",
            ],
            &["access=FullScan"],
            "generated direct LOWER(field) ordered text-range delete explain should preserve the shared expression index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_json_direct_lower_text_range_preserves_index_range_route() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN JSON SELECT id, name FROM User WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
        )
        .expect("generated direct LOWER(field) ordered text-range JSON EXPLAIN should succeed");
        assert_explain_route(
            payload,
            "User",
            &[
                "\"mode\":{\"type\":\"Load\"",
                "\"access\":{\"type\":\"IndexRange\"",
                "\"predicate\":\"And([Compare",
                "id: TextCasefold",
            ],
            &["\"type\":\"FullScan\""],
            "generated direct LOWER(field) ordered text-range JSON explain should preserve the shared expression index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_json_equivalent_direct_lower_prefix_forms_preserve_index_range_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let cases = [
            (
                "EXPLAIN JSON SELECT id, name FROM User WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 2",
                "direct LOWER(field) LIKE JSON explain route",
            ),
            (
                "EXPLAIN JSON SELECT id, name FROM User WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
                "direct LOWER(field) STARTS_WITH JSON explain route",
            ),
            (
                "EXPLAIN JSON SELECT id, name FROM User WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
                "direct LOWER(field) ordered text-range JSON explain route",
            ),
        ];

        for (sql, context) in cases {
            let payload = query_result(pic, canister_id, sql)
                .unwrap_or_else(|err| panic!("{context} should return an Ok payload: {err}"));
            assert_explain_route(
                payload,
                "User",
                &[
                    "\"mode\":{\"type\":\"Load\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                ],
                &["\"type\":\"FullScan\""],
                context,
            );
        }
    });
}

#[test]
fn sql_canister_query_lane_explain_json_delete_direct_lower_text_range_preserves_index_range_route()
{
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN JSON DELETE FROM User WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
        )
        .expect("generated direct LOWER(field) ordered text-range JSON delete EXPLAIN should succeed");
        assert_explain_route(
            payload,
            "User",
            &[
                "\"mode\":{\"type\":\"Delete\"",
                "\"access\":{\"type\":\"IndexRange\"",
                "\"predicate\":\"And([Compare",
                "id: TextCasefold",
            ],
            &["\"type\":\"FullScan\""],
            "generated direct LOWER(field) ordered text-range JSON delete explain should preserve the shared expression index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_json_delete_direct_lower_equivalent_prefix_forms_preserve_index_range_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let cases = [
            (
                "EXPLAIN JSON DELETE FROM User WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 1",
                "direct LOWER(field) LIKE JSON delete explain route",
            ),
            (
                "EXPLAIN JSON DELETE FROM User WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 1",
                "direct LOWER(field) STARTS_WITH JSON delete explain route",
            ),
            (
                "EXPLAIN JSON DELETE FROM User WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
                "direct LOWER(field) ordered text-range JSON delete explain route",
            ),
        ];

        for (sql, context) in cases {
            let payload = query_result(pic, canister_id, sql)
                .unwrap_or_else(|err| panic!("{context} should return an Ok payload: {err}"));
            assert_explain_route(
                payload,
                "User",
                &[
                    "\"mode\":{\"type\":\"Delete\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                ],
                &["\"type\":\"FullScan\""],
                context,
            );
        }
    });
}

#[test]
fn sql_canister_query_lane_supports_direct_upper_starts_with_predicate() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
            "query direct UPPER(field) STARTS_WITH predicate should return projected rows",
        );

        assert_eq!(rows.entity, "User");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][1], "alice".to_string());
    });
}

#[test]
fn sql_canister_query_lane_supports_direct_upper_strict_text_range_predicate() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
            "query direct UPPER(field) ordered text-range predicate should return projected rows",
        );

        assert_eq!(rows.entity, "User");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][1], "alice".to_string());
    });
}

#[test]
fn sql_canister_query_lane_equivalent_direct_upper_prefix_forms_match_projection_rows() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 2",
            "query direct UPPER(field) LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
            "query direct UPPER(field) STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
            "query direct UPPER(field) ordered text-range predicate should return projected rows",
        );

        assert_eq!(
            starts_with_rows, like_rows,
            "direct UPPER(field) STARTS_WITH and LIKE prefix canister query rows should stay identical",
        );
        assert_eq!(
            range_rows, like_rows,
            "direct UPPER(field) ordered text-range and LIKE prefix canister query rows should stay identical",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_direct_upper_strict_text_range_index_range_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM User WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
        )
        .expect(
            "query direct UPPER(field) ordered text-range EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "User",
            &[
                "IndexRangeScan",
                "OrderByMaterializedSort",
                "proj_fields",
                "id",
                "name",
            ],
            &["FullScan"],
            "direct UPPER(field) ordered text-range EXPLAIN EXECUTION should preserve the shared expression index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_equivalent_direct_upper_prefix_forms_preserve_index_range_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let cases = [
            (
                "EXPLAIN EXECUTION SELECT id, name FROM User WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 2",
                "direct UPPER(field) LIKE prefix EXPLAIN EXECUTION route",
            ),
            (
                "EXPLAIN EXECUTION SELECT id, name FROM User WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
                "direct UPPER(field) STARTS_WITH EXPLAIN EXECUTION route",
            ),
            (
                "EXPLAIN EXECUTION SELECT id, name FROM User WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
                "direct UPPER(field) ordered text-range EXPLAIN EXECUTION route",
            ),
        ];

        for (sql, context) in cases {
            let payload = query_result(pic, canister_id, sql)
                .unwrap_or_else(|err| panic!("{context} should return an Ok payload: {err}"));
            assert_explain_route(
                payload,
                "User",
                &[
                    "IndexRangeScan",
                    "OrderByMaterializedSort",
                    "proj_fields",
                    "id",
                    "name",
                ],
                &["FullScan"],
                context,
            );
        }
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_strict_like_prefix_covering_route() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("query Character strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload");
        assert_explain_route(
            payload,
            "Character",
            &[
                "IndexRangeScan",
                "covering_read",
                "cov_read_route",
                "OrderByAccessSatisfied",
                "proj_fields",
                "id",
                "name",
            ],
            &[],
            "Character strict LIKE prefix EXPLAIN EXECUTION should expose the bounded covering index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_strict_text_range_covering_route() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("query Character strict text-range EXPLAIN EXECUTION should return an Ok payload");
        assert_explain_route(
            payload,
            "Character",
            &[
                "IndexRangeScan",
                "covering_read",
                "cov_read_route",
                "OrderByAccessSatisfied",
                "proj_fields",
                "id",
                "name",
            ],
            &[],
            "Character strict text-range EXPLAIN EXECUTION should expose the bounded covering index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_strict_text_range_desc_covering_route() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending Character strict text-range EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Character",
            &[
                "IndexRangeScan",
                "covering_read",
                "cov_read_route",
                "OrderByAccessSatisfied",
                "scan_dir=Text(\"desc\")",
                "proj_fields",
                "id",
                "name",
            ],
            &[],
            "descending Character strict text-range EXPLAIN EXECUTION should expose the bounded reverse covering index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_equivalent_strict_prefix_forms_preserve_character_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute the three accepted strict text-prefix spellings on
        // the same ordered Character explain window.
        let explains = [
            (
                "strict LIKE prefix",
                query_explain_text(
                    pic,
                    canister_id,
                    "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
                    "Character strict LIKE prefix EXPLAIN EXECUTION should return Explain payload",
                ),
            ),
            (
                "direct STARTS_WITH",
                query_explain_text(
                    pic,
                    canister_id,
                    "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2",
                    "Character direct STARTS_WITH EXPLAIN EXECUTION should return Explain payload",
                ),
            ),
            (
                "strict text range",
                query_explain_text(
                    pic,
                    canister_id,
                    "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
                    "Character strict text-range EXPLAIN EXECUTION should return Explain payload",
                ),
            ),
        ];

        // Phase 2: keep the canister explain lane pinned to one shared
        // covering route across the equivalent strict prefix spellings.
        for (context, explain) in explains {
            assert!(
                explain.contains("IndexRangeScan")
                    && explain.contains("covering_read")
                    && explain.contains("cov_read_route")
                    && explain.contains("OrderByAccessSatisfied"),
                "{context} EXPLAIN EXECUTION should expose the bounded covering index-range route: {explain}",
            );
            assert!(
                explain.contains("proj_fields")
                    && explain.contains("id")
                    && explain.contains("name"),
                "{context} EXPLAIN EXECUTION should expose the projected fields: {explain}",
            );
        }
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_equivalent_desc_strict_prefix_forms_preserve_character_covering_route()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: execute the three accepted descending strict text-prefix
        // spellings on the same reverse Character explain window.
        let explains = [
            (
                "descending strict LIKE prefix",
                query_explain_text(
                    pic,
                    canister_id,
                    "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
                    "descending Character strict LIKE prefix EXPLAIN EXECUTION should return Explain payload",
                ),
            ),
            (
                "descending direct STARTS_WITH",
                query_explain_text(
                    pic,
                    canister_id,
                    "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2",
                    "descending Character direct STARTS_WITH EXPLAIN EXECUTION should return Explain payload",
                ),
            ),
            (
                "descending strict text range",
                query_explain_text(
                    pic,
                    canister_id,
                    "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
                    "descending Character strict text-range EXPLAIN EXECUTION should return Explain payload",
                ),
            ),
        ];

        // Phase 2: keep the reverse canister explain lane pinned to one shared
        // covering route across the equivalent strict prefix spellings.
        for (context, explain) in explains {
            assert!(
                explain.contains("IndexRangeScan")
                    && explain.contains("covering_read")
                    && explain.contains("cov_read_route")
                    && explain.contains("OrderByAccessSatisfied")
                    && explain.contains("scan_dir=Text(\"desc\")"),
                "{context} EXPLAIN EXECUTION should expose the bounded reverse covering index-range route: {explain}",
            );
            assert!(
                explain.contains("proj_fields")
                    && explain.contains("id")
                    && explain.contains("name"),
                "{context} EXPLAIN EXECUTION should expose the projected fields: {explain}",
            );
        }
    });
}

#[test]
fn sql_canister_query_lane_rejects_non_casefold_wrapped_direct_starts_with_predicate() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let err = query_result(
            pic,
            canister_id,
            "SELECT id, name FROM User WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 2",
        )
        .expect_err("query non-casefold wrapped direct STARTS_WITH should fail closed");

        assert!(
            matches!(
                err.kind(),
                icydb::error::ErrorKind::Runtime(icydb::error::RuntimeErrorKind::Unsupported)
            ),
            "non-casefold wrapped direct STARTS_WITH should map to Runtime::Unsupported: {err:?}",
        );
        assert!(
            err.message().contains(
                "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers"
            ),
            "non-casefold wrapped direct STARTS_WITH should preserve the stable unsupported-feature detail: {err:?}",
        );
    });
}

#[test]
#[expect(clippy::too_many_lines)]
fn sql_canister_perf_harness_reports_positive_instruction_samples() {
    run_with_pic(|pic| {
        let scenarios = vec![
            SqlPerfScenario {
                scenario_key: "generated.dispatch.projection.user_name_eq_limit",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM User WHERE name = 'alice' ORDER BY id LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.projection.user_name_eq_limit.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM User WHERE name = 'alice' ORDER BY id DESC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.projection.user_name_eq_limit",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id, name FROM User WHERE name = 'alice' ORDER BY id LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.projection.user_name_eq_limit.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id, name FROM User WHERE name = 'alice' ORDER BY id DESC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.primary_key_covering.user_id_limit1",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id FROM User ORDER BY id ASC LIMIT 1".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.primary_key_covering.user_id_limit1",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id FROM User ORDER BY id ASC LIMIT 1".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.secondary_covering.user_name_order_only_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM User ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.secondary_covering.user_name_order_only_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM User ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.secondary_covering.user_name_strict_range_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM User WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.secondary_covering.user_name_strict_range_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM User WHERE name >= 'a' AND name < 'c' ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.secondary_covering.user_name_order_only_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id, name FROM User ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.secondary_covering.user_name_order_only_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id, name FROM User ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.secondary_covering.user_name_strict_range_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id, name FROM User WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.secondary_covering.user_name_strict_range_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id, name FROM User WHERE name >= 'a' AND name < 'c' ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.query_from_sql.execute.scalar_limit",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedQueryFromSqlUserExecute,
                    sql: "SELECT * FROM User ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql.scalar_limit",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlUser,
                    sql: "SELECT * FROM User ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.describe.user",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "DESCRIBE User".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.explain.user_name_eq_limit",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql:
                        "EXPLAIN SELECT id, name FROM User WHERE name = 'alice' ORDER BY id LIMIT 1"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.explain.grouped.user_age_count",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "EXPLAIN SELECT age, COUNT(*) FROM User GROUP BY age ORDER BY age ASC LIMIT 10"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.explain.aggregate.user_count",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "EXPLAIN SELECT COUNT(*) FROM User".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_age_count",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedUser,
                    sql: "SELECT age, COUNT(*) FROM User GROUP BY age ORDER BY age ASC LIMIT 10"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_age_count.having_empty",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedUser,
                    sql:
                        "SELECT age, COUNT(*) FROM User GROUP BY age HAVING COUNT(*) > 1000 ORDER BY age ASC LIMIT 10"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_age_count.limit2.first_page",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedUser,
                    sql: "SELECT age, COUNT(*) FROM User GROUP BY age ORDER BY age ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_age_count.limit2.second_page",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedUserSecondPage,
                    sql: "SELECT age, COUNT(*) FROM User GROUP BY age ORDER BY age ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_age_count.invalid_cursor",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedUser,
                    sql: "SELECT age, COUNT(*) FROM User GROUP BY age ORDER BY age ASC LIMIT 2"
                        .to_string(),
                    cursor_token: Some("zz".to_string()),
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_count",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateUser,
                    sql: "SELECT COUNT(*) FROM User".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_count_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateUser,
                    sql: "SELECT COUNT(age) FROM User".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_min_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateUser,
                    sql: "SELECT MIN(age) FROM User".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_max_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateUser,
                    sql: "SELECT MAX(age) FROM User".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_sum_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateUser,
                    sql: "SELECT SUM(age) FROM User".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_avg_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateUser,
                    sql: "SELECT AVG(age) FROM User".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert.user_single",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertUser,
                    sql: "INSERT User".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert_many_atomic.user_10",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertManyAtomicUser10,
                    sql: "INSERT MANY User ATOMIC x10".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert_many_atomic.user_100",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertManyAtomicUser100,
                    sql: "INSERT MANY User ATOMIC x100".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert_many_atomic.user_1000",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertManyAtomicUser1000,
                    sql: "INSERT MANY User ATOMIC x1000".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert_many_non_atomic.user_10",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertManyNonAtomicUser10,
                    sql: "INSERT MANY User NON_ATOMIC x10".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert_many_non_atomic.user_100",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertManyNonAtomicUser100,
                    sql: "INSERT MANY User NON_ATOMIC x100".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert_many_non_atomic.user_1000",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertManyNonAtomicUser1000,
                    sql: "INSERT MANY User NON_ATOMIC x1000".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.update.user_single",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedUpdateUser,
                    sql: "UPDATE User".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.delete.user_order_id_limit1.count",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentDeleteUserOrderIdLimit1Count,
                    sql: "DELETE FROM User ORDER BY id LIMIT 1".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.show_indexes.user",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SHOW INDEXES User".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.show_columns.user",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SHOW COLUMNS User".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.show_entities",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SHOW ENTITIES".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.computed_projection.lower_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT LOWER(name) FROM User ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.predicate.starts_with_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM User WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.predicate.lower_starts_with_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM User WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.predicate.upper_starts_with_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM User WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.predicate.upper_strict_range_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM User WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.predicate.lower_strict_range_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM User WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.computed_projection.lower_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT LOWER(name) FROM User ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.predicate.starts_with_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id, name FROM User WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.predicate.lower_starts_with_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id, name FROM User WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.predicate.upper_starts_with_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id, name FROM User WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.predicate.upper_strict_range_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id, name FROM User WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.predicate.lower_strict_range_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id, name FROM User WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.active_user_filtered_order_only_name_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.active_user_filtered_order_only_name_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.active_user_filtered_strict_like_name_limit1.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_direct_starts_with_name_limit1.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM ActiveUser WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name ASC, id ASC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.active_user_filtered_strict_range_name_limit1.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM ActiveUser WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name ASC, id ASC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.active_user_filtered_strict_like_name_limit1.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_direct_starts_with_name_limit1.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM ActiveUser WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name DESC, id DESC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.active_user_filtered_strict_range_name_limit1.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM ActiveUser WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name DESC, id DESC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_order_only_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_strict_like_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_direct_starts_with_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_strict_range_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_order_only_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_strict_like_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_direct_starts_with_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_strict_range_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_expression_order_only_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_expression_key_only_order_only_tier_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_expression_key_only_order_only_tier_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_expression_key_only_strict_range_tier_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_expression_key_only_direct_starts_with_tier_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_expression_strict_like_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_expression_direct_starts_with_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_expression_strict_range_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_expression_order_only_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_expression_strict_like_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_expression_direct_starts_with_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.active_user_filtered_composite_expression_strict_range_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.active_user_filtered_order_only_name_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.active_user_filtered_order_only_name_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.active_user_filtered_composite_order_only_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.active_user_filtered_composite_order_only_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.active_user_filtered_composite_expression_order_only_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.active_user_filtered_composite_expression_key_only_order_only_tier_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.active_user_filtered_composite_expression_key_only_order_only_tier_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.active_user_filtered_composite_expression_key_only_strict_range_tier_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.active_user_filtered_composite_expression_key_only_direct_starts_with_tier_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, tier FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.active_user_filtered_composite_expression_direct_starts_with_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.active_user_filtered_composite_expression_strict_range_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.active_user_filtered_composite_expression_order_only_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.active_user_filtered_composite_expression_direct_starts_with_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.active_user_filtered_composite_expression_strict_range_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchActiveUser,
                    sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.character_strict_like_name_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.character_strict_like_name_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.character_direct_starts_with_name_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Character WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.character_direct_starts_with_name_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Character WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.character_strict_range_name_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.character_strict_range_name_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.user_expression_order.lower_name_id_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM User ORDER BY LOWER(name) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.user_expression_order.lower_name_id_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM User ORDER BY LOWER(name) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.user_expression_order.lower_name_id_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id, name FROM User ORDER BY LOWER(name) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.user_expression_order.lower_name_id_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "SELECT id, name FROM User ORDER BY LOWER(name) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.character_order_only_composite.level_class_id_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, level, class_name FROM Character ORDER BY level ASC, class_name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.character_order_only_composite.level_class_id_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, level, class_name FROM Character ORDER BY level DESC, class_name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.character_numeric_equality.level_eq20_class_id_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.character_numeric_equality.level_eq20_class_id_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.character_numeric_equality_bounded_class_name.level_eq20_class_bd_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.character_numeric_equality_bounded_class_name.level_eq20_class_bd_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.character_order_only_composite.level_class_id_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCharacter,
                    sql: "SELECT id, level, class_name FROM Character ORDER BY level ASC, class_name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.character_order_only_composite.level_class_id_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCharacter,
                    sql: "SELECT id, level, class_name FROM Character ORDER BY level DESC, class_name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.character_numeric_equality.level_eq20_class_id_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCharacter,
                    sql: "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.character_numeric_equality.level_eq20_class_id_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCharacter,
                    sql: "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.character_numeric_equality_bounded_class_name.level_eq20_class_bd_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCharacter,
                    sql: "SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.character_numeric_equality_bounded_class_name.level_eq20_class_bd_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCharacter,
                    sql: "SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.load.user_order_id_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentLoadUserOrderIdLimit2,
                    sql: "SELECT * FROM User ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.load.user_name_eq_limit1",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentLoadUserNameEqLimit1,
                    sql: "SELECT * FROM User WHERE name = 'alice' ORDER BY id LIMIT 1".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.paged.user_order_id_limit2.first_page",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentPagedUserOrderIdLimit2FirstPage,
                    sql: "SELECT * FROM User ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.paged.user_order_id_limit2.second_page",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentPagedUserOrderIdLimit2SecondPage,
                    sql: "SELECT * FROM User ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.paged.user_order_id_limit2.invalid_cursor",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentPagedUserOrderIdLimit2InvalidCursor,
                    sql: "SELECT * FROM User ORDER BY id LIMIT 2".to_string(),
                    cursor_token: Some("zz".to_string()),
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.explain_delete",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "EXPLAIN DELETE FROM User ORDER BY id LIMIT 1".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.delete",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "DELETE FROM User ORDER BY id LIMIT 1".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.delete",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchUser,
                    sql: "DELETE FROM User ORDER BY id LIMIT 1".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
        ];
        let rows = run_sql_perf_scenarios(pic, scenarios);

        println!(
            "{}",
            serde_json::to_string_pretty(&rows)
                .expect("sql perf scenario rows should serialize to JSON")
        );
    });
}

#[test]
fn sql_canister_perf_non_user_ordered_covering_generated_and_typed_dispatch_stay_aligned() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let mut rows = Vec::new();

        // Phase 1: measure the representative non-User ordered covering cohort
        // through both generated dispatch and the matching typed dispatch lane.
        for (scenario_key, sql, typed_surface, expected_entity, expected_row_count) in
            NON_USER_ORDERED_COVERING_PERF_CASES.iter().copied()
        {
            let generated = sql_perf_sample(
                pic,
                canister_id,
                &SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: sql.to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            );
            let typed = sql_perf_sample(
                pic,
                canister_id,
                &SqlPerfRequest {
                    surface: typed_surface,
                    sql: sql.to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            );

            // Phase 2: assert the generated and typed lanes stay aligned on
            // entity binding and result shape for the same optimized route.
            assert_matching_perf_outcomes(
                scenario_key,
                &generated,
                &typed,
                expected_entity,
                expected_row_count,
            );

            rows.push(serde_json::json!({
                "scenario_key": scenario_key,
                "generated": generated,
                "typed": typed,
            }));
        }

        println!(
            "{}",
            serde_json::to_string_pretty(&rows)
                .expect("non-User ordered covering perf rows should serialize to JSON")
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_user_expression_order_reports_positive_instruction_samples()
{
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the new
        // expression-order User covering shape so perf regression checks track
        // the exact canister lane this slice changed.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM User ORDER BY LOWER(name) ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected User projection window.
        assert!(
            sample.first_local_instructions > 0,
            "User expression-order first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "User expression-order min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "User expression-order max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "User expression-order total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "User expression-order repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "User expression-order generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("User"),
            "User expression-order perf sample should stay on the User route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "User expression-order perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_user_expression_order_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // expression-order User covering shape so reverse traversal stays
        // pinned in the checked-in perf suite.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM User ORDER BY LOWER(name) DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected User projection window.
        assert!(
            sample.first_local_instructions > 0,
            "descending User expression-order first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending User expression-order min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending User expression-order max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending User expression-order total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending User expression-order repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending User expression-order generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("User"),
            "descending User expression-order perf sample should stay on the User route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending User expression-order perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_order_only_composite_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the new order-only
        // composite Character covering shape so perf regression checks track
        // the exact canister lane this slice changed.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, level, class_name FROM Character ORDER BY level ASC, class_name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected Character projection window.
        assert!(
            sample.first_local_instructions > 0,
            "Character order-only composite first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "Character order-only composite min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "Character order-only composite max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "Character order-only composite total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "Character order-only composite repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "Character order-only composite generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "Character order-only composite perf sample should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "Character order-only composite perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_order_only_composite_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // order-only composite Character covering shape so reverse traversal
        // stays pinned in the checked-in perf suite.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, level, class_name FROM Character ORDER BY level DESC, class_name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected Character projection
        // window.
        assert!(
            sample.first_local_instructions > 0,
            "descending Character order-only composite first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending Character order-only composite min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending Character order-only composite max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending Character order-only composite total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending Character order-only composite repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending Character order-only composite generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "descending Character order-only composite perf sample should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending Character order-only composite perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_numeric_equality_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the narrowed
        // Character numeric-equality covering shape so perf checks pin the
        // concrete equality-prefix witness cohort we just unblocked.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected narrowed Character projection window.
        assert!(
            sample.first_local_instructions > 0,
            "Character numeric-equality first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "Character numeric-equality min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "Character numeric-equality max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "Character numeric-equality total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "Character numeric-equality repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "Character numeric-equality generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "Character numeric-equality perf sample should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "Character numeric-equality perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_numeric_equality_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // Character numeric-equality covering shape so reverse suffix order
        // stays pinned in the checked-in perf suite.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected narrowed projection
        // window.
        assert!(
            sample.first_local_instructions > 0,
            "descending Character numeric-equality first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending Character numeric-equality min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending Character numeric-equality max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending Character numeric-equality total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending Character numeric-equality repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending Character numeric-equality generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "descending Character numeric-equality perf sample should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending Character numeric-equality perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_numeric_equality_bounded_class_name_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the bounded suffix
        // Character numeric-equality covering shape so perf checks pin the
        // concrete composite bounded-range witness cohort.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected bounded suffix Character projection.
        assert!(
            sample.first_local_instructions > 0,
            "Character numeric-equality bounded class_name first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "Character numeric-equality bounded class_name min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "Character numeric-equality bounded class_name max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "Character numeric-equality bounded class_name total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "Character numeric-equality bounded class_name repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "Character numeric-equality bounded class_name generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "Character numeric-equality bounded class_name perf sample should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "Character numeric-equality bounded class_name perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_numeric_equality_bounded_class_name_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // bounded suffix Character numeric-equality covering shape so reverse
        // traversal stays pinned in the checked-in perf suite.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected bounded suffix window.
        assert!(
            sample.first_local_instructions > 0,
            "descending Character numeric-equality bounded class_name first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending Character numeric-equality bounded class_name min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending Character numeric-equality bounded class_name max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending Character numeric-equality bounded class_name total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending Character numeric-equality bounded class_name repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending Character numeric-equality bounded class_name generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "descending Character numeric-equality bounded class_name perf sample should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending Character numeric-equality bounded class_name perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_order_only_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the guarded
        // filtered-index order-only ActiveUser covering shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected ActiveUser projection window.
        assert!(
            sample.first_local_instructions > 0,
            "ActiveUser filtered order-only first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "ActiveUser filtered order-only min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "ActiveUser filtered order-only max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "ActiveUser filtered order-only total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "ActiveUser filtered order-only repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "ActiveUser filtered order-only generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "ActiveUser filtered order-only perf sample should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "ActiveUser filtered order-only perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_order_only_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // guarded filtered-index order-only ActiveUser covering shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected ActiveUser window.
        assert!(
            sample.first_local_instructions > 0,
            "descending ActiveUser filtered order-only first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending ActiveUser filtered order-only min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending ActiveUser filtered order-only max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending ActiveUser filtered order-only total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending ActiveUser filtered order-only repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending ActiveUser filtered order-only generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "descending ActiveUser filtered order-only perf sample should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending ActiveUser filtered order-only perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_strict_like_prefix_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the guarded
        // filtered-index strict LIKE prefix ActiveUser covering shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected bounded ActiveUser projection window.
        assert!(
            sample.first_local_instructions > 0,
            "ActiveUser filtered strict LIKE prefix first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "ActiveUser filtered strict LIKE prefix min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "ActiveUser filtered strict LIKE prefix max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "ActiveUser filtered strict LIKE prefix total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "ActiveUser filtered strict LIKE prefix repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "ActiveUser filtered strict LIKE prefix generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "ActiveUser filtered strict LIKE prefix perf sample should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(1),
            "ActiveUser filtered strict LIKE prefix perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_strict_like_prefix_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // guarded filtered-index strict LIKE prefix ActiveUser covering shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected bounded ActiveUser window.
        assert!(
            sample.first_local_instructions > 0,
            "descending ActiveUser filtered strict LIKE prefix first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending ActiveUser filtered strict LIKE prefix min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending ActiveUser filtered strict LIKE prefix max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending ActiveUser filtered strict LIKE prefix total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending ActiveUser filtered strict LIKE prefix repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending ActiveUser filtered strict LIKE prefix generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "descending ActiveUser filtered strict LIKE prefix perf sample should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(1),
            "descending ActiveUser filtered strict LIKE prefix perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_composite_order_only_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the guarded
        // composite filtered order-only ActiveUser covering shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected ordered composite ActiveUser window.
        assert!(
            sample.first_local_instructions > 0,
            "ActiveUser filtered composite order-only first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "ActiveUser filtered composite order-only min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "ActiveUser filtered composite order-only max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "ActiveUser filtered composite order-only total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "ActiveUser filtered composite order-only repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "ActiveUser filtered composite order-only generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "ActiveUser filtered composite order-only perf sample should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "ActiveUser filtered composite order-only perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_composite_order_only_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // guarded composite filtered order-only ActiveUser shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected composite ActiveUser window.
        assert!(
            sample.first_local_instructions > 0,
            "descending ActiveUser filtered composite order-only first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending ActiveUser filtered composite order-only min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending ActiveUser filtered composite order-only max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending ActiveUser filtered composite order-only total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending ActiveUser filtered composite order-only repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending ActiveUser filtered composite order-only generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "descending ActiveUser filtered composite order-only perf sample should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending ActiveUser filtered composite order-only perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_composite_strict_like_prefix_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the guarded
        // composite filtered strict LIKE prefix ActiveUser covering shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected bounded composite ActiveUser window.
        assert!(
            sample.first_local_instructions > 0,
            "ActiveUser filtered composite strict LIKE prefix first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "ActiveUser filtered composite strict LIKE prefix min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "ActiveUser filtered composite strict LIKE prefix max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "ActiveUser filtered composite strict LIKE prefix total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "ActiveUser filtered composite strict LIKE prefix repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "ActiveUser filtered composite strict LIKE prefix generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "ActiveUser filtered composite strict LIKE prefix perf sample should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "ActiveUser filtered composite strict LIKE prefix perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_composite_strict_like_prefix_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // guarded composite filtered strict LIKE prefix ActiveUser shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected composite ActiveUser window.
        assert!(
            sample.first_local_instructions > 0,
            "descending ActiveUser filtered composite strict LIKE prefix first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending ActiveUser filtered composite strict LIKE prefix min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending ActiveUser filtered composite strict LIKE prefix max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending ActiveUser filtered composite strict LIKE prefix total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending ActiveUser filtered composite strict LIKE prefix repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending ActiveUser filtered composite strict LIKE prefix generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "descending ActiveUser filtered composite strict LIKE prefix perf sample should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending ActiveUser filtered composite strict LIKE prefix perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_strict_text_range_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the explicit
        // Character strict text-range covering shape so perf regression checks
        // stay on the checked-in bounded-range SQL form.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected Character projection window.
        assert!(
            sample.first_local_instructions > 0,
            "Character strict text-range first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "Character strict text-range min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "Character strict text-range max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "Character strict text-range total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "Character strict text-range repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "Character strict text-range generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "Character strict text-range perf sample should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "Character strict text-range perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_strict_text_range_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // explicit Character strict text-range covering shape so reverse
        // bounded traversal stays pinned in the checked-in perf suite.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected Character projection
        // window.
        assert!(
            sample.first_local_instructions > 0,
            "descending Character strict text-range first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending Character strict text-range min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending Character strict text-range max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending Character strict text-range total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending Character strict text-range repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending Character strict text-range generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "descending Character strict text-range perf sample should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending Character strict text-range perf sample should return the requested window size",
        );
    });
}

#[test]
#[ignore = "manual perf probe for before/after measurement runs"]
fn sql_canister_perf_probe_reports_sample_as_json() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: resolve one repo-owned sample probe request from env so
        // before/after perf runs can reuse this checked-in harness instead of
        // ad hoc temp crates.
        let request = SqlPerfRequest {
            surface: sql_perf_probe_sample_surface(),
            sql: sql_perf_probe_sql(),
            cursor_token: sql_perf_probe_cursor_token(),
            repeat_count: sql_perf_probe_repeat_count(),
        };
        let sample = sql_perf_sample(pic, canister_id, &request);

        // Phase 2: fail loudly if the probe stopped producing a usable
        // successful sample, then print the JSON payload for external diffing.
        assert!(
            sample.first_local_instructions > 0,
            "manual perf probe first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "manual perf probe should stay on a successful SQL surface: {sample:?}",
        );

        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "mode": "sample",
                "request": request,
                "sample": sample,
            }))
            .expect("manual perf probe sample should serialize to JSON")
        );
    });
}

#[test]
fn sql_canister_perf_operation_repeat_benchmarks_are_segregated() {
    run_with_pic(|pic| {
        let rows = run_sql_perf_scenarios(pic, sql_operation_repeat_scenarios());

        println!(
            "{}",
            serde_json::to_string_pretty(&rows)
                .expect("operation repeat scenario rows should serialize to JSON")
        );
    });
}

#[test]
#[ignore = "manual perf probe for before/after measurement runs"]
fn sql_canister_perf_probe_reports_attribution_as_json() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: resolve one repo-owned attribution probe request from env
        // so stage-by-stage before/after comparisons stay on the checked-in
        // checked-in canic-testkit-backed harness.
        let request = SqlPerfAttributionRequest {
            surface: sql_perf_probe_attribution_surface(),
            sql: sql_perf_probe_sql(),
            cursor_token: sql_perf_probe_cursor_token(),
        };
        let sample = sql_perf_attribution_sample(pic, canister_id, &request);

        // Phase 2: keep the manual attribution probe useful as a perf-report
        // building block by requiring the emitted sample to stay successful.
        assert!(
            sample.parse_local_instructions > 0,
            "manual attribution probe parse phase must stay positive: {sample:?}",
        );
        assert!(
            sample.execute_local_instructions > 0,
            "manual attribution probe execute phase must stay positive: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "manual attribution probe should stay on a successful SQL surface: {sample:?}",
        );

        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "mode": "attribution",
                "request": request,
                "sample": sample,
            }))
            .expect("manual attribution probe sample should serialize to JSON")
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_user_expression_order_attribution_reports_positive_stages()
{
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the exact User
        // expression-order covering shape added in this slice.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM User ORDER BY LOWER(name) ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the new expression-backed index route.
        assert_positive_scalar_attribution_sample("generated.user_expression_order", &sample, true);
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("User"),
            "User expression-order attribution should stay on the User route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "User expression-order attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_user_expression_order_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // User expression-order covering shape added to the harness.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM User ORDER BY LOWER(name) DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the descending generated dispatch attribution keeps
        // positive stage accounting on the reverse expression-backed route.
        assert_positive_scalar_attribution_sample(
            "generated.user_expression_order_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("User"),
            "descending User expression-order attribution should stay on the User route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending User expression-order attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_order_only_composite_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the exact
        // Character order-only composite covering shape added in this slice.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, level, class_name FROM Character ORDER BY level ASC, class_name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the new dynamic-entity index-backed route.
        assert_positive_scalar_attribution_sample(
            "generated.character_order_only_composite",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "Character order-only composite attribution should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "Character order-only composite attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_order_only_composite_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // Character order-only composite covering shape added to the harness.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, level, class_name FROM Character ORDER BY level DESC, class_name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the descending generated dispatch attribution keeps
        // positive stage accounting on the reverse index-backed route.
        assert_positive_scalar_attribution_sample(
            "generated.character_order_only_composite_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "descending Character order-only composite attribution should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending Character order-only composite attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_numeric_equality_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the narrowed
        // Character numeric-equality covering shape added to the harness.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the equality-prefix covering route.
        assert_positive_scalar_attribution_sample(
            "generated.character_numeric_equality",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "Character numeric-equality attribution should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "Character numeric-equality attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_numeric_equality_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // Character numeric-equality covering shape added to the harness.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, level, class_name FROM Character WHERE level = 20 ORDER BY class_name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the descending generated dispatch attribution keeps
        // positive stage accounting on the reverse equality-prefix route.
        assert_positive_scalar_attribution_sample(
            "generated.character_numeric_equality_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "descending Character numeric-equality attribution should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending Character numeric-equality attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_numeric_equality_bounded_class_name_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the bounded
        // suffix Character numeric-equality covering shape added to the
        // harness.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the composite bounded-range covering route.
        assert_positive_scalar_attribution_sample(
            "generated.character_numeric_equality_bounded_class_name",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "Character numeric-equality bounded class_name attribution should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "Character numeric-equality bounded class_name attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_numeric_equality_bounded_class_name_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // bounded suffix Character numeric-equality covering shape added to
        // the harness.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, level, class_name FROM Character WHERE level = 20 AND class_name >= 'B' AND class_name < 'D' ORDER BY class_name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the descending generated dispatch attribution keeps
        // positive stage accounting on the reverse bounded-range route.
        assert_positive_scalar_attribution_sample(
            "generated.character_numeric_equality_bounded_class_name_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "descending Character numeric-equality bounded class_name attribution should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending Character numeric-equality bounded class_name attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_order_only_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the guarded
        // filtered-index order-only ActiveUser covering shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the guarded filtered-index route.
        assert_positive_scalar_attribution_sample(
            "generated.active_user_filtered_order_only",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "ActiveUser filtered order-only attribution should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "ActiveUser filtered order-only attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_order_only_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // guarded filtered-index order-only ActiveUser covering shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the descending generated dispatch attribution keeps
        // positive stage accounting on the reverse guarded filtered-index route.
        assert_positive_scalar_attribution_sample(
            "generated.active_user_filtered_order_only_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "descending ActiveUser filtered order-only attribution should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending ActiveUser filtered order-only attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_composite_order_only_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the guarded
        // composite filtered order-only ActiveUser covering shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the ordered composite filtered route.
        assert_positive_scalar_attribution_sample(
            "generated.active_user_filtered_composite_order_only",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "ActiveUser filtered composite order-only attribution should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "ActiveUser filtered composite order-only attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_composite_order_only_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // guarded composite filtered order-only ActiveUser shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the descending generated dispatch attribution keeps
        // positive stage accounting on the reverse composite route.
        assert_positive_scalar_attribution_sample(
            "generated.active_user_filtered_composite_order_only_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "descending ActiveUser filtered composite order-only attribution should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending ActiveUser filtered composite order-only attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_strict_like_prefix_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the guarded
        // filtered-index strict LIKE prefix ActiveUser covering shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the bounded filtered route.
        assert_positive_scalar_attribution_sample(
            "generated.active_user_filtered_strict_like_prefix",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "ActiveUser filtered strict LIKE prefix attribution should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(1),
            "ActiveUser filtered strict LIKE prefix attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_strict_like_prefix_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // guarded filtered-index strict LIKE prefix ActiveUser covering shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the descending generated dispatch attribution keeps
        // positive stage accounting on the reverse bounded filtered route.
        assert_positive_scalar_attribution_sample(
            "generated.active_user_filtered_strict_like_prefix_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "descending ActiveUser filtered strict LIKE prefix attribution should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(1),
            "descending ActiveUser filtered strict LIKE prefix attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_composite_strict_like_prefix_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the guarded
        // composite filtered strict LIKE prefix ActiveUser covering shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the bounded composite filtered route.
        assert_positive_scalar_attribution_sample(
            "generated.active_user_filtered_composite_strict_like_prefix",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "ActiveUser filtered composite strict LIKE prefix attribution should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "ActiveUser filtered composite strict LIKE prefix attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_active_user_filtered_composite_strict_like_prefix_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // guarded composite filtered strict LIKE prefix ActiveUser shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the descending generated dispatch attribution keeps
        // positive stage accounting on the reverse bounded composite route.
        assert_positive_scalar_attribution_sample(
            "generated.active_user_filtered_composite_strict_like_prefix_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("ActiveUser"),
            "descending ActiveUser filtered composite strict LIKE prefix attribution should stay on the ActiveUser route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending ActiveUser filtered composite strict LIKE prefix attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_strict_text_range_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the explicit
        // Character strict text-range covering shape added to the harness.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the bounded Character text-range route.
        assert_positive_scalar_attribution_sample(
            "generated.character_strict_text_range",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "Character strict text-range attribution should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "Character strict text-range attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_character_strict_text_range_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // explicit Character strict text-range covering shape added to the
        // harness.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the descending generated dispatch attribution keeps
        // positive stage accounting on the reverse bounded Character route.
        assert_positive_scalar_attribution_sample(
            "generated.character_strict_text_range_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Character"),
            "descending Character strict text-range attribution should stay on the Character route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending Character strict text-range attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_query_phase_attribution_reports_positive_stages() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let mut rows = Vec::new();

        for (scenario_key, sql, typed_surface, expected_entity, expected_row_count) in
            SCALAR_SELECT_ATTRIBUTION_CASES
        {
            let generated = sql_perf_attribution_sample(
                pic,
                canister_id,
                &SqlPerfAttributionRequest {
                    surface: SqlPerfAttributionSurface::GeneratedDispatch,
                    sql: sql.to_string(),
                    cursor_token: None,
                },
            );
            let typed = sql_perf_attribution_sample(
                pic,
                canister_id,
                &SqlPerfAttributionRequest {
                    surface: *typed_surface,
                    sql: sql.to_string(),
                    cursor_token: None,
                },
            );

            assert_positive_scalar_attribution_sample(
                &format!("generated.{scenario_key}"),
                &generated,
                true,
            );
            assert_positive_scalar_attribution_sample(
                &format!("typed.{scenario_key}"),
                &typed,
                false,
            );
            assert_eq!(
                generated.outcome.entity.as_deref(),
                Some(*expected_entity),
                "generated.{scenario_key} attribution should stay on the expected entity route",
            );
            assert_eq!(
                typed.outcome.entity.as_deref(),
                Some(*expected_entity),
                "typed.{scenario_key} attribution should stay on the expected entity route",
            );
            assert_eq!(
                generated.outcome.row_count,
                Some(*expected_row_count),
                "generated.{scenario_key} attribution should return the requested window size",
            );
            assert_eq!(
                typed.outcome.row_count,
                Some(*expected_row_count),
                "typed.{scenario_key} attribution should return the requested window size",
            );

            rows.push(serde_json::json!({
                "scenario_key": scenario_key,
                "generated": generated,
                "typed": typed,
            }));
        }

        println!(
            "{}",
            serde_json::to_string_pretty(&rows)
                .expect("query attribution samples should serialize to JSON")
        );
    });
}

#[test]
fn sql_canister_perf_grouped_phase_attribution_reports_positive_stages() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let sql = "SELECT age, COUNT(*) FROM User GROUP BY age ORDER BY age ASC LIMIT 10";

        let grouped = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::TypedGroupedUser,
                sql: sql.to_string(),
                cursor_token: None,
            },
        );

        assert!(
            grouped.outcome.success,
            "grouped attribution must keep the representative GROUP BY SELECT successful: {grouped:?}",
        );
        assert!(
            grouped.parse_local_instructions > 0,
            "grouped parse phase must be positive: {grouped:?}",
        );
        assert!(
            grouped.lower_local_instructions > 0,
            "grouped lower phase must be positive: {grouped:?}",
        );
        assert!(
            grouped.dispatch_local_instructions > 0,
            "grouped typed dispatch/setup phase must be positive: {grouped:?}",
        );
        assert!(
            grouped.execute_local_instructions > 0,
            "grouped execute phase must be positive: {grouped:?}",
        );
        assert_eq!(
            grouped.route_local_instructions, 0,
            "typed grouped attribution should not report dynamic route-authority cost: {grouped:?}",
        );
        assert!(
            grouped.total_local_instructions
                >= grouped.parse_local_instructions
                    + grouped.lower_local_instructions
                    + grouped.dispatch_local_instructions
                    + grouped.execute_local_instructions
                    + grouped.wrapper_local_instructions,
            "grouped total must cover every attributed phase: {grouped:?}",
        );

        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "grouped": grouped,
            }))
            .expect("grouped attribution sample should serialize to JSON")
        );
    });
}

#[test]
fn sql_canister_perf_grouped_window_phase_attribution_reports_positive_stages() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let full_page = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::TypedGroupedUser,
                sql: "SELECT age, COUNT(*) FROM User GROUP BY age ORDER BY age ASC LIMIT 10"
                    .to_string(),
                cursor_token: None,
            },
        );
        assert!(
            full_page.outcome.success && full_page.outcome.has_cursor == Some(false),
            "grouped full-page attribution must stay successful without emitting a cursor: {full_page:?}",
        );

        let first_page = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::TypedGroupedUser,
                sql: "SELECT age, COUNT(*) FROM User GROUP BY age ORDER BY age ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );
        assert!(
            first_page.outcome.success && first_page.outcome.has_cursor == Some(true),
            "grouped first-page attribution must stay successful and emit a cursor: {first_page:?}",
        );

        let second_page = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::TypedGroupedUserSecondPage,
                sql: "SELECT age, COUNT(*) FROM User GROUP BY age ORDER BY age ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );
        assert!(
            second_page.outcome.success && second_page.outcome.has_cursor == Some(false),
            "grouped second-page attribution must stay successful without emitting a cursor: {second_page:?}",
        );

        for (label, sample) in [
            ("full page", &full_page),
            ("first page", &first_page),
            ("second page", &second_page),
        ] {
            assert!(
                sample.parse_local_instructions > 0,
                "{label} grouped parse phase must be positive: {sample:?}",
            );
            assert!(
                sample.lower_local_instructions > 0,
                "{label} grouped lower phase must be positive: {sample:?}",
            );
            assert!(
                sample.dispatch_local_instructions > 0,
                "{label} grouped dispatch/setup phase must be positive: {sample:?}",
            );
            assert!(
                sample.execute_local_instructions > 0,
                "{label} grouped execute phase must be positive: {sample:?}",
            );
            assert_eq!(
                sample.route_local_instructions, 0,
                "{label} grouped attribution should not report dynamic route-authority cost: {sample:?}",
            );
            assert!(
                sample.total_local_instructions
                    >= sample.parse_local_instructions
                        + sample.lower_local_instructions
                        + sample.dispatch_local_instructions
                        + sample.execute_local_instructions
                        + sample.wrapper_local_instructions,
                "{label} grouped total must cover every attributed phase: {sample:?}",
            );
        }

        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "full_page": full_page,
                "first_page": first_page,
                "second_page": second_page,
            }))
            .expect("grouped paged attribution samples should serialize to JSON")
        );
    });
}

#[test]
#[expect(clippy::too_many_lines)]
fn sql_canister_dispatch_is_entity_keyed_and_deterministic() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        // Property 1: resolution is by parsed SQL entity name for Character.
        let character_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT name FROM Character ORDER BY name ASC LIMIT 1",
            "Character query should return projection rows",
        );
        assert_eq!(character_rows.entity, "Character");
        assert_eq!(character_rows.columns, vec!["name".to_string()]);
        assert_eq!(character_rows.row_count, 1);
        assert_eq!(character_rows.rows, vec![vec!["Alex Ander".to_string()]]);

        // Property 1: resolution is by parsed SQL entity name for User.
        let user_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT name FROM User ORDER BY name ASC LIMIT 1",
            "User query should return projection rows",
        );
        assert_eq!(user_rows.entity, "User");
        assert_eq!(user_rows.columns, vec!["name".to_string()]);
        assert_eq!(user_rows.row_count, 1);
        assert_eq!(user_rows.rows, vec![vec!["alice".to_string()]]);

        // Property 3: no fallthrough; invalid field on User must be validated as User.
        let bad_user_field_error = query_result(
            pic,
            canister_id,
            "SELECT total_cents FROM User ORDER BY id ASC LIMIT 1",
        )
        .expect_err("bad User field should return error");
        assert!(
            bad_user_field_error
                .message()
                .contains("unknown expression field 'total_cents'"),
            "bad User field should stay on User route: {bad_user_field_error:?}",
        );
        assert!(
            !bad_user_field_error.message().contains("last_error"),
            "bad User field must not include fallback chaining text: {bad_user_field_error:?}",
        );

        // Property 3: no fallthrough; invalid field on Character must be validated as Character.
        let bad_character_field_error = query_result(
            pic,
            canister_id,
            "SELECT age FROM Character ORDER BY id ASC LIMIT 1",
        )
        .expect_err("bad Character field should return error");
        assert!(
            bad_character_field_error
                .message()
                .contains("unknown expression field 'age'"),
            "bad Character field should stay on Character route: {bad_character_field_error:?}",
        );
        assert!(
            !bad_character_field_error.message().contains("last_error"),
            "bad Character field must not include fallback chaining text: {bad_character_field_error:?}",
        );

        // Property 2: unsupported entity errors are immediate, deterministic, and enumerate support.
        let unknown_entity_error =
            query_result(pic, canister_id, "SELECT * FROM MissingEntity LIMIT 1")
                .expect_err("MissingEntity query should return error");
        assert!(
            matches!(
                unknown_entity_error.kind(),
                icydb::error::ErrorKind::Runtime(icydb::error::RuntimeErrorKind::Unsupported)
            ),
            "MissingEntity should map to Runtime::Unsupported: {unknown_entity_error:?}",
        );
        assert!(
            unknown_entity_error
                .message()
                .contains("query endpoint does not support entity 'MissingEntity'"),
            "MissingEntity dispatch error should include unsupported entity detail: {unknown_entity_error:?}",
        );
        assert!(
            unknown_entity_error.message().contains("User")
                && unknown_entity_error.message().contains("Order")
                && unknown_entity_error.message().contains("Character"),
            "MissingEntity dispatch error should enumerate supported entities: {unknown_entity_error:?}",
        );
        assert!(
            !unknown_entity_error.message().contains("last_error"),
            "MissingEntity dispatch error must not include fallback trial chaining details: {unknown_entity_error:?}",
        );

        // EXPLAIN failures should preserve execution parity and expose SQL-surface guidance.
        let explain_unordered_error =
            query_result(pic, canister_id, "EXPLAIN SELECT * FROM Character LIMIT 1")
                .expect_err("unordered EXPLAIN should return error");
        assert!(
            matches!(
                explain_unordered_error.kind(),
                icydb::error::ErrorKind::Query(icydb::error::QueryErrorKind::UnorderedPagination)
            ),
            "unordered EXPLAIN should map to Query::UnorderedPagination: {explain_unordered_error:?}",
        );
        assert!(
            explain_unordered_error
                .message()
                .contains("Cannot EXPLAIN this SQL statement."),
            "unordered EXPLAIN should include SQL-surface heading: {explain_unordered_error:?}",
        );
        assert!(
            explain_unordered_error
                .message()
                .contains("SQL:\nSELECT * FROM Character LIMIT 1"),
            "unordered EXPLAIN should include wrapped SQL statement: {explain_unordered_error:?}",
        );
        assert!(
            explain_unordered_error
                .message()
                .contains("EXPLAIN SELECT * FROM Character ORDER BY id ASC LIMIT 1"),
            "unordered EXPLAIN should include stable-order fix suggestion: {explain_unordered_error:?}",
        );
    });
}

#[test]
#[expect(clippy::redundant_closure_for_method_calls)]
fn sql_canister_query_lane_supports_describe_show_indexes_and_show_columns() {
    run_with_loaded_quickstart_canister(|pic, canister_id| {
        let describe_payload = query_result(pic, canister_id, "DESCRIBE Character")
            .expect("query DESCRIBE should return an Ok payload");
        let describe_lines = describe_payload.render_lines();
        match describe_payload {
            SqlQueryResult::Describe(description) => {
                assert_eq!(description.entity_name(), "Character");
                assert_eq!(description.primary_key(), "id");
                assert!(
                    description
                        .fields()
                        .iter()
                        .any(|field| field.name() == "name"),
                    "describe payload should include the name field",
                );
            }
            other => panic!("query DESCRIBE should return Describe payload, got {other:?}"),
        }
        assert!(
            describe_lines
                .iter()
                .any(|line| line == "entity: Character"),
            "DESCRIBE lines should include canonical entity name",
        );

        assert_metadata_entity_name(
            pic,
            canister_id,
            " dEsCrIbE public.Character; ",
            "Character",
            "query normalized DESCRIBE should return Character metadata payload",
        );

        let show_indexes_payload = query_result(pic, canister_id, "SHOW INDEXES Character")
            .expect("query SHOW INDEXES should return an Ok payload");
        let show_indexes_lines = show_indexes_payload.render_lines();
        match show_indexes_payload {
            SqlQueryResult::ShowIndexes { entity, indexes } => {
                assert_eq!(entity, "Character");
                assert!(
                    indexes.iter().any(|index| index.contains("PRIMARY KEY")),
                    "SHOW INDEXES payload should include at least the primary-key row",
                );
            }
            other => panic!("query SHOW INDEXES should return ShowIndexes payload, got {other:?}"),
        }
        assert!(
            show_indexes_lines
                .first()
                .is_some_and(|line| line.starts_with("surface=indexes entity=Character")),
            "SHOW INDEXES lines should include deterministic surface header",
        );

        assert_metadata_entity_name(
            pic,
            canister_id,
            "sHoW InDeXeS public.Character;",
            "Character",
            "query normalized SHOW INDEXES should return Character metadata payload",
        );

        let show_columns_payload = query_result(pic, canister_id, "SHOW COLUMNS Character")
            .expect("query SHOW COLUMNS should return an Ok payload");
        let show_columns_lines = show_columns_payload.render_lines();
        match show_columns_payload {
            SqlQueryResult::ShowColumns { entity, columns } => {
                assert_eq!(entity, "Character");
                assert!(
                    columns.iter().any(|column| column.name() == "name"),
                    "SHOW COLUMNS payload should include the name field",
                );
                assert!(
                    columns.iter().any(|column| column.primary_key()),
                    "SHOW COLUMNS payload should include one primary-key field",
                );
            }
            other => panic!("query SHOW COLUMNS should return ShowColumns payload, got {other:?}"),
        }
        assert!(
            show_columns_lines
                .first()
                .is_some_and(|line| line.starts_with("surface=columns entity=Character")),
            "SHOW COLUMNS lines should include deterministic surface header",
        );

        assert_metadata_entity_name(
            pic,
            canister_id,
            "sHoW CoLuMnS public.Character;",
            "Character",
            "query normalized SHOW COLUMNS should return Character metadata payload",
        );
    });
}
