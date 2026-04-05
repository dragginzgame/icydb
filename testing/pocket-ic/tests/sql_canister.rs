use candid::{Principal, encode_one};
use canic_testkit::pic::{Pic, acquire_pic_serial_guard, pic as new_pic};
use icydb::db::sql::{SqlQueryResult, SqlQueryRowsOutput};
use icydb_testing_integration::build_canister;
use serde::Serialize;
use std::{fs, path::PathBuf, sync::OnceLock};

const INIT_CYCLES: u128 = 2_000_000_000_000;
const POCKET_IC_BIN_ENV: &str = "POCKET_IC_BIN";
const SQL_PERF_PROBE_SQL_ENV: &str = "ICYDB_SQL_PERF_PROBE_SQL";
const SQL_PERF_PROBE_SURFACE_ENV: &str = "ICYDB_SQL_PERF_PROBE_SURFACE";
const SQL_PERF_PROBE_CURSOR_ENV: &str = "ICYDB_SQL_PERF_PROBE_CURSOR";
const SQL_PERF_PROBE_REPEAT_ENV: &str = "ICYDB_SQL_PERF_PROBE_REPEAT_COUNT";
const DEFAULT_SQL_PERF_PROBE_SQL: &str = "SELECT id, level, class_name FROM Character ORDER BY level ASC, class_name ASC, id ASC LIMIT 2";
static QUICKSTART_CANISTER_WASM: OnceLock<Vec<u8>> = OnceLock::new();

// Resolve the PocketIC server binary lazily so tests can skip cleanly when
// the executable is unavailable in local environments.
fn pocket_ic_server_binary() -> Option<PathBuf> {
    let Some(server_binary_raw) = std::env::var_os(POCKET_IC_BIN_ENV) else {
        eprintln!(
            "skipping PocketIC SQL canister integration test: set {POCKET_IC_BIN_ENV} \
             to an executable pocket-ic server binary"
        );

        return None;
    };
    let server_binary = PathBuf::from(server_binary_raw);
    assert!(
        server_binary.is_file(),
        "{POCKET_IC_BIN_ENV} points to {}, but that file does not exist",
        server_binary.display()
    );

    Some(server_binary)
}

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

// Install the quickstart fixture canister into one fresh Pocket-IC canister id.
fn install_quickstart_canister(pic: &Pic) -> Principal {
    let canister_id = pic.create_canister();
    pic.add_cycles(canister_id, INIT_CYCLES);

    let wasm = build_quickstart_canister_wasm();
    pic.install_canister(
        canister_id,
        wasm,
        encode_one(()).expect("encode init args"),
        None,
    );

    canister_id
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

// Execute one PocketIC integration test body against a fresh canic-testkit
// Pic instance. Keeping the lifecycle per-test matches the harness contract
// and avoids reusing one shared PocketIC process across the whole test binary.
fn run_with_pocket_ic(test_body: impl FnOnce(&Pic)) {
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

    let Some(_server_binary) = pocket_ic_server_binary() else {
        return;
    };

    let _serial_guard = acquire_pic_serial_guard();
    let pic = new_pic();
    let test_result = catch_unwind(AssertUnwindSafe(|| test_body(&pic)));
    drop(pic);

    if let Err(test_panic) = test_result {
        resume_unwind(test_panic);
    }
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

//
// SqlPerfSurface
//
// Mirror of the quickstart canister perf-surface enum used for Candid decode
// and request construction in PocketIC integration tests.
//

#[derive(candid::CandidType, Clone, Copy, Debug, candid::Deserialize, Serialize)]
enum SqlPerfSurface {
    GeneratedDispatch,
    TypedDispatchUser,
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
// PocketIC perf attribution test.
//

#[derive(candid::CandidType, Clone, Copy, Debug, candid::Deserialize, Serialize)]
enum SqlPerfAttributionSurface {
    GeneratedDispatch,
    TypedDispatchUser,
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
// Compact quickstart perf-harness outcome mirror used by PocketIC tests.
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

        assert_eq!(
            sample.repeat_count, scenario.request.repeat_count,
            "repeat_count must echo request for {}",
            scenario.scenario_key,
        );
        assert!(
            sample.first_local_instructions > 0,
            "first instruction sample must be positive for {}: {:?}",
            scenario.scenario_key,
            sample,
        );
        assert!(
            sample.min_local_instructions > 0,
            "min instruction sample must be positive for {}: {:?}",
            scenario.scenario_key,
            sample,
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "max must be >= min for {}: {:?}",
            scenario.scenario_key,
            sample,
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "total must cover the first run for {}: {:?}",
            scenario.scenario_key,
            sample,
        );
        assert!(
            sample.outcome_stable,
            "repeated outcome must stay stable for {}: {:?}",
            scenario.scenario_key, sample,
        );

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

// Keep scalar attribution focused on a small representative SELECT cohort so
// read-path tuning does not overfit one especially friendly benchmark query.
const fn scalar_select_attribution_cases() -> [(&'static str, &'static str); 4] {
    [
        (
            "user_name_eq_limit1",
            "SELECT id, name FROM User WHERE name = 'alice' ORDER BY id LIMIT 1",
        ),
        (
            "user_full_row_limit2",
            "SELECT * FROM User ORDER BY id LIMIT 2",
        ),
        (
            "user_name_order_name_limit1",
            "SELECT name FROM User ORDER BY name ASC LIMIT 1",
        ),
        (
            "user_age_order_id_limit1",
            "SELECT age FROM User ORDER BY id ASC LIMIT 1",
        ),
    ]
}

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);

        let entities: Vec<String> = pic
            .query_call(canister_id, "sql_entities", ())
            .expect("sql_entities query call should succeed");
        assert!(entities.iter().any(|name| name == "User"));
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
        let explain_lines = explain_payload.render_lines();
        assert!(
            !explain_lines.is_empty(),
            "EXPLAIN output should be non-empty"
        );
        assert_eq!(
            explain_lines.first().map(String::as_str),
            Some("surface=explain"),
            "EXPLAIN output should be tagged as explain surface",
        );
        match explain_payload {
            SqlQueryResult::Explain { entity, explain } => {
                assert_eq!(entity, "User");
                assert!(
                    !explain.is_empty(),
                    "EXPLAIN payload should include non-empty explain text",
                );
            }
            other => panic!("EXPLAIN should return Explain payload, got {other:?}"),
        }

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
fn sql_canister_query_lane_supports_delete_projection() {
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);

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
                "DELETE FROM User WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 1",
                "DELETE FROM User WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 1",
                "generated direct UPPER(field) STARTS_WITH delete",
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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
        let explain_lines = payload.render_lines();

        assert_eq!(
            explain_lines.first().map(String::as_str),
            Some("surface=explain"),
            "User expression-order EXPLAIN output should be tagged as explain surface",
        );

        // Phase 2: assert the generated query lane preserves the stable
        // index-range and non-covering materialized route labels from the
        // shared descriptor.
        match payload {
            SqlQueryResult::Explain { entity, explain } => {
                assert_eq!(entity, "User");
                assert!(
                    explain.contains("IndexRangeScan")
                        && explain.contains("cov_read_route")
                        && explain.contains("materialized")
                        && explain.contains("LOWER(name)"),
                    "User expression-order EXPLAIN EXECUTION should expose the index-range materialized route: {explain}",
                );
                assert!(
                    explain.contains("proj_fields")
                        && explain.contains("id")
                        && explain.contains("name"),
                    "User expression-order EXPLAIN EXECUTION should expose the projected fields: {explain}",
                );
            }
            other => panic!(
                "User expression-order EXPLAIN EXECUTION should return Explain payload, got {other:?}"
            ),
        }
    });
}

#[test]
fn sql_canister_query_lane_supports_user_expression_order_desc_covering_projection() {
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
        let explain_lines = payload.render_lines();

        assert_eq!(
            explain_lines.first().map(String::as_str),
            Some("surface=explain"),
            "descending User expression-order EXPLAIN output should be tagged as explain surface",
        );

        // Phase 2: assert the generated query lane preserves the stable
        // reverse index-range and non-covering materialized labels from the shared
        // execution descriptor.
        match payload {
            SqlQueryResult::Explain { entity, explain } => {
                assert_eq!(entity, "User");
                assert!(
                    explain.contains("IndexRangeScan")
                        && explain.contains("cov_read_route")
                        && explain.contains("materialized")
                        && explain.contains("LOWER(name)"),
                    "descending User expression-order EXPLAIN EXECUTION should expose the index-range materialized route: {explain}",
                );
                assert!(
                    explain.contains("proj_fields")
                        && explain.contains("id")
                        && explain.contains("name"),
                    "descending User expression-order EXPLAIN EXECUTION should expose the projected fields: {explain}",
                );
            }
            other => panic!(
                "descending User expression-order EXPLAIN EXECUTION should return Explain payload, got {other:?}"
            ),
        }
    });
}

#[test]
fn sql_canister_query_lane_supports_character_covering_projection() {
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

        // Phase 1: request one execution descriptor for the same indexed
        // Character covering projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name = 'Alex Ander' ORDER BY id ASC LIMIT 1",
        )
        .expect("query Character covering EXPLAIN EXECUTION should return an Ok payload");
        let explain_lines = payload.render_lines();

        assert_eq!(
            explain_lines.first().map(String::as_str),
            Some("surface=explain"),
            "Character covering EXPLAIN output should be tagged as explain surface",
        );

        // Phase 2: assert the generated query lane preserves the stable
        // covering-read route labels from the shared execution descriptor.
        match payload {
            SqlQueryResult::Explain { entity, explain } => {
                assert_eq!(entity, "Character");
                assert!(
                    explain.contains("cov_read_route") && explain.contains("covering_read"),
                    "Character covering EXPLAIN EXECUTION should expose the explicit covering-read route: {explain}",
                );
                assert!(
                    explain.contains("covering_fields")
                        && explain.contains("id")
                        && explain.contains("name"),
                    "Character covering EXPLAIN EXECUTION should expose the projected covering fields: {explain}",
                );
            }
            other => panic!(
                "Character covering EXPLAIN EXECUTION should return Explain payload, got {other:?}"
            ),
        }
    });
}

#[test]
fn sql_canister_query_lane_supports_character_order_only_composite_covering_projection() {
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
        let explain_lines = payload.render_lines();

        assert_eq!(
            explain_lines.first().map(String::as_str),
            Some("surface=explain"),
            "Character composite covering EXPLAIN output should be tagged as explain surface",
        );

        // Phase 2: assert the generated query lane preserves the stable
        // index-range and covering-read labels from the shared descriptor.
        match payload {
            SqlQueryResult::Explain { entity, explain } => {
                assert_eq!(entity, "Character");
                assert!(
                    explain.contains("IndexRangeScan")
                        && explain.contains("cov_read_route")
                        && explain.contains("covering_read"),
                    "Character order-only composite EXPLAIN EXECUTION should expose the index-range covering route: {explain}",
                );
                assert!(
                    explain.contains("covering_fields")
                        && explain.contains("id")
                        && explain.contains("level")
                        && explain.contains("class_name"),
                    "Character order-only composite EXPLAIN EXECUTION should expose the projected covering fields: {explain}",
                );
            }
            other => panic!(
                "Character order-only composite EXPLAIN EXECUTION should return Explain payload, got {other:?}"
            ),
        }
    });
}

#[test]
fn sql_canister_query_lane_supports_character_order_only_composite_desc_covering_projection() {
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
        let explain_lines = payload.render_lines();

        assert_eq!(
            explain_lines.first().map(String::as_str),
            Some("surface=explain"),
            "descending Character composite covering EXPLAIN output should be tagged as explain surface",
        );

        // Phase 2: assert the generated query lane preserves the stable
        // reverse index-range and covering-read labels from the shared
        // execution descriptor.
        match payload {
            SqlQueryResult::Explain { entity, explain } => {
                assert_eq!(entity, "Character");
                assert!(
                    explain.contains("IndexRangeScan")
                        && explain.contains("cov_read_route")
                        && explain.contains("covering_read"),
                    "descending Character order-only composite EXPLAIN EXECUTION should expose the index-range covering route: {explain}",
                );
                assert!(
                    explain.contains("covering_fields")
                        && explain.contains("id")
                        && explain.contains("level")
                        && explain.contains("class_name"),
                    "descending Character order-only composite EXPLAIN EXECUTION should expose the projected covering fields: {explain}",
                );
            }
            other => panic!(
                "descending Character order-only composite EXPLAIN EXECUTION should return Explain payload, got {other:?}"
            ),
        }
    });
}

#[test]
fn sql_canister_query_lane_rejects_grouped_sql_execution() {
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN SELECT age, COUNT(*) FROM User GROUP BY age",
        )
        .expect("query grouped EXPLAIN should return an Ok payload");

        match payload {
            SqlQueryResult::Explain { entity, explain } => {
                assert_eq!(entity, "User");
                assert!(
                    !explain.is_empty(),
                    "grouped EXPLAIN payload should include non-empty explain text",
                );
            }
            other => panic!("grouped EXPLAIN should return Explain payload, got {other:?}"),
        }
    });
}

#[test]
fn sql_canister_query_lane_explain_delete_direct_starts_with_family_matches_like_output() {
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
fn sql_canister_query_lane_explain_delete_rejects_non_casefold_wrapped_direct_starts_with() {
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
fn sql_canister_query_lane_supports_direct_starts_with_predicate() {
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
fn sql_canister_query_lane_supports_direct_upper_starts_with_predicate() {
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
fn sql_canister_query_lane_rejects_non_casefold_wrapped_direct_starts_with_predicate() {
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
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
fn sql_canister_perf_generated_dispatch_user_expression_order_reports_positive_instruction_samples()
{
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
#[ignore = "manual perf probe for before/after measurement runs"]
fn sql_canister_perf_probe_reports_sample_as_json() {
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

        // Phase 1: resolve one repo-owned attribution probe request from env
        // so stage-by-stage before/after comparisons stay on the checked-in
        // PocketIC harness.
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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
fn sql_canister_perf_query_phase_attribution_reports_positive_stages() {
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);
        let mut rows = Vec::new();

        for (scenario_key, sql) in scalar_select_attribution_cases() {
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
                    surface: SqlPerfAttributionSurface::TypedDispatchUser,
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
    run_with_pocket_ic(|pic| {
        let sql = "SELECT age, COUNT(*) FROM User GROUP BY age ORDER BY age ASC LIMIT 10";
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
    run_with_pocket_ic(|pic| {
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

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
