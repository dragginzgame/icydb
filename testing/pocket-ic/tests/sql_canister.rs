use candid::{Principal, decode_one, encode_one};
use icydb::db::sql::{SqlQueryResult, SqlQueryRowsOutput};
use icydb_testing_integration::build_canister;
use pocket_ic::{PocketIc, PocketIcBuilder};
use serde::Serialize;
use std::{
    env, fs,
    path::PathBuf,
    sync::{Mutex, OnceLock},
};

const INIT_CYCLES: u128 = 2_000_000_000_000;
const POCKET_IC_BIN_ENV: &str = "POCKET_IC_BIN";
static POCKET_IC_TEST_LOCK: Mutex<()> = Mutex::new(());
static QUICKSTART_CANISTER_WASM: OnceLock<Vec<u8>> = OnceLock::new();

// PocketIC reuses one per-process port file under the system temp dir.
// Clearing it before every builder run forces each serialized test to connect
// to the server it just spawned instead of inheriting an older port from a
// previous test's server lifecycle.
fn clear_stale_pocket_ic_port_file() {
    let port_file = env::temp_dir().join(format!("pocket_ic_{}.port", std::process::id()));
    let _ = fs::remove_file(&port_file);
}

// Build Pocket-IC with an explicit server binary to avoid implicit network
// downloads during local test execution.
fn new_pocket_ic() -> Option<PocketIc> {
    clear_stale_pocket_ic_port_file();

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

    Some(
        PocketIcBuilder::new()
            // Match PocketIc::new() topology expectations: at least one subnet.
            .with_application_subnet()
            .with_server_binary(server_binary)
            .build(),
    )
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
fn install_quickstart_canister(pic: &PocketIc) -> Principal {
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
fn expect_unit_update_ok(pic: &PocketIc, canister_id: Principal, method: &str) {
    let response_bytes = pic
        .update_call(
            canister_id,
            Principal::anonymous(),
            method,
            encode_one(()).expect("encode unit update args"),
        )
        .unwrap_or_else(|err| panic!("{method} update call should succeed: {err}"));
    let response: Result<(), icydb::Error> =
        decode_one(&response_bytes).unwrap_or_else(|err| panic!("decode {method} response: {err}"));
    assert!(response.is_ok(), "{method} returned error: {response:?}");
}

// Load the default fixture dataset and assert the update call returned `Ok(())`.
fn load_default_fixtures(pic: &PocketIc, canister_id: Principal) {
    expect_unit_update_ok(pic, canister_id, "fixtures_load_default");
}

// Reset the default fixture dataset and assert the update call returned `Ok(())`.
fn reset_fixtures(pic: &PocketIc, canister_id: Principal) {
    expect_unit_update_ok(pic, canister_id, "fixtures_reset");
}

// Execute one PocketIC integration test body and keep teardown panics from
// masking the primary failure when the test is already unwinding.
fn run_with_pocket_ic(test_body: impl FnOnce(&PocketIc)) {
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

    // PocketIC tests must not run concurrently.
    // The PocketIC server and test canister install path are not stable under
    // parallel execution in CI; serialize test bodies to keep runs deterministic.
    let _guard = POCKET_IC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let Some(pic) = new_pocket_ic() else {
        return;
    };
    let test_result = catch_unwind(AssertUnwindSafe(|| test_body(&pic)));
    let cleanup_result = catch_unwind(AssertUnwindSafe(|| drop(pic)));

    match test_result {
        Ok(()) => {
            if let Err(cleanup_panic) = cleanup_result {
                resume_unwind(cleanup_panic);
            }
        }
        Err(test_panic) => {
            if cleanup_result.is_err() {
                eprintln!(
                    "suppressed secondary PocketIC cleanup panic while propagating primary test panic"
                );
            }
            resume_unwind(test_panic);
        }
    }
}

fn query_result(
    pic: &PocketIc,
    canister_id: Principal,
    sql: &str,
) -> Result<SqlQueryResult, icydb::Error> {
    let query_bytes = pic
        .query_call(
            canister_id,
            Principal::anonymous(),
            "query",
            encode_one(sql.to_string()).expect("encode query SQL args"),
        )
        .expect("query call should return encoded Result");

    decode_one(&query_bytes).expect("decode query response")
}

fn query_projection_rows(
    pic: &PocketIc,
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

///
/// SqlPerfSurface
///
/// Mirror of the quickstart canister perf-surface enum used for Candid decode
/// and request construction in PocketIC integration tests.
///

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

///
/// SqlPerfAttributionSurface
///
/// Mirror of the quickstart canister SQL attribution surface enum used by the
/// PocketIC perf attribution test.
///

#[derive(candid::CandidType, Clone, Copy, Debug, candid::Deserialize, Serialize)]
enum SqlPerfAttributionSurface {
    GeneratedDispatch,
    TypedDispatchUser,
}

///
/// SqlPerfRequest
///
/// One integration-test request into the quickstart canister perf harness.
/// This keeps scenario identity explicit in the test runner instead of hiding
/// request shape inside inline Candid tuples.
///

#[derive(candid::CandidType, Clone, Debug, candid::Deserialize, Serialize)]
struct SqlPerfRequest {
    surface: SqlPerfSurface,
    sql: String,
    cursor_token: Option<String>,
    repeat_count: u32,
}

///
/// SqlPerfAttributionRequest
///
/// One integration-test request into the quickstart canister SQL attribution
/// endpoint.
///

#[derive(candid::CandidType, Clone, Debug, candid::Deserialize, Serialize)]
struct SqlPerfAttributionRequest {
    surface: SqlPerfAttributionSurface,
    sql: String,
}

///
/// SqlPerfOutcome
///
/// Compact quickstart perf-harness outcome mirror used by PocketIC tests.
/// The audit collector only needs stable surface kind and cardinality metadata
/// here; full SQL payload inspection remains in the main SQL integration tests.
///

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

///
/// SqlPerfSample
///
/// One repeated wasm-side instruction sample returned by the quickstart
/// canister perf harness.
///

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

///
/// SqlPerfAttributionSample
///
/// One fixed-cost SQL query attribution sample returned by the quickstart
/// canister perf harness.
///

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

///
/// SqlPerfScenario
///
/// One named audit scenario captured through the quickstart canister perf
/// harness.
///

#[derive(Clone, Debug, Serialize)]
struct SqlPerfScenario {
    scenario_key: &'static str,
    request: SqlPerfRequest,
}

///
/// SqlPerfScenarioRow
///
/// Serializable row pairing one stable scenario identity with one measured
/// quickstart perf-harness sample.
///

#[derive(Clone, Debug, Serialize)]
struct SqlPerfScenarioRow {
    scenario_key: &'static str,
    sample: SqlPerfSample,
}

fn sql_perf_sample(
    pic: &PocketIc,
    canister_id: Principal,
    request: &SqlPerfRequest,
) -> SqlPerfSample {
    let query_bytes = pic
        .query_call(
            canister_id,
            Principal::anonymous(),
            "sql_perf",
            encode_one(request).expect("encode sql_perf request"),
        )
        .expect("sql_perf query call should return encoded Result");

    let response: Result<SqlPerfSample, icydb::Error> =
        decode_one(&query_bytes).expect("decode sql_perf response");

    response.expect("sql_perf should succeed for integration scenario")
}

fn sql_perf_attribution_sample(
    pic: &PocketIc,
    canister_id: Principal,
    request: &SqlPerfAttributionRequest,
) -> SqlPerfAttributionSample {
    let query_bytes = pic
        .query_call(
            canister_id,
            Principal::anonymous(),
            "sql_perf_attribution",
            encode_one(request).expect("encode sql_perf_attribution request"),
        )
        .expect("sql_perf_attribution query call should return encoded Result");

    let response: Result<SqlPerfAttributionSample, icydb::Error> =
        decode_one(&query_bytes).expect("decode sql_perf_attribution response");

    response.expect("sql_perf_attribution should succeed for integration scenario")
}

fn run_sql_perf_scenarios(
    pic: &PocketIc,
    scenarios: Vec<SqlPerfScenario>,
) -> Vec<SqlPerfScenarioRow> {
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
    pic: &PocketIc,
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

        let entities_bytes = pic
            .query_call(
                canister_id,
                Principal::anonymous(),
                "sql_entities",
                encode_one(()).expect("encode sql_entities args"),
            )
            .expect("sql_entities query call should succeed");
        let entities: Vec<String> =
            decode_one(&entities_bytes).expect("decode sql_entities response");
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
fn sql_canister_perf_query_phase_attribution_reports_positive_stages() {
    run_with_pocket_ic(|pic| {
        let sql = "SELECT id, name FROM User WHERE name = 'alice' ORDER BY id LIMIT 1";
        let canister_id = install_quickstart_canister(pic);
        load_default_fixtures(pic, canister_id);

        let generated = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: sql.to_string(),
            },
        );
        let typed = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::TypedDispatchUser,
                sql: sql.to_string(),
            },
        );

        for (label, sample) in [("generated", &generated), ("typed", &typed)] {
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
        }

        assert!(
            generated.route_local_instructions > 0,
            "generated attribution must report positive authority routing cost: {generated:?}",
        );
        assert_eq!(
            typed.route_local_instructions, 0,
            "typed attribution should not report dynamic route-authority cost: {typed:?}",
        );

        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "generated": generated,
                "typed": typed,
            }))
            .expect("query attribution samples should serialize to JSON")
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
