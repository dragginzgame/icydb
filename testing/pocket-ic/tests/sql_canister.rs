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
const SQL_PERF_PROBE_CANISTER_ENV: &str = "ICYDB_SQL_PERF_PROBE_CANISTER";
const DEFAULT_SQL_PERF_PROBE_SQL: &str = "SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2";
static SQL_PARITY_CANISTER_WASM: OnceLock<Vec<u8>> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FixtureCanister {
    SqlParity,
}

fn build_cached_fixture_canister_wasm(
    cache: &OnceLock<Vec<u8>>,
    canister_name: &'static str,
) -> Vec<u8> {
    cache
        .get_or_init(|| {
            let wasm_path = build_canister(canister_name)
                .unwrap_or_else(|err| panic!("build {canister_name} canister: {err}"));
            fs::read(&wasm_path).unwrap_or_else(|err| {
                panic!(
                    "failed to read built canister wasm at {}: {err}",
                    wasm_path.display()
                )
            })
        })
        .clone()
}

fn build_fixture_canister_wasm(fixture_canister: FixtureCanister) -> Vec<u8> {
    match fixture_canister {
        FixtureCanister::SqlParity => {
            build_cached_fixture_canister_wasm(&SQL_PARITY_CANISTER_WASM, "sql_parity")
        }
    }
}

const fn fixture_canister_name(fixture_canister: FixtureCanister) -> &'static str {
    match fixture_canister {
        FixtureCanister::SqlParity => "sql_parity",
    }
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

// Install the sql_parity fixture canister into one existing Pic instance.
//
// Keep the bridge narrow: this suite still owns the repo-specific wasm build
// path and the sql_parity canister's empty init-arg contract, but the actual
// installation now goes through canic-testkit's generic public install helper.
fn install_fixture_canister(pic: &Pic, fixture_canister: FixtureCanister) -> Principal {
    let wasm = build_fixture_canister_wasm(fixture_canister);
    let canister_name = fixture_canister_name(fixture_canister);

    pic.try_create_and_install_with_args(
        wasm,
        encode_one(()).expect("encode init args"),
        INIT_CYCLES,
    )
    .unwrap_or_else(|err| panic!("failed to install {canister_name} canister: {err}"))
}

// Install one sql_parity canister into a fresh canic-testkit fixture.
//
// This is the common integration-test shape: one fresh Pic, one real
// sql_parity canister, public update/query calls only. Keep it on the public
// prebuilt-install helper so the suite stays testkit-first.
fn install_fresh_fixture(fixture_canister: FixtureCanister) -> Option<StandaloneCanisterFixture> {
    let canister_name = fixture_canister_name(fixture_canister);

    match try_install_prebuilt_canister_with_cycles(
        build_fixture_canister_wasm(fixture_canister),
        encode_one(()).expect("encode init args"),
        INIT_CYCLES,
    ) {
        Ok(fixture) => Some(fixture),
        Err(StandaloneCanisterFixtureError::Start(err)) if should_skip_pic_start(&err) => {
            skip_sql_canister_test(err);
            None
        }
        Err(err) => panic!("failed to install {canister_name} fixture: {err}"),
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

// Remove the leading Customer base row while keeping the secondary `name`
// entry intact so integration tests can exercise stale-row fallback.
fn make_customer_name_order_stale(pic: &Pic, canister_id: Principal) {
    expect_unit_update_ok(pic, canister_id, "fixtures_make_customer_name_order_stale");
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
// fresh Pic with one installed sql_parity canister.
fn run_with_fixture_canister(
    fixture_canister: FixtureCanister,
    test_body: impl FnOnce(&Pic, Principal),
) {
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

    let Some(fixture) = install_fresh_fixture(fixture_canister) else {
        return;
    };
    let test_result = catch_unwind(AssertUnwindSafe(|| {
        test_body(fixture.pic(), fixture.canister_id());
    }));
    drop(fixture);

    if let Err(test_panic) = test_result {
        resume_unwind(test_panic);
    }
}

// Execute one integration test body against the common loaded-fixture shape:
// a fresh Pic, one installed sql_parity canister, and the default dataset.
fn run_with_loaded_fixture_canister(
    fixture_canister: FixtureCanister,
    test_body: impl FnOnce(&Pic, Principal),
) {
    run_with_fixture_canister(fixture_canister, |pic, canister_id| {
        load_default_fixtures(pic, canister_id);
        test_body(pic, canister_id);
    });
}

fn run_with_loaded_sql_parity_canister(test_body: impl FnOnce(&Pic, Principal)) {
    run_with_loaded_fixture_canister(FixtureCanister::SqlParity, test_body);
}

fn run_with_sql_parity_canister(test_body: impl FnOnce(&Pic, Principal)) {
    run_with_fixture_canister(FixtureCanister::SqlParity, test_body);
}

fn sql_perf_probe_canister() -> FixtureCanister {
    let Some(raw_canister_name) = optional_non_empty_env(SQL_PERF_PROBE_CANISTER_ENV) else {
        return FixtureCanister::SqlParity;
    };

    match raw_canister_name.to_ascii_lowercase().as_str() {
        "sql_parity" | "sql-parity" | "sql" => FixtureCanister::SqlParity,
        other => panic!(
            "unsupported {SQL_PERF_PROBE_CANISTER_ENV} value '{other}', expected 'sql_parity'"
        ),
    }
}

const fn perf_fixture_canister_for_sql(sql: &str) -> FixtureCanister {
    let _ = sql;
    FixtureCanister::SqlParity
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
// Mirror of the sql_parity canister perf-surface enum used for Candid decode
// and request construction in canic-testkit-backed integration tests.
//

#[derive(candid::CandidType, Clone, Copy, Debug, candid::Deserialize, Serialize)]
enum SqlPerfSurface {
    GeneratedDispatch,
    TypedDispatchCustomer,
    TypedDispatchCustomerOrder,
    TypedDispatchCustomerAccount,
    TypedQueryFromSqlCustomerExecute,
    TypedExecuteSqlCustomer,
    TypedInsertCustomer,
    TypedInsertManyAtomicCustomer10,
    TypedInsertManyAtomicCustomer100,
    TypedInsertManyAtomicCustomer1000,
    TypedInsertManyNonAtomicCustomer10,
    TypedInsertManyNonAtomicCustomer100,
    TypedInsertManyNonAtomicCustomer1000,
    TypedUpdateCustomer,
    FluentDeleteCustomerByIdLimit1Count,
    FluentDeletePerfCustomerCount,
    TypedExecuteSqlGroupedCustomer,
    TypedExecuteSqlGroupedCustomerSecondPage,
    TypedExecuteSqlAggregateCustomer,
    FluentLoadCustomerByIdLimit2,
    FluentLoadCustomerNameEqLimit1,
    FluentPagedCustomerByIdLimit2FirstPage,
    FluentPagedCustomerByIdLimit2SecondPage,
    FluentPagedCustomerByIdLimit2InvalidCursor,
}

//
// SqlPerfAttributionSurface
//
// Mirror of the sql_parity canister SQL attribution surface enum used by the
// canic-testkit-backed perf attribution test.
//

#[derive(candid::CandidType, Clone, Copy, Debug, candid::Deserialize, Serialize)]
enum SqlPerfAttributionSurface {
    GeneratedDispatch,
    TypedDispatchCustomer,
    TypedDispatchCustomerOrder,
    TypedDispatchCustomerAccount,
    TypedGroupedCustomer,
    TypedGroupedCustomerSecondPage,
}

//
// SqlPerfRequest
//
// One integration-test request into the sql_parity canister perf harness.
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
// One integration-test request into the sql_parity canister SQL attribution
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
// Compact sql_parity perf-harness outcome mirror used by integration tests.
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
    structural_read_metrics: Option<SqlPerfStructuralReadMetrics>,
    projection_materialization_metrics: Option<SqlPerfProjectionMaterializationMetrics>,
    row_check_metrics: Option<SqlPerfRowCheckMetrics>,
}

#[derive(candid::CandidType, Clone, Debug, candid::Deserialize, Serialize)]
struct SqlPerfStructuralReadMetrics {
    rows_opened: u64,
    declared_slots_validated: u64,
    validated_non_scalar_slots: u64,
    materialized_non_scalar_slots: u64,
    rows_without_lazy_non_scalar_materializations: u64,
}

#[derive(candid::CandidType, Clone, Debug, candid::Deserialize, Serialize)]
struct SqlPerfProjectionMaterializationMetrics {
    projected_rows_path_hits: u64,
    slot_rows_path_hits: u64,
    data_rows_path_hits: u64,
    data_rows_scalar_fallback_hits: u64,
    data_rows_generic_fallback_hits: u64,
    data_rows_projected_slot_accesses: u64,
    data_rows_non_projected_slot_accesses: u64,
    full_row_decode_materializations: u64,
}

#[derive(candid::CandidType, Clone, Debug, candid::Deserialize, Eq, PartialEq, Serialize)]
struct SqlPerfRowCheckMetrics {
    index_entries_scanned: u64,
    index_membership_single_key_entries: u64,
    index_membership_multi_key_entries: u64,
    index_membership_keys_decoded: u64,
    row_check_covering_candidates_seen: u64,
    row_check_rows_emitted: u64,
    row_presence_probe_count: u64,
    row_presence_probe_hits: u64,
    row_presence_probe_misses: u64,
    row_presence_probe_borrowed_data_store_count: u64,
    row_presence_probe_store_handle_count: u64,
    row_presence_key_to_raw_encodes: u64,
}

//
// SqlPerfSample
//
// One repeated wasm-side instruction sample returned by the sql_parity
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
// One fixed-cost SQL query attribution sample returned by the sql_parity
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
// One named audit scenario captured through the sql_parity canister perf
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
// sql_parity perf-harness sample.
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

#[expect(clippy::too_many_lines)]
fn sql_perf_probe_sample_surface() -> SqlPerfSurface {
    let Some(surface_key) = normalized_perf_probe_surface_key() else {
        return SqlPerfSurface::GeneratedDispatch;
    };

    match surface_key.as_str() {
        "generated" | "generateddispatch" | "generated_dispatch" => {
            SqlPerfSurface::GeneratedDispatch
        }
        "typeddispatchcustomer"
        | "typed_dispatch_customer"
        | "typeddispatchuser"
        | "typed_dispatch_user" => SqlPerfSurface::TypedDispatchCustomer,
        "typeddispatchcustomerorder"
        | "typed_dispatch_customer_order"
        | "typeddispatchorder"
        | "typed_dispatch_order"
        | "typeddispatchcustomer_order" => SqlPerfSurface::TypedDispatchCustomerOrder,
        "typeddispatchcustomeraccount"
        | "typed_dispatch_customer_account"
        | "typeddispatchactiveuser"
        | "typed_dispatch_active_user" => SqlPerfSurface::TypedDispatchCustomerAccount,
        "typedqueryfromsqlcustomerexecute"
        | "typed_query_from_sql_customer_execute"
        | "typedqueryfromsqluserexecute"
        | "typed_query_from_sql_user_execute" => SqlPerfSurface::TypedQueryFromSqlCustomerExecute,
        "typedexecutesqlcustomer"
        | "typed_execute_sql_customer"
        | "typedexecutesqluser"
        | "typed_execute_sql_user" => SqlPerfSurface::TypedExecuteSqlCustomer,
        "typedinsertcustomer"
        | "typed_insert_customer"
        | "typedinsertuser"
        | "typed_insert_user" => SqlPerfSurface::TypedInsertCustomer,
        "typedinsertmanyatomiccustomer10"
        | "typed_insert_many_atomic_customer_10"
        | "typedinsertmanyatomicuser10"
        | "typed_insert_many_atomic_user_10" => SqlPerfSurface::TypedInsertManyAtomicCustomer10,
        "typedinsertmanyatomiccustomer100"
        | "typed_insert_many_atomic_customer_100"
        | "typedinsertmanyatomicuser100"
        | "typed_insert_many_atomic_user_100" => SqlPerfSurface::TypedInsertManyAtomicCustomer100,
        "typedinsertmanyatomiccustomer1000"
        | "typed_insert_many_atomic_customer_1000"
        | "typedinsertmanyatomicuser1000"
        | "typed_insert_many_atomic_user_1000" => SqlPerfSurface::TypedInsertManyAtomicCustomer1000,
        "typedinsertmanynonatomiccustomer10"
        | "typed_insert_many_non_atomic_customer_10"
        | "typedinsertmanynonatomicuser10"
        | "typed_insert_many_non_atomic_user_10" => {
            SqlPerfSurface::TypedInsertManyNonAtomicCustomer10
        }
        "typedinsertmanynonatomiccustomer100"
        | "typed_insert_many_non_atomic_customer_100"
        | "typedinsertmanynonatomicuser100"
        | "typed_insert_many_non_atomic_user_100" => {
            SqlPerfSurface::TypedInsertManyNonAtomicCustomer100
        }
        "typedinsertmanynonatomiccustomer1000"
        | "typed_insert_many_non_atomic_customer_1000"
        | "typedinsertmanynonatomicuser1000"
        | "typed_insert_many_non_atomic_user_1000" => {
            SqlPerfSurface::TypedInsertManyNonAtomicCustomer1000
        }
        "typedupdatecustomer"
        | "typed_update_customer"
        | "typedupdateuser"
        | "typed_update_user" => SqlPerfSurface::TypedUpdateCustomer,
        "fluentdeletecustomerbyidlimit1count"
        | "fluent_delete_customer_by_id_limit_1_count"
        | "fluentdeleteuserorderidlimit1count"
        | "fluent_delete_user_order_id_limit_1_count" => {
            SqlPerfSurface::FluentDeleteCustomerByIdLimit1Count
        }
        "fluentdeleteperfcustomercount"
        | "fluent_delete_perf_customer_count"
        | "fluentdeleteperfusercount"
        | "fluent_delete_perf_user_count" => SqlPerfSurface::FluentDeletePerfCustomerCount,
        "typedexecutesqlgroupedcustomer"
        | "typed_execute_sql_grouped_customer"
        | "typedexecutesqlgroupeduser"
        | "typed_execute_sql_grouped_user" => SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
        "typedexecutesqlgroupedcustomersecondpage"
        | "typed_execute_sql_grouped_customer_second_page"
        | "typedexecutesqlgroupedusersecondpage"
        | "typed_execute_sql_grouped_user_second_page" => {
            SqlPerfSurface::TypedExecuteSqlGroupedCustomerSecondPage
        }
        "typedexecutesqlaggregatecustomer"
        | "typed_execute_sql_aggregate_customer"
        | "typedexecutesqlaggregateuser"
        | "typed_execute_sql_aggregate_user" => SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
        "fluentloadcustomerbyidlimit2"
        | "fluent_load_customer_by_id_limit_2"
        | "fluentloaduserorderidlimit2"
        | "fluent_load_user_order_id_limit_2" => SqlPerfSurface::FluentLoadCustomerByIdLimit2,
        "fluentloadcustomernameeqlimit1"
        | "fluent_load_customer_name_eq_limit_1"
        | "fluentloadusernameeqlimit1"
        | "fluent_load_user_name_eq_limit_1" => SqlPerfSurface::FluentLoadCustomerNameEqLimit1,
        "fluentpagedcustomerbyidlimit2firstpage"
        | "fluent_paged_customer_by_id_limit_2_first_page"
        | "fluentpageduserorderidlimit2firstpage"
        | "fluent_paged_user_order_id_limit_2_first_page" => {
            SqlPerfSurface::FluentPagedCustomerByIdLimit2FirstPage
        }
        "fluentpagedcustomerbyidlimit2secondpage"
        | "fluent_paged_customer_by_id_limit_2_second_page"
        | "fluentpageduserorderidlimit2secondpage"
        | "fluent_paged_user_order_id_limit_2_second_page" => {
            SqlPerfSurface::FluentPagedCustomerByIdLimit2SecondPage
        }
        "fluentpagedcustomerbyidlimit2invalidcursor"
        | "fluent_paged_customer_by_id_limit_2_invalid_cursor"
        | "fluentpageduserorderidlimit2invalidcursor"
        | "fluent_paged_user_order_id_limit_2_invalid_cursor" => {
            SqlPerfSurface::FluentPagedCustomerByIdLimit2InvalidCursor
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
        "typeddispatchcustomer"
        | "typed_dispatch_customer"
        | "typeddispatchuser"
        | "typed_dispatch_user" => SqlPerfAttributionSurface::TypedDispatchCustomer,
        "typeddispatchcustomerorder"
        | "typed_dispatch_customer_order"
        | "typeddispatchorder"
        | "typed_dispatch_order"
        | "typeddispatchcustomer_order" => SqlPerfAttributionSurface::TypedDispatchCustomerOrder,
        "typeddispatchcustomeraccount"
        | "typed_dispatch_customer_account"
        | "typeddispatchactiveuser"
        | "typed_dispatch_active_user" => SqlPerfAttributionSurface::TypedDispatchCustomerAccount,
        "typedgroupedcustomer"
        | "typed_grouped_customer"
        | "typedgroupeduser"
        | "typed_grouped_user" => SqlPerfAttributionSurface::TypedGroupedCustomer,
        "typedgroupedcustomersecondpage"
        | "typed_grouped_customer_second_page"
        | "typedgroupedusersecondpage"
        | "typed_grouped_user_second_page" => {
            SqlPerfAttributionSurface::TypedGroupedCustomerSecondPage
        }
        _ => panic!(
            "unsupported {SQL_PERF_PROBE_SURFACE_ENV} value '{surface_key}' for sql perf attribution probe"
        ),
    }
}

fn run_sql_perf_scenarios(pic: &Pic, scenarios: Vec<SqlPerfScenario>) -> Vec<SqlPerfScenarioRow> {
    let mut rows = Vec::with_capacity(scenarios.len());

    for scenario in scenarios {
        let canister_id = install_fixture_canister(
            pic,
            perf_fixture_canister_for_sql(scenario.request.sql.as_str()),
        );
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
const SCALAR_SELECT_ATTRIBUTION_CASES: &[(
    &str,
    FixtureCanister,
    &str,
    SqlPerfAttributionSurface,
    &str,
    u32,
)] = &[
    (
        "user_name_eq_limit1",
        FixtureCanister::SqlParity,
        "SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 1",
        SqlPerfAttributionSurface::TypedDispatchCustomer,
        "Customer",
        1,
    ),
    (
        "user_full_row_limit2",
        FixtureCanister::SqlParity,
        "SELECT * FROM Customer ORDER BY id LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomer,
        "Customer",
        2,
    ),
    (
        "user_name_order_name_limit1",
        FixtureCanister::SqlParity,
        "SELECT name FROM Customer ORDER BY name ASC LIMIT 1",
        SqlPerfAttributionSurface::TypedDispatchCustomer,
        "Customer",
        1,
    ),
    (
        "user_age_order_id_limit1",
        FixtureCanister::SqlParity,
        "SELECT age FROM Customer ORDER BY id ASC LIMIT 1",
        SqlPerfAttributionSurface::TypedDispatchCustomer,
        "Customer",
        1,
    ),
    (
        "user_primary_key_covering_id_limit1",
        FixtureCanister::SqlParity,
        "SELECT id FROM Customer ORDER BY id ASC LIMIT 1",
        SqlPerfAttributionSurface::TypedDispatchCustomer,
        "Customer",
        1,
    ),
    (
        "user_secondary_covering_name_limit2_asc",
        FixtureCanister::SqlParity,
        "SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomer,
        "Customer",
        2,
    ),
    (
        "user_secondary_covering_name_limit2_desc",
        FixtureCanister::SqlParity,
        "SELECT id, name FROM Customer ORDER BY name DESC, id DESC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomer,
        "Customer",
        2,
    ),
    (
        "user_secondary_covering_name_strict_range_limit2_asc",
        FixtureCanister::SqlParity,
        "SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomer,
        "Customer",
        2,
    ),
    (
        "user_secondary_covering_name_strict_range_limit2_desc",
        FixtureCanister::SqlParity,
        "SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name DESC, id DESC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomer,
        "Customer",
        2,
    ),
    (
        "customer_order_order_only_composite_limit2_asc",
        FixtureCanister::SqlParity,
        "SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomerOrder,
        "CustomerOrder",
        2,
    ),
    (
        "customer_order_order_only_composite_limit2_desc",
        FixtureCanister::SqlParity,
        "SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomerOrder,
        "CustomerOrder",
        2,
    ),
    (
        "customer_order_numeric_equality_priority20_limit2_asc",
        FixtureCanister::SqlParity,
        "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomerOrder,
        "CustomerOrder",
        2,
    ),
    (
        "customer_order_numeric_equality_priority20_limit2_desc",
        FixtureCanister::SqlParity,
        "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomerOrder,
        "CustomerOrder",
        2,
    ),
    (
        "customer_account_filtered_order_only_name_limit2_asc",
        FixtureCanister::SqlParity,
        "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_composite_expression_order_only_handle_limit2_asc",
        FixtureCanister::SqlParity,
        "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_composite_expression_key_only_order_only_tier_limit2_asc",
        FixtureCanister::SqlParity,
        "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_composite_expression_key_only_order_only_tier_limit2_desc",
        FixtureCanister::SqlParity,
        "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_composite_expression_key_only_strict_range_tier_limit2_asc",
        FixtureCanister::SqlParity,
        "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_composite_expression_key_only_direct_starts_with_tier_limit2_asc",
        FixtureCanister::SqlParity,
        "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
];

// Keep non-Customer ordered covering perf parity focused on the read shapes that
// drove the recent `0.68` planner and route work.
const NON_USER_ORDERED_COVERING_PERF_CASES: &[(
    &str,
    FixtureCanister,
    &str,
    SqlPerfSurface,
    &str,
    u32,
)] = &[
    (
        "customer_order_order_only_composite.priority_status_id_limit2.asc",
        FixtureCanister::SqlParity,
        "SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerOrder,
        "CustomerOrder",
        2,
    ),
    (
        "customer_order_order_only_composite.priority_status_id_limit2.desc",
        FixtureCanister::SqlParity,
        "SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerOrder,
        "CustomerOrder",
        2,
    ),
    (
        "customer_order_numeric_equality.priority_eq20_status_id_limit2.asc",
        FixtureCanister::SqlParity,
        "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerOrder,
        "CustomerOrder",
        2,
    ),
    (
        "customer_order_numeric_equality.priority_eq20_status_id_limit2.desc",
        FixtureCanister::SqlParity,
        "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerOrder,
        "CustomerOrder",
        2,
    ),
    (
        "customer_order_numeric_equality_bounded_status.priority_eq20_status_bd_limit2.asc",
        FixtureCanister::SqlParity,
        "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerOrder,
        "CustomerOrder",
        2,
    ),
    (
        "customer_order_numeric_equality_bounded_status.priority_eq20_status_bd_limit2.desc",
        FixtureCanister::SqlParity,
        "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerOrder,
        "CustomerOrder",
        2,
    ),
    (
        "customer_account_filtered_order_only_name_limit2.asc",
        FixtureCanister::SqlParity,
        "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_order_only_name_limit2.desc",
        FixtureCanister::SqlParity,
        "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_composite_order_only_handle_limit2.asc",
        FixtureCanister::SqlParity,
        "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_composite_order_only_handle_limit2.desc",
        FixtureCanister::SqlParity,
        "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_composite_expression_order_only_handle_limit2.asc",
        FixtureCanister::SqlParity,
        "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_composite_expression_order_only_handle_limit2.desc",
        FixtureCanister::SqlParity,
        "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_composite_expression_key_only_order_only_tier_limit2.asc",
        FixtureCanister::SqlParity,
        "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_composite_expression_key_only_order_only_tier_limit2.desc",
        FixtureCanister::SqlParity,
        "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_composite_expression_key_only_strict_range_tier_limit2.asc",
        FixtureCanister::SqlParity,
        "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
        2,
    ),
    (
        "customer_account_filtered_composite_expression_key_only_direct_starts_with_tier_limit2.asc",
        FixtureCanister::SqlParity,
        "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        SqlPerfSurface::TypedDispatchCustomerAccount,
        "CustomerAccount",
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
    let sql = "SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 1";

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
            SqlPerfSurface::TypedDispatchCustomer,
            sql,
            1,
        ),
        sql_perf_scenario(
            "select.typed.dispatch.user_name_eq_limit.x10",
            SqlPerfSurface::TypedDispatchCustomer,
            sql,
            10,
        ),
        sql_perf_scenario(
            "select.typed.dispatch.user_name_eq_limit.x100",
            SqlPerfSurface::TypedDispatchCustomer,
            sql,
            100,
        ),
    ]
}

fn insert_operation_repeat_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        sql_perf_scenario(
            "insert.typed.user_single.x1",
            SqlPerfSurface::TypedInsertCustomer,
            "INSERT Customer",
            1,
        ),
        sql_perf_scenario(
            "insert.typed.user_single.x10",
            SqlPerfSurface::TypedInsertCustomer,
            "INSERT Customer",
            10,
        ),
        sql_perf_scenario(
            "insert.typed.user_single.x100",
            SqlPerfSurface::TypedInsertCustomer,
            "INSERT Customer",
            100,
        ),
    ]
}

fn update_operation_repeat_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        sql_perf_scenario(
            "update.typed.user_single.x1",
            SqlPerfSurface::TypedUpdateCustomer,
            "UPDATE Customer",
            1,
        ),
        sql_perf_scenario(
            "update.typed.user_single.x10",
            SqlPerfSurface::TypedUpdateCustomer,
            "UPDATE Customer",
            10,
        ),
        sql_perf_scenario(
            "update.typed.user_single.x100",
            SqlPerfSurface::TypedUpdateCustomer,
            "UPDATE Customer",
            100,
        ),
    ]
}

fn delete_operation_repeat_scenarios() -> Vec<SqlPerfScenario> {
    vec![
        sql_perf_scenario(
            "delete.fluent.user_single.count.x1",
            SqlPerfSurface::FluentDeletePerfCustomerCount,
            "DELETE PERF Customer COUNT",
            1,
        ),
        sql_perf_scenario(
            "delete.fluent.user_single.count.x10",
            SqlPerfSurface::FluentDeletePerfCustomerCount,
            "DELETE PERF Customer COUNT",
            10,
        ),
        sql_perf_scenario(
            "delete.fluent.user_single.count.x100",
            SqlPerfSurface::FluentDeletePerfCustomerCount,
            "DELETE PERF Customer COUNT",
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
fn sql_canister_sql_parity_smoke_flow() {
    run_with_sql_parity_canister(|pic, canister_id| {
        let entities: Vec<String> = pic
            .query_call(canister_id, "sql_entities", ())
            .expect("sql_entities query call should succeed");
        assert!(entities.iter().any(|name| name == "Customer"));
        assert!(entities.iter().any(|name| name == "CustomerAccount"));
        assert!(entities.iter().any(|name| name == "CustomerOrder"));

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
                    show_entities.iter().any(|entity| entity == "Customer"),
                    "SHOW ENTITIES payload should include Customer",
                );
                assert!(
                    show_entities
                        .iter()
                        .any(|entity| entity == "CustomerAccount"),
                    "SHOW ENTITIES payload should include CustomerAccount",
                );
                assert!(
                    show_entities.iter().any(|entity| entity == "CustomerOrder"),
                    "SHOW ENTITIES payload should include CustomerOrder",
                );
            }
            other => panic!("SHOW ENTITIES should return ShowEntities payload, got {other:?}"),
        }

        load_default_fixtures(pic, canister_id);

        let explain_payload = query_result(
            pic,
            canister_id,
            "EXPLAIN SELECT name FROM Customer ORDER BY name LIMIT 1",
        )
        .expect("EXPLAIN query should return an Ok payload");
        assert_explain_route(
            explain_payload,
            "Customer",
            &[],
            &[],
            "EXPLAIN query should return a Customer explain payload",
        );

        let query_sql = "SELECT name FROM Customer ORDER BY name LIMIT 1";
        let projection =
            query_projection_rows(pic, canister_id, query_sql, "query endpoint should project");
        assert_eq!(projection.entity, "Customer");
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let asc_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2",
            "ascending Customer secondary covering projection should return projected rows",
        );
        assert_projection_window(
            &asc_rows,
            "Customer",
            &["id", "name"],
            &[
                &[ANY_PROJECTION_VALUE, "alice"],
                &[ANY_PROJECTION_VALUE, "bob"],
            ],
            "ascending Customer secondary covering projection should preserve ordered rows",
        );

        let desc_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer ORDER BY name DESC, id DESC LIMIT 2",
            "descending Customer secondary covering projection should return projected rows",
        );
        assert_projection_window(
            &desc_rows,
            "Customer",
            &["id", "name"],
            &[
                &[ANY_PROJECTION_VALUE, "charlie"],
                &[ANY_PROJECTION_VALUE, "bob"],
            ],
            "descending Customer secondary covering projection should preserve ordered rows",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_secondary_covering_witness_validated_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("query Customer secondary covering EXPLAIN EXECUTION should return an Ok payload");
        assert_explain_route(
            payload,
            "Customer",
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
            "Customer secondary covering EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_secondary_covering_equality_witness_validated_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 1",
        )
        .expect(
            "query Customer secondary covering equality EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
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
            "Customer secondary covering equality EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_secondary_covering_equality_desc_witness_validated_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id DESC LIMIT 1",
        )
        .expect(
            "query Customer secondary covering equality desc EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
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
            "Customer secondary covering equality desc EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_user_secondary_covering_strict_range_projection_window() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2",
            "ascending Customer secondary covering range projection should return projected rows",
        );
        assert_projection_window(
            &rows,
            "Customer",
            &["id", "name"],
            &[
                &[ANY_PROJECTION_VALUE, "alice"],
                &[ANY_PROJECTION_VALUE, "bob"],
            ],
            "ascending Customer secondary covering range projection should preserve ordered rows",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_secondary_covering_strict_range_witness_validated_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect(
            "query Customer secondary covering range EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
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
            "Customer secondary covering range EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_user_secondary_covering_strict_range_desc_projection_window() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name DESC, id DESC LIMIT 2",
            "descending Customer secondary covering range projection should return projected rows",
        );
        assert_projection_window(
            &rows,
            "Customer",
            &["id", "name"],
            &[
                &[ANY_PROJECTION_VALUE, "bob"],
                &[ANY_PROJECTION_VALUE, "alice"],
            ],
            "descending Customer secondary covering range projection should preserve ordered rows",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_secondary_covering_strict_range_desc_witness_validated_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name DESC, id DESC LIMIT 2",
        )
        .expect(
            "query Customer secondary covering desc range EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
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
            "Customer secondary covering desc range EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_delete_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let deleted_rows = query_projection_rows(
            pic,
            canister_id,
            "DELETE FROM Customer ORDER BY id LIMIT 1",
            "query DELETE should return deleted projection rows",
        );

        assert_eq!(deleted_rows.entity, "Customer");
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
    run_with_sql_parity_canister(|pic, canister_id| {
        // Phase 1: compare the accepted direct family against the established
        // LIKE forms on the generated query/delete boundary.
        let cases = [
            (
                "DELETE FROM Customer WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 1",
                "DELETE FROM Customer WHERE name LIKE 'a%' ORDER BY id LIMIT 1",
                "generated strict direct STARTS_WITH delete",
            ),
            (
                "DELETE FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 1",
                "DELETE FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 1",
                "generated direct LOWER(field) STARTS_WITH delete",
            ),
            (
                "DELETE FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
                "DELETE FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 1",
                "generated direct LOWER(field) ordered text-range delete",
            ),
            (
                "DELETE FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 1",
                "DELETE FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 1",
                "generated direct UPPER(field) STARTS_WITH delete",
            ),
            (
                "DELETE FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
                "DELETE FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 1",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let err = query_result(
            pic,
            canister_id,
            "DELETE FROM Customer WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 1",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT LOWER(name) FROM Customer ORDER BY id LIMIT 2",
            "query computed projection should return projected rows",
        );

        assert_eq!(rows.entity, "Customer");
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one expression-order Customer projection so the
        // generated SQL lane proves the new LOWER(name) secondary order path.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            "query Customer expression-order covering projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the expected
        // projected Customer window and column order.
        assert_eq!(rows.entity, "Customer");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0][1],
            "alice".to_string(),
            "expression-order Customer query should start from the lowercased first row",
        );
        assert_eq!(
            rows.rows[1][1],
            "bob".to_string(),
            "expression-order Customer query should keep stable lowercased ordering",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_expression_order_covering_route() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the new expression
        // order-only Customer projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query Customer expression-order covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
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
            "Customer expression-order EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_user_expression_key_only_order_covering_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            "query Customer expression key-only order covering projection should return projected rows",
        );

        assert_eq!(rows.entity, "Customer");
        assert_eq!(rows.columns, vec!["id".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0].len(), 1);
        assert_eq!(rows.rows[1].len(), 1);
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_expression_key_only_order_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query Customer expression key-only order covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
            &[
                "CoveringRead",
                "covering_read",
                "existing_row_mode",
                "witness_validated",
                "LOWER(name)",
                "proj_fields",
                "id",
            ],
            &["row_check_required"],
            "Customer expression key-only order EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_user_expression_order_desc_covering_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one descending expression-order Customer projection so
        // reverse traversal stays locked in the generated SQL harness.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            "query Customer descending expression-order covering projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the expected
        // descending projected Customer window.
        assert_eq!(rows.entity, "Customer");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0][1],
            "charlie".to_string(),
            "descending expression-order Customer query should start from the last lowercased row",
        );
        assert_eq!(
            rows.rows[1][1],
            "bob".to_string(),
            "descending expression-order Customer query should keep stable reverse lowercased ordering",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_expression_order_desc_covering_route() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending
        // expression order-only Customer projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query Customer descending expression-order covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
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
            "descending Customer expression-order EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_user_expression_key_only_order_desc_covering_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            "query Customer descending expression key-only order covering projection should return projected rows",
        );

        assert_eq!(rows.entity, "Customer");
        assert_eq!(rows.columns, vec!["id".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0].len(), 1);
        assert_eq!(rows.rows[1].len(), 1);
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_expression_key_only_order_desc_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query Customer descending expression key-only order covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
            &[
                "CoveringRead",
                "covering_read",
                "existing_row_mode",
                "witness_validated",
                "LOWER(name)",
                "proj_fields",
                "id",
            ],
            &["row_check_required"],
            "descending Customer expression key-only order EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_user_expression_key_only_strict_text_range_covering_projection()
{
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            "query Customer expression key-only strict text-range covering projection should return projected rows",
        );

        assert_eq!(rows.entity, "Customer");
        assert_eq!(rows.columns, vec!["id".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].len(), 1);
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_expression_key_only_strict_text_range_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query Customer expression key-only strict text-range covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
            &[
                "CoveringRead",
                "covering_read",
                "existing_row_mode",
                "witness_validated",
                "LOWER(name)",
                "proj_fields",
                "id",
            ],
            &["row_check_required"],
            "Customer expression key-only strict text-range EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_user_expression_key_only_strict_text_range_desc_covering_projection()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            "query Customer descending expression key-only strict text-range covering projection should return projected rows",
        );

        assert_eq!(rows.entity, "Customer");
        assert_eq!(rows.columns, vec!["id".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].len(), 1);
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_expression_key_only_strict_text_range_desc_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query Customer descending expression key-only strict text-range covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
            &[
                "CoveringRead",
                "covering_read",
                "existing_row_mode",
                "witness_validated",
                "LOWER(name)",
                "proj_fields",
                "id",
            ],
            &["row_check_required"],
            "descending Customer expression key-only strict text-range EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_user_primary_key_covering_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id FROM Customer ORDER BY id ASC LIMIT 1",
            "query Customer PK-only covering projection should return projected rows",
        );

        assert_eq!(rows.entity, "Customer");
        assert_eq!(rows.columns, vec!["id".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].len(), 1);
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_primary_key_covering_route() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id FROM Customer ORDER BY id ASC LIMIT 1",
        )
        .expect("query Customer PK-only covering EXPLAIN EXECUTION should return an Ok payload");
        assert_explain_route(
            payload,
            "Customer",
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
            "Customer PK-only covering EXPLAIN EXECUTION should expose the planner-proven covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_order_covering_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerOrder WHERE name = 'A-101' ORDER BY id ASC LIMIT 1",
            "query CustomerOrder covering projection should return projected rows",
        );

        assert_eq!(rows.entity, "CustomerOrder");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][1], "A-101".to_string());
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_order_covering_read_route() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name = 'A-101' ORDER BY id ASC LIMIT 1",
        )
        .expect("query CustomerOrder covering EXPLAIN EXECUTION should return an Ok payload");
        assert_explain_route(
            payload,
            "CustomerOrder",
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
            "CustomerOrder covering EXPLAIN EXECUTION should expose the explicit covering-read route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_order_order_only_composite_covering_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
            "query CustomerOrder order-only composite covering projection should return projected rows",
        );

        assert_projection_window(
            &rows,
            "CustomerOrder",
            &["id", "priority", "status"],
            &[
                &[ANY_PROJECTION_VALUE, "10", "Alpha"],
                &[ANY_PROJECTION_VALUE, "20", "Backlog"],
            ],
            "CustomerOrder order-only composite covering projection should preserve the expected composite window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_order_order_only_composite_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerOrder order-only composite covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "CustomerOrder",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "priority",
                "status",
            ],
            &["row_check_required"],
            "CustomerOrder order-only composite EXPLAIN EXECUTION should expose the index-range covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_order_order_only_composite_desc_covering_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
            "query CustomerOrder descending order-only composite covering projection should return projected rows",
        );

        assert_projection_window(
            &rows,
            "CustomerOrder",
            &["id", "priority", "status"],
            &[
                &[ANY_PROJECTION_VALUE, "30", "Closed"],
                &[ANY_PROJECTION_VALUE, "20", "Draft"],
            ],
            "descending CustomerOrder order-only composite covering projection should preserve the reverse composite window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_order_order_only_composite_desc_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
        )
        .expect(
            "query CustomerOrder descending order-only composite covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "CustomerOrder",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "priority",
                "status",
            ],
            &["row_check_required"],
            "descending CustomerOrder order-only composite EXPLAIN EXECUTION should expose the index-range covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_order_numeric_equality_covering_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
            "query CustomerOrder numeric-equality covering projection should return projected rows",
        );

        assert_projection_window(
            &rows,
            "CustomerOrder",
            &["id", "priority", "status"],
            &[
                &[ANY_PROJECTION_VALUE, "20", "Backlog"],
                &[ANY_PROJECTION_VALUE, "20", "Billing"],
            ],
            "CustomerOrder numeric-equality covering projection should preserve the equality-prefix ordered window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_order_numeric_equality_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerOrder numeric-equality covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "CustomerOrder",
            &[
                "CoveringRead",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "priority",
                "status",
            ],
            &["row_check_required"],
            "CustomerOrder numeric-equality EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_order_numeric_equality_desc_covering_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
            "query descending CustomerOrder numeric-equality covering projection should return projected rows",
        );

        assert_projection_window(
            &rows,
            "CustomerOrder",
            &["id", "priority", "status"],
            &[
                &[ANY_PROJECTION_VALUE, "20", "Draft"],
                &[ANY_PROJECTION_VALUE, "20", "Closed"],
            ],
            "descending CustomerOrder numeric-equality covering projection should preserve the reverse equality-prefix ordered window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_order_numeric_equality_desc_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerOrder numeric-equality covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "CustomerOrder",
            &[
                "CoveringRead",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "priority",
                "status",
            ],
            &["row_check_required"],
            "descending CustomerOrder numeric-equality EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_order_numeric_equality_status_strict_text_range_covering_projection()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status ASC, id ASC LIMIT 2",
            "query CustomerOrder numeric-equality bounded status covering projection should return projected rows",
        );

        assert_projection_window(
            &rows,
            "CustomerOrder",
            &["id", "priority", "status"],
            &[
                &[ANY_PROJECTION_VALUE, "20", "Backlog"],
                &[ANY_PROJECTION_VALUE, "20", "Billing"],
            ],
            "CustomerOrder numeric-equality bounded status covering projection should preserve the bounded suffix ordered window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_order_numeric_equality_status_strict_text_range_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerOrder numeric-equality bounded status covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "CustomerOrder",
            &[
                "CoveringRead",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "priority",
                "status",
            ],
            &["row_check_required"],
            "CustomerOrder numeric-equality bounded status EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_order_numeric_equality_status_strict_text_range_desc_covering_projection()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status DESC, id DESC LIMIT 2",
            "query descending CustomerOrder numeric-equality bounded status covering projection should return projected rows",
        );

        assert_projection_window(
            &rows,
            "CustomerOrder",
            &["id", "priority", "status"],
            &[
                &[ANY_PROJECTION_VALUE, "20", "Closed"],
                &[ANY_PROJECTION_VALUE, "20", "Billing"],
            ],
            "descending CustomerOrder numeric-equality bounded status covering projection should preserve the reverse bounded suffix ordered window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_order_numeric_equality_status_strict_text_range_desc_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerOrder numeric-equality bounded status covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "CustomerOrder",
            &[
                "CoveringRead",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "existing_row_mode",
                "witness_validated",
                "id",
                "priority",
                "status",
            ],
            &["row_check_required"],
            "descending CustomerOrder numeric-equality bounded status EXPLAIN EXECUTION should expose the witness-backed covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_order_only_covering_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one filtered-index guarded order-only projection so
        // the generated SQL lane reaches the guarded secondary-index route.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
            "query CustomerAccount filtered order-only covering projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the guarded
        // filtered-index window instead of falling back to materialized rows.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "name"],
            &[
                &[ANY_PROJECTION_VALUE, "bravo"],
                &[ANY_PROJECTION_VALUE, "charlie"],
            ],
            "CustomerAccount filtered order-only covering projection should expose the guarded filtered-index window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_order_only_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded filtered
        // order-only CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerAccount filtered order-only covering EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // index-range and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "id",
                "name",
            ],
            &[],
            "CustomerAccount filtered order-only EXPLAIN EXECUTION should expose the index-range covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_order_only_desc_covering_projection()
{
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one descending filtered-index guarded order-only
        // projection so reverse traversal stays locked in the generated lane.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
            "query descending CustomerAccount filtered order-only covering projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the expected
        // descending filtered-index window.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "name"],
            &[
                &[ANY_PROJECTION_VALUE, "echo"],
                &[ANY_PROJECTION_VALUE, "charlie"],
            ],
            "descending CustomerAccount filtered order-only covering projection should expose the reverse filtered-index window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_order_only_desc_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // filtered order-only CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerAccount filtered order-only covering EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse index-range and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
            &[
                "IndexRangeScan",
                "cov_read_route",
                "covering_read",
                "covering_fields",
                "id",
                "name",
            ],
            &[],
            "descending CustomerAccount filtered order-only EXPLAIN EXECUTION should expose the index-range covering route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_strict_like_prefix_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one guarded filtered-index strict prefix projection
        // so the generated SQL lane reaches the bounded filtered route.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
            "query CustomerAccount filtered strict LIKE prefix projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the guarded
        // bounded window on the CustomerAccount filtered index.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "name"],
            &[&[ANY_PROJECTION_VALUE, "bravo"]],
            "CustomerAccount filtered strict LIKE prefix projection should expose the bounded filtered window",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_equivalent_strict_prefix_forms_match_customer_account_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute the three accepted guarded strict prefix spellings
        // against the same ordered CustomerAccount projection window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
            "CustomerAccount filtered strict LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerAccount WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name ASC, id ASC LIMIT 1",
            "CustomerAccount filtered direct STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name ASC, id ASC LIMIT 1",
            "CustomerAccount filtered strict text-range predicate should return projected rows",
        );

        // Phase 2: keep the canister query lane pinned to one shared filtered
        // result set across the equivalent strict prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "CustomerAccount filtered direct STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "CustomerAccount filtered strict text-range and LIKE prefix query rows should stay in parity",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_strict_like_prefix_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded filtered
        // strict-prefix CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
        )
        .expect(
            "query CustomerAccount filtered strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // bounded index-range and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "CustomerAccount filtered strict LIKE prefix EXPLAIN EXECUTION should expose the bounded covering index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_strict_like_prefix_desc_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one descending guarded filtered-index strict prefix
        // projection so the generated SQL lane reaches the reverse bounded route.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
            "query descending CustomerAccount filtered strict LIKE prefix projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the guarded
        // reverse bounded window on the CustomerAccount filtered index.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "name"],
            &[&[ANY_PROJECTION_VALUE, "bravo"]],
            "descending CustomerAccount filtered strict LIKE prefix projection should expose the reverse bounded filtered window",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_equivalent_desc_strict_prefix_forms_match_customer_account_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute the three accepted descending guarded strict prefix
        // spellings against the same reverse CustomerAccount projection window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
            "descending CustomerAccount filtered strict LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerAccount WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name DESC, id DESC LIMIT 1",
            "descending CustomerAccount filtered direct STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name DESC, id DESC LIMIT 1",
            "descending CustomerAccount filtered strict text-range predicate should return projected rows",
        );

        // Phase 2: keep the reverse canister query lane pinned to one shared
        // filtered result set across the equivalent strict prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "descending CustomerAccount filtered direct STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "descending CustomerAccount filtered strict text-range and LIKE prefix query rows should stay in parity",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_strict_like_prefix_desc_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // filtered strict-prefix CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
        )
        .expect(
            "query descending CustomerAccount filtered strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse bounded index-range and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "descending CustomerAccount filtered strict LIKE prefix EXPLAIN EXECUTION should expose the reverse bounded covering index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_rejects_grouped_sql_execution() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let err = query_result(
            pic,
            canister_id,
            "SELECT age, COUNT(*) FROM Customer GROUP BY age",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN SELECT age, COUNT(*) FROM Customer GROUP BY age",
        )
        .expect("query grouped EXPLAIN should return an Ok payload");
        assert_explain_route(
            payload,
            "Customer",
            &[],
            &[],
            "grouped EXPLAIN should return a Customer explain payload",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_delete_direct_starts_with_family_matches_like_output() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: compare the accepted direct family against the established
        // LIKE delete explain outputs on the generated query surface.
        let cases = [
            (
                "EXPLAIN DELETE FROM Customer WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 1",
                "EXPLAIN DELETE FROM Customer WHERE name LIKE 'a%' ORDER BY id LIMIT 1",
                "generated strict direct STARTS_WITH delete explain",
            ),
            (
                "EXPLAIN DELETE FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 1",
                "EXPLAIN DELETE FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 1",
                "generated direct LOWER(field) STARTS_WITH delete explain",
            ),
            (
                "EXPLAIN DELETE FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 1",
                "EXPLAIN DELETE FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 1",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN DELETE FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
        )
        .expect("generated direct UPPER(field) ordered text-range delete EXPLAIN should succeed");
        assert_explain_route(
            payload,
            "Customer",
            &[
                "mode=Delete",
                "access=IndexRange",
                "Customer|LOWER(name)",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN JSON SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
        )
        .expect("generated direct UPPER(field) ordered text-range JSON EXPLAIN should succeed");
        assert_explain_route(
            payload,
            "Customer",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            (
                "EXPLAIN JSON SELECT id, name FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 2",
                "direct UPPER(field) LIKE JSON explain route",
            ),
            (
                "EXPLAIN JSON SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
                "direct UPPER(field) STARTS_WITH JSON explain route",
            ),
            (
                "EXPLAIN JSON SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
                "direct UPPER(field) ordered text-range JSON explain route",
            ),
        ];

        for (sql, context) in cases {
            let payload = query_result(pic, canister_id, sql)
                .unwrap_or_else(|err| panic!("{context} should return an Ok payload: {err}"));
            assert_explain_route(
                payload,
                "Customer",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN JSON DELETE FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
        )
        .expect("generated direct UPPER(field) ordered text-range JSON delete EXPLAIN should succeed");
        assert_explain_route(
            payload,
            "Customer",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            (
                "EXPLAIN JSON DELETE FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 1",
                "direct UPPER(field) LIKE JSON delete explain route",
            ),
            (
                "EXPLAIN JSON DELETE FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 1",
                "direct UPPER(field) STARTS_WITH JSON delete explain route",
            ),
            (
                "EXPLAIN JSON DELETE FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
                "direct UPPER(field) ordered text-range JSON delete explain route",
            ),
        ];

        for (sql, context) in cases {
            let payload = query_result(pic, canister_id, sql)
                .unwrap_or_else(|err| panic!("{context} should return an Ok payload: {err}"));
            assert_explain_route(
                payload,
                "Customer",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let err = query_result(
            pic,
            canister_id,
            "EXPLAIN DELETE FROM Customer WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 1",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let err = query_result(
            pic,
            canister_id,
            "EXPLAIN JSON DELETE FROM Customer WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 1",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
            "query strict LIKE prefix predicate should return projected CustomerOrder rows",
        );

        assert_eq!(rows.entity, "CustomerOrder");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0][1], "A-100".to_string());
        assert_eq!(rows.rows[1][1], "A-101".to_string());
    });
}

#[test]
fn sql_canister_query_lane_supports_strict_like_prefix_desc_predicate() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
            "query descending strict LIKE prefix predicate should return projected CustomerOrder rows",
        );

        assert_eq!(rows.entity, "CustomerOrder");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0][1], "A-102".to_string());
        assert_eq!(rows.rows[1][1], "A-101".to_string());
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_composite_strict_like_prefix_projection()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one guarded composite filtered strict-prefix
        // projection so the generated SQL lane reaches the equality-prefix
        // plus bounded-suffix route.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
            "query CustomerAccount filtered composite strict LIKE prefix projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the bounded suffix
        // window on the composite filtered CustomerAccount index.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
            ],
            "CustomerAccount filtered composite strict LIKE prefix projection should expose the bounded composite window",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_composite_equivalent_strict_prefix_forms_match_customer_account_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute the three accepted guarded composite strict prefix
        // spellings against the same equality-prefix CustomerAccount projection window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
            "CustomerAccount filtered composite strict LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle ASC, id ASC LIMIT 2",
            "CustomerAccount filtered composite direct STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle ASC, id ASC LIMIT 2",
            "CustomerAccount filtered composite strict text-range predicate should return projected rows",
        );

        // Phase 2: keep the canister query lane pinned to one shared composite
        // filtered result set across the equivalent strict prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "CustomerAccount filtered composite direct STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "CustomerAccount filtered composite strict text-range and LIKE prefix query rows should stay in parity",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_strict_like_prefix_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded composite
        // filtered strict-prefix CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerAccount filtered composite strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // composite index-range and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "CustomerAccount filtered composite strict LIKE prefix EXPLAIN EXECUTION should expose the bounded covering index-range route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_composite_strict_like_prefix_desc_projection()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one descending guarded composite filtered
        // strict-prefix projection so the generated SQL lane reaches the
        // reverse equality-prefix plus bounded-suffix route.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
            "query descending CustomerAccount filtered composite strict LIKE prefix projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the reverse
        // bounded suffix window on the composite filtered CustomerAccount index.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
            ],
            "descending CustomerAccount filtered composite strict LIKE prefix projection should expose the reverse bounded composite window",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_composite_equivalent_desc_strict_prefix_forms_match_customer_account_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute the three accepted descending guarded composite
        // strict prefix spellings against the same reverse equality-prefix window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
            "descending CustomerAccount filtered composite strict LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle DESC, id DESC LIMIT 2",
            "descending CustomerAccount filtered composite direct STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle DESC, id DESC LIMIT 2",
            "descending CustomerAccount filtered composite strict text-range predicate should return projected rows",
        );

        // Phase 2: keep the reverse canister query lane pinned to one shared
        // composite filtered result set across the equivalent strict prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "descending CustomerAccount filtered composite direct STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "descending CustomerAccount filtered composite strict text-range and LIKE prefix query rows should stay in parity",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_strict_like_prefix_desc_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // composite filtered strict-prefix CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerAccount filtered composite strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse composite index-range and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "descending CustomerAccount filtered composite strict LIKE prefix EXPLAIN EXECUTION should expose the reverse bounded covering index-range route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_composite_order_only_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one guarded composite filtered order-only
        // projection so the generated SQL lane reaches the equality-prefix
        // suffix-order route without an extra bounded text predicate.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
            "query CustomerAccount filtered composite order-only projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the ordered
        // equality-prefix window on the composite filtered CustomerAccount index.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
            ],
            "CustomerAccount filtered composite order-only projection should expose the ordered equality-prefix window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_order_only_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded composite
        // filtered order-only CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerAccount filtered composite order-only EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // composite index-prefix and covering-read labels from the shared descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "CustomerAccount filtered composite order-only EXPLAIN EXECUTION should expose the covering index-prefix route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_composite_order_only_desc_projection()
{
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one descending guarded composite filtered
        // order-only projection so reverse suffix traversal stays pinned.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
            "query descending CustomerAccount filtered composite order-only projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the reverse
        // ordered equality-prefix window on the composite filtered index.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
            ],
            "descending CustomerAccount filtered composite order-only projection should expose the reverse equality-prefix window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_order_only_desc_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // composite filtered order-only CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerAccount filtered composite order-only EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse composite index-prefix and covering-read labels while
        // failing closed to a materialized sort on the non-unique suffix.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "descending CustomerAccount filtered composite order-only EXPLAIN EXECUTION should expose the reverse covering index-prefix route with one equality prefix and a fail-closed materialized sort without TopN",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_composite_order_only_desc_offset_projection()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one descending guarded composite filtered offset
        // projection so the materialized-boundary route stays pinned on the
        // existing equality-prefix index path.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
            "query descending CustomerAccount filtered composite order-only offset projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the reverse
        // equality-prefix window while honoring the retained offset.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "tier", "handle"],
            &[&[ANY_PROJECTION_VALUE, "gold", "bravo"]],
            "descending CustomerAccount filtered composite order-only offset projection should expose the retained one-row offset window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_order_only_desc_offset_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // composite filtered offset order-only CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
        )
        .expect(
            "query descending CustomerAccount filtered composite order-only offset EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane keeps the index-prefix
        // route, stays on the materialized boundary, and suppresses Top-N.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "descending CustomerAccount filtered composite order-only offset EXPLAIN EXECUTION should expose the materialized-boundary index-prefix route without TopN",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_composite_desc_residual_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one descending guarded composite residual
        // projection so the generated SQL lane proves the `tier, handle` route
        // still owns ordering while `name >= 'a'` remains residual.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND name >= 'a' ORDER BY handle DESC, id DESC LIMIT 2",
            "query descending CustomerAccount filtered composite residual projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the reverse
        // equality-prefix window while preserving the residual filter result.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
            ],
            "descending CustomerAccount filtered composite residual projection should preserve the reverse equality-prefix window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_desc_residual_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // composite residual CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND name >= 'a' ORDER BY handle DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerAccount filtered composite residual EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane fails closed to the
        // materialized residual route and suppresses Top-N.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "descending CustomerAccount filtered composite residual EXPLAIN EXECUTION should expose the fail-closed materialized residual route without TopN",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_expression_order_only_projection() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one filtered expression-order CustomerAccount
        // projection so the generated SQL lane proves the guarded
        // `LOWER(handle)` secondary order path.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "query CustomerAccount filtered expression-order projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the expected
        // guarded `LOWER(handle)` window and column order.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "bravo"],
                &[ANY_PROJECTION_VALUE, "Brisk"],
            ],
            "CustomerAccount filtered expression order-only projection should expose the guarded LOWER(handle) window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_expression_order_only_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded filtered
        // expression-order CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerAccount filtered expression-order EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // expression index-range and materialized route labels from the shared descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "CustomerAccount filtered expression-order EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_expression_order_only_desc_projection()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one descending filtered expression-order
        // CustomerAccount projection so reverse traversal stays locked in the
        // generated SQL harness.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "query descending CustomerAccount filtered expression-order projection should return projected rows",
        );

        // Phase 2: assert the generated query surface returns the expected
        // descending guarded `LOWER(handle)` window.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "bristle"],
                &[ANY_PROJECTION_VALUE, "Brisk"],
            ],
            "descending CustomerAccount filtered expression order-only projection should expose the reverse LOWER(handle) window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_expression_order_only_desc_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending
        // filtered expression-order CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerAccount filtered expression-order EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse expression index-range and materialized labels from the
        // shared execution descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "descending CustomerAccount filtered expression-order EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_expression_equivalent_prefix_forms_match_customer_account_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute the accepted guarded expression prefix spellings
        // against the same ordered CustomerAccount projection window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "CustomerAccount filtered expression LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "CustomerAccount filtered expression STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "CustomerAccount filtered expression text-range predicate should return projected rows",
        );

        // Phase 2: keep the canister query lane pinned to one shared filtered
        // result set across the equivalent expression prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "CustomerAccount filtered expression STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "CustomerAccount filtered expression text-range and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(like_rows.entity, "CustomerAccount");
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
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_expression_strict_like_prefix_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded filtered
        // expression strict-prefix CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerAccount filtered expression strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // expression index-range and materialized route labels from the shared descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "CustomerAccount filtered expression strict LIKE prefix EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_expression_strict_text_range_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerAccount filtered expression strict text-range EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "CustomerAccount filtered expression strict text-range EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_expression_equivalent_desc_prefix_forms_match_customer_account_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute the accepted descending guarded expression prefix
        // spellings against the same reverse CustomerAccount projection window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "descending CustomerAccount filtered expression LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "descending CustomerAccount filtered expression STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "descending CustomerAccount filtered expression text-range predicate should return projected rows",
        );

        // Phase 2: keep the reverse canister query lane pinned to one shared
        // filtered result set across the equivalent expression prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "descending CustomerAccount filtered expression STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "descending CustomerAccount filtered expression text-range and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(like_rows.entity, "CustomerAccount");
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
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_expression_strict_like_prefix_desc_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending
        // filtered expression strict-prefix CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerAccount filtered expression strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse expression index-range and materialized labels from the shared
        // execution descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "descending CustomerAccount filtered expression strict LIKE prefix EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_expression_strict_text_range_desc_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerAccount filtered expression strict text-range EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "descending CustomerAccount filtered expression strict text-range EXPLAIN EXECUTION should expose the index-range materialized route",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_composite_expression_order_only_projection()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one guarded composite expression order-only
        // projection so the generated SQL lane proves the equality-prefix
        // `tier, LOWER(handle)` route.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "query CustomerAccount filtered composite expression order-only projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the canonical
        // guarded `LOWER(handle)` suffix window on the gold tier.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
            ],
            "CustomerAccount filtered composite expression order-only projection should expose the guarded LOWER(handle) suffix window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_expression_order_only_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded composite
        // expression order-only CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerAccount filtered composite expression order-only EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // index-prefix and materialized route labels from the shared descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "CustomerAccount filtered composite expression order-only EXPLAIN EXECUTION should expose the materialized index-prefix route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_composite_expression_key_only_order_only_projection()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "query CustomerAccount filtered composite expression key-only order-only projection should return projected rows",
        );

        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "tier"],
            &[
                &[ANY_PROJECTION_VALUE, "gold"],
                &[ANY_PROJECTION_VALUE, "gold"],
            ],
            "CustomerAccount filtered composite expression key-only order-only projection should expose the guarded covering window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_expression_key_only_order_only_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerAccount filtered composite expression key-only order-only EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "CustomerAccount filtered composite expression key-only order-only EXPLAIN EXECUTION should expose the witness-backed covering route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_expression_key_only_order_only_desc_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerAccount filtered composite expression key-only order-only EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "descending CustomerAccount filtered composite expression key-only order-only EXPLAIN EXECUTION should expose the witness-backed covering route with one equality prefix and a fail-closed materialized sort",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_composite_expression_key_only_strict_text_range_projection()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "query CustomerAccount filtered composite expression key-only strict text-range projection should return projected rows",
        );

        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "tier"],
            &[
                &[ANY_PROJECTION_VALUE, "gold"],
                &[ANY_PROJECTION_VALUE, "gold"],
            ],
            "CustomerAccount filtered composite expression key-only strict text-range projection should expose the guarded covering window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_expression_key_only_strict_text_range_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerAccount filtered composite expression key-only strict text-range EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "CustomerAccount filtered composite expression key-only strict text-range EXPLAIN EXECUTION should expose the witness-backed covering route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_composite_expression_key_only_strict_text_range_desc_projection()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "query descending CustomerAccount filtered composite expression key-only strict text-range projection should return projected rows",
        );

        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "tier"],
            &[
                &[ANY_PROJECTION_VALUE, "gold"],
                &[ANY_PROJECTION_VALUE, "gold"],
            ],
            "descending CustomerAccount filtered composite expression key-only strict text-range projection should expose the guarded covering window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_expression_key_only_strict_text_range_desc_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerAccount filtered composite expression key-only strict text-range EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "descending CustomerAccount filtered composite expression key-only strict text-range EXPLAIN EXECUTION should expose the witness-backed covering route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_composite_expression_key_only_equivalent_direct_prefix_forms_match_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "query CustomerAccount filtered composite expression key-only LIKE prefix projection should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "query CustomerAccount filtered composite expression key-only STARTS_WITH projection should return projected rows",
        );

        assert_eq!(
            starts_with_rows, like_rows,
            "CustomerAccount filtered composite expression key-only STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_projection_window(
            &like_rows,
            "CustomerAccount",
            &["id", "tier"],
            &[
                &[ANY_PROJECTION_VALUE, "gold"],
                &[ANY_PROJECTION_VALUE, "gold"],
            ],
            "CustomerAccount filtered composite expression key-only direct prefix projection should expose the guarded covering window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_expression_key_only_direct_starts_with_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerAccount filtered composite expression key-only direct STARTS_WITH EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "CustomerAccount filtered composite expression key-only direct STARTS_WITH EXPLAIN EXECUTION should expose the witness-backed covering route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_customer_account_filtered_composite_expression_order_only_desc_projection()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute one descending guarded composite expression
        // order-only projection so reverse `LOWER(handle)` traversal stays pinned.
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "query descending CustomerAccount filtered composite expression order-only projection should return projected rows",
        );

        // Phase 2: assert the generated query surface keeps the reverse
        // guarded `LOWER(handle)` suffix window on the gold tier.
        assert_projection_window(
            &rows,
            "CustomerAccount",
            &["id", "tier", "handle"],
            &[
                &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                &[ANY_PROJECTION_VALUE, "gold", "bravo"],
            ],
            "descending CustomerAccount filtered composite expression order-only projection should expose the reverse LOWER(handle) suffix window",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_expression_order_only_desc_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // composite expression order-only CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerAccount filtered composite expression order-only EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse index-prefix and materialized labels while failing closed
        // to a materialized sort on the non-unique suffix.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "descending CustomerAccount filtered composite expression order-only EXPLAIN EXECUTION should expose the reverse materialized index-prefix route with one equality prefix and a fail-closed materialized sort",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_composite_expression_equivalent_strict_prefix_forms_match_customer_account_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute the accepted guarded composite expression prefix
        // spellings against the same equality-prefix CustomerAccount window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "CustomerAccount filtered composite expression LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "CustomerAccount filtered composite expression STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "CustomerAccount filtered composite expression text-range predicate should return projected rows",
        );

        // Phase 2: keep the canister query lane pinned to one shared composite
        // expression result set across the equivalent strict prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "CustomerAccount filtered composite expression STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "CustomerAccount filtered composite expression text-range and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(like_rows.entity, "CustomerAccount");
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
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_expression_strict_like_prefix_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the guarded composite
        // expression strict-prefix CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerAccount filtered composite expression strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // index-range and materialized route labels from the shared descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "CustomerAccount filtered composite expression strict LIKE prefix EXPLAIN EXECUTION should expose the materialized index-range route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_expression_strict_text_range_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect(
            "query CustomerAccount filtered composite expression strict text-range EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "CustomerAccount filtered composite expression strict text-range EXPLAIN EXECUTION should expose the materialized index-range route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_composite_expression_equivalent_desc_strict_prefix_forms_match_customer_account_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: execute the accepted descending guarded composite
        // expression prefix spellings against the same reverse equality-prefix window.
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "descending CustomerAccount filtered composite expression LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "descending CustomerAccount filtered composite expression STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "descending CustomerAccount filtered composite expression text-range predicate should return projected rows",
        );

        // Phase 2: keep the reverse canister query lane pinned to one shared
        // composite expression result set across the equivalent prefix spellings.
        assert_eq!(
            starts_with_rows, like_rows,
            "descending CustomerAccount filtered composite expression STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "descending CustomerAccount filtered composite expression text-range and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(like_rows.entity, "CustomerAccount");
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
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_expression_strict_like_prefix_desc_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: request one execution descriptor for the descending guarded
        // composite expression strict-prefix CustomerAccount projection shape.
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerAccount filtered composite expression strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload",
        );
        // Phase 2: assert the generated query lane preserves the stable
        // reverse index-range and materialized labels from the shared descriptor.
        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "descending CustomerAccount filtered composite expression strict LIKE prefix EXPLAIN EXECUTION should expose the reverse materialized index-range route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_customer_account_filtered_composite_expression_strict_text_range_desc_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerAccount filtered composite expression strict text-range EXPLAIN EXECUTION should return an Ok payload",
        );

        assert_explain_route(
            payload,
            "CustomerAccount",
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
            "descending CustomerAccount filtered composite expression strict text-range EXPLAIN EXECUTION should expose the reverse materialized index-range route with one equality prefix",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_strict_text_range_predicate() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
            "query strict text-range predicate should return projected CustomerOrder rows",
        );

        assert_eq!(rows.entity, "CustomerOrder");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0][1], "A-100".to_string());
        assert_eq!(rows.rows[1][1], "A-101".to_string());
    });
}

#[test]
fn sql_canister_query_lane_supports_strict_text_range_desc_predicate() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
            "query descending strict text-range predicate should return projected CustomerOrder rows",
        );

        assert_eq!(rows.entity, "CustomerOrder");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 2);
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0][1], "A-102".to_string());
        assert_eq!(rows.rows[1][1], "A-101".to_string());
    });
}

#[test]
fn sql_canister_query_lane_equivalent_strict_prefix_forms_match_customer_order_projection_rows() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
            "CustomerOrder strict LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2",
            "CustomerOrder direct STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
            "CustomerOrder strict text-range predicate should return projected rows",
        );

        assert_eq!(
            starts_with_rows, like_rows,
            "CustomerOrder direct STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "CustomerOrder strict text-range and LIKE prefix query rows should stay in parity",
        );
    });
}

#[test]
fn sql_canister_query_lane_equivalent_desc_strict_prefix_forms_match_customer_order_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
            "descending CustomerOrder strict LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2",
            "descending CustomerOrder direct STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
            "descending CustomerOrder strict text-range predicate should return projected rows",
        );

        assert_eq!(
            starts_with_rows, like_rows,
            "descending CustomerOrder direct STARTS_WITH and LIKE prefix query rows should stay in parity",
        );
        assert_eq!(
            range_rows, like_rows,
            "descending CustomerOrder strict text-range and LIKE prefix query rows should stay in parity",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_direct_starts_with_predicate() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 2",
            "query direct STARTS_WITH predicate should return projected rows",
        );

        assert_eq!(rows.entity, "Customer");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][1], "alice".to_string());
    });
}

#[test]
fn sql_canister_query_lane_supports_direct_lower_starts_with_predicate() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
            "query direct LOWER(field) STARTS_WITH predicate should return projected rows",
        );

        assert_eq!(rows.entity, "Customer");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][1], "alice".to_string());
    });
}

#[test]
fn sql_canister_query_lane_supports_direct_lower_strict_text_range_predicate() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
            "query direct LOWER(field) ordered text-range predicate should return projected rows",
        );

        assert_eq!(rows.entity, "Customer");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][1], "alice".to_string());
    });
}

#[test]
fn sql_canister_query_lane_equivalent_direct_lower_prefix_forms_match_projection_rows() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 2",
            "query direct LOWER(field) LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
            "query direct LOWER(field) STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
        )
        .expect(
            "query direct LOWER(field) ordered text-range EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            (
                "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 2",
                "direct LOWER(field) LIKE prefix EXPLAIN EXECUTION route",
            ),
            (
                "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
                "direct LOWER(field) STARTS_WITH EXPLAIN EXECUTION route",
            ),
            (
                "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
                "direct LOWER(field) ordered text-range EXPLAIN EXECUTION route",
            ),
        ];

        for (sql, context) in cases {
            let payload = query_result(pic, canister_id, sql)
                .unwrap_or_else(|err| panic!("{context} should return an Ok payload: {err}"));
            assert_explain_route(
                payload,
                "Customer",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN DELETE FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
        )
        .expect("generated direct LOWER(field) ordered text-range delete EXPLAIN should succeed");
        assert_explain_route(
            payload,
            "Customer",
            &[
                "mode=Delete",
                "access=IndexRange",
                "Customer|LOWER(name)",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN JSON SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
        )
        .expect("generated direct LOWER(field) ordered text-range JSON EXPLAIN should succeed");
        assert_explain_route(
            payload,
            "Customer",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            (
                "EXPLAIN JSON SELECT id, name FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 2",
                "direct LOWER(field) LIKE JSON explain route",
            ),
            (
                "EXPLAIN JSON SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
                "direct LOWER(field) STARTS_WITH JSON explain route",
            ),
            (
                "EXPLAIN JSON SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
                "direct LOWER(field) ordered text-range JSON explain route",
            ),
        ];

        for (sql, context) in cases {
            let payload = query_result(pic, canister_id, sql)
                .unwrap_or_else(|err| panic!("{context} should return an Ok payload: {err}"));
            assert_explain_route(
                payload,
                "Customer",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN JSON DELETE FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
        )
        .expect("generated direct LOWER(field) ordered text-range JSON delete EXPLAIN should succeed");
        assert_explain_route(
            payload,
            "Customer",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            (
                "EXPLAIN JSON DELETE FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 1",
                "direct LOWER(field) LIKE JSON delete explain route",
            ),
            (
                "EXPLAIN JSON DELETE FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 1",
                "direct LOWER(field) STARTS_WITH JSON delete explain route",
            ),
            (
                "EXPLAIN JSON DELETE FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
                "direct LOWER(field) ordered text-range JSON delete explain route",
            ),
        ];

        for (sql, context) in cases {
            let payload = query_result(pic, canister_id, sql)
                .unwrap_or_else(|err| panic!("{context} should return an Ok payload: {err}"));
            assert_explain_route(
                payload,
                "Customer",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
            "query direct UPPER(field) STARTS_WITH predicate should return projected rows",
        );

        assert_eq!(rows.entity, "Customer");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][1], "alice".to_string());
    });
}

#[test]
fn sql_canister_query_lane_supports_direct_upper_strict_text_range_predicate() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
            "query direct UPPER(field) ordered text-range predicate should return projected rows",
        );

        assert_eq!(rows.entity, "Customer");
        assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.row_count, 1);
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][1], "alice".to_string());
    });
}

#[test]
fn sql_canister_query_lane_equivalent_direct_upper_prefix_forms_match_projection_rows() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let like_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 2",
            "query direct UPPER(field) LIKE prefix predicate should return projected rows",
        );
        let starts_with_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
            "query direct UPPER(field) STARTS_WITH predicate should return projected rows",
        );
        let range_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
        )
        .expect(
            "query direct UPPER(field) ordered text-range EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            (
                "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 2",
                "direct UPPER(field) LIKE prefix EXPLAIN EXECUTION route",
            ),
            (
                "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
                "direct UPPER(field) STARTS_WITH EXPLAIN EXECUTION route",
            ),
            (
                "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
                "direct UPPER(field) ordered text-range EXPLAIN EXECUTION route",
            ),
        ];

        for (sql, context) in cases {
            let payload = query_result(pic, canister_id, sql)
                .unwrap_or_else(|err| panic!("{context} should return an Ok payload: {err}"));
            assert_explain_route(
                payload,
                "Customer",
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("query CustomerOrder strict LIKE prefix EXPLAIN EXECUTION should return an Ok payload");
        assert_explain_route(
            payload,
            "CustomerOrder",
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
            "CustomerOrder strict LIKE prefix EXPLAIN EXECUTION should expose the bounded covering index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_strict_text_range_covering_route() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("query CustomerOrder strict text-range EXPLAIN EXECUTION should return an Ok payload");
        assert_explain_route(
            payload,
            "CustomerOrder",
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
            "CustomerOrder strict text-range EXPLAIN EXECUTION should expose the bounded covering index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_strict_text_range_desc_covering_route() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
        )
        .expect(
            "query descending CustomerOrder strict text-range EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "CustomerOrder",
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
            "descending CustomerOrder strict text-range EXPLAIN EXECUTION should expose the bounded reverse covering index-range route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_equivalent_strict_prefix_forms_preserve_customer_order_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let explains = [
            (
                "strict LIKE prefix",
                query_explain_text(
                    pic,
                    canister_id,
                    "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
                    "CustomerOrder strict LIKE prefix EXPLAIN EXECUTION should return Explain payload",
                ),
            ),
            (
                "direct STARTS_WITH",
                query_explain_text(
                    pic,
                    canister_id,
                    "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2",
                    "CustomerOrder direct STARTS_WITH EXPLAIN EXECUTION should return Explain payload",
                ),
            ),
            (
                "strict text range",
                query_explain_text(
                    pic,
                    canister_id,
                    "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
                    "CustomerOrder strict text-range EXPLAIN EXECUTION should return Explain payload",
                ),
            ),
        ];

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
fn sql_canister_query_lane_explain_execution_equivalent_desc_strict_prefix_forms_preserve_customer_order_covering_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let explains = [
            (
                "descending strict LIKE prefix",
                query_explain_text(
                    pic,
                    canister_id,
                    "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
                    "descending CustomerOrder strict LIKE prefix EXPLAIN EXECUTION should return Explain payload",
                ),
            ),
            (
                "descending direct STARTS_WITH",
                query_explain_text(
                    pic,
                    canister_id,
                    "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2",
                    "descending CustomerOrder direct STARTS_WITH EXPLAIN EXECUTION should return Explain payload",
                ),
            ),
            (
                "descending strict text range",
                query_explain_text(
                    pic,
                    canister_id,
                    "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
                    "descending CustomerOrder strict text-range EXPLAIN EXECUTION should return Explain payload",
                ),
            ),
        ];

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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let err = query_result(
            pic,
            canister_id,
            "SELECT id, name FROM Customer WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 2",
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
                    sql: "SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.projection.user_name_eq_limit.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id DESC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.projection.user_name_eq_limit",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.projection.user_name_eq_limit.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id DESC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.primary_key_covering.user_id_limit1",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id FROM Customer ORDER BY id ASC LIMIT 1".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.primary_key_covering.user_id_limit1",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id FROM Customer ORDER BY id ASC LIMIT 1".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.secondary_covering.user_name_order_only_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.secondary_covering.user_name_order_only_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Customer ORDER BY name DESC, id DESC LIMIT 2"
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
                    sql: "SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2"
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
                    sql: "SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.secondary_covering.user_name_order_only_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.secondary_covering.user_name_order_only_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id, name FROM Customer ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.secondary_covering.user_name_strict_range_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.secondary_covering.user_name_strict_range_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.query_from_sql.execute.scalar_limit",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedQueryFromSqlCustomerExecute,
                    sql: "SELECT * FROM Customer ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql.scalar_limit",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlCustomer,
                    sql: "SELECT * FROM Customer ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.describe.user",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "DESCRIBE Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.explain.user_name_eq_limit",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql:
                        "EXPLAIN SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 1"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.explain.grouped.user_age_count",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "EXPLAIN SELECT age, COUNT(*) FROM Customer GROUP BY age ORDER BY age ASC LIMIT 10"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.explain.aggregate.user_count",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "EXPLAIN SELECT COUNT(*) FROM Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_age_count",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql: "SELECT age, COUNT(*) FROM Customer GROUP BY age ORDER BY age ASC LIMIT 10"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_age_count.having_empty",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql:
                        "SELECT age, COUNT(*) FROM Customer GROUP BY age HAVING COUNT(*) > 1000 ORDER BY age ASC LIMIT 10"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_age_count.limit2.first_page",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql: "SELECT age, COUNT(*) FROM Customer GROUP BY age ORDER BY age ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_age_count.limit2.second_page",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomerSecondPage,
                    sql: "SELECT age, COUNT(*) FROM Customer GROUP BY age ORDER BY age ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_age_count.invalid_cursor",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql: "SELECT age, COUNT(*) FROM Customer GROUP BY age ORDER BY age ASC LIMIT 2"
                        .to_string(),
                    cursor_token: Some("zz".to_string()),
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_count",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
                    sql: "SELECT COUNT(*) FROM Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_count_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
                    sql: "SELECT COUNT(age) FROM Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_min_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
                    sql: "SELECT MIN(age) FROM Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_max_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
                    sql: "SELECT MAX(age) FROM Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_sum_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
                    sql: "SELECT SUM(age) FROM Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_avg_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
                    sql: "SELECT AVG(age) FROM Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert.user_single",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertCustomer,
                    sql: "INSERT Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert_many_atomic.user_10",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertManyAtomicCustomer10,
                    sql: "INSERT MANY Customer ATOMIC x10".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert_many_atomic.user_100",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertManyAtomicCustomer100,
                    sql: "INSERT MANY Customer ATOMIC x100".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert_many_atomic.user_1000",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertManyAtomicCustomer1000,
                    sql: "INSERT MANY Customer ATOMIC x1000".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert_many_non_atomic.user_10",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertManyNonAtomicCustomer10,
                    sql: "INSERT MANY Customer NON_ATOMIC x10".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert_many_non_atomic.user_100",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertManyNonAtomicCustomer100,
                    sql: "INSERT MANY Customer NON_ATOMIC x100".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.insert_many_non_atomic.user_1000",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedInsertManyNonAtomicCustomer1000,
                    sql: "INSERT MANY Customer NON_ATOMIC x1000".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.update.user_single",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedUpdateCustomer,
                    sql: "UPDATE Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.delete.user_order_id_limit1.count",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentDeleteCustomerByIdLimit1Count,
                    sql: "DELETE FROM Customer ORDER BY id LIMIT 1".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.show_indexes.user",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SHOW INDEXES Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.show_columns.user",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SHOW COLUMNS Customer".to_string(),
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
                    sql: "SELECT LOWER(name) FROM Customer ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.predicate.starts_with_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Customer WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.predicate.lower_starts_with_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.predicate.upper_starts_with_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.predicate.upper_strict_range_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.predicate.lower_strict_range_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.computed_projection.lower_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT LOWER(name) FROM Customer ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.predicate.starts_with_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id, name FROM Customer WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.predicate.lower_starts_with_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.predicate.upper_starts_with_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.predicate.upper_strict_range_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.predicate.lower_strict_range_name_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.customer_account_filtered_order_only_name_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.customer_account_filtered_order_only_name_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.customer_account_filtered_strict_like_name_limit1.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_direct_starts_with_name_limit1.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name ASC, id ASC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.customer_account_filtered_strict_range_name_limit1.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name ASC, id ASC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.customer_account_filtered_strict_like_name_limit1.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_direct_starts_with_name_limit1.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name DESC, id DESC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.customer_account_filtered_strict_range_name_limit1.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name DESC, id DESC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_order_only_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_strict_like_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_direct_starts_with_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_strict_range_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_order_only_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_strict_like_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_direct_starts_with_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_strict_range_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_expression_order_only_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_expression_key_only_order_only_tier_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_expression_key_only_order_only_tier_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_expression_key_only_strict_range_tier_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_expression_key_only_direct_starts_with_tier_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_expression_strict_like_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_expression_direct_starts_with_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_expression_strict_range_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_expression_order_only_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_expression_strict_like_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_expression_direct_starts_with_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_account_filtered_composite_expression_strict_range_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.customer_account_filtered_order_only_name_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.customer_account_filtered_order_only_name_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_account_filtered_composite_order_only_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_account_filtered_composite_order_only_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_account_filtered_composite_expression_order_only_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_account_filtered_composite_expression_key_only_order_only_tier_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_account_filtered_composite_expression_key_only_order_only_tier_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_account_filtered_composite_expression_key_only_strict_range_tier_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_account_filtered_composite_expression_key_only_direct_starts_with_tier_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_account_filtered_composite_expression_direct_starts_with_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_account_filtered_composite_expression_strict_range_handle_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_account_filtered_composite_expression_order_only_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_account_filtered_composite_expression_direct_starts_with_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_account_filtered_composite_expression_strict_range_handle_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.customer_order_strict_like_name_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.customer_order_strict_like_name_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.customer_order_direct_starts_with_name_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.customer_order_direct_starts_with_name_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.customer_order_strict_range_name_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.customer_order_strict_range_name_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.user_expression_order.lower_name_id_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.user_expression_order.lower_name_id_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, name FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.user_expression_order.lower_name_id_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id, name FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.user_expression_order.lower_name_id_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT id, name FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_order_order_only_composite.priority_status_id_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_order_order_only_composite.priority_status_id_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_order_numeric_equality.priority_eq20_status_id_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_order_numeric_equality.priority_eq20_status_id_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_order_numeric_equality_bounded_status.priority_eq20_status_bd_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "generated.dispatch.customer_order_numeric_equality_bounded_status.priority_eq20_status_bd_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.customer_order_order_only_composite.priority_status_id_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerOrder,
                    sql: "SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.customer_order_order_only_composite.priority_status_id_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerOrder,
                    sql: "SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_order_numeric_equality.priority_eq20_status_id_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerOrder,
                    sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_order_numeric_equality.priority_eq20_status_id_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerOrder,
                    sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_order_numeric_equality_bounded_status.priority_eq20_status_bd_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerOrder,
                    sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key:
                    "typed.dispatch.customer_order_numeric_equality_bounded_status.priority_eq20_status_bd_limit2.desc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerOrder,
                    sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status DESC, id DESC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.load.user_order_id_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentLoadCustomerByIdLimit2,
                    sql: "SELECT * FROM Customer ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.load.user_name_eq_limit1",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentLoadCustomerNameEqLimit1,
                    sql: "SELECT * FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 1".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.paged.user_order_id_limit2.first_page",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentPagedCustomerByIdLimit2FirstPage,
                    sql: "SELECT * FROM Customer ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.paged.user_order_id_limit2.second_page",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentPagedCustomerByIdLimit2SecondPage,
                    sql: "SELECT * FROM Customer ORDER BY id LIMIT 2".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.paged.user_order_id_limit2.invalid_cursor",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentPagedCustomerByIdLimit2InvalidCursor,
                    sql: "SELECT * FROM Customer ORDER BY id LIMIT 2".to_string(),
                    cursor_token: Some("zz".to_string()),
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.explain_delete",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "EXPLAIN DELETE FROM Customer ORDER BY id LIMIT 1".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "generated.dispatch.delete",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "DELETE FROM Customer ORDER BY id LIMIT 1".to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.delete",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "DELETE FROM Customer ORDER BY id LIMIT 1".to_string(),
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
    run_with_pic(|pic| {
        let mut rows = Vec::new();

        // Phase 1: measure the representative non-Customer ordered covering cohort
        // through both generated dispatch and the matching typed dispatch lane.
        for (
            scenario_key,
            fixture_canister,
            sql,
            typed_surface,
            expected_entity,
            expected_row_count,
        ) in NON_USER_ORDERED_COVERING_PERF_CASES.iter().copied()
        {
            let canister_id = install_fixture_canister(pic, fixture_canister);
            load_default_fixtures(pic, canister_id);

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
                .expect("non-Customer ordered covering perf rows should serialize to JSON")
        );
    });
}

#[test]
fn sql_canister_perf_customer_name_order_keeps_row_check_metrics_zero_in_parity() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sql = "SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2".to_string();
        let generated = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: sql.clone(),
                cursor_token: None,
                repeat_count: 1,
            },
        );
        let typed = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::TypedDispatchCustomer,
                sql,
                cursor_token: None,
                repeat_count: 1,
            },
        );

        assert!(
            generated.outcome.success,
            "generated Customer name-order perf sample should succeed: {generated:?}",
        );
        assert!(
            typed.outcome.success,
            "typed Customer name-order perf sample should succeed: {typed:?}",
        );
        assert_eq!(
            generated.outcome.row_count,
            Some(2),
            "generated Customer name-order perf sample should return the requested window",
        );
        assert_eq!(
            typed.outcome.row_count,
            Some(2),
            "typed Customer name-order perf sample should return the requested window",
        );

        let generated_metrics = generated
            .outcome
            .row_check_metrics
            .expect("generated Customer name-order perf sample should attach row_check metrics");
        let typed_metrics = typed
            .outcome
            .row_check_metrics
            .expect("typed Customer name-order perf sample should attach row_check metrics");

        assert_eq!(
            generated_metrics.row_check_covering_candidates_seen, 0,
            "generated Customer name-order perf sample should not enter the row_check covering candidate lane on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_count, 0,
            "generated Customer name-order perf sample should not execute row-presence probes on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_hits, 0,
            "generated Customer name-order perf sample should not perform row-presence probes on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_misses, 0,
            "generated Customer name-order perf sample should not report stale-row misses on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_borrowed_data_store_count, 0,
            "generated Customer name-order perf sample should not keep row checks on the borrowed data-store boundary on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_store_handle_count, 0,
            "generated Customer name-order perf sample should not route row checks back through the store-handle helper on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_key_to_raw_encodes, 0,
            "generated Customer name-order perf sample should not encode row-check primary keys on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_check_rows_emitted, 0,
            "generated Customer name-order perf sample should not report row_check-emitted rows on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed Customer name-order perf samples should keep row_check metrics in parity",
        );
    });
}

#[test]
fn sql_canister_perf_customer_name_order_stale_reports_row_check_metrics_in_parity() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        make_customer_name_order_stale(pic, canister_id);

        let sql = "SELECT name FROM Customer ORDER BY name ASC LIMIT 2".to_string();
        let generated = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: sql.clone(),
                cursor_token: None,
                repeat_count: 1,
            },
        );
        let typed = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::TypedDispatchCustomer,
                sql,
                cursor_token: None,
                repeat_count: 1,
            },
        );

        assert!(
            generated.outcome.success,
            "generated stale Customer name-order perf sample should succeed: {generated:?}",
        );
        assert!(
            typed.outcome.success,
            "typed stale Customer name-order perf sample should succeed: {typed:?}",
        );
        assert_eq!(
            generated.outcome.row_count,
            Some(1),
            "generated stale Customer name-order perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );
        assert_eq!(
            typed.outcome.row_count,
            Some(1),
            "typed stale Customer name-order perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );

        let generated_metrics = generated.outcome.row_check_metrics.expect(
            "generated stale Customer name-order perf sample should attach row_check metrics",
        );
        let typed_metrics = typed
            .outcome
            .row_check_metrics
            .expect("typed stale Customer name-order perf sample should attach row_check metrics");

        assert_eq!(
            generated_metrics.row_check_covering_candidates_seen, 2,
            "generated stale Customer name-order perf sample should inspect two secondary candidates before exhausting the requested window",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_count, 2,
            "generated stale Customer name-order perf sample should execute one authoritative probe per decoded secondary candidate",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_hits, 1,
            "generated stale Customer name-order perf sample should find exactly one live row in the scanned window",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_misses, 1,
            "generated stale Customer name-order perf sample should report the missing leading base row",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_borrowed_data_store_count, 2,
            "generated stale Customer name-order perf sample should keep stale-row checks on the borrowed data-store authority boundary",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_store_handle_count, 0,
            "generated stale Customer name-order perf sample should not bounce stale-row checks through the store-handle helper",
        );
        assert_eq!(
            generated_metrics.row_presence_key_to_raw_encodes, 2,
            "generated stale Customer name-order perf sample should encode one authoritative row key per candidate",
        );
        assert_eq!(
            generated_metrics.row_check_rows_emitted, 1,
            "generated stale Customer name-order perf sample should emit exactly one live row after stale-row filtering",
        );
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed stale Customer name-order perf samples should keep row_check metrics in parity",
        );
    });
}

#[test]
#[ignore = "manual stale-row perf probe for before/after measurement runs"]
fn sql_canister_perf_customer_name_order_stale_probe_reports_samples_as_json() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        make_customer_name_order_stale(pic, canister_id);
        let sql = "SELECT name FROM Customer ORDER BY name ASC LIMIT 2".to_string();
        let generated = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: sql.clone(),
                cursor_token: None,
                repeat_count: 5,
            },
        );
        let typed = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::TypedDispatchCustomer,
                sql,
                cursor_token: None,
                repeat_count: 5,
            },
        );

        assert!(
            generated.outcome.success,
            "generated stale Customer name-order perf probe should succeed: {generated:?}",
        );
        assert!(
            typed.outcome.success,
            "typed stale Customer name-order perf probe should succeed: {typed:?}",
        );

        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "mode": "stale_customer_name_order",
                "generated": generated,
                "typed": typed,
            }))
            .expect("stale Customer name-order perf probe should serialize to JSON")
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_user_expression_order_reports_positive_instruction_samples()
{
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the new
        // expression-order Customer covering shape so perf regression checks track
        // the exact canister lane this slice changed.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected Customer projection window.
        assert!(
            sample.first_local_instructions > 0,
            "Customer expression-order first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "Customer expression-order min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "Customer expression-order max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "Customer expression-order total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "Customer expression-order repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "Customer expression-order generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Customer"),
            "Customer expression-order perf sample should stay on the Customer route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "Customer expression-order perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_user_expression_order_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // expression-order Customer covering shape so reverse traversal stays
        // pinned in the checked-in perf suite.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected Customer projection window.
        assert!(
            sample.first_local_instructions > 0,
            "descending Customer expression-order first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending Customer expression-order min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending Customer expression-order max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending Customer expression-order total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending Customer expression-order repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending Customer expression-order generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Customer"),
            "descending Customer expression-order perf sample should stay on the Customer route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending Customer expression-order perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_order_only_composite_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        assert!(
            sample.first_local_instructions > 0,
            "CustomerOrder order-only composite first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "CustomerOrder order-only composite min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "CustomerOrder order-only composite max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "CustomerOrder order-only composite total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "CustomerOrder order-only composite repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "CustomerOrder order-only composite generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "CustomerOrder order-only composite perf sample should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerOrder order-only composite perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_order_only_composite_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        assert!(
            sample.first_local_instructions > 0,
            "descending CustomerOrder order-only composite first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending CustomerOrder order-only composite min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending CustomerOrder order-only composite max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending CustomerOrder order-only composite total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending CustomerOrder order-only composite repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending CustomerOrder order-only composite generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "descending CustomerOrder order-only composite perf sample should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerOrder order-only composite perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_numeric_equality_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        assert!(
            sample.first_local_instructions > 0,
            "CustomerOrder numeric-equality first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "CustomerOrder numeric-equality min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "CustomerOrder numeric-equality max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "CustomerOrder numeric-equality total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "CustomerOrder numeric-equality repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "CustomerOrder numeric-equality generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "CustomerOrder numeric-equality perf sample should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerOrder numeric-equality perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_numeric_equality_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        assert!(
            sample.first_local_instructions > 0,
            "descending CustomerOrder numeric-equality first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending CustomerOrder numeric-equality min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending CustomerOrder numeric-equality max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending CustomerOrder numeric-equality total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending CustomerOrder numeric-equality repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending CustomerOrder numeric-equality generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "descending CustomerOrder numeric-equality perf sample should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerOrder numeric-equality perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_numeric_equality_bounded_status_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        assert!(
            sample.first_local_instructions > 0,
            "CustomerOrder numeric-equality bounded status first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "CustomerOrder numeric-equality bounded status min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "CustomerOrder numeric-equality bounded status max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "CustomerOrder numeric-equality bounded status total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "CustomerOrder numeric-equality bounded status repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "CustomerOrder numeric-equality bounded status generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "CustomerOrder numeric-equality bounded status perf sample should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerOrder numeric-equality bounded status perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_numeric_equality_bounded_status_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        assert!(
            sample.first_local_instructions > 0,
            "descending CustomerOrder numeric-equality bounded status first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending CustomerOrder numeric-equality bounded status min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending CustomerOrder numeric-equality bounded status max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending CustomerOrder numeric-equality bounded status total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending CustomerOrder numeric-equality bounded status repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending CustomerOrder numeric-equality bounded status generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "descending CustomerOrder numeric-equality bounded status perf sample should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerOrder numeric-equality bounded status perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_order_only_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the guarded
        // filtered-index order-only CustomerAccount covering shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected CustomerAccount projection window.
        assert!(
            sample.first_local_instructions > 0,
            "CustomerAccount filtered order-only first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "CustomerAccount filtered order-only min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "CustomerAccount filtered order-only max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "CustomerAccount filtered order-only total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "CustomerAccount filtered order-only repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "CustomerAccount filtered order-only generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "CustomerAccount filtered order-only perf sample should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerAccount filtered order-only perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_order_only_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // guarded filtered-index order-only CustomerAccount covering shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected CustomerAccount window.
        assert!(
            sample.first_local_instructions > 0,
            "descending CustomerAccount filtered order-only first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending CustomerAccount filtered order-only min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending CustomerAccount filtered order-only max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending CustomerAccount filtered order-only total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending CustomerAccount filtered order-only repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending CustomerAccount filtered order-only generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "descending CustomerAccount filtered order-only perf sample should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerAccount filtered order-only perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_strict_like_prefix_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the guarded
        // filtered-index strict LIKE prefix CustomerAccount covering shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected bounded CustomerAccount projection window.
        assert!(
            sample.first_local_instructions > 0,
            "CustomerAccount filtered strict LIKE prefix first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "CustomerAccount filtered strict LIKE prefix min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "CustomerAccount filtered strict LIKE prefix max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "CustomerAccount filtered strict LIKE prefix total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "CustomerAccount filtered strict LIKE prefix repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "CustomerAccount filtered strict LIKE prefix generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "CustomerAccount filtered strict LIKE prefix perf sample should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(1),
            "CustomerAccount filtered strict LIKE prefix perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_strict_like_prefix_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // guarded filtered-index strict LIKE prefix CustomerAccount covering shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected bounded CustomerAccount window.
        assert!(
            sample.first_local_instructions > 0,
            "descending CustomerAccount filtered strict LIKE prefix first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending CustomerAccount filtered strict LIKE prefix min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending CustomerAccount filtered strict LIKE prefix max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending CustomerAccount filtered strict LIKE prefix total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending CustomerAccount filtered strict LIKE prefix repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending CustomerAccount filtered strict LIKE prefix generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "descending CustomerAccount filtered strict LIKE prefix perf sample should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(1),
            "descending CustomerAccount filtered strict LIKE prefix perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_composite_order_only_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the guarded
        // composite filtered order-only CustomerAccount covering shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected ordered composite CustomerAccount window.
        assert!(
            sample.first_local_instructions > 0,
            "CustomerAccount filtered composite order-only first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "CustomerAccount filtered composite order-only min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "CustomerAccount filtered composite order-only max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "CustomerAccount filtered composite order-only total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "CustomerAccount filtered composite order-only repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "CustomerAccount filtered composite order-only generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "CustomerAccount filtered composite order-only perf sample should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerAccount filtered composite order-only perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_composite_order_only_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // guarded composite filtered order-only CustomerAccount shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected composite CustomerAccount window.
        assert!(
            sample.first_local_instructions > 0,
            "descending CustomerAccount filtered composite order-only first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending CustomerAccount filtered composite order-only min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending CustomerAccount filtered composite order-only max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending CustomerAccount filtered composite order-only total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending CustomerAccount filtered composite order-only repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending CustomerAccount filtered composite order-only generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "descending CustomerAccount filtered composite order-only perf sample should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerAccount filtered composite order-only perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_composite_strict_like_prefix_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the guarded
        // composite filtered strict LIKE prefix CustomerAccount covering shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the generated dispatch sample stays structurally
        // sane and returns the expected bounded composite CustomerAccount window.
        assert!(
            sample.first_local_instructions > 0,
            "CustomerAccount filtered composite strict LIKE prefix first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "CustomerAccount filtered composite strict LIKE prefix min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "CustomerAccount filtered composite strict LIKE prefix max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "CustomerAccount filtered composite strict LIKE prefix total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "CustomerAccount filtered composite strict LIKE prefix repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "CustomerAccount filtered composite strict LIKE prefix generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "CustomerAccount filtered composite strict LIKE prefix perf sample should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerAccount filtered composite strict LIKE prefix perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_composite_strict_like_prefix_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: sample the generated query surface for the descending
        // guarded composite filtered strict LIKE prefix CustomerAccount shape.
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        // Phase 2: assert the descending generated dispatch sample stays
        // structurally sane and returns the expected composite CustomerAccount window.
        assert!(
            sample.first_local_instructions > 0,
            "descending CustomerAccount filtered composite strict LIKE prefix first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending CustomerAccount filtered composite strict LIKE prefix min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending CustomerAccount filtered composite strict LIKE prefix max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending CustomerAccount filtered composite strict LIKE prefix total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending CustomerAccount filtered composite strict LIKE prefix repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending CustomerAccount filtered composite strict LIKE prefix generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "descending CustomerAccount filtered composite strict LIKE prefix perf sample should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerAccount filtered composite strict LIKE prefix perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_strict_text_range_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        assert!(
            sample.first_local_instructions > 0,
            "CustomerOrder strict text-range first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "CustomerOrder strict text-range min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "CustomerOrder strict text-range max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "CustomerOrder strict text-range total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "CustomerOrder strict text-range repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "CustomerOrder strict text-range generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "CustomerOrder strict text-range perf sample should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerOrder strict text-range perf sample should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_strict_text_range_desc_reports_positive_instruction_samples()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_sample(
            pic,
            canister_id,
            &SqlPerfRequest {
                surface: SqlPerfSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
                repeat_count: 5,
            },
        );

        assert!(
            sample.first_local_instructions > 0,
            "descending CustomerOrder strict text-range first instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.min_local_instructions > 0,
            "descending CustomerOrder strict text-range min instruction sample must be positive: {sample:?}",
        );
        assert!(
            sample.max_local_instructions >= sample.min_local_instructions,
            "descending CustomerOrder strict text-range max must be >= min: {sample:?}",
        );
        assert!(
            sample.total_local_instructions >= sample.first_local_instructions,
            "descending CustomerOrder strict text-range total must cover the first run: {sample:?}",
        );
        assert!(
            sample.outcome_stable,
            "descending CustomerOrder strict text-range repeated outcome must stay stable: {sample:?}",
        );
        assert!(
            sample.outcome.success,
            "descending CustomerOrder strict text-range generated dispatch sample must succeed: {sample:?}",
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "descending CustomerOrder strict text-range perf sample should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerOrder strict text-range perf sample should return the requested window size",
        );
    });
}

#[test]
#[ignore = "manual perf probe for before/after measurement runs"]
fn sql_canister_perf_probe_reports_sample_as_json() {
    run_with_loaded_fixture_canister(sql_perf_probe_canister(), |pic, canister_id| {
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
    run_with_loaded_fixture_canister(sql_perf_probe_canister(), |pic, canister_id| {
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the exact Customer
        // expression-order covering shape added in this slice.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the new expression-backed index route.
        assert_positive_scalar_attribution_sample("generated.user_expression_order", &sample, true);
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("Customer"),
            "Customer expression-order attribution should stay on the Customer route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "Customer expression-order attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_user_expression_order_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // Customer expression-order covering shape added to the harness.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2"
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
            Some("Customer"),
            "descending Customer expression-order attribution should stay on the Customer route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending Customer expression-order attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_order_only_composite_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        assert_positive_scalar_attribution_sample(
            "generated.customer_order_order_only_composite",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "CustomerOrder order-only composite attribution should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerOrder order-only composite attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_order_only_composite_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        assert_positive_scalar_attribution_sample(
            "generated.customer_order_order_only_composite_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "descending CustomerOrder order-only composite attribution should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerOrder order-only composite attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_numeric_equality_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        assert_positive_scalar_attribution_sample(
            "generated.customer_order_numeric_equality",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "CustomerOrder numeric-equality attribution should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerOrder numeric-equality attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_numeric_equality_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        assert_positive_scalar_attribution_sample(
            "generated.customer_order_numeric_equality_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "descending CustomerOrder numeric-equality attribution should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerOrder numeric-equality attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_numeric_equality_bounded_status_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        assert_positive_scalar_attribution_sample(
            "generated.customer_order_numeric_equality_bounded_status",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "CustomerOrder numeric-equality bounded status attribution should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerOrder numeric-equality bounded status attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_numeric_equality_bounded_status_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        assert_positive_scalar_attribution_sample(
            "generated.customer_order_numeric_equality_bounded_status_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "descending CustomerOrder numeric-equality bounded status attribution should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerOrder numeric-equality bounded status attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_order_only_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the guarded
        // filtered-index order-only CustomerAccount covering shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the guarded filtered-index route.
        assert_positive_scalar_attribution_sample(
            "generated.customer_account_filtered_order_only",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "CustomerAccount filtered order-only attribution should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerAccount filtered order-only attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_order_only_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // guarded filtered-index order-only CustomerAccount covering shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the descending generated dispatch attribution keeps
        // positive stage accounting on the reverse guarded filtered-index route.
        assert_positive_scalar_attribution_sample(
            "generated.customer_account_filtered_order_only_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "descending CustomerAccount filtered order-only attribution should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerAccount filtered order-only attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_composite_order_only_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the guarded
        // composite filtered order-only CustomerAccount covering shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the ordered composite filtered route.
        assert_positive_scalar_attribution_sample(
            "generated.customer_account_filtered_composite_order_only",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "CustomerAccount filtered composite order-only attribution should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerAccount filtered composite order-only attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_composite_order_only_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // guarded composite filtered order-only CustomerAccount shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the descending generated dispatch attribution keeps
        // positive stage accounting on the reverse composite route.
        assert_positive_scalar_attribution_sample(
            "generated.customer_account_filtered_composite_order_only_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "descending CustomerAccount filtered composite order-only attribution should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerAccount filtered composite order-only attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_strict_like_prefix_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the guarded
        // filtered-index strict LIKE prefix CustomerAccount covering shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the bounded filtered route.
        assert_positive_scalar_attribution_sample(
            "generated.customer_account_filtered_strict_like_prefix",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "CustomerAccount filtered strict LIKE prefix attribution should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(1),
            "CustomerAccount filtered strict LIKE prefix attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_strict_like_prefix_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // guarded filtered-index strict LIKE prefix CustomerAccount covering shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the descending generated dispatch attribution keeps
        // positive stage accounting on the reverse bounded filtered route.
        assert_positive_scalar_attribution_sample(
            "generated.customer_account_filtered_strict_like_prefix_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "descending CustomerAccount filtered strict LIKE prefix attribution should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(1),
            "descending CustomerAccount filtered strict LIKE prefix attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_composite_strict_like_prefix_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the guarded
        // composite filtered strict LIKE prefix CustomerAccount covering shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the generated dispatch attribution keeps positive
        // stage accounting on the bounded composite filtered route.
        assert_positive_scalar_attribution_sample(
            "generated.customer_account_filtered_composite_strict_like_prefix",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "CustomerAccount filtered composite strict LIKE prefix attribution should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerAccount filtered composite strict LIKE prefix attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_account_filtered_composite_strict_like_prefix_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: attribute the generated query surface for the descending
        // guarded composite filtered strict LIKE prefix CustomerAccount shape.
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        // Phase 2: assert the descending generated dispatch attribution keeps
        // positive stage accounting on the reverse bounded composite route.
        assert_positive_scalar_attribution_sample(
            "generated.customer_account_filtered_composite_strict_like_prefix_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerAccount"),
            "descending CustomerAccount filtered composite strict LIKE prefix attribution should stay on the CustomerAccount route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerAccount filtered composite strict LIKE prefix attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_strict_text_range_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        assert_positive_scalar_attribution_sample(
            "generated.customer_order_strict_text_range",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "CustomerOrder strict text-range attribution should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "CustomerOrder strict text-range attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_customer_order_strict_text_range_desc_attribution_reports_positive_stages()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sample = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::GeneratedDispatch,
                sql: "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2"
                    .to_string(),
                cursor_token: None,
            },
        );

        assert_positive_scalar_attribution_sample(
            "generated.customer_order_strict_text_range_desc",
            &sample,
            true,
        );
        assert_eq!(
            sample.outcome.entity.as_deref(),
            Some("CustomerOrder"),
            "descending CustomerOrder strict text-range attribution should stay on the CustomerOrder route",
        );
        assert_eq!(
            sample.outcome.row_count,
            Some(2),
            "descending CustomerOrder strict text-range attribution should return the requested window size",
        );
    });
}

#[test]
fn sql_canister_perf_query_phase_attribution_reports_positive_stages() {
    run_with_pic(|pic| {
        let mut rows = Vec::new();

        for (
            scenario_key,
            fixture_canister,
            sql,
            typed_surface,
            expected_entity,
            expected_row_count,
        ) in SCALAR_SELECT_ATTRIBUTION_CASES
        {
            let canister_id = install_fixture_canister(pic, *fixture_canister);
            load_default_fixtures(pic, canister_id);

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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sql = "SELECT age, COUNT(*) FROM Customer GROUP BY age ORDER BY age ASC LIMIT 10";

        let grouped = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::TypedGroupedCustomer,
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
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let full_page = sql_perf_attribution_sample(
            pic,
            canister_id,
            &SqlPerfAttributionRequest {
                surface: SqlPerfAttributionSurface::TypedGroupedCustomer,
                sql: "SELECT age, COUNT(*) FROM Customer GROUP BY age ORDER BY age ASC LIMIT 10"
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
                surface: SqlPerfAttributionSurface::TypedGroupedCustomer,
                sql: "SELECT age, COUNT(*) FROM Customer GROUP BY age ORDER BY age ASC LIMIT 2"
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
                surface: SqlPerfAttributionSurface::TypedGroupedCustomerSecondPage,
                sql: "SELECT age, COUNT(*) FROM Customer GROUP BY age ORDER BY age ASC LIMIT 2"
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
fn sql_canister_sql_parity_dispatch_is_entity_keyed_and_deterministic() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let user_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT name FROM Customer ORDER BY name ASC LIMIT 1",
            "Customer query should return projection rows",
        );
        assert_eq!(user_rows.entity, "Customer");
        assert_eq!(user_rows.columns, vec!["name".to_string()]);
        assert_eq!(user_rows.row_count, 1);
        assert_eq!(user_rows.rows, vec![vec!["alice".to_string()]]);

        // Property 3: no fallthrough; invalid field on Customer must be validated as Customer.
        let bad_user_field_error = query_result(
            pic,
            canister_id,
            "SELECT total_cents FROM Customer ORDER BY id ASC LIMIT 1",
        )
        .expect_err("bad Customer field should return error");
        assert!(
            bad_user_field_error
                .message()
                .contains("unknown expression field 'total_cents'"),
            "bad Customer field should stay on Customer route: {bad_user_field_error:?}",
        );
        assert!(
            !bad_user_field_error.message().contains("last_error"),
            "bad Customer field must not include fallback chaining text: {bad_user_field_error:?}",
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
            unknown_entity_error.message().contains("Customer")
                && unknown_entity_error.message().contains("CustomerAccount")
                && unknown_entity_error.message().contains("CustomerOrder"),
            "MissingEntity dispatch error should enumerate supported entities: {unknown_entity_error:?}",
        );
        assert!(
            !unknown_entity_error.message().contains("last_error"),
            "MissingEntity dispatch error must not include fallback trial chaining details: {unknown_entity_error:?}",
        );
    });
}

#[test]
#[expect(clippy::redundant_closure_for_method_calls)]
fn sql_canister_query_lane_supports_describe_show_indexes_and_show_columns() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let describe_payload = query_result(pic, canister_id, "DESCRIBE CustomerOrder")
            .expect("query DESCRIBE should return an Ok payload");
        let describe_lines = describe_payload.render_lines();
        match describe_payload {
            SqlQueryResult::Describe(description) => {
                assert_eq!(description.entity_name(), "CustomerOrder");
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
                .any(|line| line == "entity: CustomerOrder"),
            "DESCRIBE lines should include canonical entity name",
        );

        assert_metadata_entity_name(
            pic,
            canister_id,
            " dEsCrIbE public.CustomerOrder; ",
            "CustomerOrder",
            "query normalized DESCRIBE should return CustomerOrder metadata payload",
        );

        let show_indexes_payload = query_result(pic, canister_id, "SHOW INDEXES CustomerOrder")
            .expect("query SHOW INDEXES should return an Ok payload");
        let show_indexes_lines = show_indexes_payload.render_lines();
        match show_indexes_payload {
            SqlQueryResult::ShowIndexes { entity, indexes } => {
                assert_eq!(entity, "CustomerOrder");
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
                .is_some_and(|line| line.starts_with("surface=indexes entity=CustomerOrder")),
            "SHOW INDEXES lines should include deterministic surface header",
        );

        assert_metadata_entity_name(
            pic,
            canister_id,
            "sHoW InDeXeS public.CustomerOrder;",
            "CustomerOrder",
            "query normalized SHOW INDEXES should return CustomerOrder metadata payload",
        );

        let show_columns_payload = query_result(pic, canister_id, "SHOW COLUMNS CustomerOrder")
            .expect("query SHOW COLUMNS should return an Ok payload");
        let show_columns_lines = show_columns_payload.render_lines();
        match show_columns_payload {
            SqlQueryResult::ShowColumns { entity, columns } => {
                assert_eq!(entity, "CustomerOrder");
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
                .is_some_and(|line| line.starts_with("surface=columns entity=CustomerOrder")),
            "SHOW COLUMNS lines should include deterministic surface header",
        );

        assert_metadata_entity_name(
            pic,
            canister_id,
            "sHoW CoLuMnS public.CustomerOrder;",
            "CustomerOrder",
            "query normalized SHOW COLUMNS should return CustomerOrder metadata payload",
        );
    });
}
