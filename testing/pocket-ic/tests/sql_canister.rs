use candid::{Principal, encode_one};
use canic_testkit::pic::{
    Pic, PicStartError, StandaloneCanisterFixture, StandaloneCanisterFixtureError,
    try_acquire_pic_serial_guard, try_install_prebuilt_canister_with_cycles, try_pic,
};
use icydb::db::sql::{SqlGroupedRowsOutput, SqlQueryResult, SqlQueryRowsOutput};
use icydb_testing_integration::build_canister;
use serde::Serialize;
use std::{fs, sync::OnceLock, time::Instant};

const INIT_CYCLES: u128 = 50_000_000_000_000;
const ANY_PROJECTION_VALUE: &str = "<any>";
const SQL_PERF_PROBE_SQL_ENV: &str = "ICYDB_SQL_PERF_PROBE_SQL";
const SQL_PERF_PROBE_SQLS_ENV: &str = "ICYDB_SQL_PERF_PROBE_SQLS";
const SQL_PERF_PROBE_SQL_FILE_ENV: &str = "ICYDB_SQL_PERF_PROBE_SQL_FILE";
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

// Load the larger perf-audit fixture dataset and assert the update call
// returned `Ok(())`.
fn load_perf_audit_fixtures(pic: &Pic, canister_id: Principal) {
    expect_unit_update_ok(pic, canister_id, "fixtures_load_perf_audit");
}

// Reset the default fixture dataset and assert the update call returned `Ok(())`.
fn reset_fixtures(pic: &Pic, canister_id: Principal) {
    expect_unit_update_ok(pic, canister_id, "fixtures_reset");
}

// Mark the shared Customer index store as Building so integration tests can
// lock the fail-closed explain surface for one previously probe-free
// secondary covering cohort.
fn mark_customer_index_building(pic: &Pic, canister_id: Principal) {
    expect_unit_update_ok(pic, canister_id, "fixtures_mark_customer_index_building");
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

// Execute one integration test body against a fresh Pic with the larger
// perf-audit fixture dataset loaded into the installed sql_parity canister.
fn run_with_perf_fixture_canister(
    fixture_canister: FixtureCanister,
    test_body: impl FnOnce(&Pic, Principal),
) {
    run_with_fixture_canister(fixture_canister, |pic, canister_id| {
        load_perf_audit_fixtures(pic, canister_id);
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

fn query_grouped_rows(
    pic: &Pic,
    canister_id: Principal,
    sql: &str,
    context: &str,
) -> SqlGroupedRowsOutput {
    let payload = query_result(pic, canister_id, sql).expect(context);
    match payload {
        SqlQueryResult::Grouped(rows) => rows,
        other => panic!("{context}: expected Grouped payload, got {other:?}"),
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

// Drop regenerated id values from one delete payload so parity checks can
// compare the stable semantic row content across fresh fixture reloads.
fn stable_delete_rows(rows: &SqlQueryRowsOutput) -> Vec<Vec<String>> {
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
}

///
/// DeleteParityCase
///
/// One shared generated delete parity case.
/// Each case compares one accepted direct or text-range spelling against the
/// established LIKE delete payload while ignoring regenerated identity values.
///

struct DeleteParityCase {
    name: &'static str,
    direct_sql: &'static str,
    like_sql: &'static str,
}

// Assert one generated delete parity case by comparing the stable row payload
// emitted by the direct or text-range spelling against the LIKE baseline.
fn assert_delete_parity_case(pic: &Pic, canister_id: Principal, case: &DeleteParityCase) {
    reset_fixtures(pic, canister_id);
    load_default_fixtures(pic, canister_id);
    let direct = query_projection_rows(
        pic,
        canister_id,
        case.direct_sql,
        "generated direct STARTS_WITH delete should return projection rows",
    );

    reset_fixtures(pic, canister_id);
    load_default_fixtures(pic, canister_id);
    let like = query_projection_rows(
        pic,
        canister_id,
        case.like_sql,
        "generated LIKE delete should return projection rows",
    );

    assert_eq!(
        direct.columns, like.columns,
        "{}: generated delete columns should stay in parity",
        case.name,
    );
    assert_eq!(
        stable_delete_rows(&direct),
        stable_delete_rows(&like),
        "{}: generated delete payload should stay in parity aside from regenerated ids",
        case.name,
    );
}

///
/// UnsupportedStartsWithCase
///
/// One shared unsupported direct `STARTS_WITH` wrapper case.
/// Each case locks one public query or explain surface to the same
/// `Runtime::Unsupported` error class and stable detail message.
///

struct UnsupportedStartsWithCase {
    name: &'static str,
    sql: &'static str,
}

// Assert one unsupported direct `STARTS_WITH` wrapper case against the shared
// error taxonomy and stable unsupported-feature message.
fn assert_unsupported_starts_with_case(
    pic: &Pic,
    canister_id: Principal,
    case: &UnsupportedStartsWithCase,
) {
    let err = query_result(pic, canister_id, case.sql).expect_err(case.name);

    assert!(
        matches!(
            err.kind(),
            icydb::error::ErrorKind::Runtime(icydb::error::RuntimeErrorKind::Unsupported)
        ),
        "{} should map to Runtime::Unsupported: {err:?}",
        case.name,
    );
    assert!(
        err.message().contains(
            "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers"
        ),
        "{} should preserve the stable unsupported-feature detail: {err:?}",
        case.name,
    );
}

///
/// UserExpressionCoveringCase
///
/// One shared Customer `LOWER(name)` covering-route matrix case.
/// Each case locks both the public projection payload and the paired
/// `EXPLAIN EXECUTION` route surface for one ordering or range shape.
///

struct UserExpressionCoveringCase {
    name: &'static str,
    projection_sql: &'static str,
    projection_columns: &'static [&'static str],
    projection_rows: &'static [&'static [&'static str]],
    explain_sql: &'static str,
    explain_required_tokens: &'static [&'static str],
    explain_forbidden_tokens: &'static [&'static str],
}

// Assert one shared Customer `LOWER(name)` covering-route case across both
// the public projection and `EXPLAIN EXECUTION` surfaces.
fn assert_user_expression_covering_case(
    pic: &Pic,
    canister_id: Principal,
    case: &UserExpressionCoveringCase,
) {
    // Phase 1: execute the public projection and keep the projected window
    // pinned to the expected column and row shape.
    let rows = query_projection_rows(pic, canister_id, case.projection_sql, case.name);
    assert_projection_window(
        &rows,
        "Customer",
        case.projection_columns,
        case.projection_rows,
        case.name,
    );

    // Phase 2: request the paired execution descriptor and keep the route
    // token surface aligned with the same Customer expression-order shape.
    let payload = query_result(pic, canister_id, case.explain_sql)
        .unwrap_or_else(|err| panic!("{}: explain should return an Ok payload: {err}", case.name));
    assert_explain_route(
        payload,
        "Customer",
        case.explain_required_tokens,
        case.explain_forbidden_tokens,
        case.name,
    );
}

///
/// CustomerOrderCoveringCase
///
/// One shared CustomerOrder covering-route matrix case.
/// Each case locks both the public projection payload and the paired
/// `EXPLAIN EXECUTION` route surface for one equality, range, or order shape.
///

struct CustomerOrderCoveringCase {
    name: &'static str,
    projection_sql: &'static str,
    projection_columns: &'static [&'static str],
    projection_rows: &'static [&'static [&'static str]],
    explain_sql: &'static str,
    explain_required_tokens: &'static [&'static str],
    explain_forbidden_tokens: &'static [&'static str],
}

// Assert one shared CustomerOrder covering-route case across both the public
// projection and `EXPLAIN EXECUTION` surfaces.
fn assert_customer_order_covering_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerOrderCoveringCase,
) {
    // Phase 1: execute the public projection and keep the projected window
    // pinned to the expected CustomerOrder row shape.
    let rows = query_projection_rows(pic, canister_id, case.projection_sql, case.name);
    assert_projection_window(
        &rows,
        "CustomerOrder",
        case.projection_columns,
        case.projection_rows,
        case.name,
    );

    // Phase 2: request the paired execution descriptor and keep the route
    // token surface aligned with the same CustomerOrder access shape.
    let payload = query_result(pic, canister_id, case.explain_sql)
        .unwrap_or_else(|err| panic!("{}: explain should return an Ok payload: {err}", case.name));
    assert_explain_route(
        payload,
        "CustomerOrder",
        case.explain_required_tokens,
        case.explain_forbidden_tokens,
        case.name,
    );
}

///
/// GroupedOrderedExplainCase
///
/// One shared ordered-group explain case for the generated Customer SQL lane.
/// Each case locks one grouped `EXPLAIN` or `EXPLAIN EXECUTION` surface to
/// the expected access and grouping tokens for a specific aggregate shape.
///

struct GroupedOrderedExplainCase {
    name: &'static str,
    sql: &'static str,
    required_tokens: &'static [&'static str],
}

// Assert one shared grouped ordered explain case by checking that the explain
// text contains every route token required by the admitted grouped shape.
fn assert_grouped_ordered_explain_case(
    pic: &Pic,
    canister_id: Principal,
    case: &GroupedOrderedExplainCase,
) {
    let explain = query_explain_text(pic, canister_id, case.sql, case.name);

    for token in case.required_tokens {
        assert!(
            explain.contains(token),
            "{}: missing explain token `{token}` in {explain}",
            case.name
        );
    }
}

///
/// CustomerAccountFilteredCoveringCase
///
/// One shared simple filtered CustomerAccount covering-route matrix case.
/// Each case locks both the public projection payload and the paired
/// `EXPLAIN EXECUTION` route surface for one guarded filtered-index shape.
///

struct CustomerAccountFilteredCoveringCase {
    name: &'static str,
    projection_sql: &'static str,
    projection_columns: &'static [&'static str],
    projection_rows: &'static [&'static [&'static str]],
    explain_sql: &'static str,
    explain_required_tokens: &'static [&'static str],
    explain_forbidden_tokens: &'static [&'static str],
}

// Assert one shared simple filtered CustomerAccount covering-route case
// across both the public projection and `EXPLAIN EXECUTION` surfaces.
fn assert_customer_account_filtered_covering_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerAccountFilteredCoveringCase,
) {
    let rows = query_projection_rows(pic, canister_id, case.projection_sql, case.name);
    assert_projection_window(
        &rows,
        "CustomerAccount",
        case.projection_columns,
        case.projection_rows,
        case.name,
    );

    let payload = query_result(pic, canister_id, case.explain_sql)
        .unwrap_or_else(|err| panic!("{}: explain should return an Ok payload: {err}", case.name));
    assert_explain_route(
        payload,
        "CustomerAccount",
        case.explain_required_tokens,
        case.explain_forbidden_tokens,
        case.name,
    );
}

///
/// CustomerAccountFilteredPrefixParityCase
///
/// One shared simple filtered CustomerAccount strict-prefix parity case.
/// Each case checks that LIKE, direct STARTS_WITH, and strict text-range
/// spellings all project the same guarded filtered-index row window.
///

struct CustomerAccountFilteredPrefixParityCase {
    name: &'static str,
    like_sql: &'static str,
    starts_with_sql: &'static str,
    range_sql: &'static str,
}

// Assert one shared strict-prefix parity case by keeping all accepted
// spellings pinned to the same CustomerAccount projection payload.
fn assert_customer_account_filtered_prefix_parity_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerAccountFilteredPrefixParityCase,
) {
    let like_rows = query_projection_rows(pic, canister_id, case.like_sql, case.name);
    let starts_with_rows = query_projection_rows(pic, canister_id, case.starts_with_sql, case.name);
    let range_rows = query_projection_rows(pic, canister_id, case.range_sql, case.name);

    assert_eq!(
        starts_with_rows, like_rows,
        "{}: direct STARTS_WITH and LIKE rows should stay in parity",
        case.name,
    );
    assert_eq!(
        range_rows, like_rows,
        "{}: strict text-range and LIKE rows should stay in parity",
        case.name,
    );
}

///
/// CustomerAccountFilteredCompositeCoveringCase
///
/// One shared composite filtered CustomerAccount covering-route matrix case.
/// Each case locks both the public projection payload and the paired
/// `EXPLAIN EXECUTION` route surface for one composite filtered-index shape.
///

struct CustomerAccountFilteredCompositeCoveringCase {
    name: &'static str,
    projection_sql: &'static str,
    projection_columns: &'static [&'static str],
    projection_rows: &'static [&'static [&'static str]],
    explain_sql: &'static str,
    explain_required_tokens: &'static [&'static str],
    explain_forbidden_tokens: &'static [&'static str],
}

// Assert one shared composite filtered CustomerAccount covering-route case
// across both the public projection and `EXPLAIN EXECUTION` surfaces.
fn assert_customer_account_filtered_composite_covering_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerAccountFilteredCompositeCoveringCase,
) {
    let rows = query_projection_rows(pic, canister_id, case.projection_sql, case.name);
    assert_projection_window(
        &rows,
        "CustomerAccount",
        case.projection_columns,
        case.projection_rows,
        case.name,
    );

    let payload = query_result(pic, canister_id, case.explain_sql)
        .unwrap_or_else(|err| panic!("{}: explain should return an Ok payload: {err}", case.name));
    assert_explain_route(
        payload,
        "CustomerAccount",
        case.explain_required_tokens,
        case.explain_forbidden_tokens,
        case.name,
    );
}

///
/// CustomerAccountFilteredCompositePrefixParityCase
///
/// One shared composite filtered CustomerAccount strict-prefix parity case.
/// Each case checks that LIKE, direct STARTS_WITH, and strict text-range
/// spellings all project the same guarded composite filtered row window.
///

struct CustomerAccountFilteredCompositePrefixParityCase {
    name: &'static str,
    like_sql: &'static str,
    starts_with_sql: &'static str,
    range_sql: &'static str,
}

// Assert one shared composite strict-prefix parity case by keeping all
// accepted spellings pinned to the same CustomerAccount projection payload.
fn assert_customer_account_filtered_composite_prefix_parity_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerAccountFilteredCompositePrefixParityCase,
) {
    let like_rows = query_projection_rows(pic, canister_id, case.like_sql, case.name);
    let starts_with_rows = query_projection_rows(pic, canister_id, case.starts_with_sql, case.name);
    let range_rows = query_projection_rows(pic, canister_id, case.range_sql, case.name);

    assert_eq!(
        starts_with_rows, like_rows,
        "{}: direct STARTS_WITH and LIKE rows should stay in parity",
        case.name,
    );
    assert_eq!(
        range_rows, like_rows,
        "{}: strict text-range and LIKE rows should stay in parity",
        case.name,
    );
}

///
/// CustomerAccountFilteredExpressionCoveringCase
///
/// One shared filtered-expression CustomerAccount covering-route matrix case.
/// Each case locks both the public projection payload and the paired
/// `EXPLAIN EXECUTION` route surface for one guarded `LOWER(handle)` shape.
///

struct CustomerAccountFilteredExpressionCoveringCase {
    name: &'static str,
    projection_sql: &'static str,
    projection_columns: &'static [&'static str],
    projection_rows: &'static [&'static [&'static str]],
    explain_sql: &'static str,
    explain_required_tokens: &'static [&'static str],
    explain_forbidden_tokens: &'static [&'static str],
}

// Assert one shared filtered-expression CustomerAccount covering-route case
// across both the public projection and `EXPLAIN EXECUTION` surfaces.
fn assert_customer_account_filtered_expression_covering_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerAccountFilteredExpressionCoveringCase,
) {
    let rows = query_projection_rows(pic, canister_id, case.projection_sql, case.name);
    assert_projection_window(
        &rows,
        "CustomerAccount",
        case.projection_columns,
        case.projection_rows,
        case.name,
    );

    let payload = query_result(pic, canister_id, case.explain_sql)
        .unwrap_or_else(|err| panic!("{}: explain should return an Ok payload: {err}", case.name));
    assert_explain_route(
        payload,
        "CustomerAccount",
        case.explain_required_tokens,
        case.explain_forbidden_tokens,
        case.name,
    );
}

///
/// CustomerAccountFilteredExpressionPrefixParityCase
///
/// One shared filtered-expression CustomerAccount strict-prefix parity case.
/// Each case checks that LIKE, direct STARTS_WITH, and strict text-range
/// spellings all project the same guarded expression-order row window.
///

struct CustomerAccountFilteredExpressionPrefixParityCase {
    name: &'static str,
    like_sql: &'static str,
    starts_with_sql: &'static str,
    range_sql: &'static str,
    expected_rows: &'static [&'static [&'static str]],
}

// Assert one shared filtered-expression strict-prefix parity case by keeping
// all accepted spellings pinned to the same CustomerAccount projection payload.
fn assert_customer_account_filtered_expression_prefix_parity_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerAccountFilteredExpressionPrefixParityCase,
) {
    let like_rows = query_projection_rows(pic, canister_id, case.like_sql, case.name);
    let starts_with_rows = query_projection_rows(pic, canister_id, case.starts_with_sql, case.name);
    let range_rows = query_projection_rows(pic, canister_id, case.range_sql, case.name);

    assert_eq!(
        starts_with_rows, like_rows,
        "{}: STARTS_WITH and LIKE rows should stay in parity",
        case.name,
    );
    assert_eq!(
        range_rows, like_rows,
        "{}: text-range and LIKE rows should stay in parity",
        case.name,
    );
    assert_projection_window(
        &like_rows,
        "CustomerAccount",
        &["id", "handle"],
        case.expected_rows,
        case.name,
    );
}

///
/// CustomerAccountFilteredCompositeExpressionCase
///
/// One shared composite-expression CustomerAccount route case.
/// Each case can lock an optional public projection payload and one paired
/// `EXPLAIN EXECUTION` route surface for a guarded `tier, LOWER(handle)` shape.
///

struct CustomerAccountFilteredCompositeExpressionCase {
    name: &'static str,
    projection_sql: Option<&'static str>,
    projection_columns: &'static [&'static str],
    projection_rows: &'static [&'static [&'static str]],
    explain_sql: &'static str,
    explain_required_tokens: &'static [&'static str],
    explain_forbidden_tokens: &'static [&'static str],
}

// Assert one shared composite-expression CustomerAccount route case across an
// optional public projection payload and one paired `EXPLAIN EXECUTION` route.
fn assert_customer_account_filtered_composite_expression_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerAccountFilteredCompositeExpressionCase,
) {
    if let Some(sql) = case.projection_sql {
        let rows = query_projection_rows(pic, canister_id, sql, case.name);
        assert_projection_window(
            &rows,
            "CustomerAccount",
            case.projection_columns,
            case.projection_rows,
            case.name,
        );
    }

    let payload = query_result(pic, canister_id, case.explain_sql)
        .unwrap_or_else(|err| panic!("{}: explain should return an Ok payload: {err}", case.name));
    assert_explain_route(
        payload,
        "CustomerAccount",
        case.explain_required_tokens,
        case.explain_forbidden_tokens,
        case.name,
    );
}

///
/// CustomerAccountFilteredCompositeExpressionParityCase
///
/// One shared full-row composite-expression strict-prefix parity case.
/// Each case checks that LIKE, direct STARTS_WITH, and strict text-range
/// spellings all project the same guarded `tier, LOWER(handle)` row window.
///

struct CustomerAccountFilteredCompositeExpressionParityCase {
    name: &'static str,
    like_sql: &'static str,
    starts_with_sql: &'static str,
    range_sql: &'static str,
    columns: &'static [&'static str],
    expected_rows: &'static [&'static [&'static str]],
}

// Assert one shared full-row composite-expression prefix parity case by
// keeping all accepted spellings pinned to the same projection payload.
fn assert_customer_account_filtered_composite_expression_parity_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerAccountFilteredCompositeExpressionParityCase,
) {
    let like_rows = query_projection_rows(pic, canister_id, case.like_sql, case.name);
    let starts_with_rows = query_projection_rows(pic, canister_id, case.starts_with_sql, case.name);
    let range_rows = query_projection_rows(pic, canister_id, case.range_sql, case.name);

    assert_eq!(
        starts_with_rows, like_rows,
        "{}: STARTS_WITH and LIKE rows should stay in parity",
        case.name,
    );
    assert_eq!(
        range_rows, like_rows,
        "{}: text-range and LIKE rows should stay in parity",
        case.name,
    );
    assert_projection_window(
        &like_rows,
        "CustomerAccount",
        case.columns,
        case.expected_rows,
        case.name,
    );
}

///
/// CustomerAccountFilteredCompositeExpressionKeyOnlyParityCase
///
/// One shared key-only composite-expression direct-prefix parity case.
/// Each case checks that accepted prefix spellings stay aligned on the
/// guarded covering payload and that the direct route keeps its explain shape.
///

struct CustomerAccountFilteredCompositeExpressionKeyOnlyParityCase {
    name: &'static str,
    like_sql: &'static str,
    starts_with_sql: &'static str,
    range_sql: Option<&'static str>,
    columns: &'static [&'static str],
    expected_rows: &'static [&'static [&'static str]],
    explain_sql: &'static str,
    explain_required_tokens: &'static [&'static str],
    explain_forbidden_tokens: &'static [&'static str],
}

// Assert one shared key-only composite-expression direct-prefix parity case by
// keeping accepted prefix spellings aligned and then checking the direct route.
fn assert_customer_account_filtered_composite_expression_key_only_parity_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerAccountFilteredCompositeExpressionKeyOnlyParityCase,
) {
    let like_rows = query_projection_rows(pic, canister_id, case.like_sql, case.name);
    let starts_with_rows = query_projection_rows(pic, canister_id, case.starts_with_sql, case.name);

    assert_eq!(
        starts_with_rows, like_rows,
        "{}: STARTS_WITH and LIKE rows should stay in parity",
        case.name,
    );
    if let Some(range_sql) = case.range_sql {
        let range_rows = query_projection_rows(pic, canister_id, range_sql, case.name);
        assert_eq!(
            range_rows, like_rows,
            "{}: text-range and LIKE rows should stay in parity",
            case.name,
        );
    }
    assert_projection_window(
        &like_rows,
        "CustomerAccount",
        case.columns,
        case.expected_rows,
        case.name,
    );

    let payload = query_result(pic, canister_id, case.explain_sql)
        .unwrap_or_else(|err| panic!("{}: explain should return an Ok payload: {err}", case.name));
    assert_explain_route(
        payload,
        "CustomerAccount",
        case.explain_required_tokens,
        case.explain_forbidden_tokens,
        case.name,
    );
}

///
/// CustomerOrderStrictPrefixProjectionCase
///
/// One shared CustomerOrder strict-prefix projection parity case.
/// Each case keeps LIKE, direct STARTS_WITH, and strict text-range
/// spellings aligned on the same ordered covering projection window.
///

struct CustomerOrderStrictPrefixProjectionCase {
    name: &'static str,
    like_sql: &'static str,
    starts_with_sql: &'static str,
    range_sql: &'static str,
    expected_rows: &'static [&'static [&'static str]],
}

// Assert one shared CustomerOrder strict-prefix projection case by keeping
// accepted prefix spellings pinned to the same public row window.
fn assert_customer_order_strict_prefix_projection_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerOrderStrictPrefixProjectionCase,
) {
    let like_rows = query_projection_rows(pic, canister_id, case.like_sql, case.name);
    let starts_with_rows = query_projection_rows(pic, canister_id, case.starts_with_sql, case.name);
    let range_rows = query_projection_rows(pic, canister_id, case.range_sql, case.name);

    assert_eq!(
        starts_with_rows, like_rows,
        "{}: direct STARTS_WITH and LIKE rows should stay in parity",
        case.name,
    );
    assert_eq!(
        range_rows, like_rows,
        "{}: strict text-range and LIKE rows should stay in parity",
        case.name,
    );
    assert_projection_window(
        &like_rows,
        "CustomerOrder",
        &["id", "name"],
        case.expected_rows,
        case.name,
    );
}

///
/// CustomerOrderStrictPrefixExplainCase
///
/// One shared CustomerOrder strict-prefix explain-route case.
/// Each case keeps LIKE, direct STARTS_WITH, and strict text-range
/// spellings aligned on the same bounded covering route surface.
///

struct CustomerOrderStrictPrefixExplainCase {
    name: &'static str,
    like_sql: &'static str,
    starts_with_sql: &'static str,
    range_sql: &'static str,
    required_tokens: &'static [&'static str],
    forbidden_tokens: &'static [&'static str],
}

// Assert one shared CustomerOrder strict-prefix explain case by keeping all
// accepted prefix spellings aligned on the same covering-route token surface.
fn assert_customer_order_strict_prefix_explain_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerOrderStrictPrefixExplainCase,
) {
    for sql in [case.like_sql, case.starts_with_sql, case.range_sql] {
        let payload = query_result(pic, canister_id, sql).unwrap_or_else(|err| {
            panic!("{}: explain should return an Ok payload: {err}", case.name)
        });
        assert_explain_route(
            payload,
            "CustomerOrder",
            case.required_tokens,
            case.forbidden_tokens,
            case.name,
        );
    }
}

///
/// CustomerCasefoldPrefixProjectionCase
///
/// One shared Customer casefold-prefix projection parity case.
/// Each case keeps LIKE, direct STARTS_WITH, and strict text-range
/// spellings aligned for one accepted `LOWER` or `UPPER` field wrapper.
///

struct CustomerCasefoldPrefixProjectionCase {
    name: &'static str,
    like_sql: &'static str,
    starts_with_sql: &'static str,
    range_sql: &'static str,
    expected_rows: &'static [&'static [&'static str]],
}

// Assert one shared Customer casefold-prefix projection case by keeping all
// accepted spellings pinned to the same row window.
fn assert_customer_casefold_prefix_projection_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerCasefoldPrefixProjectionCase,
) {
    let like_rows = query_projection_rows(pic, canister_id, case.like_sql, case.name);
    let starts_with_rows = query_projection_rows(pic, canister_id, case.starts_with_sql, case.name);
    let range_rows = query_projection_rows(pic, canister_id, case.range_sql, case.name);

    assert_eq!(
        starts_with_rows, like_rows,
        "{}: direct STARTS_WITH and LIKE rows should stay in parity",
        case.name,
    );
    assert_eq!(
        range_rows, like_rows,
        "{}: strict text-range and LIKE rows should stay in parity",
        case.name,
    );
    assert_projection_window(
        &like_rows,
        "Customer",
        &["id", "name"],
        case.expected_rows,
        case.name,
    );
}

///
/// CustomerCasefoldPrefixExplainCase
///
/// One shared Customer casefold-prefix explain-route case.
/// Each case keeps LIKE, direct STARTS_WITH, and strict text-range
/// spellings aligned for one explain surface and casefold wrapper.
///

struct CustomerCasefoldPrefixExplainCase {
    name: &'static str,
    like_sql: &'static str,
    starts_with_sql: &'static str,
    range_sql: &'static str,
    required_tokens: &'static [&'static str],
    forbidden_tokens: &'static [&'static str],
}

// Assert one shared Customer casefold-prefix explain case by keeping all
// accepted spellings aligned on the same explain-route token contract.
fn assert_customer_casefold_prefix_explain_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerCasefoldPrefixExplainCase,
) {
    for sql in [case.like_sql, case.starts_with_sql, case.range_sql] {
        let payload = query_result(pic, canister_id, sql).unwrap_or_else(|err| {
            panic!("{}: explain should return an Ok payload: {err}", case.name)
        });
        assert_explain_route(
            payload,
            "Customer",
            case.required_tokens,
            case.forbidden_tokens,
            case.name,
        );
    }
}

///
/// CustomerCasefoldRangeExplainCase
///
/// One shared Customer casefold text-range explain case.
/// Each case locks one direct `LOWER` or `UPPER` range explain surface to
/// the expected index-range route tokens.
///

struct CustomerCasefoldRangeExplainCase {
    name: &'static str,
    sql: &'static str,
    required_tokens: &'static [&'static str],
    forbidden_tokens: &'static [&'static str],
}

// Assert one shared Customer casefold range explain case against the expected
// explain-route token surface.
fn assert_customer_casefold_range_explain_case(
    pic: &Pic,
    canister_id: Principal,
    case: &CustomerCasefoldRangeExplainCase,
) {
    let payload = query_result(pic, canister_id, case.sql)
        .unwrap_or_else(|err| panic!("{}: explain should return an Ok payload: {err}", case.name));
    assert_explain_route(
        payload,
        "Customer",
        case.required_tokens,
        case.forbidden_tokens,
        case.name,
    );
}

///
/// PlannerProjectionCase
///
/// One shared planner projection-window case.
/// Each case pins a planner query to one expected ordered row window so
/// offset and ordering policy can be checked without repeated boilerplate.
///

struct PlannerProjectionCase {
    name: &'static str,
    entity: &'static str,
    sql: &'static str,
    columns: &'static [&'static str],
    expected_rows: &'static [&'static [&'static str]],
}

// Assert one shared planner projection case against the expected ordered row
// window for the named planner entity.
fn assert_planner_projection_case(pic: &Pic, canister_id: Principal, case: &PlannerProjectionCase) {
    let rows = query_projection_rows(pic, canister_id, case.sql, case.name);
    assert_projection_window(
        &rows,
        case.entity,
        case.columns,
        case.expected_rows,
        case.name,
    );
}

///
/// PlannerExplainCase
///
/// One shared planner explain-route case.
/// Each case locks one planner `EXPLAIN JSON` or `EXPLAIN EXECUTION`
/// surface to the chosen access path, ordering contract, and exclusions.
///

struct PlannerExplainCase {
    name: &'static str,
    entity: &'static str,
    sql: &'static str,
    required_tokens: &'static [&'static str],
    forbidden_tokens: &'static [&'static str],
}

// Assert one shared planner explain case against the expected route tokens
// for the named planner entity.
fn assert_planner_explain_case(pic: &Pic, canister_id: Principal, case: &PlannerExplainCase) {
    let payload = query_result(pic, canister_id, case.sql)
        .unwrap_or_else(|err| panic!("{}: explain should return an Ok payload: {err}", case.name));
    assert_explain_route(
        payload,
        case.entity,
        case.required_tokens,
        case.forbidden_tokens,
        case.name,
    );
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
    TypedDispatchSqlWriteProbe,
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
    FluentExplainCustomerExists,
    FluentExplainCustomerMin,
    FluentExplainCustomerLast,
    FluentExplainCustomerSumByAge,
    FluentExplainCustomerAvgDistinctByAge,
    FluentExplainCustomerCountDistinctByAge,
    FluentExplainCustomerLastValueByAge,
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
    grouped_count_fold_metrics: Option<SqlPerfGroupedCountFoldMetrics>,
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

#[derive(candid::CandidType, Clone, Debug, candid::Deserialize, Eq, PartialEq, Serialize)]
struct SqlPerfGroupedCountFoldMetrics {
    fold_stage_runs: u64,
    rows_folded: u64,
    borrowed_probe_rows: u64,
    borrowed_hash_computations: u64,
    owned_group_fallback_rows: u64,
    owned_key_materializations: u64,
    bucket_candidate_checks: u64,
    existing_group_hits: u64,
    new_group_inserts: u64,
    finalize_stage_runs: u64,
    finalized_group_count: u64,
    window_rows_considered: u64,
    having_rows_rejected: u64,
    resume_boundary_rows_rejected: u64,
    candidate_rows_qualified: u64,
    bounded_selection_candidates_seen: u64,
    bounded_selection_heap_replacements: u64,
    bounded_selection_rows_sorted: u64,
    unbounded_selection_rows_sorted: u64,
    page_rows_skipped_for_offset: u64,
    projection_rows_input: u64,
    page_rows_emitted: u64,
    cursor_construction_attempts: u64,
    next_cursor_emitted: u64,
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
// SqlPerfExecutorAttribution
//
// Nested execute-phase attribution mirror returned by the sql_parity
// canister perf harness.
//

#[derive(candid::CandidType, Clone, Debug, candid::Deserialize, Serialize)]
struct SqlPerfExecutorAttribution {
    bind_local_instructions: u64,
    visible_indexes_local_instructions: u64,
    build_plan_local_instructions: u64,
    projection_labels_local_instructions: u64,
    projection_executor: SqlPerfProjectionTextExecutorAttribution,
    dispatch_result_local_instructions: u64,
    total_local_instructions: u64,
}

//
// SqlPerfProjectionTextExecutorAttribution
//
// Nested rendered-row executor attribution mirror returned by the sql_parity
// canister perf harness.
//

#[derive(candid::CandidType, Clone, Debug, candid::Deserialize, Serialize)]
struct SqlPerfProjectionTextExecutorAttribution {
    prepare_projection: u64,
    scalar_runtime: u64,
    materialize_projection: u64,
    result_rows: u64,
    total: u64,
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
    executor_breakdown: Option<SqlPerfExecutorAttribution>,
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

// Lock grouped continuation behavior for one typed grouped SQL window across
// the initial page, first paged window, and resumed second page.
fn assert_grouped_window_attribution(
    pic: &Pic,
    canister_id: Principal,
    sql_full_page: &str,
    sql_windowed: &str,
    context: &str,
) {
    let full_page = sql_perf_attribution_sample(
        pic,
        canister_id,
        &SqlPerfAttributionRequest {
            surface: SqlPerfAttributionSurface::TypedGroupedCustomer,
            sql: sql_full_page.to_string(),
            cursor_token: None,
        },
    );
    assert!(
        full_page.outcome.success && full_page.outcome.has_cursor == Some(false),
        "{context}: grouped full-page attribution must stay successful without emitting a cursor: {full_page:?}",
    );

    let first_page = sql_perf_attribution_sample(
        pic,
        canister_id,
        &SqlPerfAttributionRequest {
            surface: SqlPerfAttributionSurface::TypedGroupedCustomer,
            sql: sql_windowed.to_string(),
            cursor_token: None,
        },
    );
    assert!(
        first_page.outcome.success && first_page.outcome.has_cursor == Some(true),
        "{context}: grouped first-page attribution must stay successful and emit a cursor: {first_page:?}",
    );

    let second_page = sql_perf_attribution_sample(
        pic,
        canister_id,
        &SqlPerfAttributionRequest {
            surface: SqlPerfAttributionSurface::TypedGroupedCustomerSecondPage,
            sql: sql_windowed.to_string(),
            cursor_token: None,
        },
    );
    assert!(
        second_page.outcome.success && second_page.outcome.has_cursor == Some(false),
        "{context}: grouped second-page attribution must stay successful without emitting a cursor: {second_page:?}",
    );
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

// Resolve one batch SQL probe list from the first explicit batch source.
//
// The manual PocketIC perf workflow needs a mode that can reuse one loaded
// canister across many queries. Keeping batch parsing here avoids building a
// separate ad hoc runner crate just to amortize fixture startup cost.
fn sql_perf_probe_sql_batch() -> Vec<String> {
    // Phase 1: prefer an explicit file input so long query sets stay easy to
    // maintain and shell quoting does not dominate the measurement workflow.
    if let Some(path) = optional_non_empty_env(SQL_PERF_PROBE_SQL_FILE_ENV) {
        let raw = fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!("failed to read {SQL_PERF_PROBE_SQL_FILE_ENV} file at '{path}': {err}")
        });

        return parse_sql_perf_probe_batch(raw.as_str());
    }

    // Phase 2: otherwise accept one inline newline-delimited batch env for
    // quick local triage without another temp file.
    if let Some(raw_sqls) = optional_non_empty_env(SQL_PERF_PROBE_SQLS_ENV) {
        return parse_sql_perf_probe_batch(raw_sqls.as_str());
    }

    vec![sql_perf_probe_sql()]
}

// Parse one newline-delimited SQL probe batch while ignoring blank lines and
// shell-friendly comment lines.
fn parse_sql_perf_probe_batch(raw: &str) -> Vec<String> {
    let queries = raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    assert!(
        !queries.is_empty(),
        "sql perf batch must contain at least one non-empty SQL statement",
    );

    queries
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

// Emit one manual perf-probe phase timestamp relative to one shared start.
//
// The PocketIC perf workflow has become opaque enough that we need an
// explicit timing trace before optimizing further. Keeping the helper local
// to this harness avoids another ad hoc debug runner binary.
fn log_perf_probe_phase(started_at: Instant, label: &str) {
    eprintln!(
        "[sql-perf-probe] +{:.3}s {label}",
        started_at.elapsed().as_secs_f64()
    );
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
        "typeddispatchsqlwriteprobe"
        | "typed_dispatch_sql_write_probe"
        | "typeddispatchwriteprobe"
        | "typed_dispatch_write_probe" => SqlPerfSurface::TypedDispatchSqlWriteProbe,
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
        "fluentexplaincustomerexists"
        | "fluent_explain_customer_exists"
        | "fluentexplainexists"
        | "fluent_explain_exists" => SqlPerfSurface::FluentExplainCustomerExists,
        "fluentexplaincustomermin"
        | "fluent_explain_customer_min"
        | "fluentexplainmin"
        | "fluent_explain_min" => SqlPerfSurface::FluentExplainCustomerMin,
        "fluentexplaincustomerlast"
        | "fluent_explain_customer_last"
        | "fluentexplainlast"
        | "fluent_explain_last" => SqlPerfSurface::FluentExplainCustomerLast,
        "fluentexplaincustomersumbyage"
        | "fluent_explain_customer_sum_by_age"
        | "fluentexplainsumbyage"
        | "fluent_explain_sum_by_age" => SqlPerfSurface::FluentExplainCustomerSumByAge,
        "fluentexplaincustomeravgdistinctbyage"
        | "fluent_explain_customer_avg_distinct_by_age"
        | "fluentexplainavgdistinctbyage"
        | "fluent_explain_avg_distinct_by_age" => {
            SqlPerfSurface::FluentExplainCustomerAvgDistinctByAge
        }
        "fluentexplaincustomercountdistinctbyage"
        | "fluent_explain_customer_count_distinct_by_age"
        | "fluentexplaincountdistinctbyage"
        | "fluent_explain_count_distinct_by_age" => {
            SqlPerfSurface::FluentExplainCustomerCountDistinctByAge
        }
        "fluentexplaincustomerlastvaluebyage"
        | "fluent_explain_customer_last_value_by_age"
        | "fluentexplainlastvaluebyage"
        | "fluent_explain_last_value_by_age" => SqlPerfSurface::FluentExplainCustomerLastValueByAge,
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
        load_perf_audit_fixtures(pic, canister_id);

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
        "user_id_order_id_limit1",
        FixtureCanister::SqlParity,
        "SELECT id FROM Customer ORDER BY id ASC LIMIT 1",
        SqlPerfAttributionSurface::TypedDispatchCustomer,
        "Customer",
        1,
    ),
    (
        "user_name_order_id_name_limit2",
        FixtureCanister::SqlParity,
        "SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2",
        SqlPerfAttributionSurface::TypedDispatchCustomer,
        "Customer",
        2,
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
        "customer_order_distinct_priority_limit2_asc",
        FixtureCanister::SqlParity,
        "SELECT DISTINCT priority FROM CustomerOrder ORDER BY priority ASC LIMIT 2",
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
        "customer_order_distinct_priority_limit2.asc",
        FixtureCanister::SqlParity,
        "SELECT DISTINCT priority FROM CustomerOrder ORDER BY priority ASC LIMIT 2",
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

// Keep the generated-dispatch sample and attribution matrices aligned on the
// same ordered covering cohort so sample-level and stage-level perf checks
// cannot silently drift onto different query shapes.
const GENERATED_DISPATCH_ORDERED_PERF_CASES: &[(&str, &str, &str, u32)] = &[
    (
        "generated.user_expression_order.asc",
        "SELECT id, name FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        "Customer",
        2,
    ),
    (
        "generated.user_expression_order.desc",
        "SELECT id, name FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        "Customer",
        2,
    ),
    (
        "generated.customer_order_order_only_composite.asc",
        "SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
        "CustomerOrder",
        2,
    ),
    (
        "generated.customer_order_order_only_composite.desc",
        "SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
        "CustomerOrder",
        2,
    ),
    (
        "generated.customer_order_numeric_equality.asc",
        "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
        "CustomerOrder",
        2,
    ),
    (
        "generated.customer_order_numeric_equality.desc",
        "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
        "CustomerOrder",
        2,
    ),
    (
        "generated.customer_order_numeric_equality_bounded_status.asc",
        "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status ASC, id ASC LIMIT 2",
        "CustomerOrder",
        2,
    ),
    (
        "generated.customer_order_numeric_equality_bounded_status.desc",
        "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status DESC, id DESC LIMIT 2",
        "CustomerOrder",
        2,
    ),
    (
        "generated.customer_account_filtered_order_only.asc",
        "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
        "CustomerAccount",
        2,
    ),
    (
        "generated.customer_account_filtered_order_only.desc",
        "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
        "CustomerAccount",
        2,
    ),
    (
        "generated.customer_account_filtered_strict_like_prefix.asc",
        "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
        "CustomerAccount",
        1,
    ),
    (
        "generated.customer_account_filtered_strict_like_prefix.desc",
        "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
        "CustomerAccount",
        1,
    ),
    (
        "generated.customer_account_filtered_composite_order_only.asc",
        "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
        "CustomerAccount",
        2,
    ),
    (
        "generated.customer_account_filtered_composite_order_only.desc",
        "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
        "CustomerAccount",
        2,
    ),
    (
        "generated.customer_account_filtered_composite_strict_like_prefix.asc",
        "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
        "CustomerAccount",
        2,
    ),
    (
        "generated.customer_account_filtered_composite_strict_like_prefix.desc",
        "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
        "CustomerAccount",
        2,
    ),
    (
        "generated.customer_order_strict_text_range.asc",
        "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
        "CustomerOrder",
        2,
    ),
    (
        "generated.customer_order_strict_text_range.desc",
        "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
        "CustomerOrder",
        2,
    ),
];

// Keep the typed-dispatch projection smoke cohort in one table so alias,
// insert, and update shapes all stay on the same lightweight success contract.
type TypedDispatchProjectionPerfCase<'a> = (&'a str, SqlPerfSurface, &'a str, &'a str, u32, u32);

const TYPED_DISPATCH_PROJECTION_PERF_CASES: &[TypedDispatchProjectionPerfCase<'_>] = &[
    (
        "typed.dispatch.customer_account.lower_order_alias",
        SqlPerfSurface::TypedDispatchCustomerAccount,
        "SELECT LOWER(handle) AS normalized_handle, id FROM CustomerAccount WHERE active = true ORDER BY normalized_handle ASC, id ASC LIMIT 2",
        "CustomerAccount",
        2,
        5,
    ),
    (
        "typed.dispatch.customer.table_alias",
        SqlPerfSurface::TypedDispatchCustomer,
        "SELECT customer.name FROM Customer customer WHERE customer.name = 'alice' ORDER BY customer.id ASC LIMIT 1",
        "Customer",
        1,
        5,
    ),
    (
        "typed.dispatch.sql_write_probe.insert",
        SqlPerfSurface::TypedDispatchSqlWriteProbe,
        "INSERT INTO SqlWriteProbe (id, name, age) VALUES (2, 'inserted', 22)",
        "SqlWriteProbe",
        1,
        1,
    ),
    (
        "typed.dispatch.sql_write_probe.insert_alias",
        SqlPerfSurface::TypedDispatchSqlWriteProbe,
        "INSERT INTO SqlWriteProbe s (id, name, age) VALUES (2, 'inserted-alias', 22)",
        "SqlWriteProbe",
        1,
        1,
    ),
    (
        "typed.dispatch.customer.generated_pk_insert",
        SqlPerfSurface::TypedDispatchCustomer,
        "INSERT INTO Customer (name, age) VALUES ('inserted-generated', 22)",
        "Customer",
        1,
        1,
    ),
    (
        "typed.dispatch.customer.insert_select_generated_pk",
        SqlPerfSurface::TypedDispatchCustomer,
        "INSERT INTO Customer (name, age) SELECT name, age FROM Customer WHERE name = 'alice' ORDER BY id ASC LIMIT 1",
        "Customer",
        1,
        1,
    ),
    (
        "typed.dispatch.customer.insert_select_computed_generated_pk",
        SqlPerfSurface::TypedDispatchCustomer,
        "INSERT INTO Customer (name, age) SELECT LOWER(name), age FROM Customer WHERE name = 'alice' ORDER BY id ASC LIMIT 1",
        "Customer",
        1,
        1,
    ),
    (
        "typed.dispatch.sql_write_probe.multi_insert",
        SqlPerfSurface::TypedDispatchSqlWriteProbe,
        "INSERT INTO SqlWriteProbe (id, name, age) VALUES (2, 'inserted-a', 22), (3, 'inserted-b', 23)",
        "SqlWriteProbe",
        2,
        1,
    ),
    (
        "typed.dispatch.sql_write_probe.positional_insert",
        SqlPerfSurface::TypedDispatchSqlWriteProbe,
        "INSERT INTO SqlWriteProbe VALUES (2, 'positional', 22)",
        "SqlWriteProbe",
        1,
        1,
    ),
    (
        "typed.dispatch.sql_write_probe.update",
        SqlPerfSurface::TypedDispatchSqlWriteProbe,
        "UPDATE SqlWriteProbe SET name = 'updated', age = 22 WHERE id = 1",
        "SqlWriteProbe",
        1,
        5,
    ),
    (
        "typed.dispatch.sql_write_probe.update_alias",
        SqlPerfSurface::TypedDispatchSqlWriteProbe,
        "UPDATE SqlWriteProbe s SET s.name = 'updated-alias', s.age = 22 WHERE s.id = 1",
        "SqlWriteProbe",
        1,
        5,
    ),
    (
        "typed.dispatch.sql_write_probe.equality_predicate_update",
        SqlPerfSurface::TypedDispatchSqlWriteProbe,
        "UPDATE SqlWriteProbe SET name = 'updated-by-eq', age = 22 WHERE age = 21",
        "SqlWriteProbe",
        1,
        1,
    ),
    (
        "typed.dispatch.sql_write_probe.predicate_update",
        SqlPerfSurface::TypedDispatchSqlWriteProbe,
        "UPDATE SqlWriteProbe SET name = 'updated-by-age', age = 22 WHERE age >= 21",
        "SqlWriteProbe",
        1,
        5,
    ),
    (
        "typed.dispatch.sql_write_probe.ordered_window_update",
        SqlPerfSurface::TypedDispatchSqlWriteProbe,
        "UPDATE SqlWriteProbe SET name = 'updated-window', age = 22 WHERE id >= 1 ORDER BY id ASC LIMIT 1",
        "SqlWriteProbe",
        1,
        5,
    ),
];

// Keep the typed grouped-customer smoke cohort on one shared grouped-response
// contract so projection and aggregate variants stay easy to audit together.
const TYPED_GROUPED_CUSTOMER_PERF_CASES: &[(&str, &str)] = &[
    (
        "typed.execute_sql_grouped.customer.extrema.min",
        "SELECT name, MIN(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
    ),
    (
        "typed.execute_sql_grouped.customer.extrema.max",
        "SELECT name, MAX(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
    ),
    (
        "typed.execute_sql_grouped.customer.extrema.distinct_min",
        "SELECT name, MIN(DISTINCT age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
    ),
    (
        "typed.execute_sql_grouped.customer.distinct.count",
        "SELECT name, COUNT(DISTINCT age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
    ),
    (
        "typed.execute_sql_grouped.customer.distinct.sum",
        "SELECT name, SUM(DISTINCT age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
    ),
    (
        "typed.execute_sql_grouped.customer.distinct.avg",
        "SELECT name, AVG(DISTINCT age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
    ),
    (
        "typed.execute_sql_grouped.customer.top_level_distinct",
        "SELECT DISTINCT name, COUNT(*) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
    ),
    (
        "typed.execute_sql_grouped.customer.computed",
        "SELECT TRIM(name), COUNT(*) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
    ),
    (
        "typed.execute_sql_grouped.customer.computed_alias",
        "SELECT TRIM(name) AS trimmed_name, COUNT(*) total FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
    ),
    (
        "typed.execute_sql_grouped.customer.order_alias",
        "SELECT age years, COUNT(*) total FROM Customer GROUP BY age ORDER BY years ASC LIMIT 10",
    ),
];

// Keep the typed aggregate-customer smoke cohort on one shared aggregate-value
// contract so count, numeric, distinct, filtered, and window variants all stay
// aligned on the same result surface.
const TYPED_CUSTOMER_AGGREGATE_PERF_CASES: &[(&str, &str, &str)] = &[
    (
        "typed.execute_sql_aggregate.customer.count.star",
        "SELECT COUNT(*) FROM Customer",
        "Uint(3)",
    ),
    (
        "typed.execute_sql_aggregate.customer.count.field",
        "SELECT COUNT(age) FROM Customer",
        "Uint(3)",
    ),
    (
        "typed.execute_sql_aggregate.customer.numeric.min",
        "SELECT MIN(age) FROM Customer",
        "Int(24)",
    ),
    (
        "typed.execute_sql_aggregate.customer.numeric.max",
        "SELECT MAX(age) FROM Customer",
        "Int(43)",
    ),
    (
        "typed.execute_sql_aggregate.customer.numeric.sum",
        "SELECT SUM(age) FROM Customer",
        "Decimal(Decimal { mantissa: 98, scale: 0 })",
    ),
    (
        "typed.execute_sql_aggregate.customer.numeric.avg",
        "SELECT AVG(age) FROM Customer",
        "Decimal(Decimal { mantissa: 32666666666666666667, scale: 18 })",
    ),
    (
        "typed.execute_sql_aggregate.customer.distinct.count",
        "SELECT COUNT(DISTINCT age) FROM Customer",
        "Uint(3)",
    ),
    (
        "typed.execute_sql_aggregate.customer.distinct.sum",
        "SELECT SUM(DISTINCT age) FROM Customer",
        "Decimal(Decimal { mantissa: 98, scale: 0 })",
    ),
    (
        "typed.execute_sql_aggregate.customer.distinct.avg",
        "SELECT AVG(DISTINCT age) FROM Customer",
        "Decimal(Decimal { mantissa: 32666666666666666667, scale: 18 })",
    ),
    (
        "typed.execute_sql_aggregate.customer.distinct.min",
        "SELECT MIN(DISTINCT age) FROM Customer",
        "Int(24)",
    ),
    (
        "typed.execute_sql_aggregate.customer.distinct.max",
        "SELECT MAX(DISTINCT age) FROM Customer",
        "Int(43)",
    ),
    (
        "typed.execute_sql_aggregate.customer.filtered.count.star",
        "SELECT COUNT(*) FROM Customer WHERE age >= 30",
        "Uint(2)",
    ),
    (
        "typed.execute_sql_aggregate.customer.filtered.count.field",
        "SELECT COUNT(age) FROM Customer WHERE age >= 30",
        "Uint(2)",
    ),
    (
        "typed.execute_sql_aggregate.customer.filtered.min",
        "SELECT MIN(age) FROM Customer WHERE age >= 30",
        "Int(31)",
    ),
    (
        "typed.execute_sql_aggregate.customer.filtered.max",
        "SELECT MAX(age) FROM Customer WHERE age >= 30",
        "Int(43)",
    ),
    (
        "typed.execute_sql_aggregate.customer.filtered.sum",
        "SELECT SUM(age) FROM Customer WHERE age >= 30",
        "Decimal(Decimal { mantissa: 74, scale: 0 })",
    ),
    (
        "typed.execute_sql_aggregate.customer.filtered.avg",
        "SELECT AVG(age) FROM Customer WHERE age >= 30",
        "Decimal(Decimal { mantissa: 37, scale: 0 })",
    ),
    (
        "typed.execute_sql_aggregate.customer.empty.count.star",
        "SELECT COUNT(*) FROM Customer WHERE age < 0",
        "Uint(0)",
    ),
    (
        "typed.execute_sql_aggregate.customer.empty.sum",
        "SELECT SUM(age) FROM Customer WHERE age < 0",
        "Null",
    ),
    (
        "typed.execute_sql_aggregate.customer.empty.avg",
        "SELECT AVG(age) FROM Customer WHERE age < 0",
        "Null",
    ),
    (
        "typed.execute_sql_aggregate.customer.empty.min",
        "SELECT MIN(age) FROM Customer WHERE age < 0",
        "Null",
    ),
    (
        "typed.execute_sql_aggregate.customer.empty.max",
        "SELECT MAX(age) FROM Customer WHERE age < 0",
        "Null",
    ),
    (
        "typed.execute_sql_aggregate.customer.window.count.star",
        "SELECT COUNT(*) FROM Customer ORDER BY age DESC LIMIT 2 OFFSET 1",
        "Uint(2)",
    ),
    (
        "typed.execute_sql_aggregate.customer.window.sum",
        "SELECT SUM(age) FROM Customer ORDER BY age DESC LIMIT 1 OFFSET 1",
        "Decimal(Decimal { mantissa: 31, scale: 0 })",
    ),
    (
        "typed.execute_sql_aggregate.customer.window.avg",
        "SELECT AVG(age) FROM Customer ORDER BY age ASC LIMIT 2 OFFSET 1",
        "Decimal(Decimal { mantissa: 37, scale: 0 })",
    ),
    (
        "typed.execute_sql_aggregate.customer.offset_beyond.count.star",
        "SELECT COUNT(*) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
        "Uint(0)",
    ),
    (
        "typed.execute_sql_aggregate.customer.offset_beyond.sum",
        "SELECT SUM(age) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
        "Null",
    ),
    (
        "typed.execute_sql_aggregate.customer.offset_beyond.avg",
        "SELECT AVG(age) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
        "Null",
    ),
    (
        "typed.execute_sql_aggregate.customer.offset_beyond.min",
        "SELECT MIN(age) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
        "Null",
    ),
    (
        "typed.execute_sql_aggregate.customer.offset_beyond.max",
        "SELECT MAX(age) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
        "Null",
    ),
];

// Keep the typed aggregate-customer reject-path smoke cases together because
// they share the same unsupported-error contract and only vary by input shape.
const TYPED_CUSTOMER_AGGREGATE_REJECT_CASES: &[(&str, &str, &str)] = &[
    (
        "typed.execute_sql_aggregate.customer.reject.non_aggregate",
        "SELECT age FROM Customer",
        "execute_sql_aggregate requires constrained global aggregate SELECT",
    ),
    (
        "typed.execute_sql_aggregate.customer.reject.grouped",
        "SELECT age, COUNT(*) FROM Customer GROUP BY age",
        "execute_sql_aggregate rejects grouped SELECT",
    ),
];

// Keep the grouped window attribution cursor-contract checks in one table so
// SUM and AVG variants stay aligned on the same pagination assertions.
const GROUPED_WINDOW_ATTRIBUTION_CASES: &[(&str, &str, &str)] = &[
    (
        "SUM(field) grouped window attribution",
        "SELECT name, SUM(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
        "SELECT name, SUM(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 2",
    ),
    (
        "AVG(field) grouped window attribution",
        "SELECT name, AVG(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
        "SELECT name, AVG(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 2",
    ),
];

// Keep the generated delete attribution smoke cases together because they
// share the same stage-accounting contract and only vary by delete window.
const GENERATED_DELETE_ATTRIBUTION_CASES: &[(&str, &str, Option<u32>, bool)] = &[
    (
        "generated_delete",
        "DELETE FROM Customer ORDER BY id LIMIT 1",
        Some(1),
        false,
    ),
    (
        "generated_delete_offset",
        "DELETE FROM Customer ORDER BY id LIMIT 1 OFFSET 1",
        None,
        true,
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

// Assert one generated-dispatch perf sample case against the shared positive
// sample contract plus the expected entity and row-window surface.
fn assert_generated_dispatch_perf_case(
    pic: &Pic,
    canister_id: Principal,
    label: &str,
    sql: &str,
    expected_entity: &str,
    expected_row_count: u32,
) {
    // Phase 1: request one repeated generated-dispatch sample for the
    // table-driven ordered covering query shape under test.
    let sample = sql_perf_sample(
        pic,
        canister_id,
        &SqlPerfRequest {
            surface: SqlPerfSurface::GeneratedDispatch,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 5,
        },
    );

    // Phase 2: keep the shared sample-shape contract, entity binding, and
    // requested row window aligned across the whole generated cohort.
    assert_positive_perf_sample(label, &sample);
    assert!(
        sample.outcome.success,
        "{label} generated dispatch perf sample must succeed: {sample:?}",
    );
    assert_eq!(
        sample.outcome.entity.as_deref(),
        Some(expected_entity),
        "{label} generated dispatch perf sample should stay on the expected entity route",
    );
    assert_eq!(
        sample.outcome.row_count,
        Some(expected_row_count),
        "{label} generated dispatch perf sample should return the requested window size",
    );
}

// Assert one generated-dispatch attribution case against the shared positive
// stage-accounting contract plus the expected entity and row-window surface.
fn assert_generated_dispatch_attribution_case(
    pic: &Pic,
    canister_id: Principal,
    label: &str,
    sql: &str,
    expected_entity: &str,
    expected_row_count: u32,
) {
    // Phase 1: request one generated-dispatch attribution sample for the
    // same ordered covering query shape used by the sample-level matrix.
    let sample = sql_perf_attribution_sample(
        pic,
        canister_id,
        &SqlPerfAttributionRequest {
            surface: SqlPerfAttributionSurface::GeneratedDispatch,
            sql: sql.to_string(),
            cursor_token: None,
        },
    );

    // Phase 2: keep positive stage accounting, entity binding, and row-window
    // expectations aligned across the generated attribution cohort.
    assert_positive_scalar_attribution_sample(label, &sample, true);
    assert_eq!(
        sample.outcome.entity.as_deref(),
        Some(expected_entity),
        "{label} generated dispatch attribution should stay on the expected entity route",
    );
    assert_eq!(
        sample.outcome.row_count,
        Some(expected_row_count),
        "{label} generated dispatch attribution should return the requested window size",
    );
}

// Assert one typed-dispatch projection perf case against the shared positive
// sample contract plus the common projection result surface.
fn assert_typed_dispatch_projection_perf_case(
    pic: &Pic,
    canister_id: Principal,
    case: TypedDispatchProjectionPerfCase<'_>,
) {
    let (label, surface, sql, expected_entity, expected_row_count, repeat_count) = case;

    // Phase 1: request one typed-dispatch sample for the alias, insert, or
    // update shape under test.
    let sample = sql_perf_sample(
        pic,
        canister_id,
        &SqlPerfRequest {
            surface,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count,
        },
    );

    // Phase 2: keep the shared projection result contract stable across the
    // typed-dispatch smoke cohort.
    assert_positive_perf_sample(label, &sample);
    assert!(
        sample.outcome.success,
        "{label} typed-dispatch perf sample must succeed: {sample:?}",
    );
    assert_eq!(
        sample.outcome.result_kind, "projection",
        "{label} typed-dispatch perf sample should emit the projection result kind",
    );
    assert_eq!(
        sample.outcome.entity.as_deref(),
        Some(expected_entity),
        "{label} typed-dispatch perf sample should stay on the expected entity route",
    );
    assert_eq!(
        sample.outcome.row_count,
        Some(expected_row_count),
        "{label} typed-dispatch perf sample should emit the expected projected row count",
    );
}

// Assert one typed grouped-customer perf case against the shared grouped
// response contract.
fn assert_typed_grouped_customer_perf_case(
    pic: &Pic,
    canister_id: Principal,
    label: &str,
    sql: &str,
) {
    // Phase 1: request one grouped-customer perf sample for the table-driven
    // grouped query shape under test.
    let sample = sql_perf_sample(
        pic,
        canister_id,
        &SqlPerfRequest {
            surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 5,
        },
    );

    // Phase 2: keep the grouped result contract stable across grouped
    // aggregate, distinct, computed, and alias shapes.
    assert_positive_perf_sample(label, &sample);
    assert!(
        sample.outcome.success,
        "{label} typed grouped-customer perf sample must succeed: {sample:?}",
    );
    assert_eq!(
        sample.outcome.result_kind, "grouped_response",
        "{label} typed grouped-customer perf sample should stay on the grouped response lane",
    );
    assert_eq!(
        sample.outcome.entity.as_deref(),
        Some("Customer"),
        "{label} typed grouped-customer perf sample should stay on the Customer grouped lane",
    );
    assert_eq!(
        sample.outcome.row_count,
        Some(3),
        "{label} typed grouped-customer perf sample should emit the expected grouped row count",
    );
}

// Assert one typed aggregate-customer perf case against the shared aggregate
// value contract.
fn assert_typed_customer_aggregate_perf_case(
    pic: &Pic,
    canister_id: Principal,
    label: &str,
    sql: &str,
    expected_rendered_value: &str,
) {
    // Phase 1: request one aggregate-customer perf sample for the table-driven
    // scalar query shape under test.
    let sample = sql_perf_sample(
        pic,
        canister_id,
        &SqlPerfRequest {
            surface: SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 5,
        },
    );

    // Phase 2: keep the aggregate result contract stable across count,
    // numeric, distinct, filtered, empty-window, and paged variants.
    assert_positive_perf_sample(label, &sample);
    assert!(
        sample.outcome.success,
        "{label} typed aggregate-customer perf sample must succeed: {sample:?}",
    );
    assert_eq!(
        sample.outcome.result_kind, "aggregate_value",
        "{label} typed aggregate-customer perf sample should keep the aggregate outcome kind",
    );
    assert_eq!(
        sample.outcome.entity.as_deref(),
        Some("Customer"),
        "{label} typed aggregate-customer perf sample should stay on the Customer aggregate lane",
    );
    assert_eq!(
        sample.outcome.rendered_value.as_deref(),
        Some(expected_rendered_value),
        "{label} typed aggregate-customer perf sample should render the expected scalar value",
    );
    assert_eq!(
        sample.outcome.row_count, None,
        "{label} typed aggregate-customer perf sample should stay scalar",
    );
    assert_eq!(
        sample.outcome.has_cursor, None,
        "{label} typed aggregate-customer perf sample should not expose cursor state",
    );
}

// Assert one typed aggregate-customer reject path against the shared
// unsupported-error contract.
fn assert_typed_customer_aggregate_reject_case(
    pic: &Pic,
    canister_id: Principal,
    label: &str,
    sql: &str,
    expected_message_fragment: &str,
) {
    // Phase 1: request one aggregate-customer perf sample for a deliberately
    // unsupported SQL shape.
    let sample = sql_perf_sample(
        pic,
        canister_id,
        &SqlPerfRequest {
            surface: SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 5,
        },
    );

    // Phase 2: keep the unsupported-error classification stable across the
    // aggregate reject-path cohort.
    assert_positive_perf_sample(label, &sample);
    assert!(
        !sample.outcome.success,
        "{label} typed aggregate-customer perf sample should fail: {sample:?}",
    );
    assert_eq!(
        sample.outcome.result_kind, "error",
        "{label} typed aggregate-customer perf sample should classify the request as an error",
    );
    assert_eq!(
        sample.outcome.error_kind.as_deref(),
        Some("Runtime(Unsupported)"),
        "{label} typed aggregate-customer perf sample should preserve Runtime::Unsupported",
    );
    assert_eq!(
        sample.outcome.error_origin.as_deref(),
        Some("Query"),
        "{label} typed aggregate-customer perf sample should preserve Query origin",
    );
    assert!(
        sample
            .outcome
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains(expected_message_fragment)),
        "{label} typed aggregate-customer perf sample should preserve the expected guidance",
    );
}

// Assert one generated delete attribution case against the shared delete-stage
// contract plus the expected write outcome surface.
fn assert_generated_delete_attribution_case(
    pic: &Pic,
    canister_id: Principal,
    label: &str,
    sql: &str,
    expected_row_count: Option<u32>,
    expect_concrete_write_outcome: bool,
) -> SqlPerfAttributionSample {
    // Phase 1: request one generated-dispatch attribution sample for the
    // table-driven delete shape under test.
    let sample = sql_perf_attribution_sample(
        pic,
        canister_id,
        &SqlPerfAttributionRequest {
            surface: SqlPerfAttributionSurface::GeneratedDispatch,
            sql: sql.to_string(),
            cursor_token: None,
        },
    );

    // Phase 2: keep positive delete stage accounting and the expected write
    // outcome contract aligned across the generated delete cohort.
    assert!(
        sample.outcome.success,
        "{label} attribution must keep the representative DELETE successful: {sample:?}",
    );
    assert!(
        sample.parse_local_instructions > 0,
        "{label} parse phase must be positive: {sample:?}",
    );
    assert!(
        sample.route_local_instructions > 0,
        "{label} route phase must be positive: {sample:?}",
    );
    assert!(
        sample.lower_local_instructions > 0,
        "{label} lower phase must be positive: {sample:?}",
    );
    assert!(
        sample.dispatch_local_instructions > 0,
        "{label} dispatch phase must be positive: {sample:?}",
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
    assert_eq!(
        sample.outcome.entity.as_deref(),
        Some("Customer"),
        "{label} should stay on the Customer route",
    );

    if let Some(expected_row_count) = expected_row_count {
        assert_eq!(
            sample.outcome.row_count,
            Some(expected_row_count),
            "{label} should report the expected write row count",
        );
    } else if expect_concrete_write_outcome {
        assert!(
            sample.outcome.row_count.is_some(),
            "{label} should still report a concrete write outcome: {sample:?}",
        );
    }

    sample
}

// Assert one grouped attribution sample keeps the shared typed-grouped phase
// accounting contract.
fn assert_positive_grouped_attribution_sample(label: &str, sample: &SqlPerfAttributionSample) {
    assert!(
        sample.outcome.success,
        "{label} grouped attribution must keep the representative GROUP BY SELECT successful: {sample:?}",
    );
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
fn sql_canister_query_lane_explain_execution_surfaces_user_secondary_covering_building_index_full_scan_fallback()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        mark_customer_index_building(pic, canister_id);

        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect(
            "query building-index Customer secondary covering EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
            &["FullScan", "OrderByMaterializedSort", "id", "name"],
            &[
                "CoveringRead",
                "planner_proven",
                "storage_existence_witness",
                "authority_decision",
                "authority_reason",
                "index_state",
            ],
            "building-index Customer secondary covering EXPLAIN EXECUTION should fall back to the planner-visible full-scan route",
        );
    });
}

#[test]
fn sql_canister_query_lane_explain_execution_surfaces_user_secondary_non_covering_without_removed_authority_labels()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT age FROM Customer ORDER BY name ASC LIMIT 2",
        )
        .expect(
            "query non-covering Customer secondary-order EXPLAIN EXECUTION should return an Ok payload",
        );
        assert_explain_route(
            payload,
            "Customer",
            &["cov_read_route", "materialized", "age"],
            &[
                "planner_proven",
                "storage_existence_witness",
                "authority_decision",
                "authority_reason",
                "index_state",
            ],
            "non-covering Customer secondary-order EXPLAIN EXECUTION should stay off the removed authority-label surface",
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
        let cases = [
            DeleteParityCase {
                name: "generated strict direct STARTS_WITH delete",
                direct_sql: "DELETE FROM Customer WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 1",
                like_sql: "DELETE FROM Customer WHERE name LIKE 'a%' ORDER BY id LIMIT 1",
            },
            DeleteParityCase {
                name: "generated direct LOWER(field) STARTS_WITH delete",
                direct_sql: "DELETE FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 1",
                like_sql: "DELETE FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 1",
            },
            DeleteParityCase {
                name: "generated direct LOWER(field) ordered text-range delete",
                direct_sql: "DELETE FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
                like_sql: "DELETE FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 1",
            },
            DeleteParityCase {
                name: "generated direct UPPER(field) STARTS_WITH delete",
                direct_sql: "DELETE FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 1",
                like_sql: "DELETE FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 1",
            },
            DeleteParityCase {
                name: "generated direct UPPER(field) ordered text-range delete",
                direct_sql: "DELETE FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
                like_sql: "DELETE FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 1",
            },
        ];

        for case in &cases {
            assert_delete_parity_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_unsupported_direct_starts_with_surface_matrix_rejects_non_casefold_wrapped_forms()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            UnsupportedStartsWithCase {
                name: "query non-casefold wrapped direct STARTS_WITH",
                sql: "SELECT id, name FROM Customer WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 2",
            },
            UnsupportedStartsWithCase {
                name: "generated direct STARTS_WITH delete wrapper",
                sql: "DELETE FROM Customer WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 1",
            },
            UnsupportedStartsWithCase {
                name: "generated direct STARTS_WITH delete EXPLAIN wrapper",
                sql: "EXPLAIN DELETE FROM Customer WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 1",
            },
            UnsupportedStartsWithCase {
                name: "generated direct STARTS_WITH JSON delete EXPLAIN wrapper",
                sql: "EXPLAIN JSON DELETE FROM Customer WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 1",
            },
        ];

        for case in &cases {
            assert_unsupported_starts_with_case(pic, canister_id, case);
        }
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
// This matrix intentionally keeps the covering projection/explain cohort in
// one contiguous test body so the route family stays auditable as one table.
#[expect(
    clippy::too_many_lines,
    reason = "table-driven covering-route matrix is intentionally kept together"
)]
fn sql_canister_query_lane_user_expression_covering_matrix_preserves_projection_and_explain_routes()
{
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: lock the shared Customer `LOWER(name)` covering family into
        // one table-driven projection and `EXPLAIN EXECUTION` matrix.
        let cases = [
            UserExpressionCoveringCase {
                name: "Customer expression-order covering ASC full-row",
                projection_sql: "SELECT id, name FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
                projection_columns: &["id", "name"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "alice"],
                    &[ANY_PROJECTION_VALUE, "bob"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "cov_read_route",
                    "materialized",
                    "LOWER(name)",
                    "proj_fields",
                    "id",
                    "name",
                ],
                explain_forbidden_tokens: &[],
            },
            UserExpressionCoveringCase {
                name: "Customer expression-order covering ASC key-only",
                projection_sql: "SELECT id FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
                projection_columns: &["id"],
                projection_rows: &[&[ANY_PROJECTION_VALUE], &[ANY_PROJECTION_VALUE]],
                explain_sql: "EXPLAIN EXECUTION SELECT id FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "CoveringRead",
                    "covering_read",
                    "existing_row_mode",
                    "planner_proven",
                    "LOWER(name)",
                    "proj_fields",
                    "id",
                ],
                explain_forbidden_tokens: &[
                    "row_check_required",
                    "authority_decision",
                    "authority_reason",
                    "index_state",
                ],
            },
            UserExpressionCoveringCase {
                name: "Customer expression-order covering DESC full-row",
                projection_sql: "SELECT id, name FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
                projection_columns: &["id", "name"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "charlie"],
                    &[ANY_PROJECTION_VALUE, "bob"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "cov_read_route",
                    "materialized",
                    "LOWER(name)",
                    "proj_fields",
                    "id",
                    "name",
                ],
                explain_forbidden_tokens: &[],
            },
            UserExpressionCoveringCase {
                name: "Customer expression-order covering DESC key-only",
                projection_sql: "SELECT id FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
                projection_columns: &["id"],
                projection_rows: &[&[ANY_PROJECTION_VALUE], &[ANY_PROJECTION_VALUE]],
                explain_sql: "EXPLAIN EXECUTION SELECT id FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "CoveringRead",
                    "covering_read",
                    "existing_row_mode",
                    "planner_proven",
                    "LOWER(name)",
                    "proj_fields",
                    "id",
                ],
                explain_forbidden_tokens: &[
                    "row_check_required",
                    "authority_decision",
                    "authority_reason",
                    "index_state",
                ],
            },
            UserExpressionCoveringCase {
                name: "Customer expression-order covering ASC strict-range key-only",
                projection_sql: "SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
                projection_columns: &["id"],
                projection_rows: &[&[ANY_PROJECTION_VALUE]],
                explain_sql: "EXPLAIN EXECUTION SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "CoveringRead",
                    "covering_read",
                    "existing_row_mode",
                    "planner_proven",
                    "LOWER(name)",
                    "proj_fields",
                    "id",
                ],
                explain_forbidden_tokens: &[
                    "row_check_required",
                    "authority_decision",
                    "authority_reason",
                    "index_state",
                ],
            },
            UserExpressionCoveringCase {
                name: "Customer expression-order covering DESC strict-range key-only",
                projection_sql: "SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
                projection_columns: &["id"],
                projection_rows: &[&[ANY_PROJECTION_VALUE]],
                explain_sql: "EXPLAIN EXECUTION SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "CoveringRead",
                    "covering_read",
                    "existing_row_mode",
                    "planner_proven",
                    "LOWER(name)",
                    "proj_fields",
                    "id",
                ],
                explain_forbidden_tokens: &[
                    "row_check_required",
                    "authority_decision",
                    "authority_reason",
                    "index_state",
                ],
            },
        ];

        // Phase 2: execute each shared case so the projection payload and the
        // paired execution descriptor stay locked to the same route family.
        for case in &cases {
            assert_user_expression_covering_case(pic, canister_id, case);
        }
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
// This matrix intentionally keeps the CustomerOrder covering projection/explain
// cohort in one contiguous test body so the route family stays auditable.
#[expect(
    clippy::too_many_lines,
    reason = "table-driven covering-route matrix is intentionally kept together"
)]
fn sql_canister_query_lane_customer_order_covering_matrix_preserves_projection_and_explain_routes()
{
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: lock the shared CustomerOrder covering family into one
        // table-driven projection and `EXPLAIN EXECUTION` matrix.
        let cases = [
            CustomerOrderCoveringCase {
                name: "CustomerOrder exact-name covering read",
                projection_sql: "SELECT id, name FROM CustomerOrder WHERE name = 'A-101' ORDER BY id ASC LIMIT 1",
                projection_columns: &["id", "name"],
                projection_rows: &[&[ANY_PROJECTION_VALUE, "A-101"]],
                explain_sql: "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name = 'A-101' ORDER BY id ASC LIMIT 1",
                explain_required_tokens: &[
                    "cov_read_route",
                    "covering_read",
                    "covering_fields",
                    "existing_row_mode",
                    "planner_proven",
                    "id",
                    "name",
                ],
                explain_forbidden_tokens: &["row_check_required"],
            },
            CustomerOrderCoveringCase {
                name: "CustomerOrder order-only composite covering ASC",
                projection_sql: "SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
                projection_columns: &["id", "priority", "status"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "10", "Alpha"],
                    &[ANY_PROJECTION_VALUE, "20", "Backlog"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "cov_read_route",
                    "covering_read",
                    "covering_fields",
                    "existing_row_mode",
                    "planner_proven",
                    "id",
                    "priority",
                    "status",
                ],
                explain_forbidden_tokens: &[
                    "row_check_required",
                    "authority_decision",
                    "authority_reason",
                    "index_state",
                ],
            },
            CustomerOrderCoveringCase {
                name: "CustomerOrder order-only composite covering DESC",
                projection_sql: "SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
                projection_columns: &["id", "priority", "status"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "30", "Closed"],
                    &[ANY_PROJECTION_VALUE, "20", "Draft"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "cov_read_route",
                    "covering_read",
                    "covering_fields",
                    "existing_row_mode",
                    "planner_proven",
                    "id",
                    "priority",
                    "status",
                ],
                explain_forbidden_tokens: &[
                    "row_check_required",
                    "authority_decision",
                    "authority_reason",
                    "index_state",
                ],
            },
            CustomerOrderCoveringCase {
                name: "CustomerOrder numeric-equality covering ASC",
                projection_sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
                projection_columns: &["id", "priority", "status"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "20", "Backlog"],
                    &[ANY_PROJECTION_VALUE, "20", "Billing"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "CoveringRead",
                    "cov_read_route",
                    "covering_read",
                    "covering_fields",
                    "existing_row_mode",
                    "planner_proven",
                    "id",
                    "priority",
                    "status",
                ],
                explain_forbidden_tokens: &["row_check_required"],
            },
            CustomerOrderCoveringCase {
                name: "CustomerOrder numeric-equality covering DESC",
                projection_sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
                projection_columns: &["id", "priority", "status"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "20", "Draft"],
                    &[ANY_PROJECTION_VALUE, "20", "Closed"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "CoveringRead",
                    "cov_read_route",
                    "covering_read",
                    "covering_fields",
                    "existing_row_mode",
                    "planner_proven",
                    "id",
                    "priority",
                    "status",
                ],
                explain_forbidden_tokens: &["row_check_required"],
            },
            CustomerOrderCoveringCase {
                name: "CustomerOrder numeric-equality bounded-status covering ASC",
                projection_sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status ASC, id ASC LIMIT 2",
                projection_columns: &["id", "priority", "status"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "20", "Backlog"],
                    &[ANY_PROJECTION_VALUE, "20", "Billing"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "CoveringRead",
                    "cov_read_route",
                    "covering_read",
                    "covering_fields",
                    "existing_row_mode",
                    "planner_proven",
                    "id",
                    "priority",
                    "status",
                ],
                explain_forbidden_tokens: &["row_check_required"],
            },
            CustomerOrderCoveringCase {
                name: "CustomerOrder numeric-equality bounded-status covering DESC",
                projection_sql: "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status DESC, id DESC LIMIT 2",
                projection_columns: &["id", "priority", "status"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "20", "Closed"],
                    &[ANY_PROJECTION_VALUE, "20", "Billing"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "CoveringRead",
                    "cov_read_route",
                    "covering_read",
                    "covering_fields",
                    "existing_row_mode",
                    "planner_proven",
                    "id",
                    "priority",
                    "status",
                ],
                explain_forbidden_tokens: &["row_check_required"],
            },
        ];

        // Phase 2: execute each shared case so the projection payload and the
        // paired execution descriptor stay locked to the same route family.
        for case in &cases {
            assert_customer_order_covering_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_customer_account_filtered_simple_covering_matrix_preserves_projection_and_explain_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerAccountFilteredCoveringCase {
                name: "CustomerAccount filtered order-only covering ASC",
                projection_sql: "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
                projection_columns: &["id", "name"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "bravo"],
                    &[ANY_PROJECTION_VALUE, "charlie"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "cov_read_route",
                    "covering_read",
                    "covering_fields",
                    "id",
                    "name",
                ],
                explain_forbidden_tokens: &[],
            },
            CustomerAccountFilteredCoveringCase {
                name: "CustomerAccount filtered order-only covering DESC",
                projection_sql: "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
                projection_columns: &["id", "name"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "echo"],
                    &[ANY_PROJECTION_VALUE, "charlie"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "cov_read_route",
                    "covering_read",
                    "covering_fields",
                    "id",
                    "name",
                ],
                explain_forbidden_tokens: &[],
            },
            CustomerAccountFilteredCoveringCase {
                name: "CustomerAccount filtered strict LIKE prefix covering ASC",
                projection_sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
                projection_columns: &["id", "name"],
                projection_rows: &[&[ANY_PROJECTION_VALUE, "bravo"]],
                explain_sql: "EXPLAIN EXECUTION SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "covering_read",
                    "cov_read_route",
                    "OrderByAccessSatisfied",
                    "proj_fields",
                    "id",
                    "name",
                ],
                explain_forbidden_tokens: &[],
            },
            CustomerAccountFilteredCoveringCase {
                name: "CustomerAccount filtered strict LIKE prefix covering DESC",
                projection_sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
                projection_columns: &["id", "name"],
                projection_rows: &[&[ANY_PROJECTION_VALUE, "bravo"]],
                explain_sql: "EXPLAIN EXECUTION SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "covering_read",
                    "cov_read_route",
                    "OrderByAccessSatisfied",
                    "proj_fields",
                    "id",
                    "name",
                ],
                explain_forbidden_tokens: &[],
            },
        ];

        for case in &cases {
            assert_customer_account_filtered_covering_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_customer_account_filtered_simple_prefix_parity_matrix_preserves_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerAccountFilteredPrefixParityCase {
                name: "CustomerAccount filtered strict prefix parity ASC",
                like_sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
                starts_with_sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name ASC, id ASC LIMIT 1",
                range_sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name ASC, id ASC LIMIT 1",
            },
            CustomerAccountFilteredPrefixParityCase {
                name: "CustomerAccount filtered strict prefix parity DESC",
                like_sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
                starts_with_sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name DESC, id DESC LIMIT 1",
                range_sql: "SELECT id, name FROM CustomerAccount WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name DESC, id DESC LIMIT 1",
            },
        ];

        for case in &cases {
            assert_customer_account_filtered_prefix_parity_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_returns_projection_payload_for_global_aggregate_execution() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_projection_rows(
            pic,
            canister_id,
            "SELECT COUNT(*) FROM Customer",
            "query global aggregate SQL execution should return projection payload",
        );

        assert_projection_window(
            &payload,
            "Customer",
            &["COUNT(*)"],
            &[&["3"]],
            "global aggregate query payload should render one scalar aggregate row through the unified query surface",
        );
    });
}

#[test]
fn sql_canister_query_lane_returns_grouped_payload_for_grouped_sql_execution() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_grouped_rows(
            pic,
            canister_id,
            "SELECT age, COUNT(*) FROM Customer GROUP BY age ORDER BY age ASC LIMIT 10",
            "query grouped SQL execution should return grouped payload",
        );

        assert_eq!(
            payload.columns,
            vec!["age".to_string(), "COUNT(*)".to_string()],
            "grouped query payload should preserve grouped projection labels",
        );
        assert_eq!(
            payload.rows,
            vec![
                vec!["24".to_string(), "1".to_string()],
                vec!["31".to_string(), "1".to_string()],
                vec!["43".to_string(), "1".to_string()],
            ],
            "grouped query payload should render grouped rows through the unified query surface",
        );
        assert_eq!(
            payload.row_count, 3,
            "grouped query payload should report grouped row count"
        );
        assert!(
            payload.next_cursor.is_none(),
            "grouped query payload should not emit continuation cursor for one-page result",
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
// This grouped explain matrix is intentionally table-driven in one place so
// ordered grouped route tokens stay locked across the whole admitted cohort.
#[expect(
    clippy::too_many_lines,
    reason = "table-driven grouped explain matrix is intentionally kept together"
)]
fn sql_canister_query_lane_grouped_ordered_explain_matrix_preserves_grouped_route_tokens() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        // Phase 1: lock the ordered grouped Customer explain family into one
        // table-driven matrix across logical and execution explain surfaces.
        let cases = [
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN ordered group order-only COUNT(*)",
                sql: "EXPLAIN SELECT name, COUNT(*) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "access=IndexRange",
                    "grouping=Grouped { strategy: \"ordered_group\", fallback_reason: None",
                ],
            },
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN ordered group exact-prefix COUNT(*)",
                sql: "EXPLAIN SELECT name, COUNT(*) FROM Customer WHERE name = 'alice' GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "access=IndexPrefix",
                    "grouping=Grouped { strategy: \"ordered_group\", fallback_reason: None",
                ],
            },
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN ordered group order-only COUNT(field)",
                sql: "EXPLAIN SELECT name, COUNT(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "access=IndexRange",
                    "grouping=Grouped { strategy: \"ordered_group\", fallback_reason: None",
                ],
            },
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN ordered group exact-prefix COUNT(field)",
                sql: "EXPLAIN SELECT name, COUNT(age) FROM Customer WHERE name = 'alice' GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "access=IndexPrefix",
                    "grouping=Grouped { strategy: \"ordered_group\", fallback_reason: None",
                ],
            },
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN ordered group order-only SUM(field)",
                sql: "EXPLAIN SELECT name, SUM(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "access=IndexRange",
                    "grouping=Grouped { strategy: \"ordered_group\", fallback_reason: None",
                ],
            },
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN ordered group order-only AVG(field)",
                sql: "EXPLAIN SELECT name, AVG(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "access=IndexRange",
                    "grouping=Grouped { strategy: \"ordered_group\", fallback_reason: None",
                ],
            },
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN ordered group exact-prefix AVG(field)",
                sql: "EXPLAIN SELECT name, AVG(age) FROM Customer WHERE name = 'alice' GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "access=IndexPrefix",
                    "grouping=Grouped { strategy: \"ordered_group\", fallback_reason: None",
                ],
            },
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN EXECUTION ordered group order-only COUNT(*)",
                sql: "EXPLAIN EXECUTION SELECT name, COUNT(*) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "IndexRangeScan",
                    "OrderByAccessSatisfied",
                    "GroupedAggregateOrderedMaterialized",
                    "grouped_plan_fallback_reason=Text(\"none\")",
                    "grouped_execution_mode=Text(\"ordered_materialized\")",
                ],
            },
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN EXECUTION ordered group exact-prefix COUNT(*)",
                sql: "EXPLAIN EXECUTION SELECT name, COUNT(*) FROM Customer WHERE name = 'alice' GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "IndexPrefixScan",
                    "GroupedAggregateOrderedMaterialized",
                    "grouped_plan_fallback_reason=Text(\"none\")",
                    "grouped_execution_mode=Text(\"ordered_materialized\")",
                ],
            },
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN EXECUTION ordered group order-only COUNT(field)",
                sql: "EXPLAIN EXECUTION SELECT name, COUNT(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "IndexRangeScan",
                    "OrderByAccessSatisfied",
                    "GroupedAggregateOrderedMaterialized",
                    "grouped_plan_fallback_reason=Text(\"none\")",
                    "grouped_execution_mode=Text(\"ordered_materialized\")",
                ],
            },
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN EXECUTION ordered group exact-prefix COUNT(field)",
                sql: "EXPLAIN EXECUTION SELECT name, COUNT(age) FROM Customer WHERE name = 'alice' GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "IndexPrefixScan",
                    "GroupedAggregateOrderedMaterialized",
                    "grouped_plan_fallback_reason=Text(\"none\")",
                    "grouped_execution_mode=Text(\"ordered_materialized\")",
                ],
            },
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN EXECUTION ordered group order-only SUM(field)",
                sql: "EXPLAIN EXECUTION SELECT name, SUM(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "IndexRangeScan",
                    "OrderByAccessSatisfied",
                    "GroupedAggregateOrderedMaterialized",
                    "grouped_plan_fallback_reason=Text(\"none\")",
                    "grouped_execution_mode=Text(\"ordered_materialized\")",
                ],
            },
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN EXECUTION ordered group order-only AVG(field)",
                sql: "EXPLAIN EXECUTION SELECT name, AVG(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "IndexRangeScan",
                    "OrderByAccessSatisfied",
                    "GroupedAggregateOrderedMaterialized",
                    "grouped_plan_fallback_reason=Text(\"none\")",
                    "grouped_execution_mode=Text(\"ordered_materialized\")",
                ],
            },
            GroupedOrderedExplainCase {
                name: "Customer grouped EXPLAIN EXECUTION ordered group exact-prefix AVG(field)",
                sql: "EXPLAIN EXECUTION SELECT name, AVG(age) FROM Customer WHERE name = 'alice' GROUP BY name ORDER BY name ASC LIMIT 10",
                required_tokens: &[
                    "IndexPrefixScan",
                    "GroupedAggregateOrderedMaterialized",
                    "grouped_plan_fallback_reason=Text(\"none\")",
                    "grouped_execution_mode=Text(\"ordered_materialized\")",
                ],
            },
        ];

        // Phase 2: execute each grouped explain case so the logical and
        // execution surfaces stay pinned to the admitted ordered-group route.
        for case in &cases {
            assert_grouped_ordered_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_supports_global_aggregate_explain() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(pic, canister_id, "EXPLAIN SELECT COUNT(*) FROM Customer")
            .expect("query global aggregate EXPLAIN should return an Ok payload");

        assert_explain_route(
            payload,
            "Customer",
            &["mode=Load", "access="],
            &[],
            "global aggregate EXPLAIN should return a Customer logical explain payload",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_global_aggregate_explain_respects_customer_index_visibility() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sql = "EXPLAIN SELECT COUNT(*) FROM Customer WHERE name = 'alice'";

        let ready_payload = query_result(pic, canister_id, sql)
            .expect("query filtered global aggregate EXPLAIN should return an Ok payload while the Customer index is ready");
        assert_explain_route(
            ready_payload,
            "Customer",
            &["access=IndexPrefix"],
            &["access=FullScan"],
            "ready filtered global aggregate EXPLAIN should keep the planner-visible Customer name index",
        );

        mark_customer_index_building(pic, canister_id);

        let building_payload = query_result(pic, canister_id, sql)
            .expect("query filtered global aggregate EXPLAIN should return an Ok payload after the Customer index becomes building");
        assert_explain_route(
            building_payload,
            "Customer",
            &["access=FullScan"],
            &["access=IndexPrefix"],
            "building filtered global aggregate EXPLAIN should fall back once the Customer name index becomes planner-invisible",
        );
    });
}

#[test]
fn sql_canister_query_lane_supports_global_aggregate_explain_execution() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT COUNT(*) FROM Customer",
        )
        .expect("query global aggregate EXPLAIN EXECUTION should return an Ok payload");

        assert_explain_route(
            payload,
            "Customer",
            &["AggregateCount execution_mode=", "node_id=0"],
            &[],
            "global aggregate EXPLAIN EXECUTION should expose the aggregate terminal descriptor",
        );
    });
}

#[test]
fn sql_canister_query_lane_filtered_global_aggregate_explain_execution_respects_customer_index_visibility()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let sql = "EXPLAIN EXECUTION SELECT COUNT(*) FROM Customer WHERE name = 'alice'";

        let ready_payload = query_result(pic, canister_id, sql)
            .expect("query filtered global aggregate EXPLAIN EXECUTION should return an Ok payload while the Customer index is ready");
        assert_explain_route(
            ready_payload,
            "Customer",
            &["AggregateCount execution_mode=", "access=IndexPrefix"],
            &[
                "access=FullScan",
                "authority_decision",
                "authority_reason",
                "index_state",
            ],
            "ready filtered global aggregate EXPLAIN EXECUTION should keep the planner-visible Customer name index and stay off the removed secondary-read label surface",
        );

        mark_customer_index_building(pic, canister_id);

        let building_payload = query_result(pic, canister_id, sql)
            .expect("query filtered global aggregate EXPLAIN EXECUTION should return an Ok payload after the Customer index becomes building");
        assert_explain_route(
            building_payload,
            "Customer",
            &["AggregateCount execution_mode=", "access=FullScan"],
            &[
                "access=IndexPrefix",
                "authority_decision",
                "authority_reason",
                "index_state",
            ],
            "building filtered global aggregate EXPLAIN EXECUTION should fall back once the Customer name index becomes planner-invisible and should stay off the removed secondary-read label surface",
        );
    });
}

#[test]
fn sql_canister_query_lane_global_aggregate_explain_execution_stays_off_secondary_authority_surface()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN EXECUTION SELECT COUNT(*) FROM Customer",
        )
        .expect("query global aggregate EXPLAIN EXECUTION should return an Ok payload");

        match payload {
            SqlQueryResult::Explain { entity, explain } => {
                assert_eq!(
                    entity, "Customer",
                    "global aggregate EXPLAIN EXECUTION should return a Customer explain payload",
                );
                assert!(
                    !explain.contains("authority_decision")
                        && !explain.contains("authority_reason")
                        && !explain.contains("index_state"),
                    "aggregate EXPLAIN EXECUTION should stay off the removed secondary-read label surface",
                );
            }
            other => panic!(
                "global aggregate EXPLAIN EXECUTION should return an explain payload: {other:?}"
            ),
        }
    });
}

#[test]
fn sql_canister_perf_typed_execute_sql_aggregate_customer_matrix_surfaces_expected_values() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        for (label, sql, expected_rendered_value) in
            TYPED_CUSTOMER_AGGREGATE_PERF_CASES.iter().copied()
        {
            assert_typed_customer_aggregate_perf_case(
                pic,
                canister_id,
                label,
                sql,
                expected_rendered_value,
            );
        }
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_delete_attribution_matrix_reports_positive_stages() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let mut rows = Vec::new();

        for (label, sql, expected_row_count, expect_concrete_write_outcome) in
            GENERATED_DELETE_ATTRIBUTION_CASES.iter().copied()
        {
            let sample = assert_generated_delete_attribution_case(
                pic,
                canister_id,
                label,
                sql,
                expected_row_count,
                expect_concrete_write_outcome,
            );

            rows.push(serde_json::json!({
                "label": label,
                "sample": sample,
            }));
        }

        println!(
            "{}",
            serde_json::to_string_pretty(&rows)
                .expect("generated delete attribution samples should serialize to JSON")
        );
    });
}

#[test]
fn sql_canister_perf_typed_execute_sql_grouped_customer_matrix_surfaces_expected_values() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        for (label, sql) in TYPED_GROUPED_CUSTOMER_PERF_CASES.iter().copied() {
            assert_typed_grouped_customer_perf_case(pic, canister_id, label, sql);
        }
    });
}

#[test]
fn sql_canister_perf_typed_dispatch_projection_matrix_surfaces_expected_values() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        for case in TYPED_DISPATCH_PROJECTION_PERF_CASES.iter().copied() {
            assert_typed_dispatch_projection_perf_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_perf_typed_execute_sql_aggregate_customer_reject_matrix_reports_expected_errors() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        for (label, sql, expected_message_fragment) in
            TYPED_CUSTOMER_AGGREGATE_REJECT_CASES.iter().copied()
        {
            assert_typed_customer_aggregate_reject_case(
                pic,
                canister_id,
                label,
                sql,
                expected_message_fragment,
            );
        }
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
fn sql_canister_query_lane_customer_casefold_range_delete_explain_matrix_preserves_index_range_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerCasefoldRangeExplainCase {
                name: "Customer direct LOWER(name) text-range delete explain route",
                sql: "EXPLAIN DELETE FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
                required_tokens: &[
                    "mode=Delete",
                    "access=IndexRange",
                    "Customer|LOWER(name)",
                    "lower: Included(Text(\"a\"))",
                    "upper: Excluded(Text(\"b\"))",
                ],
                forbidden_tokens: &["access=FullScan"],
            },
            CustomerCasefoldRangeExplainCase {
                name: "Customer direct UPPER(name) text-range delete explain route",
                sql: "EXPLAIN DELETE FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
                required_tokens: &[
                    "mode=Delete",
                    "access=IndexRange",
                    "Customer|LOWER(name)",
                    "lower: Included(Text(\"a\"))",
                    "upper: Excluded(Text(\"b\"))",
                ],
                forbidden_tokens: &["access=FullScan"],
            },
        ];

        for case in &cases {
            assert_customer_casefold_range_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_customer_casefold_range_json_explain_matrix_preserves_index_range_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerCasefoldRangeExplainCase {
                name: "Customer direct LOWER(name) text-range JSON explain route",
                sql: "EXPLAIN JSON SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Load\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                    "\"predicate\":\"And([Compare",
                    "id: TextCasefold",
                ],
                forbidden_tokens: &["\"type\":\"FullScan\""],
            },
            CustomerCasefoldRangeExplainCase {
                name: "Customer direct UPPER(name) text-range JSON explain route",
                sql: "EXPLAIN JSON SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Load\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                    "\"predicate\":\"And([Compare",
                    "id: TextCasefold",
                ],
                forbidden_tokens: &["\"type\":\"FullScan\""],
            },
        ];

        for case in &cases {
            assert_customer_casefold_range_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_customer_casefold_prefix_json_explain_matrix_preserves_index_range_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerCasefoldPrefixExplainCase {
                name: "Customer direct LOWER(name) prefix JSON explain parity",
                like_sql: "EXPLAIN JSON SELECT id, name FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 2",
                starts_with_sql: "EXPLAIN JSON SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
                range_sql: "EXPLAIN JSON SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Load\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                ],
                forbidden_tokens: &["\"type\":\"FullScan\""],
            },
            CustomerCasefoldPrefixExplainCase {
                name: "Customer direct UPPER(name) prefix JSON explain parity",
                like_sql: "EXPLAIN JSON SELECT id, name FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 2",
                starts_with_sql: "EXPLAIN JSON SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
                range_sql: "EXPLAIN JSON SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Load\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                ],
                forbidden_tokens: &["\"type\":\"FullScan\""],
            },
        ];

        for case in &cases {
            assert_customer_casefold_prefix_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_customer_casefold_range_json_delete_explain_matrix_preserves_index_range_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerCasefoldRangeExplainCase {
                name: "Customer direct LOWER(name) text-range JSON delete explain route",
                sql: "EXPLAIN JSON DELETE FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Delete\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                    "\"predicate\":\"And([Compare",
                    "id: TextCasefold",
                ],
                forbidden_tokens: &["\"type\":\"FullScan\""],
            },
            CustomerCasefoldRangeExplainCase {
                name: "Customer direct UPPER(name) text-range JSON delete explain route",
                sql: "EXPLAIN JSON DELETE FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Delete\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                    "\"predicate\":\"And([Compare",
                    "id: TextCasefold",
                ],
                forbidden_tokens: &["\"type\":\"FullScan\""],
            },
        ];

        for case in &cases {
            assert_customer_casefold_range_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_customer_casefold_prefix_json_delete_explain_matrix_preserves_index_range_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerCasefoldPrefixExplainCase {
                name: "Customer direct LOWER(name) prefix JSON delete explain parity",
                like_sql: "EXPLAIN JSON DELETE FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 1",
                starts_with_sql: "EXPLAIN JSON DELETE FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 1",
                range_sql: "EXPLAIN JSON DELETE FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Delete\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                ],
                forbidden_tokens: &["\"type\":\"FullScan\""],
            },
            CustomerCasefoldPrefixExplainCase {
                name: "Customer direct UPPER(name) prefix JSON delete explain parity",
                like_sql: "EXPLAIN JSON DELETE FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 1",
                starts_with_sql: "EXPLAIN JSON DELETE FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 1",
                range_sql: "EXPLAIN JSON DELETE FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Delete\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                ],
                forbidden_tokens: &["\"type\":\"FullScan\""],
            },
        ];

        for case in &cases {
            assert_customer_casefold_prefix_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_customer_order_strict_prefix_projection_matrix_preserves_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerOrderStrictPrefixProjectionCase {
                name: "CustomerOrder strict prefix projection parity ASC",
                like_sql: "SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
                starts_with_sql: "SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2",
                range_sql: "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
                expected_rows: &[
                    &[ANY_PROJECTION_VALUE, "A-100"],
                    &[ANY_PROJECTION_VALUE, "A-101"],
                ],
            },
            CustomerOrderStrictPrefixProjectionCase {
                name: "CustomerOrder strict prefix projection parity DESC",
                like_sql: "SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
                starts_with_sql: "SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2",
                range_sql: "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
                expected_rows: &[
                    &[ANY_PROJECTION_VALUE, "A-102"],
                    &[ANY_PROJECTION_VALUE, "A-101"],
                ],
            },
        ];

        for case in &cases {
            assert_customer_order_strict_prefix_projection_case(pic, canister_id, case);
        }
    });
}

#[test]
// This filtered-composite covering matrix is intentionally kept in one test so
// its projection and explain routes remain auditable as one cohort.
#[expect(
    clippy::too_many_lines,
    reason = "table-driven covering-route matrix is intentionally kept together"
)]
fn sql_canister_query_lane_customer_account_filtered_composite_covering_matrix_preserves_projection_and_explain_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerAccountFilteredCompositeCoveringCase {
                name: "CustomerAccount filtered composite strict LIKE prefix covering ASC",
                projection_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
                projection_columns: &["id", "tier", "handle"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                    &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "covering_read",
                    "cov_read_route",
                    "existing_row_mode",
                    "planner_proven",
                    "prefix_len",
                    "Uint(1)",
                    "prefix_values",
                    "gold",
                    "OrderByAccessSatisfied",
                    "proj_fields",
                    "tier",
                    "handle",
                ],
                explain_forbidden_tokens: &["row_check_required"],
            },
            CustomerAccountFilteredCompositeCoveringCase {
                name: "CustomerAccount filtered composite strict LIKE prefix covering DESC",
                projection_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
                projection_columns: &["id", "tier", "handle"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                    &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "covering_read",
                    "cov_read_route",
                    "existing_row_mode",
                    "planner_proven",
                    "prefix_len",
                    "Uint(1)",
                    "prefix_values",
                    "gold",
                    "OrderByAccessSatisfied",
                    "proj_fields",
                    "tier",
                    "handle",
                ],
                explain_forbidden_tokens: &["row_check_required"],
            },
            CustomerAccountFilteredCompositeCoveringCase {
                name: "CustomerAccount filtered composite order-only covering ASC",
                projection_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
                projection_columns: &["id", "tier", "handle"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                    &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "IndexPrefixScan",
                    "covering_read",
                    "cov_read_route",
                    "existing_row_mode",
                    "planner_proven",
                    "prefix_len",
                    "Uint(1)",
                    "prefix_values",
                    "gold",
                    "OrderByAccessSatisfied",
                    "proj_fields",
                    "tier",
                    "handle",
                ],
                explain_forbidden_tokens: &["row_check_required"],
            },
            CustomerAccountFilteredCompositeCoveringCase {
                name: "CustomerAccount filtered composite order-only covering DESC",
                projection_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
                projection_columns: &["id", "tier", "handle"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                    &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "IndexPrefixScan",
                    "covering_read",
                    "cov_read_route",
                    "existing_row_mode",
                    "planner_proven",
                    "prefix_len",
                    "Uint(1)",
                    "prefix_values",
                    "gold",
                    "OrderByMaterializedSort",
                    "proj_fields",
                    "tier",
                    "handle",
                ],
                explain_forbidden_tokens: &[
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                    "row_check_required",
                ],
            },
            CustomerAccountFilteredCompositeCoveringCase {
                name: "CustomerAccount filtered composite order-only offset covering DESC",
                projection_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
                projection_columns: &["id", "tier", "handle"],
                projection_rows: &[&[ANY_PROJECTION_VALUE, "gold", "bravo"]],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
                explain_required_tokens: &[
                    "IndexPrefixScan",
                    "covering_read",
                    "cov_read_route",
                    "existing_row_mode",
                    "planner_proven",
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
                explain_forbidden_tokens: &[
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                    "row_check_required",
                ],
            },
            CustomerAccountFilteredCompositeCoveringCase {
                name: "CustomerAccount filtered composite residual covering DESC",
                projection_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND name >= 'a' ORDER BY handle DESC, id DESC LIMIT 2",
                projection_columns: &["id", "tier", "handle"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                    &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND name >= 'a' ORDER BY handle DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "cov_read_route",
                    "materialized",
                    "ResidualPredicateFilter",
                    "OrderByMaterializedSort",
                    "proj_fields",
                    "tier",
                    "handle",
                ],
                explain_forbidden_tokens: &["TopNSeek", "OrderByAccessSatisfied"],
            },
        ];

        for case in &cases {
            assert_customer_account_filtered_composite_covering_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_customer_account_filtered_composite_prefix_parity_matrix_preserves_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerAccountFilteredCompositePrefixParityCase {
                name: "CustomerAccount filtered composite strict prefix parity ASC",
                like_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
                starts_with_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle ASC, id ASC LIMIT 2",
                range_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle ASC, id ASC LIMIT 2",
            },
            CustomerAccountFilteredCompositePrefixParityCase {
                name: "CustomerAccount filtered composite strict prefix parity DESC",
                like_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
                starts_with_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle DESC, id DESC LIMIT 2",
                range_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle DESC, id DESC LIMIT 2",
            },
        ];

        for case in &cases {
            assert_customer_account_filtered_composite_prefix_parity_case(pic, canister_id, case);
        }
    });
}

#[test]
// This filtered-expression covering matrix is intentionally kept in one test
// so its projection and explain routes remain auditable as one cohort.
#[expect(
    clippy::too_many_lines,
    reason = "table-driven covering-route matrix is intentionally kept together"
)]
fn sql_canister_query_lane_customer_account_filtered_expression_covering_matrix_preserves_projection_and_explain_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerAccountFilteredExpressionCoveringCase {
                name: "CustomerAccount filtered expression order-only covering ASC",
                projection_sql: "SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                projection_columns: &["id", "handle"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "bravo"],
                    &[ANY_PROJECTION_VALUE, "Brisk"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "cov_read_route",
                    "materialized",
                    "LOWER(handle)",
                    "OrderByAccessSatisfied",
                    "proj_fields",
                    "id",
                    "handle",
                ],
                explain_forbidden_tokens: &[],
            },
            CustomerAccountFilteredExpressionCoveringCase {
                name: "CustomerAccount filtered expression order-only covering DESC",
                projection_sql: "SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                projection_columns: &["id", "handle"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "bristle"],
                    &[ANY_PROJECTION_VALUE, "Brisk"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "cov_read_route",
                    "materialized",
                    "LOWER(handle)",
                    "OrderByAccessSatisfied",
                    "proj_fields",
                    "id",
                    "handle",
                ],
                explain_forbidden_tokens: &[],
            },
            CustomerAccountFilteredExpressionCoveringCase {
                name: "CustomerAccount filtered expression strict LIKE prefix covering ASC",
                projection_sql: "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                projection_columns: &["id", "handle"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "bravo"],
                    &[ANY_PROJECTION_VALUE, "Brisk"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "cov_read_route",
                    "materialized",
                    "LOWER(handle)",
                    "OrderByAccessSatisfied",
                    "proj_fields",
                    "id",
                    "handle",
                ],
                explain_forbidden_tokens: &[],
            },
            CustomerAccountFilteredExpressionCoveringCase {
                name: "CustomerAccount filtered expression strict text-range covering ASC",
                projection_sql: "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                projection_columns: &["id", "handle"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "bravo"],
                    &[ANY_PROJECTION_VALUE, "Brisk"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "cov_read_route",
                    "materialized",
                    "LOWER(handle)",
                    "OrderByAccessSatisfied",
                    "proj_fields",
                    "id",
                    "handle",
                ],
                explain_forbidden_tokens: &[],
            },
            CustomerAccountFilteredExpressionCoveringCase {
                name: "CustomerAccount filtered expression strict LIKE prefix covering DESC",
                projection_sql: "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                projection_columns: &["id", "handle"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "bristle"],
                    &[ANY_PROJECTION_VALUE, "Brisk"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "cov_read_route",
                    "materialized",
                    "LOWER(handle)",
                    "OrderByAccessSatisfied",
                    "proj_fields",
                    "id",
                    "handle",
                ],
                explain_forbidden_tokens: &[],
            },
            CustomerAccountFilteredExpressionCoveringCase {
                name: "CustomerAccount filtered expression strict text-range covering DESC",
                projection_sql: "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                projection_columns: &["id", "handle"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "bristle"],
                    &[ANY_PROJECTION_VALUE, "Brisk"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "cov_read_route",
                    "materialized",
                    "LOWER(handle)",
                    "OrderByAccessSatisfied",
                    "proj_fields",
                    "id",
                    "handle",
                ],
                explain_forbidden_tokens: &[],
            },
        ];

        for case in &cases {
            assert_customer_account_filtered_expression_covering_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_customer_account_filtered_expression_prefix_parity_matrix_preserves_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerAccountFilteredExpressionPrefixParityCase {
                name: "CustomerAccount filtered expression strict prefix parity ASC",
                like_sql: "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                starts_with_sql: "SELECT id, handle FROM CustomerAccount WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                range_sql: "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                expected_rows: &[
                    &[ANY_PROJECTION_VALUE, "bravo"],
                    &[ANY_PROJECTION_VALUE, "Brisk"],
                ],
            },
            CustomerAccountFilteredExpressionPrefixParityCase {
                name: "CustomerAccount filtered expression strict prefix parity DESC",
                like_sql: "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                starts_with_sql: "SELECT id, handle FROM CustomerAccount WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                range_sql: "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                expected_rows: &[
                    &[ANY_PROJECTION_VALUE, "bristle"],
                    &[ANY_PROJECTION_VALUE, "Brisk"],
                ],
            },
        ];

        for case in &cases {
            assert_customer_account_filtered_expression_prefix_parity_case(pic, canister_id, case);
        }
    });
}

#[test]
// This filtered composite-expression route matrix is intentionally table-driven
// in one place because it locks the largest shared projection/explain cohort.
#[expect(
    clippy::too_many_lines,
    reason = "table-driven composite-expression route matrix is intentionally kept together"
)]
fn sql_canister_query_lane_customer_account_filtered_composite_expression_route_matrix_preserves_projection_and_explain_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerAccountFilteredCompositeExpressionCase {
                name: "CustomerAccount filtered composite expression order-only ASC",
                projection_sql: Some(
                    "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                ),
                projection_columns: &["id", "tier", "handle"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                    &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
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
                explain_forbidden_tokens: &[],
            },
            CustomerAccountFilteredCompositeExpressionCase {
                name: "CustomerAccount filtered composite expression order-only DESC",
                projection_sql: Some(
                    "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                ),
                projection_columns: &["id", "tier", "handle"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                    &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
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
                explain_forbidden_tokens: &[],
            },
            CustomerAccountFilteredCompositeExpressionCase {
                name: "CustomerAccount filtered composite expression key-only order-only ASC",
                projection_sql: Some(
                    "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                ),
                projection_columns: &["id", "tier"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold"],
                    &[ANY_PROJECTION_VALUE, "gold"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "IndexPrefixScan",
                    "covering_read",
                    "cov_read_route",
                    "existing_row_mode",
                    "planner_proven",
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
                explain_forbidden_tokens: &["row_check_required"],
            },
            CustomerAccountFilteredCompositeExpressionCase {
                name: "CustomerAccount filtered composite expression key-only order-only DESC",
                projection_sql: Some(
                    "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                ),
                projection_columns: &["id", "tier"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold"],
                    &[ANY_PROJECTION_VALUE, "gold"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "IndexPrefixScan",
                    "covering_read",
                    "cov_read_route",
                    "existing_row_mode",
                    "planner_proven",
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
                explain_forbidden_tokens: &["row_check_required"],
            },
            CustomerAccountFilteredCompositeExpressionCase {
                name: "CustomerAccount filtered composite expression key-only strict text-range ASC",
                projection_sql: Some(
                    "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                ),
                projection_columns: &["id", "tier"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold"],
                    &[ANY_PROJECTION_VALUE, "gold"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "covering_read",
                    "cov_read_route",
                    "existing_row_mode",
                    "planner_proven",
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
                explain_forbidden_tokens: &["row_check_required"],
            },
            CustomerAccountFilteredCompositeExpressionCase {
                name: "CustomerAccount filtered composite expression key-only strict text-range DESC",
                projection_sql: Some(
                    "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                ),
                projection_columns: &["id", "tier"],
                projection_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold"],
                    &[ANY_PROJECTION_VALUE, "gold"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "covering_read",
                    "cov_read_route",
                    "existing_row_mode",
                    "planner_proven",
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
                explain_forbidden_tokens: &["row_check_required"],
            },
            CustomerAccountFilteredCompositeExpressionCase {
                name: "CustomerAccount filtered composite expression strict LIKE prefix ASC route",
                projection_sql: None,
                projection_columns: &[],
                projection_rows: &[],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
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
                explain_forbidden_tokens: &[],
            },
            CustomerAccountFilteredCompositeExpressionCase {
                name: "CustomerAccount filtered composite expression strict text-range ASC route",
                projection_sql: None,
                projection_columns: &[],
                projection_rows: &[],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
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
                explain_forbidden_tokens: &[],
            },
            CustomerAccountFilteredCompositeExpressionCase {
                name: "CustomerAccount filtered composite expression strict LIKE prefix DESC route",
                projection_sql: None,
                projection_columns: &[],
                projection_rows: &[],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
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
                explain_forbidden_tokens: &[],
            },
            CustomerAccountFilteredCompositeExpressionCase {
                name: "CustomerAccount filtered composite expression strict text-range DESC route",
                projection_sql: None,
                projection_columns: &[],
                projection_rows: &[],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
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
                explain_forbidden_tokens: &[],
            },
        ];

        for case in &cases {
            assert_customer_account_filtered_composite_expression_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_customer_account_filtered_composite_expression_full_row_prefix_parity_matrix_preserves_projection_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerAccountFilteredCompositeExpressionParityCase {
                name: "CustomerAccount filtered composite expression strict prefix parity ASC",
                like_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                starts_with_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                range_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                columns: &["id", "tier", "handle"],
                expected_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                    &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                ],
            },
            CustomerAccountFilteredCompositeExpressionParityCase {
                name: "CustomerAccount filtered composite expression strict prefix parity DESC",
                like_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                starts_with_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                range_sql: "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                columns: &["id", "tier", "handle"],
                expected_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold", "bristle"],
                    &[ANY_PROJECTION_VALUE, "gold", "bravo"],
                ],
            },
        ];

        for case in &cases {
            assert_customer_account_filtered_composite_expression_parity_case(
                pic,
                canister_id,
                case,
            );
        }
    });
}

#[test]
fn sql_canister_query_lane_customer_account_filtered_composite_expression_key_only_prefix_parity_matrix_preserves_projection_and_direct_route()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerAccountFilteredCompositeExpressionKeyOnlyParityCase {
                name: "CustomerAccount filtered composite expression key-only direct prefix parity ASC",
                like_sql: "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                starts_with_sql: "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                range_sql: Some(
                    "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                ),
                columns: &["id", "tier"],
                expected_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold"],
                    &[ANY_PROJECTION_VALUE, "gold"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "covering_read",
                    "cov_read_route",
                    "existing_row_mode",
                    "planner_proven",
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
                explain_forbidden_tokens: &["row_check_required"],
            },
            CustomerAccountFilteredCompositeExpressionKeyOnlyParityCase {
                name: "CustomerAccount filtered composite expression key-only direct prefix parity DESC",
                like_sql: "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                starts_with_sql: "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                range_sql: Some(
                    "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                ),
                columns: &["id", "tier"],
                expected_rows: &[
                    &[ANY_PROJECTION_VALUE, "gold"],
                    &[ANY_PROJECTION_VALUE, "gold"],
                ],
                explain_sql: "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
                explain_required_tokens: &[
                    "IndexRangeScan",
                    "covering_read",
                    "cov_read_route",
                    "existing_row_mode",
                    "planner_proven",
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
                explain_forbidden_tokens: &["row_check_required"],
            },
        ];

        for case in &cases {
            assert_customer_account_filtered_composite_expression_key_only_parity_case(
                pic,
                canister_id,
                case,
            );
        }
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
fn sql_canister_query_lane_customer_casefold_prefix_projection_matrix_preserves_projection_rows() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerCasefoldPrefixProjectionCase {
                name: "Customer direct LOWER(name) prefix projection parity",
                like_sql: "SELECT id, name FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 2",
                starts_with_sql: "SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
                range_sql: "SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
                expected_rows: &[&[ANY_PROJECTION_VALUE, "alice"]],
            },
            CustomerCasefoldPrefixProjectionCase {
                name: "Customer direct UPPER(name) prefix projection parity",
                like_sql: "SELECT id, name FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 2",
                starts_with_sql: "SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
                range_sql: "SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
                expected_rows: &[&[ANY_PROJECTION_VALUE, "alice"]],
            },
        ];

        for case in &cases {
            assert_customer_casefold_prefix_projection_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_customer_casefold_prefix_explain_execution_matrix_preserves_index_range_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerCasefoldPrefixExplainCase {
                name: "Customer direct LOWER(name) prefix EXPLAIN EXECUTION parity",
                like_sql: "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 2",
                starts_with_sql: "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
                range_sql: "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
                required_tokens: &[
                    "IndexRangeScan",
                    "OrderByMaterializedSort",
                    "proj_fields",
                    "id",
                    "name",
                ],
                forbidden_tokens: &["FullScan"],
            },
            CustomerCasefoldPrefixExplainCase {
                name: "Customer direct UPPER(name) prefix EXPLAIN EXECUTION parity",
                like_sql: "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 2",
                starts_with_sql: "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
                range_sql: "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
                required_tokens: &[
                    "IndexRangeScan",
                    "OrderByMaterializedSort",
                    "proj_fields",
                    "id",
                    "name",
                ],
                forbidden_tokens: &["FullScan"],
            },
        ];

        for case in &cases {
            assert_customer_casefold_prefix_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_explain_json_planner_prefix_choice_prefers_order_compatible_index() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN JSON SELECT id, tier FROM PlannerPrefixChoice WHERE tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
        )
        .expect("planner prefix-choice JSON explain should succeed");

        assert_explain_route(
            payload,
            "PlannerPrefixChoice",
            &[
                "\"mode\":{\"type\":\"Load\"",
                "\"access\":{\"type\":\"IndexPrefix\"",
                "\"name\":\"PlannerPrefixChoice|tier|handle\"",
            ],
            &["\"name\":\"PlannerPrefixChoice|tier|label\""],
            "planner prefix-choice JSON explain should lock the order-compatible prefix index",
        );
    });
}

#[test]
fn sql_canister_query_lane_planner_range_choice_json_explain_matrix_prefers_order_compatible_index()
{
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            PlannerExplainCase {
                name: "planner range-choice JSON explain ASC",
                entity: "PlannerChoice",
                sql: "EXPLAIN JSON SELECT id, tier FROM PlannerChoice WHERE tier = 'gold' AND label >= 'br' AND label < 'd' ORDER BY label ASC, handle ASC, id ASC LIMIT 2",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Load\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                    "\"name\":\"PlannerChoice|tier|label|handle\"",
                ],
                forbidden_tokens: &["\"name\":\"PlannerChoice|tier|label|alpha\""],
            },
            PlannerExplainCase {
                name: "planner range-choice JSON explain DESC",
                entity: "PlannerChoice",
                sql: "EXPLAIN JSON SELECT id, tier FROM PlannerChoice WHERE tier = 'gold' AND label >= 'br' AND label < 'd' ORDER BY label DESC, handle DESC, id DESC LIMIT 2",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Load\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                    "\"name\":\"PlannerChoice|tier|label|handle\"",
                ],
                forbidden_tokens: &["\"name\":\"PlannerChoice|tier|label|alpha\""],
            },
        ];

        for case in &cases {
            assert_planner_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_planner_equality_prefix_suffix_order_json_explain_matrix_prefers_order_compatible_index()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            PlannerExplainCase {
                name: "planner equality-prefix suffix-order JSON explain ASC",
                entity: "PlannerChoice",
                sql: "EXPLAIN JSON SELECT id, tier FROM PlannerChoice WHERE tier = 'gold' AND label = 'bravo' ORDER BY handle ASC, id ASC LIMIT 2",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Load\"",
                    "\"access\":{\"type\":\"IndexPrefix\"",
                    "\"name\":\"PlannerChoice|tier|label|handle\"",
                ],
                forbidden_tokens: &["\"name\":\"PlannerChoice|tier|label|alpha\""],
            },
            PlannerExplainCase {
                name: "planner equality-prefix suffix-order JSON explain DESC",
                entity: "PlannerChoice",
                sql: "EXPLAIN JSON SELECT id, tier FROM PlannerChoice WHERE tier = 'gold' AND label = 'bravo' ORDER BY handle DESC, id DESC LIMIT 2",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Load\"",
                    "\"access\":{\"type\":\"IndexPrefix\"",
                    "\"name\":\"PlannerChoice|tier|label|handle\"",
                ],
                forbidden_tokens: &["\"name\":\"PlannerChoice|tier|label|alpha\""],
            },
        ];

        for case in &cases {
            assert_planner_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_planner_equality_prefix_suffix_order_explain_execution_matrix_preserves_ordered_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            PlannerExplainCase {
                name: "planner equality-prefix suffix-order EXPLAIN EXECUTION ASC",
                entity: "PlannerChoice",
                sql: "EXPLAIN EXECUTION SELECT id, tier FROM PlannerChoice WHERE tier = 'gold' AND label = 'bravo' ORDER BY handle ASC, id ASC LIMIT 2",
                required_tokens: &[
                    "IndexPrefixScan",
                    "PlannerChoice|tier|label|handle",
                    "SecondaryOrderPushdown",
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                ],
                forbidden_tokens: &["IndexRangeLimitPushdown"],
            },
            PlannerExplainCase {
                name: "planner equality-prefix suffix-order EXPLAIN EXECUTION DESC",
                entity: "PlannerChoice",
                sql: "EXPLAIN EXECUTION SELECT id, tier FROM PlannerChoice WHERE tier = 'gold' AND label = 'bravo' ORDER BY handle DESC, id DESC LIMIT 2",
                required_tokens: &[
                    "IndexPrefixScan",
                    "PlannerChoice|tier|label|handle",
                    "SecondaryOrderPushdown",
                    "OrderByMaterializedSort",
                    "scan_dir=Text(\"desc\")",
                ],
                forbidden_tokens: &[
                    "IndexRangeLimitPushdown",
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                ],
            },
            PlannerExplainCase {
                name: "planner equality-prefix suffix-order EXPLAIN EXECUTION ASC offset",
                entity: "PlannerChoice",
                sql: "EXPLAIN EXECUTION SELECT tier, handle FROM PlannerChoice WHERE tier = 'gold' AND label = 'bravo' ORDER BY handle ASC, id ASC LIMIT 2 OFFSET 1",
                required_tokens: &[
                    "IndexPrefixScan",
                    "PlannerChoice|tier|label|handle",
                    "SecondaryOrderPushdown",
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                    "offset=Uint(1)",
                ],
                forbidden_tokens: &["IndexRangeLimitPushdown", "OrderByMaterializedSort"],
            },
            PlannerExplainCase {
                name: "planner equality-prefix suffix-order EXPLAIN EXECUTION DESC offset",
                entity: "PlannerChoice",
                sql: "EXPLAIN EXECUTION SELECT tier, handle FROM PlannerChoice WHERE tier = 'gold' AND label = 'bravo' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
                required_tokens: &[
                    "IndexPrefixScan",
                    "PlannerChoice|tier|label|handle",
                    "SecondaryOrderPushdown",
                    "OrderByMaterializedSort",
                    "scan_dir=Text(\"desc\")",
                    "offset=Uint(1)",
                ],
                forbidden_tokens: &[
                    "IndexRangeLimitPushdown",
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                ],
            },
        ];

        for case in &cases {
            assert_planner_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_planner_equality_prefix_suffix_order_offset_projection_matrix_preserves_ordered_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            PlannerProjectionCase {
                name: "planner equality-prefix suffix-order offset projection ASC",
                entity: "PlannerChoice",
                sql: "SELECT tier, handle FROM PlannerChoice WHERE tier = 'gold' AND label = 'bravo' ORDER BY handle ASC, id ASC LIMIT 2 OFFSET 1",
                columns: &["tier", "handle"],
                expected_rows: &[&["gold", "echo"], &["gold", "lima"]],
            },
            PlannerProjectionCase {
                name: "planner equality-prefix suffix-order offset projection DESC",
                entity: "PlannerChoice",
                sql: "SELECT tier, handle FROM PlannerChoice WHERE tier = 'gold' AND label = 'bravo' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
                columns: &["tier", "handle"],
                expected_rows: &[&["gold", "echo"], &["gold", "charlie"]],
            },
        ];

        for case in &cases {
            assert_planner_projection_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_planner_unique_prefix_offset_projection_matrix_preserves_ordered_rows() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            PlannerProjectionCase {
                name: "planner unique-prefix offset projection ASC",
                entity: "PlannerUniquePrefixChoice",
                sql: "SELECT tier, note FROM PlannerUniquePrefixChoice WHERE tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2 OFFSET 1",
                columns: &["tier", "note"],
                expected_rows: &[&["gold", "B"], &["gold", "C"]],
            },
            PlannerProjectionCase {
                name: "planner unique-prefix offset projection DESC",
                entity: "PlannerUniquePrefixChoice",
                sql: "SELECT tier, note FROM PlannerUniquePrefixChoice WHERE tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
                columns: &["tier", "note"],
                expected_rows: &[&["gold", "C"], &["gold", "B"]],
            },
        ];

        for case in &cases {
            assert_planner_projection_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_planner_unique_prefix_offset_explain_execution_matrix_preserves_ordered_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            PlannerExplainCase {
                name: "planner unique-prefix offset EXPLAIN EXECUTION ASC",
                entity: "PlannerUniquePrefixChoice",
                sql: "EXPLAIN EXECUTION SELECT tier, note FROM PlannerUniquePrefixChoice WHERE tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2 OFFSET 1",
                required_tokens: &[
                    "IndexPrefixScan",
                    "PlannerUniquePrefixChoice|tier|handle",
                    "SecondaryOrderPushdown",
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                ],
                forbidden_tokens: &["OrderByMaterializedSort"],
            },
            PlannerExplainCase {
                name: "planner unique-prefix offset EXPLAIN EXECUTION DESC",
                entity: "PlannerUniquePrefixChoice",
                sql: "EXPLAIN EXECUTION SELECT tier, note FROM PlannerUniquePrefixChoice WHERE tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
                required_tokens: &[
                    "IndexPrefixScan",
                    "PlannerUniquePrefixChoice|tier|handle",
                    "SecondaryOrderPushdown",
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                    "scan_dir=Text(\"desc\")",
                ],
                forbidden_tokens: &["OrderByMaterializedSort"],
            },
        ];

        for case in &cases {
            assert_planner_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_planner_range_choice_explain_execution_matrix_preserves_ordered_routes()
{
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            PlannerExplainCase {
                name: "planner range-choice EXPLAIN EXECUTION ASC",
                entity: "PlannerChoice",
                sql: "EXPLAIN EXECUTION SELECT id, tier FROM PlannerChoice WHERE tier = 'gold' AND label >= 'br' AND label < 'd' ORDER BY label ASC, handle ASC, id ASC LIMIT 2",
                required_tokens: &[
                    "IndexRangeScan",
                    "PlannerChoice|tier|label|handle",
                    "SecondaryOrderPushdown",
                    "IndexRangeLimitPushdown",
                    "OrderByAccessSatisfied",
                ],
                forbidden_tokens: &["TopNSeek"],
            },
            PlannerExplainCase {
                name: "planner range-choice EXPLAIN EXECUTION DESC",
                entity: "PlannerChoice",
                sql: "EXPLAIN EXECUTION SELECT id, tier FROM PlannerChoice WHERE tier = 'gold' AND label >= 'br' AND label < 'd' ORDER BY label DESC, handle DESC, id DESC LIMIT 2",
                required_tokens: &[
                    "IndexRangeScan",
                    "PlannerChoice|tier|label|handle",
                    "SecondaryOrderPushdown",
                    "IndexRangeLimitPushdown",
                    "OrderByAccessSatisfied",
                    "scan_dir=Text(\"desc\")",
                ],
                forbidden_tokens: &["TopNSeek"],
            },
            PlannerExplainCase {
                name: "planner range-choice EXPLAIN EXECUTION ASC offset",
                entity: "PlannerChoice",
                sql: "EXPLAIN EXECUTION SELECT id, tier FROM PlannerChoice WHERE tier = 'gold' AND label >= 'br' AND label < 'd' ORDER BY label ASC, handle ASC, id ASC LIMIT 2 OFFSET 1",
                required_tokens: &[
                    "IndexRangeScan",
                    "PlannerChoice|tier|label|handle",
                    "SecondaryOrderPushdown",
                    "IndexRangeLimitPushdown",
                    "OrderByAccessSatisfied",
                    "offset=Uint(1)",
                ],
                forbidden_tokens: &["TopNSeek"],
            },
            PlannerExplainCase {
                name: "planner range-choice EXPLAIN EXECUTION DESC offset",
                entity: "PlannerChoice",
                sql: "EXPLAIN EXECUTION SELECT id, tier FROM PlannerChoice WHERE tier = 'gold' AND label >= 'br' AND label < 'd' ORDER BY label DESC, handle DESC, id DESC LIMIT 2 OFFSET 1",
                required_tokens: &[
                    "IndexRangeScan",
                    "PlannerChoice|tier|label|handle",
                    "SecondaryOrderPushdown",
                    "IndexRangeLimitPushdown",
                    "OrderByAccessSatisfied",
                    "scan_dir=Text(\"desc\")",
                    "offset=Uint(1)",
                ],
                forbidden_tokens: &["TopNSeek"],
            },
        ];

        for case in &cases {
            assert_planner_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_planner_range_choice_offset_projection_matrix_preserves_ordered_rows() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            PlannerProjectionCase {
                name: "planner range-choice offset projection ASC",
                entity: "PlannerChoice",
                sql: "SELECT tier, handle FROM PlannerChoice WHERE tier = 'gold' AND label >= 'br' AND label < 'd' ORDER BY label ASC, handle ASC, id ASC LIMIT 2 OFFSET 1",
                columns: &["tier", "handle"],
                expected_rows: &[&["gold", "echo"], &["gold", "lima"]],
            },
            PlannerProjectionCase {
                name: "planner range-choice offset projection DESC",
                entity: "PlannerChoice",
                sql: "SELECT tier, handle FROM PlannerChoice WHERE tier = 'gold' AND label >= 'br' AND label < 'd' ORDER BY label DESC, handle DESC, id DESC LIMIT 2 OFFSET 1",
                columns: &["tier", "handle"],
                expected_rows: &[&["gold", "lima"], &["gold", "echo"]],
            },
        ];

        for case in &cases {
            assert_planner_projection_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_explain_json_planner_order_only_choice_prefers_order_compatible_index() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let payload = query_result(
            pic,
            canister_id,
            "EXPLAIN JSON SELECT id, alpha FROM PlannerChoice ORDER BY alpha ASC, id ASC LIMIT 2",
        )
        .expect("planner order-only choice JSON explain should succeed");

        assert_explain_route(
            payload,
            "PlannerChoice",
            &[
                "\"mode\":{\"type\":\"Load\"",
                "\"access\":{\"type\":\"IndexRange\"",
                "\"name\":\"PlannerChoice|alpha\"",
            ],
            &["\"name\":\"PlannerChoice|beta\""],
            "planner order-only choice JSON explain should lock the order-compatible fallback index",
        );
    });
}

#[test]
fn sql_canister_query_lane_planner_order_only_offset_projection_matrix_preserves_ordered_rows() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            PlannerProjectionCase {
                name: "planner order-only offset projection ASC",
                entity: "PlannerChoice",
                sql: "SELECT alpha FROM PlannerChoice ORDER BY alpha ASC, id ASC LIMIT 2 OFFSET 1",
                columns: &["alpha"],
                expected_rows: &[&["bravo"], &["charlie"]],
            },
            PlannerProjectionCase {
                name: "planner order-only offset projection DESC",
                entity: "PlannerChoice",
                sql: "SELECT alpha FROM PlannerChoice ORDER BY alpha DESC, id DESC LIMIT 2 OFFSET 1",
                columns: &["alpha"],
                expected_rows: &[&["foxtrot"], &["delta"]],
            },
        ];

        for case in &cases {
            assert_planner_projection_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_planner_order_only_offset_explain_execution_matrix_preserves_ordered_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            PlannerExplainCase {
                name: "planner order-only offset EXPLAIN EXECUTION ASC",
                entity: "PlannerChoice",
                sql: "EXPLAIN EXECUTION SELECT id, alpha FROM PlannerChoice ORDER BY alpha ASC, id ASC LIMIT 2 OFFSET 1",
                required_tokens: &[
                    "IndexRangeScan",
                    "PlannerChoice|alpha",
                    "SecondaryOrderPushdown",
                    "IndexRangeLimitPushdown",
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                    "offset=Uint(1)",
                ],
                forbidden_tokens: &["OrderByMaterializedSort"],
            },
            PlannerExplainCase {
                name: "planner order-only offset EXPLAIN EXECUTION DESC",
                entity: "PlannerChoice",
                sql: "EXPLAIN EXECUTION SELECT id, alpha FROM PlannerChoice ORDER BY alpha DESC, id DESC LIMIT 2 OFFSET 1",
                required_tokens: &[
                    "IndexRangeScan",
                    "PlannerChoice|alpha",
                    "SecondaryOrderPushdown",
                    "IndexRangeLimitPushdown",
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                    "scan_dir=Text(\"desc\")",
                    "offset=Uint(1)",
                ],
                forbidden_tokens: &["OrderByMaterializedSort"],
            },
        ];

        for case in &cases {
            assert_planner_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_planner_composite_order_only_json_explain_matrix_prefers_order_compatible_index()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            PlannerExplainCase {
                name: "planner composite order-only JSON explain ASC",
                entity: "PlannerPrefixChoice",
                sql: "EXPLAIN JSON SELECT id, tier FROM PlannerPrefixChoice ORDER BY tier ASC, handle ASC, id ASC LIMIT 2",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Load\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                    "\"name\":\"PlannerPrefixChoice|tier|handle\"",
                ],
                forbidden_tokens: &["\"name\":\"PlannerPrefixChoice|tier|label\""],
            },
            PlannerExplainCase {
                name: "planner composite order-only JSON explain DESC",
                entity: "PlannerPrefixChoice",
                sql: "EXPLAIN JSON SELECT id, tier FROM PlannerPrefixChoice ORDER BY tier DESC, handle DESC, id DESC LIMIT 2",
                required_tokens: &[
                    "\"mode\":{\"type\":\"Load\"",
                    "\"access\":{\"type\":\"IndexRange\"",
                    "\"name\":\"PlannerPrefixChoice|tier|handle\"",
                ],
                forbidden_tokens: &["\"name\":\"PlannerPrefixChoice|tier|label\""],
            },
        ];

        for case in &cases {
            assert_planner_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_planner_composite_order_only_explain_execution_matrix_preserves_ordered_routes()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            PlannerExplainCase {
                name: "planner composite order-only EXPLAIN EXECUTION ASC",
                entity: "PlannerPrefixChoice",
                sql: "EXPLAIN EXECUTION SELECT id, tier FROM PlannerPrefixChoice ORDER BY tier ASC, handle ASC, id ASC LIMIT 2",
                required_tokens: &[
                    "IndexRangeScan",
                    "PlannerPrefixChoice|tier|handle",
                    "SecondaryOrderPushdown",
                    "IndexRangeLimitPushdown",
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                ],
                forbidden_tokens: &[],
            },
            PlannerExplainCase {
                name: "planner composite order-only EXPLAIN EXECUTION DESC",
                entity: "PlannerPrefixChoice",
                sql: "EXPLAIN EXECUTION SELECT id, tier FROM PlannerPrefixChoice ORDER BY tier DESC, handle DESC, id DESC LIMIT 2",
                required_tokens: &[
                    "IndexRangeScan",
                    "PlannerPrefixChoice|tier|handle",
                    "SecondaryOrderPushdown",
                    "IndexRangeLimitPushdown",
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                    "scan_dir=Text(\"desc\")",
                ],
                forbidden_tokens: &[],
            },
            PlannerExplainCase {
                name: "planner composite order-only EXPLAIN EXECUTION ASC offset",
                entity: "PlannerPrefixChoice",
                sql: "EXPLAIN EXECUTION SELECT tier, handle FROM PlannerPrefixChoice ORDER BY tier ASC, handle ASC, id ASC LIMIT 2 OFFSET 1",
                required_tokens: &[
                    "IndexRangeScan",
                    "PlannerPrefixChoice|tier|handle",
                    "SecondaryOrderPushdown",
                    "IndexRangeLimitPushdown",
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                    "offset=Uint(1)",
                ],
                forbidden_tokens: &["OrderByMaterializedSort"],
            },
            PlannerExplainCase {
                name: "planner composite order-only EXPLAIN EXECUTION DESC offset",
                entity: "PlannerPrefixChoice",
                sql: "EXPLAIN EXECUTION SELECT tier, handle FROM PlannerPrefixChoice ORDER BY tier DESC, handle DESC, id DESC LIMIT 2 OFFSET 1",
                required_tokens: &[
                    "IndexRangeScan",
                    "PlannerPrefixChoice|tier|handle",
                    "SecondaryOrderPushdown",
                    "IndexRangeLimitPushdown",
                    "TopNSeek",
                    "OrderByAccessSatisfied",
                    "scan_dir=Text(\"desc\")",
                    "offset=Uint(1)",
                ],
                forbidden_tokens: &["OrderByMaterializedSort"],
            },
        ];

        for case in &cases {
            assert_planner_explain_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_planner_composite_order_only_offset_projection_matrix_preserves_ordered_rows()
 {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            PlannerProjectionCase {
                name: "planner composite order-only offset projection ASC",
                entity: "PlannerPrefixChoice",
                sql: "SELECT tier, handle FROM PlannerPrefixChoice ORDER BY tier ASC, handle ASC, id ASC LIMIT 2 OFFSET 1",
                columns: &["tier", "handle"],
                expected_rows: &[&["gold", "charlie"], &["silver", "delta"]],
            },
            PlannerProjectionCase {
                name: "planner composite order-only offset projection DESC",
                entity: "PlannerPrefixChoice",
                sql: "SELECT tier, handle FROM PlannerPrefixChoice ORDER BY tier DESC, handle DESC, id DESC LIMIT 2 OFFSET 1",
                columns: &["tier", "handle"],
                expected_rows: &[&["gold", "charlie"], &["gold", "bravo"]],
            },
        ];

        for case in &cases {
            assert_planner_projection_case(pic, canister_id, case);
        }
    });
}

#[test]
fn sql_canister_query_lane_customer_order_strict_prefix_explain_matrix_preserves_covering_routes() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        let cases = [
            CustomerOrderStrictPrefixExplainCase {
                name: "CustomerOrder strict prefix covering route parity ASC",
                like_sql: "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
                starts_with_sql: "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2",
                range_sql: "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
                required_tokens: &[
                    "IndexRangeScan",
                    "covering_read",
                    "cov_read_route",
                    "OrderByAccessSatisfied",
                    "proj_fields",
                    "id",
                    "name",
                ],
                forbidden_tokens: &[],
            },
            CustomerOrderStrictPrefixExplainCase {
                name: "CustomerOrder strict prefix covering route parity DESC",
                like_sql: "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
                starts_with_sql: "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2",
                range_sql: "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
                required_tokens: &[
                    "IndexRangeScan",
                    "covering_read",
                    "cov_read_route",
                    "OrderByAccessSatisfied",
                    "scan_dir=Text(\"desc\")",
                    "proj_fields",
                    "id",
                    "name",
                ],
                forbidden_tokens: &[],
            },
        ];

        for case in &cases {
            assert_customer_order_strict_prefix_explain_case(pic, canister_id, case);
        }
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
                scenario_key: "generated.dispatch.order_alias.user_name_age_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT name AS customer_name, age years FROM Customer ORDER BY customer_name ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.order_alias.user_name_age_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT name AS customer_name, age years FROM Customer ORDER BY customer_name ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.order_alias.customer_account_lower_handle_limit2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerAccount,
                    sql: "SELECT LOWER(handle) AS normalized_handle, id FROM CustomerAccount WHERE active = true ORDER BY normalized_handle ASC, id ASC LIMIT 2"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.table_alias.customer_name_limit1",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "SELECT customer.name FROM Customer customer WHERE customer.name = 'alice' ORDER BY customer.id ASC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.customer.generated_pk_insert",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "INSERT INTO Customer (name, age) VALUES ('inserted-generated', 22)"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.customer.insert_select_generated_pk",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "INSERT INTO Customer (name, age) SELECT name, age FROM Customer WHERE name = 'alice' ORDER BY id ASC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.customer.insert_select_computed_generated_pk",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomer,
                    sql: "INSERT INTO Customer (name, age) SELECT LOWER(name), age FROM Customer WHERE name = 'alice' ORDER BY id ASC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.sql_write_probe.insert.id2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchSqlWriteProbe,
                    sql: "INSERT INTO SqlWriteProbe (id, name, age) VALUES (2, 'inserted', 22)"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.sql_write_probe.insert.alias.id2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchSqlWriteProbe,
                    sql: "INSERT INTO SqlWriteProbe s (id, name, age) VALUES (2, 'inserted-alias', 22)"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.sql_write_probe.insert.id2_id3",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchSqlWriteProbe,
                    sql: "INSERT INTO SqlWriteProbe (id, name, age) VALUES (2, 'inserted-a', 22), (3, 'inserted-b', 23)"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.sql_write_probe.insert.positional.id2",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchSqlWriteProbe,
                    sql: "INSERT INTO SqlWriteProbe VALUES (2, 'positional', 22)"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.sql_write_probe.update.id1",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchSqlWriteProbe,
                    sql: "UPDATE SqlWriteProbe SET name = 'updated', age = 22 WHERE id = 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.sql_write_probe.update.alias.id1",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchSqlWriteProbe,
                    sql: "UPDATE SqlWriteProbe s SET s.name = 'updated-alias', s.age = 22 WHERE s.id = 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.sql_write_probe.update.eq.age21",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchSqlWriteProbe,
                    sql: "UPDATE SqlWriteProbe SET name = 'updated-by-eq', age = 22 WHERE age = 21"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.sql_write_probe.update.age21",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchSqlWriteProbe,
                    sql: "UPDATE SqlWriteProbe SET name = 'updated-by-age', age = 22 WHERE age >= 21"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.dispatch.sql_write_probe.update.window.id1_limit1",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchSqlWriteProbe,
                    sql: "UPDATE SqlWriteProbe SET name = 'updated-window', age = 22 WHERE id >= 1 ORDER BY id ASC LIMIT 1"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 1,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.order_alias.customer_age_count_limit10",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql: "SELECT age years, COUNT(*) total FROM Customer GROUP BY age ORDER BY years ASC LIMIT 10"
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
                scenario_key: "fluent.aggregate.explain.customer.exists",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentExplainCustomerExists,
                    sql: "FLUENT EXPLAIN Customer EXISTS".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.aggregate.explain.customer.min",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentExplainCustomerMin,
                    sql: "FLUENT EXPLAIN Customer MIN".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.aggregate.explain.customer.last",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentExplainCustomerLast,
                    sql: "FLUENT EXPLAIN Customer LAST".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.aggregate.explain.customer.sum_by_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentExplainCustomerSumByAge,
                    sql: "FLUENT EXPLAIN Customer SUM BY age".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.aggregate.explain.customer.avg_distinct_by_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentExplainCustomerAvgDistinctByAge,
                    sql: "FLUENT EXPLAIN Customer AVG DISTINCT BY age".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.aggregate.explain.customer.count_distinct_by_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentExplainCustomerCountDistinctByAge,
                    sql: "FLUENT EXPLAIN Customer COUNT DISTINCT BY age".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "fluent.aggregate.explain.customer.last_value_by_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::FluentExplainCustomerLastValueByAge,
                    sql: "FLUENT EXPLAIN Customer LAST VALUE BY age".to_string(),
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
                scenario_key: "typed.execute_sql_grouped.user_name_count",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql:
                        "SELECT name, COUNT(*) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_name_count.top_level_distinct",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql:
                        "SELECT DISTINCT name, COUNT(*) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_trimmed_name_count",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql:
                        "SELECT TRIM(name), COUNT(*) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_trimmed_name_count.aliases",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql:
                        "SELECT TRIM(name) AS trimmed_name, COUNT(*) total FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_name_count.limit3.first_page",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql:
                        "SELECT name, COUNT(*) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 3"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_name_count.limit3.second_page",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomerSecondPage,
                    sql:
                        "SELECT name, COUNT(*) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 3"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_name_sum_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql:
                        "SELECT name, SUM(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_name_count_distinct_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql:
                        "SELECT name, COUNT(DISTINCT age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_name_sum_distinct_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql:
                        "SELECT name, SUM(DISTINCT age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_name_avg_distinct_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql:
                        "SELECT name, AVG(DISTINCT age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_age_count.filtered",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql:
                        "SELECT age, COUNT(*) FROM Customer WHERE age >= 36 GROUP BY age ORDER BY age ASC LIMIT 10"
                            .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_name_min_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql: "SELECT name, MIN(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_name_max_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql: "SELECT name, MAX(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10"
                        .to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_grouped.user_name_min_distinct_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlGroupedCustomer,
                    sql:
                        "SELECT name, MIN(DISTINCT age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10"
                            .to_string(),
                    cursor_token: None,
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
                scenario_key: "typed.execute_sql_aggregate.user_count_distinct_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
                    sql: "SELECT COUNT(DISTINCT age) FROM Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_sum_distinct_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
                    sql: "SELECT SUM(DISTINCT age) FROM Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_avg_distinct_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
                    sql: "SELECT AVG(DISTINCT age) FROM Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_min_distinct_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
                    sql: "SELECT MIN(DISTINCT age) FROM Customer".to_string(),
                    cursor_token: None,
                    repeat_count: 5,
                },
            },
            SqlPerfScenario {
                scenario_key: "typed.execute_sql_aggregate.user_max_distinct_age",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
                    sql: "SELECT MAX(DISTINCT age) FROM Customer".to_string(),
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
                scenario_key: "generated.dispatch.delete.user_order_id_limit1_offset1",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "DELETE FROM Customer ORDER BY id LIMIT 1 OFFSET 1".to_string(),
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
                scenario_key: "generated.dispatch.customer_order_distinct_priority_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::GeneratedDispatch,
                    sql: "SELECT DISTINCT priority FROM CustomerOrder ORDER BY priority ASC LIMIT 2"
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
                scenario_key: "typed.dispatch.customer_order_distinct_priority_limit2.asc",
                request: SqlPerfRequest {
                    surface: SqlPerfSurface::TypedDispatchCustomerOrder,
                    sql: "SELECT DISTINCT priority FROM CustomerOrder ORDER BY priority ASC LIMIT 2"
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
            "generated Customer name-order perf sample should not enter the row_check covering candidate lane on the planner-proven default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_count, 0,
            "generated Customer name-order perf sample should not execute row-presence probes on the planner-proven default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_hits, 0,
            "generated Customer name-order perf sample should not perform row-presence probes on the planner-proven default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_misses, 0,
            "generated Customer name-order perf sample should not report row-presence misses on the planner-proven default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_borrowed_data_store_count, 0,
            "generated Customer name-order perf sample should not keep row checks on the borrowed data-store boundary on the planner-proven default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_store_handle_count, 0,
            "generated Customer name-order perf sample should not route row checks back through the store-handle helper on the planner-proven default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_key_to_raw_encodes, 0,
            "generated Customer name-order perf sample should not encode row-check primary keys on the planner-proven default fixture set",
        );
        assert_eq!(
            generated_metrics.row_check_rows_emitted, 0,
            "generated Customer name-order perf sample should not report row_check-emitted rows on the planner-proven default fixture set",
        );
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed Customer name-order perf samples should keep row_check metrics in parity",
        );
    });
}

#[test]
fn sql_canister_perf_generated_dispatch_ordered_matrix_reports_positive_instruction_samples() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        for (label, sql, expected_entity, expected_row_count) in
            GENERATED_DISPATCH_ORDERED_PERF_CASES.iter().copied()
        {
            assert_generated_dispatch_perf_case(
                pic,
                canister_id,
                label,
                sql,
                expected_entity,
                expected_row_count,
            );
        }
    });
}

#[test]
#[ignore = "manual perf probe for before/after measurement runs"]
fn sql_canister_perf_probe_reports_sample_as_json() {
    run_with_perf_fixture_canister(sql_perf_probe_canister(), |pic, canister_id| {
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
#[ignore = "manual perf probe timing trace"]
fn sql_canister_perf_probe_reports_timing_trace() {
    let started_at = Instant::now();
    log_perf_probe_phase(started_at, "start");

    let _serial_guard = try_acquire_pic_serial_guard()
        .unwrap_or_else(|err| panic!("failed to acquire PocketIC serial guard: {err}"));
    log_perf_probe_phase(started_at, "serial-guard-acquired");

    let _pic = match try_pic() {
        Ok(pic) => pic,
        Err(err) if should_skip_pic_start(&err) => {
            skip_sql_canister_test(err);
            return;
        }
        Err(err) => panic!("failed to start PocketIC: {err}"),
    };
    log_perf_probe_phase(started_at, "pic-started");

    let Some(fixture) = install_fresh_fixture(sql_perf_probe_canister()) else {
        return;
    };
    log_perf_probe_phase(started_at, "fixture-installed");

    load_perf_audit_fixtures(fixture.pic(), fixture.canister_id());
    log_perf_probe_phase(started_at, "perf-fixtures-loaded");

    let request = SqlPerfRequest {
        surface: sql_perf_probe_sample_surface(),
        sql: sql_perf_probe_sql(),
        cursor_token: sql_perf_probe_cursor_token(),
        repeat_count: sql_perf_probe_repeat_count(),
    };
    log_perf_probe_phase(started_at, "sample-request-built");

    let sample = sql_perf_sample(fixture.pic(), fixture.canister_id(), &request);
    log_perf_probe_phase(started_at, "sample-returned");

    assert!(
        sample.first_local_instructions > 0,
        "manual perf probe timing trace first instruction sample must be positive: {sample:?}",
    );
    assert!(
        sample.outcome.success,
        "manual perf probe timing trace should stay on a successful SQL surface: {sample:?}",
    );

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "mode": "sample-timing-trace",
            "request": request,
            "elapsed_ms": started_at.elapsed().as_millis(),
            "sample": sample,
        }))
        .expect("manual perf probe timing trace should serialize to JSON")
    );
}

#[test]
#[ignore = "manual batch perf probe for before/after measurement runs"]
fn sql_canister_perf_probe_reports_batch_as_json() {
    run_with_perf_fixture_canister(sql_perf_probe_canister(), |pic, canister_id| {
        // Phase 1: resolve one shared probe envelope that stays constant for
        // the whole batch so the loaded canister and fixture dataset are
        // reused across every query in this run.
        let surface = sql_perf_probe_sample_surface();
        let cursor_token = sql_perf_probe_cursor_token();
        let repeat_count = sql_perf_probe_repeat_count();
        let queries = sql_perf_probe_sql_batch();

        // Phase 2: run one sample per query against the same loaded canister
        // and emit each JSON record immediately so long runs remain observable.
        for sql in queries {
            let request = SqlPerfRequest {
                surface,
                sql,
                cursor_token: cursor_token.clone(),
                repeat_count,
            };
            let sample = sql_perf_sample(pic, canister_id, &request);

            assert!(
                sample.first_local_instructions > 0,
                "manual batch perf probe first instruction sample must be positive: {sample:?}",
            );
            assert!(
                sample.outcome.success,
                "manual batch perf probe should stay on a successful SQL surface: {sample:?}",
            );

            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "mode": "sample-batch",
                    "request": request,
                    "sample": sample,
                }))
                .expect("manual batch perf probe sample should serialize to JSON")
            );
        }
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
    run_with_perf_fixture_canister(sql_perf_probe_canister(), |pic, canister_id| {
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
fn sql_canister_perf_generated_dispatch_ordered_attribution_matrix_reports_positive_stages() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        for (label, sql, expected_entity, expected_row_count) in
            GENERATED_DISPATCH_ORDERED_PERF_CASES.iter().copied()
        {
            assert_generated_dispatch_attribution_case(
                pic,
                canister_id,
                label,
                sql,
                expected_entity,
                expected_row_count,
            );
        }
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
            assert_positive_grouped_attribution_sample(label, sample);
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
fn sql_canister_perf_grouped_window_attribution_matrix_preserves_cursor_contract() {
    run_with_loaded_sql_parity_canister(|pic, canister_id| {
        for (label, full_page_sql, paged_sql) in GROUPED_WINDOW_ATTRIBUTION_CASES.iter().copied() {
            assert_grouped_window_attribution(pic, canister_id, full_page_sql, paged_sql, label);
        }
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
                assert!(
                    indexes.iter().all(|index| index.contains("[state=ready]")),
                    "SHOW INDEXES payload should surface the current ready index lifecycle state for the default metadata fixture",
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
