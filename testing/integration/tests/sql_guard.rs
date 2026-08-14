use std::{collections::BTreeSet, fs, process::Command};

use candid::{CandidType, Principal};
use icydb::{
    Error, ErrorCode,
    db::{EntitySchemaDescription, sql::SqlQueryPerfResult},
};
use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterCandidExportMode, CanisterWasmProfile,
    build_canister_with_options,
    canister_artifact::{CanisterMethod, CanisterMethodMode, inspect_canister_artifacts},
    install_fixture_canister,
};
use serde::Deserialize;

const WRAPPER_INSTRUCTION_CEILING: u64 = 5_000_000;
const ALLOWLIST_INSTRUCTION_CEILING: u64 = 1_000_000;
const GUARDED_SQL_RAW_WASM_GROWTH_CEILING: u64 = 128 * 1024;

#[derive(CandidType, Debug, Deserialize)]
struct ReadAuthorizationCostResult {
    caller_instructions: u64,
    helper_instructions: u64,
    guard_instructions: u64,
    authorization_instructions: u64,
    wrapper_instructions: u64,
    query_instructions: u64,
    helper_allowed: bool,
}

fn principal(seed: u8) -> Principal {
    Principal::self_authenticating([seed; 32])
}

#[test]
fn guarded_sql_authorizes_one_reader_and_preserves_the_read_only_lane() {
    let fixture = install_fixture_canister("sql_guard");
    let reader = principal(42);
    let outsider = principal(43);

    for sql in [
        "SELECT id FROM OneSimpleEntity01 ORDER BY id ASC LIMIT 1",
        "SHOW ENTITIES",
        "DESCRIBE OneSimpleEntity01",
        "EXPLAIN SELECT id FROM OneSimpleEntity01 ORDER BY id ASC LIMIT 1",
    ] {
        let allowed: Result<SqlQueryPerfResult, Error> = fixture
            .query_candid_as(reader, "icydb_query", (sql.to_string(),))
            .expect("guarded SQL success should decode");
        allowed.unwrap_or_else(|error| panic!("the SQL guard should admit {sql:?}: {error:?}"));
    }

    let absent_schema = fixture.query_candid_as::<Result<Vec<EntitySchemaDescription>, Error>, _>(
        reader,
        "icydb_schema",
        (),
    );
    assert!(
        absent_schema.is_err(),
        "SQL introspection authority must not imply the dedicated schema method",
    );

    let denied: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid_as(outsider, "icydb_query", ("not valid SQL".to_string(),))
        .expect("guarded SQL denial should decode");
    assert_eq!(
        denied.expect_err("the outsider should be denied").code(),
        ErrorCode::RUNTIME_BOUNDARY_SQL_SURFACE_POLICY_DENIED,
    );

    let controller: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid("icydb_query", ("SHOW ENTITIES".to_string(),))
        .expect("controller denial should decode");
    assert_eq!(
        controller
            .expect_err("guarded mode must not include controllers implicitly")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_SQL_SURFACE_POLICY_DENIED,
    );

    let mutation: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid_as(
            reader,
            "icydb_query",
            (
                "INSERT INTO OneSimpleEntity01 (id) VALUES ('00000000000000000000000000')"
                    .to_string(),
            ),
        )
        .expect("authorized mutation rejection should decode");
    assert_eq!(
        mutation
            .expect_err("authorization must not admit SQL mutation")
            .code(),
        ErrorCode::SQL_SURFACE_QUERY_REJECTS_INSERT,
    );
}

#[test]
fn guarded_sql_rejects_anonymous_before_application_code_and_propagates_guard_traps() {
    let fixture = install_fixture_canister("sql_guard");

    let anonymous: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid_as(
            Principal::anonymous(),
            "icydb_query",
            ("SHOW ENTITIES".to_string(),),
        )
        .expect("anonymous denial should decode instead of reaching the trapping guard");
    assert_eq!(
        anonymous.expect_err("anonymous must be denied").code(),
        ErrorCode::RUNTIME_BOUNDARY_SQL_SURFACE_POLICY_DENIED,
    );

    let trapped = fixture.query_candid_as::<Result<SqlQueryPerfResult, Error>, _>(
        principal(44),
        "icydb_query",
        ("SHOW ENTITIES".to_string(),),
    );
    assert!(trapped.is_err(), "application guard traps must propagate");
}

#[test]
fn guarded_sql_local_authorization_cost_stays_below_frozen_ceilings() {
    let fixture = install_fixture_canister("sql_guard");
    let cost: Result<ReadAuthorizationCostResult, Error> = fixture
        .query_candid_as(principal(42), "read_authorization_cost", ())
        .expect("authorization cost result should decode");
    let cost = cost.expect("non-anonymous authorization cost probe should succeed");

    eprintln!(
        "guarded SQL authorization cost: caller={} helper={} guard={} authorization={} wrapper={} query={}",
        cost.caller_instructions,
        cost.helper_instructions,
        cost.guard_instructions,
        cost.authorization_instructions,
        cost.wrapper_instructions,
        cost.query_instructions,
    );

    assert!(cost.helper_allowed);
    assert!(
        cost.helper_instructions <= ALLOWLIST_INSTRUCTION_CEILING,
        "64-principal helper cost {} exceeds {}",
        cost.helper_instructions,
        ALLOWLIST_INSTRUCTION_CEILING,
    );
    assert!(
        cost.wrapper_instructions <= WRAPPER_INSTRUCTION_CEILING,
        "wrapper cost {} exceeds {}",
        cost.wrapper_instructions,
        WRAPPER_INSTRUCTION_CEILING,
    );
}

#[test]
fn guarded_and_controller_sql_artifacts_keep_one_identical_public_surface() {
    let build = |build_profile| {
        build_canister_with_options(
            "sql_guard",
            CanisterBuildOptions {
                profile: CanisterWasmProfile::WasmRelease,
                candid_export: CanisterCandidExportMode::Enabled,
                build_profile,
                ..CanisterBuildOptions::default()
            },
        )
        .expect("SQL guard evidence canister should build")
    };
    let controller_wasm = build(CanisterBuildProfile::Production);
    let guarded_wasm = build(CanisterBuildProfile::LocalTest);

    let expected = BTreeSet::from([CanisterMethod {
        name: "icydb_query".to_string(),
        mode: CanisterMethodMode::Query,
    }]);
    for wasm in [&controller_wasm, &guarded_wasm] {
        let manifest =
            inspect_canister_artifacts(wasm).expect("Candid and raw Wasm exports should agree");
        assert_eq!(manifest.icydb_methods(), expected);
    }

    let extract_candid = |wasm: &std::path::Path| {
        let output = Command::new("candid-extractor")
            .arg(wasm)
            .output()
            .expect("candid-extractor should run");
        assert!(output.status.success(), "Candid extraction should succeed");
        output.stdout
    };
    assert_eq!(
        extract_candid(&controller_wasm),
        extract_candid(&guarded_wasm),
        "guarded SQL must not change the complete Candid service",
    );

    let controller_bytes = fs::metadata(&controller_wasm)
        .expect("controller Wasm metadata should be available")
        .len();
    let guarded_bytes = fs::metadata(&guarded_wasm)
        .expect("guarded Wasm metadata should be available")
        .len();
    let growth = guarded_bytes.saturating_sub(controller_bytes);
    eprintln!(
        "guarded SQL raw Wasm: controller={controller_bytes} guarded={guarded_bytes} growth={growth}"
    );
    assert!(growth <= GUARDED_SQL_RAW_WASM_GROWTH_CEILING);
}
