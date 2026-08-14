use std::{collections::BTreeSet, fs, process::Command};

use candid::{CandidType, Principal};
use icydb::{Error, ErrorCode, db::EntitySchemaDescription};
use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterCandidExportMode, CanisterWasmProfile,
    build_canister_with_options, build_fixture_canister_wasm_bytes_with_options,
    canister_artifact::{CanisterMethod, CanisterMethodMode, inspect_canister_artifacts},
    install_fixture_canister, install_prebuilt_fixture_canister,
};
use serde::Deserialize;

const WRAPPER_INSTRUCTION_CEILING: u64 = 5_000_000;
const ALLOWLIST_INSTRUCTION_CEILING: u64 = 1_000_000;
const GUARDED_SCHEMA_RAW_WASM_GROWTH_CEILING: u64 = 128 * 1024;

#[derive(CandidType, Debug, Deserialize)]
struct ReadAuthorizationCostResult {
    caller_instructions: u64,
    helper_instructions: u64,
    guard_instructions: u64,
    authorization_instructions: u64,
    wrapper_instructions: u64,
    schema_instructions: u64,
    helper_allowed: bool,
}

fn principal(seed: u8) -> Principal {
    Principal::self_authenticating([seed; 32])
}

fn query_schema(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    caller: Principal,
) -> Result<Vec<EntitySchemaDescription>, Error> {
    fixture
        .query_candid_as(caller, "icydb_schema", ())
        .expect("schema response should decode")
}

#[test]
fn public_controller_and_guarded_schema_modes_preserve_distinct_authority() {
    let public = install_fixture_canister("schema_public");
    let public_schema = query_schema(&public, Principal::anonymous())
        .expect("the explicit public schema mode should admit anonymous callers");
    assert!(!public_schema.is_empty());

    let production_wasm = build_fixture_canister_wasm_bytes_with_options(
        "schema_guard",
        CanisterBuildOptions {
            build_profile: CanisterBuildProfile::Production,
            ..CanisterBuildOptions::default()
        },
    );
    let controller = install_prebuilt_fixture_canister("schema_guard", production_wasm);
    let controller_schema: Result<Vec<EntitySchemaDescription>, Error> = controller
        .query_candid("icydb_schema", ())
        .expect("controller schema response should decode");
    assert!(
        !controller_schema
            .expect("the controller should be admitted")
            .is_empty()
    );
    assert_eq!(
        query_schema(&controller, principal(43))
            .expect_err("an outsider must be rejected by controller mode")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_SCHEMA_SURFACE_CONTROLLER_REQUIRED,
    );

    let guarded = install_fixture_canister("schema_guard");
    let guarded_schema =
        query_schema(&guarded, principal(42)).expect("the application reader should be admitted");
    assert_eq!(guarded_schema, public_schema);
    assert_eq!(
        query_schema(&guarded, principal(43))
            .expect_err("an outsider must be rejected by guarded mode")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_SCHEMA_SURFACE_POLICY_DENIED,
    );
    let guarded_controller: Result<Vec<EntitySchemaDescription>, Error> = guarded
        .query_candid("icydb_schema", ())
        .expect("guarded controller denial should decode");
    assert_eq!(
        guarded_controller
            .expect_err("guarded mode must not include controllers implicitly")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_SCHEMA_SURFACE_POLICY_DENIED,
    );

    let absent_sql = guarded.query_candid_as::<Result<(), Error>, _>(
        principal(42),
        "icydb_query",
        ("SHOW ENTITIES".to_string(),),
    );
    assert!(
        absent_sql.is_err(),
        "the dedicated schema method must not create a second SQL spelling",
    );
}

#[test]
fn guarded_schema_rejects_anonymous_before_application_code_and_propagates_guard_traps() {
    let fixture = install_fixture_canister("schema_guard");

    let anonymous = query_schema(&fixture, Principal::anonymous())
        .expect_err("anonymous must be denied before the trapping guard");
    assert_eq!(
        anonymous.code(),
        ErrorCode::RUNTIME_BOUNDARY_SCHEMA_SURFACE_POLICY_DENIED,
    );

    let trapped = fixture.query_candid_as::<Result<Vec<EntitySchemaDescription>, Error>, _>(
        principal(44),
        "icydb_schema",
        (),
    );
    assert!(trapped.is_err(), "application guard traps must propagate");
}

#[test]
fn guarded_schema_local_authorization_cost_stays_below_frozen_ceilings() {
    let fixture = install_fixture_canister("schema_guard");
    let cost: Result<ReadAuthorizationCostResult, Error> = fixture
        .query_candid_as(principal(42), "read_authorization_cost", ())
        .expect("authorization cost result should decode");
    let cost = cost.expect("non-anonymous authorization cost probe should succeed");

    eprintln!(
        "guarded schema authorization cost: caller={} helper={} guard={} authorization={} wrapper={} schema={}",
        cost.caller_instructions,
        cost.helper_instructions,
        cost.guard_instructions,
        cost.authorization_instructions,
        cost.wrapper_instructions,
        cost.schema_instructions,
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
        "schema wrapper cost {} exceeds {}",
        cost.wrapper_instructions,
        WRAPPER_INSTRUCTION_CEILING,
    );
}

#[test]
fn guarded_and_controller_schema_artifacts_keep_one_identical_public_surface() {
    let build = |build_profile| {
        build_canister_with_options(
            "schema_guard",
            CanisterBuildOptions {
                profile: CanisterWasmProfile::WasmRelease,
                candid_export: CanisterCandidExportMode::Enabled,
                build_profile,
                ..CanisterBuildOptions::default()
            },
        )
        .expect("schema guard evidence canister should build")
    };
    let controller_wasm = build(CanisterBuildProfile::Production);
    let guarded_wasm = build(CanisterBuildProfile::LocalTest);

    let expected = BTreeSet::from([CanisterMethod {
        name: "icydb_schema".to_string(),
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
        "guarded schema must not change the complete Candid service",
    );

    let controller_bytes = fs::metadata(&controller_wasm)
        .expect("controller Wasm metadata should be available")
        .len();
    let guarded_bytes = fs::metadata(&guarded_wasm)
        .expect("guarded Wasm metadata should be available")
        .len();
    let growth = guarded_bytes.saturating_sub(controller_bytes);
    eprintln!(
        "guarded schema raw Wasm: controller={controller_bytes} guarded={guarded_bytes} growth={growth}"
    );
    assert!(growth <= GUARDED_SCHEMA_RAW_WASM_GROWTH_CEILING);
}
