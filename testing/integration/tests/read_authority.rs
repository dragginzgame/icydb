use std::{collections::BTreeSet, fs, process::Command};

use candid::Principal;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error, ErrorCode,
    db::{
        EntitySchemaDescription,
        sql::{SqlQueryPerfResult, SqlQueryResult},
    },
};
use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterCandidExportMode, CanisterWasmProfile,
    build_canister_with_options, build_fixture_canister_wasm_bytes_with_options,
    canister_artifact::{CanisterMethod, CanisterMethodMode, inspect_canister_artifacts},
    deliver_fixture_startup_watchdog, install_fixture_canister,
};
use sha2::{Digest, Sha256};

const CUMULATIVE_GUARDED_READ_RAW_WASM_GROWTH_CEILING: u64 = 128 * 1024;

fn principal(seed: u8) -> Principal {
    Principal::self_authenticating([seed; 32])
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn query_sql(
    fixture: &StandaloneCanisterFixture,
    caller: Principal,
) -> Result<SqlQueryResult, Error> {
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .query_candid_as(caller, "icydb_query", ("SHOW ENTITIES".to_string(),))
        .expect("SQL response should decode");
    result.map(|response| response.result)
}

fn query_schema(
    fixture: &StandaloneCanisterFixture,
    caller: Principal,
) -> Result<Vec<EntitySchemaDescription>, Error> {
    fixture
        .query_candid_as(caller, "icydb_schema", ())
        .expect("schema response should decode")
}

fn set_policy(
    fixture: &StandaloneCanisterFixture,
    caller: Principal,
    method: &str,
    enabled: bool,
) -> bool {
    fixture
        .update_candid_as(caller, method, (enabled,))
        .expect("application policy update should decode")
}

fn delegate_to_control_canister(
    fixture: &StandaloneCanisterFixture,
    reader: Principal,
) -> Principal {
    let target = fixture.canister_id();
    let control_canister = fixture.pocket_ic().create_canister();
    fixture
        .pocket_ic()
        .set_controllers(target, None, vec![control_canister])
        .expect("the installation controller should delegate to the control canister");
    let status = fixture
        .pocket_ic()
        .canister_status(target, Some(control_canister))
        .expect("the non-human controller should retain lifecycle authority");
    assert_eq!(status.settings.controllers, [control_canister]);
    assert!(
        fixture
            .pocket_ic()
            .canister_status(target, Some(reader))
            .is_err(),
        "the application reader must not inherit lifecycle authority",
    );
    control_canister
}

fn exercise_current_policy(
    fixture: &StandaloneCanisterFixture,
    admin: Principal,
    reader: Principal,
    stable_before_authorization: &[u8],
) -> (SqlQueryResult, Vec<EntitySchemaDescription>) {
    assert_eq!(
        query_sql(fixture, reader)
            .expect_err("SQL must initially fail closed")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_SQL_SURFACE_POLICY_DENIED,
    );
    assert_eq!(
        query_schema(fixture, reader)
            .expect_err("schema must initially fail closed")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_SCHEMA_SURFACE_POLICY_DENIED,
    );
    assert!(
        !set_policy(fixture, reader, "set_sql_reader_enabled", true),
        "the reader must not grant its own authority",
    );

    assert!(set_policy(fixture, admin, "set_sql_reader_enabled", true));
    let allowed_sql = query_sql(fixture, reader).expect("the current SQL policy should allow");
    assert_eq!(
        query_schema(fixture, reader)
            .expect_err("SQL authority must not imply schema authority")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_SCHEMA_SURFACE_POLICY_DENIED,
    );

    assert!(set_policy(
        fixture,
        admin,
        "set_schema_reader_enabled",
        true
    ));
    let allowed_schema =
        query_schema(fixture, reader).expect("the current schema policy should allow");
    assert!(!allowed_schema.is_empty());

    assert!(set_policy(fixture, admin, "set_sql_reader_enabled", false));
    assert_eq!(
        query_sql(fixture, reader)
            .expect_err("SQL revocation must be observed immediately")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_SQL_SURFACE_POLICY_DENIED,
    );
    assert_eq!(
        query_schema(fixture, reader).expect("schema authority remains independent"),
        allowed_schema,
    );
    assert!(set_policy(
        fixture,
        admin,
        "set_schema_reader_enabled",
        false
    ));
    assert_eq!(
        query_schema(fixture, reader)
            .expect_err("schema revocation must be observed immediately")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_SCHEMA_SURFACE_POLICY_DENIED,
    );
    assert_eq!(
        fixture.pocket_ic().get_stable_memory(fixture.canister_id()),
        stable_before_authorization,
        "denial, application policy, and IcyDB authorization must not write stable memory",
    );
    (allowed_sql, allowed_schema)
}

fn assert_reader_cannot_control(
    fixture: &StandaloneCanisterFixture,
    reader: Principal,
    upgrade_args: &[u8],
) {
    let target = fixture.canister_id();
    assert!(
        fixture
            .pocket_ic()
            .set_controllers(target, Some(reader), vec![reader])
            .is_err(),
        "the reader must not change controllers",
    );
    assert!(
        fixture
            .pocket_ic()
            .stop_canister(target, Some(reader))
            .is_err(),
        "the reader must not stop the canister",
    );
    assert!(
        fixture
            .pocket_ic()
            .upgrade_canister(target, vec![0], upgrade_args.to_vec(), Some(reader))
            .is_err(),
        "the reader must not upgrade the canister",
    );
}

fn assert_same_release_upgrade(
    fixture: &StandaloneCanisterFixture,
    admin: Principal,
    reader: Principal,
    control_canister: Principal,
    allowed_sql: &SqlQueryResult,
    allowed_schema: &[EntitySchemaDescription],
) {
    let target = fixture.canister_id();
    let wasm = build_fixture_canister_wasm_bytes_with_options(
        "read_authority",
        CanisterBuildOptions::default(),
    );
    let upgrade_args = candid::encode_args(()).expect("empty upgrade args should encode");
    assert_reader_cannot_control(fixture, reader, &upgrade_args);

    assert!(set_policy(fixture, admin, "set_sql_reader_enabled", true));
    assert!(set_policy(
        fixture,
        admin,
        "set_schema_reader_enabled",
        true
    ));
    fixture
        .pocket_ic()
        .upgrade_canister(target, wasm, upgrade_args, Some(control_canister))
        .expect("the control canister should perform the same-release upgrade");

    assert_eq!(
        query_sql(fixture, reader)
            .expect_err("heap-local SQL policy must reset fail closed")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_SQL_SURFACE_POLICY_DENIED,
    );
    assert_eq!(
        query_schema(fixture, reader)
            .expect_err("heap-local schema policy must reset fail closed")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_SCHEMA_SURFACE_POLICY_DENIED,
    );
    assert!(set_policy(fixture, admin, "set_sql_reader_enabled", true));
    assert!(set_policy(
        fixture,
        admin,
        "set_schema_reader_enabled",
        true
    ));
    // Immediate healthy recovery may finish while the policy is being
    // re-enabled, so the old externally observable one-second pending window
    // is no longer a stable authorization contract.
    deliver_fixture_startup_watchdog(fixture);
    assert_eq!(
        &query_sql(fixture, reader).expect("SQL should recover after upgrade"),
        allowed_sql
    );
    assert_eq!(
        query_schema(fixture, reader).expect("schema should recover after upgrade"),
        allowed_schema
    );
    let status = fixture
        .pocket_ic()
        .canister_status(target, Some(control_canister))
        .expect("the non-human controller should survive upgrade");
    assert_eq!(status.settings.controllers, [control_canister]);
}

#[test]
fn framework_neutral_policy_is_current_least_privilege_and_upgrade_safe() {
    let fixture = install_fixture_canister("read_authority");
    let admin = principal(41);
    let reader = principal(42);
    let stable_before_authorization = fixture.pocket_ic().get_stable_memory(fixture.canister_id());
    let control_canister = delegate_to_control_canister(&fixture, reader);
    assert_eq!(
        query_sql(&fixture, control_canister)
            .expect_err("the non-human controller must not join guarded SQL authority")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_SQL_SURFACE_POLICY_DENIED,
    );
    assert_eq!(
        query_schema(&fixture, control_canister)
            .expect_err("the non-human controller must not join guarded schema authority")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_SCHEMA_SURFACE_POLICY_DENIED,
    );
    let (allowed_sql, allowed_schema) =
        exercise_current_policy(&fixture, admin, reader, &stable_before_authorization);

    assert_same_release_upgrade(
        &fixture,
        admin,
        reader,
        control_canister,
        &allowed_sql,
        &allowed_schema,
    );
}

#[test]
fn controller_and_guarded_combined_artifacts_preserve_one_bounded_surface() {
    let build = |build_profile| {
        build_canister_with_options(
            "read_authority",
            CanisterBuildOptions {
                profile: CanisterWasmProfile::WasmRelease,
                candid_export: CanisterCandidExportMode::Enabled,
                build_profile,
                ..CanisterBuildOptions::default()
            },
        )
        .expect("combined read-authority evidence canister should build")
    };
    let controller_wasm = build(CanisterBuildProfile::Production);
    let guarded_wasm = build(CanisterBuildProfile::LocalTest);

    let expected = BTreeSet::from([
        CanisterMethod {
            name: "icydb_query".to_string(),
            mode: CanisterMethodMode::Query,
        },
        CanisterMethod {
            name: "icydb_schema".to_string(),
            mode: CanisterMethodMode::Query,
        },
    ]);
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
    let controller_candid = extract_candid(&controller_wasm);
    let guarded_candid = extract_candid(&guarded_wasm);
    assert_eq!(
        controller_candid, guarded_candid,
        "guarded reads must not change the complete Candid service",
    );

    let controller_bytes = fs::metadata(&controller_wasm)
        .expect("controller Wasm metadata should be available")
        .len();
    let guarded_bytes = fs::metadata(&guarded_wasm)
        .expect("guarded Wasm metadata should be available")
        .len();
    let growth = guarded_bytes.saturating_sub(controller_bytes);
    eprintln!(
        "combined guarded-read raw Wasm: controller={controller_bytes} guarded={guarded_bytes} growth={growth} controller_hash={} guarded_hash={} candid_bytes={} candid_hash={}",
        sha256(&fs::read(&controller_wasm).expect("controller Wasm should read")),
        sha256(&fs::read(&guarded_wasm).expect("guarded Wasm should read")),
        guarded_candid.len(),
        sha256(&guarded_candid),
    );
    assert!(growth <= CUMULATIVE_GUARDED_READ_RAW_WASM_GROWTH_CEILING);
}
