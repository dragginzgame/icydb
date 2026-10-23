use ic_platform::{pocket_ic::RejectCode, principal::ExternalPrincipal as Principal};
use ic_testkit::pic::{InstallSpec, StandaloneCanisterFixture};
use ic_wire::native;
use icydb_testing_build::{
    CanisterBuildOptions, CanisterBuildProfile, CanisterSqlMode, CanisterWasmProfile,
    build_canister_with_options,
    canister_artifact::{MethodMode, inspect_canister_artifacts},
};
use icydb_testing_integration::WireCallExt as _;
use icydb_testing_integration::{install_fixture_canister, install_prebuilt_fixture_canister};
use icydb_testing_wasm_helpers::{
    fixture_wire::{self, WireRollbackAction},
    wire::DynamicRowsQuery,
};

use std::{env, fs, process::Command};

#[test]
fn application_owned_sql_syntax_survives_queries_and_rebuilds_after_upgrade() {
    let fixture = install_fixture_canister("one_entity_sql_query");
    for _ in 0..3 {
        let rows: u32 = fixture
            .pocket_ic()
            .query_wire(fixture.canister_id(), "query_one_entity_sql", ())
            .expect("domain query reply");
        assert_eq!(
            rows, 0,
            "a missing lifecycle-initialized dispatch returns the failure sentinel"
        );
    }
    icydb_testing_integration::upgrade_fixture_canister(
        fixture.pocket_ic(),
        fixture.canister_id(),
        "one_entity_sql_query",
    );
    icydb_testing_integration::deliver_fixture_startup_watchdog(fixture.pocket_ic());
    for _ in 0..3 {
        let rows: u32 = fixture
            .pocket_ic()
            .query_wire(fixture.canister_id(), "query_one_entity_sql", ())
            .expect("post-upgrade domain query reply");
        assert_eq!(rows, 0);
    }
}

fn audited_typed_query_row_count(
    fixture: &StandaloneCanisterFixture,
    method: &str,
) -> Result<u32, u16> {
    fixture
        .pocket_ic()
        .query_wire(fixture.canister_id(), method, ())
        .expect("typed query endpoint response should decode")
}

#[test]
fn query_only_typed_and_dynamic_canisters_execute_without_sql() {
    let dynamic_fixture = install_fixture_canister("one_entity_dynamic_query");
    let dynamic_row_count = dynamic_wire_row_count(&dynamic_fixture);
    assert_eq!(dynamic_row_count, 0);

    let audited_fixture = install_fixture_canister("one_entity_typed_query");
    let audited_row_count =
        audited_typed_query_row_count(&audited_fixture, "query_one_entity_typed_lifecycle_rows")
            .unwrap_or_else(|error| panic!("typed schema/query initialization failed: {error}"));
    assert_eq!(audited_row_count, 0);

    let ten_entity_fixture = install_fixture_canister("ten_entity_typed_query");
    let ten_entity_row_count: u32 = ten_entity_fixture
        .pocket_ic()
        .query_wire(
            ten_entity_fixture.canister_id(),
            "query_ten_entity_typed_rows",
            (),
        )
        .expect("ten-entity typed query endpoint response should decode");
    assert_eq!(ten_entity_row_count, 0);
}

fn dynamic_wire_row_count(fixture: &StandaloneCanisterFixture) -> u32 {
    let request =
        native::encode_request(DynamicRowsQuery::ID, &(), DynamicRowsQuery::REQUEST_LIMITS)
            .expect("Wire request should encode");
    let reply = fixture
        .pocket_ic()
        .query_call(
            fixture.canister_id(),
            Principal::anonymous(),
            DynamicRowsQuery::NAME,
            request,
        )
        .expect("dynamic query raw transport should succeed");
    native::decode_response::<u32>(
        DynamicRowsQuery::ID,
        &reply,
        DynamicRowsQuery::RESPONSE_LIMITS,
    )
    .expect("dynamic query Wire reply should decode")
}

#[test]
fn dynamic_query_wire_endpoint_rejects_other_bytes_and_survives_upgrade() {
    let options = CanisterBuildOptions {
        profile: CanisterWasmProfile::WasmRelease,
        sql_mode: CanisterSqlMode::Disabled,
        build_profile: CanisterBuildProfile::Production,
    };
    let artifacts = build_canister_with_options("one_entity_dynamic_query", options)
        .expect("Wire query artifact should build");
    let manifest = inspect_canister_artifacts(artifacts.as_ref())
        .expect("mixed declarations should agree with exports");
    let method = manifest
        .wire_methods
        .iter()
        .find(|method| method.name == DynamicRowsQuery::NAME)
        .unwrap();
    assert_eq!(method.name, DynamicRowsQuery::NAME);
    assert_eq!(method.mode, MethodMode::Query);

    let wasm = fs::read(artifacts.as_ref()).expect("retained Wire artifact should read");
    let fixture = install_prebuilt_fixture_canister("one_entity_dynamic_query", wasm.clone());
    assert_eq!(dynamic_wire_row_count(&fixture), 0);
    let request =
        native::encode_request(DynamicRowsQuery::ID, &(), DynamicRowsQuery::REQUEST_LIMITS)
            .unwrap();
    let mut invalid = (0..request.len())
        .map(|length| request[..length].to_vec())
        .collect::<Vec<_>>();
    invalid.push(Vec::new());
    let mut wrong_identity = request.clone();
    wrong_identity[2] ^= 1;
    invalid.push(wrong_identity);
    let mut wrong_direction = request.clone();
    wrong_direction[10] = 1;
    invalid.push(wrong_direction);
    let mut oversized = request;
    oversized.extend([0; 1024]);
    invalid.push(oversized);
    for bytes in invalid {
        assert!(
            fixture
                .pocket_ic()
                .query_call(
                    fixture.canister_id(),
                    Principal::anonymous(),
                    DynamicRowsQuery::NAME,
                    bytes,
                )
                .is_err()
        );
        assert_eq!(dynamic_wire_row_count(&fixture), 0);
    }
    fixture
        .pocket_ic()
        .upgrade_canister(fixture.canister_id(), wasm, Vec::new(), None)
        .expect("same-contract upgrade should succeed");
    icydb_testing_integration::deliver_fixture_startup_watchdog(fixture.pocket_ic());
    assert_eq!(dynamic_wire_row_count(&fixture), 0);
}

#[test]
fn reachable_entity_audit_pair_exercises_the_shared_operation_mix() {
    for (canister, entity) in [
        ("one_entity_reachable_operations", 0_u8),
        ("ten_entity_reachable_operations", 9_u8),
    ] {
        let fixture = install_fixture_canister(canister);
        for operation in 0_u8..=7 {
            let method = if operation <= 1 {
                "exercise_reachable_entity_read"
            } else {
                "exercise_reachable_entity_write"
            };
            let selected_entity = if operation >= 6 { 0 } else { entity };
            let response = if operation <= 1 {
                fixture.pocket_ic().query_wire(
                    fixture.canister_id(),
                    method,
                    (selected_entity, operation),
                )
            } else {
                fixture.pocket_ic().update_wire(
                    fixture.canister_id(),
                    method,
                    (selected_entity, operation),
                )
            };
            let ((succeeded, local_instructions),): ((u32, u64),) =
                response.expect("reachable generated operation response should decode");
            println!(
                "icydb_0250_reachable_entity canister={canister} entity={selected_entity} operation={operation} succeeded={succeeded} local_instructions={local_instructions}",
            );
            assert!(local_instructions > 0);
            if matches!(operation, 0 | 1 | 2 | 4 | 6 | 7) {
                assert_eq!(succeeded, 1);
            }
        }
    }
}

#[test]
#[ignore = "requires one exact prebuilt point-query Wasm subject"]
fn repeated_dynamic_point_query_instruction_measurement() {
    let wasm_path = env::var("ICYDB_POINT_QUERY_WASM")
        .expect("ICYDB_POINT_QUERY_WASM should name the measured Wasm");
    let wasm = fs::read(&wasm_path).expect("measured point-query Wasm should read");
    let fixture = install_prebuilt_fixture_canister("one_entity_dynamic_query", wasm);
    for (shape, method) in [
        ("point", "measure_repeated_point_queries"),
        ("point_distinct", "measure_parameterized_point_queries"),
        ("scan", "measure_repeated_scan_queries"),
        ("grouped", "measure_repeated_grouped_queries"),
    ] {
        let ((executions, failures, rows, local_instructions),): ((u16, u16, u32, u64),) = fixture
            .pocket_ic()
            .query_wire(fixture.canister_id(), method, (200_u16,))
            .expect("repeated dynamic-query measurement should decode");

        assert_eq!(executions, 200);
        assert_eq!(failures, 0);
        assert_eq!(rows, 0);
        assert!(local_instructions > 0);
        println!(
            "icydb_repeated_queries wasm={wasm_path} shape={shape} executions={executions} local_instructions={local_instructions}",
        );
    }
}

#[test]
#[ignore = "requires one exact prebuilt native exact-key Wasm subject"]
fn native_exact_key_batch_instruction_measurement() {
    let wasm_path = env::var("ICYDB_EXACT_KEY_WASM")
        .expect("ICYDB_EXACT_KEY_WASM should name the measured Wasm");
    let wasm = fs::read(&wasm_path).expect("measured exact-key Wasm should read");
    let fixture = install_prebuilt_fixture_canister("one_entity_typed_query", wasm);
    for (shape, method, distinct, requested_items) in [
        (
            "dynamic_distinct",
            "measure_dynamic_key_loop",
            true,
            200_u16,
        ),
        ("exact_distinct", "measure_exact_key_batch", true, 1_000_u16),
        (
            "dynamic_duplicate",
            "measure_dynamic_key_loop",
            false,
            200_u16,
        ),
        (
            "exact_duplicate",
            "measure_exact_key_batch",
            false,
            1_000_u16,
        ),
    ] {
        let ((items, failures, rows, local_instructions),): ((u16, u16, u32, u64),) = fixture
            .pocket_ic()
            .query_wire(fixture.canister_id(), method, (requested_items, distinct))
            .expect("exact-key instruction measurement should decode");

        assert_eq!(items, requested_items);
        assert_eq!(failures, 0);
        assert_eq!(rows, 0);
        assert!(local_instructions > 0);
        println!(
            "icydb_0221_exact_keys wasm={wasm_path} shape={shape} items={items} local_instructions={local_instructions}",
        );
    }
}

#[test]
fn generated_metrics_endpoint_uses_only_wire_and_survives_invalid_requests_and_upgrade() {
    let options = CanisterBuildOptions {
        profile: CanisterWasmProfile::WasmRelease,
        sql_mode: CanisterSqlMode::Enabled,
        build_profile: CanisterBuildProfile::Production,
    };
    let artifact = build_canister_with_options("default_empty_metrics", options).unwrap();
    let manifest = inspect_canister_artifacts(artifact.as_ref()).unwrap();
    assert_eq!(manifest.wire_methods.len(), 2);
    let wasm = fs::read(artifact.as_ref()).unwrap();
    let fixture = install_prebuilt_fixture_canister("default_empty_metrics", wasm.clone());
    let endpoint = icydb::wire::Endpoint::Metrics;
    let query = |phase| {
        let report: Result<icydb::metrics::MetricsReport, icydb::Error> = fixture
            .pocket_ic()
            .query_wire(fixture.canister_id(), endpoint.name(), ())
            .unwrap();
        assert!(report.is_ok(), "{phase} metrics reply: {report:?}");
    };
    query("installed");
    let request = endpoint.encode_request(()).unwrap();
    let mut trailing = request.clone();
    trailing.push(0);
    let mut wrong_id = request.clone();
    wrong_id[2] ^= 0x80;
    let invalid = [
        Vec::new(),
        request[..10].to_vec(),
        trailing,
        wrong_id,
        endpoint.encode_response(()).unwrap(),
        vec![0; icydb::wire::MAX_REQUEST_BYTES + 1],
    ];
    for bytes in invalid {
        assert!(
            fixture
                .pocket_ic()
                .query_call(
                    fixture.canister_id(),
                    Principal::anonymous(),
                    endpoint.name(),
                    bytes
                )
                .is_err()
        );
        query("after ingress refusal");
    }
    fixture
        .pocket_ic()
        .upgrade_canister(fixture.canister_id(), wasm, Vec::new(), None)
        .unwrap();
    query("immediately after upgrade");
}

#[test]
fn wire_reply_encoding_failure_rolls_back_mutations_and_retries() {
    let wasm = icydb_testing_build::build_fixture_variant_wasm_with_profile(
        "canister_test_wire_rollback",
        "",
        "wire-rollback",
        CanisterWasmProfile::WasmRelease,
    )
    .expect("rollback fixture should build");
    let fixture = StandaloneCanisterFixture::try_install(
        icydb_testing_integration::start_fixture_pocket_ic(),
        InstallSpec {
            wasm,
            init_bytes: Vec::new(),
            cycles: 100_000_000_000_000,
            install_sender: None,
            label: Some("wire-rollback".to_string()),
        },
    )
    .expect("rollback fixture should install");
    let state = || {
        fixture
            .pocket_ic()
            .query_wire::<u64, _>(fixture.canister_id(), "wire_rollback_state", ())
            .unwrap()
    };
    let call = |bytes| {
        fixture.pocket_ic().update_call(
            fixture.canister_id(),
            Principal::anonymous(),
            "wire_rollback_apply",
            bytes,
        )
    };
    let request = |action| fixture_wire::encode_request("wire_rollback_apply", (action,)).unwrap();
    let decode = |bytes: &[u8]| {
        fixture_wire::decode_response::<Result<u64, u8>>("wire_rollback_apply", bytes).unwrap()
    };
    assert_eq!(state(), 0);

    let failure = request(WireRollbackAction::EncodingFailure);
    // The second attempt reuses the exact request bytes. Neither failed attempt
    // may leave an increment for a later successful operation to repeat.
    for expected in [0, 1] {
        for _ in 0..2 {
            let error = call(failure.clone()).expect_err("encoding failure must trap");
            assert_eq!(
                (error.reject_code, state()),
                (RejectCode::CanisterError, expected)
            );
        }
        let reply = call(request(WireRollbackAction::Commit)).unwrap();
        assert_eq!(decode(&reply), Ok(expected + 1));
        assert_eq!(state(), expected + 1);
    }

    let error_reply = call(request(WireRollbackAction::ApplicationError)).unwrap();
    assert_eq!(decode(&error_reply), Err(7));
    assert_eq!(state(), 2);
    let mut trailing = request(WireRollbackAction::Commit);
    trailing.push(0);
    for malformed in [Vec::new(), trailing] {
        let error = call(malformed).expect_err("malformed ingress must reject before mutation");
        assert_eq!(error.reject_code, RejectCode::CanisterReject);
        assert_eq!(state(), 2);
    }
    assert_eq!(
        decode(&call(request(WireRollbackAction::Commit)).unwrap()),
        Ok(3)
    );
    assert_eq!(state(), 3);
}

#[test]
#[ignore = "requires prepared pinned Node/browser dependencies and the governed PocketIC server"]
fn native_wire_browser_qualifies_the_installed_endpoint_roster() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let browser = root.join("testing/wire-browser");
    let checked = Command::new("node")
        .arg(browser.join("check.mjs"))
        .status()
        .unwrap();
    assert!(checked.success(), "native/browser frame qualification");
    let identity = Command::new("node")
        .arg(browser.join("installed.mjs"))
        .arg("--identity")
        .output()
        .unwrap();
    assert!(identity.status.success());
    let caller = Principal::from_text(String::from_utf8(identity.stdout).unwrap().trim()).unwrap();
    let wasm = icydb_testing_build::build_fixture_variant_wasm_with_profile(
        "canister_test_sql",
        "local-sql-query,test-admin-api,schema-migration-api",
        "wire-browser",
        CanisterWasmProfile::WasmRelease,
    )
    .unwrap();
    let exports = icydb_testing_build::canister_artifact::inspect_wasm_methods(&wasm).unwrap();
    let contracts: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("target/wire-browser/contracts.json")).unwrap())
            .unwrap();
    for contract in contracts["methods"].as_array().unwrap() {
        let mode = match contract["mode"].as_str().unwrap() {
            "query" => MethodMode::Query,
            "update" => MethodMode::Update,
            _ => panic!("unexpected generated browser method mode"),
        };
        assert!(
            exports.contains(&icydb_testing_build::canister_artifact::CanisterMethod {
                name: contract["name"].as_str().unwrap().to_owned(),
                mode,
            })
        );
    }
    let fixture = install_prebuilt_fixture_canister("sql", wasm);
    fixture
        .pocket_ic()
        .set_controllers(
            fixture.canister_id(),
            None,
            vec![Principal::anonymous(), caller],
        )
        .unwrap();
    let (mut instance, canister) = fixture.into_parts();
    // The admitted Testkit instance retains its server and gateway for the
    // browser call; dropping it tears down the disposable local network.
    let gateway = instance.instance.make_live(None);
    let report = root.join("target/wire-browser/installed-report.json");
    let status = Command::new("node")
        .arg(browser.join("installed.mjs"))
        .arg(gateway.as_str())
        .arg(canister.to_text())
        .arg(&report)
        .status()
        .unwrap();
    assert!(status.success(), "installed Chromium caller");
    let report: serde_json::Value = serde_json::from_slice(&fs::read(report).unwrap()).unwrap();
    let frames = report["frames"].as_array().unwrap();
    assert!(frames.len() >= 12);
    for frame in frames {
        let endpoint = icydb::wire::Endpoint::from_name(frame["method"].as_str().unwrap()).unwrap();
        let request: Vec<u8> = serde_json::from_value(frame["request"].clone()).unwrap();
        let response: Vec<u8> = serde_json::from_value(frame["response"].clone()).unwrap();
        qualify_native_browser_frame(endpoint, &request, &response);
    }
}

fn qualify_native_browser_frame(endpoint: icydb::wire::Endpoint, request: &[u8], response: &[u8]) {
    use icydb::{
        Error,
        db::{self, sql::SqlQueryResult},
        wire::Endpoint,
    };

    // Decode actual cross-language frames with the native request/reply types,
    // not a second frozen DTO definition or the generator's metadata interpreter.
    macro_rules! qualify {
        ($request:ty, $response:ty) => {{
            let decoded: $request = endpoint.decode_request(request).unwrap();
            assert_eq!(endpoint.encode_request(decoded).unwrap(), request);
            let decoded: $response = endpoint.decode_response(response).unwrap();
            assert_eq!(endpoint.encode_response(decoded).unwrap(), response);
        }};
    }
    match endpoint {
        Endpoint::Query | Endpoint::Ddl | Endpoint::Update => {
            qualify!(String, Result<SqlQueryResult, Error>);
        }
        Endpoint::Integrity => {
            qualify!(String, Result<db::IntegrityCheckResult, db::SqlIntegrityError>);
        }
        Endpoint::FixturesReset | Endpoint::FixturesLoad | Endpoint::MetricsReset => {
            qualify!((), Result<(), Error>);
        }
        Endpoint::Metrics => qualify!((), Result<icydb::metrics::MetricsReport, Error>),
        Endpoint::Snapshot => qualify!((), Result<db::StorageReport, Error>),
        Endpoint::Schema => qualify!((), Result<Vec<db::EntitySchemaDescription>, Error>),
        Endpoint::SchemaMigrate => {
            qualify!(db::SchemaMigrationCommand, Result<db::SchemaMigrationStatusPage, Error>);
        }
        Endpoint::SchemaMigration => {
            qualify!(db::SchemaMigrationStatusRequest, Result<db::SchemaMigrationStatusPage, Error>);
        }
    }
}
