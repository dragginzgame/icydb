//! Controller-only allocation reporting through the generated snapshot endpoint.

use ic_platform::principal::ExternalPrincipal as Principal;
use icydb::{
    Error, ErrorOrigin,
    db::StorageReport,
    diagnostic::RuntimeBoundaryCode,
};
use icydb_testing_build::{
    CanisterBuildOptions, CanisterBuildProfile, build_fixture_canister_wasm_bytes_with_options,
};
use icydb_testing_integration::WireCallExt as _;
use icydb_testing_integration::install_prebuilt_fixture_canister;

#[test]
fn generated_snapshot_reports_allocation_totals_without_mutation() {
    let wasm = build_fixture_canister_wasm_bytes_with_options(
        "sql",
        CanisterBuildOptions {
            build_profile: CanisterBuildProfile::Production,
            ..CanisterBuildOptions::default()
        },
    );
    let fixture = install_prebuilt_fixture_canister("sql", wasm);
    let before = fixture.pocket_ic().get_stable_memory(fixture.canister_id());
    let response: Result<StorageReport, Error> = fixture
        .pocket_ic()
        .query_wire(fixture.canister_id(), "icydb_snapshot", ())
        .unwrap();
    let report = response.unwrap();
    let allocations = report.memory_allocations().unwrap();
    assert!(allocations.current_generation > 0);
    assert!(!allocations.memories.is_empty());
    assert_eq!(allocations.virtual_extent.bytes, allocations.virtual_extent.wasm_pages * 65_536);
    assert_eq!(allocations.virtual_extent.bytes,
        allocations.memories.iter().map(|entry| entry.virtual_extent.bytes).sum::<u64>());
    assert!(allocations.memories.iter().all(|entry| entry.allocation_id > 0 && entry.stable_key.starts_with("icydb.")));

    let repeated: Result<StorageReport, Error> = fixture
        .pocket_ic()
        .query_wire(fixture.canister_id(), "icydb_snapshot", ())
        .unwrap();
    assert_eq!(repeated.unwrap().memory_allocations(), Some(allocations));
    let denied: Result<StorageReport, Error> = fixture
        .pocket_ic()
        .query_wire_as(
            fixture.canister_id(),
            Principal::self_authenticating([42; 32]),
            "icydb_snapshot",
            (),
        )
        .unwrap();
    assert_eq!(
        denied.expect_err("allocation diagnostics require the operational controller"),
        Error::from_runtime_boundary(
            RuntimeBoundaryCode::OperationalSurfaceControllerRequired,
            ErrorOrigin::Interface,
        ),
    );
    assert_eq!(
        fixture.pocket_ic().get_stable_memory(fixture.canister_id()),
        before
    );
}
